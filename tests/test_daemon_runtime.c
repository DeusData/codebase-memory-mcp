/*
 * test_daemon_runtime.c — RED contract for the mandatory daemon runtime.
 *
 * These tests exercise a real in-process service over the authenticated IPC
 * transport. A focused isolated host child also pins listener-before-background
 * ordering; frontend bootstrap remains covered at its own layer.
 */
#include "test_framework.h"
#include "test_helpers.h"
#include "test_daemon_runtime_contract.h"

#include "daemon/application.h"
#include "daemon/host.h"
#include "daemon/host_internal.h"
#include "daemon/ipc.h"
#include "daemon/runtime.h"
#include "daemon/service.h"
#include "daemon/version_cohort.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/compat_thread.h"
#include "foundation/log.h"
#include "foundation/platform.h"
#include "pipeline/pipeline.h"
#include "store/store.h"

#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdatomic.h>
#include <string.h>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include "foundation/win_utf8.h"
#include <stddef.h>
#include <windows.h>
#else
#include <errno.h>
#include <fcntl.h>
#ifdef __APPLE__
#include <libproc.h>
#endif
#include <signal.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

enum {
    RUNTIME_TEST_PATH_CAP = 1024,
    RUNTIME_TEST_LOG_CAP = 4096,
    RUNTIME_TEST_TIMEOUT_MS = 2000,
    RUNTIME_TEST_CLEANUP_TIMEOUT_MS = 60000,
    RUNTIME_TEST_CLEANUP_FREE_ATTEMPTS = 3,
    /* Generation-zero rendezvous layout, deliberately repeated rather than
     * derived from production macros so an accidental resize fails loudly. */
    RUNTIME_TEST_RENDEZVOUS_ABI = 1,
    RUNTIME_TEST_RENDEZVOUS_REQUEST_SIZE = 133,
    RUNTIME_TEST_RENDEZVOUS_RESPONSE_SIZE = 798,
    RUNTIME_TEST_RENDEZVOUS_VERSION_OFFSET = 4,
    RUNTIME_TEST_RENDEZVOUS_BUILD_OFFSET = 68,
    RUNTIME_TEST_RENDEZVOUS_ACTIVE_VERSION_OFFSET = 28,
    RUNTIME_TEST_RENDEZVOUS_ACTIVE_BUILD_OFFSET = 92,
    RUNTIME_TEST_RENDEZVOUS_REQUESTED_VERSION_OFFSET = 157,
    RUNTIME_TEST_RENDEZVOUS_REQUESTED_BUILD_OFFSET = 221,
    RUNTIME_TEST_RENDEZVOUS_MESSAGE_OFFSET = 286,
    /* Activation shutdown is a separate frozen first-frame envelope:
     * u32 action followed byte-for-byte by the 133-byte identity above.
     * Response: u32 ABI, u32 accepted, u64 clients, u64 connections. */
    RUNTIME_TEST_ACTIVATION_REQUEST_SIZE = 137,
    RUNTIME_TEST_ACTIVATION_RESPONSE_SIZE = 24,
    RUNTIME_TEST_ACTIVATION_IDENTITY_OFFSET = 4,
    RUNTIME_TEST_ACTIVATION_RESPONSE_CLIENTS_OFFSET = 8,
    RUNTIME_TEST_ACTIVATION_RESPONSE_CONNECTIONS_OFFSET = 16,
};

#define RUNTIME_TEST_BLOCKING_GIT_MARKER_ENV "CBM_TEST_RUNTIME_BLOCKING_GIT_PID_FILE"

_Static_assert(CBM_DAEMON_RUNTIME_OP_ACTIVATION_SHUTDOWN == 8 &&
                   CBM_DAEMON_ACTIVATION_SHUTDOWN_REQUEST_SIZE ==
                       RUNTIME_TEST_ACTIVATION_REQUEST_SIZE &&
                   CBM_DAEMON_ACTIVATION_SHUTDOWN_RESPONSE_SIZE ==
                       RUNTIME_TEST_ACTIVATION_RESPONSE_SIZE,
               "activation shutdown frozen wire contract changed");

static const char RUNTIME_BUILD_B[] =
    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
#ifndef _WIN32
static const char RUNTIME_CACHE_A[] =
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
#endif
static char runtime_self_build[CBM_DAEMON_BUILD_FINGERPRINT_SIZE];
static atomic_bool runtime_conflict_log_fallback_seen;
static atomic_bool runtime_activation_shutdown_log_seen;

static void runtime_test_conflict_log_fallback_sink(const char *line) {
    if (line && strstr(line, "daemon.conflict_log_append_failed")) {
        atomic_store_explicit(&runtime_conflict_log_fallback_seen, true, memory_order_release);
    }
}

static void runtime_test_activation_shutdown_sink(const char *line) {
    if (line && strstr(line, "daemon.activation_shutdown") && strstr(line, "requester_pid") &&
        strstr(line, "requester_build") && strstr(line, "action") &&
        strstr(line, "active_clients") && strstr(line, "active_connections")) {
        atomic_store_explicit(&runtime_activation_shutdown_log_seen, true, memory_order_release);
    }
}

typedef struct {
    char parent[RUNTIME_TEST_PATH_CAP];
    char runtime_dir[RUNTIME_TEST_PATH_CAP];
    char key[CBM_DAEMON_KEY_SIZE];
    char log_path[RUNTIME_TEST_PATH_CAP];
    char rotated_log_path[RUNTIME_TEST_PATH_CAP];
    char lock_log_path[RUNTIME_TEST_PATH_CAP];
    cbm_daemon_ipc_endpoint_t *endpoint;
    cbm_daemon_runtime_service_t *service;
} runtime_test_fixture_t;

typedef struct {
    atomic_int opened;
    atomic_int requests;
    atomic_int request_cancels;
    atomic_int cancelled;
    atomic_int closed;
    atomic_bool block_first_request;
    atomic_bool first_request_started;
    atomic_bool ignore_first_request_cancel;
    atomic_bool release_first_request;
    atomic_bool block_second_open;
    atomic_bool second_open_started;
    atomic_bool release_second_open;
} runtime_application_context_t;

typedef struct {
    runtime_application_context_t *context;
    atomic_bool cancel_requested;
    cbm_daemon_client_id_t client_id;
    uint64_t authenticated_process_id;
} runtime_application_session_t;

typedef struct {
    cbm_daemon_runtime_client_t *client;
    const uint8_t *request;
    uint32_t request_length;
    cbm_daemon_runtime_application_token_t request_token;
    bool tagged;
    atomic_bool *completed;
    cbm_daemon_runtime_application_status_t status;
    uint8_t *response;
    uint32_t response_length;
} runtime_application_client_call_t;

typedef struct {
    cbm_daemon_runtime_client_t *client;
    char arguments[RUNTIME_TEST_PATH_CAP + 64];
    uint32_t timeout_ms;
    atomic_bool completed;
    cbm_daemon_runtime_application_status_t status;
    uint8_t *response;
    uint32_t response_length;
} runtime_real_application_call_t;

typedef struct {
    const cbm_daemon_ipc_endpoint_t *endpoint;
    cbm_daemon_build_identity_t identity;
    cbm_daemon_runtime_connect_result_t result;
    cbm_daemon_runtime_client_t *client;
    atomic_bool completed;
} runtime_application_connect_call_t;

static cbm_daemon_build_identity_t runtime_test_identity(const char *version, const char *build) {
    cbm_daemon_build_identity_t identity = {
        .semantic_version = version,
        .build_fingerprint = build,
        .protocol_abi = 3,
        .store_abi = 11,
        .feature_abi = 7,
    };
    return identity;
}

static uint64_t runtime_test_process_id(void) {
#ifdef _WIN32
    return (uint64_t)GetCurrentProcessId();
#else
    return (uint64_t)getpid();
#endif
}

static const char *runtime_test_self_build(void) {
    if (runtime_self_build[0] == '\0') {
        (void)cbm_daemon_runtime_process_build_fingerprint(runtime_test_process_id(),
                                                           runtime_self_build);
    }
    return runtime_self_build;
}

static bool runtime_test_is_fingerprint(const char *value) {
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

static bool runtime_test_copy_path(char out[RUNTIME_TEST_PATH_CAP], const char *path) {
    if (!path) {
        out[0] = '\0';
        return false;
    }
    int written = snprintf(out, RUNTIME_TEST_PATH_CAP, "%s", path);
    return written >= 0 && written < RUNTIME_TEST_PATH_CAP;
}

#ifdef _WIN32

enum {
    RUNTIME_TEST_FILE_RENAME_INFO_EX = 22,
    RUNTIME_TEST_FILE_RENAME_REPLACE_IF_EXISTS = 0x00000001,
    RUNTIME_TEST_FILE_RENAME_POSIX_SEMANTICS = 0x00000002,
};

typedef struct {
    DWORD flags;
    HANDLE root_directory;
    DWORD file_name_length;
    wchar_t file_name[1];
} runtime_test_windows_rename_info_t;

static bool runtime_test_windows_copy_self(const char *destination) {
    wchar_t source[32768];
    DWORD source_length =
        GetModuleFileNameW(NULL, source, (DWORD)(sizeof(source) / sizeof(source[0])));
    wchar_t *destination_wide = cbm_utf8_to_wide(destination);
    bool copied = source_length > 0 &&
                  source_length < (DWORD)(sizeof(source) / sizeof(source[0])) && destination_wide &&
                  CopyFileW(source, destination_wide, TRUE) != 0;
    free(destination_wide);
    return copied;
}

static bool runtime_test_windows_wait_image_probe(HANDLE process) {
    if (!process) {
        return false;
    }
    DWORD wait_status = WaitForSingleObject(process, TF_RUNTIME_IMAGE_WATCHDOG_MS);
    if (wait_status == WAIT_OBJECT_0) {
        return true;
    }
    (void)TerminateProcess(process, 30);
    if (WaitForSingleObject(process, RUNTIME_TEST_TIMEOUT_MS) != WAIT_OBJECT_0) {
        fprintf(stderr, "daemon_runtime copied-image child could not be reaped\n");
        abort();
    }
    return false;
}

static bool runtime_test_windows_posix_replace(const char *source, const char *destination) {
    wchar_t *source_wide = cbm_utf8_to_wide(source);
    wchar_t *destination_wide = cbm_utf8_to_wide(destination);
    size_t destination_length = destination_wide ? wcslen(destination_wide) : 0;
    size_t destination_bytes = destination_length * sizeof(wchar_t);
    size_t information_size = offsetof(runtime_test_windows_rename_info_t, file_name) +
                              destination_bytes + sizeof(wchar_t);
    bool sizes_ok =
        destination_length > 0 && destination_bytes <= MAXDWORD && information_size <= MAXDWORD;
    runtime_test_windows_rename_info_t *information = sizes_ok ? calloc(1, information_size) : NULL;
    if (information) {
        information->flags =
            RUNTIME_TEST_FILE_RENAME_REPLACE_IF_EXISTS | RUNTIME_TEST_FILE_RENAME_POSIX_SEMANTICS;
        information->file_name_length = (DWORD)destination_bytes;
        memcpy(information->file_name, destination_wide, destination_bytes + sizeof(wchar_t));
    }
    HANDLE source_file =
        source_wide && information
            ? CreateFileW(source_wide, DELETE | SYNCHRONIZE,
                          FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL,
                          OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT, NULL)
            : INVALID_HANDLE_VALUE;
    bool replaced = source_file != INVALID_HANDLE_VALUE &&
                    SetFileInformationByHandle(
                        source_file, (FILE_INFO_BY_HANDLE_CLASS)RUNTIME_TEST_FILE_RENAME_INFO_EX,
                        information, (DWORD)information_size) != 0;
    if (source_file != INVALID_HANDLE_VALUE && !CloseHandle(source_file)) {
        replaced = false;
    }
    free(information);
    free(destination_wide);
    free(source_wide);
    return replaced;
}

static bool runtime_test_windows_spawn_image_holder(const char *image_path, const char *ready_event,
                                                    PROCESS_INFORMATION *process_out) {
    if (!image_path || !ready_event || !process_out) {
        return false;
    }
    memset(process_out, 0, sizeof(*process_out));
    char command_line[RUNTIME_TEST_PATH_CAP * 2];
    int written = snprintf(command_line, sizeof(command_line),
                           "\"%s\" __cbm_runtime_image_holder \"%s\"", image_path, ready_event);
    wchar_t *application = cbm_utf8_to_wide(image_path);
    wchar_t *command =
        written > 0 && written < (int)sizeof(command_line) ? cbm_utf8_to_wide(command_line) : NULL;
    STARTUPINFOW startup;
    memset(&startup, 0, sizeof(startup));
    startup.cb = sizeof(startup);
    bool created = application && command &&
                   CreateProcessW(application, command, NULL, NULL, FALSE, CREATE_NO_WINDOW, NULL,
                                  NULL, &startup, process_out) != 0;
    free(command);
    free(application);
    if (created) {
        (void)CloseHandle(process_out->hThread);
        process_out->hThread = NULL;
    }
    return created;
}

#endif

#if defined(__APPLE__) || defined(__linux__)

static bool runtime_test_copy_executable(const char *source, const char *destination) {
    int source_fd = open(source, O_RDONLY | O_CLOEXEC);
    int destination_fd =
        source_fd >= 0 ? open(destination, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0700) : -1;
    unsigned char buffer[64 * 1024];
    bool ok = source_fd >= 0 && destination_fd >= 0;
    while (ok) {
        ssize_t count = read(source_fd, buffer, sizeof(buffer));
        if (count == 0) {
            break;
        }
        if (count < 0) {
            if (errno == EINTR) {
                continue;
            }
            ok = false;
            break;
        }
        size_t written = 0;
        while (written < (size_t)count) {
            ssize_t chunk = write(destination_fd, buffer + written, (size_t)count - written);
            if (chunk > 0) {
                written += (size_t)chunk;
            } else if (chunk < 0 && errno == EINTR) {
                continue;
            } else {
                ok = false;
                break;
            }
        }
    }
    if (destination_fd >= 0 && fchmod(destination_fd, 0700) != 0) {
        ok = false;
    }
    if (destination_fd >= 0 && close(destination_fd) != 0) {
        ok = false;
    }
    if (source_fd >= 0 && close(source_fd) != 0) {
        ok = false;
    }
    if (!ok) {
        (void)unlink(destination);
    }
    return ok;
}

static pid_t runtime_test_spawn_blocked_executable(const char *path, int *release_fd_out) {
    int ready[2] = {-1, -1};
    int input[2] = {-1, -1};
    if (!path || !release_fd_out || pipe(ready) != 0 || pipe(input) != 0) {
        if (ready[0] >= 0) {
            (void)close(ready[0]);
            (void)close(ready[1]);
        }
        if (input[0] >= 0) {
            (void)close(input[0]);
            (void)close(input[1]);
        }
        return -1;
    }
    int ready_flags = fcntl(ready[1], F_GETFD);
    if (ready_flags < 0 || fcntl(ready[1], F_SETFD, ready_flags | FD_CLOEXEC) != 0) {
        (void)close(ready[0]);
        (void)close(ready[1]);
        (void)close(input[0]);
        (void)close(input[1]);
        return -1;
    }
    pid_t child = fork();
    if (child == 0) {
        (void)close(ready[0]);
        (void)close(input[1]);
        if (dup2(input[0], STDIN_FILENO) < 0) {
            _exit(126);
        }
        if (input[0] != STDIN_FILENO) {
            (void)close(input[0]);
        }
        execl(path, path, (char *)NULL);
        const char failed = 'x';
        (void)write(ready[1], &failed, 1);
        _exit(127);
    }
    (void)close(ready[1]);
    (void)close(input[0]);
    char unexpected = '\0';
    ssize_t ready_count;
    do {
        ready_count = read(ready[0], &unexpected, 1);
    } while (ready_count < 0 && errno == EINTR);
    (void)close(ready[0]);
    int status = 0;
    bool running = child > 0 && ready_count == 0 && waitpid(child, &status, WNOHANG) == 0;
    if (!running) {
        (void)close(input[1]);
        if (child > 0) {
            (void)kill(child, SIGKILL);
            while (waitpid(child, &status, 0) < 0 && errno == EINTR) {}
        }
        return -1;
    }
    *release_fd_out = input[1];
    return child;
}

static void runtime_test_stop_blocked_executable(pid_t child, int release_fd) {
    if (release_fd >= 0) {
        (void)close(release_fd);
    }
    if (child <= 0) {
        return;
    }
    (void)kill(child, SIGTERM);
    int status = 0;
    while (waitpid(child, &status, 0) < 0 && errno == EINTR) {}
}

#endif

#if defined(__APPLE__) || defined(__linux__)
static bool runtime_test_self_image_path(char source[RUNTIME_TEST_PATH_CAP]) {
#ifdef __APPLE__
    int length = proc_pidpath(getpid(), source, RUNTIME_TEST_PATH_CAP);
    bool resolved = length > 0 && length < RUNTIME_TEST_PATH_CAP;
    if (resolved) {
        source[length] = '\0';
    }
#else
    ssize_t length = readlink("/proc/self/exe", source, RUNTIME_TEST_PATH_CAP - 1);
    bool resolved = length > 0 && length < (ssize_t)RUNTIME_TEST_PATH_CAP - 1;
    if (resolved) {
        source[length] = '\0';
    }
#endif
    return resolved;
}
#endif

static bool runtime_test_copy_self_image(const char *destination) {
#ifdef _WIN32
    return runtime_test_windows_copy_self(destination);
#elif defined(__APPLE__) || defined(__linux__)
    char source[RUNTIME_TEST_PATH_CAP];
    return runtime_test_self_image_path(source) &&
           runtime_test_copy_executable(source, destination);
#else
    (void)destination;
    return false;
#endif
}

static bool runtime_test_paths_refer_to_same_file(const char *left, const char *right) {
    enum { CANONICAL_CAP = 4096 };
    char left_canonical[CANONICAL_CAP];
    char right_canonical[CANONICAL_CAP];
    if (!left || !right || !cbm_canonical_path(left, left_canonical, sizeof(left_canonical)) ||
        !cbm_canonical_path(right, right_canonical, sizeof(right_canonical))) {
        return false;
    }
#ifdef _WIN32
    return _stricmp(left_canonical, right_canonical) == 0;
#else
    return strcmp(left_canonical, right_canonical) == 0;
#endif
}

static bool runtime_test_process_image_matches(uint64_t process_id, const char *expected_image) {
    if (process_id <= 1 || process_id == runtime_test_process_id() || !expected_image) {
        return false;
    }
    char observed[4096] = {0};
#ifdef _WIN32
    if (process_id > UINT32_MAX) {
        return false;
    }
    HANDLE process = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, FALSE, (DWORD)process_id);
    wchar_t wide[32768];
    DWORD wide_length = (DWORD)(sizeof(wide) / sizeof(wide[0]));
    bool queried = process && QueryFullProcessImageNameW(process, 0, wide, &wide_length) != 0;
    char *utf8 = queried ? cbm_wide_to_utf8(wide) : NULL;
    if (utf8) {
        (void)snprintf(observed, sizeof(observed), "%s", utf8);
    }
    free(utf8);
    if (process) {
        (void)CloseHandle(process);
    }
#elif defined(__APPLE__)
    int length = process_id <= INT_MAX
                     ? proc_pidpath((int)process_id, observed, (uint32_t)sizeof(observed))
                     : 0;
    if (length <= 0 || length >= (int)sizeof(observed)) {
        observed[0] = '\0';
    }
#elif defined(__linux__)
    char proc_path[64];
    int written =
        snprintf(proc_path, sizeof(proc_path), "/proc/%llu/exe", (unsigned long long)process_id);
    ssize_t length = written > 0 && written < (int)sizeof(proc_path)
                         ? readlink(proc_path, observed, sizeof(observed) - 1)
                         : -1;
    if (length > 0 && length < (ssize_t)sizeof(observed)) {
        observed[length] = '\0';
    } else {
        observed[0] = '\0';
    }
#else
    (void)process_id;
#endif
    return observed[0] && runtime_test_paths_refer_to_same_file(observed, expected_image);
}

static bool runtime_test_wait_pid_marker(const char *path, uint32_t timeout_ms,
                                         uint64_t *process_id_out) {
    if (process_id_out) {
        *process_id_out = 0;
    }
    uint64_t deadline = cbm_now_ms() + timeout_ms;
    do {
        FILE *marker = cbm_fopen(path, "rb");
        unsigned long long parsed = 0;
        bool valid = marker && fscanf(marker, "%llu", &parsed) == 1 && parsed > 1 &&
                     parsed != runtime_test_process_id();
        if (marker) {
            (void)fclose(marker);
        }
        if (valid) {
            if (process_id_out) {
                *process_id_out = (uint64_t)parsed;
            }
            return true;
        }
        cbm_usleep(1000);
    } while (cbm_now_ms() < deadline);
    return false;
}

static bool runtime_test_wait_process_image_gone(uint64_t process_id, const char *expected_image,
                                                 uint32_t timeout_ms) {
    uint64_t deadline = cbm_now_ms() + timeout_ms;
    do {
        if (!runtime_test_process_image_matches(process_id, expected_image)) {
            return true;
        }
        cbm_usleep(1000);
    } while (cbm_now_ms() < deadline);
    return !runtime_test_process_image_matches(process_id, expected_image);
}

static bool runtime_test_force_terminate_verified(uint64_t process_id, const char *expected_image) {
    if (!runtime_test_process_image_matches(process_id, expected_image)) {
        return true;
    }
#ifdef _WIN32
    HANDLE process =
        process_id <= UINT32_MAX
            ? OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_TERMINATE | SYNCHRONIZE,
                          FALSE, (DWORD)process_id)
            : NULL;
    if (!process) {
        return false;
    }
    wchar_t wide[32768];
    DWORD wide_length = (DWORD)(sizeof(wide) / sizeof(wide[0]));
    bool queried = QueryFullProcessImageNameW(process, 0, wide, &wide_length) != 0;
    char *utf8 = queried ? cbm_wide_to_utf8(wide) : NULL;
    bool exact = utf8 && runtime_test_paths_refer_to_same_file(utf8, expected_image);
    free(utf8);
    bool terminated = exact && TerminateProcess(process, 99) != 0;
    if (terminated) {
        terminated = WaitForSingleObject(process, RUNTIME_TEST_TIMEOUT_MS) == WAIT_OBJECT_0;
    }
    (void)CloseHandle(process);
    return terminated;
#elif defined(__APPLE__) || defined(__linux__)
    /* The marker lives in a private test directory and was written by this
     * exact copied image. Revalidate immediately before signaling so the
     * cleanup backstop never targets an unrelated or PID-reused process. */
    return runtime_test_process_image_matches(process_id, expected_image) &&
           kill((pid_t)process_id, SIGKILL) == 0 &&
           runtime_test_wait_process_image_gone(process_id, expected_image,
                                                RUNTIME_TEST_TIMEOUT_MS);
#else
    return false;
#endif
}

#if defined(_WIN32) || defined(__linux__)
static bool runtime_test_append_image_marker(const char *path) {
    FILE *file = cbm_fopen(path, "ab");
    bool written = file && fputc('\n', file) != EOF;
    if (file) {
        written = fclose(file) == 0 && written;
    }
    return written;
}
#endif

#ifdef __APPLE__
static bool runtime_test_mac_ad_hoc_sign(const char *path) {
    if (!path) {
        return false;
    }
    pid_t child = fork();
    if (child == 0) {
        execl("/usr/bin/codesign", "codesign", "--force", "--sign", "-", "--timestamp=none",
              "--identifier", "org.deusdata.cbm.foreign-test", path, (char *)NULL);
        _exit(127);
    }
    int status = 0;
    pid_t waited;
    do {
        waited = child > 0 ? waitpid(child, &status, 0) : -1;
    } while (waited < 0 && errno == EINTR);
    return waited == child && WIFEXITED(status) && WEXITSTATUS(status) == 0;
}
#endif

static bool runtime_test_run_hello_image(const char *image_path,
                                         const runtime_test_fixture_t *fixture,
                                         const cbm_daemon_build_identity_t *identity,
                                         int *exit_code_out) {
    if (!image_path || !fixture || !identity || !identity->semantic_version ||
        !identity->build_fingerprint || !exit_code_out) {
        return false;
    }
    *exit_code_out = -1;
#ifdef _WIN32
    char command_line[RUNTIME_TEST_PATH_CAP * 3];
    int written =
        snprintf(command_line, sizeof(command_line),
                 "\"%s\" __cbm_runtime_hello_client \"%s\" %s %s %s", image_path, fixture->parent,
                 fixture->key, identity->semantic_version, identity->build_fingerprint);
    wchar_t *application = cbm_utf8_to_wide(image_path);
    wchar_t *command =
        written > 0 && written < (int)sizeof(command_line) ? cbm_utf8_to_wide(command_line) : NULL;
    STARTUPINFOW startup;
    PROCESS_INFORMATION process;
    memset(&startup, 0, sizeof(startup));
    memset(&process, 0, sizeof(process));
    startup.cb = sizeof(startup);
    bool started = application && command &&
                   CreateProcessW(application, command, NULL, NULL, FALSE, CREATE_NO_WINDOW, NULL,
                                  NULL, &startup, &process) != 0;
    free(command);
    free(application);
    bool waited = started && runtime_test_windows_wait_image_probe(process.hProcess);
    DWORD exit_code = 0;
    bool read = waited && GetExitCodeProcess(process.hProcess, &exit_code) != 0;
    if (started) {
        (void)CloseHandle(process.hThread);
        (void)CloseHandle(process.hProcess);
    }
    if (read && exit_code <= INT_MAX) {
        *exit_code_out = (int)exit_code;
    }
    return read && exit_code <= INT_MAX;
#elif defined(__APPLE__) || defined(__linux__)
    pid_t child = fork();
    if (child == 0) {
        (void)alarm(TF_RUNTIME_IMAGE_WATCHDOG_SECONDS);
        execl(image_path, image_path, "__cbm_runtime_hello_client", fixture->parent, fixture->key,
              identity->semantic_version, identity->build_fingerprint, (char *)NULL);
        _exit(127);
    }
    int status = 0;
    pid_t waited;
    do {
        waited = child > 0 ? waitpid(child, &status, 0) : -1;
    } while (waited < 0 && errno == EINTR);
    if (waited != child || !WIFEXITED(status)) {
        return false;
    }
    *exit_code_out = WEXITSTATUS(status);
    return true;
#else
    (void)image_path;
    (void)fixture;
    (void)identity;
    return false;
#endif
}

static bool runtime_test_run_activation_image(const char *image_path,
                                              const runtime_test_fixture_t *fixture,
                                              const cbm_daemon_build_identity_t *identity,
                                              cbm_daemon_runtime_activation_action_t action,
                                              int *exit_code_out) {
    if (!image_path || !fixture || !identity || !identity->semantic_version ||
        !identity->build_fingerprint || !exit_code_out) {
        return false;
    }
    *exit_code_out = -1;
#ifdef _WIN32
    char command_line[RUNTIME_TEST_PATH_CAP * 3];
    int written = snprintf(command_line, sizeof(command_line),
                           "\"%s\" __cbm_runtime_activation_client \"%s\" %s %s %s %u", image_path,
                           fixture->parent, fixture->key, identity->semantic_version,
                           identity->build_fingerprint, (unsigned int)action);
    wchar_t *application = cbm_utf8_to_wide(image_path);
    wchar_t *command =
        written > 0 && written < (int)sizeof(command_line) ? cbm_utf8_to_wide(command_line) : NULL;
    STARTUPINFOW startup;
    PROCESS_INFORMATION process;
    memset(&startup, 0, sizeof(startup));
    memset(&process, 0, sizeof(process));
    startup.cb = sizeof(startup);
    bool started = application && command &&
                   CreateProcessW(application, command, NULL, NULL, FALSE, CREATE_NO_WINDOW, NULL,
                                  NULL, &startup, &process) != 0;
    free(command);
    free(application);
    bool waited = started && runtime_test_windows_wait_image_probe(process.hProcess);
    DWORD exit_code = 0;
    bool read = waited && GetExitCodeProcess(process.hProcess, &exit_code) != 0;
    if (started) {
        (void)CloseHandle(process.hThread);
        (void)CloseHandle(process.hProcess);
    }
    if (read && exit_code <= INT_MAX) {
        *exit_code_out = (int)exit_code;
    }
    return read && exit_code <= INT_MAX;
#elif defined(__APPLE__) || defined(__linux__)
    char action_text[16];
    int action_written = snprintf(action_text, sizeof(action_text), "%u", (unsigned int)action);
    pid_t child = action_written > 0 && action_written < (int)sizeof(action_text) ? fork() : -1;
    if (child == 0) {
        (void)alarm(TF_RUNTIME_IMAGE_WATCHDOG_SECONDS);
        execl(image_path, image_path, "__cbm_runtime_activation_client", fixture->parent,
              fixture->key, identity->semantic_version, identity->build_fingerprint, action_text,
              (char *)NULL);
        _exit(127);
    }
    int status = 0;
    pid_t waited;
    do {
        waited = child > 0 ? waitpid(child, &status, 0) : -1;
    } while (waited < 0 && errno == EINTR);
    if (waited != child || !WIFEXITED(status)) {
        return false;
    }
    *exit_code_out = WEXITSTATUS(status);
    return true;
#else
    (void)image_path;
    (void)fixture;
    (void)identity;
    (void)action;
    return false;
#endif
}

#ifdef __APPLE__
static bool runtime_test_run_mapped_hello_image(const char *image_path,
                                                const char *mapped_image_path,
                                                const runtime_test_fixture_t *fixture,
                                                const cbm_daemon_build_identity_t *identity,
                                                int *exit_code_out) {
    if (!image_path || !mapped_image_path || !fixture || !identity || !identity->semantic_version ||
        !identity->build_fingerprint || !exit_code_out) {
        return false;
    }
    *exit_code_out = -1;
    pid_t child = fork();
    if (child == 0) {
        execl(image_path, image_path, "__cbm_runtime_mapped_hello_client", mapped_image_path,
              fixture->parent, fixture->key, identity->semantic_version,
              identity->build_fingerprint, (char *)NULL);
        _exit(127);
    }
    int status = 0;
    pid_t waited;
    do {
        waited = child > 0 ? waitpid(child, &status, 0) : -1;
    } while (waited < 0 && errno == EINTR);
    if (waited != child || !WIFEXITED(status)) {
        return false;
    }
    *exit_code_out = WEXITSTATUS(status);
    return true;
}
#endif

static cbm_daemon_runtime_application_session_t *runtime_application_session_open(
    void *opaque, cbm_daemon_client_id_t client_id, uint64_t authenticated_process_id) {
    runtime_application_context_t *context = opaque;
    runtime_application_session_t *session = calloc(1, sizeof(*session));
    if (!context || !session || client_id == CBM_DAEMON_CLIENT_ID_INVALID ||
        authenticated_process_id == 0) {
        free(session);
        return NULL;
    }
    session->context = context;
    session->client_id = client_id;
    session->authenticated_process_id = authenticated_process_id;
    atomic_init(&session->cancel_requested, false);
    int open_index = atomic_fetch_add_explicit(&context->opened, 1, memory_order_relaxed);
    if (open_index == 1 &&
        atomic_load_explicit(&context->block_second_open, memory_order_acquire)) {
        atomic_store_explicit(&context->second_open_started, true, memory_order_release);
        while (!atomic_load_explicit(&context->release_second_open, memory_order_acquire)) {
            struct timespec pause = {.tv_sec = 0, .tv_nsec = 1000000};
            (void)cbm_nanosleep(&pause, NULL);
        }
    }
    return (cbm_daemon_runtime_application_session_t *)session;
}

static cbm_daemon_runtime_application_status_t runtime_application_request(
    void *opaque, cbm_daemon_runtime_application_session_t *opaque_session,
    cbm_daemon_runtime_application_token_t request_token, const uint8_t *request,
    uint32_t request_length, uint8_t **response_out, uint32_t *response_length_out) {
    (void)request_token;
    runtime_application_context_t *context = opaque;
    runtime_application_session_t *session = (runtime_application_session_t *)opaque_session;
    if (!context || !session || session->context != context || !response_out ||
        !response_length_out || (request_length > 0 && !request)) {
        return CBM_DAEMON_RUNTIME_APPLICATION_HANDLER_ERROR;
    }
    *response_out = NULL;
    *response_length_out = 0;
    int request_index = atomic_fetch_add_explicit(&context->requests, 1, memory_order_relaxed);
    if (request_index == 0 &&
        atomic_load_explicit(&context->block_first_request, memory_order_acquire)) {
        atomic_store_explicit(&context->first_request_started, true, memory_order_release);
        bool ignore_cancel =
            atomic_load_explicit(&context->ignore_first_request_cancel, memory_order_acquire);
        while (!(ignore_cancel
                     ? atomic_load_explicit(&context->release_first_request, memory_order_acquire)
                     : atomic_load_explicit(&session->cancel_requested, memory_order_acquire))) {
            struct timespec pause = {.tv_sec = 0, .tv_nsec = 1000000};
            (void)cbm_nanosleep(&pause, NULL);
        }
        return CBM_DAEMON_RUNTIME_APPLICATION_CANCELLED;
    }
    if (request_length == 0) {
        return CBM_DAEMON_RUNTIME_APPLICATION_OK;
    }
    uint8_t *response = malloc(request_length);
    if (!response) {
        return CBM_DAEMON_RUNTIME_APPLICATION_HANDLER_ERROR;
    }
    memcpy(response, request, request_length);
    *response_out = response;
    *response_length_out = request_length;
    return CBM_DAEMON_RUNTIME_APPLICATION_OK;
}

static void *runtime_application_client_request_thread(void *opaque) {
    runtime_application_client_call_t *call = opaque;
    call->status = call->tagged
                       ? cbm_daemon_runtime_client_application_request_tagged(
                             call->client, call->request_token, call->request, call->request_length,
                             &call->response, &call->response_length, RUNTIME_TEST_TIMEOUT_MS)
                       : cbm_daemon_runtime_client_application_request(
                             call->client, call->request, call->request_length, &call->response,
                             &call->response_length, RUNTIME_TEST_TIMEOUT_MS);
    if (call->completed) {
        /* This must remain the helper's final access to call/client state. A
         * failed OS join can then fall back to this lifetime sentinel without
         * racing stack teardown or client cleanup. */
        atomic_store_explicit(call->completed, true, memory_order_release);
    }
    return NULL;
}

static void *runtime_real_application_detect_changes_thread(void *opaque) {
    runtime_real_application_call_t *call = opaque;
    uint32_t timeout_ms = call->timeout_ms ? call->timeout_ms : RUNTIME_TEST_TIMEOUT_MS;
    call->status =
        cbm_daemon_application_client_tool(call->client, "detect_changes", call->arguments,
                                           &call->response, &call->response_length, timeout_ms);
    atomic_store_explicit(&call->completed, true, memory_order_release);
    return NULL;
}

static bool runtime_real_application_ingest_probe(cbm_daemon_runtime_client_t *client) {
    uint8_t *response = NULL;
    uint32_t response_length = 0;
    cbm_daemon_runtime_application_status_t status =
        cbm_daemon_application_client_tool(client, "ingest_traces", "{\"traces\":[]}", &response,
                                           &response_length, RUNTIME_TEST_TIMEOUT_MS);
    bool usable = status == CBM_DAEMON_RUNTIME_APPLICATION_OK && response && response_length > 0 &&
                  strstr((const char *)response, "traces_received");
    free(response);
    return usable;
}

static void runtime_test_restore_environment(const char *name, const char *saved_value,
                                             bool was_set) {
    if (was_set) {
        (void)cbm_setenv(name, saved_value, 1);
    } else {
        (void)cbm_unsetenv(name);
    }
}

static void *runtime_application_client_connect_thread(void *opaque) {
    runtime_application_connect_call_t *call = opaque;
    call->client = cbm_daemon_runtime_client_connect(call->endpoint, &call->identity,
                                                     RUNTIME_TEST_TIMEOUT_MS, &call->result);
    atomic_store_explicit(&call->completed, true, memory_order_release);
    return NULL;
}

static void runtime_application_request_cancel(
    void *opaque, cbm_daemon_runtime_application_session_t *opaque_session,
    cbm_daemon_runtime_application_token_t request_token) {
    (void)request_token;
    runtime_application_context_t *context = opaque;
    runtime_application_session_t *session = (runtime_application_session_t *)opaque_session;
    if (!context || !session || session->context != context) {
        return;
    }
    atomic_store_explicit(&session->cancel_requested, true, memory_order_release);
    (void)atomic_fetch_add_explicit(&context->request_cancels, 1, memory_order_relaxed);
}

static void runtime_application_session_cancel(
    void *opaque, cbm_daemon_runtime_application_session_t *opaque_session) {
    runtime_application_context_t *context = opaque;
    runtime_application_session_t *session = (runtime_application_session_t *)opaque_session;
    if (!context || !session || session->context != context) {
        return;
    }
    atomic_store_explicit(&session->cancel_requested, true, memory_order_release);
    (void)atomic_fetch_add_explicit(&context->cancelled, 1, memory_order_relaxed);
}

static void runtime_application_session_close(
    void *opaque, cbm_daemon_runtime_application_session_t *opaque_session) {
    runtime_application_context_t *context = opaque;
    runtime_application_session_t *session = (runtime_application_session_t *)opaque_session;
    if (!context || !session || session->context != context) {
        return;
    }
    (void)atomic_fetch_add_explicit(&context->closed, 1, memory_order_relaxed);
    free(session);
}

static cbm_daemon_runtime_application_callbacks_t runtime_application_callbacks(
    runtime_application_context_t *context) {
    cbm_daemon_runtime_application_callbacks_t callbacks = {
        .context = context,
        .session_open = runtime_application_session_open,
        .request = runtime_application_request,
        .request_cancel = runtime_application_request_cancel,
        .session_cancel = runtime_application_session_cancel,
        .session_close = runtime_application_session_close,
    };
    return callbacks;
}

static void runtime_application_context_init(runtime_application_context_t *context,
                                             bool block_first_request) {
    memset(context, 0, sizeof(*context));
    atomic_init(&context->opened, 0);
    atomic_init(&context->requests, 0);
    atomic_init(&context->request_cancels, 0);
    atomic_init(&context->cancelled, 0);
    atomic_init(&context->closed, 0);
    atomic_init(&context->block_first_request, block_first_request);
    atomic_init(&context->first_request_started, false);
    atomic_init(&context->ignore_first_request_cancel, false);
    atomic_init(&context->release_first_request, false);
    atomic_init(&context->block_second_open, false);
    atomic_init(&context->second_open_started, false);
    atomic_init(&context->release_second_open, false);
}

static bool runtime_test_wait_atomic_bool(atomic_bool *value, uint32_t timeout_ms) {
    uint64_t deadline = cbm_now_ms() + (uint64_t)timeout_ms;
    while (!atomic_load_explicit(value, memory_order_acquire)) {
        if (cbm_now_ms() >= deadline) {
            return false;
        }
        struct timespec pause = {.tv_sec = 0, .tv_nsec = 1000000};
        (void)cbm_nanosleep(&pause, NULL);
    }
    return true;
}

static bool runtime_test_wait_atomic_int(atomic_int *value, int expected, uint32_t timeout_ms) {
    uint64_t deadline = cbm_now_ms() + (uint64_t)timeout_ms;
    while (atomic_load_explicit(value, memory_order_acquire) != expected) {
        if (cbm_now_ms() >= deadline) {
            return false;
        }
        struct timespec pause = {.tv_sec = 0, .tv_nsec = 1000000};
        (void)cbm_nanosleep(&pause, NULL);
    }
    return true;
}

static void runtime_test_put_u32(uint8_t out[4], uint32_t value) {
    out[0] = (uint8_t)(value >> 24);
    out[1] = (uint8_t)(value >> 16);
    out[2] = (uint8_t)(value >> 8);
    out[3] = (uint8_t)value;
}

static uint32_t runtime_test_get_u32(const uint8_t in[4]) {
    return ((uint32_t)in[0] << 24) | ((uint32_t)in[1] << 16) | ((uint32_t)in[2] << 8) |
           (uint32_t)in[3];
}

static void runtime_test_put_u64(uint8_t out[8], uint64_t value) {
    for (size_t index = 0; index < 8; index++) {
        out[index] = (uint8_t)(value >> (56U - index * 8U));
    }
}

static uint64_t runtime_test_get_u64(const uint8_t in[8]) {
    uint64_t value = 0;
    for (size_t index = 0; index < 8; index++) {
        value = (value << 8U) | in[index];
    }
    return value;
}

static bool runtime_test_raw_activation_exchange(
    const cbm_daemon_ipc_endpoint_t *endpoint, const uint8_t *request, uint32_t request_length,
    cbm_daemon_runtime_activation_result_t *result_out) {
    if (result_out) {
        memset(result_out, 0, sizeof(*result_out));
    }
    if (!endpoint || !request || !result_out) {
        return false;
    }
    cbm_daemon_ipc_connection_t *connection =
        cbm_daemon_ipc_connect(endpoint, RUNTIME_TEST_TIMEOUT_MS);
    bool sent = connection && cbm_daemon_ipc_send_frame(connection, CBM_DAEMON_FRAME_REQUEST,
                                                        CBM_DAEMON_RUNTIME_OP_ACTIVATION_SHUTDOWN,
                                                        request, request_length);
    cbm_daemon_frame_t frame = {0};
    uint8_t *response = NULL;
    int received =
        sent ? cbm_daemon_ipc_receive_frame(connection, RUNTIME_TEST_TIMEOUT_MS, &frame, &response)
             : 0;
    uint32_t status = received == 1 && response ? runtime_test_get_u32(response + 4) : UINT32_MAX;
    bool valid = received == 1 && frame.type == CBM_DAEMON_FRAME_RESPONSE &&
                 frame.flags == CBM_DAEMON_RUNTIME_OP_ACTIVATION_SHUTDOWN &&
                 frame.length == RUNTIME_TEST_ACTIVATION_RESPONSE_SIZE && response &&
                 runtime_test_get_u32(response) == RUNTIME_TEST_RENDEZVOUS_ABI && status <= 1U;
    if (valid) {
        result_out->accepted = status == 1U;
        result_out->active_clients =
            runtime_test_get_u64(response + RUNTIME_TEST_ACTIVATION_RESPONSE_CLIENTS_OFFSET);
        result_out->active_connections =
            runtime_test_get_u64(response + RUNTIME_TEST_ACTIVATION_RESPONSE_CONNECTIONS_OFFSET);
    }
    free(response);
    cbm_daemon_ipc_connection_close(connection);
    return valid;
}

static bool runtime_test_activation_request_encode(
    uint8_t out[RUNTIME_TEST_ACTIVATION_REQUEST_SIZE],
    cbm_daemon_runtime_activation_action_t action, const cbm_daemon_build_identity_t *identity) {
    if (!out || !identity) {
        return false;
    }
    memset(out, 0, RUNTIME_TEST_ACTIVATION_REQUEST_SIZE);
    runtime_test_put_u32(out, (uint32_t)action);
    return cbm_daemon_runtime_hello_request_encode(out + RUNTIME_TEST_ACTIVATION_IDENTITY_OFFSET,
                                                   identity);
}

static bool runtime_test_fixed_string_equals(const uint8_t *wire, size_t capacity,
                                             const char *expected) {
    if (!wire || !expected) {
        return false;
    }
    size_t length = strlen(expected);
    if (length >= capacity || memcmp(wire, expected, length) != 0 || wire[length] != 0) {
        return false;
    }
    for (size_t i = length + 1; i < capacity; i++) {
        if (wire[i] != 0) {
            return false;
        }
    }
    return true;
}

static cbm_daemon_ipc_connection_t *runtime_test_raw_client_connect(
    const cbm_daemon_ipc_endpoint_t *endpoint, const cbm_daemon_build_identity_t *identity) {
    uint8_t hello[CBM_DAEMON_RENDEZVOUS_REQUEST_SIZE];
    if (!cbm_daemon_runtime_hello_request_encode(hello, identity)) {
        return NULL;
    }
    cbm_daemon_ipc_connection_t *connection =
        cbm_daemon_ipc_connect(endpoint, RUNTIME_TEST_TIMEOUT_MS);
    if (!connection ||
        !cbm_daemon_ipc_send_frame(connection, CBM_DAEMON_FRAME_REQUEST,
                                   CBM_DAEMON_RUNTIME_OP_HELLO, hello, (uint32_t)sizeof(hello))) {
        cbm_daemon_ipc_connection_close(connection);
        return NULL;
    }
    cbm_daemon_frame_t frame = {0};
    uint8_t *payload = NULL;
    int received =
        cbm_daemon_ipc_receive_frame(connection, RUNTIME_TEST_TIMEOUT_MS, &frame, &payload);
    bool accepted = received == 1 && frame.type == CBM_DAEMON_FRAME_RESPONSE &&
                    frame.flags == CBM_DAEMON_RUNTIME_OP_HELLO && frame.length > 0;
    free(payload);
    if (!accepted) {
        cbm_daemon_ipc_connection_close(connection);
        return NULL;
    }
    return connection;
}

static bool runtime_test_raw_application_send_token(cbm_daemon_ipc_connection_t *connection,
                                                    uint64_t request_token, const void *payload,
                                                    uint32_t payload_length,
                                                    uint32_t declared_length) {
    uint64_t wire_length = 12ULL + payload_length;
    if (!connection || request_token == 0 || wire_length > UINT32_MAX ||
        (payload_length > 0 && !payload)) {
        return false;
    }
    uint8_t *wire = malloc((size_t)wire_length);
    if (!wire) {
        return false;
    }
    runtime_test_put_u64(wire, request_token);
    runtime_test_put_u32(wire + 8, declared_length);
    if (payload_length > 0) {
        memcpy(wire + 12, payload, payload_length);
    }
    bool sent = cbm_daemon_ipc_send_frame(connection, CBM_DAEMON_FRAME_REQUEST,
                                          CBM_DAEMON_RUNTIME_OP_APPLICATION_REQUEST, wire,
                                          (uint32_t)wire_length);
    free(wire);
    return sent;
}

static bool runtime_test_raw_application_send(cbm_daemon_ipc_connection_t *connection,
                                              const void *payload, uint32_t payload_length,
                                              uint32_t declared_length) {
    static atomic_uint_fast64_t next_token = ATOMIC_VAR_INIT(1);
    uint64_t request_token = atomic_fetch_add_explicit(&next_token, 1, memory_order_relaxed);
    return runtime_test_raw_application_send_token(connection, request_token, payload,
                                                   payload_length, declared_length);
}

static bool runtime_test_raw_application_cancel(cbm_daemon_ipc_connection_t *connection,
                                                uint64_t request_token) {
    uint8_t wire[8];
    if (!connection || request_token == 0) {
        return false;
    }
    runtime_test_put_u64(wire, request_token);
    return cbm_daemon_ipc_send_frame(connection, CBM_DAEMON_FRAME_REQUEST,
                                     CBM_DAEMON_RUNTIME_OP_APPLICATION_CANCEL, wire,
                                     (uint32_t)sizeof(wire));
}

static bool runtime_test_raw_application_receive_status_token(
    cbm_daemon_ipc_connection_t *connection, uint64_t expected_token,
    cbm_daemon_runtime_application_status_t expected_status) {
    cbm_daemon_frame_t frame = {0};
    uint8_t *payload = NULL;
    int received =
        cbm_daemon_ipc_receive_frame(connection, RUNTIME_TEST_TIMEOUT_MS, &frame, &payload);
    bool valid = received == 1 && frame.type == CBM_DAEMON_FRAME_RESPONSE &&
                 frame.flags == CBM_DAEMON_RUNTIME_OP_APPLICATION_REQUEST && frame.length >= 16 &&
                 payload && runtime_test_get_u64(payload) == expected_token &&
                 runtime_test_get_u32(payload + 8) == (uint32_t)expected_status &&
                 runtime_test_get_u32(payload + 12) == frame.length - 16;
    free(payload);
    return valid;
}

static bool runtime_test_raw_application_receive_status(
    cbm_daemon_ipc_connection_t *connection,
    cbm_daemon_runtime_application_status_t expected_status) {
    cbm_daemon_frame_t frame = {0};
    uint8_t *payload = NULL;
    int received =
        cbm_daemon_ipc_receive_frame(connection, RUNTIME_TEST_TIMEOUT_MS, &frame, &payload);
    bool valid = received == 1 && frame.type == CBM_DAEMON_FRAME_RESPONSE &&
                 frame.flags == CBM_DAEMON_RUNTIME_OP_APPLICATION_REQUEST && frame.length >= 16 &&
                 payload && runtime_test_get_u64(payload) != 0 &&
                 runtime_test_get_u32(payload + 8) == (uint32_t)expected_status &&
                 runtime_test_get_u32(payload + 12) == frame.length - 16;
    free(payload);
    return valid;
}

static long long runtime_test_last_os_error(void) {
#ifdef _WIN32
    return (long long)GetLastError();
#else
    return (long long)errno;
#endif
}

static bool runtime_test_fixture_start_failed(const char *tag, const char *stage,
                                              long long detail) {
    printf("  runtime fixture startup failed: tag=%s stage=%s detail=%lld\n", tag ? tag : "(null)",
           stage ? stage : "(null)", detail);
    return false;
}

static bool runtime_test_fixture_permanent = false;

static bool runtime_test_fixture_start_configured(
    runtime_test_fixture_t *fixture, const char *tag, const cbm_daemon_build_identity_t *identity,
    uint32_t max_clients, uint64_t lease_timeout_ms,
    const cbm_daemon_runtime_application_callbacks_t *application) {
    memset(fixture, 0, sizeof(*fixture));
    if (!th_secure_runtime_parent_new(fixture->parent, sizeof(fixture->parent), tag)) {
        return runtime_test_fixture_start_failed(tag, "temporary-directory",
                                                 runtime_test_last_os_error());
    }

    if (!cbm_daemon_rendezvous_key(fixture->key)) {
        return runtime_test_fixture_start_failed(tag, "rendezvous-key",
                                                 runtime_test_last_os_error());
    }
    fixture->endpoint = cbm_daemon_ipc_endpoint_new(fixture->key, fixture->parent);
    if (!fixture->endpoint) {
        return runtime_test_fixture_start_failed(tag, "endpoint", runtime_test_last_os_error());
    }
    if (!runtime_test_copy_path(fixture->runtime_dir,
                                cbm_daemon_ipc_endpoint_runtime_dir(fixture->endpoint))) {
        return runtime_test_fixture_start_failed(tag, "runtime-path", 0);
    }

    int log_written = snprintf(fixture->log_path, sizeof(fixture->log_path), "%s/conflicts.ndjson",
                               fixture->parent);
    int rotated_written = snprintf(fixture->rotated_log_path, sizeof(fixture->rotated_log_path),
                                   "%s.1", fixture->log_path);
    int lock_written = snprintf(fixture->lock_log_path, sizeof(fixture->lock_log_path), "%s.lock",
                                fixture->log_path);
    if (log_written <= 0 || log_written >= (int)sizeof(fixture->log_path)) {
        return runtime_test_fixture_start_failed(tag, "conflict-log-path", log_written);
    }
    if (rotated_written <= 0 || rotated_written >= (int)sizeof(fixture->rotated_log_path)) {
        return runtime_test_fixture_start_failed(tag, "rotated-log-path", rotated_written);
    }
    if (lock_written <= 0 || lock_written >= (int)sizeof(fixture->lock_log_path)) {
        return runtime_test_fixture_start_failed(tag, "lock-log-path", lock_written);
    }

    cbm_daemon_runtime_service_config_t config = {
        .endpoint = fixture->endpoint,
        .identity = *identity,
        .conflict_log_path = fixture->log_path,
        .conflict_log_cap_bytes = 64U * 1024U,
        .max_clients = max_clients,
        .lease_timeout_ms = lease_timeout_ms,
        .request_timeout_ms = RUNTIME_TEST_TIMEOUT_MS,
        .shutdown_timeout_ms = RUNTIME_TEST_TIMEOUT_MS,
        /* Default false: every teardown-latency fixture depends on prompt
         * last-client-exit. Only the permanent-lifecycle tests flip this. */
        .permanent = runtime_test_fixture_permanent,
    };
    if (application) {
        config.application = *application;
    }
    fixture->service = cbm_daemon_runtime_service_start(&config);
    if (!fixture->service) {
        return runtime_test_fixture_start_failed(tag, "service-start",
                                                 runtime_test_last_os_error());
    }
    cbm_daemon_runtime_service_state_t state = cbm_daemon_runtime_service_state(fixture->service);
    if (state != CBM_DAEMON_RUNTIME_SERVICE_RUNNING) {
        return runtime_test_fixture_start_failed(tag, "service-state", (long long)state);
    }
    return true;
}

static bool runtime_test_fixture_start_limited(runtime_test_fixture_t *fixture, const char *tag,
                                               const cbm_daemon_build_identity_t *identity,
                                               uint32_t max_clients) {
    return runtime_test_fixture_start_configured(fixture, tag, identity, max_clients, 5000, NULL);
}

static bool runtime_test_fixture_start(runtime_test_fixture_t *fixture, const char *tag,
                                       const cbm_daemon_build_identity_t *identity) {
    return runtime_test_fixture_start_limited(fixture, tag, identity, 8);
}

static void runtime_test_fixture_finish(runtime_test_fixture_t *fixture);

/* The public convenience constructor participates in the migration guard for
 * the service's complete lifetime. free must refuse a running service without
 * consuming retry authority, then prove participant release after stop. */
TEST(daemon_runtime_convenience_service_owns_participant_guard) {
    cbm_daemon_build_identity_t identity =
        runtime_test_identity("2.4.0", runtime_test_self_build());
    runtime_test_fixture_t fixture;
    bool started = runtime_test_fixture_start(&fixture, "participant", &identity);
    int active = started ? cbm_daemon_ipc_legacy_generation_probe(fixture.endpoint) : -1;
    bool running_free_refused = started && !cbm_daemon_runtime_service_free(fixture.service);
    bool stopped =
        started && cbm_daemon_runtime_service_stop(fixture.service, RUNTIME_TEST_TIMEOUT_MS);
    bool freed = stopped && cbm_daemon_runtime_service_free(fixture.service);
    if (freed) {
        fixture.service = NULL;
    }
    int released = freed ? cbm_daemon_ipc_legacy_generation_probe(fixture.endpoint) : -1;
    runtime_test_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_EQ(active, 1);
    ASSERT_TRUE(running_free_refused);
    ASSERT_TRUE(stopped);
    ASSERT_TRUE(freed);
    ASSERT_EQ(released, 0);
    PASS();
}

static bool runtime_test_fixture_start_application(runtime_test_fixture_t *fixture, const char *tag,
                                                   const cbm_daemon_build_identity_t *identity,
                                                   runtime_application_context_t *context) {
    cbm_daemon_runtime_application_callbacks_t application = runtime_application_callbacks(context);
    return runtime_test_fixture_start_configured(fixture, tag, identity, 8, 5000, &application);
}

static void runtime_test_fixture_finish(runtime_test_fixture_t *fixture) {
    if (fixture->service) {
        cbm_daemon_runtime_service_state_t state =
            cbm_daemon_runtime_service_state(fixture->service);
        bool stopped =
            state == CBM_DAEMON_RUNTIME_SERVICE_EXITED ||
            cbm_daemon_runtime_service_stop(fixture->service, RUNTIME_TEST_CLEANUP_TIMEOUT_MS);
        bool freed = false;
        for (size_t attempt = 0; stopped && !freed && attempt < RUNTIME_TEST_CLEANUP_FREE_ATTEMPTS;
             attempt++) {
            freed = cbm_daemon_runtime_service_free(fixture->service);
            if (!freed) {
                cbm_usleep(1000);
            }
        }
        if (!freed) {
            fprintf(stderr, "daemon_runtime fixture teardown failed\n");
            abort();
        }
        fixture->service = NULL;
    }
    cbm_daemon_ipc_endpoint_free(fixture->endpoint);
    (void)cbm_unlink(fixture->rotated_log_path);
    (void)cbm_unlink(fixture->log_path);
    (void)cbm_unlink(fixture->lock_log_path);
    (void)cbm_rmdir(fixture->runtime_dir);
    (void)cbm_rmdir(fixture->parent);
    memset(fixture, 0, sizeof(*fixture));
}

static bool runtime_test_read_log(const char *path, char out[RUNTIME_TEST_LOG_CAP]) {
    FILE *file = cbm_fopen(path, "rb");
    if (!file) {
        return false;
    }
    size_t used = fread(out, 1, RUNTIME_TEST_LOG_CAP - 1, file);
    bool complete = !ferror(file) && feof(file);
    out[used] = '\0';
    (void)fclose(file);
    return complete;
}

/* A detached daemon loses its inherited stderr by design. Failures before the
 * runtime listener exists must therefore reach the owner-private operation log
 * or users and smoke tests see only a generic bootstrap timeout. An existing
 * daemon claim deterministically fails before runtime startup on POSIX. The
 * Windows same-process participant guard rejects the nested host one stage
 * earlier, and that platform-correct refusal must be equally durable. */
TEST(daemon_host_early_coordination_failure_is_durable) {
    const char *old_cache = getenv("CBM_CACHE_DIR");
    bool had_cache = old_cache != NULL;
    char *saved_cache = old_cache ? cbm_strdup(old_cache) : NULL;
    bool snapshot_ok = !had_cache || saved_cache;

    char parent[RUNTIME_TEST_PATH_CAP] = {0};
    char cache[RUNTIME_TEST_PATH_CAP] = {0};
    char log_path[RUNTIME_TEST_PATH_CAP] = {0};
    bool parent_created =
        snapshot_ok && th_secure_runtime_parent_new(parent, sizeof(parent), "host-early-log");
    int cache_written = parent_created ? snprintf(cache, sizeof(cache), "%s/cache", parent) : -1;
    int log_written =
        parent_created ? snprintf(log_path, sizeof(log_path), "%s/logs/cbm-daemon.log", cache) : -1;
    bool environment_ready = cache_written > 0 && cache_written < (int)sizeof(cache) &&
                             log_written > 0 && log_written < (int)sizeof(log_path) &&
                             cbm_mkdir_p(cache, 0700) && cbm_setenv("CBM_CACHE_DIR", cache, 1) == 0;
    cbm_daemon_ipc_endpoint_t *endpoint =
        environment_ready ? cbm_daemon_ipc_endpoint_new("0123456789abcdef", parent) : NULL;
    cbm_version_cohort_manager_t *active_manager =
        endpoint ? cbm_version_cohort_manager_new(endpoint) : NULL;
    cbm_version_cohort_lease_t *active_lease = NULL;
    cbm_version_cohort_daemon_claim_t *active_claim = NULL;
    cbm_daemon_conflict_t active_conflict;
    cbm_daemon_build_identity_t active_identity =
        runtime_test_identity("active-host", runtime_test_self_build());
    active_identity.cache_fingerprint = RUNTIME_BUILD_B;
    cbm_version_cohort_status_t active_status =
        active_manager ? cbm_version_cohort_acquire(active_manager, &active_identity, UINT64_MAX,
                                                    &active_lease, &active_conflict)
                       : CBM_VERSION_COHORT_IO;
    cbm_version_cohort_status_t claim_status =
        active_status == CBM_VERSION_COHORT_OK
            ? cbm_version_cohort_daemon_claim_acquire(active_manager, &active_claim)
            : CBM_VERSION_COHORT_IO;
    atomic_int stop_requested = ATOMIC_VAR_INIT(0);
    cbm_daemon_host_config_t config = {
        .endpoint = endpoint,
        .identity = active_identity,
        .executable_path = "/host-early-log-test",
        .stop_requested = &stop_requested,
    };
    int run_result = claim_status == CBM_VERSION_COHORT_OK ? cbm_daemon_host_run(&config) : 0;

    char log[RUNTIME_TEST_LOG_CAP] = {0};
    bool log_read = runtime_test_read_log(log_path, log);
    bool durable_component = false;
#ifdef _WIN32
    durable_component = strstr(log, "participant") != NULL;
#else
    durable_component = strstr(log, "claim") != NULL;
#endif
    bool durable = log_read && strstr(log, "daemon.start_failed") != NULL && durable_component;
    bool endpoint_created = endpoint != NULL;
    while (active_claim &&
           cbm_version_cohort_daemon_claim_release(&active_claim) != CBM_PRIVATE_FILE_LOCK_OK) {
        cbm_usleep(1000);
    }
    while (active_lease &&
           cbm_version_cohort_lease_release(&active_lease) != CBM_PRIVATE_FILE_LOCK_OK) {
        cbm_usleep(1000);
    }
    while (active_manager &&
           cbm_version_cohort_manager_free(&active_manager) != CBM_PRIVATE_FILE_LOCK_OK) {
        cbm_usleep(1000);
    }
    cbm_daemon_ipc_endpoint_free(endpoint);
    runtime_test_restore_environment("CBM_CACHE_DIR", saved_cache, had_cache);
    free(saved_cache);
    bool cleaned = !parent_created || th_rmtree(parent) == 0;

    ASSERT_TRUE(snapshot_ok);
    ASSERT_TRUE(parent_created);
    ASSERT_TRUE(environment_ready);
    ASSERT_TRUE(endpoint_created);
    ASSERT_EQ(active_status, CBM_VERSION_COHORT_OK);
    ASSERT_EQ(claim_status, CBM_VERSION_COHORT_OK);
    ASSERT_EQ(run_result, -1);
    ASSERT_TRUE(durable);
    ASSERT_TRUE(cleaned);
    PASS();
}

/* RED on the former host preparation contract: a failed _config.db open was
 * silently converted into default settings, so startup continued and could
 * enable background behavior the user had explicitly disabled. */
TEST(daemon_host_refuses_unopenable_runtime_config_database) {
    const char *old_cache = getenv("CBM_CACHE_DIR");
    bool had_cache = old_cache != NULL;
    char *saved_cache = old_cache ? cbm_strdup(old_cache) : NULL;
    bool snapshot_ok = !had_cache || saved_cache;

    char parent[RUNTIME_TEST_PATH_CAP] = {0};
    char cache[RUNTIME_TEST_PATH_CAP] = {0};
    char config_blocker[RUNTIME_TEST_PATH_CAP] = {0};
    bool parent_created =
        snapshot_ok && th_secure_runtime_parent_new(parent, sizeof(parent), "host-config");
    int cache_written = parent_created ? snprintf(cache, sizeof(cache), "%s/cache", parent) : -1;
    int blocker_written =
        parent_created ? snprintf(config_blocker, sizeof(config_blocker), "%s/_config.db", cache)
                       : -1;
    bool blocker_created = cache_written > 0 && cache_written < (int)sizeof(cache) &&
                           blocker_written > 0 && blocker_written < (int)sizeof(config_blocker) &&
                           cbm_mkdir_p(cache, 0700) && cbm_mkdir_p(config_blocker, 0700);
    bool environment_ready = blocker_created && cbm_setenv("CBM_CACHE_DIR", cache, 1) == 0;
    cbm_daemon_ipc_endpoint_t *endpoint =
        environment_ready ? cbm_daemon_ipc_endpoint_new("0123456789abcdef", parent) : NULL;
    bool endpoint_created = endpoint != NULL;
    bool prepared = endpoint && cbm_daemon_host_state_prepare_for_test(endpoint);

    cbm_daemon_ipc_endpoint_free(endpoint);
    runtime_test_restore_environment("CBM_CACHE_DIR", saved_cache, had_cache);
    free(saved_cache);
    bool cleaned = !parent_created || th_rmtree(parent) == 0;

    ASSERT_TRUE(snapshot_ok);
    ASSERT_TRUE(parent_created);
    ASSERT_TRUE(blocker_created);
    ASSERT_TRUE(environment_ready);
    ASSERT_TRUE(endpoint_created);
    ASSERT_FALSE(prepared);
    ASSERT_TRUE(cleaned);
    PASS();
}

TEST(daemon_host_http_reconcile_rate_limits_and_retries_transient_failures) {
    const uint64_t timestamps[] = {
        0, 100, 999, 1000, 1500, 2000, 2999, 3000,
    };
    cbm_daemon_host_http_reconcile_test_result_t result = {0};
    bool driven = cbm_daemon_host_http_reconcile_sequence_for_test(
        timestamps, sizeof(timestamps) / sizeof(timestamps[0]), 1, 1, &result);

    ASSERT_TRUE(driven);
    ASSERT_EQ(result.config_loads, 4);
    ASSERT_EQ(result.server_create_attempts, 3);
    ASSERT_EQ(result.thread_start_attempts, 2);
    ASSERT_TRUE(result.active_after_sequence);
    ASSERT_EQ(result.largest_scheduled_retry_ms, 2000);
    ASSERT_EQ(result.next_retry_ms, 0);
    ASSERT_EQ(result.server_stops, 1);
    ASSERT_EQ(result.server_frees, 2);
    ASSERT_EQ(result.thread_joins, 1);
    PASS();
}

TEST(daemon_host_http_retry_backoff_is_bounded) {
    const uint64_t timestamps[] = {
        0, 1000, 3000, 7000, 15000, 31000, 61000, 91000,
    };
    cbm_daemon_host_http_reconcile_test_result_t result = {0};
    bool driven = cbm_daemon_host_http_reconcile_sequence_for_test(
        timestamps, sizeof(timestamps) / sizeof(timestamps[0]),
        sizeof(timestamps) / sizeof(timestamps[0]), 0, &result);

    ASSERT_TRUE(driven);
    ASSERT_EQ(result.config_loads, sizeof(timestamps) / sizeof(timestamps[0]));
    ASSERT_EQ(result.server_create_attempts, sizeof(timestamps) / sizeof(timestamps[0]));
    ASSERT_EQ(result.thread_start_attempts, 0);
    ASSERT_FALSE(result.active_after_sequence);
    ASSERT_EQ(result.largest_scheduled_retry_ms, 30000);
    ASSERT_EQ(result.next_retry_ms, 121000);
    ASSERT_EQ(result.server_stops, 0);
    ASSERT_EQ(result.server_frees, 0);
    ASSERT_EQ(result.thread_joins, 0);
    PASS();
}

/* A server can refuse destruction while an index callback still owns its host
 * context. Reconfiguration must retain that server and retry its retirement
 * even if the desired port reverts to the cached value before the next poll;
 * only successful retirement permits a replacement. */
TEST(daemon_host_http_reconcile_retains_busy_server_until_free_succeeds) {
    cbm_daemon_host_http_free_refusal_test_result_t result = {0};
    bool driven = cbm_daemon_host_http_reconcile_free_refusal_for_test(&result);

    ASSERT_TRUE(driven);
    ASSERT_TRUE(result.retained_after_refusal);
    ASSERT_EQ(result.server_create_attempts_after_refusal, 1);
    ASSERT_TRUE(result.replacement_active_after_retry);
    ASSERT_EQ(result.server_create_attempts, 2);
    ASSERT_EQ(result.thread_start_attempts, 2);
    ASSERT_EQ(result.server_stops, 1);
    ASSERT_EQ(result.server_free_attempts, 2);
    ASSERT_EQ(result.thread_joins, 1);
    PASS();
}

#ifndef _WIN32
static int runtime_test_failed_host_child(const char *parent, const char *key) {
    char cache[RUNTIME_TEST_PATH_CAP];
    char log_path[RUNTIME_TEST_PATH_CAP];
    int cache_written = snprintf(cache, sizeof(cache), "%s/cache", parent);
    int log_written = snprintf(log_path, sizeof(log_path), "%s/logs/cbm-daemon.log", cache);
    if (cache_written <= 0 || cache_written >= (int)sizeof(cache) || log_written <= 0 ||
        log_written >= (int)sizeof(log_path) || cbm_setenv("CBM_CACHE_DIR", cache, 1) != 0) {
        return 40;
    }

    cbm_daemon_ipc_endpoint_t *endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
    atomic_int stop_requested = ATOMIC_VAR_INIT(0);
    cbm_daemon_build_identity_t mismatched =
        runtime_test_identity("host-order-test", RUNTIME_BUILD_B);
    mismatched.cache_fingerprint = RUNTIME_CACHE_A;
    cbm_daemon_host_config_t config = {
        .endpoint = endpoint,
        .identity = mismatched,
        .executable_path = "/host-order-test",
        .stop_requested = &stop_requested,
    };
    int run_result = endpoint ? cbm_daemon_host_run(&config) : 0;

    char log[RUNTIME_TEST_LOG_CAP] = {0};
    bool read = runtime_test_read_log(log_path, log);
    bool failed_at_runtime =
        read && strstr(log, "daemon.start_failed") != NULL && strstr(log, "runtime") != NULL;
    bool watcher_started = read && strstr(log, "watcher.start") != NULL;
    cbm_daemon_ipc_endpoint_free(endpoint);
    if (run_result != -1 || !failed_at_runtime) {
        return 41;
    }
    return watcher_started ? 42 : 0;
}

/* RED on the former host order: host_state_start launched the watcher before
 * runtime fingerprint validation/listener reservation, so the isolated log
 * always contained watcher.start even though no daemon could serve a client. */
TEST(daemon_host_failed_listener_reservation_starts_no_background_work) {
    char parent[RUNTIME_TEST_PATH_CAP];
    int written = snprintf(parent, sizeof(parent), "%s/cbm-host-order-XXXXXX", cbm_tmpdir());
    bool parent_created =
        written > 0 && written < (int)sizeof(parent) && cbm_mkdtemp(parent) != NULL;
    char key[CBM_DAEMON_KEY_SIZE] = {0};
    cbm_daemon_ipc_endpoint_t *endpoint = parent_created && cbm_daemon_rendezvous_key(key)
                                              ? cbm_daemon_ipc_endpoint_new(key, parent)
                                              : NULL;
    cbm_daemon_ipc_startup_lock_t *startup = NULL;
    bool setup = endpoint && cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &startup) == 1 &&
                 cbm_daemon_ipc_startup_lock_prepare_handoff(startup);
    pid_t child = setup ? fork() : -1;
    if (child == 0) {
        _exit(runtime_test_failed_host_child(parent, key));
    }

    int status = 0;
    bool waited = child > 0 && waitpid(child, &status, 0) == child;
    bool startup_released = cbm_daemon_ipc_startup_lock_release(&startup);
    cbm_daemon_ipc_endpoint_free(endpoint);
    th_cleanup(parent_created ? parent : NULL);

    ASSERT_TRUE(setup);
    ASSERT_TRUE(child > 0);
    ASSERT_TRUE(waited);
    ASSERT_TRUE(startup_released);
    ASSERT_TRUE(WIFEXITED(status));
    ASSERT_EQ(WEXITSTATUS(status), 0);
    PASS();
}

static _Noreturn void runtime_test_forced_host_shutdown_child(const char *parent) {
    char cache[RUNTIME_TEST_PATH_CAP];
    int written = snprintf(cache, sizeof(cache), "%s/cache", parent);
    if (written <= 0 || written >= (int)sizeof(cache) ||
        cbm_setenv("CBM_CACHE_DIR", cache, 1) != 0) {
        _exit(80);
    }
    (void)alarm(2);
    cbm_daemon_host_force_terminate_for_test("noncooperative_callback");
}

static bool runtime_test_cleanup_release_never_succeeds(void *context) {
    (void)context;
    return false;
}

static _Noreturn void runtime_test_persistent_cleanup_failure_child(const char *parent) {
    char cache[RUNTIME_TEST_PATH_CAP];
    int written = snprintf(cache, sizeof(cache), "%s/cache", parent);
    if (written <= 0 || written >= (int)sizeof(cache) ||
        cbm_setenv("CBM_CACHE_DIR", cache, 1) != 0) {
        _exit(81);
    }
    cbm_daemon_host_cleanup_release_until_complete_for_test(
        runtime_test_cleanup_release_never_succeeds, NULL);
    /* Continuing after an unreleased native coordination claim would let the
     * daemon report a clean stop that never actually completed. */
    _exit(82);
}

/* Native unlock/close failures are retryable, but a persistent OS error must
 * not turn daemon shutdown into an immortal process. The parent polls with a
 * hard deadline and always kills/reaps a regressed child so this test itself
 * can fail without hanging the rest of the suite. */
TEST(daemon_host_persistent_cleanup_release_failure_is_process_bounded) {
    char parent[RUNTIME_TEST_PATH_CAP];
    char log_path[RUNTIME_TEST_PATH_CAP];
    int parent_written =
        snprintf(parent, sizeof(parent), "%s/cbm-host-cleanup-XXXXXX", cbm_tmpdir());
    bool parent_created =
        parent_written > 0 && parent_written < (int)sizeof(parent) && cbm_mkdtemp(parent) != NULL;
    int log_written = parent_created ? snprintf(log_path, sizeof(log_path),
                                                "%s/cache/logs/cbm-daemon.log", parent)
                                     : -1;
    bool path_ok = log_written > 0 && log_written < (int)sizeof(log_path);
    pid_t child = path_ok ? fork() : -1;
    if (child == 0) {
        runtime_test_persistent_cleanup_failure_child(parent);
    }

    int status = 0;
    bool completed = false;
    uint64_t deadline = cbm_now_ms() + RUNTIME_TEST_TIMEOUT_MS;
    while (child > 0 && cbm_now_ms() < deadline) {
        pid_t waited = waitpid(child, &status, WNOHANG);
        if (waited == child) {
            completed = true;
            break;
        }
        if (waited < 0 && errno != EINTR) {
            break;
        }
        cbm_usleep(1000);
    }
    if (child > 0 && !completed) {
        (void)kill(child, SIGKILL);
        while (waitpid(child, &status, 0) < 0 && errno == EINTR) {}
    }

    char log[RUNTIME_TEST_LOG_CAP] = {0};
    bool durable = completed && runtime_test_read_log(log_path, log) &&
                   strstr(log, "daemon.forced_shutdown") != NULL &&
                   strstr(log, "coordination_cleanup") != NULL;
    th_cleanup(parent_created ? parent : NULL);

    ASSERT_TRUE(parent_created);
    ASSERT_TRUE(path_ok);
    ASSERT_TRUE(child > 0);
    ASSERT_TRUE(completed);
    ASSERT_TRUE(WIFEXITED(status));
    ASSERT_EQ(WEXITSTATUS(status), EXIT_FAILURE);
    ASSERT_TRUE(durable);
    PASS();
}

TEST(daemon_host_forced_shutdown_is_logged_flushed_and_process_bounded) {
    char parent[RUNTIME_TEST_PATH_CAP];
    char log_path[RUNTIME_TEST_PATH_CAP];
    int parent_written = snprintf(parent, sizeof(parent), "%s/cbm-host-force-XXXXXX", cbm_tmpdir());
    bool parent_created =
        parent_written > 0 && parent_written < (int)sizeof(parent) && cbm_mkdtemp(parent) != NULL;
    int log_written = parent_created ? snprintf(log_path, sizeof(log_path),
                                                "%s/cache/logs/cbm-daemon.log", parent)
                                     : -1;
    bool path_ok = log_written > 0 && log_written < (int)sizeof(log_path);
    pid_t child = path_ok ? fork() : -1;
    if (child == 0) {
        runtime_test_forced_host_shutdown_child(parent);
    }

    int status = 0;
    bool waited = child > 0 && waitpid(child, &status, 0) == child;
    char log[RUNTIME_TEST_LOG_CAP] = {0};
    bool durable = waited && runtime_test_read_log(log_path, log) &&
                   strstr(log, "daemon.forced_shutdown") != NULL &&
                   strstr(log, "noncooperative_callback") != NULL;
    th_cleanup(parent_created ? parent : NULL);

    ASSERT_TRUE(parent_created);
    ASSERT_TRUE(path_ok);
    ASSERT_TRUE(child > 0);
    ASSERT_TRUE(waited);
    ASSERT_TRUE(WIFEXITED(status));
    ASSERT_EQ(WEXITSTATUS(status), EXIT_FAILURE);
    ASSERT_TRUE(durable);
    PASS();
}
#endif

TEST(daemon_runtime_exact_hello_issues_connection_bound_identity) {
    cbm_daemon_build_identity_t identity =
        runtime_test_identity("2.4.0", runtime_test_self_build());
    runtime_test_fixture_t fixture;
    bool started = runtime_test_fixture_start(&fixture, "exact-hello", &identity);
    cbm_daemon_runtime_connect_result_t result = {0};
    cbm_daemon_runtime_client_t *client = NULL;
    bool accepted = false;
    bool identity_anchored = false;
    bool closed = false;
    bool exited = false;

    if (started) {
        /* Service and client are the same process image here, exercising the
         * native-identity HELLO path rather than copied-image fallback. */
        client = cbm_daemon_runtime_client_connect(fixture.endpoint, &identity,
                                                   RUNTIME_TEST_TIMEOUT_MS, &result);
    }
    if (client) {
        uint64_t expected_pid = runtime_test_process_id();
        accepted = result.status == CBM_DAEMON_RUNTIME_CONNECT_ACCEPTED &&
                   result.hello_status == CBM_DAEMON_HELLO_COMPATIBLE &&
                   result.client_id != CBM_DAEMON_CLIENT_ID_INVALID &&
                   result.client_id == cbm_daemon_runtime_client_id(client);
        identity_anchored = result.authenticated_process_id == expected_pid &&
                            cbm_daemon_runtime_client_process_id(client) == expected_pid &&
                            cbm_daemon_runtime_service_client_process_id(
                                fixture.service, result.client_id) == expected_pid;
        closed = cbm_daemon_runtime_client_close(client, RUNTIME_TEST_TIMEOUT_MS);
        client = NULL;
        exited = cbm_daemon_runtime_service_wait_exited(fixture.service, RUNTIME_TEST_TIMEOUT_MS);
    }

    if (client) {
        (void)cbm_daemon_runtime_client_close(client, RUNTIME_TEST_TIMEOUT_MS);
    }
    runtime_test_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_TRUE(accepted);
    ASSERT_TRUE(identity_anchored);
    ASSERT_TRUE(closed);
    ASSERT_TRUE(exited);
    PASS();
}

TEST(daemon_runtime_unexpected_frame_payload_is_freed_once) {
    static const uint8_t unexpected_payload[] = {0xde, 0xad, 0xbe, 0xef};
    cbm_daemon_build_identity_t identity =
        runtime_test_identity("2.4.0", runtime_test_self_build());
    runtime_test_fixture_t fixture;
    bool started = runtime_test_fixture_start(&fixture, "unexpected-frame", &identity);
    cbm_daemon_runtime_connect_result_t owner_result = {0};
    cbm_daemon_runtime_client_t *owner = NULL;
    cbm_daemon_ipc_connection_t *raw = NULL;
    bool raw_connected = false;
    bool unexpected_sent = false;
    bool bad_peer_released = false;
    bool bad_peer_closed = false;
    bool owner_survived = false;
    bool owner_closed = false;
    bool exited = false;

    if (started) {
        owner = cbm_daemon_runtime_client_connect(fixture.endpoint, &identity,
                                                  RUNTIME_TEST_TIMEOUT_MS, &owner_result);
    }
    if (owner) {
        raw = runtime_test_raw_client_connect(fixture.endpoint, &identity);
        raw_connected = raw != NULL;
    }
    if (raw) {
        unexpected_sent = cbm_daemon_ipc_send_frame(
            raw, CBM_DAEMON_FRAME_RESPONSE, CBM_DAEMON_RUNTIME_OP_HEARTBEAT, unexpected_payload,
            (uint32_t)sizeof(unexpected_payload));
    }
    if (unexpected_sent) {
        bad_peer_released = cbm_daemon_runtime_service_wait_for_clients(fixture.service, 1,
                                                                        RUNTIME_TEST_TIMEOUT_MS);
    }
    if (bad_peer_released) {
        cbm_daemon_frame_t frame = {0};
        uint8_t *payload = NULL;
        int received = cbm_daemon_ipc_receive_frame(raw, RUNTIME_TEST_TIMEOUT_MS, &frame, &payload);
        bad_peer_closed = received != 1;
        free(payload);
        owner_survived = cbm_daemon_runtime_client_heartbeat(owner, RUNTIME_TEST_TIMEOUT_MS);
    }
    cbm_daemon_ipc_connection_close(raw);
    raw = NULL;
    if (owner) {
        owner_closed = cbm_daemon_runtime_client_close(owner, RUNTIME_TEST_TIMEOUT_MS);
        owner = NULL;
    }
    if (started) {
        exited = cbm_daemon_runtime_service_wait_exited(fixture.service, RUNTIME_TEST_TIMEOUT_MS);
    }
    if (owner) {
        (void)cbm_daemon_runtime_client_close(owner, RUNTIME_TEST_TIMEOUT_MS);
    }
    cbm_daemon_ipc_connection_close(raw);
    runtime_test_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_TRUE(raw_connected);
    ASSERT_TRUE(unexpected_sent);
    ASSERT_TRUE(bad_peer_released);
    ASSERT_TRUE(bad_peer_closed);
    ASSERT_TRUE(owner_survived);
    ASSERT_TRUE(owner_closed);
    ASSERT_TRUE(exited);
    PASS();
}

TEST(daemon_runtime_activation_rejects_forged_and_malformed_without_stop) {
    cbm_daemon_build_identity_t identity =
        runtime_test_identity("2.4.0", runtime_test_self_build());
    cbm_daemon_build_identity_t forged = runtime_test_identity("9.9.9", RUNTIME_BUILD_B);
    runtime_test_fixture_t fixture;
    bool started = runtime_test_fixture_start(&fixture, "activation-reject", &identity);
    cbm_daemon_runtime_connect_result_t owner_result = {0};
    cbm_daemon_runtime_client_t *owner = NULL;
    uint8_t request[RUNTIME_TEST_ACTIVATION_REQUEST_SIZE];
    bool encoded = runtime_test_activation_request_encode(
        request, CBM_DAEMON_RUNTIME_ACTIVATION_UPDATE, &identity);
    cbm_daemon_runtime_activation_result_t short_result = {0};
    cbm_daemon_runtime_activation_result_t action_result = {0};
    cbm_daemon_runtime_activation_result_t abi_result = {0};
    cbm_daemon_runtime_activation_result_t forged_result = {0};
    bool short_rejected = false;
    bool action_rejected = false;
    bool abi_rejected = false;
    bool forged_rejected = false;
    bool unchanged = false;
    bool heartbeat = false;
    bool closed = false;
    bool exited = false;

    if (started) {
        owner = cbm_daemon_runtime_client_connect(fixture.endpoint, &identity,
                                                  RUNTIME_TEST_TIMEOUT_MS, &owner_result);
    }
    if (owner && encoded) {
        short_rejected = runtime_test_raw_activation_exchange(
                             fixture.endpoint, request, RUNTIME_TEST_ACTIVATION_REQUEST_SIZE - 1U,
                             &short_result) &&
                         !short_result.accepted;
        runtime_test_put_u32(request, UINT32_C(99));
        action_rejected = runtime_test_raw_activation_exchange(fixture.endpoint, request,
                                                               RUNTIME_TEST_ACTIVATION_REQUEST_SIZE,
                                                               &action_result) &&
                          !action_result.accepted;
        runtime_test_put_u32(request, (uint32_t)CBM_DAEMON_RUNTIME_ACTIVATION_UPDATE);
        runtime_test_put_u32(request + RUNTIME_TEST_ACTIVATION_IDENTITY_OFFSET,
                             RUNTIME_TEST_RENDEZVOUS_ABI + 1U);
        abi_rejected = runtime_test_raw_activation_exchange(fixture.endpoint, request,
                                                            RUNTIME_TEST_ACTIVATION_REQUEST_SIZE,
                                                            &abi_result) &&
                       !abi_result.accepted;
        forged_rejected = cbm_daemon_runtime_request_activation_shutdown(
                              fixture.endpoint, &forged, CBM_DAEMON_RUNTIME_ACTIVATION_UNINSTALL,
                              RUNTIME_TEST_TIMEOUT_MS, &forged_result) &&
                          !forged_result.accepted;
        unchanged = cbm_daemon_runtime_service_state(fixture.service) ==
                        CBM_DAEMON_RUNTIME_SERVICE_RUNNING &&
                    cbm_daemon_runtime_service_active_clients(fixture.service) == 1 &&
                    cbm_daemon_runtime_service_wait_for_connections(fixture.service, 1,
                                                                    RUNTIME_TEST_TIMEOUT_MS);
        heartbeat = cbm_daemon_runtime_client_heartbeat(owner, RUNTIME_TEST_TIMEOUT_MS);
    }
    if (owner) {
        closed = cbm_daemon_runtime_client_close(owner, RUNTIME_TEST_TIMEOUT_MS);
        owner = NULL;
    }
    if (started) {
        exited = cbm_daemon_runtime_service_wait_exited(fixture.service, RUNTIME_TEST_TIMEOUT_MS);
    }
    runtime_test_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_EQ(owner_result.status, CBM_DAEMON_RUNTIME_CONNECT_ACCEPTED);
    ASSERT_TRUE(encoded);
    ASSERT_TRUE(short_rejected);
    ASSERT_TRUE(action_rejected);
    ASSERT_TRUE(abi_rejected);
    ASSERT_TRUE(forged_rejected);
    ASSERT_TRUE(unchanged);
    ASSERT_TRUE(heartbeat);
    ASSERT_TRUE(closed);
    ASSERT_TRUE(exited);
    PASS();
}

TEST(daemon_runtime_activation_ack_snapshots_then_interrupts_all_clients) {
    cbm_daemon_build_identity_t identity =
        runtime_test_identity("2.4.0", runtime_test_self_build());
    runtime_test_fixture_t fixture;
    bool started = runtime_test_fixture_start(&fixture, "activation-drain", &identity);
    cbm_daemon_runtime_connect_result_t first_result = {0};
    cbm_daemon_runtime_connect_result_t second_result = {0};
    cbm_daemon_runtime_client_t *first = NULL;
    cbm_daemon_runtime_client_t *second = NULL;
    cbm_daemon_runtime_activation_result_t activation = {0};
    bool requested = false;
    bool first_interrupted = false;
    bool second_interrupted = false;
    bool exited = false;
    atomic_store_explicit(&runtime_activation_shutdown_log_seen, false, memory_order_release);
    cbm_log_set_sink(runtime_test_activation_shutdown_sink);

    if (started) {
        first = cbm_daemon_runtime_client_connect(fixture.endpoint, &identity,
                                                  RUNTIME_TEST_TIMEOUT_MS, &first_result);
        second = cbm_daemon_runtime_client_connect(fixture.endpoint, &identity,
                                                   RUNTIME_TEST_TIMEOUT_MS, &second_result);
    }
    if (first && second) {
        requested = cbm_daemon_runtime_request_activation_shutdown(
            fixture.endpoint, &identity, CBM_DAEMON_RUNTIME_ACTIVATION_UPDATE,
            RUNTIME_TEST_TIMEOUT_MS, &activation);
        first_interrupted = !cbm_daemon_runtime_client_heartbeat(first, RUNTIME_TEST_TIMEOUT_MS);
        second_interrupted = !cbm_daemon_runtime_client_heartbeat(second, RUNTIME_TEST_TIMEOUT_MS);
    }
    cbm_log_set_sink(NULL);
    if (first) {
        (void)cbm_daemon_runtime_client_close(first, RUNTIME_TEST_TIMEOUT_MS);
        first = NULL;
    }
    if (second) {
        (void)cbm_daemon_runtime_client_close(second, RUNTIME_TEST_TIMEOUT_MS);
        second = NULL;
    }
    if (started) {
        exited = cbm_daemon_runtime_service_wait_exited(fixture.service, RUNTIME_TEST_TIMEOUT_MS);
    }
    runtime_test_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_EQ(first_result.status, CBM_DAEMON_RUNTIME_CONNECT_ACCEPTED);
    ASSERT_EQ(second_result.status, CBM_DAEMON_RUNTIME_CONNECT_ACCEPTED);
    ASSERT_TRUE(requested);
    ASSERT_TRUE(activation.accepted);
    ASSERT_EQ(activation.active_clients, 2);
    /* The one-shot activation requester is not part of the drain snapshot. */
    ASSERT_EQ(activation.active_connections, 2);
    ASSERT_TRUE(atomic_load_explicit(&runtime_activation_shutdown_log_seen, memory_order_acquire));
    ASSERT_TRUE(first_interrupted);
    ASSERT_TRUE(second_interrupted);
    ASSERT_TRUE(exited);
    PASS();
}

#if defined(_WIN32) || defined(__APPLE__) || defined(__linux__)
TEST(daemon_runtime_activation_accepts_authenticated_different_build) {
    cbm_daemon_build_identity_t active_identity =
        runtime_test_identity("2.4.0", runtime_test_self_build());
    char directory[RUNTIME_TEST_PATH_CAP] = {0};
    char foreign_image[RUNTIME_TEST_PATH_CAP] = {0};
    int directory_written =
        snprintf(directory, sizeof(directory), "%s/cbm-runtime-activation-XXXXXX", cbm_tmpdir());
    bool directory_created = directory_written > 0 && directory_written < (int)sizeof(directory) &&
                             cbm_mkdtemp(directory) != NULL;
    int image_written = directory_created ? snprintf(foreign_image, sizeof(foreign_image),
                                                     "%s/foreign-activation", directory)
                                          : -1;
    bool copied = image_written > 0 && image_written < (int)sizeof(foreign_image) &&
                  runtime_test_copy_self_image(foreign_image);
#ifdef __APPLE__
    /* Appending an overlay invalidates Mach-O strict validation. A distinct
     * signing identifier changes the executable bytes while keeping the copy
     * runnable, exactly like the mapped-main-image adversarial fixture. */
    bool changed = copied;
    bool runnable = changed && runtime_test_mac_ad_hoc_sign(foreign_image);
#else
    bool changed = copied && runtime_test_append_image_marker(foreign_image);
    bool runnable = changed;
#endif
    char foreign_build[CBM_DAEMON_BUILD_FINGERPRINT_SIZE] = {0};
    bool fingerprinted =
        runnable && cbm_daemon_build_fingerprint_file(foreign_image, foreign_build);
    bool differs = fingerprinted && strcmp(foreign_build, active_identity.build_fingerprint) != 0;
    cbm_daemon_build_identity_t foreign_identity = runtime_test_identity("9.9.9", foreign_build);

    /* Finalize the foreign executable before the runtime starts any worker
     * threads. In particular, macOS code signing is an external process; the
     * test must not fork that helper from the live multithreaded daemon. The
     * immutable signed copy remains the actual peer image authenticated by
     * the activation request below. */
    runtime_test_fixture_t fixture;
    memset(&fixture, 0, sizeof(fixture));
    bool started =
        differs && runtime_test_fixture_start(&fixture, "activation-foreign", &active_identity);
    cbm_daemon_runtime_connect_result_t owner_result = {0};
    cbm_daemon_runtime_client_t *owner = NULL;
    if (started) {
        owner = cbm_daemon_runtime_client_connect(fixture.endpoint, &active_identity,
                                                  RUNTIME_TEST_TIMEOUT_MS, &owner_result);
    }
    int foreign_exit = -1;
    bool foreign_ran =
        owner && differs &&
        runtime_test_run_activation_image(foreign_image, &fixture, &foreign_identity,
                                          CBM_DAEMON_RUNTIME_ACTIVATION_INSTALL, &foreign_exit);
    bool owner_interrupted = owner && foreign_ran &&
                             !cbm_daemon_runtime_client_heartbeat(owner, RUNTIME_TEST_TIMEOUT_MS);
    if (owner) {
        (void)cbm_daemon_runtime_client_close(owner, RUNTIME_TEST_TIMEOUT_MS);
        owner = NULL;
    }
    bool exited =
        started && cbm_daemon_runtime_service_wait_exited(fixture.service, RUNTIME_TEST_TIMEOUT_MS);
    runtime_test_fixture_finish(&fixture);
    if (image_written > 0 && image_written < (int)sizeof(foreign_image)) {
        (void)cbm_unlink(foreign_image);
    }
    if (directory_created) {
        (void)cbm_rmdir(directory);
    }

    ASSERT_TRUE(started);
    ASSERT_EQ(owner_result.status, CBM_DAEMON_RUNTIME_CONNECT_ACCEPTED);
    ASSERT_TRUE(directory_created);
    ASSERT_TRUE(copied);
    ASSERT_TRUE(changed);
    ASSERT_TRUE(runnable);
    ASSERT_TRUE(fingerprinted);
    ASSERT_TRUE(differs);
    ASSERT_TRUE(foreign_ran);
    ASSERT_EQ(foreign_exit, 0);
    ASSERT_TRUE(owner_interrupted);
    ASSERT_TRUE(exited);
    PASS();
}
#endif

TEST(daemon_runtime_rendezvous_layout_is_frozen_and_detailed_abi_independent) {
    cbm_daemon_build_identity_t first = runtime_test_identity("2.4.0", runtime_test_self_build());
    cbm_daemon_build_identity_t different_detail = first;
    different_detail.protocol_abi = 0;
    different_detail.store_abi = 0;
    different_detail.feature_abi = 0;

    uint8_t first_wire[CBM_DAEMON_RENDEZVOUS_REQUEST_SIZE];
    uint8_t second_wire[CBM_DAEMON_RENDEZVOUS_REQUEST_SIZE];
    bool first_encoded = cbm_daemon_runtime_hello_request_encode(first_wire, &first);
    bool second_encoded = cbm_daemon_runtime_hello_request_encode(second_wire, &different_detail);

    ASSERT_TRUE(first_encoded);
    ASSERT_TRUE(second_encoded);
    ASSERT_EQ(CBM_DAEMON_RENDEZVOUS_REQUEST_SIZE, RUNTIME_TEST_RENDEZVOUS_REQUEST_SIZE);
    ASSERT_EQ(CBM_DAEMON_RENDEZVOUS_RESPONSE_SIZE, RUNTIME_TEST_RENDEZVOUS_RESPONSE_SIZE);
    ASSERT_EQ(CBM_DAEMON_RENDEZVOUS_VERSION_TEXT_CAP, CBM_DAEMON_VERSION_TEXT_SIZE);
    ASSERT_EQ(CBM_DAEMON_RENDEZVOUS_BUILD_FINGERPRINT_CAP, CBM_DAEMON_BUILD_FINGERPRINT_SIZE);
    ASSERT_EQ(CBM_DAEMON_RENDEZVOUS_MESSAGE_CAP, CBM_DAEMON_CONFLICT_MESSAGE_SIZE);
    ASSERT_EQ(runtime_test_get_u32(first_wire), RUNTIME_TEST_RENDEZVOUS_ABI);
    ASSERT_TRUE(
        runtime_test_fixed_string_equals(first_wire + RUNTIME_TEST_RENDEZVOUS_VERSION_OFFSET,
                                         CBM_DAEMON_VERSION_TEXT_SIZE, first.semantic_version));
    ASSERT_TRUE(runtime_test_fixed_string_equals(first_wire + RUNTIME_TEST_RENDEZVOUS_BUILD_OFFSET,
                                                 CBM_DAEMON_BUILD_FINGERPRINT_SIZE,
                                                 first.build_fingerprint));
    ASSERT_EQ(memcmp(first_wire, second_wire, sizeof(first_wire)), 0);
    PASS();
}

TEST(daemon_runtime_future_generation_gets_stable_explicit_conflict) {
    const char *active_build = runtime_test_self_build();
    cbm_daemon_build_identity_t active = runtime_test_identity("2.4.0", active_build);
    cbm_daemon_build_identity_t future =
        runtime_test_identity("9.0.0-future-wire-v2", RUNTIME_BUILD_B);
    future.protocol_abi = active.protocol_abi + 1000;
    future.store_abi = active.store_abi + 2000;
    future.feature_abi = active.feature_abi + 3000;

    runtime_test_fixture_t fixture;
    bool started = runtime_test_fixture_start(&fixture, "future-generation", &active);
    cbm_daemon_runtime_connect_result_t owner_result = {0};
    cbm_daemon_runtime_client_t *owner = NULL;
    cbm_daemon_ipc_connection_t *future_connection = NULL;
    uint8_t future_wire[CBM_DAEMON_RENDEZVOUS_REQUEST_SIZE];
    bool encoded = false;
    bool sent = false;
    bool explicit_conflict = false;
    bool logged = false;
    bool exited = false;
    cbm_daemon_frame_t response_frame = {0};
    uint8_t *response_payload = NULL;
    char log[RUNTIME_TEST_LOG_CAP] = {0};
    char expected_message[CBM_DAEMON_CONFLICT_MESSAGE_SIZE] = {0};
    int expected_length = snprintf(
        expected_message, sizeof(expected_message),
        "CBM could not start because a conflicting CBM process is active "
        "(version; active version %s, build %s; requested version %s, build %s). "
        "Close all CBM sessions and commands, then retry.",
        active.semantic_version, active_build, future.semantic_version, future.build_fingerprint);
    bool expected_message_valid =
        expected_length > 0 && (size_t)expected_length < sizeof(expected_message);

    if (started) {
        owner = cbm_daemon_runtime_client_connect(fixture.endpoint, &active,
                                                  RUNTIME_TEST_TIMEOUT_MS, &owner_result);
    }
    if (owner) {
        encoded = cbm_daemon_runtime_hello_request_encode(future_wire, &future);
        future_connection = cbm_daemon_ipc_connect(fixture.endpoint, RUNTIME_TEST_TIMEOUT_MS);
    }
    if (future_connection && encoded) {
        /* A future generation sends only the permanent identity envelope here;
         * its detailed runtime layout is negotiated by exact executable build,
         * never added to this stable endpoint message. */
        sent = cbm_daemon_ipc_send_frame(future_connection, CBM_DAEMON_FRAME_REQUEST,
                                         CBM_DAEMON_RUNTIME_OP_HELLO, future_wire,
                                         RUNTIME_TEST_RENDEZVOUS_REQUEST_SIZE);
        int received = cbm_daemon_ipc_receive_frame(future_connection, RUNTIME_TEST_TIMEOUT_MS,
                                                    &response_frame, &response_payload);
        explicit_conflict =
            expected_message_valid && received == 1 && response_payload &&
            response_frame.type == CBM_DAEMON_FRAME_RESPONSE &&
            response_frame.flags == CBM_DAEMON_RUNTIME_OP_HELLO &&
            response_frame.length == RUNTIME_TEST_RENDEZVOUS_RESPONSE_SIZE &&
            runtime_test_get_u32(response_payload) == CBM_DAEMON_RUNTIME_CONNECT_CONFLICT &&
            runtime_test_get_u32(response_payload + 4) == CBM_DAEMON_HELLO_VERSION_CONFLICT &&
            runtime_test_fixed_string_equals(
                response_payload + RUNTIME_TEST_RENDEZVOUS_ACTIVE_VERSION_OFFSET,
                CBM_DAEMON_VERSION_TEXT_SIZE, active.semantic_version) &&
            runtime_test_fixed_string_equals(response_payload +
                                                 RUNTIME_TEST_RENDEZVOUS_ACTIVE_BUILD_OFFSET,
                                             CBM_DAEMON_BUILD_FINGERPRINT_SIZE, active_build) &&
            runtime_test_fixed_string_equals(
                response_payload + RUNTIME_TEST_RENDEZVOUS_REQUESTED_VERSION_OFFSET,
                CBM_DAEMON_VERSION_TEXT_SIZE, future.semantic_version) &&
            runtime_test_fixed_string_equals(
                response_payload + RUNTIME_TEST_RENDEZVOUS_REQUESTED_BUILD_OFFSET,
                CBM_DAEMON_BUILD_FINGERPRINT_SIZE, future.build_fingerprint) &&
            runtime_test_fixed_string_equals(response_payload +
                                                 RUNTIME_TEST_RENDEZVOUS_MESSAGE_OFFSET,
                                             CBM_DAEMON_CONFLICT_MESSAGE_SIZE, expected_message);
    }
    cbm_daemon_ipc_connection_close(future_connection);
    future_connection = NULL;
    free(response_payload);
    response_payload = NULL;

    if (owner) {
        logged = runtime_test_read_log(fixture.log_path, log) &&
                 strstr(log, "\"reason\":\"version\"") != NULL &&
                 strstr(log, active_build) != NULL && strstr(log, RUNTIME_BUILD_B) != NULL;
        (void)cbm_daemon_runtime_client_close(owner, RUNTIME_TEST_TIMEOUT_MS);
        owner = NULL;
        exited = cbm_daemon_runtime_service_wait_exited(fixture.service, RUNTIME_TEST_TIMEOUT_MS);
    }

    cbm_daemon_ipc_connection_close(future_connection);
    free(response_payload);
    if (owner) {
        (void)cbm_daemon_runtime_client_close(owner, RUNTIME_TEST_TIMEOUT_MS);
    }
    runtime_test_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_TRUE(encoded);
    ASSERT_TRUE(sent);
    ASSERT_TRUE(explicit_conflict);
    ASSERT_TRUE(logged);
    ASSERT_TRUE(exited);
    PASS();
}

TEST(daemon_runtime_matching_clients_share_one_service_endpoint) {
    cbm_daemon_build_identity_t identity =
        runtime_test_identity("2.4.0", runtime_test_self_build());
    runtime_test_fixture_t fixture;
    bool started = runtime_test_fixture_start(&fixture, "shared-endpoint", &identity);
    cbm_daemon_ipc_endpoint_t *same_endpoint = NULL;
    cbm_daemon_runtime_client_t *first = NULL;
    cbm_daemon_runtime_client_t *second = NULL;
    cbm_daemon_runtime_connect_result_t first_result = {0};
    cbm_daemon_runtime_connect_result_t second_result = {0};
    bool same_address = false;
    bool both_registered = false;
    bool one_remains = false;
    bool exited = false;

    char key[CBM_DAEMON_KEY_SIZE];
    if (started && cbm_daemon_rendezvous_key(key)) {
        same_endpoint = cbm_daemon_ipc_endpoint_new(key, fixture.parent);
    }
    if (same_endpoint) {
        same_address = strcmp(cbm_daemon_ipc_endpoint_address(fixture.endpoint),
                              cbm_daemon_ipc_endpoint_address(same_endpoint)) == 0;
        first = cbm_daemon_runtime_client_connect(fixture.endpoint, &identity,
                                                  RUNTIME_TEST_TIMEOUT_MS, &first_result);
        second = cbm_daemon_runtime_client_connect(same_endpoint, &identity,
                                                   RUNTIME_TEST_TIMEOUT_MS, &second_result);
    }
    if (first && second) {
        both_registered = first_result.client_id != second_result.client_id &&
                          cbm_daemon_runtime_service_active_clients(fixture.service) == 2;
        (void)cbm_daemon_runtime_client_close(first, RUNTIME_TEST_TIMEOUT_MS);
        first = NULL;
        one_remains = cbm_daemon_runtime_service_wait_for_clients(fixture.service, 1,
                                                                  RUNTIME_TEST_TIMEOUT_MS);
        (void)cbm_daemon_runtime_client_close(second, RUNTIME_TEST_TIMEOUT_MS);
        second = NULL;
        exited = cbm_daemon_runtime_service_wait_exited(fixture.service, RUNTIME_TEST_TIMEOUT_MS);
    }

    if (second) {
        (void)cbm_daemon_runtime_client_close(second, RUNTIME_TEST_TIMEOUT_MS);
    }
    if (first) {
        (void)cbm_daemon_runtime_client_close(first, RUNTIME_TEST_TIMEOUT_MS);
    }
    cbm_daemon_ipc_endpoint_free(same_endpoint);
    runtime_test_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_TRUE(same_address);
    ASSERT_TRUE(both_registered);
    ASSERT_TRUE(one_remains);
    ASSERT_TRUE(exited);
    PASS();
}

TEST(daemon_runtime_same_version_different_build_is_visible_and_logged) {
    const char *active_build = runtime_test_self_build();
    cbm_daemon_build_identity_t active = runtime_test_identity("2.4.0", active_build);
    cbm_daemon_build_identity_t rebuilt = runtime_test_identity("2.4.0", RUNTIME_BUILD_B);
    runtime_test_fixture_t fixture;
    bool started = runtime_test_fixture_start(&fixture, "build-conflict", &active);
    cbm_daemon_runtime_connect_result_t owner_result = {0};
    cbm_daemon_runtime_connect_result_t conflict_result = {0};
    cbm_daemon_runtime_client_t *owner = NULL;
    cbm_daemon_runtime_client_t *rejected = NULL;
    bool explicit_conflict = false;
    bool logged = false;
    bool owner_unchanged = false;
    bool exited = false;
    char log[RUNTIME_TEST_LOG_CAP] = {0};

    if (started) {
        owner = cbm_daemon_runtime_client_connect(fixture.endpoint, &active,
                                                  RUNTIME_TEST_TIMEOUT_MS, &owner_result);
    }
    if (owner) {
        rejected = cbm_daemon_runtime_client_connect(fixture.endpoint, &rebuilt,
                                                     RUNTIME_TEST_TIMEOUT_MS, &conflict_result);
        explicit_conflict = rejected == NULL &&
                            conflict_result.status == CBM_DAEMON_RUNTIME_CONNECT_CONFLICT &&
                            conflict_result.hello_status == CBM_DAEMON_HELLO_BUILD_CONFLICT &&
                            strstr(conflict_result.message, "could not start") != NULL &&
                            strstr(conflict_result.message, active_build) != NULL &&
                            strstr(conflict_result.message, RUNTIME_BUILD_B) != NULL;
        owner_unchanged = cbm_daemon_runtime_service_active_clients(fixture.service) == 1;
        logged = runtime_test_read_log(fixture.log_path, log) &&
                 strstr(log, "daemon.version_conflict") != NULL &&
                 strstr(log, "\"reason\":\"build\"") != NULL && strstr(log, active_build) != NULL &&
                 strstr(log, RUNTIME_BUILD_B) != NULL;
        (void)cbm_daemon_runtime_client_close(owner, RUNTIME_TEST_TIMEOUT_MS);
        owner = NULL;
        exited = cbm_daemon_runtime_service_wait_exited(fixture.service, RUNTIME_TEST_TIMEOUT_MS);
    }

    if (rejected) {
        (void)cbm_daemon_runtime_client_close(rejected, RUNTIME_TEST_TIMEOUT_MS);
    }
    if (owner) {
        (void)cbm_daemon_runtime_client_close(owner, RUNTIME_TEST_TIMEOUT_MS);
    }
    runtime_test_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_TRUE(explicit_conflict);
    ASSERT_TRUE(owner_unchanged);
    ASSERT_TRUE(logged);
    ASSERT_TRUE(exited);
    PASS();
}

TEST(daemon_runtime_conflict_log_failure_uses_operation_log_fallback) {
    const char *active_build = runtime_test_self_build();
    cbm_daemon_build_identity_t active = runtime_test_identity("2.4.0", active_build);
    cbm_daemon_build_identity_t rebuilt = runtime_test_identity("2.4.0", RUNTIME_BUILD_B);
    runtime_test_fixture_t fixture;
    bool started = runtime_test_fixture_start(&fixture, "conflict-log-fallback", &active);
    bool obstruction_created = started && cbm_mkdir_p(fixture.log_path, 0700);
    cbm_daemon_runtime_connect_result_t owner_result = {0};
    cbm_daemon_runtime_connect_result_t conflict_result = {0};
    cbm_daemon_runtime_client_t *owner = NULL;
    cbm_daemon_runtime_client_t *rejected = NULL;
    CBMLogLevel prior_level = cbm_log_get_level();
    bool explicit_conflict = false;
    bool exited = false;

    atomic_store_explicit(&runtime_conflict_log_fallback_seen, false, memory_order_release);
    if (obstruction_created) {
        cbm_log_set_level(CBM_LOG_ERROR);
        cbm_log_set_sink(runtime_test_conflict_log_fallback_sink);
        owner = cbm_daemon_runtime_client_connect(fixture.endpoint, &active,
                                                  RUNTIME_TEST_TIMEOUT_MS, &owner_result);
    }
    if (owner) {
        rejected = cbm_daemon_runtime_client_connect(fixture.endpoint, &rebuilt,
                                                     RUNTIME_TEST_TIMEOUT_MS, &conflict_result);
        explicit_conflict = rejected == NULL &&
                            conflict_result.status == CBM_DAEMON_RUNTIME_CONNECT_CONFLICT &&
                            conflict_result.hello_status == CBM_DAEMON_HELLO_BUILD_CONFLICT;
        if (!explicit_conflict) {
            printf("  conflict fallback diagnostic: owner=%d rejected=%d owner_status=%d "
                   "conflict_status=%d hello_status=%d message=%s\n",
                   owner != NULL ? 1 : 0, rejected != NULL ? 1 : 0, (int)owner_result.status,
                   (int)conflict_result.status, (int)conflict_result.hello_status,
                   conflict_result.message);
        }
        (void)cbm_daemon_runtime_client_close(owner, RUNTIME_TEST_TIMEOUT_MS);
        owner = NULL;
        exited = cbm_daemon_runtime_service_wait_exited(fixture.service, RUNTIME_TEST_TIMEOUT_MS);
    }
    cbm_log_set_sink(NULL);
    cbm_log_set_level(prior_level);

    if (rejected) {
        (void)cbm_daemon_runtime_client_close(rejected, RUNTIME_TEST_TIMEOUT_MS);
    }
    if (owner) {
        (void)cbm_daemon_runtime_client_close(owner, RUNTIME_TEST_TIMEOUT_MS);
    }
    bool fallback_seen =
        atomic_load_explicit(&runtime_conflict_log_fallback_seen, memory_order_acquire);
    if (obstruction_created) {
        (void)cbm_rmdir(fixture.log_path);
    }
    runtime_test_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_TRUE(obstruction_created);
    ASSERT_TRUE(explicit_conflict);
    ASSERT_TRUE(fallback_seen);
    ASSERT_TRUE(exited);
    PASS();
}

TEST(daemon_runtime_disconnect_releases_only_connection_subscriptions) {
    static const char project[] = "runtime-shared-project";
    cbm_daemon_build_identity_t identity =
        runtime_test_identity("2.4.0", runtime_test_self_build());
    runtime_test_fixture_t fixture;
    bool started = runtime_test_fixture_start(&fixture, "owned-subscriptions", &identity);
    cbm_daemon_runtime_client_t *first = NULL;
    cbm_daemon_runtime_client_t *second = NULL;
    cbm_daemon_runtime_connect_result_t first_result = {0};
    cbm_daemon_runtime_connect_result_t second_result = {0};
    cbm_daemon_subscription_id_t first_subscription = CBM_DAEMON_SUBSCRIPTION_ID_INVALID;
    cbm_daemon_subscription_id_t second_subscription = CBM_DAEMON_SUBSCRIPTION_ID_INVALID;
    bool subscribed = false;
    bool second_survived = false;
    bool reaped = false;
    bool exited = false;

    if (started) {
        first = cbm_daemon_runtime_client_connect(fixture.endpoint, &identity,
                                                  RUNTIME_TEST_TIMEOUT_MS, &first_result);
        second = cbm_daemon_runtime_client_connect(fixture.endpoint, &identity,
                                                   RUNTIME_TEST_TIMEOUT_MS, &second_result);
    }
    if (first && second) {
        cbm_daemon_subscription_result_t first_status = cbm_daemon_runtime_client_job_subscribe(
            first, project, &first_subscription, RUNTIME_TEST_TIMEOUT_MS);
        cbm_daemon_subscription_result_t second_status = cbm_daemon_runtime_client_job_subscribe(
            second, project, &second_subscription, RUNTIME_TEST_TIMEOUT_MS);
        subscribed = first_status == CBM_DAEMON_SUBSCRIPTION_STARTED &&
                     second_status == CBM_DAEMON_SUBSCRIPTION_JOINED &&
                     first_subscription != CBM_DAEMON_SUBSCRIPTION_ID_INVALID &&
                     second_subscription != CBM_DAEMON_SUBSCRIPTION_ID_INVALID &&
                     first_subscription != second_subscription &&
                     cbm_daemon_runtime_service_job_subscribers(fixture.service, project) == 2;

        (void)cbm_daemon_runtime_client_close(first, RUNTIME_TEST_TIMEOUT_MS);
        first = NULL;
        second_survived =
            cbm_daemon_runtime_service_wait_for_clients(fixture.service, 1,
                                                        RUNTIME_TEST_TIMEOUT_MS) &&
            cbm_daemon_runtime_service_job_subscribers(fixture.service, project) == 1 &&
            cbm_daemon_runtime_client_job_unsubscribe(second, second_subscription,
                                                      RUNTIME_TEST_TIMEOUT_MS) &&
            cbm_daemon_runtime_service_job_subscribers(fixture.service, project) == 0;
        reaped = cbm_daemon_runtime_service_job_reaped(fixture.service, project);
        (void)cbm_daemon_runtime_client_close(second, RUNTIME_TEST_TIMEOUT_MS);
        second = NULL;
        exited = cbm_daemon_runtime_service_wait_exited(fixture.service, RUNTIME_TEST_TIMEOUT_MS);
    }

    if (second) {
        (void)cbm_daemon_runtime_client_close(second, RUNTIME_TEST_TIMEOUT_MS);
    }
    if (first) {
        (void)cbm_daemon_runtime_client_close(first, RUNTIME_TEST_TIMEOUT_MS);
    }
    runtime_test_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_TRUE(subscribed);
    ASSERT_TRUE(second_survived);
    ASSERT_TRUE(reaped);
    ASSERT_TRUE(exited);
    PASS();
}

TEST(daemon_runtime_final_disconnect_automatically_exits_within_bound) {
    cbm_daemon_build_identity_t identity =
        runtime_test_identity("2.4.0", runtime_test_self_build());
    runtime_test_fixture_t fixture;
    bool started = runtime_test_fixture_start(&fixture, "bounded-exit", &identity);
    cbm_daemon_runtime_client_t *client = NULL;
    cbm_daemon_runtime_connect_result_t result = {0};
    bool terminal_transition = false;
    bool exited = false;

    if (started) {
        client = cbm_daemon_runtime_client_connect(fixture.endpoint, &identity,
                                                   RUNTIME_TEST_TIMEOUT_MS, &result);
    }
    if (client) {
        (void)cbm_daemon_runtime_client_close(client, RUNTIME_TEST_TIMEOUT_MS);
        client = NULL;
        cbm_daemon_runtime_service_state_t after_close =
            cbm_daemon_runtime_service_state(fixture.service);
        terminal_transition = after_close == CBM_DAEMON_RUNTIME_SERVICE_STOPPING ||
                              after_close == CBM_DAEMON_RUNTIME_SERVICE_EXITED;
        exited = cbm_daemon_runtime_service_wait_exited(fixture.service, RUNTIME_TEST_TIMEOUT_MS);
    }

    if (client) {
        (void)cbm_daemon_runtime_client_close(client, RUNTIME_TEST_TIMEOUT_MS);
    }
    runtime_test_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_TRUE(terminal_transition);
    ASSERT_TRUE(exited);
    PASS();
}

TEST(daemon_runtime_authenticated_idle_connection_outlives_lease_interval) {
    cbm_daemon_build_identity_t identity =
        runtime_test_identity("2.4.0", runtime_test_self_build());
    runtime_test_fixture_t fixture;
    bool started =
        runtime_test_fixture_start_configured(&fixture, "idle-connection", &identity, 8, 20, NULL);
    cbm_daemon_runtime_connect_result_t result = {0};
    cbm_daemon_runtime_client_t *client = NULL;
    bool remained_connected = false;
    bool exited = false;

    if (started) {
        client = cbm_daemon_runtime_client_connect(fixture.endpoint, &identity,
                                                   RUNTIME_TEST_TIMEOUT_MS, &result);
    }
    if (client) {
        struct timespec beyond_lease = {.tv_sec = 0, .tv_nsec = 60000000};
        (void)cbm_nanosleep(&beyond_lease, NULL);
        remained_connected = cbm_daemon_runtime_service_active_clients(fixture.service) == 1 &&
                             cbm_daemon_runtime_client_heartbeat(client, RUNTIME_TEST_TIMEOUT_MS);
        (void)cbm_daemon_runtime_client_close(client, RUNTIME_TEST_TIMEOUT_MS);
        client = NULL;
        exited = cbm_daemon_runtime_service_wait_exited(fixture.service, RUNTIME_TEST_TIMEOUT_MS);
    }

    if (client) {
        (void)cbm_daemon_runtime_client_close(client, RUNTIME_TEST_TIMEOUT_MS);
    }
    runtime_test_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_TRUE(remained_connected);
    ASSERT_TRUE(exited);
    PASS();
}

TEST(daemon_runtime_connection_cap_covers_slow_hello_and_stopping_is_terminal) {
    cbm_daemon_build_identity_t identity =
        runtime_test_identity("2.4.0", runtime_test_self_build());
    runtime_test_fixture_t fixture;
    bool started = runtime_test_fixture_start_limited(&fixture, "connection-cap", &identity, 2);
    cbm_daemon_ipc_connection_t *slow_hello = NULL;
    cbm_daemon_runtime_client_t *accepted = NULL;
    cbm_daemon_runtime_client_t *overflow = NULL;
    cbm_daemon_runtime_client_t *resurrection = NULL;
    cbm_daemon_runtime_connect_result_t accepted_result = {0};
    cbm_daemon_runtime_connect_result_t overflow_result = {0};
    cbm_daemon_runtime_connect_result_t resurrection_result = {0};
    bool slow_slot_counted = false;
    bool capacity_rejected = false;
    bool slow_slot_released = false;
    bool exited = false;
    bool no_resurrection = false;

    if (started) {
        slow_hello = cbm_daemon_ipc_connect(fixture.endpoint, RUNTIME_TEST_TIMEOUT_MS);
    }
    if (slow_hello) {
        slow_slot_counted = cbm_daemon_runtime_service_wait_for_connections(
            fixture.service, 1, RUNTIME_TEST_TIMEOUT_MS);
        accepted = cbm_daemon_runtime_client_connect(fixture.endpoint, &identity,
                                                     RUNTIME_TEST_TIMEOUT_MS, &accepted_result);
    }
    if (accepted) {
        overflow = cbm_daemon_runtime_client_connect(fixture.endpoint, &identity,
                                                     RUNTIME_TEST_TIMEOUT_MS, &overflow_result);
        capacity_rejected = overflow == NULL &&
                            overflow_result.status == CBM_DAEMON_RUNTIME_CONNECT_REJECTED &&
                            strstr(overflow_result.message, "capacity") != NULL &&
                            cbm_daemon_runtime_service_active_connections(fixture.service) == 2 &&
                            cbm_daemon_runtime_service_active_clients(fixture.service) == 1;

        cbm_daemon_ipc_connection_close(slow_hello);
        slow_hello = NULL;
        slow_slot_released = cbm_daemon_runtime_service_wait_for_connections(
            fixture.service, 1, RUNTIME_TEST_TIMEOUT_MS);
        (void)cbm_daemon_runtime_client_close(accepted, RUNTIME_TEST_TIMEOUT_MS);
        accepted = NULL;
        exited = cbm_daemon_runtime_service_wait_exited(fixture.service, RUNTIME_TEST_TIMEOUT_MS);

        resurrection = cbm_daemon_runtime_client_connect(fixture.endpoint, &identity, 100,
                                                         &resurrection_result);
        cbm_daemon_runtime_service_state_t terminal_state =
            cbm_daemon_runtime_service_state(fixture.service);
        no_resurrection = resurrection == NULL &&
                          (resurrection_result.status == CBM_DAEMON_RUNTIME_CONNECT_ERROR ||
                           resurrection_result.status == CBM_DAEMON_RUNTIME_CONNECT_REJECTED) &&
                          cbm_daemon_runtime_service_active_clients(fixture.service) == 0 &&
                          (terminal_state == CBM_DAEMON_RUNTIME_SERVICE_STOPPING ||
                           terminal_state == CBM_DAEMON_RUNTIME_SERVICE_EXITED);
    }

    if (resurrection) {
        (void)cbm_daemon_runtime_client_close(resurrection, RUNTIME_TEST_TIMEOUT_MS);
    }
    if (overflow) {
        (void)cbm_daemon_runtime_client_close(overflow, RUNTIME_TEST_TIMEOUT_MS);
    }
    if (accepted) {
        (void)cbm_daemon_runtime_client_close(accepted, RUNTIME_TEST_TIMEOUT_MS);
    }
    cbm_daemon_ipc_connection_close(slow_hello);
    runtime_test_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_TRUE(slow_slot_counted);
    ASSERT_TRUE(capacity_rejected);
    ASSERT_TRUE(slow_slot_released);
    ASSERT_TRUE(exited);
    ASSERT_TRUE(no_resurrection);
    PASS();
}

TEST(daemon_runtime_rejects_forged_identity_extension) {
    cbm_daemon_build_identity_t identity =
        runtime_test_identity("2.4.0", runtime_test_self_build());
    runtime_test_fixture_t fixture;
    bool started = runtime_test_fixture_start(&fixture, "forged-identity", &identity);
    cbm_daemon_ipc_connection_t *raw = NULL;
    uint8_t forged[CBM_DAEMON_RENDEZVOUS_REQUEST_SIZE + sizeof(uint64_t) * 2] = {0};
    uint64_t forged_client_id = UINT64_MAX - 1;
    uint64_t forged_process_id = UINT64_MAX;
    bool encoded = false;
    bool sent = false;
    bool rejected = false;
    cbm_daemon_frame_t response_frame = {0};
    uint8_t *response_payload = NULL;

    if (started) {
        encoded = cbm_daemon_runtime_hello_request_encode(forged, &identity);
        memcpy(forged + CBM_DAEMON_RENDEZVOUS_REQUEST_SIZE, &forged_client_id,
               sizeof(forged_client_id));
        memcpy(forged + CBM_DAEMON_RENDEZVOUS_REQUEST_SIZE + sizeof(forged_client_id),
               &forged_process_id, sizeof(forged_process_id));
        raw = cbm_daemon_ipc_connect(fixture.endpoint, RUNTIME_TEST_TIMEOUT_MS);
    }
    if (raw && encoded) {
        sent = cbm_daemon_ipc_send_frame(raw, CBM_DAEMON_FRAME_REQUEST, CBM_DAEMON_RUNTIME_OP_HELLO,
                                         forged, (uint32_t)sizeof(forged));
        int received = cbm_daemon_ipc_receive_frame(raw, RUNTIME_TEST_TIMEOUT_MS, &response_frame,
                                                    &response_payload);
        rejected = received != 1 && cbm_daemon_runtime_service_active_clients(fixture.service) == 0;
    }
    free(response_payload);
    cbm_daemon_ipc_connection_close(raw);

    /* A malformed peer must not poison the stable service for valid clients. */
    cbm_daemon_runtime_connect_result_t valid_result = {0};
    cbm_daemon_runtime_client_t *valid = NULL;
    bool valid_after_rejection = false;
    bool exited = false;
    if (started) {
        valid = cbm_daemon_runtime_client_connect(fixture.endpoint, &identity,
                                                  RUNTIME_TEST_TIMEOUT_MS, &valid_result);
    }
    if (valid) {
        valid_after_rejection =
            valid_result.status == CBM_DAEMON_RUNTIME_CONNECT_ACCEPTED &&
            valid_result.authenticated_process_id == runtime_test_process_id() &&
            valid_result.client_id != forged_client_id;
        (void)cbm_daemon_runtime_client_close(valid, RUNTIME_TEST_TIMEOUT_MS);
        valid = NULL;
        exited = cbm_daemon_runtime_service_wait_exited(fixture.service, RUNTIME_TEST_TIMEOUT_MS);
    }

    if (valid) {
        (void)cbm_daemon_runtime_client_close(valid, RUNTIME_TEST_TIMEOUT_MS);
    }
    runtime_test_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_TRUE(encoded);
    ASSERT_TRUE(sent);
    ASSERT_TRUE(rejected);
    ASSERT_TRUE(valid_after_rejection);
    ASSERT_TRUE(exited);
    PASS();
}

TEST(daemon_runtime_application_response_roundtrip_is_byte_exact) {
    static const uint8_t request[] = {
        0x00, 0x7f, 0x80, 0xff, 'c', 'b', 'm', 0x00, 0x13,
    };
    cbm_daemon_build_identity_t identity =
        runtime_test_identity("2.4.0", runtime_test_self_build());
    runtime_application_context_t context;
    runtime_application_context_init(&context, false);
    runtime_test_fixture_t fixture;
    bool started = runtime_test_fixture_start_application(&fixture, "application-roundtrip",
                                                          &identity, &context);
    cbm_daemon_runtime_connect_result_t result = {0};
    cbm_daemon_runtime_client_t *client = NULL;
    uint8_t *response = NULL;
    uint32_t response_length = 0;
    cbm_daemon_runtime_application_status_t status = CBM_DAEMON_RUNTIME_APPLICATION_TRANSPORT_ERROR;
    bool exact = false;
    bool closed = false;
    bool exited = false;

    if (started) {
        client = cbm_daemon_runtime_client_connect(fixture.endpoint, &identity,
                                                   RUNTIME_TEST_TIMEOUT_MS, &result);
    }
    if (client) {
        status = cbm_daemon_runtime_client_application_request(
            client, request, (uint32_t)sizeof(request), &response, &response_length,
            RUNTIME_TEST_TIMEOUT_MS);
        exact = status == CBM_DAEMON_RUNTIME_APPLICATION_OK && response_length == sizeof(request) &&
                response && memcmp(response, request, sizeof(request)) == 0;
        free(response);
        response = NULL;
        closed = cbm_daemon_runtime_client_close(client, RUNTIME_TEST_TIMEOUT_MS);
        client = NULL;
        exited = cbm_daemon_runtime_service_wait_exited(fixture.service, RUNTIME_TEST_TIMEOUT_MS);
    }

    free(response);
    if (client) {
        (void)cbm_daemon_runtime_client_close(client, RUNTIME_TEST_TIMEOUT_MS);
    }
    runtime_test_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_TRUE(exact);
    ASSERT_TRUE(closed);
    ASSERT_TRUE(exited);
    ASSERT_EQ(atomic_load(&context.opened), 1);
    ASSERT_EQ(atomic_load(&context.requests), 1);
    /* A close after a completed exchange has nothing to cancel: the client
     * only sends APPLICATION_CANCEL for an interrupted in-flight token, and
     * the server treats stale tokens as deliberate one-way no-ops. */
    ASSERT_EQ(atomic_load(&context.request_cancels), 0);
    ASSERT_EQ(atomic_load(&context.cancelled), 1);
    ASSERT_EQ(atomic_load(&context.closed), 1);
    PASS();
}

TEST(daemon_runtime_final_disconnect_rejects_blocked_provisional_session) {
    cbm_daemon_build_identity_t identity =
        runtime_test_identity("2.4.0", runtime_test_self_build());
    runtime_application_context_t context;
    runtime_application_context_init(&context, false);
    atomic_store_explicit(&context.block_second_open, true, memory_order_release);
    runtime_test_fixture_t fixture;
    bool started = runtime_test_fixture_start_application(&fixture, "application-open-race",
                                                          &identity, &context);
    cbm_daemon_runtime_connect_result_t owner_result = {0};
    cbm_daemon_runtime_client_t *owner = NULL;
    runtime_application_connect_call_t contender = {
        .identity = identity,
    };
    atomic_init(&contender.completed, false);
    cbm_thread_t connect_thread;
    int connect_thread_create_rc = -1;
    int connect_thread_join_rc = -1;
    bool connect_thread_started = false;
    bool provisional_started = false;
    bool owner_closed = false;
    bool shutdown_won = false;
    bool contender_accepted = false;
    bool exited = false;

    if (started) {
        owner = cbm_daemon_runtime_client_connect(fixture.endpoint, &identity,
                                                  RUNTIME_TEST_TIMEOUT_MS, &owner_result);
    }
    if (owner) {
        contender.endpoint = fixture.endpoint;
        connect_thread_create_rc = cbm_thread_create(
            &connect_thread, 128U * 1024U, runtime_application_client_connect_thread, &contender);
        connect_thread_started = connect_thread_create_rc == 0;
        provisional_started =
            connect_thread_started &&
            runtime_test_wait_atomic_bool(&context.second_open_started, RUNTIME_TEST_TIMEOUT_MS);
    }
    if (provisional_started) {
        owner_closed = cbm_daemon_runtime_client_close(owner, RUNTIME_TEST_TIMEOUT_MS);
        owner = NULL;
        shutdown_won = cbm_daemon_runtime_service_state(fixture.service) ==
                       CBM_DAEMON_RUNTIME_SERVICE_STOPPING;
    }

    /* Release on every setup outcome so neither the server worker nor the
     * helper can retain stack-owned test state during cleanup. */
    atomic_store_explicit(&context.release_second_open, true, memory_order_release);
    if (connect_thread_started) {
        connect_thread_join_rc = cbm_thread_join(&connect_thread);
        if (connect_thread_join_rc == 0) {
            connect_thread_started = false;
        }
    }
    if (connect_thread_started) {
        while (!atomic_load_explicit(&contender.completed, memory_order_acquire)) {
            struct timespec pause = {.tv_sec = 0, .tv_nsec = 1000000};
            (void)cbm_nanosleep(&pause, NULL);
        }
    }
    contender_accepted = contender.client != NULL;
    if (owner) {
        (void)cbm_daemon_runtime_client_close(owner, RUNTIME_TEST_TIMEOUT_MS);
        owner = NULL;
    }
    if (contender.client) {
        (void)cbm_daemon_runtime_client_close(contender.client, RUNTIME_TEST_TIMEOUT_MS);
        contender.client = NULL;
    }
    if (started) {
        exited = cbm_daemon_runtime_service_wait_exited(fixture.service, RUNTIME_TEST_TIMEOUT_MS);
    }
    runtime_test_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_EQ(owner_result.status, CBM_DAEMON_RUNTIME_CONNECT_ACCEPTED);
    ASSERT_EQ(connect_thread_create_rc, 0);
    ASSERT_TRUE(provisional_started);
    ASSERT_TRUE(owner_closed);
    ASSERT_TRUE(shutdown_won);
    ASSERT_EQ(connect_thread_join_rc, 0);
    ASSERT_TRUE(atomic_load_explicit(&contender.completed, memory_order_acquire));
    ASSERT_FALSE(contender_accepted);
    ASSERT_TRUE(exited);
    ASSERT_EQ(atomic_load(&context.opened), 2);
    ASSERT_EQ(atomic_load(&context.cancelled), 2);
    ASSERT_EQ(atomic_load(&context.closed), 2);
    PASS();
}

TEST(daemon_runtime_request_cancel_is_exact_and_session_remains_usable) {
    static const uint8_t blocking_request[] = {'b', 'l', 'o', 'c', 'k'};
    static const uint8_t next_request[] = {0xde, 0xad, 0x00, 0xbe, 0xef};
    cbm_daemon_build_identity_t identity =
        runtime_test_identity("2.4.0", runtime_test_self_build());
    runtime_application_context_t context;
    runtime_application_context_init(&context, true);
    atomic_bool request_thread_completed;
    atomic_init(&request_thread_completed, false);
    runtime_test_fixture_t fixture;
    bool started = runtime_test_fixture_start_application(&fixture, "application-request-cancel",
                                                          &identity, &context);
    cbm_daemon_runtime_connect_result_t result = {0};
    cbm_daemon_runtime_client_t *client = NULL;
    cbm_daemon_runtime_application_token_t request_token =
        CBM_DAEMON_RUNTIME_APPLICATION_TOKEN_INVALID;
    runtime_application_client_call_t call = {
        .request = blocking_request,
        .request_length = (uint32_t)sizeof(blocking_request),
        .tagged = true,
        .completed = &request_thread_completed,
        .status = CBM_DAEMON_RUNTIME_APPLICATION_TRANSPORT_ERROR,
    };
    cbm_thread_t request_thread;
    int request_thread_create_rc = -1;
    int request_thread_join_rc = -1;
    bool request_thread_join_attempted = false;
    bool token_reserved = false;
    bool request_thread_started = false;
    bool callback_started = false;
    cbm_daemon_runtime_cancel_result_t wrong_cancel = CBM_DAEMON_RUNTIME_CANCEL_ERROR;
    int cancels_after_wrong = -1;
    cbm_daemon_runtime_cancel_result_t exact_cancel = CBM_DAEMON_RUNTIME_CANCEL_ERROR;
    bool cancel_delivered = false;
    cbm_daemon_runtime_cancel_result_t late_duplicate_cancel = CBM_DAEMON_RUNTIME_CANCEL_ERROR;
    int cancels_after_duplicate = -1;
    bool close_begun = false;
    bool request_thread_joined = false;
    uint8_t *next_response = NULL;
    uint32_t next_response_length = 0;
    cbm_daemon_runtime_application_status_t next_status =
        CBM_DAEMON_RUNTIME_APPLICATION_TRANSPORT_ERROR;
    bool next_exact = false;
    bool heartbeat = false;
    bool closed = false;
    bool exited = false;

    if (started) {
        client = cbm_daemon_runtime_client_connect(fixture.endpoint, &identity,
                                                   RUNTIME_TEST_TIMEOUT_MS, &result);
    }
    if (client) {
        token_reserved =
            cbm_daemon_runtime_client_application_token_reserve(client, &request_token);
    }
    if (token_reserved) {
        call.client = client;
        call.request_token = request_token;
        request_thread_create_rc = cbm_thread_create(
            &request_thread, 128U * 1024U, runtime_application_client_request_thread, &call);
        request_thread_started = request_thread_create_rc == 0;
        if (!request_thread_started) {
            printf("  runtime helper thread create failed: rc=%d\n", request_thread_create_rc);
        }
        callback_started =
            request_thread_started &&
            runtime_test_wait_atomic_bool(&context.first_request_started, RUNTIME_TEST_TIMEOUT_MS);
    }
    if (callback_started) {
        wrong_cancel = cbm_daemon_runtime_client_application_cancel(client, request_token + 1U);
        cancels_after_wrong = atomic_load_explicit(&context.request_cancels, memory_order_acquire);
        exact_cancel = cbm_daemon_runtime_client_application_cancel(client, request_token);
        cancel_delivered =
            exact_cancel == CBM_DAEMON_RUNTIME_CANCEL_ACCEPTED &&
            runtime_test_wait_atomic_int(&context.request_cancels, 1, RUNTIME_TEST_TIMEOUT_MS);
    }
    if (request_thread_started && !cancel_delivered && client) {
        close_begun = cbm_daemon_runtime_client_close_begin(client);
    }
    if (request_thread_started && (cancel_delivered || close_begun)) {
        request_thread_join_attempted = true;
        request_thread_join_rc = cbm_thread_join(&request_thread);
        request_thread_joined = request_thread_join_rc == 0;
        if (request_thread_joined) {
            request_thread_started = false;
        } else {
            printf("  runtime helper thread join failed: rc=%d\n", request_thread_join_rc);
        }
    }
    if (request_thread_joined && cancel_delivered && !close_begun) {
        late_duplicate_cancel = cbm_daemon_runtime_client_application_cancel(client, request_token);
        cancels_after_duplicate =
            atomic_load_explicit(&context.request_cancels, memory_order_acquire);
        next_status = cbm_daemon_runtime_client_application_request(
            client, next_request, (uint32_t)sizeof(next_request), &next_response,
            &next_response_length, RUNTIME_TEST_TIMEOUT_MS);
        next_exact = next_status == CBM_DAEMON_RUNTIME_APPLICATION_OK &&
                     next_response_length == sizeof(next_request) && next_response &&
                     memcmp(next_response, next_request, sizeof(next_request)) == 0;
        heartbeat = cbm_daemon_runtime_client_heartbeat(client, RUNTIME_TEST_TIMEOUT_MS);
    }
    free(next_response);
    if (request_thread_started) {
        /* Keep call/client storage alive until the helper's final release
         * store even when an exceptional OS join failure prevents proving
         * termination through the thread API. */
        while (!atomic_load_explicit(&request_thread_completed, memory_order_acquire)) {
            struct timespec pause = {.tv_sec = 0, .tv_nsec = 1000000};
            (void)cbm_nanosleep(&pause, NULL);
        }
        if (!request_thread_join_attempted) {
            request_thread_join_attempted = true;
            request_thread_join_rc = cbm_thread_join(&request_thread);
            request_thread_joined = request_thread_join_rc == 0;
            if (request_thread_joined) {
                request_thread_started = false;
            } else {
                printf("  runtime helper thread join failed: rc=%d\n", request_thread_join_rc);
            }
        }
    }
    if (client) {
        closed = close_begun
                     ? cbm_daemon_runtime_client_close_finish(client, RUNTIME_TEST_TIMEOUT_MS)
                     : cbm_daemon_runtime_client_close(client, RUNTIME_TEST_TIMEOUT_MS);
        client = NULL;
    }
    if (started) {
        exited = cbm_daemon_runtime_service_wait_exited(fixture.service, RUNTIME_TEST_TIMEOUT_MS);
    }
    bool cancelled_response = call.status == CBM_DAEMON_RUNTIME_APPLICATION_CANCELLED &&
                              call.response == NULL && call.response_length == 0;
    free(call.response);
    runtime_test_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_TRUE(token_reserved);
    ASSERT_EQ(request_thread_create_rc, 0);
    ASSERT_TRUE(request_thread_join_attempted);
    ASSERT_EQ(request_thread_join_rc, 0);
    ASSERT_TRUE(callback_started);
    ASSERT_EQ(wrong_cancel, CBM_DAEMON_RUNTIME_CANCEL_STALE);
    ASSERT_EQ(cancels_after_wrong, 0);
    ASSERT_EQ(exact_cancel, CBM_DAEMON_RUNTIME_CANCEL_ACCEPTED);
    ASSERT_TRUE(cancel_delivered);
    ASSERT_TRUE(request_thread_joined);
    ASSERT_TRUE(cancelled_response);
    ASSERT_EQ(late_duplicate_cancel, CBM_DAEMON_RUNTIME_CANCEL_STALE);
    ASSERT_EQ(cancels_after_duplicate, 1);
    ASSERT_TRUE(next_exact);
    ASSERT_TRUE(heartbeat);
    ASSERT_TRUE(closed);
    ASSERT_TRUE(exited);
    ASSERT_EQ(atomic_load(&context.opened), 1);
    ASSERT_EQ(atomic_load(&context.requests), 2);
    ASSERT_EQ(atomic_load(&context.request_cancels), 1);
    ASSERT_EQ(atomic_load(&context.cancelled), 1);
    ASSERT_EQ(atomic_load(&context.closed), 1);
    PASS();
}

TEST(daemon_runtime_presend_request_cancel_is_sticky_and_nonterminal) {
    static const uint8_t blocking_request[] = {'p', 'r', 'e'};
    static const uint8_t next_request[] = {'n', 'e', 'x', 't'};
    cbm_daemon_build_identity_t identity =
        runtime_test_identity("2.4.0", runtime_test_self_build());
    runtime_application_context_t context;
    runtime_application_context_init(&context, true);
    runtime_test_fixture_t fixture;
    bool started = runtime_test_fixture_start_application(&fixture, "application-presend-cancel",
                                                          &identity, &context);
    cbm_daemon_runtime_connect_result_t result = {0};
    cbm_daemon_runtime_client_t *client = NULL;
    cbm_daemon_runtime_application_token_t request_token =
        CBM_DAEMON_RUNTIME_APPLICATION_TOKEN_INVALID;
    bool token_reserved = false;
    cbm_daemon_runtime_cancel_result_t cancel = CBM_DAEMON_RUNTIME_CANCEL_ERROR;
    uint8_t *cancelled_response = NULL;
    uint32_t cancelled_response_length = 0;
    cbm_daemon_runtime_application_status_t cancelled_status =
        CBM_DAEMON_RUNTIME_APPLICATION_TRANSPORT_ERROR;
    uint8_t *next_response = NULL;
    uint32_t next_response_length = 0;
    cbm_daemon_runtime_application_status_t next_status =
        CBM_DAEMON_RUNTIME_APPLICATION_TRANSPORT_ERROR;
    bool next_exact = false;
    bool closed = false;
    bool exited = false;

    if (started) {
        client = cbm_daemon_runtime_client_connect(fixture.endpoint, &identity,
                                                   RUNTIME_TEST_TIMEOUT_MS, &result);
    }
    if (client) {
        token_reserved =
            cbm_daemon_runtime_client_application_token_reserve(client, &request_token);
    }
    if (token_reserved) {
        cancel = cbm_daemon_runtime_client_application_cancel(client, request_token);
        cancelled_status = cbm_daemon_runtime_client_application_request_tagged(
            client, request_token, blocking_request, (uint32_t)sizeof(blocking_request),
            &cancelled_response, &cancelled_response_length, RUNTIME_TEST_TIMEOUT_MS);
    }
    if (cancelled_status == CBM_DAEMON_RUNTIME_APPLICATION_CANCELLED) {
        next_status = cbm_daemon_runtime_client_application_request(
            client, next_request, (uint32_t)sizeof(next_request), &next_response,
            &next_response_length, RUNTIME_TEST_TIMEOUT_MS);
        next_exact = next_status == CBM_DAEMON_RUNTIME_APPLICATION_OK &&
                     next_response_length == sizeof(next_request) && next_response &&
                     memcmp(next_response, next_request, sizeof(next_request)) == 0;
    }
    free(cancelled_response);
    free(next_response);
    if (client) {
        closed = cbm_daemon_runtime_client_close(client, RUNTIME_TEST_TIMEOUT_MS);
        client = NULL;
    }
    if (started) {
        exited = cbm_daemon_runtime_service_wait_exited(fixture.service, RUNTIME_TEST_TIMEOUT_MS);
    }
    runtime_test_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_TRUE(token_reserved);
    ASSERT_EQ(cancel, CBM_DAEMON_RUNTIME_CANCEL_ACCEPTED);
    ASSERT_EQ(cancelled_status, CBM_DAEMON_RUNTIME_APPLICATION_CANCELLED);
    ASSERT_EQ(cancelled_response_length, 0);
    ASSERT_TRUE(next_exact);
    ASSERT_TRUE(closed);
    ASSERT_TRUE(exited);
    ASSERT_EQ(atomic_load(&context.opened), 1);
    ASSERT_EQ(atomic_load(&context.requests), 2);
    ASSERT_EQ(atomic_load(&context.request_cancels), 1);
    ASSERT_EQ(atomic_load(&context.cancelled), 1);
    ASSERT_EQ(atomic_load(&context.closed), 1);
    PASS();
}

TEST(daemon_runtime_allows_only_one_unstarted_application_token) {
    static const uint8_t first_request[] = {'f', 'i', 'r', 's', 't'};
    static const uint8_t second_request[] = {'s', 'e', 'c', 'o', 'n', 'd'};
    cbm_daemon_build_identity_t identity =
        runtime_test_identity("2.4.0", runtime_test_self_build());
    runtime_application_context_t context;
    runtime_application_context_init(&context, false);
    runtime_test_fixture_t fixture;
    bool started = runtime_test_fixture_start_application(&fixture, "application-token-reservation",
                                                          &identity, &context);
    cbm_daemon_runtime_connect_result_t result = {0};
    cbm_daemon_runtime_client_t *client = NULL;
    cbm_daemon_runtime_application_token_t first_token =
        CBM_DAEMON_RUNTIME_APPLICATION_TOKEN_INVALID;
    cbm_daemon_runtime_application_token_t rejected_token = UINT64_MAX;
    cbm_daemon_runtime_application_token_t second_token =
        CBM_DAEMON_RUNTIME_APPLICATION_TOKEN_INVALID;
    bool first_reserved = false;
    bool duplicate_reservation_rejected = false;
    bool first_exact = false;
    bool second_reserved = false;
    bool second_exact = false;
    bool closed = false;
    bool exited = false;

    if (started) {
        client = cbm_daemon_runtime_client_connect(fixture.endpoint, &identity,
                                                   RUNTIME_TEST_TIMEOUT_MS, &result);
    }
    if (client) {
        first_reserved = cbm_daemon_runtime_client_application_token_reserve(client, &first_token);
        duplicate_reservation_rejected =
            !cbm_daemon_runtime_client_application_token_reserve(client, &rejected_token) &&
            rejected_token == CBM_DAEMON_RUNTIME_APPLICATION_TOKEN_INVALID;
    }
    if (first_reserved && duplicate_reservation_rejected) {
        uint8_t *response = NULL;
        uint32_t response_length = 0;
        cbm_daemon_runtime_application_status_t status =
            cbm_daemon_runtime_client_application_request_tagged(
                client, first_token, first_request, (uint32_t)sizeof(first_request), &response,
                &response_length, RUNTIME_TEST_TIMEOUT_MS);
        first_exact = status == CBM_DAEMON_RUNTIME_APPLICATION_OK &&
                      response_length == sizeof(first_request) && response &&
                      memcmp(response, first_request, sizeof(first_request)) == 0;
        free(response);
    }
    if (first_exact) {
        second_reserved =
            cbm_daemon_runtime_client_application_token_reserve(client, &second_token);
    }
    if (second_reserved) {
        uint8_t *response = NULL;
        uint32_t response_length = 0;
        cbm_daemon_runtime_application_status_t status =
            cbm_daemon_runtime_client_application_request_tagged(
                client, second_token, second_request, (uint32_t)sizeof(second_request), &response,
                &response_length, RUNTIME_TEST_TIMEOUT_MS);
        second_exact = status == CBM_DAEMON_RUNTIME_APPLICATION_OK &&
                       response_length == sizeof(second_request) && response &&
                       memcmp(response, second_request, sizeof(second_request)) == 0;
        free(response);
    }
    if (client) {
        closed = cbm_daemon_runtime_client_close(client, RUNTIME_TEST_TIMEOUT_MS);
        client = NULL;
    }
    if (started) {
        exited = cbm_daemon_runtime_service_wait_exited(fixture.service, RUNTIME_TEST_TIMEOUT_MS);
    }
    runtime_test_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_TRUE(first_reserved);
    ASSERT_TRUE(duplicate_reservation_rejected);
    ASSERT_TRUE(first_exact);
    ASSERT_TRUE(second_reserved);
    ASSERT_EQ(second_token, first_token + 1U);
    ASSERT_TRUE(second_exact);
    ASSERT_TRUE(closed);
    ASSERT_TRUE(exited);
    ASSERT_EQ(atomic_load(&context.requests), 2);
    PASS();
}

TEST(daemon_runtime_consumes_busy_application_token_before_response) {
    static const uint8_t blocking_request[] = {'b', 'l', 'o', 'c', 'k'};
    static const uint8_t busy_request[] = {'b', 'u', 's', 'y'};
    enum { FIRST_TOKEN = 41, BUSY_TOKEN = 42 };
    cbm_daemon_build_identity_t identity =
        runtime_test_identity("2.4.0", runtime_test_self_build());
    runtime_application_context_t context;
    runtime_application_context_init(&context, true);
    runtime_test_fixture_t fixture;
    bool started = runtime_test_fixture_start_application(&fixture, "application-token-replay",
                                                          &identity, &context);
    cbm_daemon_ipc_connection_t *raw = NULL;
    bool first_sent = false;
    bool first_started = false;
    bool busy_sent = false;
    bool busy_received = false;
    bool cancel_sent = false;
    bool cancellation_received = false;
    bool replay_sent = false;
    bool replay_rejected = false;
    bool exited = false;

    if (started) {
        raw = runtime_test_raw_client_connect(fixture.endpoint, &identity);
    }
    if (raw) {
        first_sent = runtime_test_raw_application_send_token(raw, FIRST_TOKEN, blocking_request,
                                                             (uint32_t)sizeof(blocking_request),
                                                             (uint32_t)sizeof(blocking_request));
        first_started = first_sent && runtime_test_wait_atomic_bool(&context.first_request_started,
                                                                    RUNTIME_TEST_TIMEOUT_MS);
    }
    if (first_started) {
        busy_sent = runtime_test_raw_application_send_token(raw, BUSY_TOKEN, busy_request,
                                                            (uint32_t)sizeof(busy_request),
                                                            (uint32_t)sizeof(busy_request));
        busy_received = busy_sent && runtime_test_raw_application_receive_status_token(
                                         raw, BUSY_TOKEN, CBM_DAEMON_RUNTIME_APPLICATION_BUSY);
    }
    if (busy_received) {
        cancel_sent = runtime_test_raw_application_cancel(raw, FIRST_TOKEN);
        cancellation_received =
            cancel_sent && runtime_test_raw_application_receive_status_token(
                               raw, FIRST_TOKEN, CBM_DAEMON_RUNTIME_APPLICATION_CANCELLED);
    }
    if (cancellation_received) {
        replay_sent = runtime_test_raw_application_send_token(raw, BUSY_TOKEN, busy_request,
                                                              (uint32_t)sizeof(busy_request),
                                                              (uint32_t)sizeof(busy_request));
    }
    if (replay_sent) {
        cbm_daemon_frame_t frame = {0};
        uint8_t *payload = NULL;
        int received = cbm_daemon_ipc_receive_frame(raw, RUNTIME_TEST_TIMEOUT_MS, &frame, &payload);
        replay_rejected = received != 1;
        free(payload);
    }
    cbm_daemon_ipc_connection_close(raw);
    raw = NULL;
    if (started) {
        exited = cbm_daemon_runtime_service_wait_exited(fixture.service, RUNTIME_TEST_TIMEOUT_MS);
    }
    runtime_test_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_TRUE(first_sent);
    ASSERT_TRUE(first_started);
    ASSERT_TRUE(busy_sent);
    ASSERT_TRUE(busy_received);
    ASSERT_TRUE(cancel_sent);
    ASSERT_TRUE(cancellation_received);
    ASSERT_TRUE(replay_sent);
    ASSERT_TRUE(replay_rejected);
    ASSERT_TRUE(exited);
    ASSERT_EQ(atomic_load(&context.requests), 1);
    ASSERT_EQ(atomic_load(&context.request_cancels), 1);
    PASS();
}

TEST(daemon_runtime_close_begin_retains_storage_and_rejects_late_exchange) {
    static const uint8_t request[] = {'t', 'o', 'o', '-', 'l', 'a', 't', 'e'};
    cbm_daemon_build_identity_t identity =
        runtime_test_identity("2.4.0", runtime_test_self_build());
    runtime_application_context_t context;
    runtime_application_context_init(&context, false);
    runtime_test_fixture_t fixture;
    bool started = runtime_test_fixture_start_application(&fixture, "application-close-gap",
                                                          &identity, &context);
    cbm_daemon_runtime_connect_result_t result = {0};
    cbm_daemon_runtime_client_t *client = NULL;
    uint8_t *response = NULL;
    uint32_t response_length = 0;
    cbm_daemon_runtime_application_status_t status = CBM_DAEMON_RUNTIME_APPLICATION_OK;
    bool close_begun = false;
    bool duplicate_begin_rejected = false;
    bool close_acknowledged = false;
    bool exited = false;

    if (started) {
        client = cbm_daemon_runtime_client_connect(fixture.endpoint, &identity,
                                                   RUNTIME_TEST_TIMEOUT_MS, &result);
    }
    if (client) {
        close_begun = cbm_daemon_runtime_client_close_begin(client);
        duplicate_begin_rejected = close_begun && !cbm_daemon_runtime_client_close_begin(client);
    }
    if (close_begun) {
        /* Deterministically models the frontend boundary where close begins
         * after a worker claims an item but before it enters the runtime API.
         * The retained allocation must reject the call without touching IPC. */
        status = cbm_daemon_runtime_client_application_request(
            client, request, (uint32_t)sizeof(request), &response, &response_length,
            RUNTIME_TEST_TIMEOUT_MS);
        close_acknowledged =
            cbm_daemon_runtime_client_close_finish(client, RUNTIME_TEST_TIMEOUT_MS);
        client = NULL;
        exited = cbm_daemon_runtime_service_wait_exited(fixture.service, RUNTIME_TEST_TIMEOUT_MS);
    }

    free(response);
    if (client) {
        if (close_begun) {
            (void)cbm_daemon_runtime_client_close_finish(client, RUNTIME_TEST_TIMEOUT_MS);
        } else {
            (void)cbm_daemon_runtime_client_close(client, RUNTIME_TEST_TIMEOUT_MS);
        }
    }
    runtime_test_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_TRUE(close_begun);
    ASSERT_TRUE(duplicate_begin_rejected);
    ASSERT_EQ(status, CBM_DAEMON_RUNTIME_APPLICATION_TRANSPORT_ERROR);
    ASSERT_NULL(response);
    ASSERT_EQ(response_length, 0);
    ASSERT_TRUE(close_acknowledged);
    ASSERT_TRUE(exited);
    ASSERT_EQ(atomic_load(&context.opened), 1);
    ASSERT_EQ(atomic_load(&context.requests), 0);
    ASSERT_EQ(atomic_load(&context.cancelled), 1);
    ASSERT_EQ(atomic_load(&context.closed), 1);
    PASS();
}

TEST(daemon_runtime_disconnect_cancels_blocked_application_before_exit) {
    static const uint8_t request[] = {'b', 'l', 'o', 'c', 'k'};
    cbm_daemon_build_identity_t identity =
        runtime_test_identity("2.4.0", runtime_test_self_build());
    runtime_application_context_t context;
    runtime_application_context_init(&context, true);
    atomic_bool request_thread_completed;
    atomic_init(&request_thread_completed, false);
    runtime_test_fixture_t fixture;
    bool started =
        runtime_test_fixture_start_application(&fixture, "application-cancel", &identity, &context);
    cbm_daemon_runtime_client_t *client = NULL;
    cbm_daemon_runtime_connect_result_t result = {0};
    runtime_application_client_call_t call = {
        .request = request,
        .request_length = (uint32_t)sizeof(request),
        .completed = &request_thread_completed,
        .status = CBM_DAEMON_RUNTIME_APPLICATION_OK,
    };
    cbm_thread_t request_thread;
    int request_thread_create_rc = -1;
    int request_thread_join_rc = -1;
    bool request_thread_join_attempted = false;
    bool request_thread_started = false;
    bool callback_started = false;
    bool close_begun = false;
    bool close_acknowledged = false;
    bool request_thread_joined = false;
    bool request_interrupted = false;
    bool exited = false;

    if (started) {
        client = cbm_daemon_runtime_client_connect(fixture.endpoint, &identity,
                                                   RUNTIME_TEST_TIMEOUT_MS, &result);
    }
    if (client) {
        call.client = client;
        request_thread_create_rc = cbm_thread_create(
            &request_thread, 128U * 1024U, runtime_application_client_request_thread, &call);
        request_thread_started = request_thread_create_rc == 0;
        if (!request_thread_started) {
            printf("  runtime helper thread create failed: rc=%d\n", request_thread_create_rc);
        }
        callback_started =
            request_thread_started &&
            runtime_test_wait_atomic_bool(&context.first_request_started, RUNTIME_TEST_TIMEOUT_MS);
    }
    if (callback_started) {
        close_begun = cbm_daemon_runtime_client_close_begin(client);
    }
    if (client && request_thread_started && !close_begun) {
        close_begun = cbm_daemon_runtime_client_close_begin(client);
    }
    if (request_thread_started && close_begun) {
        request_thread_join_attempted = true;
        request_thread_join_rc = cbm_thread_join(&request_thread);
        request_thread_joined = request_thread_join_rc == 0;
        if (request_thread_joined) {
            request_thread_started = false;
        } else {
            printf("  runtime helper thread join failed: rc=%d\n", request_thread_join_rc);
        }
    }

    if (request_thread_started) {
        /* A join error does not prove the helper stopped. Wait for its final
         * release-store before touching call/client/fixture state. This is
         * intentionally unbounded, matching the successful join path's
         * semantics while keeping an exceptional cleanup path free of UAF. */
        while (!atomic_load_explicit(&request_thread_completed, memory_order_acquire)) {
            struct timespec pause = {.tv_sec = 0, .tv_nsec = 1000000};
            (void)cbm_nanosleep(&pause, NULL);
        }
        if (!request_thread_join_attempted) {
            request_thread_join_attempted = true;
            request_thread_join_rc = cbm_thread_join(&request_thread);
            request_thread_joined = request_thread_join_rc == 0;
            if (request_thread_joined) {
                request_thread_started = false;
            } else {
                printf("  runtime helper thread join failed: rc=%d\n", request_thread_join_rc);
            }
        }
    }

    if (client) {
        close_acknowledged =
            close_begun ? cbm_daemon_runtime_client_close_finish(client, RUNTIME_TEST_TIMEOUT_MS)
                        : cbm_daemon_runtime_client_close(client, RUNTIME_TEST_TIMEOUT_MS);
        client = NULL;
    }
    if (started) {
        exited = cbm_daemon_runtime_service_wait_exited(fixture.service, RUNTIME_TEST_TIMEOUT_MS);
    }
    /* close_begin cancels the exchange and, on Windows, also sends the
     * active token's APPLICATION_CANCEL. Whether the local interrupt or the
     * server's CANCELLED response wins that race, the request ended promptly
     * without a payload — both outcomes are the contract. */
    request_interrupted = (call.status == CBM_DAEMON_RUNTIME_APPLICATION_TRANSPORT_ERROR ||
                           call.status == CBM_DAEMON_RUNTIME_APPLICATION_CANCELLED) &&
                          call.response == NULL && call.response_length == 0;
    free(call.response);
    runtime_test_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_EQ(request_thread_create_rc, 0);
    ASSERT_TRUE(request_thread_join_attempted);
    ASSERT_EQ(request_thread_join_rc, 0);
    ASSERT_TRUE(request_thread_joined);
    ASSERT_TRUE(callback_started);
    ASSERT_TRUE(close_begun);
    /* close_begin intentionally interrupted the in-flight transport, so the
     * two-phase close cannot also receive a DISCONNECT acknowledgement. EOF
     * still closes the server session and must cancel the request/tree. */
    ASSERT_FALSE(close_acknowledged);
    ASSERT_TRUE(request_interrupted);
    ASSERT_TRUE(exited);
    ASSERT_EQ(atomic_load(&context.opened), 1);
    ASSERT_EQ(atomic_load(&context.requests), 1);
    ASSERT_EQ(atomic_load(&context.cancelled), 1);
    ASSERT_EQ(atomic_load(&context.closed), 1);
    PASS();
}

TEST(daemon_runtime_disconnect_cancels_blocked_non_index_child_and_preserves_other_session) {
#if !defined(_WIN32) && !defined(__APPLE__) && !defined(__linux__)
    SKIP_PLATFORM("requires a queryable copied process image");
#else
    enum {
        CHILD_READY_BOUND_MS = 5000,
        CHILD_CANCEL_BOUND_MS = 3000,
        CHILD_CLEANUP_BOUND_MS = 5000,
        REQUEST_TIMEOUT_MS = 15000,
    };
    const char *old_cache = getenv("CBM_CACHE_DIR");
    const char *old_path = getenv("PATH");
    const char *old_marker = getenv(RUNTIME_TEST_BLOCKING_GIT_MARKER_ENV);
    bool had_cache = old_cache != NULL;
    bool had_path = old_path != NULL;
    bool had_marker = old_marker != NULL;
    char *saved_cache = old_cache ? cbm_strdup(old_cache) : NULL;
    char *saved_path = old_path ? cbm_strdup(old_path) : NULL;
    char *saved_marker = old_marker ? cbm_strdup(old_marker) : NULL;
    bool snapshots_ok =
        (!had_cache || saved_cache) && (!had_path || saved_path) && (!had_marker || saved_marker);

    char work[RUNTIME_TEST_PATH_CAP] = {0};
    char root[RUNTIME_TEST_PATH_CAP] = {0};
    char cache[RUNTIME_TEST_PATH_CAP] = {0};
    char bin[RUNTIME_TEST_PATH_CAP] = {0};
    char fake_git[RUNTIME_TEST_PATH_CAP] = {0};
    char marker[RUNTIME_TEST_PATH_CAP] = {0};
#ifdef _WIN32
    /* cmd.exe searches a native PATH. Keep the copied git probe out of the
     * MSYS2 TEMP ancestry and canonicalize its directory before publication. */
    bool work_ready =
        snapshots_ok && th_secure_runtime_parent_new(work, sizeof(work), "non-index-work");
#else
    (void)snprintf(work, sizeof(work), "%s/cbm-runtime-non-index-XXXXXX", cbm_tmpdir());
    bool work_ready = snapshots_ok && cbm_mkdtemp(work) != NULL;
#endif
    int root_written = work_ready ? snprintf(root, sizeof(root), "%s/root", work) : -1;
    int cache_written = work_ready ? snprintf(cache, sizeof(cache), "%s/cache", work) : -1;
    int bin_written = work_ready ? snprintf(bin, sizeof(bin), "%s/bin", work) : -1;
#ifdef _WIN32
    int git_written = work_ready ? snprintf(fake_git, sizeof(fake_git), "%s/git.exe", bin) : -1;
#else
    int git_written = work_ready ? snprintf(fake_git, sizeof(fake_git), "%s/git", bin) : -1;
#endif
    int marker_written =
        work_ready ? snprintf(marker, sizeof(marker), "%s/blocking-git.pid", work) : -1;
    bool paths_ready = work_ready && root_written > 0 && root_written < (int)sizeof(root) &&
                       cache_written > 0 && cache_written < (int)sizeof(cache) && bin_written > 0 &&
                       bin_written < (int)sizeof(bin) && git_written > 0 &&
                       git_written < (int)sizeof(fake_git) && marker_written > 0 &&
                       marker_written < (int)sizeof(marker) && cbm_mkdir_p(root, 0700) &&
                       cbm_mkdir_p(cache, 0700) && cbm_mkdir_p(bin, 0700) &&
                       runtime_test_copy_self_image(fake_git);

    char path_bin[RUNTIME_TEST_PATH_CAP] = {0};
#ifdef _WIN32
    bool path_bin_ready = paths_ready && cbm_canonical_path(bin, path_bin, sizeof(path_bin));
    if (path_bin_ready) {
        for (char *cursor = path_bin; *cursor; cursor++) {
            if (*cursor == '/') {
                *cursor = '\\';
            }
        }
    }
#else
    int path_bin_written = paths_ready ? snprintf(path_bin, sizeof(path_bin), "%s", bin) : -1;
    bool path_bin_ready = path_bin_written > 0 && path_bin_written < (int)sizeof(path_bin);
#endif

    char *project = paths_ready ? cbm_project_name_from_path(root) : NULL;
    char db_path[RUNTIME_TEST_PATH_CAP] = {0};
    int db_written = project ? snprintf(db_path, sizeof(db_path), "%s/%s.db", cache, project) : -1;
    cbm_store_t *seed =
        db_written > 0 && db_written < (int)sizeof(db_path) ? cbm_store_open_path(db_path) : NULL;
    bool seeded = seed && cbm_store_upsert_project(seed, project, root) == CBM_STORE_OK;
    cbm_store_close(seed);

    size_t path_length = strlen(path_bin) + 2U + (saved_path ? strlen(saved_path) : 0U);
    char *test_path = seeded && path_bin_ready ? malloc(path_length) : NULL;
    if (test_path) {
#ifdef _WIN32
        (void)snprintf(test_path, path_length, "%s;%s", path_bin, saved_path ? saved_path : "");
#else
        (void)snprintf(test_path, path_length, "%s:%s", path_bin, saved_path ? saved_path : "");
#endif
    }
    bool environment_ready = test_path && cbm_setenv("CBM_CACHE_DIR", cache, 1) == 0 &&
                             cbm_setenv("PATH", test_path, 1) == 0 &&
                             cbm_setenv(RUNTIME_TEST_BLOCKING_GIT_MARKER_ENV, marker, 1) == 0;

    cbm_daemon_application_t *application =
        environment_ready ? cbm_daemon_application_new(NULL) : NULL;
    cbm_daemon_runtime_application_callbacks_t callbacks =
        cbm_daemon_application_runtime_callbacks(application);
    cbm_daemon_build_identity_t identity =
        runtime_test_identity("2.4.0", runtime_test_self_build());
    runtime_test_fixture_t fixture = {0};
    bool started =
        application && runtime_test_fixture_start_configured(&fixture, "non-index-child-cancel",
                                                             &identity, 8, 5000, &callbacks);
    cbm_daemon_runtime_connect_result_t first_result = {0};
    cbm_daemon_runtime_connect_result_t second_result = {0};
    cbm_daemon_runtime_client_t *first =
        started ? cbm_daemon_runtime_client_connect(fixture.endpoint, &identity,
                                                    RUNTIME_TEST_TIMEOUT_MS, &first_result)
                : NULL;
    cbm_daemon_runtime_client_t *second =
        first ? cbm_daemon_runtime_client_connect(fixture.endpoint, &identity,
                                                  RUNTIME_TEST_TIMEOUT_MS, &second_result)
              : NULL;
    bool contexts_set =
        first && second &&
        cbm_daemon_application_client_set_context(first, root, root, CBM_MCP_TOOL_PROFILE_ALL, NULL,
                                                  NULL, RUNTIME_TEST_TIMEOUT_MS) ==
            CBM_DAEMON_RUNTIME_APPLICATION_OK &&
        cbm_daemon_application_client_set_context(second, root, root, CBM_MCP_TOOL_PROFILE_ALL,
                                                  NULL, NULL, RUNTIME_TEST_TIMEOUT_MS) ==
            CBM_DAEMON_RUNTIME_APPLICATION_OK;
    bool second_usable_before =
        contexts_set && runtime_real_application_ingest_probe(second) &&
        cbm_daemon_runtime_client_heartbeat(second, RUNTIME_TEST_TIMEOUT_MS);

    runtime_real_application_call_t call = {
        .client = first,
        .timeout_ms = REQUEST_TIMEOUT_MS,
        .status = CBM_DAEMON_RUNTIME_APPLICATION_TRANSPORT_ERROR,
    };
    atomic_init(&call.completed, false);
    int arguments_written =
        project ? snprintf(call.arguments, sizeof(call.arguments), "{\"project\":\"%s\"}", project)
                : -1;
    cbm_thread_t request_thread;
    int request_thread_create_rc =
        second_usable_before && arguments_written > 0 &&
                arguments_written < (int)sizeof(call.arguments)
            ? cbm_thread_create(&request_thread, 128U * 1024U,
                                runtime_real_application_detect_changes_thread, &call)
            : -1;
    bool request_thread_started = request_thread_create_rc == 0;
    uint64_t child_process_id = 0;
    bool marker_published =
        request_thread_started &&
        runtime_test_wait_pid_marker(marker, CHILD_READY_BOUND_MS, &child_process_id);
    if (request_thread_started && !marker_published) {
        printf("  blocking git marker missing: image=%s completed=%d status=%d response=%.*s\n",
               fake_git, atomic_load_explicit(&call.completed, memory_order_acquire) ? 1 : 0,
               (int)call.status, (int)call.response_length,
               call.response ? (const char *)call.response : "");
    }
    bool child_identity_exact =
        marker_published && runtime_test_process_image_matches(child_process_id, fake_git);

    bool first_close_begun = child_identity_exact && cbm_daemon_runtime_client_close_begin(first);
    /* Admission must drop at close_begin on every platform: POSIX signals it
     * through shutdown()/EOF, Windows through the CLOSE_INTENT frame — a
     * named-pipe client has no transport-level half-close to lean on. */
    bool only_second_admitted =
        first_close_begun &&
        cbm_daemon_runtime_service_wait_for_clients(fixture.service, 1, RUNTIME_TEST_TIMEOUT_MS);
    size_t clients_after_admission_wait =
        started ? cbm_daemon_runtime_service_active_clients(fixture.service) : 0;
    size_t connections_after_admission_wait =
        started ? cbm_daemon_runtime_service_active_connections(fixture.service) : 0;
    bool child_gone_without_backstop =
        first_close_begun &&
        runtime_test_wait_process_image_gone(child_process_id, fake_git, CHILD_CANCEL_BOUND_MS);
    bool cleanup_backstop_used = child_identity_exact && !child_gone_without_backstop;
    bool child_cleanup_complete = child_gone_without_backstop ||
                                  (cleanup_backstop_used && runtime_test_force_terminate_verified(
                                                                child_process_id, fake_git));
    if (child_cleanup_complete && !child_gone_without_backstop) {
        child_cleanup_complete = runtime_test_wait_process_image_gone(child_process_id, fake_git,
                                                                      CHILD_CLEANUP_BOUND_MS);
    }

    bool request_completed = request_thread_started && child_cleanup_complete &&
                             runtime_test_wait_atomic_bool(&call.completed, CHILD_CLEANUP_BOUND_MS);
    int request_thread_join_rc = request_completed ? cbm_thread_join(&request_thread) : -1;
    if (request_thread_join_rc == 0) {
        request_thread_started = false;
    }
    bool first_transport_interrupted =
        request_completed && call.status == CBM_DAEMON_RUNTIME_APPLICATION_TRANSPORT_ERROR &&
        call.response == NULL && call.response_length == 0;
    bool first_close_finished = false;
    if (first && !request_thread_started) {
        first_close_finished =
            first_close_begun
                ? cbm_daemon_runtime_client_close_finish(first, RUNTIME_TEST_TIMEOUT_MS)
                : cbm_daemon_runtime_client_close(first, RUNTIME_TEST_TIMEOUT_MS);
        first = NULL;
    }

    bool second_usable_after = second && child_cleanup_complete &&
                               runtime_real_application_ingest_probe(second) &&
                               cbm_daemon_runtime_client_heartbeat(second, RUNTIME_TEST_TIMEOUT_MS);
    bool second_closed = false;
    if (second) {
        second_closed = cbm_daemon_runtime_client_close(second, RUNTIME_TEST_TIMEOUT_MS);
        second = NULL;
    }
    bool exited =
        started && cbm_daemon_runtime_service_wait_exited(fixture.service, RUNTIME_TEST_TIMEOUT_MS);
    if (first && !request_thread_started) {
        (void)cbm_daemon_runtime_client_close(first, RUNTIME_TEST_TIMEOUT_MS);
        first = NULL;
    }
    runtime_test_fixture_finish(&fixture);
    bool application_stopped =
        application && cbm_daemon_application_shutdown(application, RUNTIME_TEST_TIMEOUT_MS);
    cbm_daemon_application_free(application);

    runtime_test_restore_environment(RUNTIME_TEST_BLOCKING_GIT_MARKER_ENV, saved_marker,
                                     had_marker);
    runtime_test_restore_environment("PATH", saved_path, had_path);
    runtime_test_restore_environment("CBM_CACHE_DIR", saved_cache, had_cache);
    free(call.response);
    free(test_path);
    free(project);
    free(saved_marker);
    free(saved_path);
    free(saved_cache);
    bool files_removed = !work_ready || th_rmtree(work) == 0;

    if (!only_second_admitted || !child_gone_without_backstop || !request_completed ||
        !second_usable_after) {
        printf("  non-index cancellation diagnostic: close_begun=%d clients_after_wait=%zu "
               "connections_after_wait=%zu child_gone=%d backstop=%d request_completed=%d "
               "request_status=%d second_after=%d\n",
               first_close_begun ? 1 : 0, clients_after_admission_wait,
               connections_after_admission_wait, child_gone_without_backstop ? 1 : 0,
               cleanup_backstop_used ? 1 : 0, request_completed ? 1 : 0, (int)call.status,
               second_usable_after ? 1 : 0);
    }

    ASSERT_TRUE(snapshots_ok);
    ASSERT_TRUE(paths_ready);
    ASSERT_TRUE(path_bin_ready);
    ASSERT_TRUE(seeded);
    ASSERT_TRUE(environment_ready);
    ASSERT_TRUE(started);
    ASSERT_EQ(first_result.status, CBM_DAEMON_RUNTIME_CONNECT_ACCEPTED);
    ASSERT_EQ(second_result.status, CBM_DAEMON_RUNTIME_CONNECT_ACCEPTED);
    ASSERT_TRUE(contexts_set);
    ASSERT_TRUE(second_usable_before);
    ASSERT_EQ(request_thread_create_rc, 0);
    ASSERT_TRUE(marker_published);
    ASSERT_TRUE(child_identity_exact);
    ASSERT_TRUE(first_close_begun);
    ASSERT_TRUE(only_second_admitted);
    /* The contained non-index child belongs to the first request and must be
     * gone before the independently owned second session is exercised again. */
    ASSERT_TRUE(child_gone_without_backstop);
    ASSERT_FALSE(cleanup_backstop_used);
    ASSERT_TRUE(child_cleanup_complete);
    ASSERT_TRUE(request_completed);
    ASSERT_EQ(request_thread_join_rc, 0);
    ASSERT_TRUE(first_transport_interrupted);
    /* close_begin interrupted an active exchange, so there is intentionally no
     * DISCONNECT acknowledgement even though server-side teardown completes. */
    ASSERT_FALSE(first_close_finished);
    ASSERT_TRUE(second_usable_after);
    ASSERT_TRUE(second_closed);
    ASSERT_TRUE(exited);
    ASSERT_TRUE(application_stopped);
    ASSERT_TRUE(files_removed);
    PASS();
#endif
}

TEST(daemon_runtime_noncooperative_callback_does_not_detach_or_unbound_stop) {
    static const uint8_t request[] = {'i', 'g', 'n', 'o', 'r', 'e'};
    /* STOP_OBSERVED_MAX_MS is a coarse hang-detector, not a tight latency
     * bound: the invariant is that a deadline-bounded stop RETURNS (proven by
     * reaching the assertions below — a hang would deadlock the test) rather
     * than blocking indefinitely on the stuck callback. The window is well
     * above the STOP_BOUND_MS deadline yet far below a real hang, so heavy
     * scheduler starvation (the CBM_LOCAL_CI_CPUS=4 fidelity pass, CI runners)
     * cannot make it test-significant. */
    enum { STOP_BOUND_MS = 50, STOP_OBSERVED_MAX_MS = 5000 };
    cbm_daemon_build_identity_t identity =
        runtime_test_identity("2.4.0", runtime_test_self_build());
    runtime_application_context_t context;
    runtime_application_context_init(&context, true);
    atomic_store_explicit(&context.ignore_first_request_cancel, true, memory_order_release);
    atomic_bool request_thread_completed;
    atomic_init(&request_thread_completed, false);
    runtime_test_fixture_t fixture;
    bool started = runtime_test_fixture_start_application(&fixture, "application-noncooperative",
                                                          &identity, &context);
    cbm_daemon_runtime_connect_result_t result = {0};
    cbm_daemon_runtime_client_t *client = NULL;
    runtime_application_client_call_t call = {
        .request = request,
        .request_length = (uint32_t)sizeof(request),
        .completed = &request_thread_completed,
        .status = CBM_DAEMON_RUNTIME_APPLICATION_OK,
    };
    cbm_thread_t request_thread;
    int request_thread_create_rc = -1;
    int request_thread_join_rc = -1;
    bool request_thread_started = false;
    bool callback_started = false;
    bool close_begun = false;
    bool stop_returned = true;
    uint64_t stop_elapsed_ms = UINT64_MAX;
    bool exited_before_release = false;
    bool close_finished = false;
    bool exited_after_release = false;

    if (started) {
        client = cbm_daemon_runtime_client_connect(fixture.endpoint, &identity,
                                                   RUNTIME_TEST_TIMEOUT_MS, &result);
    }
    if (client) {
        call.client = client;
        request_thread_create_rc = cbm_thread_create(
            &request_thread, 128U * 1024U, runtime_application_client_request_thread, &call);
        request_thread_started = request_thread_create_rc == 0;
        callback_started =
            request_thread_started &&
            runtime_test_wait_atomic_bool(&context.first_request_started, RUNTIME_TEST_TIMEOUT_MS);
    }
    if (callback_started) {
        close_begun = cbm_daemon_runtime_client_close_begin(client);
        uint64_t stop_started_ms = cbm_now_ms();
        stop_returned = cbm_daemon_runtime_service_stop(fixture.service, STOP_BOUND_MS);
        stop_elapsed_ms = cbm_now_ms() - stop_started_ms;
        exited_before_release =
            cbm_daemon_runtime_service_state(fixture.service) == CBM_DAEMON_RUNTIME_SERVICE_EXITED;
    }

    /* A bounded stop failure retains every callback/session allocation. The
     * test supplies eventual cooperation so the runner can prove clean join
     * and teardown after observing the production host's force boundary. */
    atomic_store_explicit(&context.release_first_request, true, memory_order_release);
    if (request_thread_started) {
        request_thread_join_rc = cbm_thread_join(&request_thread);
        if (request_thread_join_rc == 0) {
            request_thread_started = false;
        }
    }
    if (request_thread_started) {
        while (!atomic_load_explicit(&request_thread_completed, memory_order_acquire)) {
            struct timespec pause = {.tv_sec = 0, .tv_nsec = 1000000};
            (void)cbm_nanosleep(&pause, NULL);
        }
    }
    if (client) {
        close_finished =
            close_begun ? cbm_daemon_runtime_client_close_finish(client, RUNTIME_TEST_TIMEOUT_MS)
                        : cbm_daemon_runtime_client_close(client, RUNTIME_TEST_TIMEOUT_MS);
        client = NULL;
    }
    if (started) {
        exited_after_release =
            cbm_daemon_runtime_service_wait_exited(fixture.service, RUNTIME_TEST_TIMEOUT_MS);
    }
    free(call.response);
    runtime_test_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_EQ(result.status, CBM_DAEMON_RUNTIME_CONNECT_ACCEPTED);
    ASSERT_EQ(request_thread_create_rc, 0);
    ASSERT_TRUE(callback_started);
    ASSERT_TRUE(close_begun);
    ASSERT_FALSE(stop_returned);
    ASSERT_TRUE(stop_elapsed_ms <= STOP_OBSERVED_MAX_MS);
    ASSERT_FALSE(exited_before_release);
    ASSERT_EQ(request_thread_join_rc, 0);
    /* The interrupted transport cannot receive a final disconnect ACK. */
    ASSERT_FALSE(close_finished);
    ASSERT_TRUE(exited_after_release);
    ASSERT_EQ(atomic_load(&context.opened), 1);
    ASSERT_EQ(atomic_load(&context.requests), 1);
    ASSERT_EQ(atomic_load(&context.cancelled), 1);
    ASSERT_EQ(atomic_load(&context.closed), 1);
    PASS();
}

TEST(daemon_runtime_application_busy_cap_and_malformed_are_isolated) {
    static const uint8_t blocking_request[] = {'f', 'i', 'r', 's', 't'};
    static const uint8_t busy_request[] = {'b', 'u', 's', 'y'};
    static const uint8_t malformed_request[] = {'x', 'y'};
    static const uint8_t valid_request[] = {0xde, 0xad, 0x00, 0xbe, 0xef};
    cbm_daemon_build_identity_t identity =
        runtime_test_identity("2.4.0", runtime_test_self_build());
    runtime_application_context_t context;
    runtime_application_context_init(&context, true);
    runtime_test_fixture_t fixture;
    bool started = runtime_test_fixture_start_application(&fixture, "application-isolation",
                                                          &identity, &context);
    cbm_daemon_runtime_client_t *owner = NULL;
    cbm_daemon_runtime_connect_result_t owner_result = {0};
    cbm_daemon_ipc_connection_t *raw = NULL;
    bool first_sent = false;
    bool callback_started = false;
    bool busy_sent = false;
    bool busy_rejected = false;
    bool malformed_sent = false;
    bool bad_peer_released = false;
    uint8_t sentinel = 0x5a;
    uint8_t *oversize_response = (uint8_t *)&sentinel;
    uint32_t oversize_response_length = UINT32_MAX;
    cbm_daemon_runtime_application_status_t oversize_status = CBM_DAEMON_RUNTIME_APPLICATION_OK;
    uint8_t *valid_response = NULL;
    uint32_t valid_response_length = 0;
    cbm_daemon_runtime_application_status_t valid_status =
        CBM_DAEMON_RUNTIME_APPLICATION_TRANSPORT_ERROR;
    bool owner_survived = false;
    bool exited = false;

    if (started) {
        owner = cbm_daemon_runtime_client_connect(fixture.endpoint, &identity,
                                                  RUNTIME_TEST_TIMEOUT_MS, &owner_result);
        raw = runtime_test_raw_client_connect(fixture.endpoint, &identity);
    }
    if (owner && raw) {
        first_sent = runtime_test_raw_application_send(raw, blocking_request,
                                                       (uint32_t)sizeof(blocking_request),
                                                       (uint32_t)sizeof(blocking_request));
        callback_started =
            first_sent &&
            runtime_test_wait_atomic_bool(&context.first_request_started, RUNTIME_TEST_TIMEOUT_MS);
    }
    if (callback_started) {
        busy_sent = runtime_test_raw_application_send(
            raw, busy_request, (uint32_t)sizeof(busy_request), (uint32_t)sizeof(busy_request));
        busy_rejected = busy_sent && runtime_test_raw_application_receive_status(
                                         raw, CBM_DAEMON_RUNTIME_APPLICATION_BUSY);
    }
    if (busy_rejected) {
        malformed_sent = runtime_test_raw_application_send(
            raw, malformed_request, (uint32_t)sizeof(malformed_request),
            (uint32_t)sizeof(malformed_request) + 1U);
    }
    cbm_daemon_ipc_connection_close(raw);
    raw = NULL;
    if (malformed_sent) {
        bad_peer_released = cbm_daemon_runtime_service_wait_for_clients(fixture.service, 1,
                                                                        RUNTIME_TEST_TIMEOUT_MS);
    }
    if (owner && bad_peer_released) {
        oversize_status = cbm_daemon_runtime_client_application_request(
            owner, &sentinel, CBM_DAEMON_RUNTIME_APPLICATION_PAYLOAD_MAX + 1U, &oversize_response,
            &oversize_response_length, RUNTIME_TEST_TIMEOUT_MS);
        valid_status = cbm_daemon_runtime_client_application_request(
            owner, valid_request, (uint32_t)sizeof(valid_request), &valid_response,
            &valid_response_length, RUNTIME_TEST_TIMEOUT_MS);
        owner_survived = oversize_status == CBM_DAEMON_RUNTIME_APPLICATION_TRANSPORT_ERROR &&
                         oversize_response == NULL && oversize_response_length == 0 &&
                         valid_status == CBM_DAEMON_RUNTIME_APPLICATION_OK &&
                         valid_response_length == sizeof(valid_request) && valid_response &&
                         memcmp(valid_response, valid_request, sizeof(valid_request)) == 0 &&
                         cbm_daemon_runtime_client_heartbeat(owner, RUNTIME_TEST_TIMEOUT_MS);
        free(valid_response);
        valid_response = NULL;
        (void)cbm_daemon_runtime_client_close(owner, RUNTIME_TEST_TIMEOUT_MS);
        owner = NULL;
        exited = cbm_daemon_runtime_service_wait_exited(fixture.service, RUNTIME_TEST_TIMEOUT_MS);
    }

    free(valid_response);
    if (owner) {
        (void)cbm_daemon_runtime_client_close(owner, RUNTIME_TEST_TIMEOUT_MS);
    }
    cbm_daemon_ipc_connection_close(raw);
    runtime_test_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_TRUE(first_sent);
    ASSERT_TRUE(callback_started);
    ASSERT_TRUE(busy_sent);
    ASSERT_TRUE(busy_rejected);
    ASSERT_TRUE(malformed_sent);
    ASSERT_TRUE(bad_peer_released);
    ASSERT_TRUE(owner_survived);
    ASSERT_TRUE(exited);
    ASSERT_EQ(atomic_load(&context.opened), 2);
    ASSERT_EQ(atomic_load(&context.requests), 2);
    ASSERT_EQ(atomic_load(&context.cancelled), 2);
    ASSERT_EQ(atomic_load(&context.closed), 2);
    PASS();
}

TEST(daemon_runtime_malformed_and_zero_cancel_close_only_offending_connections) {
    static const uint8_t valid_request[] = {'s', 'u', 'r', 'v', 'i', 'v', 'e'};
    static const uint8_t malformed_cancel[7] = {1};
    static const uint8_t zero_token_cancel[8] = {0};
    cbm_daemon_build_identity_t identity =
        runtime_test_identity("2.4.0", runtime_test_self_build());
    runtime_application_context_t context;
    runtime_application_context_init(&context, false);
    runtime_test_fixture_t fixture;
    bool started = runtime_test_fixture_start_application(&fixture, "application-cancel-isolation",
                                                          &identity, &context);
    cbm_daemon_runtime_connect_result_t owner_result = {0};
    cbm_daemon_runtime_client_t *owner = NULL;
    cbm_daemon_ipc_connection_t *raw = NULL;
    bool malformed_connected = false;
    bool malformed_sent = false;
    bool malformed_released = false;
    bool malformed_closed = false;
    bool zero_connected = false;
    bool zero_sent = false;
    bool zero_released = false;
    bool zero_closed = false;
    uint8_t *valid_response = NULL;
    uint32_t valid_response_length = 0;
    cbm_daemon_runtime_application_status_t valid_status =
        CBM_DAEMON_RUNTIME_APPLICATION_TRANSPORT_ERROR;
    bool owner_survived = false;
    bool owner_closed = false;
    bool exited = false;

    if (started) {
        owner = cbm_daemon_runtime_client_connect(fixture.endpoint, &identity,
                                                  RUNTIME_TEST_TIMEOUT_MS, &owner_result);
    }
    if (owner) {
        raw = runtime_test_raw_client_connect(fixture.endpoint, &identity);
        malformed_connected = raw != NULL;
    }
    if (raw) {
        malformed_sent = cbm_daemon_ipc_send_frame(
            raw, CBM_DAEMON_FRAME_REQUEST, CBM_DAEMON_RUNTIME_OP_APPLICATION_CANCEL,
            malformed_cancel, (uint32_t)sizeof(malformed_cancel));
    }
    if (malformed_sent) {
        malformed_released = cbm_daemon_runtime_service_wait_for_clients(fixture.service, 1,
                                                                         RUNTIME_TEST_TIMEOUT_MS);
    }
    if (malformed_released) {
        cbm_daemon_frame_t frame = {0};
        uint8_t *payload = NULL;
        int received = cbm_daemon_ipc_receive_frame(raw, RUNTIME_TEST_TIMEOUT_MS, &frame, &payload);
        malformed_closed = received != 1;
        free(payload);
    }
    cbm_daemon_ipc_connection_close(raw);
    raw = NULL;

    if (owner && malformed_closed) {
        raw = runtime_test_raw_client_connect(fixture.endpoint, &identity);
        zero_connected = raw != NULL;
    }
    if (raw) {
        zero_sent = cbm_daemon_ipc_send_frame(
            raw, CBM_DAEMON_FRAME_REQUEST, CBM_DAEMON_RUNTIME_OP_APPLICATION_CANCEL,
            zero_token_cancel, (uint32_t)sizeof(zero_token_cancel));
    }
    if (zero_sent) {
        zero_released = cbm_daemon_runtime_service_wait_for_clients(fixture.service, 1,
                                                                    RUNTIME_TEST_TIMEOUT_MS);
    }
    if (zero_released) {
        cbm_daemon_frame_t frame = {0};
        uint8_t *payload = NULL;
        int received = cbm_daemon_ipc_receive_frame(raw, RUNTIME_TEST_TIMEOUT_MS, &frame, &payload);
        zero_closed = received != 1;
        free(payload);
    }
    cbm_daemon_ipc_connection_close(raw);
    raw = NULL;

    if (owner && zero_closed) {
        valid_status = cbm_daemon_runtime_client_application_request(
            owner, valid_request, (uint32_t)sizeof(valid_request), &valid_response,
            &valid_response_length, RUNTIME_TEST_TIMEOUT_MS);
        owner_survived = valid_status == CBM_DAEMON_RUNTIME_APPLICATION_OK &&
                         valid_response_length == sizeof(valid_request) && valid_response &&
                         memcmp(valid_response, valid_request, sizeof(valid_request)) == 0 &&
                         cbm_daemon_runtime_client_heartbeat(owner, RUNTIME_TEST_TIMEOUT_MS);
    }
    free(valid_response);
    if (owner) {
        owner_closed = cbm_daemon_runtime_client_close(owner, RUNTIME_TEST_TIMEOUT_MS);
        owner = NULL;
    }
    if (started) {
        exited = cbm_daemon_runtime_service_wait_exited(fixture.service, RUNTIME_TEST_TIMEOUT_MS);
    }
    cbm_daemon_ipc_connection_close(raw);
    runtime_test_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_TRUE(malformed_connected);
    ASSERT_TRUE(malformed_sent);
    ASSERT_TRUE(malformed_released);
    ASSERT_TRUE(malformed_closed);
    ASSERT_TRUE(zero_connected);
    ASSERT_TRUE(zero_sent);
    ASSERT_TRUE(zero_released);
    ASSERT_TRUE(zero_closed);
    ASSERT_TRUE(owner_survived);
    ASSERT_TRUE(owner_closed);
    ASSERT_TRUE(exited);
    ASSERT_EQ(atomic_load(&context.opened), 3);
    ASSERT_EQ(atomic_load(&context.requests), 1);
    ASSERT_EQ(atomic_load(&context.request_cancels), 0);
    ASSERT_EQ(atomic_load(&context.cancelled), 3);
    ASSERT_EQ(atomic_load(&context.closed), 3);
    PASS();
}

TEST(daemon_runtime_kernel_process_fingerprint_is_stable_and_fail_closed) {
    char first[CBM_DAEMON_BUILD_FINGERPRINT_SIZE] = {0};
    char repeated[CBM_DAEMON_BUILD_FINGERPRINT_SIZE] = {0};
    char invalid[CBM_DAEMON_BUILD_FINGERPRINT_SIZE] = {0};
    bool first_ok = cbm_daemon_runtime_process_build_fingerprint(runtime_test_process_id(), first);
    bool repeated_ok =
        cbm_daemon_runtime_process_build_fingerprint(runtime_test_process_id(), repeated);
    bool invalid_rejected = !cbm_daemon_runtime_process_build_fingerprint(UINT64_MAX, invalid);

    ASSERT_TRUE(first_ok);
    ASSERT_TRUE(repeated_ok);
    ASSERT_TRUE(runtime_test_is_fingerprint(first));
    ASSERT_STR_EQ(first, repeated);
    ASSERT_TRUE(invalid_rejected);
    ASSERT_STR_EQ(invalid, "");
    PASS();
}

#ifdef __APPLE__
/* RED: the former macOS fast path accepted the daemon vnode in any RX mapping,
 * even when a differently fingerprinted main executable owned the connection.
 * Re-signing the changed copy keeps the probe runnable on arm64 while ensuring
 * that only the deliberately injected mapping has the active identity. */
TEST(daemon_runtime_mac_fast_path_rejects_foreign_main_image_mapping_active) {
    cbm_daemon_build_identity_t identity =
        runtime_test_identity("2.4.0", runtime_test_self_build());
    char directory[RUNTIME_TEST_PATH_CAP] = {0};
    char active_image[RUNTIME_TEST_PATH_CAP] = {0};
    char foreign_image[RUNTIME_TEST_PATH_CAP] = {0};
    char foreign_fingerprint[CBM_DAEMON_BUILD_FINGERPRINT_SIZE] = {0};
    int directory_written =
        snprintf(directory, sizeof(directory), "%s/cbm-runtime-mapped-image-XXXXXX", cbm_tmpdir());
    bool directory_created = directory_written > 0 && directory_written < (int)sizeof(directory) &&
                             cbm_mkdtemp(directory) != NULL;
    int foreign_written = directory_created ? snprintf(foreign_image, sizeof(foreign_image),
                                                       "%s/foreign-client", directory)
                                            : -1;
    bool active_resolved = runtime_test_self_image_path(active_image);
    bool foreign_copied = active_resolved && foreign_written > 0 &&
                          foreign_written < (int)sizeof(foreign_image) &&
                          runtime_test_copy_executable(active_image, foreign_image);
    /* A distinct signing identifier changes the Mach-O signature bytes while
     * keeping the copied executable valid under macOS strict validation. */
    bool foreign_changed = foreign_copied;
    bool foreign_signed = foreign_changed && runtime_test_mac_ad_hoc_sign(foreign_image);
    bool foreign_fingerprinted =
        foreign_signed && cbm_daemon_build_fingerprint_file(foreign_image, foreign_fingerprint);
    bool fingerprint_differs =
        foreign_fingerprinted && strcmp(foreign_fingerprint, identity.build_fingerprint) != 0;

    runtime_test_fixture_t fixture;
    memset(&fixture, 0, sizeof(fixture));
    bool fixture_attempted = directory_created && active_resolved && fingerprint_differs;
    bool started =
        fixture_attempted && runtime_test_fixture_start(&fixture, "mapped-foreign", &identity);
    int foreign_exit = -1;
    bool foreign_ran =
        started && runtime_test_run_mapped_hello_image(foreign_image, active_image, &fixture,
                                                       &identity, &foreign_exit);

    if (fixture_attempted) {
        runtime_test_fixture_finish(&fixture);
    }
    if (foreign_written > 0 && foreign_written < (int)sizeof(foreign_image)) {
        (void)cbm_unlink(foreign_image);
    }
    if (directory_created) {
        (void)cbm_rmdir(directory);
    }

    ASSERT_TRUE(directory_created);
    ASSERT_TRUE(active_resolved);
    ASSERT_TRUE(foreign_copied);
    ASSERT_TRUE(foreign_changed);
    ASSERT_TRUE(foreign_signed);
    ASSERT_TRUE(foreign_fingerprinted);
    ASSERT_TRUE(fingerprint_differs);
    ASSERT_TRUE(started);
    ASSERT_TRUE(foreign_ran);
    ASSERT_EQ(foreign_exit, 26);
    PASS();
}
#endif

TEST(daemon_runtime_copied_image_fallback_accepts_identical_and_rejects_changed) {
    cbm_daemon_build_identity_t identity =
        runtime_test_identity("2.4.0", runtime_test_self_build());
    runtime_test_fixture_t identical_fixture;
    bool identical_started =
        runtime_test_fixture_start(&identical_fixture, "copied-identical", &identity);
    char identical_path[RUNTIME_TEST_PATH_CAP] = {0};
    int identical_path_written = identical_started
#ifdef _WIN32
                                     ? snprintf(identical_path, sizeof(identical_path),
                                                "%s/client-copy.exe", identical_fixture.parent)
#else
                                     ? snprintf(identical_path, sizeof(identical_path),
                                                "%s/client-copy", identical_fixture.parent)
#endif
                                     : -1;
    bool identical_copied = identical_path_written > 0 &&
                            identical_path_written < (int)sizeof(identical_path) &&
                            runtime_test_copy_self_image(identical_path);
    char identical_fingerprint[CBM_DAEMON_BUILD_FINGERPRINT_SIZE] = {0};
    bool identical_bytes =
        identical_copied &&
        cbm_daemon_build_fingerprint_file(identical_path, identical_fingerprint) &&
        strcmp(identical_fingerprint, identity.build_fingerprint) == 0;
    int identical_exit = -1;
    bool identical_ran =
        identical_bytes && runtime_test_run_hello_image(identical_path, &identical_fixture,
                                                        &identity, &identical_exit);
    bool identical_exited =
        identical_ran && identical_exit == 0 &&
        cbm_daemon_runtime_service_wait_exited(identical_fixture.service, RUNTIME_TEST_TIMEOUT_MS);
    (void)cbm_unlink(identical_path);
    runtime_test_fixture_finish(&identical_fixture);

#ifdef __linux__
    runtime_test_fixture_t changed_fixture;
    bool changed_started =
        runtime_test_fixture_start(&changed_fixture, "copied-changed", &identity);
    char changed_path[RUNTIME_TEST_PATH_CAP] = {0};
    int changed_path_written = changed_started ? snprintf(changed_path, sizeof(changed_path),
                                                          "%s/client-copy", changed_fixture.parent)
                                               : -1;
    bool changed_copied = changed_path_written > 0 &&
                          changed_path_written < (int)sizeof(changed_path) &&
                          runtime_test_copy_self_image(changed_path) &&
                          runtime_test_append_image_marker(changed_path);
    char changed_fingerprint[CBM_DAEMON_BUILD_FINGERPRINT_SIZE] = {0};
    bool changed_bytes = changed_copied &&
                         cbm_daemon_build_fingerprint_file(changed_path, changed_fingerprint) &&
                         strcmp(changed_fingerprint, identity.build_fingerprint) != 0;
    int changed_exit = -1;
    bool changed_ran = changed_bytes && runtime_test_run_hello_image(changed_path, &changed_fixture,
                                                                     &identity, &changed_exit);
    (void)cbm_unlink(changed_path);
    runtime_test_fixture_finish(&changed_fixture);
#endif

    ASSERT_TRUE(identical_started);
    ASSERT_TRUE(identical_copied);
    ASSERT_TRUE(identical_bytes);
    ASSERT_TRUE(identical_ran);
    ASSERT_EQ(identical_exit, 0);
    ASSERT_TRUE(identical_exited);
#ifdef __linux__
    ASSERT_TRUE(changed_started);
    ASSERT_TRUE(changed_copied);
    ASSERT_TRUE(changed_bytes);
    ASSERT_TRUE(changed_ran);
    ASSERT_EQ(changed_exit, 26);
#endif
    PASS();
}

#ifdef _WIN32
TEST(daemon_runtime_process_fingerprint_never_hashes_replacement_path) {
    char directory[RUNTIME_TEST_PATH_CAP] = {0};
    char image_path[RUNTIME_TEST_PATH_CAP] = {0};
    char replacement_path[RUNTIME_TEST_PATH_CAP] = {0};
    char event_name[128] = {0};
    int directory_written =
        snprintf(directory, sizeof(directory), "%s/cbm-runtime-image-XXXXXX", cbm_tmpdir());
    bool setup = directory_written > 0 && directory_written < (int)sizeof(directory) &&
                 cbm_mkdtemp(directory) != NULL;
    int image_written =
        setup ? snprintf(image_path, sizeof(image_path), "%s/image.exe", directory) : -1;
    int replacement_written = setup ? snprintf(replacement_path, sizeof(replacement_path),
                                               "%s/replacement.exe", directory)
                                    : -1;
    int event_written =
        snprintf(event_name, sizeof(event_name), "Local\\cbm-runtime-image-%lu-%llu",
                 (unsigned long)GetCurrentProcessId(), (unsigned long long)GetTickCount64());
    setup = setup && image_written > 0 && image_written < (int)sizeof(image_path) &&
            replacement_written > 0 && replacement_written < (int)sizeof(replacement_path) &&
            event_written > 0 && event_written < (int)sizeof(event_name) &&
            runtime_test_windows_copy_self(image_path);

    FILE *replacement_file = setup ? cbm_fopen(replacement_path, "wb") : NULL;
    bool replacement_written_ok =
        replacement_file && fputs("cbm-windows-replacement-image", replacement_file) >= 0;
    if (replacement_file) {
        replacement_written_ok = fclose(replacement_file) == 0 && replacement_written_ok;
    }
    setup = setup && replacement_written_ok;

    char original[CBM_DAEMON_BUILD_FINGERPRINT_SIZE] = {0};
    char replacement[CBM_DAEMON_BUILD_FINGERPRINT_SIZE] = {0};
    char observed[CBM_DAEMON_BUILD_FINGERPRINT_SIZE] = {0};
    setup = setup && cbm_daemon_build_fingerprint_file(image_path, original) &&
            cbm_daemon_build_fingerprint_file(replacement_path, replacement) &&
            strcmp(original, replacement) != 0;

    HANDLE ready_event = setup ? CreateEventA(NULL, TRUE, FALSE, event_name) : NULL;
    bool event_private = ready_event && GetLastError() != ERROR_ALREADY_EXISTS;
    PROCESS_INFORMATION process;
    memset(&process, 0, sizeof(process));
    bool spawned =
        event_private && runtime_test_windows_spawn_image_holder(image_path, event_name, &process);
    bool ready = spawned && WaitForSingleObject(ready_event, 5000) == WAIT_OBJECT_0;
    bool replaced = ready && runtime_test_windows_posix_replace(replacement_path, image_path);
    bool fingerprinted = ready && cbm_daemon_runtime_process_build_fingerprint(
                                      (uint64_t)process.dwProcessId, observed);
    bool replacement_safe = replaced ? (!fingerprinted || strcmp(observed, original) == 0)
                                     : (fingerprinted && strcmp(observed, original) == 0);

    bool stopped = !spawned;
    if (spawned) {
        DWORD exit_code = 0;
        bool running =
            GetExitCodeProcess(process.hProcess, &exit_code) != 0 && exit_code == STILL_ACTIVE;
        bool termination_requested = !running || TerminateProcess(process.hProcess, 26) != 0;
        stopped =
            termination_requested && WaitForSingleObject(process.hProcess, 5000) == WAIT_OBJECT_0;
        (void)CloseHandle(process.hProcess);
    }
    if (ready_event) {
        (void)CloseHandle(ready_event);
    }
    (void)cbm_unlink(replacement_path);
    (void)cbm_unlink(image_path);
    (void)cbm_rmdir(directory);

    ASSERT_TRUE(setup);
    ASSERT_TRUE(event_private);
    ASSERT_TRUE(spawned);
    ASSERT_TRUE(ready);
    ASSERT_TRUE(stopped);
    ASSERT_TRUE(replacement_safe);
    ASSERT_TRUE(!fingerprinted || strcmp(observed, replacement) != 0);
    PASS();
}
#elif defined(__APPLE__) || defined(__linux__)
TEST(daemon_runtime_process_fingerprint_never_hashes_replacement_path) {
    char directory[RUNTIME_TEST_PATH_CAP] = {0};
    char image_path[RUNTIME_TEST_PATH_CAP] = {0};
    char replacement_path[RUNTIME_TEST_PATH_CAP] = {0};
    int directory_written =
        snprintf(directory, sizeof(directory), "%s/cbm-runtime-image-XXXXXX", cbm_tmpdir());
    bool setup = directory_written > 0 && directory_written < (int)sizeof(directory) &&
                 cbm_mkdtemp(directory) != NULL;
    int image_written =
        setup ? snprintf(image_path, sizeof(image_path), "%s/image", directory) : -1;
    int replacement_written =
        setup ? snprintf(replacement_path, sizeof(replacement_path), "%s/replacement", directory)
              : -1;
    setup = setup && image_written > 0 && image_written < (int)sizeof(image_path) &&
            replacement_written > 0 && replacement_written < (int)sizeof(replacement_path) &&
            runtime_test_copy_executable("/bin/cat", image_path) &&
            runtime_test_copy_executable("/bin/echo", replacement_path);

    char original[CBM_DAEMON_BUILD_FINGERPRINT_SIZE] = {0};
    char replacement[CBM_DAEMON_BUILD_FINGERPRINT_SIZE] = {0};
    char observed[CBM_DAEMON_BUILD_FINGERPRINT_SIZE] = {0};
    setup = setup && cbm_daemon_build_fingerprint_file(image_path, original);
    int release_fd = -1;
    pid_t child = setup ? runtime_test_spawn_blocked_executable(image_path, &release_fd) : -1;
    setup = setup && child > 0 && rename(replacement_path, image_path) == 0 &&
            cbm_daemon_build_fingerprint_file(image_path, replacement) &&
            strcmp(original, replacement) != 0;
    bool fingerprinted =
        setup && cbm_daemon_runtime_process_build_fingerprint((uint64_t)child, observed);

    runtime_test_stop_blocked_executable(child, release_fd);
    (void)unlink(replacement_path);
    (void)unlink(image_path);
    (void)rmdir(directory);

    ASSERT_TRUE(setup);
#ifdef __linux__
    /* /proc/<pid>/exe is an openable kernel link to the mapped inode, even
     * after that inode has been unlinked by the atomic replacement. */
    ASSERT_TRUE(fingerprinted);
    ASSERT_STR_EQ(observed, original);
#else
    /* macOS exposes mapped vnode identity but not a public handle to the
     * mapped executable. If the old vnode no longer has an openable path we
     * must fail closed; resolving and hashing the new path is forbidden. */
    ASSERT_TRUE(!fingerprinted || strcmp(observed, original) == 0);
#endif
    ASSERT_TRUE(!fingerprinted || strcmp(observed, replacement) != 0);
    PASS();
}
#endif


TEST(daemon_runtime_close_begin_releases_admission_with_inflight_request) {
    static const uint8_t request[] = {'b', 'l', 'o', 'c', 'k'};
    cbm_daemon_build_identity_t identity =
        runtime_test_identity("2.4.0", runtime_test_self_build());
    runtime_application_context_t context;
    runtime_application_context_init(&context, true);
    atomic_bool request_thread_completed;
    atomic_init(&request_thread_completed, false);
    runtime_test_fixture_t fixture;
    bool started = runtime_test_fixture_start_application(&fixture, "close-intent-admission",
                                                          &identity, &context);
    cbm_daemon_runtime_client_t *client = NULL;
    cbm_daemon_runtime_connect_result_t result = {0};
    runtime_application_client_call_t call = {
        .request = request,
        .request_length = (uint32_t)sizeof(request),
        .completed = &request_thread_completed,
        .status = CBM_DAEMON_RUNTIME_APPLICATION_OK,
    };
    cbm_thread_t request_thread;
    int request_thread_create_rc = -1;
    bool request_thread_started = false;
    bool callback_started = false;
    bool close_begun = false;
    bool admission_released_at_begin = false;
    bool request_thread_joined = false;
    bool request_ended_without_payload = false;
    bool exited = false;

    if (started) {
        client = cbm_daemon_runtime_client_connect(fixture.endpoint, &identity,
                                                   RUNTIME_TEST_TIMEOUT_MS, &result);
    }
    if (client) {
        call.client = client;
        request_thread_create_rc = cbm_thread_create(
            &request_thread, 128U * 1024U, runtime_application_client_request_thread, &call);
        request_thread_started = request_thread_create_rc == 0;
        callback_started =
            request_thread_started &&
            runtime_test_wait_atomic_bool(&context.first_request_started, RUNTIME_TEST_TIMEOUT_MS);
    }
    if (callback_started) {
        close_begun = cbm_daemon_runtime_client_close_begin(client);
    }
    /* The parity contract: after close_begin alone — before the handle
     * closes — the daemon has released this client's admission. POSIX learns
     * through shutdown()/EOF; Windows through the CLOSE_INTENT frame. */
    admission_released_at_begin =
        close_begun &&
        cbm_daemon_runtime_service_wait_for_clients(fixture.service, 0, RUNTIME_TEST_TIMEOUT_MS);
    if (request_thread_started) {
        request_thread_joined = cbm_thread_join(&request_thread) == 0;
        request_thread_started = false;
    }
    request_ended_without_payload =
        request_thread_joined &&
        (call.status == CBM_DAEMON_RUNTIME_APPLICATION_TRANSPORT_ERROR ||
         call.status == CBM_DAEMON_RUNTIME_APPLICATION_CANCELLED) &&
        call.response == NULL && call.response_length == 0;
    if (client) {
        (void)(close_begun
                   ? cbm_daemon_runtime_client_close_finish(client, RUNTIME_TEST_TIMEOUT_MS)
                   : cbm_daemon_runtime_client_close(client, RUNTIME_TEST_TIMEOUT_MS));
        client = NULL;
    }
    if (started) {
        exited = cbm_daemon_runtime_service_wait_exited(fixture.service, RUNTIME_TEST_TIMEOUT_MS);
    }
    free(call.response);
    runtime_test_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_EQ(request_thread_create_rc, 0);
    ASSERT_TRUE(callback_started);
    ASSERT_TRUE(close_begun);
    ASSERT_TRUE(admission_released_at_begin);
    ASSERT_TRUE(request_thread_joined);
    ASSERT_TRUE(request_ended_without_payload);
    ASSERT_TRUE(exited);
    ASSERT_EQ(atomic_load(&context.opened), 1);
    ASSERT_EQ(atomic_load(&context.requests), 1);
    ASSERT_EQ(atomic_load(&context.cancelled), 1);
    ASSERT_EQ(atomic_load(&context.closed), 1);
    PASS();
}

TEST(daemon_runtime_permanent_service_survives_last_disconnect_until_stop) {
    cbm_daemon_build_identity_t identity =
        runtime_test_identity("2.4.0", runtime_test_self_build());
    runtime_test_fixture_t fixture;
    runtime_test_fixture_permanent = true;
    bool started = runtime_test_fixture_start(&fixture, "permanent-lifecycle", &identity);
    runtime_test_fixture_permanent = false;
    cbm_daemon_runtime_client_t *client = NULL;
    cbm_daemon_runtime_connect_result_t connect_result = {0};
    bool survived = false;
    bool status_ok = false;
    bool stop_ok = false;
    bool exited = false;
    if (started) {
        client = cbm_daemon_runtime_client_connect(fixture.endpoint, &identity,
                                                   RUNTIME_TEST_TIMEOUT_MS, &connect_result);
    }
    if (client) {
        (void)cbm_daemon_runtime_client_close(client, RUNTIME_TEST_TIMEOUT_MS);
        client = NULL;
        /* Give a wrongly-armed teardown time to fire before asserting. */
        survived =
            !cbm_daemon_runtime_service_wait_exited(fixture.service, 300U) &&
            cbm_daemon_runtime_service_state(fixture.service) == CBM_DAEMON_RUNTIME_SERVICE_RUNNING;
        cbm_daemon_runtime_status_t status = {0};
        status_ok = cbm_daemon_runtime_request_status(fixture.endpoint, &identity,
                                                      RUNTIME_TEST_TIMEOUT_MS, &status) &&
                    status.permanent && !status.stopping && status.committed_clients == 0;
        cbm_daemon_runtime_stop_result_t stop_result = {0};
        stop_ok = cbm_daemon_runtime_request_stop(fixture.endpoint, &identity,
                                                  RUNTIME_TEST_TIMEOUT_MS, &stop_result) &&
                  stop_result.accepted && !stop_result.busy;
        exited = cbm_daemon_runtime_service_wait_exited(fixture.service, RUNTIME_TEST_TIMEOUT_MS);
    }
    runtime_test_fixture_finish(&fixture);
    ASSERT_TRUE(started);
    ASSERT_TRUE(survived);
    ASSERT_TRUE(status_ok);
    ASSERT_TRUE(stop_ok);
    ASSERT_TRUE(exited);
    PASS();
}

TEST(daemon_runtime_stop_refuses_while_committed_clients_exist) {
    cbm_daemon_build_identity_t identity =
        runtime_test_identity("2.4.0", runtime_test_self_build());
    runtime_test_fixture_t fixture;
    runtime_test_fixture_permanent = true;
    bool started = runtime_test_fixture_start(&fixture, "stop-refuse-busy", &identity);
    runtime_test_fixture_permanent = false;
    cbm_daemon_runtime_client_t *client = NULL;
    cbm_daemon_runtime_connect_result_t connect_result = {0};
    bool refused = false;
    bool accepted_after_close = false;
    bool exited = false;
    if (started) {
        client = cbm_daemon_runtime_client_connect(fixture.endpoint, &identity,
                                                   RUNTIME_TEST_TIMEOUT_MS, &connect_result);
    }
    if (client) {
        cbm_daemon_runtime_stop_result_t busy_result = {0};
        refused = cbm_daemon_runtime_request_stop(fixture.endpoint, &identity,
                                                  RUNTIME_TEST_TIMEOUT_MS, &busy_result) &&
                  busy_result.busy && !busy_result.accepted && busy_result.committed_clients == 1 &&
                  busy_result.client_count == 1 &&
                  busy_result.client_pids[0] == (uint32_t)connect_result.authenticated_process_id;
        (void)cbm_daemon_runtime_client_close(client, RUNTIME_TEST_TIMEOUT_MS);
        client = NULL;
        cbm_daemon_runtime_stop_result_t stop_result = {0};
        accepted_after_close =
            cbm_daemon_runtime_service_wait_for_clients(fixture.service, 0,
                                                        RUNTIME_TEST_TIMEOUT_MS) &&
            cbm_daemon_runtime_request_stop(fixture.endpoint, &identity, RUNTIME_TEST_TIMEOUT_MS,
                                            &stop_result) &&
            stop_result.accepted && !stop_result.busy;
        exited = cbm_daemon_runtime_service_wait_exited(fixture.service, RUNTIME_TEST_TIMEOUT_MS);
    }
    runtime_test_fixture_finish(&fixture);
    ASSERT_TRUE(started);
    ASSERT_TRUE(refused);
    ASSERT_TRUE(accepted_after_close);
    ASSERT_TRUE(exited);
    PASS();
}

SUITE(daemon_runtime) {
    RUN_TEST(daemon_runtime_permanent_service_survives_last_disconnect_until_stop);
    RUN_TEST(daemon_runtime_stop_refuses_while_committed_clients_exist);
    RUN_TEST(daemon_host_early_coordination_failure_is_durable);
    RUN_TEST(daemon_host_refuses_unopenable_runtime_config_database);
    RUN_TEST(daemon_host_http_reconcile_rate_limits_and_retries_transient_failures);
    RUN_TEST(daemon_host_http_retry_backoff_is_bounded);
    RUN_TEST(daemon_host_http_reconcile_retains_busy_server_until_free_succeeds);
#ifndef _WIN32
    RUN_TEST(daemon_host_failed_listener_reservation_starts_no_background_work);
    RUN_TEST(daemon_host_persistent_cleanup_release_failure_is_process_bounded);
    RUN_TEST(daemon_host_forced_shutdown_is_logged_flushed_and_process_bounded);
#endif
    RUN_TEST(daemon_runtime_kernel_process_fingerprint_is_stable_and_fail_closed);
#ifdef __APPLE__
    RUN_TEST(daemon_runtime_mac_fast_path_rejects_foreign_main_image_mapping_active);
#endif
    RUN_TEST(daemon_runtime_copied_image_fallback_accepts_identical_and_rejects_changed);
#if defined(_WIN32) || defined(__APPLE__) || defined(__linux__)
    RUN_TEST(daemon_runtime_process_fingerprint_never_hashes_replacement_path);
#endif
    RUN_TEST(daemon_runtime_convenience_service_owns_participant_guard);
    RUN_TEST(daemon_runtime_rendezvous_layout_is_frozen_and_detailed_abi_independent);
    RUN_TEST(daemon_runtime_exact_hello_issues_connection_bound_identity);
    RUN_TEST(daemon_runtime_unexpected_frame_payload_is_freed_once);
    RUN_TEST(daemon_runtime_activation_rejects_forged_and_malformed_without_stop);
    RUN_TEST(daemon_runtime_activation_ack_snapshots_then_interrupts_all_clients);
#if defined(_WIN32) || defined(__APPLE__) || defined(__linux__)
    RUN_TEST(daemon_runtime_activation_accepts_authenticated_different_build);
#endif
    RUN_TEST(daemon_runtime_future_generation_gets_stable_explicit_conflict);
    RUN_TEST(daemon_runtime_matching_clients_share_one_service_endpoint);
    RUN_TEST(daemon_runtime_same_version_different_build_is_visible_and_logged);
    RUN_TEST(daemon_runtime_conflict_log_failure_uses_operation_log_fallback);
    RUN_TEST(daemon_runtime_disconnect_releases_only_connection_subscriptions);
    RUN_TEST(daemon_runtime_final_disconnect_automatically_exits_within_bound);
    RUN_TEST(daemon_runtime_authenticated_idle_connection_outlives_lease_interval);
    RUN_TEST(daemon_runtime_connection_cap_covers_slow_hello_and_stopping_is_terminal);
    RUN_TEST(daemon_runtime_rejects_forged_identity_extension);
    RUN_TEST(daemon_runtime_application_response_roundtrip_is_byte_exact);
    RUN_TEST(daemon_runtime_final_disconnect_rejects_blocked_provisional_session);
    RUN_TEST(daemon_runtime_request_cancel_is_exact_and_session_remains_usable);
    RUN_TEST(daemon_runtime_presend_request_cancel_is_sticky_and_nonterminal);
    RUN_TEST(daemon_runtime_allows_only_one_unstarted_application_token);
    RUN_TEST(daemon_runtime_consumes_busy_application_token_before_response);
    RUN_TEST(daemon_runtime_close_begin_retains_storage_and_rejects_late_exchange);
    RUN_TEST(daemon_runtime_disconnect_cancels_blocked_application_before_exit);
    RUN_TEST(daemon_runtime_close_begin_releases_admission_with_inflight_request);
    RUN_TEST(daemon_runtime_disconnect_cancels_blocked_non_index_child_and_preserves_other_session);
    RUN_TEST(daemon_runtime_noncooperative_callback_does_not_detach_or_unbound_stop);
    RUN_TEST(daemon_runtime_application_busy_cap_and_malformed_are_isolated);
    RUN_TEST(daemon_runtime_malformed_and_zero_cancel_close_only_offending_connections);
}
