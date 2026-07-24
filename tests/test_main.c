/*
 * test_main.c — Test runner entry point for pure C rewrite.
 *
 * Includes all test suites and runs them sequentially.
 */
/* Global test counters (declared extern in test_framework.h) */
int tf_pass_count = 0;
int tf_fail_count = 0;
int tf_skip_count = 0;

#include "test_framework.h"
#include "test_helpers.h"
#include "test_daemon_runtime_contract.h"
#include "foundation/compat.h"     /* cbm_setenv — #845 supervisor kill switch */
#include "foundation/compat_fs.h"  /* cbm_fopen — worker response file */
#include "foundation/mem.h"        /* cbm_mem_init — worker budget */
#include "foundation/platform.h"   /* cbm_file_exists — blocking-git marker */
#include "daemon/runtime.h"        /* bounded worker response probe */
#include "daemon/ipc.h"            /* Windows private-lock re-exec probe */
#include "daemon/version_cohort.h" /* Windows crash-turnover re-exec probe */
#include "mcp/index_supervisor.h"  /* cbm_index_set_worker_role */
#include "mcp/mcp.h"               /* cbm_mcp_handle_tool — act as a real worker */
#include <sqlite3.h>
#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#ifdef _WIN32
#include <winsock2.h> /* #798 follow-up: socket-isolation re-exec probe */
#else
#include <unistd.h>
#ifdef __APPLE__
#include <fcntl.h>
#include <sys/mman.h>
#endif
#endif

/* daemon_runtime places an exact copy of this runner at a private PATH entry
 * named git/git.exe. Only that copied basename plus this private marker opt in
 * to the probe, so an inherited environment value cannot turn the ordinary
 * test runner into a blocking process. The first invocation publishes its PID
 * with create-exclusive semantics and ignores graceful termination; later git
 * invocations see the marker and exit cleanly, allowing the parent shell to
 * unwind after either production containment or the test's verified backstop. */
#define TF_BLOCKING_GIT_MARKER_ENV "CBM_TEST_RUNTIME_BLOCKING_GIT_PID_FILE"

#ifdef _WIN32
static bool tf_invoked_as_windows_git_module(void) {
    /* CreateProcessW authenticates the executable with lpApplicationName, but
     * the child CRT derives argv[0] from the separately supplied command line.
     * Inspect the actual loaded module so the copied git.exe probe cannot fall
     * through into the ordinary test runner when argv[0] is merely "git". */
    wchar_t image[32768];
    DWORD image_length =
        GetModuleFileNameW(NULL, image, (DWORD)(sizeof(image) / sizeof(image[0])));
    if (image_length == 0 || image_length >= (DWORD)(sizeof(image) / sizeof(image[0]))) {
        return false;
    }
    const wchar_t *base = image;
    for (const wchar_t *cursor = image; *cursor; cursor++) {
        if (*cursor == L'/' || *cursor == L'\\') {
            base = cursor + 1;
        }
    }
    return CompareStringOrdinal(base, -1, L"git.exe", -1, TRUE) == CSTR_EQUAL;
}
#endif

static bool tf_invoked_as_blocking_git(const char *argv0) {
    if (!getenv(TF_BLOCKING_GIT_MARKER_ENV)) {
        return false;
    }
#ifdef _WIN32
    (void)argv0;
    return tf_invoked_as_windows_git_module();
#else
    if (!argv0 || !argv0[0]) {
        return false;
    }
    const char *base = argv0;
    for (const char *cursor = argv0; *cursor; cursor++) {
        if (*cursor == '/' || *cursor == '\\') {
            base = cursor + 1;
        }
    }
    return strcmp(base, "git") == 0 || strcmp(base, "git.exe") == 0 ||
           strcmp(base, "GIT.EXE") == 0;
#endif
}

#ifdef _WIN32
static BOOL WINAPI tf_blocking_git_control_handler(DWORD event) {
    return event == CTRL_C_EVENT || event == CTRL_BREAK_EVENT;
}
#endif

static int tf_maybe_run_blocking_git_probe(int argc, char **argv) {
    (void)argc;
#ifdef _WIN32
    bool windows_git_module = tf_invoked_as_windows_git_module();
    if (windows_git_module && !getenv(TF_BLOCKING_GIT_MARKER_ENV)) {
        (void)puts("TF_BLOCKING_GIT_DIAGNOSTIC marker_environment_missing");
        (void)fflush(stdout);
        return 32;
    }
#endif
    if (!argv || !tf_invoked_as_blocking_git(argv[0])) {
        return -1;
    }
    const char *marker_path = getenv(TF_BLOCKING_GIT_MARKER_ENV);
    errno = 0;
#ifdef _WIN32
    SetLastError(ERROR_SUCCESS);
#endif
    FILE *marker = cbm_fopen(marker_path, "wbx");
    if (!marker) {
        /* The first invocation already owns the blocking role. detect_changes
         * runs two more git commands after it terminates; those must not block. */
#ifdef _WIN32
        int marker_errno = errno;
        DWORD marker_error = GetLastError();
        bool marker_exists = cbm_file_exists(marker_path);
        (void)printf("TF_BLOCKING_GIT_DIAGNOSTIC marker_open_failed errno=%d win_error=%lu "
                     "exists=%d\n",
                     marker_errno, (unsigned long)marker_error, marker_exists ? 1 : 0);
        (void)fflush(stdout);
        return marker_exists ? 0 : 30;
#else
        return cbm_file_exists(marker_path) ? 0 : 30;
#endif
    }
#ifdef _WIN32
    unsigned long long process_id = (unsigned long long)GetCurrentProcessId();
#else
    unsigned long long process_id = (unsigned long long)getpid();
#endif
    bool published =
        fprintf(marker, "%llu\n", process_id) > 0 && fflush(marker) == 0 && fclose(marker) == 0;
    if (!published) {
        return 31;
    }
#ifdef _WIN32
    (void)SetConsoleCtrlHandler(tf_blocking_git_control_handler, TRUE);
#else
    (void)signal(SIGTERM, SIG_IGN);
    (void)signal(SIGINT, SIG_IGN);
#endif
    for (;;) {
        cbm_usleep(100000);
    }
}

/* Test handlers that exercise the production index_repository flow must never
 * inherit the user's real cache. Individual tests may temporarily override
 * this sentinel and restore it; atexit removes anything left behind by an
 * assertion that returns before fixture cleanup. */
static char tf_home_sentinel[512];

static void tf_cleanup_cache_sentinel(void) {
    if (tf_home_sentinel[0]) {
        th_rmtree(tf_home_sentinel);
    }
}

static bool tf_setup_cache_sentinel(void) {
    snprintf(tf_home_sentinel, sizeof(tf_home_sentinel), "/tmp/cbm-test-home-XXXXXX");
    if (!cbm_mkdtemp(tf_home_sentinel)) {
        return false;
    }
    /* Legacy integration fixtures derive DB paths from HOME, while production
     * cache_dir() prefers CBM_CACHE_DIR. A private HOME plus no inherited cache
     * override keeps both conventions pointed at the same isolated tree. */
    cbm_setenv("HOME", tf_home_sentinel, 1);
    cbm_unsetenv("CBM_CACHE_DIR");
    atexit(tf_cleanup_cache_sentinel);
    return true;
}

/* Fast real-process probes for the async index-supervisor contract. They run
 * only in a child admitted by the exact build-bound worker grammar. */
static void tf_index_worker_probe(const char *args_json, const char *response_out) {
    if (!args_json || !strstr(args_json, "\"__cbm_test_worker\"")) {
        return;
    }
    if (strstr(args_json, "\"clean\"")) {
        FILE *response = response_out ? cbm_fopen(response_out, "wb") : NULL;
        if (response) {
            (void)fputs("{\"probe\":\"clean\"}", response);
            (void)fclose(response);
        }
        (void)fprintf(stderr, "async worker clean probe\n");
        fflush(NULL);
        _Exit(response ? 0 : 1);
    }
    if (strstr(args_json, "\"crash\"")) {
        (void)fprintf(stderr, "async worker crash probe\n");
        fflush(NULL);
        abort();
    }
    if (strstr(args_json, "\"oversize\"")) {
        FILE *response = response_out ? cbm_fopen(response_out, "wb") : NULL;
        bool written = false;
        if (response) {
            written =
                fseek(response, (long)CBM_DAEMON_RUNTIME_APPLICATION_PAYLOAD_MAX, SEEK_SET) == 0 &&
                fputc('x', response) != EOF;
            written = fclose(response) == 0 && written;
        }
        (void)fprintf(stderr, "async worker oversized response probe\n");
        fflush(NULL);
        _Exit(written ? 0 : 1);
    }
    if (strstr(args_json, "\"hang-tree\"")) {
        (void)signal(SIGTERM, SIG_IGN);
        long descendant = 0;
#ifndef _WIN32
        pid_t child = fork();
        if (child == 0) {
            for (;;) {
                cbm_usleep(100000);
            }
        }
        if (child > 0) {
            descendant = (long)child;
        }
#endif
        const char *marker = getenv("CBM_INDEX_MARKER_FILE");
        FILE *ready = marker ? cbm_fopen(marker, "wb") : NULL;
        if (ready) {
            (void)fprintf(
                ready, "single=%s\nmarker=%s\nquarantine=%s\nbudget=%zu\ndescendant=%ld\n",
                getenv("CBM_INDEX_SINGLE_THREAD") ? getenv("CBM_INDEX_SINGLE_THREAD") : "", marker,
                getenv("CBM_INDEX_QUARANTINE_FILE") ? getenv("CBM_INDEX_QUARANTINE_FILE") : "",
                cbm_mem_budget(), descendant);
            (void)fclose(ready);
        }
        (void)fprintf(stderr, "async worker hang-tree probe\n");
        fflush(NULL);
        for (;;) {
            cbm_usleep(100000);
        }
    }
}

/* #832 guard support: when the index supervisor spawns THIS binary with the
 * exact build-bound worker grammar produced by cbm_index_worker_start(), act
 * as a faithful in-process index worker instead of re-running the test suites.
 * This lets the deterministic
 * gating guard (test_mcp.c) spawn a REAL worker child that indexes the fixture and
 * writes its response back, using only public APIs — no production test seam.
 * Returns an exit code (>=0) when it handled a worker invocation, else -1. */
static int tf_maybe_run_index_worker(int argc, char **argv) {
    cbm_index_worker_invocation_t invocation;
    cbm_index_worker_argv_status_t status =
        cbm_index_worker_parse_process_argv(argc, argv, &invocation);
    if (status == CBM_INDEX_WORKER_ARGV_NOT_WORKER) {
        return -1;
    }
    if (status != CBM_INDEX_WORKER_ARGV_VALID) {
        (void)fprintf(stderr, "CBM test index worker could not start: %s\n",
                      cbm_index_worker_argv_status_message(status));
        return 1;
    }

    cbm_index_set_worker_role_options(true, invocation.response_out, invocation.single_thread,
                                      invocation.marker_file, invocation.quarantine_file,
                                      invocation.memory_budget_bytes);
    cbm_mem_init_with_cap(0.5, invocation.memory_budget_bytes);
    tf_index_worker_probe(invocation.args_json, invocation.response_out);
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    if (!srv) {
        return 1;
    }
    char *result = cbm_mcp_handle_tool(srv, "index_repository", invocation.args_json);
    if (result) {
        const char *ro = cbm_index_worker_response_out();
        if (ro) {
            FILE *rf = cbm_fopen(ro, "wb");
            if (rf) {
                (void)fputs(result, rf);
                (void)fclose(rf);
            }
        }
    }
    /* Faithful worker exit: mirror run_cli's supervised-worker fast path.
     * The worker-role pipeline deliberately skips its teardown (the OS
     * reclaims everything wholesale on process death), so a normal return
     * through main() lets LeakSanitizer run at exit, report the
     * intentionally-unfreed pipeline, and force exit code 1 — the
     * supervisor then reads a HEALTHY index as worker_failed (the
     * Linux-only IDX832 red: LSan is active in Linux gcc ASan builds,
     * absent on macOS/Windows). _Exit skips atexit/LSan by design,
     * exactly like the production worker in run_cli. */
    fflush(NULL);
    _Exit(result ? 0 : 1);
}

/* #798 follow-up: socket-isolation probe. The parent test
 * (popen_isolates_listening_socket, test_security.c) spawns THIS binary through
 * cbm_popen — the same cmd.exe-grandchild path git takes — passing the numeric
 * value of an inheritable listening-socket handle. If cbm_popen correctly
 * isolates handles, that socket is NOT present in this child and getsockopt
 * fails; a regression to raw _popen leaks it (bInheritHandles=TRUE propagates it
 * transitively through cmd.exe) and getsockopt succeeds. We report via exit code
 * so the verdict survives `cmd.exe /c` (proven by popen_isolated_propagates_exit_code).
 * Returns an exit code (>=0) when it handled a probe invocation, else -1. */
static int tf_maybe_run_socket_probe(int argc, char **argv) {
#ifdef _WIN32
    if (argc < 3 || strcmp(argv[1], "__cbm_sockprobe") != 0) {
        return -1;
    }
    WSADATA wsa;
    if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) {
        return 0; /* no winsock in child ⇒ cannot observe a socket ⇒ not leaked */
    }
    unsigned long long hv = strtoull(argv[2], NULL, 10);
    SOCKET s = (SOCKET)(uintptr_t)hv;
    int type = 0;
    int len = (int)sizeof(type);
    int rc = getsockopt(s, SOL_SOCKET, SO_TYPE, (char *)&type, &len);
    /* rc==0 ⇒ the handle is a live socket in THIS child ⇒ it was inherited. */
    return rc == 0 ? 42 : 0;
#else
    (void)argc;
    (void)argv;
    return -1;
#endif
}

/* Windows cannot use fork to prove process-to-process daemon lock ownership.
 * The daemon IPC suite re-execs this runner in this narrow mode and observes
 * the tri-state through a stable exit-code mapping: 0 acquired, 20 busy,
 * 21 validation/OS error. */
static int tf_maybe_run_daemon_ipc_lock_probe(int argc, char **argv) {
#ifdef _WIN32
    if (argc != 5 || strcmp(argv[1], "__cbm_daemon_ipc_lock_probe") != 0) {
        return -1;
    }
    cbm_daemon_ipc_endpoint_t *endpoint = cbm_daemon_ipc_endpoint_new(argv[3], argv[4]);
    if (!endpoint) {
        return 21;
    }
    int result = -1;
    if (strcmp(argv[2], "startup") == 0) {
        cbm_daemon_ipc_startup_lock_t *lock = NULL;
        result = cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &lock);
        if (!cbm_daemon_ipc_startup_lock_release(&lock)) {
            result = -1;
        }
    } else if (strcmp(argv[2], "lifetime") == 0) {
        cbm_daemon_ipc_lifetime_reservation_t *reservation = NULL;
        result = cbm_daemon_ipc_lifetime_reservation_try_acquire(endpoint, &reservation);
        cbm_daemon_ipc_lifetime_reservation_release(reservation);
    }
    cbm_daemon_ipc_endpoint_free(endpoint);
    return result == 1 ? 0 : (result == 0 ? 20 : 21);
#else
    (void)argc;
    (void)argv;
    return -1;
#endif
}

static int tf_maybe_run_version_cohort_crash_holder(int argc, char **argv) {
#ifdef _WIN32
    if (argc != 5 || strcmp(argv[1], "__cbm_version_cohort_crash_holder") != 0) {
        return -1;
    }
    cbm_daemon_ipc_endpoint_t *endpoint = cbm_daemon_ipc_endpoint_new(argv[2], argv[3]);
    cbm_version_cohort_manager_t *manager =
        endpoint ? cbm_version_cohort_manager_new(endpoint) : NULL;
    cbm_daemon_build_identity_t identity = {
        .semantic_version = "2.4.0",
        .build_fingerprint = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        .cache_fingerprint = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        .protocol_abi = 3,
        .store_abi = 11,
        .feature_abi = 7,
    };
    cbm_version_cohort_lease_t *lease = NULL;
    cbm_daemon_conflict_t conflict;
    cbm_version_cohort_status_t status =
        manager ? cbm_version_cohort_acquire(manager, &identity, UINT64_MAX, &lease, &conflict)
                : CBM_VERSION_COHORT_IO;
    FILE *ready = status == CBM_VERSION_COHORT_OK ? cbm_fopen(argv[4], "wb") : NULL;
    bool announced = false;
    if (ready) {
        bool written = fputc('R', ready) != EOF;
        announced = fclose(ready) == 0 && written;
    }
    if (announced) {
        Sleep(INFINITE);
        return 23;
    }
    while (lease && cbm_version_cohort_lease_release(&lease) != CBM_PRIVATE_FILE_LOCK_OK) {
        cbm_usleep(1000);
    }
    while (manager && cbm_version_cohort_manager_free(&manager) != CBM_PRIVATE_FILE_LOCK_OK) {
        cbm_usleep(1000);
    }
    cbm_daemon_ipc_endpoint_free(endpoint);
    return 22;
#else
    (void)argc;
    (void)argv;
    return -1;
#endif
}

static int tf_maybe_run_runtime_image_holder(int argc, char **argv) {
#ifdef _WIN32
    if (argc != 3 || strcmp(argv[1], "__cbm_runtime_image_holder") != 0) {
        return -1;
    }
    HANDLE ready = OpenEventA(EVENT_MODIFY_STATE, FALSE, argv[2]);
    bool announced = ready && SetEvent(ready) != 0;
    if (ready) {
        (void)CloseHandle(ready);
    }
    if (!announced) {
        return 24;
    }
    Sleep(INFINITE);
    return 25;
#else
    (void)argc;
    (void)argv;
    return -1;
#endif
}

/* Real copied-image HELLO probe used by daemon_runtime. Keeping this in the
 * test runner avoids any production-only test hook: the daemon authenticates
 * and fingerprints an ordinary, separately executed process image. */
static int tf_maybe_run_runtime_hello_client(int argc, char **argv) {
    if (argc != 6 || strcmp(argv[1], "__cbm_runtime_hello_client") != 0) {
        return -1;
    }
    cbm_daemon_ipc_endpoint_t *endpoint = cbm_daemon_ipc_endpoint_new(argv[3], argv[2]);
    cbm_daemon_build_identity_t identity = {
        .semantic_version = argv[4],
        .build_fingerprint = argv[5],
    };
    cbm_daemon_runtime_connect_result_t result;
    memset(&result, 0, sizeof(result));
    cbm_daemon_runtime_client_t *client =
        endpoint ? cbm_daemon_runtime_client_connect(endpoint, &identity,
                                                     TF_RUNTIME_IMAGE_EXCHANGE_TIMEOUT_MS, &result)
                 : NULL;
    bool accepted = client && result.status == CBM_DAEMON_RUNTIME_CONNECT_ACCEPTED &&
                    result.hello_status == CBM_DAEMON_HELLO_COMPATIBLE;
    bool closed =
        !client || cbm_daemon_runtime_client_close(client, TF_RUNTIME_IMAGE_EXCHANGE_TIMEOUT_MS);
    cbm_daemon_ipc_endpoint_free(endpoint);
    if (!accepted) {
        return 26;
    }
    return closed ? 0 : 27;
}

/* Real copied-image activation probe. The request identity is supplied by the
 * executing copy so the daemon must authenticate that peer image rather than
 * require the active generation's build fingerprint. */
static int tf_maybe_run_runtime_activation_client(int argc, char **argv) {
    if (argc != 7 || strcmp(argv[1], "__cbm_runtime_activation_client") != 0) {
        return -1;
    }
    char *action_end = NULL;
    unsigned long action_value = strtoul(argv[6], &action_end, 10);
    bool action_valid = action_end != argv[6] && *action_end == '\0' &&
                        action_value >= (unsigned long)CBM_DAEMON_RUNTIME_ACTIVATION_INSTALL &&
                        action_value <= (unsigned long)CBM_DAEMON_RUNTIME_ACTIVATION_UNINSTALL;
    cbm_daemon_ipc_endpoint_t *endpoint =
        action_valid ? cbm_daemon_ipc_endpoint_new(argv[3], argv[2]) : NULL;
    cbm_daemon_build_identity_t identity = {
        .semantic_version = argv[4],
        .build_fingerprint = argv[5],
    };
    cbm_daemon_runtime_activation_result_t result;
    memset(&result, 0, sizeof(result));
    bool exchanged =
        endpoint && cbm_daemon_runtime_request_activation_shutdown(
                        endpoint, &identity, (cbm_daemon_runtime_activation_action_t)action_value,
                        TF_RUNTIME_IMAGE_EXCHANGE_TIMEOUT_MS, &result);
    cbm_daemon_ipc_endpoint_free(endpoint);
    return exchanged && result.accepted ? 0 : 29;
}

/* macOS adversarial HELLO probe: execute this mode from a foreign process
 * image while mapping the genuine runner RX. The daemon must authenticate the
 * main image, not merely find its own vnode in an arbitrary executable map. */
static int tf_maybe_run_runtime_mapped_hello_client(int argc, char **argv) {
#ifdef __APPLE__
    if (argc != 7 || strcmp(argv[1], "__cbm_runtime_mapped_hello_client") != 0) {
        return -1;
    }
    int image_fd = open(argv[2], O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    void *mapping =
        image_fd >= 0 ? mmap(NULL, 1, PROT_READ | PROT_EXEC, MAP_PRIVATE, image_fd, 0) : MAP_FAILED;
    bool mapped = mapping != MAP_FAILED;
    if (image_fd >= 0 && close(image_fd) != 0) {
        mapped = false;
    }
    if (!mapped) {
        if (mapping != MAP_FAILED) {
            (void)munmap(mapping, 1);
        }
        return 28;
    }

    char *hello_argv[] = {argv[0], "__cbm_runtime_hello_client", argv[3], argv[4], argv[5],
                          argv[6]};
    int result = tf_maybe_run_runtime_hello_client(6, hello_argv);
    if (munmap(mapping, 1) != 0) {
        return 28;
    }
    return result >= 0 ? result : 28;
#else
    (void)argc;
    (void)argv;
    return -1;
#endif
}

static int tf_maybe_run_mcp_idxfailclosed_probe(int argc, char **argv) {
#ifndef _WIN32
    if (argc != 4 || strcmp(argv[1], "__cbm_mcp_idxfailclosed_probe") != 0) {
        return -1;
    }
    extern int mcp_test_idxfailclosed_supervisor_start_check(const char *repo_dir,
                                                             const char *cache_dir);
    alarm(20);
    return mcp_test_idxfailclosed_supervisor_start_check(argv[2], argv[3]);
#else
    (void)argc;
    (void)argv;
    return -1;
#endif
}

static int g_suite_argc = 0;
static char **g_suite_argv = NULL;
static bool *g_suite_arg_matched = NULL;

static bool suite_requested(const char *name) {
    if (g_suite_argc <= 1) {
        return true;
    }
    bool requested = false;
    for (int i = 1; i < g_suite_argc; i++) {
        if (strcmp(g_suite_argv[i], name) == 0) {
            g_suite_arg_matched[i] = true;
            requested = true;
        }
    }
    return requested;
}

/* --list-suites: print every registered suite name, one per line, without
 * running anything. The list and the run share this ONE macro table, so the
 * list can never drift from what actually executes — shard runners (make
 * test-par, the CI shard matrix) enumerate suites from here and their union
 * guard compares against it. */
static bool g_list_only = false;

/* CBM_SKIP_PERF=1 excludes throughput/scale/perf-metric suites from the run.
 * The fidelity pass (CBM_LOCAL_CI_CPUS=4) and CI PRs set it: those suites
 * measure performance, which is meaningless under artificial CPU starvation,
 * and they dominate wall-clock. Correctness coverage is unaffected — the perf
 * suites run at full power in the perf/release legs (CBM_SKIP_PERF unset). The
 * skip applies to BOTH --list-suites and the run through the same macro, so
 * the shard union guard stays consistent. */
static bool g_skip_perf = false;

#define RUN_SELECTED_SUITE(name)             \
    do {                                     \
        if (g_list_only) {                   \
            printf("%s\n", #name);           \
        } else if (suite_requested(#name)) { \
            RUN_SUITE(name);                 \
        }                                    \
    } while (0)

#define RUN_SELECTED_SUITE_PERF(name)        \
    do {                                     \
        if (g_skip_perf) {                   \
            break;                           \
        }                                    \
        if (g_list_only) {                   \
            printf("%s\n", #name);           \
        } else if (suite_requested(#name)) { \
            RUN_SUITE(name);                 \
        }                                    \
    } while (0)

/* Forward declarations of suite functions */
extern void suite_arena(void);
extern void suite_hash_table(void);
extern void suite_dyn_array(void);
extern void suite_str_intern(void);
extern void suite_log(void);
extern void suite_str_util(void);
extern void suite_platform(void);
extern void suite_diagnostics(void);
extern void suite_subprocess(void);
extern void suite_private_file_lock(void);
extern void suite_lock_registry(void);
extern void suite_extraction(void);
extern void suite_extraction_inheritance(void);
extern void suite_extraction_imports(void);
extern void suite_parse_coverage(void);
extern void suite_grammar_regression(void);
extern void suite_grammar_labels(void);
extern void suite_grammar_imports(void);
extern void suite_ac(void);
extern void suite_store_nodes(void);
extern void suite_store_edges(void);
extern void suite_store_search(void);
extern void suite_cypher(void);
extern void suite_mcp(void);
extern void suite_mcp_mutation_guard(void);
extern void suite_index_supervisor(void);
extern void suite_daemon(void);
extern void suite_project_lock(void);
extern void suite_version_cohort(void);
extern void suite_daemon_version(void);
extern void suite_daemon_runtime(void);
extern void suite_daemon_application(void);
extern void suite_daemon_frontend(void);
extern void suite_daemon_bootstrap(void);
extern void suite_daemon_ipc(void);
extern void suite_language(void);
extern void suite_userconfig(void);
extern void suite_gitignore(void);
extern void suite_git_context(void);
extern void suite_discover(void);
extern void suite_graph_buffer(void);
extern void suite_registry(void);
extern void suite_pipeline(void);
extern void suite_cross_repo(void);
extern void suite_index_resilience(void);
extern void suite_fqn(void);
extern void suite_route_canon(void);
extern void suite_path_alias(void);
extern void suite_watcher(void);
extern void suite_lz4(void);
extern void suite_zstd(void);
extern void suite_artifact(void);
extern void suite_sqlite_writer(void);
extern void suite_go_lsp(void);
extern void suite_c_lsp(void);
extern void suite_php_lsp(void);
extern void suite_cs_lsp(void);
extern void suite_cs_lsp_bench(void);
extern void suite_perl_lsp(void);
extern void suite_scope(void);
extern void suite_type_rep(void);
extern void suite_py_lsp(void);
extern void suite_py_lsp_bench(void);
extern void suite_py_lsp_stress(void);
extern void suite_py_lsp_scale(void);
extern void suite_ts_lsp(void);
extern void suite_java_lsp(void);
extern void suite_java_lsp_coverage(void);
extern void suite_kotlin_lsp(void);
extern void suite_rust_lsp(void);
extern void suite_store_arch(void);
extern void suite_store_bulk(void);
extern void suite_store_pragmas(void);
extern void suite_store_checkpoint(void);
extern void suite_traces(void);
extern void suite_configlink(void);
extern void suite_infrascan(void);
extern void suite_cli(void);
extern void suite_agent_clients(void);
extern void suite_agent_profiles(void);
extern void suite_config_json_like(void);
extern void suite_config_toml_edit(void);
extern void suite_config_yaml_edit(void);
extern void suite_config_text_edit(void);
extern void suite_activation_transaction(void);
extern void suite_windows_launcher_state(void);
extern void suite_system_info(void);
extern void suite_worker_pool(void);
extern void suite_parallel(void);
extern void suite_mem(void);
extern void suite_ui(void);
extern void suite_httpd(void);
extern void suite_security(void);
extern void suite_yaml(void);
extern void suite_integration(void);
extern void suite_lang_contract(void);
extern void suite_edge_imports(void);
extern void suite_edge_structural(void);
extern void suite_lsp_resolution_probe(void);
extern void suite_node_creation_probe(void);
extern void suite_edge_types_probe(void);
extern void suite_convergence_probe(void);
extern void suite_matrix_known_classes(void);
extern void suite_matrix_new_constructs(void);
extern void suite_grammar_probe_a(void);
extern void suite_grammar_probe_b(void);
extern void suite_grammar_probe_c(void);
extern void suite_grammar_probe_d(void);
extern void suite_grammar_probe_e(void);
extern void suite_grammar_probe_f(void);
extern void suite_grammar_probe_g(void);
extern void suite_incremental(void);
extern void suite_semantic(void);
extern void suite_ast_profile(void);
extern void suite_slab_alloc(void);
extern void suite_simhash(void);
extern void suite_stack_overflow_a(void);
extern void suite_stack_overflow_b(void);
extern void suite_stack_overflow_c(void);
extern void suite_dump_verify(void);
extern void suite_dump_verify_io(void);

/* Free the main thread's thread-local node-type bitset cache before exit so
 * LeakSanitizer (Linux x64) doesn't report it. Worker threads free their own
 * caches at thread teardown (pass_parallel.c). */
extern void cbm_kind_in_set_free_cache(void);

int main(int argc, char **argv) {
    /* Skip the multi-hundred-MB executable-image hash that computes the exact
     * build fingerprint: it is tens of seconds per spawned worker/daemon under
     * ASan on constrained CI runners and the sole cause of the daemon-family
     * readiness-timeout flakes. Set once here; every forked child and re-exec'd
     * worker inherits it, so exact-build match/mismatch still works (a
     * mismatch test still passes a DIFFERENT fingerprint via argv). Honoured
     * only under CBM_CLI_ENABLE_TEST_API — never in a production binary. */
    if (!getenv("CBM_TEST_BUILD_FINGERPRINT")) {
        (void)cbm_setenv("CBM_TEST_BUILD_FINGERPRINT",
                         "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", 1);
    }
    int blocking_git_rc = tf_maybe_run_blocking_git_probe(argc, argv);
    if (blocking_git_rc >= 0) {
        return blocking_git_rc;
    }
    /* Installation tests use this executable as a structurally real candidate.
     * Mirror the production binary's minimal verification contract. */
    if (argc == 2 && strcmp(argv[1], "--version") == 0) {
        (void)puts("codebase-memory-mcp test-runner");
        return 0;
    }
    int mcp_idxfailclosed_rc = tf_maybe_run_mcp_idxfailclosed_probe(argc, argv);
    if (mcp_idxfailclosed_rc >= 0) {
        return mcp_idxfailclosed_rc;
    }
    int runtime_image_rc = tf_maybe_run_runtime_image_holder(argc, argv);
    if (runtime_image_rc >= 0) {
        return runtime_image_rc;
    }
    int runtime_hello_rc = tf_maybe_run_runtime_hello_client(argc, argv);
    if (runtime_hello_rc >= 0) {
        return runtime_hello_rc;
    }
    int runtime_activation_rc = tf_maybe_run_runtime_activation_client(argc, argv);
    if (runtime_activation_rc >= 0) {
        return runtime_activation_rc;
    }
    int runtime_mapped_hello_rc = tf_maybe_run_runtime_mapped_hello_client(argc, argv);
    if (runtime_mapped_hello_rc >= 0) {
        return runtime_mapped_hello_rc;
    }
    int cohort_crash_rc = tf_maybe_run_version_cohort_crash_holder(argc, argv);
    if (cohort_crash_rc >= 0) {
        return cohort_crash_rc;
    }
    int daemon_ipc_probe_rc = tf_maybe_run_daemon_ipc_lock_probe(argc, argv);
    if (daemon_ipc_probe_rc >= 0) {
        return daemon_ipc_probe_rc;
    }

    /* #798 follow-up: if spawned as the socket-isolation probe, report whether an
     * inheritable socket handle crossed into this child and exit before any suite. */
    int probe_rc = tf_maybe_run_socket_probe(argc, argv);
    if (probe_rc >= 0) {
        return probe_rc;
    }

    /* Capture once so the parent tests and any re-exec'd worker bind to this
     * executable image. A worker with an unavailable/mismatched identity is
     * rejected by the exact parser below before any suite or MCP state exists. */
    (void)cbm_index_supervisor_capture_build_fingerprint();

    /* #832: if spawned as a supervised index worker, do the real work and exit
     * before any suite runs (see tf_maybe_run_index_worker). */
    int worker_rc = tf_maybe_run_index_worker(argc, argv);
    if (worker_rc >= 0) {
        return worker_rc;
    }

    /* #845 belt-and-suspenders: this binary EMBEDS cbm_mcp_handle_tool. The
     * supervisor gate already ignores unmarked hosts, but pin the kill switch
     * too so even a future supervisor-marked test host can never resolve THIS
     * binary as `<self> cli --index-worker …` and recursively re-run suites.
     * A test that exercises the supervisor must explicitly re-enable it. */
    cbm_setenv("CBM_INDEX_SUPERVISOR", "0", 1);
    if (!tf_setup_cache_sentinel()) {
        fprintf(stderr, "failed to create isolated test cache\n");
        return 2;
    }

    const char *skip_perf_env = getenv("CBM_SKIP_PERF");
    g_skip_perf = skip_perf_env != NULL && strcmp(skip_perf_env, "1") == 0;
    if (argc == 2 && strcmp(argv[1], "--list-suites") == 0) {
        g_list_only = true;
        g_suite_argc = 1; /* no suite-name args to match */
    } else {
        g_suite_argc = argc;
        g_suite_argv = argv;
    }
    if (g_suite_argc > 1) {
        g_suite_arg_matched = calloc((size_t)argc, sizeof(*g_suite_arg_matched));
        if (!g_suite_arg_matched) {
            fprintf(stderr, "Failed to allocate test-suite argument tracking\n");
            return 1;
        }
    }
    if (!g_list_only) {
        printf("\n  codebase-memory-mcp  C test suite\n");
    }

    /* Foundation */
    RUN_SELECTED_SUITE(arena);
    RUN_SELECTED_SUITE(hash_table);
    RUN_SELECTED_SUITE(dyn_array);
    RUN_SELECTED_SUITE(str_intern);
    RUN_SELECTED_SUITE(log);
    RUN_SELECTED_SUITE(str_util);
    RUN_SELECTED_SUITE(platform);
    RUN_SELECTED_SUITE(diagnostics);
    RUN_SELECTED_SUITE(subprocess);
    RUN_SELECTED_SUITE(private_file_lock);
    RUN_SELECTED_SUITE(lock_registry);
    RUN_SELECTED_SUITE(dump_verify);

    /* Existing C code regression tests */
    RUN_SELECTED_SUITE(ac);
    RUN_SELECTED_SUITE(extraction);
    RUN_SELECTED_SUITE(extraction_inheritance);
    RUN_SELECTED_SUITE(extraction_imports);
    RUN_SELECTED_SUITE(parse_coverage);
    RUN_SELECTED_SUITE(grammar_regression);
    RUN_SELECTED_SUITE(grammar_labels);
    RUN_SELECTED_SUITE(grammar_imports);

    /* Store (M5) */
    RUN_SELECTED_SUITE(store_nodes);
    RUN_SELECTED_SUITE(store_edges);
    RUN_SELECTED_SUITE(store_search);
    RUN_SELECTED_SUITE(store_bulk);
    RUN_SELECTED_SUITE(store_pragmas);
    RUN_SELECTED_SUITE(store_checkpoint);
    RUN_SELECTED_SUITE(dump_verify_io);

    /* Cypher (M6) */
    RUN_SELECTED_SUITE(cypher);

    /* MCP Server (M9) */
    RUN_SELECTED_SUITE(mcp);
    RUN_SELECTED_SUITE(mcp_mutation_guard);
    RUN_SELECTED_SUITE(index_supervisor);

    /* Shared MCP daemon coordination + private framing */
    RUN_SELECTED_SUITE(daemon);
    RUN_SELECTED_SUITE(project_lock);
    RUN_SELECTED_SUITE(version_cohort);
    RUN_SELECTED_SUITE(daemon_version);
    RUN_SELECTED_SUITE(daemon_runtime);
    RUN_SELECTED_SUITE(daemon_application);
    RUN_SELECTED_SUITE(daemon_frontend);
    RUN_SELECTED_SUITE(daemon_bootstrap);
    RUN_SELECTED_SUITE(daemon_ipc);

    /* Discover (M2) */
    RUN_SELECTED_SUITE(language);
    RUN_SELECTED_SUITE(userconfig);
    RUN_SELECTED_SUITE(gitignore);
    RUN_SELECTED_SUITE(git_context);
    RUN_SELECTED_SUITE(discover);

    /* Graph Buffer (M7) */
    RUN_SELECTED_SUITE(graph_buffer);

    /* Pipeline (M8) */
    RUN_SELECTED_SUITE(registry);
    RUN_SELECTED_SUITE(pipeline);
    RUN_SELECTED_SUITE(cross_repo);
    RUN_SELECTED_SUITE(index_resilience);
    RUN_SELECTED_SUITE(fqn);
    RUN_SELECTED_SUITE(route_canon);
    RUN_SELECTED_SUITE(path_alias);

    /* Watcher (M10) */
    RUN_SELECTED_SUITE(watcher);

    /* LZ4 + zstd + SQLite writer */
    RUN_SELECTED_SUITE(lz4);
    RUN_SELECTED_SUITE(zstd);
    RUN_SELECTED_SUITE(sqlite_writer);

    /* Persistent artifact export/import */
    RUN_SELECTED_SUITE(artifact);

    /* LSP resolvers */
    RUN_SELECTED_SUITE(scope);
    RUN_SELECTED_SUITE(type_rep);
    RUN_SELECTED_SUITE(go_lsp);
    RUN_SELECTED_SUITE(c_lsp);
    RUN_SELECTED_SUITE(php_lsp);
    RUN_SELECTED_SUITE(cs_lsp);
    RUN_SELECTED_SUITE_PERF(cs_lsp_bench);
    RUN_SELECTED_SUITE(perl_lsp);
    RUN_SELECTED_SUITE(py_lsp);
    RUN_SELECTED_SUITE(kotlin_lsp);
    RUN_SELECTED_SUITE(rust_lsp);
    RUN_SELECTED_SUITE_PERF(py_lsp_bench);
    RUN_SELECTED_SUITE(py_lsp_stress);
    RUN_SELECTED_SUITE_PERF(py_lsp_scale);
    RUN_SELECTED_SUITE(ts_lsp);
    RUN_SELECTED_SUITE(java_lsp);
    RUN_SELECTED_SUITE(java_lsp_coverage);

    /* Architecture + ADR + Louvain */
    RUN_SELECTED_SUITE(store_arch);

    /* HTTP link */

    /* Traces helpers */
    RUN_SELECTED_SUITE(traces);

    /* Config link */
    RUN_SELECTED_SUITE(configlink);

    /* Infrastructure scanning */
    RUN_SELECTED_SUITE(infrascan);

    /* CLI (install, update, config) */
    RUN_SELECTED_SUITE(cli);
    RUN_SELECTED_SUITE(agent_clients);
    RUN_SELECTED_SUITE(agent_profiles);
    RUN_SELECTED_SUITE(config_json_like);
    RUN_SELECTED_SUITE(config_toml_edit);
    RUN_SELECTED_SUITE(config_yaml_edit);
    RUN_SELECTED_SUITE(config_text_edit);
    RUN_SELECTED_SUITE(activation_transaction);
    RUN_SELECTED_SUITE(windows_launcher_state);

    /* System info + worker pool (parallelism) */
    RUN_SELECTED_SUITE(system_info);
    RUN_SELECTED_SUITE(worker_pool);

    /* Parallel pipeline */
    RUN_SELECTED_SUITE(parallel);

    /* mem + arena + slab integration */
    RUN_SELECTED_SUITE(slab_alloc);
    RUN_SELECTED_SUITE(mem);

    /* UI (config, embedded assets, layout) */
    RUN_SELECTED_SUITE(ui);

    /* UI HTTP server (transport + routing) */
    RUN_SELECTED_SUITE(httpd);

    /* Security defenses */
    RUN_SELECTED_SUITE(security);

    /* YAML parser */
    RUN_SELECTED_SUITE(yaml);

    /* SimHash / SIMILAR_TO */
    RUN_SELECTED_SUITE(semantic);
    RUN_SELECTED_SUITE(ast_profile);
    RUN_SELECTED_SUITE(simhash);

    /* Stack overflow regression (GitHub #199) — split a/b/c so no single
     * suite serializes a parallel run (each ~1/3 of the old wall time). */
    RUN_SELECTED_SUITE(stack_overflow_a);
    RUN_SELECTED_SUITE(stack_overflow_b);
    RUN_SELECTED_SUITE(stack_overflow_c);

    /* Integration (end-to-end) */
    RUN_SELECTED_SUITE(integration);

    /* Per-language graph contracts (node/edge types, attribution, no-crash) */
    RUN_SELECTED_SUITE(lang_contract);
    RUN_SELECTED_SUITE(edge_imports);
    RUN_SELECTED_SUITE(edge_structural);
    RUN_SELECTED_SUITE(lsp_resolution_probe);
    RUN_SELECTED_SUITE(node_creation_probe);
    RUN_SELECTED_SUITE(edge_types_probe);
    RUN_SELECTED_SUITE(convergence_probe);
    RUN_SELECTED_SUITE(matrix_known_classes);
    RUN_SELECTED_SUITE(matrix_new_constructs);
    RUN_SELECTED_SUITE(grammar_probe_a);
    RUN_SELECTED_SUITE(grammar_probe_b);
    RUN_SELECTED_SUITE(grammar_probe_c);
    RUN_SELECTED_SUITE(grammar_probe_d);
    RUN_SELECTED_SUITE(grammar_probe_e);
    RUN_SELECTED_SUITE(grammar_probe_f);
    RUN_SELECTED_SUITE(grammar_probe_g);

    RUN_SELECTED_SUITE_PERF(incremental);

    if (g_list_only) {
        fflush(stdout);
        cbm_kind_in_set_free_cache();
        sqlite3_shutdown();
        return 0;
    }
    bool any_suite_matched = false;
    for (int i = 1; i < g_suite_argc; i++) {
        any_suite_matched = any_suite_matched || g_suite_arg_matched[i];
    }
    fflush(stdout);
    for (int i = 1; i < g_suite_argc; i++) {
        if (!g_suite_arg_matched[i]) {
            fprintf(stderr, "Unknown test suite: %s\n", g_suite_argv[i]);
            tf_fail_count++;
        }
    }
    if (g_suite_argc > 1 && !any_suite_matched) {
        fprintf(stderr, "No matching test suites requested\n");
    }
    free(g_suite_arg_matched);
    g_suite_arg_matched = NULL;

    /* Release process-lifetime caches so LeakSanitizer reports no leaks. */
    cbm_kind_in_set_free_cache();
    sqlite3_shutdown();
    TEST_SUMMARY();
}
