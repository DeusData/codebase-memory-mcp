/*
 * test_main.c — Test runner entry point for pure C rewrite.
 *
 * Includes all test suites and runs them sequentially.
 */
/* Global test counters (declared extern in test_framework.h) */
int tf_pass_count = 0;
int tf_fail_count = 0;
int tf_skip_count = 0;
int tf_deselected_count = 0;

#include "test_framework.h"
#include "test_helpers.h"
#include "test_daemon_runtime_contract.h"
#include "foundation/compat.h"         /* cbm_setenv — #845 supervisor kill switch */
#include "foundation/compat_fs.h"      /* cbm_fopen — worker response file */
#include "foundation/constants.h"      /* CBM_SZ_4K — forced stderr buffer */
#include "foundation/log.h"            /* crash-durable worker log probe */
#include "foundation/mem.h"            /* cbm_mem_init — worker budget */
#include "foundation/log.h"            /* worker liveness heartbeat probe */
#include "foundation/platform.h"       /* cbm_file_exists — blocking-git marker */
#include "foundation/test_selection.h" /* shared executable selection token bound */
#include "daemon/bootstrap.h"          /* runner rendezvous isolation */
#include "daemon/runtime.h"            /* bounded worker response probe */
#include "daemon/ipc.h"                /* Windows private-lock re-exec probe */
#include "daemon/version_cohort.h"     /* Windows crash-turnover re-exec probe */
#include "mcp/index_supervisor.h"      /* cbm_index_set_worker_role */
#include "mcp/mcp.h"                   /* cbm_mcp_handle_tool — act as a real worker */
#include "ui/http_server.h"            /* deleted-self executable probe */
#include "result_spill.h"              /* pinned free disk: spill verdicts ignore the host disk */
#include <sqlite3.h>
#include <ctype.h>
#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#include <winsock2.h> /* #798 follow-up: socket-isolation re-exec probe */
#include <windows.h>
#include "foundation/win_utf8.h" /* cbm_module_path_utf8 — the runner's own image */
#else
#include <unistd.h>
#ifdef __APPLE__
#include <fcntl.h>
#include <sys/mman.h>
#endif
#ifdef CBM_TEST_COVERAGE
#include <dlfcn.h>    /* the libc functions behind the profile-keeping ones */
#include <fcntl.h>    /* the marker of a forked child */
#include <poll.h>     /* non-consuming child-failure observation */
#include <pthread.h>  /* pthread_atfork: a forked child names its own profile */
#include <stdarg.h>   /* execl */
#include <sys/stat.h> /* reject detectable failure-channel descriptor replacement */
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

/* Native child for subprocess_windows_job_object_enforces_memory_limit. The
 * fixed-size commit keeps the RED path bounded: without a Job memory limit it
 * succeeds and exits 0; with the limit it is denied and exits with the sentinel
 * code expected by the parent test. */
static int tf_maybe_run_windows_memory_limit_probe(int argc, char **argv) {
#ifdef _WIN32
    if (argc == 2 && argv && strcmp(argv[1], "__cbm_windows_memory_limit_probe") == 0) {
        const SIZE_T allocation_size = (SIZE_T)2U * 1024U * 1024U * 1024U;
        void *allocation =
            VirtualAlloc(NULL, allocation_size, MEM_RESERVE | MEM_COMMIT, PAGE_READWRITE);
        if (!allocation) {
            return 73;
        }
        (void)VirtualFree(allocation, 0, MEM_RELEASE);
        return 0;
    }
#else
    (void)argc;
    (void)argv;
#endif
    return -1;
}

#ifdef _WIN32
static bool tf_invoked_as_windows_git_module(void) {
    /* CreateProcessW authenticates the executable with lpApplicationName, but
     * the child CRT derives argv[0] from the separately supplied command line.
     * Inspect the actual loaded module so the copied git.exe probe cannot fall
     * through into the ordinary test runner when argv[0] is merely "git". */
    wchar_t image[32768];
    DWORD image_length = GetModuleFileNameW(NULL, image, (DWORD)(sizeof(image) / sizeof(image[0])));
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
    return strcmp(base, "git") == 0 || strcmp(base, "git.exe") == 0 || strcmp(base, "GIT.EXE") == 0;
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
static char tf_runtime_sentinel[512];

static void tf_cleanup_cache_sentinel(void) {
    if (tf_home_sentinel[0]) {
        th_rmtree(tf_home_sentinel);
    }
    if (tf_runtime_sentinel[0]) {
        th_rmtree(tf_runtime_sentinel);
    }
}

/* Client home overrides the CLI honours BEFORE $HOME, so redirecting HOME alone
 * does not isolate them: cbm_codex_config_dir() and its siblings return the
 * ambient path and the suite resolves against the developer's real config —
 * reading its state and writing to it. Same inventory the shell fixtures are
 * already required to neutralize (tests/test_smoke_fixture_contract.sh), kept
 * in one place so a new client cannot be isolated in the smoke scripts and
 * forgotten here. A test that exercises one of these sets it after setup. */
static const char *const tf_client_home_overrides[] = {
    "CLAUDE_CONFIG_DIR",
    "CODEX_HOME",
    "KIRO_HOME",
    "HERMES_HOME",
    "QWEN_HOME",
    "CLINE_DATA_DIR",
    "OPENCLAW_HOME",
    "OPENCLAW_STATE_DIR",
    "OPENCLAW_PROFILE",
    "OPENCLAW_CONFIG_PATH",
    "OPENCLAW_WORKSPACE_DIR",
    "OPENCODE_CONFIG",
    "OPENCODE_CONFIG_DIR",
    "COPILOT_HOME",
    "CRUSH_GLOBAL_CONFIG",
    "VIBE_HOME",
    "GLAB_CONFIG_DIR",
    "KIMI_CODE_HOME",
    "CBM_CONTINUE_CONFIG_PATH",
    "CBM_TRAE_CONFIG_PATH",
    "CBM_ROO_CONFIG_PATH",
    "CBM_CODY_CONFIG_PATH",
    "OMP_PROFILE",
    "PI_CODING_AGENT_DIR",
};

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
    for (size_t i = 0U; i < sizeof(tf_client_home_overrides) / sizeof(tf_client_home_overrides[0]);
         i++) {
        cbm_unsetenv(tf_client_home_overrides[i]);
    }
    atexit(tf_cleanup_cache_sentinel);
    /* HOME and CBM_CACHE_DIR do not move the daemon rendezvous: its key is a
     * constant product hash under the system temp root. Without this, an
     * install/uninstall/update test or any endpoint resolved with no explicit
     * parent reaches the developer's live daemon. The run always gets a fresh
     * private directory; an inherited value is replaced, never reused (the
     * rule scripts/test-runtime.sh applies to the shell harness).
     * th_secure_runtime_parent_new anchors it where the IPC ancestry check
     * accepts it (LocalAppData on Windows). */
    if (!th_secure_runtime_parent_new(tf_runtime_sentinel, sizeof(tf_runtime_sentinel), "run")) {
        tf_runtime_sentinel[0] = '\0';
        return false;
    }
    if (cbm_setenv("CBM_RUNTIME_DIR", tf_runtime_sentinel, 1) != 0) {
        return false;
    }
#ifdef CBM_ENABLE_TEST_SEAMS
    /* Belt and braces: a fixture that later drops the variable gets a loud
     * refusal instead of the default rendezvous. */
    cbm_daemon_bootstrap_forbid_default_runtime_for_test(true);
#endif
    return true;
}

static bool tf_path_has_parent(const char *path, const char *parent) {
    size_t length = parent ? strlen(parent) : 0;
    return path && length > 0 && strncmp(path, parent, length) == 0 &&
           (path[length] == '/' || path[length] == '\\');
}

/* The daemon rendezvous is not derived from HOME or the cache: its key is a
 * constant product hash and its default parent is the system temp root. With
 * CBM_RUNTIME_DIR unset, every endpoint a test resolves is therefore the
 * developer's live daemon socket (<tmp>/cbm-daemon-<uid>/cbm-<key>.sock).
 * Every check runs before anything is resolved, so a run without isolation
 * fails here and this test never reaches that socket itself. */
TEST(runner_isolation_daemon_runtime_dir_is_private) {
    char runtime_env[sizeof(tf_runtime_sentinel)] = {0};
    const char *value = cbm_safe_getenv("CBM_RUNTIME_DIR", runtime_env, sizeof(runtime_env), NULL);
    ASSERT_NOT_NULL(value);
    ASSERT_TRUE(value[0] != '\0');
    /* The fresh per-run directory, never a value inherited from the caller. */
    ASSERT_STR_EQ(value, tf_runtime_sentinel);

    const char *root = th_secure_runtime_base();
    ASSERT_NOT_NULL(root);
    char canonical_root[CBM_SZ_4K] = {0};
    char canonical_run[CBM_SZ_4K] = {0};
    ASSERT_TRUE(cbm_canonical_path(root, canonical_root, sizeof(canonical_root)) != 0);
    ASSERT_TRUE(cbm_canonical_path(value, canonical_run, sizeof(canonical_run)) != 0);
    /* Strictly below the temp root: the root itself is the default rendezvous
     * parent (/tmp on POSIX, LocalAppData on Windows). */
    ASSERT_TRUE(tf_path_has_parent(canonical_run, canonical_root));

    /* Only now resolve through the product resolver. A NULL parent is what
     * every product call site passes: daemon, MCP client, CLI, activation. */
    cbm_daemon_ipc_endpoint_t *endpoint = cbm_daemon_bootstrap_endpoint_new(NULL);
    const char *resolved = endpoint ? cbm_daemon_ipc_endpoint_runtime_dir(endpoint) : NULL;
    char canonical_resolved[CBM_SZ_4K] = {0};
    bool resolved_ok = resolved && cbm_canonical_path(resolved, canonical_resolved,
                                                      sizeof(canonical_resolved)) != 0;
    cbm_daemon_ipc_endpoint_free(endpoint);
    ASSERT_TRUE(resolved_ok);
    ASSERT_TRUE(tf_path_has_parent(canonical_resolved, canonical_run));
#ifndef _WIN32
    char live_default[CBM_SZ_4K];
    (void)snprintf(live_default, sizeof(live_default), "%s/cbm-daemon-%lu", canonical_root,
                   (unsigned long)geteuid());
    ASSERT_TRUE(strcmp(canonical_resolved, live_default) != 0);
#endif
    PASS();
}

#ifdef CBM_ENABLE_TEST_SEAMS
/* The runner arms the bootstrap guard, so a fixture that drops the variable
 * gets a refusal instead of the default rendezvous. Only the decision is
 * exercised: no endpoint is ever built while the variable is absent. */
TEST(runner_isolation_refuses_default_daemon_runtime) {
    ASSERT_TRUE(tf_runtime_sentinel[0] != '\0');
    bool allowed_with_run_dir = !cbm_daemon_bootstrap_default_runtime_refused_for_test(NULL);

    (void)cbm_unsetenv("CBM_RUNTIME_DIR");
    bool refused_without = cbm_daemon_bootstrap_default_runtime_refused_for_test(NULL);
    bool explicit_allowed =
        !cbm_daemon_bootstrap_default_runtime_refused_for_test(tf_runtime_sentinel);
    /* Restore before asserting: a failed assertion returns immediately. */
    bool restored = cbm_setenv("CBM_RUNTIME_DIR", tf_runtime_sentinel, 1) == 0;

    ASSERT_TRUE(restored);
    ASSERT_TRUE(allowed_with_run_dir);
    ASSERT_TRUE(refused_without);
    ASSERT_TRUE(explicit_allowed);
    PASS();
}
#endif

SUITE(runner_isolation) {
    RUN_TEST(runner_isolation_daemon_runtime_dir_is_private);
#ifdef CBM_ENABLE_TEST_SEAMS
    RUN_TEST(runner_isolation_refuses_default_daemon_runtime);
#endif
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
    if (strstr(args_json, "\"heartbeat\"")) {
        FILE *response = response_out ? cbm_fopen(response_out, "wb") : NULL;
        if (response) {
            (void)fputs("{\"probe\":\"heartbeat\"}", response);
            (void)fclose(response);
        }
        cbm_log_info("pipeline.discover", "files", "1");
        (void)fprintf(stderr, "async worker heartbeat probe ready\n");
        fflush(NULL);
        _Exit(response ? 0 : 1);
    }
    if (strstr(args_json, "\"silent-exit\"")) {
        /* #1300: reach a phase, then exit 0 without writing the response. The
         * trailing plain-text line must not be mistaken for a phase. */
        cbm_log_info("incremental.edge_snapshot", "captured", "3");
        (void)fprintf(stderr, "async worker silent-exit probe\n");
        fflush(NULL);
        _Exit(0);
    }
    if (strstr(args_json, "\"crash\"")) {
        (void)fprintf(stderr, "async worker crash probe\n");
        fflush(NULL);
        abort();
    }
    if (strstr(args_json, "\"buffered-kill\"")) {
        /* The 0-byte-worker-log repro. tf_maybe_run_index_worker has already
         * put stderr into the FULL buffering a redirected stderr gets from the
         * Windows CRT (see there), so this line only reaches the log if the
         * production worker-log entry made the stream crash-durable.
         *
         * Then die the way the reports die. NOT abort(): Darwin's abort() runs
         * the stdio cleanup handler, so it flushes the very buffer this probe
         * exists to strand — under abort the reverted build still produced a
         * populated log and the repro was silently toothless. SIGKILL cannot be
         * caught, blocked or handled, so no cleanup of any kind runs. It is
         * also literally #1070's death (`signal=9`) and how #1130's hung worker
         * is terminated. */
        cbm_log_info("index.worker.buffered_kill_probe", "phase", "before_kill");
#ifdef _WIN32
        TerminateProcess(GetCurrentProcess(), 9);
#else
        (void)raise(SIGKILL);
#endif
        _Exit(2); /* unreachable: neither primitive returns */
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

    /* WHY force full buffering: on POSIX stderr is unbuffered by default, so the
     * 0-byte worker log of #1070/#1130/#1132/#1133/#1145/#1450 is invisible on
     * two thirds of the ladder — the Windows CRT is what gives a redirected
     * stderr FULL buffering. Starting the probe from the Windows default makes
     * the crash-durability contract testable identically on every OS we own,
     * instead of a Windows-only claim nobody can run locally. Scoped to the one
     * probe that asserts it, and set before the production entry below, which is
     * the code under test. */
    static char tf_worker_forced_buffer[CBM_SZ_4K];
    if (invocation.args_json && strstr(invocation.args_json, "\"buffered-kill\"")) {
        (void)setvbuf(stderr, tf_worker_forced_buffer, _IOFBF, sizeof(tf_worker_forced_buffer));
    }
    /* Mirror the production worker entry (run_cli's caller in main.c): the log
     * header is the first thing a worker records. */
    char *worker_repo_path = cbm_mcp_get_string_arg(invocation.args_json, "repo_path");
    cbm_index_worker_log_begin(invocation.args_json, worker_repo_path);
    free(worker_repo_path);
    cbm_index_set_worker_role_options(true, invocation.response_out, invocation.single_thread,
                                      invocation.marker_file, invocation.quarantine_file,
                                      invocation.memory_budget_bytes);
    cbm_mem_init_with_cap(0.5, invocation.memory_budget_bytes);
    cbm_log_init_for_process(false, true);
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
    /* POSIX copied-image holder: block reading stdin until the parent closes
     * the release pipe, exactly like the cat(1) donor this replaced. A system
     * utility cannot serve as the copied image — a multi-call coreutils
     * binary (uutils cat) refuses to execute under the copied name. */
    if (argc != 2 || strcmp(argv[1], "__cbm_runtime_image_holder") != 0) {
        return -1;
    }
    char release[16];
    ssize_t count;
    do {
        count = read(STDIN_FILENO, release, sizeof(release));
    } while (count > 0 || (count < 0 && errno == EINTR));
    return count == 0 ? 0 : 25;
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

static int tf_maybe_run_deleted_self_probe(int argc, char **argv) {
#if defined(__linux__) || defined(__APPLE__)
    if (argc != 5 || strcmp(argv[1], "__cbm_deleted_self_probe") != 0) {
        return -1;
    }
    int ready_fd = atoi(argv[2]);
    int continue_fd = atoi(argv[3]);
    cbm_http_server_set_binary_path(argv[4]);
    if (write(ready_fd, "R", 1) != 1) {
        return 41;
    }
    char go = '\0';
    if (read(continue_fd, &go, 1) != 1) {
        return 42;
    }
    char resolved[1024];
    bool ok = cbm_http_server_resolve_binary_path(NULL, resolved, sizeof(resolved));
#if defined(__linux__)
    /* Contract (#1204 strategy ruling): after a rename-over, the resolver
     * hands back the /proc/self/exe magic link — the in-memory OLD build,
     * the only spawn the worker's build-fingerprint gate accepts. First
     * prove we really are in the deleted state, or the assertions below
     * would pass vacuously on an intact image. */
    char link_target[1024];
    ssize_t n = readlink("/proc/self/exe", link_target, sizeof(link_target) - 1);
    if (n <= 0) {
        return 45;
    }
    link_target[n] = '\0';
    if (strstr(link_target, " (deleted)") == NULL) {
        return 46;
    }
    if (!ok) {
        return 43;
    }
    return strcmp(resolved, "/proc/self/exe") == 0 && access(resolved, X_OK) == 0 ? 0 : 44;
#else
    /* macOS has no magic link: the ruling is fail-closed. Success here is
     * the resolver REFUSING, so the supervisor logs no_self_path instead of
     * spawning a missing or mismatched binary. */
    return ok ? 44 : 0;
#endif
#else
    (void)argc;
    (void)argv;
    return -1;
#endif
}

/* ── Per-test selection: CBM_TEST_ONLY / CBM_TEST_ONLY_FILE ──────────
 * The test-impact selector names whole suites as `suite` and individual tests
 * as `suite:test`. CBM_TEST_ONLY accepts comma-separated tokens; the file named
 * by CBM_TEST_ONLY_FILE accepts one per line, with blank lines and # comments.
 * Both may be set; the selection is their union.
 * Argv keeps selecting suites and the tokens narrow inside that selection, so
 * the parallel harness can hand every per-suite process the same list.
 *
 * Tokens are strict. After the run, one that names a suite this build does not
 * register, or a test its suite never ran, fails the run by name: a selection
 * that quietly ran less than it named would be a green for work never done. A
 * token for a registered suite that argv left out belongs to another process
 * and is not this one to judge. */
typedef enum {
    TF_ONLY_UNSEEN = 0, /* no registered suite carries the suite name */
    TF_ONLY_REGISTERED, /* the suite exists; argv did not ask this process for it */
    TF_ONLY_ENTERED,    /* the suite ran and no RUN_TEST carried the test name */
    TF_ONLY_MATCHED,
} tf_only_state_t;

typedef struct {
    char *suite;      /* one allocation holding "suite\0test" or "suite\0" */
    const char *test; /* empty names a whole suite; otherwise points past the colon */
    tf_only_state_t state;
} tf_only_token_t;

static tf_only_token_t *tf_only_tokens = NULL;
static size_t tf_only_count = 0;
static size_t tf_only_cap = 0;
static bool tf_only_active = false;
/* Tokens are sorted by suite, so the running suite has one run [first, end). */
static size_t tf_only_suite_first = 0;
static size_t tf_only_suite_end = 0;

const char *tf_current_suite = NULL;

static bool tf_only_name_valid(const char *name, size_t length) {
    for (size_t i = 0; i < length; i++) {
        if (!isalnum((unsigned char)name[i]) && name[i] != '_') {
            return false;
        }
    }
    return length > 0;
}

/* Adds the token in text[0..length). Surrounding whitespace is not part of it
 * (the CR of a CRLF file included), and an empty span is not a token. */
static bool tf_only_add(const char *text, size_t length) {
    while (length > 0 && isspace((unsigned char)text[0])) {
        text++;
        length--;
    }
    while (length > 0 && isspace((unsigned char)text[length - 1])) {
        length--;
    }
    if (length == 0) {
        return true;
    }
    /* No test or suite name comes near this; it also bounds the copy below. */
    const char *colon = length <= CBM_TEST_SELECTION_TOKEN_MAX ? memchr(text, ':', length) : NULL;
    size_t suite_length = colon ? (size_t)(colon - text) : length;
    if (length > CBM_TEST_SELECTION_TOKEN_MAX || !tf_only_name_valid(text, suite_length) ||
        (colon && !tf_only_name_valid(colon + 1, length - suite_length - 1))) {
        fprintf(stderr, "malformed test selection token: %.*s (expected suite or suite:test)\n",
                (int)length, text);
        return false;
    }
    if (tf_only_count == tf_only_cap) {
        size_t cap = tf_only_cap ? tf_only_cap * 2 : CBM_SZ_64;
        tf_only_token_t *grown = realloc(tf_only_tokens, cap * sizeof(*grown));
        if (!grown) {
            fprintf(stderr, "Failed to allocate the test selection\n");
            return false;
        }
        tf_only_tokens = grown;
        tf_only_cap = cap;
    }
    char *copy = malloc(length + 1);
    if (!copy) {
        fprintf(stderr, "Failed to allocate the test selection\n");
        return false;
    }
    memcpy(copy, text, length);
    copy[suite_length] = '\0';
    copy[length] = '\0';
    tf_only_tokens[tf_only_count++] = (tf_only_token_t){
        .suite = copy, .test = copy + suite_length + (colon ? 1 : 0), .state = TF_ONLY_UNSEEN};
    return true;
}

static bool tf_only_add_list(const char *list) {
    for (const char *cursor = list;;) {
        const char *comma = strchr(cursor, ',');
        if (!tf_only_add(cursor, comma ? (size_t)(comma - cursor) : strlen(cursor))) {
            return false;
        }
        if (!comma) {
            return true;
        }
        cursor = comma + 1;
    }
}

static bool tf_only_add_file(const char *path) {
    FILE *file = cbm_fopen(path, "r");
    if (!file) {
        fprintf(stderr, "cannot read test selection file: %s\n", path);
        return false;
    }
    char line[CBM_SZ_1K];
    bool ok = true;
    while (ok && fgets(line, sizeof(line), file)) {
        size_t length = strlen(line);
        if (length == sizeof(line) - 1 && line[length - 1] != '\n') {
            /* The rest of the line would be read back as a token of its own. */
            fprintf(stderr, "test selection file line too long: %s\n", path);
            ok = false;
        } else {
            ok = tf_only_add(line, strcspn(line, "#\n"));
        }
    }
    if (ok && ferror(file)) {
        fprintf(stderr, "cannot read test selection file: %s\n", path);
        ok = false;
    }
    (void)fclose(file);
    return ok;
}

static int tf_only_compare(const void *left, const void *right) {
    const tf_only_token_t *a = left;
    const tf_only_token_t *b = right;
    int by_suite = strcmp(a->suite, b->suite);
    return by_suite != 0 ? by_suite : strcmp(a->test, b->test);
}

static void tf_only_free(void) {
    for (size_t i = 0; i < tf_only_count; i++) {
        free(tf_only_tokens[i].suite);
    }
    free(tf_only_tokens);
    tf_only_tokens = NULL;
    tf_only_count = 0;
    tf_only_cap = 0;
    tf_only_active = false;
}

/* Reads the selection once. False means the selection itself is unusable: the
 * caller stops before any suite, because falling back to "everything" or to
 * "nothing" would decide a gate by accident. An empty variable is unset, as it
 * is in a shell; a selection that is set and names no test is an error, since
 * a caller who wants every test leaves both variables unset. */
static bool tf_only_init(void) {
    const char *list = getenv("CBM_TEST_ONLY");
    const char *path = getenv("CBM_TEST_ONLY_FILE");
    bool has_list = list && list[0];
    bool has_path = path && path[0];
    if (!has_list && !has_path) {
        return true;
    }
    if ((has_list && !tf_only_add_list(list)) || (has_path && !tf_only_add_file(path))) {
        return false;
    }
    if (tf_only_count == 0) {
        fprintf(stderr, "test selection names no test (CBM_TEST_ONLY, CBM_TEST_ONLY_FILE)\n");
        return false;
    }
    qsort(tf_only_tokens, tf_only_count, sizeof(*tf_only_tokens), tf_only_compare);
    tf_only_active = true;
    return true;
}

/* Returns the first token of `suite` and stores one past its last. */
static size_t tf_only_suite_run(const char *suite, size_t *end_out) {
    size_t first = 0;
    while (first < tf_only_count && strcmp(tf_only_tokens[first].suite, suite) != 0) {
        first++;
    }
    size_t end = first;
    while (end < tf_only_count && strcmp(tf_only_tokens[end].suite, suite) == 0) {
        end++;
    }
    *end_out = end;
    return first;
}

static void tf_only_raise(size_t first, size_t end, tf_only_state_t state) {
    for (size_t i = first; i < end; i++) {
        if (tf_only_tokens[i].state < state) {
            tf_only_tokens[i].state = state;
        }
    }
}

bool tf_suite_enter(const char *suite) {
    if (tf_current_suite) {
        /* A suite run from inside another: its tests belong to the registered
         * suite that runs it, which is the name a token and argv both use. */
        return true;
    }
    if (tf_only_active) {
        tf_only_suite_first = tf_only_suite_run(suite, &tf_only_suite_end);
        if (tf_only_suite_first == tf_only_suite_end) {
            return false; /* no token names it: its body, setup included, never runs */
        }
        tf_only_raise(tf_only_suite_first, tf_only_suite_end, TF_ONLY_ENTERED);
        for (size_t i = tf_only_suite_first; i < tf_only_suite_end; i++) {
            if (!tf_only_tokens[i].test[0]) {
                /* Entering fulfills a whole-suite token even when it has no tests. */
                tf_only_tokens[i].state = TF_ONLY_MATCHED;
            }
        }
    }
    tf_current_suite = suite;
    return true;
}

void tf_suite_leave(const char *suite) {
    if (tf_current_suite && strcmp(tf_current_suite, suite) == 0) {
        tf_current_suite = NULL;
        tf_only_suite_first = 0;
        tf_only_suite_end = 0;
    }
}

bool tf_test_selected(const char *test) {
    if (!tf_only_active) {
        return true;
    }
    bool selected = false;
    for (size_t i = tf_only_suite_first; i < tf_only_suite_end; i++) {
        if (!tf_only_tokens[i].test[0] || strcmp(tf_only_tokens[i].test, test) == 0) {
            tf_only_tokens[i].state = TF_ONLY_MATCHED;
            selected = true;
        }
    }
    if (!selected) {
        tf_deselected_count++;
    }
    return selected;
}

/* The strict half of the selection: every token this process was responsible
 * for must have entered its whole suite or matched its explicit test. */
static void tf_only_report_unmatched(void) {
    for (size_t i = 0; i < tf_only_count; i++) {
        const tf_only_token_t *token = &tf_only_tokens[i];
        if (token->state != TF_ONLY_UNSEEN && token->state != TF_ONLY_ENTERED) {
            continue;
        }
        if (i > 0 && tf_only_compare(token, &tf_only_tokens[i - 1]) == 0) {
            continue; /* named twice, reported once */
        }
        fprintf(stderr, "selected test not compiled into this build or unknown: %s%s%s\n",
                token->suite, token->test[0] ? ":" : "", token->test);
        tf_fail_count++;
    }
}

#ifdef CBM_TEST_COVERAGE
/* ── Per-test coverage profiles ──────────────────────────────────────
 * Compiled only into the instrumented runner (make test-runner-cov: clang
 * source coverage, no sanitizer). With CBM_TEST_COVERAGE_DIR set, every test
 * leaves its own raw profiles under <dir>/<suite>/:
 *   <test>.parent.profraw       what this process executed inside the test
 *   <test>.<pid>.profraw        one per child the test spawned. Children
 *                               inherit LLVM_PROFILE_FILE, so a re-exec of this
 *                               runner or a product binary names its own file
 *   <test>.<pid>.forked         an unaccounted forked child: the coverage of
 *                               this test remains incomplete
 *   _setup.<n>.profraw          what ran between tests: suite setup, and the
 *                               prologue and epilogue of the runner itself
 *   _setup.child.<pid>.profraw  children spawned between tests
 * The first two are the coverage of the test; the others belong to no test. */
void __llvm_profile_set_filename(const char *pattern);
int __llvm_profile_write_file(void);
void __llvm_profile_reset_counters(void);

enum { TF_COVERAGE_PATH_CAP = CBM_SZ_4K + CBM_SZ_1K };
static char tf_coverage_dir[CBM_SZ_4K]; /* canonical; empty = not a coverage run */
/* The runtime keeps the pointer it is handed, not a copy: these outlive it. */
static char tf_coverage_profile[TF_COVERAGE_PATH_CAP];
static char tf_coverage_child_pattern[TF_COVERAGE_PATH_CAP];
static unsigned int tf_coverage_setup_index = 0;
/* Keep the first failed interval named and unreset for the rest of the run. */
static bool tf_coverage_write_failed = false;
/* This process started with a `%c` profile: its counters are mapped onto the
 * file, and it neither names nor writes a profile itself (see Children). */
static bool tf_coverage_continuous = false;

#ifndef _WIN32
/* Negative evidence only: these private pipe ends must retain exclusive custody.
 * Identity/flag checks reject detectable damage, not arbitrary aliasing/history.
 * Supported children keep BOTH inherited ends; parent disposal cannot remove
 * their last reader. No reader drains this pipe, even after failure is latched. */
static struct {
    int fd[2];
    struct stat identity[2];
    pid_t owner;
    bool started;
    bool active;
    bool failed;
    bool reported;
    bool transport_broken; /* child-local unsupported delivery failure */
} tf_coverage_channel = {.fd = {-1, -1}};
static bool tf_coverage_atfork_attempted;
static int tf_coverage_atfork_status = -1;

static int tf_coverage_channel_flags(int fd, int command) {
    int result;
    do {
        result = fcntl(fd, command);
    } while (result < 0 && errno == EINTR);
    return result;
}

static bool tf_coverage_channel_end(int index, bool require_flags) {
    int fd = tf_coverage_channel.fd[index];
    struct stat current;
    if (fd < 0) {
        return false;
    }
    int status;
    do {
        status = fstat(fd, &current);
    } while (status < 0 && errno == EINTR);
    if (status != 0 || !S_ISFIFO(current.st_mode) ||
        current.st_dev != tf_coverage_channel.identity[index].st_dev ||
        current.st_ino != tf_coverage_channel.identity[index].st_ino) {
        return false;
    }
    int flags = tf_coverage_channel_flags(fd, F_GETFL);
    if (flags < 0 || (flags & O_ACCMODE) != (index == 0 ? O_RDONLY : O_WRONLY)) {
        return false;
    }
    if (!require_flags) {
        return true;
    }
    int descriptor_flags = tf_coverage_channel_flags(fd, F_GETFD);
    return (flags & O_NONBLOCK) != 0 && descriptor_flags >= 0 &&
           (descriptor_flags & FD_CLOEXEC) != 0;
}

static bool tf_coverage_channel_intact(void) {
    return tf_coverage_channel.fd[0] >= 0 && tf_coverage_channel.fd[1] >= 0 &&
           tf_coverage_channel.fd[0] != tf_coverage_channel.fd[1] &&
           tf_coverage_channel_end(0, true) && tf_coverage_channel_end(1, true);
}

static void tf_coverage_channel_fail(const char *reason) {
    if (getpid() != tf_coverage_channel.owner) {
        return;
    }
    tf_coverage_channel.failed = true;
    if (!tf_coverage_channel.reported) {
        tf_coverage_channel.reported = true;
        fprintf(stderr, "coverage child accounting failed: owner_pid=%ld reason=%s\n",
                (long)tf_coverage_channel.owner, reason);
        tf_fail_count++;
    }
}

static void tf_coverage_channel_observe(void) {
    if (!tf_coverage_channel.active || getpid() != tf_coverage_channel.owner) {
        return;
    }
    if (!tf_coverage_channel_intact()) {
        tf_coverage_channel_fail("channel-invalid");
        return;
    }
    struct pollfd event = {.fd = tf_coverage_channel.fd[0], .events = POLLIN};
    int ready;
    do {
        ready = poll(&event, 1, 0);
    } while (ready < 0 && errno == EINTR);
    if (ready < 0 || (ready > 0 && (event.revents & (POLLERR | POLLHUP | POLLNVAL)))) {
        tf_coverage_channel_fail("observation");
    } else if (ready > 0) {
        tf_coverage_channel_fail((event.revents & POLLIN) ? "child-marker" : "observation");
    }
}

static bool tf_coverage_channel_blocked(void) {
    return getpid() == tf_coverage_channel.owner && tf_coverage_channel.failed;
}

static bool tf_coverage_channel_prepare_end(int index) {
    int fd = tf_coverage_channel.fd[index];
    if (fstat(fd, &tf_coverage_channel.identity[index]) != 0 ||
        !S_ISFIFO(tf_coverage_channel.identity[index].st_mode)) {
        return false;
    }
    int flags = fcntl(fd, F_GETFL);
    int descriptor_flags = fcntl(fd, F_GETFD);
    return flags >= 0 && descriptor_flags >= 0 && fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0 &&
           fcntl(fd, F_SETFD, descriptor_flags | FD_CLOEXEC) == 0;
}

/* Only the single-threaded setup path uses this: both obtained ends are still
 * exclusively owned even if preparing their identity/flags failed. */
static bool tf_coverage_channel_abort_setup(void) {
    bool closed = true;
    for (int i = 0; i < 2; i++) {
        int fd = tf_coverage_channel.fd[i];
        tf_coverage_channel.fd[i] = -1;
        if (fd >= 0 && close(fd) != 0) {
            closed = false;
        }
    }
    return closed;
}

static bool tf_coverage_channel_unavailable(const char *reason) {
    tf_coverage_channel.failed = true;
    fprintf(stderr, "coverage child accounting unavailable: reason=%s\n", reason);
    return false;
}

static bool tf_coverage_channel_activate(void) {
    if (tf_coverage_channel.started) {
        return tf_coverage_channel_unavailable("already-started");
    }
    tf_coverage_channel.started = true;
    tf_coverage_channel.owner = getpid();
    if (pipe(tf_coverage_channel.fd) != 0) {
        return tf_coverage_channel_unavailable("pipe");
    }
    if (!tf_coverage_channel_prepare_end(0) || !tf_coverage_channel_prepare_end(1) ||
        !tf_coverage_channel_intact()) {
        bool closed = tf_coverage_channel_abort_setup();
        return tf_coverage_channel_unavailable(closed ? "pipe-setup" : "pipe-setup-close");
    }
    tf_coverage_channel.active = true;
    return true;
}

static void tf_coverage_channel_finish(void) {
    if (!tf_coverage_channel.active || getpid() != tf_coverage_channel.owner) {
        return;
    }
    tf_coverage_channel_observe();
    tf_coverage_channel.active = false;
    if (tf_coverage_channel.fd[0] == tf_coverage_channel.fd[1]) {
        tf_coverage_channel_fail("channel-invalid");
        tf_coverage_channel.fd[0] = tf_coverage_channel.fd[1] = -1;
        return;
    }
    for (int i = 0; i < 2; i++) {
        int fd = tf_coverage_channel.fd[i];
        bool owned = tf_coverage_channel_end(i, false);
        tf_coverage_channel.fd[i] = -1;
        if (!owned) {
            /* Do not close a detectable replacement. Unknown ownership remains
             * for kernel process-exit cleanup, never a blind retry/close. */
            tf_coverage_channel_fail("channel-invalid");
        } else if (close(fd) != 0) {
            tf_coverage_channel_fail("channel-close");
        }
    }
}

/* Fixed storage and descriptor syscalls only in the post-fork failure path.
 * EAGAIN is evidence solely under intact, never-drained exclusive custody.
 * Unsupported transport loss cannot report itself magically to the parent. */
static void tf_coverage_channel_notify(void) {
    int saved_errno = errno;
    if (tf_coverage_channel.active && !tf_coverage_channel.transport_broken) {
        bool delivered = false;
        if (tf_coverage_channel_intact()) {
            const unsigned char byte = 1;
            ssize_t written;
            do {
                written = write(tf_coverage_channel.fd[1], &byte, sizeof(byte));
            } while (written < 0 && errno == EINTR);
            delivered = written == 1 || (written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK));
        }
        tf_coverage_channel.transport_broken = !delivered;
    }
    errno = saved_errno;
}
#endif /* !_WIN32 */

/* Names the profile this process writes next and the one its children will. */
static bool tf_coverage_route(const char *directory, const char *own, const char *child) {
    int own_length =
        snprintf(tf_coverage_profile, sizeof(tf_coverage_profile), "%s/%s.profraw", directory, own);
    /* `%c`: an exec'd child maps its counters onto the file (see Children). */
    int child_length = snprintf(tf_coverage_child_pattern, sizeof(tf_coverage_child_pattern),
                                "%s/%s.%%p%%c.profraw", directory, child);
    if (own_length <= 0 || (size_t)own_length >= sizeof(tf_coverage_profile) || child_length <= 0 ||
        (size_t)child_length >= sizeof(tf_coverage_child_pattern)) {
        return false;
    }
    __llvm_profile_set_filename(tf_coverage_profile);
    return cbm_setenv("LLVM_PROFILE_FILE", tf_coverage_child_pattern, 1) == 0;
}

/* Between tests the interval is suite setup, and so is any child it spawns. */
static bool tf_coverage_route_setup(const char *suite_dir) {
    char own[CBM_SZ_64];
    (void)snprintf(own, sizeof(own), "_setup.%u", tf_coverage_setup_index);
    return tf_coverage_route(suite_dir, own, "_setup.child");
}

/* Writes the current interval and resets counters only after a successful write. */
static bool tf_coverage_flush(void) {
    if (__llvm_profile_write_file() != 0) {
        tf_coverage_write_failed = true;
        fprintf(stderr, "coverage profile write failed: %s\n", tf_coverage_profile);
        tf_fail_count++;
        return false;
    }
    __llvm_profile_reset_counters();
    return true;
}

/* Check the current parent interval before choosing the runner status. Keep
 * counters and routing intact: the automatic exit writer still follows, and
 * this checked prefix does not certify its outcome or later callbacks. */
static void tf_coverage_finish(void) {
    if (!tf_coverage_dir[0] || tf_coverage_write_failed) {
        return;
    }
#ifndef _WIN32
    if (getpid() != tf_coverage_channel.owner || tf_coverage_channel_blocked()) {
        return;
    }
#endif
    if (__llvm_profile_write_file() != 0) {
        tf_coverage_write_failed = true;
        fprintf(stderr, "coverage profile write failed: %s\n", tf_coverage_profile);
        tf_fail_count++;
    }
}

static bool tf_coverage_suite_dir(char *out, size_t cap) {
    int length = snprintf(out, cap, "%s/%s", tf_coverage_dir,
                          tf_current_suite ? tf_current_suite : "_nosuite");
    return length > 0 && (size_t)length < cap && cbm_mkdir_p(out, 0755);
}

/* A coverage run that loses a profile would report the test as executing
 * nothing, and the impact map would trust that: fail the run instead. */
static void tf_coverage_lost(const char *test) {
    fprintf(stderr, "coverage profile could not be routed: %s:%s\n",
            tf_current_suite ? tf_current_suite : "_nosuite", test);
    tf_fail_count++;
}

void tf_coverage_test_begin(const char *test) {
#ifndef _WIN32
    tf_coverage_channel_observe();
    if (tf_coverage_channel_blocked()) {
        return;
    }
#endif
    char suite_dir[TF_COVERAGE_PATH_CAP];
    char own[CBM_SZ_512];
    if (!tf_coverage_dir[0] || tf_coverage_write_failed) {
        return;
    }
    int own_length = snprintf(own, sizeof(own), "%s.parent", test);
    if (own_length <= 0 || (size_t)own_length >= sizeof(own) ||
        !tf_coverage_suite_dir(suite_dir, sizeof(suite_dir)) ||
        !tf_coverage_route_setup(suite_dir)) {
        tf_coverage_lost(test);
        return;
    }
    if (!tf_coverage_flush()) {
        return;
    }
    tf_coverage_setup_index++;
    /* Named before the test runs, not after: a forked child that leaves through
     * exit() writes to the name it inherited, which must not be the setup file. */
    if (!tf_coverage_route(suite_dir, own, test)) {
        tf_coverage_lost(test);
    }
}

void tf_coverage_test_end(const char *test) {
#ifndef _WIN32
    tf_coverage_channel_observe();
    if (tf_coverage_channel_blocked()) {
        return;
    }
#endif
    char suite_dir[TF_COVERAGE_PATH_CAP];
    char own[CBM_SZ_512];
    if (!tf_coverage_dir[0] || tf_coverage_write_failed) {
        return;
    }
    int own_length = snprintf(own, sizeof(own), "%s.parent", test);
    if (own_length <= 0 || (size_t)own_length >= sizeof(own) ||
        !tf_coverage_suite_dir(suite_dir, sizeof(suite_dir)) ||
        !tf_coverage_route(suite_dir, own, test)) {
        tf_coverage_lost(test);
        return;
    }
    if (!tf_coverage_flush()) {
        return;
    }
    /* The runtime writes once more at exit, to whatever name is current. Left
     * on the file of this test, that write would replace the profile just
     * taken with the teardown that follows it. */
    if (!tf_coverage_route_setup(suite_dir)) {
        tf_coverage_lost(test);
    }
}

/* False when the run asked for coverage and cannot have it. Children run in
 * other working directories, so they are handed an absolute name. */
static bool tf_coverage_init(void) {
    const char *dir = getenv("CBM_TEST_COVERAGE_DIR");
    if (!dir || !dir[0]) {
        return true;
    }
#ifndef _WIN32
    /* A binary built for continuous profiles starts in that mode unless
     * LLVM_PROFILE_FILE names a plain file, and then ignores every name this
     * runner gives it: all tests would land in one default.profraw. */
    const char *initial = getenv("LLVM_PROFILE_FILE");
    if (!initial || !initial[0] || tf_coverage_continuous) {
        fprintf(stderr,
                "a coverage run starts with LLVM_PROFILE_FILE=/dev/null "
                "(got: %s)\n",
                initial && initial[0] ? initial : "unset");
        return false;
    }
    if (!tf_coverage_atfork_attempted || tf_coverage_atfork_status != 0) {
        fprintf(stderr, "coverage child accounting unavailable: reason=atfork code=%d\n",
                tf_coverage_atfork_status);
        return false;
    }
#endif
    if (!cbm_mkdir_p(dir, 0755) ||
        !cbm_canonical_path(dir, tf_coverage_dir, sizeof(tf_coverage_dir))) {
        tf_coverage_dir[0] = '\0';
        fprintf(stderr, "cannot use coverage directory: %s\n", dir);
        return false;
    }
#ifndef _WIN32
    if (!tf_coverage_channel_activate()) {
        tf_coverage_dir[0] = '\0';
        return false;
    }
#endif
    /* Routing sets the environment, which may move what `dir` points at. */
    if (!tf_coverage_route(tf_coverage_dir, "_runner.%p", "_runner.child")) {
        fprintf(stderr, "cannot route coverage profiles under: %s\n", tf_coverage_dir);
        tf_coverage_dir[0] = '\0';
#ifndef _WIN32
        tf_coverage_channel_finish();
#endif
        return false;
    }
    return true;
}

#ifndef _WIN32
/* ── Children ────────────────────────────────────────────────────────
 * A same-image, non-continuous fork gets its own profile and an obligation
 * marker. Explicit _exit/_Exit writes retire that marker only after the native
 * writer reports success. A failed write or signal leaves the marker behind.
 *
 * Normal exit retains its marker: delegating to libc does not establish the
 * outcome of the final atexit writer. Exec retains the original-image marker
 * too; a successor profile cannot establish that this image was accounted for.
 * A failed exec may return to this image and later finish with a successful
 * explicit write, which can then retire its marker.
 *
 * These wrappers establish retirement behavior only for markers that were
 * successfully placed. The private channel reports marker-open/close failures
 * for controlled same-image children retaining exclusive descriptor custody.
 * Continuous mode, successor images, libc-internal spawning and arbitrary
 * descendants remain separate obligations; marker absence certifies none. */
static char tf_coverage_fork_profile[TF_COVERAGE_PATH_CAP];
static char tf_coverage_fork_marker[TF_COVERAGE_PATH_CAP]; /* empty: not a counted fork */

static void tf_coverage_fork_marker_place(void) {
    if (tf_coverage_fork_marker[0]) {
        int fd = open(tf_coverage_fork_marker, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd < 0 || close(fd) != 0) {
            tf_coverage_channel_notify();
        }
    }
}

static void tf_coverage_fork_marker_lift(void) {
    if (tf_coverage_fork_marker[0]) {
        (void)unlink(tf_coverage_fork_marker);
    }
}

/* Runs in the child of every fork() of this image. Only what is safe between
 * fork and exec of a threaded process: no allocation, static buffers. */
static void tf_coverage_fork_child(void) {
    static const char tail[] = ".%p%c.profraw";
    tf_coverage_fork_marker[0] = '\0';
    if (tf_coverage_continuous) {
        return; /* the counters are mapped onto the file of the parent */
    }
    const char *pattern = getenv("LLVM_PROFILE_FILE");
    size_t length = pattern ? strlen(pattern) : 0;
    if (length <= sizeof(tail) - 1 || strcmp(pattern + length - (sizeof(tail) - 1), tail) != 0) {
        return; /* not a coverage run */
    }
    int stem = (int)(length - (sizeof(tail) - 1));
    long pid = (long)getpid();
    int profile_length = snprintf(tf_coverage_fork_profile, sizeof(tf_coverage_fork_profile),
                                  "%.*s.%ld.profraw", stem, pattern, pid);
    int marker_length = snprintf(tf_coverage_fork_marker, sizeof(tf_coverage_fork_marker),
                                 "%.*s.%ld.forked", stem, pattern, pid);
    if (profile_length <= 0 || (size_t)profile_length >= sizeof(tf_coverage_fork_profile) ||
        marker_length <= 0 || (size_t)marker_length >= sizeof(tf_coverage_fork_marker)) {
        tf_coverage_fork_marker[0] = '\0';
        return;
    }
    __llvm_profile_set_filename(tf_coverage_fork_profile);
    tf_coverage_fork_marker_place();
}

static void tf_coverage_exit_now(int status) __attribute__((noreturn));
static void tf_coverage_exit_now(int status) {
    if (!tf_coverage_continuous && __llvm_profile_write_file() == 0) {
        tf_coverage_fork_marker_lift();
    }
    void (*libc_exit)(int) = (void (*)(int))dlsym(RTLD_NEXT, "_exit");
    if (libc_exit) {
        libc_exit(status);
    }
    abort();
}

void _exit(int status) {
    tf_coverage_exit_now(status);
}

void _Exit(int status) {
    tf_coverage_exit_now(status);
}

void exit(int status) {
    void (*libc_exit)(int) = (void (*)(int))dlsym(RTLD_NEXT, "exit");
    if (libc_exit) {
        libc_exit(status);
    }
    abort();
}

/* Keep the original-image obligation across both successful and failed exec. */
int execve(const char *path, char *const argv[], char *const envp[]) {
    int (*libc_execve)(const char *, char *const[], char *const[]) =
        (int (*)(const char *, char *const[], char *const[]))dlsym(RTLD_NEXT, "execve");
    return libc_execve ? libc_execve(path, argv, envp) : -1;
}

int execv(const char *path, char *const argv[]) {
    int (*libc_execv)(const char *, char *const[]) =
        (int (*)(const char *, char *const[]))dlsym(RTLD_NEXT, "execv");
    return libc_execv ? libc_execv(path, argv) : -1;
}

int execvp(const char *file, char *const argv[]) {
    int (*libc_execvp)(const char *, char *const[]) =
        (int (*)(const char *, char *const[]))dlsym(RTLD_NEXT, "execvp");
    return libc_execvp ? libc_execvp(file, argv) : -1;
}

enum { TF_COVERAGE_EXECL_ARG_CAP = 64 };

int execl(const char *path, const char *arg0, ...) {
    char *argv[TF_COVERAGE_EXECL_ARG_CAP];
    int argc = 0;
    va_list args;
    va_start(args, arg0);
    for (const char *arg = arg0; arg != NULL && argc < TF_COVERAGE_EXECL_ARG_CAP - 1;
         arg = va_arg(args, const char *)) {
        argv[argc++] = (char *)arg;
    }
    va_end(args);
    argv[argc] = NULL;
    return execv(path, argv);
}
#endif /* !_WIN32 */

/* Runs first in every process of this image, role children included: they
 * fork too. Windows has neither fork nor an interposable _exit here. */
static void tf_coverage_process_init(void) {
#ifndef _WIN32
    /* Read before anything routes: tf_coverage_route puts a `%c` pattern into
     * the environment of a process that is not continuous itself. */
    const char *initial = getenv("LLVM_PROFILE_FILE");
    tf_coverage_continuous = initial != NULL && strstr(initial, "%c") != NULL;
    if (!tf_coverage_atfork_attempted) {
        tf_coverage_atfork_attempted = true;
        tf_coverage_atfork_status = pthread_atfork(NULL, NULL, tf_coverage_fork_child);
    }
#endif
}
#endif /* CBM_TEST_COVERAGE */

static int g_suite_argc = 0;
static char **g_suite_argv = NULL;
static bool *g_suite_arg_matched = NULL;

static bool suite_requested(const char *name) {
    /* Every registered suite passes through here, requested or not: this is
     * where a selection token learns that its suite exists in this build. */
    size_t token_end = 0;
    size_t token_first = tf_only_suite_run(name, &token_end);
    tf_only_raise(token_first, token_end, TF_ONLY_REGISTERED);
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
extern void suite_index_policy(void);
extern void suite_workspace(void);
extern void suite_platform(void);
extern void suite_diagnostics(void);
extern void suite_complexity(void);
extern void suite_subprocess(void);
extern void suite_private_file_lock(void);
extern void suite_lock_registry(void);
extern void suite_extraction(void);
extern void suite_callable_sig(void);
extern void suite_test_conventions(void);
extern void suite_extraction_inheritance(void);
extern void suite_extraction_imports(void);
extern void suite_parse_coverage(void);
extern void suite_grammar_regression(void);
extern void suite_grammar_labels(void);
extern void suite_grammar_imports(void);
extern void suite_ac(void);
extern void suite_store_nodes(void);
extern void suite_store_edges(void);
extern void suite_store_impact(void);
extern void suite_store_scope(void);
extern void suite_store_graph_digest(void);
extern void suite_test_impact(void);
extern void suite_test_impact_profiles(void);
extern void suite_test_impact_changes(void);
extern void suite_test_impact_origins(void);
extern void suite_test_impact_git(void);
extern void suite_test_impact_tree(void);
extern void suite_test_impact_tree_read(void);
extern void suite_test_impact_inventory(void);
extern void suite_test_impact_classify(void);
extern void suite_test_impact_source(void);
extern void suite_test_impact_seed(void);
extern void suite_test_impact_engine(void);
extern void suite_test_impact_artifact(void);
extern void suite_test_impact_runner_filter(void);
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
extern void suite_gitignore_checked(void);
extern void suite_inventory_filter(void);
#if defined(CBM_TEST_COVERAGE) && !defined(_WIN32)
extern void suite_coverage_children(void);
extern void suite_coverage_setup_success(void);
extern void suite_coverage_setup_uncertain(void);
extern int tf_coverage_children_dispatch(int argc, char **argv);
extern void suite_coverage_parent_failure(void);
extern int tf_coverage_parent_failure_args(int *argc, char **argv);
extern void tf_coverage_parent_failure_finish(void);
#endif
extern void suite_git_context(void);
extern void suite_discover(void);
extern void suite_graph_buffer(void);
extern void suite_registry(void);
extern void suite_pipeline(void);
extern void suite_pipeline_conventions(void);
extern void suite_pipeline_frozen(void);
extern void suite_importance(void);
extern void suite_pipeline_semantic_manifest_repro(void);
extern void suite_cross_repo(void);
extern void suite_index_resilience(void);
extern void suite_index_format(void);
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
extern void suite_ruby_lsp(void);
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
extern void suite_doclinks(void);
extern void suite_infrascan(void);
extern void suite_cli(void);
extern void suite_agent_clients(void);
extern void suite_agent_profiles(void);
extern void suite_config_json_like(void);
extern void suite_config_toml_edit(void);
extern void suite_config_yaml_edit(void);
extern void suite_config_text_edit(void);
extern void suite_activation_transaction(void);
extern void suite_system_info(void);
extern void suite_worker_pool(void);
extern void suite_parallel(void);
extern void suite_call_reference_language_complex_contract(void);
extern void suite_repro_call_scope_usages(void);
extern void suite_repro_call_argument_usages(void);
extern void suite_repro_reference_precision(void);
extern void suite_repro_lexical_binding_precision(void);
extern void suite_repro_call_argument_matrix_a(void);
extern void suite_repro_call_argument_matrix_b(void);
extern void suite_repro_call_node_behaviors(void);
extern void suite_repro_language_registry(void);
extern void suite_repro_call_node_manifest(void);
extern void suite_repro_lsp_ordered_signatures(void);
extern void suite_repro_lsp_ordered_local(void);
extern void suite_repro_ts_overload_return_chains(void);
extern void suite_repro_harness_cleanup(void);
extern void suite_repro_runner_filter(void);
extern void suite_call_reference_contract(void);
extern void suite_mem(void);
extern void suite_mem_events(void);
extern void suite_ui(void);
extern void suite_httpd(void);
extern void suite_security(void);
extern void suite_yaml(void);
extern void suite_integration(void);
extern void suite_lang_contract(void);
extern void suite_conditional_variants(void);
extern void suite_spawns(void);
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

/* Native subprocess capture probe; implemented with its tests. */
extern int tf_maybe_run_subprocess_stdout_probe(int argc, char **argv);
extern void tf_test_impact_runner_filter_set_binary(const char *path);
extern int tf_maybe_run_git_facts_diff_probe(int argc, char **argv);

/* The runner's own image, for the tests that start or copy it again. argv[0]
 * is the caller's spelling, and a Windows parent that found the runner by
 * search (the native python scheduler, scripts/run-test-wave.py) passes it
 * without ".exe", which an exact spawn or copy of that path cannot open. */
const char *tf_runner_image(int argc, char **argv);
const char *tf_runner_image(int argc, char **argv) {
#ifdef _WIN32
    static char image[4096];
    if (!image[0]) {
        char *module = cbm_module_path_utf8();
        if (module && strlen(module) < sizeof(image)) {
            memcpy(image, module, strlen(module) + 1);
        }
        free(module);
    }
    if (image[0]) {
        return image;
    }
#endif
    return argc > 0 && argv ? argv[0] : NULL;
}

int main(int argc, char **argv) {
    /* #2003: never let a caller's GIT_DIR/GIT_INDEX_FILE/... redirect fixture
     * git commands at the caller's real repository. */
    th_clear_git_repo_env();
    tf_test_impact_runner_filter_set_binary(tf_runner_image(argc, argv));
#ifdef CBM_TEST_COVERAGE
    tf_coverage_process_init();
#ifndef _WIN32
    int coverage_child_rc = tf_coverage_children_dispatch(argc, argv);
    if (coverage_child_rc >= 0) {
        return coverage_child_rc;
    }
#endif
#endif
    int stdout_probe_rc = tf_maybe_run_subprocess_stdout_probe(argc, argv);
    if (stdout_probe_rc >= 0) {
        return stdout_probe_rc;
    }
    int git_facts_probe_rc = tf_maybe_run_git_facts_diff_probe(argc, argv);
    if (git_facts_probe_rc >= 0) {
        return git_facts_probe_rc;
    }
    int memory_limit_probe_rc = tf_maybe_run_windows_memory_limit_probe(argc, argv);
    if (memory_limit_probe_rc >= 0) {
        return memory_limit_probe_rc;
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
    if (argc == 2 && strcmp(argv[1], "--build-config") == 0) {
#ifdef _WIN32
        if (_setmode(cbm_fileno(stdout), _O_BINARY) == -1) {
            fprintf(stderr, "failed to set build-config stdout to binary mode\n");
            return 2;
        }
#endif
#if defined(CBM_SANITIZED_BUILD) && CBM_SANITIZED_BUILD
        const int sanitized = 1;
#else
        const int sanitized = 0;
#endif
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
        const int test_seams = 1;
#else
        const int test_seams = 0;
#endif
        (void)printf("sanitized=%d test_seams=%d\n", sanitized, test_seams);
        return 0;
    }
    /* A test's verdict is a pure function of code, test, platform and seed --
     * never of how full this machine's disk is. The spill store refuses to open
     * below 10 GB free, so without this pin every spill test failed, and every
     * budget-driven pipeline test silently took the no-store path, on a host
     * with less free space. Pin ample space for this process and every worker
     * it re-execs; the refusal itself is pinned on purpose by
     * extraction::extract_spill_refuses_below_the_free_disk_floor. */
    (void)cbm_result_spill_pin_free_bytes_for_tests((size_t)64 * 1024 * 1024 * 1024);
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
    /* #1830 userns smoke probe -- see the test that spawns it.
     *
     * The ancestor overflow uid is derived ONCE per process (pthread_once in
     * src/daemon/ipc.c) from /proc/self/uid_map. That file is not immutable:
     * unshare(CLONE_NEWUSER) is exactly what changes it, and pthread_once state
     * survives fork(). A forked child therefore keeps the HOST answer and never
     * re-derives inside its new namespace, so a fork-only smoke test refuses and
     * cannot pass once anything earlier in the suite has primed the cache --
     * seven call sites do. Re-exec into this probe so the decision is made by a
     * process that STARTED inside the namespace, which is the production shape
     * the test means to cover. */
#if defined(__linux__) && defined(CBM_ENABLE_TEST_SEAMS)
    if (argc == 3 && strcmp(argv[1], "--userns-secure-probe") == 0) {
        /* _exit, not return: this process exists to answer ONE boolean. A
         * return runs the atexit chain, and the runner is built with
         * -fsanitize=address, so a future leak anywhere in the prologue would
         * exit 23 and read as a security verdict on a test that has none. */
        _exit(cbm_daemon_ipc_private_directory_secure(argv[2]) ? 0 : 1);
    }
#endif
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
    int deleted_self_rc = tf_maybe_run_deleted_self_probe(argc, argv);
    if (deleted_self_rc >= 0) {
        return deleted_self_rc;
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
        fprintf(stderr, "failed to create isolated test cache and daemon runtime\n");
        return 2;
    }

#if defined(CBM_TEST_COVERAGE) && !defined(_WIN32)
    if (tf_coverage_parent_failure_args(&argc, argv) != 0) {
        return 2;
    }
#endif
    const char *skip_perf_env = getenv("CBM_SKIP_PERF");
    g_skip_perf = skip_perf_env != NULL && strcmp(skip_perf_env, "1") == 0;
    if (argc == 2 && strcmp(argv[1], "--list-suites") == 0) {
        g_list_only = true;
        g_suite_argc = 1; /* no suite-name args to match */
    } else {
        g_suite_argc = argc;
        g_suite_argv = argv;
    }
    if (!g_list_only && !tf_only_init()) {
        tf_only_free();
        return 2;
    }
#ifdef CBM_TEST_COVERAGE
    if (!g_list_only && !tf_coverage_init()) {
#ifndef _WIN32
        tf_coverage_channel_finish();
#endif
        tf_only_free();
        return 2;
    }
#endif
    if (g_suite_argc > 1) {
        g_suite_arg_matched = calloc((size_t)argc, sizeof(*g_suite_arg_matched));
        if (!g_suite_arg_matched) {
            fprintf(stderr, "Failed to allocate test-suite argument tracking\n");
#if defined(CBM_TEST_COVERAGE) && !defined(_WIN32)
            tf_coverage_channel_finish();
#endif
            return 1;
        }
    }
    if (!g_list_only) {
        printf("\n  codebase-memory-mcp  C test suite\n");
    }

    /* Runner isolation first: nothing below may reach the developer's daemon. */
    RUN_SELECTED_SUITE(runner_isolation);

    /* Foundation */
    RUN_SELECTED_SUITE(arena);
    RUN_SELECTED_SUITE(hash_table);
    RUN_SELECTED_SUITE(dyn_array);
    RUN_SELECTED_SUITE(str_intern);
    RUN_SELECTED_SUITE(log);
    RUN_SELECTED_SUITE(str_util);
    RUN_SELECTED_SUITE(index_policy);
    RUN_SELECTED_SUITE(workspace);
    RUN_SELECTED_SUITE(platform);
    RUN_SELECTED_SUITE(diagnostics);
    RUN_SELECTED_SUITE(complexity);
    RUN_SELECTED_SUITE(subprocess);
    RUN_SELECTED_SUITE(private_file_lock);
    RUN_SELECTED_SUITE(lock_registry);
    RUN_SELECTED_SUITE(dump_verify);

    /* Existing C code regression tests */
    RUN_SELECTED_SUITE(ac);
    RUN_SELECTED_SUITE(extraction);
    RUN_SELECTED_SUITE(callable_sig);
    RUN_SELECTED_SUITE(test_conventions);
    RUN_SELECTED_SUITE(extraction_inheritance);
    RUN_SELECTED_SUITE(extraction_imports);
    RUN_SELECTED_SUITE(parse_coverage);
    RUN_SELECTED_SUITE(grammar_regression);
    RUN_SELECTED_SUITE(grammar_labels);
    RUN_SELECTED_SUITE(grammar_imports);

    /* Store (M5) */
    RUN_SELECTED_SUITE(store_nodes);
    RUN_SELECTED_SUITE(store_edges);
    RUN_SELECTED_SUITE(store_impact);
    RUN_SELECTED_SUITE(store_scope);
    RUN_SELECTED_SUITE(store_graph_digest);
    RUN_SELECTED_SUITE(test_impact);
    RUN_SELECTED_SUITE(test_impact_profiles);
    RUN_SELECTED_SUITE(test_impact_changes);
    RUN_SELECTED_SUITE(test_impact_origins);
    RUN_SELECTED_SUITE(test_impact_git);
    RUN_SELECTED_SUITE(test_impact_tree);
    RUN_SELECTED_SUITE(test_impact_tree_read);
    RUN_SELECTED_SUITE(test_impact_inventory);
    RUN_SELECTED_SUITE(test_impact_classify);
    RUN_SELECTED_SUITE(test_impact_source);
    RUN_SELECTED_SUITE(test_impact_seed);
    RUN_SELECTED_SUITE(test_impact_engine);
    RUN_SELECTED_SUITE(test_impact_artifact);
    RUN_SELECTED_SUITE(test_impact_runner_filter);
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
    RUN_SELECTED_SUITE(gitignore_checked);
    RUN_SELECTED_SUITE(inventory_filter);
#if defined(CBM_TEST_COVERAGE) && !defined(_WIN32)
    RUN_SELECTED_SUITE(coverage_children);
    RUN_SELECTED_SUITE(coverage_setup_success);
    RUN_SELECTED_SUITE(coverage_setup_uncertain);
    RUN_SELECTED_SUITE(coverage_parent_failure);
#endif
    RUN_SELECTED_SUITE(git_context);
    RUN_SELECTED_SUITE(discover);

    /* Graph Buffer (M7) */
    RUN_SELECTED_SUITE(graph_buffer);

    /* Pipeline (M8) */
    RUN_SELECTED_SUITE(registry);
    RUN_SELECTED_SUITE(pipeline);
    RUN_SELECTED_SUITE(pipeline_conventions);
    RUN_SELECTED_SUITE(pipeline_frozen);
    RUN_SELECTED_SUITE(importance);
    RUN_SELECTED_SUITE(index_format);
    RUN_SELECTED_SUITE(pipeline_semantic_manifest_repro);
    RUN_SELECTED_SUITE(call_reference_contract);
    RUN_SELECTED_SUITE(call_reference_language_complex_contract);
    RUN_SELECTED_SUITE(repro_call_scope_usages);
    RUN_SELECTED_SUITE(repro_call_argument_usages);
    RUN_SELECTED_SUITE(repro_reference_precision);
    RUN_SELECTED_SUITE(repro_call_argument_matrix_a);
    RUN_SELECTED_SUITE(repro_call_argument_matrix_b);
    RUN_SELECTED_SUITE(repro_call_node_behaviors);
    RUN_SELECTED_SUITE(repro_language_registry);
    RUN_SELECTED_SUITE(repro_call_node_manifest);
    RUN_SELECTED_SUITE(repro_lsp_ordered_signatures);
    RUN_SELECTED_SUITE(repro_lsp_ordered_local);
    RUN_SELECTED_SUITE(repro_ts_overload_return_chains);
    RUN_SELECTED_SUITE(repro_harness_cleanup);
    RUN_SELECTED_SUITE(repro_runner_filter);
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
    RUN_SELECTED_SUITE(ruby_lsp);
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

    /* Markdown file reference link */
    RUN_SELECTED_SUITE(doclinks);

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

    /* System info + worker pool (parallelism) */
    RUN_SELECTED_SUITE(system_info);
    RUN_SELECTED_SUITE(worker_pool);

    /* Parallel pipeline */
    RUN_SELECTED_SUITE(parallel);

    /* Promoted lexical-resolution regression contracts. */
    RUN_SELECTED_SUITE(repro_lexical_binding_precision);

    /* mem + arena + slab integration */
    RUN_SELECTED_SUITE(slab_alloc);
    RUN_SELECTED_SUITE(mem);
    RUN_SELECTED_SUITE(mem_events);

    /* UI (config, external asset pack, layout) */
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
    RUN_SELECTED_SUITE(conditional_variants);
    RUN_SELECTED_SUITE(spawns);
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
#if defined(CBM_TEST_COVERAGE) && !defined(_WIN32)
    tf_coverage_parent_failure_finish();
#endif
    tf_only_report_unmatched();
    tf_only_free();
    free(g_suite_arg_matched);
    g_suite_arg_matched = NULL;

    /* Release process-lifetime caches so LeakSanitizer reports no leaks. */
    cbm_kind_in_set_free_cache();
    sqlite3_shutdown();
#if defined(CBM_TEST_COVERAGE) && !defined(_WIN32)
    tf_coverage_channel_finish();
#endif
#ifdef CBM_TEST_COVERAGE
    tf_coverage_finish();
#endif
    TEST_SUMMARY();
}
