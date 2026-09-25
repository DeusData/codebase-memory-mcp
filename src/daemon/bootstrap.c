/*
 * bootstrap.c — Mandatory per-account daemon startup policy.
 */
#include "daemon/bootstrap.h"

#include "daemon/ipc.h"
#include "daemon/service.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/log.h"
#include "foundation/platform.h"

#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include "foundation/subprocess.h"
#include "foundation/win_utf8.h"
#include <windows.h>
#else
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#ifdef __APPLE__
#include <pthread.h>
#include <spawn.h>
extern char **environ;
#endif
#endif

enum {
    BOOTSTRAP_RETRY_NS = 1000000,
    /* #1828: how often a waiting client re-reads the start-failure log
     * (the wait loop itself ticks every millisecond). */
    BOOTSTRAP_START_FAILURE_CHECK_MS = 50,
    /* A record this many seconds OLDER than the client's own spawn instant is
     * still accepted: a sibling client's attempt that just failed is the same
     * evidence, and second-granularity clocks need the slack anyway. */
    BOOTSTRAP_START_FAILURE_SKEW_S = 2,
    BOOTSTRAP_START_FAILURE_LOG_CAP = 65536,
    BOOTSTRAP_COORDINATION_CLEANUP_MS = 500,
    BOOTSTRAP_PATH_CAP = 4096,
};

static bool bootstrap_arg_is(const char *arg, const char *expected) {
    return arg && expected && strcmp(arg, expected) == 0;
}

static int bootstrap_find_arg(int argc, char *const argv[], const char *expected) {
    for (int i = 1; argv && i < argc; i++) {
        if (bootstrap_arg_is(argv[i], expected)) {
            return i;
        }
    }
    return -1;
}

static bool bootstrap_has_help_after(int argc, char *const argv[], int start) {
    for (int i = start; argv && i < argc; i++) {
        if (bootstrap_arg_is(argv[i], "--help") || bootstrap_arg_is(argv[i], "-h")) {
            return true;
        }
    }
    return false;
}

static bool bootstrap_worker_fingerprint_valid(const char *fingerprint) {
    if (!fingerprint || strlen(fingerprint) != 64U) {
        return false;
    }
    for (size_t i = 0; i < 64U; i++) {
        char ch = fingerprint[i];
        if (!((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f'))) {
            return false;
        }
    }
    return true;
}

static bool bootstrap_worker_budget_valid(const char *text) {
    if (!text || !text[0]) {
        return false;
    }
    size_t value = 0;
    for (const unsigned char *cursor = (const unsigned char *)text; *cursor; cursor++) {
        if (*cursor < '0' || *cursor > '9') {
            return false;
        }
        size_t digit = (size_t)(*cursor - '0');
        if (value > (SIZE_MAX - digit) / 10U) {
            return false;
        }
        value = value * 10U + digit;
    }
    return value > 0;
}

/* Keep the bootstrap role boundary exact and fail closed before any client or
 * worker state is initialized. index_supervisor owns the matching builder and
 * performs the captured-build comparison after this syntax-only classification. */
static bool bootstrap_worker_argv_exact(int argc, char *const argv[]) {
    if (argc < 9 || !argv || !bootstrap_arg_is(argv[1], "cli") ||
        !bootstrap_arg_is(argv[2], "--index-worker") ||
        !bootstrap_arg_is(argv[3], "--index-worker-build") ||
        !bootstrap_worker_fingerprint_valid(argv[4]) ||
        !bootstrap_arg_is(argv[5], "index_repository") || !argv[6] || !argv[6][0] ||
        !bootstrap_arg_is(argv[7], "--response-out") || !argv[8] || !argv[8][0]) {
        return false;
    }
    int next = 9;
    if (next < argc && bootstrap_arg_is(argv[next], "--index-worker-memory-budget-bytes")) {
        if (next + 1 >= argc || !bootstrap_worker_budget_valid(argv[next + 1])) {
            return false;
        }
        next += 2;
    }
    if (next < argc && bootstrap_arg_is(argv[next], "--index-worker-single-thread")) {
        next++;
    }
    if (next < argc && bootstrap_arg_is(argv[next], "--index-worker-marker")) {
        if (next + 1 >= argc || !argv[next + 1] || !argv[next + 1][0]) {
            return false;
        }
        next += 2;
    }
    if (next < argc && bootstrap_arg_is(argv[next], "--index-worker-quarantine")) {
        if (next + 1 >= argc || !argv[next + 1] || !argv[next + 1][0]) {
            return false;
        }
        next += 2;
    }
    return next == argc;
}

cbm_daemon_process_role_t cbm_daemon_process_role(int argc, char *const argv[]) {
    if (argc <= 0 || !argv || !argv[0] || argv[0][0] == '\0') {
        return CBM_DAEMON_PROCESS_INVALID;
    }

    int daemon_arg = bootstrap_find_arg(argc, argv, CBM_DAEMON_INTERNAL_ARG);
    if (daemon_arg >= 0) {
        /* Byte-exact daemon-role grammar, deliberately unforgiving: the bare
         * internal marker, or the marker followed by exactly the permanent
         * flag. Every other shape — reordered, repeated, or extended — is
         * INVALID, so argv smuggling cannot reach the daemon role. */
        if (argc == 2 && daemon_arg == 1) {
            return CBM_DAEMON_PROCESS_DAEMON;
        }
        if (argc == 3 && daemon_arg == 1 && bootstrap_arg_is(argv[2], CBM_DAEMON_PERMANENT_ARG)) {
            return CBM_DAEMON_PROCESS_DAEMON;
        }
        return CBM_DAEMON_PROCESS_INVALID;
    }

    int worker_arg = bootstrap_find_arg(argc, argv, "--index-worker");
    if (worker_arg >= 0) {
        return bootstrap_worker_argv_exact(argc, argv) ? CBM_DAEMON_PROCESS_WORKER
                                                       : CBM_DAEMON_PROCESS_INVALID;
    }

    static const char *const stateless_commands[] = {
        "install",
        "uninstall",
        "update",
        /* allow-root writes one line of user-level config and reads nothing from a
         * project, so it needs no daemon. Listed here rather than routed through
         * the daemon so enrolling a root cannot depend on daemon state. */
        "allow-root",
    };
    /* Stop at the first top-level mode token. Tool names, flag values, and JSON
     * following `cli` are opaque user input: a search query named "install"
     * or containing "--version" must never bypass the mandatory daemon. */
    for (int arg = 1; arg < argc; arg++) {
        if (bootstrap_arg_is(argv[arg], "cli")) {
            if (bootstrap_has_help_after(argc, argv, arg + 1)) {
                return CBM_DAEMON_PROCESS_STATELESS;
            }
            return CBM_DAEMON_PROCESS_LOCAL_CLI;
        }
        if (bootstrap_arg_is(argv[arg], "hook-augment")) {
            return CBM_DAEMON_PROCESS_HOOK_CLIENT;
        }
        if (bootstrap_arg_is(argv[arg], "config")) {
            return bootstrap_has_help_after(argc, argv, arg + 1) ? CBM_DAEMON_PROCESS_STATELESS
                                                                 : CBM_DAEMON_PROCESS_LOCAL_CLI;
        }
        /* Placed after the `cli` check on purpose: `cbm cli search "daemon
         * start"` is opaque tool input and must stay LOCAL_CLI. */
        if (bootstrap_arg_is(argv[arg], "daemon")) {
            return bootstrap_has_help_after(argc, argv, arg + 1) ? CBM_DAEMON_PROCESS_STATELESS
                                                                 : CBM_DAEMON_PROCESS_DAEMON_CTL;
        }
        if (bootstrap_arg_is(argv[arg], "--version") || bootstrap_arg_is(argv[arg], "--help") ||
            bootstrap_arg_is(argv[arg], "-h")) {
            return CBM_DAEMON_PROCESS_STATELESS;
        }
        for (size_t command = 0;
             command < sizeof(stateless_commands) / sizeof(stateless_commands[0]); command++) {
            if (bootstrap_arg_is(argv[arg], stateless_commands[command])) {
                return CBM_DAEMON_PROCESS_STATELESS;
            }
        }
    }
    return CBM_DAEMON_PROCESS_MCP_CLIENT;
}

bool cbm_daemon_process_role_requires_client(cbm_daemon_process_role_t role) {
    return role == CBM_DAEMON_PROCESS_MCP_CLIENT || role == CBM_DAEMON_PROCESS_HOOK_CLIENT;
}

/* #1574/#1621: the rendezvous directory is created under %LOCALAPPDATA%
 * (Windows) or /tmp — /private/tmp on macOS — and that ancestry is not always
 * acceptable to the private-directory walk. A profile that carries a
 * mutation-granting ACE for an untrusted identity (an AppContainer capability
 * SID, for instance) fails it, and the binary then cannot start at all: every
 * command needs this endpoint, `config list` included, so the operator cannot
 * even reconfigure their way out. The only relocation hook was
 * CBM_TEST_DAEMON_RUNTIME_PARENT, compiled out unless CBM_ENABLE_TEST_SEAMS is
 * defined, so a test build started while the shipped build did not. CBM_CACHE_DIR
 * does not help either — it moves the cache, never the rendezvous.
 *
 * CBM_RUNTIME_DIR does NOT relax the check. The directory it names goes through
 * exactly the same validation as the default; the operator only chooses an
 * ancestry that passes, and a value that fails is refused rather than ignored.
 * cbm_safe_getenv never truncates: a value too long for the buffer is reported
 * as absent, so no half a path can ever become a runtime parent. */
static const char *bootstrap_runtime_parent_override(char *buffer, size_t capacity) {
    const char *value = cbm_safe_getenv("CBM_RUNTIME_DIR", buffer, capacity, NULL);
    return value && value[0] != '\0' ? value : NULL;
}

cbm_daemon_ipc_endpoint_t *cbm_daemon_bootstrap_endpoint_new(const char *runtime_parent) {
    char key[CBM_DAEMON_KEY_SIZE];
    if (!cbm_daemon_rendezvous_key(key)) {
        return NULL;
    }
    /* An explicit parent keeps precedence: it carries the compile-time test
     * seam and the lifecycle guards' isolated namespace. The override is
     * resolved HERE, the one function every product endpoint goes through
     * (daemon, MCP client, local CLI, index worker, activation), so no call
     * site can silently keep the default. */
    char override_parent[BOOTSTRAP_PATH_CAP];
    const char *parent =
        runtime_parent
            ? runtime_parent
            : bootstrap_runtime_parent_override(override_parent, sizeof(override_parent));
    return cbm_daemon_ipc_endpoint_new(key, parent);
}

bool cbm_daemon_bootstrap_launch_spec_init(const char *executable_path,
                                           cbm_daemon_bootstrap_launch_spec_t *spec_out) {
    if (!executable_path || executable_path[0] == '\0' || !spec_out) {
        return false;
    }
    memset(spec_out, 0, sizeof(*spec_out));
    spec_out->executable_path = executable_path;
    spec_out->argv[0] = executable_path;
    spec_out->argv[1] = CBM_DAEMON_INTERNAL_ARG;
    spec_out->argc = 2U;
    spec_out->detached = true;
    spec_out->inherit_standard_handles = false;
    spec_out->use_shell = false;
    return true;
}

bool cbm_daemon_bootstrap_launch_spec_init_permanent(const char *executable_path,
                                                     cbm_daemon_bootstrap_launch_spec_t *spec_out) {
    if (!cbm_daemon_bootstrap_launch_spec_init(executable_path, spec_out)) {
        return false;
    }
    spec_out->argv[2] = CBM_DAEMON_PERMANENT_ARG;
    spec_out->argc = 3U;
    return true;
}

static uint64_t bootstrap_deadline_after(uint32_t timeout_ms) {
    uint64_t now = cbm_now_ms();
    return UINT64_MAX - now < timeout_ms ? UINT64_MAX : now + (uint64_t)timeout_ms;
}

static _Noreturn void bootstrap_cleanup_fail_stop(const char *component) {
    (void)fprintf(stderr,
                  "codebase-memory-mcp: coordination cleanup failed (%s); "
                  "terminating so the OS releases retained claims\n",
                  component ? component : "unknown");
    (void)fflush(stderr);
#ifdef _WIN32
    (void)TerminateProcess(GetCurrentProcess(), EXIT_FAILURE);
    abort();
#else
    _exit(EXIT_FAILURE);
#endif
}

static const char BOOTSTRAP_START_FAILURE_LOG_NAME[] = "cbm-daemon.start-failures.log";

bool cbm_daemon_bootstrap_log_directory(char *out, size_t capacity) {
    if (!out || capacity == 0) {
        return false;
    }
    out[0] = '\0';
    const char *cache = cbm_resolve_cache_dir();
    if (!cache || !cache[0]) {
        return false;
    }
    int written = snprintf(out, capacity, "%s/logs", cache);
    if (written <= 0 || (size_t)written >= capacity) {
        out[0] = '\0';
        return false;
    }
    return true;
}

/* One record field: the line format is tab-separated, so a tab or newline in
 * a value (a path, in theory) becomes '?' rather than a parser ambiguity. */
static void bootstrap_record_field(char *out, size_t capacity, const char *value) {
    size_t used = 0;
    for (const char *cursor = value ? value : ""; *cursor && used + 1 < capacity; cursor++) {
        unsigned char ch = (unsigned char)*cursor;
        out[used++] = ch == '\t' || ch == '\n' || ch == '\r' ? '?' : (char)ch;
    }
    out[used] = '\0';
}

static uint64_t bootstrap_current_pid(void) {
#ifdef _WIN32
    return (uint64_t)GetCurrentProcessId();
#else
    return (uint64_t)getpid();
#endif
}

bool cbm_daemon_bootstrap_start_failure_record(const char *log_directory,
                                               const cbm_daemon_ipc_endpoint_t *endpoint,
                                               const char *component) {
    if (!log_directory || !log_directory[0] || !endpoint || !component || !component[0]) {
        return false;
    }
    const char *runtime_dir = cbm_daemon_ipc_endpoint_runtime_dir(endpoint);
    if (!runtime_dir || !runtime_dir[0]) {
        return false;
    }
    cbm_daemon_ipc_listen_failure_t detail;
    if (!cbm_daemon_ipc_listen_failure_detail(&detail)) {
        memset(&detail, 0, sizeof(detail));
    }
    char safe_runtime_dir[BOOTSTRAP_PATH_CAP];
    char safe_component[CBM_DAEMON_BOOTSTRAP_COMPONENT_CAP];
    char safe_stage[CBM_DAEMON_IPC_LISTEN_FAILURE_STAGE_CAP];
    char safe_path[CBM_DAEMON_IPC_LISTEN_FAILURE_PATH_CAP];
    bootstrap_record_field(safe_runtime_dir, sizeof(safe_runtime_dir), runtime_dir);
    bootstrap_record_field(safe_component, sizeof(safe_component), component);
    bootstrap_record_field(safe_stage, sizeof(safe_stage), detail.stage);
    bootstrap_record_field(safe_path, sizeof(safe_path), detail.path);
    FILE *stream = cbm_daemon_ipc_private_log_open(log_directory, BOOTSTRAP_START_FAILURE_LOG_NAME,
                                                   BOOTSTRAP_START_FAILURE_LOG_CAP);
    if (!stream) {
        return false;
    }
    int written =
        fprintf(stream, "v1\t%llu\t%llu\t%s\t%s\t%s\t%d\t%s\n", (unsigned long long)time(NULL),
                (unsigned long long)bootstrap_current_pid(), safe_runtime_dir, safe_component,
                safe_stage, detail.errno_value, safe_path);
    bool ok = written > 0 && fflush(stream) == 0;
    if (fclose(stream) != 0) {
        ok = false;
    }
    return ok;
}

/* Parse one "v1\tts\tpid\truntime_dir\tcomponent\tstage\terrno\tpath" line
 * (line is NUL-terminated, newline already stripped, and is modified). */
static bool bootstrap_start_failure_parse(char *line, char **runtime_dir_out,
                                          cbm_daemon_bootstrap_start_failure_t *out) {
    char *fields[8];
    size_t count = 0;
    char *cursor = line;
    while (count < 8) {
        fields[count++] = cursor;
        char *tab = strchr(cursor, '\t');
        if (!tab) {
            break;
        }
        *tab = '\0';
        cursor = tab + 1;
    }
    if (count != 8 || strcmp(fields[0], "v1") != 0) {
        return false;
    }
    memset(out, 0, sizeof(*out));
    out->recorded_at_s = strtoull(fields[1], NULL, 10);
    out->pid = strtoull(fields[2], NULL, 10);
    *runtime_dir_out = fields[3];
    (void)snprintf(out->component, sizeof(out->component), "%s", fields[4]);
    (void)snprintf(out->stage, sizeof(out->stage), "%s", fields[5]);
    out->errno_value = (int)strtol(fields[6], NULL, 10);
    (void)snprintf(out->path, sizeof(out->path), "%s", fields[7]);
    return true;
}

static FILE *bootstrap_start_failure_open(const char *log_directory, int *status_out) {
    char path[BOOTSTRAP_PATH_CAP];
    int written =
        snprintf(path, sizeof(path), "%s/%s", log_directory, BOOTSTRAP_START_FAILURE_LOG_NAME);
    if (written <= 0 || written >= (int)sizeof(path)) {
        *status_out = -1;
        return NULL;
    }
#ifndef _WIN32
    struct stat status;
    if (lstat(path, &status) != 0) {
        *status_out = errno == ENOENT ? 0 : -1;
        return NULL;
    }
    if (!S_ISREG(status.st_mode) || status.st_uid != geteuid() || status.st_nlink != 1) {
        *status_out = -1;
        return NULL;
    }
#endif
    /* The directory is the trust boundary (owner-only, no symlinked
     * ancestry); the same validation guards cbm-daemon.log itself. */
    if (!cbm_daemon_ipc_private_directory_secure(log_directory)) {
        *status_out = -1;
        return NULL;
    }
    FILE *file = cbm_fopen(path, "rb");
    if (!file) {
        *status_out = errno == ENOENT ? 0 : -1;
        return NULL;
    }
    *status_out = 1;
    return file;
}

int cbm_daemon_bootstrap_start_failure_read(const char *log_directory,
                                            const cbm_daemon_ipc_endpoint_t *endpoint,
                                            uint64_t not_before_s,
                                            cbm_daemon_bootstrap_start_failure_t *out_failure) {
    if (!out_failure) {
        return -1;
    }
    memset(out_failure, 0, sizeof(*out_failure));
    if (!log_directory || !log_directory[0] || !endpoint) {
        return -1;
    }
    const char *runtime_dir = cbm_daemon_ipc_endpoint_runtime_dir(endpoint);
    if (!runtime_dir || !runtime_dir[0]) {
        return -1;
    }
    int status = 0;
    FILE *file = bootstrap_start_failure_open(log_directory, &status);
    if (!file) {
        return status;
    }
    char *buffer = malloc(BOOTSTRAP_START_FAILURE_LOG_CAP + 1U);
    if (!buffer) {
        (void)fclose(file);
        return -1;
    }
    size_t used = fread(buffer, 1, BOOTSTRAP_START_FAILURE_LOG_CAP, file);
    bool read_ok = !ferror(file);
    (void)fclose(file);
    buffer[used] = '\0';
    int found = 0;
    if (read_ok) {
        char *line = buffer;
        while (line && *line) {
            char *end = strchr(line, '\n');
            if (!end) {
                break; /* a partial trailing line is a record still being written */
            }
            *end = '\0';
            char *record_runtime_dir = NULL;
            cbm_daemon_bootstrap_start_failure_t candidate;
            if (bootstrap_start_failure_parse(line, &record_runtime_dir, &candidate) &&
                strcmp(record_runtime_dir, runtime_dir) == 0 &&
                candidate.recorded_at_s >= not_before_s) {
                *out_failure = candidate; /* later lines are newer */
                found = 1;
            }
            line = end + 1;
        }
    } else {
        found = -1;
    }
    free(buffer);
    return found;
}

void cbm_daemon_bootstrap_start_failure_format(const cbm_daemon_bootstrap_start_failure_t *failure,
                                               const char *log_directory, char *out,
                                               size_t capacity) {
    if (!out || capacity == 0) {
        return;
    }
    if (!failure) {
        out[0] = '\0';
        return;
    }
    const char *what = failure->stage[0]       ? failure->stage
                       : failure->component[0] ? failure->component
                                               : "startup";
    char where[BOOTSTRAP_PATH_CAP];
    if (log_directory && log_directory[0]) {
        (void)snprintf(where, sizeof(where), "see %s/cbm-daemon.log", log_directory);
    } else {
        (void)snprintf(where, sizeof(where), "see the daemon log");
    }
    if (failure->errno_value != 0 && failure->path[0]) {
        (void)snprintf(out, capacity,
                       "CBM daemon failed to start: %s failed with %s (%s) at %s; %s", what,
                       cbm_errno_name(failure->errno_value), strerror(failure->errno_value),
                       failure->path, where);
    } else if (failure->errno_value != 0) {
        (void)snprintf(out, capacity, "CBM daemon failed to start: %s failed with %s (%s); %s",
                       what, cbm_errno_name(failure->errno_value), strerror(failure->errno_value),
                       where);
    } else if (failure->path[0]) {
        (void)snprintf(out, capacity, "CBM daemon failed to start: %s failed at %s; %s", what,
                       failure->path, where);
    } else {
        (void)snprintf(out, capacity, "CBM daemon failed to start: %s failed; %s", what, where);
    }
}

/* After this attempt spawned a daemon: did that daemon (or a sibling attempt
 * moments earlier) record a start failure? A found record ends the wait with
 * its cause -- respawning a deterministically failing daemon until the
 * deadline only produced "active or starting" (#1828). */
static bool bootstrap_start_failure_detected(const cbm_daemon_bootstrap_config_t *config,
                                             const cbm_daemon_bootstrap_ops_t *ops,
                                             uint64_t spawn_wall_s, bool force,
                                             uint64_t *next_check_ms,
                                             cbm_daemon_bootstrap_result_t *result_out) {
    if (spawn_wall_s == 0 || !ops->start_failure_probe) {
        return false;
    }
    uint64_t now_ms = cbm_now_ms();
    if (!force && now_ms < *next_check_ms) {
        return false;
    }
    *next_check_ms = now_ms + BOOTSTRAP_START_FAILURE_CHECK_MS;
    uint64_t not_before_s = spawn_wall_s > BOOTSTRAP_START_FAILURE_SKEW_S
                                ? spawn_wall_s - BOOTSTRAP_START_FAILURE_SKEW_S
                                : 0;
    cbm_daemon_bootstrap_start_failure_t failure;
    if (ops->start_failure_probe(ops->context, config->endpoint, not_before_s, &failure) != 1) {
        return false;
    }
    char logs[BOOTSTRAP_PATH_CAP];
    if (!cbm_daemon_bootstrap_log_directory(logs, sizeof(logs))) {
        logs[0] = '\0';
    }
    cbm_daemon_bootstrap_start_failure_format(&failure, logs, result_out->message,
                                              sizeof(result_out->message));
    return true;
}

static void bootstrap_pause(uint64_t deadline) {
    uint64_t now = cbm_now_ms();
    if (now >= deadline) {
        return;
    }
    uint64_t remaining_ms = deadline - now;
    struct timespec pause = {
        .tv_sec = 0,
        .tv_nsec = remaining_ms > 1 ? BOOTSTRAP_RETRY_NS : (long)(remaining_ms * 1000000ULL),
    };
    (void)cbm_nanosleep(&pause, NULL);
}

static void bootstrap_startup_lock_release_complete(const cbm_daemon_bootstrap_ops_t *ops,
                                                    cbm_daemon_bootstrap_lock_t *lock_io) {
    uint64_t deadline = bootstrap_deadline_after(BOOTSTRAP_COORDINATION_CLEANUP_MS);
    while (ops && ops->startup_lock_release && lock_io && *lock_io) {
        (void)ops->startup_lock_release(ops->context, lock_io);
        if (!*lock_io) {
            return;
        }
        if (cbm_now_ms() >= deadline) {
            bootstrap_cleanup_fail_stop("startup_lock_cleanup");
        }
        cbm_usleep(1000);
    }
}

static void bootstrap_result_reset(cbm_daemon_bootstrap_result_t *result,
                                   cbm_daemon_process_role_t role) {
    memset(result, 0, sizeof(*result));
    result->status = cbm_daemon_process_role_requires_client(role) ? CBM_DAEMON_BOOTSTRAP_FAILED
                                                                   : CBM_DAEMON_BOOTSTRAP_BYPASSED;
}

static cbm_daemon_bootstrap_status_t bootstrap_finish_probe(
    cbm_daemon_bootstrap_probe_status_t probe, cbm_daemon_runtime_client_t *client,
    const cbm_daemon_runtime_connect_result_t *connect_result,
    const cbm_daemon_bootstrap_ops_t *ops, cbm_daemon_bootstrap_result_t *result) {
    if (connect_result) {
        result->connect_result = *connect_result;
    }
    result->client = client;
    if (probe == CBM_DAEMON_BOOTSTRAP_PROBE_CONNECTED && client) {
        result->status = CBM_DAEMON_BOOTSTRAP_CONNECTED;
        return result->status;
    }
    result->client = NULL;
    if (probe == CBM_DAEMON_BOOTSTRAP_PROBE_CONFLICT) {
        result->status = CBM_DAEMON_BOOTSTRAP_CONFLICT;
        (void)snprintf(result->message, sizeof(result->message), "%s",
                       connect_result && connect_result->message[0]
                           ? connect_result->message
                           : "CBM could not start because a conflicting CBM process is active; "
                             "close all CBM sessions and commands, then retry. If a permanent "
                             "daemon from another build is running, `codebase-memory-mcp daemon "
                             "stop` retires it");
        if (ops->visible_diagnostic) {
            ops->visible_diagnostic(ops->context, result->message);
        }
        return result->status;
    }
    return CBM_DAEMON_BOOTSTRAP_FAILED;
}

static cbm_daemon_bootstrap_probe_status_t bootstrap_probe(
    const cbm_daemon_bootstrap_config_t *config, const cbm_daemon_bootstrap_ops_t *ops,
    cbm_daemon_runtime_client_t **client_out, cbm_daemon_runtime_connect_result_t *connect_result,
    uint64_t *muted_holder_pid_io) {
    memset(connect_result, 0, sizeof(*connect_result));
    *client_out = NULL;
    cbm_daemon_bootstrap_probe_status_t status =
        ops->probe(ops->context, config->endpoint, config->identity, config->connect_timeout_ms,
                   client_out, connect_result);
    /* Sticky across probes: a later fast-path probe never attempts a connect
     * and reports no holder, but a mute holder seen once must survive into
     * the final timeout diagnostic. */
    if (muted_holder_pid_io && connect_result->muted_endpoint_holder_pid != 0) {
        *muted_holder_pid_io = connect_result->muted_endpoint_holder_pid;
    }
    return status;
}

static bool bootstrap_probe_is_finishable(cbm_daemon_bootstrap_probe_status_t probe) {
    return probe == CBM_DAEMON_BOOTSTRAP_PROBE_CONNECTED ||
           probe == CBM_DAEMON_BOOTSTRAP_PROBE_CONFLICT;
}

static bool bootstrap_probe_is_waitable(cbm_daemon_bootstrap_probe_status_t probe) {
    return probe == CBM_DAEMON_BOOTSTRAP_PROBE_UNAVAILABLE ||
           probe == CBM_DAEMON_BOOTSTRAP_PROBE_RESERVED ||
           probe == CBM_DAEMON_BOOTSTRAP_PROBE_TERMINAL;
}

static bool bootstrap_config_valid(const cbm_daemon_bootstrap_config_t *config,
                                   const cbm_daemon_bootstrap_ops_t *ops) {
    return config && ops && config->endpoint && config->identity && config->executable_path &&
           config->executable_path[0] && config->connect_timeout_ms > 0 &&
           config->startup_timeout_ms > 0 && ops->cohort_acquire && ops->cohort_release &&
           ops->probe && ops->startup_lock_try_acquire && ops->startup_lock_prepare_handoff &&
           ops->startup_lock_release && ops->spawn_daemon;
}

cbm_daemon_bootstrap_status_t cbm_daemon_bootstrap_execute_with_ops(
    const cbm_daemon_bootstrap_config_t *config, const cbm_daemon_bootstrap_ops_t *ops,
    cbm_daemon_bootstrap_result_t *result_out) {
    if (!result_out) {
        return CBM_DAEMON_BOOTSTRAP_FAILED;
    }
    cbm_daemon_process_role_t role = config ? config->role : CBM_DAEMON_PROCESS_INVALID;
    bootstrap_result_reset(result_out, role);
    if (!cbm_daemon_process_role_requires_client(role)) {
        return role == CBM_DAEMON_PROCESS_INVALID ? CBM_DAEMON_BOOTSTRAP_FAILED
                                                  : CBM_DAEMON_BOOTSTRAP_BYPASSED;
    }
    if (!bootstrap_config_valid(config, ops)) {
        return CBM_DAEMON_BOOTSTRAP_FAILED;
    }

    uint64_t deadline = bootstrap_deadline_after(config->startup_timeout_ms);
    cbm_daemon_bootstrap_cohort_t cohort = NULL;
    cbm_daemon_conflict_t cohort_conflict;
    cbm_version_cohort_status_t cohort_status = ops->cohort_acquire(
        ops->context, config->endpoint, config->identity, deadline, &cohort, &cohort_conflict);
    if (cohort_status != CBM_VERSION_COHORT_OK) {
        result_out->status = cohort_status == CBM_VERSION_COHORT_CONFLICT
                                 ? CBM_DAEMON_BOOTSTRAP_CONFLICT
                                 : CBM_DAEMON_BOOTSTRAP_FAILED;
        bool formatted = cohort_status == CBM_VERSION_COHORT_CONFLICT &&
                         cbm_daemon_conflict_format(&cohort_conflict, result_out->message,
                                                    sizeof(result_out->message));
        if (!formatted) {
            const char *reason = cohort_status == CBM_VERSION_COHORT_BUSY
                                     ? "another CBM activation is in progress"
                                     : "exact-build admission could not be verified";
            (void)snprintf(result_out->message, sizeof(result_out->message),
                           "CBM daemon could not start: %s", reason);
        }
        if (ops->visible_diagnostic) {
            ops->visible_diagnostic(ops->context, result_out->message);
        }
        if (cohort) {
            ops->cohort_release(ops->context, cohort);
        }
        return result_out->status;
    }

    cbm_daemon_runtime_client_t *client = NULL;
    cbm_daemon_runtime_connect_result_t connect_result;
    uint64_t muted_holder_pid = 0;
    cbm_daemon_bootstrap_probe_status_t probe =
        bootstrap_probe(config, ops, &client, &connect_result, &muted_holder_pid);
    if (bootstrap_probe_is_finishable(probe)) {
        cbm_daemon_bootstrap_status_t status =
            bootstrap_finish_probe(probe, client, &connect_result, ops, result_out);
        ops->cohort_release(ops->context, cohort);
        return status;
    }
    if (!bootstrap_probe_is_waitable(probe)) {
        probe = CBM_DAEMON_BOOTSTRAP_PROBE_ERROR;
    }

    cbm_daemon_bootstrap_lock_t startup_lock = NULL;
    bool lock_acquired = false;
    bool generation_observed = probe == CBM_DAEMON_BOOTSTRAP_PROBE_RESERVED ||
                               probe == CBM_DAEMON_BOOTSTRAP_PROBE_TERMINAL;
    uint64_t spawn_wall_s = 0;
    uint64_t next_failure_check_ms = 0;
    bool start_failed = false;
    while (cbm_now_ms() < deadline) {
        if (bootstrap_start_failure_detected(config, ops, spawn_wall_s, false,
                                             &next_failure_check_ms, result_out)) {
            start_failed = true;
            break;
        }
        if (!bootstrap_probe_is_waitable(probe)) {
            break;
        }
        if (probe == CBM_DAEMON_BOOTSTRAP_PROBE_RESERVED ||
            probe == CBM_DAEMON_BOOTSTRAP_PROBE_TERMINAL) {
            /* A live or stopping generation owns the transition for now. Its
             * disappearance is not sticky: after observing true absence, the
             * same bootstrap attempt may serialize and become the next first
             * client. The startup lock and the re-probe below prevent two
             * replacements from being launched. */
            generation_observed = true;
            bootstrap_pause(deadline);
            probe = bootstrap_probe(config, ops, &client, &connect_result, &muted_holder_pid);
            continue;
        }

        int lock_status =
            ops->startup_lock_try_acquire(ops->context, config->endpoint, &startup_lock);
        if (lock_status < 0) {
            probe = CBM_DAEMON_BOOTSTRAP_PROBE_ERROR;
            break;
        }
        if (lock_status == 0) {
            bootstrap_pause(deadline);
            probe = bootstrap_probe(config, ops, &client, &connect_result, &muted_holder_pid);
            continue;
        }
        if (lock_status != 1 || !startup_lock) {
            probe = CBM_DAEMON_BOOTSTRAP_PROBE_ERROR;
            break;
        }

        lock_acquired = true;
        probe = bootstrap_probe(config, ops, &client, &connect_result, &muted_holder_pid);
        if (probe == CBM_DAEMON_BOOTSTRAP_PROBE_RESERVED ||
            probe == CBM_DAEMON_BOOTSTRAP_PROBE_TERMINAL) {
            generation_observed = true;
            bootstrap_startup_lock_release_complete(ops, &startup_lock);
            lock_acquired = false;
            continue;
        }
        if (bootstrap_probe_is_finishable(probe) || probe == CBM_DAEMON_BOOTSTRAP_PROBE_ERROR) {
            break;
        }
        if (probe != CBM_DAEMON_BOOTSTRAP_PROBE_UNAVAILABLE) {
            probe = CBM_DAEMON_BOOTSTRAP_PROBE_ERROR;
            break;
        }

        /* Never relaunch a daemon whose predecessor already recorded why it
         * could not start; the record is written before that daemon lets go
         * of its lifetime reservation, so this unconditional read sees it. */
        if (bootstrap_start_failure_detected(config, ops, spawn_wall_s, true,
                                             &next_failure_check_ms, result_out)) {
            start_failed = true;
            break;
        }
        cbm_daemon_bootstrap_launch_spec_t spec;
        bool spec_ready =
            config->spawn_permanent
                ? cbm_daemon_bootstrap_launch_spec_init_permanent(config->executable_path, &spec)
                : cbm_daemon_bootstrap_launch_spec_init(config->executable_path, &spec);
        spawn_wall_s = (uint64_t)time(NULL);
        if (!spec_ready || !ops->startup_lock_prepare_handoff(ops->context, startup_lock) ||
            !ops->spawn_daemon(ops->context, &spec)) {
            probe = CBM_DAEMON_BOOTSTRAP_PROBE_ERROR;
            break;
        }
        result_out->daemon_spawned = true;

        /* Keep startup ownership only until the child becomes observable.
         * Windows participant teardown reacquires the startup transition;
         * retaining it while probing an observable generation can deadlock the
         * bootstrap against a daemon that is trying to cleanly stand down. */
        do {
            bootstrap_pause(deadline);
            probe = bootstrap_probe(config, ops, &client, &connect_result, &muted_holder_pid);
            if (probe == CBM_DAEMON_BOOTSTRAP_PROBE_RESERVED ||
                probe == CBM_DAEMON_BOOTSTRAP_PROBE_TERMINAL) {
                generation_observed = true;
                bootstrap_startup_lock_release_complete(ops, &startup_lock);
                lock_acquired = false;
                break;
            }
            if (!bootstrap_probe_is_waitable(probe)) {
                break;
            }
            if (bootstrap_start_failure_detected(config, ops, spawn_wall_s, false,
                                                 &next_failure_check_ms, result_out)) {
                start_failed = true;
                break;
            }
        } while (cbm_now_ms() < deadline);
        if (start_failed) {
            break;
        }
        if (!lock_acquired) {
            continue;
        }
        break;
    }

    if (lock_acquired) {
        bootstrap_startup_lock_release_complete(ops, &startup_lock);
    }
    if (bootstrap_probe_is_finishable(probe)) {
        cbm_daemon_bootstrap_status_t status =
            bootstrap_finish_probe(probe, client, &connect_result, ops, result_out);
        ops->cohort_release(ops->context, cohort);
        return status;
    }

    result_out->status = CBM_DAEMON_BOOTSTRAP_FAILED;
    if (start_failed) {
        /* result_out->message already names the recorded cause. */
    } else if (muted_holder_pid != 0) {
        /* The one diagnostic the 2026-08-29 zombie recovery had to assemble by
         * hand from process, pipe, and log correlation: name the pid that
         * holds the endpoint without answering, and say what to do with it. */
        (void)snprintf(result_out->message, sizeof(result_out->message),
                       "CBM daemon endpoint is held by pid %llu but that process answered no "
                       "rendezvous within %u ms; the daemon runtime is likely dead — stop that "
                       "process, then retry",
                       (unsigned long long)muted_holder_pid, config->startup_timeout_ms);
    } else if (generation_observed) {
        (void)snprintf(result_out->message, sizeof(result_out->message),
                       "CBM daemon is active or starting but could not accept this client "
                       "within %u ms",
                       config->startup_timeout_ms);
    } else {
        (void)snprintf(result_out->message, sizeof(result_out->message),
                       "CBM daemon could not start within %u ms", config->startup_timeout_ms);
    }
    if (ops->visible_diagnostic) {
        ops->visible_diagnostic(ops->context, result_out->message);
    }
    ops->cohort_release(ops->context, cohort);
    return result_out->status;
}

cbm_daemon_bootstrap_probe_status_t cbm_daemon_bootstrap_classify_failed_connect(
    const cbm_daemon_runtime_connect_result_t *connect_result, int lifetime_status) {
    if (!connect_result) {
        return CBM_DAEMON_BOOTSTRAP_PROBE_ERROR;
    }
    if (connect_result->status == CBM_DAEMON_RUNTIME_CONNECT_CONFLICT) {
        return CBM_DAEMON_BOOTSTRAP_PROBE_CONFLICT;
    }
    if (connect_result->status == CBM_DAEMON_RUNTIME_CONNECT_REJECTED) {
        if (strstr(connect_result->message, "stopping") ||
            strstr(connect_result->message, "shutting down")) {
            return CBM_DAEMON_BOOTSTRAP_PROBE_TERMINAL;
        }
        /* Capacity, admission, and other protocol-level rejections prove an
         * existing generation answered. Never reinterpret them as absence. */
        return CBM_DAEMON_BOOTSTRAP_PROBE_RESERVED;
    }
    if (connect_result->status == CBM_DAEMON_RUNTIME_CONNECT_ERROR &&
        connect_result->muted_endpoint_holder_pid != 0) {
        /* The transport connected to a live process that answered nothing.
         * That endpoint is OWNED, whatever the advisory locks read right now
         * (a wedged generation can hold pipes while its lock files churn).
         * Classifying this as absence made the 2026-08-29 zombie invisible:
         * the starter spawned doomed competitors for 30 s and then reported a
         * bare timeout with no holder named. */
        return CBM_DAEMON_BOOTSTRAP_PROBE_RESERVED;
    }
    if (lifetime_status == 1) {
        return CBM_DAEMON_BOOTSTRAP_PROBE_RESERVED;
    }
    if (lifetime_status != 0) {
        return CBM_DAEMON_BOOTSTRAP_PROBE_ERROR;
    }
    return connect_result->status == CBM_DAEMON_RUNTIME_CONNECT_ERROR
               ? CBM_DAEMON_BOOTSTRAP_PROBE_UNAVAILABLE
               : CBM_DAEMON_BOOTSTRAP_PROBE_ERROR;
}

typedef struct bootstrap_production_cohort {
    cbm_version_cohort_manager_t *manager;
    cbm_version_cohort_lease_t *lease;
} bootstrap_production_cohort_t;

typedef struct {
    bootstrap_production_cohort_t *cohort;
#ifdef _WIN32
    DWORD spawn_error;
#elif defined(__APPLE__)
    int spawn_error;
#endif
} bootstrap_production_context_t;

static cbm_daemon_bootstrap_probe_status_t bootstrap_production_probe(
    void *context, const cbm_daemon_ipc_endpoint_t *endpoint,
    const cbm_daemon_build_identity_t *identity, uint32_t timeout_ms,
    cbm_daemon_runtime_client_t **client_out, cbm_daemon_runtime_connect_result_t *result_out) {
    bootstrap_production_context_t *production = context;
    if (!production || !production->cohort || !production->cohort->manager) {
        return CBM_DAEMON_BOOTSTRAP_PROBE_ERROR;
    }
    cbm_version_cohort_daemon_presence_t claim =
        cbm_version_cohort_daemon_claim_presence(production->cohort->manager);
    if (claim == CBM_VERSION_COHORT_DAEMON_ABSENT) {
        int lifetime = cbm_daemon_ipc_lifetime_reservation_probe(endpoint);
        if (lifetime == 0) {
            /* Do not spend the per-connect timeout polling a generation that
             * both independent ownership signals prove absent. The startup
             * lock and its mandatory re-probe serialize a concurrent launch. */
            return CBM_DAEMON_BOOTSTRAP_PROBE_UNAVAILABLE;
        }
        if (lifetime != 1) {
            return CBM_DAEMON_BOOTSTRAP_PROBE_ERROR;
        }
    } else if (claim != CBM_VERSION_COHORT_DAEMON_COORDINATED) {
        return CBM_DAEMON_BOOTSTRAP_PROBE_ERROR;
    }

    *client_out = cbm_daemon_runtime_client_connect(endpoint, identity, timeout_ms, result_out);
    if (*client_out) {
        return CBM_DAEMON_BOOTSTRAP_PROBE_CONNECTED;
    }

    /* Ownership may turn over while the connection attempt is in flight.
     * Re-observe both signals so disappearance is not sticky and a live or
     * cleaning-up generation is never mistaken for absence. */
    claim = cbm_version_cohort_daemon_claim_presence(production->cohort->manager);
    if (claim == CBM_VERSION_COHORT_DAEMON_COORDINATED) {
        return cbm_daemon_bootstrap_classify_failed_connect(result_out, 1);
    }
    if (claim != CBM_VERSION_COHORT_DAEMON_ABSENT) {
        return CBM_DAEMON_BOOTSTRAP_PROBE_ERROR;
    }
    int lifetime_status = cbm_daemon_ipc_lifetime_reservation_probe(endpoint);
    return cbm_daemon_bootstrap_classify_failed_connect(result_out, lifetime_status);
}

static cbm_version_cohort_status_t bootstrap_production_cohort_acquire(
    void *context, const cbm_daemon_ipc_endpoint_t *endpoint,
    const cbm_daemon_build_identity_t *identity, uint64_t deadline_ms,
    cbm_daemon_bootstrap_cohort_t *cohort_out, cbm_daemon_conflict_t *conflict_out) {
    *cohort_out = NULL;
    bootstrap_production_cohort_t *cohort = calloc(1, sizeof(*cohort));
    if (cohort) {
        cohort->manager = cbm_version_cohort_manager_new(endpoint);
    }
    if (!cohort || !cohort->manager) {
        free(cohort);
        return CBM_VERSION_COHORT_IO;
    }
    cbm_version_cohort_status_t status = cbm_version_cohort_acquire(
        cohort->manager, identity, deadline_ms, &cohort->lease, conflict_out);
    if (status == CBM_VERSION_COHORT_CONFLICT) {
        (void)cbm_version_cohort_log_conflict(conflict_out);
    }
    if (status == CBM_VERSION_COHORT_OK || cohort->lease) {
        *cohort_out = cohort;
        if (context) {
            bootstrap_production_context_t *production = context;
            production->cohort = cohort;
        }
        return status;
    }
    uint64_t cleanup_deadline = bootstrap_deadline_after(BOOTSTRAP_COORDINATION_CLEANUP_MS);
    cbm_private_file_lock_status_t cleanup = CBM_PRIVATE_FILE_LOCK_OK;
    while (cohort->manager) {
        cleanup = cbm_version_cohort_manager_free(&cohort->manager);
        if (!cohort->manager) {
            break;
        }
        if (cbm_now_ms() >= cleanup_deadline) {
            bootstrap_cleanup_fail_stop("cohort_manager_cleanup");
        }
        cbm_usleep(1000);
    }
    free(cohort);
    return cleanup == CBM_PRIVATE_FILE_LOCK_OK ? status : CBM_VERSION_COHORT_IO;
}

static void bootstrap_production_cohort_release(void *context,
                                                cbm_daemon_bootstrap_cohort_t opaque) {
    bootstrap_production_context_t *production = context;
    bootstrap_production_cohort_t *cohort = opaque;
    if (!cohort) {
        return;
    }
    uint64_t cleanup_deadline = bootstrap_deadline_after(BOOTSTRAP_COORDINATION_CLEANUP_MS);
    while (cohort->lease) {
        (void)cbm_version_cohort_lease_release(&cohort->lease);
        if (!cohort->lease) {
            break;
        }
        if (cbm_now_ms() >= cleanup_deadline) {
            bootstrap_cleanup_fail_stop("cohort_lease_cleanup");
        }
        cbm_usleep(1000);
    }
    cleanup_deadline = bootstrap_deadline_after(BOOTSTRAP_COORDINATION_CLEANUP_MS);
    while (cohort->manager) {
        (void)cbm_version_cohort_manager_free(&cohort->manager);
        if (!cohort->manager) {
            break;
        }
        if (cbm_now_ms() >= cleanup_deadline) {
            bootstrap_cleanup_fail_stop("cohort_manager_cleanup");
        }
        cbm_usleep(1000);
    }
    if (production && production->cohort == cohort) {
        production->cohort = NULL;
    }
    free(cohort);
}

static int bootstrap_production_lock(void *context, const cbm_daemon_ipc_endpoint_t *endpoint,
                                     cbm_daemon_bootstrap_lock_t *lock_out) {
    (void)context;
    cbm_daemon_ipc_startup_lock_t *lock = NULL;
    int status = cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &lock);
    *lock_out = lock;
    return status;
}

static bool bootstrap_production_unlock(void *context, cbm_daemon_bootstrap_lock_t *lock_io) {
    (void)context;
    if (!lock_io) {
        return false;
    }
    cbm_daemon_ipc_startup_lock_t *lock = *lock_io;
    bool released = cbm_daemon_ipc_startup_lock_release(&lock);
    *lock_io = lock;
    return released;
}

static int bootstrap_production_start_failure_probe(
    void *context, const cbm_daemon_ipc_endpoint_t *endpoint, uint64_t not_before_s,
    cbm_daemon_bootstrap_start_failure_t *out_failure) {
    (void)context;
    char logs[BOOTSTRAP_PATH_CAP];
    if (!cbm_daemon_bootstrap_log_directory(logs, sizeof(logs))) {
        return -1;
    }
    return cbm_daemon_bootstrap_start_failure_read(logs, endpoint, not_before_s, out_failure);
}

static bool bootstrap_production_handoff(void *context, cbm_daemon_bootstrap_lock_t lock) {
    (void)context;
    return cbm_daemon_ipc_startup_lock_prepare_handoff((cbm_daemon_ipc_startup_lock_t *)lock);
}

#ifdef _WIN32
static bool bootstrap_production_spawn(void *context,
                                       const cbm_daemon_bootstrap_launch_spec_t *spec) {
    bootstrap_production_context_t *production = context;
    if (production) {
        production->spawn_error = ERROR_SUCCESS;
    }
    if (!spec || !spec->detached || spec->inherit_standard_handles || spec->use_shell) {
        if (production) {
            production->spawn_error = ERROR_INVALID_PARAMETER;
        }
        return false;
    }
    char command_line[BOOTSTRAP_PATH_CAP * 2];
    if (!cbm_build_win_cmdline(command_line, sizeof(command_line), spec->argv)) {
        if (production) {
            production->spawn_error = ERROR_INVALID_PARAMETER;
        }
        return false;
    }
    wchar_t *application = cbm_utf8_to_wide(spec->executable_path);
    wchar_t *command = cbm_utf8_to_wide(command_line);
    if (!application || !command) {
        if (production) {
            production->spawn_error = ERROR_NOT_ENOUGH_MEMORY;
        }
        free(application);
        free(command);
        return false;
    }
    STARTUPINFOW startup;
    PROCESS_INFORMATION child;
    ZeroMemory(&startup, sizeof(startup));
    ZeroMemory(&child, sizeof(child));
    startup.cb = sizeof(startup);
    DWORD flags = DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW;
    /* A managed frontend payload is intentionally contained in the permanent
     * launcher's kill-on-close job. The account daemon outlives that one
     * frontend and therefore uses breakaway when the containing job explicitly
     * permits it. Do not request breakaway from an unrelated restrictive job:
     * CreateProcess would fail and regress portable/package-manager payloads. */
    BOOL in_job = FALSE;
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION job_limits;
    memset(&job_limits, 0, sizeof(job_limits));
    if (IsProcessInJob(GetCurrentProcess(), NULL, &in_job) && in_job &&
        QueryInformationJobObject(NULL, JobObjectExtendedLimitInformation, &job_limits,
                                  sizeof(job_limits), NULL) &&
        (job_limits.BasicLimitInformation.LimitFlags &
         (JOB_OBJECT_LIMIT_BREAKAWAY_OK | JOB_OBJECT_LIMIT_SILENT_BREAKAWAY_OK)) != 0) {
        flags |= CREATE_BREAKAWAY_FROM_JOB;
    }
    BOOL created = CreateProcessW(application, command, NULL, NULL, FALSE, flags, NULL, NULL,
                                  &startup, &child);
    DWORD spawn_error = created ? ERROR_SUCCESS : GetLastError();
    free(application);
    free(command);
    if (production) {
        production->spawn_error = spawn_error;
    }
    if (!created) {
        return false;
    }
    (void)CloseHandle(child.hThread);
    (void)CloseHandle(child.hProcess);
    return true;
}
#elif defined(__APPLE__)
static bool bootstrap_darwin_spawn_state_init(posix_spawn_file_actions_t *actions,
                                              posix_spawnattr_t *attributes) {
    if (posix_spawn_file_actions_init(actions) != 0) {
        return false;
    }
    bool actions_ready =
        posix_spawn_file_actions_addopen(actions, STDIN_FILENO, "/dev/null", O_RDWR, 0) == 0 &&
        posix_spawn_file_actions_addopen(actions, STDOUT_FILENO, "/dev/null", O_RDWR, 0) == 0 &&
        posix_spawn_file_actions_addopen(actions, STDERR_FILENO, "/dev/null", O_RDWR, 0) == 0;
    if (!actions_ready) {
        (void)posix_spawn_file_actions_destroy(actions);
        return false;
    }
    if (posix_spawnattr_init(attributes) != 0) {
        (void)posix_spawn_file_actions_destroy(actions);
        return false;
    }
    sigset_t empty;
    (void)sigemptyset(&empty);
    short flags = POSIX_SPAWN_SETSIGMASK | POSIX_SPAWN_SETSID | POSIX_SPAWN_CLOEXEC_DEFAULT;
    if (posix_spawnattr_setsigmask(attributes, &empty) != 0 ||
        posix_spawnattr_setflags(attributes, flags) != 0) {
        (void)posix_spawnattr_destroy(attributes);
        (void)posix_spawn_file_actions_destroy(actions);
        return false;
    }
    return true;
}

typedef struct {
    pid_t pid;
} bootstrap_darwin_reaper_t;

static void *bootstrap_darwin_reap(void *opaque) {
    bootstrap_darwin_reaper_t *reaper = opaque;
    int status = 0;
    pid_t waited;
    do {
        waited = waitpid(reaper->pid, &status, 0);
    } while (waited < 0 && errno == EINTR);
    free(reaper);
    return NULL;
}

static bool bootstrap_darwin_reaper_start(pid_t daemon) {
    bootstrap_darwin_reaper_t *reaper = malloc(sizeof(*reaper));
    if (!reaper) {
        return false;
    }
    reaper->pid = daemon;
    pthread_attr_t attributes;
    bool attributes_ready = pthread_attr_init(&attributes) == 0;
    bool detached =
        attributes_ready && pthread_attr_setdetachstate(&attributes, PTHREAD_CREATE_DETACHED) == 0;
    pthread_t thread;
    int thread_status =
        detached ? pthread_create(&thread, &attributes, bootstrap_darwin_reap, reaper) : -1;
    if (attributes_ready) {
        (void)pthread_attr_destroy(&attributes);
    }
    if (thread_status != 0) {
        free(reaper);
        return false;
    }
    return true;
}

static bool bootstrap_production_spawn(void *context,
                                       const cbm_daemon_bootstrap_launch_spec_t *spec) {
    bootstrap_production_context_t *production = context;
    if (production) {
        production->spawn_error = 0;
    }
    if (!spec || !spec->detached || spec->inherit_standard_handles || spec->use_shell) {
        return false;
    }
    posix_spawn_file_actions_t actions;
    posix_spawnattr_t attributes;
    if (!bootstrap_darwin_spawn_state_init(&actions, &attributes)) {
        return false;
    }
    /* Calling library-heavy process creation between fork() and exec is not a
     * safe boundary once another thread may own libc state. Darwin's spawn
     * primitive can create the detached session and close inherited FDs in
     * one operation, so launch it from the original process and retain only a
     * tiny detached waiter to prevent a crashed daemon from becoming a
     * long-lived zombie owned by the first frontend. */
    pid_t daemon = 0;
    int spawn_status = posix_spawn(&daemon, spec->executable_path, &actions, &attributes,
                                   (char *const *)spec->argv, environ);
    (void)posix_spawnattr_destroy(&attributes);
    (void)posix_spawn_file_actions_destroy(&actions);
    if (production) {
        production->spawn_error = spawn_status;
    }
    if (spawn_status != 0) {
        return false;
    }
    if (!bootstrap_darwin_reaper_start(daemon)) {
        (void)kill(daemon, SIGKILL);
        int status = 0;
        while (waitpid(daemon, &status, 0) < 0 && errno == EINTR) {}
        if (production) {
            production->spawn_error = EAGAIN;
        }
        return false;
    }
    return true;
}
#else
static void bootstrap_child_close_fds(void) {
    long open_max = sysconf(_SC_OPEN_MAX);
    if (open_max < 0 || open_max > 1048576L) {
        open_max = 65536L;
    }
    for (int fd = 3; fd < open_max; fd++) {
        (void)close(fd);
    }
}

static void bootstrap_daemon_grandchild(const cbm_daemon_bootstrap_launch_spec_t *spec) {
    (void)umask(077);
    sigset_t empty;
    (void)sigemptyset(&empty);
    (void)sigprocmask(SIG_SETMASK, &empty, NULL);
    int null_fd = open("/dev/null", O_RDWR);
    if (null_fd < 0 || dup2(null_fd, STDIN_FILENO) < 0 || dup2(null_fd, STDOUT_FILENO) < 0 ||
        dup2(null_fd, STDERR_FILENO) < 0) {
        _exit(127);
    }
    if (null_fd > STDERR_FILENO) {
        (void)close(null_fd);
    }
    bootstrap_child_close_fds();
    execv(spec->executable_path, (char *const *)spec->argv);
    _exit(127);
}

static bool bootstrap_production_spawn(void *context,
                                       const cbm_daemon_bootstrap_launch_spec_t *spec) {
    (void)context;
    if (!spec || !spec->detached || spec->inherit_standard_handles || spec->use_shell) {
        return false;
    }
    pid_t first = fork();
    if (first < 0) {
        return false;
    }
    if (first == 0) {
        if (setsid() < 0) {
            _exit(127);
        }
        pid_t daemon = fork();
        if (daemon < 0) {
            _exit(127);
        }
        if (daemon > 0) {
            _exit(0);
        }
        bootstrap_daemon_grandchild(spec);
    }

    int status = 0;
    pid_t waited;
    do {
        waited = waitpid(first, &status, 0);
    } while (waited < 0 && errno == EINTR);
    return waited == first && WIFEXITED(status) && WEXITSTATUS(status) == 0;
}
#endif

static void bootstrap_production_diagnostic(void *context, const char *message) {
    bootstrap_production_context_t *production = context;
#ifdef _WIN32
    if (production && production->spawn_error != ERROR_SUCCESS) {
        (void)fprintf(stderr, "codebase-memory-mcp: %s (daemon launch error %lu)\n",
                      message ? message : "daemon startup failed",
                      (unsigned long)production->spawn_error);
        (void)fflush(stderr);
        return;
    }
#elif defined(__APPLE__)
    if (production && production->spawn_error != 0) {
        (void)fprintf(stderr, "codebase-memory-mcp: %s (daemon launch: %s)\n",
                      message ? message : "daemon startup failed",
                      strerror(production->spawn_error));
        (void)fflush(stderr);
        return;
    }
#else
    (void)production;
#endif
    (void)fprintf(stderr, "codebase-memory-mcp: %s\n", message ? message : "daemon startup failed");
    (void)fflush(stderr);
}

#ifdef CBM_ENABLE_TEST_SEAMS
static cbm_daemon_bootstrap_spawn_fn g_bootstrap_spawn_override_for_test;
static void *g_bootstrap_spawn_override_context_for_test;

void cbm_daemon_bootstrap_spawn_override_set_for_test(cbm_daemon_bootstrap_spawn_fn spawn,
                                                      void *context) {
    g_bootstrap_spawn_override_context_for_test = context;
    g_bootstrap_spawn_override_for_test = spawn;
}

static bool bootstrap_seam_spawn(void *context, const cbm_daemon_bootstrap_launch_spec_t *spec) {
    (void)context;
    return g_bootstrap_spawn_override_for_test(g_bootstrap_spawn_override_context_for_test, spec);
}
#endif

cbm_daemon_bootstrap_status_t cbm_daemon_bootstrap_execute(
    const cbm_daemon_bootstrap_config_t *config, cbm_daemon_bootstrap_result_t *result_out) {
    bootstrap_production_context_t context = {0};
    cbm_daemon_bootstrap_ops_t ops = {
        .context = &context,
        .cohort_acquire = bootstrap_production_cohort_acquire,
        .cohort_release = bootstrap_production_cohort_release,
        .probe = bootstrap_production_probe,
        .startup_lock_try_acquire = bootstrap_production_lock,
        .startup_lock_prepare_handoff = bootstrap_production_handoff,
        .startup_lock_release = bootstrap_production_unlock,
        .spawn_daemon = bootstrap_production_spawn,
        .visible_diagnostic = bootstrap_production_diagnostic,
        .start_failure_probe = bootstrap_production_start_failure_probe,
    };
#ifdef CBM_ENABLE_TEST_SEAMS
    if (g_bootstrap_spawn_override_for_test) {
        ops.spawn_daemon = bootstrap_seam_spawn;
    }
#endif
    return cbm_daemon_bootstrap_execute_with_ops(config, &ops, result_out);
}
