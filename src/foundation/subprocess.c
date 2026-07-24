/*
 * subprocess.c — cross-platform spawn + supervise + classify.
 * See subprocess.h. The spawn/reap skeleton mirrors src/ui/http_server.c's
 * index subprocess; this generalizes it and adds crash/hang classification.
 */
#include "subprocess.h"

#include "compat.h" /* cbm_nanosleep */
#include "compat_fs.h"
#include "log.h"
#include "platform.h" /* cbm_now_ms */

#include <stdio.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <windows.h>
#include "win_utf8.h" /* cbm_utf8_to_wide — spawn the worker with a wide command line so a
                       * non-ASCII repo path survives CreateProcess (#423/#20) */
#include <stdlib.h>   /* free */
#else
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

/* NTSTATUS severity ERROR (top two bits set) covers the Windows crash exception
 * exit codes: 0xC0000005 (access violation), 0xC00000FD (stack overflow),
 * 0xC000001D (illegal instruction), 0xC0000094 (integer divide by zero), … */
#define CBM_WIN_CRASH_CODE_MIN 0xC0000000u
#define CBM_WIN_CONTROL_C_EXIT 0xC000013Au

#ifndef _WIN32
static bool cbm_is_fault_signal(int sig) {
    switch (sig) {
    case SIGSEGV:
    case SIGBUS:
    case SIGILL:
    case SIGFPE:
    case SIGABRT:
    case SIGSYS:
        return true;
    default:
        return false;
    }
}
#endif

cbm_proc_outcome_t cbm_proc_classify(bool exited_normally, int exit_code, int term_signal,
                                     bool timed_out) {
    if (timed_out) {
        return CBM_PROC_HANG;
    }
    if (!exited_normally) {
        /* POSIX signal death. */
#ifndef _WIN32
        if (cbm_is_fault_signal(term_signal)) {
            return CBM_PROC_CRASH;
        }
#else
        (void)term_signal;
#endif
        return CBM_PROC_KILLED;
    }
    /* Exited with a code. A Windows NTSTATUS exception code is a crash; on POSIX
     * exit codes are 0..255 so this branch never misfires there. */
    if ((unsigned)exit_code == CBM_WIN_CONTROL_C_EXIT) {
        return CBM_PROC_KILLED;
    }
    if ((unsigned)exit_code >= CBM_WIN_CRASH_CODE_MIN) {
        return CBM_PROC_CRASH;
    }
    return (exit_code == 0) ? CBM_PROC_CLEAN : CBM_PROC_EXIT_NONZERO;
}

const char *cbm_proc_outcome_str(cbm_proc_outcome_t o) {
    switch (o) {
    case CBM_PROC_CLEAN:
        return "clean";
    case CBM_PROC_EXIT_NONZERO:
        return "exit_nonzero";
    case CBM_PROC_CRASH:
        return "crash";
    case CBM_PROC_HANG:
        return "hang";
    case CBM_PROC_KILLED:
        return "killed";
    case CBM_PROC_SPAWN_FAILED:
    default:
        return "spawn_failed";
    }
}

typedef enum {
    CBM_TAIL_MORE = 0,
    CBM_TAIL_CAUGHT_UP,
    CBM_TAIL_ERROR,
} cbm_tail_status_t;

typedef struct {
    cbm_tail_status_t status;
    bool progressed;
} cbm_tail_result_t;

/* Tail one bounded batch from the child log. While the owned tree can still
 * write, a partial final line remains buffered. Once the tree is quiescent,
 * final=true delivers that last fragment exactly once. */
static cbm_tail_result_t cbm_tail_log(const char *log_file, long *tail_pos, cbm_proc_log_cb cb,
                                      void *ud, bool final) {
    cbm_tail_result_t result = {.status = CBM_TAIL_ERROR, .progressed = false};
    if (!log_file || !tail_pos) {
        return result;
    }
#ifdef _WIN32
    FILE *lf = cbm_fopen(log_file, "r");
#else
    int open_flags = O_RDONLY | O_NONBLOCK;
#ifdef O_CLOEXEC
    open_flags |= O_CLOEXEC;
#endif
#ifdef O_NOFOLLOW
    open_flags |= O_NOFOLLOW;
#endif
    int log_fd = open(log_file, open_flags);
    if (log_fd < 0) {
        return result;
    }
    struct stat log_status;
    if (fstat(log_fd, &log_status) != 0 || !S_ISREG(log_status.st_mode)) {
        (void)close(log_fd);
        return result;
    }
    FILE *lf = fdopen(log_fd, "r");
#endif
    if (!lf) {
#ifndef _WIN32
        (void)close(log_fd);
#endif
        return result;
    }
    if (fseek(lf, *tail_pos, SEEK_SET) == 0) {
        char line[1024];
        size_t delivered_lines = 0;
        size_t delivered_bytes = 0;
        enum {
            CBM_TAIL_MAX_LINES_PER_POLL = 64,
            CBM_TAIL_MAX_BYTES_PER_POLL = 64 * 1024,
        };
        while (delivered_lines < CBM_TAIL_MAX_LINES_PER_POLL &&
               delivered_bytes < CBM_TAIL_MAX_BYTES_PER_POLL) {
            long before = ftell(lf);
            if (!fgets(line, sizeof(line), lf)) {
                result.status = ferror(lf) ? CBM_TAIL_ERROR : CBM_TAIL_CAUGHT_UP;
                break;
            }
            size_t l = strlen(line);
            delivered_bytes += l;
            bool complete = (l > 0 && line[l - 1] == '\n');
            if (complete) {
                line[l - 1] = '\0';
                *tail_pos = ftell(lf);
                result.progressed = true;
                delivered_lines++;
                if (line[0] && cb) {
                    cb(line, ud);
                }
            } else if (l == sizeof(line) - 1) {
                /* Oversized line filled the buffer without a newline — consume it
                 * anyway (counts as progress) so we never stall on one long line. */
                *tail_pos = ftell(lf);
                result.progressed = true;
                delivered_lines++;
                if (cb) {
                    cb(line, ud);
                }
            } else if (!final) {
                /* Genuine partial final line — keep it buffered for next poll. */
                *tail_pos = before;
                result.status = CBM_TAIL_MORE;
                break;
            } else {
                /* No writer remains: deliver the final unterminated fragment. */
                *tail_pos = ftell(lf);
                result.progressed = true;
                if (line[0] && cb) {
                    cb(line, ud);
                }
                result.status = CBM_TAIL_CAUGHT_UP;
                break;
            }
        }
        if (result.status == CBM_TAIL_ERROR && !ferror(lf)) {
            /* Reaching either work cap is conservatively MORE. An exact batch
             * boundary needs one empty follow-up poll to prove EOF. */
            result.status = CBM_TAIL_MORE;
        }
    }
    if (fclose(lf) != 0) {
        result.status = CBM_TAIL_ERROR;
    }
    return result;
}

/* ── Windows command-line quoting (pure; unit-tested on every platform) ─────── */

/* Append char `c` to buf[cap], reserving the final byte for a NUL terminator.
 * On overflow: sets *ovf, stops writing, and returns pos UNCHANGED — callers detect
 * the overflow via the *ovf flag (not via the return value). */
static size_t cbm_cmdline_put(char *buf, size_t cap, size_t pos, char c, bool *ovf) {
    if (pos + 1 >= cap) {
        *ovf = true;
        return pos;
    }
    buf[pos] = c;
    return pos + 1;
}

/* Append one argv element to the command line using the Microsoft C runtime
 * quoting rules (see MS "Parsing C Command-Line Arguments"). CreateProcess takes
 * a SINGLE string that the child re-parses back into argv, so any element with a
 * space, tab or double-quote must be wrapped in quotes and its embedded quotes /
 * preceding backslashes escaped. Without this a JSON argument like
 * {"repo_path":"C:/r"} loses its inner quotes and the child receives the invalid
 * {repo_path:C:/r} — the Windows-only index-worker cmdline-quoting bug (the worker exited
 * non-zero at JSON-arg parse, misattributed to the last-marked file). POSIX is
 * unaffected: the POSIX spawn path passes the argv array straight to execv. */
static size_t cbm_cmdline_append_arg(char *buf, size_t cap, size_t pos, const char *arg, bool first,
                                     bool *ovf) {
    if (!first) {
        pos = cbm_cmdline_put(buf, cap, pos, ' ', ovf);
    }
    pos = cbm_cmdline_put(buf, cap, pos, '"', ovf);
    for (const char *p = arg; *p;) {
        size_t nbs = 0;
        while (*p == '\\') {
            nbs++;
            p++;
        }
        if (*p == '\0') {
            /* Trailing backslashes precede the closing quote: double them so the
             * quote stays a delimiter, not an escaped literal. */
            for (size_t k = 0; k < nbs * 2; k++) {
                pos = cbm_cmdline_put(buf, cap, pos, '\\', ovf);
            }
            break;
        }
        if (*p == '"') {
            /* N backslashes then a quote -> 2N+1 backslashes then an escaped quote. */
            for (size_t k = 0; k < nbs * 2 + 1; k++) {
                pos = cbm_cmdline_put(buf, cap, pos, '\\', ovf);
            }
            pos = cbm_cmdline_put(buf, cap, pos, '"', ovf);
            p++;
        } else {
            for (size_t k = 0; k < nbs; k++) {
                pos = cbm_cmdline_put(buf, cap, pos, '\\', ovf);
            }
            pos = cbm_cmdline_put(buf, cap, pos, *p, ovf);
            p++;
        }
    }
    pos = cbm_cmdline_put(buf, cap, pos, '"', ovf);
    return pos;
}

/* Build a full Windows CreateProcess command line from a NULL-terminated argv,
 * applying the MS C runtime quoting rules so the child re-parses byte-identical
 * argv. Returns true on success, false if the result would overflow `buf`.
 *
 * Defined unconditionally (pure string logic, no Windows headers) so the quoting
 * contract is unit-tested on Linux/macOS CI too — even though the real spawn path
 * only runs on Windows. Shared by cbm_subprocess_spawn_win AND the UI http_server
 * index spawn so both escape identically; a naive `"%s"` wrap corrupts any argument
 * containing a quote (e.g. the index JSON {"repo_path":"…"}), corrupting the
 * spawned child's argv. */
bool cbm_build_win_cmdline(char *buf, size_t cap, const char *const *argv) {
    if (!buf || cap == 0 || !argv) {
        return false;
    }
    size_t pos = 0;
    bool ovf = false;
    for (int i = 0; argv[i]; i++) {
        pos = cbm_cmdline_append_arg(buf, cap, pos, argv[i], i == 0, &ovf);
        if (ovf) {
            buf[0] = '\0'; /* overflow: leave buf a valid (empty) string, never unterminated */
            return false;
        }
    }
    buf[pos] = '\0';
    return true;
}

static bool cbm_ascii_case_equal(const char *left, const char *right) {
    if (!left || !right) {
        return false;
    }
    while (*left && *right) {
        unsigned char left_ch = (unsigned char)*left++;
        unsigned char right_ch = (unsigned char)*right++;
        if (left_ch >= 'A' && left_ch <= 'Z') {
            left_ch = (unsigned char)(left_ch + ('a' - 'A'));
        }
        if (right_ch >= 'A' && right_ch <= 'Z') {
            right_ch = (unsigned char)(right_ch + ('a' - 'A'));
        }
        if (left_ch != right_ch) {
            return false;
        }
    }
    return *left == '\0' && *right == '\0';
}

static bool cbm_win_cmd_path_is_absolute(const char *path) {
    if (!path) {
        return false;
    }
    size_t path_len = strlen(path);
    if (path_len < 3) {
        return false;
    }
    bool drive_absolute =
        ((path[0] >= 'A' && path[0] <= 'Z') || (path[0] >= 'a' && path[0] <= 'z')) &&
        path[1] == ':' && (path[2] == '\\' || path[2] == '/');
    bool unc_absolute = (path[0] == '\\' || path[0] == '/') && path[1] == path[0] && path[2];
    if (!drive_absolute && !unc_absolute) {
        return false;
    }

    const char *basename = path;
    for (const char *cursor = path; *cursor; cursor++) {
        if (*cursor == '"' || *cursor == '\r' || *cursor == '\n') {
            return false;
        }
        if (*cursor == '\\' || *cursor == '/') {
            basename = cursor + 1;
        }
    }
    return cbm_ascii_case_equal(basename, "cmd.exe");
}

bool cbm_build_win_cmd_payload(char *buf, size_t cap, const char *cmd_executable,
                               const char *payload) {
    if (!buf || cap == 0) {
        return false;
    }
    buf[0] = '\0';
    if (!cbm_win_cmd_path_is_absolute(cmd_executable) || !payload || !payload[0]) {
        return false;
    }

    static const char prefix[] = "\" /D /S /V:OFF /C ";
    size_t executable_len = strlen(cmd_executable);
    size_t payload_len = strlen(payload);
    size_t remaining = cap - 1; /* reserve the final NUL */
    if (remaining < 1) {
        return false;
    }
    remaining--; /* opening quote */
    if (executable_len > remaining) {
        return false;
    }
    remaining -= executable_len;
    if (sizeof(prefix) - 1 > remaining) {
        return false;
    }
    remaining -= sizeof(prefix) - 1;
    if (payload_len > remaining) {
        return false;
    }

    size_t pos = 0;
    buf[pos++] = '"';
    memcpy(buf + pos, cmd_executable, executable_len);
    pos += executable_len;
    memcpy(buf + pos, prefix, sizeof(prefix) - 1);
    pos += sizeof(prefix) - 1;
    memcpy(buf + pos, payload, payload_len);
    pos += payload_len;
    buf[pos] = '\0';
    return true;
}

/* ── Nonblocking contained-process supervisor ─────────────────────────────── */

enum { CBM_SUBPROCESS_ARGV_LIMIT = 4096 };

typedef enum {
    CBM_SUBPROCESS_ACTIVE = 0,
    CBM_SUBPROCESS_CANCEL_REQUESTED,
    CBM_SUBPROCESS_DRAINING,
    CBM_SUBPROCESS_TERMINAL,
} cbm_subprocess_lifecycle_t;

struct cbm_subprocess {
    char *bin;
    char **argv;
    size_t argc;
    char *windows_cmd_payload;
    char *log_file;
    cbm_proc_log_cb on_log_line;
    void *log_ud;
    int quiet_timeout_ms;
    int cancel_grace_ms;
    bool delete_log_on_exit;

    long tail_pos;
    uint64_t last_activity_ms;
    uint64_t termination_started_ms;
    atomic_int lifecycle;
    bool timed_out;
    bool termination_started;
    bool force_sent;
    bool root_reaped;
    uint64_t force_started_ms;
    bool containment_failed;
    cbm_proc_result_t result;

#ifdef _WIN32
    HANDLE process;
    HANDLE job;
    DWORD process_id;
    bool root_forced;
#else
    pid_t pid;
    pid_t pgid;
#endif
};

static void cbm_subprocess_result_init(cbm_proc_result_t *result) {
    result->outcome = CBM_PROC_SPAWN_FAILED;
    result->exit_code = -1;
    result->term_signal = 0;
    result->cancellation_requested = false;
    result->forced = false;
    result->tree_quiesced = false;
    result->supervision_failed = false;
}

static void cbm_subprocess_free_config(cbm_subprocess_t *process) {
    if (!process) {
        return;
    }
    if (process->argv) {
        for (size_t i = 0; i < process->argc; i++) {
            free(process->argv[i]);
        }
    }
    free(process->argv);
    free(process->bin);
    free(process->windows_cmd_payload);
    free(process->log_file);
    free(process);
}

static cbm_subprocess_t *cbm_subprocess_copy_opts(const cbm_proc_opts_t *opts) {
    if (!opts || !opts->bin || !opts->bin[0]) {
        return NULL;
    }
#ifdef _WIN32
    if (opts->windows_cmd_payload &&
        (!opts->windows_cmd_payload[0] || opts->argv || !cbm_win_cmd_path_is_absolute(opts->bin))) {
        return NULL;
    }
#else
    if (opts->windows_cmd_payload) {
        return NULL;
    }
#endif

    size_t argc = 1;
    if (opts->argv) {
        argc = 0;
        while (argc < CBM_SUBPROCESS_ARGV_LIMIT && opts->argv[argc]) {
            argc++;
        }
        if (argc == 0 || argc == CBM_SUBPROCESS_ARGV_LIMIT) {
            return NULL;
        }
    }

    cbm_subprocess_t *process = (cbm_subprocess_t *)calloc(1, sizeof(*process));
    if (!process) {
        return NULL;
    }
    process->bin = cbm_strdup(opts->bin);
    process->windows_cmd_payload =
        opts->windows_cmd_payload ? cbm_strdup(opts->windows_cmd_payload) : NULL;
    process->argv = (char **)calloc(argc + 1, sizeof(*process->argv));
    process->argc = argc;
    if (!process->bin || !process->argv ||
        (opts->windows_cmd_payload && !process->windows_cmd_payload)) {
        cbm_subprocess_free_config(process);
        return NULL;
    }
    for (size_t i = 0; i < argc; i++) {
        const char *arg = opts->argv ? opts->argv[i] : opts->bin;
        process->argv[i] = cbm_strdup(arg);
        if (!process->argv[i]) {
            cbm_subprocess_free_config(process);
            return NULL;
        }
    }
    if (opts->log_file) {
        process->log_file = cbm_strdup(opts->log_file);
        if (!process->log_file) {
            cbm_subprocess_free_config(process);
            return NULL;
        }
    }
    process->on_log_line = opts->on_log_line;
    process->log_ud = opts->log_ud;
    process->quiet_timeout_ms = opts->quiet_timeout_ms;
    process->cancel_grace_ms =
        opts->cancel_grace_ms > 0 ? opts->cancel_grace_ms : CBM_SUBPROCESS_DEFAULT_CANCEL_GRACE_MS;
    if (process->cancel_grace_ms > CBM_SUBPROCESS_MAX_CANCEL_GRACE_MS) {
        process->cancel_grace_ms = CBM_SUBPROCESS_MAX_CANCEL_GRACE_MS;
    }
    process->delete_log_on_exit = opts->delete_log_on_exit;
    atomic_init(&process->lifecycle, CBM_SUBPROCESS_ACTIVE);
    cbm_subprocess_result_init(&process->result);
    return process;
}

static cbm_tail_result_t cbm_subprocess_poll_log(cbm_subprocess_t *process, bool final) {
    cbm_tail_result_t result = cbm_tail_log(process->log_file, &process->tail_pos,
                                            process->on_log_line, process->log_ud, final);
    if (result.progressed) {
        process->last_activity_ms = cbm_now_ms();
    }
    return result;
}

static bool cbm_subprocess_cancellation_requested(const cbm_subprocess_t *process) {
    return atomic_load_explicit(&process->lifecycle, memory_order_acquire) ==
           CBM_SUBPROCESS_CANCEL_REQUESTED;
}

static void cbm_subprocess_delete_log(cbm_subprocess_t *process) {
    if (!process->log_file || !process->delete_log_on_exit) {
        return;
    }
#ifdef _WIN32
    wchar_t *path = cbm_path_to_wide(process->log_file);
    if (path) {
        (void)DeleteFileW(path);
        free(path);
    }
#else
    (void)unlink(process->log_file);
#endif
}

static bool cbm_subprocess_begin_terminal_transition(cbm_subprocess_t *process) {
    int lifecycle = atomic_load_explicit(&process->lifecycle, memory_order_acquire);
    for (;;) {
        if (lifecycle == CBM_SUBPROCESS_DRAINING) {
            return true;
        }
        if (lifecycle == CBM_SUBPROCESS_TERMINAL) {
            return false;
        }
        int desired = CBM_SUBPROCESS_DRAINING;
        if (atomic_compare_exchange_weak_explicit(&process->lifecycle, &lifecycle, desired,
                                                  memory_order_acq_rel, memory_order_acquire)) {
            process->result.cancellation_requested = lifecycle == CBM_SUBPROCESS_CANCEL_REQUESTED;
            return true;
        }
    }
}

static cbm_proc_poll_t cbm_subprocess_publish_terminal(cbm_subprocess_t *process,
                                                       cbm_proc_result_t *out, bool delete_log) {
    if (delete_log) {
        cbm_subprocess_delete_log(process);
    }
    atomic_store_explicit(&process->lifecycle, CBM_SUBPROCESS_TERMINAL, memory_order_release);
    if (out) {
        *out = process->result;
    }
    return CBM_PROC_POLL_TERMINAL;
}

static cbm_proc_poll_t cbm_subprocess_finish(cbm_subprocess_t *process, cbm_proc_result_t *out) {
    if (!cbm_subprocess_begin_terminal_transition(process)) {
        return CBM_PROC_POLL_ERROR;
    }
    process->result.tree_quiesced = true;
    process->result.supervision_failed = false;
    if (process->log_file && process->on_log_line) {
        return CBM_PROC_POLL_RUNNING;
    }
    return cbm_subprocess_publish_terminal(process, out, true);
}

static cbm_proc_poll_t cbm_subprocess_finish_failed(cbm_subprocess_t *process,
                                                    cbm_proc_result_t *out) {
    if (!cbm_subprocess_begin_terminal_transition(process)) {
        return CBM_PROC_POLL_ERROR;
    }
    process->result.tree_quiesced = false;
    process->result.supervision_failed = true;
    process->result.forced = true;
    if (process->timed_out) {
        process->result.outcome = CBM_PROC_HANG;
    } else if (process->result.outcome == CBM_PROC_SPAWN_FAILED) {
        process->result.outcome = CBM_PROC_KILLED;
    }
    process->containment_failed = true;
    return cbm_subprocess_publish_terminal(process, out, false);
}

#ifdef _WIN32

static void cbm_win_close_spawn_handles(HANDLE nul, HANDLE log, LPPROC_THREAD_ATTRIBUTE_LIST attrs,
                                        bool attrs_init) {
    if (attrs) {
        if (attrs_init) {
            DeleteProcThreadAttributeList(attrs);
        }
        free(attrs);
    }
    if (log != INVALID_HANDLE_VALUE) {
        CloseHandle(log);
    }
    if (nul != INVALID_HANDLE_VALUE) {
        CloseHandle(nul);
    }
}

static int cbm_subprocess_spawn_win(cbm_subprocess_t *process) {
    char cmdline[8192];
    bool built =
        process->windows_cmd_payload
            ? cbm_build_win_cmd_payload(cmdline, sizeof(cmdline), process->bin,
                                        process->windows_cmd_payload)
            : cbm_build_win_cmdline(cmdline, sizeof(cmdline), (const char *const *)process->argv);
    if (!built) {
        return -1;
    }
    wchar_t *wbin = cbm_utf8_to_wide(process->bin);
    wchar_t *wcmdline = cbm_utf8_to_wide(cmdline);
    if (!wbin || !wcmdline) {
        free(wbin);
        free(wcmdline);
        return -1;
    }

    JOBOBJECT_EXTENDED_LIMIT_INFORMATION limits;
    ZeroMemory(&limits, sizeof(limits));
    limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
    HANDLE job = CreateJobObjectW(NULL, NULL);
    if (!job ||
        !SetInformationJobObject(job, JobObjectExtendedLimitInformation, &limits, sizeof(limits))) {
        if (job) {
            CloseHandle(job);
        }
        free(wbin);
        free(wcmdline);
        return -1;
    }

    SECURITY_ATTRIBUTES security;
    ZeroMemory(&security, sizeof(security));
    security.nLength = sizeof(security);
    security.bInheritHandle = TRUE;
    HANDLE nul = CreateFileW(L"NUL", GENERIC_READ | GENERIC_WRITE,
                             FILE_SHARE_READ | FILE_SHARE_WRITE, &security, OPEN_EXISTING, 0, NULL);
    HANDLE log = INVALID_HANDLE_VALUE;
    if (nul == INVALID_HANDLE_VALUE) {
        CloseHandle(job);
        free(wbin);
        free(wcmdline);
        return -1;
    }
    if (process->log_file) {
        wchar_t *wlog = cbm_path_to_wide(process->log_file);
        if (wlog) {
            /* FILE_SHARE_WRITE keeps POSIX parity: on unix nothing stops a
             * second producer from appending to the redirected log while the
             * child holds it (supervision wrappers rely on that), and the
             * log's owner-only directory is what gates who can. Without it
             * the child's handle mandatory-locks every other writer out. */
            log = CreateFileW(wlog, GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE, &security,
                              CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
            free(wlog);
        }
        if (log == INVALID_HANDLE_VALUE) {
            CloseHandle(nul);
            CloseHandle(job);
            free(wbin);
            free(wcmdline);
            return -1;
        }
    }

    HANDLE inherit[2];
    SIZE_T inherit_count = 0;
    inherit[inherit_count++] = nul;
    if (log != INVALID_HANDLE_VALUE) {
        inherit[inherit_count++] = log;
    }
    SIZE_T attrs_size = 0;
    (void)InitializeProcThreadAttributeList(NULL, 1, 0, &attrs_size);
    LPPROC_THREAD_ATTRIBUTE_LIST attrs = (LPPROC_THREAD_ATTRIBUTE_LIST)malloc(attrs_size);
    bool attrs_init = attrs && InitializeProcThreadAttributeList(attrs, 1, 0, &attrs_size);
    bool attrs_ready = attrs_init && UpdateProcThreadAttribute(
                                         attrs, 0, PROC_THREAD_ATTRIBUTE_HANDLE_LIST, inherit,
                                         inherit_count * sizeof(inherit[0]), NULL, NULL);
    if (!attrs_ready) {
        cbm_win_close_spawn_handles(nul, log, attrs, attrs_init);
        CloseHandle(job);
        free(wbin);
        free(wcmdline);
        return -1;
    }

    STARTUPINFOEXW startup;
    ZeroMemory(&startup, sizeof(startup));
    startup.StartupInfo.cb = sizeof(startup);
    startup.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
    startup.StartupInfo.hStdInput = nul;
    startup.StartupInfo.hStdOutput = log != INVALID_HANDLE_VALUE ? log : nul;
    startup.StartupInfo.hStdError = log != INVALID_HANDLE_VALUE ? log : nul;
    startup.lpAttributeList = attrs;

    PROCESS_INFORMATION child;
    ZeroMemory(&child, sizeof(child));
    DWORD flags = EXTENDED_STARTUPINFO_PRESENT | CREATE_SUSPENDED | CREATE_NEW_PROCESS_GROUP;
    BOOL created = CreateProcessW(wbin, wcmdline, NULL, NULL, TRUE, flags, NULL, NULL,
                                  &startup.StartupInfo, &child);
    cbm_win_close_spawn_handles(nul, log, attrs, attrs_init);
    free(wbin);
    free(wcmdline);
    if (!created) {
        CloseHandle(job);
        return -1;
    }

    /* Assignment while suspended closes the process-creation race: no worker
     * instruction executes unless all descendants are already contained. */
    if (!AssignProcessToJobObject(job, child.hProcess)) {
        (void)TerminateProcess(child.hProcess, 1);
        (void)WaitForSingleObject(child.hProcess, 5000);
        CloseHandle(child.hThread);
        CloseHandle(child.hProcess);
        CloseHandle(job);
        return -1;
    }
    if (ResumeThread(child.hThread) == (DWORD)-1) {
        /* KILL_ON_JOB_CLOSE terminates the still-suspended contained process. */
        CloseHandle(job);
        (void)WaitForSingleObject(child.hProcess, 5000);
        CloseHandle(child.hThread);
        CloseHandle(child.hProcess);
        return -1;
    }

    CloseHandle(child.hThread);
    process->process = child.hProcess;
    process->process_id = child.dwProcessId;
    process->job = job;
    return 0;
}

static bool cbm_win_job_active(cbm_subprocess_t *process, bool *known) {
    JOBOBJECT_BASIC_ACCOUNTING_INFORMATION accounting;
    ZeroMemory(&accounting, sizeof(accounting));
    if (!QueryInformationJobObject(process->job, JobObjectBasicAccountingInformation, &accounting,
                                   sizeof(accounting), NULL)) {
        *known = false;
        return true; /* fail closed: an unqueryable job is never declared quiescent */
    }
    *known = true;
    return accounting.ActiveProcesses != 0;
}

static void cbm_win_begin_termination(cbm_subprocess_t *process, uint64_t now) {
    if (process->termination_started) {
        return;
    }
    process->termination_started = true;
    process->termination_started_ms = now;
    /* Best effort only: services and detached parents may have no shared console.
     * The Job Object remains the authoritative force-containment boundary. */
    (void)GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, process->process_id);
}

static void cbm_win_force_tree(cbm_subprocess_t *process, uint64_t now) {
    if (process->force_sent) {
        return;
    }
    if (process->force_started_ms == 0) {
        process->force_started_ms = now;
    }
    if (TerminateJobObject(process->job, 1)) {
        if (!process->root_reaped) {
            process->root_forced = true;
        }
        process->result.forced = true;
        process->force_sent = true;
    } else {
        process->containment_failed = true;
    }
}

static void cbm_win_capture_root(cbm_subprocess_t *process, DWORD code) {
    process->root_reaped = true;
    process->result.exit_code = (int)code;
    process->result.term_signal = 0;
    if (process->timed_out) {
        process->result.outcome = CBM_PROC_HANG;
    } else if (process->root_forced ||
               (cbm_subprocess_cancellation_requested(process) && code == CBM_WIN_CONTROL_C_EXIT)) {
        process->result.outcome = CBM_PROC_KILLED;
    } else {
        process->result.outcome = cbm_proc_classify(true, (int)code, 0, false);
    }
}

static cbm_proc_poll_t cbm_subprocess_poll_win(cbm_subprocess_t *process, cbm_proc_result_t *out) {
    uint64_t now = cbm_now_ms();

    if (!process->root_reaped) {
        DWORD waited = WaitForSingleObject(process->process, 0);
        if (waited == WAIT_OBJECT_0) {
            DWORD code = 1;
            (void)GetExitCodeProcess(process->process, &code);
            cbm_win_capture_root(process, code);
        } else if (waited == WAIT_FAILED) {
            DWORD code = STILL_ACTIVE;
            if (GetExitCodeProcess(process->process, &code) && code != STILL_ACTIVE) {
                cbm_win_capture_root(process, code);
            } else {
                cbm_win_begin_termination(process, now);
                cbm_win_force_tree(process, now);
            }
        }
    }

    bool job_known = false;
    bool job_active = cbm_win_job_active(process, &job_known);
    if (!process->termination_started) {
        if (cbm_subprocess_cancellation_requested(process)) {
            cbm_win_begin_termination(process, now);
        } else if (!process->root_reaped && process->quiet_timeout_ms > 0 &&
                   now - process->last_activity_ms >= (uint64_t)process->quiet_timeout_ms) {
            process->timed_out = true;
            cbm_win_begin_termination(process, now);
        } else if (process->root_reaped && job_active) {
            /* Preserve the root's classification while draining escaped work. */
            cbm_win_begin_termination(process, now);
        }
    }
    if (!job_known) {
        cbm_win_begin_termination(process, now);
        cbm_win_force_tree(process, now);
    }
    if (process->termination_started && job_active && !process->force_sent &&
        now - process->termination_started_ms >= (uint64_t)process->cancel_grace_ms) {
        cbm_win_force_tree(process, now);
    }
    if (process->force_started_ms != 0 &&
        now - process->force_started_ms >= CBM_SUBPROCESS_FORCE_SETTLE_MS &&
        (!job_known || job_active || !process->root_reaped)) {
        return cbm_subprocess_finish_failed(process, out);
    }
    if (process->root_reaped && !job_active) {
        return cbm_subprocess_finish(process, out);
    }
    return CBM_PROC_POLL_RUNNING;
}

#else /* POSIX */

static void cbm_posix_reset_child_signals(void) {
    struct sigaction action = {0};
    action.sa_handler = SIG_DFL;
    (void)sigemptyset(&action.sa_mask);
    for (int sig = 1; sig < NSIG; sig++) {
        if (sig != SIGKILL && sig != SIGSTOP) {
            (void)sigaction(sig, &action, NULL);
        }
    }
    sigset_t empty;
    (void)sigemptyset(&empty);
    (void)sigprocmask(SIG_SETMASK, &empty, NULL);
}

static void cbm_posix_child_exec(cbm_subprocess_t *process, int input, int output, long max_fd) {
    if (setpgid(0, 0) < 0) {
        _exit(127);
    }
    cbm_posix_reset_child_signals();

    /* Never let a worker consume the MCP transport inherited as stdin. Only
     * async-signal-safe calls are used between fork and exec. */
    if (input < 0 || output < 0 || dup2(input, STDIN_FILENO) < 0 ||
        dup2(output, STDOUT_FILENO) < 0 || dup2(output, STDERR_FILENO) < 0) {
        _exit(127);
    }
    if (input > STDERR_FILENO) {
        (void)close(input);
    }
    if (output > STDERR_FILENO) {
        (void)close(output);
    }
    for (int fd = STDERR_FILENO + 1; fd < max_fd; fd++) {
        (void)close(fd);
    }
    /* A fixed literal tool name (for example "git" or "curl") uses the
     * caller's normal PATH without introducing a shell. An explicit path
     * still has execvp's exact-path semantics because it contains '/'. */
    execvp(process->bin, process->argv);
    _exit(127);
}

static int cbm_posix_fd_at_least_three(int fd) {
    if (fd < 0 || fd > STDERR_FILENO) {
        return fd;
    }
#ifdef F_DUPFD_CLOEXEC
    int duplicate = fcntl(fd, F_DUPFD_CLOEXEC, STDERR_FILENO + 1);
#else
    int duplicate = fcntl(fd, F_DUPFD, STDERR_FILENO + 1);
    if (duplicate >= 0) {
        (void)fcntl(duplicate, F_SETFD, FD_CLOEXEC);
    }
#endif
    (void)close(fd);
    return duplicate;
}

static int cbm_subprocess_spawn_posix(cbm_subprocess_t *process) {
    int input_flags = O_RDONLY;
#ifdef O_CLOEXEC
    input_flags |= O_CLOEXEC;
#endif
    int input = cbm_posix_fd_at_least_three(open("/dev/null", input_flags));
    const char *target = process->log_file ? process->log_file : "/dev/null";
    int output_flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef O_CLOEXEC
    output_flags |= O_CLOEXEC;
#endif
#ifdef O_NOFOLLOW
    output_flags |= O_NOFOLLOW;
#endif
    int output = cbm_posix_fd_at_least_three(open(target, output_flags, 0600));
    if (input < 0 || output < 0) {
        if (input >= 0) {
            (void)close(input);
        }
        if (output >= 0) {
            (void)close(output);
        }
        return -1;
    }
    struct stat output_status;
    if (fstat(output, &output_status) != 0 ||
        (process->log_file && !S_ISREG(output_status.st_mode))) {
        (void)close(input);
        (void)close(output);
        return -1;
    }
    if (process->log_file && fchmod(output, 0600) != 0) {
        (void)close(input);
        (void)close(output);
        return -1;
    }

    long max_fd = sysconf(_SC_OPEN_MAX);
    if (max_fd < 0 || max_fd > 1048576L) {
        max_fd = 65536L;
    }

    pid_t pid = fork();
    if (pid < 0) {
        (void)close(input);
        (void)close(output);
        return -1;
    }
    if (pid == 0) {
        cbm_posix_child_exec(process, input, output, max_fd);
    }
    (void)close(input);
    (void)close(output);

    /* Parent and child both establish the group, removing scheduler-order races.
     * If the child won and already execed, EACCES is accepted only after proving
     * that its process group is the expected isolated one. */
    bool contained = setpgid(pid, pid) == 0;
    if (!contained && (errno == EACCES || errno == EPERM || errno == ESRCH)) {
        contained = getpgid(pid) == pid;
    }
    if (!contained) {
        (void)kill(pid, SIGKILL);
        int status = 0;
        for (int attempt = 0; attempt < 4; attempt++) {
            if (waitpid(pid, &status, 0) >= 0 || errno != EINTR) {
                break;
            }
        }
        return -1;
    }

    process->pid = pid;
    process->pgid = pid;
    return 0;
}

static bool cbm_posix_group_active(cbm_subprocess_t *process) {
    if (kill(-process->pgid, 0) == 0) {
        return true;
    }
    return errno != ESRCH; /* EPERM/other errors fail closed as still active */
}

static void cbm_posix_begin_termination(cbm_subprocess_t *process, uint64_t now) {
    if (process->termination_started) {
        return;
    }
    process->termination_started = true;
    process->termination_started_ms = now;
    (void)kill(-process->pgid, SIGTERM);
}

static void cbm_posix_force_tree(cbm_subprocess_t *process, uint64_t now) {
    if (process->force_sent) {
        return;
    }
    if (process->force_started_ms == 0) {
        process->force_started_ms = now;
    }
    if (kill(-process->pgid, SIGKILL) == 0) {
        process->result.forced = true;
        process->force_sent = true;
    } else if (errno == ESRCH) {
        process->force_sent = true;
    } else {
        process->containment_failed = true;
    }
}

static void cbm_posix_capture_root(cbm_subprocess_t *process, int status) {
    process->root_reaped = true;
    if (WIFEXITED(status)) {
        process->result.exit_code = WEXITSTATUS(status);
        process->result.term_signal = 0;
        process->result.outcome =
            cbm_proc_classify(true, process->result.exit_code, 0, process->timed_out);
    } else if (WIFSIGNALED(status)) {
        process->result.exit_code = -1;
        process->result.term_signal = WTERMSIG(status);
        process->result.outcome =
            cbm_proc_classify(false, -1, process->result.term_signal, process->timed_out);
    } else {
        process->result.exit_code = -1;
        process->result.term_signal = 0;
        process->result.outcome = process->timed_out ? CBM_PROC_HANG : CBM_PROC_KILLED;
    }
}

static cbm_proc_poll_t cbm_subprocess_poll_posix(cbm_subprocess_t *process,
                                                 cbm_proc_result_t *out) {
    uint64_t now = cbm_now_ms();

    if (!process->root_reaped) {
        int status = 0;
        pid_t waited = waitpid(process->pid, &status, WNOHANG);
        if (waited == process->pid) {
            cbm_posix_capture_root(process, status);
        } else if (waited < 0 && errno != EINTR) {
            /* ECHILD means another reaper consumed the status. Other permanent
             * wait failures are treated the same: retain containment, stop the
             * tree, and never spin forever on the failed wait operation. */
            process->root_reaped = true;
            process->result.outcome = process->timed_out ? CBM_PROC_HANG : CBM_PROC_KILLED;
            process->result.exit_code = -1;
            process->result.term_signal = 0;
            cbm_posix_begin_termination(process, now);
        }
    }

    bool group_active = cbm_posix_group_active(process);
    if (!process->termination_started) {
        if (cbm_subprocess_cancellation_requested(process)) {
            cbm_posix_begin_termination(process, now);
        } else if (!process->root_reaped && process->quiet_timeout_ms > 0 &&
                   now - process->last_activity_ms >= (uint64_t)process->quiet_timeout_ms) {
            process->timed_out = true;
            cbm_posix_begin_termination(process, now);
        } else if (process->root_reaped && group_active) {
            /* A root that daemonizes children is not terminal. Preserve its exit
             * classification, but drain the descendants through the same path. */
            cbm_posix_begin_termination(process, now);
        }
    }
    if (process->termination_started && group_active && !process->force_sent &&
        now - process->termination_started_ms >= (uint64_t)process->cancel_grace_ms) {
        cbm_posix_force_tree(process, now);
    }
    group_active = cbm_posix_group_active(process);
    if (process->force_started_ms != 0 && group_active &&
        now - process->force_started_ms >= CBM_SUBPROCESS_FORCE_SETTLE_MS) {
        return cbm_subprocess_finish_failed(process, out);
    }
    if (process->root_reaped && !group_active) {
        return cbm_subprocess_finish(process, out);
    }
    return CBM_PROC_POLL_RUNNING;
}

#endif /* _WIN32 */

int cbm_subprocess_spawn(const cbm_proc_opts_t *opts, cbm_subprocess_t **out) {
    if (!out) {
        return -1;
    }
    *out = NULL;
    cbm_subprocess_t *process = cbm_subprocess_copy_opts(opts);
    if (!process) {
        return -1;
    }
#ifdef _WIN32
    int spawn_rc = cbm_subprocess_spawn_win(process);
#else
    int spawn_rc = cbm_subprocess_spawn_posix(process);
#endif
    if (spawn_rc != 0) {
        cbm_subprocess_free_config(process);
        return -1;
    }
    process->last_activity_ms = cbm_now_ms();
    *out = process;
    return 0;
}

cbm_proc_poll_t cbm_subprocess_poll(cbm_subprocess_t *process, cbm_proc_result_t *out) {
    if (!process) {
        return CBM_PROC_POLL_ERROR;
    }
    int lifecycle = atomic_load_explicit(&process->lifecycle, memory_order_acquire);
    if (lifecycle == CBM_SUBPROCESS_TERMINAL) {
        if (out) {
            *out = process->result;
        }
        return CBM_PROC_POLL_TERMINAL;
    }
    if (lifecycle == CBM_SUBPROCESS_DRAINING) {
        cbm_tail_result_t tail = cbm_subprocess_poll_log(process, true);
        if (tail.status == CBM_TAIL_MORE) {
            return CBM_PROC_POLL_RUNNING;
        }
        if (tail.status == CBM_TAIL_ERROR) {
            cbm_log_error("subprocess.log_drain_failed", "reason", "io_error");
            return cbm_subprocess_publish_terminal(process, out, false);
        }
        return cbm_subprocess_publish_terminal(process, out, true);
    }
    /* The one owner-thread tail batch for this public poll. Platform-specific
     * reap paths never tail again, preserving the exact per-poll work cap. */
    (void)cbm_subprocess_poll_log(process, false);
#ifdef _WIN32
    return cbm_subprocess_poll_win(process, out);
#else
    return cbm_subprocess_poll_posix(process, out);
#endif
}

bool cbm_subprocess_request_cancel(cbm_subprocess_t *process) {
    if (!process) {
        return false;
    }
    int lifecycle = atomic_load_explicit(&process->lifecycle, memory_order_acquire);
    for (;;) {
        if (lifecycle == CBM_SUBPROCESS_CANCEL_REQUESTED) {
            return true;
        }
        if (lifecycle != CBM_SUBPROCESS_ACTIVE) {
            return false;
        }
        int desired = CBM_SUBPROCESS_CANCEL_REQUESTED;
        if (atomic_compare_exchange_weak_explicit(&process->lifecycle, &lifecycle, desired,
                                                  memory_order_acq_rel, memory_order_acquire)) {
            return true;
        }
    }
}

void cbm_subprocess_destroy(cbm_subprocess_t *process) {
    if (!process || atomic_load_explicit(&process->lifecycle, memory_order_acquire) !=
                        CBM_SUBPROCESS_TERMINAL) {
        return;
    }
#ifdef _WIN32
    CloseHandle(process->process);
    CloseHandle(process->job);
#endif
    cbm_subprocess_free_config(process);
}

int cbm_subprocess_run(const cbm_proc_opts_t *opts, cbm_proc_result_t *out) {
    cbm_proc_result_t local;
    if (!out) {
        out = &local;
    }
    cbm_subprocess_result_init(out);

    cbm_subprocess_t *process = NULL;
    if (cbm_subprocess_spawn(opts, &process) != 0) {
        return -1;
    }
    for (;;) {
        cbm_proc_poll_t state = cbm_subprocess_poll(process, out);
        if (state == CBM_PROC_POLL_TERMINAL) {
            bool supervised = out->tree_quiesced && !out->supervision_failed;
            cbm_subprocess_destroy(process);
            return supervised ? 0 : -1;
        }
        /* A valid owned handle has no ERROR state; invalid-argument errors are
         * rejected before this compatibility loop is entered. */
        const struct timespec delay = {0, 10000000L}; /* 10 ms */
        (void)cbm_nanosleep(&delay, NULL);
    }
}
