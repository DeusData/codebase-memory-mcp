/*
 * subprocess.c — cross-platform spawn + supervise + classify.
 * See subprocess.h. The spawn/reap skeleton mirrors src/ui/http_server.c's
 * index subprocess; this generalizes it and adds crash/hang classification.
 */
#include "subprocess.h"

#include "compat.h"   /* cbm_nanosleep */
#include "platform.h" /* cbm_now_ms */

#include <stdio.h>
#include <string.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

/* NTSTATUS severity ERROR (top two bits set) covers the Windows crash exception
 * exit codes: 0xC0000005 (access violation), 0xC00000FD (stack overflow),
 * 0xC000001D (illegal instruction), 0xC0000094 (integer divide by zero), … */
#define CBM_WIN_CRASH_CODE_MIN 0xC0000000u

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

/* Tail newly-appended complete lines from the child log, starting at *tail_pos.
 * A partial (non-newline-terminated) final line is left buffered: *tail_pos is
 * not advanced past it, so it is re-read once completed. Returns true if any
 * complete line was consumed (i.e. there was progress). */
static bool cbm_tail_log(const char *log_file, long *tail_pos, cbm_proc_log_cb cb, void *ud) {
    if (!log_file) {
        return false;
    }
    FILE *lf = fopen(log_file, "r");
    if (!lf) {
        return false;
    }
    bool progressed = false;
    if (fseek(lf, *tail_pos, SEEK_SET) == 0) {
        char line[1024];
        for (;;) {
            long before = ftell(lf);
            if (!fgets(line, sizeof(line), lf)) {
                break;
            }
            size_t l = strlen(line);
            bool complete = (l > 0 && line[l - 1] == '\n');
            if (complete) {
                line[l - 1] = '\0';
                *tail_pos = ftell(lf);
                progressed = true;
                if (line[0] && cb) {
                    cb(line, ud);
                }
            } else if (l == sizeof(line) - 1) {
                /* Oversized line filled the buffer without a newline — consume it
                 * anyway (counts as progress) so we never stall on one long line. */
                *tail_pos = ftell(lf);
                progressed = true;
                if (cb) {
                    cb(line, ud);
                }
            } else {
                /* Genuine partial final line — keep it buffered for next poll. */
                *tail_pos = before;
                break;
            }
        }
    }
    fclose(lf);
    return progressed;
}

#ifdef _WIN32

static int cbm_run_win(const cbm_proc_opts_t *opts, cbm_proc_result_t *out) {
    const char *bin = opts->bin;
    const char *const default_argv[] = {bin, NULL};
    const char *const *argv = opts->argv ? opts->argv : default_argv;

    /* Build a quoted command line from argv. */
    char cmdline[8192];
    size_t pos = 0;
    for (int i = 0; argv[i]; i++) {
        int n = snprintf(cmdline + pos, sizeof(cmdline) - pos, "%s\"%s\"", (i ? " " : ""), argv[i]);
        if (n < 0 || (size_t)n >= sizeof(cmdline) - pos) {
            out->outcome = CBM_PROC_SPAWN_FAILED;
            out->exit_code = -1;
            out->term_signal = 0;
            return -1;
        }
        pos += (size_t)n;
    }

    HANDLE hlog = INVALID_HANDLE_VALUE;
    STARTUPINFOA si = {.cb = sizeof(si)};
    if (opts->log_file) {
        hlog = CreateFileA(opts->log_file, GENERIC_WRITE, FILE_SHARE_READ, NULL, CREATE_ALWAYS,
                           FILE_ATTRIBUTE_NORMAL, NULL);
        if (hlog != INVALID_HANDLE_VALUE) {
            si.dwFlags = STARTF_USESTDHANDLES;
            si.hStdError = hlog;
            si.hStdOutput = hlog;
        }
    }

    PROCESS_INFORMATION pi = {0};
    BOOL ok = CreateProcessA(NULL, cmdline, NULL, NULL, TRUE, 0, NULL, NULL, &si, &pi);
    if (hlog != INVALID_HANDLE_VALUE) {
        CloseHandle(hlog);
    }
    if (!ok) {
        out->outcome = CBM_PROC_SPAWN_FAILED;
        out->exit_code = -1;
        out->term_signal = 0;
        return -1;
    }

    long tail_pos = 0;
    uint64_t last_activity = cbm_now_ms();
    bool timed_out = false;
    for (;;) {
        DWORD w = WaitForSingleObject(pi.hProcess, 200);
        if (cbm_tail_log(opts->log_file, &tail_pos, opts->on_log_line, opts->log_ud)) {
            last_activity = cbm_now_ms();
        }
        if (w == WAIT_OBJECT_0) {
            break;
        }
        if (opts->quiet_timeout_ms > 0 &&
            (cbm_now_ms() - last_activity) >= (uint64_t)opts->quiet_timeout_ms) {
            TerminateProcess(pi.hProcess, 1);
            WaitForSingleObject(pi.hProcess, INFINITE);
            timed_out = true;
            break;
        }
    }

    DWORD code = 1;
    GetExitCodeProcess(pi.hProcess, &code);
    CloseHandle(pi.hProcess);
    CloseHandle(pi.hThread);
    if (opts->log_file && opts->delete_log_on_exit) {
        DeleteFileA(opts->log_file);
    }

    out->exit_code = (int)code;
    out->term_signal = 0;
    out->outcome = cbm_proc_classify(true, (int)code, 0, timed_out);
    return 0;
}

#else /* POSIX */

static int cbm_run_posix(const cbm_proc_opts_t *opts, cbm_proc_result_t *out) {
    pid_t pid = fork();
    if (pid < 0) {
        out->outcome = CBM_PROC_SPAWN_FAILED;
        out->exit_code = -1;
        out->term_signal = 0;
        return -1;
    }
    if (pid == 0) {
        /* Child: redirect stdout+stderr to the log (or discard), then exec.
         * Use open()+dup2() (async-signal-safe, no malloc) rather than freopen():
         * the parent may be multithreaded (the MCP server holds worker/watcher/http
         * threads plus mimalloc/sqlite/libgit2 global state), and a fork() copies
         * only the calling thread — a malloc between fork and exec could deadlock on
         * a lock another thread held at fork time. open/dup2/execv touch no heap. */
        const char *bin = opts->bin;
        const char *const default_argv[] = {bin, NULL};
        const char *const *argv = opts->argv ? opts->argv : default_argv;
        const char *target = opts->log_file ? opts->log_file : "/dev/null";
        int fd = open(target, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd >= 0) {
            (void)dup2(fd, STDOUT_FILENO);
            (void)dup2(fd, STDERR_FILENO);
            if (fd > STDERR_FILENO) {
                (void)close(fd);
            }
        }
        execv(bin, (char *const *)argv);
        _exit(127); /* exec failed */
    }

    long tail_pos = 0;
    uint64_t last_activity = cbm_now_ms();
    bool timed_out = false;
    int wstatus = 0;
    for (;;) {
        pid_t wr;
        do {
            wr = waitpid(pid, &wstatus, WNOHANG);
        } while (wr < 0 && errno == EINTR);
        bool done = (wr == pid);

        if (cbm_tail_log(opts->log_file, &tail_pos, opts->on_log_line, opts->log_ud)) {
            last_activity = cbm_now_ms();
        }
        if (done) {
            break;
        }
        if (opts->quiet_timeout_ms > 0 &&
            (cbm_now_ms() - last_activity) >= (uint64_t)opts->quiet_timeout_ms) {
            kill(pid, SIGKILL);
            do {
                wr = waitpid(pid, &wstatus, 0);
            } while (wr < 0 && errno == EINTR);
            timed_out = true;
            break;
        }
        struct timespec ts = {0, 100000000L}; /* 100 ms poll */
        cbm_nanosleep(&ts, NULL);
    }

    if (opts->log_file && opts->delete_log_on_exit) {
        (void)unlink(opts->log_file);
    }

    if (WIFEXITED(wstatus)) {
        out->exit_code = WEXITSTATUS(wstatus);
        out->term_signal = 0;
        out->outcome = cbm_proc_classify(true, out->exit_code, 0, timed_out);
    } else if (WIFSIGNALED(wstatus)) {
        out->exit_code = -1;
        out->term_signal = WTERMSIG(wstatus);
        out->outcome = cbm_proc_classify(false, -1, out->term_signal, timed_out);
    } else {
        out->exit_code = -1;
        out->term_signal = 0;
        out->outcome = timed_out ? CBM_PROC_HANG : CBM_PROC_KILLED;
    }
    return 0;
}

#endif

int cbm_subprocess_run(const cbm_proc_opts_t *opts, cbm_proc_result_t *out) {
    cbm_proc_result_t local;
    if (!out) {
        out = &local;
    }
    out->outcome = CBM_PROC_SPAWN_FAILED;
    out->exit_code = -1;
    out->term_signal = 0;
    if (!opts || !opts->bin || !opts->bin[0]) {
        return -1;
    }
#ifdef _WIN32
    return cbm_run_win(opts, out);
#else
    return cbm_run_posix(opts, out);
#endif
}
