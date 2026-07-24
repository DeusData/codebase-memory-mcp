/*
 * subprocess.h — spawn a child process, supervise it, and classify how it ended.
 *
 * Generalized from the crash-isolating index spawn in src/ui/http_server.c so the
 * crash/hang supervisor (Track C) can reuse one primitive across platforms.
 *
 * Beyond a plain spawn+wait it adds the two things a supervisor needs and the
 * ad-hoc harness lacked:
 *   1. Exit CLASSIFICATION — {clean, exit-nonzero, crash, hang, killed} — from
 *      POSIX WIFSIGNALED/WTERMSIG and the Windows NTSTATUS exception exit codes
 *      (0xC0000005 access-violation, 0xC00000FD stack-overflow, …).
 *   2. A quiet-timeout — kill + report HANG when the child makes no progress
 *      (emits no new log line) for a configurable window. This catches external
 *      tree-sitter scanners that infinite-loop (a hang, not a crash).
 *
 * The reap loop is EINTR-safe. Line tailing keeps a partial final line buffered
 * while the child tree can still write, then delivers that final fragment once
 * the tree is quiescent.
 */
#ifndef CBM_SUBPROCESS_H
#define CBM_SUBPROCESS_H

#include <stdbool.h>
#include <stddef.h> /* size_t (cbm_build_win_cmdline) */

/* How a supervised child ended. */
typedef enum {
    CBM_PROC_CLEAN = 0,    /* exited with code 0 */
    CBM_PROC_EXIT_NONZERO, /* exited with a nonzero code (a graceful failure) */
    CBM_PROC_CRASH,        /* died from a fault: POSIX SIGSEGV/BUS/ILL/FPE/ABRT/SYS,
                            * or a Windows NTSTATUS exception exit code (>= 0xC0000000) */
    CBM_PROC_HANG,         /* made no progress within the quiet-timeout; we killed it */
    CBM_PROC_KILLED,       /* terminated by a non-fault signal we did not initiate */
    CBM_PROC_SPAWN_FAILED  /* fork/exec/CreateProcess failed — no child ever ran */
} cbm_proc_outcome_t;

typedef struct {
    cbm_proc_outcome_t outcome;
    int exit_code;               /* WEXITSTATUS / GetExitCodeProcess; -1 for a POSIX signal */
    int term_signal;             /* WTERMSIG on POSIX; 0 otherwise */
    bool cancellation_requested; /* an explicit cancel request was accepted before terminal */
    bool forced; /* force was needed after grace expiry or to reap descendants after root exit */
    bool tree_quiesced;      /* the owned process tree has no surviving processes */
    bool supervision_failed; /* the bounded containment deadline expired; tree_quiesced is false */
} cbm_proc_result_t;

/* Called synchronously for each newly-completed log chunk while the child runs.
 * Newline-terminated lines are delivered without their newline; oversized lines
 * may be split into 1023-byte chunks, and the final unterminated remainder is
 * delivered after the tree is quiescent. Each delivered chunk resets the quiet
 * timeout. Poll bounds callback work by chunk/byte count, not elapsed time, so
 * callbacks must return promptly. */
typedef void (*cbm_proc_log_cb)(const char *line, void *ud);

typedef struct {
    const char *bin;                 /* executable path or literal PATH name;
                                      * also argv[0] when argv is NULL */
    const char *const *argv;         /* NULL-terminated argv; NULL => { bin, NULL } */
    const char *windows_cmd_payload; /* Windows-only cmd.exe command text. When set, bin must
                                      * be an absolute path ending in cmd.exe and argv must be
                                      * NULL. The fixed /D /S /V:OFF /C prefix is added while
                                      * this payload is copied verbatim for cmd.exe to parse. */
    const char *log_file;            /* child stdout+stderr are redirected here and tailed;
                                      * NULL => discard child output, no tailing */
    cbm_proc_log_cb on_log_line;     /* optional per-line callback */
    void *log_ud;                    /* user data for on_log_line */
    int quiet_timeout_ms;            /* <= 0 => no timeout; else kill+HANG after this many
                                      * ms with no new completed log line */
    int cancel_grace_ms;             /* graceful tree-termination window; <= 0 uses the finite
                                      * CBM_SUBPROCESS_DEFAULT_CANCEL_GRACE_MS */
    bool delete_log_on_exit;         /* unlink log_file after reaping */
} cbm_proc_opts_t;

#define CBM_SUBPROCESS_DEFAULT_CANCEL_GRACE_MS 1000
#define CBM_SUBPROCESS_MAX_CANCEL_GRACE_MS 1000
#define CBM_SUBPROCESS_FORCE_SETTLE_MS 1000

/* Opaque, owned supervisor for one child and its contained descendant tree.
 * POSIX implementations contain the child in its own process group; Windows
 * implementations use a Job Object. The containment is what makes a terminal
 * result stronger than merely observing that the direct child exited. */
typedef struct cbm_subprocess cbm_subprocess_t;

typedef enum {
    CBM_PROC_POLL_ERROR = -1, /* invalid handle/arguments */
    CBM_PROC_POLL_RUNNING = 0,
    CBM_PROC_POLL_TERMINAL = 1
} cbm_proc_poll_t;

/* Spawn opts->bin and return immediately with a supervisor handle. On success,
 * *out owns the process until a terminal poll followed by destroy. Spawn copies
 * the option strings/argv it needs after return; log_ud remains caller-owned until
 * terminal. Returns 0 on success or -1 on validation/spawn failure (*out == NULL).
 *
 * A quiet timeout begins at successful spawn and is reset by each completed log
 * line. Expiry starts the same graceful->forced tree shutdown as explicit cancel,
 * but the terminal outcome remains CBM_PROC_HANG. */
int cbm_subprocess_spawn(const cbm_proc_opts_t *opts, cbm_subprocess_t **out);

/* Advance supervision without sleeping or waiting for the child. RUNNING means
 * the caller must poll again. TERMINAL is returned only after the direct child is
 * reaped AND the entire owned process tree is quiescent; *out is then filled with
 * the cached immutable result. Every later poll returns TERMINAL with that same
 * result. If an OS containment operation remains failed through the bounded
 * force-settle deadline, TERMINAL is still returned but supervision_failed is
 * true and tree_quiesced is false; callers must log this as a critical teardown
 * failure. *out is optional and is not modified for RUNNING or ERROR.
 *
 * Each poll delivers at most 64 log chunks / 64 KiB. On the normal callback
 * delivery path, RUNNING may continue after the process tree is quiescent while
 * remaining log batches drain, and TERMINAL follows successful catch-up. A
 * final-drain I/O error instead terminates without changing process-tree
 * classification and does not attempt to delete the log.
 *
 * Poll performs the graceful->force state transition: after explicit cancel (or
 * quiet-timeout), it requests graceful termination once, then force-terminates the
 * tree when cancel_grace_ms elapses. Callers must keep polling to make progress. */
cbm_proc_poll_t cbm_subprocess_poll(cbm_subprocess_t *process, cbm_proc_result_t *out);

/* Record an explicit cancellation request without waiting. Safe to repeat and
 * safe to call from a cancellation thread while one owner thread polls. The
 * owner must stop cancellation producers before destroying the handle. true
 * means the process tree is live and cancellation is now/already pending; false
 * means process is NULL, draining terminal logs, or already terminal. Poll
 * performs signal delivery/escalation. */
bool cbm_subprocess_request_cancel(cbm_subprocess_t *process);

/* Release a terminal handle. This never waits or implicitly cancels; passing a
 * still-running handle violates the API contract. NULL is a no-op. */
void cbm_subprocess_destroy(cbm_subprocess_t *process);

/* Spawn opts->bin, supervise (tail + optional quiet-timeout), block until it ends,
 * and classify the result into *out. Compatibility wrapper around spawn + repeated
 * poll + bounded sleeps. Returns 0 if a child was spawned and its tree reached
 * terminal (out filled), or -1 if spawning/supervision failed
 * (out->outcome == CBM_PROC_SPAWN_FAILED). */
int cbm_subprocess_run(const cbm_proc_opts_t *opts, cbm_proc_result_t *out);

/* Pure outcome classifier — exposed so the platform-specific exit-code mapping
 * (notably the Windows NTSTATUS crash codes) is unit-testable on every platform.
 *   exited_normally: the child returned an exit code (POSIX WIFEXITED; always true
 *                    on Windows, which has no signals — crashes surface as codes).
 *   exit_code:       the exit / exception code (meaningful when exited_normally).
 *   term_signal:     POSIX terminating signal (meaningful when !exited_normally).
 *   timed_out:       we killed the child for exceeding the quiet-timeout. */
cbm_proc_outcome_t cbm_proc_classify(bool exited_normally, int exit_code, int term_signal,
                                     bool timed_out);

/* Stable lowercase name for an outcome (for structured logs / skip reasons). */
const char *cbm_proc_outcome_str(cbm_proc_outcome_t o);

/* Build a Windows CreateProcess command line from a NULL-terminated argv, applying
 * the Microsoft C runtime quoting rules (quote-wrap + escape embedded quotes and
 * their preceding backslashes) so the spawned child re-parses byte-identical argv.
 * Returns true on success, false on overflow (on overflow buf is set to an empty
 * string, never left unterminated).
 *
 * CreateProcess re-parses a SINGLE command string into argv, so a naive `"%s"` wrap
 * silently corrupts any element containing a double-quote — e.g. the index worker's
 * JSON arg {"repo_path":"…"} arrives as {repo_path:…}, the Windows index-worker bug.
 * Exposed (and compiled on every platform — it is pure string logic) so the quoting
 * is unit-tested on Linux/macOS CI, and so both spawn sites (cbm_subprocess_run and
 * the UI http_server index spawn) escape through one shared, tested implementation. */
bool cbm_build_win_cmdline(char *buf, size_t cap, const char *const *argv);

/* Build the special CreateProcess command line needed to invoke cmd.exe with a
 * command-language payload. Unlike cbm_build_win_cmdline(), payload is not a C
 * runtime argv element: cmd.exe must receive its embedded quotes and backslashes
 * unchanged. cmd_executable must be an absolute Windows path whose basename is
 * cmd.exe. The result is:
 *
 *   "<absolute-cmd.exe>" /D /S /V:OFF /C <payload-verbatim>
 *
 * Returns false for invalid input or overflow and leaves a valid empty string
 * whenever buf/cap permit one. Pure string logic, available on every platform
 * so the Windows serialization contract is unit-testable everywhere. */
bool cbm_build_win_cmd_payload(char *buf, size_t cap, const char *cmd_executable,
                               const char *payload);

#endif /* CBM_SUBPROCESS_H */
