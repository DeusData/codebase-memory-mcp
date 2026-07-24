/*
 * test_subprocess.c — foundation/subprocess: spawn + supervise + classify.
 *
 * Two layers:
 *   1. cbm_proc_classify() — pure, exercised on EVERY platform (so the Windows
 *      NTSTATUS crash-code mapping is guarded on Linux/macOS CI too, not just an
 *      untested Windows branch).
 *   2. cbm_subprocess_run() — real spawn/reap, exercised on POSIX via /bin/sh
 *      (SKIP_PLATFORM on Windows, which lacks it).
 */
#include "test_framework.h"
#include "../src/foundation/subprocess.h"
#include "../src/foundation/compat.h"
#include "../src/foundation/platform.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>

#ifndef _WIN32
#include <fcntl.h>
#include <signal.h>
#include <unistd.h>
#endif

/* ── Layer 1: pure classifier (all platforms) ─────────────────────────────── */

TEST(subprocess_classify_clean) {
    ASSERT_EQ(cbm_proc_classify(true, 0, 0, false), CBM_PROC_CLEAN);
    PASS();
}

TEST(subprocess_classify_exit_nonzero) {
    ASSERT_EQ(cbm_proc_classify(true, 3, 0, false), CBM_PROC_EXIT_NONZERO);
    ASSERT_EQ(cbm_proc_classify(true, 127, 0, false), CBM_PROC_EXIT_NONZERO);
    PASS();
}

/* Windows exception exit codes classify as CRASH — the key reason to unit-test
 * the classifier cross-platform. 0xC0000005 = access violation (SIGSEGV analog),
 * 0xC00000FD = stack overflow (the #668 reporter's 0xC00000FD). */
TEST(subprocess_classify_windows_crash_codes) {
    ASSERT_EQ(cbm_proc_classify(true, (int)0xC0000005u, 0, false), CBM_PROC_CRASH);
    ASSERT_EQ(cbm_proc_classify(true, (int)0xC00000FDu, 0, false), CBM_PROC_CRASH);
    ASSERT_EQ(cbm_proc_classify(true, (int)0xC000001Du, 0, false), CBM_PROC_CRASH);
    ASSERT_EQ(cbm_proc_classify(true, (int)0xC000013Au, 0, false), CBM_PROC_KILLED);
    PASS();
}

TEST(subprocess_classify_posix_fault_signal_is_crash) {
#ifndef _WIN32
    ASSERT_EQ(cbm_proc_classify(false, -1, SIGSEGV, false), CBM_PROC_CRASH);
    ASSERT_EQ(cbm_proc_classify(false, -1, SIGABRT, false), CBM_PROC_CRASH);
    ASSERT_EQ(cbm_proc_classify(false, -1, SIGBUS, false), CBM_PROC_CRASH);
#endif
    PASS();
}

TEST(subprocess_classify_non_fault_signal_is_killed) {
#ifndef _WIN32
    ASSERT_EQ(cbm_proc_classify(false, -1, SIGTERM, false), CBM_PROC_KILLED);
    ASSERT_EQ(cbm_proc_classify(false, -1, SIGKILL, false), CBM_PROC_KILLED);
#endif
    PASS();
}

/* timed_out dominates every other signal — a killed-for-hang child is HANG,
 * not KILLED, even though we deliver SIGKILL/TerminateProcess to end it. */
TEST(subprocess_classify_timeout_dominates) {
    ASSERT_EQ(cbm_proc_classify(false, -1, 9 /*SIGKILL*/, true), CBM_PROC_HANG);
    ASSERT_EQ(cbm_proc_classify(true, 0, 0, true), CBM_PROC_HANG);
    PASS();
}

TEST(subprocess_outcome_str) {
    ASSERT_STR_EQ(cbm_proc_outcome_str(CBM_PROC_CLEAN), "clean");
    ASSERT_STR_EQ(cbm_proc_outcome_str(CBM_PROC_CRASH), "crash");
    ASSERT_STR_EQ(cbm_proc_outcome_str(CBM_PROC_HANG), "hang");
    ASSERT_STR_EQ(cbm_proc_outcome_str(CBM_PROC_EXIT_NONZERO), "exit_nonzero");
    ASSERT_STR_EQ(cbm_proc_outcome_str(CBM_PROC_KILLED), "killed");
    PASS();
}

/* ── Layer 2: real spawn/reap (POSIX) ─────────────────────────────────────── */

#ifndef _WIN32
static cbm_proc_result_t run_sh(const char *script, int quiet_timeout_ms) {
    const char *argv[] = {"/bin/sh", "-c", script, NULL};
    cbm_proc_opts_t opts = {0};
    opts.bin = "/bin/sh";
    opts.argv = argv;
    opts.log_file = NULL;
    opts.quiet_timeout_ms = quiet_timeout_ms;
    cbm_proc_result_t r;
    cbm_subprocess_run(&opts, &r);
    return r;
}
#endif

TEST(subprocess_run_clean) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX /bin/sh spawn");
#else
    cbm_proc_result_t r = run_sh("exit 0", 0);
    ASSERT_EQ(r.outcome, CBM_PROC_CLEAN);
    ASSERT_EQ(r.exit_code, 0);
    PASS();
#endif
}

TEST(subprocess_run_exit_nonzero) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX /bin/sh spawn");
#else
    cbm_proc_result_t r = run_sh("exit 7", 0);
    ASSERT_EQ(r.outcome, CBM_PROC_EXIT_NONZERO);
    ASSERT_EQ(r.exit_code, 7);
    PASS();
#endif
}

/* Daemon background helpers intentionally invoke fixed tool names such as
 * `curl` and `git`. A shell-free spawn must still perform the normal PATH
 * lookup for a name without a directory separator; exact binary paths keep
 * their existing exec semantics. */
TEST(subprocess_run_resolves_literal_binary_name_from_path) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX PATH/execvp semantics");
#else
    const char *argv[] = {"sh", "-c", "exit 0", NULL};
    cbm_proc_opts_t opts = {
        .bin = "sh",
        .argv = argv,
    };
    cbm_proc_result_t result;
    int rc = cbm_subprocess_run(&opts, &result);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(result.outcome, CBM_PROC_CLEAN);
    ASSERT_EQ(result.exit_code, 0);
    PASS();
#endif
}

/* A child that dies of SIGSEGV must classify as CRASH — NOT exit_nonzero and NOT
 * killed. This is the whole point of the primitive: distinguish a crash from a
 * clean failure so the supervisor can quarantine the culprit. */
TEST(subprocess_run_crash_is_crash) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX signal semantics");
#else
    cbm_proc_result_t r = run_sh("kill -SEGV $$", 0);
    ASSERT_EQ(r.outcome, CBM_PROC_CRASH);
    ASSERT_EQ(r.term_signal, SIGSEGV);
    PASS();
#endif
}

/* A child that makes no progress within the quiet-timeout is killed and reported
 * as HANG — the sibling failure mode of a crash (external-scanner infinite loop). */
TEST(subprocess_run_hang_is_hang) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX /bin/sh spawn");
#else
    cbm_proc_result_t r = run_sh("sleep 30", 300 /* ms quiet-timeout */);
    ASSERT_EQ(r.outcome, CBM_PROC_HANG);
    PASS();
#endif
}

/* A spawn of a non-existent binary fails cleanly (no child), not a crash. */
TEST(subprocess_run_spawn_failure) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX exec semantics");
#else
    /* execvp of a bogus path: fork succeeds, child _exit(127). We classify the
     * reaped 127 as exit_nonzero — spawn_failed is reserved for fork() failing. */
    const char *argv[] = {"/nonexistent/cbm-bogus-binary", NULL};
    cbm_proc_opts_t opts = {0};
    opts.bin = "/nonexistent/cbm-bogus-binary";
    opts.argv = argv;
    cbm_proc_result_t r;
    int rc = cbm_subprocess_run(&opts, &r);
    ASSERT_EQ(rc, 0); /* fork itself succeeded */
    ASSERT_EQ(r.outcome, CBM_PROC_EXIT_NONZERO);
    ASSERT_EQ(r.exit_code, 127);
    PASS();
#endif
}

TEST(subprocess_run_null_bin_rejected) {
    cbm_proc_opts_t opts = {0};
    opts.bin = NULL;
    cbm_proc_result_t r;
    int rc = cbm_subprocess_run(&opts, &r);
    ASSERT_EQ(rc, -1);
    ASSERT_EQ(r.outcome, CBM_PROC_SPAWN_FAILED);
    PASS();
}

/* ── Layer 3: nonblocking handle + whole-tree cancellation (POSIX) ──────────
 *
 * The daemon cannot block its coordinator thread in cbm_subprocess_run(). It
 * owns an opaque handle, polls it, and requests cancellation when the final job
 * subscriber leaves. These probes deliberately use /bin/sh descendants which
 * ignore SIGTERM: a direct-child-only implementation leaves the grandchild alive,
 * while a correct process-group implementation escalates and reports quiescence.
 * All test-side waits have explicit monotonic deadlines. */

#ifndef _WIN32

static void subprocess_test_pause(void) {
    const struct timespec delay = {0, 10000000L}; /* 10 ms */
    (void)cbm_nanosleep(&delay, NULL);
}

static bool poll_until_terminal(cbm_subprocess_t *process, int timeout_ms, cbm_proc_result_t *out) {
    uint64_t deadline = cbm_now_ms() + (uint64_t)timeout_ms;
    do {
        cbm_proc_poll_t state = cbm_subprocess_poll(process, out);
        if (state == CBM_PROC_POLL_TERMINAL) {
            return true;
        }
        if (state == CBM_PROC_POLL_ERROR) {
            return false;
        }
        subprocess_test_pause();
    } while (cbm_now_ms() < deadline);
    return cbm_subprocess_poll(process, out) == CBM_PROC_POLL_TERMINAL;
}

static bool make_tree_pid_path(char path[64]) {
    strcpy(path, "/tmp/cbm-subprocess-tree-XXXXXX");
    int fd = cbm_mkstemp(path);
    if (fd < 0) {
        return false;
    }
    (void)close(fd);
    return unlink(path) == 0; /* child creates it only after both traps are installed */
}

static bool wait_for_tree_pids(const char *path, cbm_subprocess_t *process, pid_t *parent_pid,
                               pid_t *grandchild_pid, int timeout_ms) {
    uint64_t deadline = cbm_now_ms() + (uint64_t)timeout_ms;
    do {
        FILE *f = fopen(path, "r");
        if (f) {
            long parent_value = 0;
            long grandchild_value = 0;
            int fields = fscanf(f, "%ld %ld", &parent_value, &grandchild_value);
            fclose(f);
            if (fields == 2 && parent_value > 1 && grandchild_value > 1) {
                *parent_pid = (pid_t)parent_value;
                *grandchild_pid = (pid_t)grandchild_value;
                return true;
            }
        }
        cbm_proc_result_t ignored;
        if (cbm_subprocess_poll(process, &ignored) != CBM_PROC_POLL_RUNNING) {
            return false;
        }
        subprocess_test_pause();
    } while (cbm_now_ms() < deadline);
    return false;
}

static bool wait_pid_gone(pid_t pid, int timeout_ms) {
    uint64_t deadline = cbm_now_ms() + (uint64_t)timeout_ms;
    do {
        errno = 0;
        if (kill(pid, 0) < 0 && errno == ESRCH) {
            return true;
        }
        subprocess_test_pause();
    } while (cbm_now_ms() < deadline);
    errno = 0;
    return kill(pid, 0) < 0 && errno == ESRCH;
}

/* Best-effort cleanup for a failing implementation, so a red tree test does not
 * leave its TERM-ignoring probes behind for later tests. The production API must
 * still report terminal itself; callers never use this escape hatch. */
static void force_probe_cleanup(pid_t parent_pid, pid_t grandchild_pid) {
    if (parent_pid > 1) {
        (void)kill(-parent_pid, SIGKILL);
        (void)kill(parent_pid, SIGKILL);
    }
    if (grandchild_pid > 1) {
        (void)kill(grandchild_pid, SIGKILL);
    }
}

static int spawn_ignoring_tree(const char *pid_path, int quiet_timeout_ms, int cancel_grace_ms,
                               cbm_subprocess_t **out) {
    /* The direct child installs its trap before starting a nested shell. The two
     * PIDs are written only after the nested process exists, eliminating the
     * cancellation-before-trap race from the test. */
    const char *script = "trap '' TERM; "
                         "/bin/sh -c 'trap \"\" TERM; while :; do sleep 1; done' cbm-grandchild & "
                         "grandchild=$!; echo \"$$ $grandchild\" > \"$1\"; wait";
    const char *argv[] = {"/bin/sh", "-c", script, "cbm-parent", pid_path, NULL};
    cbm_proc_opts_t opts = {0};
    opts.bin = "/bin/sh";
    opts.argv = argv;
    opts.quiet_timeout_ms = quiet_timeout_ms;
    opts.cancel_grace_ms = cancel_grace_ms;
    return cbm_subprocess_spawn(&opts, out);
}

typedef struct {
    cbm_subprocess_t *process;
    int count;
    bool ordered;
    bool late_cancel_attempted;
    bool late_cancel_accepted;
} subprocess_log_capture_t;

static void ordered_log_callback(const char *line, void *opaque) {
    subprocess_log_capture_t *capture = opaque;
    int index = -1;
    char trailing = '\0';
    bool parsed = sscanf(line, "line-%d%c", &index, &trailing) == 1;
    capture->ordered = capture->ordered && parsed && index == capture->count;
    capture->count++;
    if (index == 399) {
        capture->late_cancel_attempted = true;
        capture->late_cancel_accepted = cbm_subprocess_request_cancel(capture->process);
    }
}

static bool wait_for_log_marker(const char *path, const char *marker, uint64_t deadline_ms) {
    char contents[8192];
    do {
        FILE *file = fopen(path, "rb");
        if (file) {
            size_t used = fread(contents, 1, sizeof(contents) - 1, file);
            contents[used] = '\0';
            (void)fclose(file);
            if (strstr(contents, marker)) {
                return true;
            }
        }
        subprocess_test_pause();
    } while (cbm_now_ms() < deadline_ms);
    return false;
}

#endif /* !_WIN32 */

TEST(subprocess_spawn_returns_while_child_is_running) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX /bin/sh nonblocking spawn probe; native Job Object coverage pending");
#else
    const char *argv[] = {"/bin/sh", "-c", "sleep 4", NULL};
    cbm_proc_opts_t opts = {0};
    opts.bin = "/bin/sh";
    opts.argv = argv;
    opts.cancel_grace_ms = 100;
    cbm_subprocess_t *process = NULL;

    uint64_t before_spawn = cbm_now_ms();
    int spawn_rc = cbm_subprocess_spawn(&opts, &process);
    uint64_t spawn_elapsed = cbm_now_ms() - before_spawn;
    ASSERT_EQ(spawn_rc, 0);
    ASSERT_NOT_NULL(process);

    cbm_proc_result_t result;
    uint64_t before_poll = cbm_now_ms();
    cbm_proc_poll_t first_poll = cbm_subprocess_poll(process, &result);
    uint64_t poll_elapsed = cbm_now_ms() - before_poll;
    bool cancel_accepted = cbm_subprocess_request_cancel(process);
    bool terminal = poll_until_terminal(process, 2000, &result);
    if (terminal) {
        cbm_subprocess_destroy(process);
    }

    /* first_poll == RUNNING is the authoritative proof of non-blocking: the
     * 4s child is still running when spawn+poll returned. The wall-clock bounds
     * are only a coarse backstop against a regression that blocks on the child;
     * they stay well under the 4s child so heavy scheduler starvation (the
     * CBM_LOCAL_CI_CPUS=4 fidelity pass, CI runners) can never make them
     * test-significant. */
    ASSERT_EQ(first_poll, CBM_PROC_POLL_RUNNING);
    ASSERT_LT(spawn_elapsed, 3500);
    ASSERT_LT(poll_elapsed, 3500);
    ASSERT_TRUE(cancel_accepted);
    ASSERT_TRUE(terminal);
    ASSERT_TRUE(result.cancellation_requested);
    ASSERT_TRUE(result.tree_quiesced);
    PASS();
#endif
}

TEST(subprocess_natural_completion_is_cached_across_polls) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX /bin/sh completion-cache probe; native Job Object coverage pending");
#else
    const char *argv[] = {"/bin/sh", "-c", "exit 7", NULL};
    cbm_proc_opts_t opts = {0};
    opts.bin = "/bin/sh";
    opts.argv = argv;
    opts.cancel_grace_ms = 100;
    cbm_subprocess_t *process = NULL;
    ASSERT_EQ(cbm_subprocess_spawn(&opts, &process), 0);
    ASSERT_NOT_NULL(process);

    cbm_proc_result_t first;
    ASSERT_TRUE(poll_until_terminal(process, 2000, &first));
    cbm_proc_result_t second;
    cbm_proc_result_t third;
    cbm_proc_poll_t second_poll = cbm_subprocess_poll(process, &second);
    cbm_proc_poll_t third_poll = cbm_subprocess_poll(process, &third);
    cbm_subprocess_destroy(process);

    ASSERT_EQ(second_poll, CBM_PROC_POLL_TERMINAL);
    ASSERT_EQ(third_poll, CBM_PROC_POLL_TERMINAL);
    ASSERT_EQ(first.outcome, CBM_PROC_EXIT_NONZERO);
    ASSERT_EQ(first.exit_code, 7);
    ASSERT_EQ(second.outcome, first.outcome);
    ASSERT_EQ(second.exit_code, first.exit_code);
    ASSERT_EQ(second.term_signal, first.term_signal);
    ASSERT_EQ(second.cancellation_requested, first.cancellation_requested);
    ASSERT_EQ(second.forced, first.forced);
    ASSERT_EQ(second.tree_quiesced, first.tree_quiesced);
    ASSERT_EQ(third.outcome, first.outcome);
    ASSERT_EQ(third.exit_code, first.exit_code);
    ASSERT_EQ(third.term_signal, first.term_signal);
    ASSERT_EQ(third.cancellation_requested, first.cancellation_requested);
    ASSERT_EQ(third.forced, first.forced);
    ASSERT_EQ(third.tree_quiesced, first.tree_quiesced);
    ASSERT_FALSE(first.cancellation_requested);
    ASSERT_FALSE(first.forced);
    ASSERT_TRUE(first.tree_quiesced);
    PASS();
#endif
}

TEST(subprocess_cancel_is_idempotent_and_kills_ignoring_tree) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX process-group probe; native Windows Job Object tree probe pending");
#else
    char pid_path[64];
    ASSERT_TRUE(make_tree_pid_path(pid_path));
    cbm_subprocess_t *process = NULL;
    ASSERT_EQ(spawn_ignoring_tree(pid_path, 0, 100, &process), 0);
    ASSERT_NOT_NULL(process);

    pid_t parent_pid = -1;
    pid_t grandchild_pid = -1;
    bool ready = wait_for_tree_pids(pid_path, process, &parent_pid, &grandchild_pid, 1000);
    bool first_cancel = ready && cbm_subprocess_request_cancel(process);
    bool second_cancel = ready && cbm_subprocess_request_cancel(process);
    cbm_proc_result_t result;
    bool terminal = ready && poll_until_terminal(process, 2500, &result);
    bool parent_gone = terminal && wait_pid_gone(parent_pid, 1000);
    bool grandchild_gone = terminal && wait_pid_gone(grandchild_pid, 1000);
    if (!terminal) {
        force_probe_cleanup(parent_pid, grandchild_pid);
        cbm_proc_result_t cleanup_result;
        if (poll_until_terminal(process, 1000, &cleanup_result)) {
            cbm_subprocess_destroy(process);
        }
    } else {
        cbm_subprocess_destroy(process);
    }
    (void)unlink(pid_path);

    ASSERT_TRUE(ready);
    ASSERT_TRUE(first_cancel);
    ASSERT_TRUE(second_cancel);
    ASSERT_TRUE(terminal);
    ASSERT_EQ(result.outcome, CBM_PROC_KILLED);
    ASSERT_TRUE(result.cancellation_requested);
    ASSERT_TRUE(result.forced);
    ASSERT_TRUE(result.tree_quiesced);
    ASSERT_TRUE(parent_gone);
    ASSERT_TRUE(grandchild_gone);
    PASS();
#endif
}

TEST(subprocess_quiet_timeout_kills_ignoring_tree) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX process-group probe; native Windows Job Object tree probe pending");
#else
    char pid_path[64];
    ASSERT_TRUE(make_tree_pid_path(pid_path));
    cbm_subprocess_t *process = NULL;
    ASSERT_EQ(spawn_ignoring_tree(pid_path, 750, 100, &process), 0);
    ASSERT_NOT_NULL(process);

    pid_t parent_pid = -1;
    pid_t grandchild_pid = -1;
    bool ready = wait_for_tree_pids(pid_path, process, &parent_pid, &grandchild_pid, 500);
    cbm_proc_result_t result;
    bool terminal = ready && poll_until_terminal(process, 3000, &result);
    bool parent_gone = terminal && wait_pid_gone(parent_pid, 1000);
    bool grandchild_gone = terminal && wait_pid_gone(grandchild_pid, 1000);
    if (!terminal) {
        force_probe_cleanup(parent_pid, grandchild_pid);
        cbm_proc_result_t cleanup_result;
        if (poll_until_terminal(process, 1000, &cleanup_result)) {
            cbm_subprocess_destroy(process);
        }
    } else {
        cbm_subprocess_destroy(process);
    }
    (void)unlink(pid_path);

    ASSERT_TRUE(ready);
    ASSERT_TRUE(terminal);
    ASSERT_EQ(result.outcome, CBM_PROC_HANG);
    ASSERT_FALSE(result.cancellation_requested);
    ASSERT_TRUE(result.forced);
    ASSERT_TRUE(result.tree_quiesced);
    ASSERT_TRUE(parent_gone);
    ASSERT_TRUE(grandchild_gone);
    PASS();
#endif
}

TEST(subprocess_cancel_grace_is_hard_capped) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX process-group grace cap probe; native Windows coverage pending");
#else
    char pid_path[64];
    ASSERT_TRUE(make_tree_pid_path(pid_path));
    cbm_subprocess_t *process = NULL;
    ASSERT_EQ(spawn_ignoring_tree(pid_path, 0, INT_MAX, &process), 0);
    ASSERT_NOT_NULL(process);

    pid_t parent_pid = -1;
    pid_t grandchild_pid = -1;
    bool ready = wait_for_tree_pids(pid_path, process, &parent_pid, &grandchild_pid, 1000);
    bool cancel_accepted = ready && cbm_subprocess_request_cancel(process);
    cbm_proc_result_t result = {0};
    bool terminal = cancel_accepted && poll_until_terminal(process, 3500, &result);
    if (!terminal) {
        force_probe_cleanup(parent_pid, grandchild_pid);
        cbm_proc_result_t cleanup_result;
        if (poll_until_terminal(process, 1000, &cleanup_result)) {
            cbm_subprocess_destroy(process);
        }
    } else {
        cbm_subprocess_destroy(process);
    }
    (void)unlink(pid_path);

    ASSERT_TRUE(ready);
    ASSERT_TRUE(cancel_accepted);
    ASSERT_TRUE(terminal);
    ASSERT_TRUE(result.forced);
    ASSERT_TRUE(result.tree_quiesced);
    PASS();
#endif
}

TEST(subprocess_poll_log_delivery_is_bounded_and_terminal_is_lossless) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX shell log budget probe; native Windows coverage pending");
#else
    char log_path[] = "/tmp/cbm-subprocess-log-budget-XXXXXX";
    int log_fd = cbm_mkstemp(log_path);
    ASSERT_TRUE(log_fd >= 0);
    (void)close(log_fd);

    const char *script =
        "i=0; while [ $i -lt 399 ]; do echo line-$i; i=$((i+1)); done; printf line-399";
    const char *argv[] = {"/bin/sh", "-c", script, NULL};
    subprocess_log_capture_t capture = {.ordered = true};
    cbm_proc_opts_t opts = {0};
    opts.bin = "/bin/sh";
    opts.argv = argv;
    opts.log_file = log_path;
    opts.on_log_line = ordered_log_callback;
    opts.log_ud = &capture;
    opts.cancel_grace_ms = 100;
    opts.delete_log_on_exit = true;
    cbm_subprocess_t *process = NULL;
    ASSERT_EQ(cbm_subprocess_spawn(&opts, &process), 0);
    ASSERT_NOT_NULL(process);
    capture.process = process;

    bool backlog_ready = wait_for_log_marker(log_path, "line-399", cbm_now_ms() + 2000U);
    cbm_proc_result_t result = {0};
    bool terminal = false;
    int max_poll_delivery = 0;
    uint64_t deadline = cbm_now_ms() + 5000U;
    while (backlog_ready && cbm_now_ms() < deadline) {
        int before = capture.count;
        cbm_proc_poll_t state = cbm_subprocess_poll(process, &result);
        int delivered = capture.count - before;
        if (delivered > max_poll_delivery) {
            max_poll_delivery = delivered;
        }
        if (state == CBM_PROC_POLL_TERMINAL) {
            terminal = true;
            break;
        }
        if (state == CBM_PROC_POLL_ERROR) {
            break;
        }
        subprocess_test_pause();
    }
    bool log_deleted = access(log_path, F_OK) != 0 && errno == ENOENT;
    if (terminal) {
        cbm_subprocess_destroy(process);
    } else {
        (void)cbm_subprocess_request_cancel(process);
        cbm_proc_result_t cleanup_result;
        if (poll_until_terminal(process, 1000, &cleanup_result)) {
            cbm_subprocess_destroy(process);
        }
    }
    (void)unlink(log_path);

    ASSERT_TRUE(backlog_ready);
    ASSERT_TRUE(terminal);
    ASSERT_EQ(max_poll_delivery, 64);
    ASSERT_EQ(capture.count, 400);
    ASSERT_TRUE(capture.ordered);
    ASSERT_TRUE(capture.late_cancel_attempted);
    ASSERT_FALSE(capture.late_cancel_accepted);
    ASSERT_FALSE(result.cancellation_requested);
    ASSERT_TRUE(log_deleted);
    PASS();
#endif
}

TEST(subprocess_final_log_drain_error_is_terminal_and_preserves_classification) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX log-rename final-drain probe; UTF-8 Windows open uses cbm_fopen");
#else
    char log_path[] = "/tmp/cbm-subprocess-log-drain-XXXXXX";
    int log_fd = cbm_mkstemp(log_path);
    ASSERT_TRUE(log_fd >= 0);
    (void)close(log_fd);
    char saved_path[sizeof(log_path) + 16];
    int saved_written = snprintf(saved_path, sizeof(saved_path), "%s.saved", log_path);
    ASSERT_TRUE(saved_written > 0 && (size_t)saved_written < sizeof(saved_path));

    const char *script = "i=0; while [ $i -lt 130 ]; do echo line-$i; i=$((i+1)); done";
    const char *argv[] = {"/bin/sh", "-c", script, NULL};
    subprocess_log_capture_t capture = {.ordered = true};
    cbm_proc_opts_t opts = {0};
    opts.bin = "/bin/sh";
    opts.argv = argv;
    opts.log_file = log_path;
    opts.on_log_line = ordered_log_callback;
    opts.log_ud = &capture;
    opts.cancel_grace_ms = 100;
    opts.delete_log_on_exit = true;
    cbm_subprocess_t *process = NULL;
    ASSERT_EQ(cbm_subprocess_spawn(&opts, &process), 0);
    ASSERT_NOT_NULL(process);
    capture.process = process;

    bool backlog_ready = wait_for_log_marker(log_path, "line-129", cbm_now_ms() + 2000U);
    cbm_proc_result_t result = {0};
    cbm_proc_poll_t first =
        backlog_ready ? cbm_subprocess_poll(process, &result) : CBM_PROC_POLL_ERROR;
    int first_delivery = capture.count;
    bool moved = first == CBM_PROC_POLL_RUNNING && rename(log_path, saved_path) == 0;
    bool terminal = moved && poll_until_terminal(process, 2000, &result);
    int callbacks_at_terminal = capture.count;
    cbm_proc_result_t cached = {0};
    cbm_proc_poll_t cached_state =
        terminal ? cbm_subprocess_poll(process, &cached) : CBM_PROC_POLL_ERROR;
    bool callbacks_stable = capture.count == callbacks_at_terminal;
    bool saved_preserved = access(saved_path, F_OK) == 0;
    bool original_absent = access(log_path, F_OK) != 0 && errno == ENOENT;
    if (terminal) {
        cbm_subprocess_destroy(process);
    } else {
        (void)cbm_subprocess_request_cancel(process);
        cbm_proc_result_t cleanup_result;
        if (poll_until_terminal(process, 1000, &cleanup_result)) {
            cbm_subprocess_destroy(process);
        }
    }
    (void)unlink(log_path);
    (void)unlink(saved_path);

    ASSERT_TRUE(backlog_ready);
    ASSERT_EQ(first, CBM_PROC_POLL_RUNNING);
    ASSERT_EQ(first_delivery, 64);
    ASSERT_TRUE(moved);
    ASSERT_TRUE(terminal);
    ASSERT_TRUE(capture.ordered);
    ASSERT_EQ(callbacks_at_terminal, 64);
    ASSERT_EQ(cached_state, CBM_PROC_POLL_TERMINAL);
    ASSERT_TRUE(callbacks_stable);
    ASSERT_EQ(result.outcome, CBM_PROC_CLEAN);
    ASSERT_EQ(result.exit_code, 0);
    ASSERT_TRUE(result.tree_quiesced);
    ASSERT_FALSE(result.supervision_failed);
    ASSERT_EQ(cached.outcome, result.outcome);
    ASSERT_EQ(cached.exit_code, result.exit_code);
    ASSERT_EQ(cached.tree_quiesced, result.tree_quiesced);
    ASSERT_TRUE(saved_preserved);
    ASSERT_TRUE(original_absent);
    PASS();
#endif
}

TEST(subprocess_posix_child_closes_unrelated_descriptors) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX descriptor-inheritance probe; Windows uses a handle allow-list");
#else
    char sentinel_path[] = "/tmp/cbm-subprocess-sentinel-XXXXXX";
    int sentinel = cbm_mkstemp(sentinel_path);
    ASSERT_TRUE(sentinel > STDERR_FILENO);
    int flags = fcntl(sentinel, F_GETFD);
    ASSERT_TRUE(flags >= 0);
    ASSERT_EQ(fcntl(sentinel, F_SETFD, flags & ~FD_CLOEXEC), 0);
    char fd_text[32];
    snprintf(fd_text, sizeof(fd_text), "%d", sentinel);
    const char *script = "if [ -e /dev/fd/$1 ]; then exit 42; else exit 0; fi";
    const char *argv[] = {"/bin/sh", "-c", script, "cbm-fd-probe", fd_text, NULL};
    cbm_proc_opts_t opts = {0};
    opts.bin = "/bin/sh";
    opts.argv = argv;
    cbm_proc_result_t result;
    int run_rc = cbm_subprocess_run(&opts, &result);
    (void)close(sentinel);
    (void)unlink(sentinel_path);

    ASSERT_EQ(run_rc, 0);
    ASSERT_EQ(result.outcome, CBM_PROC_CLEAN);
    ASSERT_EQ(result.exit_code, 0);
    PASS();
#endif
}

TEST(subprocess_root_exit_drains_surviving_descendant) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX process-group descendant probe; native Windows coverage pending");
#else
    char pid_path[64];
    ASSERT_TRUE(make_tree_pid_path(pid_path));
    const char *script = "sleep 30 & child=$!; echo $child > \"$1\"; exit 0";
    const char *argv[] = {"/bin/sh", "-c", script, "cbm-root-exit", pid_path, NULL};
    cbm_proc_opts_t opts = {0};
    opts.bin = "/bin/sh";
    opts.argv = argv;
    opts.cancel_grace_ms = 100;
    cbm_subprocess_t *process = NULL;
    ASSERT_EQ(cbm_subprocess_spawn(&opts, &process), 0);
    ASSERT_NOT_NULL(process);

    pid_t descendant = -1;
    uint64_t deadline = cbm_now_ms() + 1000;
    while (descendant <= 1 && cbm_now_ms() < deadline) {
        FILE *file = fopen(pid_path, "r");
        long value = -1;
        if (file) {
            if (fscanf(file, "%ld", &value) == 1) {
                descendant = (pid_t)value;
            }
            (void)fclose(file);
        }
        subprocess_test_pause();
    }
    cbm_proc_result_t result = {0};
    bool terminal = descendant > 1 && poll_until_terminal(process, 2500, &result);
    bool descendant_gone = terminal && wait_pid_gone(descendant, 1000);
    if (!terminal) {
        force_probe_cleanup(-1, descendant);
        cbm_proc_result_t cleanup_result;
        if (poll_until_terminal(process, 1000, &cleanup_result)) {
            cbm_subprocess_destroy(process);
        }
    } else {
        cbm_subprocess_destroy(process);
    }
    (void)unlink(pid_path);

    ASSERT_TRUE(descendant > 1);
    ASSERT_TRUE(terminal);
    ASSERT_TRUE(descendant_gone);
    ASSERT_EQ(result.outcome, CBM_PROC_CLEAN);
    ASSERT_TRUE(result.tree_quiesced);
    PASS();
#endif
}

/* ── Layer 4: Windows command-line quoting (pure; every platform) ─────────────
 *
 * The Windows index-worker "crash" was a quoting bug: the Windows spawn wrapped each
 * argv element in bare quotes without escaping, so a JSON argument like
 * {"repo_path":"C:/r"} lost its inner quotes when the child re-parsed the command
 * line — the worker then failed at JSON-arg parse and exited non-zero, which the
 * supervisor misreported as a per-file crash. We guard cbm_build_win_cmdline by
 * ROUND-TRIP: a reference implementation of the Windows CommandLineToArgvW rules
 * (the inverse of the builder) must re-parse the emitted line back into the exact
 * original argv. Testing the invariant — not a hand-computed escaped string — keeps
 * the guard honest and readable, and runs on Linux/macOS CI (the builder is pure). */

/* Reference re-parser: the subset of CommandLineToArgvW our builder emits (every
 * arg quote-wrapped; \" for embedded quotes; backslashes doubled before a quote). */
static int parse_win_cmdline(const char *cmd, char out[][256], int max_args) {
    int argc = 0;
    const char *p = cmd;
    while (*p) {
        while (*p == ' ' || *p == '\t') {
            p++;
        }
        if (!*p || argc >= max_args) {
            break;
        }
        char *o = out[argc];
        size_t oi = 0;
        bool in_quotes = false;
        /* Guard every write: each out[] row is 256 bytes; test args stay well under
         * that, but cap defensively so a future longer arg fails a length assertion
         * rather than smashing the stack. */
#define PUTO(ch)            \
    do {                    \
        if (oi < 255) {     \
            o[oi++] = (ch); \
        }                   \
    } while (0)
        for (;;) {
            size_t nbs = 0;
            while (*p == '\\') {
                nbs++;
                p++;
            }
            if (*p == '"') {
                for (size_t k = 0; k < nbs / 2; k++) {
                    PUTO('\\');
                }
                if (nbs % 2) {
                    PUTO('"'); /* odd run → the quote is an escaped literal */
                } else {
                    in_quotes = !in_quotes; /* even run → the quote is a delimiter */
                }
                p++;
            } else {
                for (size_t k = 0; k < nbs; k++) {
                    PUTO('\\');
                }
                if (*p == '\0' || (!in_quotes && (*p == ' ' || *p == '\t'))) {
                    break;
                }
                PUTO(*p);
                p++;
            }
        }
#undef PUTO
        o[oi] = '\0';
        argc++;
    }
    return argc;
}

static bool cmdline_roundtrips(const char *const *argv) {
    char cmd[4096];
    if (!cbm_build_win_cmdline(cmd, sizeof(cmd), argv)) {
        return false;
    }
    char parsed[16][256];
    int pc = parse_win_cmdline(cmd, parsed, 16);
    int oc = 0;
    while (argv[oc]) {
        oc++;
    }
    if (pc != oc) {
        return false;
    }
    for (int i = 0; i < oc; i++) {
        if (strcmp(argv[i], parsed[i]) != 0) {
            return false;
        }
    }
    return true;
}

/* The exact index-worker argv: the command line with a JSON arg full of quotes.
 * Round-trips, AND the emitted line must contain an ESCAPED quote (\") — the bare
 * `"%s"` wrap that caused the bug never would. */
TEST(win_cmdline_index_worker_json) {
    const char *const argv[] = {
        "C:/bin/cbm.exe",
        "cli",
        "--index-worker",
        "--index-worker-build",
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "index_repository",
        "{\"repo_path\":\"C:/r\"}",
        "--response-out",
        "C:/c/w.response",
        NULL,
    };
    ASSERT(cmdline_roundtrips(argv));
    char cmd[4096];
    ASSERT(cbm_build_win_cmdline(cmd, sizeof(cmd), argv));
    ASSERT(strstr(cmd, "\\\"repo_path\\\"") != NULL); /* inner quotes are escaped */
    PASS();
}

/* A battery of adversarial argv (spaces, tabs, embedded quotes, backslash runs,
 * trailing backslashes, backslash-before-quote, real Windows paths) must all
 * round-trip byte-for-byte through the builder + reference parser. */
TEST(win_cmdline_roundtrip_battery) {
    const char *const a1[] = {"a", "b", NULL};
    const char *const a2[] = {"has space", "tab\there", NULL};
    const char *const a3[] = {"trailing\\", "a\\b\\c", NULL};
    const char *const a4[] = {"a\\\"b", "\"", "\\\\\"", NULL};
    const char *const a5[] = {"C:\\Users\\me\\my repo", "{\"name\":\"a b\",\"x\":\"y\\\\z\"}",
                              NULL};
    const char *const a6[] = {"", "plain", NULL};
    ASSERT(cmdline_roundtrips(a1));
    ASSERT(cmdline_roundtrips(a2));
    ASSERT(cmdline_roundtrips(a3));
    ASSERT(cmdline_roundtrips(a4));
    ASSERT(cmdline_roundtrips(a5));
    ASSERT(cmdline_roundtrips(a6));
    PASS();
}

/* Overflow is reported (false), never a silent truncation that would spawn a
 * corrupted command line. */
TEST(win_cmdline_overflow_rejected) {
    const char *const argv[] = {"aaaaaaaaaa", "bbbbbbbbbb", NULL};
    char tiny[8];
    ASSERT_FALSE(cbm_build_win_cmdline(tiny, sizeof(tiny), argv));
    PASS();
}

/* Reproduce-first guard for the overflow CONTRACT (subprocess.h): on overflow buf
 * must be left a valid (empty) string. RED on the pre-fix code — the overflow path
 * returned without terminating buf, so buf[0] held the first quoted byte ('"') —
 * and GREEN once the overflow path sets buf[0] = '\0'. */
TEST(win_cmdline_overflow_leaves_empty_string) {
    char buf[8];
    const char *const argv[] = {"averylongprogramname", "x", NULL};
    ASSERT_FALSE(cbm_build_win_cmdline(buf, sizeof(buf), argv)); /* overflows cap */
    ASSERT_EQ(buf[0], '\0'); /* pre-fix: buf[0] == '"' (a partial byte), not NUL */
    PASS();
}

/* cmd.exe /C receives command-language text, not another CRT argv element.
 * Preserve its quoted Windows paths byte-for-byte: generic argv quoting would
 * turn the payload's quotes into backslash-quote sequences before cmd sees it. */
TEST(win_cmd_payload_is_verbatim_and_capacity_checked) {
    const char *cmd = "C:\\Windows\\System32\\cmd.exe";
    const char *payload =
        "git -C \"C:\\Users\\test\\source repo\" diff --name-only \"main\"...HEAD 2>NUL";
    const char *expected =
        "\"C:\\Windows\\System32\\cmd.exe\" /D /S /V:OFF /C "
        "git -C \"C:\\Users\\test\\source repo\" diff --name-only \"main\"...HEAD 2>NUL";
    char result[512];

    ASSERT(cbm_build_win_cmd_payload(result, strlen(expected) + 1, cmd, payload));
    ASSERT_STR_EQ(result, expected);
    ASSERT_NULL(strstr(result, "\\\"C:\\Users"));

    memset(result, 'x', sizeof(result));
    ASSERT_FALSE(cbm_build_win_cmd_payload(result, strlen(expected), cmd, payload));
    ASSERT_EQ(result[0], '\0');
    PASS();
}

/* Input validation must inspect no bytes beyond short strings and must never
 * permit PATH lookup or a different executable behind the raw-payload mode. */
TEST(win_cmd_payload_rejects_short_relative_and_non_cmd_paths) {
    const char *payload = "echo ok";
    char result[256];
    ASSERT_FALSE(cbm_build_win_cmd_payload(result, sizeof(result), "", payload));
    ASSERT_FALSE(cbm_build_win_cmd_payload(result, sizeof(result), "C", payload));
    ASSERT_FALSE(cbm_build_win_cmd_payload(result, sizeof(result), "C:", payload));
    ASSERT_FALSE(cbm_build_win_cmd_payload(result, sizeof(result), "cmd.exe", payload));
    ASSERT_FALSE(
        cbm_build_win_cmd_payload(result, sizeof(result), "C:\\tools\\other.exe", payload));
    ASSERT_FALSE(
        cbm_build_win_cmd_payload(result, sizeof(result), "C:\\tools\\cmd.exe\\child", payload));
    ASSERT(
        cbm_build_win_cmd_payload(result, sizeof(result), "c:/Windows/System32/CMD.EXE", payload));
    PASS();
}

SUITE(subprocess) {
    RUN_TEST(subprocess_classify_clean);
    RUN_TEST(subprocess_classify_exit_nonzero);
    RUN_TEST(subprocess_classify_windows_crash_codes);
    RUN_TEST(subprocess_classify_posix_fault_signal_is_crash);
    RUN_TEST(subprocess_classify_non_fault_signal_is_killed);
    RUN_TEST(subprocess_classify_timeout_dominates);
    RUN_TEST(subprocess_outcome_str);
    RUN_TEST(subprocess_run_clean);
    RUN_TEST(subprocess_run_exit_nonzero);
    RUN_TEST(subprocess_run_resolves_literal_binary_name_from_path);
    RUN_TEST(subprocess_run_crash_is_crash);
    RUN_TEST(subprocess_run_hang_is_hang);
    RUN_TEST(subprocess_run_spawn_failure);
    RUN_TEST(subprocess_run_null_bin_rejected);
    RUN_TEST(subprocess_spawn_returns_while_child_is_running);
    RUN_TEST(subprocess_natural_completion_is_cached_across_polls);
    RUN_TEST(subprocess_cancel_is_idempotent_and_kills_ignoring_tree);
    RUN_TEST(subprocess_quiet_timeout_kills_ignoring_tree);
    RUN_TEST(subprocess_cancel_grace_is_hard_capped);
    RUN_TEST(subprocess_poll_log_delivery_is_bounded_and_terminal_is_lossless);
    RUN_TEST(subprocess_final_log_drain_error_is_terminal_and_preserves_classification);
    RUN_TEST(subprocess_posix_child_closes_unrelated_descriptors);
    RUN_TEST(subprocess_root_exit_drains_surviving_descendant);
    RUN_TEST(win_cmdline_index_worker_json);
    RUN_TEST(win_cmdline_roundtrip_battery);
    RUN_TEST(win_cmdline_overflow_rejected);
    RUN_TEST(win_cmdline_overflow_leaves_empty_string);
    RUN_TEST(win_cmd_payload_is_verbatim_and_capacity_checked);
    RUN_TEST(win_cmd_payload_rejects_short_relative_and_non_cmd_paths);
}
