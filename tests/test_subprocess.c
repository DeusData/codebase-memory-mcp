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
#include "test_helpers.h"
#include "../src/foundation/subprocess.h"
#include "../src/foundation/compat.h"
#include "../src/foundation/platform.h"
#include "../src/foundation/compat_fs.h"
#include "../src/foundation/git_env.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef _WIN32
#include <fcntl.h>
#include <signal.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <unistd.h>
#ifdef __linux__
#include <sys/syscall.h>
#if !defined(SYS_close_range) && (defined(__x86_64__) || defined(__aarch64__))
#define SYS_close_range 436
#endif
#endif
#else
#include <fcntl.h>
#include <io.h>
#include <windows.h>
#include "../src/foundation/win_utf8.h"
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

/* A short command can finish before its parent checks the child's process
 * group; macOS then answers ESRCH for the exited, unreaped child. That is a
 * finished run with its own exit status, never a spawn failure (macOS CI read
 * `git write-tree` as SPAWN_FAILED errno 3 under load). The seam holds the
 * parent until the child has exited: that order, deterministically. */
TEST(subprocess_run_child_exited_before_group_check) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX process groups");
#else
    cbm_subprocess_hold_parent_until_child_exits_for_testing(1);
    cbm_proc_result_t r = run_sh("exit 3", 0);
    cbm_subprocess_hold_parent_until_child_exits_for_testing(0);
    ASSERT_EQ(r.outcome, CBM_PROC_EXIT_NONZERO);
    ASSERT_EQ(r.exit_code, 3);
    PASS();
#endif
}

/* The window before that: macOS answers ESRCH for a child that has started
 * exiting, before waitid can report it. A non-blocking "has it exited?"
 * probe said no there, and macos-15-intel CI read a short `git config` as
 * SPAWN_FAILED errno 3. The seam makes the parent observe exactly that. */
TEST(subprocess_run_child_exiting_at_group_check) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX process groups");
#else
    cbm_subprocess_observe_exiting_child_for_testing(1);
    cbm_proc_result_t r = run_sh("exit 3", 0);
    cbm_subprocess_observe_exiting_child_for_testing(0);
    ASSERT_EQ(r.outcome, CBM_PROC_EXIT_NONZERO);
    ASSERT_EQ(r.exit_code, 3);
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
/* The kernel refusing a spawn with EAGAIN means "not right now", not "never" —
 * a momentarily full process table on a busy machine. We used to treat it as a
 * permanent failure, so a git probe or LSP server refused to start for a reason
 * the user could neither see nor act on, and `subprocess_run_spawn_failure`
 * failed on loaded CI runners with the contract intact.
 *
 * These pin the retry itself rather than the constant. Injecting refusals is
 * deterministic, so this proves the loop retries on EVERY machine instead of
 * only on one that happens to be starved. */
TEST(subprocess_retries_transient_spawn_refusal) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX fork/EAGAIN path");
#else
    /* Fewer refusals than the budget: the spawn must still succeed. */
    cbm_subprocess_force_spawn_eagain_for_testing(3);
    cbm_proc_result_t r = run_sh("exit 0", 0);
    ASSERT_EQ(cbm_subprocess_pending_spawn_eagain_for_testing(), 0);
    ASSERT_EQ(r.outcome, CBM_PROC_CLEAN);
    ASSERT_EQ(r.exit_code, 0);
    PASS();
#endif
}

#ifndef _WIN32
static volatile sig_atomic_t g_spawn_backoff_alarm_count = 0;

static void spawn_backoff_alarm_handler(int signal_number) {
    (void)signal_number;
    g_spawn_backoff_alarm_count++;
}
#endif

TEST(subprocess_spawn_backoff_resumes_after_eintr) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX signal interruption");
#else
    struct sigaction action = {0};
    struct sigaction previous_action = {0};
    action.sa_handler = spawn_backoff_alarm_handler;
    (void)sigemptyset(&action.sa_mask);
    bool handler_installed = sigaction(SIGALRM, &action, &previous_action) == 0;

    struct itimerval timer = {
        .it_interval = {.tv_sec = 0, .tv_usec = 1000},
        .it_value = {.tv_sec = 0, .tv_usec = 1000},
    };
    g_spawn_backoff_alarm_count = 0;
    bool timer_started = handler_installed && setitimer(ITIMER_REAL, &timer, NULL) == 0;

    uint64_t started_at = cbm_now_ms();
    cbm_subprocess_force_spawn_eagain_for_testing(3);
    cbm_proc_result_t result = run_sh("exit 0", 0);
    uint64_t elapsed_ms = cbm_now_ms() - started_at;

    struct itimerval disabled = {0};
    (void)setitimer(ITIMER_REAL, &disabled, NULL);
    if (handler_installed) {
        (void)sigaction(SIGALRM, &previous_action, NULL);
    }

    ASSERT_TRUE(handler_installed);
    ASSERT_TRUE(timer_started);
    ASSERT_TRUE(g_spawn_backoff_alarm_count > 0);
    ASSERT_TRUE(elapsed_ms >= 50);
    ASSERT_EQ(result.outcome, CBM_PROC_CLEAN);
    PASS();
#endif
}

TEST(subprocess_gives_up_after_the_retry_budget) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX fork/EAGAIN path");
#else
    /* More refusals than the budget: it must fail rather than retry forever.
     * A machine still refusing after ~0.6s is genuinely out of capacity, and
     * failing fast beats hanging. */
    cbm_subprocess_force_spawn_eagain_for_testing(50);
    cbm_proc_result_t r = run_sh("exit 0", 0);
    bool refused = r.outcome == CBM_PROC_SPAWN_FAILED;
    cbm_subprocess_force_spawn_eagain_for_testing(0); /* never leak into later tests */
    ASSERT_TRUE(refused);
    PASS();
#endif
}

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
    /* Reusing a previous result must not expose stale Job Object diagnostics
     * when validation rejects the next spawn before a process exists. */
    cbm_proc_result_t r = {
        .job_memory_limit_bytes = 123,
        .peak_job_memory_bytes = 456,
        .job_memory_available = true,
    };
    int rc = cbm_subprocess_run(&opts, &r);
    ASSERT_EQ(rc, -1);
    ASSERT_EQ(r.outcome, CBM_PROC_SPAWN_FAILED);
    ASSERT_EQ(r.exit_code, -1);
    ASSERT_TRUE(r.job_memory_limit_bytes == 0);
    ASSERT_TRUE(r.peak_job_memory_bytes == 0);
    ASSERT_FALSE(r.job_memory_available);
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

TEST(subprocess_windows_job_object_cancellation_quiesces_descendant_tree) {
#ifndef _WIN32
    SKIP_PLATFORM("native Windows Job Object descendant-tree probe");
#else
    char temp_dir[MAX_PATH];
    DWORD temp_length = GetTempPathA((DWORD)sizeof(temp_dir), temp_dir);
    ASSERT_TRUE(temp_length > 0 && temp_length < sizeof(temp_dir));
    char pid_path[MAX_PATH];
    ASSERT_TRUE(GetTempFileNameA(temp_dir, "cbm", 0, pid_path) != 0);
    ASSERT_TRUE(DeleteFileA(pid_path));

    char system_directory[MAX_PATH];
    UINT system_length = GetSystemDirectoryA(system_directory, (UINT)sizeof(system_directory));
    ASSERT_TRUE(system_length > 0 && system_length < sizeof(system_directory));
    char powershell_path[MAX_PATH];
    ASSERT_TRUE(snprintf(powershell_path, sizeof(powershell_path),
                         "%s\\WindowsPowerShell\\v1.0\\powershell.exe", system_directory) > 0);

    char script[4096];
    int script_length = snprintf(
        script, sizeof(script),
        "$child=Start-Process powershell.exe "
        "-ArgumentList '-NoProfile','-Command','while ($true) { Start-Sleep -Milliseconds 100 }' "
        "-WindowStyle Hidden -PassThru; Set-Content -Encoding ASCII -LiteralPath '%s' "
        "-Value ($PID.ToString() + ' ' + $child.Id.ToString()); "
        "while ($true) { Start-Sleep -Milliseconds 100 }",
        pid_path);
    ASSERT_TRUE(script_length > 0 && (size_t)script_length < sizeof(script));
    const char *argv[] = {powershell_path, "-NoProfile", "-Command", script, NULL};

    cbm_proc_opts_t opts = {0};
    opts.bin = powershell_path;
    opts.argv = argv;
    opts.cancel_grace_ms = 100;
    cbm_subprocess_t *process = NULL;
    ASSERT_EQ(cbm_subprocess_spawn(&opts, &process), 0);
    ASSERT_NOT_NULL(process);

    DWORD root_pid = 0;
    DWORD grandchild_pid = 0;
    uint64_t ready_deadline = cbm_now_ms() + 3000U;
    bool ready = false;
    while (cbm_now_ms() < ready_deadline) {
        FILE *pid_file = fopen(pid_path, "r");
        if (pid_file) {
            unsigned long root_value = 0;
            unsigned long grandchild_value = 0;
            int fields = fscanf(pid_file, "%lu %lu", &root_value, &grandchild_value);
            (void)fclose(pid_file);
            if (fields == 2 && root_value > 0 && grandchild_value > 0) {
                root_pid = (DWORD)root_value;
                grandchild_pid = (DWORD)grandchild_value;
                ready = true;
                break;
            }
        }
        cbm_proc_result_t ignored;
        if (cbm_subprocess_poll(process, &ignored) != CBM_PROC_POLL_RUNNING) {
            break;
        }
        Sleep(10);
    }

    HANDLE root_handle =
        ready ? OpenProcess(SYNCHRONIZE | PROCESS_TERMINATE, FALSE, root_pid) : NULL;
    HANDLE grandchild_handle =
        ready ? OpenProcess(SYNCHRONIZE | PROCESS_TERMINATE, FALSE, grandchild_pid) : NULL;
    /* Request cancellation even if the PID probe failed so a failing test never
     * leaves its supervised tree running in the shared Windows VM. */
    bool cancelled = cbm_subprocess_request_cancel(process);
    cbm_proc_result_t result = {0};
    bool terminal = false;
    uint64_t terminal_deadline = cbm_now_ms() + 4000U;
    while (cbm_now_ms() < terminal_deadline) {
        cbm_proc_poll_t state = cbm_subprocess_poll(process, &result);
        if (state == CBM_PROC_POLL_TERMINAL) {
            terminal = true;
            break;
        }
        if (state == CBM_PROC_POLL_ERROR) {
            break;
        }
        Sleep(10);
    }
    /* If the behavior under test regresses, terminate both known Job members
     * directly and give the supervisor one bounded drain interval. The test
     * still fails on the original `terminal` verdict, but it cannot strand its
     * infinite root/grandchild or retain a terminal subprocess/Job handle. */
    bool cleanup_terminal = terminal;
    if (!cleanup_terminal) {
        if (grandchild_handle) {
            (void)TerminateProcess(grandchild_handle, 1);
        }
        if (root_handle) {
            (void)TerminateProcess(root_handle, 1);
        }
        (void)cbm_subprocess_request_cancel(process);
        uint64_t cleanup_deadline = cbm_now_ms() + 2000U;
        while (cbm_now_ms() < cleanup_deadline) {
            cbm_proc_result_t cleanup_result;
            cbm_proc_poll_t state = cbm_subprocess_poll(process, &cleanup_result);
            if (state == CBM_PROC_POLL_TERMINAL) {
                cleanup_terminal = true;
                break;
            }
            if (state == CBM_PROC_POLL_ERROR) {
                break;
            }
            Sleep(10);
        }
    }
    bool root_gone = root_handle && WaitForSingleObject(root_handle, 2000) == WAIT_OBJECT_0;
    bool grandchild_gone =
        grandchild_handle && WaitForSingleObject(grandchild_handle, 2000) == WAIT_OBJECT_0;
    if (root_handle) {
        CloseHandle(root_handle);
    }
    if (grandchild_handle) {
        CloseHandle(grandchild_handle);
    }
    if (cleanup_terminal) {
        cbm_subprocess_destroy(process);
    }
    (void)DeleteFileA(pid_path);

    ASSERT_TRUE(ready);
    ASSERT_TRUE(cancelled);
    ASSERT_TRUE(terminal);
    ASSERT_TRUE(result.cancellation_requested);
    ASSERT_TRUE(result.tree_quiesced);
    ASSERT_FALSE(result.supervision_failed);
    ASSERT_TRUE(root_gone);
    ASSERT_TRUE(grandchild_gone);
    PASS();
#endif
}

TEST(subprocess_windows_job_object_enforces_memory_limit) {
#ifndef _WIN32
    SKIP_PLATFORM("native Windows Job Object memory-limit probe");
#else
    char *self_path = cbm_module_path_utf8();
    ASSERT_TRUE(self_path != NULL);
    const char *argv[] = {self_path, "__cbm_windows_memory_limit_probe", NULL};
    cbm_proc_opts_t opts = {0};
    opts.bin = self_path;
    opts.argv = argv;
    opts.quiet_timeout_ms = 5000;

    /* Without the cap the SAME allocation must succeed. A machine-wide commit
     * shortage must fail this test, not masquerade as Job Object enforcement. */
    cbm_proc_result_t uncapped = {0};
    int uncapped_rc = cbm_subprocess_run(&opts, &uncapped);
    opts.memory_limit_bytes = (size_t)1024U * 1024U * 1024U;
    cbm_proc_result_t result = {0};
    int run_rc = cbm_subprocess_run(&opts, &result);
    free(self_path);
    ASSERT_EQ(uncapped_rc, 0);
    ASSERT_EQ(uncapped.outcome, CBM_PROC_CLEAN);
    ASSERT_EQ(uncapped.exit_code, 0);
    ASSERT_TRUE(uncapped.tree_quiesced);
    ASSERT_FALSE(uncapped.supervision_failed);
    ASSERT_TRUE(uncapped.job_memory_limit_bytes == 0);
    ASSERT_EQ(run_rc, 0);
    ASSERT_EQ(result.outcome, CBM_PROC_EXIT_NONZERO);
    ASSERT_EQ(result.exit_code, 73);
    ASSERT_TRUE(result.tree_quiesced);
    ASSERT_FALSE(result.supervision_failed);
    ASSERT_TRUE(result.job_memory_available);
    ASSERT_TRUE(result.job_memory_limit_bytes == opts.memory_limit_bytes);
    ASSERT_TRUE(result.peak_job_memory_bytes > 0);
    /* Windows may include the denied reservation in its peak counter, so peak
     * can exceed the cap. The uncapped/capped exit codes prove enforcement. */
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

/* #1484: the child's close-inherited-descriptors step must not cost
 * O(RLIMIT_NOFILE) syscalls. The seam runs the exact child routine in this
 * process against two descriptors parked at the TOP of the descriptor table
 * (nothing else lives there), and reports which strategy did the work. On
 * Linux that must be close_range(2) whenever the kernel has it -- the per-fd
 * loop is the bug. The ENOSYS-injected leg proves the fallback still closes. */
#ifndef _WIN32
static bool subprocess_fd_is_closed(int fd) {
    return fcntl(fd, F_GETFD) == -1 && errno == EBADF;
}

static cbm_fd_close_strategy_t subprocess_expected_close_strategy(void) {
#if defined(__linux__) && defined(SYS_close_range)
    /* Probe the kernel on an empty range: an old kernel (< 5.9) or a seccomp
     * filter without it answers ENOSYS/EPERM, and then the loop is correct. */
    long probe = syscall(SYS_close_range, ~0U, ~0U, 0U);
    return (probe == 0 || errno == EINVAL) ? CBM_FD_CLOSE_RANGE : CBM_FD_CLOSE_LOOP;
#elif defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__) || defined(__DragonFly__)
    return CBM_FD_CLOSEFROM;
#else
    return CBM_FD_CLOSE_LOOP;
#endif
}

/* Park two non-CLOEXEC descriptors near the top of the table; returns the lower
 * of the two and hands back the original (low) descriptor in *keep_low. The
 * kernel may cap the table below _SC_OPEN_MAX (macOS: kern.maxfilesperproc),
 * so the start point halves until F_DUPFD accepts it. */
static int subprocess_park_high_fds(int *keep_low) {
    char path[] = "/tmp/cbm-subprocess-highfd-XXXXXX";
    int low = cbm_mkstemp(path);
    if (low < 0) {
        return -1;
    }
    (void)unlink(path);
    long top = sysconf(_SC_OPEN_MAX);
    if (top <= 0 || top > 1048576L) {
        top = 1048576L;
    }
    int high = -1;
    for (long base = top - 4; high < 0 && base > 64; base /= 2) {
        high = fcntl(low, F_DUPFD, (int)base);
    }
    int higher = high >= 0 ? fcntl(low, F_DUPFD, high + 1) : -1;
    if (higher != high + 1) {
        (void)close(low);
        if (high >= 0) {
            (void)close(high);
        }
        if (higher >= 0) {
            (void)close(higher);
        }
        return -1;
    }
    *keep_low = low;
    return high;
}
#endif

TEST(subprocess_child_close_fds_uses_one_syscall_not_rlimit_loop) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX fork+exec descriptor closing; Windows uses a handle allow-list");
#else
    for (int inject_enosys = 0; inject_enosys <= 1; inject_enosys++) {
        int keep_low = -1;
        int high = subprocess_park_high_fds(&keep_low);
        ASSERT_TRUE(high > STDERR_FILENO);
        cbm_subprocess_force_close_range_enosys_for_testing(inject_enosys != 0);
        /* The loop bound covers exactly the parked pair; close_range has none. */
        cbm_fd_close_strategy_t used =
            cbm_subprocess_close_fds_from_for_testing(high, (long)high + 2);
        cbm_subprocess_force_close_range_enosys_for_testing(false);
        bool closed = subprocess_fd_is_closed(high) && subprocess_fd_is_closed(high + 1);
        bool low_survived = fcntl(keep_low, F_GETFD) >= 0;
        (void)close(keep_low);

        ASSERT_TRUE(closed);
        ASSERT_TRUE(low_survived); /* only descriptors >= lowfd are touched */
        cbm_fd_close_strategy_t expected = subprocess_expected_close_strategy();
        if (inject_enosys && expected == CBM_FD_CLOSE_RANGE) {
            expected = CBM_FD_CLOSE_LOOP; /* kernel "lacks" it: the fallback must run */
        }
        ASSERT_EQ((int)used, (int)expected);
    }
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

/* ── Git child environment (#2003) ────────────────────────────────────────────
 * strip_git_repo_env / cbm_popen_git drop every variable of
 * `git rev-parse --local-env-vars` from the CHILD only: the parent keeps its
 * environment, other variables pass through, and without the flag a child
 * still inherits everything. The test sets the variables itself (a runner may
 * clear them at startup) and restores them before asserting. */
#ifndef _WIN32
typedef struct {
    char *saved[CBM_GIT_REPO_ENV_VAR_COUNT];
    bool present[CBM_GIT_REPO_ENV_VAR_COUNT];
} gitenv_snapshot_t;

static void gitenv_enter(gitenv_snapshot_t *snap) {
    for (int i = 0; i < CBM_GIT_REPO_ENV_VAR_COUNT; i++) {
        const char *v = getenv(cbm_git_repo_env_vars[i]);
        snap->present[i] = v != NULL;
        snap->saved[i] = v ? strdup(v) : NULL;
        setenv(cbm_git_repo_env_vars[i], "/cbm-decoy-repo", 1);
    }
    setenv("CBM_GITENV_KEEP", "kept", 1);
}

static void gitenv_leave(gitenv_snapshot_t *snap) {
    for (int i = 0; i < CBM_GIT_REPO_ENV_VAR_COUNT; i++) {
        if (snap->present[i]) {
            setenv(cbm_git_repo_env_vars[i], snap->saved[i], 1);
        } else {
            unsetenv(cbm_git_repo_env_vars[i]);
        }
        free(snap->saved[i]);
    }
    unsetenv("CBM_GITENV_KEEP");
}

/* exit 0: no git repo-local var is set and CBM_GITENV_KEEP=kept; exit 3: a git
 * var leaked; exit 4: the unrelated variable was lost. */
static void gitenv_probe_script(char *buf, size_t cap) {
    size_t n = (size_t)snprintf(buf, cap, "[ \"$CBM_GITENV_KEEP\" = kept ] || exit 4;");
    for (int i = 0; i < CBM_GIT_REPO_ENV_VAR_COUNT && n < cap; i++) {
        n += (size_t)snprintf(buf + n, cap - n, " [ -z \"${%s+x}\" ] || exit 3;",
                              cbm_git_repo_env_vars[i]);
    }
    if (n < cap) {
        (void)snprintf(buf + n, cap - n, " exit 0");
    }
}

static int gitenv_spawn_probe(bool strip) {
    char script[2048];
    gitenv_probe_script(script, sizeof(script));
    const char *argv[] = {"/bin/sh", "-c", script, NULL};
    cbm_proc_opts_t opts = {.bin = "/bin/sh", .argv = argv, .strip_git_repo_env = strip};
    cbm_proc_result_t r;
    if (cbm_subprocess_run(&opts, &r) != 0) {
        return -1;
    }
    return r.exit_code;
}
#endif

TEST(subprocess_strip_git_repo_env_is_per_child) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX /bin/sh spawn");
#else
    gitenv_snapshot_t snap;
    gitenv_enter(&snap);
    int stripped = gitenv_spawn_probe(true);
    int inherited = gitenv_spawn_probe(false);
    const char *parent_git_dir = getenv("GIT_DIR");
    bool parent_kept = parent_git_dir && strcmp(parent_git_dir, "/cbm-decoy-repo") == 0;
    gitenv_leave(&snap);
    ASSERT_EQ(stripped, 0);   /* git vars gone, CBM_GITENV_KEEP passed through */
    ASSERT_EQ(inherited, 3);  /* without the flag the child inherits as before */
    ASSERT_TRUE(parent_kept); /* the parent environment is never modified */
    PASS();
#endif
}

TEST(popen_git_strips_repo_env_and_reports_exit_status) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX /bin/sh spawn");
#else
    char script[2048];
    gitenv_probe_script(script, sizeof(script));
    char cmd[2200];
    snprintf(cmd, sizeof(cmd), "echo probe; %s", script);
    gitenv_snapshot_t snap;
    gitenv_enter(&snap);
    FILE *fp = cbm_popen_git(cmd);
    char line[64] = {0};
    bool got_line = fp && fgets(line, sizeof(line), fp) != NULL;
    int status = fp ? cbm_pclose(fp) : -1;
    FILE *fail = cbm_popen_git("exit 7");
    int fail_status = fail ? cbm_pclose(fail) : -1;
    const char *parent_git_dir = getenv("GIT_DIR");
    bool parent_kept = parent_git_dir && strcmp(parent_git_dir, "/cbm-decoy-repo") == 0;
    gitenv_leave(&snap);
    ASSERT_TRUE(got_line);
    ASSERT_STR_EQ(line, "probe\n");
    ASSERT_TRUE(status >= 0 && WIFEXITED(status));
    ASSERT_EQ(WEXITSTATUS(status), 0);
    ASSERT_TRUE(fail_status >= 0 && WIFEXITED(fail_status));
    ASSERT_EQ(WEXITSTATUS(fail_status), 7); /* pclose() semantics: raw wait status */
    ASSERT_TRUE(parent_kept);
    PASS();
#endif
}

/* Native fixed-byte child and independent stdout-capture contract tests. */
enum { SP_STDOUT_BYTES = 2305, SP_STDOUT_PATH = 1024 };
static const char *sp_stdout_test_binary;
static const char sp_stderr_text[] = "stderr-first\nstderr-tail";
static const char sp_stdout_text[] = "stdout-only\n";

static void sp_stdout_payload(unsigned char bytes[SP_STDOUT_BYTES]) {
    for (int i = 0; i < SP_STDOUT_BYTES; i++)
        bytes[i] = (unsigned char)('a' + i % 26);
    bytes[0] = 'B';
    bytes[1] = '\0';
    bytes[2] = '\r';
    bytes[3] = '\n';
    bytes[4] = '\n';
    bytes[1022] = 'X';
    bytes[1023] = '\r';
    bytes[1024] = '\n';
    bytes[2048] = '\0';
    bytes[SP_STDOUT_BYTES - 1] = 'Z'; /* final byte has no newline */
}

/* Called before normal harness setup. Normal entry remembers argv[0], whose
 * storage lasts through main; the private exact argv grammar never runs suites. */
static int sp_stdin_probe(int argc, char **argv);
const char *tf_runner_image(int argc, char **argv); /* test_main.c */
int tf_maybe_run_subprocess_stdout_probe(int argc, char **argv);
int tf_maybe_run_subprocess_stdout_probe(int argc, char **argv) {
    if (argc >= 2 && argv && strcmp(argv[1], "__cbm_subprocess_stdin_probe") == 0)
        return sp_stdin_probe(argc, argv);
    if (argc < 2 || !argv || strcmp(argv[1], "__cbm_subprocess_stdout_probe") != 0) {
        sp_stdout_test_binary = tf_runner_image(argc, argv);
        return -1;
    }
    if (argc != 4 || (strcmp(argv[2], "binary") != 0 && strcmp(argv[2], "text") != 0 &&
                      strcmp(argv[2], "empty") != 0))
        return 91;
#ifdef _WIN32
    if (_setmode(cbm_fileno(stdout), _O_BINARY) == -1 ||
        _setmode(cbm_fileno(stderr), _O_BINARY) == -1)
        return 92;
#endif
    if (argv[3][0]) {
        /* A state barrier, not a transient timing assertion. The deadline only
         * bounds cleanup if the parent fails before releasing this child. */
        uint64_t deadline = cbm_now_ms() + 30000U;
        while (!cbm_file_exists(argv[3])) {
            if (cbm_now_ms() >= deadline)
                return 93;
            cbm_usleep(1000);
        }
    }
    if (strcmp(argv[2], "binary") == 0) {
        unsigned char bytes[SP_STDOUT_BYTES];
        sp_stdout_payload(bytes);
        if (fwrite(bytes, 1, sizeof(bytes), stdout) != sizeof(bytes))
            return 94;
    } else if (strcmp(argv[2], "text") == 0) {
        if (fwrite(sp_stdout_text, 1, sizeof(sp_stdout_text) - 1, stdout) !=
            sizeof(sp_stdout_text) - 1)
            return 94;
    }
    if (fflush(stdout) != 0)
        return 94;
    if (fwrite(sp_stderr_text, 1, sizeof(sp_stderr_text) - 1, stderr) !=
            sizeof(sp_stderr_text) - 1 ||
        fflush(stderr) != 0)
        return 95;
    return 0;
}

typedef struct {
    char root[SP_STDOUT_PATH];
    char out[SP_STDOUT_PATH];
    char log[SP_STDOUT_PATH];
    char other[SP_STDOUT_PATH];
    char gate[SP_STDOUT_PATH];
} sp_stdout_fixture_t;

static bool sp_stdout_path(char *out, const char *root, const char *leaf) {
    int n = snprintf(out, SP_STDOUT_PATH, "%s/%s", root, leaf);
    return n > 0 && n < SP_STDOUT_PATH;
}

static bool sp_stdout_fixture_open(sp_stdout_fixture_t *fx) {
    memset(fx, 0, sizeof(*fx));
    if (!sp_stdout_test_binary || !sp_stdout_test_binary[0])
        return false;
    const char *root = th_mktempdir("cbm-sp-stdout");
    if (!root)
        return false;
    int n = snprintf(fx->root, sizeof(fx->root), "%s", root);
    bool ok = n > 0 && (size_t)n < sizeof(fx->root) && sp_stdout_path(fx->out, root, "out.bin") &&
              sp_stdout_path(fx->log, root, "err.log") &&
              sp_stdout_path(fx->other, root, "other.bin") &&
              sp_stdout_path(fx->gate, root, "release");
    if (!ok)
        (void)th_rmtree(root);
    return ok;
}

static bool sp_stdout_file_equals(const char *path, const void *bytes, size_t count) {
    FILE *file = cbm_fopen(path, "rb");
    if (!file)
        return false;
    const unsigned char *expected = bytes;
    bool equal = true;
    for (size_t i = 0; i < count; i++) {
        int byte = fgetc(file);
        if (byte == EOF || (unsigned char)byte != expected[i]) {
            equal = false;
            break;
        }
    }
    if (equal)
        equal = fgetc(file) == EOF && !ferror(file);
    if (fclose(file) != 0)
        equal = false;
    return equal;
}

typedef struct {
    char lines[4][64];
    int count;
    bool overflow;
} sp_stdout_logs_t;

static void sp_stdout_log(const char *line, void *opaque) {
    sp_stdout_logs_t *log = opaque;
    if (log->count >= 4 || strlen(line) >= sizeof(log->lines[0])) {
        log->overflow = true;
    } else {
        memcpy(log->lines[log->count], line, strlen(line) + 1);
    }
    log->count++;
}

static bool sp_stdout_stderr_only(const sp_stdout_logs_t *log) {
    return !log->overflow && log->count == 2 && strcmp(log->lines[0], "stderr-first") == 0 &&
           strcmp(log->lines[1], "stderr-tail") == 0;
}

static int sp_stdout_run(const char *mode, cbm_proc_opts_t *opts, cbm_proc_result_t *result) {
    const char *argv[] = {sp_stdout_test_binary, "__cbm_subprocess_stdout_probe", mode, "", NULL};
    opts->bin = sp_stdout_test_binary;
    opts->argv = argv;
    return cbm_subprocess_run(opts, result);
}

static bool sp_stdout_poll_terminal(cbm_subprocess_t *process, cbm_proc_result_t *result,
                                    uint64_t deadline) {
    do {
        cbm_proc_poll_t state = cbm_subprocess_poll(process, result);
        if (state == CBM_PROC_POLL_TERMINAL)
            return true;
        if (state == CBM_PROC_POLL_ERROR)
            return false;
        cbm_usleep(1000);
    } while (cbm_now_ms() < deadline);
    return false;
}

/* The finite child normally exits immediately. On a RED path, cancel and drain
 * before destroying; never pass a live handle to destroy. */
static bool sp_stdout_reap(cbm_subprocess_t *process, cbm_proc_result_t *result) {
    bool terminal = sp_stdout_poll_terminal(process, result, cbm_now_ms() + 10000U);
    bool cleaned = terminal;
    if (!cleaned) {
        (void)cbm_subprocess_request_cancel(process);
        cleaned = sp_stdout_poll_terminal(process, result, cbm_now_ms() + 4000U);
    }
    if (cleaned)
        cbm_subprocess_destroy(process);
    return terminal;
}

TEST(subprocess_stdout_null_preserves_merged_log) {
    sp_stdout_fixture_t fx;
    ASSERT_TRUE(sp_stdout_fixture_open(&fx));
    sp_stdout_logs_t logs = {0};
    cbm_proc_opts_t opts = {
        .log_file = fx.log, .stdout_file = NULL, .on_log_line = sp_stdout_log, .log_ud = &logs};
    cbm_proc_result_t result = {0};
    int rc = sp_stdout_run("text", &opts, &result);
    const char expected[] = "stdout-only\nstderr-first\nstderr-tail";
    bool exact = sp_stdout_file_equals(fx.log, expected, sizeof(expected) - 1);
    bool callbacks =
        !logs.overflow && logs.count == 3 && strcmp(logs.lines[0], "stdout-only") == 0 &&
        strcmp(logs.lines[1], "stderr-first") == 0 && strcmp(logs.lines[2], "stderr-tail") == 0;
    int cleanup = th_rmtree(fx.root);
    ASSERT_EQ(cleanup, 0);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(result.outcome, CBM_PROC_CLEAN);
    ASSERT_TRUE(result.tree_quiesced);
    ASSERT_TRUE(exact);
    ASSERT_TRUE(callbacks);
    PASS();
}

TEST(subprocess_stdout_capture_exact_binary_and_stderr_isolation) {
    sp_stdout_fixture_t fx;
    ASSERT_TRUE(sp_stdout_fixture_open(&fx));
    sp_stdout_logs_t logs = {0};
    cbm_proc_opts_t opts = {
        .stdout_file = fx.out, .log_file = fx.log, .on_log_line = sp_stdout_log, .log_ud = &logs};
    cbm_proc_result_t result = {0};
    int rc = sp_stdout_run("binary", &opts, &result);
    unsigned char expected[SP_STDOUT_BYTES];
    sp_stdout_payload(expected);
    bool exact = sp_stdout_file_equals(fx.out, expected, sizeof(expected));
    bool stderr_exact = sp_stdout_file_equals(fx.log, sp_stderr_text, sizeof(sp_stderr_text) - 1);
    bool callbacks = sp_stdout_stderr_only(&logs);
    int cleanup = th_rmtree(fx.root);
    ASSERT_EQ(cleanup, 0);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(result.outcome, CBM_PROC_CLEAN);
    ASSERT_TRUE(result.tree_quiesced);
    ASSERT_TRUE(exact);
    ASSERT_TRUE(stderr_exact);
    ASSERT_TRUE(callbacks);
    PASS();
}

TEST(subprocess_stdout_capture_without_log_discards_only_stderr) {
    sp_stdout_fixture_t fx;
    ASSERT_TRUE(sp_stdout_fixture_open(&fx));
    sp_stdout_logs_t logs = {0};
    cbm_proc_opts_t opts = {
        .stdout_file = fx.out, .log_file = NULL, .on_log_line = sp_stdout_log, .log_ud = &logs};
    cbm_proc_result_t result = {0};
    int rc = sp_stdout_run("binary", &opts, &result);
    unsigned char expected[SP_STDOUT_BYTES];
    sp_stdout_payload(expected);
    bool exact = sp_stdout_file_equals(fx.out, expected, sizeof(expected));
    bool absent_log = !cbm_file_exists(fx.log);
    int cleanup = th_rmtree(fx.root);
    ASSERT_EQ(cleanup, 0);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(result.outcome, CBM_PROC_CLEAN);
    ASSERT_TRUE(exact);
    ASSERT_TRUE(absent_log);
    ASSERT_EQ(logs.count, 0);
    PASS();
}

TEST(subprocess_stdout_empty_capture_survives_log_deletion) {
    sp_stdout_fixture_t fx;
    ASSERT_TRUE(sp_stdout_fixture_open(&fx));
    sp_stdout_logs_t logs = {0};
    cbm_proc_opts_t opts = {.stdout_file = fx.out,
                            .log_file = fx.log,
                            .on_log_line = sp_stdout_log,
                            .log_ud = &logs,
                            .delete_log_on_exit = true};
    cbm_proc_result_t result = {0};
    int rc = sp_stdout_run("empty", &opts, &result);
    bool exact_empty = sp_stdout_file_equals(fx.out, "", 0);
    bool deleted_log = !cbm_file_exists(fx.log);
    bool callbacks = sp_stdout_stderr_only(&logs);
    int cleanup = th_rmtree(fx.root);
    ASSERT_EQ(cleanup, 0);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(result.outcome, CBM_PROC_CLEAN);
    ASSERT_TRUE(exact_empty);
    ASSERT_TRUE(deleted_log);
    ASSERT_TRUE(callbacks);
    PASS();
}

TEST(subprocess_stdout_capture_owns_caller_path) {
    sp_stdout_fixture_t fx;
    ASSERT_TRUE(sp_stdout_fixture_open(&fx));
    char caller_path[SP_STDOUT_PATH];
    memcpy(caller_path, fx.out, strlen(fx.out) + 1);
    const char sentinel[] = "untouched-alternate-file";
    int sentinel_rc = th_write_file(fx.other, sentinel);
    const char *argv[] = {sp_stdout_test_binary, "__cbm_subprocess_stdout_probe", "binary", fx.gate,
                          NULL};
    cbm_proc_opts_t opts = {
        .bin = sp_stdout_test_binary, .argv = argv, .stdout_file = caller_path, .log_file = fx.log};
    cbm_subprocess_t *process = NULL;
    int spawn_rc = sentinel_rc == 0 ? cbm_subprocess_spawn(&opts, &process) : -1;
    bool got_process = process != NULL;
    /* The child cannot emit before this mutation: its gate is still absent. */
    memcpy(caller_path, fx.other, strlen(fx.other) + 1);
    int gate_rc = th_write_file(fx.gate, "release");
    cbm_proc_result_t result = {0};
    bool terminal = process && sp_stdout_reap(process, &result);
    unsigned char expected[SP_STDOUT_BYTES];
    sp_stdout_payload(expected);
    bool exact = sp_stdout_file_equals(fx.out, expected, sizeof(expected));
    bool alternate_unchanged = sp_stdout_file_equals(fx.other, sentinel, sizeof(sentinel) - 1);
    int cleanup = th_rmtree(fx.root);
    ASSERT_EQ(cleanup, 0);
    ASSERT_EQ(sentinel_rc, 0);
    ASSERT_EQ(spawn_rc, 0);
    ASSERT_TRUE(got_process);
    ASSERT_EQ(gate_rc, 0);
    ASSERT_TRUE(terminal);
    ASSERT_EQ(result.outcome, CBM_PROC_CLEAN);
    ASSERT_TRUE(result.tree_quiesced);
    ASSERT_TRUE(exact);
    ASSERT_TRUE(alternate_unchanged);
    PASS();
}

TEST(subprocess_stdout_rejects_invalid_capture_paths_and_cleans_up) {
    sp_stdout_fixture_t fx;
    ASSERT_TRUE(sp_stdout_fixture_open(&fx));
    char missing_parent[SP_STDOUT_PATH];
    bool path_ok = sp_stdout_path(missing_parent, fx.root, "missing-parent/out.bin");
    const char *paths[] = {"", fx.log, fx.root, missing_parent};
    bool rejected[4] = {false};
    bool null_handle[4] = {false};
    bool cleanup_terminal[4] = {true, true, true, true};
    if (path_ok) {
        for (int i = 0; i < 4; i++) {
            const char *argv[] = {sp_stdout_test_binary, "__cbm_subprocess_stdout_probe", "text",
                                  "", NULL};
            cbm_proc_opts_t opts = {.bin = sp_stdout_test_binary,
                                    .argv = argv,
                                    .stdout_file = paths[i],
                                    .log_file = fx.log};
            cbm_subprocess_t *process = NULL;
            int rc = cbm_subprocess_spawn(&opts, &process);
            rejected[i] = rc == -1;
            null_handle[i] = process == NULL;
            if (process) {
                cbm_proc_result_t ignored;
                cleanup_terminal[i] = sp_stdout_reap(process, &ignored);
            }
        }
    }
    int cleanup = th_rmtree(fx.root);
    ASSERT_EQ(cleanup, 0);
    ASSERT_TRUE(path_ok);
    for (int i = 0; i < 4; i++) {
        ASSERT_TRUE(cleanup_terminal[i]);
        ASSERT_TRUE(rejected[i]);
        ASSERT_TRUE(null_handle[i]);
    }
    PASS();
}

/* Independent stdin_file contract fixtures. No shell and no owned heap allocation. */
enum { SP_STDIN_BYTES = 4099 };
static const char sp_stdin_out_sentinel[] = "output-sentinel";
static const char sp_stdin_log_sentinel[] = "log-sentinel";

static void sp_stdin_payload(unsigned char bytes[SP_STDIN_BYTES]) {
    for (int i = 0; i < SP_STDIN_BYTES; i++)
        bytes[i] = (unsigned char)(i % 256);
    bytes[0] = 'I';
    bytes[1] = 0;
    bytes[2] = '\r';
    bytes[3] = '\n';
    bytes[4] = '\n';
    bytes[5] = 0x1a; /* Windows text-mode EOF must not truncate the fixture. */
    bytes[1023] = 0;
    bytes[SP_STDIN_BYTES - 1] = 0x7f; /* No final LF. */
}

static bool sp_stdin_write(const char *path, const void *bytes, size_t count) {
    FILE *file = cbm_fopen(path, "wb");
    if (!file)
        return false;
    bool ok = count == 0 || fwrite(bytes, 1, count, file) == count;
    if (fclose(file) != 0)
        ok = false;
    return ok;
}

static bool sp_stdin_clean(int rc, const cbm_proc_result_t *result) {
    return rc == 0 && result->outcome == CBM_PROC_CLEAN && result->exit_code == 0 &&
           result->tree_quiesced && !result->supervision_failed;
}

/* Do not remove inputs/captures following failed containment. Terminal handles
 * may be destroyed, but a nonquiescent tree's fixture is intentionally retained. */
static bool sp_stdin_safe_run(int rc, const cbm_proc_result_t *result) {
    return result->tree_quiesced ||
           (rc == -1 && result->outcome == CBM_PROC_SPAWN_FAILED && !result->supervision_failed);
}

static bool sp_stdin_finish(cbm_subprocess_t *process, cbm_proc_result_t *result) {
    bool terminal = sp_stdout_poll_terminal(process, result, cbm_now_ms() + 10000U);
    if (!terminal) {
        (void)cbm_subprocess_request_cancel(process);
        terminal = sp_stdout_poll_terminal(process, result, cbm_now_ms() + 4000U);
    }
    if (terminal)
        cbm_subprocess_destroy(process);
    return terminal && result->tree_quiesced && !result->supervision_failed;
}

static int sp_stdin_run(cbm_proc_opts_t *opts, cbm_proc_result_t *result) {
    const char *argv[] = {sp_stdout_test_binary, "__cbm_subprocess_stdin_probe", "echo", "", NULL};
    opts->bin = sp_stdout_test_binary;
    opts->argv = argv;
    opts->quiet_timeout_ms = 10000; /* Finite containment guard, not a timing oracle. */
    return cbm_subprocess_run(opts, result);
}

typedef struct {
    bool prepared;
    bool rejected;
    bool no_handle;
    bool outputs_unchanged;
    bool no_child_marker;
    bool reusable;
    bool safe_cleanup;
} sp_stdin_rejection_t;

static sp_stdin_rejection_t sp_stdin_check_rejection(const char *binary, const char *input,
                                                     const char *out, const char *log,
                                                     const char *marker,
                                                     const char *valid_empty_input) {
    sp_stdin_rejection_t check = {.safe_cleanup = true};
    (void)cbm_unlink(marker);
    check.prepared =
        sp_stdin_write(out, sp_stdin_out_sentinel, sizeof(sp_stdin_out_sentinel) - 1) &&
        sp_stdin_write(log, sp_stdin_log_sentinel, sizeof(sp_stdin_log_sentinel) - 1) &&
        sp_stdout_file_equals(valid_empty_input, "", 0) && !cbm_file_exists(marker);
    if (!check.prepared)
        return check;
    const char *argv[] = {binary, "__cbm_subprocess_stdin_probe", "stamp", marker, NULL};
    cbm_proc_opts_t opts = {.bin = binary,
                            .argv = argv,
                            .stdin_file = input,
                            .stdout_file = out,
                            .log_file = log,
                            .quiet_timeout_ms = 10000};
    cbm_subprocess_t *process = NULL;
    int rc = cbm_subprocess_spawn(&opts, &process);
    check.rejected = rc == -1;
    check.no_handle = process == NULL;
    if (process) {
        cbm_proc_result_t result = {0};
        check.safe_cleanup = sp_stdin_finish(process, &result);
    }
    if (!check.safe_cleanup)
        return check;
    check.outputs_unchanged =
        sp_stdout_file_equals(out, sp_stdin_out_sentinel, sizeof(sp_stdin_out_sentinel) - 1) &&
        sp_stdout_file_equals(log, sp_stdin_log_sentinel, sizeof(sp_stdin_log_sentinel) - 1);
    check.no_child_marker = !cbm_file_exists(marker);

    /* A real empty regular file remains a usable input after each rejection.
     * This control also passes when the API-only baseline ignores stdin_file. */
    const char *valid_argv[] = {binary, "__cbm_subprocess_stdin_probe", "echo", "", NULL};
    opts.argv = valid_argv;
    opts.stdin_file = valid_empty_input;
    cbm_proc_result_t result = {0};
    rc = cbm_subprocess_run(&opts, &result);
    check.safe_cleanup = sp_stdin_safe_run(rc, &result);
    check.reusable = sp_stdin_clean(rc, &result) && sp_stdout_file_equals(out, "", 0) &&
                     sp_stdout_file_equals(log, sp_stderr_text, sizeof(sp_stderr_text) - 1) &&
                     sp_stdout_file_equals(valid_empty_input, "", 0);
    return check;
}

/* Dispatched through the existing early native probe entry, before harness setup. */
static int sp_stdin_probe(int argc, char **argv) {
#ifdef _WIN32
    if (_setmode(cbm_fileno(stdin), _O_BINARY) == -1 ||
        _setmode(cbm_fileno(stdout), _O_BINARY) == -1 ||
        _setmode(cbm_fileno(stderr), _O_BINARY) == -1)
        return 101;
#endif
    if (argc == 8 && strcmp(argv[2], "reject") == 0) {
        sp_stdin_rejection_t check =
            sp_stdin_check_rejection(argv[0], argv[3], argv[4], argv[5], argv[6], argv[7]);
        if (!check.safe_cleanup)
            return 125; /* Parent must retain files if nested containment failed. */
        if (!check.prepared)
            return 102;
        if (!check.reusable)
            return 103;
        return check.rejected && check.no_handle && check.outputs_unchanged && check.no_child_marker
                   ? 0
                   : 104;
    }
    if (argc == 6 && strcmp(argv[2], "parent-eof") == 0) {
        /* Isolated process only: furnish genuinely readable parent stdin, then
         * verify a NULL stdin_file child still gets the null device. */
        FILE *file = cbm_fopen(argv[3], "rb");
        if (!file)
            return 105;
#ifdef _WIN32
        int duplicated = _dup2(cbm_fileno(file), cbm_fileno(stdin));
#else
        int duplicated = dup2(cbm_fileno(file), cbm_fileno(stdin));
#endif
        bool closed = fclose(file) == 0;
        if (duplicated < 0 || !closed)
            return 106;
#ifdef _WIN32
        if (!SetStdHandle(STD_INPUT_HANDLE, (HANDLE)_get_osfhandle(cbm_fileno(stdin))))
            return 106;
#endif
        /* Exercise the descriptor directly: a stdio-buffer-only rewind must not
         * leave the inherited OS offset at EOF and make the control vacuous. */
        unsigned char witness;
#ifdef _WIN32
        if (_read(cbm_fileno(stdin), &witness, 1) != 1 ||
            _lseek(cbm_fileno(stdin), 0, SEEK_SET) != 0)
            return 107;
#else
        if (read(cbm_fileno(stdin), &witness, 1) != 1 || lseek(cbm_fileno(stdin), 0, SEEK_SET) != 0)
            return 107;
#endif
        const char *child_argv[] = {argv[0], "__cbm_subprocess_stdin_probe", "echo", "", NULL};
        cbm_proc_opts_t opts = {.bin = argv[0],
                                .argv = child_argv,
                                .stdin_file = NULL,
                                .stdout_file = argv[4],
                                .log_file = argv[5],
                                .quiet_timeout_ms = 10000};
        cbm_proc_result_t result = {0};
        int rc = cbm_subprocess_run(&opts, &result);
        if (!sp_stdin_safe_run(rc, &result))
            return 125;
        return sp_stdin_clean(rc, &result) && sp_stdout_file_equals(argv[4], "", 0) &&
                       sp_stdout_file_equals(argv[5], sp_stderr_text, sizeof(sp_stderr_text) - 1)
                   ? 0
                   : 108;
    }
    if (argc != 4)
        return 109;
    if (strcmp(argv[2], "stamp") == 0) {
        if (!sp_stdin_write(argv[3], "child-started", 13) ||
            fwrite(sp_stdout_text, 1, sizeof(sp_stdout_text) - 1, stdout) !=
                sizeof(sp_stdout_text) - 1)
            return 110;
    } else if (strcmp(argv[2], "echo") == 0) {
        if (argv[3][0]) {
            /* The parent releases this state barrier only after mutating its
             * borrowed path. The deadline merely bounds an abandoned fixture. */
            uint64_t deadline = cbm_now_ms() + 30000U;
            while (!cbm_file_exists(argv[3])) {
                if (cbm_now_ms() >= deadline)
                    return 111;
                cbm_usleep(1000);
            }
        }
        unsigned char bytes[512];
        size_t count;
        while ((count = fread(bytes, 1, sizeof(bytes), stdin)) != 0) {
            if (fwrite(bytes, 1, count, stdout) != count)
                return 112;
        }
        if (ferror(stdin))
            return 112;
    } else {
        return 109;
    }
    if (fflush(stdout) != 0 ||
        fwrite(sp_stderr_text, 1, sizeof(sp_stderr_text) - 1, stderr) !=
            sizeof(sp_stderr_text) - 1 ||
        fflush(stderr) != 0)
        return 113;
    return 0;
}

TEST(subprocess_stdin_null_is_eof_even_with_readable_parent_input) {
    sp_stdout_fixture_t fx;
    ASSERT_TRUE(sp_stdout_fixture_open(&fx));
    unsigned char input[SP_STDIN_BYTES];
    sp_stdin_payload(input);
    bool prepared = sp_stdin_write(fx.other, input, sizeof(input));
    const char *argv[] = {sp_stdout_test_binary,
                          "__cbm_subprocess_stdin_probe",
                          "parent-eof",
                          fx.other,
                          fx.out,
                          fx.log,
                          NULL};
    cbm_proc_opts_t opts = {.bin = sp_stdout_test_binary, .argv = argv, .quiet_timeout_ms = 20000};
    cbm_proc_result_t result = {0};
    int rc = prepared ? cbm_subprocess_run(&opts, &result) : -1;
    bool safe = !prepared || (sp_stdin_safe_run(rc, &result) && result.exit_code != 125);
    bool unchanged = sp_stdout_file_equals(fx.other, input, sizeof(input));
    bool empty = sp_stdout_file_equals(fx.out, "", 0);
    int cleanup = safe ? th_rmtree(fx.root) : -1;
    ASSERT_EQ(cleanup, 0);
    ASSERT_TRUE(prepared);
    ASSERT_TRUE(sp_stdin_clean(rc, &result));
    ASSERT_TRUE(unchanged);
    ASSERT_TRUE(empty);
    PASS();
}

TEST(subprocess_stdin_empty_regular_file_is_success) {
    sp_stdout_fixture_t fx;
    ASSERT_TRUE(sp_stdout_fixture_open(&fx));
    bool prepared = sp_stdin_write(fx.other, "", 0);
    sp_stdout_logs_t logs = {0};
    cbm_proc_opts_t opts = {.stdin_file = fx.other,
                            .stdout_file = fx.out,
                            .log_file = fx.log,
                            .on_log_line = sp_stdout_log,
                            .log_ud = &logs};
    cbm_proc_result_t result = {0};
    int rc = prepared ? sp_stdin_run(&opts, &result) : -1;
    bool safe = !prepared || sp_stdin_safe_run(rc, &result);
    bool exact = sp_stdout_file_equals(fx.out, "", 0) && sp_stdout_file_equals(fx.other, "", 0) &&
                 sp_stdout_file_equals(fx.log, sp_stderr_text, sizeof(sp_stderr_text) - 1);
    int cleanup = safe ? th_rmtree(fx.root) : -1;
    ASSERT_EQ(cleanup, 0);
    ASSERT_TRUE(prepared);
    ASSERT_TRUE(sp_stdin_clean(rc, &result));
    ASSERT_TRUE(exact);
    ASSERT_TRUE(sp_stdout_stderr_only(&logs));
    PASS();
}

TEST(subprocess_stdin_binary_roundtrip_and_stderr_isolation) {
    sp_stdout_fixture_t fx;
    ASSERT_TRUE(sp_stdout_fixture_open(&fx));
    unsigned char input[SP_STDIN_BYTES];
    sp_stdin_payload(input);
    bool prepared = sp_stdin_write(fx.other, input, sizeof(input));
    sp_stdout_logs_t logs = {0};
    cbm_proc_opts_t opts = {.stdin_file = fx.other,
                            .stdout_file = fx.out,
                            .log_file = fx.log,
                            .on_log_line = sp_stdout_log,
                            .log_ud = &logs};
    cbm_proc_result_t result = {0};
    int rc = prepared ? sp_stdin_run(&opts, &result) : -1;
    bool safe = !prepared || sp_stdin_safe_run(rc, &result);
    bool exact = sp_stdout_file_equals(fx.out, input, sizeof(input));
    bool unchanged = sp_stdout_file_equals(fx.other, input, sizeof(input));
    bool stderr_exact = sp_stdout_file_equals(fx.log, sp_stderr_text, sizeof(sp_stderr_text) - 1);
    int cleanup = safe ? th_rmtree(fx.root) : -1;
    ASSERT_EQ(cleanup, 0);
    ASSERT_TRUE(prepared);
    ASSERT_TRUE(sp_stdin_clean(rc, &result));
    ASSERT_TRUE(exact);
    ASSERT_TRUE(unchanged);
    ASSERT_TRUE(stderr_exact);
    ASSERT_TRUE(sp_stdout_stderr_only(&logs));
    PASS();
}

TEST(subprocess_stdin_stream_combinations_preserve_input) {
    sp_stdout_fixture_t fx;
    ASSERT_TRUE(sp_stdout_fixture_open(&fx));
    unsigned char input[SP_STDIN_BYTES];
    sp_stdin_payload(input);
    bool prepared = sp_stdin_write(fx.other, input, sizeof(input));
    bool all_clean = true, all_exact = true, all_unchanged = true, all_logs = true;
    bool safe = true;
    for (int mode = 0; prepared && safe && mode < 3; mode++) {
        (void)cbm_unlink(fx.log);
        sp_stdout_logs_t logs = {0};
        cbm_proc_opts_t opts = {.stdin_file = fx.other,
                                .stdout_file = mode == 2 ? NULL : fx.out,
                                .log_file = mode == 1 ? NULL : fx.log,
                                .delete_log_on_exit = mode == 0,
                                .on_log_line = mode == 2 ? NULL : sp_stdout_log,
                                .log_ud = &logs};
        cbm_proc_result_t result = {0};
        int rc = sp_stdin_run(&opts, &result);
        safe = sp_stdin_safe_run(rc, &result);
        all_clean = all_clean && sp_stdin_clean(rc, &result);
        all_unchanged = all_unchanged && sp_stdout_file_equals(fx.other, input, sizeof(input));
        if (mode == 2) {
            unsigned char merged[SP_STDIN_BYTES + sizeof(sp_stderr_text) - 1];
            memcpy(merged, input, sizeof(input));
            memcpy(merged + sizeof(input), sp_stderr_text, sizeof(sp_stderr_text) - 1);
            all_exact = all_exact && sp_stdout_file_equals(fx.log, merged, sizeof(merged));
        } else {
            all_exact = all_exact && sp_stdout_file_equals(fx.out, input, sizeof(input));
            all_logs = all_logs && !cbm_file_exists(fx.log) &&
                       (mode == 0 ? sp_stdout_stderr_only(&logs) : logs.count == 0);
        }
    }
    int cleanup = safe ? th_rmtree(fx.root) : -1;
    ASSERT_EQ(cleanup, 0);
    ASSERT_TRUE(prepared);
    ASSERT_TRUE(all_clean);
    ASSERT_TRUE(all_exact);
    ASSERT_TRUE(all_unchanged);
    ASSERT_TRUE(all_logs);
    PASS();
}

TEST(subprocess_stdin_caller_path_mutation_before_gate_release) {
    sp_stdout_fixture_t fx;
    ASSERT_TRUE(sp_stdout_fixture_open(&fx));
    char alternate[SP_STDOUT_PATH], caller_path[SP_STDOUT_PATH];
    unsigned char input[SP_STDIN_BYTES];
    sp_stdin_payload(input);
    static const char alternate_bytes[] = "different-input-must-not-be-read";
    bool prepared = sp_stdout_path(alternate, fx.root, "alternate.bin") &&
                    sp_stdin_write(fx.other, input, sizeof(input)) &&
                    sp_stdin_write(alternate, alternate_bytes, sizeof(alternate_bytes) - 1);
    memcpy(caller_path, fx.other, strlen(fx.other) + 1);
    const char *argv[] = {sp_stdout_test_binary, "__cbm_subprocess_stdin_probe", "echo", fx.gate,
                          NULL};
    cbm_proc_opts_t opts = {.bin = sp_stdout_test_binary,
                            .argv = argv,
                            .stdin_file = caller_path,
                            .stdout_file = fx.out,
                            .log_file = fx.log};
    cbm_subprocess_t *process = NULL;
    int rc = prepared ? cbm_subprocess_spawn(&opts, &process) : -1;
    bool got_process = process != NULL;
    if (prepared)
        memcpy(caller_path, alternate, strlen(alternate) + 1);
    int gate_rc = prepared ? th_write_file(fx.gate, "release") : -1;
    cbm_proc_result_t result = {0};
    bool safe = !process || sp_stdin_finish(process, &result);
    bool exact = sp_stdout_file_equals(fx.out, input, sizeof(input));
    bool original_kept = sp_stdout_file_equals(fx.other, input, sizeof(input));
    bool alternate_kept =
        prepared && sp_stdout_file_equals(alternate, alternate_bytes, sizeof(alternate_bytes) - 1);
    int cleanup = safe ? th_rmtree(fx.root) : -1;
    ASSERT_EQ(cleanup, 0);
    ASSERT_TRUE(prepared);
    ASSERT_EQ(rc, 0);
    ASSERT_TRUE(got_process);
    ASSERT_EQ(gate_rc, 0);
    ASSERT_TRUE(sp_stdin_clean(rc, &result));
    ASSERT_TRUE(exact);
    ASSERT_TRUE(original_kept);
    ASSERT_TRUE(alternate_kept);
    PASS();
}

TEST(subprocess_stdin_rejects_invalid_paths_before_outputs) {
    sp_stdout_fixture_t fx;
    ASSERT_TRUE(sp_stdout_fixture_open(&fx));
    char missing[SP_STDOUT_PATH];
    bool prepared =
        sp_stdout_path(missing, fx.root, "missing-input") && sp_stdin_write(fx.other, "", 0);
    const char *paths[] = {"", missing, fx.root};
    sp_stdin_rejection_t checks[3] = {{0}};
    bool safe = true;
    for (int i = 0; prepared && safe && i < 3; i++) {
        checks[i] = sp_stdin_check_rejection(sp_stdout_test_binary, paths[i], fx.out, fx.log,
                                             fx.gate, fx.other);
        safe = checks[i].safe_cleanup;
    }
    int cleanup = safe ? th_rmtree(fx.root) : -1;
    ASSERT_EQ(cleanup, 0);
    ASSERT_TRUE(prepared);
    for (int i = 0; i < 3; i++) {
        ASSERT_TRUE(checks[i].prepared);
        ASSERT_TRUE(checks[i].reusable);
        ASSERT_TRUE(checks[i].rejected);
        ASSERT_TRUE(checks[i].no_handle);
        ASSERT_TRUE(checks[i].outputs_unchanged);
        ASSERT_TRUE(checks[i].no_child_marker);
    }
    PASS();
}

TEST(subprocess_stdin_rejects_exact_output_aliases_before_outputs) {
    sp_stdout_fixture_t fx;
    ASSERT_TRUE(sp_stdout_fixture_open(&fx));
    bool prepared = sp_stdin_write(fx.other, "", 0);
    const char *paths[] = {fx.log, fx.out};
    sp_stdin_rejection_t checks[2] = {{0}};
    bool safe = true;
    for (int i = 0; prepared && safe && i < 2; i++) {
        checks[i] = sp_stdin_check_rejection(sp_stdout_test_binary, paths[i], fx.out, fx.log,
                                             fx.gate, fx.other);
        safe = checks[i].safe_cleanup;
    }
    int cleanup = safe ? th_rmtree(fx.root) : -1;
    ASSERT_EQ(cleanup, 0);
    ASSERT_TRUE(prepared);
    for (int i = 0; i < 2; i++) {
        ASSERT_TRUE(checks[i].prepared);
        ASSERT_TRUE(checks[i].reusable);
        ASSERT_TRUE(checks[i].rejected);
        ASSERT_TRUE(checks[i].no_handle);
        ASSERT_TRUE(checks[i].outputs_unchanged);
        ASSERT_TRUE(checks[i].no_child_marker);
    }
    PASS();
}

TEST(subprocess_stdin_posix_fifo_and_symlink_reject_without_blocking_runner) {
#ifdef _WIN32
    /* WHITELIST: POSIX FIFO/symlink semantics only. Directory rejection is covered
     * above on Windows. No verified unprivileged Windows reparse fixture exists
     * in this package; that branch is a recorded coverage limit, not a PASS. */
    SKIP_PLATFORM("POSIX FIFO and symlink fixture; Windows directory covered separately");
#else
    sp_stdout_fixture_t fx;
    ASSERT_TRUE(sp_stdout_fixture_open(&fx));
    char fifo[SP_STDOUT_PATH], link[SP_STDOUT_PATH];
    bool prepared = sp_stdout_path(fifo, fx.root, "input.fifo") &&
                    sp_stdout_path(link, fx.root, "input.link") &&
                    sp_stdin_write(fx.other, "", 0) && mkfifo(fifo, 0600) == 0 &&
                    symlink(fx.other, link) == 0;
    struct stat fifo_stat, link_stat;
    bool kinds = prepared && lstat(fifo, &fifo_stat) == 0 && S_ISFIFO(fifo_stat.st_mode) &&
                 lstat(link, &link_stat) == 0 && S_ISLNK(link_stat.st_mode);
    const char *paths[] = {fifo, link};
    bool clean[2] = {false}, no_child[2] = {false};
    bool safe = true;
    for (int i = 0; kinds && safe && i < 2; i++) {
        (void)cbm_unlink(fx.gate);
        const char *argv[] = {sp_stdout_test_binary,
                              "__cbm_subprocess_stdin_probe",
                              "reject",
                              paths[i],
                              fx.out,
                              fx.log,
                              fx.gate,
                              fx.other,
                              NULL};
        /* The probe can itself block inside spawn on a broken blocking FIFO
         * open. Only that isolated process is then killed by this safety guard.
         * No elapsed-time threshold is used as evidence of correct rejection. */
        cbm_proc_opts_t opts = {
            .bin = sp_stdout_test_binary, .argv = argv, .quiet_timeout_ms = 20000};
        cbm_proc_result_t result = {0};
        int rc = cbm_subprocess_run(&opts, &result);
        safe = sp_stdin_safe_run(rc, &result) && result.exit_code != 125;
        clean[i] = sp_stdin_clean(rc, &result);
        no_child[i] = !cbm_file_exists(fx.gate);
    }
    int cleanup = safe ? th_rmtree(fx.root) : -1;
    ASSERT_EQ(cleanup, 0);
    ASSERT_TRUE(prepared);
    ASSERT_TRUE(kinds);
    for (int i = 0; i < 2; i++) {
        ASSERT_TRUE(clean[i]);
        ASSERT_TRUE(no_child[i]);
    }
    PASS();
#endif
}

SUITE(subprocess) {
    RUN_TEST(subprocess_stdin_null_is_eof_even_with_readable_parent_input);
    RUN_TEST(subprocess_stdin_empty_regular_file_is_success);
    RUN_TEST(subprocess_stdin_binary_roundtrip_and_stderr_isolation);
    RUN_TEST(subprocess_stdin_stream_combinations_preserve_input);
    RUN_TEST(subprocess_stdin_caller_path_mutation_before_gate_release);
    RUN_TEST(subprocess_stdin_rejects_invalid_paths_before_outputs);
    RUN_TEST(subprocess_stdin_rejects_exact_output_aliases_before_outputs);
    RUN_TEST(subprocess_stdin_posix_fifo_and_symlink_reject_without_blocking_runner);
    RUN_TEST(subprocess_stdout_null_preserves_merged_log);
    RUN_TEST(subprocess_stdout_capture_exact_binary_and_stderr_isolation);
    RUN_TEST(subprocess_stdout_capture_without_log_discards_only_stderr);
    RUN_TEST(subprocess_stdout_empty_capture_survives_log_deletion);
    RUN_TEST(subprocess_stdout_capture_owns_caller_path);
    RUN_TEST(subprocess_stdout_rejects_invalid_capture_paths_and_cleans_up);
    RUN_TEST(subprocess_classify_clean);
    RUN_TEST(subprocess_classify_exit_nonzero);
    RUN_TEST(subprocess_classify_windows_crash_codes);
    RUN_TEST(subprocess_classify_posix_fault_signal_is_crash);
    RUN_TEST(subprocess_classify_non_fault_signal_is_killed);
    RUN_TEST(subprocess_classify_timeout_dominates);
    RUN_TEST(subprocess_outcome_str);
    RUN_TEST(subprocess_run_clean);
    RUN_TEST(subprocess_run_exit_nonzero);
    RUN_TEST(subprocess_run_child_exited_before_group_check);
    RUN_TEST(subprocess_run_child_exiting_at_group_check);
    RUN_TEST(subprocess_run_resolves_literal_binary_name_from_path);
    RUN_TEST(subprocess_run_crash_is_crash);
    RUN_TEST(subprocess_run_hang_is_hang);
    RUN_TEST(subprocess_retries_transient_spawn_refusal);
    RUN_TEST(subprocess_spawn_backoff_resumes_after_eintr);
    RUN_TEST(subprocess_gives_up_after_the_retry_budget);
    RUN_TEST(subprocess_run_spawn_failure);
    RUN_TEST(subprocess_run_null_bin_rejected);
    RUN_TEST(subprocess_spawn_returns_while_child_is_running);
    RUN_TEST(subprocess_natural_completion_is_cached_across_polls);
    RUN_TEST(subprocess_cancel_is_idempotent_and_kills_ignoring_tree);
    RUN_TEST(subprocess_quiet_timeout_kills_ignoring_tree);
    RUN_TEST(subprocess_windows_job_object_cancellation_quiesces_descendant_tree);
    RUN_TEST(subprocess_windows_job_object_enforces_memory_limit);
    RUN_TEST(subprocess_cancel_grace_is_hard_capped);
    RUN_TEST(subprocess_poll_log_delivery_is_bounded_and_terminal_is_lossless);
    RUN_TEST(subprocess_final_log_drain_error_is_terminal_and_preserves_classification);
    RUN_TEST(subprocess_posix_child_closes_unrelated_descriptors);
    RUN_TEST(subprocess_child_close_fds_uses_one_syscall_not_rlimit_loop);
    RUN_TEST(subprocess_root_exit_drains_surviving_descendant);
    RUN_TEST(subprocess_strip_git_repo_env_is_per_child);
    RUN_TEST(popen_git_strips_repo_env_and_reports_exit_status);
    RUN_TEST(win_cmdline_index_worker_json);
    RUN_TEST(win_cmdline_roundtrip_battery);
    RUN_TEST(win_cmdline_overflow_rejected);
    RUN_TEST(win_cmdline_overflow_leaves_empty_string);
    RUN_TEST(win_cmd_payload_is_verbatim_and_capacity_checked);
    RUN_TEST(win_cmd_payload_rejects_short_relative_and_non_cmd_paths);
}
