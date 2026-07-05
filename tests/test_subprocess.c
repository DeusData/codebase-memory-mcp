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

#include <string.h>

#ifndef _WIN32
#include <signal.h>
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
    /* execv of a bogus path: fork succeeds, child _exit(127). We classify the
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

/* ── Layer 3: Windows command-line quoting (pure; every platform) ─────────────
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
        for (;;) {
            size_t nbs = 0;
            while (*p == '\\') {
                nbs++;
                p++;
            }
            if (*p == '"') {
                for (size_t k = 0; k < nbs / 2; k++) {
                    o[oi++] = '\\';
                }
                if (nbs % 2) {
                    o[oi++] = '"'; /* odd run → the quote is an escaped literal */
                } else {
                    in_quotes = !in_quotes; /* even run → the quote is a delimiter */
                }
                p++;
            } else {
                for (size_t k = 0; k < nbs; k++) {
                    o[oi++] = '\\';
                }
                if (*p == '\0' || (!in_quotes && (*p == ' ' || *p == '\t'))) {
                    break;
                }
                o[oi++] = *p++;
            }
        }
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
    const char *const argv[] = {"C:/bin/cbm.exe",           "cli",
                                "--index-worker",           "index_repository",
                                "{\"repo_path\":\"C:/r\"}", "--response-out",
                                "C:/c/w.response",          NULL};
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
    RUN_TEST(subprocess_run_crash_is_crash);
    RUN_TEST(subprocess_run_hang_is_hang);
    RUN_TEST(subprocess_run_spawn_failure);
    RUN_TEST(subprocess_run_null_bin_rejected);
    RUN_TEST(win_cmdline_index_worker_json);
    RUN_TEST(win_cmdline_roundtrip_battery);
    RUN_TEST(win_cmdline_overflow_rejected);
}
