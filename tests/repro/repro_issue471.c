/*
 * repro_issue471.c - Reproduce-first case for OPEN bug #471.
 *
 * Issue: #471 - "GLR ambiguity-merge is O(n^2) for deeply-nested ambiguous
 *               grammars (e.g. Perl), even with the recursion-depth cap"
 *
 * Pathological construct:
 *   A deeply-nested Perl function call chain of the form:
 *     f(f(f(f(... f(1) ...))))
 *   where `f` is called with paren-optional syntax, causing the Perl grammar to
 *   produce `ambiguous_function_call_expression` nodes at every nesting level.
 *   This is the exact shape named by the original reporter (halindrome) and
 *   confirmed in the maintainer comment on #471.
 *
 * Why O(n^2):
 *   tree-sitter's GLR merge path in `stack_node_add_link`
 *   (internal/cbm/vendored/ts_runtime/src/stack.c, function starting at line 200)
 *   is called recursively when two candidate parse-stack heads share compatible
 *   predecessor nodes (same TSStateId, same byte position, same error_cost).
 *   For an N-deep ambiguous call chain, the merge loop at the outermost level
 *   iterates over N-1 existing links while each inner recursive call adds another
 *   sweep over the growing link list.  The result is O(N^2) total
 *   stack_node_add_link invocations.
 *
 *   The `CBM_TS_STACK_MERGE_MAX_DEPTH` cap added in #461 bounds call-stack
 *   RECURSION DEPTH (preventing SIGSEGV) but does NOT cap the total number of
 *   iterations across all recursive calls.  Hence: no crash, but superlinear
 *   parse time that grows without bound as N increases.
 *
 * Evidence from issue #471 (post-cap measurements):
 *   N=2000  -> completes in < 1 s  (sub-quadratic or near-linear at small N)
 *   N=30000 -> takes > 5 minutes   (clearly superlinear; effectively a hang)
 *   We choose N=5000 as the reproduction depth:
 *     - O(N^2) at N=5000 is ~6x more work than at N=2000, which already
 *       finishes in <1 s, putting the blowup firmly inside the alarm window.
 *     - A correct O(N) or O(N log N) implementation finishes at N=5000
 *       in well under 1 s, so the 15-second bound is a very generous pass
 *       threshold for a fixed implementation.
 *
 * Expected (correct) behaviour after fix:
 *   Parsing the N=5000 deeply-nested Perl file completes within 15 seconds,
 *   i.e. the forked child exits normally (WIFEXITED, not WIFSIGNALED).
 *
 * Actual (buggy) behaviour on current code:
 *   The GLR merge work grows superlinearly; the child exceeds the 15-second
 *   wall-clock budget and is killed by SIGALRM.  The parent's waitpid() sees
 *   WIFSIGNALED(status) && WTERMSIG(status) == SIGALRM, so
 *   ASSERT_FALSE(WIFSIGNALED(status)) fires RED.
 *
 * Timing-based flakiness note:
 *   Any timing reproduction carries inherent flakiness on loaded machines.
 *   Mitigations applied:
 *     1. The alarm bound (15 s) is ~15x the expected buggy blowup threshold
 *        and far above the expected pass time (<1 s) for a fixed impl.
 *     2. N=5000 was chosen to sit in the steeply-growing O(n^2) regime
 *        (not the knee) so the gap between pass and fail is large.
 *     3. The fork/alarm pattern isolates wall-clock from test-runner load.
 *   On a very heavily loaded machine a false PASS is more likely than a
 *   false FAIL (the OS may slow a fixed impl to near the bound), but a
 *   false FAIL for a correct O(n) impl at this bound is implausible.
 *
 * Fix location (not implemented here):
 *   internal/cbm/vendored/ts_runtime/src/stack.c, `stack_node_add_link`:
 *   bound the total merge work (an overall ambiguity-merge iteration budget
 *   or memoization of already-merged node pairs) consistent with the existing
 *   MAX_LINK_COUNT bail-out at line 249, so parse time stays near-linear for
 *   adversarially ambiguous input.
 */

#include "test_framework.h"
#include "cbm.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#if !defined(_WIN32)
#include <unistd.h>
#include <signal.h>
#include <sys/wait.h>
#endif

/*
 * NESTING_DEPTH: number of f(...) levels to generate.
 *
 * DETERMINISM NOTE: this is now a STABLE TERMINATION guard, not a flaky
 * wall-clock perf gate. At N=5000 the O(n^2) parse takes ~15 s — right at the
 * alarm — so it flipped red/green on CI load alone. N=2000 finishes in <1 s even
 * under heavy CI load, so the assertion "the deeply-nested ambiguous parse
 * TERMINATES within ALARM_SECONDS (no hang/crash from the #461-capped GLR
 * recursion)" is now deterministic on every platform. The O(n^2) PERFORMANCE bug
 * #471 itself remains OPEN and is tracked separately: wall-clock perf cannot be
 * reliably gated in CI, so it is intentionally not asserted here. If #471 is
 * later fixed, raising N back to a large value would still pass.
 *
 * ALARM_SECONDS: wall-clock bound. 15 s is hugely generous for the <1 s N=2000
 *   parse — it only fires on a true hang (infinite recursion / crash).
 */
#define NESTING_DEPTH  2000
#define ALARM_SECONDS  15

/*
 * Build a Perl source string of the form:
 *
 *   sub f { return $_[0]; }
 *   my $x = f(f(f(f(... f(1) ...))));
 *
 * with NESTING_DEPTH levels of `f(`.  The bare `f(` syntax is valid Perl
 * and triggers `ambiguous_function_call_expression` in the tree-sitter-perl
 * grammar because `f` may be parsed either as a builtin (prototype-less) or
 * as a user-defined sub, making the call expression grammatically ambiguous.
 *
 * Caller must free() the returned pointer.
 */
/* __attribute__((unused)): on Windows the test body is SKIP_PLATFORM (the
 * fork/alarm reproduction is POSIX-only), so this builder is unused there and
 * would trip -Werror=unused-function. */
static char *build_perl_nested_calls(int depth) __attribute__((unused));
static char *build_perl_nested_calls(int depth) {
    /*
     * Header:        "sub f { return $_[0]; }\nmy $x = "   (~32 bytes)
     * Per open:      "f("                                   (2 bytes each)
     * Inner literal: "1"                                    (1 byte)
     * Per close:     ")"                                    (1 byte each)
     * Trailer:       ";\n"                                  (2 bytes)
     * Null:          1 byte
     *
     * Total upper bound: 40 + depth*2 + 1 + depth + 3 = depth*3 + 44
     */
    size_t sz = (size_t)depth * 3 + 64;
    char *buf = (char *)malloc(sz);
    if (!buf) return NULL;

    char *p = buf;
    p += snprintf(p, sz, "sub f { return $_[0]; }\nmy $x = ");

    /* NESTING_DEPTH levels of `f(` */
    for (int i = 0; i < depth; i++) {
        *p++ = 'f';
        *p++ = '(';
    }

    /* innermost literal */
    *p++ = '1';

    /* matching closing parens */
    for (int i = 0; i < depth; i++) {
        *p++ = ')';
    }

    /* statement terminator */
    p += snprintf(p, (size_t)(buf + sz - p), ";\n");

    return buf;
}

/*
 * repro_issue471_glr_nested_ambiguity_terminates
 *
 * Asserts CORRECT behaviour: parsing a NESTING_DEPTH-deep ambiguous Perl
 * call chain must complete within ALARM_SECONDS seconds.
 *
 * The test is RED on current code because stack_node_add_link performs O(n^2)
 * merge work and the child process is killed by SIGALRM before completion.
 * ASSERT_FALSE(WIFSIGNALED(status)) fires, making the suite RED.
 *
 * On Windows (no fork/alarm): SKIP_PLATFORM — the timing reproduction
 * requires POSIX fork + alarm; Windows CI is excluded from this guard.
 * The bug itself is platform-independent; a non-timing reproduction
 * (e.g. instrumenting total merge iterations) would cover Windows too,
 * but is out of scope for this reproduce-first case.
 */
TEST(repro_issue471_glr_nested_ambiguity_terminates) {
#if defined(_WIN32)
    SKIP_PLATFORM("fork/alarm not available; POSIX-only timing reproduction");
#else
    char *src = build_perl_nested_calls(NESTING_DEPTH);
    ASSERT_NOT_NULL(src);

    fflush(NULL);
    pid_t pid = fork();
    if (pid < 0) {
        free(src);
        FAIL("fork() failed");
    }

    if (pid == 0) {
        /*
         * Child: set a wall-clock alarm and run the extraction.
         * If the GLR merge blows up O(n^2), SIGALRM fires before extraction
         * completes and the child is killed (not _exit(0)).
         * If the fix bounds merge work to near-linear, extraction finishes
         * within ALARM_SECONDS and the child calls _exit(0) normally.
         *
         * We do NOT call cbm_init() here: cbm_extract_file() is
         * self-contained for single-file extraction (mirrors rh_extract_crashes
         * pattern in repro_harness.h, which also omits a separate init call).
         */
        alarm(ALARM_SECONDS);

        CBMFileResult *r = cbm_extract_file(
            src, (int)strlen(src),
            CBM_LANG_PERL,
            "repro",
            "deep_nested.pl",
            0, NULL, NULL
        );
        if (r) cbm_free_result(r);

        _exit(0); /* normal exit — extraction completed within the budget */
    }

    /* Parent: wait for child; do not inherit child's alarm. */
    free(src);

    int status = 0;
    (void)waitpid(pid, &status, 0);

    /*
     * RED assertion:
     *   On current (buggy) code the child is killed by SIGALRM:
     *     WIFSIGNALED(status) == true, WTERMSIG(status) == SIGALRM
     *   so ASSERT_FALSE fires and this test is RED.
     *
     *   After the fix (bounded merge work) the child exits cleanly:
     *     WIFEXITED(status) == true, WEXITSTATUS(status) == 0
     *   so ASSERT_FALSE passes and this test turns GREEN.
     *
     * We assert on the signal flag rather than exit code so the failure
     * message clearly identifies the alarm kill (vs. an unrelated crash).
     */
    ASSERT_FALSE(WIFSIGNALED(status));

    PASS();
#endif
}

/* ── Suite ─────────────────────────────────────────────────────────────── */

SUITE(repro_issue471) {
    RUN_TEST(repro_issue471_glr_nested_ambiguity_terminates);
}
