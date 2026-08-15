/*
 * sanitized.h — One spelling of "is this binary instrumented?".
 *
 * Timing budgets, retry windows and stack floors all have to widen under a
 * sanitizer, so several places need to ask this question. Before this header
 * they each asked it differently — four sites, four spellings, one of which
 * (the C# LSP bench) only recognised ASan and therefore ran a NATIVE 200ms
 * budget on a ThreadSanitizer binary. Ask it here instead.
 *
 * The two sources are deliberate and neither replaces the other:
 *
 *   CBM_SANITIZED_BUILD — set by the build system (Makefile.cbm), and the ONLY
 *   source that can answer for UBSan and trap-UBSan: undefined-behaviour
 *   instrumentation leaves no macro and no __has_feature bit behind, so no
 *   probe can see it. This is why the build system stays the source of truth.
 *
 *   Compiler probes — the backstop for the three sanitizers that DO announce
 *   themselves, for when a new build lane forgets to pass the define. That is
 *   not hypothetical: CFLAGS_TSAN never included SANITIZED_DEFINE (it keys off
 *   $(SANITIZE), which TSan does not use), so every sanitized budget compiled
 *   to its native value on the one leg they were written for, and
 *   `subprocess_run_spawn_failure` failed on the very PR meant to fix it.
 *
 * Both clang and GCC spellings are listed because they disagree: clang reports
 * thread and memory instrumentation through __has_feature only, GCC through
 * __SANITIZE_*__ only, and __SANITIZE_THREAD__ alone — which is what the old
 * per-site conditions used — is invisible on the clang lane that broke.
 *
 * Deliberately no #error when a probe fires without the define. Promoting
 * leaves the binary CORRECT while the build lane is fixed, whereas an error
 * would break it, and would also break an out-of-tree
 * `make CFLAGS_EXTRA=-fsanitize=address` that never went near Makefile.cbm.
 */
#ifndef CBM_SANITIZED_H
#define CBM_SANITIZED_H

/* Define __has_feature away where it does not exist. `defined(__has_feature) &&
 * __has_feature(...)` is NOT a substitute: && does not spare a preprocessor
 * that lacks the builtin from parsing the call, and there `__has_feature(x)`
 * becomes `0 (0)` — a syntax error rather than a 0. Nesting the arm under
 * `#elif defined(__has_feature)` compiles everywhere but breaks cppcheck, which
 * walks every configuration and rejects the file outright with "failed to
 * evaluate #if condition, undefined function-like macro invocation". This is
 * the idiom clang documents, and the one tests/test_mem.c already uses. */
#ifndef __has_feature
#define __has_feature(x) 0
#endif

#if defined(CBM_SANITIZED_BUILD) && CBM_SANITIZED_BUILD
#define CBM_SANITIZED 1
#elif __has_feature(address_sanitizer) || __has_feature(thread_sanitizer) || \
    __has_feature(memory_sanitizer)
#define CBM_SANITIZED 1
#elif defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__)
#define CBM_SANITIZED 1
#else
#define CBM_SANITIZED 0
#endif

#endif /* CBM_SANITIZED_H */
