/*
 * test_framework.h — Minimal C test framework.
 *
 * Features:
 *   - Assertion macros with file:line on failure
 *   - Test suite grouping
 *   - Pass/fail counting with summary
 *   - Color output (when isatty)
 *
 * Usage:
 *   TEST(my_test) {
 *       ASSERT_EQ(1 + 1, 2);
 *       ASSERT_STR_EQ("hello", "hello");
 *       PASS();
 *   }
 *
 *   SUITE(my_suite) {
 *       RUN_TEST(my_test);
 *   }
 *
 *   int main(void) {
 *       RUN_SUITE(my_suite);
 *       TEST_SUMMARY();
 *   }
 */
#ifndef TEST_FRAMEWORK_H
#define TEST_FRAMEWORK_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <unistd.h>

/* ── Global counters (defined in test_main.c) ──────────────────── */

extern int tf_pass_count;
extern int tf_fail_count;
extern int tf_skip_count;

/* ── Color helpers ─────────────────────────────────────────────── */

static inline const char *tf_green(void) {
    return isatty(1) ? "\033[32m" : "";
}
static inline const char *tf_red(void) {
    return isatty(1) ? "\033[31m" : "";
}
static inline const char *tf_dim(void) {
    return isatty(1) ? "\033[90m" : "";
}
static inline const char *tf_reset(void) {
    return isatty(1) ? "\033[0m" : "";
}

/* ── Test definition ───────────────────────────────────────────── */

#define TEST(name) static int test_##name(void)

#define PASS() return 0

#define SKIP(reason)                                             \
    do {                                                         \
        printf("%sSKIP%s (%s)\n", tf_dim(), tf_reset(), reason); \
        tf_skip_count++;                                         \
        return -1;                                               \
    } while (0)

/* Hard failure with a message — for setup/environment failures that must NOT be
 * silently skipped (a test that cannot establish its preconditions has FAILED,
 * not "skipped"). Mirrors the ASSERT failure path (red FAIL + file:line). */
#define FAIL(reason)                                                                         \
    do {                                                                                     \
        printf("  %sFAIL%s %s:%d: %s\n", tf_red(), tf_reset(), __FILE__, __LINE__, (reason)); \
        return 1;                                                                            \
    } while (0)

/* The ONLY tolerable skip: a test that is inherently platform-specific and does
 * not apply on the current OS (e.g. a Windows-only test running on macOS).
 * Distinct from SKIP() so the no-skips linter (scripts/check-no-test-skips.sh)
 * can allow these and reject every other skip. Use sparingly + with a reason
 * that names the platform. Prefer compile-gating (#ifdef) where practical. */
#define SKIP_PLATFORM(reason)                                              \
    do {                                                                   \
        printf("%sSKIP%s (platform: %s)\n", tf_dim(), tf_reset(), reason); \
        tf_skip_count++;                                                   \
        return -1;                                                         \
    } while (0)

/* ── Assertions ────────────────────────────────────────────────── */

#define ASSERT(cond)                                                                           \
    do {                                                                                       \
        if (!(cond)) {                                                                         \
            printf("  %sFAIL%s %s:%d: ASSERT(%s)\n", tf_red(), tf_reset(), __FILE__, __LINE__, \
                   #cond);                                                                     \
            return 1;                                                                          \
        }                                                                                      \
    } while (0)

#define ASSERT_EQ(a, b)                                                                         \
    do {                                                                                        \
        long long _a = (long long)(a), _b = (long long)(b);                                     \
        if (_a != _b) {                                                                         \
            printf("  %sFAIL%s %s:%d: %s == %lld, expected %s == %lld\n", tf_red(), tf_reset(), \
                   __FILE__, __LINE__, #a, _a, #b, _b);                                         \
            return 1;                                                                           \
        }                                                                                       \
    } while (0)

#define ASSERT_NEQ(a, b)                                                                       \
    do {                                                                                       \
        long long _a = (long long)(a), _b = (long long)(b);                                    \
        if (_a == _b) {                                                                        \
            printf("  %sFAIL%s %s:%d: %s == %s (both %lld)\n", tf_red(), tf_reset(), __FILE__, \
                   __LINE__, #a, #b, _a);                                                      \
            return 1;                                                                          \
        }                                                                                      \
    } while (0)

#define ASSERT_GT(a, b)                                                                   \
    do {                                                                                  \
        long long _a = (long long)(a), _b = (long long)(b);                               \
        if (_a <= _b) {                                                                   \
            printf("  %sFAIL%s %s:%d: %s (%lld) not > %s (%lld)\n", tf_red(), tf_reset(), \
                   __FILE__, __LINE__, #a, _a, #b, _b);                                   \
            return 1;                                                                     \
        }                                                                                 \
    } while (0)

#define ASSERT_GTE(a, b)                                                                   \
    do {                                                                                   \
        long long _a = (long long)(a), _b = (long long)(b);                                \
        if (_a < _b) {                                                                     \
            printf("  %sFAIL%s %s:%d: %s (%lld) not >= %s (%lld)\n", tf_red(), tf_reset(), \
                   __FILE__, __LINE__, #a, _a, #b, _b);                                    \
            return 1;                                                                      \
        }                                                                                  \
    } while (0)

#define ASSERT_LT(a, b)                                                                   \
    do {                                                                                  \
        long long _a = (long long)(a), _b = (long long)(b);                               \
        if (_a >= _b) {                                                                   \
            printf("  %sFAIL%s %s:%d: %s (%lld) not < %s (%lld)\n", tf_red(), tf_reset(), \
                   __FILE__, __LINE__, #a, _a, #b, _b);                                   \
            return 1;                                                                     \
        }                                                                                 \
    } while (0)

#define ASSERT_LTE(a, b)                                                                   \
    do {                                                                                   \
        long long _a = (long long)(a), _b = (long long)(b);                                \
        if (_a > _b) {                                                                     \
            printf("  %sFAIL%s %s:%d: %s (%lld) not <= %s (%lld)\n", tf_red(), tf_reset(), \
                   __FILE__, __LINE__, #a, _a, #b, _b);                                    \
            return 1;                                                                      \
        }                                                                                  \
    } while (0)

#define ASSERT_STR_EQ(a, b)                                                                \
    do {                                                                                   \
        const char *_a = (a), *_b = (b);                                                   \
        if (_a == NULL && _b == NULL)                                                      \
            break;                                                                         \
        if (_a == NULL || _b == NULL || strcmp(_a, _b) != 0) {                             \
            printf("  %sFAIL%s %s:%d: \"%s\" != \"%s\"\n", tf_red(), tf_reset(), __FILE__, \
                   __LINE__, _a ? _a : "(null)", _b ? _b : "(null)");                      \
            return 1;                                                                      \
        }                                                                                  \
    } while (0)

#define ASSERT_STR_NEQ(a, b)                                                                    \
    do {                                                                                        \
        const char *_a = (a), *_b = (b);                                                        \
        if (_a != NULL && _b != NULL && strcmp(_a, _b) == 0) {                                  \
            printf("  %sFAIL%s %s:%d: strings equal: \"%s\"\n", tf_red(), tf_reset(), __FILE__, \
                   __LINE__, _a);                                                               \
            return 1;                                                                           \
        }                                                                                       \
    } while (0)

#define ASSERT_NULL(a)                                                                             \
    do {                                                                                           \
        if ((a) != NULL) {                                                                         \
            printf("  %sFAIL%s %s:%d: %s is not NULL\n", tf_red(), tf_reset(), __FILE__, __LINE__, \
                   #a);                                                                            \
            return 1;                                                                              \
        }                                                                                          \
    } while (0)

#define ASSERT_NOT_NULL(a)                                                                     \
    do {                                                                                       \
        if ((a) == NULL) {                                                                     \
            printf("  %sFAIL%s %s:%d: %s is NULL\n", tf_red(), tf_reset(), __FILE__, __LINE__, \
                   #a);                                                                        \
            return 1;                                                                          \
        }                                                                                      \
    } while (0)

#define ASSERT_TRUE(a) ASSERT(a)
#define ASSERT_FALSE(a) ASSERT(!(a))

#define ASSERT_MEM_EQ(a, b, n)                                                             \
    do {                                                                                   \
        if (memcmp((a), (b), (n)) != 0) {                                                  \
            printf("  %sFAIL%s %s:%d: memory differs (%zu bytes)\n", tf_red(), tf_reset(), \
                   __FILE__, __LINE__, (size_t)(n));                                       \
            return 1;                                                                      \
        }                                                                                  \
    } while (0)

#define ASSERT_FLOAT_EQ(a, b, eps)                                                               \
    do {                                                                                         \
        double _a = (double)(a), _b = (double)(b);                                               \
        if (fabs(_a - _b) > (eps)) {                                                             \
            printf("  %sFAIL%s %s:%d: %s (%g) != %s (%g) within eps %g\n", tf_red(), tf_reset(), \
                   __FILE__, __LINE__, #a, _a, #b, _b, (double)(eps));                           \
            return 1;                                                                            \
        }                                                                                        \
    } while (0)

/* ── Test runner ───────────────────────────────────────────────── */

#define RUN_TEST(name)                                    \
    do {                                                  \
        printf("  %-55s", #name);                         \
        fflush(stdout);                                   \
        int _result = test_##name();                      \
        if (_result == 0) {                               \
            printf("%sPASS%s\n", tf_green(), tf_reset()); \
            tf_pass_count++;                              \
        } else if (_result == -1) {                       \
            /* skip — already printed */                  \
        } else {                                          \
            tf_fail_count++;                              \
        }                                                 \
    } while (0)

/* ── Suite grouping ────────────────────────────────────────────── */

#define SUITE(name) void suite_##name(void)

#define RUN_SUITE(name)                                            \
    do {                                                           \
        printf("\n%s=== %s ===%s\n", tf_dim(), #name, tf_reset()); \
        suite_##name();                                            \
    } while (0)

/* ── Summary ───────────────────────────────────────────────────── */

#define TEST_SUMMARY()                                                       \
    do {                                                                     \
        printf("\n────────────────────────────────────────────\n");          \
        printf("  %s%d passed%s", tf_green(), tf_pass_count, tf_reset());    \
        if (tf_fail_count > 0)                                               \
            printf(", %s%d failed%s", tf_red(), tf_fail_count, tf_reset());  \
        if (tf_skip_count > 0)                                               \
            printf(", %s%d skipped%s", tf_dim(), tf_skip_count, tf_reset()); \
        printf("\n────────────────────────────────────────────\n\n");        \
        return tf_fail_count > 0 ? 1 : 0;                                    \
    } while (0)

#endif /* TEST_FRAMEWORK_H */
