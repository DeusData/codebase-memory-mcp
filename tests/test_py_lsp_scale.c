/*
 * test_py_lsp_scale.c — measure scaling behavior at 100 / 500 / 2000
 * classes-and-calls. Asserts that doubling the input doesn't more than
 * 4x the runtime (catches accidental O(n^2) in the resolver).
 */
#include "test_framework.h"
#include "cbm.h"
#include "lsp/py_lsp.h"
#include <time.h>

static double elapsed_cpu_ms(clock_t t0, clock_t t1) {
    if (t0 == (clock_t)-1 || t1 == (clock_t)-1 || t1 < t0) {
        return -1.0;
    }
    return (double)(t1 - t0) * 1000.0 / (double)CLOCKS_PER_SEC;
}

/* Build N synthetic class/call pairs into an arena-backed buffer. */
static char *build_fixture(int n_classes, int *out_len) {
    /* Per class: ~140 chars (5-line def). Per call: ~50 chars. Overhead
     * for the class number digits scales with log10(n) but the constant
     * 256 covers up to 9-digit indices comfortably. */
    int approx = n_classes * 256 + 1024;
    char *buf = (char *)malloc((size_t)approx);
    if (!buf)
        return NULL;
    int pos = 0;
    pos += snprintf(buf + pos, (size_t)(approx - pos), "from typing import Self\n");
    for (int i = 0; i < n_classes; i++) {
        int n = snprintf(buf + pos, (size_t)(approx - pos),
                         "class Cls%d:\n"
                         "    def method(self) -> int:\n"
                         "        return %d\n"
                         "    def chain(self) -> Self:\n"
                         "        return self\n",
                         i, i);
        if (n < 0 || pos + n >= approx)
            break;
        pos += n;
    }
    int n = snprintf(buf + pos, (size_t)(approx - pos), "def use():\n");
    pos += n;
    for (int i = 0; i < n_classes; i++) {
        int m = snprintf(buf + pos, (size_t)(approx - pos),
                         "    Cls%d().chain().chain().method()\n", i);
        if (m < 0 || pos + m >= approx)
            break;
        pos += m;
    }
    *out_len = pos;
    return buf;
}

static double measure(int n_classes, int *out_calls, int *out_resolved) {
    int slen = 0;
    char *src = build_fixture(n_classes, &slen);
    if (!src) return -1.0;
    /*
     * This is a complexity guard, so measure process work rather than elapsed
     * wall time. A scheduler pause during only the large fixture otherwise
     * inflates the ratio and reports a quadratic regression that did not occur.
     * ISO C clock() also keeps the measurement portable across supported hosts.
     */
    clock_t t0 = clock();
    CBMFileResult *r = cbm_extract_file(src, slen, CBM_LANG_PYTHON,
        "test", "scale.py", 0, NULL, NULL);
    clock_t t1 = clock();
    double ms = elapsed_cpu_ms(t0, t1);
    if (out_calls) *out_calls = r ? r->calls.count : 0;
    if (out_resolved) *out_resolved = r ? r->resolved_calls.count : 0;
    if (r) cbm_free_result(r);
    free(src);
    return ms;
}

TEST(pylsp_scale_linear_growth) {
    int c100 = 0, r100 = 0;
    int c500 = 0, r500 = 0;
    int c2000 = 0, r2000 = 0;
    double t100 = measure(100, &c100, &r100);
    double t500 = measure(500, &c500, &r500);
    double t2000 = measure(2000, &c2000, &r2000);
    ASSERT(t100 >= 0.0);
    ASSERT(t500 >= 0.0);
    ASSERT(t2000 >= 0.0);
    printf("    scale: 100=%.1fms (calls=%d resolved=%d)  500=%.1fms (calls=%d resolved=%d)  2000=%.1fms (calls=%d resolved=%d)\n",
        t100, c100, r100, t500, c500, r500, t2000, c2000, r2000);

    /* Sanity: each scale produces roughly the same resolution ratio. */
    double r_pct_100 = c100 ? (double)r100 / c100 : 0.0;
    double r_pct_2000 = c2000 ? (double)r2000 / c2000 : 0.0;
    ASSERT(r_pct_100 > 0.5);
    ASSERT(r_pct_2000 > 0.5);

    /* Linear growth check: 20x input should be at most 35x time
     * (allowing constant-factor overhead). Confirm a failure against the
     * less noise-sensitive 500-to-2000 interval: clear quadratic growth is
     * 16x there, so 8x retains a 2x detection margin without letting a noisy
     * 100-class baseline fabricate an asymptotic regression. */
    if (t100 > 0.5) {  // skip when t100 too small to compare reliably
        double ratio_100 = t2000 / t100;
        double ratio_500 = t500 > 0.5 ? t2000 / t500 : 16.0;
        printf("    scale ratios: 2000/100=%.1fx (linear ~20x, quadratic ~400x), "
               "2000/500=%.1fx (linear ~4x, quadratic ~16x)\n",
               ratio_100, ratio_500);
        ASSERT(ratio_100 < 35.0 || ratio_500 < 8.0);
    }
    PASS();
}

SUITE(py_lsp_scale) {
    RUN_TEST(pylsp_scale_linear_growth);
}
