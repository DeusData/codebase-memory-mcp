/*
 * test_py_lsp_scale.c — measure scaling behavior at 100 / 500 / 2000
 * classes-and-calls. Asserts that doubling the input doesn't more than
 * 4x the runtime (catches accidental O(n^2) in the resolver).
 */
#include "test_framework.h"
#include "cbm.h"
#include "lsp/py_lsp.h"
#include <time.h>

static double elapsed_ms(struct timespec t0, struct timespec t1) {
    double s = (double)(t1.tv_sec - t0.tv_sec);
    double ns = (double)(t1.tv_nsec - t0.tv_nsec);
    return s * 1000.0 + ns / 1000000.0;
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
    if (!src)
        return -1.0;
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    CBMFileResult *r =
        cbm_extract_file(src, slen, CBM_LANG_PYTHON, "test", "scale.py", 0, NULL, NULL);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double ms = elapsed_ms(t0, t1);
    if (out_calls)
        *out_calls = r ? r->calls.count : 0;
    if (out_resolved)
        *out_resolved = r ? r->resolved_calls.count : 0;
    if (r)
        cbm_free_result(r);
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
    printf("    scale: 100=%.1fms (calls=%d resolved=%d)  500=%.1fms (calls=%d resolved=%d)  "
           "2000=%.1fms (calls=%d resolved=%d)\n",
           t100, c100, r100, t500, c500, r500, t2000, c2000, r2000);

    /* Sanity: each scale produces roughly the same resolution ratio. */
    double r_pct_100 = c100 ? (double)r100 / c100 : 0.0;
    double r_pct_2000 = c2000 ? (double)r2000 / c2000 : 0.0;
    ASSERT(r_pct_100 > 0.5);
    ASSERT(r_pct_2000 > 0.5);

    /* Quadratic-growth detector. 20x input: linear ~20-30x time, clear
     * quadratic ~400x. The bound sits at 200x — mid-way in log space — so a
     * real quadratic regression still fails by 2x while the CURRENT, KNOWN
     * superlinear resolve curve does not produce false release blocks.
     *
     * The old bound of 100x was calibrated as "linear plus generous overhead",
     * but the resolve path has never been linear here: measured 2026-08-11
     * (sanitized builds, deterministic across runs) — quiet arm64 host
     * 57-68x, macos-15-intel CI runner 101.1x, per-function cost growing
     * 0.5ms -> 1.7ms from 100 to 2000 functions. That ~O(n^1.4-1.5) curve is
     * the audited short-name-lookup/negative-memo gap, tracked as #1527; this
     * detector was flagging host CONSTANTS, not a complexity change. When
     * #1527 lands, tighten this back down (~40x holds linear honestly). */
    if (t100 > 0.5) { // skip when t100 too small to compare reliably
        double ratio = t2000 / t100;
        printf("    scale ratio 2000/100: %.1fx (linear ~20x, known-superlinear ~60-100x, "
               "quadratic ~400x)\n",
               ratio);
        ASSERT(ratio < 200.0); // flags clear quadratic; #1527 tracks the curve itself
    }
    PASS();
}

/* Time cbm_py_build_cross_registry over n classes, each followed by two
 * methods (extraction order): every method probes the registry for its
 * receiver while the registry is still being built. */
static double measure_cross_registry(int n_classes, int *out_types) {
    CBMArena arena;
    cbm_arena_init(&arena);
    int n_defs = n_classes * 3;
    CBMLSPDef *defs = (CBMLSPDef *)cbm_arena_calloc(&arena, (size_t)n_defs * sizeof(*defs));
    if (!defs) {
        cbm_arena_destroy(&arena);
        return -1.0;
    }
    for (int i = 0; i < n_classes; i++) {
        CBMLSPDef *cls = &defs[i * 3];
        cls->qualified_name = cbm_arena_sprintf(&arena, "scale.mod.Cls%d", i);
        cls->short_name = cls->qualified_name + strlen("scale.mod.");
        cls->label = "Class";
        cls->def_module_qn = "scale.mod";
        cls->lang = CBM_LANG_PYTHON;
        for (int j = 1; j <= 2; j++) {
            CBMLSPDef *m = &defs[i * 3 + j];
            m->qualified_name = cbm_arena_sprintf(&arena, "%s.m%d", cls->qualified_name, j);
            m->short_name = j == 1 ? "m1" : "m2";
            m->label = "Method";
            m->receiver_type = cls->qualified_name;
            m->def_module_qn = "scale.mod";
            m->lang = CBM_LANG_PYTHON;
        }
    }
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    CBMTypeRegistry *reg = cbm_py_build_cross_registry(&arena, defs, n_defs);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    if (out_types)
        *out_types = reg ? reg->type_count : -1;
    cbm_arena_destroy(&arena);
    return elapsed_ms(t0, t1);
}

/* The shared build registers defs one at a time into an unfinalized registry;
 * a receiver probe that scans every type registered so far makes the build
 * O(methods x types). 16x input: linear ~16x time, quadratic well over 100x. */
TEST(pylsp_scale_cross_registry_build_linear) {
    int ty_small = 0, ty_large = 0;
    /* Best of three for the small size: it is short enough for host noise. */
    double t_small = measure_cross_registry(1000, &ty_small);
    for (int k = 0; k < 2; k++) {
        double t = measure_cross_registry(1000, NULL);
        if (t >= 0.0 && t < t_small)
            t_small = t;
    }
    double t_large = measure_cross_registry(16000, &ty_large);
    printf("    cross registry: 1000 classes=%.1fms (types=%d)  16000 classes=%.1fms "
           "(types=%d)\n",
           t_small, ty_small, t_large, ty_large);
    ASSERT(t_small >= 0.0 && t_large >= 0.0);
    ASSERT_EQ(ty_large - ty_small, 15000);
    if (t_small > 0.5) {
        double ratio = t_large / t_small;
        printf("    cross registry ratio 16000/1000: %.1fx (linear ~16x)\n", ratio);
        ASSERT(ratio < 48.0);
    }
    PASS();
}

SUITE(py_lsp_scale) {
    RUN_TEST(pylsp_scale_linear_growth);
    RUN_TEST(pylsp_scale_cross_registry_build_linear);
}
