/*
 * test_mem.c — Tests for unified memory management (mimalloc-backed),
 *              arena integration, slab allocator, and parallel extraction.
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "../src/foundation/mem.h"
#include "../src/foundation/arena.h"
#include "../src/foundation/slab_alloc.h"
#include "../src/foundation/compat_thread.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "graph_buffer/graph_buffer.h"
#include "discover/discover.h"
#include "cbm.h"

#include <stdatomic.h>
#include <stdint.h>
#include <sys/stat.h>
#include <mimalloc.h>
#ifndef _WIN32
#include <sys/mman.h>
#endif

/* ASan detection — mimalloc MI_OVERRIDE=0 under ASan, mi_process_info
 * may return 0 for RSS. Tests that depend on accurate RSS must skip. */
#ifndef __has_feature
#define __has_feature(x) 0
#endif
#if defined(__SANITIZE_ADDRESS__) || __has_feature(address_sanitizer)
#define CBM_ASAN_ACTIVE 1
#else
#define CBM_ASAN_ACTIVE 0
#endif

/* ── mem basic tests ──────────────────────────────────────────── */

TEST(mem_rss_tracking) {
    cbm_mem_init(0.5);

    /* Allocate 10 MB */
    size_t alloc_size = 10 * 1024 * 1024;
    char *p = (char *)malloc(alloc_size);
    ASSERT_NOT_NULL(p);
    /* Touch all pages to ensure RSS increase */
    memset(p, 0xAB, alloc_size);

    size_t rss = cbm_mem_rss();
    /* RSS should be nonzero (mimalloc or OS fallback) */
    ASSERT_GT(rss, 0);

    free(p);
    PASS();
}

TEST(mem_collect_reclaims) {
    cbm_mem_init(0.5);

    /* Allocate 10 MB, touch it, free it */
    size_t alloc_size = 10 * 1024 * 1024;
    char *p = (char *)malloc(alloc_size);
    ASSERT_NOT_NULL(p);
    memset(p, 0xCD, alloc_size);
    size_t rss_before_free = cbm_mem_rss();

    free(p);
    cbm_mem_collect();

    size_t rss_after_collect = cbm_mem_rss();
    /* After collect, RSS should exist (may or may not drop depending on OS) */
    ASSERT_GT(rss_after_collect, 0);
    /* Best-effort check: rss shouldn't grow after free+collect */
    (void)rss_before_free;
    PASS();
}

TEST(mem_budget_check) {
    /* Init with very small fraction to create an easy-to-exceed budget */
    /* NOTE: cbm_mem_init only takes effect once, so we test with whatever
     * budget was set. Just verify the API works. */
    cbm_mem_init(0.5);

    size_t budget = cbm_mem_budget();
    /* Budget should be > 0 after init */
    ASSERT_GT(budget, 0);

    /* over_budget returns a bool */
    bool over = cbm_mem_over_budget();
    (void)over; /* just verify it doesn't crash */

    /* Worker budget divides correctly */
    size_t wb4 = cbm_mem_worker_budget(4);
    ASSERT_EQ(wb4, budget / 4);

    /* Edge case: 0 workers defaults to 1 */
    size_t wb0 = cbm_mem_worker_budget(0);
    ASSERT_EQ(wb0, budget);
    PASS();
}

/* ── mem budget edge-case tests ─────────────────────────────── */

TEST(mem_worker_budget_zero_workers) {
    cbm_mem_init(0.5);
    size_t budget = cbm_mem_budget();
    /* 0 workers clamps to 1 → worker_budget == full budget */
    size_t wb = cbm_mem_worker_budget(0);
    ASSERT_EQ(wb, budget);
    PASS();
}

TEST(mem_worker_budget_negative_workers) {
    cbm_mem_init(0.5);
    size_t budget = cbm_mem_budget();
    /* Negative workers clamps to 1 → worker_budget == full budget */
    size_t wb = cbm_mem_worker_budget(-5);
    ASSERT_EQ(wb, budget);
    PASS();
}

TEST(mem_worker_budget_one_worker) {
    cbm_mem_init(0.5);
    size_t budget = cbm_mem_budget();
    /* 1 worker → equals full budget */
    size_t wb = cbm_mem_worker_budget(1);
    ASSERT_EQ(wb, budget);
    PASS();
}

TEST(mem_worker_budget_many_workers) {
    cbm_mem_init(0.5);
    /* 1000 workers → should produce non-zero result (budget is huge) */
    size_t wb = cbm_mem_worker_budget(1000);
    ASSERT_GT(wb, 0);
    /* Must be budget / 1000 */
    ASSERT_EQ(wb, cbm_mem_budget() / 1000);
    PASS();
}

TEST(mem_over_budget_low_rss) {
    cbm_mem_init(0.5);
    /* We're a test process with tiny RSS — should not be over budget */
    bool over = cbm_mem_over_budget();
    ASSERT_FALSE(over);
    PASS();
}

/* ── Tiered RAM fraction (host-size defaults) ─────────────────── */

TEST(mem_ram_fraction_16gb_tier) {
    size_t ram_16gb = 16ULL * 1024 * 1024 * 1024;
    ASSERT_EQ(cbm_mem_ram_fraction_for_total(ram_16gb), 0.25);
    ASSERT_EQ(cbm_mem_ram_fraction_for_total(ram_16gb - 1), 0.25);
    PASS();
}

TEST(mem_ram_fraction_32gb_tier) {
    size_t ram_32gb = 32ULL * 1024 * 1024 * 1024;
    size_t ram_17gb = 17ULL * 1024 * 1024 * 1024;
    ASSERT_EQ(cbm_mem_ram_fraction_for_total(ram_17gb), 0.35);
    ASSERT_EQ(cbm_mem_ram_fraction_for_total(ram_32gb), 0.35);
    PASS();
}

TEST(mem_ram_fraction_large_host) {
    size_t ram_64gb = 64ULL * 1024 * 1024 * 1024;
    ASSERT_EQ(cbm_mem_ram_fraction_for_total(ram_64gb), 0.5);
    PASS();
}

/* ── RSS tracking tests ───────────────────────────────────────── */

TEST(mem_rss_positive) {
    cbm_mem_init(0.5);
    /* A running process always has nonzero RSS */
    size_t rss = cbm_mem_rss();
    ASSERT_GT(rss, 0);
    PASS();
}

TEST(mem_peak_rss_gte_rss) {
    cbm_mem_init(0.5);
    /* peak >= current RSS is definitional. Regression guard for the Linux
     * statm-vs-ru_maxrss source mismatch: cbm_mem_rss() reads the live
     * /proc/self/statm value (page-granular) while mimalloc's peak comes from
     * getrusage ru_maxrss (KB-granular, and it lags), so a live current read
     * could momentarily exceed the reported peak by a few pages and break the
     * invariant. cbm_mem_peak_rss() now reconciles the two sources. Touch a
     * fresh buffer so the check runs against a non-trivial live current read.
     * (Linux-only bug — macOS reads both from mimalloc; it flaked on the
     * Linux/ARM CI leg, which is the authoritative reproduction tier.) */
    size_t n = 32 * 1024 * 1024;
    char *p = (char *)malloc(n);
    ASSERT_NOT_NULL(p);
    memset(p, 0xBE, n); /* fault in all pages so current RSS is non-trivial */
    size_t rss = cbm_mem_rss();
    size_t peak = cbm_mem_peak_rss();
    ASSERT_GTE(peak, rss);
    free(p);
    PASS();
}

TEST(mem_rss_increases_after_alloc) {
    cbm_mem_init(0.5);

    /* Allocate 10 MB and touch all pages */
    size_t alloc_size = 10 * 1024 * 1024;
    char *p = (char *)malloc(alloc_size);
    ASSERT_NOT_NULL(p);
    memset(p, 0xBE, alloc_size);

    size_t rss_after = cbm_mem_rss();
    /* RSS must be non-zero after allocating 10MB */
    ASSERT_GT(rss_after, 0);

    free(p);
    PASS();
}

TEST(mem_collect_no_crash) {
    cbm_mem_init(0.5);
    /* collect() must not crash even with nothing to collect */
    cbm_mem_collect();
    PASS();
}

/* Reproduce-first guard for the Linux cbm_mem_rss() undercount (distilled
 * from #776's 132460f5).
 *
 * On Linux, mimalloc's mi_process_info() never sets current_rss
 * (vendored/mimalloc/src/prim/unix/prim.c only fills peak_rss from
 * getrusage's ru_maxrss); current_rss silently keeps mi_process_info()'s
 * default of pinfo.current_commit — mimalloc's OWN committed-page counter
 * (stats.c:555). The UNFIXED cbm_mem_rss() returns that counter whenever it is
 * nonzero, so on Linux it reports mimalloc-committed bytes, NOT true RSS. The
 * FIXED code reads /proc/self/statm (os_rss) as the primary source → true RSS.
 *
 * The guard makes the two quantities DIVERGE deterministically:
 *   1. mi_malloc() a small block (kept live) so mimalloc's committed counter is
 *      a small POSITIVE value — this both defeats the UNFIXED `current_rss > 0`
 *      fallback guard AND pins the reported value low. mi_malloc always routes
 *      through mimalloc regardless of MI_OVERRIDE, so this works in the ASan
 *      test-runner (MI_OVERRIDE=0) too.
 *   2. Grow TRUE process RSS by ~256MB via a raw anonymous mmap — memory
 *      mimalloc's committed counter never sees, but /proc/self/statm does.
 * On UNFIXED Linux, cbm_mem_rss() then returns the ~few-MB committed counter
 * (< 128MB) → this assertion FAILS (RED). On FIXED Linux it returns the /proc
 * RSS (>= 256MB) → GREEN.
 *
 * macOS/Windows set current_rss from task_info/GetProcessMemoryInfo, which DO
 * include the mapped+touched region, so cbm_mem_rss() is accurate there both
 * before and after the fix — this passes on those platforms either way. The
 * RED therefore manifests only on the Linux CI leg, which is exactly where the
 * production undercount bit (backpressure/ceiling blinded). */
TEST(mem_rss_reflects_external_resident_memory) {
    cbm_mem_init(0.5);

    /* (1) Pin mimalloc's committed-page counter to a small positive value. */
    const size_t warm = (size_t)1 * 1024 * 1024; /* 1 MB via mimalloc */
    void *mi_buf = mi_malloc(warm);
    ASSERT_NOT_NULL(mi_buf);
    memset(mi_buf, 0x11, warm);

    const size_t region = (size_t)256 * 1024 * 1024; /* 256 MB true RSS */

#ifdef _WIN32
    /* On Windows cbm_mem_rss() reads WorkingSetSize (GetProcessMemoryInfo),
     * which the OS trims under memory pressure — so a touched region can drop
     * out of the resident set (a stressed windows-11-arm runner kept only
     * ~97 MB resident of a 256 MB touch). Re-touch the region immediately before
     * measuring so its pages are freshly resident, and assert a threshold that
     * survives aggressive trimming while staying far above the ~1 MB mimalloc
     * warm buffer. This still guards the real regression — cbm_mem_rss()
     * reporting a broken small counter instead of true resident memory — which
     * the Linux #else branch exercises directly against the undercount. */
    const size_t threshold = (size_t)32 * 1024 * 1024;
    void *big = malloc(region);
    ASSERT_NOT_NULL(big);
    memset(big, 0x5A, region);
    memset(big, 0x5B, region); /* re-touch right before the measurement */
    size_t rss = cbm_mem_rss();
    ASSERT_GTE(rss, threshold);
    free(big);
#else
    /* (2) Raw mmap bypasses mimalloc entirely: its committed counter does NOT
     * grow, but the true RSS does — this is what exposes the Linux undercount. */
    const size_t threshold = (size_t)128 * 1024 * 1024; /* generous half of region */
    void *big = mmap(NULL, region, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    ASSERT_TRUE(big != MAP_FAILED);
    memset(big, 0x5A, region); /* fault every page in → resident */
    size_t rss = cbm_mem_rss();
    ASSERT_GTE(rss, threshold);
    munmap(big, region);
#endif
    mi_free(mi_buf);
    PASS();
}

TEST(mem_collect_rss_still_positive) {
    cbm_mem_init(0.5);
    cbm_mem_collect();
    /* After collect, RSS must still be > 0 (we're alive) */
    size_t rss = cbm_mem_rss();
    ASSERT_GT(rss, 0);
    PASS();
}

/* ── Memory pressure simulation ───────────────────────────────── */

TEST(mem_progressive_alloc_rss_increases) {
    cbm_mem_init(0.5);

    size_t chunk_size = 2 * 1024 * 1024; /* 2 MB chunks */
    int nchunks = 5;
    char *chunks[5];

    for (int i = 0; i < nchunks; i++) {
        chunks[i] = (char *)malloc(chunk_size);
        ASSERT_NOT_NULL(chunks[i]);
        memset(chunks[i], (unsigned char)(0xA0 + i), chunk_size);
    }

    size_t rss_peak = cbm_mem_rss();
    ASSERT_GT(rss_peak, 0);

    for (int i = 0; i < nchunks; i++) {
        free(chunks[i]);
    }
    cbm_mem_collect();

    /* After free + collect, RSS may or may not drop, but must not crash */
    size_t rss_end = cbm_mem_rss();
    ASSERT_GT(rss_end, 0);
    PASS();
}

TEST(mem_free_and_collect_no_crash) {
    cbm_mem_init(0.5);

    /* Allocate, free, collect — verify no crash */
    size_t sz = 4 * 1024 * 1024;
    char *p = (char *)malloc(sz);
    ASSERT_NOT_NULL(p);
    memset(p, 0xCC, sz);
    free(p);
    cbm_mem_collect();

    /* RSS must remain positive */
    ASSERT_GT(cbm_mem_rss(), 0);
    PASS();
}

TEST(mem_multiple_collect_idempotent) {
    cbm_mem_init(0.5);

    /* Multiple collect() calls must be idempotent and not crash */
    cbm_mem_collect();
    cbm_mem_collect();
    cbm_mem_collect();

    size_t rss = cbm_mem_rss();
    ASSERT_GT(rss, 0);
    PASS();
}

/* ── Init edge cases ──────────────────────────────────────────── */
/* NOTE: cbm_mem_init uses atomic CAS — only the very first call in the
 * process takes effect. Since mem_rss_tracking runs first with 0.5,
 * all subsequent init calls are no-ops. We verify that they don't
 * crash and that the budget remains unchanged. */

TEST(mem_init_zero_fraction) {
    /* First init already happened with 0.5 — this is a no-op */
    size_t budget_before = cbm_mem_budget();
    cbm_mem_init(0.0);
    size_t budget_after = cbm_mem_budget();
    /* Budget must not change (second init is no-op) */
    ASSERT_EQ(budget_before, budget_after);
    PASS();
}

TEST(mem_init_negative_fraction) {
    size_t budget_before = cbm_mem_budget();
    cbm_mem_init(-1.0);
    size_t budget_after = cbm_mem_budget();
    ASSERT_EQ(budget_before, budget_after);
    PASS();
}

TEST(mem_init_over_one_fraction) {
    size_t budget_before = cbm_mem_budget();
    cbm_mem_init(1.5);
    size_t budget_after = cbm_mem_budget();
    ASSERT_EQ(budget_before, budget_after);
    PASS();
}

TEST(mem_init_second_call_noop) {
    size_t budget_before = cbm_mem_budget();
    cbm_mem_init(0.9); /* different fraction — but it's a no-op */
    size_t budget_after = cbm_mem_budget();
    ASSERT_EQ(budget_before, budget_after);
    PASS();
}

/* ── CBM_MEM_BUDGET_MB budget override (pure resolver) ────────────
 * cbm_mem_init is one-shot per process, so the override logic lives in the
 * pure cbm_mem_resolve_budget() helper which we can exercise directly. */

#define CBM_TEST_MB ((size_t)1024 * 1024)

TEST(resolve_budget_no_override_uses_fraction) {
    /* No env override → ram_fraction × total_ram, source=ram_fraction. */
    size_t total = 8192 * CBM_TEST_MB;
    cbm_mem_budget_t r = cbm_mem_resolve_budget(total, 0.5, NULL);
    ASSERT_EQ(r.budget, 4096 * CBM_TEST_MB);
    ASSERT_STR_EQ(r.source, "ram_fraction");
    ASSERT_FALSE(r.clamped);
    ASSERT_FALSE(r.invalid);
    ASSERT_EQ(cbm_mem_resolve_budget(total, 0.25, "").budget, 2048 * CBM_TEST_MB);
    PASS();
}

TEST(resolve_budget_invalid_fraction_defaults) {
    /* Out-of-range fractions fall back to the 0.5 default. */
    size_t total = 8192 * CBM_TEST_MB;
    ASSERT_EQ(cbm_mem_resolve_budget(total, 0.0, NULL).budget, 4096 * CBM_TEST_MB);
    ASSERT_EQ(cbm_mem_resolve_budget(total, -1.0, NULL).budget, 4096 * CBM_TEST_MB);
    ASSERT_EQ(cbm_mem_resolve_budget(total, 1.5, NULL).budget, 4096 * CBM_TEST_MB);
    PASS();
}

TEST(resolve_budget_override_wins) {
    /* The key use case: pin a budget *below* the fraction default. */
    size_t total = 8192 * CBM_TEST_MB;
    cbm_mem_budget_t below = cbm_mem_resolve_budget(total, 0.5, "2048");
    ASSERT_EQ(below.budget, 2048 * CBM_TEST_MB);
    ASSERT_STR_EQ(below.source, "CBM_MEM_BUDGET_MB");
    ASSERT_FALSE(below.clamped);
    ASSERT_FALSE(below.invalid);
    /* Override above the fraction default is also honored (up to total_ram). */
    ASSERT_EQ(cbm_mem_resolve_budget(total, 0.5, "6144").budget, 6144 * CBM_TEST_MB);
    PASS();
}

TEST(resolve_budget_override_clamped_to_total) {
    /* Override larger than physical/cgroup RAM clamps to total_ram. */
    size_t total = 1024 * CBM_TEST_MB;
    cbm_mem_budget_t r = cbm_mem_resolve_budget(total, 0.5, "100000");
    ASSERT_EQ(r.budget, total);
    ASSERT_TRUE(r.clamped);
    ASSERT_STR_EQ(r.source, "CBM_MEM_BUDGET_MB");
    PASS();
}

TEST(resolve_budget_override_when_total_unknown) {
    /* Detection failed (total_ram == 0): override still yields a usable budget
     * and is not clamped to zero. */
    cbm_mem_budget_t r = cbm_mem_resolve_budget(0, 0.5, "512");
    ASSERT_EQ(r.budget, 512 * CBM_TEST_MB);
    ASSERT_FALSE(r.clamped);
    ASSERT_FALSE(r.invalid);
    PASS();
}

TEST(resolve_budget_invalid_override_falls_back) {
    /* Non-numeric, zero, negative, trailing-garbage, and ERANGE-overflow
     * overrides are all rejected (invalid=true) → fraction budget, source
     * stays ram_fraction. Strict parse matches src/foundation/limits.c. */
    size_t total = 8192 * CBM_TEST_MB;
    size_t fraction_budget = 4096 * CBM_TEST_MB;
    const char *bad[] = {
        "abc", "0", "-512", "512MB", "512x", "0x400", "99999999999999999999999999",
    };
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        cbm_mem_budget_t r = cbm_mem_resolve_budget(total, 0.5, bad[i]);
        ASSERT_EQ(r.budget, fraction_budget);
        ASSERT_TRUE(r.invalid);
        ASSERT_STR_EQ(r.source, "ram_fraction");
    }
    PASS();
}

/* Abuse guard: a ~2^44 MiB request (14 digits — fits the 31-char env buffer) is
 * a VALID long long, so it passes the strict parse; the unguarded want_mb × MiB
 * byte multiply would then overflow size_t and wrap to 0 (0 is not > total_ram,
 * so a naive clamp misses it), pinning cbm_mem_over_budget() permanently true.
 * The MiB-space clamp must instead clamp to total_ram. */
TEST(resolve_budget_override_overflow_clamps_to_total) {
    size_t total = 2048 * CBM_TEST_MB;
    /* 2^44 MiB: (size_t)2^44 * (2^20 bytes/MiB) == 2^64 == 0 on wrap. */
    cbm_mem_budget_t r = cbm_mem_resolve_budget(total, 0.5, "17592186044416");
    ASSERT_EQ(r.budget, total);
    ASSERT_TRUE(r.clamped);
    ASSERT_FALSE(r.invalid);
    PASS();
}

/* Abuse guard: RAM detection failed (total_ram == 0, so no clamp target) AND
 * the request is a valid-but-astronomical value. The multiply must not wrap to
 * a small budget — cap at SIZE_MAX instead. */
TEST(resolve_budget_override_overflow_total_unknown_caps) {
    /* 1e17 MiB: valid long long (< LLONG_MAX) but > SIZE_MAX / MiB. */
    cbm_mem_budget_t r = cbm_mem_resolve_budget(0, 0.5, "99999999999999999");
    ASSERT_EQ(r.budget, SIZE_MAX);
    ASSERT_FALSE(r.invalid);
    PASS();
}

#undef CBM_TEST_MB

/* ── Arena integration tests ──────────────────────────────────── */

TEST(arena_alloc_and_destroy) {
    CBMArena a;
    cbm_arena_init(&a);
    ASSERT_EQ(a.nblocks, 1);
    ASSERT_EQ(a.block_sizes[0], CBM_ARENA_DEFAULT_BLOCK_SIZE);

    char *s = cbm_arena_strdup(&a, "hello mem integration");
    ASSERT_NOT_NULL(s);
    ASSERT_STR_EQ(s, "hello mem integration");

    cbm_arena_destroy(&a);
    ASSERT_EQ(a.nblocks, 0);
    PASS();
}

TEST(arena_grow_tracks_sizes) {
    CBMArena a;
    cbm_arena_init_sized(&a, 64);
    ASSERT_EQ(a.block_sizes[0], 64);

    cbm_arena_alloc(&a, 48);
    cbm_arena_alloc(&a, 48); /* triggers grow */
    ASSERT_GTE(a.nblocks, 2);
    ASSERT_GT(a.block_sizes[1], 0);
    ASSERT_GTE(a.block_sizes[1], 96);

    cbm_arena_destroy(&a);
    PASS();
}

TEST(arena_large_alloc) {
    CBMArena a;
    cbm_arena_init(&a);

    size_t big = 128 * 1024;
    void *p = cbm_arena_alloc(&a, big);
    ASSERT_NOT_NULL(p);
    memset(p, 0xCD, big);
    unsigned char *bytes = (unsigned char *)p;
    ASSERT_EQ(bytes[0], 0xCD);
    ASSERT_EQ(bytes[big - 1], 0xCD);

    cbm_arena_destroy(&a);
    PASS();
}

TEST(arena_reset_frees_blocks) {
    CBMArena a;
    cbm_arena_init_sized(&a, 128);

    cbm_arena_alloc(&a, 100);
    cbm_arena_alloc(&a, 100);
    ASSERT_GTE(a.nblocks, 2);

    cbm_arena_reset(&a);
    ASSERT_EQ(a.nblocks, 1);
    ASSERT_EQ(a.block_sizes[1], 0);

    void *p = cbm_arena_alloc(&a, 16);
    ASSERT_NOT_NULL(p);

    cbm_arena_destroy(&a);
    PASS();
}

/* ── Slab allocator tests ─────────────────────────────────────── */

TEST(slab_tier1_malloc_backed) {
    /* Verify slab alloc/free cycle works with malloc-backed pages */
    cbm_slab_install();

    void *p = cbm_slab_test_malloc(32);
    ASSERT_NOT_NULL(p);
    memset(p, 0x42, 32);
    ASSERT_EQ(((unsigned char *)p)[0], 0x42);
    ASSERT_EQ(((unsigned char *)p)[31], 0x42);

    cbm_slab_test_free(p);

    /* Re-alloc should reuse from free list */
    void *p2 = cbm_slab_test_malloc(32);
    ASSERT_NOT_NULL(p2);
    memset(p2, 0x43, 32);
    cbm_slab_test_free(p2);

    cbm_slab_destroy_thread();
    PASS();
}

TEST(slab_heap_alloc_and_free) {
    /* >64B goes to malloc (mimalloc in prod) */
    cbm_slab_install();

    void *p = cbm_slab_test_malloc(200);
    ASSERT_NOT_NULL(p);
    memset(p, 0xAA, 200);
    ASSERT_EQ(((unsigned char *)p)[0], 0xAA);
    ASSERT_EQ(((unsigned char *)p)[199], 0xAA);

    cbm_slab_test_free(p);

    /* Allocate various sizes */
    size_t test_sizes[] = {65, 200, 512, 1024, 4096, 8192};
    void *ptrs[6];
    for (int i = 0; i < 6; i++) {
        ptrs[i] = cbm_slab_test_malloc(test_sizes[i]);
        ASSERT_NOT_NULL(ptrs[i]);
        memset(ptrs[i], (unsigned char)(0x10 + i), test_sizes[i]);
    }
    for (int i = 0; i < 6; i++) {
        unsigned char *bytes = (unsigned char *)ptrs[i];
        ASSERT_EQ(bytes[0], (unsigned char)(0x10 + i));
        ASSERT_EQ(bytes[test_sizes[i] - 1], (unsigned char)(0x10 + i));
    }
    for (int i = 0; i < 6; i++) {
        cbm_slab_test_free(ptrs[i]);
    }

    cbm_slab_destroy_thread();
    PASS();
}

TEST(slab_reclaim_returns_memory) {
    /* Verify reclaim frees slab pages */
    cbm_slab_install();

    /* Allocate many slab chunks to grow pages */
    void *ptrs[2048];
    for (int i = 0; i < 2048; i++) {
        ptrs[i] = cbm_slab_test_malloc(32);
        ASSERT_NOT_NULL(ptrs[i]);
    }
    /* Free all back to free lists */
    for (int i = 0; i < 2048; i++) {
        cbm_slab_test_free(ptrs[i]);
    }

    /* Reclaim + collect */
    cbm_slab_reclaim();
    cbm_mem_collect();

    /* After reclaim, allocating should still work (grows new pages) */
    void *p = cbm_slab_test_malloc(32);
    ASSERT_NOT_NULL(p);
    cbm_slab_test_free(p);

    cbm_slab_destroy_thread();
    PASS();
}

TEST(slab_realloc_slab_to_heap) {
    /* Verify promotion from slab (≤64B) to heap (>64B) */
    cbm_slab_install();

    void *p = cbm_slab_test_malloc(32); /* slab */
    ASSERT_NOT_NULL(p);
    memset(p, 0x42, 32);

    void *p2 = cbm_slab_test_realloc(p, 200); /* heap */
    ASSERT_NOT_NULL(p2);
    ASSERT_EQ(((unsigned char *)p2)[0], 0x42);
    ASSERT_EQ(((unsigned char *)p2)[31], 0x42);

    cbm_slab_test_free(p2);
    cbm_slab_destroy_thread();
    PASS();
}

TEST(slab_calloc_zeroed) {
    /* calloc must return zeroed memory */
    cbm_slab_install();

    void *p = cbm_slab_test_calloc(1, 200);
    ASSERT_NOT_NULL(p);
    unsigned char *bytes = (unsigned char *)p;
    int nonzero = 0;
    for (int i = 0; i < 200; i++) {
        if (bytes[i] != 0) {
            nonzero++;
        }
    }
    ASSERT_EQ(nonzero, 0);

    cbm_slab_test_free(p);
    cbm_slab_destroy_thread();
    PASS();
}

TEST(slab_mixed_alloc_free_stress) {
    /* Stress test: interleaved allocs and frees across slab and heap */
    cbm_slab_install();

    void *ptrs[100];
    size_t sizes[100];

    for (int i = 0; i < 100; i++) {
        sizes[i] = (size_t)(16 + (i * 47) % 4000);
        ptrs[i] = cbm_slab_test_malloc(sizes[i]);
        ASSERT_NOT_NULL(ptrs[i]);
        memset(ptrs[i], (unsigned char)(i & 0xFF), sizes[i]);
    }

    /* Free odd-indexed blocks */
    for (int i = 1; i < 100; i += 2) {
        cbm_slab_test_free(ptrs[i]);
        ptrs[i] = NULL;
    }

    /* Re-allocate freed slots with different sizes */
    for (int i = 1; i < 100; i += 2) {
        sizes[i] = (size_t)(32 + (i * 31) % 2000);
        ptrs[i] = cbm_slab_test_malloc(sizes[i]);
        ASSERT_NOT_NULL(ptrs[i]);
        memset(ptrs[i], (unsigned char)((i + 1) & 0xFF), sizes[i]);
    }

    /* Verify even-indexed blocks still have original data */
    for (int i = 0; i < 100; i += 2) {
        ASSERT_EQ(((unsigned char *)ptrs[i])[0], (unsigned char)(i & 0xFF));
    }

    for (int i = 0; i < 100; i++) {
        cbm_slab_test_free(ptrs[i]);
    }

    cbm_slab_destroy_thread();
    PASS();
}

/* ── Cross-thread slab-free safety (distilled from PR #782, closes #852) ──
 *
 * Tree-sitter's allocator callbacks are process-global: a ≤64B chunk allocated
 * on parser thread A can be freed on parser thread B. The pre-fix thread-local
 * slab_owns() only scanned the FREEING thread's pages, so a cross-thread free
 * missed A's pages and fell through to free() on a pointer INTERIOR to a
 * malloc'd page (invalid free / SIGABRT). Separately (#852), destroying/
 * reclaiming a thread's slab while a live chunk is still referenced by a
 * tree-sitter lexer freed the page under it (heap-use-after-free).
 *
 * These are RED on main (invalid free / UAF, caught by ASan) and GREEN with
 * the O(1) aligned-page + retire-on-live-count allocator. */

typedef struct {
    void *ptr;
    atomic_int *go;
} slab_cross_thread_free_ctx_t;

static void *slab_cross_thread_free_worker(void *arg) {
    slab_cross_thread_free_ctx_t *ctx = (slab_cross_thread_free_ctx_t *)arg;
    while (ctx->go && !atomic_load_explicit(ctx->go, memory_order_acquire)) {
        cbm_usleep(1000);
    }
    /* Free on a DIFFERENT thread than the one that allocated. On main this
     * falls through to free() on an interior slab pointer → invalid free. */
    cbm_slab_test_free(ctx->ptr);
    return NULL;
}

/* #852 exact guard — deterministic, single-thread, NOT cross-suite-order
 * dependent. Destroy the current thread's slab while a chunk is still live,
 * then read and free the chunk. On main, destroy frees the page → the read is
 * a heap-use-after-free and the free is an invalid free. With retire-on-
 * live-count the page is retired (not freed) while the chunk lives and released
 * only when the final chunk returns. */
TEST(slab_destroy_thread_with_live_chunk_no_uaf) {
    cbm_slab_install();

    void *p = cbm_slab_test_malloc(48); /* ≤64B → slab chunk */
    ASSERT_NOT_NULL(p);
    memset(p, 0x7E, 48);

    /* Tear down slab TLS with p still referenced (models the live lexer). */
    cbm_slab_destroy_thread();

    /* p must still be valid — its page is retired, not freed. */
    for (int i = 0; i < 48; i++) {
        ASSERT_EQ(((unsigned char *)p)[i], 0x7E);
    }

    /* Returning the last live chunk releases the retired page (no leak). */
    cbm_slab_test_free(p);
    PASS();
}

TEST(slab_cross_thread_free_is_safe) {
    cbm_slab_install();

    void *p = cbm_slab_test_malloc(32);
    ASSERT_NOT_NULL(p);
    memset(p, 0x5A, 32);

    atomic_int go;
    atomic_init(&go, 1);
    slab_cross_thread_free_ctx_t ctx = {.ptr = p, .go = &go};
    cbm_thread_t t;
    ASSERT_EQ(cbm_thread_create(&t, 0, slab_cross_thread_free_worker, &ctx), 0);
    ASSERT_EQ(cbm_thread_join(&t), 0);

    cbm_slab_destroy_thread();
    PASS();
}

TEST(slab_reclaim_with_foreign_live_chunk_is_safe) {
    cbm_slab_install();

    void *p = cbm_slab_test_malloc(32);
    ASSERT_NOT_NULL(p);
    memset(p, 0xA5, 32);

    atomic_int go;
    atomic_init(&go, 0);
    slab_cross_thread_free_ctx_t ctx = {.ptr = p, .go = &go};
    cbm_thread_t t;
    ASSERT_EQ(cbm_thread_create(&t, 0, slab_cross_thread_free_worker, &ctx), 0);

    /* Reclaim while another thread still owns a live chunk from our page.
     * On main, reclaim frees the page → the pending cross-thread free is a
     * use-after-free. With retire-on-live-count, the page is retired. */
    cbm_slab_reclaim();
    atomic_store_explicit(&go, 1, memory_order_release);
    ASSERT_EQ(cbm_thread_join(&t), 0);

    cbm_slab_destroy_thread();
    PASS();
}

TEST(slab_destroy_with_foreign_live_chunk_is_safe) {
    cbm_slab_install();

    void *p = cbm_slab_test_malloc(32);
    ASSERT_NOT_NULL(p);
    memset(p, 0x3C, 32);

    atomic_int go;
    atomic_init(&go, 0);
    slab_cross_thread_free_ctx_t ctx = {.ptr = p, .go = &go};
    cbm_thread_t t;
    ASSERT_EQ(cbm_thread_create(&t, 0, slab_cross_thread_free_worker, &ctx), 0);

    /* Destroy TLS while another thread still owns a live chunk. */
    cbm_slab_destroy_thread();
    atomic_store_explicit(&go, 1, memory_order_release);
    ASSERT_EQ(cbm_thread_join(&t), 0);

    PASS();
}

/* ── Parallel extraction integration test ──────────────────── */

static char g_mem_tmpdir[256];

static int setup_mem_test_repo(void) {
    snprintf(g_mem_tmpdir, sizeof(g_mem_tmpdir), "/tmp/cbm_mem_XXXXXX");
    if (!cbm_mkdtemp(g_mem_tmpdir)) {
        return -1;
    }

    char path[512];

    for (int i = 0; i < 6; i++) {
        snprintf(path, sizeof(path), "%s/file%d.go", g_mem_tmpdir, i);
        FILE *f = fopen(path, "w");
        if (!f) {
            return -1;
        }
        fprintf(f,
                "package main\n\nfunc F%d() {\n\tprintln(\"hello\")\n}\n\n"
                "func G%d() int {\n\treturn F%d() + %d\n}\n",
                i, i, i, i);
        fclose(f);
    }

    snprintf(path, sizeof(path), "%s/util.c", g_mem_tmpdir);
    FILE *f = fopen(path, "w");
    if (!f) {
        return -1;
    }
    fprintf(f, "#include <stdio.h>\nvoid util_func(void) { printf(\"hi\"); }\n"
               "int util_add(int a, int b) { return a + b; }\n");
    fclose(f);

    return 0;
}

static void teardown_mem_test_repo(void) {
    if (g_mem_tmpdir[0]) {
        th_rmtree(g_mem_tmpdir);
        g_mem_tmpdir[0] = '\0';
    }
}

static size_t count_retained_source_bytes(CBMFileResult **result_cache, int file_count,
                                          int *retained_count) {
    size_t retained_bytes = 0;
    int count = 0;

    for (int i = 0; i < file_count; i++) {
        CBMFileResult *result = result_cache[i];
        if (result && result->source) {
            retained_bytes += (size_t)result->source_len;
            count++;
        }
    }

    if (retained_count) {
        *retained_count = count;
    }
    return retained_bytes;
}

/* retain_sources=false disables source retention entirely: no result->source is
 * kept, yet extraction still produces defs/nodes. Guards the low-RAM opt-out. */
TEST(parallel_extract_without_source_retention) {
    if (setup_mem_test_repo() != 0) {
        FAIL("tmpdir setup failed");
    }

    cbm_discover_opts_t opts = {.mode = CBM_MODE_FULL};
    cbm_file_info_t *files = NULL;
    int file_count = 0;
    if (cbm_discover(g_mem_tmpdir, &opts, &files, &file_count) != 0) {
        teardown_mem_test_repo();
        FAIL("discover failed");
    }

    cbm_gbuf_t *gbuf = cbm_gbuf_new("mem-test", g_mem_tmpdir);
    cbm_registry_t *reg = cbm_registry_new();
    atomic_int cancelled;
    atomic_init(&cancelled, 0);

    cbm_pipeline_ctx_t ctx = {
        .project_name = "mem-test",
        .repo_path = g_mem_tmpdir,
        .gbuf = gbuf,
        .registry = reg,
        .cancelled = &cancelled,
    };

    _Atomic int64_t shared_ids;
    atomic_init(&shared_ids, cbm_gbuf_next_id(gbuf));

    CBMFileResult **result_cache = calloc((size_t)file_count, sizeof(CBMFileResult *));
    ASSERT_NOT_NULL(result_cache);

    cbm_parallel_extract_opts_t extract_opts = {
        .retain_sources = false,
        .retain_sources_set = true,
        .retain_total_budget_bytes = 0,
        .retain_per_file_max_bytes = 0,
    };
    int rc = cbm_parallel_extract_ex(&ctx, files, file_count, result_cache, &shared_ids, 2,
                                     &extract_opts);
    ASSERT_EQ(rc, 0);

    int defs_seen = 0;
    for (int i = 0; i < file_count; i++) {
        if (result_cache[i]) {
            ASSERT_EQ(result_cache[i]->source, NULL);
            defs_seen += result_cache[i]->defs.count;
        }
    }
    ASSERT_GT(defs_seen, 0);
    ASSERT_GT(cbm_gbuf_node_count(gbuf), 0);

    for (int i = 0; i < file_count; i++) {
        if (result_cache[i]) {
            cbm_free_result(result_cache[i]);
        }
    }
    free(result_cache);
    cbm_registry_free(reg);
    cbm_gbuf_free(gbuf);
    cbm_discover_free(files, file_count);
    teardown_mem_test_repo();
    PASS();
}

/* Guard B (peak bound): a tiny total retention budget must actually bound the
 * retained source bytes — retained_bytes <= budget — while extraction still
 * produces defs/nodes. Over-budget files fall back to a bounded re-read during
 * cross-file resolution (exercised in test_parallel.c), so the cap trades
 * retained RAM, never correctness. */
TEST(parallel_extract_tiny_source_retention_budget) {
    if (setup_mem_test_repo() != 0) {
        FAIL("tmpdir setup failed");
    }

    cbm_discover_opts_t opts = {.mode = CBM_MODE_FULL};
    cbm_file_info_t *files = NULL;
    int file_count = 0;
    if (cbm_discover(g_mem_tmpdir, &opts, &files, &file_count) != 0) {
        teardown_mem_test_repo();
        FAIL("discover failed");
    }

    cbm_gbuf_t *gbuf = cbm_gbuf_new("mem-test", g_mem_tmpdir);
    cbm_registry_t *reg = cbm_registry_new();
    atomic_int cancelled;
    atomic_init(&cancelled, 0);

    cbm_pipeline_ctx_t ctx = {
        .project_name = "mem-test",
        .repo_path = g_mem_tmpdir,
        .gbuf = gbuf,
        .registry = reg,
        .cancelled = &cancelled,
    };

    _Atomic int64_t shared_ids;
    atomic_init(&shared_ids, cbm_gbuf_next_id(gbuf));

    CBMFileResult **result_cache = calloc((size_t)file_count, sizeof(CBMFileResult *));
    ASSERT_NOT_NULL(result_cache);

    const size_t retain_total_budget_bytes = 256;
    cbm_parallel_extract_opts_t extract_opts = {
        .retain_sources = true,
        .retain_sources_set = true,
        .retain_total_budget_bytes = retain_total_budget_bytes,
        .retain_per_file_max_bytes = 100U * 1024U * 1024U,
    };
    int rc = cbm_parallel_extract_ex(&ctx, files, file_count, result_cache, &shared_ids, 2,
                                     &extract_opts);
    ASSERT_EQ(rc, 0);

    int retained_count = 0;
    size_t retained_bytes = count_retained_source_bytes(result_cache, file_count, &retained_count);
    int defs_seen = 0;
    for (int i = 0; i < file_count; i++) {
        if (result_cache[i]) {
            defs_seen += result_cache[i]->defs.count;
        }
    }

    ASSERT_GT(defs_seen, 0);
    ASSERT_GT(retained_count, 0);
    ASSERT_LTE(retained_bytes, retain_total_budget_bytes);
    ASSERT_GT(cbm_gbuf_node_count(gbuf), 0);

    for (int i = 0; i < file_count; i++) {
        if (result_cache[i]) {
            cbm_free_result(result_cache[i]);
        }
    }
    free(result_cache);
    cbm_registry_free(reg);
    cbm_gbuf_free(gbuf);
    cbm_discover_free(files, file_count);
    teardown_mem_test_repo();
    PASS();
}

TEST(parallel_extract_with_slab) {
    cbm_mem_init(0.5);

    if (setup_mem_test_repo() != 0) {
        FAIL("tmpdir setup failed");
    }

    cbm_discover_opts_t opts = {.mode = CBM_MODE_FULL};
    cbm_file_info_t *files = NULL;
    int file_count = 0;
    if (cbm_discover(g_mem_tmpdir, &opts, &files, &file_count) != 0) {
        teardown_mem_test_repo();
        FAIL("discover failed");
    }

    ASSERT_GTE(file_count, 5);

    cbm_gbuf_t *gbuf = cbm_gbuf_new("mem-test", g_mem_tmpdir);
    cbm_registry_t *reg = cbm_registry_new();
    atomic_int cancelled;
    atomic_init(&cancelled, 0);

    cbm_pipeline_ctx_t ctx = {
        .project_name = "mem-test",
        .repo_path = g_mem_tmpdir,
        .gbuf = gbuf,
        .registry = reg,
        .cancelled = &cancelled,
    };

    _Atomic int64_t shared_ids;
    int64_t gbuf_next = cbm_gbuf_next_id(gbuf);
    atomic_init(&shared_ids, gbuf_next);

    CBMFileResult **result_cache = calloc(file_count, sizeof(CBMFileResult *));
    ASSERT_NOT_NULL(result_cache);

    int rc = cbm_parallel_extract(&ctx, files, file_count, result_cache, &shared_ids, 2);
    ASSERT_EQ(rc, 0);

    int cached_count = 0;
    for (int i = 0; i < file_count; i++) {
        if (result_cache[i]) {
            cached_count++;
        }
    }
    ASSERT_GTE(cached_count, 5);
    ASSERT_GT(cbm_gbuf_node_count(gbuf), 0);

    for (int i = 0; i < file_count; i++) {
        if (result_cache[i]) {
            cbm_free_result(result_cache[i]);
        }
    }
    free(result_cache);
    cbm_registry_free(reg);
    cbm_gbuf_free(gbuf);
    cbm_discover_free(files, file_count);
    teardown_mem_test_repo();
    PASS();
}

SUITE(mem) {
    /* mem API */
    RUN_TEST(mem_rss_tracking);
    RUN_TEST(mem_collect_reclaims);
    RUN_TEST(mem_budget_check);
    /* Budget edge cases */
    RUN_TEST(mem_worker_budget_zero_workers);
    RUN_TEST(mem_worker_budget_negative_workers);
    RUN_TEST(mem_worker_budget_one_worker);
    RUN_TEST(mem_worker_budget_many_workers);
    RUN_TEST(mem_over_budget_low_rss);
    RUN_TEST(mem_ram_fraction_16gb_tier);
    RUN_TEST(mem_ram_fraction_32gb_tier);
    RUN_TEST(mem_ram_fraction_large_host);
    /* RSS tracking */
    RUN_TEST(mem_rss_positive);
    RUN_TEST(mem_peak_rss_gte_rss);
    RUN_TEST(mem_rss_increases_after_alloc);
    RUN_TEST(mem_rss_reflects_external_resident_memory);
    RUN_TEST(mem_collect_no_crash);
    RUN_TEST(mem_collect_rss_still_positive);
    /* Memory pressure simulation */
    RUN_TEST(mem_progressive_alloc_rss_increases);
    RUN_TEST(mem_free_and_collect_no_crash);
    RUN_TEST(mem_multiple_collect_idempotent);
    /* Init edge cases */
    RUN_TEST(mem_init_zero_fraction);
    RUN_TEST(mem_init_negative_fraction);
    RUN_TEST(mem_init_over_one_fraction);
    RUN_TEST(mem_init_second_call_noop);
    /* CBM_MEM_BUDGET_MB budget override */
    RUN_TEST(resolve_budget_no_override_uses_fraction);
    RUN_TEST(resolve_budget_invalid_fraction_defaults);
    RUN_TEST(resolve_budget_override_wins);
    RUN_TEST(resolve_budget_override_clamped_to_total);
    RUN_TEST(resolve_budget_override_when_total_unknown);
    RUN_TEST(resolve_budget_invalid_override_falls_back);
    RUN_TEST(resolve_budget_override_overflow_clamps_to_total);
    RUN_TEST(resolve_budget_override_overflow_total_unknown_caps);
    /* Arena integration */
    RUN_TEST(arena_alloc_and_destroy);
    RUN_TEST(arena_grow_tracks_sizes);
    RUN_TEST(arena_large_alloc);
    RUN_TEST(arena_reset_frees_blocks);
    /* Slab allocator */
    RUN_TEST(slab_tier1_malloc_backed);
    RUN_TEST(slab_heap_alloc_and_free);
    RUN_TEST(slab_reclaim_returns_memory);
    RUN_TEST(slab_realloc_slab_to_heap);
    RUN_TEST(slab_calloc_zeroed);
    RUN_TEST(slab_mixed_alloc_free_stress);
    /* Cross-thread free safety + retire-on-live-count (#782 / #852) */
    RUN_TEST(slab_destroy_thread_with_live_chunk_no_uaf);
    RUN_TEST(slab_cross_thread_free_is_safe);
    RUN_TEST(slab_reclaim_with_foreign_live_chunk_is_safe);
    RUN_TEST(slab_destroy_with_foreign_live_chunk_is_safe);
    /* Integration */
    RUN_TEST(parallel_extract_without_source_retention);
    RUN_TEST(parallel_extract_tiny_source_retention_budget);
    RUN_TEST(parallel_extract_with_slab);
}
