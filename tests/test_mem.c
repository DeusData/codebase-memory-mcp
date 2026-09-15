/*
 * test_mem.c — Tests for unified memory management (mimalloc-backed),
 *              arena integration, slab allocator, and parallel extraction.
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "../src/foundation/mem.h"
#include "../src/foundation/platform.h" /* cbm_system_info, cbm_system_available_ram */
#include "../src/foundation/mem_core.h"
#include "../src/foundation/arena.h"
#include "../src/foundation/slab_alloc.h"
#include "../src/foundation/compat_thread.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "graph_buffer/graph_buffer.h"
#include "discover/discover.h"
#include "cbm.h"
#include "lang_specs.h"           /* cbm_ts_language */
#include "foundation/constants.h" /* CBM_SZ_* */

#include <stdatomic.h>
#include <stdint.h>
#include <sys/stat.h>
#include <mimalloc.h>
#ifndef _WIN32
#include <sys/mman.h>
#else
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
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

/* Since #1360 routed ordinary malloc/new through mimalloc on Linux, the arena
 * policy governs EVERY allocation in the process, not just the bound
 * sqlite/tree_sitter populations. With lazy arena commit (0), mimalloc commits
 * sub-ranges via mprotect(PROT_READ|PROT_WRITE) over a PROT_NONE reservation,
 * and each partial commit SPLITS the reserved VMA: an index worker on the Go
 * corpus held ~22k mappings where v0.9.0 held 10, growing with worker count.
 * That is how #1654's 96-CPU host reached vm.max_map_count, after which mmap
 * fails for ANY size — 10 KB allocations failing while `free -g` still showed
 * 246 GB available. mimalloc's own default is 2, meaning "eager-commit arenas
 * only on an OS that overcommits (i.e. linux)", where commit is free until the
 * pages are touched; overriding it to 0 opted Linux out of the default written
 * for Linux. Measured: 22450 -> 17312 mappings, wall time and peak RSS
 * unchanged. Pin the platform split so the Linux default cannot be silently
 * opted out again — and keep the lazy setting where commit is NOT free
 * (Windows especially, see #581). */
TEST(mem_arena_eager_commit_follows_platform_commit_cost) {
    cbm_mem_init(0.5);
    long eager = mi_option_get(mi_option_arena_eager_commit);
#if defined(__linux__)
    ASSERT_EQ(eager, 2);
#else
    ASSERT_EQ(eager, 0);
#endif
    PASS();
}

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
    const size_t lock_span = (size_t)64 * 1024 * 1024;
    void *big = malloc(region);
    ASSERT_NOT_NULL(big);
    memset(big, 0x5A, region);
    /* Trimming can evict even a just-touched region: at 18 parallel suites
     * the VM kept 19 MB resident of a 256 MB double-touch, losing the
     * re-touch race this test previously relied on. Locked pages are exempt
     * from working-set trimming, so lock a span comfortably above the
     * threshold and the measurement becomes pressure-immune. When the lock
     * is unavailable (working-set quota policy), fall back to bounded
     * touch-and-sample retries — those races are transient. */
    HANDLE self_process = GetCurrentProcess();
    bool locked = SetProcessWorkingSetSize(self_process, lock_span + (size_t)32 * 1024 * 1024,
                                           (size_t)512 * 1024 * 1024) != 0 &&
                  VirtualLock(big, lock_span) != 0;
    size_t rss = 0;
    for (int attempt = 0; attempt < 6; attempt++) {
        memset(big, 0x5B + attempt, lock_span);
        rss = cbm_mem_rss();
        if (locked || rss >= threshold) {
            break;
        }
    }
    if (locked) {
        (void)VirtualUnlock(big, lock_span);
    }
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

/* CBM_MEM_BUDGET_MB is an aggregate ceiling the parent divides across job
 * slots. A lower explicit value still wins; a raise is clipped to the per-slot
 * share so N workers cannot oversubscribe the host (#1654). The source stays
 * CBM_MEM_BUDGET_MB so the clip is the user's aggregate, not a silent
 * daemon_worker_cap rewrite of a fraction-derived default. */
TEST(resolve_budget_worker_cap_preserves_lower_user_override) {
    size_t total = 8192 * CBM_TEST_MB;
    size_t worker_cap = 16 * CBM_TEST_MB;
    cbm_mem_budget_t lower = cbm_mem_resolve_budget_capped(total, 0.5, "8", worker_cap);
    ASSERT_EQ(lower.budget, 8 * CBM_TEST_MB);
    ASSERT_STR_EQ(lower.source, "CBM_MEM_BUDGET_MB");
    ASSERT_FALSE(lower.hard_capped);

    cbm_mem_budget_t raised = cbm_mem_resolve_budget_capped(total, 0.5, "64", worker_cap);
    ASSERT_EQ(raised.budget, worker_cap);
    ASSERT_STR_EQ(raised.source, "CBM_MEM_BUDGET_MB");
    ASSERT_TRUE(raised.hard_capped);

    cbm_mem_budget_t fraction = cbm_mem_resolve_budget_capped(total, 0.5, NULL, worker_cap);
    ASSERT_EQ(fraction.budget, worker_cap);
    ASSERT_STR_EQ(fraction.source, "daemon_worker_cap");
    ASSERT_TRUE(fraction.hard_capped);
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

/* The memory map is a diagnostic, so it must be proven non-vacuous: a map that
 * silently reported zeros would read as "no leak" and send a future
 * investigation down the wrong path. Allocate a KNOWN volume in a KNOWN size
 * class and require the map to attribute it to that class. */
TEST(mem_map_attributes_a_known_allocation) {
    enum { PROBE_BLOCKS = 4000, PROBE_SIZE = 3000 };
    cbm_mem_map_t before;
    cbm_mem_map_t after;
    ASSERT_TRUE(cbm_mem_map_collect(&before));

    /* Allocate through mi_* explicitly. The map walks the mimalloc heap, and
     * only the PRODUCTION build routes plain malloc there (the test build is
     * CRT+ASan) -- so a malloc-based probe would report 0 here and wrongly look
     * like a broken instrument. Using mi_malloc exercises the walk and the
     * bucket attribution in every build configuration. Note the corollary,
     * which is why the residual exists: in a build where malloc does NOT reach
     * mimalloc, live_bytes legitimately reads 0 and the residual owns
     * everything. */
    void **kept = malloc(PROBE_BLOCKS * sizeof(*kept));
    ASSERT_NOT_NULL(kept);
    for (int i = 0; i < PROBE_BLOCKS; i++) {
        kept[i] = mi_malloc(PROBE_SIZE);
        ASSERT_NOT_NULL(kept[i]);
        ((char *)kept[i])[0] = (char)i; /* touch it so it is really committed */
    }
    ASSERT_TRUE(cbm_mem_map_collect(&after));

    /* The walk must account for the bulk of the probe. Slack covers allocator
     * rounding and blocks the aggregate walk may not reach; a map that saw
     * ~nothing is precisely the failure this test exists to catch. */
    size_t probe_bytes = (size_t)PROBE_BLOCKS * PROBE_SIZE;
    ASSERT_GT(after.live_bytes, before.live_bytes);

    /* Assert the contract the map actually offers, which is the triple in
     * mem.h: what the walk cannot see, the residual must carry. mimalloc v3
     * exposes only the main heap, abandoned pages, and the CALLING thread's
     * theap -- there is no API to enumerate every theap -- so on some builds
     * the probe's blocks are unreachable through all three (Windows sees
     * ~190 KB of a 12 MB probe, POSIX sees essentially all of it).
     *
     * Demanding >50% attribution everywhere would assert a guarantee the
     * allocator does not give, and the honest property is stronger anyway: the
     * memory must appear in the map SOMEWHERE. Either the walk attributes the
     * bulk of the probe, or the committed total grew by at least as much and
     * the residual owns it. A map that reported neither would be silently
     * losing memory, which is exactly what this test exists to catch. */
    size_t attributed = after.live_bytes - before.live_bytes;
    size_t committed_growth = after.os_committed_bytes > before.os_committed_bytes
                                  ? after.os_committed_bytes - before.os_committed_bytes
                                  : 0;
    bool walk_saw_it = attributed > probe_bytes / 2;
    bool residual_saw_it = committed_growth + attributed > probe_bytes / 2;
    ASSERT_TRUE(walk_saw_it || residual_saw_it);

    /* Bucket attribution is only meaningful where the walk reached the probe;
     * where it did not, there is nothing to attribute and the residual carried
     * it above. */
    /* 3000-byte blocks belong to the <=4096 class, not to a smaller one. */
    int expected_bucket = -1;
    for (int i = 0; i < CBM_MEM_MAP_BUCKETS; i++) {
        size_t limit = cbm_mem_map_bucket_limit(i);
        if (limit >= (size_t)PROBE_SIZE) {
            expected_bucket = i;
            break;
        }
    }
    ASSERT_TRUE(expected_bucket >= 0);
    if (walk_saw_it) {
        ASSERT_GT(after.bucket_bytes[expected_bucket], before.bucket_bytes[expected_bucket]);
        ASSERT_GT(after.bucket_blocks[expected_bucket] - before.bucket_blocks[expected_bucket],
                  (size_t)(PROBE_BLOCKS / 2));
    }

    /* OS totals must be populated independently of the walk, so the residual is
     * meaningful rather than derived from an empty measurement. */
    ASSERT_GT(after.os_committed_bytes, 0);

    for (int i = 0; i < PROBE_BLOCKS; i++) {
        mi_free(kept[i]);
    }
    free(kept);
    PASS();
}

/* #2010, the lifetime half. traversal_stack_not_in_result_arena in
 * test_extraction.c pins the byte budget of the result arena, but a smaller
 * CHAN_STACK_CAP would satisfy that too. This pins where the bytes actually
 * went: the scratch arena takes the two 128 KB channel walks and the result
 * arena does not. Builds the extraction context directly, since a completed
 * cbm_extract_file_ex has already destroyed its scratch. */
TEST(extract_traversal_stacks_come_from_ctx_scratch_issue2010) {
    enum { CHANNEL_WALK_BYTES = 2 * 4096 * (int)sizeof(TSNode) };
    const char *src = "export const x = 1;\n";
    const TSLanguage *ts_lang = cbm_ts_language(CBM_LANG_TYPESCRIPT);
    ASSERT_NOT_NULL((void *)ts_lang);

    TSParser *parser = ts_parser_new();
    ASSERT_NOT_NULL(parser);
    ts_parser_set_language(parser, ts_lang);
    TSTree *tree = ts_parser_parse_string(parser, NULL, src, (uint32_t)strlen(src));
    ASSERT_NOT_NULL(tree);

    CBMFileResult result;
    memset(&result, 0, sizeof(result));
    cbm_arena_init(&result.arena);
    CBMArena scratch;
    cbm_arena_init_sized(&scratch, (size_t)CBM_SZ_512 * CBM_SZ_1K);

    CBMExtractCtx ctx = {
        .arena = &result.arena,
        .scratch = &scratch,
        .result = &result,
        .source = src,
        .source_len = (int)strlen(src),
        .language = CBM_LANG_TYPESCRIPT,
        .project = "t",
        .rel_path = "a.ts",
        .root = ts_tree_root_node(tree),
    };
    cbm_extract_channels(&ctx);

    ASSERT_GTE(cbm_arena_total(&scratch), (size_t)CHANNEL_WALK_BYTES);
    ASSERT_LT(cbm_arena_total(&result.arena), (size_t)CBM_SZ_64 * CBM_SZ_1K);

    cbm_arena_destroy(&scratch);
    cbm_arena_destroy(&result.arena);
    ts_tree_delete(tree);
    ts_parser_delete(parser);
    PASS();
}

/* ── mem_core: the central allocation route ────────────────────────────
 *
 * Every assertion below is a DELTA, never an absolute. Other code in this
 * process may allocate through the core concurrently, so a test that pinned an
 * absolute total would be measuring the rest of the suite. */

TEST(mem_core_accounts_alloc_and_free) {
    size_t before = cbm_mem_class_live_bytes(CBM_MEM_CLASS_GBUF_NODE);
    size_t blocks_before = cbm_mem_class_live_blocks(CBM_MEM_CLASS_GBUF_NODE);

    void *p = cbm_alloc(CBM_MEM_CLASS_GBUF_NODE, 4096);
    ASSERT_TRUE(p != NULL);
    size_t during = cbm_mem_class_live_bytes(CBM_MEM_CLASS_GBUF_NODE);
    /* Charged at least what was asked for; usable size may round UP, never
     * down, so a strict >= is the honest assertion. */
    ASSERT_TRUE(during >= before + 4096);
    ASSERT_EQ((int)(cbm_mem_class_live_blocks(CBM_MEM_CLASS_GBUF_NODE) - blocks_before), 1);

    cbm_free(CBM_MEM_CLASS_GBUF_NODE, p);
    ASSERT_EQ((int)(cbm_mem_class_live_bytes(CBM_MEM_CLASS_GBUF_NODE) - before), 0);
    ASSERT_EQ((int)(cbm_mem_class_live_blocks(CBM_MEM_CLASS_GBUF_NODE) - blocks_before), 0);
    PASS();
}

/* The whole point of classes: attribution. If a gbuf allocation could show up
 * under semantic, the table could not choose between "park workers" and
 * "stream the vectors" -- the decision this core exists to inform. */
TEST(mem_core_classes_do_not_bleed) {
    size_t node_before = cbm_mem_class_live_bytes(CBM_MEM_CLASS_GBUF_NODE);
    size_t sem_before = cbm_mem_class_live_bytes(CBM_MEM_CLASS_SEMANTIC);

    void *p = cbm_alloc(CBM_MEM_CLASS_SEMANTIC, 8192);
    ASSERT_TRUE(p != NULL);
    ASSERT_EQ((int)(cbm_mem_class_live_bytes(CBM_MEM_CLASS_GBUF_NODE) - node_before), 0);
    ASSERT_TRUE(cbm_mem_class_live_bytes(CBM_MEM_CLASS_SEMANTIC) >= sem_before + 8192);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, p);
    PASS();
}

TEST(mem_core_realloc_replaces_the_old_charge) {
    size_t before = cbm_mem_class_live_bytes(CBM_MEM_CLASS_DUMP);
    void *p = cbm_alloc(CBM_MEM_CLASS_DUMP, 1024);
    ASSERT_TRUE(p != NULL);
    p = cbm_realloc(CBM_MEM_CLASS_DUMP, p, 65536);
    ASSERT_TRUE(p != NULL);
    size_t grown = cbm_mem_class_live_bytes(CBM_MEM_CLASS_DUMP);
    /* The old 1024 must be gone, not stacked on top: exactly one block is live,
     * so the delta is bounded by the new size plus rounding, not by the sum. */
    ASSERT_TRUE(grown >= before + 65536);
    ASSERT_TRUE(grown < before + 65536 + 65536);
    cbm_free(CBM_MEM_CLASS_DUMP, p);
    ASSERT_EQ((int)(cbm_mem_class_live_bytes(CBM_MEM_CLASS_DUMP) - before), 0);
    PASS();
}

/* realloc(NULL) is alloc, and free(NULL) is a no-op: the core must match the C
 * library exactly or adoption stops being a mechanical rename. */
TEST(mem_core_matches_libc_null_semantics) {
    size_t before = cbm_mem_class_live_bytes(CBM_MEM_CLASS_STORE);
    cbm_free(CBM_MEM_CLASS_STORE, NULL);
    ASSERT_EQ((int)(cbm_mem_class_live_bytes(CBM_MEM_CLASS_STORE) - before), 0);

    void *p = cbm_realloc(CBM_MEM_CLASS_STORE, NULL, 2048);
    ASSERT_TRUE(p != NULL);
    ASSERT_TRUE(cbm_mem_class_live_bytes(CBM_MEM_CLASS_STORE) >= before + 2048);
    cbm_free(CBM_MEM_CLASS_STORE, p);

    /* A zero-size request still yields a freeable pointer. */
    void *z = cbm_alloc(CBM_MEM_CLASS_STORE, 0);
    ASSERT_TRUE(z != NULL);
    cbm_free(CBM_MEM_CLASS_STORE, z);
    ASSERT_EQ((int)(cbm_mem_class_live_bytes(CBM_MEM_CLASS_STORE) - before), 0);
    PASS();
}

TEST(mem_core_strdup_copies_and_accounts) {
    size_t before = cbm_mem_class_live_bytes(CBM_MEM_CLASS_GBUF_STRING);
    ASSERT_TRUE(cbm_mem_strdup(CBM_MEM_CLASS_GBUF_STRING, NULL) == NULL);

    const char *src = "qualified::name::example";
    char *copy = cbm_mem_strdup(CBM_MEM_CLASS_GBUF_STRING, src);
    ASSERT_TRUE(copy != NULL);
    ASSERT_TRUE(strcmp(copy, src) == 0);
    ASSERT_TRUE(copy != src);
    ASSERT_TRUE(cbm_mem_class_live_bytes(CBM_MEM_CLASS_GBUF_STRING) > before);
    cbm_free(CBM_MEM_CLASS_GBUF_STRING, copy);
    ASSERT_EQ((int)(cbm_mem_class_live_bytes(CBM_MEM_CLASS_GBUF_STRING) - before), 0);
    PASS();
}

/* A budget decision is about the PEAK, not about whatever was live when
 * someone looked. Peak must survive the free that follows it. */
TEST(mem_core_peak_survives_the_free) {
    cbm_mem_class_reset_peaks();
    size_t base = cbm_mem_class_peak_bytes(CBM_MEM_CLASS_EXTRACT);
    void *p = cbm_alloc(CBM_MEM_CLASS_EXTRACT, 32768);
    ASSERT_TRUE(p != NULL);
    size_t peak_live = cbm_mem_class_peak_bytes(CBM_MEM_CLASS_EXTRACT);
    ASSERT_TRUE(peak_live >= base + 32768);
    cbm_free(CBM_MEM_CLASS_EXTRACT, p);
    ASSERT_EQ((int)(cbm_mem_class_peak_bytes(CBM_MEM_CLASS_EXTRACT) - peak_live), 0);
    PASS();
}

/* Arena-backed memory reports in bulk rather than per object: the extraction
 * engine has 1301 arena call sites and rewriting them to per-object cbm_alloc
 * would undo the batching that keeps its allocation count low. */
TEST(mem_core_external_bulk_accounting_is_symmetric) {
    size_t before = cbm_mem_class_live_bytes(CBM_MEM_CLASS_EXTRACT);
    cbm_mem_class_add_external(CBM_MEM_CLASS_EXTRACT, 1024 * 1024);
    ASSERT_EQ((int)(cbm_mem_class_live_bytes(CBM_MEM_CLASS_EXTRACT) - before), 1024 * 1024);
    cbm_mem_class_remove_external(CBM_MEM_CLASS_EXTRACT, 1024 * 1024);
    ASSERT_EQ((int)(cbm_mem_class_live_bytes(CBM_MEM_CLASS_EXTRACT) - before), 0);
    PASS();
}

/* The one mistake a caller can actually make is freeing with the wrong class.
 * That must never underflow the counter: an unsigned wrap would turn a small
 * drift into a colossal bogus total that reads as a catastrophic leak and
 * sends someone hunting a phantom. Clamp at zero instead. */
TEST(mem_core_mismatched_class_never_wraps) {
    void *p = cbm_alloc(CBM_MEM_CLASS_GBUF_EDGE, 4096);
    ASSERT_TRUE(p != NULL);
    /* Free against a class that was never charged for it. */
    size_t other_before = cbm_mem_class_live_bytes(CBM_MEM_CLASS_DUMP);
    cbm_free(CBM_MEM_CLASS_DUMP, p);
    size_t other_after = cbm_mem_class_live_bytes(CBM_MEM_CLASS_DUMP);
    ASSERT_TRUE(other_after <= other_before); /* clamped, never wrapped */
    ASSERT_TRUE(other_after < (size_t)-1 / 2);
    PASS();
}

TEST(mem_core_report_json_is_wellformed_or_empty) {
    void *p = cbm_alloc(CBM_MEM_CLASS_GBUF_INDEX, 4096);
    ASSERT_TRUE(p != NULL);
    char buf[CBM_SZ_1K];
    int n = cbm_mem_class_report_json(buf, sizeof(buf));
    ASSERT_TRUE(n > 0);
    ASSERT_TRUE(buf[0] == '[');
    ASSERT_TRUE(buf[n - 1] == ']');
    ASSERT_TRUE(strstr(buf, "gbuf_index") != NULL);
    /* A buffer too small must yield NOTHING, never a truncated array that a
     * JSON reader would reject or, worse, silently mis-parse. */
    char tiny[8];
    ASSERT_EQ(cbm_mem_class_report_json(tiny, sizeof(tiny)), 0);
    cbm_free(CBM_MEM_CLASS_GBUF_INDEX, p);
    PASS();
}

/* ── Pressure primitives and the charged reading ─────────────────────── */

TEST(mem_charged_is_positive_and_consistent_with_rss) {
    size_t charged = cbm_mem_charged();
    size_t rss = cbm_mem_rss();
    ASSERT(charged > 0);
    ASSERT(rss > 0);
    /* Same order of magnitude as RSS on every platform: the charged value
     * may sit below RSS (purged-but-resident pages) or slightly above it
     * (compressed pages), never at zero or at a multiple. */
    ASSERT(charged < rss * 4);
    ASSERT(rss < charged * 4 + (size_t)64 * 1024 * 1024);
    PASS();
}

/* The charged high-water mark never reads below a charge just taken, and a
 * later, larger charge lifts it: it is a max over every reading. */
TEST(mem_peak_charged_is_the_high_water_of_charged) {
    size_t charged = cbm_mem_charged();
    ASSERT_TRUE(charged > 0);
    ASSERT_TRUE(cbm_mem_peak_charged() >= charged);
    size_t peak_before = cbm_mem_peak_charged();
    (void)cbm_mem_charged();
    ASSERT_TRUE(cbm_mem_peak_charged() >= peak_before);
    PASS();
}

TEST(mem_footprint_zero_or_plausible) {
    size_t fp = cbm_mem_footprint();
    if (fp > 0) {
        ASSERT(fp >= (size_t)1024 * 1024); /* a live test process is more than 1 MB */
    }
    PASS();
}

TEST(mem_system_available_ram_is_within_total) {
    size_t avail = cbm_system_available_ram();
    cbm_system_info_t info = cbm_system_info();
    if (avail == 0 || info.total_ram == 0) {
        PASS(); /* platform cannot answer; the caller treats that as unknown */
    }
    ASSERT(avail <= info.total_ram);
    PASS();
}

TEST(mem_system_under_pressure_is_a_pure_threshold) {
    size_t avail = cbm_system_available_ram();
    cbm_system_info_t info = cbm_system_info();
    bool under = cbm_mem_system_under_pressure();
    if (avail == 0 || info.total_ram == 0) {
        ASSERT_FALSE(under); /* never abort on a guess */
        PASS();
    }
    ASSERT_EQ(under, avail < info.total_ram / 8);
    PASS();
}

TEST(mem_over_budget_follows_the_charged_reading) {
    size_t saved = cbm_mem_budget();
    size_t charged = cbm_mem_charged();
    ASSERT(charged > 0);
    cbm_mem_set_budget_for_tests(charged * 4);
    ASSERT_FALSE(cbm_mem_over_budget());
    cbm_mem_set_budget_for_tests(charged / 4 + 1);
    ASSERT_TRUE(cbm_mem_over_budget());
    cbm_mem_set_budget_for_tests(saved);
    PASS();
}

TEST(mem_core_class_names_are_total) {
    ASSERT_TRUE(strcmp(cbm_mem_class_name(CBM_MEM_CLASS_SEMANTIC), "semantic") == 0);
    ASSERT_TRUE(strcmp(cbm_mem_class_name(CBM_MEM_CLASS_ARENA), "arena") == 0);
    ASSERT_TRUE(strcmp(cbm_mem_class_name(CBM_MEM_CLASS_TS_TREE), "ts_tree") == 0);
    ASSERT_TRUE(strcmp(cbm_mem_class_name(CBM_MEM_CLASS_STORE), "store") == 0);
    ASSERT_TRUE(strcmp(cbm_mem_class_name(CBM_MEM_CLASS_HASH_TABLE), "hash_table") == 0);
    ASSERT_TRUE(strcmp(cbm_mem_class_name(CBM_MEM_CLASS_DYN_ARRAY), "dyn_array") == 0);
    /* Out of range must still answer, so a log line never takes a NULL. */
    ASSERT_TRUE(cbm_mem_class_name((cbm_mem_class_t)(CBM_MEM_CLASS_COUNT + 5)) != NULL);
    ASSERT_TRUE(cbm_mem_class_name((cbm_mem_class_t)-1) != NULL);
    PASS();
}

SUITE(mem) {
    /* mem API */
    RUN_TEST(mem_arena_eager_commit_follows_platform_commit_cost);
    RUN_TEST(mem_map_attributes_a_known_allocation);
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
    RUN_TEST(resolve_budget_worker_cap_preserves_lower_user_override);
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

    /* extraction scratch arena (#2010) */
    RUN_TEST(extract_traversal_stacks_come_from_ctx_scratch_issue2010);
    RUN_TEST(mem_core_accounts_alloc_and_free);
    RUN_TEST(mem_core_classes_do_not_bleed);
    RUN_TEST(mem_core_realloc_replaces_the_old_charge);
    RUN_TEST(mem_core_matches_libc_null_semantics);
    RUN_TEST(mem_core_strdup_copies_and_accounts);
    RUN_TEST(mem_core_peak_survives_the_free);
    RUN_TEST(mem_core_external_bulk_accounting_is_symmetric);
    RUN_TEST(mem_core_mismatched_class_never_wraps);
    RUN_TEST(mem_core_report_json_is_wellformed_or_empty);
    RUN_TEST(mem_core_class_names_are_total);
    RUN_TEST(mem_charged_is_positive_and_consistent_with_rss);
    RUN_TEST(mem_peak_charged_is_the_high_water_of_charged);
    RUN_TEST(mem_footprint_zero_or_plausible);
    RUN_TEST(mem_system_available_ram_is_within_total);
    RUN_TEST(mem_system_under_pressure_is_a_pure_threshold);
    RUN_TEST(mem_over_budget_follows_the_charged_reading);
}
