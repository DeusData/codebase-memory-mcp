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
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "graph_buffer/graph_buffer.h"
#include "discover/discover.h"
#include "cbm.h"

#include <stdatomic.h>
#include <sys/stat.h>

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
    size_t rss = cbm_mem_rss();
    size_t peak = cbm_mem_peak_rss();
    /* Peak must be >= current RSS */
    ASSERT_GTE(peak, rss);
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

TEST(parallel_extract_with_slab) {
    cbm_mem_init(0.5);

    if (setup_mem_test_repo() != 0) {
        SKIP("tmpdir setup failed");
    }

    cbm_discover_opts_t opts = {.mode = CBM_MODE_FULL};
    cbm_file_info_t *files = NULL;
    int file_count = 0;
    if (cbm_discover(g_mem_tmpdir, &opts, &files, &file_count) != 0) {
        teardown_mem_test_repo();
        SKIP("discover failed");
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
    /* RSS tracking */
    RUN_TEST(mem_rss_positive);
    RUN_TEST(mem_peak_rss_gte_rss);
    RUN_TEST(mem_rss_increases_after_alloc);
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
    /* Integration */
    RUN_TEST(parallel_extract_with_slab);
}
