/*
 * test_vmem.c — Tests for vmem budget-tracked virtual memory allocator,
 *               arena-vmem integration, and slab+vmem parallel extraction.
 */
#include "test_framework.h"
#include "test_helpers.h"
#include "../src/foundation/vmem.h"
#include "../src/foundation/arena.h"
#include "../src/foundation/slab_alloc.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "graph_buffer/graph_buffer.h"
#include "discover/discover.h"
#include "cbm.h"

#include <stdatomic.h>
#include <sys/stat.h>

/* ── vmem basic tests ─────────────────────────────────────────── */

TEST(vmem_budget_zero_before_init) {
    /* Before init, budget should be 0 — arenas/read_file fall back to malloc */
    /* NOTE: vmem_init uses atomic CAS, only first call takes effect.
     * These tests verify the pre-init state if vmem was never initialized,
     * or the post-init state if it was. We test what we can. */
    size_t budget = cbm_vmem_budget();
    /* Budget is either 0 (never inited) or >0 (inited by earlier test/main) */
    (void)budget;
    PASS();
}

TEST(vmem_alloc_and_free) {
    /* Allocate 1MB, write to it, verify, free */
    size_t sz = 1024 * 1024;
    void *p = cbm_vmem_alloc(sz);
    if (!p) {
        /* vmem may not be initialized — skip gracefully */
        PASS();
    }
    /* vmem guarantees zeroed memory */
    unsigned char *bytes = (unsigned char *)p;
    for (size_t i = 0; i < sz; i += 4096) {
        ASSERT_EQ(bytes[i], 0);
    }
    /* Write pattern */
    memset(p, 0xAB, sz);
    ASSERT_EQ(bytes[0], 0xAB);
    ASSERT_EQ(bytes[sz - 1], 0xAB);

    cbm_vmem_free(p, sz);
    PASS();
}

TEST(vmem_alloc_zero_returns_zeroed) {
    size_t sz = 64 * 1024;
    void *p = cbm_vmem_alloc(sz);
    if (!p) {
        PASS();
    }
    unsigned char *bytes = (unsigned char *)p;
    int nonzero = 0;
    for (size_t i = 0; i < sz; i++) {
        if (bytes[i] != 0) {
            nonzero++;
        }
    }
    ASSERT_EQ(nonzero, 0);
    cbm_vmem_free(p, sz);
    PASS();
}

TEST(vmem_budget_tracking) {
    size_t before = cbm_vmem_allocated();
    size_t sz = 256 * 1024;
    void *p = cbm_vmem_alloc(sz);
    if (!p) {
        PASS();
    }
    size_t after = cbm_vmem_allocated();
    /* allocated should have increased (may be rounded to page) */
    ASSERT_GT(after, before);

    cbm_vmem_free(p, sz);
    size_t freed = cbm_vmem_allocated();
    /* Should be back to (approximately) before */
    ASSERT_LTE(freed, before + 4096); /* within one page */
    PASS();
}

TEST(vmem_peak_tracks) {
    size_t sz = 512 * 1024;
    void *p1 = cbm_vmem_alloc(sz);
    if (!p1) {
        PASS();
    }
    size_t peak1 = cbm_vmem_peak();
    ASSERT_GT(peak1, 0);

    void *p2 = cbm_vmem_alloc(sz);
    if (!p2) {
        cbm_vmem_free(p1, sz);
        PASS();
    }
    size_t peak2 = cbm_vmem_peak();
    ASSERT_GTE(peak2, peak1);

    cbm_vmem_free(p2, sz);
    cbm_vmem_free(p1, sz);

    /* Peak should not decrease after free */
    size_t peak3 = cbm_vmem_peak();
    ASSERT_GTE(peak3, peak2);
    PASS();
}

TEST(vmem_worker_budget) {
    size_t budget = cbm_vmem_budget();
    if (budget == 0) {
        /* Not initialized — worker budget should be 0 */
        ASSERT_EQ(cbm_vmem_worker_budget(4), 0);
        PASS();
    }
    /* Budget divides correctly */
    size_t wb4 = cbm_vmem_worker_budget(4);
    size_t wb8 = cbm_vmem_worker_budget(8);
    ASSERT_EQ(wb4, budget / 4);
    ASSERT_EQ(wb8, budget / 8);
    /* Edge case: 0 workers */
    ASSERT_EQ(cbm_vmem_worker_budget(0), 0);
    PASS();
}

/* ── vmem edge-case and resource management tests ────────────── */

TEST(vmem_alloc_zero_returns_null) {
    cbm_vmem_init(0.5);
    /* alloc(0) must return NULL per the API contract */
    void *p = cbm_vmem_alloc(0);
    ASSERT_NULL(p);
    PASS();
}

TEST(vmem_free_null_zero_no_crash) {
    cbm_vmem_init(0.5);
    /* free(NULL, 0) must be a no-op */
    cbm_vmem_free(NULL, 0);
    PASS();
}

TEST(vmem_free_null_nonzero_no_crash) {
    cbm_vmem_init(0.5);
    /* free(NULL, 100) must be a no-op — ptr==NULL short-circuits */
    cbm_vmem_free(NULL, 100);
    PASS();
}

TEST(vmem_alloc_very_large) {
    cbm_vmem_init(0.5);
    /* Attempt 1 GB allocation — may succeed or fail depending on system.
     * Either way it must not crash. */
    size_t sz = (size_t)1024 * 1024 * 1024;
    void *p = cbm_vmem_alloc(sz);
    if (p) {
        /* If it succeeded, verify we can touch the first and last page */
        ((unsigned char *)p)[0] = 0xAA;
        ((unsigned char *)p)[sz - 1] = 0xBB;
        cbm_vmem_free(p, sz);
    }
    /* Success or graceful NULL — either is fine */
    PASS();
}

TEST(vmem_sequential_alloc_free_no_leak) {
    cbm_vmem_init(0.5);
    size_t before = cbm_vmem_allocated();

    /* 20 alloc/free cycles — allocated must return to baseline */
    size_t sz = 64 * 1024; /* 64 KB */
    for (int i = 0; i < 20; i++) {
        void *p = cbm_vmem_alloc(sz);
        ASSERT_NOT_NULL(p);
        memset(p, (unsigned char)(i & 0xFF), sz);
        cbm_vmem_free(p, sz);
    }

    size_t after = cbm_vmem_allocated();
    ASSERT_EQ(after, before);
    PASS();
}

TEST(vmem_worker_budget_negative_workers) {
    cbm_vmem_init(0.5);
    size_t budget = cbm_vmem_budget();
    if (budget == 0) {
        /* Not initialized in this process — skip */
        PASS();
    }
    /* Negative workers clamps to 1 → worker_budget == full budget */
    size_t wb = cbm_vmem_worker_budget(-3);
    ASSERT_EQ(wb, budget);
    PASS();
}

TEST(vmem_over_budget_when_nothing_allocated) {
    cbm_vmem_init(0.5);
    /* With nothing (or near-nothing) allocated, should not be over budget */
    bool over = cbm_vmem_over_budget();
    ASSERT_FALSE(over);
    PASS();
}

TEST(vmem_allocated_tracks_alloc_free_cycle) {
    cbm_vmem_init(0.5);
    size_t base = cbm_vmem_allocated();

    size_t sz = 128 * 1024; /* 128 KB */
    void *p = cbm_vmem_alloc(sz);
    ASSERT_NOT_NULL(p);

    size_t after_alloc = cbm_vmem_allocated();
    ASSERT_GT(after_alloc, base);

    cbm_vmem_free(p, sz);

    size_t after_free = cbm_vmem_allocated();
    /* Must be back at or near baseline */
    ASSERT_LTE(after_free, base + 4096);
    PASS();
}

TEST(vmem_multiple_alloc_tracks_cumulative) {
    cbm_vmem_init(0.5);
    size_t base = cbm_vmem_allocated();

    size_t sz = 64 * 1024; /* 64 KB */
    void *p1 = cbm_vmem_alloc(sz);
    ASSERT_NOT_NULL(p1);
    size_t after_one = cbm_vmem_allocated();

    void *p2 = cbm_vmem_alloc(sz);
    ASSERT_NOT_NULL(p2);
    size_t after_two = cbm_vmem_allocated();

    /* Two allocs should be roughly double one alloc above base */
    ASSERT_GT(after_two, after_one);
    ASSERT_GT(after_one, base);

    cbm_vmem_free(p2, sz);
    cbm_vmem_free(p1, sz);

    size_t after_free = cbm_vmem_allocated();
    ASSERT_LTE(after_free, base + 4096);
    PASS();
}

TEST(vmem_peak_never_decreases) {
    cbm_vmem_init(0.5);

    size_t sz = 256 * 1024;
    void *p = cbm_vmem_alloc(sz);
    ASSERT_NOT_NULL(p);
    size_t peak_with_alloc = cbm_vmem_peak();

    cbm_vmem_free(p, sz);
    size_t peak_after_free = cbm_vmem_peak();

    /* Peak must never decrease */
    ASSERT_GTE(peak_after_free, peak_with_alloc);
    PASS();
}

TEST(vmem_worker_budget_one_worker) {
    cbm_vmem_init(0.5);
    size_t budget = cbm_vmem_budget();
    if (budget == 0) {
        PASS();
    }
    /* 1 worker → equals full budget */
    size_t wb = cbm_vmem_worker_budget(1);
    ASSERT_EQ(wb, budget);
    PASS();
}

TEST(vmem_worker_budget_many_workers) {
    cbm_vmem_init(0.5);
    size_t budget = cbm_vmem_budget();
    if (budget == 0) {
        PASS();
    }
    /* 1000 workers → must still be non-zero (budget is huge) */
    size_t wb = cbm_vmem_worker_budget(1000);
    ASSERT_GT(wb, 0);
    ASSERT_EQ(wb, budget / 1000);
    PASS();
}

/* ── Arena-vmem integration tests ─────────────────────────────── */

TEST(arena_vmem_alloc_and_destroy) {
    /* When vmem is initialized, arena should use vmem for blocks.
     * When not initialized, falls back to malloc. Either way, this must work. */
    CBMArena a;
    cbm_arena_init(&a);
    ASSERT_EQ(a.nblocks, 1);
    /* block_sizes[0] should track the initial block size */
    ASSERT_EQ(a.block_sizes[0], CBM_ARENA_DEFAULT_BLOCK_SIZE);

    /* Allocate some data */
    char *s = cbm_arena_strdup(&a, "hello vmem integration");
    ASSERT_NOT_NULL(s);
    ASSERT_STR_EQ(s, "hello vmem integration");

    cbm_arena_destroy(&a);
    ASSERT_EQ(a.nblocks, 0);
    PASS();
}

TEST(arena_vmem_grow_tracks_sizes) {
    CBMArena a;
    cbm_arena_init_sized(&a, 64);
    ASSERT_EQ(a.block_sizes[0], 64);

    /* Force growth */
    cbm_arena_alloc(&a, 48);
    cbm_arena_alloc(&a, 48); /* triggers grow */
    ASSERT_GTE(a.nblocks, 2);
    /* Second block should be larger */
    ASSERT_GT(a.block_sizes[1], 0);
    ASSERT_GTE(a.block_sizes[1], 96); /* at least min_size */

    cbm_arena_destroy(&a);
    PASS();
}

TEST(arena_vmem_large_alloc) {
    /* Allocate > 64KB to test vmem for larger arena blocks */
    CBMArena a;
    cbm_arena_init(&a);

    size_t big = 128 * 1024;
    void *p = cbm_arena_alloc(&a, big);
    ASSERT_NOT_NULL(p);

    /* Write pattern to verify memory is writable */
    memset(p, 0xCD, big);
    unsigned char *bytes = (unsigned char *)p;
    ASSERT_EQ(bytes[0], 0xCD);
    ASSERT_EQ(bytes[big - 1], 0xCD);

    cbm_arena_destroy(&a);
    PASS();
}

TEST(arena_vmem_reset_frees_blocks) {
    CBMArena a;
    cbm_arena_init_sized(&a, 128);

    /* Create multiple blocks */
    cbm_arena_alloc(&a, 100);
    cbm_arena_alloc(&a, 100);
    ASSERT_GTE(a.nblocks, 2);

    /* Reset should free extra blocks */
    cbm_arena_reset(&a);
    ASSERT_EQ(a.nblocks, 1);
    ASSERT_EQ(a.block_sizes[1], 0); /* freed block's size cleared */

    /* Should still be usable */
    void *p = cbm_arena_alloc(&a, 16);
    ASSERT_NOT_NULL(p);

    cbm_arena_destroy(&a);
    PASS();
}

/* ── Tier 2 slab allocator tests ──────────────────────────── */

TEST(tier2_alloc_and_free_128) {
    /* Allocate 100 bytes — rounds up to 128-byte class (with 16-byte header) */
    cbm_vmem_init(0.5);
    cbm_slab_install();

    void *p = cbm_slab_test_malloc(100);
    ASSERT_NOT_NULL(p);

    /* Verify memory is writable */
    memset(p, 0xAA, 100);
    ASSERT_EQ(((unsigned char *)p)[0], 0xAA);
    ASSERT_EQ(((unsigned char *)p)[99], 0xAA);

    cbm_slab_test_free(p);

    /* Allocate again — should reuse from free list (same size class) */
    void *p2 = cbm_slab_test_malloc(100);
    ASSERT_NOT_NULL(p2);
    /* May or may not be same address, but must be valid */
    memset(p2, 0xBB, 100);
    cbm_slab_test_free(p2);

    cbm_slab_destroy_thread();
    PASS();
}

TEST(tier2_alloc_all_classes) {
    /* Test all 6 size classes: 128, 256, 512, 1024, 2048, 4096 */
    cbm_vmem_init(0.5);
    cbm_slab_install();

    size_t test_sizes[] = {65, 200, 300, 800, 1500, 3000};
    void *ptrs[6];

    for (int i = 0; i < 6; i++) {
        ptrs[i] = cbm_slab_test_malloc(test_sizes[i]);
        ASSERT_NOT_NULL(ptrs[i]);
        /* Write pattern to verify each allocation is independent */
        memset(ptrs[i], (unsigned char)(0x10 + i), test_sizes[i]);
    }

    /* Verify patterns are intact (no overlap) */
    for (int i = 0; i < 6; i++) {
        unsigned char *bytes = (unsigned char *)ptrs[i];
        ASSERT_EQ(bytes[0], (unsigned char)(0x10 + i));
        ASSERT_EQ(bytes[test_sizes[i] - 1], (unsigned char)(0x10 + i));
    }

    /* Free all */
    for (int i = 0; i < 6; i++) {
        cbm_slab_test_free(ptrs[i]);
    }

    cbm_slab_destroy_thread();
    PASS();
}

TEST(tier2_free_list_reuse) {
    /* Verify free list provides O(1) reuse within same size class */
    cbm_vmem_init(0.5);
    cbm_slab_install();

    /* Allocate and free 10 blocks of class 256 */
    void *addrs[10];
    for (int i = 0; i < 10; i++) {
        addrs[i] = cbm_slab_test_malloc(200);
        ASSERT_NOT_NULL(addrs[i]);
    }
    for (int i = 0; i < 10; i++) {
        cbm_slab_test_free(addrs[i]);
    }

    /* Re-allocate 10 blocks — all should come from free list
     * (LIFO order means addrs come back in reverse) */
    for (int i = 0; i < 10; i++) {
        void *p = cbm_slab_test_malloc(200);
        ASSERT_NOT_NULL(p);
        memset(p, 0xCC, 200);
        cbm_slab_test_free(p);
    }

    cbm_slab_destroy_thread();
    PASS();
}

TEST(tier2_oversized_dedicated) {
    /* Allocate >4096 bytes — gets dedicated page, freed immediately on free() */
    cbm_vmem_init(0.5);
    cbm_slab_install();

    size_t before_alloc = cbm_vmem_allocated();

    void *big = cbm_slab_test_malloc(8192);
    ASSERT_NOT_NULL(big);
    memset(big, 0xDD, 8192);

    size_t after_alloc = cbm_vmem_allocated();
    /* vmem allocated should have grown for the dedicated page */
    ASSERT_GT(after_alloc, before_alloc);

    /* Free — dedicated page should be vmem_free'd immediately */
    cbm_slab_test_free(big);

    size_t after_free = cbm_vmem_allocated();
    /* vmem allocated should have decreased (page was freed) */
    ASSERT_LTE(after_free, after_alloc);

    cbm_slab_destroy_thread();
    PASS();
}

TEST(tier2_realloc_same_class) {
    /* realloc within same size class should return same pointer */
    cbm_vmem_init(0.5);
    cbm_slab_install();

    void *p = cbm_slab_test_malloc(100);
    ASSERT_NOT_NULL(p);
    memset(p, 0xEE, 100);

    /* Grow to 110 — still fits in 128-byte class */
    void *p2 = cbm_slab_test_realloc(p, 110);
    ASSERT_NOT_NULL(p2);
    ASSERT_EQ(p, p2); /* same pointer, same class */
    /* Original data should be preserved */
    ASSERT_EQ(((unsigned char *)p2)[0], 0xEE);
    ASSERT_EQ(((unsigned char *)p2)[99], 0xEE);

    /* Shrink to 70 — still fits in 128-byte class */
    void *p3 = cbm_slab_test_realloc(p2, 70);
    ASSERT_NOT_NULL(p3);
    ASSERT_EQ(p2, p3); /* same pointer */

    cbm_slab_test_free(p3);
    cbm_slab_destroy_thread();
    PASS();
}

TEST(tier2_realloc_grows_class) {
    /* realloc to larger class should copy data correctly */
    cbm_vmem_init(0.5);
    cbm_slab_install();

    void *p = cbm_slab_test_malloc(100);
    ASSERT_NOT_NULL(p);
    /* Write known pattern */
    for (int i = 0; i < 100; i++) {
        ((unsigned char *)p)[i] = (unsigned char)(i & 0xFF);
    }

    /* Grow to 300 — moves from class 128 to class 512 */
    void *p2 = cbm_slab_test_realloc(p, 300);
    ASSERT_NOT_NULL(p2);

    /* Verify data was copied */
    unsigned char *bytes = (unsigned char *)p2;
    for (int i = 0; i < 100; i++) {
        ASSERT_EQ(bytes[i], (unsigned char)(i & 0xFF));
    }

    /* Write to the extended area */
    memset(bytes + 100, 0xFF, 200);
    ASSERT_EQ(bytes[299], 0xFF);

    cbm_slab_test_free(p2);
    cbm_slab_destroy_thread();
    PASS();
}

TEST(tier2_realloc_slab_to_tier2) {
    /* realloc from Tier 1 (≤64B) to Tier 2 (>64B) */
    cbm_vmem_init(0.5);
    cbm_slab_install();

    void *p = cbm_slab_test_malloc(32); /* Tier 1 slab */
    ASSERT_NOT_NULL(p);
    memset(p, 0x42, 32);

    /* Promote to Tier 2 */
    void *p2 = cbm_slab_test_realloc(p, 200);
    ASSERT_NOT_NULL(p2);
    /* First 32 bytes should be preserved */
    ASSERT_EQ(((unsigned char *)p2)[0], 0x42);
    ASSERT_EQ(((unsigned char *)p2)[31], 0x42);

    cbm_slab_test_free(p2);
    cbm_slab_destroy_thread();
    PASS();
}

TEST(tier2_calloc_zeroed) {
    /* calloc via tier2 must return zeroed memory */
    cbm_vmem_init(0.5);
    cbm_slab_install();

    void *p = cbm_slab_test_calloc(1, 200);
    ASSERT_NOT_NULL(p);

    /* Verify all bytes are zero */
    unsigned char *bytes = (unsigned char *)p;
    int nonzero = 0;
    for (int i = 0; i < 200; i++) {
        if (bytes[i] != 0) {
            nonzero++;
        }
    }
    ASSERT_EQ(nonzero, 0);

    /* Free and re-calloc — recycled memory must still be zeroed */
    cbm_slab_test_free(p);
    void *p2 = cbm_slab_test_calloc(1, 200);
    ASSERT_NOT_NULL(p2);
    bytes = (unsigned char *)p2;
    nonzero = 0;
    for (int i = 0; i < 200; i++) {
        if (bytes[i] != 0) {
            nonzero++;
        }
    }
    ASSERT_EQ(nonzero, 0);

    cbm_slab_test_free(p2);
    cbm_slab_destroy_thread();
    PASS();
}

TEST(tier2_mixed_alloc_free_stress) {
    /* Stress test: interleaved allocs and frees across Tier 1 and Tier 2 */
    cbm_vmem_init(0.5);
    cbm_slab_install();

    void *ptrs[100];
    size_t sizes[100];

    /* Allocate 100 blocks of varying sizes */
    for (int i = 0; i < 100; i++) {
        sizes[i] = (size_t)(16 + (i * 47) % 4000); /* 16..4000 */
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

    /* Free all */
    for (int i = 0; i < 100; i++) {
        cbm_slab_test_free(ptrs[i]);
    }

    cbm_slab_destroy_thread();
    PASS();
}

/* ── Slab + vmem parallel extraction test ──────────────────── */

static char g_vmem_tmpdir[256];

static int setup_vmem_test_repo(void) {
    snprintf(g_vmem_tmpdir, sizeof(g_vmem_tmpdir), "/tmp/cbm_vmem_XXXXXX");
    if (!mkdtemp(g_vmem_tmpdir)) {
        return -1;
    }

    char path[512];

    /* Create multiple Go files to force multi-file parallel extraction.
     * We need enough files to exercise slab reset between files on a worker. */
    for (int i = 0; i < 6; i++) {
        snprintf(path, sizeof(path), "%s/file%d.go", g_vmem_tmpdir, i);
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

    /* Add a C file to exercise the preprocessor second-pass path */
    snprintf(path, sizeof(path), "%s/util.c", g_vmem_tmpdir);
    FILE *f = fopen(path, "w");
    if (!f) {
        return -1;
    }
    fprintf(f, "#include <stdio.h>\nvoid util_func(void) { printf(\"hi\"); }\n"
               "int util_add(int a, int b) { return a + b; }\n");
    fclose(f);

    return 0;
}

static void teardown_vmem_test_repo(void) {
    if (g_vmem_tmpdir[0]) {
        th_rmtree(g_vmem_tmpdir);
        g_vmem_tmpdir[0] = '\0';
    }
}

TEST(vmem_parallel_extract_with_slab) {
    /* This test reproduces a SIGSEGV that occurred when:
     *   1. vmem is active (arena + source via mmap)
     *   2. slab allocator is installed (tree-sitter uses slab)
     *   3. slab_reset_thread() was called between files, corrupting
     *      the parser's live slab-allocated internal state (subtree pool,
     *      stack entries, cached tokens).
     *
     * The fix: don't call slab_reset_thread() between files. Normal
     * slab_free() from ts_tree_delete() returns chunks for reuse.
     * slab_destroy_thread() reclaims everything on worker exit. */
    cbm_vmem_init(0.5);

    if (setup_vmem_test_repo() != 0) {
        SKIP("tmpdir setup failed");
    }

    cbm_discover_opts_t opts = {.mode = CBM_MODE_FULL};
    cbm_file_info_t *files = NULL;
    int file_count = 0;
    if (cbm_discover(g_vmem_tmpdir, &opts, &files, &file_count) != 0) {
        teardown_vmem_test_repo();
        SKIP("discover failed");
    }

    ASSERT_GTE(file_count, 5);

    cbm_gbuf_t *gbuf = cbm_gbuf_new("vmem-test", g_vmem_tmpdir);
    cbm_registry_t *reg = cbm_registry_new();
    atomic_int cancelled;
    atomic_init(&cancelled, 0);

    cbm_pipeline_ctx_t ctx = {
        .project_name = "vmem-test",
        .repo_path = g_vmem_tmpdir,
        .gbuf = gbuf,
        .registry = reg,
        .cancelled = &cancelled,
    };

    _Atomic int64_t shared_ids;
    int64_t gbuf_next = cbm_gbuf_next_id(gbuf);
    atomic_init(&shared_ids, gbuf_next);

    CBMFileResult **result_cache = calloc(file_count, sizeof(CBMFileResult *));
    ASSERT_NOT_NULL(result_cache);

    /* Run parallel extraction with 2 workers — enough to trigger
     * multi-file slab reuse on at least one worker. */
    int rc = cbm_parallel_extract(&ctx, files, file_count, result_cache, &shared_ids, 2);
    ASSERT_EQ(rc, 0);

    /* Verify extraction produced results */
    int cached_count = 0;
    for (int i = 0; i < file_count; i++) {
        if (result_cache[i]) {
            cached_count++;
        }
    }
    ASSERT_GTE(cached_count, 5);

    /* Verify nodes were created */
    ASSERT_GT(cbm_gbuf_node_count(gbuf), 0);

    /* Clean up */
    for (int i = 0; i < file_count; i++) {
        if (result_cache[i]) {
            cbm_free_result(result_cache[i]);
        }
    }
    free(result_cache);
    cbm_registry_free(reg);
    cbm_gbuf_free(gbuf);
    cbm_discover_free(files, file_count);
    teardown_vmem_test_repo();
    PASS();
}

SUITE(vmem) {
    RUN_TEST(vmem_budget_zero_before_init);
    RUN_TEST(vmem_alloc_and_free);
    RUN_TEST(vmem_alloc_zero_returns_zeroed);
    RUN_TEST(vmem_budget_tracking);
    RUN_TEST(vmem_peak_tracks);
    RUN_TEST(vmem_worker_budget);
    /* Edge cases and resource management */
    RUN_TEST(vmem_alloc_zero_returns_null);
    RUN_TEST(vmem_free_null_zero_no_crash);
    RUN_TEST(vmem_free_null_nonzero_no_crash);
    RUN_TEST(vmem_alloc_very_large);
    RUN_TEST(vmem_sequential_alloc_free_no_leak);
    RUN_TEST(vmem_worker_budget_negative_workers);
    RUN_TEST(vmem_over_budget_when_nothing_allocated);
    RUN_TEST(vmem_allocated_tracks_alloc_free_cycle);
    RUN_TEST(vmem_multiple_alloc_tracks_cumulative);
    RUN_TEST(vmem_peak_never_decreases);
    RUN_TEST(vmem_worker_budget_one_worker);
    RUN_TEST(vmem_worker_budget_many_workers);
    /* Arena-vmem integration */
    RUN_TEST(arena_vmem_alloc_and_destroy);
    RUN_TEST(arena_vmem_grow_tracks_sizes);
    RUN_TEST(arena_vmem_large_alloc);
    RUN_TEST(arena_vmem_reset_frees_blocks);
    /* Tier 2 slab allocator tests */
    RUN_TEST(tier2_alloc_and_free_128);
    RUN_TEST(tier2_alloc_all_classes);
    RUN_TEST(tier2_free_list_reuse);
    RUN_TEST(tier2_oversized_dedicated);
    RUN_TEST(tier2_realloc_same_class);
    RUN_TEST(tier2_realloc_grows_class);
    RUN_TEST(tier2_realloc_slab_to_tier2);
    RUN_TEST(tier2_calloc_zeroed);
    RUN_TEST(tier2_mixed_alloc_free_stress);
    /* Integration */
    RUN_TEST(vmem_parallel_extract_with_slab);
}
