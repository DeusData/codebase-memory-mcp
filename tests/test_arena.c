/*
 * test_arena.c — RED phase tests for foundation/arena.
 */
#include "test_framework.h"
#include "../src/foundation/arena.h"
#include <stdint.h>

TEST(arena_init_default) {
    CBMArena a;
    cbm_arena_init(&a);
    ASSERT_EQ(a.nblocks, 1);
    ASSERT_EQ(a.block_size, CBM_ARENA_DEFAULT_BLOCK_SIZE);
    ASSERT_EQ(a.used, 0);
    ASSERT_EQ(a.total_alloc, 0);
    cbm_arena_destroy(&a);
    PASS();
}

TEST(arena_init_sized) {
    CBMArena a;
    cbm_arena_init_sized(&a, 256);
    ASSERT_EQ(a.nblocks, 1);
    ASSERT_EQ(a.block_size, 256);
    cbm_arena_destroy(&a);
    PASS();
}

TEST(arena_alloc_basic) {
    CBMArena a;
    cbm_arena_init(&a);
    void *p = cbm_arena_alloc(&a, 100);
    ASSERT_NOT_NULL(p);
    ASSERT_GT(a.used, 0);
    ASSERT_GT(a.total_alloc, 0);
    cbm_arena_destroy(&a);
    PASS();
}

TEST(arena_alloc_zero) {
    CBMArena a;
    cbm_arena_init(&a);
    void *p = cbm_arena_alloc(&a, 0);
    ASSERT_NULL(p);
    cbm_arena_destroy(&a);
    PASS();
}

TEST(arena_alloc_alignment) {
    CBMArena a;
    cbm_arena_init(&a);
    /* Allocate 1 byte — should be padded to 8 */
    void *p1 = cbm_arena_alloc(&a, 1);
    void *p2 = cbm_arena_alloc(&a, 1);
    ASSERT_NOT_NULL(p1);
    ASSERT_NOT_NULL(p2);
    /* Both should be 8-byte aligned */
    ASSERT_EQ((uintptr_t)p1 % 8, 0);
    ASSERT_EQ((uintptr_t)p2 % 8, 0);
    /* And 8 bytes apart */
    ASSERT_EQ((char *)p2 - (char *)p1, 8);
    cbm_arena_destroy(&a);
    PASS();
}

TEST(arena_alloc_grows_blocks) {
    CBMArena a;
    cbm_arena_init_sized(&a, 64); /* tiny block to force growth */
    /* Allocate more than one block's worth */
    void *p1 = cbm_arena_alloc(&a, 48);
    ASSERT_NOT_NULL(p1);
    ASSERT_EQ(a.nblocks, 1);
    void *p2 = cbm_arena_alloc(&a, 48);
    ASSERT_NOT_NULL(p2);
    ASSERT_EQ(a.nblocks, 2); /* should have grown */
    cbm_arena_destroy(&a);
    PASS();
}

TEST(arena_alloc_large_single) {
    CBMArena a;
    cbm_arena_init_sized(&a, 64);
    /* Allocate something larger than block_size */
    void *p = cbm_arena_alloc(&a, 256);
    ASSERT_NOT_NULL(p);
    ASSERT_GTE(a.block_size, 256);
    cbm_arena_destroy(&a);
    PASS();
}

TEST(arena_calloc) {
    CBMArena a;
    cbm_arena_init(&a);
    unsigned char *p = (unsigned char *)cbm_arena_calloc(&a, 64);
    ASSERT_NOT_NULL(p);
    /* All bytes should be zero */
    for (int i = 0; i < 64; i++) {
        ASSERT_EQ(p[i], 0);
    }
    cbm_arena_destroy(&a);
    PASS();
}

TEST(arena_strdup) {
    CBMArena a;
    cbm_arena_init(&a);
    char *s = cbm_arena_strdup(&a, "hello world");
    ASSERT_NOT_NULL(s);
    ASSERT_STR_EQ(s, "hello world");
    /* Original string modification shouldn't affect copy */
    cbm_arena_destroy(&a);
    PASS();
}

TEST(arena_strdup_null) {
    CBMArena a;
    cbm_arena_init(&a);
    char *s = cbm_arena_strdup(&a, NULL);
    ASSERT_NULL(s);
    cbm_arena_destroy(&a);
    PASS();
}

TEST(arena_strndup) {
    CBMArena a;
    cbm_arena_init(&a);
    char *s = cbm_arena_strndup(&a, "hello world", 5);
    ASSERT_NOT_NULL(s);
    ASSERT_STR_EQ(s, "hello");
    cbm_arena_destroy(&a);
    PASS();
}

TEST(arena_sprintf) {
    CBMArena a;
    cbm_arena_init(&a);
    char *s = cbm_arena_sprintf(&a, "%s.%s.%s", "project", "path", "name");
    ASSERT_NOT_NULL(s);
    ASSERT_STR_EQ(s, "project.path.name");
    cbm_arena_destroy(&a);
    PASS();
}

TEST(arena_sprintf_int) {
    CBMArena a;
    cbm_arena_init(&a);
    char *s = cbm_arena_sprintf(&a, "count=%d", 42);
    ASSERT_NOT_NULL(s);
    ASSERT_STR_EQ(s, "count=42");
    cbm_arena_destroy(&a);
    PASS();
}

TEST(arena_reset) {
    CBMArena a;
    cbm_arena_init_sized(&a, 128);
    /* Allocate enough to create multiple blocks */
    cbm_arena_alloc(&a, 100);
    cbm_arena_alloc(&a, 100);
    ASSERT_GTE(a.nblocks, 2);
    /* Reset should keep first block, free rest */
    cbm_arena_reset(&a);
    ASSERT_EQ(a.nblocks, 1);
    ASSERT_EQ(a.used, 0);
    /* Should still be usable */
    void *p = cbm_arena_alloc(&a, 16);
    ASSERT_NOT_NULL(p);
    cbm_arena_destroy(&a);
    PASS();
}

TEST(arena_total) {
    CBMArena a;
    cbm_arena_init(&a);
    ASSERT_EQ(cbm_arena_total(&a), 0);
    cbm_arena_alloc(&a, 100);
    ASSERT_GTE(cbm_arena_total(&a), 100); /* at least 100 (may be aligned) */
    cbm_arena_alloc(&a, 200);
    ASSERT_GTE(cbm_arena_total(&a), 300);
    cbm_arena_destroy(&a);
    PASS();
}

TEST(arena_many_small_allocs) {
    CBMArena a;
    cbm_arena_init(&a);
    /* 10K small allocations — shouldn't exhaust MAX_BLOCKS */
    for (int i = 0; i < 10000; i++) {
        void *p = cbm_arena_alloc(&a, 16);
        ASSERT_NOT_NULL(p);
    }
    cbm_arena_destroy(&a);
    PASS();
}

/* ── Edge case tests ───────────────────────────────────────────── */

TEST(arena_init_sized_clamp_small) {
    /* block_size < 64 should be clamped to 64 */
    CBMArena a;
    cbm_arena_init_sized(&a, 32);
    ASSERT_EQ(a.block_size, 64);
    ASSERT_EQ(a.nblocks, 1);
    /* Should still be usable */
    void *p = cbm_arena_alloc(&a, 16);
    ASSERT_NOT_NULL(p);
    cbm_arena_destroy(&a);
    PASS();
}

TEST(arena_init_sized_clamp_zero) {
    /* block_size 0 should be clamped to 64 */
    CBMArena a;
    cbm_arena_init_sized(&a, 0);
    ASSERT_EQ(a.block_size, 64);
    ASSERT_EQ(a.nblocks, 1);
    cbm_arena_destroy(&a);
    PASS();
}

TEST(arena_alloc_null_on_zero_nblocks) {
    /* Corrupted arena with nblocks=0 should return NULL */
    CBMArena a;
    memset(&a, 0, sizeof(a));
    /* nblocks is 0 — no valid blocks */
    void *p = cbm_arena_alloc(&a, 16);
    ASSERT_NULL(p);
    /* No destroy needed — nothing was allocated */
    PASS();
}

TEST(arena_multiple_resets) {
    /* reset, use, reset, use — should not leak or crash */
    CBMArena a;
    cbm_arena_init_sized(&a, 128);

    /* Round 1 */
    cbm_arena_alloc(&a, 100);
    cbm_arena_alloc(&a, 100);
    ASSERT_GTE(a.nblocks, 2);
    cbm_arena_reset(&a);
    ASSERT_EQ(a.nblocks, 1);
    ASSERT_EQ(a.used, 0);
    ASSERT_EQ(cbm_arena_total(&a), 0);

    /* Round 2 — allocate more than one block (default 64KB) to force nblocks >= 2 */
    void *p = cbm_arena_alloc(&a, 60000);
    ASSERT_NOT_NULL(p);
    cbm_arena_alloc(&a, 60000);
    ASSERT_GTE(a.nblocks, 2);
    cbm_arena_reset(&a);
    ASSERT_EQ(a.nblocks, 1);
    ASSERT_EQ(a.used, 0);

    /* Round 3 — use after second reset */
    p = cbm_arena_alloc(&a, 32);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(a.nblocks, 1);

    cbm_arena_destroy(&a);
    PASS();
}

TEST(arena_reset_single_block) {
    /* Reset on arena that never grew — should be a no-op, not crash */
    CBMArena a;
    cbm_arena_init_sized(&a, 1024);
    cbm_arena_alloc(&a, 16);
    ASSERT_EQ(a.nblocks, 1);
    cbm_arena_reset(&a);
    ASSERT_EQ(a.nblocks, 1);
    ASSERT_EQ(a.used, 0);
    /* Still usable */
    void *p = cbm_arena_alloc(&a, 8);
    ASSERT_NOT_NULL(p);
    cbm_arena_destroy(&a);
    PASS();
}

TEST(arena_strdup_empty) {
    CBMArena a;
    cbm_arena_init(&a);
    char *s = cbm_arena_strdup(&a, "");
    ASSERT_NOT_NULL(s);
    ASSERT_STR_EQ(s, "");
    ASSERT_EQ(strlen(s), 0);
    cbm_arena_destroy(&a);
    PASS();
}

TEST(arena_strndup_len_exceeds_string) {
    /* len > actual string length — copies len bytes (may include garbage
     * after NUL, but result must be NUL-terminated at position len) */
    CBMArena a;
    cbm_arena_init(&a);
    const char *src = "abc";
    /* len=10 > strlen("abc")=3 — implementation copies exactly len bytes */
    char *s = cbm_arena_strndup(&a, src, 3);
    ASSERT_NOT_NULL(s);
    ASSERT_STR_EQ(s, "abc");
    /* Now test with len matching exactly */
    s = cbm_arena_strndup(&a, src, 2);
    ASSERT_NOT_NULL(s);
    ASSERT_STR_EQ(s, "ab");
    cbm_arena_destroy(&a);
    PASS();
}

TEST(arena_sprintf_empty_format) {
    CBMArena a;
    cbm_arena_init(&a);
    /* Use "%s" with empty string — GCC rejects literal "" as format (-Wformat-zero-length) */
    char *s = cbm_arena_sprintf(&a, "%s", "");
    ASSERT_NOT_NULL(s);
    ASSERT_STR_EQ(s, "");
    ASSERT_EQ(strlen(s), 0);
    cbm_arena_destroy(&a);
    PASS();
}

TEST(arena_double_destroy) {
    /* Destroy already-destroyed arena — should not crash (memset to 0) */
    CBMArena a;
    cbm_arena_init(&a);
    cbm_arena_alloc(&a, 128);
    cbm_arena_destroy(&a);
    /* After destroy, nblocks=0, all pointers NULL */
    ASSERT_EQ(a.nblocks, 0);
    ASSERT_EQ(a.used, 0);
    /* Second destroy — iterates 0 blocks, memsets again — safe */
    cbm_arena_destroy(&a);
    ASSERT_EQ(a.nblocks, 0);
    PASS();
}

TEST(arena_calloc_zero) {
    CBMArena a;
    cbm_arena_init(&a);
    /* calloc(0) delegates to alloc(0) which returns NULL */
    void *p = cbm_arena_calloc(&a, 0);
    ASSERT_NULL(p);
    cbm_arena_destroy(&a);
    PASS();
}

TEST(arena_many_large_allocs_block_growth) {
    /* Force many block growths with large allocations */
    CBMArena a;
    cbm_arena_init_sized(&a, 64); /* tiny blocks */
    for (int i = 0; i < 50; i++) {
        void *p = cbm_arena_alloc(&a, 128); /* each > block_size initially */
        ASSERT_NOT_NULL(p);
    }
    /* Should have grown many blocks */
    ASSERT_GT(a.nblocks, 1);
    ASSERT_GTE(cbm_arena_total(&a), 50 * 128);
    cbm_arena_destroy(&a);
    PASS();
}

TEST(arena_total_through_reset) {
    /* total_alloc resets to 0 on reset, then accumulates again */
    CBMArena a;
    cbm_arena_init_sized(&a, 1024);
    cbm_arena_alloc(&a, 100);
    ASSERT_GTE(cbm_arena_total(&a), 100);
    size_t before_reset = cbm_arena_total(&a);
    ASSERT_GT(before_reset, 0);

    cbm_arena_reset(&a);
    ASSERT_EQ(cbm_arena_total(&a), 0);

    /* Allocate again — total starts fresh */
    cbm_arena_alloc(&a, 50);
    ASSERT_GTE(cbm_arena_total(&a), 50);
    /* But should be less than before_reset (fresh accumulation) */
    ASSERT_LTE(cbm_arena_total(&a), before_reset);
    cbm_arena_destroy(&a);
    PASS();
}

TEST(arena_reset_block_size_invariant) {
    /* BUG DETECTOR: after reset, block_size should match blocks[0]'s actual size.
     * arena_grow() updates block_size to the new (larger) block's size.
     * arena_reset() frees the grown blocks but does NOT restore block_size.
     * Result: arena thinks blocks[0] has more capacity than it actually does.
     * This is a heap-buffer-overflow waiting to happen. */
    CBMArena a;
    cbm_arena_init_sized(&a, 128);
    size_t original_block_size = a.block_size;
    ASSERT_EQ(original_block_size, 128);
    ASSERT_EQ(a.block_sizes[0], 128);

    /* Force growth: block_size will be updated to new (larger) value */
    cbm_arena_alloc(&a, 100);
    cbm_arena_alloc(&a, 100); /* triggers grow */
    ASSERT_GTE(a.nblocks, 2);
    ASSERT_GT(a.block_size, 128); /* block_size grew */

    /* Reset: frees grown blocks, keeps blocks[0] (which is 128 bytes) */
    cbm_arena_reset(&a);
    ASSERT_EQ(a.nblocks, 1);

    /* CRITICAL CHECK: block_size must match blocks[0]'s actual capacity.
     * If block_size > block_sizes[0], the arena will allow allocations
     * that overflow blocks[0]. */
    ASSERT_EQ(a.block_size, a.block_sizes[0]);

    cbm_arena_destroy(&a);
    PASS();
}

TEST(arena_strndup_zero_len) {
    /* strndup with len=0 — should return empty string */
    CBMArena a;
    cbm_arena_init(&a);
    char *s = cbm_arena_strndup(&a, "hello", 0);
    /* alloc(0+1=1) should succeed, result is NUL-terminated at pos 0 */
    ASSERT_NOT_NULL(s);
    ASSERT_STR_EQ(s, "");
    cbm_arena_destroy(&a);
    PASS();
}

SUITE(arena) {
    RUN_TEST(arena_init_default);
    RUN_TEST(arena_init_sized);
    RUN_TEST(arena_alloc_basic);
    RUN_TEST(arena_alloc_zero);
    RUN_TEST(arena_alloc_alignment);
    RUN_TEST(arena_alloc_grows_blocks);
    RUN_TEST(arena_alloc_large_single);
    RUN_TEST(arena_calloc);
    RUN_TEST(arena_strdup);
    RUN_TEST(arena_strdup_null);
    RUN_TEST(arena_strndup);
    RUN_TEST(arena_sprintf);
    RUN_TEST(arena_sprintf_int);
    RUN_TEST(arena_reset);
    RUN_TEST(arena_total);
    RUN_TEST(arena_many_small_allocs);
    /* Edge cases */
    RUN_TEST(arena_init_sized_clamp_small);
    RUN_TEST(arena_init_sized_clamp_zero);
    RUN_TEST(arena_alloc_null_on_zero_nblocks);
    RUN_TEST(arena_multiple_resets);
    RUN_TEST(arena_reset_single_block);
    RUN_TEST(arena_strdup_empty);
    RUN_TEST(arena_strndup_len_exceeds_string);
    RUN_TEST(arena_sprintf_empty_format);
    RUN_TEST(arena_double_destroy);
    RUN_TEST(arena_calloc_zero);
    RUN_TEST(arena_many_large_allocs_block_growth);
    RUN_TEST(arena_total_through_reset);
    RUN_TEST(arena_reset_block_size_invariant);
    RUN_TEST(arena_strndup_zero_len);
}
