/*
 * test_dyn_array.c — RED phase tests for foundation/dyn_array.
 */
#include "test_framework.h"
#include "../src/foundation/dyn_array.h"
#include <stdint.h>

TEST(da_push_pop) {
    CBM_DYN_ARRAY(int) arr = {0};
    cbm_da_push(&arr, 10);
    cbm_da_push(&arr, 20);
    cbm_da_push(&arr, 30);
    ASSERT_EQ(arr.count, 3);
    ASSERT_EQ(arr.items[0], 10);
    ASSERT_EQ(arr.items[1], 20);
    ASSERT_EQ(arr.items[2], 30);

    int v = cbm_da_pop(&arr);
    ASSERT_EQ(v, 30);
    ASSERT_EQ(arr.count, 2);
    cbm_da_free(&arr);
    PASS();
}

TEST(da_last) {
    CBM_DYN_ARRAY(int) arr = {0};
    cbm_da_push(&arr, 1);
    cbm_da_push(&arr, 2);
    ASSERT_EQ(cbm_da_last(&arr), 2);
    cbm_da_free(&arr);
    PASS();
}

TEST(da_clear) {
    CBM_DYN_ARRAY(int) arr = {0};
    cbm_da_push(&arr, 1);
    cbm_da_push(&arr, 2);
    cbm_da_clear(&arr);
    ASSERT_EQ(arr.count, 0);
    /* cap should still be > 0 (memory retained) */
    ASSERT_GT(arr.cap, 0);
    cbm_da_free(&arr);
    PASS();
}

TEST(da_free) {
    CBM_DYN_ARRAY(int) arr = {0};
    cbm_da_push(&arr, 1);
    cbm_da_free(&arr);
    ASSERT_NULL(arr.items);
    ASSERT_EQ(arr.count, 0);
    ASSERT_EQ(arr.cap, 0);
    PASS();
}

TEST(da_growth) {
    CBM_DYN_ARRAY(int) arr = {0};
    /* Push 1000 items — verify capacity grows */
    for (int i = 0; i < 1000; i++) {
        cbm_da_push(&arr, i);
    }
    ASSERT_EQ(arr.count, 1000);
    ASSERT_GTE(arr.cap, 1000);
    /* Verify all values */
    for (int i = 0; i < 1000; i++) {
        ASSERT_EQ(arr.items[i], i);
    }
    cbm_da_free(&arr);
    PASS();
}

TEST(da_reserve) {
    CBM_DYN_ARRAY(int) arr = {0};
    cbm_da_reserve(&arr, 100);
    ASSERT_GTE(arr.cap, 100);
    ASSERT_EQ(arr.count, 0);
    cbm_da_free(&arr);
    PASS();
}

TEST(da_struct_type) {
    typedef struct {
        int x;
        float y;
    } Point;
    CBM_DYN_ARRAY(Point) pts = {0};
    cbm_da_push(&pts, ((Point){.x = 1, .y = 2.5f}));
    cbm_da_push(&pts, ((Point){.x = 3, .y = 4.5f}));
    ASSERT_EQ(pts.count, 2);
    ASSERT_EQ(pts.items[0].x, 1);
    ASSERT_EQ(pts.items[1].x, 3);
    cbm_da_free(&pts);
    PASS();
}

TEST(da_string_ptrs) {
    CBM_DYN_ARRAY(const char *) strs = {0};
    cbm_da_push(&strs, "hello");
    cbm_da_push(&strs, "world");
    ASSERT_EQ(strs.count, 2);
    ASSERT_STR_EQ(strs.items[0], "hello");
    ASSERT_STR_EQ(strs.items[1], "world");
    cbm_da_free(&strs);
    PASS();
}

TEST(da_remove) {
    CBM_DYN_ARRAY(int) arr = {0};
    cbm_da_push(&arr, 10);
    cbm_da_push(&arr, 20);
    cbm_da_push(&arr, 30);
    cbm_da_remove(&arr, 1); /* remove 20 */
    ASSERT_EQ(arr.count, 2);
    ASSERT_EQ(arr.items[0], 10);
    ASSERT_EQ(arr.items[1], 30);
    cbm_da_free(&arr);
    PASS();
}

TEST(da_insert) {
    CBM_DYN_ARRAY(int) arr = {0};
    cbm_da_push(&arr, 10);
    cbm_da_push(&arr, 30);
    cbm_da_insert(&arr, 1, 20); /* insert 20 at index 1 */
    ASSERT_EQ(arr.count, 3);
    ASSERT_EQ(arr.items[0], 10);
    ASSERT_EQ(arr.items[1], 20);
    ASSERT_EQ(arr.items[2], 30);
    cbm_da_free(&arr);
    PASS();
}

TEST(da_push_ptr_struct) {
    typedef struct {
        int id;
        double value;
        const char *tag;
    } Record;
    CBM_DYN_ARRAY(Record) arr = {0};

    Record *r = cbm_da_push_ptr(&arr);
    r->id = 42;
    r->value = 3.14;
    r->tag = "first";

    r = cbm_da_push_ptr(&arr);
    r->id = 99;
    r->value = 2.72;
    r->tag = "second";

    ASSERT_EQ(arr.count, 2);
    ASSERT_EQ(arr.items[0].id, 42);
    ASSERT_STR_EQ(arr.items[0].tag, "first");
    ASSERT_EQ(arr.items[1].id, 99);
    ASSERT_STR_EQ(arr.items[1].tag, "second");
    cbm_da_free(&arr);
    PASS();
}

TEST(da_insert_at_beginning) {
    CBM_DYN_ARRAY(int) arr = {0};
    cbm_da_push(&arr, 20);
    cbm_da_push(&arr, 30);
    cbm_da_push(&arr, 40);
    cbm_da_insert(&arr, 0, 10); /* insert at front */
    ASSERT_EQ(arr.count, 4);
    ASSERT_EQ(arr.items[0], 10);
    ASSERT_EQ(arr.items[1], 20);
    ASSERT_EQ(arr.items[2], 30);
    ASSERT_EQ(arr.items[3], 40);
    cbm_da_free(&arr);
    PASS();
}

TEST(da_insert_at_end) {
    CBM_DYN_ARRAY(int) arr = {0};
    cbm_da_push(&arr, 10);
    cbm_da_push(&arr, 20);
    cbm_da_insert(&arr, 2, 30); /* insert at count (append) */
    ASSERT_EQ(arr.count, 3);
    ASSERT_EQ(arr.items[0], 10);
    ASSERT_EQ(arr.items[1], 20);
    ASSERT_EQ(arr.items[2], 30);
    cbm_da_free(&arr);
    PASS();
}

TEST(da_remove_first) {
    CBM_DYN_ARRAY(int) arr = {0};
    cbm_da_push(&arr, 10);
    cbm_da_push(&arr, 20);
    cbm_da_push(&arr, 30);
    cbm_da_remove(&arr, 0); /* remove first */
    ASSERT_EQ(arr.count, 2);
    ASSERT_EQ(arr.items[0], 20);
    ASSERT_EQ(arr.items[1], 30);
    cbm_da_free(&arr);
    PASS();
}

TEST(da_remove_last) {
    CBM_DYN_ARRAY(int) arr = {0};
    cbm_da_push(&arr, 10);
    cbm_da_push(&arr, 20);
    cbm_da_push(&arr, 30);
    cbm_da_remove(&arr, 2); /* remove last element */
    ASSERT_EQ(arr.count, 2);
    ASSERT_EQ(arr.items[0], 10);
    ASSERT_EQ(arr.items[1], 20);
    cbm_da_free(&arr);
    PASS();
}

TEST(da_remove_sequence) {
    CBM_DYN_ARRAY(int) arr = {0};
    for (int i = 0; i < 5; i++)
        cbm_da_push(&arr, i * 10); /* 0, 10, 20, 30, 40 */

    cbm_da_remove(&arr, 2); /* remove 20 -> 0,10,30,40 */
    ASSERT_EQ(arr.count, 4);
    ASSERT_EQ(arr.items[2], 30);

    cbm_da_remove(&arr, 0); /* remove 0 -> 10,30,40 */
    ASSERT_EQ(arr.count, 3);
    ASSERT_EQ(arr.items[0], 10);

    cbm_da_remove(&arr, 2); /* remove 40 -> 10,30 */
    ASSERT_EQ(arr.count, 2);
    ASSERT_EQ(arr.items[0], 10);
    ASSERT_EQ(arr.items[1], 30);
    cbm_da_free(&arr);
    PASS();
}

TEST(da_reserve_then_push) {
    CBM_DYN_ARRAY(int) arr = {0};
    cbm_da_reserve(&arr, 50);
    int cap_after_reserve = arr.cap;
    ASSERT_GTE(cap_after_reserve, 50);

    /* Push 50 items — capacity should not change (no realloc needed) */
    for (int i = 0; i < 50; i++)
        cbm_da_push(&arr, i);
    ASSERT_EQ(arr.count, 50);
    ASSERT_EQ(arr.cap, cap_after_reserve);
    for (int i = 0; i < 50; i++)
        ASSERT_EQ(arr.items[i], i);
    cbm_da_free(&arr);
    PASS();
}

TEST(da_reserve_zero) {
    CBM_DYN_ARRAY(int) arr = {0};
    cbm_da_reserve(&arr, 0);
    /* Should be a no-op: cap stays 0, items stays NULL */
    ASSERT_EQ(arr.cap, 0);
    ASSERT_NULL(arr.items);
    cbm_da_free(&arr);
    PASS();
}

TEST(da_reserve_less_than_cap) {
    CBM_DYN_ARRAY(int) arr = {0};
    cbm_da_reserve(&arr, 100);
    int old_cap = arr.cap;
    /* Reserve smaller than current cap — should be a no-op */
    cbm_da_reserve(&arr, 10);
    ASSERT_EQ(arr.cap, old_cap);
    cbm_da_free(&arr);
    PASS();
}

TEST(da_clear_then_push) {
    CBM_DYN_ARRAY(int) arr = {0};
    cbm_da_push(&arr, 1);
    cbm_da_push(&arr, 2);
    cbm_da_push(&arr, 3);
    int old_cap = arr.cap;

    cbm_da_clear(&arr);
    ASSERT_EQ(arr.count, 0);
    /* Memory retained after clear */
    ASSERT_EQ(arr.cap, old_cap);
    ASSERT_NOT_NULL(arr.items);

    /* Push again — should reuse existing memory */
    cbm_da_push(&arr, 100);
    cbm_da_push(&arr, 200);
    ASSERT_EQ(arr.count, 2);
    ASSERT_EQ(arr.items[0], 100);
    ASSERT_EQ(arr.items[1], 200);
    cbm_da_free(&arr);
    PASS();
}

TEST(da_free_then_push) {
    CBM_DYN_ARRAY(int) arr = {0};
    cbm_da_push(&arr, 1);
    cbm_da_push(&arr, 2);
    cbm_da_free(&arr);
    ASSERT_NULL(arr.items);
    ASSERT_EQ(arr.count, 0);
    ASSERT_EQ(arr.cap, 0);

    /* Re-use after free — should work like fresh array */
    cbm_da_push(&arr, 42);
    ASSERT_EQ(arr.count, 1);
    ASSERT_EQ(arr.items[0], 42);
    ASSERT_GT(arr.cap, 0);
    cbm_da_free(&arr);
    PASS();
}

TEST(da_large_array_10k) {
    CBM_DYN_ARRAY(int64_t) arr = {0};
    for (int i = 0; i < 10000; i++)
        cbm_da_push(&arr, (int64_t)i * 1000);
    ASSERT_EQ(arr.count, 10000);
    ASSERT_GTE(arr.cap, 10000);

    /* Verify contiguous memory: all values accessible and correct */
    for (int i = 0; i < 10000; i++)
        ASSERT_EQ(arr.items[i], (int64_t)i * 1000);

    /* Verify truly contiguous: pointer arithmetic */
    int64_t *base = arr.items;
    for (int i = 0; i < 10000; i++)
        ASSERT_EQ(&arr.items[i], base + i);

    cbm_da_free(&arr);
    PASS();
}

TEST(da_nested_struct_with_ptrs) {
    typedef struct {
        const char *name;
        int scores[3];
        const char *category;
    } Entry;
    CBM_DYN_ARRAY(Entry) arr = {0};

    cbm_da_push(&arr, ((Entry){.name = "alpha", .scores = {10, 20, 30}, .category = "A"}));
    cbm_da_push(&arr, ((Entry){.name = "beta", .scores = {40, 50, 60}, .category = "B"}));
    cbm_da_push(&arr, ((Entry){.name = "gamma", .scores = {70, 80, 90}, .category = "A"}));

    ASSERT_EQ(arr.count, 3);
    ASSERT_STR_EQ(arr.items[0].name, "alpha");
    ASSERT_STR_EQ(arr.items[1].name, "beta");
    ASSERT_STR_EQ(arr.items[2].name, "gamma");
    ASSERT_EQ(arr.items[0].scores[2], 30);
    ASSERT_EQ(arr.items[2].scores[0], 70);
    ASSERT_STR_EQ(arr.items[0].category, "A");
    ASSERT_STR_EQ(arr.items[1].category, "B");
    cbm_da_free(&arr);
    PASS();
}

TEST(da_push_pop_lifo) {
    CBM_DYN_ARRAY(int) arr = {0};
    /* Push 1..5 */
    for (int i = 1; i <= 5; i++)
        cbm_da_push(&arr, i);

    /* Pop should return in LIFO order: 5,4,3,2,1 */
    ASSERT_EQ(cbm_da_pop(&arr), 5);
    ASSERT_EQ(cbm_da_pop(&arr), 4);
    ASSERT_EQ(cbm_da_pop(&arr), 3);
    ASSERT_EQ(cbm_da_pop(&arr), 2);
    ASSERT_EQ(cbm_da_pop(&arr), 1);
    ASSERT_EQ(arr.count, 0);

    /* Push again after full pop */
    cbm_da_push(&arr, 99);
    ASSERT_EQ(arr.count, 1);
    ASSERT_EQ(arr.items[0], 99);
    cbm_da_free(&arr);
    PASS();
}

TEST(da_mixed_operations) {
    CBM_DYN_ARRAY(int) arr = {0};

    /* push 10, 20, 30 */
    cbm_da_push(&arr, 10);
    cbm_da_push(&arr, 20);
    cbm_da_push(&arr, 30);
    ASSERT_EQ(arr.count, 3);

    /* insert 15 at index 1 -> 10,15,20,30 */
    cbm_da_insert(&arr, 1, 15);
    ASSERT_EQ(arr.count, 4);
    ASSERT_EQ(arr.items[1], 15);
    ASSERT_EQ(arr.items[2], 20);

    /* remove index 0 -> 15,20,30 */
    cbm_da_remove(&arr, 0);
    ASSERT_EQ(arr.count, 3);
    ASSERT_EQ(arr.items[0], 15);

    /* push 40 -> 15,20,30,40 */
    cbm_da_push(&arr, 40);
    ASSERT_EQ(arr.count, 4);

    /* pop -> 40, remaining: 15,20,30 */
    int v = cbm_da_pop(&arr);
    ASSERT_EQ(v, 40);
    ASSERT_EQ(arr.count, 3);

    /* last should be 30 */
    ASSERT_EQ(cbm_da_last(&arr), 30);

    /* insert at end -> 15,20,30,50 */
    cbm_da_insert(&arr, 3, 50);
    ASSERT_EQ(arr.count, 4);
    ASSERT_EQ(arr.items[3], 50);

    /* remove middle -> 15,30,50 */
    cbm_da_remove(&arr, 1);
    ASSERT_EQ(arr.count, 3);
    ASSERT_EQ(arr.items[0], 15);
    ASSERT_EQ(arr.items[1], 30);
    ASSERT_EQ(arr.items[2], 50);

    cbm_da_free(&arr);
    PASS();
}

TEST(da_push_ptr_growth) {
    /* Verify push_ptr triggers growth correctly */
    CBM_DYN_ARRAY(int) arr = {0};
    for (int i = 0; i < 100; i++) {
        int *p = cbm_da_push_ptr(&arr);
        *p = i * 7;
    }
    ASSERT_EQ(arr.count, 100);
    ASSERT_GTE(arr.cap, 100);
    for (int i = 0; i < 100; i++)
        ASSERT_EQ(arr.items[i], i * 7);
    cbm_da_free(&arr);
    PASS();
}

SUITE(dyn_array) {
    RUN_TEST(da_push_pop);
    RUN_TEST(da_last);
    RUN_TEST(da_clear);
    RUN_TEST(da_free);
    RUN_TEST(da_growth);
    RUN_TEST(da_reserve);
    RUN_TEST(da_struct_type);
    RUN_TEST(da_string_ptrs);
    RUN_TEST(da_remove);
    RUN_TEST(da_insert);
    RUN_TEST(da_push_ptr_struct);
    RUN_TEST(da_insert_at_beginning);
    RUN_TEST(da_insert_at_end);
    RUN_TEST(da_remove_first);
    RUN_TEST(da_remove_last);
    RUN_TEST(da_remove_sequence);
    RUN_TEST(da_reserve_then_push);
    RUN_TEST(da_reserve_zero);
    RUN_TEST(da_reserve_less_than_cap);
    RUN_TEST(da_clear_then_push);
    RUN_TEST(da_free_then_push);
    RUN_TEST(da_large_array_10k);
    RUN_TEST(da_nested_struct_with_ptrs);
    RUN_TEST(da_push_pop_lifo);
    RUN_TEST(da_mixed_operations);
    RUN_TEST(da_push_ptr_growth);
}
