/*
 * test_dump_verify.c — Post-dump plausibility gate (#334).
 *
 * Pure-function matrix mirrors sast-ai-app checkSilentDegradation cases.
 * I/O-level coverage that drives the gate against a real on-disk SQLite store
 * lives in test_dump_verify_io.c (store-linked, excluded from test-foundation).
 */
#include "../src/foundation/compat.h"
#include "../src/foundation/dump_verify.h"
#include "test_framework.h"

#include <stdlib.h>

TEST(dump_verify_no_baseline) {
    ASSERT_FALSE(cbm_dump_verify_is_degraded(-1, 500, 0.5, CBM_DUMP_VERIFY_MIN_FLOOR));
    PASS();
}

TEST(dump_verify_sparse_at_floor) {
    ASSERT_FALSE(cbm_dump_verify_is_degraded(50, 10, 0.5, CBM_DUMP_VERIFY_MIN_FLOOR));
    ASSERT_FALSE(cbm_dump_verify_is_degraded(12, 5, 0.5, CBM_DUMP_VERIFY_MIN_FLOOR));
    PASS();
}

TEST(dump_verify_shortfall_below_ratio) {
    ASSERT_TRUE(cbm_dump_verify_is_degraded(1000, 400, 0.5, CBM_DUMP_VERIFY_MIN_FLOOR));
    PASS();
}

TEST(dump_verify_just_above_ratio) {
    ASSERT_FALSE(cbm_dump_verify_is_degraded(1000, 500, 0.5, CBM_DUMP_VERIFY_MIN_FLOOR));
    PASS();
}

TEST(dump_verify_just_below_ratio) {
    ASSERT_TRUE(cbm_dump_verify_is_degraded(1000, 499, 0.5, CBM_DUMP_VERIFY_MIN_FLOOR));
    PASS();
}

TEST(dump_verify_zero_persisted) {
    ASSERT_TRUE(cbm_dump_verify_is_degraded(1000, 0, 0.5, CBM_DUMP_VERIFY_MIN_FLOOR));
    PASS();
}

TEST(dump_verify_growth) {
    ASSERT_FALSE(cbm_dump_verify_is_degraded(500, 750, 0.5, CBM_DUMP_VERIFY_MIN_FLOOR));
    PASS();
}

TEST(dump_verify_count_error) {
    ASSERT_TRUE(cbm_dump_verify_is_degraded(1000, -1, 0.5, CBM_DUMP_VERIFY_MIN_FLOOR));
    PASS();
}

TEST(dump_verify_ratio_zero_disables) {
    ASSERT_FALSE(cbm_dump_verify_is_degraded(1000, 10, 0.0, CBM_DUMP_VERIFY_MIN_FLOOR));
    PASS();
}

TEST(dump_verify_loosened_ratio) {
    ASSERT_FALSE(cbm_dump_verify_is_degraded(1000, 600, 0.5, CBM_DUMP_VERIFY_MIN_FLOOR));
    PASS();
}

TEST(dump_verify_tightened_ratio) {
    ASSERT_TRUE(cbm_dump_verify_is_degraded(1000, 900, 0.95, CBM_DUMP_VERIFY_MIN_FLOOR));
    PASS();
}

TEST(dump_verify_edges_shrank_nodes_ok) {
    /* Edges are not gated; this documents nodes-only semantics for integrators. */
    ASSERT_FALSE(cbm_dump_verify_is_degraded(200, 200, 0.5, CBM_DUMP_VERIFY_MIN_FLOOR));
    PASS();
}

SUITE(dump_verify) {
    RUN_TEST(dump_verify_no_baseline);
    RUN_TEST(dump_verify_sparse_at_floor);
    RUN_TEST(dump_verify_shortfall_below_ratio);
    RUN_TEST(dump_verify_just_above_ratio);
    RUN_TEST(dump_verify_just_below_ratio);
    RUN_TEST(dump_verify_zero_persisted);
    RUN_TEST(dump_verify_growth);
    RUN_TEST(dump_verify_count_error);
    RUN_TEST(dump_verify_ratio_zero_disables);
    RUN_TEST(dump_verify_loosened_ratio);
    RUN_TEST(dump_verify_tightened_ratio);
    RUN_TEST(dump_verify_edges_shrank_nodes_ok);
}
