/*
 * test_ast_profile.c — Unit tests for ast_profile.c (serialization/deserialization).
 *
 * Covers: to_str, from_str round-trip, to_vector, edge cases.
 */
#include "test_framework.h"
#include <semantic/ast_profile.h>

#include <string.h>

/* ── Helper ──────────────────────────────────────────────────────── */

static cbm_ast_profile_t make_profile(void) {
    cbm_ast_profile_t p;
    memset(&p, 0, sizeof(p));
    p.if_count = 5;
    p.for_count = 3;
    p.while_count = 1;
    p.switch_count = 2;
    p.try_count = 0;
    p.return_count = 4;
    p.max_nesting_depth = 3;
    p.avg_nesting_depth_x10 = 15;
    p.comparison_ops = 10;
    p.arithmetic_ops = 8;
    p.logical_ops = 2;
    p.assignment_count = 7;
    p.string_literals = 3;
    p.number_literals = 5;
    p.bool_literals = 1;
    p.param_count = 4;
    p.params_in_returns = 2;
    p.params_in_conditions = 1;
    p.variable_reassigns = 3;
    p.unique_operators = 12;
    p.unique_operands = 20;
    p.total_operators = 45;
    p.total_operands = 60;
    p.body_lines = 30;
    p.body_tokens = 200;
    return p;
}

/* ── to_str + from_str round-trip ────────────────────────────────── */

TEST(ast_profile_roundtrip) {
    cbm_ast_profile_t original = make_profile();
    char buf[200];
    cbm_ast_profile_to_str(&original, buf, sizeof(buf));

    cbm_ast_profile_t decoded;
    ASSERT_TRUE(cbm_ast_profile_from_str(buf, &decoded));

    ASSERT_EQ(original.if_count, decoded.if_count);
    ASSERT_EQ(original.for_count, decoded.for_count);
    ASSERT_EQ(original.while_count, decoded.while_count);
    ASSERT_EQ(original.switch_count, decoded.switch_count);
    ASSERT_EQ(original.try_count, decoded.try_count);
    ASSERT_EQ(original.return_count, decoded.return_count);
    ASSERT_EQ(original.max_nesting_depth, decoded.max_nesting_depth);
    ASSERT_EQ(original.avg_nesting_depth_x10, decoded.avg_nesting_depth_x10);
    ASSERT_EQ(original.comparison_ops, decoded.comparison_ops);
    ASSERT_EQ(original.arithmetic_ops, decoded.arithmetic_ops);
    ASSERT_EQ(original.logical_ops, decoded.logical_ops);
    ASSERT_EQ(original.assignment_count, decoded.assignment_count);
    ASSERT_EQ(original.string_literals, decoded.string_literals);
    ASSERT_EQ(original.number_literals, decoded.number_literals);
    ASSERT_EQ(original.bool_literals, decoded.bool_literals);
    ASSERT_EQ(original.param_count, decoded.param_count);
    ASSERT_EQ(original.params_in_returns, decoded.params_in_returns);
    ASSERT_EQ(original.params_in_conditions, decoded.params_in_conditions);
    ASSERT_EQ(original.variable_reassigns, decoded.variable_reassigns);
    ASSERT_EQ(original.unique_operators, decoded.unique_operators);
    ASSERT_EQ(original.unique_operands, decoded.unique_operands);
    ASSERT_EQ(original.total_operators, decoded.total_operators);
    ASSERT_EQ(original.total_operands, decoded.total_operands);
    ASSERT_EQ(original.body_lines, decoded.body_lines);
    ASSERT_EQ(original.body_tokens, decoded.body_tokens);
    PASS();
}

TEST(ast_profile_to_str_null) {
    char buf[200];
    cbm_ast_profile_to_str(NULL, buf, sizeof(buf)); /* should not crash */
    PASS();
}

TEST(ast_profile_to_str_small_buf) {
    cbm_ast_profile_t p = make_profile();
    /* buf too small: should write empty string */
    char buf[1] = {'X'};
    cbm_ast_profile_to_str(&p, buf, 0);
    /* 0-length buffer: function should handle gracefully */
    PASS();
}

TEST(ast_profile_from_str_null) {
    ASSERT_FALSE(cbm_ast_profile_from_str(NULL, NULL));
    PASS();
}

TEST(ast_profile_from_str_invalid) {
    cbm_ast_profile_t out;
    ASSERT_FALSE(cbm_ast_profile_from_str("not,a,valid,string", &out));
    ASSERT_FALSE(cbm_ast_profile_from_str("", &out));
    PASS();
}

TEST(ast_profile_from_str_too_few_fields) {
    cbm_ast_profile_t out;
    /* Only 5 fields instead of 25 */
    ASSERT_FALSE(cbm_ast_profile_from_str("1,2,3,4,5", &out));
    PASS();
}

/* ── to_vector ───────────────────────────────────────────────────── */

TEST(ast_profile_to_vector_range) {
    cbm_ast_profile_t p = make_profile();
    float vec[25];
    cbm_ast_profile_to_vector(&p, vec);
    /* All values should be in [0, 1] range (normalized) */
    for (int i = 0; i < 25; i++) {
        ASSERT_GTE(vec[i], 0.0f);
        ASSERT_LTE(vec[i], 1.0f);
    }
    PASS();
}

TEST(ast_profile_to_vector_zero) {
    cbm_ast_profile_t p;
    memset(&p, 0, sizeof(p));
    float vec[25];
    cbm_ast_profile_to_vector(&p, vec);
    /* All zeros should produce all-zero vector */
    for (int i = 0; i < 25; i++) {
        ASSERT_FLOAT_EQ(vec[i], 0.0f, 0.001f);
    }
    PASS();
}

TEST(ast_profile_to_vector_null) {
    float vec[25];
    memset(vec, 0xFF, sizeof(vec));
    cbm_ast_profile_to_vector(NULL, vec); /* should not crash or write */
    PASS();
}

/* ── to_vector extremes (saturation) ─────────────────────────────── */

TEST(ast_profile_to_vector_saturates) {
    cbm_ast_profile_t p;
    memset(&p, 0, sizeof(p));
    /* count / MAX_COUNT (100) → can exceed 1.0; body_tokens / MAX_TOKENS (2000) = 2.5 */
    p.if_count = 500;
    p.body_tokens = 5000;
    float vec[25];
    cbm_ast_profile_to_vector(&p, vec);
    ASSERT_TRUE(vec[0] > 1.0f);
    ASSERT_TRUE(vec[24] > 1.0f);
    PASS();
}

/* ── Suite ───────────────────────────────────────────────────────── */

SUITE(ast_profile) {
    RUN_TEST(ast_profile_roundtrip);
    RUN_TEST(ast_profile_to_str_null);
    RUN_TEST(ast_profile_to_str_small_buf);
    RUN_TEST(ast_profile_from_str_null);
    RUN_TEST(ast_profile_from_str_invalid);
    RUN_TEST(ast_profile_from_str_too_few_fields);
    RUN_TEST(ast_profile_to_vector_range);
    RUN_TEST(ast_profile_to_vector_zero);
    RUN_TEST(ast_profile_to_vector_null);
    RUN_TEST(ast_profile_to_vector_saturates);
}
