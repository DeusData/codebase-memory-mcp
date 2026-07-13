/*
 * test_semantic.c — Unit tests for semantic.c (pure functions).
 *
 * Covers: tokenize, cosine, normalize, vec_add_scaled, random_index,
 * proximity, diffuse, corpus lifecycle, get_config.
 */
#include "test_framework.h"
#include <semantic/rotsq.h>
#include <semantic/semantic.h>

#include <math.h>
#include <stdlib.h>
#include <string.h>

/* ── Tokenize ────────────────────────────────────────────────────── */

TEST(sem_tokenize_camel) {
    char *tokens[32];
    int n = cbm_sem_tokenize("parseUserInput", tokens, 32);
    ASSERT_GTE(n, 3);
    ASSERT_STR_EQ(tokens[0], "parse");
    ASSERT_STR_EQ(tokens[1], "user");
    ASSERT_STR_EQ(tokens[2], "input");
    for (int i = 0; i < n; i++) free(tokens[i]);
    PASS();
}

TEST(sem_tokenize_snake) {
    char *tokens[32];
    int n = cbm_sem_tokenize("handle_http_request", tokens, 32);
    ASSERT_GTE(n, 3);
    ASSERT_STR_EQ(tokens[0], "handle");
    ASSERT_STR_EQ(tokens[1], "http");
    ASSERT_STR_EQ(tokens[2], "request");
    for (int i = 0; i < n; i++) free(tokens[i]);
    PASS();
}

TEST(sem_tokenize_dot) {
    char *tokens[32];
    int n = cbm_sem_tokenize("net.http.client", tokens, 32);
    ASSERT_GTE(n, 3);
    ASSERT_STR_EQ(tokens[0], "net");
    ASSERT_STR_EQ(tokens[1], "http");
    ASSERT_STR_EQ(tokens[2], "client");
    for (int i = 0; i < n; i++) free(tokens[i]);
    PASS();
}

TEST(sem_tokenize_null) {
    int n = cbm_sem_tokenize(NULL, NULL, 0);
    ASSERT_EQ(n, 0);
    PASS();
}

TEST(sem_tokenize_max_out) {
    char *tokens[3];
    int n = cbm_sem_tokenize("a_b_c_d_e_f_g", tokens, 3);
    ASSERT_EQ(n, 3);
    for (int i = 0; i < n; i++) free(tokens[i]);
    PASS();
}

TEST(sem_tokenize_abbrev_expansion) {
    char *tokens[32];
    int n = cbm_sem_tokenize("getCtxErrMsg", tokens, 32);
    /* get, ctx, context, err, error, msg, message */
    ASSERT_GTE(n, 4);
    bool has_ctx = false, has_context = false, has_err = false, has_error = false;
    for (int i = 0; i < n; i++) {
        if (strcmp(tokens[i], "ctx") == 0) has_ctx = true;
        if (strcmp(tokens[i], "context") == 0) has_context = true;
        if (strcmp(tokens[i], "err") == 0) has_err = true;
        if (strcmp(tokens[i], "error") == 0) has_error = true;
    }
    ASSERT_TRUE(has_ctx && has_context && has_err && has_error);
    for (int i = 0; i < n; i++) free(tokens[i]);
    PASS();
}

/* ── Cosine similarity ───────────────────────────────────────────── */

static void fill_vec(cbm_sem_vec_t *v, float val) {
    for (int i = 0; i < CBM_SEM_DIM; i++) v->v[i] = val;
}

TEST(sem_cosine_identical) {
    cbm_sem_vec_t a, b;
    fill_vec(&a, 0.5f);
    fill_vec(&b, 0.5f);
    float sim = cbm_sem_cosine(&a, &b);
    ASSERT_FLOAT_EQ(sim, 1.0f, 0.001f);
    PASS();
}

TEST(sem_cosine_orthogonal) {
    cbm_sem_vec_t a, b;
    memset(&a, 0, sizeof(a));
    memset(&b, 0, sizeof(b));
    a.v[0] = 1.0f;
    b.v[1] = 1.0f;
    float sim = cbm_sem_cosine(&a, &b);
    ASSERT_FLOAT_EQ(sim, 0.0f, 0.001f);
    PASS();
}

TEST(sem_cosine_zero_vector) {
    cbm_sem_vec_t a, b;
    memset(&a, 0, sizeof(a));
    fill_vec(&b, 1.0f);
    float sim = cbm_sem_cosine(&a, &b);
    ASSERT_FLOAT_EQ(sim, 0.0f, 0.001f);
    PASS();
}

TEST(sem_cosine_negative) {
    cbm_sem_vec_t a, b;
    memset(&a, 0, sizeof(a));
    memset(&b, 0, sizeof(b));
    a.v[0] = 1.0f;
    b.v[0] = -1.0f;
    float sim = cbm_sem_cosine(&a, &b);
    ASSERT_FLOAT_EQ(sim, -1.0f, 0.001f);
    PASS();
}

TEST(sem_cosine_null) {
    ASSERT_FLOAT_EQ(cbm_sem_cosine(NULL, NULL), 0.0f, 0.001f);
    PASS();
}

/* ── Normalize ───────────────────────────────────────────────────── */

TEST(sem_normalize_unit) {
    cbm_sem_vec_t v;
    memset(&v, 0, sizeof(v));
    v.v[0] = 1.0f;
    cbm_sem_normalize(&v);
    ASSERT_FLOAT_EQ(cbm_sem_cosine(&v, &v), 1.0f, 0.001f);
    PASS();
}

TEST(sem_normalize_scales) {
    cbm_sem_vec_t v;
    fill_vec(&v, 2.0f);
    cbm_sem_normalize(&v);
    float mag_sq = 0.0f;
    for (int i = 0; i < CBM_SEM_DIM; i++) mag_sq += v.v[i] * v.v[i];
    float mag = sqrtf(mag_sq);
    ASSERT_FLOAT_EQ(mag, 1.0f, 0.01f);
    PASS();
}

TEST(sem_normalize_zero) {
    cbm_sem_vec_t v;
    memset(&v, 0, sizeof(v));
    cbm_sem_normalize(&v);
    /* Should remain zero (no division by zero) */
    PASS();
}

TEST(sem_normalize_null) {
    cbm_sem_normalize(NULL); /* should not crash */
    PASS();
}

/* ── Vec add scaled ──────────────────────────────────────────────── */

TEST(sem_vec_add_scaled_basic) {
    cbm_sem_vec_t dst;
    memset(&dst, 0, sizeof(dst));
    cbm_sem_vec_t src;
    fill_vec(&src, 1.0f);
    cbm_sem_vec_add_scaled(&dst, &src, 0.5f);
    ASSERT_FLOAT_EQ(dst.v[0], 0.5f, 0.001f);
    ASSERT_FLOAT_EQ(dst.v[CBM_SEM_DIM - 1], 0.5f, 0.001f);
    PASS();
}

TEST(sem_vec_add_scaled_null) {
    cbm_sem_vec_t v;
    fill_vec(&v, 1.0f);
    cbm_sem_vec_add_scaled(NULL, &v, 1.0f);  /* should not crash */
    cbm_sem_vec_add_scaled(&v, NULL, 1.0f);  /* should not crash */
    PASS();
}

/* ── Random index ────────────────────────────────────────────────── */

TEST(sem_random_index_deterministic) {
    cbm_sem_vec_t a, b;
    cbm_sem_random_index("hello", &a);
    cbm_sem_random_index("hello", &b);
    ASSERT_FLOAT_EQ(cbm_sem_cosine(&a, &b), 1.0f, 0.001f);
    PASS();
}

TEST(sem_random_index_different_tokens) {
    cbm_sem_vec_t a, b;
    cbm_sem_random_index("function", &a);
    cbm_sem_random_index("variable", &b);
    /* Different tokens should produce different vectors */
    float sim = cbm_sem_cosine(&a, &b);
    ASSERT_TRUE(sim < 1.0f - 1e-6f);
    PASS();
}

TEST(sem_random_index_null) {
    cbm_sem_vec_t v;
    memset(&v, 0, sizeof(v));
    cbm_sem_random_index(NULL, &v);
    /* Should produce zero vector for NULL token */
    for (int i = 0; i < CBM_SEM_DIM; i++) {
        ASSERT_FLOAT_EQ(v.v[i], 0.0f, 0.001f);
    }
    PASS();
}

/* ── Proximity ───────────────────────────────────────────────────── */

TEST(sem_proximity_same_file) {
    float p = cbm_sem_proximity("src/main.c", "src/main.c");
    ASSERT_FLOAT_EQ(p, 1.1f, 0.01f); /* CBM_SEM_UNIT_POS + CBM_SEM_PROX_MAX_BOOST */
    PASS();
}

TEST(sem_proximity_same_dir) {
    /* Files sharing 1 of 2 directory components: ratio = 0.5 → 1.0 + 0.5*0.10 = 1.05 */
    float p = cbm_sem_proximity("src/core/a.c", "src/io/b.c");
    ASSERT_TRUE(p > 1.0f && p < 1.10f);
    PASS();
}

TEST(sem_proximity_different_paths) {
    float p = cbm_sem_proximity("src/foo/a.c", "tests/bar/b.c");
    ASSERT_FLOAT_EQ(p, 1.0f, 0.01f);
    PASS();
}

TEST(sem_proximity_null) {
    ASSERT_FLOAT_EQ(cbm_sem_proximity(NULL, "foo.c"), 1.0f, 0.01f);
    ASSERT_FLOAT_EQ(cbm_sem_proximity("foo.c", NULL), 1.0f, 0.01f);
    PASS();
}

/* ── Diffuse ─────────────────────────────────────────────────────── */

TEST(sem_diffuse_zero_neighbors) {
    cbm_sem_vec_t v;
    fill_vec(&v, 0.5f);
    cbm_sem_diffuse(&v, NULL, 0, 0.3f);
    /* With zero neighbors, vector should be unchanged */
    ASSERT_FLOAT_EQ(v.v[0], 0.5f, 0.001f);
    PASS();
}

TEST(sem_diffuse_single_neighbor) {
    cbm_sem_vec_t v;
    memset(&v, 0, sizeof(v));
    v.v[0] = 0.5f;
    v.v[1] = 0.5f;
    cbm_sem_normalize(&v); /* unit-length input */
    cbm_sem_vec_t nb;
    memset(&nb, 0, sizeof(nb));
    nb.v[0] = 1.0f;
    cbm_sem_normalize(&nb);
    cbm_sem_diffuse(&v, &nb, 1, 0.3f);
    /* After diffuse+normalize, result should still be unit-length */
    float mag_sq = 0.0f;
    for (int i = 0; i < CBM_SEM_DIM; i++) mag_sq += v.v[i] * v.v[i];
    ASSERT_FLOAT_EQ(sqrtf(mag_sq), 1.0f, 0.01f);
    /* Component 0 should be pulled toward neighbor's strong dim-0 */
    ASSERT_TRUE(v.v[0] > 0.0f);
    PASS();
}

/* ── Corpus lifecycle ────────────────────────────────────────────── */

TEST(sem_corpus_new_free) {
    cbm_sem_corpus_t *c = cbm_sem_corpus_new();
    ASSERT_NOT_NULL(c);
    ASSERT_EQ(cbm_sem_corpus_doc_count(c), 0);
    ASSERT_EQ(cbm_sem_corpus_token_count(c), 0);
    cbm_sem_corpus_free(c);
    PASS();
}

TEST(sem_corpus_add_one_doc) {
    cbm_sem_corpus_t *c = cbm_sem_corpus_new();
    ASSERT_NOT_NULL(c);
    const char *tokens[] = {"parse", "user", "input"};
    cbm_sem_corpus_add_doc(c, tokens, 3);
    ASSERT_EQ(cbm_sem_corpus_doc_count(c), 1);
    ASSERT_TRUE(cbm_sem_corpus_token_count(c) > 0);
    cbm_sem_corpus_free(c);
    PASS();
}

TEST(sem_corpus_idf) {
    cbm_sem_corpus_t *c = cbm_sem_corpus_new();
    ASSERT_NOT_NULL(c);
    const char *doc1[] = {"a", "b", "c"};
    const char *doc2[] = {"a", "d", "e"};
    cbm_sem_corpus_add_doc(c, doc1, 3);
    cbm_sem_corpus_add_doc(c, doc2, 3);
    /* IDF for "a" (appears in 2 docs): log(2/2) = log(1) = 0 */
    float idf_a = cbm_sem_corpus_idf(c, "a");
    ASSERT_TRUE(idf_a < 0.01f);
    /* IDF for "b" (appears in 1 doc): log(2/1) > 0 */
    float idf_b = cbm_sem_corpus_idf(c, "b");
    ASSERT_TRUE(idf_b > 0.0f);
    cbm_sem_corpus_free(c);
    PASS();
}

TEST(sem_corpus_add_null_doc) {
    cbm_sem_corpus_t *c = cbm_sem_corpus_new();
    ASSERT_NOT_NULL(c);
    cbm_sem_corpus_add_doc(c, NULL, 0);
    cbm_sem_corpus_add_doc(c, NULL, -1);
    ASSERT_EQ(cbm_sem_corpus_doc_count(c), 0);
    cbm_sem_corpus_free(c);
    PASS();
}

TEST(sem_corpus_free_null) {
    cbm_sem_corpus_free(NULL); /* should not crash */
    PASS();
}

/* ── Config ──────────────────────────────────────────────────────── */

TEST(sem_get_config_defaults) {
    cbm_sem_config_t cfg = cbm_sem_get_config();
    ASSERT_TRUE(cfg.w_tfidf > 0.0f);
    ASSERT_TRUE(cfg.w_ri > 0.0f);
    ASSERT_TRUE(cfg.threshold > 0.0f);
    ASSERT_TRUE(cfg.max_edges > 0);
    PASS();
}

/* ── RaBitQ estimator quality (from-paper 4-bit quantization) ────── */

/* Deterministic pseudo-random unit vectors; validates that the quantized
 * inner-product estimator tracks the exact float IP within tight bounds.
 * These bounds gate the semantic pass's use of the codes: cosine scores are
 * thresholded at ~0.75, so the estimator error must be well under the
 * decision margin for typical pairs. */
TEST(sem_rotsq_ip_error_bounds) {
    enum { N = 64 };
    static float vecs[N][CBM_RSQ_IN_DIM];
    static cbm_rsq_code_t codes[N];
    uint32_t state = 0xC0FFEEu;
    for (int i = 0; i < N; i++) {
        double norm = 0.0;
        for (int d = 0; d < CBM_RSQ_IN_DIM; d++) {
            /* xorshift32 → roughly uniform in [-1, 1] */
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            vecs[i][d] = ((float)(state & 0xFFFFFF) / (float)0x7FFFFF) - 1.0F;
            norm += (double)vecs[i][d] * (double)vecs[i][d];
        }
        float inv = norm > 0.0 ? (float)(1.0 / sqrt(norm)) : 0.0F;
        for (int d = 0; d < CBM_RSQ_IN_DIM; d++) {
            vecs[i][d] *= inv;
        }
        cbm_rsq_encode(vecs[i], &codes[i]);
    }
    double max_err = 0.0;
    double sum_err = 0.0;
    int pairs = 0;
    for (int i = 0; i < N; i++) {
        for (int j = i; j < N; j++) {
            double exact = 0.0;
            for (int d = 0; d < CBM_RSQ_IN_DIM; d++) {
                exact += (double)vecs[i][d] * (double)vecs[j][d];
            }
            double est = (double)cbm_rsq_ip(&codes[i], &codes[j]);
            double err = fabs(est - exact);
            sum_err += err;
            if (err > max_err) {
                max_err = err;
            }
            pairs++;
        }
    }
    double mean_err = sum_err / pairs;
    /* Self-IP of a unit vector must estimate ~1. */
    double self_est = (double)cbm_rsq_ip(&codes[0], &codes[0]);
    ASSERT_TRUE(fabs(self_est - 1.0) < 0.05);
    /* 4-bit RaBitQ-style SQ after rotation: expect mean error well under 1%
     * of the unit scale and max under ~4% — comfortably inside the semantic
     * threshold's decision margin. */
    ASSERT_TRUE(mean_err < 0.01);
    ASSERT_TRUE(max_err < 0.04);
    PASS();
}

/* ── Suite ───────────────────────────────────────────────────────── */

SUITE(semantic) {
    RUN_TEST(sem_rotsq_ip_error_bounds);
    RUN_TEST(sem_tokenize_camel);
    RUN_TEST(sem_tokenize_snake);
    RUN_TEST(sem_tokenize_dot);
    RUN_TEST(sem_tokenize_null);
    RUN_TEST(sem_tokenize_max_out);
    RUN_TEST(sem_tokenize_abbrev_expansion);
    RUN_TEST(sem_cosine_identical);
    RUN_TEST(sem_cosine_orthogonal);
    RUN_TEST(sem_cosine_zero_vector);
    RUN_TEST(sem_cosine_negative);
    RUN_TEST(sem_cosine_null);
    RUN_TEST(sem_normalize_unit);
    RUN_TEST(sem_normalize_scales);
    RUN_TEST(sem_normalize_zero);
    RUN_TEST(sem_normalize_null);
    RUN_TEST(sem_vec_add_scaled_basic);
    RUN_TEST(sem_vec_add_scaled_null);
    RUN_TEST(sem_random_index_deterministic);
    RUN_TEST(sem_random_index_different_tokens);
    RUN_TEST(sem_random_index_null);
    RUN_TEST(sem_proximity_same_file);
    RUN_TEST(sem_proximity_same_dir);
    RUN_TEST(sem_proximity_different_paths);
    RUN_TEST(sem_proximity_null);
    RUN_TEST(sem_diffuse_zero_neighbors);
    RUN_TEST(sem_diffuse_single_neighbor);
    RUN_TEST(sem_corpus_new_free);
    RUN_TEST(sem_corpus_add_one_doc);
    RUN_TEST(sem_corpus_idf);
    RUN_TEST(sem_corpus_add_null_doc);
    RUN_TEST(sem_corpus_free_null);
    RUN_TEST(sem_get_config_defaults);
}
