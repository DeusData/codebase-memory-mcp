/*
 * test_semantic.c — Unit tests for semantic.c (pure functions).
 *
 * Covers: tokenize, cosine, normalize, vec_add_scaled, random_index,
 * proximity, diffuse, corpus lifecycle, get_config.
 */
#include "test_framework.h"
#include "../src/foundation/compat.h"
#include "../src/foundation/compat_thread.h"
#include <semantic/rotsq.h>
#include <semantic/semantic.h>

#include <math.h>
#include <stdatomic.h>
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
    for (int i = 0; i < n; i++)
        free(tokens[i]);
    PASS();
}

TEST(sem_tokenize_snake) {
    char *tokens[32];
    int n = cbm_sem_tokenize("handle_http_request", tokens, 32);
    ASSERT_GTE(n, 3);
    ASSERT_STR_EQ(tokens[0], "handle");
    ASSERT_STR_EQ(tokens[1], "http");
    ASSERT_STR_EQ(tokens[2], "request");
    for (int i = 0; i < n; i++)
        free(tokens[i]);
    PASS();
}

TEST(sem_tokenize_dot) {
    char *tokens[32];
    int n = cbm_sem_tokenize("net.http.client", tokens, 32);
    ASSERT_GTE(n, 3);
    ASSERT_STR_EQ(tokens[0], "net");
    ASSERT_STR_EQ(tokens[1], "http");
    ASSERT_STR_EQ(tokens[2], "client");
    for (int i = 0; i < n; i++)
        free(tokens[i]);
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
    for (int i = 0; i < n; i++)
        free(tokens[i]);
    PASS();
}

TEST(sem_tokenize_abbrev_expansion) {
    char *tokens[32];
    int n = cbm_sem_tokenize("getCtxErrMsg", tokens, 32);
    /* get, ctx, context, err, error, msg, message */
    ASSERT_GTE(n, 4);
    bool has_ctx = false, has_context = false, has_err = false, has_error = false;
    for (int i = 0; i < n; i++) {
        if (strcmp(tokens[i], "ctx") == 0)
            has_ctx = true;
        if (strcmp(tokens[i], "context") == 0)
            has_context = true;
        if (strcmp(tokens[i], "err") == 0)
            has_err = true;
        if (strcmp(tokens[i], "error") == 0)
            has_error = true;
    }
    ASSERT_TRUE(has_ctx && has_context && has_err && has_error);
    for (int i = 0; i < n; i++)
        free(tokens[i]);
    PASS();
}

/* ── Cosine similarity ───────────────────────────────────────────── */

static void fill_vec(cbm_sem_vec_t *v, float val) {
    for (int i = 0; i < CBM_SEM_DIM; i++)
        v->v[i] = val;
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
    for (int i = 0; i < CBM_SEM_DIM; i++)
        mag_sq += v.v[i] * v.v[i];
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
    cbm_sem_vec_add_scaled(NULL, &v, 1.0f); /* should not crash */
    cbm_sem_vec_add_scaled(&v, NULL, 1.0f); /* should not crash */
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
    for (int i = 0; i < CBM_SEM_DIM; i++)
        mag_sq += v.v[i] * v.v[i];
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

/* The vector build resolves each token once and reads idf and vector by index
 * (pass_semantic_edges.c): the index accessors must give the same answers as
 * the name-keyed ones, before and after finalize, and agree on unknown tokens. */
TEST(sem_corpus_index_accessors_match_name_lookups) {
    cbm_sem_corpus_t *c = cbm_sem_corpus_new();
    ASSERT_NOT_NULL(c);
    const char *doc1[] = {"alpha", "beta", "gamma"};
    const char *doc2[] = {"alpha", "delta", "beta"};
    const char *doc3[] = {"epsilon", "alpha"};
    cbm_sem_corpus_add_doc(c, doc1, 3);
    cbm_sem_corpus_add_doc(c, doc2, 3);
    cbm_sem_corpus_add_doc(c, doc3, 2);
    static const char *const probe[] = {"alpha", "beta", "gamma", "delta", "epsilon", "absent"};
    for (int round = 0; round < 2; round++) {
        for (size_t i = 0; i < sizeof(probe) / sizeof(probe[0]); i++) {
            int idx = cbm_sem_corpus_token_index(c, probe[i]);
            ASSERT_FLOAT_EQ(cbm_sem_corpus_idf_at(c, idx), cbm_sem_corpus_idf(c, probe[i]), 0.0);
            ASSERT_TRUE(cbm_sem_corpus_ri_vec_at(c, idx) == cbm_sem_corpus_ri_vec(c, probe[i]));
        }
        ASSERT_EQ(cbm_sem_corpus_token_index(c, "absent"), -1);
        ASSERT_NULL(cbm_sem_corpus_ri_vec_at(c, -1));
        cbm_sem_corpus_finalize(c); /* second round: the finalized corpus */
    }
    cbm_sem_corpus_free(c);
    PASS();
}

/* ── TF-IDF terms and the combined score ─────────────────────────── */

static int sem_terms_of(const cbm_sem_corpus_t *c, char **tokens, int n, cbm_sem_func_t *f,
                        int *indices, float *weights) {
    memset(f, 0, sizeof(*f));
    f->tfidf_indices = indices;
    f->tfidf_weights = weights;
    f->tfidf_len = cbm_sem_tfidf_terms(c, tokens, NULL, n, indices, weights);
    return f->tfidf_len;
}

/* Two functions share a TF-IDF term only when they share the word. Until
 * 2026-10 the term key was the token's POSITION: any two functions of equal
 * length matched on every term, so the signal measured length (a held-out
 * judged sample: admitted-edge precision 0.713 -> 0.742 with the fix). */
TEST(sem_tfidf_terms_compare_vocabulary_not_positions) {
    cbm_sem_corpus_t *c = cbm_sem_corpus_new();
    ASSERT_NOT_NULL(c);
    static char *parse[] = {"parse", "header", "value"};
    static char *render[] = {"render", "widget", "frame"};
    static char *reparse[] = {"value", "parse", "header", "parse", "absent"};
    static char *other[] = {"queue", "drain"};
    cbm_sem_corpus_add_doc(c, (const char **)parse, 3);
    cbm_sem_corpus_add_doc(c, (const char **)render, 3);
    cbm_sem_corpus_add_doc(c, (const char **)reparse, 4); /* "absent" stays out */
    cbm_sem_corpus_add_doc(c, (const char **)other, 2);
    cbm_sem_corpus_finalize(c);

    cbm_sem_func_t fa;
    cbm_sem_func_t fb;
    cbm_sem_func_t fc;
    int ia[3];
    int ib[3];
    int ic[5];
    float wa[3];
    float wb[3];
    float wc[5];
    ASSERT_EQ(sem_terms_of(c, parse, 3, &fa, ia, wa), 3);
    ASSERT_EQ(sem_terms_of(c, render, 3, &fb, ib, wb), 3);
    /* A repeated word is one term, its weight twice the idf; an unknown word
     * is no term; terms ascend by token index. */
    ASSERT_EQ(sem_terms_of(c, reparse, 5, &fc, ic, wc), 3);
    ASSERT_TRUE(ic[0] < ic[1] && ic[1] < ic[2]);
    int at_parse = ic[0] == cbm_sem_corpus_token_index(c, "parse")   ? 0
                   : ic[1] == cbm_sem_corpus_token_index(c, "parse") ? 1
                                                                     : 2;
    ASSERT_EQ(ic[at_parse], cbm_sem_corpus_token_index(c, "parse"));
    ASSERT_FLOAT_EQ(wc[at_parse], 2.0F * cbm_sem_corpus_idf(c, "parse"), 1e-6);

    cbm_sem_signals_t s;
    cbm_sem_signal_values(&fa, &fb, &s);
    ASSERT_FLOAT_EQ(s.tfidf, 0.0F, 1e-6); /* same length, no shared word */
    cbm_sem_signal_values(&fa, &fc, &s);
    ASSERT_TRUE(s.tfidf > 0.9F); /* the same words, other order and length */
    cbm_sem_corpus_free(c);
    PASS();
}

/* cbm_sem_combine is the weighted sum of the signals, times the proximity
 * multiplier, clamped to [0, 1]; a SIMILAR_TO near copy scores 0. And
 * cbm_sem_combined_score is exactly that, applied to cbm_sem_signal_values. */
TEST(sem_combine_weighs_signals) {
    cbm_sem_config_t cfg = cbm_sem_get_config();
    cbm_sem_signals_t s = {.tfidf = 0.5F, .ri = 0.25F, .proximity = 1.0F};
    ASSERT_FLOAT_EQ(cbm_sem_combine(&s, &cfg), cfg.w_tfidf * 0.5F + cfg.w_ri * 0.25F, 1e-6);
    s.proximity = 1.10F;
    ASSERT_FLOAT_EQ(cbm_sem_combine(&s, &cfg), (cfg.w_tfidf * 0.5F + cfg.w_ri * 0.25F) * 1.10F,
                    1e-6);
    cbm_sem_signals_t all = {.tfidf = 1.0F,
                             .ri = 1.0F,
                             .minhash = 1.0F,
                             .api = 1.0F,
                             .type = 1.0F,
                             .decorator = 1.0F,
                             .struct_profile = 1.0F,
                             .proximity = 1.10F};
    ASSERT_FLOAT_EQ(cbm_sem_combine(&all, &cfg), 1.0F, 0.0); /* clamped */
    all.near_copy = true;
    ASSERT_FLOAT_EQ(cbm_sem_combine(&all, &cfg), 0.0F, 0.0);
    ASSERT_FLOAT_EQ(cbm_sem_combine(NULL, &cfg), 0.0F, 0.0);

    cbm_sem_corpus_t *c = cbm_sem_corpus_new();
    ASSERT_NOT_NULL(c);
    static char *one[] = {"open", "socket", "port"};
    static char *two[] = {"open", "socket", "host", "retry"};
    cbm_sem_corpus_add_doc(c, (const char **)one, 3);
    cbm_sem_corpus_add_doc(c, (const char **)two, 4);
    cbm_sem_corpus_finalize(c);
    cbm_sem_func_t fa;
    cbm_sem_func_t fb;
    int ia[3];
    int ib[4];
    float wa[3];
    float wb[4];
    sem_terms_of(c, one, 3, &fa, ia, wa);
    sem_terms_of(c, two, 4, &fb, ib, wb);
    fa.file_path = "net/dial.go";
    fb.file_path = "net/listen.go";
    cbm_sem_signals_t v;
    cbm_sem_signal_values(&fa, &fb, &v);
    ASSERT_FLOAT_EQ(cbm_sem_combined_score(&fa, &fb, &cfg), cbm_sem_combine(&v, &cfg), 0.0);
    cbm_sem_corpus_free(c);
    PASS();
}

/* Over budget, a function keeps its BEST partners, not its first ones (a
 * held-out judged sample: the pairs first-come admitted over budget were 0 of
 * 17 related). Equal scores keep the canonical order; an ineligible pair is
 * never admitted and takes no budget. */
TEST(sem_admit_best_first_keeps_the_best) {
    /* function 0 has four partners in canonical order, budget 2 */
    const float scores[] = {0.80F, 0.90F, 0.95F, 0.99F, 0.90F};
    const int fa[] = {0, 0, 0, 0, 5};
    const int fb[] = {1, 2, 3, 4, 6};
    const bool eligible[] = {true, true, true, false, true};
    int counts[7] = {0};
    bool admitted[5];
    ASSERT_TRUE(cbm_sem_admit_best_first(scores, fa, fb, eligible, 5, 2, counts, admitted));
    ASSERT_FALSE(admitted[0]); /* first-come would have kept 0.80 */
    ASSERT_TRUE(admitted[1]);
    ASSERT_TRUE(admitted[2]);
    ASSERT_FALSE(admitted[3]); /* the best score, but recorded only */
    ASSERT_TRUE(admitted[4]);
    ASSERT_EQ(counts[0], 2);
    ASSERT_EQ(counts[4], 0);

    /* ties: the earlier pair in canonical order wins the last slot */
    const float tied[] = {0.85F, 0.85F};
    const int ta[] = {0, 0};
    const int tb[] = {1, 2};
    const bool all[] = {true, true};
    int tcounts[3] = {0};
    bool tadmitted[2];
    ASSERT_TRUE(cbm_sem_admit_best_first(tied, ta, tb, all, 2, 1, tcounts, tadmitted));
    ASSERT_TRUE(tadmitted[0]);
    ASSERT_FALSE(tadmitted[1]);
    PASS();
}

/* p by the stored (three-decimal) score: the judged bands, monotone. */
TEST(sem_calibrated_p_bands) {
    ASSERT_FLOAT_EQ(cbm_sem_calibrated_p(0.750F), 0.626F, 0.0);
    ASSERT_FLOAT_EQ(cbm_sem_calibrated_p(0.772F), 0.626F, 0.0);
    ASSERT_FLOAT_EQ(cbm_sem_calibrated_p(0.773F), 0.760F, 0.0);
    /* the cut sits at the stored precision (0.773), not the raw quartile:
     * a stored "0.773" and a stored "0.772" never share a band */
    ASSERT_FLOAT_EQ(cbm_sem_calibrated_p(0.7729F), 0.626F, 0.0);
    ASSERT_FLOAT_EQ(cbm_sem_calibrated_p(0.838F), 0.760F, 0.0);
    ASSERT_FLOAT_EQ(cbm_sem_calibrated_p(1.000F), 0.760F, 0.0);
    float last = 0.0F;
    for (int milli = 750; milli <= 1000; milli++) {
        float p = cbm_sem_calibrated_p((float)milli / 1000.0F);
        ASSERT_TRUE(p >= last);
        last = p;
    }
    PASS();
}

static float docp(cbm_sem_doc_format_t fmt, cbm_sem_doc_kind_t kind, cbm_sem_doc_pos_t pos,
                  float score) {
    return cbm_sem_doc_calibrated_p(fmt, kind, pos, score);
}

/* Doc -> code candidates, Markdown (sample 7): p by kind and position --
 * functions outside the doc's home never stored, inside it from 0.30, for a
 * doc of the whole project (samples 4 and 7 pooled) from 0.30 too; local
 * extras 0.32 inside the home
 * only; whole files from 0.40; nothing below CBM_SEM_DOC_MIN_SCORE; every
 * curve monotone. A format without a judged curve stores nothing. */
TEST(sem_doc_calibrated_p_bands) {
    const cbm_sem_doc_format_t md = CBM_SEM_DOC_FMT_MARKDOWN;
    const cbm_sem_doc_kind_t fn = CBM_SEM_DOC_KIND_FUNCTION;
    const cbm_sem_doc_pos_t global = CBM_SEM_DOC_POS_GLOBAL;
    const cbm_sem_doc_pos_t local = CBM_SEM_DOC_POS_LOCAL;
    const cbm_sem_doc_pos_t outside = CBM_SEM_DOC_POS_OUTSIDE;
    ASSERT_FLOAT_EQ(docp(md, fn, local, 0.199F), 0.0F, 0.0);
    ASSERT_FLOAT_EQ(docp(md, fn, local, CBM_SEM_DOC_MIN_SCORE), 0.0F, 0.0);
    ASSERT_FLOAT_EQ(docp(md, fn, local, 0.30F), 0.325F, 0.0);
    ASSERT_FLOAT_EQ(docp(md, fn, local, 0.40F), 0.525F, 0.0);
    ASSERT_FLOAT_EQ(docp(md, fn, global, 0.299F), 0.0F, 0.0);
    ASSERT_FLOAT_EQ(docp(md, fn, global, 0.30F), 0.217F, 0.0);
    ASSERT_FLOAT_EQ(docp(md, fn, global, 0.40F), 0.509F, 0.0);
    ASSERT_FLOAT_EQ(docp(md, fn, outside, 0.90F), 0.0F, 0.0);
    ASSERT_FLOAT_EQ(docp(md, CBM_SEM_DOC_KIND_LOCAL, local, 0.199F), 0.0F, 0.0);
    ASSERT_FLOAT_EQ(docp(md, CBM_SEM_DOC_KIND_LOCAL, local, 0.20F), 0.317F, 0.0);
    ASSERT_FLOAT_EQ(docp(md, CBM_SEM_DOC_KIND_LOCAL, local, 0.90F), 0.317F, 0.0);
    ASSERT_FLOAT_EQ(docp(md, CBM_SEM_DOC_KIND_FILE, outside, 0.399F), 0.0F, 0.0);
    ASSERT_FLOAT_EQ(docp(md, CBM_SEM_DOC_KIND_FILE, outside, 0.40F), 0.30F, 0.0);
    ASSERT_FLOAT_EQ(docp(md, CBM_SEM_DOC_KIND_FOLDER, local, 0.90F), 0.0F, 0.0);
    static const cbm_sem_doc_kind_t KINDS[] = {CBM_SEM_DOC_KIND_FUNCTION, CBM_SEM_DOC_KIND_LOCAL,
                                               CBM_SEM_DOC_KIND_FILE};
    for (size_t k = 0; k < sizeof(KINDS) / sizeof(KINDS[0]); k++) {
        for (int pos = 0; pos < CBM_SEM_DOC_POS_COUNT; pos++) {
            float last = 0.0F;
            for (int milli = 0; milli <= 1000; milli++) {
                float s = (float)milli / 1000.0F;
                float p = docp(md, KINDS[k], (cbm_sem_doc_pos_t)pos, s);
                ASSERT_TRUE(p >= last);
                last = p;
                ASSERT_TRUE(pos != CBM_SEM_DOC_POS_OUTSIDE || KINDS[k] == CBM_SEM_DOC_KIND_FILE ||
                            p == 0.0F);
                ASSERT_FLOAT_EQ(docp(CBM_SEM_DOC_FMT_OTHER, KINDS[k], (cbm_sem_doc_pos_t)pos, s),
                                0.0F, 0.0);
                ASSERT_FLOAT_EQ(docp(CBM_SEM_DOC_FMT_PDF, KINDS[k], (cbm_sem_doc_pos_t)pos, s),
                                0.0F, 0.0);
            }
        }
    }
    ASSERT_FLOAT_EQ(docp(CBM_SEM_DOC_FMT_COUNT, fn, global, 0.9F), 0.0F, 0.0);
    /* a home folder: README and index docs only, Markdown only */
    ASSERT_FLOAT_EQ(cbm_sem_doc_folder_p(md, "pkg/mail/README.md"), 0.867F, 0.0);
    ASSERT_FLOAT_EQ(cbm_sem_doc_folder_p(md, "pkg/mail/docs/index.mdx"), 0.867F, 0.0);
    ASSERT_FLOAT_EQ(cbm_sem_doc_folder_p(md, "pkg/mail/readme"), 0.867F, 0.0);
    ASSERT_FLOAT_EQ(cbm_sem_doc_folder_p(md, "pkg/mail/guide.md"), 0.0F, 0.0);
    ASSERT_FLOAT_EQ(cbm_sem_doc_folder_p(md, "pkg/mail/README-old.md"), 0.0F, 0.0);
    ASSERT_FLOAT_EQ(cbm_sem_doc_folder_p(CBM_SEM_DOC_FMT_RST, "pkg/mail/index.rst"), 0.0F, 0.0);
    ASSERT_FLOAT_EQ(cbm_sem_doc_folder_p(md, NULL), 0.0F, 0.0);
    /* reST (sample 6): 0.20-0.30 judged 0.125, not stored */
    const cbm_sem_doc_format_t rst = CBM_SEM_DOC_FMT_RST;
    ASSERT_FLOAT_EQ(
        cbm_sem_doc_calibrated_p(rst, CBM_SEM_DOC_KIND_FUNCTION, CBM_SEM_DOC_POS_GLOBAL, 0.25F),
        0.0F, 0.0);
    ASSERT_FLOAT_EQ(
        cbm_sem_doc_calibrated_p(rst, CBM_SEM_DOC_KIND_FUNCTION, CBM_SEM_DOC_POS_GLOBAL, 0.30F),
        0.35F, 0.0);
    ASSERT_FLOAT_EQ(
        cbm_sem_doc_calibrated_p(rst, CBM_SEM_DOC_KIND_FUNCTION, CBM_SEM_DOC_POS_GLOBAL, 0.40F),
        0.625F, 0.0);
    /* AsciiDoc (sample 6): every band at or above the floor is stored */
    const cbm_sem_doc_format_t adoc = CBM_SEM_DOC_FMT_ADOC;
    ASSERT_FLOAT_EQ(
        cbm_sem_doc_calibrated_p(adoc, CBM_SEM_DOC_KIND_FUNCTION, CBM_SEM_DOC_POS_GLOBAL, 0.199F),
        0.0F, 0.0);
    ASSERT_FLOAT_EQ(
        cbm_sem_doc_calibrated_p(adoc, CBM_SEM_DOC_KIND_FUNCTION, CBM_SEM_DOC_POS_GLOBAL, 0.20F),
        0.225F, 0.0);
    ASSERT_FLOAT_EQ(
        cbm_sem_doc_calibrated_p(adoc, CBM_SEM_DOC_KIND_FUNCTION, CBM_SEM_DOC_POS_GLOBAL, 0.35F),
        0.25F, 0.0);
    ASSERT_FLOAT_EQ(
        cbm_sem_doc_calibrated_p(adoc, CBM_SEM_DOC_KIND_FUNCTION, CBM_SEM_DOC_POS_GLOBAL, 0.90F),
        0.50F, 0.0);
    for (int milli = 0; milli <= 1000; milli++) {
        float s = (float)milli / 1000.0F;
        ASSERT_TRUE(
            cbm_sem_doc_calibrated_p(rst, CBM_SEM_DOC_KIND_FUNCTION, CBM_SEM_DOC_POS_GLOBAL, s) >=
            cbm_sem_doc_calibrated_p(rst, CBM_SEM_DOC_KIND_FUNCTION, CBM_SEM_DOC_POS_GLOBAL,
                                     s - 0.001F));
        ASSERT_TRUE(
            cbm_sem_doc_calibrated_p(adoc, CBM_SEM_DOC_KIND_FUNCTION, CBM_SEM_DOC_POS_GLOBAL, s) >=
            cbm_sem_doc_calibrated_p(adoc, CBM_SEM_DOC_KIND_FUNCTION, CBM_SEM_DOC_POS_GLOBAL,
                                     s - 0.001F));
    }
    PASS();
}

/* p is stored and shown at two decimals, half up, whatever the float
 * representation of the judged value (0.225F is 0.22499999...). */
TEST(sem_p_two_decimals_half_up) {
    ASSERT_FLOAT_EQ(cbm_sem_p_2dp(0.225F), 0.23, 1e-12);
    ASSERT_FLOAT_EQ(cbm_sem_p_2dp(0.325F), 0.33, 1e-12); /* float 0.32499998... */
    ASSERT_FLOAT_EQ(cbm_sem_p_2dp(0.525F), 0.53, 1e-12); /* float 0.52499997... */
    ASSERT_FLOAT_EQ(cbm_sem_p_2dp(0.324F), 0.32, 1e-12);
    ASSERT_FLOAT_EQ(cbm_sem_p_2dp(0.317F), 0.32, 1e-12);
    ASSERT_FLOAT_EQ(cbm_sem_p_2dp(0.867F), 0.87, 1e-12);
    ASSERT_FLOAT_EQ(cbm_sem_p_2dp(0.625F), 0.63, 1e-12);
    ASSERT_FLOAT_EQ(cbm_sem_p_2dp(0.548F), 0.55, 1e-12);
    ASSERT_FLOAT_EQ(cbm_sem_p_2dp(0.36F), 0.36, 1e-12);
    ASSERT_FLOAT_EQ(cbm_sem_p_2dp(0.0F), 0.0, 1e-12);
    PASS();
}

/* A section's format comes from its file's extension, case-insensitively;
 * a .txt section is reStructuredText (plain text has no sections). */
TEST(sem_doc_format_by_extension) {
    ASSERT_EQ(cbm_sem_doc_format("docs/guide.md"), CBM_SEM_DOC_FMT_MARKDOWN);
    ASSERT_EQ(cbm_sem_doc_format("README.MARKDOWN"), CBM_SEM_DOC_FMT_MARKDOWN);
    ASSERT_EQ(cbm_sem_doc_format("docs/index.rst"), CBM_SEM_DOC_FMT_RST);
    ASSERT_EQ(cbm_sem_doc_format("docs/topics/db.txt"), CBM_SEM_DOC_FMT_RST);
    ASSERT_EQ(cbm_sem_doc_format("manual/intro.adoc"), CBM_SEM_DOC_FMT_ADOC);
    ASSERT_EQ(cbm_sem_doc_format("paper.pdf"), CBM_SEM_DOC_FMT_PDF);
    ASSERT_EQ(cbm_sem_doc_format("src/main.go"), CBM_SEM_DOC_FMT_OTHER);
    ASSERT_EQ(cbm_sem_doc_format("v1.2/NOTES"), CBM_SEM_DOC_FMT_OTHER);
    ASSERT_EQ(cbm_sem_doc_format(NULL), CBM_SEM_DOC_FMT_OTHER);
    PASS();
}

/* A properties string holding a quote is read whole, and escapes are not
 * glued to the next word (until 2026-10 the read stopped at the first quote
 * and kept "\n" raw, so "\nreturn" became the token "nreturn"). */
TEST(sem_json_str_honours_escapes) {
    static const char json[] =
        "{\"docstring\":\"Say \\\"hi\\\" to\\nthe user \\u00e9 now\",\"bt\":\"x\"}";
    char buf[64];
    ASSERT_NOT_NULL(cbm_sem_json_str(json, "docstring", buf, sizeof(buf)));
    ASSERT_STR_EQ(buf, "Say \"hi\" to the user   now");
    ASSERT_NOT_NULL(cbm_sem_json_str(json, "bt", buf, sizeof(buf)));
    ASSERT_STR_EQ(buf, "x");
    ASSERT_NULL(cbm_sem_json_str(json, "absent", buf, sizeof(buf)));
    char small[6];
    ASSERT_NOT_NULL(cbm_sem_json_str(json, "docstring", small, sizeof(small)));
    ASSERT_STR_EQ(small, "Say \""); /* cut at the buffer, still terminated */
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

typedef struct {
    atomic_int *ready;
    atomic_int *start;
    float value;
    cbm_rsq_code_t code;
} rotsq_thread_ctx_t;

static void *rotsq_concurrent_first_encode(void *opaque) {
    rotsq_thread_ctx_t *ctx = opaque;
    float vec[CBM_RSQ_IN_DIM];
    for (int i = 0; i < CBM_RSQ_IN_DIM; i++) {
        vec[i] = ctx->value + (float)i / (float)CBM_RSQ_IN_DIM;
    }
    atomic_fetch_add_explicit(ctx->ready, 1, memory_order_release);
    while (atomic_load_explicit(ctx->start, memory_order_acquire) == 0) {
        cbm_usleep(1000);
    }
    cbm_rsq_encode(vec, &ctx->code);
    return NULL;
}

/* The daemon can initialize semantic encoders from multiple request threads.
 * Run this first so ThreadSanitizer observes the one-time initialization. */
TEST(sem_rotsq_concurrent_first_encode) {
    atomic_int ready;
    atomic_int start;
    atomic_init(&ready, 0);
    atomic_init(&start, 0);
    rotsq_thread_ctx_t ctx[2] = {
        {.ready = &ready, .start = &start, .value = 0.25F},
        {.ready = &ready, .start = &start, .value = -0.5F},
    };
    cbm_thread_t threads[2];
    bool started0 = cbm_thread_create(&threads[0], 0, rotsq_concurrent_first_encode, &ctx[0]) == 0;
    bool started1 = cbm_thread_create(&threads[1], 0, rotsq_concurrent_first_encode, &ctx[1]) == 0;
    for (int spins = 0; started0 && started1 && spins < 5000 &&
                        atomic_load_explicit(&ready, memory_order_acquire) < 2;
         spins++) {
        cbm_usleep(1000);
    }
    bool both_ready = atomic_load_explicit(&ready, memory_order_acquire) == 2;
    atomic_store_explicit(&start, 1, memory_order_release);
    if (started0) {
        (void)cbm_thread_join(&threads[0]);
    }
    if (started1) {
        (void)cbm_thread_join(&threads[1]);
    }

    ASSERT_TRUE(started0);
    ASSERT_TRUE(started1);
    ASSERT_TRUE(both_ready);
    ASSERT_TRUE(ctx[0].code.scale > 0.0F);
    ASSERT_TRUE(ctx[1].code.scale > 0.0F);
    PASS();
}

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
    RUN_TEST(sem_rotsq_concurrent_first_encode);
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
    RUN_TEST(sem_corpus_index_accessors_match_name_lookups);
    RUN_TEST(sem_tfidf_terms_compare_vocabulary_not_positions);
    RUN_TEST(sem_combine_weighs_signals);
    RUN_TEST(sem_admit_best_first_keeps_the_best);
    RUN_TEST(sem_calibrated_p_bands);
    RUN_TEST(sem_doc_calibrated_p_bands);
    RUN_TEST(sem_doc_format_by_extension);
    RUN_TEST(sem_p_two_decimals_half_up);
    RUN_TEST(sem_json_str_honours_escapes);
    RUN_TEST(sem_corpus_add_null_doc);
    RUN_TEST(sem_corpus_free_null);
    RUN_TEST(sem_get_config_defaults);
}
