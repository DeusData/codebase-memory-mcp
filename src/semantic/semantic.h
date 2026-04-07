/*
 * semantic.h — Algorithmic code embeddings for SEMANTICALLY_RELATED edges.
 *
 * Combines 11 signals into a unified similarity score without external
 * models or dependencies.  All signals derived from graph buffer metadata
 * and the existing AST walk.
 *
 * Signals:
 *   1. TF-IDF on metadata tokens (vocabulary overlap)
 *   2. Random Indexing with co-occurrence (within-codebase synonym bridging)
 *   3. MinHash structural (existing, decoded from "fp" property)
 *   4. API Signature vectors (same callees → related)
 *   5. Type Signature vectors (same param/return types → related)
 *   6. Module Proximity (same directory → boost)
 *   7. Decorator Pattern vectors (same annotations → related)
 *   8. AST Structural Profile (control flow shape, expression types)
 *   9. Approximate Data Flow (params→return, params→condition)
 *  10. Graph Diffusion (transitive closure via neighbor blending)
 *  11. Halstead-Lite (operator/operand complexity profile)
 *
 * Note: signals 8, 9, 11 are computed at extraction time (ast_profile.h)
 * and stored in properties_json.  The rest are computed in the post-pass.
 */
#ifndef CBM_SEMANTIC_H
#define CBM_SEMANTIC_H

#include <stdbool.h>
#include <stdint.h>

/* ── Configuration ───────────────────────────────────────────────── */

/* Random Indexing dimension. 256 is sufficient for <500K functions. */
/* 768 = nomic-embed-code embedding dimension.  Matches PRETRAINED_DIM. */
enum { CBM_SEM_DIM = 768 };

/* Random Indexing: non-zero entries per sparse random vector. */
enum { CBM_SEM_SPARSE_NNZE = 8 };

/* Co-occurrence window half-width. */
enum { CBM_SEM_WINDOW = 5 };

/* Default score threshold for SEMANTICALLY_RELATED edge emission.
 * 0.75 balances recall with precision: validated ~95% precision on
 * Linux kernel (0.80 = 100% but only 90 edges, 0.70 = 2047 edges
 * but ~80% precision). */
#define CBM_SEM_EDGE_THRESHOLD 0.75

/* Maximum SEMANTICALLY_RELATED edges per node. */
enum { CBM_SEM_MAX_EDGES = 10 };

/* AST structural profile: 25 float features per function (control flow,
 * nesting, expression types, literals, data flow, Halstead). */
enum { CBM_SEM_AST_PROFILE_DIMS = 25 };

/* MinHash fingerprint length (must match simhash/minhash.h CBM_MINHASH_K). */
enum { CBM_SEM_MINHASH_K = 64 };

/* Signal weights (sum to ~1.0, proximity is a multiplier). */
typedef struct {
    float w_tfidf;
    float w_ri;
    float w_minhash;
    float w_api;
    float w_type;
    float w_decorator;
    float w_struct_profile;
    float w_dataflow;
    float threshold;
    int max_edges;
} cbm_sem_config_t;

/* Get default config (can be overridden via env vars). */
cbm_sem_config_t cbm_sem_get_config(void);

/* Check if semantic embeddings are enabled (CBM_SEMANTIC_ENABLED=1). */
bool cbm_sem_is_enabled(void);

/* ── Token extraction ────────────────────────────────────────────── */

/* Maximum tokens per function from metadata (name + qn + path + sig + docstring + params). */
enum { CBM_SEM_MAX_TOKENS = 512 };

/* Split a name into tokens: camelCase, snake_case, dot.separated.
 * Writes up to max_out tokens into out. Returns token count.
 * Tokens are lowercased. Caller must free each token. */
int cbm_sem_tokenize(const char *name, char **out, int max_out);

/* ── Dense vectors ───────────────────────────────────────────────── */

/* A fixed-size dense vector for cosine similarity. */
typedef struct {
    float v[CBM_SEM_DIM];
} cbm_sem_vec_t;

/* Compute cosine similarity between two dense vectors. */
float cbm_sem_cosine(const cbm_sem_vec_t *a, const cbm_sem_vec_t *b);

/* Generate a deterministic sparse random vector for a token.
 * Uses xxHash(token) as seed. Output has SEM_SPARSE_NNZE non-zeros. */
void cbm_sem_random_index(const char *token, cbm_sem_vec_t *out);

/* Eagerly initialize the pretrained token lookup map.
 * Call this BEFORE dispatching parallel work that invokes cbm_sem_random_index,
 * so the lazy init races are avoided entirely on the hot path. */
void cbm_sem_ensure_ready(void);

/* Normalize a vector to unit length in-place. */
void cbm_sem_normalize(cbm_sem_vec_t *v);

/* Add src to dst: dst[i] += scale * src[i]. */
void cbm_sem_vec_add_scaled(cbm_sem_vec_t *dst, const cbm_sem_vec_t *src, float scale);

/* ── Per-function semantic data ──────────────────────────────────── */

/* All computed signals for one function. */
typedef struct {
    int64_t node_id;
    const char *file_path;
    const char *file_ext;

    /* Sparse TF-IDF: stored as parallel arrays of (token_index, weight). */
    int *tfidf_indices;
    float *tfidf_weights;
    int tfidf_len;

    /* Dense vectors for RI, API, Type, Decorator. */
    cbm_sem_vec_t ri_vec;
    cbm_sem_vec_t api_vec;
    cbm_sem_vec_t type_vec;
    cbm_sem_vec_t deco_vec;

    /* AST profile as float vector (decoded from "sp" property). */
    float struct_profile[CBM_SEM_AST_PROFILE_DIMS];

    /* MinHash fingerprint (decoded from "fp" property). */
    bool has_minhash;
    uint32_t minhash[CBM_SEM_MINHASH_K];
} cbm_sem_func_t;

/* ── Corpus-level data ───────────────────────────────────────────── */

/* Opaque corpus handle for IDF and Random Indexing state. */
typedef struct cbm_sem_corpus cbm_sem_corpus_t;

/* Create a new corpus from function data. */
cbm_sem_corpus_t *cbm_sem_corpus_new(void);

/* Register a function's tokens in the corpus (for IDF counting). */
void cbm_sem_corpus_add_doc(cbm_sem_corpus_t *corpus, const char **tokens, int count);

/* Batch-build the corpus from pre-tokenized documents (PARALLEL variant).
 * `all_tokens` layout: all_tokens[f * max_tokens_per_doc + t] = token pointer.
 * `token_counts[f]` = number of tokens in document f.
 * This replaces a loop of cbm_sem_corpus_add_doc() calls. */
void cbm_sem_corpus_add_docs_batch(cbm_sem_corpus_t *corpus, char **all_tokens,
                                   const int *token_counts, int doc_count, int max_tokens_per_doc);

/* Finalize: compute IDF, build enriched token vectors via co-occurrence. */
void cbm_sem_corpus_finalize(cbm_sem_corpus_t *corpus);

/* Get IDF weight for a token. Returns 0.0 for unknown tokens. */
float cbm_sem_corpus_idf(const cbm_sem_corpus_t *corpus, const char *token);

/* Get the enriched Random Indexing vector for a token (after co-occurrence). */
const cbm_sem_vec_t *cbm_sem_corpus_ri_vec(const cbm_sem_corpus_t *corpus, const char *token);

/* Get the total document count. */
int cbm_sem_corpus_doc_count(const cbm_sem_corpus_t *corpus);

/* Get the total token count (vocabulary size). */
int cbm_sem_corpus_token_count(const cbm_sem_corpus_t *corpus);

/* Get token name and enriched vector by index (for serialization).
 * Returns NULL if index is out of range. */
const char *cbm_sem_corpus_token_at(const cbm_sem_corpus_t *corpus, int index,
                                    const cbm_sem_vec_t **out_vec, float *out_idf);

/* Free corpus. */
void cbm_sem_corpus_free(cbm_sem_corpus_t *corpus);

/* ── Combined scoring ────────────────────────────────────────────── */

/* Compute combined similarity score between two functions. */
float cbm_sem_combined_score(const cbm_sem_func_t *a, const cbm_sem_func_t *b,
                             const cbm_sem_config_t *cfg);

/* Module proximity multiplier based on file paths. */
float cbm_sem_proximity(const char *path_a, const char *path_b);

/* ── Graph diffusion ─────────────────────────────────────────────── */

/* Apply one iteration of graph diffusion to a combined embedding.
 * Blends with mean of top-k neighbor embeddings (α=0.3). */
void cbm_sem_diffuse(cbm_sem_vec_t *combined, const cbm_sem_vec_t *neighbors, int neighbor_count,
                     float alpha);

#endif /* CBM_SEMANTIC_H */
