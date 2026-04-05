/*
 * embedding.h — Semantic embedding generation and hybrid search.
 *
 * Generates embeddings via HTTP POST to an OpenAI-compatible /v1/embeddings
 * endpoint (Ollama, llamafile, OpenAI, etc.). Configuration via env vars:
 *   CBM_EMBEDDING_URL   — Base URL (e.g., http://localhost:11434/v1)
 *   CBM_EMBEDDING_MODEL — Model name (e.g., nomic-embed-text)
 *   CBM_EMBEDDING_DIMS  — Expected vector dimensions (default: 768)
 *
 * When CBM_EMBEDDING_URL is not set, all embedding functions are no-ops.
 */
#ifndef CBM_EMBEDDING_H
#define CBM_EMBEDDING_H

#include "store/store.h"
#include <stdbool.h>

/* ── Configuration ──────────────────────────────────────────────── */

typedef struct {
    const char *url;    /* CBM_EMBEDDING_URL (NULL = disabled) */
    const char *model;  /* CBM_EMBEDDING_MODEL */
    int dims;           /* CBM_EMBEDDING_DIMS (default 768) */
    int batch_size;     /* texts per HTTP request (default 32) */
    int timeout_ms;     /* HTTP timeout (default 30000) */
} cbm_embedding_config_t;

/* Read config from environment variables. Returns config with url=NULL if disabled. */
cbm_embedding_config_t cbm_embedding_get_config(void);

/* Check if embedding is configured (CBM_EMBEDDING_URL is set). */
bool cbm_embedding_is_configured(void);

/* ── Embedding generation ──────────────────────────────────────── */

/* Embed a single text string. Returns allocated float[dims] or NULL on error.
 * Caller must free the returned array. */
float *cbm_embedding_embed_text(const cbm_embedding_config_t *cfg, const char *text);

/* Embed multiple texts in a single HTTP request.
 * Returns allocated float[count * dims] or NULL on error.
 * Caller must free the returned array. */
float *cbm_embedding_embed_batch(const cbm_embedding_config_t *cfg,
                                 const char **texts, int count);

/* ── Text generation (node → embeddable text) ──────────────────── */

/* Generate embeddable text for a node: "Label: name\nFile: path\nDir: dir\n\n<snippet>"
 * Returns allocated string. Caller must free. */
char *cbm_embedding_node_text(const cbm_node_t *node);

/* ── Hybrid search (BM25 + vector + RRF merge) ────────────────── */

/* RRF constant (from IR literature). */
#define CBM_RRF_K 60

/* Merged search result with combined RRF score. */
typedef struct {
    int64_t node_id;
    double rrf_score;    /* combined RRF score (higher = better) */
    double bm25_rank;    /* rank in BM25 results (-1 if not found by BM25) */
    double vec_rank;     /* rank in vector results (-1 if not found by vector) */
    double similarity;   /* cosine similarity (0 if not found by vector) */
} cbm_rrf_result_t;

/* Merge BM25 search results with vector search results using RRF (k=60).
 * bm25_ids: node IDs from BM25 search, in ranked order (best first).
 * vec_results: vector search results from cbm_store_vector_search.
 * Returns allocated array sorted by combined RRF score. Caller frees. */
int cbm_embedding_rrf_merge(const int64_t *bm25_ids, int bm25_count,
                            const cbm_vector_result_t *vec_results, int vec_count,
                            cbm_rrf_result_t **out, int *out_count);

/* ── Pipeline integration ──────────────────────────────────────── */

/* Generate embeddings for all embeddable nodes in a project.
 * Skips nodes that already have embeddings unless force=true.
 * Returns number of embeddings generated, or -1 on error. */
int cbm_embedding_generate_for_project(cbm_store_t *s, const char *project, bool force);

#endif /* CBM_EMBEDDING_H */
