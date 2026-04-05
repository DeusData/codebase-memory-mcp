/*
 * cross_repo.h — Cross-repository discovery, search, and flow tracing.
 *
 * Builds a unified _cross_repo.db by scanning all per-project databases.
 * Enables: cross-repo channel matching, cross-repo BM25+vector search,
 * cross-repo flow tracing, and cross-repo impact analysis.
 *
 * The cross-repo DB is read-only (built by cbm_cross_repo_build) and
 * does NOT use ATTACH — it copies data into a separate SQLite file,
 * preserving per-project security isolation.
 */
#ifndef CBM_CROSS_REPO_H
#define CBM_CROSS_REPO_H

#include "store/store.h"
#include <stdbool.h>

/* ── Build ──────────────────────────────────────────────────────── */

typedef struct {
    int repos_scanned;
    int channels_copied;
    int nodes_copied;
    int embeddings_copied;
    int cross_repo_matches;   /* channels with emit in A + listen in B */
    double build_time_ms;
} cbm_cross_repo_stats_t;

/* Build (or rebuild) the cross-repo index by scanning all project DBs.
 * Writes to ~/.cache/codebase-memory-mcp/_cross_repo.db.
 * Returns stats on success, or sets stats.repos_scanned=-1 on error. */
cbm_cross_repo_stats_t cbm_cross_repo_build(void);

/* ── Query ──────────────────────────────────────────────────────── */

/* Opaque handle for the cross-repo DB (separate from per-project stores). */
typedef struct cbm_cross_repo cbm_cross_repo_t;

/* Open the cross-repo DB for querying. Returns NULL if not built yet. */
cbm_cross_repo_t *cbm_cross_repo_open(void);

/* Close and free. NULL-safe. */
void cbm_cross_repo_close(cbm_cross_repo_t *cr);

/* ── Cross-Repo Search ──────────────────────────────────────────── */

typedef struct {
    const char *project;        /* short project name */
    int64_t orig_id;            /* node ID in the project's own DB */
    const char *label;
    const char *name;
    const char *qualified_name;
    const char *file_path;
    double score;               /* BM25 or RRF score */
    double similarity;          /* cosine similarity (0 if BM25-only) */
} cbm_cross_search_result_t;

typedef struct {
    cbm_cross_search_result_t *results;
    int count;
    int total;
    bool used_vector;           /* true if hybrid BM25+vector was used */
} cbm_cross_search_output_t;

/* Search across all repos. Uses BM25 FTS5 + optional vector search + RRF merge.
 * query_vec may be NULL (BM25-only). Caller frees output with _free(). */
int cbm_cross_repo_search(cbm_cross_repo_t *cr, const char *query,
                          const float *query_vec, int dims,
                          int limit, cbm_cross_search_output_t *out);

void cbm_cross_search_free(cbm_cross_search_output_t *out);

/* ── Cross-Repo Channel Matching ────────────────────────────────── */

typedef struct {
    const char *channel_name;
    const char *transport;
    /* Emitter side */
    const char *emit_project;
    const char *emit_file;
    const char *emit_function;
    /* Listener side */
    const char *listen_project;
    const char *listen_file;
    const char *listen_function;
} cbm_cross_channel_match_t;

/* Find cross-repo channel matches: channels where emit is in one repo
 * and listen is in another. Optional channel_name filter (partial match).
 * Returns allocated array. Caller frees with _free(). */
int cbm_cross_repo_match_channels(cbm_cross_repo_t *cr, const char *channel_filter,
                                  cbm_cross_channel_match_t **out, int *count);

void cbm_cross_channel_free(cbm_cross_channel_match_t *matches, int count);

/* ── Cross-Repo Stats ───────────────────────────────────────────── */

typedef struct {
    int total_repos;
    int total_nodes;
    int total_channels;
    int total_embeddings;
    int cross_repo_channel_count;
    const char *built_at;       /* ISO timestamp */
} cbm_cross_repo_info_t;

/* Get stats about the cross-repo index. */
int cbm_cross_repo_get_info(cbm_cross_repo_t *cr, cbm_cross_repo_info_t *out);

void cbm_cross_repo_info_free(cbm_cross_repo_info_t *info);

/* ── Cross-Repo Trace Helper ────────────────────────────────────── */

typedef struct {
    const char *name;
    const char *label;
    const char *file_path;
    int depth;
} cbm_cross_trace_step_t;

/* Trace callers (inbound) or callees (outbound) from a function in a project DB.
 * Opens the project DB read-only, resolves the function, runs BFS, closes DB.
 * Handles Class→Method resolution and (file-level) listener fallback.
 * channel_name is optional — used for file-level listener resolution.
 * Returns allocated array. Caller frees with cbm_cross_trace_free(). */
int cbm_cross_repo_trace_in_project(
    const char *project_db_path,
    const char *function_name,
    const char *file_path_hint,
    const char *channel_name,   /* optional: for resolving (file-level) listeners */
    const char *direction,      /* "inbound" or "outbound" */
    int max_depth,
    cbm_cross_trace_step_t **out, int *out_count);

void cbm_cross_trace_free(cbm_cross_trace_step_t *steps, int count);

#endif /* CBM_CROSS_REPO_H */
