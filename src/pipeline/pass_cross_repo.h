/*
 * pass_cross_repo.h — Cross-repo intelligence: match Routes, Channels, and
 * async topics across indexed projects to create CROSS_* edges.
 */
#ifndef CBM_PASS_CROSS_REPO_H
#define CBM_PASS_CROSS_REPO_H

#include "store/store.h"

#include <stdatomic.h>

/* A store the ["*"] enumeration skipped instead of linking. */
typedef struct {
    char *project;      /* heap-owned */
    const char *reason; /* static, e.g. "pre_768_schema" (reindex required) */
} cbm_cross_repo_skip_t;

/* Result of a cross-repo matching run. */
typedef struct {
    int http_edges;    /* CROSS_HTTP_CALLS edges created */
    int async_edges;   /* CROSS_ASYNC_CALLS edges created */
    int channel_edges; /* CROSS_CHANNEL edges created */
    int grpc_edges;    /* CROSS_GRPC_CALLS edges created */
    int graphql_edges; /* CROSS_GRAPHQL_CALLS edges created */
    int trpc_edges;    /* CROSS_TRPC_CALLS edges created */
    int projects_scanned;
    double elapsed_ms;
    bool failed;          /* source/target validation, open, or allocation failed */
    bool cancelled;       /* stopped at a bounded cancellation checkpoint */
    bool partial_results; /* committed writes before cancellation were retained */
    cbm_cross_repo_skip_t *skipped_projects; /* ["*"] only; free with ..._result_free */
    int skipped_count;
} cbm_cross_repo_result_t;

/* Release the heap-owned parts of a result (safe on a zeroed result). */
void cbm_cross_repo_result_free(cbm_cross_repo_result_t *result);

struct yyjson_mut_doc;
struct yyjson_mut_val;

/* Add a "cross_repo" object to `parent` describing the last cross-repo run
 * with `project` as the source, as recorded in that project's store:
 *   {"status":"never_run"}  -- no run recorded since the store was (re)built
 *   {"status":"ran","last_run_at","outcome","targets","projects_scanned",
 *    "total_cross_edges","skipped_projects"}
 * Read-only (works on a query store). Returns false only on invalid input or
 * allocation failure. */
bool cbm_cross_repo_add_status_json(struct yyjson_mut_doc *doc, struct yyjson_mut_val *parent,
                                    cbm_store_t *store, const char *project);

/* Run cross-repo matching for `project` against `target_projects`.
 * If target_count == 1 and target_projects[0] == "*", matches against all
 * indexed projects. Writes CROSS_* edges bidirectionally into both the
 * source and target project DBs.
 *
 * `project` must already be indexed (its .db must exist).
 * Returns result with edge counts. */
cbm_cross_repo_result_t cbm_cross_repo_match(const char *project, const char **target_projects,
                                             int target_count);

/* Cancellation-aware form used by session-owned frontends. The caller-owned
 * flag must outlive the call. Cancellation is not a multi-database rollback:
 * already committed target writes remain and are reported as partial results. */
cbm_cross_repo_result_t cbm_cross_repo_match_cancellable(const char *project,
                                                         const char **target_projects,
                                                         int target_count,
                                                         const atomic_int *cancelled);

#endif /* CBM_PASS_CROSS_REPO_H */
