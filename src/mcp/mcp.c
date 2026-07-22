/*
 * mcp.c — MCP server: JSON-RPC 2.0 over stdio with graph tools.
 *
 * Uses yyjson for fast JSON parsing/building.
 * Single-threaded event loop: read line → parse → dispatch → respond.
 */
#include "foundation/constants.h"

#define SLEN(s) (sizeof(s) - 1)
#define MCP_CONTENT_HEADER "Content-Length:"

enum {
    MCP_FIELD_SIZE = 1040,
    MCP_TIMEOUT_MS = 1000,
    MCP_HALF_SEC_US = 500000,
    MCP_MAX_ROWS = 100,
    MCP_COL_2 = 2,
    MCP_COL_3 = 3,
    MCP_COL_4 = 4,
    MCP_COL_7 = 7,
    MCP_COL_10 = 10,
    MCP_COL_16 = 16,
    MCP_DB_EXT = 3,      /* strlen(".db") */
    MCP_MIN_DB_NAME = 4, /* min length for "x.db" */
    MCP_SEPARATOR = 2,   /* space for separator chars */
    MCP_DEFAULT_DEPTH = 3,
    MCP_DEFAULT_BFS_DEPTH = 2,
    MCP_DEFAULT_LIMIT = 10,
    MCP_BFS_LIMIT = 100,
    MCP_BFS_LIMIT_MAX = 5000,
    MCP_DEFAULT_IMPACT_LIMIT = 200,
    MCP_TRACE_CANDIDATE_LIMIT = 5,
    MCP_N_DEFAULTS_2 = 2,
    MCP_URI_PREFIX = 7,      /* strlen("file://") */
    MCP_CONTENT_PREFIX = SLEN(MCP_CONTENT_HEADER),
    MCP_RETURN_2 = 2,
    MCP_TOOLS_PAGE_SIZE = 8,
    MCP_PROJECTS_PAGE_SIZE = 50,
    MCP_PROJECTS_PAGE_MAX = 200,
};
#define MCP_MS_TO_US 1000LL
#define MCP_S_TO_US 1000000LL

#include "mcp/mcp.h"
#include "store/store.h"
#include <sqlite3.h>
#include <ctype.h>
#include "cypher/cypher.h"
#include "discover/discover.h"
#include "pipeline/pipeline.h"
#include "depindex/depindex.h"
#include "pagerank/pagerank.h"
#include "pipeline/pass_cross_repo.h"
#include "git/git_context.h"
#include "git/git_snapshot.h"
#include "cli/cli.h"
#include "watcher/watcher.h"
#include "foundation/mem.h"
#include "foundation/diagnostics.h"
#include "foundation/platform.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/compat_thread.h"
#include "foundation/log.h"
#include "foundation/profile.h"
#include "foundation/limits.h"
#include "mcp/index_supervisor.h"
#include "mcp/compact_out.h"
#include "foundation/str_util.h"
#include "foundation/dump_verify.h"
#include "foundation/compat_regex.h"
#include <sqlite3.h>
#include "pipeline/artifact.h"
#include "helpers.h" /* cbm_kind_in_set_free_cache: auto-index thread teardown */

#ifdef _WIN32
#include <direct.h>
#include <io.h>
#include <process.h>
#define getpid _getpid
#else
#include <unistd.h>
#include <poll.h>
#include <fcntl.h>
#endif
#include <yyjson/yyjson.h>
#include <stdint.h> // int64_t
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <errno.h>
#include <stdatomic.h>

/* ── Constants ────────────────────────────────────────────────── */

/* Add a "pagerank" key with value formatted to 4 significant figures.
 * Writes directly as a raw JSON number (e.g. 4.755e-05) — no double round-trip.
 * 4 sig figs preserves ranking distinguishability while saving ~12 chars/value.
 * This is the single place pagerank values are serialized to JSON. */
static void add_pagerank_val(yyjson_mut_doc *doc, yyjson_mut_val *obj, double v) {
    char buf[32];
    snprintf(buf, sizeof(buf), "%.4g", v);
    yyjson_mut_obj_add_val(doc, obj, "pagerank", yyjson_mut_rawcpy(doc, buf));
}

static void add_response_warning(yyjson_mut_doc *doc, yyjson_mut_val *root, const char *message) {
    if (!doc || !root || !message) {
        return;
    }
    yyjson_mut_val *warnings = yyjson_mut_obj_get(root, "warnings");
    if (!warnings || !yyjson_mut_is_arr(warnings)) {
        warnings = yyjson_mut_arr(doc);
        yyjson_mut_obj_add_val(doc, root, "warnings", warnings);
    }
    yyjson_mut_arr_add_str(doc, warnings, message);
}

#define CBM_MCP_FRESHNESS_KEY "freshness"
#define CBM_MCP_FRESHNESS_STATE_KEY "state"
#define CBM_MCP_FRESHNESS_STALE_VIEWS_KEY "stale_views"
#define CBM_MCP_FRESHNESS_STALE_SCOPE_KEY "stale_scope"
#define CBM_MCP_FRESHNESS_DIRTY_PENDING_KEY "dirty_files_pending"
#define CBM_MCP_FRESHNESS_DIRTY_OVERLAY_READY_KEY "dirty_files_overlay_ready"
#define CBM_MCP_FRESHNESS_DIRTY_WITH_WARNING "dirty_with_warning"
#define CBM_MCP_FRESHNESS_SCOPE_DIRTY_FILES "dirty_files"
#define CBM_MCP_FRESHNESS_STALE_WITH_WARNING "stale_with_warning"
#define CBM_MCP_FRESHNESS_READ_MODEL_KEY "read_model"
#define CBM_MCP_FRESHNESS_READ_MODEL_CANONICAL_ONLY "canonical_only"
#define CBM_MCP_FRESHNESS_READ_MODEL_MIXED_ACTIVE_NODES "mixed_active_nodes_canonical_summaries"
#define CBM_MCP_FRESHNESS_READ_MODEL_OVERLAY_ACTIVE_NODES "overlay_active_nodes"
#define CBM_MCP_FRESHNESS_READ_MODEL_OVERLAY_ACTIVE_GRAPH "overlay_active_graph"
#define CBM_MCP_EXACT_DELTA_KEY "exact_delta"

static yyjson_mut_val *ensure_response_freshness(yyjson_mut_doc *doc, yyjson_mut_val *root) {
    if (!doc || !root) {
        return NULL;
    }
    yyjson_mut_val *freshness = yyjson_mut_obj_get(root, CBM_MCP_FRESHNESS_KEY);
    if (!freshness || !yyjson_mut_is_obj(freshness)) {
        freshness = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_val(doc, root, CBM_MCP_FRESHNESS_KEY, freshness);
    }
    return freshness;
}

static void add_response_stale_view(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                    const char *view_name) {
    if (!doc || !root || !view_name || !view_name[0]) {
        return;
    }

    yyjson_mut_val *freshness = ensure_response_freshness(doc, root);
    if (!freshness) {
        return;
    }
    if (!yyjson_mut_obj_get(freshness, CBM_MCP_FRESHNESS_STATE_KEY)) {
        yyjson_mut_obj_add_str(doc, freshness, CBM_MCP_FRESHNESS_STATE_KEY,
                               CBM_MCP_FRESHNESS_STALE_WITH_WARNING);
    }

    yyjson_mut_val *stale_views =
        yyjson_mut_obj_get(freshness, CBM_MCP_FRESHNESS_STALE_VIEWS_KEY);
    if (!stale_views || !yyjson_mut_is_arr(stale_views)) {
        stale_views = yyjson_mut_arr(doc);
        yyjson_mut_obj_add_val(doc, freshness, CBM_MCP_FRESHNESS_STALE_VIEWS_KEY, stale_views);
    }
    yyjson_mut_arr_iter iter;
    yyjson_mut_val *item;
    yyjson_mut_arr_iter_init(stale_views, &iter);
    while ((item = yyjson_mut_arr_iter_next(&iter))) {
        const char *existing = yyjson_mut_get_str(item);
        if (existing && strcmp(existing, view_name) == 0) {
            return;
        }
    }
    yyjson_mut_arr_add_str(doc, stale_views, view_name);
}

static bool get_dirty_file_counts(cbm_store_t *store, const char *project,
                                  int *out_pending, int *out_overlay_ready) {
    if (out_pending) {
        *out_pending = 0;
    }
    if (out_overlay_ready) {
        *out_overlay_ready = 0;
    }
    if (!store || !project || !project[0]) {
        return false;
    }
    int pending = 0;
    int overlay_ready = 0;
    if (cbm_store_count_dirty_files(store, project, &pending, &overlay_ready) != CBM_STORE_OK ||
        (pending <= 0 && overlay_ready <= 0)) {
        return false;
    }
    if (out_pending) {
        *out_pending = pending;
    }
    if (out_overlay_ready) {
        *out_overlay_ready = overlay_ready;
    }
    return true;
}

void cbm_mcp_add_dirty_file_freshness_counts(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                             int pending, int overlay_ready,
                                             const char *warning_message) {
    if (!doc || !root || (pending <= 0 && overlay_ready <= 0)) {
        return;
    }

    yyjson_mut_val *freshness = ensure_response_freshness(doc, root);
    if (!freshness) {
        return;
    }
    if (!yyjson_mut_obj_get(freshness, CBM_MCP_FRESHNESS_STATE_KEY)) {
        yyjson_mut_obj_add_str(doc, freshness, CBM_MCP_FRESHNESS_STATE_KEY,
                               CBM_MCP_FRESHNESS_DIRTY_WITH_WARNING);
    }
    if (!yyjson_mut_obj_get(freshness, CBM_MCP_FRESHNESS_STALE_SCOPE_KEY)) {
        yyjson_mut_obj_add_str(doc, freshness, CBM_MCP_FRESHNESS_STALE_SCOPE_KEY,
                               CBM_MCP_FRESHNESS_SCOPE_DIRTY_FILES);
    }
    yyjson_mut_obj_add_int(doc, freshness, CBM_MCP_FRESHNESS_DIRTY_PENDING_KEY, pending);
    yyjson_mut_obj_add_int(doc, freshness, CBM_MCP_FRESHNESS_DIRTY_OVERLAY_READY_KEY,
                           overlay_ready);
    add_response_warning(doc, root,
                         warning_message
                             ? warning_message
                             : "project has dirty files; canonical graph rows remain visible until "
                               "overlay or reindex completes.");
}

static void add_dirty_file_freshness_counts(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                            int pending, int overlay_ready) {
    cbm_mcp_add_dirty_file_freshness_counts(doc, root, pending, overlay_ready, NULL);
}

static void add_dirty_file_freshness(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                     cbm_store_t *store, const char *project) {
    int pending = 0;
    int overlay_ready = 0;
    if (!get_dirty_file_counts(store, project, &pending, &overlay_ready)) {
        return;
    }
    add_dirty_file_freshness_counts(doc, root, pending, overlay_ready);
}

static void add_overlay_node_read_view_summary(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                               cbm_store_t *store, const char *project,
                                               const char *warning) {
    if (!doc || !root || !store || !project || !project[0]) {
        return;
    }
    cbm_store_overlay_node_view_summary_t summary = {0};
    if (cbm_store_get_overlay_node_view_summary(store, project, &summary) != CBM_STORE_OK ||
        !cbm_store_overlay_node_view_has_ready_rows(&summary)) {
        return;
    }
    yyjson_mut_val *view = yyjson_mut_obj(doc);
    if (!view) {
        return;
    }
    yyjson_mut_obj_add_str(doc, view, "state", "overlay_ready");
    yyjson_mut_obj_add_int(doc, view, "overlay_ready_generations",
                           summary.overlay_ready_generations);
    yyjson_mut_obj_add_int(doc, view, "active_file_tombstones",
                           summary.active_file_tombstones);
    yyjson_mut_obj_add_int(doc, view, "canonical_nodes_visible",
                           summary.canonical_nodes_visible);
    yyjson_mut_obj_add_int(doc, view, "overlay_owned_nodes_visible",
                           summary.overlay_owned_nodes_visible);
    yyjson_mut_obj_add_int(doc, view, "total_nodes_visible", summary.total_nodes_visible);
    yyjson_mut_obj_add_val(doc, root, "overlay_read_view", view);
    add_response_warning(doc, root,
                         warning ? warning
                                 : "overlay_read_view is informational; graph search and trace "
                                   "results remain canonical unless a tool explicitly says it is "
                                   "overlay-aware.");
}

static void add_overlay_active_node_search_freshness(
    yyjson_mut_doc *doc, yyjson_mut_val *root,
    const cbm_store_overlay_node_view_summary_t *summary, bool uses_active_edges) {
    if (!cbm_store_overlay_node_view_has_ready_rows(summary)) {
        return;
    }
    yyjson_mut_val *freshness = ensure_response_freshness(doc, root);
    if (!freshness) {
        return;
    }
    yyjson_mut_obj_add_str(doc, freshness, CBM_MCP_FRESHNESS_READ_MODEL_KEY,
                           uses_active_edges
                               ? CBM_MCP_FRESHNESS_READ_MODEL_OVERLAY_ACTIVE_GRAPH
                               : CBM_MCP_FRESHNESS_READ_MODEL_OVERLAY_ACTIVE_NODES);
    yyjson_mut_obj_add_int(doc, freshness, "overlay_ready_generations",
                           summary->overlay_ready_generations);
    yyjson_mut_obj_add_int(doc, freshness, "active_file_tombstones",
                           summary->active_file_tombstones);
    yyjson_mut_obj_add_int(doc, freshness, "canonical_nodes_visible",
                           summary->canonical_nodes_visible);
    yyjson_mut_obj_add_int(doc, freshness, "overlay_owned_nodes_visible",
                           summary->overlay_owned_nodes_visible);
    yyjson_mut_obj_add_int(doc, freshness, "total_nodes_visible",
                           summary->total_nodes_visible);
    add_response_warning(
        doc, root,
        uses_active_edges
            ? "search_graph graph mode used overlay active node and relationship rows; "
              "include_connected uses active one-hop names when requested; query mode and "
              "trace_path also use active overlay rows where supported."
            : "search_graph graph mode used overlay active node rows; query mode and "
              "trace_path also use active overlay rows where supported.");
}

static void add_overlay_active_trace_freshness(
    yyjson_mut_doc *doc, yyjson_mut_val *root,
    const cbm_store_overlay_node_view_summary_t *summary) {
    if (!cbm_store_overlay_node_view_has_ready_rows(summary)) {
        return;
    }
    yyjson_mut_val *freshness = ensure_response_freshness(doc, root);
    if (!freshness) {
        return;
    }
    yyjson_mut_obj_add_str(doc, freshness, CBM_MCP_FRESHNESS_READ_MODEL_KEY,
                           CBM_MCP_FRESHNESS_READ_MODEL_OVERLAY_ACTIVE_GRAPH);
    yyjson_mut_obj_add_int(doc, freshness, "overlay_ready_generations",
                           summary->overlay_ready_generations);
    yyjson_mut_obj_add_int(doc, freshness, "active_file_tombstones",
                           summary->active_file_tombstones);
    yyjson_mut_obj_add_int(doc, freshness, "canonical_nodes_visible",
                           summary->canonical_nodes_visible);
    yyjson_mut_obj_add_int(doc, freshness, "overlay_owned_nodes_visible",
                           summary->overlay_owned_nodes_visible);
    yyjson_mut_obj_add_int(doc, freshness, "total_nodes_visible",
                           summary->total_nodes_visible);
    add_response_warning(doc, root,
                         "trace_path used overlay active node and relationship rows for a "
                         "resolved start node; architecture summaries and search_code remain "
                         "canonical until separately enabled.");
}

static void add_overlay_active_query_freshness(
    yyjson_mut_doc *doc, yyjson_mut_val *root,
    const cbm_store_overlay_node_view_summary_t *summary) {
    if (!cbm_store_overlay_node_view_has_ready_rows(summary)) {
        return;
    }
    yyjson_mut_val *freshness = ensure_response_freshness(doc, root);
    if (!freshness) {
        return;
    }
    yyjson_mut_obj_add_str(doc, freshness, CBM_MCP_FRESHNESS_READ_MODEL_KEY,
                           CBM_MCP_FRESHNESS_READ_MODEL_OVERLAY_ACTIVE_NODES);
    yyjson_mut_obj_add_int(doc, freshness, "overlay_ready_generations",
                           summary->overlay_ready_generations);
    yyjson_mut_obj_add_int(doc, freshness, "active_file_tombstones",
                           summary->active_file_tombstones);
    yyjson_mut_obj_add_int(doc, freshness, "canonical_nodes_visible",
                           summary->canonical_nodes_visible);
    yyjson_mut_obj_add_int(doc, freshness, "overlay_owned_nodes_visible",
                           summary->overlay_owned_nodes_visible);
    yyjson_mut_obj_add_int(doc, freshness, "total_nodes_visible",
                           summary->total_nodes_visible);
    add_response_warning(
        doc, root,
        "search_graph query used active overlay node rows: canonical BM25 rows from visible "
        "files plus changed-file overlay rows matched by node text; hidden canonical files "
        "are suppressed.");
}

static void add_overlay_active_cypher_freshness(
    yyjson_mut_doc *doc, yyjson_mut_val *root,
    const cbm_store_overlay_node_view_summary_t *summary) {
    if (!cbm_store_overlay_node_view_has_ready_rows(summary)) {
        return;
    }
    yyjson_mut_val *freshness = ensure_response_freshness(doc, root);
    if (!freshness) {
        return;
    }
    yyjson_mut_obj_add_str(doc, freshness, CBM_MCP_FRESHNESS_READ_MODEL_KEY,
                           CBM_MCP_FRESHNESS_READ_MODEL_OVERLAY_ACTIVE_NODES);
    yyjson_mut_obj_add_int(doc, freshness, "overlay_ready_generations",
                           summary->overlay_ready_generations);
    yyjson_mut_obj_add_int(doc, freshness, "active_file_tombstones",
                           summary->active_file_tombstones);
    yyjson_mut_obj_add_int(doc, freshness, "canonical_nodes_visible",
                           summary->canonical_nodes_visible);
    yyjson_mut_obj_add_int(doc, freshness, "overlay_owned_nodes_visible",
                           summary->overlay_owned_nodes_visible);
    yyjson_mut_obj_add_int(doc, freshness, "total_nodes_visible",
                           summary->total_nodes_visible);
    add_response_warning(
        doc, root,
        "query_graph used active overlay node rows and active edge-derived predicates for this "
        "Cypher query; id() Cypher queries remain canonical.");
}

static void add_overlay_active_schema_freshness(
    yyjson_mut_doc *doc, yyjson_mut_val *root,
    const cbm_store_overlay_node_view_summary_t *summary, bool include_properties,
    const char *warning) {
    if (!cbm_store_overlay_node_view_has_ready_rows(summary)) {
        return;
    }
    yyjson_mut_val *freshness = ensure_response_freshness(doc, root);
    if (!freshness) {
        return;
    }
    yyjson_mut_obj_add_str(doc, freshness, CBM_MCP_FRESHNESS_READ_MODEL_KEY,
                           CBM_MCP_FRESHNESS_READ_MODEL_OVERLAY_ACTIVE_GRAPH);
    yyjson_mut_val *active_sections = yyjson_mut_arr(doc);
    yyjson_mut_arr_add_str(doc, active_sections, "node_labels");
    yyjson_mut_arr_add_str(doc, active_sections, "edge_types");
    if (include_properties) {
        yyjson_mut_arr_add_str(doc, active_sections, "node_properties");
        yyjson_mut_arr_add_str(doc, active_sections, "edge_properties");
    }
    yyjson_mut_obj_add_val(doc, freshness, "active_sections", active_sections);
    yyjson_mut_obj_add_int(doc, freshness, "overlay_ready_generations",
                           summary->overlay_ready_generations);
    yyjson_mut_obj_add_int(doc, freshness, "active_file_tombstones",
                           summary->active_file_tombstones);
    yyjson_mut_obj_add_int(doc, freshness, "canonical_nodes_visible",
                           summary->canonical_nodes_visible);
    yyjson_mut_obj_add_int(doc, freshness, "overlay_owned_nodes_visible",
                           summary->overlay_owned_nodes_visible);
    yyjson_mut_obj_add_int(doc, freshness, "total_nodes_visible",
                           summary->total_nodes_visible);
    if (warning && warning[0]) {
        add_response_warning(doc, root, warning);
    }
}

static void add_overlay_active_search_code_freshness(
    yyjson_mut_doc *doc, yyjson_mut_val *root,
    const cbm_store_overlay_node_view_summary_t *summary) {
    if (!cbm_store_overlay_node_view_has_ready_rows(summary)) {
        return;
    }
    yyjson_mut_val *freshness = ensure_response_freshness(doc, root);
    if (!freshness) {
        return;
    }
    yyjson_mut_obj_add_str(doc, freshness, CBM_MCP_FRESHNESS_READ_MODEL_KEY,
                           CBM_MCP_FRESHNESS_READ_MODEL_OVERLAY_ACTIVE_NODES);
    yyjson_mut_obj_add_int(doc, freshness, "overlay_ready_generations",
                           summary->overlay_ready_generations);
    yyjson_mut_obj_add_int(doc, freshness, "active_file_tombstones",
                           summary->active_file_tombstones);
    yyjson_mut_obj_add_int(doc, freshness, "canonical_nodes_visible",
                           summary->canonical_nodes_visible);
    yyjson_mut_obj_add_int(doc, freshness, "overlay_owned_nodes_visible",
                           summary->overlay_owned_nodes_visible);
    yyjson_mut_obj_add_int(doc, freshness, "total_nodes_visible",
                           summary->total_nodes_visible);
    add_response_warning(
        doc, root,
        "search_code read live source files and used active overlay node rows for graph "
        "annotations where ready; raw matches remain live-source-only when no graph node "
        "contains the match line.");
}

static bool add_overlay_active_architecture_freshness(
    yyjson_mut_doc *doc, yyjson_mut_val *root, cbm_store_t *store, const char *project,
    bool include_languages, bool include_entry_points, bool include_routes,
    bool include_file_tree, const char *warning) {
    if (!doc || !root || !store || !project || !project[0]) {
        return false;
    }
    if (!include_languages && !include_entry_points && !include_routes && !include_file_tree) {
        return false;
    }
    cbm_store_overlay_node_view_summary_t summary = {0};
    if (cbm_store_get_overlay_node_view_summary(store, project, &summary) != CBM_STORE_OK ||
        !cbm_store_overlay_node_view_has_ready_rows(&summary)) {
        return false;
    }
    yyjson_mut_val *freshness = ensure_response_freshness(doc, root);
    if (!freshness) {
        return false;
    }
    yyjson_mut_obj_add_str(doc, freshness, CBM_MCP_FRESHNESS_READ_MODEL_KEY,
                           CBM_MCP_FRESHNESS_READ_MODEL_MIXED_ACTIVE_NODES);
    yyjson_mut_val *active_sections = yyjson_mut_arr(doc);
    if (include_languages) {
        yyjson_mut_arr_add_str(doc, active_sections, "languages");
    }
    if (include_entry_points) {
        yyjson_mut_arr_add_str(doc, active_sections, "entry_points");
    }
    if (include_routes) {
        yyjson_mut_arr_add_str(doc, active_sections, "routes");
    }
    if (include_file_tree) {
        yyjson_mut_arr_add_str(doc, active_sections, "file_tree");
    }
    yyjson_mut_obj_add_val(doc, freshness, "active_sections", active_sections);
    yyjson_mut_obj_add_int(doc, freshness, "overlay_ready_generations",
                           summary.overlay_ready_generations);
    yyjson_mut_obj_add_int(doc, freshness, "active_file_tombstones",
                           summary.active_file_tombstones);
    yyjson_mut_obj_add_int(doc, freshness, "canonical_nodes_visible",
                           summary.canonical_nodes_visible);
    yyjson_mut_obj_add_int(doc, freshness, "overlay_owned_nodes_visible",
                           summary.overlay_owned_nodes_visible);
    yyjson_mut_obj_add_int(doc, freshness, "total_nodes_visible", summary.total_nodes_visible);
    add_response_warning(
        doc, root,
        warning && warning[0]
            ? warning
            : "get_architecture used active overlay node rows for sections listed in "
              "freshness.active_sections; counts, relationship_patterns, and other "
              "derived summaries remain canonical or stale until active architecture "
              "views or compaction are available.");
    return true;
}

static void add_pipeline_exact_delta_stats(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                           cbm_pipeline_exact_delta_stats_t stats) {
    if (!doc || !root || (stats.changed_paths < 0 && stats.affected_paths < 0 &&
                          stats.published_paths < 0)) {
        return;
    }
    yyjson_mut_val *exact = yyjson_mut_obj(doc);
    if (!exact) {
        return;
    }
    if (stats.changed_paths >= 0) {
        yyjson_mut_obj_add_int(doc, exact, "changed_paths", stats.changed_paths);
    }
    if (stats.affected_paths >= 0) {
        yyjson_mut_obj_add_int(doc, exact, "affected_paths", stats.affected_paths);
    }
    if (stats.affected_paths_limit >= 0) {
        yyjson_mut_obj_add_int(doc, exact, "affected_paths_limit",
                               stats.affected_paths_limit);
    }
    if (stats.affected_paths_truncated) {
        yyjson_mut_obj_add_bool(doc, exact, "affected_paths_truncated", true);
    }
    if (stats.published_paths >= 0) {
        yyjson_mut_obj_add_int(doc, exact, "published_paths", stats.published_paths);
    }
    yyjson_mut_obj_add_val(doc, root, CBM_MCP_EXACT_DELTA_KEY, exact);
}

static void add_stale_derived_view_warning(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                           const char *view_name, const char *message) {
    add_response_warning(doc, root, message);
    add_response_stale_view(doc, root, view_name);
}

static void add_derived_freshness_warnings(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                           bool pagerank_stale, bool linkrank_stale,
                                           bool node_degree_stale) {
    if (pagerank_stale) {
        add_stale_derived_view_warning(
            doc, root, CBM_STORE_DERIVED_VIEW_PAGERANK,
            "pagerank derived view is stale; stale PageRank values were omitted.");
    }
    if (linkrank_stale) {
        add_stale_derived_view_warning(
            doc, root, CBM_STORE_DERIVED_VIEW_LINKRANK,
            "linkrank derived view is stale; stale LinkRank ordering was not used.");
    }
    if (node_degree_stale) {
        add_stale_derived_view_warning(
            doc, root, CBM_STORE_DERIVED_VIEW_NODE_DEGREE,
            "node_degree derived view is stale; precomputed degree data was not used.");
    }
}

static bool add_canonical_only_overlay_freshness(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                                 cbm_store_t *store, const char *project,
                                                 const char *warning) {
    if (!doc || !root || !store || !project || !project[0]) {
        return false;
    }
    cbm_store_overlay_node_view_summary_t summary = {0};
    if (cbm_store_get_overlay_node_view_summary(store, project, &summary) != CBM_STORE_OK ||
        !cbm_store_overlay_node_view_has_ready_rows(&summary)) {
        return false;
    }
    yyjson_mut_val *freshness = ensure_response_freshness(doc, root);
    if (!freshness) {
        return false;
    }
    if (!yyjson_mut_obj_get(freshness, CBM_MCP_FRESHNESS_READ_MODEL_KEY)) {
        yyjson_mut_obj_add_str(doc, freshness, CBM_MCP_FRESHNESS_READ_MODEL_KEY,
                               CBM_MCP_FRESHNESS_READ_MODEL_CANONICAL_ONLY);
    }
    yyjson_mut_obj_add_int(doc, freshness, "overlay_ready_generations",
                           summary.overlay_ready_generations);
    yyjson_mut_obj_add_int(doc, freshness, "active_file_tombstones",
                           summary.active_file_tombstones);
    yyjson_mut_obj_add_int(doc, freshness, "canonical_nodes_visible",
                           summary.canonical_nodes_visible);
    yyjson_mut_obj_add_int(doc, freshness, "overlay_owned_nodes_visible",
                           summary.overlay_owned_nodes_visible);
    yyjson_mut_obj_add_int(doc, freshness, "total_nodes_visible", summary.total_nodes_visible);
    add_response_warning(doc, root, warning);
    return true;
}

static void add_canonical_only_read_model(yyjson_mut_doc *doc, yyjson_mut_val *root) {
    yyjson_mut_val *freshness = ensure_response_freshness(doc, root);
    if (!freshness || yyjson_mut_obj_get(freshness, CBM_MCP_FRESHNESS_READ_MODEL_KEY)) {
        return;
    }
    yyjson_mut_obj_add_str(doc, freshness, CBM_MCP_FRESHNESS_READ_MODEL_KEY,
                           CBM_MCP_FRESHNESS_READ_MODEL_CANONICAL_ONLY);
}

static bool query_mentions_any(const char *query, const char *const *terms, int term_count) {
    if (!query || !terms || term_count <= 0) {
        return false;
    }
    for (int i = 0; i < term_count; i++) {
        if (terms[i] && cbm_strcasestr(query, terms[i])) {
            return true;
        }
    }
    return false;
}

static bool query_mentions_route_derived_graph(const char *query) {
    static const char *const terms[] = {"Route",        "HANDLES",          "HTTP_CALLS",
                                        "ASYNC_CALLS",  "GRPC_CALLS",       "GRAPHQL_CALLS",
                                        "TRPC_CALLS",   "CROSS_HTTP_CALLS", "CROSS_ASYNC_CALLS",
                                        "CROSS_CHANNEL", "CROSS_GRPC_CALLS", "CROSS_GRAPHQL_CALLS",
                                        "CROSS_TRPC_CALLS"};
    return query_mentions_any(query, terms, (int)(sizeof(terms) / sizeof(terms[0])));
}

static bool query_mentions_semantic_derived_graph(const char *query) {
    static const char *const terms[] = {"SEMANTICALLY_RELATED", "SIMILAR_TO"};
    return query_mentions_any(query, terms, (int)(sizeof(terms) / sizeof(terms[0])));
}

static bool search_graph_uses_route_derived_graph(const char *label, const char *relationship) {
    return (label && strcmp(label, "Route") == 0) || query_mentions_route_derived_graph(relationship);
}

static bool cypher_result_contains_route_label(const cbm_cypher_result_t *result) {
    if (!result || result->col_count <= 0 || result->row_count <= 0) {
        return false;
    }
    for (int c = 0; c < result->col_count; c++) {
        const char *col = result->columns[c];
        if (!col || !strstr(col, ".label")) {
            continue;
        }
        for (int r = 0; r < result->row_count; r++) {
            if (result->rows[r] && result->rows[r][c] &&
                strcmp(result->rows[r][c], "Route") == 0) {
                return true;
            }
        }
    }
    return false;
}

static void add_query_graph_derived_warnings(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                             cbm_store_t *store, const char *project,
                                             const char *query,
                                             const cbm_cypher_result_t *result) {
    if (!doc || !root || !store || !project || !query) {
        return;
    }
    if ((query_mentions_route_derived_graph(query) || cypher_result_contains_route_label(result)) &&
        cbm_store_derived_view_is_stale(store, project, CBM_STORE_DERIVED_VIEW_ROUTES)) {
        add_stale_derived_view_warning(
            doc, root, CBM_STORE_DERIVED_VIEW_ROUTES,
            "routes derived view is stale; query_graph route results may be stale.");
    }
    if (query_mentions_semantic_derived_graph(query) &&
        cbm_store_derived_view_is_stale(store, project, CBM_STORE_DERIVED_VIEW_SEMANTIC_EDGES)) {
        add_stale_derived_view_warning(
            doc, root, CBM_STORE_DERIVED_VIEW_SEMANTIC_EDGES,
            "semantic_edges derived view is stale; query_graph semantic edges may be stale.");
    }
}

/* Default snippet fallback line count (when end_line unknown) */
#define SNIPPET_DEFAULT_LINES 50

/* Default result limit for search_graph and search_code.
 * Prevents unbounded 500K-result responses. Callers can override.
 * Configurable via config key "search_limit". */
#define CBM_MCP_DEFAULT_SEARCH_LIMIT 50

/* Default: rank dependency sub-project symbols (proj.dep.*) LAST so a stdlib
 * symbol like 'Path' never fronts the user's own 'Path'. Tunable off via config
 * key "search_disable_dep_ranking" (true = pure relevance, deps may rank high). */
#define CBM_CONFIG_SEARCH_DISABLE_DEP_RANKING "search_disable_dep_ranking"

/* Default max source lines returned by get_code_snippet.
 * Set to 0 for unlimited. Prevents huge functions from consuming tokens.
 * Configurable via config key "snippet_max_lines". */
#define CBM_DEFAULT_SNIPPET_MAX_LINES 200
#define CBM_CONFIG_SNIPPET_MAX_LINES "snippet_max_lines"
enum {
    CBM_SNIPPET_HEAD_PERCENT = 60,
    CBM_SNIPPET_PERCENT_DENOMINATOR = 100,
};

/* Default max BFS results for trace_path per direction.
 * Configurable via config key "trace_max_results". */
#define CBM_DEFAULT_TRACE_MAX_RESULTS 25
#define CBM_CONFIG_TRACE_MAX_RESULTS "trace_max_results"

/* Default max output bytes for query_graph responses.
 * Caps worst-case at ~8000 tokens. Set to 0 for unlimited.
 * Configurable via config key "query_max_output_bytes". */
#define CBM_DEFAULT_QUERY_MAX_OUTPUT_BYTES 32768
#define CBM_CONFIG_QUERY_MAX_OUTPUT_BYTES "query_max_output_bytes"

/* Idle store eviction: close cached project store after this many seconds
 * of inactivity to free SQLite memory during idle periods. */
#define CBM_MCP_DEFAULT_STORE_IDLE_TIMEOUT_S 60
#define CBM_CONFIG_STORE_IDLE_TIMEOUT_S "store_idle_timeout_s"
/* Read-only DB validation should fail promptly when another process holds a
 * lock; this path is on startup/project discovery and must not hang MCP. */
#define CBM_DB_VALIDATE_BUSY_TIMEOUT_MS 1000
#define CBM_CONFIG_DB_VALIDATE_BUSY_TIMEOUT_MS "db_validate_busy_timeout_ms"
/* Optional background release check. Default preserves the historical 5s curl
 * bound; config value 0 disables the network probe for offline/locked-down MCP. */
#define CBM_MCP_UPDATE_CHECK_TIMEOUT_S 5
#define CBM_CONFIG_UPDATE_CHECK_TIMEOUT_S "update_check_timeout_s"

/* Config key: comma-separated glob patterns to exclude from key_functions.
 * Set via: config set key_functions_exclude "scripts/,tools/,tests/" */
#define CBM_CONFIG_KEY_FUNCTIONS_EXCLUDE "key_functions_exclude"
#define CBM_CONFIG_KEY_FUNCTIONS_COUNT   "key_functions_count"
/* Bound on the key_functions summary PUSHED in the first-response _context
 * header (closes the codebase://architecture pull-only gap). Smaller than the
 * get_architecture default (25) to keep first-response token cost modest. */
#define CBM_CONTEXT_KEY_FUNCTIONS_LIMIT  10
/* Config-tunable override for the _context key_functions push bound.
 * <=0 falls back to the CBM_CONTEXT_KEY_FUNCTIONS_LIMIT default above. */
#define CBM_CONFIG_CONTEXT_KEY_FUNCTIONS_LIMIT "context_key_functions_limit"
#define CBM_CONFIG_ARCH_HOTSPOT_LIMIT    "arch_hotspot_limit"
#define CBM_CONFIG_ARCH_RESOLUTION       "architecture_resolution"

/* Directory permissions: rwxr-xr-x */
#define ADR_DIR_PERMS 0755

/* ── Helpers ────────────────────────────────────────────────────── */

static char *heap_strdup(const char *s) {
    if (!s) {
        return NULL;
    }
    size_t len = strlen(s);
    char *d = malloc(len + SKIP_ONE);
    if (d) {
        memcpy(d, s, len + SKIP_ONE);
    }
    return d;
}

/* Write yyjson_mut_doc to heap-allocated JSON string.
 * ALLOW_INVALID_UNICODE: some database strings may contain non-UTF-8 bytes
 * from older indexing runs — don't fail serialization over it. */
static char *yy_doc_to_str(yyjson_mut_doc *doc) {
    size_t len = 0;
    char *s = yyjson_mut_write(doc, YYJSON_WRITE_ALLOW_INVALID_UNICODE, &len);
    return s;
}

/* ══════════════════════════════════════════════════════════════════
 *  JSON-RPC PARSING
 * ══════════════════════════════════════════════════════════════════ */

int cbm_jsonrpc_parse(const char *line, cbm_jsonrpc_request_t *out) {
    if (!out) {
        return CBM_JSONRPC_INTERNAL_ERROR;
    }
    memset(out, 0, sizeof(*out));
    out->id = CBM_NOT_FOUND;

    if (!line) {
        return CBM_JSONRPC_PARSE_ERROR;
    }

    yyjson_doc *doc = yyjson_read(line, strlen(line), 0);
    if (!doc) {
        return CBM_JSONRPC_PARSE_ERROR;
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    if (!yyjson_is_obj(root)) {
        yyjson_doc_free(doc);
        return CBM_JSONRPC_INVALID_REQUEST;
    }

    yyjson_val *v_jsonrpc = yyjson_obj_get(root, "jsonrpc");
    yyjson_val *v_method = yyjson_obj_get(root, "method");
    yyjson_val *v_id = yyjson_obj_get(root, "id");
    yyjson_val *v_params = yyjson_obj_get(root, "params");

    /* Preserve a valid request ID even when another required member is bad so
     * Invalid Request responses can echo it as required by JSON-RPC 2.0. */
    if (v_id) {
        out->has_id = true;
        if (yyjson_is_int(v_id)) {
            out->id = yyjson_get_int(v_id);
        } else if (yyjson_is_str(v_id)) {
            out->id_str = heap_strdup(yyjson_get_str(v_id));
            if (!out->id_str) {
                out->id_is_null = true;
                yyjson_doc_free(doc);
                return CBM_JSONRPC_INTERNAL_ERROR;
            }
        } else if (yyjson_is_null(v_id)) {
            out->id_is_null = true;
        } else {
            out->id_is_null = true;
            yyjson_doc_free(doc);
            return CBM_JSONRPC_INVALID_REQUEST;
        }
    }

    if (!v_jsonrpc || !yyjson_is_str(v_jsonrpc) || strcmp(yyjson_get_str(v_jsonrpc), "2.0") != 0 ||
        !v_method || !yyjson_is_str(v_method)) {
        yyjson_doc_free(doc);
        return CBM_JSONRPC_INVALID_REQUEST;
    }

    out->jsonrpc = heap_strdup(yyjson_get_str(v_jsonrpc));
    out->method = heap_strdup(yyjson_get_str(v_method));
    if (!out->jsonrpc || !out->method) {
        yyjson_doc_free(doc);
        return CBM_JSONRPC_INTERNAL_ERROR;
    }

    if (v_params) {
        out->params_raw = yyjson_val_write(v_params, 0, NULL);
        if (!out->params_raw) {
            yyjson_doc_free(doc);
            return CBM_JSONRPC_INTERNAL_ERROR;
        }
    }

    yyjson_doc_free(doc);
    return 0;
}

void cbm_jsonrpc_request_free(cbm_jsonrpc_request_t *r) {
    if (!r) {
        return;
    }
    safe_str_free(&r->jsonrpc);
    safe_str_free(&r->method);
    safe_str_free(&r->id_str);
    safe_str_free(&r->params_raw);
    memset(r, 0, sizeof(*r));
}

/* ══════════════════════════════════════════════════════════════════
 *  JSON-RPC FORMATTING
 * ══════════════════════════════════════════════════════════════════ */

char *cbm_jsonrpc_format_response(const cbm_jsonrpc_response_t *resp) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_obj_add_str(doc, root, "jsonrpc", "2.0");
    if (resp->id_is_null) {
        yyjson_mut_obj_add_null(doc, root, "id");
    } else if (resp->id_str) {
        yyjson_mut_obj_add_str(doc, root, "id", resp->id_str);
    } else {
        yyjson_mut_obj_add_int(doc, root, "id", resp->id);
    }

    if (resp->error_json) {
        /* Parse the error JSON and embed */
        yyjson_doc *err_doc = yyjson_read(resp->error_json, strlen(resp->error_json), 0);
        if (err_doc) {
            yyjson_mut_val *err_val = yyjson_val_mut_copy(doc, yyjson_doc_get_root(err_doc));
            yyjson_mut_obj_add_val(doc, root, "error", err_val);
            yyjson_doc_free(err_doc);
        }
    } else if (resp->error_code != 0) {
        yyjson_mut_val *error = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_int(doc, error, "code", resp->error_code);
        yyjson_mut_obj_add_str(doc, error, "message",
                               resp->error_message ? resp->error_message : "Request failed");
        yyjson_mut_obj_add_val(doc, root, "error", error);
    } else if (resp->result_json) {
        /* Parse the result JSON and embed */
        yyjson_doc *res_doc = yyjson_read(resp->result_json, strlen(resp->result_json), 0);
        if (res_doc) {
            yyjson_mut_val *res_val = yyjson_val_mut_copy(doc, yyjson_doc_get_root(res_doc));
            yyjson_mut_obj_add_val(doc, root, "result", res_val);
            yyjson_doc_free(res_doc);
        }
    } else {
        /* JSON-RPC 2.0 spec: response MUST contain "result" or "error" */
        yyjson_mut_obj_add_null(doc, root, "result");
    }

    char *out = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return out;
}

char *cbm_jsonrpc_format_error(int64_t id, int code, const char *message) {
    cbm_jsonrpc_response_t response = {
        .id = id,
        .error_code = code,
        .error_message = message,
    };
    return cbm_jsonrpc_format_response(&response);
}

/* Format an error for an already-parsed request so string and null IDs are
 * preserved through the shared cbm_jsonrpc_response_t error path. */
static char *format_request_error(const cbm_jsonrpc_request_t *req, int code, const char *message) {
    cbm_jsonrpc_response_t response = {
        .id = req ? req->id : 0,
        .id_str = req ? req->id_str : NULL,
        .id_is_null = !req || !req->has_id || req->id_is_null,
        .error_code = code,
        .error_message = message,
    };
    return cbm_jsonrpc_format_response(&response);
}

/* ══════════════════════════════════════════════════════════════════
 *  MCP PROTOCOL HELPERS
 * ══════════════════════════════════════════════════════════════════ */

char *cbm_mcp_text_result(const char *text, bool is_error) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_val *content = yyjson_mut_arr(doc);
    yyjson_mut_val *item = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, item, "type", "text");
    yyjson_mut_obj_add_str(doc, item, "text", text ? text : "");
    yyjson_mut_arr_add_val(content, item);
    yyjson_mut_obj_add_val(doc, root, "content", content);

    bool has_structured_content = false;
    if (text) {
        yyjson_doc *structured_doc = yyjson_read(text, strlen(text), 0);
        if (structured_doc) {
            yyjson_val *structured_root = yyjson_doc_get_root(structured_doc);
            if (yyjson_is_obj(structured_root)) {
                yyjson_mut_val *structured = yyjson_val_mut_copy(doc, structured_root);
                if (structured) {
                    yyjson_mut_obj_add_val(doc, root, "structuredContent", structured);
                    has_structured_content = true;
                }
            }
            yyjson_doc_free(structured_doc);
        }
    }
    if (!has_structured_content) {
        /* All tools advertise an object outputSchema. Preserve text content for
         * model clients while providing a schema-conforming structured object. */
        yyjson_mut_val *structured = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_str(doc, structured, is_error ? "error" : "text", text ? text : "");
        yyjson_mut_obj_add_val(doc, root, "structuredContent", structured);
    }
    yyjson_mut_obj_add_bool(doc, root, "isError", is_error);

    /* Token metadata: helps LLMs gauge context cost before requesting more data.
     * _result_bytes = byte length of the inner JSON text payload.
     * _est_tokens = bytes / 4 (same heuristic as RTK's estimate_tokens). */
    size_t text_len = text ? strlen(text) : 0;
    yyjson_mut_obj_add_int(doc, root, "_result_bytes", (int64_t)text_len);
    yyjson_mut_obj_add_int(doc, root, "_est_tokens", (int64_t)((text_len + 3) / 4));

    char *out = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return out;
}

bool cbm_mcp_cancel_request_matches(const char *params_json, int64_t active_id,
                                    const char *active_id_str) {
    if (!params_json) {
        return false;
    }

    yyjson_doc *doc = yyjson_read(params_json, strlen(params_json), 0);
    if (!doc) {
        return false;
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *request_id = yyjson_obj_get(root, "requestId");
    bool matches = false;
    if (request_id) {
        if (active_id_str) {
            matches =
                yyjson_is_str(request_id) && strcmp(yyjson_get_str(request_id), active_id_str) == 0;
        } else {
            matches = yyjson_is_int(request_id) && yyjson_get_int(request_id) == active_id;
        }
    }

    yyjson_doc_free(doc);
    return matches;
}

/* ── Tool definitions ─────────────────────────────────────────── */

typedef struct {
    const char *name;
    const char *title;
    const char *description;
    const char *input_schema; /* JSON string */
} tool_def_t;

static const tool_def_t TOOLS[] = {
    {"index_repository", "Index repository",
     "Index a repository into the knowledge graph. Use for explicit indexing or pre-warming; "
     "graph-backed default tools (search_graph, query_graph, trace_path, get_code) can "
     "auto-index the server CWD or explicit directory paths on first use when auto_index "
     "is enabled. "
     "Special mode 'cross-repo-intelligence': skip extraction, only match Routes/Channels "
     "across projects to create CROSS_HTTP_CALLS/CROSS_ASYNC_CALLS/CROSS_CHANNEL edges. "
     "Requires target_projects param. Ensure target projects have fresh indexes first. "
     "COVERAGE: the response reports files that were NOT fully indexed — 'skipped' (not "
     "indexed at all: oversized/read/parse failures) and 'parse_partial' (indexed, but "
     "constructs inside the listed line ranges could not be parsed and MAY be missing from "
     "the graph). Query the persisted signal any time via index_status or "
     "structurally via query_graph(graph=\"missed\"). Both signals are best-effort: absence "
     "of a flag is NOT a completeness guarantee; prefer grep inside flagged ranges. "
     "Separately, 'excluded' + 'not_indexed_files' list what was deliberately NOT indexed "
     "(gitignore/.cbmignore/skip-lists) — by design, not failures.",
     "{\"type\":\"object\",\"properties\":{\"repo_path\":{\"type\":\"string\",\"description\":"
     "\"Path to the repository\"},"
     "\"mode\":{\"type\":\"string\","
     "\"enum\":[\"full\",\"moderate\",\"fast\",\"cross-repo-intelligence\"],"
     "\"default\":\"full\",\"description\":\"All modes run type-aware LSP call/usage "
     "resolution (per-file + cross-file). full: all files + similarity/semantic edges. "
     "moderate: filtered files + similarity/semantic. fast: filtered files, no "
     "similarity/semantic. cross-repo-intelligence: match Routes/Channels across projects.\"},"
     "\"target_projects\":{\"type\":\"array\",\"items\":{\"type\":\"string\"},"
     "\"description\":\"Projects to search for cross-repo links (cross-repo-intelligence mode). "
     "Use [\\\"*\\\"] for all indexed projects. Run list_projects to see available projects.\"},"
     "\"auto_index_deps\":{\"type\":\"boolean\",\"description\":"
     "\"Set false to skip dependency package indexing for this call. Default follows config auto_index_deps.\"},"
     "\"auto_dep_limit\":{\"type\":\"integer\",\"description\":"
     "\"Dependency package cap for this call. Default follows config auto_dep_limit; 0 means unlimited.\"},"
     "\"name\":{\"type\":\"string\",\"description\":"
     "\"Override the derived project name. Non-ASCII bytes are encoded and unsafe path characters "
     "are normalized.\"},"
     "\"persistence\":{\"type\":\"boolean\",\"default\":false,\"description\":"
     "\"Write compressed artifact to .codebase-memory/graph.db.zst for team sharing. "
     "Teammates can bootstrap from the artifact instead of full re-indexing.\"}"
     ",\"format\":{\"type\":\"string\",\"enum\":[\"toon\",\"json\"],\"default\":\"toon\","
     "\"description\":\"Compact TOON by default; json returns legacy objects. Omit to use "
     "default_response_format.\"}"
     "},\"required\":[\"repo_path\"]}"},

    {"search_graph", "Search graph",
     "Search the code knowledge graph for functions, classes, routes, and variables. Prefer this "
     "over grep/glob for code definitions, implementations, or relationships. Auto-indexes "
     "the server CWD or an explicit directory project on first use when enabled. "
     "Returns structured results in one call. "
     "When has_more=true, use offset+limit to paginate. "
     "Use mode=summary for quick codebase overview without individual results.",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\",\"description\":"
     "\"Indexed project name or repository directory. Omit to use the MCP server project derived "
     "from server CWD; first use may auto-index it.\"},\"label\":{\"type\":\"string\",\"description\":\"Node label filter, "
     "for example Function, Class, Method, Route, or File.\"},\"name_pattern\":{\"type\":\"string\",\"description\":\"Regex pattern on symbol "
     "name. Glob wildcards (*tool*, foo?) auto-convert to regex.\"},"
     "\"pattern\":{\"type\":\"string\",\"description\":\"Regex or glob pattern matched against "
     "symbol name OR qualified_name. Use for broad symbol lookup.\"},"
     "\"qn_pattern\":{\"type\":\"string\",\"description\":\"Regex pattern on qualified name. "
     "Glob wildcards auto-convert to regex.\"},"
     "\"query\":{\"type\":\"string\",\"description\":\"Full-text/BM25 query over indexed symbol "
     "text. Use when searching by words rather than symbol-name regex. When set, normal results "
     "come from BM25; only project, file_pattern, limit, and offset apply. Graph filters and "
     "sort_by are ignored. semantic_query still appends separate semantic_results.\"},"
     "\"semantic_query\":{\"type\":\"array\",\"items\":{\"type\":\"string\"},\"description\":"
     "\"Array of keyword strings for vector search, e.g. [\\\"send\\\",\\\"pubsub\\\"]. "
     "Appends separate semantic_results when matching vectors exist; query/name filters control "
     "only normal results.\"},"
     "\"file_pattern\":{\"type\":\"string\",\"description\":\"Glob or substring filter on result "
     "file paths.\"},\"relationship\":{\"type\":\"string\",\"description\":\"Graph edge type to "
     "filter connected results, for example CALLS or IMPORTS.\"},"
     "\"case_sensitive\":{\"type\":\"boolean\",\"default\":false,\"description\":\"Apply name/"
     "qualified-name pattern matching case-sensitively.\"},\"min_degree\":"
     "{\"type\":\"integer\",\"description\":\"Minimum total in+out graph degree.\"},\"max_degree\":"
     "{\"type\":\"integer\",\"description\":\"Maximum total in+out graph degree.\"},"
     "\"exclude_entry_points\":{\"type\":\"boolean\",\"description\":\"Omit likely entry-point "
     "nodes when looking for implementation internals.\"},\"include_connected\":{\"type\":"
     "\"boolean\",\"description\":\"Include directly connected symbols for each match.\"},\"limit\":{\"type\":"
     "\"integer\",\"description\":\"Max results per page (configurable via search_limit config key). "
     "Response includes has_more and pagination_hint when more pages exist."
     "\"},\"offset\":{\"type\":\"integer\",\"default\":0,\"description\":\"Skip N results "
     "for pagination. Check pagination_hint in response for next page offset.\"},"
     "\"sort_by\":{\"type\":\"string\",\"enum\":[\"relevance\",\"name\",\"degree\",\"calls\",\"linkrank\"],"
     "\"description\":\"Sort order: relevance (PageRank structural importance, default), "
     "name (alphabetical), degree (most connected by edge weight), "
     "calls (most direct function calls in+out), linkrank (link-based rank score).\"},"
     "\"mode\":{\"type\":\"string\",\"enum\":[\"full\",\"summary\"],\"default\":\"full\","
     "\"description\":\"full=individual results (default), summary=aggregate counts by label and "
     "file. Use summary first to understand scope, then full with filters to drill down."
     "\"},\"summary\":{\"type\":\"boolean\",\"default\":false,\"description\":\"Alias for "
     "mode=summary. Kept for concise prompts; ignored when mode is set."
     "\"},\"compact\":{\"type\":\"boolean\",\"default\":true,\"description\":\"Per-call override for the compact config key "
     "(true by default). Omit fields at their "
     "default: name when it equals qualified_name's last segment (e.g. \\\"main\\\" in "
     "\\\"pkg.main\\\"), empty label/file_path, and zero degrees. Absent fields assume defaults: "
     "label/file_path='', degree=0. Node properties are omitted unless selected with fields; "
     "compact=false includes non-internal properties. Saves tokens.\"},"
     "\"include_dependencies\":{\"type\":\"boolean\",\"default\":true,\"description\":\"Include "
     "symbols from dependency sub-projects (marked source=dependency in results). Set false to "
     "scope to project code only. When true, project symbols rank above dependency symbols by "
     "default (config key search_disable_dep_ranking=true reverts to pure relevance).\"},"
     "\"exclude\":{\"type\":\"array\",\"items\":{\"type\":\"string\"},\"description\":\"Glob "
     "patterns for file paths to exclude from results (e.g. [\\\"tests/**\\\",\\\"scripts/**\\\"])."
     "\"},\"format\":{\"type\":\"string\",\"enum\":[\"toon\",\"json\"],\"default\":\"toon\","
     "\"description\":\"Compact TOON tables by default; json returns legacy objects.\"},"
     "\"fields\":{\"type\":\"array\",\"items\":{\"type\":\"string\"},\"description\":"
     "\"Selected node-property fields for compact TOON or JSON output, e.g. complexity or "
     "signature. Internal fp/sp/bt indexing fields are never returned.\"}}}"},

    {"query_graph", "Query graph",
     "Core compositional tool; use it early for bespoke structural questions. Create new, "
     "effective, computationally efficient custom Cypher for multi-hop paths, aggregates/hotspots, "
     "arbitrary predicates, cross-service links, or graph=\"missed\". Non-exhaustive examples: "
     "MATCH (f:Function)-[:CALLS]->(g) RETURN f.name,g.name LIMIT 20; "
     "MATCH (f:File) RETURN f.file_path,count(*). Any supported query shape is allowed; "
     "WITH can feed later MATCH/OPTIONAL MATCH stages; ORDER BY accepts multiple projected "
     "fields or aliases. Explicit labels/properties and LIMIT are optional efficiency aids. "
     "Server caps query_max_rows and "
     "query_max_output_bytes; raise only when needed. Dependency symbols use proj.dep.* and "
     "source:dependency; filter them with a supported predicate such as "
     "WHERE n.project =~ '.*\\.dep\\..*'. "
     "Supported read-only Cypher subset: MATCH/OPTIONAL MATCH, WHERE, WITH, UNWIND, RETURN, "
     "DISTINCT, ORDER BY, SKIP, LIMIT, UNION/UNION ALL; node and relationship patterns; bounded "
     "variable-length paths; property access, comparisons, regex, IN, IS NULL, EXISTS, boolean "
     "logic, CASE; count/sum/avg/min/max/collect and supported scalar functions. Unsupported "
     "syntax returns an error with a supported rewrite.",
     "{\"type\":\"object\",\"properties\":{\"query\":{\"type\":\"string\",\"description\":\"Cypher "
     "query\"},\"project\":{\"type\":\"string\",\"description\":\"Indexed project name. Omit to "
     "use the MCP server project derived from server CWD.\"},\"max_rows\":{\"type\":\"integer\","
     "\"description\":\"Maximum result rows. Omit to use query_max_rows config; set 0 to use "
     "the implementation ceiling. Matching, aggregation, and ordering remain exact before this "
     "output cap. A Cypher LIMIT can lower but not bypass the cap. For response bytes, set "
     "max_output_bytes.\"},"
     "\"max_output_bytes\":{\"type\":"
     "\"integer\",\"description\":\"Max response size in bytes (configurable via "
     "query_max_output_bytes config key). Set to 0 for unlimited. When exceeded, returns "
     "truncated=true with total_bytes and optional ways to narrow the query or raise the cap.\"},"
     "\"graph\":{\"type\":\"string\","
     "\"enum\":[\"code\",\"missed\"],\"default\":\"code\",\"description\":\"Query the code "
     "graph or the best-effort graph of files not fully indexed.\"},\"format\":{\"type\":\"string\","
     "\"enum\":[\"toon\",\"json\"],\"default\":\"toon\",\"description\":\"Compact TOON rows by "
     "default; json returns legacy objects. Omit to use default_response_format.\"}},"
     "\"required\":[\"query\"]}"},

    {"trace_path", "Trace path",
     "Trace function call paths: who calls a function and what it calls. Prefer this for callers, "
     "dependencies, or impact analysis. Auto-indexes the project on first use "
     "when enabled. "
     "Pass qualified_name from search_graph when available; otherwise pass function_name. "
     "All other params are optional defaults. Results are deduplicated and show candidates "
     "when a name is ambiguous.",
     "{\"type\":\"object\",\"properties\":{\"function_name\":{\"type\":\"string\","
     "\"description\":\"Function name to trace when qualified_name is unavailable. Exact match "
     "first, then case-insensitive fallback."
     "\"},\"qualified_name\":{\"type\":\"string\",\"description\":\"Exact qualified name from search "
     "results. Prefer this for cross-tool chaining and disambiguation.\"},\"project\":{"
     "\"type\":\"string\",\"description\":\"Indexed project name or repository directory. Omit to "
     "use the MCP server project derived from server CWD; first use may auto-index it.\"},\"direction\":{\"type\":\"string\",\"enum\":[\"inbound\",\"outbound\","
     "\"both\"],\"default\":\"both\",\"description\":\"Trace callers (inbound), callees "
     "(outbound), or both.\"},\"depth\":{\"type\":\"integer\",\"default\":3,\"description\":"
     "\"Maximum graph hops to traverse from the start function.\"},\"max_results"
     "\":{\"type\":\"integer\",\"description\":\"Max nodes per direction (configurable via "
     "trace_max_results config key). This is also the default page size when limit is omitted. "
     "Response includes callees_total/callers_total for truncation awareness.\"},\"limit\":{"
     "\"type\":\"integer\",\"minimum\":1,\"maximum\":5000,\"description\":\"Rows returned "
     "per page. Overrides max_results as the page size; use next as cursor to continue without "
     "duplicates.\"},\"cursor\":{\"type\":\"string\",\"description\":\"Opaque next token from "
     "a prior trace_path response. Keep every other traversal argument identical.\"},\"compact\":{\"type\":\"boolean\","
     "\"default\":true,\"description\":"
     "\"Per-call override for the compact config key (true by default). Omit name when it equals qualified_name's last segment (e.g. \\\"main\\\" in \\\"pkg.main\\\"). Reduces token count.\"},"
     "\"mode\":{\"type\":\"string\",\"enum\":[\"calls\",\"data_flow\",\"cross_service\"],"
     "\"default\":\"calls\",\"description\":\"Default edge set when edge_types is omitted: "
     "calls follows CALLS, data_flow follows CALLS+DATA_FLOWS, cross_service follows "
     "service-boundary edge types.\"},"
     "\"edge_types\":{\"type\":\"array\",\"items\":{\"type\":\"string\"},"
     "\"description\":\"Optional exact graph edge types to traverse. Defaults come from mode.\"},"
     "\"exclude\":{\"type\":\"array\",\"items\":{\"type\":\"string\"},"
     "\"description\":\"Optional file-path globs to omit, e.g. tests/** or vendor/**."
     "\"},\"include_tests\":{\"type\":\"boolean\",\"default\":false,"
     "\"description\":\"Include test/spec file nodes in trace results and mark them with is_test.\"},"
     "\"risk_labels\":{\"type\":\"boolean\",\"default\":false,"
     "\"description\":\"Annotate traced nodes with CRITICAL/HIGH/MEDIUM/LOW risk by hop distance.\"},"
     "\"parameter_name\":{\"type\":\"string\",\"description\":\"Accepted for upstream compatibility; "
     "reserved for future parameter-level data-flow narrowing."
     "\"},\"format\":{\"type\":\"string\",\"enum\":[\"toon\",\"json\"],\"default\":\"toon\","
     "\"description\":\"Compact TOON tables by default; json returns legacy hop objects.\"}},"
     "\"description\":\"Pass function_name OR qualified_name (at least one required).\"}"},

    {"get_code_snippet", "Get code snippet",
     "Get source code for a specific function, class, or symbol by qualified name. Prefer this over "
     "reading entire files when you need one function's implementation. Use mode=signature for "
     "API lookup without the source body. Use mode=head_tail for large functions to see both "
     "the signature and return/cleanup code. When truncated=true, set max_lines=0 for full source.",
     "{\"type\":\"object\",\"properties\":{\"qualified_name\":{\"type\":\"string\",\"description\":"
     "\"Exact qualified name from search_graph results.\"},\"project\":{"
     "\"type\":\"string\",\"description\":\"Indexed project name. Omit to use the MCP server "
     "project derived from server CWD.\"},\"auto_resolve\":{\"type\":\"boolean\",\"default\":false,\"description\":"
     "\"Auto-pick best match when name is ambiguous (by degree). Shows alternatives in response."
     "\"},\"include_neighbors\":{\"type\":\"boolean\",\"default\":false,\"description\":\"Include "
     "caller/callee names (up to 10 each). Adds context but increases response size.\"},"
     "\"compact\":{\"type\":\"boolean\",\"default\":true,\"description\":\"Per-call override "
     "for the compact config key (true by default). Omit name when it equals the last segment "
     "of qualified_name.\"},"
     "\"max_lines\":{\"type\":\"integer\",\"description\":\"Max source lines "
     "(configurable via snippet_max_lines config key). Set to 0 for unlimited. When truncated, "
     "response includes total_lines and signature for context.\"},\"mode\":{\"type\":\"string\",\"enum\":[\"full\",\"signature\","
     "\"head_tail\"],\"default\":\"full\",\"description\":\"full=source up to max_lines, "
     "signature=API signature+params+return type only (no source body), "
     "head_tail=first 60%% + last 40%% of max_lines with omission marker (preserves return/"
     "cleanup code)\"}},\"required\":[\"qualified_name\"]}"},

    {"get_graph_schema", "Get graph schema",
     "Get the schema of the knowledge graph (node labels, edge types)",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\",\"description\":"
     "\"Indexed project name or repository directory. Omit to use the MCP server project derived "
     "from server CWD; first use may auto-index it.\"},\"format\":{\"type\":\"string\","
     "\"enum\":[\"toon\",\"json\"],\"default\":\"toon\",\"description\":"
     "\"Compact TOON schema by default; json preserves the legacy object shape. Property keys "
     "and observed relationship patterns report their discovery bounds. Omit to use "
     "default_response_format.\"}}}"},

    {"get_architecture", "Get architecture",
     "Get high-level architecture overview: packages, services, dependencies, and project "
     "structure at a glance. Includes 'clusters': Leiden community detection over the call/import "
     "graph, surfacing the de-facto modules (each with a label, member count, cohesion score, "
     "representative top_nodes, and the packages/edge_types that bind it). Use these to inspect "
     "actual dependency-based module boundaries, which may differ from the folder layout.",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\",\"description\":"
     "\"Indexed project name or repository directory. Omit to use the MCP server project derived "
     "from server CWD; first use may auto-index it.\"},\"path\":{\"type\":\"string\",\"description\":"
     "\"Optional relative directory/file prefix to scope architecture counts and sections, e.g. "
     "src/server. Leading ./, leading slash, trailing slash, and backslashes are normalized.\"},"
     "\"aspects\":{\"type\":\"array\",\"items\":{\"type\":\"string\",\"enum\":[\"all\","
     "\"overview\",\"structure\",\"dependencies\",\"routes\",\"languages\",\"packages\","
     "\"entry_points\",\"hotspots\",\"boundaries\",\"layers\",\"file_tree\",\"clusters\","
     "\"cycles\"]},"
     "\"description\":\"Optional validated sections to include; omit for the default overview.\"},"
     "\"exclude\":{\"type\":\"array\",\"items\":{\"type\":\"string\"},\"description\":\"Optional "
     "file-path globs to omit from key_functions, e.g. tests/** or vendor/**.\"},"
     "\"format\":{\"type\":\"string\",\"enum\":[\"toon\",\"json\"],\"default\":\"toon\","
     "\"description\":\"Compact TOON sections by default; json returns legacy objects. Omit to "
     "use default_response_format.\"}}}"},

    {"search_code", "Search code",
     "Search source code in an indexed/current project with text or regex patterns. "
     "Does not index projects; use search_graph or index_repository first. "
     "Case-insensitive by default. "
     "Use for string literals, error messages, and config values not in the knowledge graph. "
     "Use file_pattern to narrow traversal; path_filter filters result paths and can fast-scope "
     "anchored literal file regexes.",
     "{\"type\":\"object\",\"properties\":{\"pattern\":{\"type\":\"string\",\"description\":"
     "\"Text or regex to search for.\"},\"project\":{\"type\":"
     "\"string\",\"description\":\"Indexed project name. Omit to use the current MCP "
     "server project after it has been indexed.\"},\"file_pattern\":{\"type\":\"string\",\"description\":\"Glob for grep "
     "--include; use this to reduce traversal (e.g. *.go).\"},\"path_filter\":{\"type\":\"string\",\"description\":\"Regex "
     "filter on result file paths; anchored literal file regexes such as ^src/main\\\\.go$ "
     "search only that file.\"},"
     "\"regex\":{\"type\":\"boolean\",\"default\":false,\"description\":\"Treat pattern as a "
     "regular expression instead of literal text.\"},"
     "\"case_sensitive\":{\"type\":\"boolean\",\"default\":false,"
     "\"description\":\"Match case-sensitively (default: case-insensitive).\"},"
     "\"context\":{\"type\":\"integer\",\"default\":0,"
     "\"description\":\"Number of surrounding lines to include around each match (like grep -C). Default 0.\"},"
     "\"mode\":{\"type\":\"string\",\"enum\":[\"compact\",\"full\",\"files\"],\"default\":\"compact\","
     "\"description\":\"compact=deduplicated matches, full=include source snippets, files=matching files only.\"},"
     "\"limit\":{\"type\":\"integer\",\"description\":\"Max "
     "results (configurable via search_limit config key). Set higher for exhaustive text search."
     "\"},\"format\":{\"type\":\"string\",\"enum\":[\"toon\",\"json\"],\"default\":\"toon\","
     "\"description\":\"Compact TOON matches by default; json returns legacy objects. Omit to "
     "use default_response_format.\"}},\"required\":["
     "\"pattern\"]}"},

    {"list_projects", "List projects",
     "List indexed projects in stable bounded pages. Follow next_offset while has_more=true; "
     "all=true restores the legacy unbounded inventory only when explicitly needed.",
     "{\"type\":\"object\",\"properties\":{"
     "\"limit\":{\"type\":\"integer\",\"default\":50,\"minimum\":1,\"maximum\":200,"
     "\"description\":\"Projects per page (default 50, maximum 200).\"},"
     "\"offset\":{\"type\":\"integer\",\"default\":0,\"minimum\":0,"
     "\"description\":\"Skip this many valid projects; use next_offset from the prior page.\"},"
     "\"all\":{\"type\":\"boolean\",\"default\":false,"
     "\"description\":\"Explicit compatibility path returning the full inventory; ignores "
     "limit/offset and may be slow or large.\"}}}"},

    {"delete_project", "Delete project", "Delete a project from the index",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\",\"description\":"
     "\"Indexed project name to delete.\"}},\"required\":["
     "\"project\"]}"},

    {"index_status", "Index status",
     "Report project index freshness, graph counts, overlay read-view counts, and background "
     "overlay compaction state. Git metadata labels HEAD as committed-only and reports whether "
     "the current working tree is dirty.",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\",\"description\":"
     "\"Indexed project name to inspect.\"},\"verbose\":{\"type\":\"boolean\",\"default\":false,"
     "\"description\":\"Include repository path and Git identity/freshness "
     "metadata.\"}},\"required\":["
     "\"project\"]}"},

    {"check_index_coverage", "Check index coverage",
     "Check authoritative indexing-coverage metadata for exact repository-relative paths and "
     "bounded path scopes. Use this after graph discovery for every cited or operated-on file; "
     "use scopes before negative or exhaustive claims because fully skipped files cannot appear "
     "in normal graph results. Returns coverage status separately from filesystem metadata "
     "freshness, plus structured parse-error ranges and direct-source fallback actions. The "
     "signal is best-effort: indexed_no_recorded_gap is not a completeness guarantee.",
     "{\"type\":\"object\",\"properties\":{"
     "\"project\":{\"type\":\"string\"},"
     "\"paths\":{\"type\":\"array\",\"items\":{\"type\":\"string\"},\"maxItems\":128,"
     "\"description\":\"Repository-relative files to check exactly.\"},"
     "\"scopes\":{\"type\":\"array\",\"items\":{\"type\":\"string\"},\"maxItems\":32,"
     "\"description\":\"Repository-relative path prefixes; use . for the project root.\"},"
     "\"scope_limit\":{\"type\":\"integer\",\"default\":200,\"minimum\":1,\"maximum\":1000},"
     "\"scope_offset\":{\"type\":\"integer\",\"default\":0,\"minimum\":0}},"
     "\"required\":[\"project\"],\"anyOf\":[{\"required\":[\"paths\"]},"
     "{\"required\":[\"scopes\"]}]}"},

    {"detect_changes", "Detect changes",
     "Map a git diff to its blast radius with one multi-source graph traversal. Returns the exact "
     "diff base and merge-base SHA, changed files, transitive impacted symbols, and a complete "
     "module rollup. Changed seed symbols are excluded from impact. TOON is compact; JSON returns "
     "the same data model. impacted_total and truncated make bounds explicit.",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"},\"scope\":{\"type\":"
     "\"string\",\"enum\":[\"files\",\"impact\"],\"description\":\"files returns only changed "
     "files; impact (default) also traverses the graph.\"},\"direction\":{\"type\":\"string\","
     "\"enum\":[\"inbound\",\"outbound\",\"both\"],\"default\":\"inbound\",\"description\":"
     "\"inbound finds transitive callers (blast radius); outbound finds dependencies; both returns "
     "their union.\"},\"depth\":{\"type\":\"integer\",\"default\":2,\"description\":\"Maximum "
     "traversal hops from changed symbols.\"},\"limit\":{\"type\":\"integer\",\"default\":200,"
     "\"minimum\":1,\"maximum\":5000,\"description\":\"Impacted symbol rows shown. Exact totals "
     "and the module rollup remain available when rows are truncated.\"},\"base_branch\":{\"type\":"
     "\"string\",\"default\":\"main\"},\"since\":{\"type\":\"string\",\"description\":"
     "\"Git ref or tag to compare from; takes precedence over base_branch.\"},\"format\":{\"type\":"
     "\"string\",\"enum\":[\"toon\",\"json\"],\"description\":\"Omit to use "
     "default_response_format.\"}},\"required\":[\"project\"]}"},

    {"manage_adr", "Manage ADR", "Create or update Architecture Decision Records",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\",\"description\":"
     "\"Indexed project name whose ADR data should be read or updated.\"},\"mode\":{\"type\":"
     "\"string\",\"enum\":[\"get\",\"update\",\"sections\"],\"description\":\"get returns ADRs, "
     "update writes content, sections returns selected sections.\"},\"content\":{\"type\":\"string\","
     "\"description\":\"ADR markdown/content for update mode.\"},"
     "\"sections\":{\"type\":\"array\",\"items\":{\"type\":\"string\"},\"description\":\"Section "
     "names to return in sections mode.\"}},\"required\":[\"project\"]"
     "}"},

    {"ingest_traces", "Ingest traces",
     "Accept runtime trace events and report the event count. Graph edge creation from traces is "
     "not yet implemented.",
     "{\"type\":\"object\",\"properties\":{\"traces\":{\"type\":\"array\",\"items\":{\"type\":"
     "\"object\",\"properties\":{\"caller\":{\"type\":\"string\"},\"callee\":{\"type\":"
     "\"string\"},\"count\":{\"type\":\"integer\"}},\"additionalProperties\":false},"
     "\"description\":\"Runtime trace events to merge into the graph.\"},\"project\":{\"type\":"
     "\"string\",\"description\":\"Indexed project name receiving the trace data.\"}},\"required\":[\"traces\",\"project\"]}"},

    {"index_dependencies", "Index dependencies",
     "Index dependency/library source for API reference. Works with supported languages when "
     "source_paths point to local source trees. "
     "Deps stored with {project}.dep.{name} project names, tagged source:dependency in results. "
     "PRIMARY: Use source_paths (works for all languages). "
     "SHORTCUT: package_manager auto-resolves paths for uv/cargo/npm/bun.",
     "{\"type\":\"object\",\"properties\":{"
     "\"project\":{\"type\":\"string\",\"description\":\"Existing indexed project to add deps to\"},"
     "\"source_paths\":{\"type\":\"array\",\"items\":{\"type\":\"string\"},"
     "\"description\":\"Dep source directories, paired 1:1 with packages[]. Any language.\"},"
     "\"packages\":{\"type\":\"array\",\"items\":{\"type\":\"string\"},"
     "\"description\":\"Dep names, paired 1:1 with source_paths[]. "
     "Creates {project}.dep.{name} in the graph.\"},"
     "\"package_manager\":{\"type\":\"string\","
     "\"description\":\"Auto-resolve source_paths for installed packages. "
     "Supported: uv/pip/cargo/npm/bun. Errors include source_path hints.\"},"
     "\"public_only\":{\"type\":\"boolean\",\"default\":true,"
     "\"description\":\"Index only exported/public symbols\"}"
     "},\"required\":[\"project\",\"packages\"]}"},
};

static const int TOOL_COUNT = sizeof(TOOLS) / sizeof(TOOLS[0]);

/* ── Streamlined-only tool definitions ──────────────────────────
 * Canonical tools such as search_graph, query_graph, search_code, and trace_path
 * are emitted from TOOLS[] in every mode so their schemas cannot drift between
 * classic and streamlined surfaces. This array holds fork-only concise aliases. */

static const tool_def_t STREAMLINED_TOOLS[] = {
    {"get_code", "Get code",
     "Get source code for a function, class, or symbol by qualified name. "
     "Prefer this over reading entire files. Use mode=signature for API lookup without source body. "
     "Use mode=head_tail for large functions (preserves return code). "
     "Module nodes return metadata only. Use auto_resolve=true only for ambiguous names. "
     "Get qualified_name values from search_graph results.",
     "{\"type\":\"object\",\"properties\":{"
     "\"qualified_name\":{\"type\":\"string\",\"description\":\"Exact qualified_name from "
     "search_graph results. Short or suffix names resolve only when unambiguous.\"},"
     "\"project\":{\"type\":\"string\",\"description\":\"Indexed project name. Omit to use the "
     "MCP server project derived from server CWD.\"},"
     "\"mode\":{\"type\":\"string\",\"enum\":[\"full\",\"signature\",\"head_tail\"],"
     "\"default\":\"full\",\"description\":\"full=source up to max_lines, signature=API only, "
     "head_tail=first and last lines for large functions.\"},"
     "\"max_lines\":{\"type\":\"integer\",\"description\":\"Max source lines (configurable via "
     "snippet_max_lines config key). Set to 0 for unlimited.\"},"
     "\"auto_resolve\":{\"type\":\"boolean\",\"default\":false,\"description\":\"Auto-pick the "
     "best ambiguous match; prefers non-test files, then highest graph degree.\"},"
     "\"include_neighbors\":{\"type\":\"boolean\",\"default\":false,\"description\":\"Include "
     "caller/callee names for local context.\"},"
     "\"compact\":{\"type\":\"boolean\",\"default\":true,"
     "\"description\":\"Per-call override for the compact config key (true by default). Omit name when it equals last segment of qualified_name.\"}"
     "},\"required\":[\"qualified_name\"]}"},
};
static const int STREAMLINED_TOOL_COUNT = sizeof(STREAMLINED_TOOLS) / sizeof(STREAMLINED_TOOLS[0]);

static const char MCP_TOOL_OUTPUT_SCHEMA[] =
    "{\"type\":\"object\",\"additionalProperties\":true}";
static const char MCP_HIDDEN_TOOL_INPUT_SCHEMA[] = "{\"type\":\"object\",\"properties\":{}}";

typedef struct {
    const char *name;
    bool read_only;
    bool destructive;
    bool idempotent;
    bool open_world;
} tool_annotation_def_t;

/* Keep protocol behavior hints tied to the canonical tool names. Query tools
 * use read-only store handles; indexing and maintenance tools are marked as
 * mutations even though they do not modify repository source files. */
static const tool_annotation_def_t TOOL_ANNOTATIONS[] = {
    {"index_repository", false, false, true, false},
    {"search_graph", true, false, true, false},
    {"query_graph", true, false, true, false},
    {"trace_path", true, false, true, false},
    {"get_code_snippet", true, false, true, false},
    {"get_code", true, false, true, false},
    {"get_graph_schema", true, false, true, false},
    {"get_architecture", true, false, true, false},
    {"search_code", true, false, true, false},
    {"list_projects", true, false, true, false},
    {"delete_project", false, true, true, false},
    {"index_status", true, false, true, false},
    {"check_index_coverage", true, false, true, false},
    {"detect_changes", true, false, true, false},
    {"manage_adr", false, false, false, false},
    {"ingest_traces", false, false, false, false},
    {"index_dependencies", false, false, true, false},
};

static const tool_annotation_def_t *mcp_tool_annotations(const char *name) {
    size_t count = sizeof(TOOL_ANNOTATIONS) / sizeof(TOOL_ANNOTATIONS[0]);
    for (size_t i = 0; i < count; i++) {
        if (strcmp(TOOL_ANNOTATIONS[i].name, name) == 0) {
            return &TOOL_ANNOTATIONS[i];
        }
    }
    return NULL;
}

static void mcp_add_json_schema(yyjson_mut_doc *doc, yyjson_mut_val *obj, const char *key,
                                const char *schema_json) {
    if (!schema_json) {
        return;
    }
    yyjson_doc *schema_doc = yyjson_read(schema_json, strlen(schema_json), 0);
    if (!schema_doc) {
        return;
    }
    yyjson_mut_val *schema = yyjson_val_mut_copy(doc, yyjson_doc_get_root(schema_doc));
    if (schema) {
        yyjson_mut_obj_add_val(doc, obj, key, schema);
    }
    yyjson_doc_free(schema_doc);
}

/* MCP inputs are closed globally: an undeclared key is almost always a typo
 * that would otherwise leave a real option at its default and return a
 * plausible but wrong result. Keep the property/required definitions in the
 * canonical registry and apply this policy during serialization rather than
 * duplicating it in every schema literal. Runtime dispatch enforces the same
 * policy for raw-JSON CLI and clients that do not validate tools/list. */
static void mcp_add_tool_input_schema(yyjson_mut_doc *doc, yyjson_mut_val *obj,
                                      const char *schema_json) {
    if (!schema_json) {
        return;
    }
    yyjson_doc *schema_doc = yyjson_read(schema_json, strlen(schema_json), 0);
    if (!schema_doc) {
        return;
    }
    yyjson_mut_val *schema = yyjson_val_mut_copy(doc, yyjson_doc_get_root(schema_doc));
    if (schema && yyjson_mut_is_obj(schema)) {
        (void)yyjson_mut_obj_remove_key(schema, "additionalProperties");
        yyjson_mut_obj_add_bool(doc, schema, "additionalProperties", false);
        yyjson_mut_obj_add_val(doc, obj, "inputSchema", schema);
    }
    yyjson_doc_free(schema_doc);
}

/* Canonical tool serialization for classic, streamlined, and paged lists. */
static void emit_tool(yyjson_mut_doc *doc, yyjson_mut_val *tools, const tool_def_t *tool_def,
                      const char *description_override) {
    yyjson_mut_val *tool = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, tool, "name", tool_def->name);
    yyjson_mut_obj_add_str(doc, tool, "title",
                           tool_def->title ? tool_def->title : tool_def->name);
    yyjson_mut_obj_add_str(doc, tool, "description",
                           description_override ? description_override : tool_def->description);
    mcp_add_tool_input_schema(doc, tool, tool_def->input_schema);
    mcp_add_json_schema(doc, tool, "outputSchema", MCP_TOOL_OUTPUT_SCHEMA);
    const tool_annotation_def_t *def = mcp_tool_annotations(tool_def->name);
    yyjson_mut_val *annotations = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_bool(doc, annotations, "readOnlyHint", def ? def->read_only : false);
    yyjson_mut_obj_add_bool(doc, annotations, "destructiveHint", def ? def->destructive : true);
    yyjson_mut_obj_add_bool(doc, annotations, "idempotentHint", def ? def->idempotent : false);
    yyjson_mut_obj_add_bool(doc, annotations, "openWorldHint", def ? def->open_world : true);
    yyjson_mut_obj_add_val(doc, tool, "annotations", annotations);
    yyjson_mut_arr_add_val(tools, tool);
}

static bool is_streamlined_default_tool(const char *name) {
    /* query_graph is intentionally a core tool, not merely an advanced escape
     * hatch. It composes labels, properties, relationships, predicates and
     * aggregations into new solutions without requiring a dedicated MCP
     * tool/schema for every structural code question. Its description asks
     * callers to create effective, computationally efficient custom Cypher;
     * examples and optimization guidance are explicitly non-binding. */
    return name && (strcmp(name, "search_graph") == 0 ||
                    strcmp(name, "query_graph") == 0 ||
                    strcmp(name, "search_code") == 0 ||
                    strcmp(name, "trace_path") == 0);
}

/* Return the canonical property and required definitions used by tools/list,
 * CLI flag typing, and runtime key validation. tools/list additionally applies
 * the global closed-input policy. Static lifetime; do not free. */
const char *cbm_mcp_tool_input_schema(const char *tool_name) {
    if (!tool_name) {
        return NULL;
    }
    /* Backward-compatible classic alias dispatches to trace_path and therefore
     * has the same request schema even though only the canonical name is listed. */
    if (strcmp(tool_name, "trace_call_path") == 0) {
        tool_name = "trace_path";
    }
    if (strcmp(tool_name, "_hidden_tools") == 0) {
        return MCP_HIDDEN_TOOL_INPUT_SCHEMA;
    }
    for (int i = 0; i < TOOL_COUNT; i++) {
        if (strcmp(TOOLS[i].name, tool_name) == 0) {
            return TOOLS[i].input_schema;
        }
    }
    for (int i = 0; i < STREAMLINED_TOOL_COUNT; i++) {
        if (strcmp(STREAMLINED_TOOLS[i].name, tool_name) == 0) {
            return STREAMLINED_TOOLS[i].input_schema;
        }
    }
    return NULL;
}

static bool mcp_tool_name_is_known(const char *tool_name) {
    return cbm_mcp_tool_input_schema(tool_name) != NULL;
}

static bool mcp_tool_allowed(cbm_mcp_tool_profile_t profile, const char *name) {
    static const char *const analysis_tools[] = {
        "search_graph", "query_graph",      "trace_path",     "get_code",
        "get_code_snippet", "get_graph_schema", "get_architecture", "search_code",
        "list_projects", "index_status", "check_index_coverage", "detect_changes",
    };
    static const char *const scout_tools[] = {
        "search_graph", "trace_path", "get_code", "get_code_snippet",
        "get_architecture", "list_projects", "index_status", "check_index_coverage",
    };
    if (!name) {
        return false;
    }
    if (profile == CBM_MCP_TOOL_PROFILE_ALL) {
        return true;
    }
    const char *const *allowed = profile == CBM_MCP_TOOL_PROFILE_ANALYSIS
                                     ? analysis_tools
                                     : profile == CBM_MCP_TOOL_PROFILE_SCOUT ? scout_tools : NULL;
    size_t count = profile == CBM_MCP_TOOL_PROFILE_ANALYSIS
                       ? sizeof(analysis_tools) / sizeof(analysis_tools[0])
                       : profile == CBM_MCP_TOOL_PROFILE_SCOUT
                             ? sizeof(scout_tools) / sizeof(scout_tools[0])
                             : 0U;
    for (size_t i = 0; i < count; i++) {
        if (strcmp(name, allowed[i]) == 0) {
            return true;
        }
    }
    return false;
}

static const char *mcp_tool_profile_name(cbm_mcp_tool_profile_t profile) {
    return profile == CBM_MCP_TOOL_PROFILE_SCOUT ? "scout" : "analysis";
}

int cbm_mcp_parse_tool_profile_args(int argc, const char *const argv[const],
                                    cbm_mcp_tool_profile_t *profile_out) {
    if (argc < 0 || !argv || !profile_out) {
        return -1;
    }
    *profile_out = CBM_MCP_TOOL_PROFILE_ALL;
    for (int i = 1; i < argc; i++) {
        const char *arg = argv[i];
        if (!arg) {
            return -1;
        }
        if (strcmp(arg, "--tool-profile=analysis") == 0) {
            *profile_out = CBM_MCP_TOOL_PROFILE_ANALYSIS;
        } else if (strcmp(arg, "--tool-profile=scout") == 0) {
            *profile_out = CBM_MCP_TOOL_PROFILE_SCOUT;
        } else if (strcmp(arg, "--tool-profile") == 0) {
            if (++i >= argc || !argv[i]) {
                return -1;
            }
            if (strcmp(argv[i], "analysis") == 0) {
                *profile_out = CBM_MCP_TOOL_PROFILE_ANALYSIS;
            } else if (strcmp(argv[i], "scout") == 0) {
                *profile_out = CBM_MCP_TOOL_PROFILE_SCOUT;
            } else {
                return -1;
            }
        } else if (strncmp(arg, "--tool-profile=", strlen("--tool-profile=")) == 0) {
            return -1;
        }
    }
    return 0;
}

bool cbm_mcp_tool_profile_allows_http(cbm_mcp_tool_profile_t profile) {
    return profile == CBM_MCP_TOOL_PROFILE_ALL;
}

/* cbm_mcp_tools_list() is defined after cbm_mcp_server so visibility can use config. */

/* Supported protocol versions, newest first. The server picks the newest
 * version that it shares with the client (per MCP spec version negotiation). */
static const char *SUPPORTED_PROTOCOL_VERSIONS[] = {
    "2025-11-25",
    "2025-06-18",
    "2025-03-26",
    "2024-11-05",
};
static const int SUPPORTED_VERSION_COUNT =
    (int)(sizeof(SUPPORTED_PROTOCOL_VERSIONS) / sizeof(SUPPORTED_PROTOCOL_VERSIONS[0]));

static const char MCP_SERVER_INSTRUCTIONS[] =
    "Use graph tools first for structural discovery: search_graph for symbols, query_graph for "
    "custom structural questions, trace_path for callers and callees, and get_code_snippet for "
    "exact source. Use search_code or filesystem search for literal or non-code text. Watched "
    "projects refresh automatically; use check_index_coverage for cited paths and scopes behind "
    "negative or exhaustive claims. Coverage is best-effort. Paginate when has_more or "
    "nextCursor is present.";

static const char MCP_ANALYSIS_SERVER_INSTRUCTIONS[] =
    "This is the analysis tool profile; graph and index mutation tools are unavailable. Use "
    "list_projects and index_status to select a current project, then search_graph, query_graph, "
    "trace_path, get_code_snippet, get_architecture, and search_code for read-only analysis. "
    "Call check_index_coverage for cited paths and exhaustive claims; ask the parent agent to "
    "refresh a missing or stale project.";

static const char MCP_SCOUT_SERVER_INSTRUCTIONS[] =
    "This is the scout tool profile; only fast positive-discovery tools are available. Select a "
    "current project with list_projects and index_status, then use narrow search_graph, "
    "trace_path, get_code_snippet, and get_architecture calls. Call check_index_coverage for cited "
    "paths. Do not make absence or exhaustive-impact claims; ask the parent agent to refresh "
    "stale data.";

static char *cbm_mcp_initialize_response_for_profile(const char *params_json,
                                                     cbm_mcp_tool_profile_t profile) {
    /* Determine protocol version: if client requests a version we support,
     * echo it back; otherwise respond with our latest. */
    const char *version = SUPPORTED_PROTOCOL_VERSIONS[0]; /* default: latest supported version */
    if (params_json) {
        yyjson_doc *pdoc = yyjson_read(params_json, strlen(params_json), 0);
        if (pdoc) {
            yyjson_val *pv = yyjson_obj_get(yyjson_doc_get_root(pdoc), "protocolVersion");
            if (pv && yyjson_is_str(pv)) {
                const char *requested = yyjson_get_str(pv);
                for (int i = 0; i < SUPPORTED_VERSION_COUNT; i++) {
                    if (strcmp(requested, SUPPORTED_PROTOCOL_VERSIONS[i]) == 0) {
                        version = SUPPORTED_PROTOCOL_VERSIONS[i];
                        break;
                    }
                }
            }
            yyjson_doc_free(pdoc);
        }
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_obj_add_str(doc, root, "protocolVersion", version);

    yyjson_mut_val *impl = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, impl, "name", "codebase-memory-mcp");
    yyjson_mut_obj_add_str(doc, impl, "version", cbm_cli_get_version());
    yyjson_mut_obj_add_val(doc, root, "serverInfo", impl);

    yyjson_mut_val *caps = yyjson_mut_obj(doc);
    yyjson_mut_val *tools_cap = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_bool(doc, tools_cap, "listChanged", true);
    yyjson_mut_obj_add_val(doc, caps, "tools", tools_cap);
    /* Advertise MCP resources capability — clients can read codebase://schema etc. */
    yyjson_mut_val *res_cap = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_bool(doc, res_cap, "subscribe", false);
    yyjson_mut_obj_add_val(doc, caps, "resources", res_cap);
    yyjson_mut_val *prompts_cap = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_bool(doc, prompts_cap, "listChanged", false);
    yyjson_mut_obj_add_val(doc, caps, "prompts", prompts_cap);
    yyjson_mut_obj_add_val(doc, root, "capabilities", caps);

    const char *instructions = MCP_SERVER_INSTRUCTIONS;
    if (profile == CBM_MCP_TOOL_PROFILE_ANALYSIS) {
        instructions = MCP_ANALYSIS_SERVER_INSTRUCTIONS;
    } else if (profile == CBM_MCP_TOOL_PROFILE_SCOUT) {
        instructions = MCP_SCOUT_SERVER_INSTRUCTIONS;
    }
    yyjson_mut_obj_add_str(doc, root, "instructions", instructions);

    char *out = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return out;
}

char *cbm_mcp_initialize_response(const char *params_json) {
    return cbm_mcp_initialize_response_for_profile(params_json, CBM_MCP_TOOL_PROFILE_ALL);
}

/* ── Prompt definitions ───────────────────────────────────────── */

static void mcp_add_prompt_argument(yyjson_mut_doc *doc, yyjson_mut_val *arguments,
                                    const char *name, const char *title, const char *description,
                                    bool required) {
    yyjson_mut_val *argument = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, argument, "name", name);
    yyjson_mut_obj_add_str(doc, argument, "title", title);
    yyjson_mut_obj_add_str(doc, argument, "description", description);
    yyjson_mut_obj_add_bool(doc, argument, "required", required);
    yyjson_mut_arr_add_val(arguments, argument);
}

static char *cbm_mcp_prompts_list(void) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_val *prompts = yyjson_mut_arr(doc);

    yyjson_mut_val *explore = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, explore, "name", "explore_codebase");
    yyjson_mut_obj_add_str(doc, explore, "title", "Explore codebase");
    yyjson_mut_obj_add_str(doc, explore, "description",
                           "Explore a codebase with graph-first structural discovery.");
    yyjson_mut_val *explore_args = yyjson_mut_arr(doc);
    mcp_add_prompt_argument(doc, explore_args, "project", "Project",
                            "Indexed project name from list_projects.", true);
    mcp_add_prompt_argument(doc, explore_args, "question", "Question",
                            "Architecture or implementation question to investigate.", true);
    yyjson_mut_obj_add_val(doc, explore, "arguments", explore_args);
    yyjson_mut_arr_add_val(prompts, explore);

    yyjson_mut_val *review = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, review, "name", "review_change_impact");
    yyjson_mut_obj_add_str(doc, review, "title", "Review change impact");
    yyjson_mut_obj_add_str(doc, review, "description",
                           "Review affected callers, tests, boundaries, and risks.");
    yyjson_mut_val *review_args = yyjson_mut_arr(doc);
    mcp_add_prompt_argument(doc, review_args, "project", "Project",
                            "Indexed project name from list_projects.", true);
    mcp_add_prompt_argument(doc, review_args, "change", "Change",
                            "Change, symbol, or area whose impact should be reviewed.", true);
    mcp_add_prompt_argument(doc, review_args, "base_branch", "Base branch",
                            "Git branch or ref for detect_changes; defaults to main.", false);
    yyjson_mut_obj_add_val(doc, review, "arguments", review_args);
    yyjson_mut_arr_add_val(prompts, review);

    yyjson_mut_obj_add_val(doc, root, "prompts", prompts);
    char *out = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return out;
}

static const char *mcp_prompt_string_argument(yyjson_val *arguments, const char *name) {
    yyjson_val *value = arguments && yyjson_is_obj(arguments)
                            ? yyjson_obj_get(arguments, name)
                            : NULL;
    const char *text = value && yyjson_is_str(value) ? yyjson_get_str(value) : NULL;
    return text && text[0] ? text : NULL;
}

static char *mcp_prompt_result(const char *description, const char *text) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_str(doc, root, "description", description);
    yyjson_mut_val *messages = yyjson_mut_arr(doc);
    yyjson_mut_val *message = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, message, "role", "user");
    yyjson_mut_val *content = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, content, "type", "text");
    yyjson_mut_obj_add_str(doc, content, "text", text);
    yyjson_mut_obj_add_val(doc, message, "content", content);
    yyjson_mut_arr_add_val(messages, message);
    yyjson_mut_obj_add_val(doc, root, "messages", messages);
    char *out = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return out;
}

static char *cbm_mcp_prompt_get(const char *params_json, int *error_code,
                                const char **error_message) {
    *error_code = 0;
    *error_message = NULL;
    yyjson_doc *doc = params_json ? yyjson_read(params_json, strlen(params_json), 0) : NULL;
    yyjson_val *params = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *name_value = params && yyjson_is_obj(params) ? yyjson_obj_get(params, "name") : NULL;
    if (!name_value || !yyjson_is_str(name_value)) {
        *error_code = CBM_JSONRPC_INVALID_PARAMS;
        *error_message = "Invalid prompt name";
        yyjson_doc_free(doc);
        return NULL;
    }

    const char *name = yyjson_get_str(name_value);
    bool explore = strcmp(name, "explore_codebase") == 0;
    bool review = strcmp(name, "review_change_impact") == 0;
    if (!explore && !review) {
        *error_code = CBM_JSONRPC_INVALID_PARAMS;
        *error_message = "Invalid prompt name";
        yyjson_doc_free(doc);
        return NULL;
    }

    yyjson_val *arguments = yyjson_obj_get(params, "arguments");
    const char *project = mcp_prompt_string_argument(arguments, "project");
    const char *request = mcp_prompt_string_argument(arguments, explore ? "question" : "change");
    const char *base_branch = "main";
    yyjson_val *base_value = review && arguments && yyjson_is_obj(arguments)
                                 ? yyjson_obj_get(arguments, "base_branch")
                                 : NULL;
    if (!project || !request) {
        *error_code = CBM_JSONRPC_INVALID_PARAMS;
        *error_message = "Missing required prompt arguments";
        yyjson_doc_free(doc);
        return NULL;
    }
    if (base_value) {
        if (!yyjson_is_str(base_value) || !yyjson_get_str(base_value)[0]) {
            *error_code = CBM_JSONRPC_INVALID_PARAMS;
            *error_message = "Invalid prompt arguments";
            yyjson_doc_free(doc);
            return NULL;
        }
        base_branch = yyjson_get_str(base_value);
    }

    static const char EXPLORE_TEMPLATE[] =
        "Explore project \"%s\" to answer: %s\n\nUse graph tools first: search_graph to find "
        "symbols, query_graph for custom structural questions, get_code_snippet for exact source, "
        "and trace_path(direction=\"both\") for callers and callees. Check coverage and pagination; "
        "use search_code or grep for literal, non-code, or uncovered text.";
    static const char REVIEW_TEMPLATE[] =
        "Review change impact in project \"%s\" for: %s\n\nUse detect_changes with base_branch "
        "\"%s\", then trace_path(direction=\"both\", include_tests=true). Read exact definitions "
        "with get_code_snippet and use query_graph for custom cross-boundary patterns. Report "
        "affected callers, tests, boundaries, and risks; do not modify files.";
    size_t size = strlen(project) + strlen(request) + strlen(base_branch) +
                  (explore ? sizeof(EXPLORE_TEMPLATE) : sizeof(REVIEW_TEMPLATE));
    char *text = malloc(size);
    if (!text) {
        *error_code = CBM_JSONRPC_INTERNAL_ERROR;
        *error_message = "Internal error";
        yyjson_doc_free(doc);
        return NULL;
    }
    if (explore) {
        snprintf(text, size, EXPLORE_TEMPLATE, project, request);
    } else {
        snprintf(text, size, REVIEW_TEMPLATE, project, request, base_branch);
    }
    char *result = mcp_prompt_result(
        explore ? "Graph-first codebase exploration" : "Graph-first change-impact review", text);
    free(text);
    yyjson_doc_free(doc);
    return result;
}

/* ══════════════════════════════════════════════════════════════════
 *  ARGUMENT EXTRACTION
 * ══════════════════════════════════════════════════════════════════ */

char *cbm_mcp_get_tool_name(const char *params_json) {
    yyjson_doc *doc = yyjson_read(params_json, strlen(params_json), 0);
    if (!doc) {
        return NULL;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *name = yyjson_obj_get(root, "name");
    char *result = NULL;
    if (name && yyjson_is_str(name)) {
        result = heap_strdup(yyjson_get_str(name));
    }
    yyjson_doc_free(doc);
    return result;
}

char *cbm_mcp_get_arguments(const char *params_json) {
    yyjson_doc *doc = yyjson_read(params_json, strlen(params_json), 0);
    if (!doc) {
        return NULL;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *args = yyjson_obj_get(root, "arguments");
    char *result = NULL;
    if (args) {
        result = yyjson_val_write(args, 0, NULL);
    }
    yyjson_doc_free(doc);
    return result ? result : heap_strdup("{}");
}

/* Parse and validate CallToolRequest params in one pass. Protocol errors must
 * be identified before dispatch: unknown tools and malformed requests are
 * JSON-RPC errors, while failures inside recognized tools are CallToolResult
 * values with isError=true. Returns 0 on success or a standardized
 * CBM_JSONRPC_* error code on failure. */
static int parse_tool_call_params(const char *params_json, char **name_out, char **args_out,
                                  const char **error_out) {
    *name_out = NULL;
    *args_out = NULL;
    *error_out = NULL;

    if (!params_json) {
        *error_out = "Missing tools/call params";
        return CBM_JSONRPC_INVALID_PARAMS;
    }

    yyjson_doc *doc = yyjson_read(params_json, strlen(params_json), 0);
    if (!doc) {
        *error_out = "Invalid tools/call params";
        return CBM_JSONRPC_INVALID_PARAMS;
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    if (!yyjson_is_obj(root)) {
        *error_out = "Tool call params must be an object";
        yyjson_doc_free(doc);
        return CBM_JSONRPC_INVALID_PARAMS;
    }

    yyjson_val *name = yyjson_obj_get(root, "name");
    if (!name || !yyjson_is_str(name) || yyjson_get_len(name) == 0) {
        *error_out = "Missing tool name";
        yyjson_doc_free(doc);
        return CBM_JSONRPC_INVALID_PARAMS;
    }

    yyjson_val *args = yyjson_obj_get(root, "arguments");
    if (args && !yyjson_is_obj(args)) {
        *error_out = "Tool arguments must be an object";
        yyjson_doc_free(doc);
        return CBM_JSONRPC_INVALID_PARAMS;
    }

    *name_out = heap_strdup(yyjson_get_str(name));
    *args_out = args ? yyjson_val_write(args, 0, NULL) : heap_strdup("{}");
    yyjson_doc_free(doc);

    if (!*name_out || !*args_out) {
        free(*name_out);
        free(*args_out);
        *name_out = NULL;
        *args_out = NULL;
        *error_out = "Unable to allocate tool call parameters";
        return CBM_JSONRPC_INTERNAL_ERROR;
    }
    return 0;
}

/* Check if name is the last dot/colon/slash-separated segment of qualified_name.
 * E.g. ends_with_segment("app.utils.process", "process") → true
 *      ends_with_segment("app.subprocess", "process") → false */
static bool ends_with_segment(const char *qn, const char *name) {
    if (!qn || !name) return false;
    size_t qn_len = strlen(qn);
    size_t name_len = strlen(name);
    if (name_len > qn_len) return false;
    if (name_len == qn_len) return strcmp(qn, name) == 0;
    char sep = qn[qn_len - name_len - 1];
    return (sep == '.' || sep == ':' || sep == '/') &&
           strcmp(qn + qn_len - name_len, name) == 0;
}

static char **glob_patterns_to_like(char **patterns, int count) {
    if (!patterns || count <= 0) {
        return NULL;
    }
    char **likes = calloc((size_t)count + 1, sizeof(char *));
    if (!likes) {
        return NULL;
    }
    int out = 0;
    for (int i = 0; i < count; i++) {
        if (!patterns[i] || !patterns[i][0]) {
            continue;
        }
        char *like = cbm_glob_to_like(patterns[i]);
        if (!like) {
            for (int j = 0; j < out; j++) {
                free(likes[j]);
            }
            free(likes);
            return NULL;
        }
        likes[out++] = like;
    }
    return likes;
}

static bool path_matches_like_any(const char *path, char **likes) {
    if (!path || !likes) {
        return false;
    }
    for (int i = 0; likes[i]; i++) {
        if (sqlite3_strlike(likes[i], path, 0) == 0) {
            return true;
        }
    }
    return false;
}

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
char *cbm_mcp_get_string_arg(const char *args_json, const char *key) {
    yyjson_doc *doc = yyjson_read(args_json, strlen(args_json), 0);
    if (!doc) {
        return NULL;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *val = yyjson_obj_get(root, key);
    char *result = NULL;
    if (val && yyjson_is_str(val)) {
        result = heap_strdup(yyjson_get_str(val));
    }
    yyjson_doc_free(doc);
    return result;
}

static char *canonicalize_repo_path_if_exists(char *repo_path) {
    if (!repo_path) {
        return NULL;
    }
    bool root_syntax = true;
    for (const char *p = repo_path; *p; p++) {
        if (*p != '/' && *p != '\\' && *p != ':') {
            root_syntax = false;
            break;
        }
    }
    if (root_syntax) {
        return repo_path;
    }

    char real[CBM_SZ_4K];
    /* Wide-path canonicalization: the old _access/_fullpath pair decoded the
     * UTF-8 repo_path through the ANSI codepage and corrupted CJK paths on
     * CJK-locale systems (#973). */
    if (cbm_canonical_path(repo_path, real, sizeof(real))) {
        cbm_normalize_path_sep(real);
        char *canonical = heap_strdup(real);
        if (canonical) {
            free(repo_path);
            return canonical;
        }
    }

    return repo_path;
}

static char *normalize_project_arg(char *project) {
    if (!project || (!strchr(project, '/') && !strchr(project, '\\'))) {
        return project;
    }

    project = canonicalize_repo_path_if_exists(project);
    char *normalized = cbm_project_name_from_path(project);
    if (normalized) {
        free(project);
        return normalized;
    }
    return project;
}

static bool project_is_path(const char *s);

/* Forward decls — defined below alongside store resolution. */
static const char *cache_dir(char *buf, size_t bufsz);
static bool is_project_db_file(const char *name, size_t len);
bool cbm_validate_project_name(const char *project);
/* #1025: agents naturally pass the repo FOLDER name ("codebase-memory-mcp"),
 * but indexed project names derive from the full path
 * (E:\project\graph\x -> "E-project-graph-x"), so the exact lookup fails
 * while list_projects clearly shows the project. When no <project>.db exists,
 * scan cache-dir FILENAMES for a segment-aligned tail match ("-<project>.db"):
 * exactly one match adopts the full name; zero or several keep the original so
 * the existing not-found error (which lists all candidates) fires. Filename-
 * level only — internal-name drift stays #704's fallback in resolve_store. */
static char *resolve_project_tail(char *project) {
    if (!project || !cbm_validate_project_name(project)) {
        return project;
    }
    char dir[CBM_SZ_1K];
    cache_dir(dir, sizeof(dir));
    char exact[CBM_SZ_2K];
    snprintf(exact, sizeof(exact), "%s/%s.db", dir, project);
    if (cbm_file_exists(exact)) {
        return project; /* exact name — untouched fast path */
    }
    size_t plen = strlen(project);
    char match[CBM_SZ_1K] = "";
    int matches = 0;
    cbm_dir_t *d = cbm_opendir(dir);
    if (!d) {
        return project;
    }
    cbm_dirent_t *entry;
    while ((entry = cbm_readdir(d)) != NULL) {
        const char *n = entry->name;
        size_t len = strlen(n);
        if (!is_project_db_file(n, len)) {
            continue;
        }
        size_t stem_len = len - MCP_DB_EXT; /* strip ".db" */
        if (stem_len <= plen + 1 || stem_len >= sizeof(match)) {
            continue;
        }
        if (n[stem_len - plen - 1] != '-' || strncmp(n + stem_len - plen, project, plen) != 0) {
            continue;
        }
        matches++;
        if (matches > 1) {
            break; /* ambiguous — keep the original name */
        }
        memcpy(match, n, stem_len);
        match[stem_len] = '\0';
    }
    cbm_closedir(d);
    if (matches == 1) {
        cbm_log_info("mcp.project_tail_resolved", "passed", project, "resolved", match);
        free(project);
        return heap_strdup(match);
    }
    return project;
}

/* Resolve the project argument, accepting the canonical "project" key plus the
 * aliases a caller naturally reaches for (#640): list_projects surfaces the
 * field as "name" and the not-found hint says "pass the project name", so
 * "project_name" is the usual guess; "project_id" / "projectName" are accepted
 * too. NOT bare "name" — index_repository uses "name" for an explicit
 * project-name override. Caller must free() the result. */
static char *get_raw_project_arg(const char *args_json) {
    char *p = cbm_mcp_get_string_arg(args_json, "project");
    if (!p) {
        p = cbm_mcp_get_string_arg(args_json, "project_name");
    }
    if (!p) {
        p = cbm_mcp_get_string_arg(args_json, "project_id");
    }
    if (!p) {
        p = cbm_mcp_get_string_arg(args_json, "projectName");
    }
    return p;
}

static char *get_project_arg(const char *args_json) {
    return resolve_project_tail(normalize_project_arg(get_raw_project_arg(args_json)));
}

/* Store-resolving handlers need the original filesystem path so
 * resolve_project_store() can auto-index it. Ordinary names must still take
 * the #1025 normalization/tail-resolution path used by every other handler. */
static char *get_store_project_arg(const char *args_json) {
    char *project = get_raw_project_arg(args_json);
    if (project && project_is_path(project)) {
        return project;
    }
    return resolve_project_tail(normalize_project_arg(project));
}

int cbm_mcp_get_int_arg(const char *args_json, const char *key, int default_val) {
    yyjson_doc *doc = yyjson_read(args_json, strlen(args_json), 0);
    if (!doc) {
        return default_val;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *val = yyjson_obj_get(root, key);
    int result = default_val;
    if (val && yyjson_is_int(val)) {
        result = yyjson_get_int(val);
    }
    yyjson_doc_free(doc);
    return result;
}

bool cbm_mcp_get_bool_arg(const char *args_json, const char *key) {
    return cbm_mcp_get_bool_arg_default(args_json, key, false);
}

bool cbm_mcp_get_bool_arg_default(const char *args_json, const char *key, bool default_val) {
    yyjson_doc *doc = yyjson_read(args_json, strlen(args_json), 0);
    if (!doc) {
        return default_val;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *val = yyjson_obj_get(root, key);
    bool result = default_val;
    if (val && yyjson_is_bool(val)) {
        result = yyjson_get_bool(val);
    }
    yyjson_doc_free(doc);
    return result;
}

static bool cbm_mcp_has_arg(const char *args_json, const char *key) {
    if (!args_json || !key) {
        return false;
    }
    yyjson_doc *doc = yyjson_read(args_json, strlen(args_json), 0);
    if (!doc) {
        return false;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    bool found = yyjson_obj_get(root, key) != NULL;
    yyjson_doc_free(doc);
    return found;
}

/* Extract a JSON array of strings from args. Returns heap-allocated
 * NULL-terminated array of heap-allocated strings. Caller must free each
 * string and the array itself. Returns NULL if key absent or not array; sets
 * out_count to -1 on allocation failure. */
static char **cbm_mcp_get_string_array_arg(const char *args_json, const char *key, int *out_count) {
    if (out_count) *out_count = 0;
    yyjson_doc *doc = yyjson_read(args_json, strlen(args_json), 0);
    if (!doc) return NULL;
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *arr = yyjson_obj_get(root, key);
    if (!arr || !yyjson_is_arr(arr)) {
        yyjson_doc_free(doc);
        return NULL;
    }
    int n = (int)yyjson_arr_size(arr);
    if (n == 0) {
        yyjson_doc_free(doc);
        return NULL;
    }
    char **result = calloc((size_t)(n + 1), sizeof(char *));
    if (!result) {
        if (out_count) *out_count = -1;
        yyjson_doc_free(doc);
        return NULL;
    }
    int count = 0;
    yyjson_val *item;
    yyjson_arr_iter iter = yyjson_arr_iter_with(arr);
    while ((item = yyjson_arr_iter_next(&iter))) {
        if (yyjson_is_str(item)) {
            char *copy = heap_strdup(yyjson_get_str(item));
            if (!copy) {
                for (int i = 0; i < count; i++) {
                    free(result[i]);
                }
                free(result);
                if (out_count) *out_count = -1;
                yyjson_doc_free(doc);
                return NULL;
            }
            result[count++] = copy;
        }
    }
    result[count] = NULL;
    if (out_count) *out_count = count;
    yyjson_doc_free(doc);
    return result;
}

static void free_string_array(char **arr) {
    if (!arr) return;
    for (int i = 0; arr[i]; i++) free(arr[i]);
    free(arr);
}

static void free_counted_string_array(char **arr, int count) {
    if (!arr) {
        return;
    }
    for (int i = 0; i < count; i++) {
        free(arr[i]);
    }
    free(arr);
}

/* ══════════════════════════════════════════════════════════════════
 *  MCP SERVER
 * ══════════════════════════════════════════════════════════════════ */

/* Forward declarations for functions defined after first use */
static void send_notification(cbm_mcp_server_t *srv, const char *method);
static char *build_key_functions_sql(const char *exclude_csv, const char **exclude_arr, int limit,
                                     bool path_scoped);
static bool validate_cbm_db_with_timeout(const char *path, int busy_timeout_ms);
static void *overlay_compaction_thread(void *arg);

typedef enum {
    MCP_AUTOINDEX_BLOCK_NONE = 0,
    MCP_AUTOINDEX_BLOCK_DISABLED,
    MCP_AUTOINDEX_BLOCK_FILE_COUNT,
    MCP_AUTOINDEX_BLOCK_FILE_LIMIT,
} mcp_autoindex_block_t;

struct cbm_mcp_server {
    cbm_mcp_tool_profile_t tool_profile;
    cbm_store_t *store;             /* currently open project store (or NULL) */
    bool owns_store;                /* true if we opened the store */
    char *current_project;          /* which project store is open for (heap) */
    time_t store_last_used;         /* last time resolve_store was called for a named project */
    char update_notice[CBM_SZ_256]; /* one-shot update notice, cleared after first injection */
    cbm_mutex_t update_notice_lock; /* protects update_notice across background check/request thread */
    bool update_checked;            /* true after background check has been launched */
    cbm_thread_t update_tid;        /* background update check thread */
    bool update_thread_active;      /* true if update thread was started and needs joining */

    /* Session + auto-index state */
    char session_root[CBM_SZ_1K];     /* detected project root path */
    char session_project[CBM_SZ_256]; /* derived project name */
    bool session_detected;            /* true after first detection attempt */
    struct cbm_watcher *watcher;      /* external watcher ref (not owned) */
    struct cbm_config *config;        /* external config ref (not owned) */
    cbm_thread_t autoindex_tid;
    bool autoindex_active; /* true if auto-index thread was started */
    bool autoindex_failed; /* IX-1: true if last auto-index attempt failed */
    bool just_autoindexed; /* IX-3: true after auto-index completes, reset on next search */
    /* Request-thread-owned reason startup indexing did not start. This reports
     * only this server's decision; it never guesses sibling-process liveness
     * from temp files or other crash-stale filesystem artifacts. */
    mcp_autoindex_block_t autoindex_block;
    int autoindex_observed_files;
    int autoindex_file_limit;
    bool context_injected; /* true after first _context header sent (Phase 9) */
    /* Request-thread-owned tools/list docstring. Graph workers may only mark
     * it stale atomically; they must never read, replace, or free the pointer. */
    char *query_graph_tool_description;
    atomic_bool query_graph_tool_description_stale;
    /* Set by any publication path (any thread); drained only by the request
     * thread after it finishes writing a response. Best-effort: correctness
     * never depends on delivery (query_graph_no_rows_hint self-heals). */
    atomic_bool tools_list_changed_pending;
    bool hidden_tools_revealed; /* true after _hidden_tools requests real tools/list exposure */
    FILE *out_stream;          /* protocol output stream for notifications (set in server_run) */
    bool out_content_length_framed; /* true while handling Content-Length-framed requests */
    cbm_mutex_t overlay_compaction_lock;
    cbm_thread_t overlay_compaction_tid;
    bool overlay_compaction_started;
    bool overlay_compaction_finished;
    char overlay_compaction_project[CBM_SZ_256];
    int overlay_compaction_max_generations;
    int overlay_compaction_rc;
    int overlay_compaction_compacted;

    /* Active pipeline tracking for cancellation support */
    cbm_mutex_t active_request_lock; /* protects request identity and pipeline lifetime handoff */
    _Atomic(cbm_pipeline_t *) active_pipeline; /* non-NULL while index_repository runs */
    int64_t active_request_id;       /* JSON-RPC id of the in-progress tool call */
    char *active_request_id_str;     /* string JSON-RPC id of the in-progress tool call */

    /* Shutdown request from a signal handler or watchdog thread. The run loop
     * polls with a bounded interval, so a plain atomic store here (the only
     * async-signal-safe primitive available in a handler) ends the loop within
     * one poll tick without touching stdio state from signal context. */
    atomic_bool stop_requested;

    /* Deferred store invalidation. A supervised index worker replaces the DB
     * file, so the parent's cached srv->store handle goes stale at reap time —
     * but the reap may happen on the background auto-index thread, and only the
     * request thread may close srv->store (store.h: one handle per thread; the
     * request thread may be mid-query). Reapers SET this flag; the request
     * thread consumes it at the top of resolve_store()/resolve_resource_store()
     * and performs the actual close + reopen there. */
    atomic_bool store_stale;
};

void cbm_mcp_server_request_stop(cbm_mcp_server_t *srv) {
    if (srv) {
        atomic_store(&srv->stop_requested, true);
    }
}

/* Defined with the supervisor plumbing below; used by resolve_store /
 * resolve_resource_store above it. */
static void reap_stale_store(cbm_mcp_server_t *srv);

static bool cbm_mcp_tool_mode_is_classic(cbm_mcp_server_t *srv) {
    /* Env var keeps script/test overrides independent from the persisted config. */
    char tool_mode_buf[CBM_SZ_64];
    const char *tool_mode = cbm_safe_getenv("CBM_TOOL_MODE", tool_mode_buf,
                                            sizeof(tool_mode_buf), NULL);
    if (tool_mode && tool_mode[0] != '\0') {
        return strcmp(tool_mode, CBM_CONFIG_TOOL_MODE_CLASSIC) == 0;
    }
    tool_mode = (srv && srv->config) ? cbm_config_get(srv->config, CBM_CONFIG_TOOL_MODE,
                                                      CBM_CONFIG_TOOL_MODE_STREAMLINED)
                                     : CBM_CONFIG_TOOL_MODE_STREAMLINED;
    return strcmp(tool_mode, CBM_CONFIG_TOOL_MODE_CLASSIC) == 0;
}

static cbm_mcp_output_format_t cbm_mcp_response_format(cbm_mcp_server_t *srv,
                                                       const char *args) {
    char *override = cbm_mcp_get_string_arg(args, "format");
    const char *value = override;
    if (!value) {
        value = cbm_config_get_effective(srv ? srv->config : NULL,
                                         CBM_CONFIG_DEFAULT_RESPONSE_FORMAT,
                                         CBM_MCP_OUTPUT_FORMAT_TOON);
    }
    cbm_mcp_output_format_t format = CBM_MCP_OUTPUT_INVALID;
    if (value && strcmp(value, CBM_MCP_OUTPUT_FORMAT_TOON) == 0) {
        format = CBM_MCP_OUTPUT_TOON;
    } else if (value && strcmp(value, CBM_MCP_OUTPUT_FORMAT_JSON) == 0) {
        format = CBM_MCP_OUTPUT_JSON;
    }
    free(override);
    return format;
}

static char *cbm_mcp_invalid_response_format(void) {
    return cbm_mcp_text_result(
        "unsupported response format; use format='toon' or format='json', or set "
        "default_response_format to toon or json",
        true);
}

static bool cbm_mcp_tool_config_enabled(cbm_mcp_server_t *srv, const char *tool_name) {
    if (!srv || !srv->config || !tool_name) {
        return false;
    }
    char key[CBM_SZ_64];
    int n = snprintf(key, sizeof(key), "tool_%s", tool_name);
    if (n < 0 || (size_t)n >= sizeof(key)) {
        return false;
    }
    return cbm_config_get_bool(srv->config, key, false);
}

static bool cbm_mcp_advanced_tool_visible(cbm_mcp_server_t *srv, const char *tool_name) {
    if (cbm_mcp_tool_mode_is_classic(srv)) {
        return true;
    }
    return (srv && srv->hidden_tools_revealed) ||
           cbm_mcp_tool_config_enabled(srv, tool_name);
}

static int cbm_mcp_config_int_clamped(cbm_mcp_server_t *srv, const char *key, int default_val,
                                      int min_val, int max_val) {
    int value = srv && srv->config ? cbm_config_get_int(srv->config, key, default_val) : default_val;
    if (value < min_val) {
        value = min_val;
    }
    if (value > max_val) {
        value = max_val;
    }
    return value;
}

static int cbm_mcp_get_positive_int_arg(const char *args_json, const char *key,
                                        int default_val, int fallback_val) {
    int effective_default = default_val > 0 ? default_val : fallback_val;
    int value = cbm_mcp_get_int_arg(args_json, key, effective_default);
    return value > 0 ? value : effective_default;
}

static int cbm_mcp_store_idle_timeout_s(cbm_mcp_server_t *srv) {
    return cbm_mcp_config_int_clamped(srv, CBM_CONFIG_STORE_IDLE_TIMEOUT_S,
                                      CBM_MCP_DEFAULT_STORE_IDLE_TIMEOUT_S, 1,
                                      CBM_SZ_64K);
}

static int cbm_mcp_db_validate_busy_timeout_ms(cbm_mcp_server_t *srv) {
    return cbm_mcp_config_int_clamped(srv, CBM_CONFIG_DB_VALIDATE_BUSY_TIMEOUT_MS,
                                      CBM_DB_VALIDATE_BUSY_TIMEOUT_MS, 0,
                                      CBM_SZ_64K);
}

static int cbm_mcp_update_check_timeout_s(cbm_mcp_server_t *srv) {
    if (!srv || !srv->config) {
        return 0;
    }
    return cbm_mcp_config_int_clamped(srv, CBM_CONFIG_UPDATE_CHECK_TIMEOUT_S,
                                      CBM_MCP_UPDATE_CHECK_TIMEOUT_S, 0,
                                      CBM_SZ_256);
}

static bool cbm_mcp_auto_index_enabled(cbm_mcp_server_t *srv) {
    bool default_val = (srv && srv->config);
    return cbm_config_get_effective_bool(srv ? srv->config : NULL, CBM_CONFIG_AUTO_INDEX,
                                         default_val);
}

static bool cbm_mcp_incremental_metadata_enabled(cbm_mcp_server_t *srv) {
    const char *policy =
        srv && srv->config
            ? cbm_config_get(srv->config, CBM_CONFIG_INCREMENTAL_REINDEX,
                             CBM_CONFIG_INCREMENTAL_REINDEX_OFF)
            : CBM_CONFIG_INCREMENTAL_REINDEX_OFF;
    return policy && strcmp(policy, CBM_CONFIG_INCREMENTAL_REINDEX_OFF) != 0;
}

static bool cbm_mcp_overlay_compaction_after_publish(cbm_mcp_server_t *srv) {
    const char *policy =
        srv && srv->config
            ? cbm_config_get(srv->config, CBM_CONFIG_OVERLAY_COMPACTION_POLICY,
                             CBM_CONFIG_OVERLAY_COMPACTION_POLICY_MANUAL)
            : CBM_CONFIG_OVERLAY_COMPACTION_POLICY_MANUAL;
    return policy &&
           strcmp(policy, CBM_CONFIG_OVERLAY_COMPACTION_POLICY_AFTER_PUBLISH) == 0;
}

static int cbm_mcp_overlay_compaction_max_generations(cbm_mcp_server_t *srv) {
    return cbm_mcp_config_int_clamped(srv, CBM_CONFIG_OVERLAY_COMPACTION_MAX_GENERATIONS,
                                      CBM_OVERLAY_COMPACTION_DEFAULT_MAX_GENERATIONS, 1,
                                      CBM_SZ_256);
}

static int cbm_mcp_effective_auto_dep_limit(cbm_mcp_server_t *srv, const char *args_json) {
    bool enabled = cbm_config_get_bool(srv ? srv->config : NULL,
                                       CBM_CONFIG_AUTO_INDEX_DEPS,
                                       CBM_DEFAULT_AUTO_INDEX_DEPS);
    if (cbm_mcp_has_arg(args_json, CBM_CONFIG_AUTO_INDEX_DEPS)) {
        enabled = cbm_mcp_get_bool_arg_default(args_json, CBM_CONFIG_AUTO_INDEX_DEPS, enabled);
    }
    if (!enabled) {
        return 0;
    }

    int limit = cbm_config_get_int(srv ? srv->config : NULL, CBM_CONFIG_AUTO_DEP_LIMIT,
                                   CBM_DEFAULT_AUTO_DEP_LIMIT);
    if (cbm_mcp_has_arg(args_json, CBM_CONFIG_AUTO_DEP_LIMIT)) {
        limit = cbm_mcp_get_int_arg(args_json, CBM_CONFIG_AUTO_DEP_LIMIT, limit);
    }
    return limit <= 0 ? -1 : limit;
}

/* Query routes deliberately cache read-only stores. Mutation routes must never
 * reuse those handles: open the already-validated database read-write without
 * allowing accidental database creation. In-memory/embedded stores have no
 * path and are already caller-owned, so they can be used directly. */
static cbm_store_t *cbm_mcp_writable_existing_store(cbm_store_t *resolved,
                                                     cbm_store_t **out_owned) {
    if (out_owned) {
        *out_owned = NULL;
    }
    if (!resolved || !out_owned) {
        return NULL;
    }
    const char *db_path = cbm_store_db_path(resolved);
    if (!db_path) {
        return resolved;
    }
    *out_owned = cbm_store_open_path_existing(db_path);
    return *out_owned;
}

static int cbm_mcp_auto_index_deps(cbm_mcp_server_t *srv, const char *project,
                                   const char *root_path, cbm_store_t *store,
                                   int effective_dep_limit, int *out_rc) {
    if (out_rc) {
        *out_rc = CBM_STORE_OK;
    }
    int deps_reindexed =
        cbm_dep_auto_index_effective(project, root_path, store, effective_dep_limit,
                                     srv ? srv->config : NULL);
    if (deps_reindexed > 0 && cbm_mcp_incremental_metadata_enabled(srv)) {
        int owner_rc = cbm_store_rebuild_file_delta_owners(
            store, project, CBM_PIPELINE_FILE_DELTA_GENERATION);
        if (owner_rc != CBM_STORE_OK) {
            char rc_buf[CBM_SZ_32];
            snprintf(rc_buf, sizeof(rc_buf), "%d", owner_rc);
            cbm_log_error("index_repository.err", "phase",
                          "rebuild_file_delta_owners_after_deps", "rc",
                          rc_buf);
            if (out_rc) {
                *out_rc = owner_rc;
            }
        }
    }
    return deps_reindexed;
}

/* Complete the shared post-index work against a writable handle. Query routes
 * cache read-only handles, so both session-root and explicit-path auto-indexing
 * must use this path before returning the resolved query store. */
static void cbm_mcp_refresh_auto_indexed_store(cbm_mcp_server_t *srv,
                                               cbm_store_t *resolved_store,
                                               const char *project,
                                               const char *root_path) {
    cbm_store_t *owned_writable_store = NULL;
    cbm_store_t *writable_store =
        cbm_mcp_writable_existing_store(resolved_store, &owned_writable_store);
    if (writable_store) {
        int effective_dep_limit = cbm_mcp_effective_auto_dep_limit(srv, NULL);
        (void)cbm_mcp_auto_index_deps(srv, project, root_path, writable_store,
                                      effective_dep_limit, NULL);
        cbm_pagerank_compute_with_config(writable_store, project, srv->config);
    }
    if (owned_writable_store) {
        cbm_store_close(owned_writable_store);
    }
}

static int cbm_mcp_auto_index_limit(cbm_mcp_server_t *srv) {
    return cbm_config_get_effective_int(srv ? srv->config : NULL, CBM_CONFIG_AUTO_INDEX_LIMIT,
                                        CBM_DEFAULT_AUTO_INDEX_LIMIT);
}

static bool cbm_mcp_auto_index_within_limit(cbm_mcp_server_t *srv, const char *root_path) {
    int file_limit = cbm_mcp_auto_index_limit(srv);
    if (srv) {
        srv->autoindex_block = MCP_AUTOINDEX_BLOCK_NONE;
        srv->autoindex_observed_files = 0;
        srv->autoindex_file_limit = file_limit;
    }
    if (file_limit <= 0) {
        return true;
    }
    cbm_discover_opts_t opts = {.mode = CBM_MODE_FULL, .ignore_file = NULL, .max_file_size = 0};
    int count = 0;
    if (cbm_discover_count_bounded(root_path, &opts, file_limit, &count) != 0) {
        if (srv) {
            srv->autoindex_block = MCP_AUTOINDEX_BLOCK_FILE_COUNT;
        }
        cbm_log_warn("autoindex.skip", "reason", "file_count_failed", "path",
                     root_path ? root_path : "");
        return false;
    }
    if (count > file_limit) {
        if (srv) {
            srv->autoindex_block = MCP_AUTOINDEX_BLOCK_FILE_LIMIT;
            srv->autoindex_observed_files = count;
        }
        char count_buf[CBM_SZ_32];
        snprintf(count_buf, sizeof(count_buf), "%d", count);
        cbm_log_warn("autoindex.skip", "reason", "too_many_files", "files", count_buf, "limit",
                     CBM_CONFIG_AUTO_INDEX_LIMIT, "path", root_path ? root_path : "");
        return false;
    }
    return true;
}

static bool cbm_mcp_run_sync_auto_index(cbm_mcp_server_t *srv, const char *root_path,
                                        const char *event, const char *log_key,
                                        const char *log_value) {
    cbm_pipeline_t *pipeline = cbm_pipeline_new(root_path, NULL, CBM_MODE_FULL);
    if (!pipeline) {
        if (srv) {
            srv->autoindex_failed = true;
        }
        cbm_log_error("autoindex.create_failed", "root", root_path ? root_path : "");
        return false;
    }

    cbm_pipeline_apply_config(pipeline, srv ? srv->config : NULL);
    cbm_log_info(event, log_key, log_value ? log_value : "");
    int rc = cbm_pipeline_run(pipeline);
    cbm_pipeline_free(pipeline);

    if (srv) {
        srv->autoindex_failed = (rc != 0);
        srv->just_autoindexed = (rc == 0);
        if (rc == 0) {
            /* One publication authority: store_stale + description_stale +
             * pending list_changed together, not a handler-local flag. */
            cbm_mcp_server_notify_index_published(srv);
        }
    }
    if (rc != 0) {
        cbm_log_error("autoindex.failed", log_key, log_value ? log_value : "");
        cbm_mem_collect();
        return false;
    }

    cbm_mem_collect();
    return true;
}

/* ── Tool list (needs full struct definition above) ──────────── */

typedef struct {
    int offset;
    int limit;
    int seen;
    int emitted;
} mcp_tool_page_t;

static bool mcp_tool_page_accept(mcp_tool_page_t *page) {
    int index = page->seen++;
    if (index < page->offset || page->emitted >= page->limit) {
        return false;
    }
    page->emitted++;
    return true;
}

/* Definitions live with the shared schema serializers below. The tools/list
 * description reuses them so executable patterns and base-property factoring
 * cannot drift from get_graph_schema. */
static bool schema_property_in_base(const char *property, const char *const *base,
                                    int base_count);
static char *schema_relationship_pattern_text(const cbm_schema_relationship_t *pattern,
                                              bool executable_match);
static cbm_store_t *resolve_store(cbm_mcp_server_t *srv, const char *project);

static void schema_description_append_int(cbm_sb_t *sb, int value) {
    char number[CBM_SZ_32];
    snprintf(number, sizeof(number), "%d", value);
    cbm_sb_append(sb, number);
}

static void schema_description_append_properties(cbm_sb_t *sb, char *const *properties,
                                                 int property_count,
                                                 const char *const *base, int base_count) {
    bool first = true;
    for (int i = 0; i < property_count; i++) {
        if (schema_property_in_base(properties[i], base, base_count)) {
            continue;
        }
        cbm_sb_append(sb, first ? "{" : ",");
        cbm_sb_append(sb, properties[i]);
        first = false;
    }
    if (!first) {
        cbm_sb_append(sb, "}");
    }
}

/* One schema-view selector shared by the tools/list description builder,
 * handle_get_graph_schema, and build_resource_schema: prefer the active
 * overlay view exactly when queries would see overlay rows, else canonical.
 * Guarantees the advertised vocabulary can never diverge from the view
 * queries actually run on, and ends the triplicated overlay_ready dance.
 * out_overlay_summary/out_used_overlay/out_overlay_failed are optional
 * (pass NULL when the caller does not report overlay freshness). */
/* True when queries against project would run on the active overlay view:
 * shared by mcp_get_current_schema and handle_query_graph so the two never
 * drift on what "overlay ready" means. */
static bool mcp_overlay_view_ready(cbm_store_t *store, const char *project,
                                   cbm_store_overlay_node_view_summary_t *summary) {
    return cbm_store_get_overlay_node_view_summary(store, project, summary) == CBM_STORE_OK &&
           cbm_store_overlay_node_view_has_ready_rows(summary);
}

/* How much Cypher query-writing vocabulary a schema fetch returns. The
 * language is Cypher (query_graph's read-only openCypher subset); the enum is
 * named for what a caller can DO with the result, because that is the
 * decision being made:
 *
 * MATCH_VOCABULARY — every label and edge type with its observed count, plus
 * every observed (source_label)-[type]->(target_label) pattern: exactly the
 * facts needed to compose executable MATCH clauses. Costs index-backed
 * GROUP BYs plus one capped O(E) pattern join; never parses property JSON.
 *
 * FULL_QUERY_VOCABULARY — adds the per-label/per-type property keys needed to
 * filter and project custom facts about a repo (WHERE n.key ..., RETURN
 * n.key). The extra json_each discovery is O(total property rows) — minutes-
 * scale on multi-million-node graphs — so only surfaces that actually render
 * property keys (the query_graph docstring, get_graph_schema) may request it,
 * and the docstring caches it once per publication. */
typedef enum {
    MCP_CYPHER_MATCH_VOCABULARY = 0,
    MCP_CYPHER_FULL_QUERY_VOCABULARY,
} mcp_cypher_vocabulary_t;

static int mcp_get_current_schema(cbm_store_t *store, const char *project,
                                  mcp_cypher_vocabulary_t vocabulary, cbm_schema_info_t *out,
                                  cbm_store_overlay_node_view_summary_t *out_overlay_summary,
                                  bool *out_used_overlay, bool *out_overlay_failed) {
    bool with_props = vocabulary == MCP_CYPHER_FULL_QUERY_VOCABULARY;
    if (out_used_overlay) {
        *out_used_overlay = false;
    }
    if (out_overlay_failed) {
        *out_overlay_failed = false;
    }
    if (!store) {
        memset(out, 0, sizeof(*out));
        return CBM_NOT_FOUND;
    }
    /* project may be NULL here: cbm_store_get_overlay_node_view_summary and
     * get_schema_impl are both NULL-project-safe (empty/false result, no
     * crash), matching every pre-refactor call site's tolerance. */
    cbm_store_overlay_node_view_summary_t local_summary = {0};
    cbm_store_overlay_node_view_summary_t *summary =
        out_overlay_summary ? out_overlay_summary : &local_summary;
    bool overlay_ready = mcp_overlay_view_ready(store, project, summary);
    if (overlay_ready) {
        int rc = with_props ? cbm_store_get_schema_overlay_view(store, project, out)
                            : cbm_store_get_schema_counts_overlay_view(store, project, out);
        if (rc == CBM_STORE_OK) {
            if (out_used_overlay) {
                *out_used_overlay = true;
            }
            return CBM_STORE_OK;
        }
        if (out_overlay_failed) {
            *out_overlay_failed = true;
        }
    }
    return with_props ? cbm_store_get_schema(store, project, out)
                      : cbm_store_get_schema_counts(store, project, out);
}

/* Copy only the selected project name into caller-owned stack storage. This is
 * not a database/graph snapshot: the bounded copy keeps the name valid if
 * resolve_store closes a replaced database and frees current_project. */
static const char *mcp_copy_schema_project_name(cbm_mcp_server_t *srv, char *out, size_t out_size) {
    if (!srv || !out || out_size == 0) {
        return NULL;
    }
    const char *source = srv->current_project && srv->current_project[0]
                             ? srv->current_project
                             : (srv->session_project[0] ? srv->session_project : NULL);
    if (!source) {
        return NULL;
    }
    int written = snprintf(out, out_size, "%s", source);
    return written >= 0 && (size_t)written < out_size ? out : NULL;
}

/* Build the query_graph docstring once per published graph state. It belongs in
 * MCP tools/list so reconnects and relists can restore actionable schema after
 * context compaction; it must never be appended to tools/call results. Full
 * property discovery is O(total property rows), so repeating it for every
 * paged tools/list request would turn catalog discovery into a graph scan. The
 * cache is request-thread owned; every graph mutation path must invalidate it,
 * while background indexing may only flip the atomic stale bit. */
static char *build_query_graph_tool_description(cbm_mcp_server_t *srv,
                                                const tool_def_t *tool_def) {
    cbm_sb_t sb;
    cbm_sb_init(&sb);
    cbm_sb_append(&sb, tool_def->description);
    int node_base_count = 0;
    int edge_base_count = 0;
    const char *const *node_base =
        cbm_store_schema_node_base_properties(&node_base_count);
    const char *const *edge_base =
        cbm_store_schema_edge_base_properties(&edge_base_count);
    cbm_sb_append(&sb, " Node properties: ");
    for (int i = 0; i < node_base_count; i++) {
        cbm_sb_append(&sb, i ? ", " : "");
        cbm_sb_append(&sb, node_base[i]);
    }
    cbm_sb_append(&sb, ". Relationship properties: ");
    for (int i = 0; i < edge_base_count; i++) {
        cbm_sb_append(&sb, i ? ", " : "");
        cbm_sb_append(&sb, edge_base[i]);
    }
    cbm_sb_append(&sb, ".");
    cbm_sb_append(&sb, " Discovery bounds: up to ");
    schema_description_append_int(&sb, CBM_STORE_SCHEMA_PROPERTY_KEY_LIMIT);
    cbm_sb_append(&sb, " extra property keys per label/type and ");
    schema_description_append_int(&sb, CBM_STORE_SCHEMA_RELATIONSHIP_PATTERN_LIMIT);
    cbm_sb_append(&sb, " observed relationship patterns.");

    /* Snapshot into a local buffer rather than aliasing srv->current_project
     * directly: resolve_store() below may call reap_stale_store(), which
     * frees srv->current_project mid-call when a deferred invalidation is
     * pending (cbm_mcp_server_notify_index_published sets store_stale
     * without touching current_project — the request thread reaps it on
     * next use). Passing the live pointer through would free the very
     * string resolve_store is still reading (use-after-free). */
    char project_buf[CBM_SZ_256];
    const char *project = mcp_copy_schema_project_name(srv, project_buf, sizeof(project_buf));
    /* Lazily resolve the project's real store rather than trusting
     * srv->store as-is: on the very first tools/list of a fresh session
     * (before any tool call has resolved a project), srv->store is still
     * the empty default in-memory store even though the project is
     * already indexed on disk — silently producing an empty schema
     * section. resolve_store is the same lazy-open used by every other
     * handler and no-ops (fast path) once already resolved. */
    cbm_store_t *store = srv ? resolve_store(srv, project) : NULL;
    cbm_schema_info_t schema = {0};
    if (!store || !project ||
        mcp_get_current_schema(store, project, MCP_CYPHER_FULL_QUERY_VOCABULARY, &schema, NULL,
                               NULL, NULL) !=
            CBM_STORE_OK) {
        return cbm_sb_finish(&sb);
    }

    cbm_sb_append(&sb, " Current project schema for ");
    cbm_sb_append(&sb, project);
    /* Notation legend, embedded in the description string itself so it is
     * present in EVERY tools/list response (repeated relists, reconnects,
     * post-compaction recovery — the delivery contract). It is spelled out
     * here in the Labels header and the edge/pattern sections reuse the same
     * name{extra property keys}[row count] notation, so the legend costs its
     * bytes once per description, not once per section. */
    cbm_sb_append(&sb, ". Labels name{extra property keys}[count]: ");
    for (int i = 0; i < schema.node_label_count; i++) {
        cbm_sb_append(&sb, i ? "; " : "");
        cbm_sb_append(&sb, schema.node_labels[i].label);
        schema_description_append_properties(&sb, schema.node_labels[i].properties,
                                             schema.node_labels[i].property_count, node_base,
                                             node_base_count);
        cbm_sb_append(&sb, "[");
        schema_description_append_int(&sb, schema.node_labels[i].count);
        cbm_sb_append(&sb, "]");
    }
    cbm_sb_append(&sb, ". Edge types[count]: ");
    for (int i = 0; i < schema.edge_type_count; i++) {
        cbm_sb_append(&sb, i ? "; " : "");
        cbm_sb_append(&sb, schema.edge_types[i].type);
        schema_description_append_properties(&sb, schema.edge_types[i].properties,
                                             schema.edge_types[i].property_count, edge_base,
                                             edge_base_count);
        cbm_sb_append(&sb, "[");
        schema_description_append_int(&sb, schema.edge_types[i].count);
        cbm_sb_append(&sb, "]");
    }
    cbm_sb_append(&sb, ". Observed executable patterns[count]: ");
    for (int i = 0; i < schema.rel_pattern_count; i++) {
        char *match = schema_relationship_pattern_text(&schema.rel_patterns[i], true);
        if (!match) {
            cbm_store_schema_free(&schema);
            cbm_sb_free(&sb);
            return NULL;
        }
        cbm_sb_append(&sb, i ? "; " : "");
        cbm_sb_append(&sb, match);
        cbm_sb_append(
            &sb,
            " RETURN source.qualified_name,target.qualified_name LIMIT 20 [");
        schema_description_append_int(&sb, schema.rel_patterns[i].observed_count);
        cbm_sb_append(&sb, "]");
        free(match);
    }
    cbm_sb_append(
        &sb,
        ". These are examples, not restrictions: write a custom effective, computationally "
        "efficient query for the current problem.");
    cbm_store_schema_free(&schema);
    return cbm_sb_finish(&sb);
}

static const char *query_graph_tool_description(cbm_mcp_server_t *srv,
                                                const tool_def_t *tool_def) {
    if (!srv) {
        return tool_def->description;
    }
    if (srv->query_graph_tool_description) {
        /* A sibling CLI/MCP/HTTP worker cannot flip this process's stale bit.
         * Probe the cached query-only store's stable file identity on each
         * relist so an atomic database replacement invalidates the inline
         * schema before it is served. resolve_store is O(1) on the unchanged
         * fast path and closes/reopens only on this request-owning thread. */
        char project_buf[CBM_SZ_256];
        const char *project = mcp_copy_schema_project_name(srv, project_buf, sizeof(project_buf));
        if (project) {
            (void)resolve_store(srv, project);
        }
    }
    if (atomic_exchange(&srv->query_graph_tool_description_stale, false)) {
        free(srv->query_graph_tool_description);
        srv->query_graph_tool_description = NULL;
    }
    if (!srv->query_graph_tool_description) {
        srv->query_graph_tool_description = build_query_graph_tool_description(srv, tool_def);
    }
    return srv->query_graph_tool_description ? srv->query_graph_tool_description
                                             : tool_def->description;
}

static char *cbm_mcp_tools_list_range(cbm_mcp_server_t *srv, int offset, int limit,
                                      bool include_next_cursor) {
    /* Analysis/scout are explicit curated surfaces, independent of the user's
     * classic-vs-streamlined preference. Emit every canonical tool allowed by
     * the selected profile; do not hide required diagnostics behind reveal. */
    bool curated_profile = srv && srv->tool_profile != CBM_MCP_TOOL_PROFILE_ALL;
    bool classic = curated_profile || cbm_mcp_tool_mode_is_classic(srv);
    bool reveal_hidden = (!classic && srv && srv->hidden_tools_revealed);
    mcp_tool_page_t page = {
        .offset = offset > 0 ? offset : 0,
        .limit = limit > 0 ? limit : MCP_TOOLS_PAGE_SIZE,
    };

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_val *tools = yyjson_mut_arr(doc);

    if (!classic) {
        /* Streamlined mode: default surface = focused graph/text tools from
         * TOOLS[] plus fork-only aliases from STREAMLINED_TOOLS[]. Keeping
         * canonical tools in TOOLS[] prevents schema drift between modes. */
        for (int i = 0; i < TOOL_COUNT; i++) {
            if (is_streamlined_default_tool(TOOLS[i].name) &&
                mcp_tool_allowed(srv ? srv->tool_profile : CBM_MCP_TOOL_PROFILE_ALL,
                                 TOOLS[i].name) &&
                mcp_tool_page_accept(&page)) {
                const char *description = strcmp(TOOLS[i].name, "query_graph") == 0
                                              ? query_graph_tool_description(srv, &TOOLS[i])
                                              : NULL;
                emit_tool(doc, tools, &TOOLS[i], description);
            }
        }
        for (int i = 0; i < STREAMLINED_TOOL_COUNT; i++) {
            if (mcp_tool_allowed(srv ? srv->tool_profile : CBM_MCP_TOOL_PROFILE_ALL,
                                 STREAMLINED_TOOLS[i].name) &&
                mcp_tool_page_accept(&page)) {
                emit_tool(doc, tools, &STREAMLINED_TOOLS[i], NULL);
            }
        }
        /* Also emit individually-enabled tools, or every advanced tool after
         * _hidden_tools has explicitly revealed them for this server process.
         * This keeps the initial streamlined list compact while making hidden
         * tools discoverable to real MCP clients that only call listed tools.
         * trace_path is already listed from TOOLS[] above, so skip it here.
         * (get_code is streamlined-only; get_code_snippet is the TOOLS[] name.) */
        for (int i = 0; i < TOOL_COUNT; i++) {
            if (is_streamlined_default_tool(TOOLS[i].name)) {
                continue;
            }
            if (mcp_tool_allowed(srv ? srv->tool_profile : CBM_MCP_TOOL_PROFILE_ALL,
                                 TOOLS[i].name) &&
                (reveal_hidden || cbm_mcp_tool_config_enabled(srv, TOOLS[i].name))) {
                if (mcp_tool_page_accept(&page)) {
                    const char *description = strcmp(TOOLS[i].name, "query_graph") == 0
                                                  ? query_graph_tool_description(srv, &TOOLS[i])
                                                  : NULL;
                    emit_tool(doc, tools, &TOOLS[i], description);
                }
            }
        }

        /* Progressive disclosure: list advanced tools so AI knows they exist.
         * Added as a special tool entry with description explaining how to enable.
         * Default-surface tools (search_graph, query_graph, search_code,
         * trace_path, get_code) are NOT listed here. */
        if ((!srv || srv->tool_profile == CBM_MCP_TOOL_PROFILE_ALL) &&
            mcp_tool_page_accept(&page)) {
            yyjson_mut_val *hint_tool = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_str(doc, hint_tool, "name", "_hidden_tools");
            yyjson_mut_obj_add_str(doc, hint_tool, "title", "Advanced tools");
            yyjson_mut_obj_add_str(doc, hint_tool, "description",
            "Advanced tools are normally hidden in streamlined mode. "
            "Advanced tools: index_repository, get_code_snippet, "
            "get_graph_schema, get_architecture, list_projects, "
            "delete_project, index_status, check_index_coverage, detect_changes, manage_adr, "
            "ingest_traces, index_dependencies. "
            "Graph-backed default tools auto-index the server CWD or explicit directory projects "
            "when auto_index=true and auto_index_limit is not exceeded; search_code searches "
            "source files for an already indexed/current project. "
            "Call this tool to reveal these tools in tools/list for clients that "
            "only allow discovered tools. "
            "Enable all: set env CBM_TOOL_MODE=classic or config set tool_mode classic. "
            "Enable one: config set tool_<name> true (e.g. tool_index_repository true). "
            "Resources: codebase://schema (labels, edge types, Cypher examples), "
            "codebase://architecture (key functions, graph overview), "
            "codebase://status (index state: ready/indexing/not_indexed/empty).");
        /* inputSchema MUST be a JSON object, not a string — Claude Code rejects
         * the entire tools/list if any tool has a string inputSchema. */
            yyjson_mut_val *hint_schema = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_str(doc, hint_schema, "type", "object");
            yyjson_mut_val *hint_props = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_val(doc, hint_schema, "properties", hint_props);
            yyjson_mut_obj_add_bool(doc, hint_schema, "additionalProperties", false);
            yyjson_mut_obj_add_val(doc, hint_tool, "inputSchema", hint_schema);
            mcp_add_json_schema(doc, hint_tool, "outputSchema", MCP_TOOL_OUTPUT_SCHEMA);
            yyjson_mut_arr_add_val(tools, hint_tool);
        }
    } else {
        /* Classic mode: all original tools. trace_path is the upstream-listed
         * name and the single canonical call-tracing tool. */
        for (int i = 0; i < TOOL_COUNT; i++) {
            if (mcp_tool_allowed(srv ? srv->tool_profile : CBM_MCP_TOOL_PROFILE_ALL,
                                 TOOLS[i].name) &&
                mcp_tool_page_accept(&page)) {
                const char *description = strcmp(TOOLS[i].name, "query_graph") == 0
                                              ? query_graph_tool_description(srv, &TOOLS[i])
                                              : NULL;
                emit_tool(doc, tools, &TOOLS[i], description);
            }
        }
    }

    yyjson_mut_obj_add_val(doc, root, "tools", tools);
    if (include_next_cursor && page.offset + page.emitted < page.seen) {
        char cursor[CBM_SZ_32];
        snprintf(cursor, sizeof(cursor), "%d", page.offset + page.emitted);
        yyjson_mut_obj_add_strcpy(doc, root, "nextCursor", cursor);
    }

    char *out = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return out;
}

char *cbm_mcp_tools_list(cbm_mcp_server_t *srv) {
    return cbm_mcp_tools_list_range(srv, 0, CBM_SZ_64K, false);
}

static int mcp_tools_cursor_offset(const char *params_json, bool *has_cursor_out) {
    *has_cursor_out = false;
    if (!params_json) {
        return 0;
    }
    yyjson_doc *doc = yyjson_read(params_json, strlen(params_json), 0);
    if (!doc) {
        return 0;
    }
    yyjson_val *cursor = yyjson_obj_get(yyjson_doc_get_root(doc), "cursor");
    int offset = 0;
    if (cursor) {
        *has_cursor_out = true;
        offset = CBM_SZ_64K;
        if (yyjson_is_str(cursor)) {
            const char *value = yyjson_get_str(cursor);
            char *end = NULL;
            errno = 0;
            long parsed = value ? strtol(value, &end, CBM_DECIMAL_BASE) : -1;
            if (value && value[0] && end && end[0] == '\0' && errno == 0 && parsed >= 0) {
                offset = parsed > CBM_SZ_64K ? CBM_SZ_64K : (int)parsed;
            }
        }
    }
    yyjson_doc_free(doc);
    return offset;
}

static char *cbm_mcp_tools_list_page(cbm_mcp_server_t *srv, const char *params_json) {
    bool has_cursor = false;
    int offset = mcp_tools_cursor_offset(params_json, &has_cursor);
    return has_cursor ? cbm_mcp_tools_list_range(srv, offset, MCP_TOOLS_PAGE_SIZE, true)
                      : cbm_mcp_tools_list(srv);
}

cbm_mcp_server_t *cbm_mcp_server_new(const char *store_path) {
    cbm_mcp_server_t *srv = calloc(CBM_ALLOC_ONE, sizeof(*srv));
    if (!srv) {
        return NULL;
    }
    cbm_mutex_init(&srv->update_notice_lock);
    cbm_mutex_init(&srv->active_request_lock);

    /* If a store_path is given, open that project directly.
     * Otherwise, create an in-memory store for test/embedded use. */
    if (store_path) {
        srv->store = cbm_store_open(store_path);
        srv->current_project = heap_strdup(store_path);
    } else {
        srv->store = cbm_store_open_memory();
    }
    srv->owns_store = true;
    srv->tool_profile = CBM_MCP_TOOL_PROFILE_ALL;

    cbm_mutex_init(&srv->overlay_compaction_lock);
    return srv;
}

void cbm_mcp_server_set_tool_profile(cbm_mcp_server_t *srv,
                                     cbm_mcp_tool_profile_t profile) {
    if (srv) {
        srv->tool_profile = profile;
    }
}

cbm_store_t *cbm_mcp_server_store(cbm_mcp_server_t *srv) {
    return srv ? srv->store : NULL;
}

void cbm_mcp_server_set_project(cbm_mcp_server_t *srv, const char *project) {
    if (!srv) {
        return;
    }
    free(srv->current_project);
    srv->current_project = project ? heap_strdup(project) : NULL;
    atomic_store(&srv->query_graph_tool_description_stale, true);
}

void cbm_mcp_server_set_session_project(cbm_mcp_server_t *srv, const char *name) {
    if (!srv || !name) return;
    snprintf(srv->session_project, sizeof(srv->session_project), "%s", name);
    atomic_store(&srv->query_graph_tool_description_stale, true);
}

void cbm_mcp_server_set_watcher(cbm_mcp_server_t *srv, struct cbm_watcher *w) {
    if (srv) {
        srv->watcher = w;
    }
}

void cbm_mcp_server_set_config(cbm_mcp_server_t *srv, struct cbm_config *cfg) {
    if (srv) {
        srv->config = cfg;
    }
}

int cbm_mcp_server_join_overlay_compaction(cbm_mcp_server_t *srv, int *out_compacted) {
    if (out_compacted) {
        *out_compacted = 0;
    }
    if (!srv) {
        return CBM_STORE_ERR;
    }

    cbm_mutex_lock(&srv->overlay_compaction_lock);
    bool should_join = srv->overlay_compaction_started;
    cbm_mutex_unlock(&srv->overlay_compaction_lock);
    if (!should_join) {
        return CBM_STORE_OK;
    }

    int join_rc = cbm_thread_join(&srv->overlay_compaction_tid);

    cbm_mutex_lock(&srv->overlay_compaction_lock);
    int worker_rc = srv->overlay_compaction_rc;
    int compacted = srv->overlay_compaction_compacted;
    srv->overlay_compaction_started = false;
    srv->overlay_compaction_finished = false;
    srv->overlay_compaction_project[0] = '\0';
    srv->overlay_compaction_max_generations = CBM_STORE_COMPACT_ALL_GENERATIONS;
    srv->overlay_compaction_rc = CBM_STORE_OK;
    srv->overlay_compaction_compacted = 0;
    cbm_mutex_unlock(&srv->overlay_compaction_lock);

    if (out_compacted) {
        *out_compacted = compacted;
    }
    return join_rc == 0 ? worker_rc : CBM_STORE_ERR;
}

bool cbm_mcp_server_overlay_compaction_active(cbm_mcp_server_t *srv) {
    if (!srv) {
        return false;
    }
    cbm_mutex_lock(&srv->overlay_compaction_lock);
    bool active = srv->overlay_compaction_started && !srv->overlay_compaction_finished;
    cbm_mutex_unlock(&srv->overlay_compaction_lock);
    return active;
}

static void add_overlay_compaction_worker_status(cbm_mcp_server_t *srv, yyjson_mut_doc *doc,
                                                 yyjson_mut_val *root) {
    if (!srv || !doc || !root) {
        return;
    }

    bool started = false;
    bool finished = false;
    char project[CBM_SZ_256];
    int max_generations = CBM_STORE_COMPACT_ALL_GENERATIONS;
    int rc = CBM_STORE_OK;
    int compacted = 0;
    project[0] = '\0';

    cbm_mutex_lock(&srv->overlay_compaction_lock);
    started = srv->overlay_compaction_started;
    finished = srv->overlay_compaction_finished;
    int n = snprintf(project, sizeof(project), "%s", srv->overlay_compaction_project);
    if (n < 0 || (size_t)n >= sizeof(project)) {
        project[0] = '\0';
    }
    max_generations = srv->overlay_compaction_max_generations;
    rc = srv->overlay_compaction_rc;
    compacted = srv->overlay_compaction_compacted;
    cbm_mutex_unlock(&srv->overlay_compaction_lock);

    yyjson_mut_val *status = yyjson_mut_obj(doc);
    const char *state = !started ? "idle" : (finished ? "finished" : "running");
    yyjson_mut_obj_add_str(doc, status, "state", state);
    if (project[0]) {
        yyjson_mut_obj_add_strcpy(doc, status, "project", project);
    }
    if (started) {
        yyjson_mut_obj_add_int(doc, status, "max_generations", max_generations);
    }
    if (finished) {
        yyjson_mut_obj_add_int(doc, status, "result_rc", rc);
        yyjson_mut_obj_add_int(doc, status, "compacted_generations", compacted);
    }
    yyjson_mut_obj_add_val(doc, root, "overlay_compaction", status);
}

void cbm_mcp_server_free(cbm_mcp_server_t *srv) {
    if (!srv) {
        return;
    }
    /* Free is a shutdown boundary, not a wait-for-work API. Tell a supervised
     * auto-index child to terminate before joining its owner thread. */
    cbm_mcp_server_request_stop(srv);
    if (srv->update_thread_active) {
        cbm_thread_join(&srv->update_tid);
    }
    (void)cbm_mcp_server_join_autoindex(srv);
    (void)cbm_mcp_server_join_overlay_compaction(srv, NULL);
    cbm_mutex_destroy(&srv->overlay_compaction_lock);
    cbm_mutex_destroy(&srv->update_notice_lock);
    cbm_mutex_destroy(&srv->active_request_lock);
    if (srv->owns_store && srv->store) {
        cbm_store_close(srv->store);
    }
    free(srv->current_project);
    free(srv->query_graph_tool_description);
    free(srv->active_request_id_str);
    free(srv);
}

int cbm_mcp_server_join_autoindex(cbm_mcp_server_t *srv) {
    if (!srv || !srv->autoindex_active) {
        return 0;
    }
    int rc = cbm_thread_join(&srv->autoindex_tid);
    srv->autoindex_active = false;
    return rc;
}

/* ── Idle store eviction ──────────────────────────────────────── */

void cbm_mcp_server_evict_idle(cbm_mcp_server_t *srv, int timeout_s) {
    if (!srv || !srv->store) {
        return;
    }
    /* Protect initial in-memory stores that were never accessed via a named project.
     * store_last_used stays 0 until resolve_store is called with a non-NULL project. */
    if (srv->store_last_used == 0) {
        return;
    }

    time_t now = time(NULL);
    if ((now - srv->store_last_used) < timeout_s) {
        return;
    }

    if (srv->owns_store) {
        cbm_store_close(srv->store);
    }
    srv->store = NULL;
    free(srv->current_project);
    srv->current_project = NULL;
    srv->store_last_used = 0;
}

bool cbm_mcp_server_has_cached_store(cbm_mcp_server_t *srv) {
    return (srv && srv->store != NULL) != 0;
}

cbm_pipeline_t *cbm_mcp_server_active_pipeline(cbm_mcp_server_t *srv) {
    return srv ? atomic_load_explicit(&srv->active_pipeline, memory_order_acquire) : NULL;
}

/* ── Cache dir + project DB path helpers ───────────────────────── */

/* Returns the cache directory. Writes to buf, returns buf for convenience. */
static const char *cache_dir(char *buf, size_t bufsz) {
    const char *dir = cbm_resolve_cache_dir();
    if (!dir) {
        dir = cbm_tmpdir();
    }
    int len = snprintf(buf, bufsz, "%s", dir);
    if (len <= 0 || (size_t)len >= bufsz) {
        if (bufsz > 0) {
            buf[0] = '\0';
        }
    }
    return buf;
}

/* Returns full .db path for a project: <cache_dir>/<project>.db */
static const char *project_db_path(const char *project, char *buf, size_t bufsz) {
    if (!cbm_validate_project_name(project)) {
        buf[0] = '\0';
        return buf;
    }
    char dir[CBM_SZ_1K];
    cache_dir(dir, sizeof(dir));
    int len = snprintf(buf, bufsz, "%s/%s.db", dir, project);
    if (len <= 0 || (size_t)len >= bufsz) {
        if (bufsz > 0) {
            buf[0] = '\0';
        }
    }
    return buf;
}

bool cbm_mcp_server_start_overlay_compaction(cbm_mcp_server_t *srv, const char *project,
                                             int max_generations) {
    if (!srv || !project || !project[0] ||
        max_generations < CBM_STORE_COMPACT_ALL_GENERATIONS) {
        return false;
    }
    size_t project_len = strlen(project);
    if (project_len == 0 || project_len >= sizeof(srv->overlay_compaction_project)) {
        return false;
    }
    if (!cbm_validate_project_name(project)) {
        return false;
    }

    bool reaped_finished_worker = false;
    for (;;) {
        cbm_mutex_lock(&srv->overlay_compaction_lock);
        if (!srv->overlay_compaction_started) {
            int n = snprintf(srv->overlay_compaction_project,
                             sizeof(srv->overlay_compaction_project), "%s", project);
            if (n < 0 || (size_t)n >= sizeof(srv->overlay_compaction_project)) {
                cbm_mutex_unlock(&srv->overlay_compaction_lock);
                return false;
            }
            srv->overlay_compaction_max_generations = max_generations;
            srv->overlay_compaction_rc = CBM_STORE_ERR;
            srv->overlay_compaction_compacted = 0;
            srv->overlay_compaction_finished = false;
            srv->overlay_compaction_started = true;
            cbm_mutex_unlock(&srv->overlay_compaction_lock);
            break;
        }
        bool finished = srv->overlay_compaction_finished;
        cbm_mutex_unlock(&srv->overlay_compaction_lock);
        if (!finished || reaped_finished_worker) {
            return false;
        }
        int compacted = 0;
        int join_rc = cbm_mcp_server_join_overlay_compaction(srv, &compacted);
        if (join_rc != CBM_STORE_OK) {
            char rc_buf[CBM_SZ_32];
            char compacted_buf[CBM_SZ_32];
            snprintf(rc_buf, sizeof(rc_buf), "%d", join_rc);
            snprintf(compacted_buf, sizeof(compacted_buf), "%d", compacted);
            cbm_log_warn("overlay_compaction.reap_failed", "rc", rc_buf, "compacted",
                         compacted_buf);
        }
        reaped_finished_worker = true;
    }

    if (cbm_thread_create(&srv->overlay_compaction_tid, 0, overlay_compaction_thread,
                          srv) != 0) {
        cbm_mutex_lock(&srv->overlay_compaction_lock);
        srv->overlay_compaction_started = false;
        srv->overlay_compaction_finished = false;
        srv->overlay_compaction_project[0] = '\0';
        srv->overlay_compaction_max_generations = CBM_STORE_COMPACT_ALL_GENERATIONS;
        srv->overlay_compaction_rc = CBM_STORE_ERR;
        srv->overlay_compaction_compacted = 0;
        cbm_mutex_unlock(&srv->overlay_compaction_lock);
        return false;
    }
    return true;
}

static void *overlay_compaction_thread(void *arg) {
    cbm_mcp_server_t *srv = (cbm_mcp_server_t *)arg;
    char project[CBM_SZ_256];
    int max_generations = CBM_STORE_COMPACT_ALL_GENERATIONS;
    project[0] = '\0';

    cbm_mutex_lock(&srv->overlay_compaction_lock);
    int n = snprintf(project, sizeof(project), "%s", srv->overlay_compaction_project);
    if (n < 0 || (size_t)n >= sizeof(project)) {
        project[0] = '\0';
    }
    max_generations = srv->overlay_compaction_max_generations;
    cbm_mutex_unlock(&srv->overlay_compaction_lock);

    int rc = CBM_STORE_ERR;
    int compacted = 0;
    char path[CBM_SZ_1K];
    project_db_path(project, path, sizeof(path));
    if (path[0]) {
        cbm_store_t *store = cbm_store_open_path_existing(path);
        if (store) {
            rc = cbm_store_compact_ready_overlay_generations(store, project, max_generations,
                                                             &compacted);
            cbm_store_close(store);
        } else {
            rc = CBM_STORE_NOT_FOUND;
        }
    }

    if (rc == CBM_STORE_OK && compacted > 0) {
        /* Compaction moved overlay facts into canonical rows. Visible schema
         * should be content-identical, but the description builder reads
         * different tables afterward. Flags only. */
        cbm_mcp_server_notify_index_published(srv);
    }

    cbm_mutex_lock(&srv->overlay_compaction_lock);
    srv->overlay_compaction_rc = rc;
    srv->overlay_compaction_compacted = compacted;
    srv->overlay_compaction_finished = true;
    cbm_mutex_unlock(&srv->overlay_compaction_lock);
    return NULL;
}

/* ── QN project extraction ─────────────────────────────────────── */

/* Try to identify the project prefix of a qualified name by scanning each
 * dot-separated prefix and checking if a matching DB file exists.
 * Returns a heap-allocated project name (caller must free), or NULL if no
 * matching DB is found.  Cost: one path-existence check per dot in the QN (~5-10). */
static char *extract_project_from_qn(const char *qn) {
    if (!qn) return NULL;
    if (!cbm_resolve_cache_dir()) return NULL;

    /* Scan each dot-separated prefix of the QN and test if a matching DB file
     * exists.  Walk left-to-right so the last hit is the longest (most
     * specific) match.  Record only the winning offset to do a single strdup
     * at the end — avoids repeated alloc/free on multi-dot project names. */
    size_t qn_len = strlen(qn);
    char *candidate = malloc(qn_len + 1);
    if (!candidate) return NULL;
    memcpy(candidate, qn, qn_len + 1);

    size_t best_end = 0; /* length of the longest matching prefix found */
    char db_path[MCP_FIELD_SIZE];

    for (size_t i = 0; i < qn_len; i++) {
        if (candidate[i] == '.') {
            candidate[i] = '\0';
            project_db_path(candidate, db_path, sizeof(db_path));
            if (db_path[0] && cbm_file_exists(db_path)) {
                best_end = i; /* length of this prefix */
            }
            candidate[i] = '.';
        }
    }

    char *result = NULL;
    if (best_end > 0) {
        result = malloc(best_end + 1);
        if (result) {
            memcpy(result, qn, best_end);
            result[best_end] = '\0';
        }
    }
    free(candidate);
    return result; /* NULL if no matching DB found; caller frees */
}

/* ── Store resolution ──────────────────────────────────────────── */

/* Read the sole INTERNAL project name from a .db file at full_path.
 * Opens the file query-mode (no create) and succeeds ONLY when the db holds
 * exactly one project row with a non-empty name — this filters ghost/empty
 * /corrupt dbs (0-byte file, missing `projects` table, or >1 row). On success
 * the internal name is copied into name_out; if out_store is non-NULL the open
 * handle is transferred to the caller (who must cbm_store_close it). On failure
 * the store is always closed. Defined after is_project_db_file below. */
static bool db_internal_project_name(const char *full_path, char *name_out, size_t name_sz,
                                     cbm_store_t **out_store);

/* #704 fallback: scan the cache dir for the db whose sole internal project name
 * equals `project`, returning an open store handle (caller owns it) or NULL.
 * Used only when <project>.db is absent or its internal name differs from the
 * passed name (drifted filename). Defined after is_project_db_file below. */
static cbm_store_t *resolve_store_fallback_scan(const char *project);

/* Open the right project's .db file for query tools.
 * Caches the connection — reopens only when project changes.
 * Tracks last-access time so the event loop can evict idle stores. */
/* Extract the parent project name from a dep project name.
 * "myapp.dep.pandas" → "myapp", "myapp.dep" → "myapp", "myapp" → "myapp".
 * Returns a stack buffer pointer (caller must NOT free). */
static const char *parent_project_for_db(const char *project, char *buf, size_t bufsz) {
    const char *dep = strstr(project, ".dep");
    if (dep && (dep[4] == '.' || dep[4] == '\0')) {
        size_t len = (size_t)(dep - project);
        if (len >= bufsz) len = bufsz - 1;
        memcpy(buf, project, len);
        buf[len] = '\0';
        return buf;
    }
    return project; /* no .dep → use as-is */
}

static bool root_path_looks_usable(const char *root_path) {
    if (!root_path || !root_path[0]) {
        return false;
    }
    if (root_path[0] == '/' || root_path[0] == '\\') {
        return true;
    }
    return ((root_path[0] >= 'A' && root_path[0] <= 'Z') ||
            (root_path[0] >= 'a' && root_path[0] <= 'z')) &&
           root_path[1] == ':';
}

static void sync_session_from_open_project(cbm_mcp_server_t *srv, cbm_store_t *store,
                                           const char *db_project,
                                           const cbm_project_t *opened_project) {
    if (!srv || srv->session_project[0] || !store || !db_project || !db_project[0]) {
        return;
    }

    const char *root_path = opened_project ? opened_project->root_path : NULL;
    cbm_project_t parent = {0};
    if (cbm_store_get_project(store, db_project, &parent) == CBM_STORE_OK) {
        root_path = parent.root_path;
    }

    snprintf(srv->session_project, sizeof(srv->session_project), "%s", db_project);
    if (root_path_looks_usable(root_path)) {
        snprintf(srv->session_root, sizeof(srv->session_root), "%s", root_path);
    }
    cbm_project_free_fields(&parent);
}

static bool mcp_join_suffix(char *out, size_t out_sz, const char *base, const char *suffix) {
    int n = snprintf(out, out_sz, "%s%s", base ? base : "", suffix ? suffix : "");
    return n > 0 && (size_t)n < out_sz;
}

static void quarantine_corrupt_sidecar(const char *path, const char *quarantine_path,
                                       const char *suffix) {
    char src[MCP_FIELD_SIZE];
    char dst[MCP_FIELD_SIZE];
    if (!mcp_join_suffix(src, sizeof(src), path, suffix) ||
        !mcp_join_suffix(dst, sizeof(dst), quarantine_path, suffix)) {
        cbm_log_warn("store.quarantine_sidecar_skip", "reason", "path_too_long", "suffix",
                     suffix ? suffix : "");
        return;
    }

    if (!cbm_file_exists(src)) {
        return;
    }
    if (cbm_file_exists(dst)) {
        cbm_log_warn("store.quarantine_sidecar_skip", "path", src, "reason",
                     "quarantine_exists");
        return;
    }
    if (cbm_move_file_no_replace(src, dst) != 0) {
        cbm_log_warn("store.quarantine_sidecar_failed", "path", src);
    }
}

static bool quarantine_corrupt_db(const char *path, int validate_busy_timeout_ms) {
    char quarantine_path[MCP_FIELD_SIZE];
    if (!mcp_join_suffix(quarantine_path, sizeof(quarantine_path), path, ".corrupt")) {
        cbm_log_error("store.quarantine_failed", "reason", "path_too_long", "path",
                      path ? path : "");
        return false;
    }
    if (!validate_cbm_db_with_timeout(path, validate_busy_timeout_ms)) {
        cbm_log_error("store.quarantine_failed", "reason", "not_cbm_cache_schema", "path",
                      path ? path : "");
        return false;
    }

    if (cbm_file_exists(quarantine_path)) {
        cbm_log_error("store.quarantine_failed", "reason", "quarantine_exists", "path",
                      quarantine_path);
        return false;
    }
    if (cbm_move_file_no_replace(path, quarantine_path) != 0) {
        cbm_log_error("store.quarantine_failed", "reason", "move_failed", "path",
                      path ? path : "");
        return false;
    }

    quarantine_corrupt_sidecar(path, quarantine_path, "-wal");
    quarantine_corrupt_sidecar(path, quarantine_path, "-shm");
    return true;
}

static cbm_store_t *resolve_store(cbm_mcp_server_t *srv, const char *project) {
    if (!project || project[0] == '\0') {
        /* No project name: return the current in-memory/default store if available.
         * This enables cbm_mcp_server_new(NULL) in-memory stores for tests and
         * embedded use without requiring an explicit project name. */
        return srv->store;
    }

    srv->store_last_used = time(NULL);

    /* Consume a deferred invalidation from a supervised worker reap (possibly
     * on the auto-index thread) BEFORE trusting the cached handle: the worker
     * replaced the DB file, so the fast path below must not serve the stale
     * connection to the unlinked inode. */
    reap_stale_store(srv);

    /* Dep projects (e.g., "myapp.dep.pandas") live in the parent project's DB
     * ("myapp.db"), not in a separate "myapp.dep.pandas.db". Extract parent. */
    char parent_buf[1024];
    const char *db_project = parent_project_for_db(project, parent_buf, sizeof(parent_buf));

    /* Already open for this project's DB? A sibling CLI/MCP process cannot
     * raise this server's in-memory publication flag, so compare the stable
     * filesystem identity before trusting a cached handle. Atomic replacement
     * changes identity without exposing a partial DB; a missing/replaced path
     * is closed here on the request thread, preserving store ownership. */
    if (srv->current_project && strcmp(srv->current_project, db_project) == 0 && srv->store) {
        if (!cbm_store_backing_file_replaced(srv->store)) {
            return srv->store;
        }
        /* The same identity mismatch that invalidates query rows also
         * invalidates cached tools/list schema. This is request-thread-owned;
         * sibling liveness is never inferred and no cross-thread free occurs. */
        atomic_store(&srv->query_graph_tool_description_stale, true);
        if (srv->owns_store) {
            cbm_store_close(srv->store);
        }
        srv->store = NULL;
        free(srv->current_project);
        srv->current_project = NULL;
    }

    /* Close old store */
    if (srv->owns_store && srv->store) {
        cbm_store_close(srv->store);
        srv->store = NULL;
    }

    /* Open project's .db file — query-only open (no SQLITE_OPEN_CREATE) to
     * prevent ghost .db file creation for unknown/unindexed projects. */
    char path[CBM_SZ_1K];
    project_db_path(db_project, path, sizeof(path));
    if (!path[0]) {
        return NULL;
    }
    int validate_busy_timeout_ms = cbm_mcp_db_validate_busy_timeout_ms(srv);
    srv->store = validate_cbm_db_with_timeout(path, validate_busy_timeout_ms)
                     ? cbm_store_open_path_query(path)
                     : NULL;
    if (srv->store) {
        /* Check DB integrity before serving a cache database. A bad project
         * root_path (with an otherwise-fine projects table) is cosmetic: the
         * indexed nodes/edges are intact and queries key off project name, not
         * root_path. Retain such DBs instead of deleting them, to avoid the
         * data loss reported in #557. Only genuine structural corruption is
         * quarantined out of the active derived-cache path. */
        bool path_only = false;
        if (!cbm_store_check_integrity_full(srv->store, &path_only)) {
            if (path_only) {
                cbm_log_warn("store.integrity_retain", "project", project, "path", path,
                             "reason", "bad project root_path only; data retained");
                /* Fall through and keep srv->store open. */
            } else {
                cbm_log_error("store.quarantine", "project", project, "path", path, "action",
                              "quarantining corrupt cache db to .corrupt; re-index required");
                cbm_store_close(srv->store);
                srv->store = NULL;
                if (!quarantine_corrupt_db(path, validate_busy_timeout_ms)) {
                    cbm_log_error("store.quarantine", "project", project, "path", path, "action",
                                  "corrupt cache db retained; quarantine failed");
                }
                return NULL;
            }
        }

        /* Verify the project actually exists in this database.
         * A .db file may exist but be empty (e.g., after delete_project on
         * Linux where unlink defers actual removal). Opening an empty/deleted
         * store without closing it leaks the SQLite connection. */
        cbm_project_t proj_verify = {0};
        if (cbm_store_get_project(srv->store, project, &proj_verify) == CBM_STORE_OK) {
            /* Register only usable roots: #557 showed that malformed root metadata
             * must not discard an otherwise valid graph or create a bogus watch. */
            if (srv->watcher && root_path_looks_usable(proj_verify.root_path)) {
                cbm_watcher_watch(srv->watcher, project, proj_verify.root_path);
            }
            sync_session_from_open_project(srv, srv->store, db_project, &proj_verify);
            cbm_project_free_fields(&proj_verify);
            srv->owns_store = true;
            free(srv->current_project);
            srv->current_project = heap_strdup(db_project);
            return srv->store; /* fast path: filename == internal name */
        }
        cbm_project_free_fields(&proj_verify);
        /* #704: <project>.db exists but its INTERNAL project name differs from
         * the passed name (a copied/renamed db, or a legacy '.'-vs-'-' username
         * twin). Close it and fall through to the cache-dir scan below. */
        cbm_store_close(srv->store);
        srv->store = NULL;
    }

    /* #704 fallback: either <project>.db is absent or its internal name drifted
     * from its filename. Node rows are keyed on the INTERNAL name (== the passed
     * name, since list_projects now advertises internal names), so scan the
     * cache dir for the db whose sole internal project name equals `project` and
     * adopt it. Runs ONLY on the fallback — the common fast path is unchanged.
     * No match → NULL (a genuine typo stays not-found). */
    cbm_store_t *scanned = resolve_store_fallback_scan(project);
    if (scanned) {
        srv->store = scanned;
        cbm_project_t scanned_project = {0};
        if (cbm_store_get_project(scanned, project, &scanned_project) == CBM_STORE_OK) {
            if (srv->watcher && root_path_looks_usable(scanned_project.root_path)) {
                cbm_watcher_watch(srv->watcher, project, scanned_project.root_path);
            }
            sync_session_from_open_project(srv, scanned, db_project, &scanned_project);
            cbm_project_free_fields(&scanned_project);
        }
        srv->owns_store = true;
        free(srv->current_project);
        srv->current_project = heap_strdup(db_project);
    }

    return srv->store;
}

/* Forward decl — definition lives below alongside list_projects. */
static bool is_project_db_file(const char *name, size_t len);

/* Forward decl — definition lives below in trace_path helpers. */
static void free_node_contents(cbm_node_t *n);

/* Scan cache dir for .db files, writing complete quoted JSON names into out.
 * Returns the total projects found; out may list fewer when truncated is set. */
static int collect_db_project_names(const char *dir_path, char *out, size_t out_sz,
                                    int *out_listed, bool *truncated) {
    int count = 0;
    int listed = 0;
    size_t offset = 0;
    if (truncated) {
        *truncated = false;
    }
    if (out && out_sz > 0) {
        out[0] = '\0';
    }
    if (out_listed) {
        *out_listed = 0;
    }
    cbm_dir_t *d = cbm_opendir(dir_path);
    if (!d) {
        return 0;
    }
    cbm_dirent_t *entry;
    while ((entry = cbm_readdir(d)) != NULL) {
        const char *n = entry->name;
        size_t len = strlen(n);
        if (!is_project_db_file(n, len)) {
            continue;
        }
        /* #704: advertise the internal project name rather than a possibly
         * stale filename, while skipping ghost, empty, and corrupt databases. */
        char full_path[CBM_SZ_2K];
        int full_path_len = snprintf(full_path, sizeof(full_path), "%s/%s", dir_path, n);
        if (full_path_len < 0 || (size_t)full_path_len >= sizeof(full_path)) {
            if (truncated) {
                *truncated = true;
            }
            continue;
        }
        char internal_name[CBM_SZ_1K];
        if (!db_internal_project_name(full_path, internal_name, sizeof(internal_name), NULL)) {
            continue;
        }
        count++;

        char escaped[CBM_SZ_2K];
        int escaped_len = cbm_json_escape(escaped, (int)sizeof(escaped), internal_name);
        if (escaped_len <= 0 && internal_name[0] != '\0') {
            if (truncated) {
                *truncated = true;
            }
            continue;
        }

        /* #235: append only complete JSON elements; never expose a partial name. */
        size_t item_len = (size_t)escaped_len + CBM_QUOTE_PAIR;
        if (listed > 0) {
            item_len += SKIP_ONE;
        }
        if (!out || out_sz == 0 || item_len >= out_sz - offset) {
            if (truncated) {
                *truncated = true;
            }
            continue;
        }
        if (listed > 0) {
            out[offset++] = ',';
        }
        out[offset++] = '"';
        memcpy(out + offset, escaped, (size_t)escaped_len);
        offset += (size_t)escaped_len;
        out[offset++] = '"';
        out[offset] = '\0';
        listed++;
    }
    cbm_closedir(d);
    if (out_listed) {
        *out_listed = listed;
    }
    return count;
}

static void add_git_context_string(yyjson_mut_doc *doc, yyjson_mut_val *obj, const char *key,
                                   const char *value) {
    if (value) {
        yyjson_mut_obj_add_strcpy(doc, obj, key, value);
    } else {
        yyjson_mut_obj_add_null(doc, obj, key);
    }
}

static void add_git_context_json(yyjson_mut_doc *doc, yyjson_mut_val *obj, const char *root_path) {
    cbm_git_context_t ctx = {0};
    (void)cbm_git_context_resolve(root_path, &ctx);

    yyjson_mut_val *git = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_bool(doc, git, "is_git", ctx.is_git);
    yyjson_mut_obj_add_bool(doc, git, "is_worktree", ctx.is_worktree);
    yyjson_mut_obj_add_bool(doc, git, "is_detached", ctx.is_detached);
    yyjson_mut_obj_add_bool(doc, git, "root_exists", ctx.root_exists);
    add_git_context_string(doc, git, "worktree_root", ctx.worktree_root);
    add_git_context_string(doc, git, "git_dir", ctx.git_dir);
    add_git_context_string(doc, git, "git_common_dir", ctx.git_common_dir);
    add_git_context_string(doc, git, "canonical_root", ctx.canonical_root);
    add_git_context_string(doc, git, "branch", ctx.branch);
    add_git_context_string(doc, git, "branch_slug", ctx.branch_slug);
    add_git_context_string(doc, git, "head_sha", ctx.head_sha);
    add_git_context_string(doc, git, "base_sha", ctx.base_sha);
    if (ctx.is_git) {
        yyjson_mut_obj_add_str(doc, git, "head_scope", "committed_revision_only");
        cbm_git_snapshot_t snapshot = {0};
        bool snapshot_available =
            cbm_git_snapshot_read(root_path, CBM_GIT_SNAPSHOT_DIRTY, &snapshot) == 0 &&
            snapshot.is_git;
        if (snapshot_available) {
            bool worktree_dirty = snapshot.dirty_bytes > 0;
            yyjson_mut_obj_add_bool(doc, git, "worktree_dirty", worktree_dirty);
            yyjson_mut_obj_add_bool(doc, git, "head_matches_worktree", !worktree_dirty);
            yyjson_mut_obj_add_str(doc, git, "worktree_state",
                                   worktree_dirty ? "dirty" : "clean");
            if (worktree_dirty) {
                yyjson_mut_obj_add_strcpy(doc, git, "dirty_hash", snapshot.dirty_hash);
            }
        } else {
            yyjson_mut_obj_add_null(doc, git, "worktree_dirty");
            yyjson_mut_obj_add_null(doc, git, "head_matches_worktree");
            yyjson_mut_obj_add_str(doc, git, "worktree_state", "unknown");
        }
    }
    yyjson_mut_obj_add_val(doc, obj, "git", git);

    cbm_git_context_free(&ctx);
}

/* Describe the exact indexing call sequence exposed by this server. This is
 * reached only after resolve_project_store exhausted permissible automatic
 * recovery: it joins local startup work, retries a failed thread launch with a
 * synchronous index, and attempts configured first-use indexing. Explicit
 * auto_index=false and the resource-protection limit must not be overridden.
 * The MCP profile and reveal state are the authorities; never tell a caller to
 * invoke a tool that its current tools/list hides. */
static void mcp_index_recovery_action(cbm_mcp_server_t *srv, char *out, size_t out_size) {
    if (!out || out_size == 0) {
        return;
    }
    bool allowed = !srv || mcp_tool_allowed(srv->tool_profile, "index_repository");
    bool visible = allowed && (!srv || cbm_mcp_advanced_tool_visible(srv, "index_repository"));
    if (visible) {
        snprintf(out, out_size, "call index_repository with repo_path='/absolute/path/to/repo'.");
    } else if (allowed) {
        snprintf(out, out_size,
                 "call _hidden_tools, refresh tools/list, then call index_repository with "
                 "repo_path='/absolute/path/to/repo'.");
    } else {
        snprintf(out, out_size,
                 "this MCP tool profile does not expose index_repository; run "
                 "codebase-memory-mcp cli index_repository --repo-path "
                 "'/absolute/path/to/repo'.");
    }
}

/* Build a helpful error listing available projects. Caller must free() result. */
static char *build_project_list_error_srv(cbm_mcp_server_t *srv, const char *reason) {
    char dir_path[1024];
    cache_dir(dir_path, sizeof(dir_path));

    char projects[CBM_SZ_4K] = "";
    bool projects_truncated = false;
    int listed_count = 0;
    int total_count = collect_db_project_names(dir_path, projects, sizeof(projects),
                                               &listed_count, &projects_truncated);

    char index_action[CBM_SZ_512];
    mcp_index_recovery_action(srv, index_action, sizeof(index_action));
    char recovery_hint[CBM_SZ_1K];
    switch (srv ? srv->autoindex_block : MCP_AUTOINDEX_BLOCK_NONE) {
    case MCP_AUTOINDEX_BLOCK_DISABLED:
        snprintf(recovery_hint, sizeof(recovery_hint),
                 "Automatic indexing is disabled (auto_index=false). Set auto_index=true and "
                 "retry, or %s",
                 index_action);
        break;
    case MCP_AUTOINDEX_BLOCK_FILE_COUNT:
        snprintf(recovery_hint, sizeof(recovery_hint),
                 "Automatic indexing could not count project files safely. Check project read "
                 "permissions, then retry; %s",
                 index_action);
        break;
    case MCP_AUTOINDEX_BLOCK_FILE_LIMIT:
        snprintf(recovery_hint, sizeof(recovery_hint),
                 "Automatic indexing stopped after more than %d files exceeded "
                 "auto_index_limit=%d. Check available memory before raising the limit and "
                 "retrying; if the larger run is intentional, %s",
                 srv->autoindex_observed_files, srv->autoindex_file_limit, index_action);
        break;
    case MCP_AUTOINDEX_BLOCK_NONE:
    default:
        snprintf(recovery_hint, sizeof(recovery_hint),
                 "No published index is readable. Pass the repository path as project to use "
                 "configured automatic indexing, or %s",
                 index_action);
        break;
    }

    /* Optional: session_project and _context fields for richer error context */
    char session_frag[256] = "";
    char context_frag[CBM_SZ_2K] = "";
    const char *context_hint =
        total_count == 0
            ? recovery_hint
            : "The requested project has no readable published index. Use list_projects and "
              "pass the intended project explicitly.";
    if (srv && srv->session_project[0]) {
        snprintf(session_frag, sizeof(session_frag),
                 ",\"session_project\":\"%s\"", srv->session_project);
        /* Include a minimal _context so clients can identify session state */
        bool ctx_enabled =
            cbm_config_get_bool(srv->config, CBM_CONFIG_CONTEXT_INJECTION, true);
        if (ctx_enabled && !srv->context_injected) {
            snprintf(context_frag, sizeof(context_frag),
                     ",\"_context\":{\"status\":\"not_indexed\","
                     "\"hint\":\"%s\"}",
                     context_hint);
            srv->context_injected = true;  /* one-shot: suppress from future successful responses */
        }
    }

    enum { ERR_BUF_SZ = 6144 };
    char buf[ERR_BUF_SZ];
    if (total_count > 0) {
        snprintf(buf, sizeof(buf),
                 "{\"error\":\"%s\",\"hint\":\"Use list_projects to see all indexed projects, "
                 "then pass one as the \\\"project\\\" argument.\","
                 "\"available_projects\":[%s],\"count\":%d%s%s%s}",
                 reason, projects, listed_count,
                 projects_truncated ? ",\"available_projects_truncated\":true" : "",
                 session_frag, context_frag);
        if (projects_truncated) {
            size_t len = strlen(buf);
            if (len > 0 && buf[len - 1] == '}') {
                snprintf(buf + len - 1, sizeof(buf) - len + 1, ",\"total_count\":%d}",
                         total_count);
            }
        }
    } else {
        snprintf(buf, sizeof(buf), "{\"error\":\"%s\",\"hint\":\"%s\"%s%s}", reason, recovery_hint,
                 session_frag, context_frag);
    }
    return heap_strdup(buf);
}

static char *build_project_list_error(const char *reason) {
    return build_project_list_error_srv(NULL, reason);
}

/* REQUIRE_STORE_EX: like REQUIRE_STORE but runs _pre_free_cleanup before freeing
 * project and returning.  resolve_project_store owns first-use auto-indexing;
 * this macro only joins an in-flight startup index and reports missing stores.
 * Use this in handlers that allocate extra heap locals (e.g. qn, snippet_mode)
 * that must also be freed on the early-return paths. */
#define REQUIRE_STORE_EX(store, project, _pre_free_cleanup)                                       \
    do {                                                                                          \
        if (!(store) && srv->session_root[0] && cbm_is_dir(srv->session_root)) {                     \
            if (srv->autoindex_active) {                                                          \
                /* Background thread running — wait for it to complete */                         \
                cbm_thread_join(&srv->autoindex_tid);                                              \
                srv->autoindex_active = false;                                                    \
                /* Re-resolve store after background index finished */                            \
                store = resolve_store(srv, project);                                              \
            }                                                                                     \
        }                                                                                         \
        if (!(store)) {                                                                           \
            _pre_free_cleanup;                                                                    \
            if (!(project)) {                                                                      \
                char *_err = build_missing_project_error();                                        \
                char *_res = cbm_mcp_text_result(_err, true);                                      \
                free(_err);                                                                        \
                return _res;                                                                       \
            }                                                                                     \
            if (srv->autoindex_failed) {                                                          \
                free(project);                                                                    \
                return cbm_mcp_text_result(                                                       \
                    "{\"error\":\"auto-indexing failed for this project\","                        \
                    "\"detail\":\"The pipeline failed. Check file permissions and project size.\"," \
                    "\"fix\":\"Enable classic tools: set env CBM_TOOL_MODE=classic then call index_repository. " \
                    "Or retry by passing project=\\\"/path/to/repo\\\" or project=\\\"~/path\\\" explicitly.\"}", \
                    true);                                                                        \
            }                                                                                     \
            free(project);                                                                        \
            {                                                                                     \
                char *_err = build_project_list_error_srv(srv, "project not found or not indexed");              \
                char *_res = cbm_mcp_text_result(_err, true);                                     \
                free(_err);                                                                       \
                return _res;                                                                      \
            }                                                                                     \
        }                                                                                         \
    } while (0)

/* Convenience alias for handlers with no extra locals to free. */
#define REQUIRE_STORE(store, project) REQUIRE_STORE_EX(store, project, (void)0)

/* ── Auto-context injection (Phase 9) ─────────────────────────── */

/* Inject _context header into the FIRST tool response after session starts.
 * Contains architecture, schema, status — eliminates the need for separate
 * get_architecture / get_graph_schema / index_status / list_projects calls.
 * Subsequent responses include only session_project (lightweight).
 *
 * WHY we inject for ALL clients, including those that declare MCP resources:
 *
 * MCP resources are "application-controlled" (pull-only). When a client
 * declares capabilities.resources:{}, it means the client CAN fetch resources
 * via resources/read — it does NOT mean the client will automatically read
 * codebase://schema or codebase://architecture. The spec defines no push path:
 *   - notifications/resources/updated signals that a resource changed but
 *     sends NO content; the client must explicitly call resources/read to get it
 *   - resources/read requires an explicit model action (or user @-mention)
 *   - In Claude Code, resources are only fetched on user @-mention or explicit
 *     system-prompt instruction — never spontaneously
 * References:
 *   https://modelcontextprotocol.io/specification/2025-06-18/server/resources
 *   https://modelcontextprotocol.io/docs/concepts/resources
 *   https://workos.com/blog/mcp-features-guide  ("resources = application-controlled")
 *
 * Embedding _context in the first tool response is therefore the ONLY reliable
 * delivery channel that reaches the model without requiring explicit user action.
 * The context_injected flag already prevents duplicate injection on subsequent
 * calls, so the one-shot delivery is both sufficient and non-repetitive.
 *
 * Resources remain available for explicit access (e.g. codebase://schema via
 * @-mention) — the two mechanisms are complementary, not mutually exclusive. */
static void inject_context_once(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                cbm_mcp_server_t *srv, cbm_store_t *store,
                                const char *context_project) {
    /* Always include session_project */
    if (srv->session_project[0])
        yyjson_mut_obj_add_str(doc, root, "session_project", srv->session_project);

    if (srv->context_injected) return;

    /* Configurable via config key "context_injection" (default true) or env
     * CBM_CONTEXT_INJECTION=false.  Disable to suppress the _context header
     * when token cost matters more than automatic situational awareness:
     *   - scripted / programmatic tool use (output parsed, extra JSON is noise)
     *   - CI pipelines (token cost is metered, model doesn't need schema context)
     *   - model given explicit system-prompt codebase instructions instead
     *   - benchmarking (removes schema-query overhead from latency measurements)
     * Checked before setting context_injected so toggling mid-session works. */
    bool inject_enabled =
        cbm_config_get_bool(srv->config, CBM_CONFIG_CONTEXT_INJECTION, true);
    if (!inject_enabled) return;

    srv->context_injected = true;

    yyjson_mut_val *ctx = yyjson_mut_obj(doc);

    if (!store) {
        if (srv->session_root[0]) {
            yyjson_mut_obj_add_str(doc, ctx, "status", "auto_indexing");
            yyjson_mut_obj_add_str(doc, ctx, "hint",
                "Auto-indexing your project; retry this query in a moment. "
                "Pass project='/path/to/repo' or project='~/path/to/repo' explicitly to trigger immediately.");
        } else {
            yyjson_mut_obj_add_str(doc, ctx, "status", "not_indexed");
            yyjson_mut_obj_add_str(doc, ctx, "hint",
                "No project path detected. Pass project='/path/to/repo' or project='~/path/to/repo' to index and search.");
        }
        yyjson_mut_obj_add_val(doc, root, "_context", ctx);
        return;
    }

    yyjson_mut_obj_add_str(doc, ctx, "status", "ready");

    /* The session project identifies the server CWD, while context_project
     * identifies the graph that supplied this response.  They intentionally
     * differ when a caller searches an explicit project from another CWD. */
    const char *proj = context_project && context_project[0]
                           ? context_project
                           : (srv->session_project[0] ? srv->session_project : NULL);
    if (proj) {
        yyjson_mut_obj_add_str(doc, ctx, "project", proj);
    }

    /* Node/edge counts */
    int nodes = cbm_store_count_nodes(store, proj);
    int edges = cbm_store_count_edges(store, proj);
    yyjson_mut_obj_add_int(doc, ctx, "nodes", nodes);
    yyjson_mut_obj_add_int(doc, ctx, "edges", edges);

    /* Schema: node labels + edge types. Counts-only: this context never emits
     * property keys, and the full variant's json_each discovery is O(total
     * property rows) — the wrong cost for the first response of a session.
     * Overlay-aware via the shared selector so the first response can never
     * advertise vocabulary query_graph would then contradict. */
    cbm_schema_info_t schema = {0};
    mcp_get_current_schema(store, proj, MCP_CYPHER_MATCH_VOCABULARY, &schema, NULL, NULL,
                           NULL);
    yyjson_mut_val *label_arr = yyjson_mut_arr(doc);
    for (int i = 0; i < schema.node_label_count; i++) {
        yyjson_mut_val *lbl = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_strcpy(doc, lbl, "label", schema.node_labels[i].label);
        yyjson_mut_obj_add_int(doc, lbl, "count", schema.node_labels[i].count);
        yyjson_mut_arr_add_val(label_arr, lbl);
    }
    yyjson_mut_obj_add_val(doc, ctx, "node_labels", label_arr);

    yyjson_mut_val *type_arr = yyjson_mut_arr(doc);
    for (int i = 0; i < schema.edge_type_count; i++) {
        yyjson_mut_val *et = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_strcpy(doc, et, "type", schema.edge_types[i].type);
        yyjson_mut_obj_add_int(doc, et, "count", schema.edge_types[i].count);
        yyjson_mut_arr_add_val(type_arr, et);
    }
    yyjson_mut_obj_add_val(doc, ctx, "edge_types", type_arr);
    cbm_store_schema_free(&schema);

    /* PageRank stats */
    sqlite3 *db = cbm_store_get_db(store);
    bool pagerank_stale =
        proj && cbm_store_derived_view_is_stale(store, proj, CBM_STORE_DERIVED_VIEW_PAGERANK);
    if (db && proj && !pagerank_stale) {
        sqlite3_stmt *stmt = NULL;
        if (sqlite3_prepare_v2(db,
                "SELECT COUNT(*), MAX(computed_at) FROM pagerank WHERE project = ?1",
                -1, &stmt, NULL) == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, proj, -1, SQLITE_TRANSIENT);
            if (sqlite3_step(stmt) == SQLITE_ROW) {
                int ranked = sqlite3_column_int(stmt, 0);
                if (ranked > 0) {
                    yyjson_mut_obj_add_int(doc, ctx, "ranked_nodes", ranked);
                    const char *ts = (const char *)sqlite3_column_text(stmt, 1);
                    if (ts) yyjson_mut_obj_add_strcpy(doc, ctx, "pagerank_computed_at", ts);
                }
            }
            sqlite3_finalize(stmt);
        }
    }

    /* Key functions (top by PageRank): PUSH a bounded summary so the model
     * knows where to start tracing WITHOUT having to pull codebase://architecture
     * (an MCP resource — application-controlled/pull-only by spec; the only
     * reliable delivery channel into the model is this _context header). Honors
     * key_functions_exclude (config). Bounded by CBM_CONTEXT_KEY_FUNCTIONS_LIMIT
     * to keep the first-response token cost modest. */
    if (db && proj && !pagerank_stale) {
        const char *kf_exclude = srv->config
            ? cbm_config_get(srv->config, CBM_CONFIG_KEY_FUNCTIONS_EXCLUDE, "")
            : "";
        int kf_cfg_limit = srv->config
            ? cbm_config_get_int(srv->config, CBM_CONFIG_CONTEXT_KEY_FUNCTIONS_LIMIT,
                                 CBM_CONTEXT_KEY_FUNCTIONS_LIMIT)
            : CBM_CONTEXT_KEY_FUNCTIONS_LIMIT;
        if (kf_cfg_limit <= 0) {
            kf_cfg_limit = CBM_CONTEXT_KEY_FUNCTIONS_LIMIT;
        }
        char *kf_sql = build_key_functions_sql(kf_exclude, NULL, kf_cfg_limit, false);
        if (kf_sql) {
            sqlite3_stmt *kf_stmt = NULL;
            if (sqlite3_prepare_v2(db, kf_sql, -1, &kf_stmt, NULL) == SQLITE_OK) {
                sqlite3_bind_text(kf_stmt, 1, proj, -1, SQLITE_TRANSIENT);
                yyjson_mut_val *kf_arr = yyjson_mut_arr(doc);
                while (sqlite3_step(kf_stmt) == SQLITE_ROW) {
                    yyjson_mut_val *kf = yyjson_mut_obj(doc);
                    const char *qn = (const char *)sqlite3_column_text(kf_stmt, 1);
                    if (qn) {
                        yyjson_mut_obj_add_strcpy(doc, kf, "qualified_name", qn);
                    }
                    add_pagerank_val(doc, kf, sqlite3_column_double(kf_stmt, 4));
                    yyjson_mut_arr_add_val(kf_arr, kf);
                }
                sqlite3_finalize(kf_stmt);
                yyjson_mut_obj_add_val(doc, ctx, "key_functions", kf_arr);
            }
            free(kf_sql);
        }
    }

    /* Detected ecosystem. Resolve the queried project's registered root
     * instead of reporting the session CWD's package manager. */
    cbm_project_t context_info = {0};
    const char *context_root = NULL;
    if (proj && cbm_store_get_project(store, proj, &context_info) == CBM_STORE_OK) {
        context_root = context_info.root_path;
    } else if ((!proj || (srv->session_project[0] && strcmp(proj, srv->session_project) == 0)) &&
               srv->session_root[0]) {
        context_root = srv->session_root;
    }
    if (context_root && context_root[0]) {
        cbm_pkg_manager_t eco = cbm_detect_ecosystem(context_root);
        if (eco != CBM_PKG_COUNT) {
            yyjson_mut_obj_add_str(doc, ctx, "detected_ecosystem",
                                   cbm_pkg_manager_str(eco));
        }
    }
    cbm_project_free_fields(&context_info);

    yyjson_mut_obj_add_val(doc, root, "_context", ctx);
}

static void toon_append_mut_string(cbm_sb_t *sb, yyjson_mut_val *obj, const char *json_key,
                                   const char *toon_key) {
    yyjson_mut_val *value = yyjson_mut_obj_get(obj, json_key);
    if (value && yyjson_mut_is_str(value)) {
        cbm_toon_scalar_str(sb, toon_key, yyjson_mut_get_str(value));
    }
}

static void toon_append_mut_int(cbm_sb_t *sb, yyjson_mut_val *obj, const char *json_key,
                                const char *toon_key) {
    yyjson_mut_val *value = yyjson_mut_obj_get(obj, json_key);
    if (value && yyjson_mut_is_int(value)) {
        cbm_toon_scalar_int(sb, toon_key, yyjson_mut_get_sint(value));
    }
}

static void toon_append_context_count_table(cbm_sb_t *sb, yyjson_mut_val *ctx,
                                            const char *json_key, const char *toon_key,
                                            const char *name_key) {
    yyjson_mut_val *array = yyjson_mut_obj_get(ctx, json_key);
    if (!array || !yyjson_mut_is_arr(array)) {
        return;
    }
    const char *columns[] = {name_key, "count"};
    cbm_toon_table_header(sb, toon_key, (int)yyjson_mut_arr_size(array), columns, MCP_COL_2);
    yyjson_mut_arr_iter iter;
    yyjson_mut_arr_iter_init(array, &iter);
    yyjson_mut_val *item = NULL;
    while ((item = yyjson_mut_arr_iter_next(&iter))) {
        yyjson_mut_val *name = yyjson_mut_obj_get(item, name_key);
        yyjson_mut_val *count = yyjson_mut_obj_get(item, "count");
        cbm_toon_row_begin(sb);
        cbm_toon_cell_str(sb, name && yyjson_mut_is_str(name) ? yyjson_mut_get_str(name) : "",
                          true);
        cbm_toon_cell_int(sb, count && yyjson_mut_is_int(count) ? yyjson_mut_get_sint(count) : 0,
                          false);
        cbm_toon_row_end(sb);
    }
}

static void toon_append_context_key_functions(cbm_sb_t *sb, yyjson_mut_val *ctx) {
    yyjson_mut_val *array = yyjson_mut_obj_get(ctx, "key_functions");
    if (!array || !yyjson_mut_is_arr(array)) {
        return;
    }
    const char *columns[] = {"qualified_name", "pagerank"};
    cbm_toon_table_header(sb, "_context_key_functions", (int)yyjson_mut_arr_size(array),
                          columns, MCP_COL_2);
    yyjson_mut_arr_iter iter;
    yyjson_mut_arr_iter_init(array, &iter);
    yyjson_mut_val *item = NULL;
    while ((item = yyjson_mut_arr_iter_next(&iter))) {
        yyjson_mut_val *qualified_name = yyjson_mut_obj_get(item, "qualified_name");
        yyjson_mut_val *pagerank = yyjson_mut_obj_get(item, "pagerank");
        double rank = 0.0;
        if (pagerank && yyjson_mut_is_num(pagerank)) {
            rank = yyjson_mut_get_num(pagerank);
        } else if (pagerank && yyjson_mut_is_raw(pagerank)) {
            rank = strtod(yyjson_mut_get_raw(pagerank), NULL);
        }
        cbm_toon_row_begin(sb);
        cbm_toon_cell_str(
            sb, qualified_name && yyjson_mut_is_str(qualified_name)
                    ? yyjson_mut_get_str(qualified_name)
                    : "",
            true);
        cbm_toon_cell_real(sb, rank, false);
        cbm_toon_row_end(sb);
    }
}

/* Serialize the format-neutral JSON context model as native TOON. Keeping
 * construction in inject_context_once preserves one fact authority; this
 * function owns only the TOON field mapping. */
static void toon_append_context_model(cbm_sb_t *sb, yyjson_mut_val *root) {
    toon_append_mut_string(sb, root, "session_project", "session_project");
    yyjson_mut_val *ctx = yyjson_mut_obj_get(root, "_context");
    if (!ctx || !yyjson_mut_is_obj(ctx)) {
        return;
    }
    toon_append_mut_string(sb, ctx, "status", "_context_status");
    toon_append_mut_string(sb, ctx, "hint", "_context_hint");
    toon_append_mut_string(sb, ctx, "project", "_context_project");
    toon_append_mut_int(sb, ctx, "nodes", "_context_nodes");
    toon_append_mut_int(sb, ctx, "edges", "_context_edges");
    toon_append_mut_int(sb, ctx, "ranked_nodes", "_context_ranked_nodes");
    toon_append_mut_string(sb, ctx, "pagerank_computed_at",
                           "_context_pagerank_computed_at");
    toon_append_mut_string(sb, ctx, "detected_ecosystem", "_context_detected_ecosystem");
    toon_append_context_count_table(sb, ctx, "node_labels", "_context_node_labels", "label");
    toon_append_context_count_table(sb, ctx, "edge_types", "_context_edge_types", "type");
    toon_append_context_key_functions(sb, ctx);
}

/* TOON-path context delivery: the TOON early-returns in handle_search_graph
 * bypass the yyjson response doc, which silently dropped the one-shot
 * `_context` header and `session_project` — the only reliable push channel
 * into the model (see the delivery-channel note above inject_context_once).
 * Build the facts once with inject_context_once, then serialize the mutable
 * model as native TOON; never append the scratch JSON document verbatim. */
static void toon_append_context_once(cbm_sb_t *sb, cbm_mcp_server_t *srv, cbm_store_t *store,
                                     const char *context_project) {
    if (!sb || !srv) {
        return;
    }
    yyjson_mut_doc *cdoc = yyjson_mut_doc_new(NULL);
    if (!cdoc) {
        return;
    }
    yyjson_mut_val *croot = yyjson_mut_obj(cdoc);
    yyjson_mut_doc_set_root(cdoc, croot);
    inject_context_once(cdoc, croot, srv, store, context_project);
    if (yyjson_mut_obj_size(croot) > 0) {
        toon_append_context_model(sb, croot);
    }
    yyjson_mut_doc_free(cdoc);
}

/* Same delivery for TOON payloads built as plain heap strings (the BM25 path
 * builds its table inside bm25_search and returns a finished string). Returns
 * a new heap string with the context line appended, or NULL when nothing needs
 * appending (caller keeps using the original). */
static char *toon_payload_with_context_once(const char *payload, cbm_mcp_server_t *srv,
                                            cbm_store_t *store, const char *context_project) {
    if (!payload) {
        return NULL;
    }
    cbm_sb_t sb;
    cbm_sb_init(&sb);
    toon_append_context_once(&sb, srv, store, context_project);
    if (sb.len == 0) {
        cbm_sb_free(&sb);
        return NULL;
    }
    cbm_sb_t out;
    cbm_sb_init(&out);
    cbm_sb_append(&out, payload);
    char *ctx_line = cbm_sb_finish(&sb);
    if (ctx_line) {
        cbm_sb_append(&out, ctx_line);
        free(ctx_line);
    }
    return cbm_sb_finish(&out);
}

/* BM25 builds JSON as a completed heap string and returns before the regular
 * search_graph yyjson builder. Parse that bounded response once so JSON and
 * TOON both deliver session_project and the one-shot queried-project context. */
static char *json_payload_with_context_once(const char *payload, cbm_mcp_server_t *srv,
                                            cbm_store_t *store, const char *context_project) {
    if (!payload || !srv) {
        return NULL;
    }
    yyjson_doc *source = yyjson_read(payload, strlen(payload), 0);
    if (!source) {
        return NULL;
    }
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    if (!doc) {
        yyjson_doc_free(source);
        return NULL;
    }
    yyjson_mut_val *root = yyjson_val_mut_copy(doc, yyjson_doc_get_root(source));
    yyjson_doc_free(source);
    if (!root || !yyjson_mut_is_obj(root)) {
        yyjson_mut_doc_free(doc);
        return NULL;
    }
    yyjson_mut_doc_set_root(doc, root);
    inject_context_once(doc, root, srv, store, context_project);
    char *out = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return out;
}

/* ── Smart project param expansion ─────────────────────────────── */

typedef enum { MATCH_NONE, MATCH_EXACT, MATCH_PREFIX, MATCH_GLOB } match_mode_t;

typedef struct {
    char *value;       /* expanded project string (heap) or NULL. Caller must free. */
    match_mode_t mode; /* how to match in SQL */
} project_expand_t;

/* Forward declaration — defined below, needed by handle_get_graph_schema */
static cbm_store_t *resolve_project_store(cbm_mcp_server_t *srv,
                                           char *raw_project,
                                           project_expand_t *out_pe);

/* Expand project param shorthands (self/dep/glob/prefix).
 * Takes ownership of raw — caller must NOT free raw after this call.
 * Returns expanded result. Caller must free(result.value).
 * Runtime: O(1) — fixed number of string comparisons + one snprintf + strdup.
 * Memory: one heap allocation for result.value. */
/* ── Project path helpers ─────────────────────────────────────────
 * Shared by expand_project_param (Rule 0) and get_project_root so the
 * detection condition and slug computation are defined exactly once. */

/* Returns true if s looks like a filesystem path rather than a project slug.
 * Project slugs are dot-separated identifiers (e.g. "Users-foo-bar"); they
 * never contain / (except globs starting with *) and don't start with / ~ . */
static bool project_is_path(const char *s) {
    if (!s || !s[0]) return false;
    if (s[0] == '.') {
        return s[1] == '\0' || s[1] == '/'; /* "." or "./" — relative paths */
    }
    return s[0] == '/' || s[0] == '~' ||
           (strchr(s, '/') != NULL && s[0] != '*');
}

/* Expand a leading ~ to $HOME (~/... or ~ alone).
 * ~user/... is left unexpanded (requires getpwnam — not worth the complexity).
 * Returns a heap-allocated expanded string, or NULL when no expansion is needed
 * or $HOME is unset. Caller must free the result. */
static char *expand_tilde(const char *s) {
    if (s[0] != '~') return NULL;
    if (s[1] != '\0' && s[1] != '/') return NULL; /* "~user/..." — leave as-is */
    char home_buf[CBM_SZ_1K];
    const char *home = cbm_safe_getenv("HOME", home_buf, sizeof(home_buf), NULL);
    if (!home || !home[0]) return NULL;
    /* Build: home + rest  ("~" → home, "~/rest" → home + "/rest") */
    size_t hlen = strlen(home);
    const char *rest = s + 1; /* "" or "/rest" */
    size_t rest_len = strlen(rest);
    char *result = malloc(hlen + rest_len + 1);
    if (!result) return NULL;
    memcpy(result, home, hlen);
    memcpy(result + hlen, rest, rest_len + 1);
    return result;
}

/* Return a heap-owned canonical path for an existing filesystem entry. */
static char *mcp_resolve_existing_path(const char *path) {
    if (!path || !path[0]) return NULL;
#ifdef _WIN32
    char resolved[CBM_SZ_4K];
    if (!_fullpath(resolved, path, sizeof(resolved))) return NULL;
    return heap_strdup(resolved);
#else
    return realpath(path, NULL);
#endif
}

/* Return the canonical path string used for path-derived project slugs.
 * Existing paths use the platform canonicalizer; otherwise the tilde-expanded
 * path, or the original path, is copied so missing paths still produce stable
 * error slugs. out_realpath_ok tells callers whether the returned string is a
 * canonical existing-path result for cases where only existing paths should
 * update session_root. */
static char *project_canonical_path(const char *s, bool *out_realpath_ok) {
    if (out_realpath_ok) {
        *out_realpath_ok = false;
    }
    if (!project_is_path(s)) return NULL;
    char *expanded = expand_tilde(s); /* non-NULL only for ~/ paths */
    const char *to_resolve = expanded ? expanded : s;
    char *resolved = mcp_resolve_existing_path(to_resolve);
    if (resolved) {
        free(expanded);
        if (out_realpath_ok) {
            *out_realpath_ok = true;
        }
        return resolved;
    }
    char *fallback = heap_strdup(to_resolve);
    free(expanded);
    return fallback;
}

/* Convert a filesystem path to a heap-allocated project slug.
 * Handles ~/ tilde expansion, resolves symlinks and relative components
 * when the path exists, then derives the slug from the canonical path.
 * Returns NULL if s is not a path. Caller must free the result. */
static char *project_slug_from_path(const char *s) {
    char *canonical = project_canonical_path(s, NULL);
    if (!canonical) return NULL;
    char *slug = cbm_project_name_from_path(canonical);
    free(canonical);
    return slug;
}

static project_expand_t expand_project_param(cbm_mcp_server_t *srv, char *raw) {
    project_expand_t r = {.value = NULL, .mode = MATCH_NONE};
    if (!raw) return r;

    /* Rule 0: Path detection — convert paths to project names.
     * Enables: search_graph(project="/path/to/repo") */
    if (project_is_path(raw)) {
        bool realpath_ok = false;
        char *canonical = project_canonical_path(raw, &realpath_ok);
        char *name = canonical ? cbm_project_name_from_path(canonical) : NULL;
        if (realpath_ok && name && srv->session_root[0] == '\0') {
            snprintf(srv->session_root, sizeof(srv->session_root), "%s", canonical);
            snprintf(srv->session_project, sizeof(srv->session_project), "%s", name);
        }
        free(raw);
        free(canonical);
        r.value = name;
        r.mode = MATCH_PREFIX;
        return r;
    }

    /* Guard: if session_project is empty, skip all expansion rules */
    if (!srv->session_project[0]) {
        r.value = raw;
        r.mode = strchr(raw, '*') ? MATCH_GLOB : MATCH_PREFIX;
        return r;
    }

    size_t sp_len = strlen(srv->session_project);
    char buf[4096];

    /* Rule 1: "self" prefix → replace with session project name */
    if (strncmp(raw, "self", 4) == 0 && (raw[4] == '\0' || raw[4] == '.')) {
        bool is_self_only = (raw[4] == '\0');
        snprintf(buf, sizeof(buf), "%s%s", srv->session_project, raw + 4);
        free(raw);
        r.value = heap_strdup(buf);
        r.mode = is_self_only ? MATCH_EXACT : MATCH_PREFIX;
        if (r.mode == MATCH_PREFIX && strchr(r.value, '*')) r.mode = MATCH_GLOB;
        return r;
    }

    /* Rule 2: "dep" / "deps" exactly → "{session}.dep" */
    if (strcmp(raw, "dep") == 0 || strcmp(raw, "deps") == 0) {
        snprintf(buf, sizeof(buf), "%s.dep", srv->session_project);
        free(raw);
        r.value = heap_strdup(buf);
        r.mode = MATCH_PREFIX;
        return r;
    }

    /* Rule 3: starts with "dep." → prepend session */
    if (strncmp(raw, "dep.", 4) == 0) {
        snprintf(buf, sizeof(buf), "%s.%s", srv->session_project, raw);
        free(raw);
        r.value = heap_strdup(buf);
        r.mode = strchr(r.value, '*') ? MATCH_GLOB : MATCH_PREFIX;
        return r;
    }

    /* Rule 4: starts with "{session}." but next segment isn't "dep" → insert .dep. */
    if (strncmp(raw, srv->session_project, sp_len) == 0 && raw[sp_len] == '.' &&
        !(strncmp(raw + sp_len + 1, "dep", 3) == 0 &&
          (raw[sp_len + 4] == '.' || raw[sp_len + 4] == '\0'))) {
        snprintf(buf, sizeof(buf), "%s.dep.%s", srv->session_project, raw + sp_len + 1);
        free(raw);
        r.value = heap_strdup(buf);
        r.mode = strchr(r.value, '*') ? MATCH_GLOB : MATCH_PREFIX;
        return r;
    }

    /* Rule 5: everything else — as-is (bare words are project names) */
    r.value = raw;
    r.mode = strchr(raw, '*') ? MATCH_GLOB : MATCH_PREFIX;
    return r;
}

/* Fill cbm_search_params_t project fields from an expand result.
 * Also translates * → % for SQL LIKE in glob mode. */
static void fill_project_params(const project_expand_t *pe, cbm_search_params_t *params) {
    switch (pe->mode) {
    case MATCH_GLOB:
        params->project_pattern = pe->value;
        break;
    case MATCH_EXACT:
        params->project = pe->value;
        params->project_exact = true;
        break;
    case MATCH_PREFIX:
        params->project = pe->value;
        break;
    case MATCH_NONE:
        break;
    }
}

/* Distinguish an omitted project argument from an unknown indexed name (#640). */
static char *build_missing_project_error(void) {
    return heap_strdup("{\"error\":\"missing required argument: project\","
                       "\"hint\":\"Pass the project as the \\\"project\\\" argument, e.g. "
                       "{\\\"project\\\":\\\"<name from list_projects>\\\"}. Run "
                       "list_projects to see indexed projects.\"}");
}

static bool store_has_adr(cbm_store_t *store, const char *project);

static bool project_has_adr(cbm_store_t *store, const char *project, const char *root_path) {
    if (store_has_adr(store, project)) {
        return true;
    }
    if (!root_path) {
        return false;
    }
    char adr_path[CBM_SZ_4K];
    int path_len = snprintf(adr_path, sizeof(adr_path),
                            "%s/.codebase-memory/adr.md", root_path);
    if (path_len < 0 || (size_t)path_len >= sizeof(adr_path)) {
        return false;
    }
    struct stat adr_st;
    return stat(adr_path, &adr_st) == 0;
}
/* ── Tool handler implementations ─────────────────────────────── */

/* Validate that a file is a codebase-memory-mcp SQLite database.
 * Returns true if file has SQLite magic bytes AND contains the expected
 * 'nodes' table (core schema indicator).
 * On ANY error: returns false, logs actionable warning to stderr,
 * does NOT crash, does NOT hang, does NOT modify the file.
 * Opens read-only with busy_timeout to avoid hanging on locked files. */
static bool validate_cbm_db_with_timeout(const char *path, int busy_timeout_ms) {
    if (!path) return false;

    int64_t file_size = cbm_file_size(path);
    if (file_size < 0) return false;
    if (file_size == 0) {
        cbm_log_warn("db.skip", "path", path, "reason", "empty_file");
        return false;
    }

    /* Check SQLite magic bytes (first 16 bytes = "SQLite format 3\0") */
    FILE *f = fopen(path, "rb");
    if (!f) {
        cbm_log_warn("db.skip", "path", path, "reason", "cannot_open");
        return false;
    }
    char magic[16];
    size_t n = fread(magic, 1, 16, f);
    fclose(f);
    if (n < 16 || memcmp(magic, "SQLite format 3", 15) != 0) {
        const char *base = strrchr(path, '/');
        base = base ? base + 1 : path;
        cbm_log_warn("db.skip", "file", base, "reason", "not_sqlite");
        return false;
    }

    /* Reuse the canonical query-only open so validation gets the same WAL and
     * immutable read-only-filesystem fallback as the subsequent query. */
    cbm_store_t *store = cbm_store_open_path_query(path);
    if (!store) {
        const char *base = strrchr(path, '/');
        base = base ? base + 1 : path;
        cbm_log_warn("db.skip", "file", base, "reason", "sqlite_open_failed");
        return false;
    }
    sqlite3 *db = cbm_store_get_db(store);
    sqlite3_busy_timeout(db, busy_timeout_ms);

    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(db,
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='nodes' LIMIT 1;",
        -1, &stmt, NULL);
    bool valid = false;
    if (rc == SQLITE_OK && sqlite3_step(stmt) == SQLITE_ROW) {
        valid = true;
    } else {
        const char *base = strrchr(path, '/');
        base = base ? base + 1 : path;
        cbm_log_warn("db.skip", "file", base,
                     "reason", "not_cbm_database",
                     "hint", "File in cache dir lacks codebase-memory-mcp schema. "
                             "It was not opened as a project and was not modified.");
    }
    if (stmt) sqlite3_finalize(stmt);
    cbm_store_close(store);
    return valid;
}

/* Return true if filename is a valid project .db file (not temp/internal).
 *
 * Project names derived from /tmp/... source roots legitimately begin with
 * "tmp-" (cbm_project_name_from_path: "/tmp/bench/..." → "tmp-bench-...";
 * see tests/test_pipeline.c fixtures), so the prefix must NOT be excluded.
 * The "_" prefix is reserved for internal/hidden DBs, and ":memory:" is the
 * SQLite in-memory marker (defensive — never appears as a real file). */
static bool is_project_db_file(const char *name, size_t len) {
    if (len < MCP_MIN_DB_NAME || strcmp(name + len - MCP_DB_EXT, ".db") != 0) {
        return false;
    }
    if (strncmp(name, "_", SLEN("_")) == 0 || strncmp(name, ":memory:", SLEN(":memory:")) == 0) {
        return false;
    }
    return true;
}

/* db_internal_project_name — see forward declaration above resolve_store. */
static bool db_internal_project_name(const char *full_path, char *name_out, size_t name_sz,
                                     cbm_store_t **out_store) {
    if (out_store) {
        *out_store = NULL;
    }
    cbm_store_t *st = cbm_store_open_path_query(full_path);
    if (!st) {
        return false; /* nonexistent / unreadable */
    }
    cbm_project_t *projs = NULL;
    int n = 0;
    bool ok = false;
    int primary_count = 0;
    if (cbm_store_list_projects(st, &projs, &n) == CBM_STORE_OK) {
        for (int i = 0; i < n; i++) {
            const char *candidate = projs[i].name;
            if (!candidate || !candidate[0]) {
                continue;
            }
            size_t candidate_len = strlen(candidate);
            const char *missed_suffix = "::missed";
            size_t missed_len = strlen(missed_suffix);
            if (candidate_len >= missed_len &&
                strcmp(candidate + candidate_len - missed_len, missed_suffix) == 0) {
                continue;
            }
            char parent[CBM_SZ_1K];
            if (strcmp(parent_project_for_db(candidate, parent, sizeof(parent)), candidate) != 0) {
                continue;
            }
            primary_count++;
            if (primary_count == 1) {
                snprintf(name_out, name_sz, "%s", candidate);
            }
        }
        ok = primary_count == 1;
    }
    cbm_store_free_projects(projs, n);
    if (ok && out_store) {
        *out_store = st; /* transfer ownership to caller */
    } else {
        cbm_store_close(st);
    }
    return ok;
}

/* resolve_store_fallback_scan — see forward declaration above resolve_store. */
static cbm_store_t *resolve_store_fallback_scan(const char *project) {
    char dir_path[CBM_SZ_1K];
    cache_dir(dir_path, sizeof(dir_path));
    cbm_dir_t *d = cbm_opendir(dir_path);
    if (!d) {
        return NULL;
    }
    cbm_store_t *found = NULL;
    cbm_dirent_t *entry;
    while ((entry = cbm_readdir(d)) != NULL) {
        const char *n = entry->name;
        size_t len = strlen(n);
        if (!is_project_db_file(n, len)) {
            continue;
        }
        char full_path[CBM_SZ_2K];
        snprintf(full_path, sizeof(full_path), "%s/%s", dir_path, n);
        char iname[CBM_SZ_1K];
        cbm_store_t *st = NULL;
        if (db_internal_project_name(full_path, iname, sizeof(iname), &st)) {
            if (strcmp(iname, project) == 0) {
                found = st; /* adopt — caller takes ownership */
                break;
            }
            cbm_store_close(st);
        }
    }
    cbm_closedir(d);
    return found;
}

/* Open a .db file briefly and return whether it has one resolvable internal
 * project. When emit is true, append its bounded summary to arr. */
static bool build_project_json_entry(yyjson_mut_doc *doc, yyjson_mut_val *arr, const char *dir_path,
                                     const char *name, size_t name_len, int64_t size_bytes,
                                     bool emit) {
    (void)name_len;

    char full_path[CBM_SZ_2K];
    int full_path_len = snprintf(full_path, sizeof(full_path), "%s/%s", dir_path, name);
    if (full_path_len <= 0 || (size_t)full_path_len >= sizeof(full_path)) {
        return false;
    }

    /* #704: key on the db's INTERNAL project name, not its filename. Node/edge
     * rows are tagged with the internal name, so a drifted filename (copied or
     * renamed db, legacy '.'-vs-'-' username twin) would otherwise report 0
     * nodes/edges and be unresolvable. Skip ghost/empty/corrupt dbs entirely so
     * they don't appear as resolvable projects. */
    char project_name[CBM_SZ_1K];
    cbm_store_t *pstore = NULL;
    if (!db_internal_project_name(full_path, project_name, sizeof(project_name), &pstore)) {
        return false; /* ghost / unreadable — not a resolvable project */
    }
    if (!emit) {
        cbm_store_close(pstore);
        return true;
    }

    int nodes = cbm_store_count_nodes(pstore, project_name);
    int edges = cbm_store_count_edges(pstore, project_name);
    char root_path_buf[CBM_SZ_1K] = "";
    cbm_project_t proj = {0};
    if (cbm_store_get_project(pstore, project_name, &proj) == CBM_STORE_OK) {
        if (proj.root_path) {
            snprintf(root_path_buf, sizeof(root_path_buf), "%s", proj.root_path);
        }
        cbm_project_free_fields(&proj);
    }
    cbm_store_close(pstore);

    yyjson_mut_val *p = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_strcpy(doc, p, "name", project_name);
    yyjson_mut_obj_add_strcpy(doc, p, "root_path", root_path_buf);
    /* Listing stays lean: only the branch (the one git fact that
     * disambiguates same-repo projects). The 12-field git block — mostly
     * null for non-git roots — cost ~10KB across a full cache and is one
     * index_status call away for the project you actually care about. */
    if (root_path_buf[0]) {
        char *branch = NULL;
        if (cbm_git_current_branch(root_path_buf, &branch) == 0) {
            yyjson_mut_obj_add_strcpy(doc, p, "branch", branch);
        }
        free(branch);
    }
    yyjson_mut_obj_add_int(doc, p, "nodes", nodes);
    yyjson_mut_obj_add_int(doc, p, "edges", edges);
    yyjson_mut_obj_add_int(doc, p, "size_bytes", size_bytes);
    yyjson_mut_arr_add_val(arr, p);
    return true;
}

static int compare_project_db_names(const void *left, const void *right) {
    const char *const *a = left;
    const char *const *b = right;
    return strcmp(*a, *b);
}

/* Collect only syntactically eligible filenames, then sort them so offsets are
 * stable for an unchanged cache. Database validation remains lazy in the page
 * loop: first-page work opens at most limit+1 valid databases. */
static bool collect_project_db_names(const char *dir_path, char ***out_names, int *out_count) {
    *out_names = NULL;
    *out_count = 0;
    cbm_dir_t *dir = cbm_opendir(dir_path);
    if (!dir) {
        return true;
    }

    char **names = NULL;
    int count = 0;
    int capacity = 0;
    bool ok = true;
    cbm_dirent_t *entry;
    while ((entry = cbm_readdir(dir)) != NULL) {
        size_t len = strlen(entry->name);
        if (!is_project_db_file(entry->name, len)) {
            continue;
        }
        if (count == capacity) {
            int next_capacity = capacity ? capacity * 2 : CBM_SZ_16;
            char **grown = realloc(names, (size_t)next_capacity * sizeof(*grown));
            if (!grown) {
                ok = false;
                break;
            }
            names = grown;
            capacity = next_capacity;
        }
        names[count] = heap_strdup(entry->name);
        if (!names[count]) {
            ok = false;
            break;
        }
        count++;
    }
    cbm_closedir(dir);
    if (!ok) {
        free_counted_string_array(names, count);
        return false;
    }
    qsort(names, (size_t)count, sizeof(*names), compare_project_db_names);
    *out_names = names;
    *out_count = count;
    return true;
}

/* list_projects: scan cache directory for .db files in a stable, bounded page.
 * Each project is a single .db file — no central registry needed. */
static char *handle_list_projects(cbm_mcp_server_t *srv, const char *args) {
    char dir_path[CBM_SZ_1K];
    cache_dir(dir_path, sizeof(dir_path));
    int validate_busy_timeout_ms = cbm_mcp_db_validate_busy_timeout_ms(srv);
    bool all = cbm_mcp_get_bool_arg(args, "all");
    int offset = all ? 0 : cbm_mcp_get_int_arg(args, "offset", 0);
    if (offset < 0) {
        offset = 0;
    }
    int limit =
        cbm_mcp_get_positive_int_arg(args, "limit", MCP_PROJECTS_PAGE_SIZE, MCP_PROJECTS_PAGE_SIZE);
    if (limit > MCP_PROJECTS_PAGE_MAX) {
        limit = MCP_PROJECTS_PAGE_MAX;
    }

    char **db_names = NULL;
    int db_name_count = 0;
    if (!collect_project_db_names(dir_path, &db_names, &db_name_count)) {
        return cbm_mcp_text_result("{\"error\":\"out of memory listing projects\","
                                   "\"hint\":\"Retry with a smaller cache inventory.\"}",
                                   true);
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_val *arr = yyjson_mut_arr(doc);

    int valid_seen = 0;
    int emitted = 0;
    bool has_more = false;
    for (int i = 0; i < db_name_count; i++) {
        const char *name = db_names[i];
        size_t len = strlen(name);

        /* Get file metadata */
        char full_path[CBM_SZ_2K];
        int full_path_len = snprintf(full_path, sizeof(full_path), "%s/%s", dir_path, name);
        if (full_path_len <= 0 || (size_t)full_path_len >= sizeof(full_path)) {
            continue;
        }
        struct stat st;
        if (stat(full_path, &st) != 0) {
            continue;
        }

        /* Validate db structure before opening — skip corrupt/non-cbm files */
        if (!validate_cbm_db_with_timeout(full_path, validate_busy_timeout_ms)) {
            continue;
        }

        bool should_emit = all || (valid_seen >= offset && emitted < limit);
        bool valid = build_project_json_entry(doc, arr, dir_path, name, len, (int64_t)st.st_size,
                                              should_emit);
        if (!valid) {
            continue;
        }
        if (all) {
            emitted++;
        } else if (valid_seen >= offset) {
            if (emitted >= limit) {
                has_more = true;
                break;
            }
            emitted++;
        }
        valid_seen++;
    }
    free_counted_string_array(db_names, db_name_count);

    yyjson_mut_obj_add_val(doc, root, "projects", arr);
    yyjson_mut_obj_add_int(doc, root, "offset", offset);
    yyjson_mut_obj_add_int(doc, root, "returned_count", emitted);
    yyjson_mut_obj_add_bool(doc, root, "has_more", has_more);
    if (!all) {
        yyjson_mut_obj_add_int(doc, root, "limit", limit);
    }
    if (has_more) {
        yyjson_mut_obj_add_int(doc, root, "next_offset", offset + emitted);
        yyjson_mut_obj_add_str(doc, root, "pagination_hint",
                               "Call list_projects with offset=next_offset; use all=true only for "
                               "an explicit full inventory.");
    }

    /* Distinguish an empty cache from an exhausted page. */
    if (yyjson_mut_arr_size(arr) == 0) {
        yyjson_mut_obj_add_str(
            doc, root, "hint",
            valid_seen == 0 && offset == 0
                ? "No projects indexed. Call index_repository(repo_path=...) first."
                : "No projects at this offset. Restart pagination with offset=0.");
    }

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);

    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

/* verify_project_indexed — returns a heap-allocated error JSON string when the
 * named project has not been indexed yet, or NULL when the project exists.
 * resolve_store uses cbm_store_open_path_query (no SQLITE_OPEN_CREATE), so
 * store is NULL for missing .db files (REQUIRE_STORE fires first). This
 * function catches the remaining case: a .db file exists but has no indexed
 * nodes (e.g., an empty or half-initialised project).
 * Callers that receive a non-NULL return value must free(project) themselves
 * before returning the error string. */
static char *verify_project_indexed(cbm_store_t *store, const char *project) {
    cbm_project_t proj_check = {0};
    if (cbm_store_get_project(store, project, &proj_check) != CBM_STORE_OK) {
        char *err = build_project_list_error("project not indexed; run index_repository first");
        char *res = cbm_mcp_text_result(err, true);
        free(err);
        return res;
    }
    cbm_project_free_fields(&proj_check);
    return NULL;
}

static bool store_has_adr(cbm_store_t *store, const char *project) {
    if (!store || !project || !project[0]) {
        return false;
    }
    cbm_adr_t adr = {0};
    if (cbm_store_adr_get(store, project, &adr) != CBM_STORE_OK) {
        return false;
    }
    cbm_store_adr_free(&adr);
    return true;
}

static bool schema_property_in_base(const char *property, const char *const *base,
                                    int base_count) {
    for (int i = 0; property && i < base_count; i++) {
        if (strcmp(property, base[i]) == 0) {
            return true;
        }
    }
    return false;
}

/* Join the bounded property inventory without an additional output-buffer cap.
 * The store-level discovery bound is reported separately to callers. */
static char *schema_join_properties(char *const *properties, int property_count,
                                    const char *const *base, int base_count,
                                    bool extras_only) {
    cbm_sb_t joined;
    cbm_sb_init(&joined);
    bool first = true;
    for (int i = 0; i < property_count; i++) {
        if (extras_only && schema_property_in_base(properties[i], base, base_count)) {
            continue;
        }
        if (!first) {
            cbm_sb_append(&joined, ";");
        }
        cbm_sb_append(&joined, properties[i]);
        first = false;
    }
    return cbm_sb_finish(&joined);
}

static char *schema_join_static_properties(const char *const *properties, int property_count) {
    return schema_join_properties((char *const *)properties, property_count, NULL, 0, false);
}

static char *schema_join_mut_string_array(yyjson_mut_val *array) {
    cbm_sb_t joined;
    cbm_sb_init(&joined);
    bool first = true;
    yyjson_mut_arr_iter iter;
    yyjson_mut_val *item = NULL;
    yyjson_mut_arr_iter_init(array, &iter);
    while ((item = yyjson_mut_arr_iter_next(&iter))) {
        const char *value = yyjson_mut_get_str(item);
        if (!value) {
            continue;
        }
        if (!first) {
            cbm_sb_append(&joined, ";");
        }
        cbm_sb_append(&joined, value);
        first = false;
    }
    return cbm_sb_finish(&joined);
}

static char *schema_relationship_pattern_text(const cbm_schema_relationship_t *pattern,
                                              bool executable_match) {
    if (!pattern || !pattern->source_label || !pattern->edge_type || !pattern->target_label) {
        return NULL;
    }
    cbm_sb_t sb;
    cbm_sb_init(&sb);
    if (executable_match) {
        cbm_sb_append(&sb, "MATCH (source:");
        cbm_sb_append(&sb, pattern->source_label);
        cbm_sb_append(&sb, ")-[:");
        cbm_sb_append(&sb, pattern->edge_type);
        cbm_sb_append(&sb, "]->(target:");
        cbm_sb_append(&sb, pattern->target_label);
        cbm_sb_append(&sb, ")");
    } else {
        char count[CBM_SZ_32];
        snprintf(count, sizeof(count), "%d", pattern->observed_count);
        cbm_sb_append(&sb, "(");
        cbm_sb_append(&sb, pattern->source_label);
        cbm_sb_append(&sb, ")-[");
        cbm_sb_append(&sb, pattern->edge_type);
        cbm_sb_append(&sb, "]->(");
        cbm_sb_append(&sb, pattern->target_label);
        cbm_sb_append(&sb, ") [");
        cbm_sb_append(&sb, count);
        cbm_sb_append(&sb, "x]");
    }
    return cbm_sb_finish(&sb);
}

static void schema_toon_append_freshness(cbm_sb_t *sb, yyjson_mut_val *root) {
    yyjson_mut_val *freshness = yyjson_mut_obj_get(root, CBM_MCP_FRESHNESS_KEY);
    if (!freshness || !yyjson_mut_is_obj(freshness)) {
        return;
    }
    static const char *const string_keys[] = {
        CBM_MCP_FRESHNESS_STATE_KEY, CBM_MCP_FRESHNESS_STALE_SCOPE_KEY,
        CBM_MCP_FRESHNESS_READ_MODEL_KEY,
    };
    static const char *const integer_keys[] = {
        CBM_MCP_FRESHNESS_DIRTY_PENDING_KEY, CBM_MCP_FRESHNESS_DIRTY_OVERLAY_READY_KEY,
        "overlay_ready_generations", "active_file_tombstones", "canonical_nodes_visible",
        "overlay_owned_nodes_visible", "total_nodes_visible",
    };
    char key[CBM_SZ_128];
    for (size_t i = 0; i < sizeof(string_keys) / sizeof(string_keys[0]); i++) {
        yyjson_mut_val *value = yyjson_mut_obj_get(freshness, string_keys[i]);
        if (value && yyjson_mut_is_str(value)) {
            snprintf(key, sizeof(key), "freshness_%s", string_keys[i]);
            cbm_toon_scalar_str(sb, key, yyjson_mut_get_str(value));
        }
    }
    for (size_t i = 0; i < sizeof(integer_keys) / sizeof(integer_keys[0]); i++) {
        yyjson_mut_val *value = yyjson_mut_obj_get(freshness, integer_keys[i]);
        if (value && yyjson_mut_is_int(value)) {
            snprintf(key, sizeof(key), "freshness_%s", integer_keys[i]);
            cbm_toon_scalar_int(sb, key, yyjson_mut_get_sint(value));
        }
    }
    static const char *const array_keys[] = {CBM_MCP_FRESHNESS_STALE_VIEWS_KEY,
                                             "active_sections"};
    for (size_t i = 0; i < sizeof(array_keys) / sizeof(array_keys[0]); i++) {
        yyjson_mut_val *value = yyjson_mut_obj_get(freshness, array_keys[i]);
        if (value && yyjson_mut_is_arr(value)) {
            char *joined = schema_join_mut_string_array(value);
            if (joined) {
                snprintf(key, sizeof(key), "freshness_%s", array_keys[i]);
                cbm_toon_scalar_str(sb, key, joined);
                free(joined);
            }
        }
    }
}

static char *schema_to_toon(const cbm_schema_info_t *schema, yyjson_mut_val *root) {
    /* Keep stable base properties factored once, followed by deterministically
     * ordered label/type extras and executable observed patterns. JSON and TOON
     * serializers consume the same structured facts; neither defines a second
     * schema contract. */
    cbm_sb_t sb;
    cbm_sb_init(&sb);
    int node_base_count = 0;
    int edge_base_count = 0;
    const char *const *node_base =
        cbm_store_schema_node_base_properties(&node_base_count);
    const char *const *edge_base =
        cbm_store_schema_edge_base_properties(&edge_base_count);
    char *node_base_text = schema_join_static_properties(node_base, node_base_count);
    char *edge_base_text = schema_join_static_properties(edge_base, edge_base_count);
    cbm_toon_scalar_str(&sb, "property_rule", "effective_properties=base_properties+extra_properties");
    cbm_toon_scalar_int(&sb, "property_key_limit_per_label_or_type",
                        CBM_STORE_SCHEMA_PROPERTY_KEY_LIMIT);
    cbm_toon_scalar_int(&sb, "relationship_pattern_limit",
                        CBM_STORE_SCHEMA_RELATIONSHIP_PATTERN_LIMIT);
    cbm_toon_scalar_str(&sb, "node_base_properties", node_base_text ? node_base_text : "");
    const char *node_columns[] = {"label", "count", "extra_properties"};
    cbm_toon_table_header(&sb, "node_labels", schema->node_label_count, node_columns, MCP_COL_3);
    for (int i = 0; i < schema->node_label_count; i++) {
        char *extra = schema_join_properties(schema->node_labels[i].properties,
                                             schema->node_labels[i].property_count,
                                             node_base, node_base_count, true);
        cbm_toon_row_begin(&sb);
        cbm_toon_cell_str(&sb, schema->node_labels[i].label, true);
        cbm_toon_cell_int(&sb, schema->node_labels[i].count, false);
        cbm_toon_cell_str(&sb, extra ? extra : "", false);
        cbm_toon_row_end(&sb);
        free(extra);
    }
    cbm_toon_scalar_str(&sb, "edge_base_properties", edge_base_text ? edge_base_text : "");
    const char *edge_columns[] = {"type", "count", "extra_properties"};
    cbm_toon_table_header(&sb, "edge_types", schema->edge_type_count, edge_columns, MCP_COL_3);
    for (int i = 0; i < schema->edge_type_count; i++) {
        char *extra = schema_join_properties(schema->edge_types[i].properties,
                                             schema->edge_types[i].property_count,
                                             edge_base, edge_base_count, true);
        cbm_toon_row_begin(&sb);
        cbm_toon_cell_str(&sb, schema->edge_types[i].type, true);
        cbm_toon_cell_int(&sb, schema->edge_types[i].count, false);
        cbm_toon_cell_str(&sb, extra ? extra : "", false);
        cbm_toon_row_end(&sb);
        free(extra);
    }
    const char *pattern_columns[] = {"match_pattern", "observed_count"};
    cbm_toon_table_header(&sb, "relationship_patterns", schema->rel_pattern_count,
                          pattern_columns, MCP_COL_2);
    for (int i = 0; i < schema->rel_pattern_count; i++) {
        char *match = schema_relationship_pattern_text(&schema->rel_patterns[i], true);
        if (!match) {
            free(node_base_text);
            free(edge_base_text);
            cbm_sb_free(&sb);
            return NULL;
        }
        cbm_toon_row_begin(&sb);
        cbm_toon_cell_str(&sb, match, true);
        cbm_toon_cell_int(&sb, schema->rel_patterns[i].observed_count, false);
        cbm_toon_row_end(&sb);
        free(match);
    }
    free(node_base_text);
    free(edge_base_text);

    yyjson_mut_val *adr_present = yyjson_mut_obj_get(root, "adr_present");
    if (adr_present && yyjson_mut_is_bool(adr_present)) {
        cbm_toon_scalar_bool(&sb, "architecture_decision_record_present",
                             yyjson_mut_get_bool(adr_present));
    }
    yyjson_mut_val *adr_hint = yyjson_mut_obj_get(root, "adr_hint");
    if (adr_hint && yyjson_mut_is_str(adr_hint)) {
        cbm_toon_scalar_str(&sb, "adr_hint", yyjson_mut_get_str(adr_hint));
    }
    schema_toon_append_freshness(&sb, root);
    yyjson_mut_val *warnings = yyjson_mut_obj_get(root, "warnings");
    if (warnings && yyjson_mut_is_arr(warnings)) {
        const char *warning_columns[] = {"message"};
        cbm_toon_table_header(&sb, "warnings", (int)yyjson_mut_arr_size(warnings),
                              warning_columns, 1);
        yyjson_mut_arr_iter iter;
        yyjson_mut_val *warning = NULL;
        yyjson_mut_arr_iter_init(warnings, &iter);
        while ((warning = yyjson_mut_arr_iter_next(&iter))) {
            cbm_toon_row_begin(&sb);
            cbm_toon_cell_str(&sb, yyjson_mut_get_str(warning), true);
            cbm_toon_row_end(&sb);
        }
    }
    return cbm_sb_finish(&sb);
}

static char *handle_get_graph_schema(cbm_mcp_server_t *srv, const char *args) {
    cbm_mcp_output_format_t response_format = cbm_mcp_response_format(srv, args);
    if (response_format == CBM_MCP_OUTPUT_INVALID) {
        return cbm_mcp_invalid_response_format();
    }
    char *raw_project = get_store_project_arg(args);
    project_expand_t pe = {0};
    cbm_store_t *store = resolve_project_store(srv, raw_project, &pe);
    char *project = pe.value;
    REQUIRE_STORE(store, project);

    cbm_store_overlay_node_view_summary_t overlay_summary = {0};
    bool used_active_schema = false;
    bool active_schema_failed = false;
    cbm_schema_info_t schema = {0};
    mcp_get_current_schema(store, project, MCP_CYPHER_FULL_QUERY_VOCABULARY, &schema,
                           &overlay_summary, &used_active_schema, &active_schema_failed);

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_int(doc, root, "property_key_limit_per_label_or_type",
                           CBM_STORE_SCHEMA_PROPERTY_KEY_LIMIT);
    yyjson_mut_obj_add_int(doc, root, "relationship_pattern_limit",
                           CBM_STORE_SCHEMA_RELATIONSHIP_PATTERN_LIMIT);

    yyjson_mut_val *labels = yyjson_mut_arr(doc);
    for (int i = 0; i < schema.node_label_count; i++) {
        yyjson_mut_val *lbl = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_str(doc, lbl, "label", schema.node_labels[i].label);
        yyjson_mut_obj_add_int(doc, lbl, "count", schema.node_labels[i].count);
        yyjson_mut_val *props = yyjson_mut_arr(doc);
        for (int j = 0; j < schema.node_labels[i].property_count; j++) {
            yyjson_mut_arr_add_str(doc, props, schema.node_labels[i].properties[j]);
        }
        yyjson_mut_obj_add_val(doc, lbl, "properties", props);
        yyjson_mut_arr_add_val(labels, lbl);
    }
    yyjson_mut_obj_add_val(doc, root, "node_labels", labels);

    yyjson_mut_val *types = yyjson_mut_arr(doc);
    for (int i = 0; i < schema.edge_type_count; i++) {
        yyjson_mut_val *typ = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_str(doc, typ, "type", schema.edge_types[i].type);
        yyjson_mut_obj_add_int(doc, typ, "count", schema.edge_types[i].count);
        yyjson_mut_val *eprops = yyjson_mut_arr(doc);
        for (int j = 0; j < schema.edge_types[i].property_count; j++) {
            yyjson_mut_arr_add_str(doc, eprops, schema.edge_types[i].properties[j]);
        }
        yyjson_mut_obj_add_val(doc, typ, "properties", eprops);
        yyjson_mut_arr_add_val(types, typ);
    }
    yyjson_mut_obj_add_val(doc, root, "edge_types", types);

    yyjson_mut_val *patterns = yyjson_mut_arr(doc);
    for (int i = 0; i < schema.rel_pattern_count; i++) {
        char *match = schema_relationship_pattern_text(&schema.rel_patterns[i], true);
        yyjson_mut_val *pattern = yyjson_mut_obj(doc);
        if (match) {
            yyjson_mut_obj_add_strcpy(doc, pattern, "match_pattern", match);
        }
        yyjson_mut_obj_add_int(doc, pattern, "observed_count",
                               schema.rel_patterns[i].observed_count);
        yyjson_mut_arr_add_val(patterns, pattern);
        free(match);
    }
    yyjson_mut_obj_add_val(doc, root, "relationship_patterns", patterns);

    /* SQLite is the canonical ADR backend shared by MCP and UI. Retain the
     * legacy file check so pre-migration installations still report truthfully. */
    bool adr_exists = store_has_adr(store, project);
    cbm_project_t proj_info = {0};
    if (!adr_exists && cbm_store_get_project(store, project, &proj_info) == 0 &&
        proj_info.root_path) {
        char adr_path[CBM_SZ_4K];
        int adr_len = snprintf(adr_path, sizeof(adr_path), "%s/.codebase-memory/adr.md",
                               proj_info.root_path);
        adr_exists = adr_len > 0 && (size_t)adr_len < sizeof(adr_path) && cbm_file_exists(adr_path);
    }
    cbm_project_free_fields(&proj_info);
    yyjson_mut_obj_add_bool(doc, root, "adr_present", adr_exists);
    if (!adr_exists) {
        yyjson_mut_obj_add_str(
            doc, root, "adr_hint",
            "No architecture decision record (ADR) found. Use manage_adr(mode='update') to persist architectural "
            "decisions across MCP server runs. Run get_architecture(aspects=['all']) first.");
    }

    bool overlay_limitation_reported = false;
    if (used_active_schema) {
        add_overlay_active_schema_freshness(
            doc, root, &overlay_summary, true,
            "get_graph_schema used active overlay node and edge rows for labels, edge types, and "
            "property keys.");
    } else {
        overlay_limitation_reported = add_canonical_only_overlay_freshness(
            doc, root, store, project,
            "get_graph_schema reads canonical schema counts and property keys; ready overlay rows "
            "are not included until active schema property views or compaction are available.");
        if (active_schema_failed && !overlay_limitation_reported) {
            add_response_warning(
                doc, root,
                "get_graph_schema could not read the active overlay schema view; canonical schema "
                "counts and property keys were returned.");
        }
    }
    int dirty_pending = 0;
    int dirty_overlay_ready = 0;
    if (get_dirty_file_counts(store, project, &dirty_pending, &dirty_overlay_ready)) {
        add_dirty_file_freshness_counts(doc, root, dirty_pending, dirty_overlay_ready);
        if (used_active_schema) {
            add_response_warning(
                doc, root,
                "get_graph_schema used ready overlay rows, but pending dirty file changes may be "
                "absent until overlay or reindex completes.");
        } else {
            add_canonical_only_read_model(doc, root);
        }
        if (!used_active_schema && !overlay_limitation_reported) {
            add_response_warning(
                doc, root,
                "get_graph_schema reads canonical schema counts and property keys; dirty file "
                "changes may be absent until overlay or reindex completes.");
        }
    }

    char *payload = response_format == CBM_MCP_OUTPUT_TOON ? schema_to_toon(&schema, root)
                                                           : yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    cbm_store_schema_free(&schema);
    free(project);

    char *result = cbm_mcp_text_result(payload ? payload : "out of memory", payload == NULL);
    free(payload);
    return result;
}

/* Expand a raw project param, resolve the correct store, and auto-index if needed.
 * Returns the resolved store (or NULL). Sets *out_pe to the expand result
 * (caller must free out_pe->value). Handles:
 *   - expand_project_param (Rule 0: /path → project name)
 *   - DB selection with prefix collision avoidance
 *   - Auto-index on first use (join background thread or sync index) */
static cbm_store_t *resolve_project_store(cbm_mcp_server_t *srv,
                                           char *raw_project,
                                           project_expand_t *out_pe) {
    /* Cold-start: when no project param given, use session_project so
     * auto-index fires on the correct DB instead of returning NULL. */
    /* Save the resolved filesystem path BEFORE expand_project_param consumes
     * raw_project.  Used below to auto-index paths that aren't under session_root
     * (e.g. .gitignore-excluded subdirs that are separate git repos). */
    bool raw_project_explicit = raw_project != NULL;
    bool raw_project_path = raw_project_explicit && project_is_path(raw_project);
    char *_raw_path = NULL;
    if (raw_project_path) {
        _raw_path = project_canonical_path(raw_project, NULL);
    }

    project_expand_t pe;
    if (!raw_project && srv->session_project[0]) {
        pe.value = heap_strdup(srv->session_project);
        pe.mode = MATCH_PREFIX;
    } else {
        pe = expand_project_param(srv, raw_project); /* raw_project freed inside */
    }

    /* DB selection: if expanded value IS the session project or a dep of it
     * (session.dep.X), use session store. The check requires the char after
     * session_project to be '.' or '\0' to avoid prefix collisions. */
    const char *db_project = pe.value;
    if (pe.value && srv->session_project[0]) {
        size_t sp_len = strlen(srv->session_project);
        if (strncmp(pe.value, srv->session_project, sp_len) == 0 &&
            (pe.value[sp_len] == '.' || pe.value[sp_len] == '\0')) {
            db_project = srv->session_project; /* deps are in session db */
        }
    }
    cbm_store_t *store = resolve_store(srv, db_project);
    bool session_store_selected = db_project && srv->session_project[0] &&
                                  strcmp(db_project, srv->session_project) == 0;
    bool may_use_session_root = !raw_project_explicit || session_store_selected;

    /* Auto-index on first use (same enablement as REQUIRE_STORE). Explicit
     * non-path project names are authoritative: a missing slug must report
     * not-found instead of silently searching the active session project. */
    if (!store && may_use_session_root && srv->session_root[0] &&
        cbm_is_dir(srv->session_root)) {
        if (srv->autoindex_active) {
            cbm_thread_join(&srv->autoindex_tid);
            srv->autoindex_active = false;
            store = resolve_store(srv, db_project);
        }
        if (!store && !_raw_path && cbm_mcp_auto_index_enabled(srv) &&
            cbm_mcp_auto_index_within_limit(srv, srv->session_root)) {
            if (cbm_mcp_run_sync_auto_index(srv, srv->session_root, "autoindex.sync", "project",
                                            srv->session_project)) {
                if (srv->owns_store && srv->store) {
                    cbm_store_close(srv->store);
                    srv->store = NULL;
                }
                free(srv->current_project);
                srv->current_project = NULL;
                store = resolve_store(srv, srv->session_project);
                if (store) {
                    cbm_mcp_refresh_auto_indexed_store(srv, store, srv->session_project,
                                                       srv->session_root);
                }
            }
        }
    }

    /* Path-based auto-index: fires when project= is a full path to a directory
     * that is NOT under session_root (e.g. a .gitignore-excluded subdirectory
     * that is a separate git repo).  The session_root block above only indexes
     * srv->session_root; if the requested path differs, store is still NULL here.
     *
     * Example: main project at ~/myapp, queried with
     *   project="/path/to/react-grid-layout" (in ~/myapp/.gitignore)
     * session_root stays ~/myapp; react-grid-layout is never indexed by the block
     * above.  This block catches that case and indexes the exact requested path. */
    if (!store && _raw_path && cbm_mcp_auto_index_enabled(srv)) {
        if (cbm_is_dir(_raw_path) && cbm_mcp_auto_index_within_limit(srv, _raw_path)) {
            if (cbm_mcp_run_sync_auto_index(srv, _raw_path, "autoindex.path", "path", _raw_path)) {
                store = resolve_store(srv, db_project);
                if (store) {
                    cbm_mcp_refresh_auto_indexed_store(srv, store, db_project, _raw_path);
                }
            }
        }
    }
    free(_raw_path);

    *out_pe = pe; /* caller takes ownership of pe.value */
    return store;
}

/* Validate edge type: uppercase letters + underscore only, max 64 chars. */
static bool validate_edge_type(const char *s) {
    if (!s || strlen(s) > CBM_SZ_64) {
        return false;
    }
    for (const char *c = s; *c; c++) {
        if (!(*c >= 'A' && *c <= 'Z') && *c != '_') {
            return false;
        }
    }
    return true;
}

/* Enrich search result with 1-hop connected node names. */
/* Add BFS results to a yyjson array (deduped by name). */
static void enrich_add_bfs(yyjson_mut_doc *doc, yyjson_mut_val *arr, cbm_traverse_result_t *tr) {
    for (int j = 0; j < tr->visited_count; j++) {
        if (tr->visited[j].node.name) {
            yyjson_mut_arr_add_strcpy(doc, arr, tr->visited[j].node.name);
        }
    }
}

/* Enrich search result with 1-hop connected node names (inbound + outbound). */
static void enrich_connected(yyjson_mut_doc *doc, yyjson_mut_val *item, cbm_store_t *store,
                             int64_t node_id, const char *relationship) {
    const char *et[] = {relationship ? relationship : "CALLS"};
    yyjson_mut_val *conn = yyjson_mut_arr(doc);

    /* BFS doesn't support "both" — run inbound + outbound separately. */
    cbm_traverse_result_t tr_in = {0};
    cbm_store_bfs(store, node_id, "inbound", et, SKIP_ONE, SKIP_ONE, MCP_DEFAULT_LIMIT, &tr_in);
    enrich_add_bfs(doc, conn, &tr_in);
    cbm_store_traverse_free(&tr_in);

    cbm_traverse_result_t tr_out = {0};
    cbm_store_bfs(store, node_id, "outbound", et, SKIP_ONE, SKIP_ONE, MCP_DEFAULT_LIMIT, &tr_out);
    enrich_add_bfs(doc, conn, &tr_out);
    cbm_store_traverse_free(&tr_out);

    if (yyjson_mut_arr_size(conn) > 0) {
        yyjson_mut_obj_add_val(doc, item, "connected_names", conn);
    }
}

static void add_search_result_connected_names(yyjson_mut_doc *doc, yyjson_mut_val *item,
                                              const cbm_search_result_t *sr) {
    yyjson_mut_val *conn = yyjson_mut_arr(doc);
    for (int i = 0; i < sr->connected_count; i++) {
        if (sr->connected_names[i] && sr->connected_names[i][0]) {
            yyjson_mut_arr_add_strcpy(doc, conn, sr->connected_names[i]);
        }
    }
    if (yyjson_mut_arr_size(conn) > 0) {
        yyjson_mut_obj_add_val(doc, item, "connected_names", conn);
    }
}

/* Build an FTS5 MATCH expression from a free-form query string by splitting
 * on whitespace and joining the terms with OR.  Each token is also sanitized:
 * anything that isn't alnum or underscore is dropped, so the caller can't
 * inject FTS5 operators or double-quoted phrases.  Returns the number of
 * tokens emitted (0 if the query contained no usable terms). */
enum {
    BM25_MIN_BUF = 2, /* minimum buffer size: at least NUL + one char */
    BM25_SEP_RESERVE = 1,
    BM25_QUERY_BUF = 1024,
    BM25_DEFAULT_LIMIT = 50,
    BM25_COL_ID = 0,
    BM25_COL_LABEL = 1,
    BM25_COL_NAME = 2,
    BM25_COL_QN = 3,
    BM25_COL_FILE = 4,
    BM25_COL_START = 5,
    BM25_COL_END = 6,
    BM25_COL_RANK = 7,
    BM25_BIND_QUERY = 1,
    BM25_BIND_PROJECT = 2,
    BM25_BIND_LIMIT = 3,
    BM25_BIND_OFFSET = 4,
    BM25_BIND_INNER = 5,
    BM25_BIND_FILE = 6,
    BM25_SQL_AUTO_LEN = -1,
    BM25_MAX_TERMS = 32,
    BM25_MAX_TERM_BYTES = 256,
    BM25_OVERLAY_BIND_STATUS = 1,
    BM25_OVERLAY_BIND_TOMBSTONE_KIND = 2,
    BM25_OVERLAY_BIND_QUERY = 3,
    BM25_OVERLAY_BIND_PROJECT = 4,
    BM25_OVERLAY_BIND_LIMIT = 5,
    BM25_OVERLAY_BIND_OFFSET = 6,
    BM25_OVERLAY_BIND_INNER = 7,
    BM25_OVERLAY_BIND_FILE = 8,
    BM25_OVERLAY_COL_LABEL = 1,
    BM25_OVERLAY_COL_NAME = 2,
    BM25_OVERLAY_COL_QN = 3,
    BM25_OVERLAY_COL_FILE = 4,
    BM25_OVERLAY_COL_START = 5,
    BM25_OVERLAY_COL_END = 6,
    BM25_OVERLAY_COL_RANK = 7,
    /* Inner FTS5 candidate cap.  SQLite can early-terminate a plain FTS5 query
     * (no JOIN/WHERE on outer table) of the form:
     *   SELECT rowid, bm25() FROM nodes_fts WHERE MATCH ? ORDER BY bm25() LIMIT N
     * By fetching only the top BM25_INNER_LIMIT candidates from the FTS5 index
     * and then joining/filtering/re-ranking those, we bound all work to O(N) where
     * N = BM25_INNER_LIMIT rather than the full match set size. */
    BM25_INNER_LIMIT = 2000,
};

static const double BM25_OVERLAY_BASE_RANK = -100000.0;

typedef struct {
    char *items[BM25_MAX_TERMS];
    int count;
} bm25_terms_t;

/* Module-local SQLITE_TRANSIENT wrapper to dodge performance-no-int-to-ptr.
 * See the matching helper in src/store/store.c for the same pattern. */
static sqlite3_destructor_type mcp_sqlite_transient(void) {
    static const volatile intptr_t raw = -1;
    sqlite3_destructor_type dtor = NULL;
    memcpy(&dtor, (const void *)&raw, sizeof(dtor));
    return dtor;
}
#define MCP_SQLITE_TRANSIENT (mcp_sqlite_transient())

static bool bm25_is_token_char(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
           (c >= '0' && c <= '9') || c == '_';
}

static void bm25_terms_free(bm25_terms_t *terms) {
    if (!terms) {
        return;
    }
    for (int i = 0; i < terms->count; i++) {
        free(terms->items[i]);
        terms->items[i] = NULL;
    }
    terms->count = 0;
}

static int bm25_collect_terms(const char *query, bm25_terms_t *terms) {
    if (!query || !terms) {
        return 0;
    }
    memset(terms, 0, sizeof(*terms));
    const char *p = query;
    while (*p) {
        while (*p && !bm25_is_token_char(*p)) {
            p++;
        }
        if (!*p) {
            break;
        }
        const char *tok_start = p;
        while (*p && bm25_is_token_char(*p)) {
            p++;
        }
        size_t tok_len = (size_t)(p - tok_start);
        if (tok_len == 0 || tok_len > BM25_MAX_TERM_BYTES) {
            continue;
        }
        if (terms->count >= BM25_MAX_TERMS) {
            break;
        }
        char *term = malloc(tok_len + SKIP_ONE);
        if (!term) {
            bm25_terms_free(terms);
            return CBM_STORE_ERR;
        }
        memcpy(term, tok_start, tok_len);
        term[tok_len] = '\0';
        terms->items[terms->count++] = term;
    }
    return terms->count;
}

static int bm25_build_match_from_terms(const bm25_terms_t *terms, char *out, size_t out_size) {
    if (!terms || !out || out_size < BM25_MIN_BUF) {
        return 0;
    }
    size_t pos = 0;
    for (int i = 0; i < terms->count; i++) {
        const char *term = terms->items[i];
        size_t tok_len = term ? strlen(term) : 0;
        if (tok_len == 0) {
            continue;
        }
        const char *sep = (pos > 0) ? " OR " : "";
        size_t sep_len = strlen(sep);
        if (pos + sep_len + tok_len + BM25_SEP_RESERVE >= out_size) {
            break; /* out of room — stop cleanly, keep what we have */
        }
        memcpy(out + pos, sep, sep_len);
        pos += sep_len;
        memcpy(out + pos, term, tok_len);
        pos += tok_len;
    }
    out[pos] = '\0';
    return pos > 0 ? terms->count : 0;
}

static int bm25_build_match(const char *query, char *out, size_t out_size) {
    bm25_terms_t terms = {0};
    int term_count = bm25_collect_terms(query, &terms);
    if (term_count <= 0) {
        return 0;
    }
    int emitted = bm25_build_match_from_terms(&terms, out, out_size);
    bm25_terms_free(&terms);
    return emitted;
}

static bool bm25_query_has_terms(const char *query) {
    char fts_query[BM25_QUERY_BUF];
    return bm25_build_match(query, fts_query, sizeof(fts_query)) > 0;
}

static char *bm25_file_pattern_like(const char *file_pattern) {
    if (!file_pattern) {
        return NULL;
    }
    char *like = cbm_glob_to_like(file_pattern);
    if (like && !strchr(file_pattern, '*') && !strchr(file_pattern, '?')) {
        size_t len = strlen(like);
        char *contains = malloc(len + MCP_SEPARATOR + SKIP_ONE);
        if (contains) {
            contains[0] = '%';
            memcpy(contains + SKIP_ONE, like, len);
            contains[len + SKIP_ONE] = '%';
            contains[len + MCP_SEPARATOR] = '\0';
            free(like);
            like = contains;
        }
    }
    return like;
}

static int bm25_bind_overlay_query(sqlite3_stmt *stmt, const char *fts_query,
                                   const char *project, int limit, int offset,
                                   const char *file_like) {
    sqlite3_bind_text(stmt, BM25_OVERLAY_BIND_STATUS, CBM_STORE_OVERLAY_STATUS_READY,
                      BM25_SQL_AUTO_LEN, MCP_SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, BM25_OVERLAY_BIND_TOMBSTONE_KIND, CBM_STORE_OVERLAY_TOMBSTONE_FILE,
                      BM25_SQL_AUTO_LEN, MCP_SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, BM25_OVERLAY_BIND_QUERY, fts_query, BM25_SQL_AUTO_LEN,
                      MCP_SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, BM25_OVERLAY_BIND_PROJECT, project, BM25_SQL_AUTO_LEN,
                      MCP_SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt, BM25_OVERLAY_BIND_LIMIT, limit > 0 ? limit : BM25_DEFAULT_LIMIT);
    sqlite3_bind_int(stmt, BM25_OVERLAY_BIND_OFFSET, offset > 0 ? offset : 0);
    sqlite3_bind_int(stmt, BM25_OVERLAY_BIND_INNER, BM25_INNER_LIMIT);
    if (file_like) {
        sqlite3_bind_text(stmt, BM25_OVERLAY_BIND_FILE, file_like, BM25_SQL_AUTO_LEN,
                          MCP_SQLITE_TRANSIENT);
    } else {
        sqlite3_bind_null(stmt, BM25_OVERLAY_BIND_FILE);
    }
    return CBM_STORE_OK;
}

/* Overlay-aware query mode keeps the canonical BM25 fast path for unchanged files,
 * suppresses canonical rows hidden by active file tombstones, and unions in owned
 * changed-file overlay rows by bounded node-text matching. */
static char *bm25_search_overlay_active(cbm_store_t *store, const char *project,
                                        const char *query, const char *file_pattern, int limit,
                                        int offset,
                                        const cbm_store_overlay_node_view_summary_t *summary) {
    if (!cbm_store_overlay_node_view_has_ready_rows(summary)) {
        return NULL;
    }
    sqlite3 *db = cbm_store_get_db(store);
    if (!db) {
        return NULL;
    }

    bm25_terms_t terms = {0};
    int term_count = bm25_collect_terms(query, &terms);
    if (term_count <= 0) {
        return NULL;
    }
    char fts_query[BM25_QUERY_BUF];
    if (bm25_build_match_from_terms(&terms, fts_query, sizeof(fts_query)) <= 0) {
        bm25_terms_free(&terms);
        return NULL;
    }

    char active_cte[CBM_SZ_8K];
    if (cbm_store_build_active_overlay_cte(active_cte, sizeof(active_cte), false, false) !=
        CBM_STORE_OK) {
        bm25_terms_free(&terms);
        return NULL;
    }
    char *file_like = bm25_file_pattern_like(file_pattern);

    char ranked_sql[CBM_SZ_16K];
    int n = snprintf(
        ranked_sql, sizeof(ranked_sql),
        "%s"
        ", ranked AS ("
        "  SELECT n.id, n.label, n.name, n.qualified_name, n.file_path, n.start_line, "
        "         n.end_line, "
        "         (fts.base_rank "
        "          - CASE WHEN n.label IN ('Function','Method') THEN 10.0 "
        "                 WHEN n.label = 'Route' THEN 8.0 "
        "                 WHEN n.label IN ('Class','Interface','Type','Enum') THEN 5.0 "
        "                 ELSE 0.0 END) AS rank "
        "  FROM ("
        "      SELECT rowid, bm25(nodes_fts) AS base_rank"
        "      FROM nodes_fts WHERE nodes_fts MATCH ?3"
        "      ORDER BY base_rank LIMIT ?7"
        "  ) fts "
        "  JOIN active_nodes n ON n.id = fts.rowid "
        "  WHERE n.project = ?4 "
        "    AND n.label NOT IN ('File','Folder','Module','Section','Variable','Project') "
        "    AND (?8 IS NULL OR n.file_path LIKE ?8) "
        "  UNION ALL "
        "  SELECT %d AS id, n.label, n.name, n.qualified_name, n.file_path, n.start_line, "
        "         n.end_line, "
        "         (fts.overlay_rank + %.1f "
        "          - CASE WHEN n.label IN ('Function','Method') THEN 10.0 "
        "                  WHEN n.label = 'Route' THEN 8.0 "
        "                  WHEN n.label IN ('Class','Interface','Type','Enum') THEN 5.0 "
        "                  ELSE 0.0 END) AS rank "
        "  FROM ("
        "      SELECT rowid, bm25(" CBM_STORE_DERIVED_VIEW_NODES_FTS_OVERLAY ") AS overlay_rank"
        "      FROM " CBM_STORE_DERIVED_VIEW_NODES_FTS_OVERLAY
        "      WHERE " CBM_STORE_DERIVED_VIEW_NODES_FTS_OVERLAY " MATCH ?3"
        "      ORDER BY overlay_rank LIMIT ?7"
        "  ) fts "
        "  JOIN overlay_nodes n ON n.id = fts.rowid "
        "  JOIN active_overlay_files af"
        "    ON af.project = n.project AND af.rel_path = n.rel_path"
        "   AND af.overlay_generation = n.overlay_generation "
        "  WHERE n.project = ?4 AND n.owned != 0 "
        "    AND n.label NOT IN ('File','Folder','Module','Section','Variable','Project') "
        "    AND (?8 IS NULL OR n.file_path LIKE ?8) "
        ") ",
        active_cte, CBM_STORE_NO_NODE_ID, BM25_OVERLAY_BASE_RANK);
    if (n < 0 || (size_t)n >= sizeof(ranked_sql)) {
        free(file_like);
        bm25_terms_free(&terms);
        return NULL;
    }

    char sql[CBM_SZ_16K];
    n = snprintf(sql, sizeof(sql),
                 "%sSELECT id, label, name, qualified_name, file_path, start_line, end_line, "
                 "rank FROM ranked ORDER BY rank, name, qualified_name LIMIT ?5 OFFSET ?6",
                 ranked_sql);
    if (n < 0 || (size_t)n >= sizeof(sql)) {
        free(file_like);
        bm25_terms_free(&terms);
        return NULL;
    }

    char count_sql[CBM_SZ_16K];
    n = snprintf(count_sql, sizeof(count_sql), "%sSELECT COUNT(*) FROM ranked", ranked_sql);
    if (n < 0 || (size_t)n >= sizeof(count_sql)) {
        free(file_like);
        bm25_terms_free(&terms);
        return NULL;
    }

    int total = 0;
    sqlite3_stmt *cs = NULL;
    if (sqlite3_prepare_v2(db, count_sql, BM25_SQL_AUTO_LEN, &cs, NULL) != SQLITE_OK) {
        free(file_like);
        bm25_terms_free(&terms);
        return NULL;
    }
    if (bm25_bind_overlay_query(cs, fts_query, project, limit, offset, file_like) != CBM_STORE_OK ||
        sqlite3_step(cs) != SQLITE_ROW) {
        sqlite3_finalize(cs);
        free(file_like);
        bm25_terms_free(&terms);
        return NULL;
    }
    total = sqlite3_column_int(cs, 0);
    sqlite3_finalize(cs);

    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(db, sql, BM25_SQL_AUTO_LEN, &stmt, NULL) != SQLITE_OK) {
        free(file_like);
        bm25_terms_free(&terms);
        return NULL;
    }
    if (bm25_bind_overlay_query(stmt, fts_query, project, limit, offset, file_like) !=
        CBM_STORE_OK) {
        sqlite3_finalize(stmt);
        free(file_like);
        bm25_terms_free(&terms);
        return NULL;
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    if (!doc) {
        sqlite3_finalize(stmt);
        free(file_like);
        bm25_terms_free(&terms);
        return NULL;
    }
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    if (!root) {
        yyjson_mut_doc_free(doc);
        sqlite3_finalize(stmt);
        free(file_like);
        bm25_terms_free(&terms);
        return NULL;
    }
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_int(doc, root, "total", total);
    yyjson_mut_obj_add_str(doc, root, "search_mode", "bm25");
    add_overlay_active_query_freshness(doc, root, summary);

    yyjson_mut_val *results = yyjson_mut_arr(doc);
    int emitted = 0;
    int step_rc = SQLITE_OK;
    while ((step_rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        yyjson_mut_val *item = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_strcpy(
            doc, item, "name", (const char *)sqlite3_column_text(stmt, BM25_OVERLAY_COL_NAME));
        yyjson_mut_obj_add_strcpy(doc, item, "qualified_name",
                                  (const char *)sqlite3_column_text(stmt, BM25_OVERLAY_COL_QN));
        yyjson_mut_obj_add_strcpy(
            doc, item, "label", (const char *)sqlite3_column_text(stmt, BM25_OVERLAY_COL_LABEL));
        yyjson_mut_obj_add_strcpy(
            doc, item, "file_path", (const char *)sqlite3_column_text(stmt, BM25_OVERLAY_COL_FILE));
        yyjson_mut_obj_add_int(doc, item, "start_line",
                               sqlite3_column_int(stmt, BM25_OVERLAY_COL_START));
        yyjson_mut_obj_add_int(doc, item, "end_line",
                               sqlite3_column_int(stmt, BM25_OVERLAY_COL_END));
        yyjson_mut_obj_add_real(doc, item, "rank",
                                sqlite3_column_double(stmt, BM25_OVERLAY_COL_RANK));
        yyjson_mut_arr_add_val(results, item);
        emitted++;
    }
    if (step_rc != SQLITE_DONE) {
        yyjson_mut_doc_free(doc);
        sqlite3_finalize(stmt);
        free(file_like);
        bm25_terms_free(&terms);
        return NULL;
    }
    sqlite3_finalize(stmt);
    free(file_like);
    bm25_terms_free(&terms);

    yyjson_mut_obj_add_val(doc, root, "results", results);
    yyjson_mut_obj_add_bool(doc, root, "has_more", total > offset + emitted);

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return json;
}

/* Run the BM25 full-text search path and return the JSON result string.
 * Returns NULL if FTS5 is unavailable or the query produced no usable tokens,
 * in which case the caller falls back to the regex-based search path. */
static char *bm25_search(cbm_store_t *store, const char *project, const char *query,
                         const char *file_pattern, int limit, int offset, bool toon) {
    sqlite3 *db = cbm_store_get_db(store);
    if (!db) {
        return NULL;
    }
    char fts_query[BM25_QUERY_BUF];
    int tok_count = bm25_build_match(query, fts_query, sizeof(fts_query));
    if (tok_count == 0) {
        return NULL;
    }
    char *file_like = bm25_file_pattern_like(file_pattern);

    /* BM25 ranked query using a two-step approach to enable FTS5 early termination.
     *
     * Flat queries of the form:
     *   SELECT ... FROM nodes_fts JOIN nodes WHERE MATCH ? AND n.project=? ORDER BY rank LIMIT N
     * block FTS5's WAND/MaxScore early-exit because the outer JOIN+WHERE conditions
     * are invisible to the FTS5 planner — it must score every matching document before
     * the project/label filter can discard any of them.  On a large codebase with 100K+
     * matches, this causes multi-minute queries.
     *
     * The fix: let FTS5 drive the inner subquery alone.  SQLite CAN early-terminate
     *   SELECT rowid, bm25(nodes_fts) FROM nodes_fts WHERE MATCH ? ORDER BY bm25() LIMIT N
     * because no outer predicate blocks it.  We fetch BM25_INNER_LIMIT top candidates
     * from the FTS5 index, then join/filter/boost only those rows.  bm25() returns a
     * NEGATIVE score (lower = more relevant). */
    const char *sql =
        "SELECT n.id, n.label, n.name, n.qualified_name, n.file_path, n.start_line, n.end_line, "
        "       (fts.base_rank "
        "        - CASE WHEN n.label IN ('Function','Method') THEN 10.0 "
        "               WHEN n.label = 'Route' THEN 8.0 "
        "               WHEN n.label IN ('Class','Interface','Type','Enum') THEN 5.0 "
        "               ELSE 0.0 END) AS rank "
        "FROM ("
        "    SELECT rowid, bm25(nodes_fts) AS base_rank"
        "    FROM nodes_fts WHERE nodes_fts MATCH ?1"
        "    ORDER BY base_rank LIMIT ?5"
        ") fts "
        "JOIN nodes n ON n.id = fts.rowid "
        "WHERE n.project = ?2 "
        "  AND n.label NOT IN ('File','Folder','Module','Section','Variable','Project') "
        "  AND (?6 IS NULL OR n.file_path LIKE ?6) "
        "ORDER BY rank "
        "LIMIT ?3 OFFSET ?4";

    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(db, sql, BM25_SQL_AUTO_LEN, &stmt, NULL) != SQLITE_OK) {
        free(file_like);
        return NULL;
    }
    sqlite3_bind_text(stmt, BM25_BIND_QUERY, fts_query, BM25_SQL_AUTO_LEN, MCP_SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, BM25_BIND_PROJECT, project, BM25_SQL_AUTO_LEN, MCP_SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt, BM25_BIND_LIMIT, limit > 0 ? limit : BM25_DEFAULT_LIMIT);
    sqlite3_bind_int(stmt, BM25_BIND_OFFSET, offset > 0 ? offset : 0);
    sqlite3_bind_int(stmt, BM25_BIND_INNER, BM25_INNER_LIMIT);
    if (file_like) {
        sqlite3_bind_text(stmt, BM25_BIND_FILE, file_like, BM25_SQL_AUTO_LEN, MCP_SQLITE_TRANSIENT);
    } else {
        sqlite3_bind_null(stmt, BM25_BIND_FILE);
    }

    /* Count hits within the same inner-limit window — capped at BM25_INNER_LIMIT.
     * Uses the identical subquery structure so the FTS5 early-exit applies here too. */
    int total = 0;
    {
        const char *count_sql =
            "SELECT COUNT(*) FROM ("
            "    SELECT fts.rowid FROM ("
            "        SELECT rowid FROM nodes_fts WHERE nodes_fts MATCH ?1"
            "        ORDER BY bm25(nodes_fts) LIMIT ?3"
            "    ) fts "
            "    JOIN nodes n ON n.id = fts.rowid "
            "    WHERE n.project = ?2 "
            "      AND n.label NOT IN ('File','Folder','Module','Section','Variable','Project')"
            "      AND (?6 IS NULL OR n.file_path LIKE ?6)"
            ")";
        sqlite3_stmt *cs = NULL;
        if (sqlite3_prepare_v2(db, count_sql, BM25_SQL_AUTO_LEN, &cs, NULL) == SQLITE_OK) {
            sqlite3_bind_text(cs, BM25_BIND_QUERY, fts_query, BM25_SQL_AUTO_LEN,
                              MCP_SQLITE_TRANSIENT);
            sqlite3_bind_text(cs, BM25_BIND_PROJECT, project, BM25_SQL_AUTO_LEN,
                              MCP_SQLITE_TRANSIENT);
            sqlite3_bind_int(cs, BM25_BIND_LIMIT, BM25_INNER_LIMIT);
            if (file_like) {
                sqlite3_bind_text(cs, BM25_BIND_FILE, file_like, BM25_SQL_AUTO_LEN,
                                  MCP_SQLITE_TRANSIENT);
            } else {
                sqlite3_bind_null(cs, BM25_BIND_FILE);
            }
            if (sqlite3_step(cs) == SQLITE_ROW) {
                total = sqlite3_column_int(cs, 0);
            }
            sqlite3_finalize(cs);
        }
    }

    if (toon) {
        /* TOON: rows are buffered first because the table header carries the
         * row count, which sqlite only yields by stepping to completion. */
        cbm_sb_t rows;
        cbm_sb_init(&rows);
        int emitted = 0;
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            char lines[CBM_SZ_32];
            int sl = sqlite3_column_int(stmt, BM25_COL_START);
            int el = sqlite3_column_int(stmt, BM25_COL_END);
            if (sl > 0) {
                snprintf(lines, sizeof(lines), "%d-%d", sl, el > sl ? el : sl);
            } else {
                lines[0] = '\0';
            }
            cbm_toon_row_begin(&rows);
            cbm_toon_cell_str(&rows, (const char *)sqlite3_column_text(stmt, BM25_COL_QN), true);
            cbm_toon_cell_str(&rows, (const char *)sqlite3_column_text(stmt, BM25_COL_LABEL),
                              false);
            cbm_toon_cell_str(&rows, (const char *)sqlite3_column_text(stmt, BM25_COL_FILE), false);
            cbm_toon_cell_str(&rows, lines, false);
            cbm_toon_cell_real(&rows, sqlite3_column_double(stmt, BM25_COL_RANK), false);
            cbm_toon_row_end(&rows);
            emitted++;
        }
        sqlite3_finalize(stmt);
        free(file_like);

        cbm_sb_t sb;
        cbm_sb_init(&sb);
        cbm_toon_scalar_int(&sb, "total", total);
        cbm_toon_scalar_str(&sb, "search_mode", "bm25");
        static const char *const cols[] = {"qn", "label", "file", "lines", "rank"};
        cbm_toon_table_header(&sb, "results", emitted, cols, 5);
        char *rows_text = cbm_sb_finish(&rows);
        cbm_sb_append(&sb, rows_text ? rows_text : "");
        free(rows_text);
        cbm_toon_scalar_bool(&sb, "has_more", total > offset + emitted);
        return cbm_sb_finish(&sb);
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_int(doc, root, "total", total);
    yyjson_mut_obj_add_str(doc, root, "search_mode", "bm25");

    yyjson_mut_val *results = yyjson_mut_arr(doc);
    int emitted = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        yyjson_mut_val *item = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_strcpy(doc, item, "name",
                                  (const char *)sqlite3_column_text(stmt, BM25_COL_NAME));
        yyjson_mut_obj_add_strcpy(doc, item, "qualified_name",
                                  (const char *)sqlite3_column_text(stmt, BM25_COL_QN));
        yyjson_mut_obj_add_strcpy(doc, item, "label",
                                  (const char *)sqlite3_column_text(stmt, BM25_COL_LABEL));
        yyjson_mut_obj_add_strcpy(doc, item, "file_path",
                                  (const char *)sqlite3_column_text(stmt, BM25_COL_FILE));
        yyjson_mut_obj_add_int(doc, item, "start_line", sqlite3_column_int(stmt, BM25_COL_START));
        yyjson_mut_obj_add_int(doc, item, "end_line", sqlite3_column_int(stmt, BM25_COL_END));
        yyjson_mut_obj_add_real(doc, item, "rank", sqlite3_column_double(stmt, BM25_COL_RANK));
        yyjson_mut_arr_add_val(results, item);
        emitted++;
    }
    sqlite3_finalize(stmt);
    free(file_like);

    yyjson_mut_obj_add_val(doc, root, "results", results);
    yyjson_mut_obj_add_bool(doc, root, "has_more", total > offset + emitted);

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return json;
}

/* Forward declarations — definitions live with the compact TOON helpers and
 * search property enrichment below. */
enum { SG_MAX_EXTRA_FIELDS = 12 };
static bool sg_field_blocked(const char *field);
static int sg_parse_fields(const char *args, const char *out[], int max_out,
                           yyjson_doc **out_owner);
static bool sg_field_selected(const char *field, const char *const fields[], int field_count);

/* enrich_node_properties parses the
 * node's properties_json and grafts the parsed values onto the result item.
 * It returns the parsed yyjson_doc which must outlive the serialization
 * because yyjson_mut_obj_add_val uses zero-copy strings into that doc. */
static yyjson_doc *enrich_node_properties(yyjson_mut_doc *doc, yyjson_mut_val *obj,
                                          const char *properties_json, bool compact,
                                          const char *const fields[], int field_count);

/* Emit the cbm_store_search results as a JSON "results" array on the doc.
 * Property docs created via enrich_node_properties are collected in
 * *out_pdocs (count in *out_pdoc_count) and must be freed by the caller
 * AFTER serializing doc, since yyjson_mut strings are zero-copy pointers
 * into those parsed docs. The caller also frees out_pdocs itself. */
static void emit_search_results(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                const cbm_search_output_t *out, cbm_store_t *store,
                                const char *relationship, bool include_connected,
                                bool connected_names_authoritative, int offset, int limit,
                                bool compact, const char *session_project, const char *args,
                                yyjson_doc ***out_pdocs, int *out_pdoc_count) {
    yyjson_doc **pdocs = out->count > 0 ? malloc((size_t)out->count * sizeof(yyjson_doc *)) : NULL;
    int pdoc_count = 0;
    bool properties_omitted_oom = out->count > 0 && !pdocs;
    const char *fields[SG_MAX_EXTRA_FIELDS];
    yyjson_doc *fields_owner = NULL;
    int field_count = sg_parse_fields(args, fields, SG_MAX_EXTRA_FIELDS, &fields_owner);
    yyjson_mut_obj_add_int(doc, root, "total", out->total);
    yyjson_mut_val *results = yyjson_mut_arr(doc);
    for (int i = 0; i < out->count; i++) {
        cbm_search_result_t *sr = &out->results[i];
        yyjson_mut_val *item = yyjson_mut_obj(doc);
        /* compact: omit "name" when it equals the last segment of qualified_name. */
        if ((!compact || !ends_with_segment(sr->node.qualified_name, sr->node.name)) &&
            sr->node.name && sr->node.name[0]) {
            yyjson_mut_obj_add_str(doc, item, "name", sr->node.name);
        }
        yyjson_mut_obj_add_str(doc, item, "qualified_name",
                               sr->node.qualified_name ? sr->node.qualified_name : "");
        /* compact: omit empty "label"/"file_path" (empty == absent/default). */
        if (sr->node.label && sr->node.label[0]) {
            yyjson_mut_obj_add_str(doc, item, "label", sr->node.label);
        }
        if (sr->node.file_path && sr->node.file_path[0]) {
            yyjson_mut_obj_add_str(doc, item, "file_path", sr->node.file_path);
        }
        if (sr->pagerank_score > 0.0) {
            add_pagerank_val(doc, item, sr->pagerank_score);
        } else {
            /* Degree fields only when PageRank not available — PR subsumes degree info.
             * Zero degrees add no information; omit to save tokens (compact). */
            if (sr->in_degree > 0)
                yyjson_mut_obj_add_int(doc, item, "in_degree", sr->in_degree);
            if (sr->out_degree > 0)
                yyjson_mut_obj_add_int(doc, item, "out_degree", sr->out_degree);
        }

        /* Unconditional source tagging — critical for AI grounding.
         * Every result tagged source:"project" or source:"dependency".
         * Dep results also get package name and read_only:true. */
        bool is_dep = cbm_is_dep_project(sr->node.project, session_project);
        yyjson_mut_obj_add_str(doc, item, "source", is_dep ? "dependency" : "project");
        if (is_dep && sr->node.project) {
            const char *dep_sep = strstr(sr->node.project, CBM_DEP_SEPARATOR);
            if (dep_sep) {
                const char *pkg = dep_sep + CBM_DEP_SEPARATOR_LEN;
                yyjson_mut_obj_add_strcpy(doc, item, "package", pkg);
            }
            yyjson_mut_obj_add_bool(doc, item, "read_only", true);
        }

        if (include_connected && sr->connected_count > 0) {
            add_search_result_connected_names(doc, item, sr);
        } else if (include_connected && !connected_names_authoritative && sr->node.id > 0) {
            enrich_connected(doc, item, store, sr->node.id, relationship);
        }
        yyjson_doc *pdoc =
            pdocs ? enrich_node_properties(doc, item, sr->node.properties_json, compact, fields,
                                           field_count)
                  : NULL;
        if (pdoc && pdocs) {
            pdocs[pdoc_count++] = pdoc;
        }
        yyjson_mut_arr_add_val(results, item);
    }
    yyjson_mut_obj_add_val(doc, root, "results", results);
    if (properties_omitted_oom) {
        add_response_warning(doc, root,
                             "node properties omitted: out of memory tracking property documents");
    }
    /* Pagination: tell the caller how to get the next page */
    bool more = out->total > offset + out->count;
    yyjson_mut_obj_add_bool(doc, root, "has_more", more);
    if (more && limit > 0) {
        char hint[128];
        snprintf(hint, sizeof(hint),
                 "Use offset:%d and limit:%d for next page (%d total)",
                 offset + out->count, limit, (int)out->total);
        yyjson_mut_obj_add_strcpy(doc, root, "pagination_hint", hint);
    }
    if (fields_owner) {
        yyjson_doc_free(fields_owner);
    }
    *out_pdocs = pdocs;
    *out_pdoc_count = pdoc_count;
}

/* Extract keyword strings from a yyjson array into `keywords`.  Returns the
 * number of strings copied (capped at `max_out`). */
static int extract_semantic_keywords(yyjson_val *sq_val, const char **keywords, int max_out) {
    int kw_count = (int)yyjson_arr_size(sq_val);
    if (kw_count > max_out) {
        kw_count = max_out;
    }
    size_t kw_idx = 0;
    size_t kw_max = 0;
    yyjson_val *kw_val;
    int ki = 0;
    yyjson_arr_foreach(sq_val, kw_idx, kw_max, kw_val) {
        if (ki < kw_count && yyjson_is_str(kw_val)) {
            keywords[ki++] = yyjson_get_str(kw_val);
        }
    }
    return ki;
}

/* Emit cbm_vector_result_t entries as a "semantic_results" array on the doc. */
static void emit_semantic_results(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                  cbm_vector_result_t *vresults, int vcount) {
    yyjson_mut_val *sem_results = yyjson_mut_arr(doc);
    for (int v = 0; v < vcount; v++) {
        yyjson_mut_val *vitem = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_strcpy(doc, vitem, "name", vresults[v].name);
        yyjson_mut_obj_add_strcpy(doc, vitem, "qualified_name", vresults[v].qualified_name);
        yyjson_mut_obj_add_strcpy(doc, vitem, "label", vresults[v].label);
        yyjson_mut_obj_add_strcpy(doc, vitem, "file_path", vresults[v].file_path);
        yyjson_mut_obj_add_real(doc, vitem, "score", vresults[v].score);
        yyjson_mut_arr_add_val(sem_results, vitem);
    }
    yyjson_mut_obj_add_val(doc, root, "semantic_results", sem_results);
}

/* Run the semantic_query vector search from raw args. Sets *out_vresults /
 * *out_vcount (caller frees via cbm_store_free_vector_results when vcount>0).
 * Returns true if semantic_query was provided as a non-array (type error —
 * caller should surface to the user). */
static bool run_semantic_query_core(const char *args, cbm_store_t *store, const char *project,
                                    int limit, cbm_vector_result_t **out_vresults, int *out_vcount,
                                    bool *out_present) {
    enum { MAX_KW_SEARCH = 32 };
    *out_vresults = NULL;
    *out_vcount = 0;
    if (out_present) {
        *out_present = false;
    }
    yyjson_doc *args_doc = yyjson_read(args, strlen(args), 0);
    yyjson_val *args_root = args_doc ? yyjson_doc_get_root(args_doc) : NULL;
    yyjson_val *sq_val = args_root ? yyjson_obj_get(args_root, "semantic_query") : NULL;
    if (out_present && sq_val) {
        *out_present = true;
    }
    bool type_error = false;
    if (sq_val && !yyjson_is_arr(sq_val)) {
        type_error = true;
    } else if (sq_val && yyjson_arr_size(sq_val) > 0) {
        const char *keywords[MAX_KW_SEARCH];
        int ki = extract_semantic_keywords(sq_val, keywords, MAX_KW_SEARCH);
        cbm_vector_result_t *vresults = NULL;
        int vcount = 0;
        int sem_limit = limit > 0 ? limit : CBM_SZ_16;
        if (cbm_store_vector_search(store, project, keywords, ki, sem_limit, &vresults, &vcount) ==
                CBM_STORE_OK &&
            vcount > 0) {
            *out_vresults = vresults;
            *out_vcount = vcount;
        }
    }
    if (args_doc) {
        yyjson_doc_free(args_doc);
    }
    return type_error;
}

static char *semantic_query_type_error_response(void) {
    return cbm_mcp_text_result(
        "semantic_query must be an array of keyword strings, e.g. "
        "[\"send\",\"pubsub\",\"publish\"], not a single string. Split your query "
        "into individual keywords; each is scored independently via per-keyword min-cosine.",
        true);
}

static bool run_semantic_query(yyjson_mut_doc *doc, yyjson_mut_val *root, const char *args,
                               cbm_store_t *store, const char *project, int limit,
                               bool *out_present, int *out_count) {
    cbm_vector_result_t *vresults = NULL;
    int vcount = 0;
    bool present = false;
    bool type_error =
        run_semantic_query_core(args, store, project, limit, &vresults, &vcount, &present);
    if (out_present) {
        *out_present = present;
    }
    if (out_count) {
        *out_count = vcount;
    }
    if (present && project &&
        cbm_store_derived_view_is_stale(store, project, CBM_STORE_DERIVED_VIEW_SEMANTIC_EDGES)) {
        add_stale_derived_view_warning(
            doc, root, CBM_STORE_DERIVED_VIEW_SEMANTIC_EDGES,
            "semantic_edges derived view is stale; semantic_results may be stale.");
    }
    if (vcount > 0) {
        emit_semantic_results(doc, root, vresults, vcount);
        cbm_store_free_vector_results(vresults, vcount);
    }
    return type_error;
}

/* Compact TOON helpers retained alongside the JSON/overlay path. */

static bool sg_field_blocked(const char *field) {
    return strcmp(field, "fp") == 0 || strcmp(field, "sp") == 0 ||
           strcmp(field, "bt") == 0;
}

static bool sg_field_selected(const char *field, const char *const fields[], int field_count) {
    for (int i = 0; i < field_count; i++) {
        if (strcmp(field, fields[i]) == 0) {
            return true;
        }
    }
    return false;
}

static int sg_parse_fields(const char *args, const char *out[], int max_out,
                           yyjson_doc **out_owner) {
    *out_owner = NULL;
    yyjson_doc *args_doc = yyjson_read(args, strlen(args), 0);
    yyjson_val *args_root = args_doc ? yyjson_doc_get_root(args_doc) : NULL;
    yyjson_val *fields = args_root ? yyjson_obj_get(args_root, "fields") : NULL;
    if (!fields || !yyjson_is_arr(fields)) {
        if (args_doc) {
            yyjson_doc_free(args_doc);
        }
        return 0;
    }
    int count = 0;
    size_t index = 0;
    size_t field_count = 0;
    yyjson_val *item;
    yyjson_arr_foreach(fields, index, field_count, item) {
        const char *field = yyjson_get_str(item);
        if (field && field[0] && !sg_field_blocked(field) && count < max_out) {
            out[count++] = field;
        }
    }
    if (count == 0) {
        yyjson_doc_free(args_doc);
        return 0;
    }
    *out_owner = args_doc;
    return count;
}

static void sg_toon_extra_cells(cbm_sb_t *sb, const char *properties_json,
                                const char *const *fields, int field_count) {
    yyjson_doc *properties_doc =
        properties_json && properties_json[0]
            ? yyjson_read(properties_json, strlen(properties_json), 0)
            : NULL;
    yyjson_val *properties =
        properties_doc ? yyjson_doc_get_root(properties_doc) : NULL;
    for (int i = 0; i < field_count; i++) {
        yyjson_val *value =
            properties && yyjson_is_obj(properties) ? yyjson_obj_get(properties, fields[i]) : NULL;
        if (value && yyjson_is_str(value)) {
            cbm_toon_cell_str(sb, yyjson_get_str(value), false);
        } else if (value && yyjson_is_bool(value)) {
            cbm_toon_cell_bool(sb, yyjson_get_bool(value), false);
        } else if (value && yyjson_is_int(value)) {
            cbm_toon_cell_int(sb, yyjson_get_int(value), false);
        } else if (value && yyjson_is_real(value)) {
            cbm_toon_cell_real(sb, yyjson_get_real(value), false);
        } else {
            cbm_toon_cell_str(sb, "", false);
        }
    }
    if (properties_doc) {
        yyjson_doc_free(properties_doc);
    }
}

static void sg_lines_str(char *out, size_t out_size, int start_line, int end_line) {
    if (start_line > 0) {
        snprintf(out, out_size, "%d-%d", start_line,
                 end_line > start_line ? end_line : start_line);
    } else {
        out[0] = '\0';
    }
}

static void emit_search_results_toon(cbm_sb_t *sb, const cbm_search_output_t *out, int offset,
                                     const char *const *fields, int field_count) {
    cbm_toon_scalar_int(sb, "total", out->total);
    const char *columns[6 + SG_MAX_EXTRA_FIELDS] = {
        "qn", "label", "file", "lines", "in", "out"};
    int column_count = 6;
    for (int i = 0; i < field_count; i++) {
        columns[column_count++] = fields[i];
    }
    cbm_toon_table_header(sb, "results", out->count, columns, column_count);
    for (int i = 0; i < out->count; i++) {
        const cbm_search_result_t *result = &out->results[i];
        char lines[CBM_SZ_32];
        sg_lines_str(lines, sizeof(lines), result->node.start_line, result->node.end_line);
        cbm_toon_row_begin(sb);
        cbm_toon_cell_str(sb, result->node.qualified_name, true);
        cbm_toon_cell_str(sb, result->node.label, false);
        cbm_toon_cell_str(sb, result->node.file_path, false);
        cbm_toon_cell_str(sb, lines, false);
        cbm_toon_cell_int(sb, result->in_degree, false);
        cbm_toon_cell_int(sb, result->out_degree, false);
        sg_toon_extra_cells(sb, result->node.properties_json, fields, field_count);
        cbm_toon_row_end(sb);
    }
    cbm_toon_scalar_bool(sb, "has_more", out->total > offset + out->count);
}

static void emit_semantic_results_toon(cbm_sb_t *sb,
                                       const cbm_vector_result_t *results, int result_count) {
    static const char *const columns[] = {"qn", "label", "file", "score"};
    cbm_toon_table_header(sb, "semantic", result_count, columns, 4);
    for (int i = 0; i < result_count; i++) {
        cbm_toon_row_begin(sb);
        cbm_toon_cell_str(sb, results[i].qualified_name, true);
        cbm_toon_cell_str(sb, results[i].label, false);
        cbm_toon_cell_str(sb, results[i].file_path, false);
        cbm_toon_cell_real(sb, results[i].score, false);
        cbm_toon_row_end(sb);
    }
}

static char *append_semantic_query_to_json(const char *base_json, const char *args,
                                           cbm_store_t *store, const char *project, int limit,
                                           bool *type_error) {
    if (type_error) {
        *type_error = false;
    }
    if (!base_json) {
        return NULL;
    }
    yyjson_doc *doc = yyjson_read(base_json, strlen(base_json), 0);
    if (!doc) {
        return NULL;
    }
    yyjson_mut_doc *mdoc = yyjson_mut_doc_new(NULL);
    if (!mdoc) {
        yyjson_doc_free(doc);
        return NULL;
    }
    yyjson_mut_val *root = yyjson_val_mut_copy(mdoc, yyjson_doc_get_root(doc));
    yyjson_doc_free(doc);
    if (!root) {
        yyjson_mut_doc_free(mdoc);
        return NULL;
    }
    yyjson_mut_doc_set_root(mdoc, root);

    bool sq_type_error = run_semantic_query(mdoc, root, args, store, project, limit, NULL, NULL);
    if (sq_type_error) {
        if (type_error) {
            *type_error = true;
        }
        yyjson_mut_doc_free(mdoc);
        return NULL;
    }

    char *out = yy_doc_to_str(mdoc);
    yyjson_mut_doc_free(mdoc);
    return out;
}

char *cbm_mcp_add_dirty_file_freshness_to_json(const char *base_json, cbm_store_t *store,
                                               const char *project,
                                               const char *warning_message) {
    if (!base_json || !store || !project || !project[0]) {
        return NULL;
    }
    int pending = 0;
    int overlay_ready = 0;
    if (!get_dirty_file_counts(store, project, &pending, &overlay_ready)) {
        return NULL;
    }
    yyjson_doc *doc = yyjson_read(base_json, strlen(base_json), 0);
    if (!doc) {
        return NULL;
    }
    yyjson_mut_doc *mdoc = yyjson_mut_doc_new(NULL);
    if (!mdoc) {
        yyjson_doc_free(doc);
        return NULL;
    }
    yyjson_mut_val *root = yyjson_val_mut_copy(mdoc, yyjson_doc_get_root(doc));
    yyjson_doc_free(doc);
    if (!root || !yyjson_mut_is_obj(root)) {
        yyjson_mut_doc_free(mdoc);
        return NULL;
    }
    yyjson_mut_doc_set_root(mdoc, root);
    cbm_mcp_add_dirty_file_freshness_counts(mdoc, root, pending, overlay_ready,
                                            warning_message);
    char *out = yy_doc_to_str(mdoc);
    yyjson_mut_doc_free(mdoc);
    return out;
}

static char *add_dirty_file_freshness_to_json(const char *base_json, cbm_store_t *store,
                                              const char *project) {
    return cbm_mcp_add_dirty_file_freshness_to_json(base_json, store, project, NULL);
}

/* Convert shell-glob wildcards to POSIX ERE: bare '*' → '.*', bare '?' → '.'.
 * Keep explicit regex wildcards (.* / .?), escaped characters, character
 * classes, and quantifiers following a closed regex group/class unchanged. */
static char *glob_to_regex(const char *glob) {
    size_t len = strlen(glob);
    /* Worst case: every char expands to 2 chars plus NUL */
    char *out = malloc(len * 2 + 1);
    if (!out) return NULL;
    size_t o = 0;
    bool in_class = false;
    bool escaped = false;
    for (size_t i = 0; i < len; i++) {
        char prev = i > 0 ? glob[i - 1] : 0;
        char current = glob[i];
        if (!escaped && current == '[') {
            in_class = true;
        } else if (!escaped && current == ']' && in_class) {
            in_class = false;
        }
        bool regex_quantifier_target = prev == '.' || prev == ']' || prev == ')' || prev == '}';
        if (!escaped && !in_class && current == '*' && !regex_quantifier_target) {
            out[o++] = '.';
            out[o++] = '*';
        } else if (!escaped && !in_class && current == '?' && !regex_quantifier_target) {
            out[o++] = '.';
        } else {
            out[o++] = current;
        }
        escaped = !escaped && current == '\\';
    }
    out[o] = '\0';
    return out;
}

static bool normalize_search_pattern(char **pattern, const char *field, char *error,
                                     size_t error_size) {
    if (!pattern || !*pattern) {
        return true;
    }
    char *converted = glob_to_regex(*pattern);
    if (!converted) {
        snprintf(error, error_size, "{\"error\":\"out of memory normalizing %s\"}", field);
        return false;
    }
    cbm_regex_t re;
    if (cbm_regcomp(&re, converted, CBM_REG_EXTENDED | CBM_REG_NOSUB) != 0) {
        snprintf(error, error_size,
                 "{\"error\":\"invalid regex in %s: '%s'\","
                 "\"hint\":\"Use POSIX regex syntax or glob wildcards such as *tool*\"}",
                 field, *pattern);
        free(converted);
        return false;
    }
    cbm_regfree(&re);
    free(*pattern);
    *pattern = converted;
    return true;
}

static char *handle_search_graph(cbm_mcp_server_t *srv, const char *args) {
    cbm_mcp_output_format_t response_format = cbm_mcp_response_format(srv, args);
    if (response_format == CBM_MCP_OUTPUT_INVALID) {
        return cbm_mcp_invalid_response_format();
    }
    char *raw_project = get_store_project_arg(args);
    project_expand_t pe = {0};
    cbm_store_t *store = resolve_project_store(srv, raw_project, &pe);
    char *project = pe.value;
    REQUIRE_STORE(store, project);

    /* TOON is the compact default; JSON remains available and is required
     * when connected-neighbor objects are requested. */
    bool legacy_json = response_format == CBM_MCP_OUTPUT_JSON;
    if (cbm_mcp_get_bool_arg(args, "include_connected")) {
        legacy_json = true;
    }

    /* BM25 path: if `query` is set, run FTS5 full-text search with ranking
     * and return early.  The regex/vector path below handles all other callers.
     * If FTS5 is unavailable or the query is empty after tokenization, fall
     * through to the regex path. */
    int cfg_search_limit = cbm_config_get_int(srv->config, CBM_CONFIG_SEARCH_LIMIT,
                                               CBM_MCP_DEFAULT_SEARCH_LIMIT);
    char *query = cbm_mcp_get_string_arg(args, "query");
    if (query && query[0]) {
        int q_limit = cbm_mcp_get_positive_int_arg(args, "limit", cfg_search_limit,
                                                   CBM_MCP_DEFAULT_SEARCH_LIMIT);
        int q_offset = cbm_mcp_get_int_arg(args, "offset", 0);
        char *q_file_pattern = cbm_mcp_get_string_arg(args, "file_pattern");
        cbm_store_overlay_node_view_summary_t q_overlay_summary = {0};
        bool q_overlay_ready =
            project && project[0] &&
            cbm_store_get_overlay_node_view_summary(store, project, &q_overlay_summary) ==
                CBM_STORE_OK &&
            cbm_store_overlay_node_view_has_ready_rows(&q_overlay_summary);
        bool q_has_terms = !q_overlay_ready || bm25_query_has_terms(query);
        int q_dirty_pending = 0;
        int q_dirty_overlay_ready = 0;
        bool q_has_dirty_state =
            get_dirty_file_counts(store, project, &q_dirty_pending, &q_dirty_overlay_ready) &&
            (q_dirty_pending > 0 || q_dirty_overlay_ready > 0);
        bool q_require_json =
            legacy_json || q_overlay_ready || q_has_dirty_state ||
            cbm_mcp_has_arg(args, "semantic_query");
        char *bm25_json =
            q_overlay_ready
                ? bm25_search_overlay_active(store, project, query, q_file_pattern, q_limit,
                                             q_offset, &q_overlay_summary)
                : bm25_search(store, project, query, q_file_pattern, q_limit, q_offset,
                              !q_require_json);
        free(q_file_pattern);
        if (q_overlay_ready && q_has_terms && !bm25_json) {
            free(query);
            free(pe.value);
            return cbm_mcp_text_result(
                "{\"error\":\"search_graph query overlay read failed\","
                "\"hint\":\"Retry after a full reindex or use graph-mode filters until the "
                "overlay query path can be rebuilt.\"}",
                true);
        }
        if (bm25_json) {
            bool sq_type_error = false;
            char *composed_json =
                append_semantic_query_to_json(bm25_json, args, store, project, q_limit,
                                              &sq_type_error);
            free(query);
            if (sq_type_error) {
                free(pe.value);
                free(bm25_json);
                return semantic_query_type_error_response();
            }
            const char *payload_json = composed_json ? composed_json : bm25_json;
            char *fresh_json = add_dirty_file_freshness_to_json(payload_json, store, project);
            const char *payload_final = fresh_json ? fresh_json : payload_json;
            /* BM25 returns before the regular graph-mode response builder, so
             * append context here for both output formats. */
            char *ctx_payload =
                q_require_json
                    ? json_payload_with_context_once(payload_final, srv, store, project)
                    : toon_payload_with_context_once(payload_final, srv, store, project);
            char *result = cbm_mcp_text_result(ctx_payload ? ctx_payload : payload_final, false);
            free(ctx_payload);
            free(fresh_json);
            free(pe.value);
            free(composed_json);
            free(bm25_json);
            return result;
        }
    }
    free(query);

    char *label = cbm_mcp_get_string_arg(args, "label");
    /* F1: treat empty string as "no filter" */
    if (label && label[0] == '\0') { free(label); label = NULL; }
    char *name_pattern = cbm_mcp_get_string_arg(args, "name_pattern");
    char *qn_pattern = cbm_mcp_get_string_arg(args, "qn_pattern");
    /* Normalize glob-compatible wildcards before compiling. Waiting for regex
     * compilation to fail misses ambiguous inputs such as foo?ar and foo*|bar*,
     * which are valid POSIX EREs with different semantics. */
    char pattern_error[512];
    if (!normalize_search_pattern(&name_pattern, "name_pattern", pattern_error,
                                  sizeof(pattern_error)) ||
        !normalize_search_pattern(&qn_pattern, "qn_pattern", pattern_error,
                                  sizeof(pattern_error))) {
        free(label);
        free(name_pattern);
        free(qn_pattern);
        free(pe.value);
        return cbm_mcp_text_result(pattern_error, true);
    }
    /* NEW: unified pattern — OR search across name AND qualified_name */
    char *unified_pattern = cbm_mcp_get_string_arg(args, "pattern");
    if (!normalize_search_pattern(&unified_pattern, "pattern", pattern_error,
                                  sizeof(pattern_error))) {
        free(label);
        free(name_pattern);
        free(qn_pattern);
        free(unified_pattern);
        free(pe.value);
        return cbm_mcp_text_result(pattern_error, true);
    }
    char *file_pattern = cbm_mcp_get_string_arg(args, "file_pattern");
    char *relationship = cbm_mcp_get_string_arg(args, "relationship");
    if (relationship && !validate_edge_type(relationship)) {
        free(label); free(name_pattern); free(qn_pattern); free(unified_pattern);
        free(file_pattern); free(relationship); free(pe.value);
        return cbm_mcp_text_result(
            "{\"error\":\"invalid relationship\","
            "\"hint\":\"relationship must be uppercase letters and underscores, "
            "e.g. CALLS, DEFINES, IMPORTS (max 64 chars).\"}", true);
    }
    char *sort_by = cbm_mcp_get_string_arg(args, "sort_by");
    /* Config default: heap_strdup REQUIRED — cbm_config_get returns cfg->get_buf (internal buffer),
     * NOT a heap pointer. free(sort_by) at all exits would corrupt config's buffer without strdup. */
    if (!sort_by && srv && srv->config) {
        const char *cfg_sort = cbm_config_get(srv->config, "default_sort_by", NULL);
        if (cfg_sort && cfg_sort[0]) sort_by = heap_strdup(cfg_sort);
    }
    /* F6: validate sort_by enum — O(1) string comparisons */
    if (sort_by && strcmp(sort_by, "relevance") != 0 && strcmp(sort_by, "name") != 0 &&
        strcmp(sort_by, "degree") != 0 && strcmp(sort_by, "calls") != 0 &&
        strcmp(sort_by, "linkrank") != 0) {
        char errbuf[256];
        snprintf(errbuf, sizeof(errbuf),
            "{\"error\":\"invalid sort_by '%s'\","
            "\"hint\":\"Valid values: relevance, name, degree, calls, linkrank\"}", sort_by);
        free(label); free(name_pattern); free(qn_pattern); free(unified_pattern);
        free(file_pattern); free(relationship); free(sort_by); free(pe.value);
        return cbm_mcp_text_result(errbuf, true);
    }
    int limit = cbm_mcp_get_positive_int_arg(args, "limit", cfg_search_limit,
                                             CBM_MCP_DEFAULT_SEARCH_LIMIT);
    int offset = cbm_mcp_get_int_arg(args, "offset", 0);
    bool cfg_compact = cbm_config_get_bool(srv->config, "compact", true);
    bool compact = cbm_mcp_get_bool_arg_default(args, "compact", cfg_compact);
    char *search_mode = cbm_mcp_get_string_arg(args, "mode");
    /* summary=true alias: avoids mode enum collision with get_code (full|sig|head_tail) */
    if (!search_mode && cbm_mcp_get_bool_arg(args, "summary")) {
        search_mode = heap_strdup("summary"); /* heap_strdup: freed at mode error and normal exit */
    }
    /* F7: validate mode enum — O(1) */
    if (search_mode && strcmp(search_mode, "full") != 0 && strcmp(search_mode, "summary") != 0) {
        char errbuf[512];
        if (strcmp(search_mode, "compact") == 0 || strcmp(search_mode, "files") == 0) {
            /* mode="compact" and mode="files" are valid ONLY for source grep (search_in="source").
             * Graph mode uses a different mode enum: full | summary.
             * To use compact output with graph mode, pass compact=true (a boolean param). */
            snprintf(errbuf, sizeof(errbuf),
                "{\"error\":\"mode '%s' is only valid for the search_code tool (source grep), not search_graph\","
                "\"hint\":\"For graph mode use mode='full' or mode='summary'. "
                "To reduce token output in graph mode, pass compact=true (boolean).\"}", search_mode);
        } else {
            snprintf(errbuf, sizeof(errbuf),
                "{\"error\":\"invalid mode '%s'\","
                "\"hint\":\"Valid values for graph mode: full, summary. "
                "For source grep, use the search_code tool (modes: compact, full, files).\"}", search_mode);
        }
        free(label); free(name_pattern); free(qn_pattern); free(unified_pattern);
        free(file_pattern); free(relationship); free(sort_by); free(search_mode); free(pe.value);
        return cbm_mcp_text_result(errbuf, true);
    }
    bool case_sensitive = cbm_mcp_get_bool_arg(args, "case_sensitive");
    int min_degree = cbm_mcp_get_int_arg(args, "min_degree", -1);
    int max_degree = cbm_mcp_get_int_arg(args, "max_degree", -1);
    bool exclude_entry_points = cbm_mcp_get_bool_arg_default(args, "exclude_entry_points", false);
    bool include_connected = cbm_mcp_get_bool_arg_default(args, "include_connected", false);
    /* Default true: prefix match includes myproject.dep.* sub-projects.
     * false: forces exact match (only effective when project set + not glob mode). */
    bool cfg_inc_deps = cbm_config_get_bool(srv->config, "default_include_dependencies", true);
    bool include_dependencies = cbm_mcp_get_bool_arg_default(args, "include_dependencies", cfg_inc_deps);

    /* Summary mode needs all results for accurate aggregation */
    bool is_summary = search_mode && strcmp(search_mode, "summary") == 0;
    int effective_limit = is_summary ? 10000 : limit;

    cbm_search_params_t params = {0};
    fill_project_params(&pe, &params);
    /* include_dependencies=false: force exact match to exclude dep sub-projects.
     * Guard: only effective for MATCH_PREFIX (project set, no glob pattern).
     * MATCH_GLOB (project_pattern set) and MATCH_NONE (no project) are unaffected. */
    if (!include_dependencies && params.project && !params.project_pattern) {
        params.project_exact = true;
    }
    params.label = label;
    params.name_pattern = name_pattern;
    params.qn_pattern = qn_pattern;
    params.pattern = unified_pattern;
    params.case_sensitive = case_sensitive;
    params.file_pattern = file_pattern;
    params.relationship = relationship;
    params.sort_by = sort_by;
    params.degree_mode = srv->config
        ? cbm_config_get(srv->config, "degree_mode", NULL)
        : NULL;
    /* Dep-ranking: default false (deps rank last). Config key
     * search_disable_dep_ranking=true → pure relevance (deps may rank high). */
    params.disable_dep_ranking = srv->config
        ? cbm_config_get_bool(srv->config, CBM_CONFIG_SEARCH_DISABLE_DEP_RANKING, false)
        : false;
    params.limit = effective_limit;
    params.offset = offset;
    params.min_degree = min_degree;
    params.max_degree = max_degree;
    params.exclude_entry_points = exclude_entry_points;
    params.include_connected = include_connected;
    int exclude_count = 0;
    char **exclude = cbm_mcp_get_string_array_arg(args, "exclude", &exclude_count);
    if (exclude_count < 0) {
        free(label); free(name_pattern); free(qn_pattern); free(unified_pattern);
        free(file_pattern); free(relationship); free(sort_by); free(search_mode);
        free(pe.value);
        return cbm_mcp_text_result(
            "{\"error\":\"out of memory preparing exclude patterns\","
            "\"hint\":\"Retry with fewer exclude patterns or a smaller request.\"}", true);
    }
    params.exclude_paths = (const char **)exclude;

    if (!legacy_json) {
        const char *fields[SG_MAX_EXTRA_FIELDS];
        yyjson_doc *fields_owner = NULL;
        int nfields = sg_parse_fields(args, fields, SG_MAX_EXTRA_FIELDS, &fields_owner);

        cbm_vector_result_t *vresults = NULL;
        int vcount = 0;
        bool sq_present = false;
        bool sq_type_error =
            run_semantic_query_core(args, store, project, limit, &vresults, &vcount, &sq_present);
        if (!sq_type_error) {
            /* Semantic-only calls get semantic results only: the legacy
             * behavior also ran the UNFILTERED regex search and prepended
             * up to `limit` unrelated enriched nodes to the response. */
            bool has_filters = label || name_pattern || qn_pattern || unified_pattern ||
                               file_pattern ||
                               relationship || exclude_entry_points ||
                               min_degree != CBM_NOT_FOUND || max_degree != CBM_NOT_FOUND;
            bool semantic_only = sq_present && !has_filters;

            cbm_sb_t sb;
            cbm_sb_init(&sb);
            cbm_search_output_t tout = {0};
            if (!semantic_only) {
                cbm_store_search(store, &params, &tout);
                emit_search_results_toon(&sb, &tout, offset, fields, nfields);
                if (tout.total == 0) {
                    if (name_pattern && label) {
                        cbm_toon_scalar_str(&sb, "hint",
                                            "No results. Try removing the label filter or "
                                            "broadening the name_pattern regex.");
                    } else if (name_pattern) {
                        cbm_toon_scalar_str(
                            &sb, "hint",
                            "No nodes match this pattern. Check spelling or try a broader regex.");
                    } else if (label) {
                        cbm_toon_scalar_str(&sb, "hint",
                                            "No nodes with this label. Available labels: "
                                            "Function, Method, Class, Interface, Route, "
                                            "Variable, Module, Package, File, Folder.");
                    }
                }
            }
            if (vcount > 0) {
                emit_semantic_results_toon(&sb, vresults, vcount);
            } else if (semantic_only) {
                static const char *const sem_cols[] = {"qn", "label", "file", "score"};
                cbm_toon_table_header(&sb, "semantic", 0, sem_cols, 4);
                cbm_toon_scalar_str(&sb, "hint",
                                    "No semantic matches. semantic_query needs a moderate/full "
                                    "index; try broader or fewer keywords.");
            }
            if (vcount > 0) {
                cbm_store_free_vector_results(vresults, vcount);
            }
            if (fields_owner) {
                yyjson_doc_free(fields_owner);
            }
            cbm_store_search_free(&tout);
            free(label);
            free(name_pattern);
            free(qn_pattern);
            free(unified_pattern);
            free(file_pattern);
            free(relationship);
            free(sort_by);
            free(search_mode);
            free_string_array(exclude);
            /* One-shot _context/session_project delivery on the TOON path —
             * the early return here previously skipped inject_context_once. */
            toon_append_context_once(&sb, srv, store, project);
            free(project);
            char *text = cbm_sb_finish(&sb);
            char *result = cbm_mcp_text_result(text ? text : "out of memory", text == NULL);
            free(text);
            return result;
        }
        /* semantic_query type error: fall through to the shared error text. */
        if (fields_owner) {
            yyjson_doc_free(fields_owner);
        }
        free(project);
        free(label);
        free(name_pattern);
        free(qn_pattern);
        free(unified_pattern);
        free(file_pattern);
        free(relationship);
        free(sort_by);
        free(search_mode);
        free_string_array(exclude);
        return cbm_mcp_text_result(
            "semantic_query must be an array of keyword strings, e.g. "
            "[\"send\",\"pubsub\",\"publish\"] — not a single string. Split your query "
            "into individual keywords; each is scored independently via per-keyword "
            "min-cosine.",
            true);
    }

    /* A semantic-only request must not silently prepend an unrelated unfiltered
     * graph search. Besides misleading callers, that legacy JSON behavior added
     * an unnecessary O(graph-result-page) scan to the requested vector lookup.
     * This mirrors the compact TOON path above. */
    bool has_graph_filters = label || name_pattern || qn_pattern || unified_pattern ||
                             file_pattern || relationship || exclude_entry_points ||
                             min_degree != CBM_NOT_FOUND || max_degree != CBM_NOT_FOUND;
    bool semantic_only = cbm_mcp_has_arg(args, "semantic_query") && !has_graph_filters;

    cbm_search_output_t out = {0};
    cbm_store_overlay_node_view_summary_t overlay_summary = {0};
    bool overlay_ready_for_nodes =
        project && project[0] &&
        cbm_store_get_overlay_node_view_summary(store, project, &overlay_summary) ==
            CBM_STORE_OK &&
        cbm_store_overlay_node_view_has_ready_rows(&overlay_summary);
    bool overlay_active_edges_requested =
        relationship || include_connected || exclude_entry_points ||
        min_degree >= 0 || max_degree >= 0 ||
        (sort_by && (strcmp(sort_by, "degree") == 0 || strcmp(sort_by, "calls") == 0 ||
                     strcmp(sort_by, "linkrank") == 0));
    bool overlay_search_used = overlay_ready_for_nodes;
    if (!semantic_only) {
        if (overlay_search_used) {
            cbm_store_search_overlay_view(store, &params, &out);
        } else {
            cbm_store_search(store, &params, &out);
        }
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_doc **props_docs = NULL;
    int props_doc_count = 0;
    /* Full mode emits individual results via emit_search_results (compact-aware).
     * Summary mode skips this — it builds its own aggregates and an empty
     * results array below, so emitting here would create a duplicate "results"
     * key (yyjson keeps the first on lookup). */
    if (!is_summary) {
        emit_search_results(doc, root, &out, store, relationship, include_connected,
                            overlay_search_used && include_connected, offset, limit, compact,
                            srv->session_project, args, &props_docs, &props_doc_count);
    }

    /* Auto-context: first response gets full architecture/schema/_context header.
     * Subsequent responses just get session_project. */
    inject_context_once(doc, root, srv, store, project);
    add_derived_freshness_warnings(doc, root, out.pagerank_stale, out.linkrank_stale,
                                   out.node_degree_stale);
    add_dirty_file_freshness(doc, root, store, project);
    if (overlay_search_used) {
        add_overlay_active_node_search_freshness(doc, root, &overlay_summary,
                                                 overlay_active_edges_requested);
    }
    if (search_graph_uses_route_derived_graph(label, relationship) &&
        cbm_store_derived_view_is_stale(store, project, CBM_STORE_DERIVED_VIEW_ROUTES)) {
        add_stale_derived_view_warning(
            doc, root, CBM_STORE_DERIVED_VIEW_ROUTES,
            "routes derived view is stale; search_graph route results may be stale.");
    }

    if (is_summary) {
        /* Summary mode: aggregate counts by label and file (top 20) */
        yyjson_mut_obj_add_int(doc, root, "total", out.total);
        yyjson_mut_val *by_label = yyjson_mut_obj(doc);
        yyjson_mut_val *by_file = yyjson_mut_obj(doc);

        /* Simple aggregation — 64 slots for labels (CBM defines ~12 label types),
         * 20 slots for top files. Excess entries are silently capped. */
        const char *labels[64] = {0};
        int label_counts[64] = {0};
        int label_n = 0;
        const char *files[20] = {0};
        int file_counts[20] = {0};
        int file_n = 0;

        for (int i = 0; i < out.count; i++) {
            cbm_search_result_t *sr = &out.results[i];
            /* Count by label */
            const char *lbl = sr->node.label ? sr->node.label : "(unknown)";
            int found = -1;
            for (int j = 0; j < label_n; j++) {
                if (strcmp(labels[j], lbl) == 0) { found = j; break; }
            }
            if (found >= 0) {
                label_counts[found]++;
            } else if (label_n < 64) {
                labels[label_n] = lbl;
                label_counts[label_n] = 1;
                label_n++;
            }
            /* Count by file (top 20 only) */
            const char *fp = sr->node.file_path ? sr->node.file_path : "(unknown)";
            found = -1;
            for (int j = 0; j < file_n; j++) {
                if (strcmp(files[j], fp) == 0) { found = j; break; }
            }
            if (found >= 0) {
                file_counts[found]++;
            } else if (file_n < 20) {
                files[file_n] = fp;
                file_counts[file_n] = 1;
                file_n++;
            }
        }
        for (int i = 0; i < label_n; i++) {
            yyjson_mut_obj_add_int(doc, by_label, labels[i], label_counts[i]);
        }
        for (int i = 0; i < file_n; i++) {
            yyjson_mut_obj_add_int(doc, by_file, files[i], file_counts[i]);
        }
        yyjson_mut_obj_add_val(doc, root, "by_label", by_label);
        yyjson_mut_obj_add_val(doc, root, "by_file_top20", by_file);
        /* G1: make suppression explicit so callers know results exist */
        yyjson_mut_val *empty_arr = yyjson_mut_arr(doc);
        yyjson_mut_obj_add_val(doc, root, "results", empty_arr);
        yyjson_mut_obj_add_bool(doc, root, "results_suppressed", true);
        yyjson_mut_obj_add_str(doc, root, "hint",
            "mode='summary' returns counts only. Use mode='full' with compact=true for node records.");
    }

    /* When searching for dep projects returns nothing, explain why.
     * Heuristic: dep search if expanded value ends with ".dep" (from "dep"/"deps" shorthand)
     * or project_pattern contains ".dep." — both indicate a dependency project query. */
    if (out.total == 0) {
        bool is_dep_search = false;
        if (pe.mode == MATCH_PREFIX && pe.value) {
            size_t n = strlen(pe.value);
            is_dep_search = (n >= 4 && strcmp(pe.value + n - 4, ".dep") == 0);
        } else if (pe.mode == MATCH_GLOB && pe.value) {
            is_dep_search = (strstr(pe.value, ".dep.") != NULL ||
                             strstr(pe.value, ".dep%") != NULL);
        }
        if (is_dep_search) {
            /* Detect what build system is in use to give an actionable hint */
            cbm_pkg_manager_t eco = CBM_PKG_COUNT;
            if (srv->session_root[0])
                eco = cbm_detect_ecosystem(srv->session_root);
            char hint[1024];
            /* Self-healing: when dependency auto-indexing is switched off,
             * the missing results are policy, not absence of deps — name the
             * corrective tool instead of describing build systems. */
            int effective_dep_limit = cbm_mcp_effective_auto_dep_limit(srv, NULL);
            if (effective_dep_limit == 0) {
                snprintf(hint, sizeof(hint),
                    "No dependency sub-projects indexed: auto_index_deps is disabled "
                    "in server config, so dependency indexing never ran for this "
                    "project. Call index_dependencies(project=..., packages=[...]) to "
                    "index specific dependencies now, or enable automatic dependency "
                    "indexing with `codebase-memory-mcp config set auto_index_deps "
                    "true` and re-run index_repository.");
            } else if (eco == CBM_PKG_COUNT) {
                snprintf(hint, sizeof(hint),
                    "No dependency sub-projects indexed, and no recognized build system "
                    "detected in '%s'. Supported: Python/uv (pyproject.toml, requirements.txt), "
                    "Rust/cargo, npm/bun (package.json), Go (go.mod), JVM/Maven/Gradle, "
                    ".NET/NuGet (*.csproj), Ruby/Bundler (Gemfile), PHP/Composer, "
                    "Swift/SPM, Dart/pub, Elixir/Mix, C-Make (Makefile), C-CMake, "
                    "C-Meson, C-Conan, or generic vendor/ directory. "
                    "Re-index after adding a manifest file, or call "
                    "index_dependencies(project=..., packages=[...], source_paths=[...]) "
                    "to index dependency source directly.",
                    srv->session_root[0] ? srv->session_root : "(unknown project root)");
            } else {
                snprintf(hint, sizeof(hint),
                    "No dependency sub-projects indexed yet for %s build system '%s'. "
                    "Dep scanning runs automatically on index_repository. "
                    "If deps are vendored in vendor/ vendored/ third_party/ etc., "
                    "re-run index_repository(repo_path=\"%s\") to trigger dep discovery. "
                    "If the package you need was skipped by the auto_dep_limit cap, call "
                    "index_dependencies(project=..., packages=[...]) to index it "
                    "explicitly.",
                    cbm_pkg_manager_str(eco), cbm_pkg_manager_str(eco),
                    srv->session_root[0] ? srv->session_root : "<repo_path>");
            }
            yyjson_mut_obj_add_strcpy(doc, root, "hint", hint);
        }
    }

    /* Semantic (vector) search: append semantic_results if semantic_query
     * array was provided.  Returns true on type error (non-array value). */
    bool sq_present = false;
    int semantic_count = 0;
    bool sq_type_error =
        run_semantic_query(doc, root, args, store, project, limit, &sq_present, &semantic_count);
    if (sq_type_error) {
        for (int pi = 0; pi < props_doc_count; pi++) {
            yyjson_doc_free(props_docs[pi]);
        }
        free(props_docs);
        yyjson_mut_doc_free(doc);
        cbm_store_search_free(&out);
        free(pe.value);
        free(label); free(name_pattern); free(qn_pattern); free(unified_pattern);
        free(file_pattern); free(relationship); free(search_mode); free(sort_by);
        free_string_array(exclude);
        return semantic_query_type_error_response();
    }
    if (semantic_only && sq_present && semantic_count == 0) {
        yyjson_mut_obj_add_val(doc, root, "semantic_results", yyjson_mut_arr(doc));
        yyjson_mut_obj_add_str(doc, root, "hint",
                               "No semantic matches. semantic_query needs a moderate/full index; "
                               "try broader or fewer keywords.");
    }

    char *json = yy_doc_to_str(doc);
    /* Property docs are zero-copy referenced by the mut doc — they must
     * outlive yy_doc_to_str. Free them once serialization is complete. */
    for (int pi = 0; pi < props_doc_count; pi++) {
        yyjson_doc_free(props_docs[pi]);
    }
    free(props_docs);
    yyjson_mut_doc_free(doc);
    cbm_store_search_free(&out);

    free(pe.value);
    free(label);
    free(name_pattern);
    free(qn_pattern);
    free(unified_pattern);
    free(file_pattern);
    free(relationship);
    free(search_mode);
    free(sort_by);
    free_string_array(exclude);

    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

/* Bounded seen-set for hint accusations: dedups names across patterns,
 * UNION branches, and EXISTS predicates, and skips re-probing duplicates.
 * Capped at CBM_STORE_SCHEMA_HINT_VOCAB_LIMIT — names beyond it were never
 * going to render usefully in the message anyway. Stores AST-owned pointers
 * (label/type strings live in the parsed query), so the AST must outlive
 * every hint_seen_add call — see the ast-lifetime note at its use site. */
typedef struct {
    const char *names[CBM_STORE_SCHEMA_HINT_VOCAB_LIMIT];
    int count;
} hint_seen_t;

static bool hint_seen_add(hint_seen_t *seen, const char *name) {
    for (int i = 0; i < seen->count; i++) {
        if (strcmp(seen->names[i], name) == 0) {
            return false; /* already handled */
        }
    }
    if (seen->count < (int)(sizeof(seen->names) / sizeof(seen->names[0]))) {
        seen->names[seen->count++] = name;
    }
    return true;
}

/* Recursively walks a WHERE expression tree for EXISTS predicate edge types.
 * EXISTS { (var)-[:TYPE]->() } (op=="EXISTS") carries the type in cond.value
 * (cypher.h); NULL means "any type" and is never probed. Appends
 * comma-separated unobserved type names to `unknown`. */
static void hint_walk_expr_exists_types(const cbm_expr_t *e, cbm_store_t *store,
                                        const char *view_project, bool overlay_ready,
                                        hint_seen_t *seen, cbm_sb_t *unknown,
                                        int *unknown_count) {
    if (!e) {
        return;
    }
    if (e->type == EXPR_CONDITION) {
        if (e->cond.op && strcmp(e->cond.op, "EXISTS") == 0 && e->cond.value &&
            hint_seen_add(seen, e->cond.value) &&
            !cbm_store_schema_type_observed(store, view_project, overlay_ready, e->cond.value)) {
            cbm_sb_append(unknown, (*unknown_count)++ ? ", " : "");
            cbm_sb_append(unknown, e->cond.value);
        }
        return;
    }
    hint_walk_expr_exists_types(e->left, store, view_project, overlay_ready, seen, unknown,
                                unknown_count);
    hint_walk_expr_exists_types(e->right, store, view_project, overlay_ready, seen, unknown,
                                unknown_count);
}

static void hint_walk_where_exists_types(const cbm_where_clause_t *where, cbm_store_t *store,
                                         const char *view_project, bool overlay_ready,
                                         hint_seen_t *seen, cbm_sb_t *unknown,
                                         int *unknown_count) {
    if (!where) {
        return;
    }
    hint_walk_expr_exists_types(where->root, store, view_project, overlay_ready, seen, unknown,
                                unknown_count);
}

/* Appends up to CBM_STORE_SCHEMA_HINT_VOCAB_LIMIT observed labels then edge
 * types from a counts-only (no json_each) schema read, so an "unknown
 * label" hint also states what IS known. Never claims completeness. */
/* Shared clamp/join/"(+more)" appender for hint_append_vocab_summary's two
 * name lists (node labels, edge types) — kept as one bounded loop so wording
 * for one list can never drift from the other. */
static void hint_append_name_list(cbm_sb_t *msg, const char *title, int total,
                                  const char *(*name_at)(const cbm_schema_info_t *, int),
                                  const cbm_schema_info_t *schema) {
    if (total <= 0) {
        return;
    }
    int n = total < CBM_STORE_SCHEMA_HINT_VOCAB_LIMIT ? total : CBM_STORE_SCHEMA_HINT_VOCAB_LIMIT;
    cbm_sb_append(msg, title);
    for (int i = 0; i < n; i++) {
        cbm_sb_append(msg, i ? ", " : "");
        cbm_sb_append(msg, name_at(schema, i));
    }
    cbm_sb_append(msg, total > n ? " (+more)." : ".");
}

static const char *schema_label_at(const cbm_schema_info_t *s, int i) {
    return s->node_labels[i].label;
}

static const char *schema_type_at(const cbm_schema_info_t *s, int i) {
    return s->edge_types[i].type;
}

static void hint_append_vocab_summary(cbm_sb_t *msg, cbm_store_t *store, const char *view_project,
                                      bool overlay_ready) {
    cbm_schema_info_t schema = {0};
    int rc = overlay_ready ? cbm_store_get_schema_counts_overlay_view(store, view_project, &schema)
                           : cbm_store_get_schema_counts(store, view_project, &schema);
    if (rc != CBM_STORE_OK) {
        return;
    }
    hint_append_name_list(msg, " Known labels: ", schema.node_label_count, schema_label_at,
                          &schema);
    hint_append_name_list(msg, " Known edge types: ", schema.edge_type_count, schema_type_at,
                          &schema);
    cbm_store_schema_free(&schema);
}

/* Client/tool-neutral fallback used only when the just-executed query text
 * cannot be reparsed for the vocabulary walk below. */
static const char QUERY_GRAPH_NO_ROWS_FALLBACK_HINT[] =
    "Query returned no results. Check that referenced labels, edge types, and "
    "properties match the current project; the query_graph tool description "
    "lists the current schema.";

static void hint_walk_query_vocabulary(const cbm_query_t *ast, cbm_store_t *store,
                                       const char *view_project, bool overlay_ready,
                                       hint_seen_t *seen, cbm_sb_t *unknown, int *unknown_count) {
    for (const cbm_query_t *root = ast; root; root = root->union_next) {
        for (const cbm_query_t *q = root; q; q = q->next_stage) {
            for (int p = 0; p < q->pattern_count; p++) {
                const cbm_pattern_t *pat = &q->patterns[p];
                for (int n = 0; n < pat->node_count; n++) {
                    const char *label = pat->nodes[n].label;
                    if (label && hint_seen_add(seen, label) &&
                        !cbm_store_schema_label_observed(store, view_project, overlay_ready,
                                                         label)) {
                        cbm_sb_append(unknown, (*unknown_count)++ ? ", " : "");
                        cbm_sb_append(unknown, label);
                    }
                }
                for (int r = 0; r < pat->rel_count; r++) {
                    for (int t = 0; t < pat->rels[r].type_count; t++) {
                        const char *type = pat->rels[r].types[t];
                        if (type && hint_seen_add(seen, type) &&
                            !cbm_store_schema_type_observed(store, view_project, overlay_ready,
                                                            type)) {
                            cbm_sb_append(unknown, (*unknown_count)++ ? ", " : "");
                            cbm_sb_append(unknown, type);
                        }
                    }
                }
            }
            hint_walk_where_exists_types(q->where, store, view_project, overlay_ready, seen,
                                         unknown, unknown_count);
            hint_walk_where_exists_types(q->post_with_where, store, view_project, overlay_ready,
                                         seen, unknown, unknown_count);
        }
    }
}

/* Self-healing zero-row hint: reparses the already-executed query
 * (O(|query|); zero-row path only) and probes each referenced label/edge
 * type with an indexed existence check against the SAME view the query ran
 * on (canonical or active-overlay). Labels/types are called unobserved only
 * on a definitive miss. Property names are never asserted unknown:
 * discovery is capped (store.h) and observational, so property absence is
 * unprovable here — fail open. Returns heap text the caller frees, or NULL
 * to use the generic fallback hint above. */
static char *query_graph_no_rows_hint(cbm_store_t *store, const char *view_project,
                                      bool overlay_ready, const char *query) {
    cbm_query_t *ast = NULL;
    char *perr = NULL;
    if (cbm_cypher_parse(query, &ast, &perr) != 0 || !ast) {
        free(perr);
        return NULL; /* executed queries reparse; degrade to generic hint */
    }
    if (ast->ret && ast->ret->limit == 0) {
        cbm_query_free(ast); /* explicit LIMIT 0: zero rows by construction —
                              * a vocabulary hint would be noise */
        return NULL;
    }

    cbm_sb_t unknown;
    cbm_sb_init(&unknown);
    int unknown_count = 0;
    /* Dedups names across patterns, UNION branches, and EXISTS predicates,
     * and skips re-probing a name already resolved this call — otherwise
     * "MATCH (a:Klass) MATCH (b:Klass) RETURN a" reads "Klass, Klass." to
     * the calling model. */
    hint_seen_t seen = {0};
    hint_walk_query_vocabulary(ast, store, view_project, overlay_ready, &seen, &unknown,
                               &unknown_count);
    cbm_query_free(ast);

    cbm_sb_t msg;
    cbm_sb_init(&msg);
    if (unknown_count > 0) {
        char *names = cbm_sb_finish(&unknown);
        cbm_sb_append(&msg, "Unknown label or edge type: ");
        cbm_sb_append(&msg, names ? names : "");
        cbm_sb_append(&msg, ".");
        free(names);
        hint_append_vocab_summary(&msg, store, view_project, overlay_ready);
        cbm_sb_append(&msg,
                      " The query_graph tool description lists the current schema; "
                      "request the tool list again if it may be out of date.");
    } else {
        cbm_sb_free(&unknown);
        cbm_sb_append(&msg,
                      "Query returned no results, but the referenced labels and edge types "
                      "exist. A property name or value in a WHERE clause or other predicate "
                      "may not match any row — verify property names and values against the "
                      "schema in the query_graph tool description.");
    }
    return cbm_sb_finish(&msg);
}

static char *handle_query_graph(cbm_mcp_server_t *srv, const char *args) {
    cbm_mcp_output_format_t response_format = cbm_mcp_response_format(srv, args);
    if (response_format == CBM_MCP_OUTPUT_INVALID) {
        return cbm_mcp_invalid_response_format();
    }
    /* B7: schema says "cypher" but handler read "query" — fix to read "cypher" first */
    char *query = cbm_mcp_get_string_arg(args, "cypher");
    if (!query) query = cbm_mcp_get_string_arg(args, "query"); /* backward compat */
    /* CQ-2: use resolve_project_store for "self"/"dep"/path expansion */
    char *raw_project = get_store_project_arg(args);
    project_expand_t pe = {0};
    cbm_store_t *store = resolve_project_store(srv, raw_project, &pe);
    char *project = pe.value;
    int cfg_max_rows = cbm_config_get_int(srv->config, CBM_CONFIG_QUERY_MAX_ROWS,
                                          CBM_DEFAULT_QUERY_MAX_ROWS);
    int max_rows = cbm_mcp_has_arg(args, "max_rows") ? cbm_mcp_get_int_arg(args, "max_rows", 0)
                                                     : cfg_max_rows;
    int cfg_max_output = cbm_config_get_int(srv->config, CBM_CONFIG_QUERY_MAX_OUTPUT_BYTES,
                                            CBM_DEFAULT_QUERY_MAX_OUTPUT_BYTES);
    int max_output_bytes = cbm_mcp_get_int_arg(args, "max_output_bytes", cfg_max_output);

    /* graph="missed" (#963): run the SAME cypher against the derived
     * miss-graph view (shadow project "<project>::missed") instead of the
     * code graph — file structure of not-fully-indexed files only. */
    char *graph_arg = cbm_mcp_get_string_arg(args, "graph");
    bool missed_graph = graph_arg && strcmp(graph_arg, "missed") == 0;
    free(graph_arg);

    if (!query) {
        free(project);
        return cbm_mcp_text_result(
            "{\"error\":\"query is required\","
            "\"hint\":\"Pass a Cypher query string, e.g. MATCH (n:Function) RETURN n.name LIMIT 10\"}", true);
    }
    if (missed_graph && !project) {
        free(query);
        return cbm_mcp_text_result("project is required when graph=\"missed\"", true);
    }
    if (!store) {
        char *_err = build_project_list_error("project not found or not indexed");
        char *_res = cbm_mcp_text_result(_err, true);
        free(_err);
        free(project);
        free(query);
        return _res;
    }

    /* No verify_project_indexed here: store resolution above already reports
     * missing/unindexed projects, and the check breaks in-memory/embedded
     * stores that have no project row (removed once before — commit 5d882c55 —
     * and re-added by the upstream merge). */

    char coverage_project[CBM_SZ_512];
    const char *cypher_project = project;
    if (missed_graph) {
        cbm_store_coverage_shadow_project(coverage_project, sizeof(coverage_project), project);
        cypher_project = coverage_project;
    }

    cbm_store_overlay_node_view_summary_t overlay_summary = {0};
    bool overlay_ready =
        !missed_graph && project && mcp_overlay_view_ready(store, project, &overlay_summary);
    bool used_active_cypher_nodes = false;
    cbm_cypher_result_t result = {0};
    int rc = overlay_ready
                 ? cbm_cypher_execute_active_nodes(store, query, project, max_rows, &result,
                                                   &used_active_cypher_nodes)
                 : cbm_cypher_execute(store, query, cypher_project, max_rows, &result);

    if (rc < 0) {
        char *err_msg = result.error ? result.error : "query execution failed";
        char *resp = cbm_mcp_text_result(err_msg, true);
        cbm_cypher_result_free(&result);
        free(query);
        free(project);
        return resp;
    }

    /* Preserve freshness diagnostics in JSON whenever overlays or dirty files
     * are involved; clean canonical queries use compact TOON by default. */
    bool qg_legacy_json = response_format == CBM_MCP_OUTPUT_JSON;
    int dirty_pending = 0;
    int dirty_overlay_ready = 0;
    bool has_dirty_counts =
        get_dirty_file_counts(store, project, &dirty_pending, &dirty_overlay_ready);
    qg_legacy_json = qg_legacy_json || overlay_ready ||
                     (has_dirty_counts && (dirty_pending > 0 || dirty_overlay_ready > 0));

    char *json = NULL;
    if (!qg_legacy_json) {
        cbm_sb_t sb;
        cbm_sb_init(&sb);
        cbm_toon_table_header(&sb, "rows", result.row_count, (const char *const *)result.columns,
                              result.col_count);
        for (int r = 0; r < result.row_count; r++) {
            cbm_toon_row_begin(&sb);
            for (int c = 0; c < result.col_count; c++) {
                cbm_toon_cell_str(&sb, result.rows[r][c], c == 0);
            }
            cbm_toon_row_end(&sb);
        }
        cbm_toon_scalar_int(&sb, "total", result.row_count);
        if (result.warning) {
            cbm_toon_scalar_str(&sb, "warning", result.warning);
        }
        if (result.row_count == 0) {
            char *vocab_hint =
                query_graph_no_rows_hint(store, cypher_project, overlay_ready, query);
            cbm_toon_scalar_str(&sb, "hint",
                                vocab_hint ? vocab_hint : QUERY_GRAPH_NO_ROWS_FALLBACK_HINT);
            free(vocab_hint); /* TOON builder copies into the sb */
        }
        json = cbm_sb_finish(&sb);
    } else {
        yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
        yyjson_mut_val *root = yyjson_mut_obj(doc);
        yyjson_mut_doc_set_root(doc, root);

        /* columns */
        yyjson_mut_val *cols = yyjson_mut_arr(doc);
        for (int i = 0; i < result.col_count; i++) {
            yyjson_mut_arr_add_str(doc, cols, result.columns[i]);
        }
        yyjson_mut_obj_add_val(doc, root, "columns", cols);

        /* rows */
        yyjson_mut_val *rows = yyjson_mut_arr(doc);
        for (int r = 0; r < result.row_count; r++) {
            yyjson_mut_val *row = yyjson_mut_arr(doc);
            for (int c = 0; c < result.col_count; c++) {
                yyjson_mut_arr_add_str(doc, row, result.rows[r][c]);
            }
            yyjson_mut_arr_add_val(rows, row);
        }
        yyjson_mut_obj_add_val(doc, root, "rows", rows);
        yyjson_mut_obj_add_int(doc, root, "total", result.row_count);
        if (result.warning) {
            yyjson_mut_obj_add_str(doc, root, "warning", result.warning);
        }

        if (result.row_count == 0) {
            char *vocab_hint =
                query_graph_no_rows_hint(store, cypher_project, overlay_ready, query);
            /* add_strcpy: add_str stores the pointer without copying, and the
             * heap hint is freed before yy_doc_to_str — that would be a
             * use-after-free. */
            yyjson_mut_obj_add_strcpy(doc, root, "hint",
                                      vocab_hint ? vocab_hint : QUERY_GRAPH_NO_ROWS_FALLBACK_HINT);
            free(vocab_hint);
        }

        add_query_graph_derived_warnings(doc, root, store, project, query, &result);
        bool overlay_limitation_reported = false;
        if (used_active_cypher_nodes) {
            add_overlay_active_cypher_freshness(doc, root, &overlay_summary);
        } else if (!missed_graph) {
            overlay_limitation_reported = add_canonical_only_overlay_freshness(
                doc, root, store, project,
                "query_graph preserves canonical id() semantics for this Cypher query shape; "
                "ready overlay rows require a supported active-query shape or compaction.");
        }
        if (has_dirty_counts) {
            add_dirty_file_freshness_counts(doc, root, dirty_pending, dirty_overlay_ready);
            if (!used_active_cypher_nodes) {
                add_canonical_only_read_model(doc, root);
            }
            if (!used_active_cypher_nodes && !overlay_limitation_reported && !missed_graph) {
                add_response_warning(
                    doc, root,
                    "query_graph reads canonical graph rows; dirty file changes may be absent "
                    "until overlay or reindex completes.");
            } else if (used_active_cypher_nodes && dirty_pending > 0) {
                add_response_warning(
                    doc, root,
                    "query_graph used ready overlay node rows, but pending dirty files may still "
                    "be absent until overlay or reindex completes.");
            }
        }
        char *ignored_label = cbm_mcp_get_string_arg(args, "label");
        if (ignored_label) {
            add_response_warning(
                doc, root,
                "cypher/query is present; label, name_pattern, file_pattern, sort_by, and other "
                "search filters are ignored. Express them in the Cypher WHERE clause.");
            free(ignored_label);
        }
        json = yy_doc_to_str(doc);
        yyjson_mut_doc_free(doc);
    }
    int total_rows = result.row_count;
    cbm_cypher_result_free(&result);
    free(query);
    free(project);

    if (!json) {
        return cbm_mcp_text_result("out of memory", true);
    }
    /* Configured output cap prevents a broad query from exhausting the client. */
    if (max_output_bytes > 0) {
        size_t json_len = strlen(json);
        if (json_len > (size_t)max_output_bytes) {
            char trunc_json[CBM_SZ_256];
            snprintf(trunc_json, sizeof(trunc_json),
                     "{\"truncated\":true,\"total_bytes\":%lu,"
                     "\"rows_returned\":%d,"
                     "\"hint\":\"Narrow returned fields, add LIMIT when appropriate, or raise "
                     "max_output_bytes\"}",
                     (unsigned long)json_len, total_rows);
            char *result_text = cbm_mcp_text_result(trunc_json, false);
            free(json);
            return result_text;
        }
    }

    char *res = cbm_mcp_text_result(json, false);
    free(json);
    return res;
}

/* Indexing-coverage report (#963), attached to index_status: the best-effort
 * signal from the separate index_coverage table (coverage is metadata ABOUT
 * the graph, stored outside it). Full per-project list, capped generously. */
enum { COVERAGE_FILE_CAP = 500 };

static void add_coverage_report(yyjson_mut_doc *doc, yyjson_mut_val *root, cbm_store_t *store,
                                const char *project) {
    cbm_coverage_row_t *rows = NULL;
    int count = 0;
    (void)cbm_store_coverage_get(store, project, &rows, &count);

    yyjson_mut_val *pp_files = yyjson_mut_arr(doc);
    yyjson_mut_val *sk_files = yyjson_mut_arr(doc);
    yyjson_mut_val *ni_dirs = yyjson_mut_arr(doc);
    yyjson_mut_val *ni_files = yyjson_mut_arr(doc);
    int pp_n = 0;
    int sk_n = 0;
    int ni_dir_n = 0;
    int ni_file_n = 0;
    for (int i = 0; i < count; i++) {
        const char *kind = rows[i].kind ? rows[i].kind : "";
        if (strcmp(kind, "parse_partial") == 0) {
            if (pp_n < COVERAGE_FILE_CAP) {
                yyjson_mut_val *fe = yyjson_mut_obj(doc);
                yyjson_mut_obj_add_strcpy(doc, fe, "path", rows[i].rel_path);
                yyjson_mut_obj_add_strcpy(doc, fe, "error_ranges",
                                          rows[i].detail ? rows[i].detail : "");
                yyjson_mut_arr_add_val(pp_files, fe);
            }
            pp_n++;
        } else if (strcmp(kind, "not_indexed_dir") == 0) {
            if (ni_dir_n < COVERAGE_FILE_CAP) {
                yyjson_mut_arr_add_strcpy(doc, ni_dirs, rows[i].rel_path);
            }
            ni_dir_n++;
        } else if (strcmp(kind, "not_indexed_file") == 0) {
            if (ni_file_n < COVERAGE_FILE_CAP) {
                yyjson_mut_val *fe = yyjson_mut_obj(doc);
                yyjson_mut_obj_add_strcpy(doc, fe, "path", rows[i].rel_path);
                yyjson_mut_obj_add_strcpy(doc, fe, "reason", rows[i].detail ? rows[i].detail : "");
                yyjson_mut_arr_add_val(ni_files, fe);
            }
            ni_file_n++;
        } else {
            if (sk_n < COVERAGE_FILE_CAP) {
                yyjson_mut_val *fe = yyjson_mut_obj(doc);
                yyjson_mut_obj_add_strcpy(doc, fe, "path", rows[i].rel_path);
                yyjson_mut_obj_add_strcpy(doc, fe, "reason", rows[i].detail ? rows[i].detail : "");
                yyjson_mut_obj_add_strcpy(doc, fe, "phase", rows[i].kind ? rows[i].kind : "");
                yyjson_mut_arr_add_val(sk_files, fe);
            }
            sk_n++;
        }
    }
    cbm_store_free_coverage(rows, count);

    yyjson_mut_val *pp = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_val(doc, pp, "files", pp_files);
    yyjson_mut_obj_add_int(doc, pp, "count", pp_n);
    yyjson_mut_obj_add_bool(doc, pp, "truncated", pp_n > COVERAGE_FILE_CAP);
    yyjson_mut_obj_add_val(doc, root, "parse_partial", pp);

    yyjson_mut_val *sk = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_val(doc, sk, "files", sk_files);
    yyjson_mut_obj_add_int(doc, sk, "count", sk_n);
    yyjson_mut_obj_add_bool(doc, sk, "truncated", sk_n > COVERAGE_FILE_CAP);
    yyjson_mut_obj_add_val(doc, root, "skipped", sk);

    /* By-design exclusions (#963 "purposely not indexed"): a deliberate,
     * deterministic class — NOT a failure and NOT best-effort. Dirs are
     * exhaustive; per-file entries are capped in discovery (2000) with the
     * truncation explicit. */
    yyjson_mut_val *ni = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_val(doc, ni, "dirs", ni_dirs);
    yyjson_mut_obj_add_int(doc, ni, "dirs_count", ni_dir_n);
    yyjson_mut_obj_add_val(doc, ni, "files", ni_files);
    yyjson_mut_obj_add_int(doc, ni, "files_count", ni_file_n);
    yyjson_mut_obj_add_bool(doc, ni, "truncated",
                            ni_dir_n > COVERAGE_FILE_CAP || ni_file_n > COVERAGE_FILE_CAP);
    if (ni_dir_n > 0 || ni_file_n > 0) {
        yyjson_mut_obj_add_str(doc, ni, "note",
                               "Purposely not indexed — excluded BY DESIGN via "
                               "gitignore/.cbmignore/skip-lists (see each file's reason). Not an "
                               "error: change the ignore rules and re-index to include them.");
    }
    yyjson_mut_obj_add_val(doc, root, "not_indexed", ni);

    if (pp_n > 0 || sk_n > 0) {
        yyjson_mut_obj_add_str(
            doc, root, "coverage_note",
            "Best-effort signal, not a completeness guarantee: parse_partial files WERE indexed, "
            "but constructs inside the listed line ranges (1-based) MAY be missing from the graph "
            "(tree-sitter error recovery still salvages some). skipped files were not indexed at "
            "all. Prefer text search (grep) for flagged files/ranges. Files absent from this list "
            "are NOT guaranteed to be fully indexed. (not_indexed entries are a separate, "
            "BY-DESIGN class — deliberate ignore rules, not failures.)");
    }
}

enum {
    COVERAGE_PATH_MAX = 128,
    COVERAGE_SCOPE_MAX = 32,
    COVERAGE_SCOPE_DEFAULT_LIMIT = 200,
    COVERAGE_SCOPE_MAX_LIMIT = 1000,
    COVERAGE_RANGE_MAX = 128,
};

bool cbm_path_within_root(const char *root_path, const char *abs_path); /* defined below */

typedef enum {
    COVERAGE_PATH_OK = 0,
    COVERAGE_PATH_OUTSIDE,
    COVERAGE_PATH_INVALID,
} coverage_path_result_t;

/* Normalize an untrusted repository-relative path without touching the
 * filesystem. Absolute paths, drive/UNC paths, control bytes, and any `..`
 * component are rejected. A root scope (`.`) normalizes to the empty prefix. */
static coverage_path_result_t coverage_normalize_rel(const char *input, bool allow_root, char *out,
                                                     size_t out_size) {
    if (!input || !out || out_size == 0U) {
        return COVERAGE_PATH_INVALID;
    }
    out[0] = '\0';
    size_t len = strlen(input);
    if (len == 0U || len >= out_size || input[0] == '/' || input[0] == '\\' ||
        (len >= 2U && isalpha((unsigned char)input[0]) && input[1] == ':')) {
        return COVERAGE_PATH_OUTSIDE;
    }

    size_t in = 0U;
    size_t written = 0U;
    while (in < len) {
        while (in < len && (input[in] == '/' || input[in] == '\\')) {
            in++;
        }
        if (in >= len) {
            break;
        }
        size_t start = in;
        while (in < len && input[in] != '/' && input[in] != '\\') {
            unsigned char c = (unsigned char)input[in];
            if (c < 0x20U) {
                return COVERAGE_PATH_INVALID;
            }
            in++;
        }
        size_t part_len = in - start;
        if (part_len == 1U && input[start] == '.') {
            continue;
        }
        if (part_len == 2U && input[start] == '.' && input[start + 1U] == '.') {
            return COVERAGE_PATH_OUTSIDE;
        }
        if (written > 0U) {
            if (written + 1U >= out_size) {
                return COVERAGE_PATH_INVALID;
            }
            out[written++] = '/';
        }
        if (written + part_len >= out_size) {
            return COVERAGE_PATH_INVALID;
        }
        memcpy(out + written, input + start, part_len);
        written += part_len;
    }
    out[written] = '\0';
    return written > 0U || allow_root ? COVERAGE_PATH_OK : COVERAGE_PATH_INVALID;
}

static int64_t coverage_stat_mtime_ns(const struct stat *st) {
#ifdef __APPLE__
    return ((int64_t)st->st_mtimespec.tv_sec * (int64_t)CBM_NSEC_PER_SEC) +
           (int64_t)st->st_mtimespec.tv_nsec;
#elif defined(_WIN32)
    return (int64_t)st->st_mtime * (int64_t)CBM_NSEC_PER_SEC;
#else
    return ((int64_t)st->st_mtim.tv_sec * (int64_t)CBM_NSEC_PER_SEC) + (int64_t)st->st_mtim.tv_nsec;
#endif
}

static const char *coverage_path_freshness(cbm_store_t *store, const char *project,
                                           const char *root_path, const char *rel_path,
                                           bool *outside) {
    *outside = false;
    if (!root_path || !root_path[0]) {
        return "unavailable";
    }
    char abs_path[CBM_SZ_4K];
    int n = snprintf(abs_path, sizeof(abs_path), "%s%s%s", root_path,
                     root_path[strlen(root_path) - 1U] == '/' ? "" : "/", rel_path);
    if (n < 0 || (size_t)n >= sizeof(abs_path)) {
        return "unavailable";
    }
    struct stat st;
    if (stat(abs_path, &st) != 0) {
        return "missing";
    }
    if (!cbm_path_within_root(root_path, abs_path)) {
        *outside = true;
        return "outside_project";
    }

    cbm_file_hash_t hash = {0};
    int rc = cbm_store_get_file_hash(store, project, rel_path, &hash);
    if (rc == CBM_STORE_NOT_FOUND) {
        return "not_tracked";
    }
    if (rc != CBM_STORE_OK) {
        return "unavailable";
    }
    bool matches = hash.mtime_ns == coverage_stat_mtime_ns(&st) && hash.size == st.st_size;
    cbm_store_clear_file_hash(&hash);
    return matches ? "metadata_match" : "metadata_changed";
}

static void coverage_add_ranges(yyjson_mut_doc *doc, yyjson_mut_val *row, const char *detail) {
    if (!detail || !detail[0]) {
        return;
    }
    yyjson_mut_val *ranges = yyjson_mut_arr(doc);
    const char *p = detail;
    int emitted = 0;
    while (*p && emitted < COVERAGE_RANGE_MAX) {
        while (*p == ' ' || *p == ',') {
            p++;
        }
        if (!isdigit((unsigned char)*p)) {
            break;
        }
        char *endptr = NULL;
        long start = strtol(p, &endptr, 10);
        if (endptr == p || start <= 0 || start > INT32_MAX) {
            break;
        }
        p = endptr;
        long end = start;
        if (*p == '-') {
            p++;
            long parsed = strtol(p, &endptr, 10);
            if (endptr == p || parsed < start || parsed > INT32_MAX) {
                break;
            }
            end = parsed;
            p = endptr;
        }
        yyjson_mut_val *range = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_int(doc, range, "start", start);
        yyjson_mut_obj_add_int(doc, range, "end", end);
        yyjson_mut_arr_add_val(ranges, range);
        emitted++;
        while (*p == ' ') {
            p++;
        }
        if (*p && *p != ',') {
            break;
        }
    }
    if (emitted > 0) {
        yyjson_mut_obj_add_val(doc, row, "ranges", ranges);
    }
}

static void coverage_add_row_json(yyjson_mut_doc *doc, yyjson_mut_val *array,
                                  const cbm_coverage_row_t *row, const char *requested_path) {
    yyjson_mut_val *item = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_strcpy(doc, item, "path", row->rel_path ? row->rel_path : "");
    yyjson_mut_obj_add_strcpy(doc, item, "kind", row->kind ? row->kind : "");
    yyjson_mut_obj_add_strcpy(doc, item, "detail", row->detail ? row->detail : "");
    if (requested_path) {
        yyjson_mut_obj_add_str(
            doc, item, "match",
            row->rel_path && strcmp(row->rel_path, requested_path) == 0 ? "exact" : "ancestor");
    }
    if (row->kind && strcmp(row->kind, "parse_partial") == 0) {
        coverage_add_ranges(doc, item, row->detail);
    }
    yyjson_mut_arr_add_val(array, item);
}

static const char *coverage_status(const cbm_coverage_row_t *rows, int count,
                                   const char *requested_path, const char *recording_status,
                                   bool generation_matches, bool lookup_ok) {
    if (!lookup_ok) {
        return "coverage_unavailable";
    }
    bool exact = false;
    for (int i = 0; i < count; i++) {
        if (rows[i].rel_path && strcmp(rows[i].rel_path, requested_path) == 0) {
            exact = true;
            break;
        }
    }
    for (int pass = 0; pass < 3; pass++) {
        for (int i = 0; i < count; i++) {
            if (exact && (!rows[i].rel_path || strcmp(rows[i].rel_path, requested_path) != 0)) {
                continue;
            }
            const char *kind = rows[i].kind ? rows[i].kind : "";
            if (pass == 0 && strcmp(kind, "parse_partial") == 0) {
                return "partial";
            }
            if (pass == 1 && strncmp(kind, "not_indexed", 11) == 0) {
                return "excluded";
            }
            if (pass == 2 && kind[0]) {
                return "skipped";
            }
        }
    }
    if (!generation_matches || !recording_status || strcmp(recording_status, "complete") != 0) {
        return "coverage_unavailable";
    }
    return "no_recorded_issue";
}

static const char *coverage_recommended_action(const char *status, const char *freshness) {
    if (!freshness || strcmp(freshness, "metadata_match") != 0) {
        return "read_source_and_reindex";
    }
    if (strcmp(status, "partial") == 0) {
        return "read_ranges_and_verify_scope";
    }
    if (strcmp(status, "skipped") == 0) {
        return "read_source_directly";
    }
    if (strcmp(status, "excluded") == 0) {
        return "read_source_or_change_ignore_rules";
    }
    if (strcmp(status, "no_recorded_issue") == 0) {
        return "use_graph_with_best_effort_caveat";
    }
    return "read_source_and_reindex";
}

static char *handle_check_index_coverage(cbm_mcp_server_t *srv, const char *args) {
    char *project = get_project_arg(args);
    cbm_store_t *store = resolve_store(srv, project);
    REQUIRE_STORE(store, project);

    yyjson_doc *adoc = yyjson_read(args, strlen(args), 0);
    yyjson_val *aroot = adoc ? yyjson_doc_get_root(adoc) : NULL;
    yyjson_val *paths = aroot ? yyjson_obj_get(aroot, "paths") : NULL;
    yyjson_val *scopes = aroot ? yyjson_obj_get(aroot, "scopes") : NULL;
    size_t path_count = paths && yyjson_is_arr(paths) ? yyjson_arr_size(paths) : 0U;
    size_t scope_count = scopes && yyjson_is_arr(scopes) ? yyjson_arr_size(scopes) : 0U;
    if (!aroot || (paths && !yyjson_is_arr(paths)) || (scopes && !yyjson_is_arr(scopes)) ||
        (path_count == 0U && scope_count == 0U) || path_count > COVERAGE_PATH_MAX ||
        scope_count > COVERAGE_SCOPE_MAX) {
        if (adoc) {
            yyjson_doc_free(adoc);
        }
        free(project);
        return cbm_mcp_text_result(
            "paths or scopes is required (arrays; max 128 paths and 32 scopes)", true);
    }

    cbm_project_t proj = {0};
    bool have_project = cbm_store_get_project(store, project, &proj) == CBM_STORE_OK;
    cbm_coverage_meta_t meta = {0};
    bool have_meta = cbm_store_coverage_meta_get(store, project, &meta) == CBM_STORE_OK;
    bool generation_matches = have_project && have_meta && proj.indexed_at && meta.generation &&
                              strcmp(proj.indexed_at, meta.generation) == 0;
    const char *recording_status =
        have_meta && meta.recording_status ? meta.recording_status : "unknown";

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_strcpy(doc, root, "project", project);
    yyjson_mut_obj_add_str(doc, root, "signal", "best_effort");
    yyjson_mut_obj_add_strcpy(doc, root, "indexed_at",
                              have_project && proj.indexed_at ? proj.indexed_at : "");

    yyjson_mut_val *meta_obj = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_strcpy(doc, meta_obj, "generation",
                              have_meta && meta.generation ? meta.generation : "");
    yyjson_mut_obj_add_strcpy(doc, meta_obj, "index_mode",
                              have_meta && meta.index_mode ? meta.index_mode : "unknown");
    yyjson_mut_obj_add_strcpy(doc, meta_obj, "recorded_at",
                              have_meta && meta.recorded_at ? meta.recorded_at : "");
    yyjson_mut_obj_add_strcpy(doc, meta_obj, "recording_status", recording_status);
    yyjson_mut_obj_add_int(doc, meta_obj, "ignored_files_stored",
                           have_meta ? meta.ignored_files_stored : 0);
    yyjson_mut_obj_add_int(doc, meta_obj, "ignored_files_total",
                           have_meta ? meta.ignored_files_total : 0);
    yyjson_mut_obj_add_bool(doc, meta_obj, "hash_records_complete",
                            have_meta && meta.hash_records_complete);
    yyjson_mut_obj_add_int(doc, meta_obj, "coverage_version",
                           have_meta ? meta.coverage_version : 0);
    yyjson_mut_obj_add_bool(doc, meta_obj, "generation_matches", generation_matches);
    yyjson_mut_obj_add_val(doc, root, "metadata", meta_obj);

    yyjson_mut_val *path_results = yyjson_mut_arr(doc);
    size_t idx;
    size_t max;
    yyjson_val *value;
    if (paths) {
        yyjson_arr_foreach(paths, idx, max, value) {
            yyjson_mut_val *item = yyjson_mut_obj(doc);
            const char *input = yyjson_is_str(value) ? yyjson_get_str(value) : NULL;
            yyjson_mut_obj_add_strcpy(doc, item, "requested_path", input ? input : "");
            char rel[CBM_SZ_4K];
            coverage_path_result_t normalized =
                coverage_normalize_rel(input, false, rel, sizeof(rel));
            if (normalized != COVERAGE_PATH_OK) {
                yyjson_mut_obj_add_str(doc, item, "status",
                                       normalized == COVERAGE_PATH_OUTSIDE ? "outside_project"
                                                                           : "invalid_path");
                yyjson_mut_obj_add_str(doc, item, "freshness", "unavailable");
                yyjson_mut_obj_add_str(doc, item, "recommended_action",
                                       "use_project_relative_path");
                yyjson_mut_arr_add_val(path_results, item);
                continue;
            }
            yyjson_mut_obj_add_strcpy(doc, item, "path", rel);
            cbm_coverage_row_t *rows = NULL;
            int row_count = 0;
            int cov_rc = cbm_store_coverage_get_path(store, project, rel, &rows, &row_count);
            bool lookup_ok = cov_rc == CBM_STORE_OK || cov_rc == CBM_STORE_NOT_FOUND;
            if (!lookup_ok) {
                row_count = 0;
                yyjson_mut_obj_add_str(doc, item, "coverage_lookup", "error");
            }
            bool outside = false;
            const char *freshness = coverage_path_freshness(
                store, project, have_project ? proj.root_path : NULL, rel, &outside);
            const char *status = outside ? "outside_project"
                                         : coverage_status(rows, row_count, rel, recording_status,
                                                           generation_matches, lookup_ok);
            yyjson_mut_obj_add_strcpy(doc, item, "status", status);
            yyjson_mut_obj_add_strcpy(doc, item, "freshness", freshness);
            yyjson_mut_obj_add_strcpy(doc, item, "recommended_action",
                                      coverage_recommended_action(status, freshness));
            yyjson_mut_val *coverage = yyjson_mut_arr(doc);
            for (int i = 0; i < row_count; i++) {
                coverage_add_row_json(doc, coverage, &rows[i], rel);
            }
            yyjson_mut_obj_add_val(doc, item, "coverage", coverage);
            cbm_store_free_coverage(rows, row_count);
            yyjson_mut_arr_add_val(path_results, item);
        }
    }
    yyjson_mut_obj_add_val(doc, root, "paths", path_results);

    int scope_limit = cbm_mcp_get_int_arg(args, "scope_limit", COVERAGE_SCOPE_DEFAULT_LIMIT);
    int scope_offset = cbm_mcp_get_int_arg(args, "scope_offset", 0);
    if (scope_limit < 1) {
        scope_limit = 1;
    } else if (scope_limit > COVERAGE_SCOPE_MAX_LIMIT) {
        scope_limit = COVERAGE_SCOPE_MAX_LIMIT;
    }
    if (scope_offset < 0) {
        scope_offset = 0;
    }
    yyjson_mut_val *scope_results = yyjson_mut_arr(doc);
    if (scopes) {
        yyjson_arr_foreach(scopes, idx, max, value) {
            yyjson_mut_val *item = yyjson_mut_obj(doc);
            const char *input = yyjson_is_str(value) ? yyjson_get_str(value) : NULL;
            yyjson_mut_obj_add_strcpy(doc, item, "requested_scope", input ? input : "");
            char scope[CBM_SZ_4K];
            coverage_path_result_t normalized =
                coverage_normalize_rel(input, true, scope, sizeof(scope));
            if (normalized != COVERAGE_PATH_OK) {
                yyjson_mut_obj_add_str(doc, item, "status",
                                       normalized == COVERAGE_PATH_OUTSIDE ? "outside_project"
                                                                           : "invalid_path");
                yyjson_mut_arr_add_val(scope_results, item);
                continue;
            }
            yyjson_mut_obj_add_str(doc, item, "scope", scope[0] ? scope : ".");
            cbm_coverage_row_t *rows = NULL;
            int row_count = 0;
            int cov_rc = cbm_store_coverage_get_scope(store, project, scope, &rows, &row_count);
            bool lookup_ok = cov_rc == CBM_STORE_OK || cov_rc == CBM_STORE_NOT_FOUND;
            if (!lookup_ok) {
                row_count = 0;
                yyjson_mut_obj_add_str(doc, item, "coverage_lookup", "error");
            }
            yyjson_mut_obj_add_int(doc, item, "total", row_count);
            int start = scope_offset < row_count ? scope_offset : row_count;
            int end = start + scope_limit < row_count ? start + scope_limit : row_count;
            yyjson_mut_obj_add_bool(doc, item, "has_more", end < row_count);
            if (end < row_count) {
                yyjson_mut_obj_add_int(doc, item, "next_offset", end);
            }
            yyjson_mut_val *entries = yyjson_mut_arr(doc);
            for (int i = start; i < end; i++) {
                coverage_add_row_json(doc, entries, &rows[i], NULL);
            }
            yyjson_mut_obj_add_val(doc, item, "entries", entries);
            const char *scope_status = !lookup_ok || !generation_matches ? "coverage_unavailable"
                                       : row_count > 0                   ? "known_gaps"
                                       : strcmp(recording_status, "complete") == 0
                                           ? "no_recorded_issue"
                                           : "coverage_unavailable";
            yyjson_mut_obj_add_str(doc, item, "status", scope_status);
            cbm_store_free_coverage(rows, row_count);
            yyjson_mut_arr_add_val(scope_results, item);
        }
    }
    yyjson_mut_obj_add_val(doc, root, "scopes", scope_results);
    yyjson_mut_obj_add_str(
        doc, root, "caveat",
        "Best-effort signal only. No recorded issue does not prove graph or source completeness; "
        "read flagged source and qualify claims when metadata is changed or unavailable.");

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    yyjson_doc_free(adoc);
    if (have_meta) {
        cbm_store_coverage_meta_clear(&meta);
    }
    if (have_project) {
        safe_str_free(&proj.name);
        safe_str_free(&proj.indexed_at);
        safe_str_free(&proj.root_path);
    }
    free(project);
    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

static char *handle_index_status(cbm_mcp_server_t *srv, const char *args) {
    char *raw_project = get_store_project_arg(args);
    project_expand_t pe = {0};
    cbm_store_t *store = resolve_project_store(srv, raw_project, &pe);
    char *project = pe.value;
    REQUIRE_STORE(store, project);
    bool verbose = cbm_mcp_get_bool_arg(args, "verbose");

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    if (srv->session_project[0])
        yyjson_mut_obj_add_str(doc, root, "session_project", srv->session_project);
    add_overlay_compaction_worker_status(srv, doc, root);

    if (project) {
        int nodes = cbm_store_count_nodes(store, project);
        int edges = cbm_store_count_edges(store, project);
        yyjson_mut_obj_add_str(doc, root, "project", project);
        yyjson_mut_obj_add_int(doc, root, "nodes", nodes);
        yyjson_mut_obj_add_int(doc, root, "edges", edges);
        yyjson_mut_obj_add_str(doc, root, "status", nodes > 0 ? "ready" : "empty");

        /* Report indexed dependencies by searching for {project}.dep.% nodes.
         * Uses project_pattern for LIKE query to find all dep projects. */
        char dep_like[4096];
        snprintf(dep_like, sizeof(dep_like), "%s.dep.%%", project);
        cbm_search_params_t dep_params = {0};
        dep_params.project_pattern = dep_like;
        dep_params.limit = 100;
        cbm_search_output_t dep_out = {0};
        if (cbm_store_search(store, &dep_params, &dep_out) == 0) {
            /* Collect unique dep project names */
            if (dep_out.count > 0) {
                yyjson_mut_val *dep_arr = yyjson_mut_arr(doc);
                const char *last_dep_proj = "";
                int dep_count = 0;
                for (int i = 0; i < dep_out.count; i++) {
                    const char *proj = dep_out.results[i].node.project;
                    if (!proj || strcmp(proj, last_dep_proj) == 0) continue;
                    last_dep_proj = proj;
                    /* Extract package name from "myproj.dep.pandas" */
                    const char *dep_sep = strstr(proj, CBM_DEP_SEPARATOR);
                    if (!dep_sep) continue;
                    const char *pkg = dep_sep + CBM_DEP_SEPARATOR_LEN;
                    yyjson_mut_val *d = yyjson_mut_obj(doc);
                    yyjson_mut_obj_add_strcpy(doc, d, "package", pkg);
                    int dn = cbm_store_count_nodes(store, proj);
                    yyjson_mut_obj_add_int(doc, d, "nodes", dn);
                    yyjson_mut_arr_add_val(dep_arr, d);
                    dep_count++;
                }
                if (dep_count > 0) {
                    yyjson_mut_obj_add_val(doc, root, "dependencies", dep_arr);
                    yyjson_mut_obj_add_int(doc, root, "dependency_count", dep_count);
                }
            }
            /* Always free search results — cbm_store_search allocates even when count==0 */
            cbm_store_search_free(&dep_out);
        }

        /* Report detected ecosystem + root_path + git metadata */
        cbm_project_t proj_info;
        if (cbm_store_get_project(store, project, &proj_info) == 0) {
            if (proj_info.root_path) {
                /* root_path + git context — capture before free (fields are heap-alloc'd) */
                yyjson_mut_obj_add_strcpy(doc, root, "root_path", proj_info.root_path);
                if (verbose) {
                    add_git_context_json(doc, root, proj_info.root_path);
                }
                cbm_pkg_manager_t eco = cbm_detect_ecosystem(proj_info.root_path);
                if (eco != CBM_PKG_COUNT) {
                    yyjson_mut_obj_add_str(doc, root, "detected_ecosystem",
                                           cbm_pkg_manager_str(eco));
                }
            }
            /* Always free project fields — cbm_store_get_project heap-allocates strings */
            cbm_project_free_fields(&proj_info);
        add_coverage_report(doc, root, store, project);
        if (nodes == 0) {
            yyjson_mut_obj_add_str(
                doc, root, "hint",
                "Project is empty. Re-run index_repository(repo_path=...) to populate.");
        }
        }
        /* Report PageRank stats */
        {
            sqlite3 *db = cbm_store_get_db(store);
            if (db) {
                sqlite3_stmt *pr_stmt = NULL;
                const char *pr_sql = "SELECT COUNT(*), MAX(computed_at) "
                                     "FROM pagerank WHERE project = ?1";
                if (sqlite3_prepare_v2(db, pr_sql, -1, &pr_stmt, NULL) == SQLITE_OK) {
                    sqlite3_bind_text(pr_stmt, 1, project, -1, SQLITE_TRANSIENT);
                    if (sqlite3_step(pr_stmt) == SQLITE_ROW) {
                        int ranked = sqlite3_column_int(pr_stmt, 0);
                        if (ranked > 0) {
                            yyjson_mut_val *pr_obj = yyjson_mut_obj(doc);
                            yyjson_mut_obj_add_int(doc, pr_obj, "ranked_nodes", ranked);
                            const char *ts = (const char *)sqlite3_column_text(pr_stmt, 1);
                            if (ts)
                                yyjson_mut_obj_add_strcpy(doc, pr_obj, "computed_at", ts);
                            yyjson_mut_obj_add_val(doc, root, "pagerank", pr_obj);
                        }
                    }
                    sqlite3_finalize(pr_stmt);
                }
            }
        }

        int dirty_pending = 0;
        int dirty_overlay_ready = 0;
        if (get_dirty_file_counts(store, project, &dirty_pending, &dirty_overlay_ready)) {
            add_dirty_file_freshness_counts(doc, root, dirty_pending, dirty_overlay_ready);
            add_response_warning(doc, root,
                                 "index_status counts canonical graph rows; dirty file changes "
                                 "may be absent until overlay or reindex completes.");
        }
        add_overlay_node_read_view_summary(
            doc, root, store, project,
            "index_status includes overlay_read_view counts, but nodes/edges are canonical counts "
            "while overlay-aware tools may read active overlay rows.");
    } else {
        yyjson_mut_obj_add_str(doc, root, "status", "no_project");
    }

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    free(project);

    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

/* delete_project: just erase the .db file (and WAL/SHM). */
static char *handle_delete_project(cbm_mcp_server_t *srv, const char *args) {
    char *name = get_project_arg(args);
    if (!name) {
        return cbm_mcp_text_result(
            "{\"error\":\"project_name is required\","
            "\"hint\":\"Pass the project name to delete. Use list_projects to see available projects.\"}", true);
    }

    /* Close store if it's the project being deleted */
    if (srv->current_project && strcmp(srv->current_project, name) == 0) {
        if (srv->owns_store && srv->store) {
            cbm_store_close(srv->store);
            srv->store = NULL;
        }
        free(srv->current_project);
        srv->current_project = NULL;
    }

    /* Wait for any in-progress pipeline to finish before deleting */
    cbm_pipeline_lock();

    /* Delete the .db file + WAL/SHM */
    char path[CBM_SZ_1K];
    project_db_path(name, path, sizeof(path));
    if (!path[0]) {
        cbm_pipeline_unlock();
        free(name);
        return cbm_mcp_text_result("{\"status\":\"delete_failed\",\"error\":\"project path too long\"}",
                                   true);
    }

    char wal[CBM_SZ_1K];
    char shm[CBM_SZ_1K];
    int wal_len = snprintf(wal, sizeof(wal), "%s-wal", path);
    int shm_len = snprintf(shm, sizeof(shm), "%s-shm", path);

    bool exists = cbm_file_exists(path);
    const char *status = "not_found";
    const char *error_detail = NULL;
    bool is_error = false;

    if (exists) {
        int rc = cbm_unlink(path);
        if (wal_len > 0 && (size_t)wal_len < sizeof(wal)) {
            (void)cbm_unlink(wal);
        }
        if (shm_len > 0 && (size_t)shm_len < sizeof(shm)) {
            (void)cbm_unlink(shm);
        }
        if (rc == 0) {
            status = "deleted";
        } else {
            status = "delete_failed";
            error_detail = strerror(errno);
            is_error = true;
        }
    } else {
        is_error = true;
    }

    cbm_pipeline_unlock();

    if (srv->watcher) {
        cbm_watcher_unwatch(srv->watcher, name);
    }
    if (!is_error) {
        /* The graph for `name` is gone (store closed / current_project freed
         * above when it was the active project). A cached tools/list
         * description could still advertise its schema. Never fires on the
         * no-op/error path: nothing changed, so staling the cache would buy
         * a full O(V+E+P) rediscovery for free. */
        cbm_mcp_server_notify_index_published(srv);
    }

    cbm_mem_collect(); /* return freed pages to OS after closing database */

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_str(doc, root, "project", name);
    yyjson_mut_obj_add_str(doc, root, "status", status);
    if (error_detail) {
        yyjson_mut_obj_add_str(doc, root, "error", error_detail);
    }

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    free(name);

    char *result = cbm_mcp_text_result(json, is_error);
    free(json);
    return result;
}

/* Canonical list of valid aspect tokens for get_architecture. Single source
 * of truth for the server-side validation (authoritative); the JSON-Schema
 * enum in the TOOLS entry above is the advisory client-side mirror — update
 * both together when the aspect set changes. */
static const char *VALID_ASPECTS[] = {
    "all",          "overview", "structure",  "dependencies", "routes",    "languages", "packages",
    "entry_points", "hotspots", "boundaries", "layers",       "file_tree", "clusters",  "cycles", NULL};

/* ── SCC / cycle condensation (get_architecture "cycles") ─────────
 * Iterative Tarjan over the CALLS call graph. Recursion would overflow on a
 * large graph, so the DFS state lives on explicit heap stacks. Reports the
 * strongly-connected components of size > 1 — the circular call dependencies,
 * the non-trivial content of the condensation quotient. */
typedef struct {
    int64_t *ids;  /* sorted unique node ids; index = position */
    int nverts;    /* |V| */
    int *adj_head; /* CSR row starts, length nverts+1 */
    int *adj;      /* CSR column (target vertex indices), length nedges */
    int nedges;    /* |E| within the vertex set */
} scc_graph_t;

static int scc_id_index(const int64_t *ids, int n, int64_t id) {
    int lo = 0;
    int hi = n - 1;
    while (lo <= hi) {
        int mid = (lo + hi) / 2;
        if (ids[mid] < id) {
            lo = mid + 1;
        } else if (ids[mid] > id) {
            hi = mid - 1;
        } else {
            return mid;
        }
    }
    return -1;
}

static int cmp_int64(const void *a, const void *b) {
    int64_t x = *(const int64_t *)a;
    int64_t y = *(const int64_t *)b;
    return (x > y) - (x < y);
}

/* Build the CSR call graph from parallel (src,tgt) edge arrays. Returns false
 * on OOM/empty. */
static bool scc_build(const int64_t *src, const int64_t *tgt, int ecount, scc_graph_t *g) {
    memset(g, 0, sizeof(*g));
    if (ecount <= 0) {
        return false;
    }
    int64_t *all = malloc((size_t)ecount * 2 * sizeof(int64_t));
    if (!all) {
        return false;
    }
    for (int i = 0; i < ecount; i++) {
        all[2 * i] = src[i];
        all[2 * i + 1] = tgt[i];
    }
    qsort(all, (size_t)ecount * 2, sizeof(int64_t), cmp_int64);
    int nv = 0;
    for (int i = 0; i < ecount * 2; i++) {
        if (i == 0 || all[i] != all[i - 1]) {
            all[nv++] = all[i];
        }
    }
    g->ids = all;
    g->nverts = nv;
    g->adj_head = calloc((size_t)nv + 1, sizeof(int));
    g->adj = malloc((size_t)ecount * sizeof(int));
    if (!g->adj_head || !g->adj) {
        free(g->ids);
        free(g->adj_head);
        free(g->adj);
        memset(g, 0, sizeof(*g));
        return false;
    }
    /* two-pass CSR fill */
    for (int i = 0; i < ecount; i++) {
        int u = scc_id_index(g->ids, nv, src[i]);
        g->adj_head[u + 1]++;
    }
    for (int i = 0; i < nv; i++) {
        g->adj_head[i + 1] += g->adj_head[i];
    }
    int *cursor = malloc((size_t)nv * sizeof(int));
    if (!cursor) {
        free(g->ids);
        free(g->adj_head);
        free(g->adj);
        memset(g, 0, sizeof(*g));
        return false;
    }
    for (int i = 0; i < nv; i++) {
        cursor[i] = g->adj_head[i];
    }
    for (int i = 0; i < ecount; i++) {
        int u = scc_id_index(g->ids, nv, src[i]);
        int v = scc_id_index(g->ids, nv, tgt[i]);
        g->adj[cursor[u]++] = v;
    }
    free(cursor);
    g->nedges = ecount;
    return true;
}

static void scc_free(scc_graph_t *g) {
    free(g->ids);
    free(g->adj_head);
    free(g->adj);
    memset(g, 0, sizeof(*g));
}

/* Iterative Tarjan. Fills comp[v] with a component id; components are numbered
 * in discovery order. Returns the component count, or -1 on OOM. */
static int scc_tarjan(const scc_graph_t *g, int *comp) {
    enum { SCC_UNVISITED = -1 };
    int nv = g->nverts;
    int *index = malloc((size_t)nv * sizeof(int));
    int *low = malloc((size_t)nv * sizeof(int));
    bool *on_stack = calloc((size_t)nv, sizeof(bool));
    int *tstack = malloc((size_t)nv * sizeof(int)); /* Tarjan's node stack */
    int *dfs_v = malloc((size_t)nv * sizeof(int));  /* explicit DFS: vertex */
    int *dfs_i = malloc((size_t)nv * sizeof(int));  /* explicit DFS: adj cursor */
    if (!index || !low || !on_stack || !tstack || !dfs_v || !dfs_i) {
        free(index);
        free(low);
        free(on_stack);
        free(tstack);
        free(dfs_v);
        free(dfs_i);
        return -1;
    }
    for (int i = 0; i < nv; i++) {
        index[i] = SCC_UNVISITED;
        comp[i] = SCC_UNVISITED;
    }
    int counter = 0;
    int tsp = 0;   /* Tarjan stack pointer */
    int ncomp = 0; /* component id allocator */
    for (int s = 0; s < nv; s++) {
        if (index[s] != SCC_UNVISITED) {
            continue;
        }
        int dsp = 0; /* DFS stack pointer */
        dfs_v[dsp] = s;
        dfs_i[dsp] = g->adj_head[s];
        index[s] = low[s] = counter++;
        tstack[tsp++] = s;
        on_stack[s] = true;
        while (dsp >= 0) {
            int v = dfs_v[dsp];
            if (dfs_i[dsp] < g->adj_head[v + 1]) {
                int w = g->adj[dfs_i[dsp]++];
                if (index[w] == SCC_UNVISITED) {
                    index[w] = low[w] = counter++;
                    tstack[tsp++] = w;
                    on_stack[w] = true;
                    dsp++;
                    dfs_v[dsp] = w;
                    dfs_i[dsp] = g->adj_head[w];
                } else if (on_stack[w] && index[w] < low[v]) {
                    low[v] = index[w];
                }
            } else {
                /* v fully explored: it is a root iff low==index -> pop an SCC */
                if (low[v] == index[v]) {
                    int w;
                    do {
                        w = tstack[--tsp];
                        on_stack[w] = false;
                        comp[w] = ncomp;
                    } while (w != v);
                    ncomp++;
                }
                dsp--;
                if (dsp >= 0 && low[v] < low[dfs_v[dsp]]) {
                    low[dfs_v[dsp]] = low[v];
                }
            }
        }
    }
    free(index);
    free(low);
    free(on_stack);
    free(tstack);
    free(dfs_v);
    free(dfs_i);
    return ncomp;
}

static bool aspect_is_valid(const char *name) {
    if (!name) {
        return false;
    }
    for (int i = 0; VALID_ASPECTS[i]; i++) {
        if (strcmp(name, VALID_ASPECTS[i]) == 0) {
            return true;
        }
    }
    return false;
}

/* Check if an aspect is requested. NULL aspects = all. The array can contain
 * "all" (everything), "overview" (everything except file_tree — see
 * cbm_store_arch_aspect_in_overview in store.c), or the aspect name itself. */
/* True ONLY when `name` is explicitly present in the aspects array — never via
 * the no-filter default, "all", or "overview". For expensive opt-in aspects
 * (cycles scans the whole call graph) that must not run on a bare call. */
static bool aspect_explicitly_named(yyjson_val *aspects_arr, const char *name) {
    if (!aspects_arr) {
        return false;
    }
    yyjson_arr_iter iter;
    yyjson_arr_iter_init(aspects_arr, &iter);
    yyjson_val *val;
    while ((val = yyjson_arr_iter_next(&iter)) != NULL) {
        const char *s = yyjson_get_str(val);
        if (s && strcmp(s, name) == 0) {
            return true;
        }
    }
    return false;
}

static bool aspect_wanted(yyjson_doc *aspects_doc, yyjson_val *aspects_arr, const char *name) {
    if (!aspects_arr) {
        return true; /* no filter = all */
    }
    yyjson_arr_iter iter;
    yyjson_arr_iter_init(aspects_arr, &iter);
    yyjson_val *val;
    while ((val = yyjson_arr_iter_next(&iter)) != NULL) {
        const char *s = yyjson_get_str(val);
        if (!s) {
            continue;
        }
        if (strcmp(s, "all") == 0) {
            return true;
        }
        if (strcmp(s, "overview") == 0 && cbm_store_arch_aspect_in_overview(name)) {
            return true;
        }
        if (strcmp(s, name) == 0) {
            return true;
        }
    }
    (void)aspects_doc;
    return false;
}

/* Append cross_repo_links summary to architecture JSON if CROSS_* edges exist. */
static void append_cross_repo_summary(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                      const cbm_schema_info_t *schema) {
    /* Scan edge types for any CROSS_* edges and sum them */
    int cross_total = 0;
    yyjson_mut_val *cr = yyjson_mut_obj(doc);
    static const char *cross_types[] = {"CROSS_HTTP_CALLS",    "CROSS_ASYNC_CALLS",
                                        "CROSS_CHANNEL",       "CROSS_GRPC_CALLS",
                                        "CROSS_GRAPHQL_CALLS", "CROSS_TRPC_CALLS"};
    for (int t = 0; t < (int)(sizeof(cross_types) / sizeof(cross_types[0])); t++) {
        for (int i = 0; i < schema->edge_type_count; i++) {
            if (strcmp(schema->edge_types[i].type, cross_types[t]) == 0) {
                yyjson_mut_obj_add_int(doc, cr, cross_types[t], schema->edge_types[i].count);
                cross_total += schema->edge_types[i].count;
                break;
            }
        }
    }
    if (cross_total > 0) {
        yyjson_mut_obj_add_int(doc, cr, "total", cross_total);
        yyjson_mut_obj_add_val(doc, root, "cross_repo_links", cr);
    }
}

static void add_architecture_languages_json(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                            const cbm_architecture_info_t *arch) {
    if (!arch || arch->language_count <= 0) {
        return;
    }
    yyjson_mut_val *langs = yyjson_mut_arr(doc);
    for (int i = 0; i < arch->language_count; i++) {
        yyjson_mut_val *item = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_strcpy(doc, item, "language",
                                  arch->languages[i].language ? arch->languages[i].language : "");
        yyjson_mut_obj_add_int(doc, item, "file_count", arch->languages[i].file_count);
        yyjson_mut_arr_add_val(langs, item);
    }
    yyjson_mut_obj_add_val(doc, root, "languages", langs);
}

static void add_architecture_entry_points_json(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                               const cbm_architecture_info_t *arch) {
    if (!arch || arch->entry_point_count <= 0) {
        return;
    }
    yyjson_mut_val *eps = yyjson_mut_arr(doc);
    for (int i = 0; i < arch->entry_point_count; i++) {
        yyjson_mut_val *item = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_strcpy(doc, item, "name",
                                  arch->entry_points[i].name ? arch->entry_points[i].name : "");
        yyjson_mut_obj_add_strcpy(doc, item, "qualified_name",
                                  arch->entry_points[i].qualified_name
                                      ? arch->entry_points[i].qualified_name
                                      : "");
        yyjson_mut_obj_add_strcpy(doc, item, "file",
                                  arch->entry_points[i].file ? arch->entry_points[i].file : "");
        yyjson_mut_arr_add_val(eps, item);
    }
    yyjson_mut_obj_add_val(doc, root, "entry_points", eps);
}

static void add_architecture_routes_json(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                         const cbm_architecture_info_t *arch) {
    if (!arch || arch->route_count <= 0) {
        return;
    }
    yyjson_mut_val *routes = yyjson_mut_arr(doc);
    for (int i = 0; i < arch->route_count; i++) {
        yyjson_mut_val *item = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_strcpy(doc, item, "method",
                                  arch->routes[i].method ? arch->routes[i].method : "");
        yyjson_mut_obj_add_strcpy(doc, item, "path",
                                  arch->routes[i].path ? arch->routes[i].path : "");
        yyjson_mut_obj_add_strcpy(doc, item, "handler",
                                  arch->routes[i].handler ? arch->routes[i].handler : "");
        yyjson_mut_arr_add_val(routes, item);
    }
    yyjson_mut_obj_add_val(doc, root, "routes", routes);
}

/* Join a string list with semicolons so compact TOON cells need no quoting. */
static void arch_join_list(char *buf, size_t size, const char **items, int count) {
    size_t offset = 0;
    if (!buf || size == 0) {
        return;
    }
    buf[0] = '\0';
    for (int i = 0; i < count && offset < size; i++) {
        const char *item = items[i] ? items[i] : "";
        int written = snprintf(buf + offset, size - offset, "%s%s", i > 0 ? ";" : "", item);
        if (written < 0 || (size_t)written >= size - offset) {
            break;
        }
        offset += (size_t)written;
    }
}

/* Compute the circular-dependency SCCs (size > 1) of the CALLS graph. Returns
 * a malloc'd array of components, each a malloc'd int64 array of member node
 * ids, with sizes in *out_sizes and count in *out_ncycles; sets *scanned_edges
 * and *edges_truncated. Caller frees each component + the arrays. Returns
 * CBM_STORE_OK, or CBM_STORE_ERR on failure (all outs zeroed). */
enum { ARCH_SCC_MAX_EDGES = 400000, ARCH_SCC_MAX_CYCLES = 100, ARCH_SCC_MEMBERS_SHOWN = 20 };

static int arch_compute_cycles(cbm_store_t *store, const char *project, int64_t ***out_members,
                               int **out_sizes, int *out_ncycles, int *out_total_cycles,
                               int *scanned_edges, bool *edges_truncated) {
    *out_members = NULL;
    *out_sizes = NULL;
    *out_ncycles = 0;
    *out_total_cycles = 0;
    *scanned_edges = 0;
    *edges_truncated = false;

    int64_t *src = NULL;
    int64_t *tgt = NULL;
    int ecount = 0;
    if (cbm_store_fetch_call_edges(store, project, ARCH_SCC_MAX_EDGES, &src, &tgt, &ecount,
                                   edges_truncated) != CBM_STORE_OK) {
        return CBM_STORE_ERR;
    }
    *scanned_edges = ecount;
    scc_graph_t g;
    if (!scc_build(src, tgt, ecount, &g)) {
        free(src);
        free(tgt);
        return CBM_STORE_OK; /* no edges = no cycles, not an error */
    }
    free(src);
    free(tgt);

    int *comp = malloc((size_t)g.nverts * sizeof(int));
    if (!comp) {
        scc_free(&g);
        return CBM_STORE_ERR;
    }
    int ncomp = scc_tarjan(&g, comp);
    if (ncomp < 0) {
        free(comp);
        scc_free(&g);
        return CBM_STORE_ERR;
    }
    /* size per component */
    int *csize = calloc((size_t)ncomp, sizeof(int));
    if (!csize) {
        free(comp);
        scc_free(&g);
        return CBM_STORE_ERR;
    }
    for (int v = 0; v < g.nverts; v++) {
        csize[comp[v]]++;
    }
    /* collect components with size > 1 (the cycles) */
    int ncyc = 0;
    for (int c = 0; c < ncomp; c++) {
        if (csize[c] > 1) {
            ncyc++;
        }
    }
    *out_total_cycles = ncyc; /* the TRUE count, before the display clamp */
    if (ncyc > ARCH_SCC_MAX_CYCLES) {
        ncyc = ARCH_SCC_MAX_CYCLES;
    }
    int64_t **members = ncyc > 0 ? calloc((size_t)ncyc, sizeof(int64_t *)) : NULL;
    int *sizes = ncyc > 0 ? calloc((size_t)ncyc, sizeof(int)) : NULL;
    /* map component id -> output slot (only for the first ARCH_SCC_MAX_CYCLES
     * size>1 comps, in component-id order) */
    int *slot = malloc((size_t)ncomp * sizeof(int));
    if ((ncyc > 0 && (!members || !sizes)) || !slot) {
        free(members);
        free(sizes);
        free(slot);
        free(csize);
        free(comp);
        scc_free(&g);
        return CBM_STORE_ERR;
    }
    int next_slot = 0;
    for (int c = 0; c < ncomp; c++) {
        if (csize[c] > 1 && next_slot < ncyc) {
            slot[c] = next_slot;
            sizes[next_slot] = csize[c];
            members[next_slot] = malloc((size_t)csize[c] * sizeof(int64_t));
            next_slot++;
        } else {
            slot[c] = -1;
        }
    }
    int *fill = calloc((size_t)ncyc, sizeof(int));
    if (ncyc > 0 && !fill) {
        for (int i = 0; i < ncyc; i++) {
            free(members[i]);
        }
        free(members);
        free(sizes);
        free(slot);
        free(csize);
        free(comp);
        scc_free(&g);
        return CBM_STORE_ERR;
    }
    for (int v = 0; v < g.nverts; v++) {
        int sl = slot[comp[v]];
        if (sl >= 0) {
            members[sl][fill[sl]++] = g.ids[v];
        }
    }
    free(fill);
    free(slot);
    free(csize);
    free(comp);
    scc_free(&g);
    *out_members = members;
    *out_sizes = sizes;
    *out_ncycles = ncyc;
    return CBM_STORE_OK;
}

/* Fetch the qualified_name for a node id, or a "#<id>" fallback. */
static void arch_node_qn(cbm_store_t *store, int64_t id, char *out, size_t outsz) {
    cbm_node_t n = {0};
    if (cbm_store_find_node_by_id(store, id, &n) == CBM_STORE_OK && n.qualified_name) {
        snprintf(out, outsz, "%s", n.qualified_name);
    } else {
        snprintf(out, outsz, "#%lld", (long long)id);
    }
    free_node_contents(&n);
}

static char *handle_get_architecture(cbm_mcp_server_t *srv, const char *args) {
    cbm_mcp_output_format_t response_format = cbm_mcp_response_format(srv, args);
    if (response_format == CBM_MCP_OUTPUT_INVALID) {
        return cbm_mcp_invalid_response_format();
    }
    char *raw_project = get_store_project_arg(args);
    project_expand_t pe = {0};
    cbm_store_t *store = resolve_project_store(srv, raw_project, &pe);
    char *project = pe.value;
    REQUIRE_STORE(store, project);
    char *scope_path = cbm_mcp_get_string_arg(args, "path");

    char *not_indexed = verify_project_indexed(store, project);
    if (not_indexed) {
        free(project);
        free(scope_path);
        return not_indexed;
    }

    /* Parse aspects array from args */
    yyjson_doc *aspects_doc = NULL;
    yyjson_val *aspects_arr = NULL;
    {
        yyjson_doc *args_doc = yyjson_read(args, strlen(args), 0);
        if (args_doc) {
            yyjson_val *aval = yyjson_obj_get(yyjson_doc_get_root(args_doc), "aspects");
            if (yyjson_is_arr(aval)) {
                aspects_doc = args_doc; /* keep alive */
                aspects_arr = aval;
            } else {
                yyjson_doc_free(args_doc);
            }
        }
    }

    /* Build a C string array from aspects for cbm_store_get_architecture.
     * Strings point into aspects_doc memory so aspects_doc must outlive this array. */
    const char *aspects_strs[MCP_COL_16];
    int aspects_strs_count = 0;
    if (aspects_arr) {
        size_t aspect_idx;
        size_t aspect_max;
        yyjson_val *aspect_val;
        yyjson_arr_foreach(aspects_arr, aspect_idx, aspect_max, aspect_val) {
            const char *s = yyjson_get_str(aspect_val);
            if (s && aspects_strs_count < MCP_COL_16) {
                aspects_strs[aspects_strs_count++] = s;
            }
        }
    }

    /* Server-side validation: reject unknown aspect tokens with an isError
     * result listing the valid values. The JSON-Schema enum is advisory —
     * many MCP clients do not validate arguments against tool schemas — so
     * without this check a typo degraded to a silent near-empty payload. */
    for (int i = 0; i < aspects_strs_count; i++) {
        if (!aspect_is_valid(aspects_strs[i])) {
            char valid_list[CBM_SZ_256];
            size_t off = 0;
            for (int j = 0; VALID_ASPECTS[j] && off < sizeof(valid_list); j++) {
                int n = snprintf(valid_list + off, sizeof(valid_list) - off, "%s%s",
                                 j > 0 ? ", " : "", VALID_ASPECTS[j]);
                if (n < 0) {
                    break;
                }
                off += (size_t)n;
            }
            char msg[CBM_SZ_512];
            snprintf(msg, sizeof(msg), "Unknown aspect '%s'. Valid: %s.", aspects_strs[i],
                     valid_list);
            char *err = cbm_mcp_text_result(msg, true);
            free(project);
            free(scope_path);
            if (aspects_doc) {
                yyjson_doc_free(aspects_doc);
            }
            return err;
        }
    }

    /* Default (no aspects) = compact summary. The old default rendered ALL
     * aspects including the full file_tree — ~94KB (~23K tokens) on a
     * mid-size repo, a context bomb for the LLM consumers. Explicit
     * aspects (or ["all"]) keep full access to every section. */
    bool default_summary = false;
    if (aspects_strs_count == 0) {
        /* NOT "overview" — that means everything-except-file_tree. Totals and
         * node_labels/edge_types counts are always emitted alongside. */
        aspects_strs[aspects_strs_count++] = "languages";
        aspects_strs[aspects_strs_count++] = "packages";
        aspects_strs[aspects_strs_count++] = "entry_points";
        default_summary = true;
    }

    cbm_schema_info_t schema = {0};
    /* Counts-only: this handler renders label/type counts but never property
     * keys, and full key discovery json_each-scans every row (seconds-to-
     * minutes on multi-million-node graphs). */
    cbm_store_get_schema_counts_scoped(store, project, scope_path, &schema);

    cbm_architecture_info_t arch = {0};
    int arch_hotspot_limit = srv && srv->config
        ? cbm_config_get_int(srv->config, CBM_CONFIG_ARCH_HOTSPOT_LIMIT, 0)
        : 0;
    /* Leiden resolution (gamma) — cluster granularity, tunable via config.
     * Default 1.0; >1 → smaller/more clusters, <1 → larger/fewer. */
    double arch_leiden_resolution = srv && srv->config
        ? cbm_config_get_double(srv->config, CBM_CONFIG_ARCH_RESOLUTION, 1.0)
        : 1.0;
    cbm_store_get_architecture_scoped(store, project, scope_path,
                                      aspects_strs_count > 0 ? aspects_strs : NULL,
                                      aspects_strs_count, &arch, arch_hotspot_limit,
                                      arch_leiden_resolution);

    int node_count = cbm_store_count_nodes_scoped(store, project, scope_path);
    int edge_count = cbm_store_count_edges_scoped(store, project, scope_path);
    char norm_path[CBM_SZ_512];
    bool path_scoped = cbm_store_normalize_arch_path(scope_path, norm_path, sizeof(norm_path));

    /* Response encoding: TOON tables by default; format:"json" restores the
     * legacy per-item objects. */
    bool arch_legacy_json = response_format == CBM_MCP_OUTPUT_JSON;

    if (!arch_legacy_json) {
        cbm_sb_t sb;
        cbm_sb_init(&sb);
        if (project) {
            cbm_toon_scalar_str(&sb, "project", project);
        }
        if (default_summary) {
            cbm_toon_scalar_str(&sb, "aspects_hint",
                                "Summary view (default). More on request via aspects:[...] — "
                                "structure, dependencies, routes, hotspots, boundaries, layers, "
                                "clusters, file_tree — or [\"all\"] for everything.");
        }
        if (path_scoped) {
            cbm_toon_scalar_str(&sb, "path", norm_path);
            cbm_toon_scalar_int(&sb, "root_total_nodes", cbm_store_count_nodes(store, project));
            cbm_toon_scalar_int(&sb, "root_total_edges", cbm_store_count_edges(store, project));
            cbm_toon_scalar_int(&sb, "scoped_total_nodes", node_count);
            cbm_toon_scalar_int(&sb, "scoped_total_edges", edge_count);
        }
        cbm_toon_scalar_int(&sb, "total_nodes", node_count);
        cbm_toon_scalar_int(&sb, "total_edges", edge_count);

        if (aspect_wanted(aspects_doc, aspects_arr, "structure") && schema.node_label_count > 0) {
            static const char *const lcols[] = {"label", "count"};
            cbm_toon_table_header(&sb, "node_labels", schema.node_label_count, lcols, 2);
            for (int i = 0; i < schema.node_label_count; i++) {
                cbm_toon_row_begin(&sb);
                cbm_toon_cell_str(&sb, schema.node_labels[i].label, true);
                cbm_toon_cell_int(&sb, schema.node_labels[i].count, false);
                cbm_toon_row_end(&sb);
            }
        }
        if (aspect_wanted(aspects_doc, aspects_arr, "dependencies") && schema.edge_type_count > 0) {
            static const char *const tcols[] = {"type", "count"};
            cbm_toon_table_header(&sb, "edge_types", schema.edge_type_count, tcols, 2);
            for (int i = 0; i < schema.edge_type_count; i++) {
                cbm_toon_row_begin(&sb);
                cbm_toon_cell_str(&sb, schema.edge_types[i].type, true);
                cbm_toon_cell_int(&sb, schema.edge_types[i].count, false);
                cbm_toon_row_end(&sb);
            }
        }
        if (aspect_wanted(aspects_doc, aspects_arr, "routes") && schema.rel_pattern_count > 0) {
            static const char *const pcols[] = {"match_pattern", "observed_count"};
            cbm_toon_table_header(&sb, "relationship_patterns", schema.rel_pattern_count, pcols,
                                  MCP_COL_2);
            for (int i = 0; i < schema.rel_pattern_count; i++) {
                char *match = schema_relationship_pattern_text(&schema.rel_patterns[i], true);
                cbm_toon_row_begin(&sb);
                cbm_toon_cell_str(&sb, match ? match : "", true);
                cbm_toon_cell_int(&sb, schema.rel_patterns[i].observed_count, false);
                cbm_toon_row_end(&sb);
                free(match);
            }
        }
        if (arch.language_count > 0) {
            static const char *const gcols[] = {"language", "files"};
            cbm_toon_table_header(&sb, "languages", arch.language_count, gcols, 2);
            for (int i = 0; i < arch.language_count; i++) {
                cbm_toon_row_begin(&sb);
                cbm_toon_cell_str(&sb, arch.languages[i].language, true);
                cbm_toon_cell_int(&sb, arch.languages[i].file_count, false);
                cbm_toon_row_end(&sb);
            }
        }
        if (arch.package_count > 0) {
            static const char *const kcols[] = {"name", "nodes", "fan_in", "fan_out"};
            cbm_toon_table_header(&sb, "packages", arch.package_count, kcols, 4);
            for (int i = 0; i < arch.package_count; i++) {
                cbm_toon_row_begin(&sb);
                cbm_toon_cell_str(&sb, arch.packages[i].name, true);
                cbm_toon_cell_int(&sb, arch.packages[i].node_count, false);
                cbm_toon_cell_int(&sb, arch.packages[i].fan_in, false);
                cbm_toon_cell_int(&sb, arch.packages[i].fan_out, false);
                cbm_toon_row_end(&sb);
            }
        }
        if (arch.entry_point_count > 0) {
            /* qn only — `name` is its last segment. */
            static const char *const ecols[] = {"qn", "file"};
            cbm_toon_table_header(&sb, "entry_points", arch.entry_point_count, ecols, 2);
            for (int i = 0; i < arch.entry_point_count; i++) {
                cbm_toon_row_begin(&sb);
                cbm_toon_cell_str(&sb, arch.entry_points[i].qualified_name, true);
                cbm_toon_cell_str(&sb, arch.entry_points[i].file, false);
                cbm_toon_row_end(&sb);
            }
        }
        if (arch.route_count > 0) {
            static const char *const rcols[] = {"method", "path", "handler"};
            cbm_toon_table_header(&sb, "routes", arch.route_count, rcols, 3);
            for (int i = 0; i < arch.route_count; i++) {
                cbm_toon_row_begin(&sb);
                cbm_toon_cell_str(&sb, arch.routes[i].method, true);
                cbm_toon_cell_str(&sb, arch.routes[i].path, false);
                cbm_toon_cell_str(&sb, arch.routes[i].handler, false);
                cbm_toon_row_end(&sb);
            }
        }
        if (arch.hotspot_count > 0) {
            static const char *const hcols[] = {"qn", "fan_in"};
            cbm_toon_table_header(&sb, "hotspots", arch.hotspot_count, hcols, 2);
            for (int i = 0; i < arch.hotspot_count; i++) {
                cbm_toon_row_begin(&sb);
                cbm_toon_cell_str(&sb, arch.hotspots[i].qualified_name, true);
                cbm_toon_cell_int(&sb, arch.hotspots[i].fan_in, false);
                cbm_toon_row_end(&sb);
            }
        }
        if (arch.boundary_count > 0) {
            static const char *const bcols[] = {"from", "to", "calls"};
            cbm_toon_table_header(&sb, "boundaries", arch.boundary_count, bcols, 3);
            for (int i = 0; i < arch.boundary_count; i++) {
                cbm_toon_row_begin(&sb);
                cbm_toon_cell_str(&sb, arch.boundaries[i].from, true);
                cbm_toon_cell_str(&sb, arch.boundaries[i].to, false);
                cbm_toon_cell_int(&sb, arch.boundaries[i].call_count, false);
                cbm_toon_row_end(&sb);
            }
        }
        if (arch.service_count > 0) {
            static const char *const scols[] = {"from", "to", "type", "count"};
            cbm_toon_table_header(&sb, "services", arch.service_count, scols, 4);
            for (int i = 0; i < arch.service_count; i++) {
                cbm_toon_row_begin(&sb);
                cbm_toon_cell_str(&sb, arch.services[i].from, true);
                cbm_toon_cell_str(&sb, arch.services[i].to, false);
                cbm_toon_cell_str(&sb, arch.services[i].type, false);
                cbm_toon_cell_int(&sb, arch.services[i].count, false);
                cbm_toon_row_end(&sb);
            }
        }
        if (arch.layer_count > 0) {
            static const char *const ycols[] = {"name", "layer", "reason"};
            cbm_toon_table_header(&sb, "layers", arch.layer_count, ycols, 3);
            for (int i = 0; i < arch.layer_count; i++) {
                cbm_toon_row_begin(&sb);
                cbm_toon_cell_str(&sb, arch.layers[i].name, true);
                cbm_toon_cell_str(&sb, arch.layers[i].layer, false);
                cbm_toon_cell_str(&sb, arch.layers[i].reason, false);
                cbm_toon_row_end(&sb);
            }
        }
        if (arch.cluster_count > 0) {
            /* Nested lists become ';'-joined cells. */
            static const char *const ccols[] = {"id",        "label",    "members",   "cohesion",
                                                "top_nodes", "packages", "edge_types"};
            cbm_toon_table_header(&sb, "clusters", arch.cluster_count, ccols, 7);
            for (int i = 0; i < arch.cluster_count; i++) {
                const cbm_cluster_info_t *c = &arch.clusters[i];
                char joined[CBM_SZ_1K];
                cbm_toon_row_begin(&sb);
                cbm_toon_cell_int(&sb, c->id, true);
                cbm_toon_cell_str(&sb, c->label, false);
                cbm_toon_cell_int(&sb, c->members, false);
                cbm_toon_cell_real(&sb, c->cohesion, false);
                arch_join_list(joined, sizeof(joined), c->top_nodes, c->top_node_count);
                cbm_toon_cell_str(&sb, joined, false);
                arch_join_list(joined, sizeof(joined), c->packages, c->package_count);
                cbm_toon_cell_str(&sb, joined, false);
                arch_join_list(joined, sizeof(joined), c->edge_types, c->edge_type_count);
                cbm_toon_cell_str(&sb, joined, false);
                cbm_toon_row_end(&sb);
            }
        }
        if (arch.file_tree_count > 0) {
            static const char *const fcols[] = {"path", "type", "children"};
            cbm_toon_table_header(&sb, "file_tree", arch.file_tree_count, fcols, 3);
            for (int i = 0; i < arch.file_tree_count; i++) {
                cbm_toon_row_begin(&sb);
                cbm_toon_cell_str(&sb, arch.file_tree[i].path, true);
                cbm_toon_cell_str(&sb, arch.file_tree[i].type, false);
                cbm_toon_cell_int(&sb, arch.file_tree[i].children, false);
                cbm_toon_row_end(&sb);
            }
        }
        /* Cross-repo edge summary (mirrors append_cross_repo_summary). */
        {
            static const char *const cross_types[] = {"CROSS_HTTP_CALLS",    "CROSS_ASYNC_CALLS",
                                                      "CROSS_CHANNEL",       "CROSS_GRPC_CALLS",
                                                      "CROSS_GRAPHQL_CALLS", "CROSS_TRPC_CALLS"};
            int cross_total = 0;
            for (int t = 0; t < (int)(sizeof(cross_types) / sizeof(cross_types[0])); t++) {
                for (int i = 0; i < schema.edge_type_count; i++) {
                    if (strcmp(schema.edge_types[i].type, cross_types[t]) == 0) {
                        cross_total += schema.edge_types[i].count;
                        break;
                    }
                }
            }
            if (cross_total > 0) {
                cbm_toon_scalar_int(&sb, "cross_repo_links_total", cross_total);
            }
        }

        /* cycles: circular CALLS dependencies (SCCs of size > 1) — opt-in, it
         * scans the whole call graph. A quotient/condensation view. */
        if (aspect_explicitly_named(aspects_arr, "cycles")) {
            int64_t **members = NULL;
            int *sizes = NULL;
            int ncyc = 0;
            int total_cyc = 0;
            int scanned = 0;
            bool etrunc = false;
            if (arch_compute_cycles(store, project, &members, &sizes, &ncyc, &total_cyc, &scanned,
                                    &etrunc) == CBM_STORE_OK) {
                cbm_toon_scalar_int(&sb, "call_edges_scanned", scanned);
                cbm_toon_scalar_int(&sb, "cycles_total", total_cyc);
                if (etrunc) {
                    cbm_toon_scalar_bool(&sb, "cycles_partial", true);
                    cbm_toon_scalar_str(&sb, "cycles_hint",
                                        "call graph exceeded the scan budget; cycle list may be "
                                        "incomplete");
                }
                if (total_cyc > ncyc) {
                    char omit[CBM_SZ_128];
                    snprintf(omit, sizeof(omit), "cycles_omitted: %d  (showing the first %d)\n",
                             total_cyc - ncyc, ncyc);
                    cbm_sb_append(&sb, omit);
                }
                char hdr[CBM_SZ_128];
                snprintf(hdr, sizeof(hdr),
                         "cycles: %d  (rows: size members; circular CALLS dependencies)\n", ncyc);
                cbm_sb_append(&sb, hdr);
                for (int c = 0; c < ncyc; c++) {
                    char row[CBM_SZ_2K];
                    int off = snprintf(row, sizeof(row), "  %d ", sizes[c]);
                    bool clipped = sizes[c] > ARCH_SCC_MEMBERS_SHOWN;
                    int show = clipped ? ARCH_SCC_MEMBERS_SHOWN : sizes[c];
                    for (int m = 0; m < show && off < (int)sizeof(row) - 2; m++) {
                        char qn[CBM_SZ_512];
                        arch_node_qn(store, members[c][m], qn, sizeof(qn));
                        off += snprintf(row + off, sizeof(row) - off, "%s%s", m ? ";" : "", qn);
                    }
                    if (clipped && off < (int)sizeof(row) - 8) {
                        snprintf(row + off, sizeof(row) - off, ";+%d", sizes[c] - show);
                    }
                    cbm_sb_append(&sb, row);
                    cbm_sb_append(&sb, "\n");
                    free(members[c]);
                }
                free(members);
                free(sizes);
            }
        }

        cbm_store_architecture_free(&arch);
        cbm_store_schema_free(&schema);

        cbm_store_architecture_free(&arch);
        cbm_store_schema_free(&schema);
        if (aspects_doc) {
            yyjson_doc_free(aspects_doc);
        }
        free(project);
        free(scope_path);
        char *text = cbm_sb_finish(&sb);
        char *result = cbm_mcp_text_result(text ? text : "out of memory", text == NULL);
        free(text);
        return result;
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    if (srv->session_project[0])
        yyjson_mut_obj_add_str(doc, root, "session_project", srv->session_project);

    if (project) {
        yyjson_mut_obj_add_str(doc, root, "project", project);
    }
    if (default_summary) {
        yyjson_mut_obj_add_str(doc, root, "aspects_hint",
                               "Summary view (default). More on request via aspects:[...] — "
                               "structure, dependencies, routes, hotspots, boundaries, layers, "
                               "clusters, file_tree — or [\"all\"] for everything.");
    }
    if (path_scoped) {
        yyjson_mut_obj_add_str(doc, root, "path", norm_path);
        int root_nodes = cbm_store_count_nodes(store, project);
        int root_edges = cbm_store_count_edges(store, project);
        yyjson_mut_obj_add_int(doc, root, "root_total_nodes", root_nodes);
        yyjson_mut_obj_add_int(doc, root, "root_total_edges", root_edges);
        yyjson_mut_obj_add_int(doc, root, "scoped_total_nodes", node_count);
        yyjson_mut_obj_add_int(doc, root, "scoped_total_edges", edge_count);
    }
    yyjson_mut_obj_add_int(doc, root, "total_nodes", node_count);
    yyjson_mut_obj_add_int(doc, root, "total_edges", edge_count);
    bool architecture_stale =
        project && cbm_store_derived_view_is_stale(store, project,
                                                   CBM_STORE_DERIVED_VIEW_ARCHITECTURE);
    bool routes_stale =
        project && cbm_store_derived_view_is_stale(store, project, CBM_STORE_DERIVED_VIEW_ROUTES);
    bool pagerank_stale =
        project && cbm_store_derived_view_is_stale(store, project, CBM_STORE_DERIVED_VIEW_PAGERANK);
    if (architecture_stale) {
        add_stale_derived_view_warning(
            doc, root, CBM_STORE_DERIVED_VIEW_ARCHITECTURE,
            "architecture derived view is stale; summaries may need a full refresh.");
    }
    if (routes_stale) {
        add_stale_derived_view_warning(doc, root, CBM_STORE_DERIVED_VIEW_ROUTES,
                                       "routes derived view is stale; route results may be stale.");
    }
    int dirty_pending = 0;
    int dirty_overlay_ready = 0;
    bool active_languages_requested = aspect_wanted(aspects_doc, aspects_arr, "languages");
    bool active_entry_points_requested = aspect_wanted(aspects_doc, aspects_arr, "entry_points");
    bool active_routes_requested = aspect_wanted(aspects_doc, aspects_arr, "routes");
    bool active_file_tree_requested = aspect_wanted(aspects_doc, aspects_arr, "file_tree");
    bool active_architecture_reported =
        add_overlay_active_architecture_freshness(doc, root, store, project,
                                                  active_languages_requested,
                                                  active_entry_points_requested,
                                                  active_routes_requested,
                                                  active_file_tree_requested, NULL);
    bool overlay_limitation_reported =
        !active_architecture_reported &&
        add_canonical_only_overlay_freshness(
            doc, root, store, project,
            "get_architecture reads canonical graph summaries; ready overlay rows are not "
            "included until active architecture views or compaction are available.");
    if (get_dirty_file_counts(store, project, &dirty_pending, &dirty_overlay_ready)) {
        add_dirty_file_freshness_counts(doc, root, dirty_pending, dirty_overlay_ready);
        if (!active_architecture_reported) {
            add_canonical_only_read_model(doc, root);
        }
        if (!overlay_limitation_reported) {
            add_response_warning(
                doc, root,
                active_architecture_reported
                    ? "get_architecture used active overlay node rows for requested sections; "
                      "dirty file changes outside ready overlays may still be absent from "
                      "canonical summaries until overlay or reindex completes."
                    : "get_architecture reads canonical graph summaries; dirty file changes may "
                      "be absent until overlay or reindex completes.");
        }
    } else if (overlay_limitation_reported) {
        add_canonical_only_read_model(doc, root);
    }

    /* Node label summary */
    if (aspect_wanted(aspects_doc, aspects_arr, "structure")) {
        yyjson_mut_val *labels = yyjson_mut_arr(doc);
        for (int i = 0; i < schema.node_label_count; i++) {
            yyjson_mut_val *item = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_str(doc, item, "label", schema.node_labels[i].label);
            yyjson_mut_obj_add_int(doc, item, "count", schema.node_labels[i].count);
            yyjson_mut_arr_add_val(labels, item);
        }
        yyjson_mut_obj_add_val(doc, root, "node_labels", labels);
    }

    /* Edge type summary */
    if (aspect_wanted(aspects_doc, aspects_arr, "dependencies")) {
        yyjson_mut_val *types = yyjson_mut_arr(doc);
        for (int i = 0; i < schema.edge_type_count; i++) {
            yyjson_mut_val *item = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_str(doc, item, "type", schema.edge_types[i].type);
            yyjson_mut_obj_add_int(doc, item, "count", schema.edge_types[i].count);
            yyjson_mut_arr_add_val(types, item);
        }
        yyjson_mut_obj_add_val(doc, root, "edge_types", types);
    }

    /* Relationship patterns */
    if (aspect_wanted(aspects_doc, aspects_arr, "routes") && schema.rel_pattern_count > 0) {
        yyjson_mut_val *pats = yyjson_mut_arr(doc);
        for (int i = 0; i < schema.rel_pattern_count; i++) {
            char *display = schema_relationship_pattern_text(&schema.rel_patterns[i], false);
            if (display) {
                yyjson_mut_arr_add_strcpy(doc, pats, display);
                free(display);
            }
        }
        yyjson_mut_obj_add_val(doc, root, "relationship_patterns", pats);
    }

    /* Key functions: top 10 by PageRank with config + param exclude patterns */
    if (pagerank_stale) {
        add_stale_derived_view_warning(
            doc, root, CBM_STORE_DERIVED_VIEW_PAGERANK,
            "pagerank derived view is stale; key_functions were omitted.");
    } else {
        sqlite3 *db = cbm_store_get_db(store);
        if (db) {
            int excl_count = 0;
            char **excl_arr = cbm_mcp_get_string_array_arg(args, "exclude", &excl_count);
            if (excl_count < 0) {
                add_response_warning(
                    doc, root,
                    "key_functions omitted: out of memory preparing exclude patterns");
            } else {
                const char *excl_csv = srv->config
                    ? cbm_config_get(srv->config, CBM_CONFIG_KEY_FUNCTIONS_EXCLUDE, "")
                    : "";
                int kf_limit = srv->config
                    ? cbm_config_get_int(srv->config, CBM_CONFIG_KEY_FUNCTIONS_COUNT, 25)
                    : 25;
                char *kf_sql_heap = build_key_functions_sql(excl_csv, (const char **)excl_arr,
                                                            kf_limit, path_scoped);
                if (!kf_sql_heap) {
                    add_response_warning(doc, root,
                                         "key_functions omitted: out of memory building SQL");
                } else {
                    const char *kf_sql = kf_sql_heap;
                    sqlite3_stmt *kf_stmt = NULL;
                    if (sqlite3_prepare_v2(db, kf_sql, -1, &kf_stmt, NULL) == SQLITE_OK) {
                        if (project) sqlite3_bind_text(kf_stmt, 1, project, -1, SQLITE_TRANSIENT);
                        if (path_scoped) {
                            char scope_like[CBM_SZ_512 + 3];
                            snprintf(scope_like, sizeof(scope_like), "%s/%%", norm_path);
                            sqlite3_bind_text(kf_stmt, 2, norm_path, -1, SQLITE_TRANSIENT);
                            sqlite3_bind_text(kf_stmt, 3, scope_like, -1, SQLITE_TRANSIENT);
                        }
                        yyjson_mut_val *kf_arr = yyjson_mut_arr(doc);
                        while (sqlite3_step(kf_stmt) == SQLITE_ROW) {
                            yyjson_mut_val *kf = yyjson_mut_obj(doc);
                            const char *n = (const char *)sqlite3_column_text(kf_stmt, 0);
                            const char *qn = (const char *)sqlite3_column_text(kf_stmt, 1);
                            const char *lbl = (const char *)sqlite3_column_text(kf_stmt, 2);
                            const char *fp = (const char *)sqlite3_column_text(kf_stmt, 3);
                            double rank = sqlite3_column_double(kf_stmt, 4);
                            if (n && !ends_with_segment(qn, n))
                                yyjson_mut_obj_add_strcpy(doc, kf, "name", n);
                            if (qn)  yyjson_mut_obj_add_strcpy(doc, kf, "qualified_name", qn);
                            if (lbl && lbl[0]) yyjson_mut_obj_add_strcpy(doc, kf, "label", lbl);
                            if (fp  && fp[0])  yyjson_mut_obj_add_strcpy(doc, kf, "file_path", fp);
                            add_pagerank_val(doc, kf, rank);
                            yyjson_mut_arr_add_val(kf_arr, kf);
                        }
                        sqlite3_finalize(kf_stmt);
                        yyjson_mut_obj_add_val(doc, root, "key_functions", kf_arr);
                    }
                    free(kf_sql_heap);
                }
            }
            free_string_array(excl_arr);
        }
    }

    /* Languages */
    add_architecture_languages_json(doc, root, &arch);

    /* Packages */
    if (arch.package_count > 0) {
        yyjson_mut_val *pkgs = yyjson_mut_arr(doc);
        for (int i = 0; i < arch.package_count; i++) {
            yyjson_mut_val *item = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_str(doc, item, "name",
                                   arch.packages[i].name ? arch.packages[i].name : "");
            yyjson_mut_obj_add_int(doc, item, "node_count", arch.packages[i].node_count);
            yyjson_mut_obj_add_int(doc, item, "fan_in", arch.packages[i].fan_in);
            yyjson_mut_obj_add_int(doc, item, "fan_out", arch.packages[i].fan_out);
            yyjson_mut_arr_add_val(pkgs, item);
        }
        yyjson_mut_obj_add_val(doc, root, "packages", pkgs);
    }

    /* Entry points */
    add_architecture_entry_points_json(doc, root, &arch);

    /* HTTP routes */
    add_architecture_routes_json(doc, root, &arch);

    /* Hotspots */
    if (arch.hotspot_count > 0) {
        yyjson_mut_val *hotspots = yyjson_mut_arr(doc);
        for (int i = 0; i < arch.hotspot_count; i++) {
            yyjson_mut_val *item = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_str(doc, item, "name",
                                   arch.hotspots[i].name ? arch.hotspots[i].name : "");
            yyjson_mut_obj_add_str(doc, item, "qualified_name",
                                   arch.hotspots[i].qualified_name ? arch.hotspots[i].qualified_name
                                                                   : "");
            yyjson_mut_obj_add_int(doc, item, "fan_in", arch.hotspots[i].fan_in);
            yyjson_mut_arr_add_val(hotspots, item);
        }
        yyjson_mut_obj_add_val(doc, root, "hotspots", hotspots);
    }

    /* Cross-package boundaries */
    if (arch.boundary_count > 0) {
        yyjson_mut_val *boundaries = yyjson_mut_arr(doc);
        for (int i = 0; i < arch.boundary_count; i++) {
            yyjson_mut_val *item = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_str(doc, item, "from",
                                   arch.boundaries[i].from ? arch.boundaries[i].from : "");
            yyjson_mut_obj_add_str(doc, item, "to",
                                   arch.boundaries[i].to ? arch.boundaries[i].to : "");
            yyjson_mut_obj_add_int(doc, item, "call_count", arch.boundaries[i].call_count);
            yyjson_mut_arr_add_val(boundaries, item);
        }
        yyjson_mut_obj_add_val(doc, root, "boundaries", boundaries);
    }

    /* Cross-service links (HTTP/async between services) */
    if (arch.service_count > 0) {
        yyjson_mut_val *services = yyjson_mut_arr(doc);
        for (int i = 0; i < arch.service_count; i++) {
            yyjson_mut_val *item = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_str(doc, item, "from",
                                   arch.services[i].from ? arch.services[i].from : "");
            yyjson_mut_obj_add_str(doc, item, "to", arch.services[i].to ? arch.services[i].to : "");
            yyjson_mut_obj_add_str(doc, item, "type",
                                   arch.services[i].type ? arch.services[i].type : "");
            yyjson_mut_obj_add_int(doc, item, "count", arch.services[i].count);
            yyjson_mut_arr_add_val(services, item);
        }
        yyjson_mut_obj_add_val(doc, root, "services", services);
    }

    /* Package layers */
    if (arch.layer_count > 0) {
        yyjson_mut_val *layers = yyjson_mut_arr(doc);
        for (int i = 0; i < arch.layer_count; i++) {
            yyjson_mut_val *item = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_str(doc, item, "name",
                                   arch.layers[i].name ? arch.layers[i].name : "");
            yyjson_mut_obj_add_str(doc, item, "layer",
                                   arch.layers[i].layer ? arch.layers[i].layer : "");
            yyjson_mut_obj_add_str(doc, item, "reason",
                                   arch.layers[i].reason ? arch.layers[i].reason : "");
            yyjson_mut_arr_add_val(layers, item);
        }
        yyjson_mut_obj_add_val(doc, root, "layers", layers);
    }

    /* Clusters (community detection) */
    if (arch.cluster_count > 0) {
        yyjson_mut_val *clusters = yyjson_mut_arr(doc);
        for (int i = 0; i < arch.cluster_count; i++) {
            const cbm_cluster_info_t *c = &arch.clusters[i];
            yyjson_mut_val *item = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_int(doc, item, "id", c->id);
            yyjson_mut_obj_add_str(doc, item, "label", c->label ? c->label : "");
            yyjson_mut_obj_add_int(doc, item, "members", c->members);
            yyjson_mut_obj_add_real(doc, item, "cohesion", c->cohesion);
            yyjson_mut_val *top = yyjson_mut_arr(doc);
            for (int j = 0; j < c->top_node_count; j++) {
                yyjson_mut_arr_add_str(doc, top, c->top_nodes[j] ? c->top_nodes[j] : "");
            }
            yyjson_mut_obj_add_val(doc, item, "top_nodes", top);
            yyjson_mut_val *pkgs = yyjson_mut_arr(doc);
            for (int j = 0; j < c->package_count; j++) {
                yyjson_mut_arr_add_str(doc, pkgs, c->packages[j] ? c->packages[j] : "");
            }
            yyjson_mut_obj_add_val(doc, item, "packages", pkgs);
            yyjson_mut_val *etypes = yyjson_mut_arr(doc);
            for (int j = 0; j < c->edge_type_count; j++) {
                yyjson_mut_arr_add_str(doc, etypes, c->edge_types[j] ? c->edge_types[j] : "");
            }
            yyjson_mut_obj_add_val(doc, item, "edge_types", etypes);
            yyjson_mut_arr_add_val(clusters, item);
        }
        yyjson_mut_obj_add_val(doc, root, "clusters", clusters);
    }

    /* File tree */
    if (arch.file_tree_count > 0) {
        yyjson_mut_val *file_tree = yyjson_mut_arr(doc);
        for (int i = 0; i < arch.file_tree_count; i++) {
            yyjson_mut_val *item = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_str(doc, item, "path",
                                   arch.file_tree[i].path ? arch.file_tree[i].path : "");
            yyjson_mut_obj_add_str(doc, item, "type",
                                   arch.file_tree[i].type ? arch.file_tree[i].type : "");
            yyjson_mut_obj_add_int(doc, item, "children", arch.file_tree[i].children);
            yyjson_mut_arr_add_val(file_tree, item);
        }
        yyjson_mut_obj_add_val(doc, root, "file_tree", file_tree);
    }

    append_cross_repo_summary(doc, root, &schema);

    /* cycles: SCCs of size > 1 in the CALLS graph (same model as tree). */
    if (aspect_explicitly_named(aspects_arr, "cycles")) {
        int64_t **members = NULL;
        int *sizes = NULL;
        int ncyc = 0;
        int total_cyc = 0;
        int scanned = 0;
        bool etrunc = false;
        if (arch_compute_cycles(store, project, &members, &sizes, &ncyc, &total_cyc, &scanned,
                                &etrunc) == CBM_STORE_OK) {
            yyjson_mut_obj_add_int(doc, root, "call_edges_scanned", scanned);
            yyjson_mut_obj_add_int(doc, root, "cycles_total", total_cyc);
            yyjson_mut_obj_add_bool(doc, root, "cycles_partial", etrunc);
            yyjson_mut_val *cyc = yyjson_mut_arr(doc);
            for (int c = 0; c < ncyc; c++) {
                yyjson_mut_val *o = yyjson_mut_obj(doc);
                yyjson_mut_obj_add_int(doc, o, "size", sizes[c]);
                yyjson_mut_val *mem = yyjson_mut_arr(doc);
                int show = sizes[c] < ARCH_SCC_MEMBERS_SHOWN ? sizes[c] : ARCH_SCC_MEMBERS_SHOWN;
                for (int m = 0; m < show; m++) {
                    char qn[CBM_SZ_512];
                    arch_node_qn(store, members[c][m], qn, sizeof(qn));
                    yyjson_mut_arr_add_strcpy(doc, mem, qn);
                }
                yyjson_mut_obj_add_val(doc, o, "members", mem);
                yyjson_mut_arr_add_val(cyc, o);
                free(members[c]);
            }
            yyjson_mut_obj_add_val(doc, root, "cycles", cyc);
            free(members);
            free(sizes);
        }
    }

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    cbm_store_architecture_free(&arch);
    cbm_store_schema_free(&schema);
    if (aspects_doc) {
        yyjson_doc_free(aspects_doc);
    }
    free(project);
    free(scope_path);

    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

/* Check if a file path looks like a test file. */
static bool is_test_file(const char *path) {
    if (!path) {
        return false;
    }
    return strstr(path, "/test") != NULL || strstr(path, "test_") != NULL ||
           strstr(path, "_test.") != NULL || strstr(path, "/tests/") != NULL ||
           strstr(path, "/spec/") != NULL || strstr(path, ".test.") != NULL;
}

/* Convert BFS traversal results into a yyjson_mut array. */
/* Find the CALLS-edge "args" JSON (the serialized arg expressions) on the edge
 * that leads to the given hop node, so data_flow mode can surface argument
 * expressions (#514). Returns the borrowed substring "[...]" inside the edge's
 * properties_json, with its length, or NULL when no args are recorded. */
static const char *bfs_edge_args_for_hop(const cbm_traverse_result_t *tr, int64_t hop_node_id,
                                         size_t *out_len) {
    for (int e = 0; e < tr->edge_count; e++) {
        /* The hop node is the edge endpoint reached from the root side: for an
         * outbound trace it is the target, for inbound it is the source. Match
         * on either so both directions surface their args. */
        if (tr->edges[e].target_id != hop_node_id && tr->edges[e].source_id != hop_node_id) {
            continue;
        }
        const char *pj = tr->edges[e].properties_json;
        if (!pj) {
            continue;
        }
        const char *args = strstr(pj, "\"args\"");
        if (!args) {
            continue;
        }
        const char *open = strchr(args, '[');
        if (!open) {
            continue;
        }
        int depth = 0;
        const char *p = open;
        for (; *p; p++) {
            if (*p == '[') {
                depth++;
            } else if (*p == ']') {
                depth--;
                if (depth == 0) {
                    p++;
                    break;
                }
            }
        }
        *out_len = (size_t)(p - open);
        return open;
    }
    return NULL;
}

/* Stateless trace cursors bind the page watermark to the store generation and
 * every traversal-shaping argument. Reindexing or changing an argument fails
 * loudly instead of resuming against different node identities or results. */
static uint64_t trace_cursor_hash_string(const char *value, uint64_t hash) {
    while (value && *value) {
        hash ^= (uint64_t)(unsigned char)*value++;
        hash *= UINT64_C(0x100000001b3);
    }
    return hash;
}

typedef struct {
    char leg;
    char generation[96];
    uint64_t query_hash;
    int hop;
    int64_t node_id;
} trace_cursor_t;

static uint64_t trace_params_hash(const char *project, const char *symbol,
                                  const char *direction, const char *mode, int depth,
                                  bool include_tests, int limit, const char **edge_types,
                                  int edge_type_count, char **exclude_patterns,
                                  int exclude_count) {
    uint64_t hash = UINT64_C(0xcbf29ce484222325);
    const char *parts[] = {project ? project : "", symbol ? symbol : "",
                           direction ? direction : "", mode ? mode : ""};
    for (size_t i = 0; i < sizeof(parts) / sizeof(parts[0]); i++) {
        hash = trace_cursor_hash_string("|", hash);
        hash = trace_cursor_hash_string(parts[i], hash);
    }
    char numbers[64];
    snprintf(numbers, sizeof(numbers), "|%d|%d|%d", depth, include_tests ? 1 : 0, limit);
    hash = trace_cursor_hash_string(numbers, hash);
    for (int i = 0; i < edge_type_count; i++) {
        hash = trace_cursor_hash_string("|edge:", hash);
        hash = trace_cursor_hash_string(edge_types[i], hash);
    }
    for (int i = 0; i < exclude_count; i++) {
        hash = trace_cursor_hash_string("|exclude:", hash);
        hash = trace_cursor_hash_string(exclude_patterns[i], hash);
    }
    return hash;
}

static void trace_cursor_encode(const trace_cursor_t *cursor, char *out, size_t out_size) {
    snprintf(out, out_size, "c1.%c.%s.%016llx.%d.%lld", cursor->leg,
             cursor->generation, (unsigned long long)cursor->query_hash, cursor->hop,
             (long long)cursor->node_id);
}

static const char *trace_cursor_decode(const char *token, const char *generation,
                                       uint64_t expected_hash, trace_cursor_t *out) {
    memset(out, 0, sizeof(*out));
    if (!token || strncmp(token, "c1.", 3) != 0 ||
        (token[3] != 'o' && token[3] != 'i') || token[4] != '.') {
        return "invalid_cursor: unrecognized token; rerun without cursor";
    }
    out->leg = token[3];
    const char *generation_start = token + 5;
    const char *generation_end = strchr(generation_start, '.');
    if (!generation_end || (size_t)(generation_end - generation_start) >=
                               sizeof(out->generation)) {
        return "invalid_cursor: unrecognized token; rerun without cursor";
    }
    memcpy(out->generation, generation_start,
           (size_t)(generation_end - generation_start));
    out->generation[generation_end - generation_start] = '\0';
    unsigned long long parsed_hash = 0;
    long long parsed_node_id = 0;
    if (sscanf(generation_end + 1, "%16llx.%d.%lld", &parsed_hash, &out->hop,
               &parsed_node_id) != 3) {
        return "invalid_cursor: unrecognized token; rerun without cursor";
    }
    out->query_hash = (uint64_t)parsed_hash;
    out->node_id = (int64_t)parsed_node_id;
    if (out->query_hash != expected_hash) {
        return "cursor_params_mismatch: pass cursor with every other traversal argument unchanged";
    }
    if (strcmp(out->generation, generation) != 0) {
        return "stale_cursor: project was reindexed; rerun without cursor";
    }
    return NULL;
}

static int trace_watermark_index(const cbm_traverse_result_t *tr, int hop,
                                 int64_t node_id) {
    for (int i = 0; i < tr->visited_count; i++) {
        if (tr->visited[i].hop > hop ||
            (tr->visited[i].hop == hop && tr->visited[i].node.id > node_id)) {
            return i;
        }
    }
    return tr->visited_count;
}

static bool trace_row_visible(const cbm_node_hop_t *row, bool include_tests,
                              char **exclude_likes) {
    return (include_tests || !is_test_file(row->node.file_path)) &&
           !path_matches_like_any(row->node.file_path, exclude_likes);
}

static int trace_visible_count(const cbm_traverse_result_t *tr, bool include_tests,
                               char **exclude_likes) {
    int visible = 0;
    for (int i = 0; i < tr->visited_count; i++) {
        if (trace_row_visible(&tr->visited[i], include_tests, exclude_likes)) {
            visible++;
        }
    }
    return visible;
}

/* Return a raw-array end index whose half-open window contains at most
 * visible_limit emitted rows. Hidden test/excluded rows do not consume page
 * budget, so sparse filters cannot create empty or undersized middle pages. */
static int trace_page_end(const cbm_traverse_result_t *tr, int start, int visible_limit,
                          bool include_tests, char **exclude_likes) {
    int visible = 0;
    int end = start;
    while (end < tr->visited_count && visible < visible_limit) {
        if (trace_row_visible(&tr->visited[end], include_tests, exclude_likes)) {
            visible++;
        }
        end++;
    }
    return end;
}

static bool trace_has_visible_after(const cbm_traverse_result_t *tr, int start,
                                    bool include_tests, char **exclude_likes) {
    for (int i = start; i < tr->visited_count; i++) {
        if (trace_row_visible(&tr->visited[i], include_tests, exclude_likes)) {
            return true;
        }
    }
    return false;
}

/* TOON table for one trace direction: callees[N]{qn,hop,...} with optional
 * risk / test / args columns. `name` is omitted (it is the qn's last
 * segment); the per-item JSON key envelope was 84% of the legacy payload. */
static void bfs_to_toon_table(cbm_sb_t *sb, const char *key, cbm_traverse_result_t *tr,
                              bool risk_labels, bool include_tests, bool data_flow) {
    int visible = 0;
    for (int i = 0; i < tr->visited_count; i++) {
        if (!include_tests && is_test_file(tr->visited[i].node.file_path)) {
            continue;
        }
        visible++;
    }
    const char *cols[5] = {"qn", "hop"};
    int ncols = 2;
    if (risk_labels) {
        cols[ncols++] = "risk";
    }
    if (include_tests) {
        cols[ncols++] = "test";
    }
    if (data_flow) {
        cols[ncols++] = "args";
    }
    cbm_toon_table_header(sb, key, visible, cols, ncols);
    for (int i = 0; i < tr->visited_count; i++) {
        const char *fp = tr->visited[i].node.file_path;
        bool test = is_test_file(fp);
        if (!include_tests && test) {
            continue;
        }
        cbm_toon_row_begin(sb);
        cbm_toon_cell_str(sb, tr->visited[i].node.qualified_name, true);
        cbm_toon_cell_int(sb, tr->visited[i].hop, false);
        if (risk_labels) {
            cbm_toon_cell_str(sb, cbm_risk_label(cbm_hop_to_risk(tr->visited[i].hop)), false);
        }
        if (include_tests) {
            cbm_toon_cell_bool(sb, test, false);
        }
        if (data_flow) {
            size_t alen = 0;
            const char *ea = bfs_edge_args_for_hop(tr, tr->visited[i].node.id, &alen);
            if (ea && alen > 0 && alen < CBM_SZ_1K) {
                char abuf[CBM_SZ_1K];
                memcpy(abuf, ea, alen);
                abuf[alen] = '\0';
                cbm_toon_cell_str(sb, abuf, false);
            } else {
                cbm_toon_cell_str(sb, "", false);
            }
        }
        cbm_toon_row_end(sb);
    }
}
static char *snippet_suggestions(const char *input, cbm_node_t *nodes, int count);

/* Rank a candidate for name resolution. The label tier (callable > class-like >
 * module/file) is the primary key; WITHIN a tier the larger definition by line
 * span wins. In practice the .c-over-.h and C-main-over-shell-main preferences
 * come primarily from span (the real definition has the larger body), since the
 * competing matches usually share a tier — no file extension is hardcoded.
 * Consequence: two same-tier candidates with equal span tie and are reported
 * ambiguous (see pick_resolved_node) rather than guessed. */
enum {
    RES_RANK_CALLABLE = 2,     /* Function / Method */
    RES_RANK_OTHER = 1,        /* Class / Struct / etc. */
    RES_RANK_MODULE = 0,       /* Module / File */
    RES_LABEL_WEIGHT = 1000000 /* label tier dominates span */
};
static long node_resolution_score(const cbm_node_t *n) {
    long label_rank = RES_RANK_MODULE;
    if (n->label) {
        if (strcmp(n->label, "Function") == 0 || strcmp(n->label, "Method") == 0) {
            label_rank = RES_RANK_CALLABLE;
        } else if (strcmp(n->label, "Module") != 0 && strcmp(n->label, "File") != 0) {
            label_rank = RES_RANK_OTHER;
        }
    }
    long span = (long)n->end_line - (long)n->start_line;
    if (span < 0) {
        span = 0;
    }
    return label_rank * (long)RES_LABEL_WEIGHT + span;
}

/* A "real" callable definition: a Function/Method node with a non-empty body
 * span (end_line > start_line). A body-less node (start_line == end_line) is an
 * ambient declaration / signature stub — e.g. a TypeScript `.d.ts` declaration
 * — which is a *fragment* of one logical symbol, not a distinct definition. The
 * distinction lets pick_resolved_node union a stub with its real implementation
 * (#546) while still treating two genuinely-different same-named functions as
 * ambiguous rather than conflating their caller sets. */
static bool node_is_real_callable_def(const cbm_node_t *n) {
    if (!n->label) {
        return false;
    }
    if (strcmp(n->label, "Function") != 0 && strcmp(n->label, "Method") != 0) {
        return false;
    }
    return (long)n->end_line - (long)n->start_line > 0;
}

/* Pick the best-resolving node among name matches. Sets *ambiguous when the
 * matches can't be reduced to one logical symbol, so resolution never silently
 * traces (or conflates) the wrong same-named node:
 *   1. the top score is shared by >1 candidate (a genuine rank/span tie), or
 *   2. two or more *real* callable definitions share the name — distinct
 *      implementations, not a definition plus its body-less stub(s).
 * Rule 2 completes rule 1: without it, two same-named functions whose bodies
 * differ in length score differently, dodge the tie, and get their caller sets
 * unioned by bfs_union_same_name (#546) into one confidently-conflated answer.
 * Body-less .d.ts stubs still union with their implementation (#650). */
static int pick_resolved_node(const cbm_node_t *nodes, int count, bool *ambiguous,
                              cbm_store_t *store) {
    *ambiguous = false;
    if (count <= 1) {
        return 0;
    }
    int best = 0;
    long best_score = node_resolution_score(&nodes[0]);
    for (int i = 1; i < count; i++) {
        long s = node_resolution_score(&nodes[i]);
        if (s > best_score) {
            best_score = s;
            best = i;
        }
    }
    /* Collect all candidates sharing the top score (a label+span tie). */
    int tied_idx[CBM_SZ_64];
    int tie_count = 0;
    int real_def_count = 0;
    for (int i = 0; i < count; i++) {
        if (node_resolution_score(&nodes[i]) == best_score && tie_count < CBM_SZ_64) {
            tied_idx[tie_count++] = i;
        }
        if (node_is_real_callable_def(&nodes[i])) {
            real_def_count++;
        }
    }
    if (real_def_count > 1) {
        *ambiguous = true;
    }
    if (tie_count > 1) {
        /* Degree tiebreaker: prefer the node with the most graph edges.
         * This resolves cases where one candidate is a real definition (in a
         * call chain) and the other is an isolated stub, without which two
         * same-span functions could never be distinguished. */
        int best_deg = -1;
        int best_tied = tied_idx[0];
        bool deg_tie = true;
        for (int t = 0; t < tie_count; t++) {
            int in_d = 0, out_d = 0;
            if (store && nodes[tied_idx[t]].id > 0) {
                cbm_store_node_degree(store, nodes[tied_idx[t]].id, &in_d, &out_d);
            }
            int deg = in_d + out_d;
            if (deg > best_deg) {
                best_deg = deg;
                best_tied = tied_idx[t];
                deg_tie = false;
            } else if (deg == best_deg) {
                deg_tie = true;
            }
        }
        best = best_tied;
        /* Only genuinely ambiguous when degree ALSO ties (e.g. both isolated). */
        if (deg_tie) {
            *ambiguous = true;
        }
    }
    return best;
}

static void trace_append_nodes(cbm_mcp_server_t *srv, yyjson_mut_doc *doc, yyjson_mut_val *arr,
                               const cbm_traverse_result_t *tr, bool compact,
                               bool include_tests, bool risk_labels, bool data_flow,
                               char **exclude_likes) {
    /* yyjson borrows node strings here; callers must serialize before
     * cbm_store_traverse_free(). */
    int64_t *seen = calloc((size_t)tr->visited_count + SKIP_ONE, sizeof(int64_t));
    const char **seen_qn = calloc((size_t)tr->visited_count + SKIP_ONE, sizeof(char *));
    int seen_count = 0;
    for (int i = 0; i < tr->visited_count; i++) {
        const cbm_node_hop_t *hop = &tr->visited[i];
        bool is_test = cbm_is_test_file_path(hop->node.file_path);
        if (!include_tests && is_test) {
            continue;
        }
        if (path_matches_like_any(hop->node.file_path, exclude_likes)) {
            continue;
        }
        if (seen) {
            bool dup = false;
            for (int j = 0; j < seen_count; j++) {
                if (hop->node.id > CBM_STORE_NO_NODE_ID && seen[j] == hop->node.id) {
                    dup = true;
                    break;
                }
                if (hop->node.id <= CBM_STORE_NO_NODE_ID && seen[j] <= CBM_STORE_NO_NODE_ID &&
                    hop->node.qualified_name && seen_qn && seen_qn[j] &&
                    strcmp(seen_qn[j], hop->node.qualified_name) == 0) {
                    dup = true;
                    break;
                }
            }
            if (dup) {
                continue;
            }
            seen[seen_count++] = hop->node.id;
            if (seen_qn) {
                seen_qn[seen_count - 1] = hop->node.qualified_name;
            }
        }

        yyjson_mut_val *item = yyjson_mut_obj(doc);
        if ((!compact || !ends_with_segment(hop->node.qualified_name, hop->node.name)) &&
            hop->node.name && hop->node.name[0]) {
            yyjson_mut_obj_add_str(doc, item, "name", hop->node.name);
        }
        yyjson_mut_obj_add_str(doc, item, "qualified_name",
                               hop->node.qualified_name ? hop->node.qualified_name : "");
        yyjson_mut_obj_add_int(doc, item, "hop", hop->hop);
        if (risk_labels) {
            yyjson_mut_obj_add_str(doc, item, "risk", cbm_risk_label(cbm_hop_to_risk(hop->hop)));
        }
        if (is_test) {
            yyjson_mut_obj_add_bool(doc, item, "is_test", true);
        }
        if (data_flow) {
            size_t args_len = 0;
            const char *edge_args = bfs_edge_args_for_hop(tr, hop->node.id, &args_len);
            if (edge_args && args_len > 0) {
                yyjson_mut_val *args_value = yyjson_mut_rawn(doc, edge_args, args_len);
                if (args_value) {
                    yyjson_mut_obj_add_val(doc, item, "args", args_value);
                }
            }
        }
        if (hop->pagerank_score > 0.0) {
            add_pagerank_val(doc, item, hop->pagerank_score);
        }
        bool dep_node = cbm_is_dep_project(hop->node.project, srv->session_project);
        yyjson_mut_obj_add_str(doc, item, "source", dep_node ? "dependency" : "project");
        if (dep_node) {
            yyjson_mut_obj_add_bool(doc, item, "read_only", true);
        }
        yyjson_mut_arr_add_val(arr, item);
    }
    free(seen_qn);
    free(seen);
}

static int node_hop_cmp_hop_id(const void *left, const void *right) {
    const cbm_node_hop_t *a = left;
    const cbm_node_hop_t *b = right;
    if (a->hop != b->hop) {
        return a->hop < b->hop ? -1 : 1;
    }
    if (a->node.id != b->node.id) {
        return a->node.id < b->node.id ? -1 : 1;
    }
    return 0;
}

/* Union traversal fragments for one logical symbol (#546), transferring each
 * unique node/edge exactly once so the shared traversal destructor owns all
 * retained allocations. */
static void bfs_union_same_name(cbm_store_t *store, const cbm_node_t *nodes, int node_count,
                                const char *direction, const char **edge_types,
                                int edge_type_count, int depth, int max_results,
                                cbm_traverse_result_t *out) {
    memset(out, 0, sizeof(*out));
    int visited_capacity = 0;
    int edge_capacity = 0;
    for (int node_index = 0; node_index < node_count; node_index++) {
        cbm_traverse_result_t traversal = {0};
        cbm_store_bfs(store, nodes[node_index].id, direction, edge_types, edge_type_count,
                      depth, max_results, &traversal);
        out->pagerank_stale = out->pagerank_stale || traversal.pagerank_stale;
        out->linkrank_stale = out->linkrank_stale || traversal.linkrank_stale;
        for (int i = 0; i < traversal.visited_count; i++) {
            bool duplicate = false;
            for (int j = 0; j < out->visited_count; j++) {
                if (out->visited[j].node.id == traversal.visited[i].node.id) {
                    if (traversal.visited[i].hop < out->visited[j].hop) {
                        out->visited[j].hop = traversal.visited[i].hop;
                    }
                    duplicate = true;
                    break;
                }
            }
            if (duplicate) {
                continue;
            }
            if (out->visited_count >= visited_capacity) {
                visited_capacity =
                    visited_capacity ? visited_capacity * 2 : CBM_SZ_8;
                out->visited =
                    safe_realloc(out->visited,
                                 (size_t)visited_capacity * sizeof(*out->visited));
            }
            out->visited[out->visited_count++] = traversal.visited[i];
            memset(&traversal.visited[i], 0, sizeof(traversal.visited[i]));
        }
        for (int i = 0; i < traversal.edge_count; i++) {
            bool duplicate = false;
            for (int j = 0; j < out->edge_count; j++) {
                duplicate =
                    out->edges[j].source_id == traversal.edges[i].source_id &&
                    out->edges[j].target_id == traversal.edges[i].target_id &&
                    strcmp(out->edges[j].type ? out->edges[j].type : "",
                           traversal.edges[i].type ? traversal.edges[i].type : "") == 0;
                if (duplicate) {
                    break;
                }
            }
            if (duplicate) {
                continue;
            }
            if (out->edge_count >= edge_capacity) {
                edge_capacity = edge_capacity ? edge_capacity * 2 : CBM_SZ_8;
                out->edges =
                    safe_realloc(out->edges, (size_t)edge_capacity * sizeof(*out->edges));
            }
            out->edges[out->edge_count++] = traversal.edges[i];
            memset(&traversal.edges[i], 0, sizeof(traversal.edges[i]));
        }
        cbm_store_traverse_free(&traversal);
    }
    if (out->visited_count > 1) {
        qsort(out->visited, (size_t)out->visited_count, sizeof(*out->visited),
              node_hop_cmp_hop_id);
    }
}

/* Never silently exceed the configured traversal ceiling (#887). */
static int clamp_mcp_depth(int depth, const char *tool_name) {
    int depth_limit = cbm_mcp_max_depth();
    if (depth > depth_limit) {
        char requested[CBM_SZ_16];
        char limit[CBM_SZ_16];
        snprintf(requested, sizeof(requested), "%d", depth);
        snprintf(limit, sizeof(limit), "%d", depth_limit);
        cbm_log_warn("mcp.depth_capped", "tool", tool_name, "requested", requested,
                     "cap", limit);
        return depth_limit;
    }
    return depth;
}

static char *handle_trace_path(cbm_mcp_server_t *srv, const char *args) {
    cbm_mcp_output_format_t response_format = cbm_mcp_response_format(srv, args);
    if (response_format == CBM_MCP_OUTPUT_INVALID) {
        return cbm_mcp_invalid_response_format();
    }
    char *func_name = cbm_mcp_get_string_arg(args, "function_name");
    char *qn_input = cbm_mcp_get_string_arg(args, "qualified_name"); /* cross-tool chaining */
    char *raw_project = get_store_project_arg(args);
    char *direction = cbm_mcp_get_string_arg(args, "direction");
    char *trace_mode = cbm_mcp_get_string_arg(args, "mode"); /* calls|data_flow|cross_service */
    char *param_name = cbm_mcp_get_string_arg(args, "parameter_name");
    int depth = cbm_mcp_get_int_arg(args, "depth", MCP_DEFAULT_DEPTH);
    /* F10 and #887: keep traversal within the shared MCP bounds. */
    if (depth < 1) {
        depth = 1;
    }
    depth = clamp_mcp_depth(depth, "trace_path");
    int cfg_trace_max = cbm_config_get_int(srv->config, CBM_CONFIG_TRACE_MAX_RESULTS,
                                            CBM_DEFAULT_TRACE_MAX_RESULTS);
    int max_results = cbm_mcp_get_positive_int_arg(args, "max_results", cfg_trace_max,
                                                   CBM_DEFAULT_TRACE_MAX_RESULTS);
    int trace_limit = cbm_mcp_get_positive_int_arg(args, "limit", max_results, max_results);
    if (trace_limit > MCP_BFS_LIMIT_MAX) {
        trace_limit = MCP_BFS_LIMIT_MAX;
    }
    bool cfg_compact_t = cbm_config_get_bool(srv->config, "compact", true);
    bool compact = cbm_mcp_get_bool_arg_default(args, "compact", cfg_compact_t);
    bool include_tests = cbm_mcp_get_bool_arg(args, "include_tests");
    bool risk_labels = cbm_mcp_get_bool_arg(args, "risk_labels");
    int exclude_count = 0;
    char **exclude_patterns = cbm_mcp_get_string_array_arg(args, "exclude", &exclude_count);
    char **exclude_likes = glob_patterns_to_like(exclude_patterns, exclude_count);

    /* Validate cheap request parameters before resolving the project. Project resolution can
     * auto-index on first use, so malformed requests should fail without side effects. */
    if (exclude_count < 0 || (exclude_count > 0 && !exclude_likes)) {
        free(func_name);
        free(qn_input);
        free(raw_project);
        free(direction);
        free(trace_mode);
        free(param_name);
        free_string_array(exclude_patterns);
        return cbm_mcp_text_result(
            "{\"error\":\"out of memory preparing exclude patterns\","
            "\"hint\":\"Retry with fewer exclude patterns or a smaller request.\"}", true);
    }

    if (!func_name && !qn_input) {
        free(raw_project);
        free(direction);
        free(trace_mode);
        free(param_name);
        free_string_array(exclude_patterns);
        free_string_array(exclude_likes);
        free(qn_input);
        return cbm_mcp_text_result(
            "{\"error\":\"function_name or qualified_name is required\","
            "\"hint\":\"Pass the name of a function to trace, e.g. {\\\"function_name\\\":\\\"main\\\"}\"}", true);
    }

    /* Validate direction enum */
    if (direction && strcmp(direction, "inbound") != 0 &&
        strcmp(direction, "outbound") != 0 && strcmp(direction, "both") != 0) {
        char errbuf[256];
        snprintf(errbuf, sizeof(errbuf),
            "{\"error\":\"invalid direction '%s'\","
            "\"hint\":\"Valid values: inbound, outbound, both\"}", direction);
        free(func_name);
        free(qn_input);
        free(raw_project);
        free(direction);
        free(trace_mode);
        free(param_name);
        free_string_array(exclude_patterns);
        free_string_array(exclude_likes);
        return cbm_mcp_text_result(errbuf, true);
    }
    if (trace_mode && strcmp(trace_mode, "calls") != 0 &&
        strcmp(trace_mode, "data_flow") != 0 &&
        strcmp(trace_mode, "cross_service") != 0) {
        char errbuf[256];
        snprintf(errbuf, sizeof(errbuf),
            "{\"error\":\"invalid mode '%s'\","
            "\"hint\":\"Valid values: calls, data_flow, cross_service\"}", trace_mode);
        free(func_name);
        free(qn_input);
        free(raw_project);
        free(direction);
        free(trace_mode);
        free(param_name);
        free_string_array(exclude_patterns);
        free_string_array(exclude_likes);
        return cbm_mcp_text_result(errbuf, true);
    }

    project_expand_t pe = {0};
    cbm_store_t *store = resolve_project_store(srv, raw_project, &pe);
    char *project = pe.value; /* take ownership for free() below; raw_project consumed */
    if (!store) {
        char *_err = build_project_list_error("project not found or not indexed");
        char *_res = cbm_mcp_text_result(_err, true);
        free(_err);
        free(func_name);
        free(qn_input);
        free(project);
        free(direction);
        free(trace_mode);
        free(param_name);
        free_string_array(exclude_patterns);
        free_string_array(exclude_likes);
        return _res;
    }
    const char *effective_direction = direction ? direction : "both";
    cbm_store_overlay_node_view_summary_t overlay_summary = {0};
    bool overlay_ready_for_trace =
        (qn_input || func_name) && project && project[0] &&
        cbm_store_get_overlay_node_view_summary(store, project, &overlay_summary) ==
            CBM_STORE_OK &&
        cbm_store_overlay_node_view_has_ready_rows(&overlay_summary);

    /* QN-first lookup: if qualified_name provided, resolve to node directly */
    cbm_node_t *qn_node = NULL;
    if (qn_input && store) {
        cbm_node_t qn_tmp = {0};
        int qn_rc = overlay_ready_for_trace
                        ? cbm_store_find_node_by_qn_overlay_view(store, project, qn_input,
                                                                 &qn_tmp)
                        : cbm_store_find_node_by_qn(store, project, qn_input, &qn_tmp);
        if (qn_rc == CBM_STORE_OK) {
            qn_node = calloc(1, sizeof(cbm_node_t));
            if (qn_node) {
                *qn_node = qn_tmp;  /* shallow copy; ownership of heap fields transferred */
            } else {
                free_node_contents(&qn_tmp);  /* OOM: free fields to avoid leak */
            }
        }
    }

    /* Find the node by name */
    cbm_node_t *nodes = NULL;
    int node_count = 0;
    if (qn_node) {
        /* Use QN-resolved node directly */
        nodes = qn_node;
        node_count = 1;
    } else {
        if (overlay_ready_for_trace) {
            cbm_store_find_nodes_by_name_overlay_view(store, project,
                func_name ? func_name : (qn_input ? qn_input : ""), &nodes, &node_count);
        } else {
            cbm_store_find_nodes_by_name(store, project,
                func_name ? func_name : (qn_input ? qn_input : ""), &nodes, &node_count);
        }
    }

    if (node_count == 0 && func_name) {
        /* Fallback: case-insensitive substring search via cbm_store_search.
         * Only fires when exact match misses — zero overhead on hit.
         * Skipped when func_name is NULL (only qualified_name given): QN lookup already ran
         * and returned nothing; falling back with name_pattern=NULL would return random nodes. */
        cbm_search_params_t sp = {0};
        fill_project_params(&pe, &sp);
        sp.name_pattern = func_name;
        sp.case_sensitive = false;
        sp.limit = 5;
        sp.min_degree = -1;
        sp.max_degree = -1;
        cbm_search_output_t sout = {0};
        int search_rc = overlay_ready_for_trace ? cbm_store_search_overlay_view(store, &sp, &sout)
                                                : cbm_store_search(store, &sp, &sout);
        if (search_rc == 0 && sout.count > 0) {
            const char *found_project = sout.results[0].node.project;
            /* Free the empty allocation from the first exact-name lookup before
             * overwriting nodes/node_count with the fallback result. */
            cbm_store_free_nodes(nodes, node_count);
            nodes = NULL;
            node_count = 0;
            if (overlay_ready_for_trace) {
                cbm_store_find_nodes_by_name_overlay_view(
                    store, found_project ? found_project : project,
                    sout.results[0].node.name, &nodes, &node_count);
            } else {
                cbm_store_find_nodes_by_name(store,
                                             found_project ? found_project : project,
                                             sout.results[0].node.name, &nodes, &node_count);
            }
        }
        cbm_store_search_free(&sout);
    }
    if (node_count == 0) {
        char errbuf[512];
        if (qn_input && !func_name) {
            snprintf(errbuf, sizeof(errbuf),
                "{\"error\":\"function not found for qualified_name: '%s'\","
                "\"hint\":\"Use search_graph with pattern= to find the correct qualified_name, "
                "then pass it here.\"}",
                qn_input);
        } else {
            snprintf(errbuf, sizeof(errbuf),
                "{\"error\":\"function not found: '%s'\","
                "\"hint\":\"Use search_graph with name_pattern to find similar symbols.\"}",
                func_name ? func_name : "");
        }
        free(func_name);
        free(qn_input);
        free(project);
        free(direction);
        free(trace_mode);
        free(param_name);
        free_string_array(exclude_patterns);
        free_string_array(exclude_likes);
        cbm_store_free_nodes(nodes, node_count);
        return cbm_mcp_text_result(errbuf, true);
    }

    /* Disambiguate same-named matches: prefer the real definition, and report
     * ambiguity (rather than silently tracing nodes[0]) on a genuine tie — e.g.
     * two same-named functions with equal body span. */
    bool trace_ambiguous = false;
    int sel = pick_resolved_node(nodes, node_count, &trace_ambiguous, store);
    if (trace_ambiguous) {
        char *result = snippet_suggestions(func_name ? func_name : (qn_input ? qn_input : ""),
                                           nodes, node_count);
        free(func_name);
        free(qn_input);
        free(project);
        free(direction);
        free(trace_mode);
        free(param_name);
        free_string_array(exclude_patterns);
        free_string_array(exclude_likes);
        cbm_store_free_nodes(nodes, node_count);
        return result;
    }

    /* Response encoding: TOON tables by default; format:"json" restores the
     * legacy verbose per-hop objects. */
    bool trace_legacy_json = response_format == CBM_MCP_OUTPUT_JSON;

    /* Extract edge_types here — after all early returns — to avoid memory leaks.
     * free_string_array(NULL) is NULL-safe.
     * Resolution order: explicit edge_types array > mode-based defaults > CALLS. */
    int edge_type_count_user = 0;
    char **edge_types_user = cbm_mcp_get_string_array_arg(args, "edge_types",
                                                           &edge_type_count_user);
    if (edge_type_count_user < 0) {
        cbm_store_free_nodes(nodes, node_count);
        free(func_name);
        free(qn_input);
        free(project);
        free(direction);
        free(trace_mode);
        free(param_name);
        free_string_array(exclude_patterns);
        free_string_array(exclude_likes);
        return cbm_mcp_text_result(
            "{\"error\":\"out of memory preparing edge_types\","
            "\"hint\":\"Retry with fewer edge_types or a smaller request.\"}", true);
    }
    const char **edge_types;
    int edge_type_count;
    /* Mode-based default edge sets (stack-local; no ownership transfer). */
    static const char *mode_calls[] = {"CALLS"};
    static const char *mode_data_flow[] = {"CALLS", "DATA_FLOWS"};
    static const char *mode_cross_svc[] = {
        "HTTP_CALLS",          "ASYNC_CALLS",       "DATA_FLOWS",    "CALLS",
        "CROSS_HTTP_CALLS",    "CROSS_ASYNC_CALLS", "CROSS_CHANNEL", "CROSS_GRPC_CALLS",
        "CROSS_GRAPHQL_CALLS", "CROSS_TRPC_CALLS"};
    if (edge_type_count_user > 0) {
        edge_types = (const char **)edge_types_user;
        edge_type_count = edge_type_count_user;
    } else if (trace_mode && strcmp(trace_mode, "data_flow") == 0) {
        edge_types = mode_data_flow;
        edge_type_count = (int)(sizeof(mode_data_flow) / sizeof(mode_data_flow[0]));
    } else if (trace_mode && strcmp(trace_mode, "cross_service") == 0) {
        edge_types = mode_cross_svc;
        edge_type_count = (int)(sizeof(mode_cross_svc) / sizeof(mode_cross_svc[0]));
    } else {
        edge_types = mode_calls;
        edge_type_count = 1;
    }

    char generation[96] = "legacy";
    (void)cbm_store_generation(store, generation, sizeof(generation));
    bool legacy_generation = strcmp(generation, "legacy") == 0;
    char *cursor_arg = cbm_mcp_get_string_arg(args, "cursor");
    trace_cursor_t cursor = {0};
    bool have_cursor = cursor_arg && cursor_arg[0];
    uint64_t query_hash = trace_params_hash(
        project, qn_input ? qn_input : func_name, effective_direction, trace_mode, depth,
        include_tests, trace_limit, edge_types, edge_type_count, exclude_patterns, exclude_count);
    const char *cursor_error = NULL;
    if (have_cursor && legacy_generation) {
        cursor_error = "cursor_unsupported: reindex this project to enable generation-safe pagination";
    } else if (have_cursor) {
        cursor_error = trace_cursor_decode(cursor_arg, generation, query_hash, &cursor);
    }
    free(cursor_arg);
    if (cursor_error) {
        cbm_store_free_nodes(nodes, node_count);
        free(func_name);
        free(qn_input);
        free(project);
        free(direction);
        free(trace_mode);
        free(param_name);
        free_string_array(exclude_patterns);
        free_string_array(exclude_likes);
        free_string_array(edge_types_user);
        return cbm_mcp_text_result(cursor_error, true);
    }

    /* Run BFS for each requested direction.
     * IMPORTANT: yyjson_mut_obj_add_str borrows pointers — we must keep
     * traversal results alive until after yy_doc_to_str serialization. */
    bool do_outbound = strcmp(effective_direction, "outbound") == 0 ||
                       strcmp(effective_direction, "both") == 0;
    bool do_inbound = strcmp(effective_direction, "inbound") == 0 ||
                      strcmp(effective_direction, "both") == 0;

    cbm_traverse_result_t tr_out = {0};
    cbm_traverse_result_t tr_in = {0};
    bool overlay_trace_requested =
        overlay_ready_for_trace && nodes[sel].qualified_name &&
        nodes[sel].qualified_name[0];
    bool overlay_trace_succeeded = false;

    bool data_flow = trace_mode && strcmp(trace_mode, "data_flow") == 0;

    if (do_outbound) {
        int bfs_rc = CBM_STORE_OK;
        if (overlay_trace_requested) {
            bfs_rc = cbm_store_bfs_overlay_view(store, project, nodes[sel].qualified_name,
                                                "outbound", edge_types, edge_type_count,
                                                depth, MCP_BFS_LIMIT_MAX, &tr_out);
            overlay_trace_succeeded = bfs_rc == CBM_STORE_OK;
        } else {
            bfs_union_same_name(store, nodes, node_count, "outbound", edge_types,
                                edge_type_count, depth, MCP_BFS_LIMIT_MAX, &tr_out);
        }
    }
    if (do_inbound) {
        int bfs_rc = CBM_STORE_OK;
        if (overlay_trace_requested) {
            bfs_rc = cbm_store_bfs_overlay_view(store, project, nodes[sel].qualified_name,
                                                "inbound", edge_types, edge_type_count,
                                                depth, MCP_BFS_LIMIT_MAX, &tr_in);
            overlay_trace_succeeded = overlay_trace_succeeded || bfs_rc == CBM_STORE_OK;
        } else {
            bfs_union_same_name(store, nodes, node_count, "inbound", edge_types,
                                edge_type_count, depth, MCP_BFS_LIMIT_MAX, &tr_in);
        }
    }

    int out_total = trace_visible_count(&tr_out, include_tests, exclude_likes);
    int in_total = trace_visible_count(&tr_in, include_tests, exclude_likes);
    int out_start = 0;
    int in_start = 0;
    if (have_cursor) {
        if (cursor.leg == 'o') {
            out_start = trace_watermark_index(&tr_out, cursor.hop, cursor.node_id);
        } else {
            out_start = tr_out.visited_count;
            in_start = trace_watermark_index(&tr_in, cursor.hop, cursor.node_id);
        }
    }

    int page_budget = trace_limit;
    int out_end = out_start;
    if (do_outbound) {
        out_end = trace_page_end(&tr_out, out_start, page_budget, include_tests, exclude_likes);
        cbm_traverse_result_t counted = tr_out;
        counted.visited += out_start;
        counted.visited_count = out_end - out_start;
        page_budget -= trace_visible_count(&counted, include_tests, exclude_likes);
    }
    int in_end = in_start;
    if (do_inbound && page_budget > 0) {
        in_end = trace_page_end(&tr_in, in_start, page_budget, include_tests, exclude_likes);
    }

    bool out_more = do_outbound &&
                    trace_has_visible_after(&tr_out, out_end, include_tests, exclude_likes);
    bool in_more = do_inbound &&
                   trace_has_visible_after(&tr_in, in_end, include_tests, exclude_likes);
    bool more_rows = out_more || in_more;
    cbm_traverse_result_t view_out = tr_out;
    if (view_out.visited) {
        view_out.visited += out_start;
    }
    view_out.visited_count = out_end - out_start;
    cbm_traverse_result_t view_in = tr_in;
    if (view_in.visited) {
        view_in.visited += in_start;
    }
    view_in.visited_count = in_end - in_start;

    char next_cursor[192] = "";
    if (more_rows && !legacy_generation) {
        trace_cursor_t next = {.query_hash = query_hash};
        snprintf(next.generation, sizeof(next.generation), "%s", generation);
        if (view_in.visited_count > 0) {
            const cbm_node_hop_t *last = &view_in.visited[view_in.visited_count - 1];
            next.leg = 'i';
            next.hop = last->hop;
            next.node_id = last->node.id;
        } else if (view_out.visited_count > 0) {
            const cbm_node_hop_t *last = &view_out.visited[view_out.visited_count - 1];
            next.leg = 'o';
            next.hop = last->hop;
            next.node_id = last->node.id;
        }
        if (next.leg) {
            trace_cursor_encode(&next, next_cursor, sizeof(next_cursor));
        }
    }

    int dirty_pending = 0;
    int dirty_overlay_ready = 0;
    bool has_dirty_counts =
        get_dirty_file_counts(store, project, &dirty_pending, &dirty_overlay_ready);
    bool derived_stale =
        (do_outbound && (tr_out.pagerank_stale || tr_out.linkrank_stale)) ||
        (do_inbound && (tr_in.pagerank_stale || tr_in.linkrank_stale));
    trace_legacy_json = trace_legacy_json || overlay_trace_requested || derived_stale ||
                        exclude_count > 0 || !compact || node_count > 1 ||
                        (has_dirty_counts && (dirty_pending > 0 || dirty_overlay_ready > 0));

    const char *display_name = func_name ? func_name : (qn_input ? qn_input : "");
    char *json = NULL;
    if (!trace_legacy_json) {
        cbm_sb_t sb;
        cbm_sb_init(&sb);
        cbm_toon_scalar_str(&sb, "function", display_name);
        cbm_toon_scalar_str(&sb, "direction", effective_direction);
        if (trace_mode) {
            cbm_toon_scalar_str(&sb, "mode", trace_mode);
        }
        if (do_outbound) {
            cbm_toon_scalar_int(&sb, "callees_total", out_total);
            bfs_to_toon_table(&sb, "callees", &view_out, risk_labels, include_tests, data_flow);
        }
        if (do_inbound) {
            cbm_toon_scalar_int(&sb, "callers_total", in_total);
            bfs_to_toon_table(&sb, "callers", &view_in, risk_labels, include_tests, data_flow);
        }
        if (more_rows) {
            cbm_toon_scalar_bool(&sb, "truncated", true);
            if (next_cursor[0]) {
                cbm_toon_scalar_str(&sb, "next", next_cursor);
                cbm_toon_scalar_str(&sb, "hint",
                                    "More rows exist; pass next as cursor with every other "
                                    "traversal argument unchanged.");
            } else {
                cbm_toon_scalar_str(&sb, "hint",
                                    "More rows exist; reindex to enable safe cursors or raise limit.");
            }
        }
        json = cbm_sb_finish(&sb);
    } else {
        yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
        yyjson_mut_val *root = yyjson_mut_obj(doc);
        yyjson_mut_doc_set_root(doc, root);
        yyjson_mut_obj_add_str(doc, root, "function", display_name);
        yyjson_mut_obj_add_str(doc, root, "direction", effective_direction);
        if (trace_mode) {
            yyjson_mut_obj_add_str(doc, root, "mode", trace_mode);
        }
        if (node_count > 1) {
            yyjson_mut_val *candidates = yyjson_mut_arr(doc);
            int candidate_limit =
                node_count < MCP_TRACE_CANDIDATE_LIMIT ? node_count
                                                       : MCP_TRACE_CANDIDATE_LIMIT;
            for (int i = 0; i < candidate_limit; i++) {
                yyjson_mut_val *candidate = yyjson_mut_obj(doc);
                yyjson_mut_obj_add_str(doc, candidate, "qualified_name",
                                       nodes[i].qualified_name ? nodes[i].qualified_name : "");
                if (nodes[i].file_path && nodes[i].file_path[0]) {
                    yyjson_mut_obj_add_str(doc, candidate, "file_path", nodes[i].file_path);
                }
                yyjson_mut_arr_append(candidates, candidate);
            }
            yyjson_mut_obj_add_val(doc, root, "candidates", candidates);
            yyjson_mut_obj_add_str(doc, root, "resolved",
                                   nodes[sel].qualified_name ? nodes[sel].qualified_name : "");
        }
        if (do_outbound) {
            yyjson_mut_val *callees = yyjson_mut_arr(doc);
            trace_append_nodes(srv, doc, callees, &view_out, compact, include_tests,
                               risk_labels, data_flow, exclude_likes);
            yyjson_mut_obj_add_val(doc, root, "callees", callees);
            yyjson_mut_obj_add_int(doc, root, "callees_total", out_total);
        }
        if (do_inbound) {
            yyjson_mut_val *callers = yyjson_mut_arr(doc);
            trace_append_nodes(srv, doc, callers, &view_in, compact, include_tests,
                               risk_labels, data_flow, exclude_likes);
            yyjson_mut_obj_add_val(doc, root, "callers", callers);
            yyjson_mut_obj_add_int(doc, root, "callers_total", in_total);
        }
        if (more_rows) {
            yyjson_mut_obj_add_bool(doc, root, "truncated", true);
            if (next_cursor[0]) {
                yyjson_mut_obj_add_strcpy(doc, root, "next_cursor", next_cursor);
            }
        }
        add_derived_freshness_warnings(
            doc, root,
            (do_outbound && tr_out.pagerank_stale) || (do_inbound && tr_in.pagerank_stale),
            (do_outbound && tr_out.linkrank_stale) || (do_inbound && tr_in.linkrank_stale),
            false);
        if (overlay_trace_succeeded) {
            add_overlay_active_trace_freshness(doc, root, &overlay_summary);
        }
        if (has_dirty_counts) {
            add_dirty_file_freshness_counts(doc, root, dirty_pending, dirty_overlay_ready);
            if (!overlay_trace_succeeded) {
                add_response_warning(doc, root,
                                     "trace_path reads canonical graph rows; dirty file changes "
                                     "may be absent until overlay or reindex completes.");
            }
        }
        if (srv->session_project[0]) {
            yyjson_mut_obj_add_str(doc, root, "session_project", srv->session_project);
        }
        json = yy_doc_to_str(doc);
        yyjson_mut_doc_free(doc);
    }

    /* Now safe to free traversal data */
    if (do_outbound) {
        cbm_store_traverse_free(&tr_out);
    }
    if (do_inbound) {
        cbm_store_traverse_free(&tr_in);
    }

    cbm_store_free_nodes(nodes, node_count);
    free(func_name);
    free(qn_input);
    free(project);
    free(direction);
    free(trace_mode);
    free(param_name);
    free_string_array(exclude_patterns);
    free_string_array(exclude_likes);
    free_string_array(edge_types_user);

    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

/* ── Helper: free heap fields of a stack-allocated node ────────── */

static void free_node_contents(cbm_node_t *n) {
    safe_str_free(&n->project);
    safe_str_free(&n->label);
    safe_str_free(&n->name);
    safe_str_free(&n->qualified_name);
    safe_str_free(&n->file_path);
    safe_str_free(&n->properties_json);
    memset(n, 0, sizeof(*n));
}

/* ── Helper: read lines [start, end] from a file ─────────────── */

static char *read_file_lines(const char *path, int start, int end) {
    FILE *fp = cbm_fopen(path, "r");
    if (!fp) {
        return NULL;
    }

    size_t cap = CBM_SZ_4K;
    char *buf = malloc(cap);
    size_t len = 0;
    buf[0] = '\0';

    char line[CBM_SZ_2K];
    int lineno = 0;
    while (fgets(line, sizeof(line), fp)) {
        lineno++;
        if (lineno < start) {
            continue;
        }
        if (lineno > end) {
            break;
        }
        size_t ll = strlen(line);
        while (len + ll + SKIP_ONE > cap) {
            cap *= PAIR_LEN;
            buf = safe_realloc(buf, cap);
        }
        memcpy(buf + len, line, ll);
        len += ll;
        buf[len] = '\0';
    }

    (void)fclose(fp);
    if (len == 0) {
        free(buf);
        return NULL;
    }
    return buf;
}

/* ── Helper: get project root_path from store ─────────────────── */

static char *get_project_root_from_store(cbm_store_t *store, const char *project) {
    if (!store || !project || !project[0]) {
        return NULL;
    }
    cbm_project_t proj = {0};
    if (cbm_store_get_project(store, project, &proj) != CBM_STORE_OK) {
        return NULL;
    }
    char *root = heap_strdup(proj.root_path);
    free((void *)proj.name);
    free((void *)proj.indexed_at);
    free((void *)proj.root_path);
    return root;
}

static char *get_project_root(cbm_mcp_server_t *srv, const char *project) {
    /* Resolve the project slug: accept either a slug or a filesystem path.
     * Also fall back to session_project when project is NULL. */
    const char *slug = NULL;
    char *slug_owned = NULL; /* heap-allocated slug, must free before return */

    if (!project || project[0] == '\0') {
        /* No project arg — use session_project (set by cold-start detect_session) */
        if (srv->session_project[0])
            slug = srv->session_project;
        else
            return NULL;
    } else if (project_is_path(project)) {
        /* Path-based arg: convert to slug (shared helper, same logic as expand_project_param Rule 0) */
        slug_owned = project_slug_from_path(project);
        if (!slug_owned) return NULL;
        slug = slug_owned;
    } else {
        slug = project;
    }

    cbm_store_t *store = resolve_store(srv, slug);
    if (!store) {
        free(slug_owned);
        return NULL;
    }
    char *root = get_project_root_from_store(store, slug);
    free(slug_owned);
    return root;
}

/* ── index_repository ─────────────────────────────────────────── */

/* Handle mode="cross-repo-intelligence" — extract to reduce complexity.
 * name_override (may be NULL) selects the same project a prior
 * index_repository call with that name wrote; otherwise the project name is
 * derived from repo_path exactly like a name-less indexing call. */
static char *handle_cross_repo_mode(const char *repo_path, const char *name_override,
                                    const char *args) {
    char *project = NULL;
    if (name_override && name_override[0]) {
        project = cbm_project_name_from_path(name_override);
        if (!project || !cbm_validate_project_name(project)) {
            free(project);
            return cbm_mcp_text_result(
                "{\"error\":\"invalid name for cross-repo-intelligence mode\","
                "\"hint\":\"Pass the same name used when indexing, or omit name to use "
                "the project derived from repo_path.\"}",
                true);
        }
    } else {
        /* cbm_project_name_from_path returns owned memory; the previous
         * heap_strdup wrapper leaked the inner allocation. */
        project = cbm_project_name_from_path(repo_path);
    }
    if (!project) {
        return cbm_mcp_text_result("cannot derive project name", true);
    }

    yyjson_doc *jdoc = yyjson_read(args, strlen(args), 0);
    yyjson_val *jroot = jdoc ? yyjson_doc_get_root(jdoc) : NULL;
    yyjson_val *tp_arr = jroot ? yyjson_obj_get(jroot, "target_projects") : NULL;

    if (!tp_arr || !yyjson_is_arr(tp_arr) || yyjson_arr_size(tp_arr) == 0) {
        yyjson_doc_free(jdoc);
        free(project);
        return cbm_mcp_text_result(
            "{\"error\":\"target_projects is required for cross-repo-intelligence mode. "
            "Use [\\\"*\\\"] for all projects. Run list_projects to see available.\"}",
            true);
    }

    int tp_count = (int)yyjson_arr_size(tp_arr);
    const char **targets = malloc((size_t)tp_count * sizeof(char *));
    size_t idx;
    size_t max;
    yyjson_val *val;
    int ti = 0;
    yyjson_arr_foreach(tp_arr, idx, max, val) {
        targets[ti++] = yyjson_get_str(val);
    }

    cbm_cross_repo_result_t result = cbm_cross_repo_match(project, targets, tp_count);
    free(targets);
    yyjson_doc_free(jdoc);

    if (result.source_missing) {
        char msg[CBM_SZ_512];
        snprintf(msg, sizeof(msg),
                 "{\"error\":\"project '%s' is not indexed; cross-repo-intelligence matches "
                 "existing indexes only\",\"hint\":\"Run index_repository on this repo_path "
                 "(with the same name, if any) first, then rerun cross-repo-intelligence. "
                 "Run list_projects to see indexed projects.\"}",
                 project);
        free(project);
        return cbm_mcp_text_result(msg, true);
    }

    int total = result.http_edges + result.async_edges + result.channel_edges + result.grpc_edges +
                result.graphql_edges + result.trpc_edges;
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_str(doc, root, "status", "success");
    yyjson_mut_obj_add_str(doc, root, "mode", "cross-repo-intelligence");
    yyjson_mut_obj_add_strcpy(doc, root, "project", project);
    yyjson_mut_obj_add_int(doc, root, "projects_scanned", result.projects_scanned);
    if (result.targets_missing > 0) {
        yyjson_mut_obj_add_int(doc, root, "targets_missing", result.targets_missing);
    }
    yyjson_mut_obj_add_int(doc, root, "cross_http_calls", result.http_edges);
    yyjson_mut_obj_add_int(doc, root, "cross_async_calls", result.async_edges);
    yyjson_mut_obj_add_int(doc, root, "cross_channel", result.channel_edges);
    yyjson_mut_obj_add_int(doc, root, "cross_grpc_calls", result.grpc_edges);
    yyjson_mut_obj_add_int(doc, root, "cross_graphql_calls", result.graphql_edges);
    yyjson_mut_obj_add_int(doc, root, "cross_trpc_calls", result.trpc_edges);
    yyjson_mut_obj_add_int(doc, root, "total_cross_edges", total);
    yyjson_mut_obj_add_real(doc, root, "elapsed_ms", result.elapsed_ms);

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    free(project);
    char *out = cbm_mcp_text_result(json, false);
    free(json);
    return out;
}

/* Cap on excluded dir paths listed in the response — keep it compact on large
 * repos (node_modules / vendor / etc. can produce many skip points). The full
 * count is still reported via "count" + "truncated". */
enum { INDEX_EXCLUDED_DIR_CAP = 25 };

/* Attach a compact summary of directory subtrees skipped during discovery (#411).
 * Shape: "excluded": {"dirs": [up to 25 rel-paths], "count": <total>, "truncated": <bool>}.
 * No-op when nothing was excluded. excluded_dirs[] is borrowed (copied into doc). */
static void add_excluded_summary(yyjson_mut_doc *doc, yyjson_mut_val *root, char **excluded_dirs,
                                 int excluded_count) {
    if (!excluded_dirs || excluded_count <= 0) {
        return;
    }
    yyjson_mut_val *excluded = yyjson_mut_obj(doc);
    yyjson_mut_val *dirs = yyjson_mut_arr(doc);
    int shown = excluded_count < INDEX_EXCLUDED_DIR_CAP ? excluded_count : INDEX_EXCLUDED_DIR_CAP;
    for (int i = 0; i < shown; i++) {
        if (excluded_dirs[i]) {
            yyjson_mut_arr_add_strcpy(doc, dirs, excluded_dirs[i]);
        }
    }
    yyjson_mut_obj_add_val(doc, excluded, "dirs", dirs);
    yyjson_mut_obj_add_int(doc, excluded, "count", excluded_count);
    yyjson_mut_obj_add_bool(doc, excluded, "truncated", excluded_count > INDEX_EXCLUDED_DIR_CAP);
    yyjson_mut_obj_add_val(doc, root, "excluded", excluded);
}

/* Cap on per-file skips embedded in the JSON response — keep it compact on
 * large repos. The FULL, uncapped list always goes to the per-run logfile;
 * the JSON carries "count" + "truncated" so nothing is silently hidden. */
enum { INDEX_SKIPPED_FILE_CAP = 50 };

/* Attach the by-design ignored-FILES summary (#963 "purposely not indexed").
 * Individual files dropped by ignore rules — deliberate, not failures; whole
 * excluded subtrees are reported separately via "excluded". Always emits
 * "not_indexed_files_count" (the uncapped total); the list itself is capped
 * like skipped[] and marked truncated when discovery hit its storage cap. */
static void add_not_indexed_files_summary(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                          cbm_pipeline_t *p) {
    cbm_ignored_file_t *ignored = NULL;
    int stored = 0;
    int total = 0;
    cbm_pipeline_get_ignored(p, &ignored, &stored, &total);
    yyjson_mut_obj_add_int(doc, root, "not_indexed_files_count", total);
    if (!ignored || stored <= 0) {
        return;
    }
    yyjson_mut_val *ni = yyjson_mut_obj(doc);
    yyjson_mut_val *files = yyjson_mut_arr(doc);
    int shown = stored < INDEX_SKIPPED_FILE_CAP ? stored : INDEX_SKIPPED_FILE_CAP;
    for (int i = 0; i < shown; i++) {
        yyjson_mut_val *fe = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_strcpy(doc, fe, "path", ignored[i].rel_path ? ignored[i].rel_path : "");
        yyjson_mut_obj_add_strcpy(doc, fe, "reason", ignored[i].reason ? ignored[i].reason : "");
        yyjson_mut_arr_add_val(files, fe);
    }
    yyjson_mut_obj_add_val(doc, ni, "files", files);
    yyjson_mut_obj_add_int(doc, ni, "count", total);
    yyjson_mut_obj_add_bool(doc, ni, "truncated", total > shown);
    yyjson_mut_obj_add_str(doc, ni, "note",
                           "Excluded by design via gitignore/.cbmignore/skip-lists, not a failure. "
                           "Change ignore rules and re-index to include them; see the logfile "
                           "field for the full record and excluded for whole subtrees.");
    yyjson_mut_obj_add_val(doc, root, "not_indexed_files", ni);
}

/* True when a recorded per-file entry is the parse-partial coverage signal
 * (#963) rather than a genuine skip. Kept out of skipped[]/skipped_count so
 * the "skipped" contract (file NOT indexed) stays exact. */
static bool is_parse_partial(const cbm_file_error_t *e) {
    return e->phase && strcmp(e->phase, "parse_partial") == 0;
}

/* Attach a summary of per-file skips (Stage 2 / Track B). Always emits a
 * top-level "skipped_count" (0 on clean runs) so consumers can rely on it.
 * When there are skips, also emits:
 *   "skipped": {"files":[{path,reason,phase}..(<=50)], "count":N, "truncated":bool}
 * and, if a per-run logfile was written, "logfile": "<path>".
 * The run status stays "indexed" — a skipped file is the expected handled
 * outcome, not a failure. errs[] is borrowed (copied into doc) and may contain
 * parse_partial entries, which are filtered out here (reported separately by
 * add_parse_partial_summary). */
static void add_skipped_summary(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                const cbm_file_error_t *errs, int count, const char *logfile) {
    int skips = 0;
    for (int i = 0; i < count; i++) {
        if (!is_parse_partial(&errs[i])) {
            skips++;
        }
    }
    yyjson_mut_obj_add_int(doc, root, "skipped_count", skips);
    if (logfile && logfile[0]) {
        yyjson_mut_obj_add_strcpy(doc, root, "logfile", logfile);
    }
    if (!errs || skips <= 0) {
        return;
    }
    yyjson_mut_val *skipped = yyjson_mut_obj(doc);
    yyjson_mut_val *files = yyjson_mut_arr(doc);
    int shown = 0;
    for (int i = 0; i < count && shown < INDEX_SKIPPED_FILE_CAP; i++) {
        if (is_parse_partial(&errs[i])) {
            continue;
        }
        yyjson_mut_val *fe = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_strcpy(doc, fe, "path", errs[i].path ? errs[i].path : "");
        yyjson_mut_obj_add_strcpy(doc, fe, "reason", errs[i].reason ? errs[i].reason : "");
        yyjson_mut_obj_add_strcpy(doc, fe, "phase", errs[i].phase ? errs[i].phase : "");
        yyjson_mut_arr_add_val(files, fe);
        shown++;
    }
    yyjson_mut_obj_add_val(doc, skipped, "files", files);
    yyjson_mut_obj_add_int(doc, skipped, "count", skips);
    yyjson_mut_obj_add_bool(doc, skipped, "truncated", skips > INDEX_SKIPPED_FILE_CAP);
    yyjson_mut_obj_add_val(doc, root, "skipped", skipped);
}

/* Attach the best-effort parse-coverage summary (#963). Always emits a
 * top-level "parse_partial_count" (0 on clean runs). When files were flagged:
 *   "parse_partial": {"files":[{path,error_ranges}..(<=50)], "count":N,
 *                     "truncated":bool, "note":"..."}
 * These files WERE indexed — constructs inside the listed 1-based line ranges
 * are missing from the graph because tree-sitter could not parse them. The
 * note spells out the best-effort framing: absence from this list is NOT a
 * completeness guarantee. */
static void add_parse_partial_summary(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                      const cbm_file_error_t *errs, int count) {
    int partials = 0;
    for (int i = 0; i < count; i++) {
        if (is_parse_partial(&errs[i])) {
            partials++;
        }
    }
    yyjson_mut_obj_add_int(doc, root, "parse_partial_count", partials);
    if (!errs || partials <= 0) {
        return;
    }
    yyjson_mut_val *pp = yyjson_mut_obj(doc);
    yyjson_mut_val *files = yyjson_mut_arr(doc);
    int shown = 0;
    for (int i = 0; i < count && shown < INDEX_SKIPPED_FILE_CAP; i++) {
        if (!is_parse_partial(&errs[i])) {
            continue;
        }
        yyjson_mut_val *fe = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_strcpy(doc, fe, "path", errs[i].path ? errs[i].path : "");
        yyjson_mut_obj_add_strcpy(doc, fe, "error_ranges", errs[i].reason ? errs[i].reason : "");
        yyjson_mut_arr_add_val(files, fe);
        shown++;
    }
    yyjson_mut_obj_add_val(doc, pp, "files", files);
    yyjson_mut_obj_add_int(doc, pp, "count", partials);
    yyjson_mut_obj_add_bool(doc, pp, "truncated", partials > INDEX_SKIPPED_FILE_CAP);
    yyjson_mut_obj_add_str(doc, pp, "note",
                           "best-effort signal, not a completeness guarantee: these files were "
                           "indexed, but constructs in the listed 1-based ranges may be missing. "
                           "Search those ranges directly; see the logfile field for the full "
                           "record and index_status for persisted coverage.");
    yyjson_mut_obj_add_val(doc, root, "parse_partial", pp);
}

/* Write the FULL (uncapped) skip list to a per-run logfile — ONLY when >=1 file
 * was skipped (no logfile on a clean run). Location:
 *   $CBM_INDEX_LOG (override) else <cache_dir>/logs/<project>-<epoch>.log
 * Returns true and fills out_path on success. */
static bool write_skip_logfile(const char *project, const cbm_file_error_t *errs, int count,
                               char *out_path, size_t out_sz) {
    if (!errs || count <= 0) {
        return false;
    }
    char path[CBM_SZ_1K];
    const char *override = getenv("CBM_INDEX_LOG");
    if (override && override[0]) {
        snprintf(path, sizeof(path), "%s", override);
    } else {
        const char *cdir = cbm_resolve_cache_dir();
        if (!cdir) {
            return false;
        }
        char logdir[CBM_SZ_1K];
        snprintf(logdir, sizeof(logdir), "%s/logs", cdir);
        cbm_mkdir_p(logdir, 0755);
        snprintf(path, sizeof(path), "%s/%s-%lld.log", logdir, project ? project : "index",
                 (long long)time(NULL));
    }
    FILE *f = cbm_fopen(path, "wb");
    if (!f) {
        cbm_log_warn("index.logfile_open_fail", "path", path);
        return false;
    }
    int partials = 0;
    for (int i = 0; i < count; i++) {
        if (is_parse_partial(&errs[i])) {
            partials++;
        }
    }
    (void)fprintf(f, "# codebase-memory-mcp index coverage report\n");
    (void)fprintf(f, "# project=%s skipped=%d parse_partial=%d\n", project ? project : "",
                  count - partials, partials);
    (void)fprintf(f, "# columns: phase\treason\tpath\n");
    for (int i = 0; i < count; i++) {
        (void)fprintf(f, "%s\t%s\t%s\n", errs[i].phase ? errs[i].phase : "",
                      errs[i].reason ? errs[i].reason : "", errs[i].path ? errs[i].path : "");
    }
    (void)fclose(f);
    if (out_path && out_sz) {
        snprintf(out_path, out_sz, "%s", path);
    }
    return true;
}

/* Build the success portion of the index_repository response.
 * Returns true when status should be "degraded" (#334 plausibility gate). */
static bool build_index_success_response(cbm_mcp_server_t *srv, yyjson_mut_doc *doc,
                                         yyjson_mut_val *root, const char *project_name,
                                         const char *repo_path, bool persistence, cbm_pipeline_t *p,
                                         char **excluded_dirs, int excluded_count,
                                         const cbm_file_error_t *file_errors, int file_error_count,
                                         const char *logfile) {
    add_excluded_summary(doc, root, excluded_dirs, excluded_count);
    add_skipped_summary(doc, root, file_errors, file_error_count, logfile);
    add_parse_partial_summary(doc, root, file_errors, file_error_count);
    add_not_indexed_files_summary(doc, root, p);

    int exp_nodes = -1;
    int exp_edges = -1;
    cbm_pipeline_get_committed_counts(p, &exp_nodes, &exp_edges);

    const double ratio = cbm_dump_verify_min_ratio();
    const int min_floor = CBM_DUMP_VERIFY_MIN_FLOOR;

    cbm_store_t *store = resolve_store(srv, project_name);
    int nodes = 0;
    int edges = 0;
    bool degraded = false;

    if (!store) {
        degraded = true;
    } else {
        nodes = cbm_store_count_nodes(store, project_name);
        edges = cbm_store_count_edges(store, project_name);
        if (nodes < 0) {
            degraded = true;
            nodes = 0;
            edges = edges >= 0 ? edges : 0;
        } else if (cbm_dump_verify_is_degraded(exp_nodes, nodes, ratio, min_floor)) {
            (void)cbm_store_checkpoint(store);
            int nodes2 = cbm_store_count_nodes(store, project_name);
            int edges2 = cbm_store_count_edges(store, project_name);
            if (nodes2 >= 0) {
                nodes = nodes2;
            }
            if (edges2 >= 0) {
                edges = edges2;
            }
            degraded = cbm_dump_verify_is_degraded(exp_nodes, nodes, ratio, min_floor);
        }
    }

    yyjson_mut_obj_add_int(doc, root, "nodes", nodes);
    yyjson_mut_obj_add_int(doc, root, "edges", edges);
    if (exp_nodes >= 0) {
        yyjson_mut_obj_add_int(doc, root, "expected_nodes", exp_nodes);
        yyjson_mut_obj_add_int(doc, root, "expected_edges", exp_edges);
    }

    if (degraded) {
        if (!store) {
            yyjson_mut_obj_add_str(doc, root, "hint",
                                   "Index database failed integrity check and was removed. "
                                   "Re-run index_repository(repo_path=...) to rebuild.");
            cbm_log_warn("dump.verify", "reason", "store_missing", "expected_nodes",
                         exp_nodes >= 0 ? "set" : "unknown");
        } else {
            char exp_buf[MCP_FIELD_SIZE];
            char got_buf[MCP_FIELD_SIZE];
            snprintf(exp_buf, sizeof(exp_buf), "%d", exp_nodes);
            snprintf(got_buf, sizeof(got_buf), "%d", nodes);
            yyjson_mut_obj_add_str(
                doc, root, "hint",
                "Persisted far fewer nodes than indexed — likely durability loss from a "
                "hard-killed sibling process. Re-run index_repository(repo_path=...) to rebuild.");
            cbm_log_warn("dump.verify", "expected_nodes", exp_buf, "persisted_nodes", got_buf);
        }
    }

    bool adr_exists = project_has_adr(store, project_name, repo_path);
    yyjson_mut_obj_add_bool(doc, root, "adr_present", adr_exists);
    if (!adr_exists && !degraded) {
        yyjson_mut_obj_add_str(
            doc, root, "adr_hint",
            "Project indexed. Consider creating an Architecture Decision Record: "
            "explore the codebase with get_architecture(aspects=['all']), then use "
            "manage_adr(mode='update') to persist architectural insights across sessions.");
    }

    bool has_artifact = cbm_artifact_exists(repo_path);
    yyjson_mut_obj_add_bool(doc, root, "artifact_present", has_artifact);
    if (persistence && has_artifact) {
        yyjson_mut_obj_add_str(doc, root, "artifact_hint",
                               "Persistent artifact written to .codebase-memory/graph.db.zst. "
                               "Commit this file to share the index with teammates.");
    }

    return degraded;
}

/* Build the response for a worker that crashed/hung/failed without producing a
 * result. The crash is already contained (this process survived); we report it
 * rather than dying. Precise skip-and-continue (quarantine the culprit, index the
 * rest) is layered on in the probe stage. */
static char *build_worker_failure_response(const char *args, cbm_proc_outcome_t outcome) {
    char *repo_path = cbm_mcp_get_string_arg(args, "repo_path");
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_str(doc, root, "status", "error");
    yyjson_mut_obj_add_str(doc, root, "outcome", cbm_proc_outcome_str(outcome));
    yyjson_mut_obj_add_str(
        doc, root, "hint",
        outcome == CBM_PROC_HANG
            ? "Indexing worker timed out (a file made no progress). The worker was "
              "terminated and the server survived. Re-run to retry."
            : "Indexing worker crashed on a file. The crash was contained (the server "
              "survived). Re-run to retry; a future release isolates the culprit file.");
    if (repo_path) {
        yyjson_mut_obj_add_strcpy(doc, root, "repo_path", repo_path);
    }
    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    free(repo_path);
    char *result = cbm_mcp_text_result(json, true);
    free(json);
    return result;
}

bool cbm_mcp_index_response_published(const char *response) {
    if (!response) {
        return false;
    }
    yyjson_doc *outer = yyjson_read(response, strlen(response), 0);
    if (!outer) {
        return false;
    }
    yyjson_val *content = yyjson_obj_get(yyjson_doc_get_root(outer), "content");
    yyjson_val *item = content && yyjson_is_arr(content) ? yyjson_arr_get(content, 0) : NULL;
    yyjson_val *text_value = item ? yyjson_obj_get(item, "text") : NULL;
    const char *text = text_value ? yyjson_get_str(text_value) : NULL;
    yyjson_doc *inner = text ? yyjson_read(text, strlen(text), 0) : NULL;
    yyjson_val *status_value = inner ? yyjson_obj_get(yyjson_doc_get_root(inner), "status") : NULL;
    const char *status = status_value ? yyjson_get_str(status_value) : NULL;
    bool published = status && (strcmp(status, "indexed") == 0 || strcmp(status, "degraded") == 0);
    yyjson_doc_free(inner);
    yyjson_doc_free(outer);
    return published;
}

/* Drop the cached store so the next query reopens whatever the worker wrote (each
 * worker is a fresh process that deletes + recreates the .db). NULL-safe: the
 * background watcher path (main.c) has no MCP server / cached store — the child
 * writes the DB and the parent only needs the return code, so there is nothing
 * to invalidate. */
void cbm_mcp_server_notify_index_published(cbm_mcp_server_t *srv) {
    if (!srv) {
        return;
    }
    /* Deferred: only MARK the cached handle stale. This runs both on the
     * request thread (handle_index_repository) and on the background
     * auto-index thread (autoindex_thread → index_run_supervised); closing
     * srv->store or freeing srv->current_project here would race a request
     * mid-query on the same handle. The request thread consumes the flag in
     * resolve_store()/resolve_resource_store() and closes/reopens there. */
    atomic_store(&srv->store_stale, true);
    atomic_store(&srv->query_graph_tool_description_stale, true);
    atomic_store(&srv->tools_list_changed_pending, true);
}

/* Request-thread half of the deferred invalidation above: close the cached
 * handle if a worker reap marked it stale since it was opened. Call only from
 * the request thread, before trusting srv->store / srv->current_project. */
static void reap_stale_store(cbm_mcp_server_t *srv) {
    if (!atomic_exchange(&srv->store_stale, false)) {
        return;
    }
    if (srv->owns_store && srv->store) {
        cbm_store_close(srv->store);
        srv->store = NULL;
    }
    free(srv->current_project);
    srv->current_project = NULL;
}

/* Resolve a per-supervisor-run temp path <cache_dir>/logs/.supervisor-<pid><suffix>
 * (falls back to the CWD if the cache dir is unresolvable). Used for the crash-
 * attribution marker and the quarantine list during the recovery re-run. */
static void supervisor_tmp_path(char *out, size_t out_sz, const char *suffix) {
    const char *cdir = cbm_resolve_cache_dir();
    if (cdir && cdir[0]) {
        char logdir[CBM_SZ_1K];
        snprintf(logdir, sizeof(logdir), "%s/logs", cdir);
        cbm_mkdir_p(logdir, 0755);
        snprintf(out, out_sz, "%s/.supervisor-%d%s", logdir, (int)getpid(), suffix);
    } else {
        snprintf(out, out_sz, ".supervisor-%d%s", (int)getpid(), suffix);
    }
}

/* Parse the worker's marker JOURNAL ("S <rel>" / "D <rel>" lines, one event
 * per line — see cbm_index_mark_start/done) into the crash/hang SUSPECT set:
 * files whose last event is an S with no closing D, i.e. the in-flight set
 * at kill time. Recovery runs are PARALLEL, so there are up to worker_count
 * suspects; a torn final line (no trailing newline) is discarded by design.
 * Returns a malloc'd array of malloc'd rel paths, OLDEST OPEN S FIRST (for a
 * hang, the oldest still-open file IS the stuck one). Caller frees via
 * supervisor_free_suspects. */
static char **supervisor_read_suspects(const char *path, int *out_n) {
    *out_n = 0;
    FILE *f = cbm_fopen(path, "rb");
    if (!f) {
        return NULL;
    }
    char **open_paths = NULL; /* open (S-without-D) files in first-S order */
    int open_n = 0;
    int open_cap = 0;
    char line[CBM_SZ_1K];
    while (fgets(line, sizeof(line), f)) {
        size_t len = strlen(line);
        if (len == 0 || line[len - 1] != '\n') {
            break; /* torn final line — discard and stop */
        }
        line[--len] = '\0';
        if (len > 0 && line[len - 1] == '\r') {
            line[--len] = '\0';
        }
        if (len < 3 || (line[0] != 'S' && line[0] != 'D') || line[1] != ' ') {
            continue;
        }
        const char *rel = line + 2;
        if (line[0] == 'S') {
            bool already = false;
            for (int i = 0; i < open_n && !already; i++) {
                already = strcmp(open_paths[i], rel) == 0;
            }
            if (already) {
                continue;
            }
            if (open_n == open_cap) {
                int ncap = open_cap ? open_cap * 2 : 16;
                char **np = (char **)realloc(open_paths, (size_t)ncap * sizeof(char *));
                if (!np) {
                    break;
                }
                open_paths = np;
                open_cap = ncap;
            }
            open_paths[open_n++] = cbm_strdup(rel);
        } else {
            for (int i = 0; i < open_n; i++) {
                if (strcmp(open_paths[i], rel) == 0) {
                    free(open_paths[i]);
                    memmove(&open_paths[i], &open_paths[i + 1],
                            (size_t)(open_n - i - 1) * sizeof(char *));
                    open_n--;
                    break;
                }
            }
        }
    }
    (void)fclose(f);
    if (open_n == 0) {
        free(open_paths);
        return NULL;
    }
    *out_n = open_n;
    return open_paths;
}

static void supervisor_free_suspects(char **s, int n) {
    if (!s) {
        return;
    }
    for (int i = 0; i < n; i++) {
        free(s[i]);
    }
    free(s);
}

static bool supervisor_suspect_contains(char **s, int n, const char *rel) {
    for (int i = 0; i < n; i++) {
        if (s[i] && strcmp(s[i], rel) == 0) {
            return true;
        }
    }
    return false;
}

/* Append one quarantine entry "rel\tphase\n" (phase = "crash"|"hang") to the
 * quarantine list. The worker's loader parses this back and reports the skip's
 * phase in skipped[]; a bare "rel" line is still tolerated there (defaults crash). */
static bool supervisor_append_quarantine(const char *path, const char *rel, const char *phase) {
    FILE *f = cbm_fopen(path, "ab");
    if (!f) {
        return false;
    }
    (void)fprintf(f, "%s\t%s\n", rel, phase);
    (void)fclose(f);
    return true;
}

/* Run index_repository in a supervised worker subprocess with skip-and-continue
 * (Stage 3c). Returns the response string (caller frees):
 *   - the worker's own response on a clean first run (the common path);
 *   - after a crash/hang, the response from a clean single-threaded RECOVERY run
 *     that quarantines the culprit file(s) — status="indexed" with them listed in
 *     skipped[] as phase="crash"/"hang", and the good files indexed;
 *   - a best-effort PARTIAL index (one final quarantine-only run) if the recovery
 *     loop cannot converge but at least one file was quarantined;
 *   - a contained-failure response only if even that cannot produce a clean run.
 * Returns NULL only when the worker could not be spawned at all, so the caller
 * degrades to the in-process path. */
static char *index_run_supervised(cbm_mcp_server_t *srv, const char *args) {
    const atomic_bool *cancel_requested = srv ? &srv->stop_requested : NULL;

    /* First attempt: normal parallel run. */
    cbm_index_worker_result_t wr;
    int rc = cbm_index_spawn_worker(args, false, NULL, NULL, cancel_requested, &wr);

    if (rc != 0 || wr.outcome == CBM_PROC_SPAWN_FAILED) {
        cbm_index_worker_result_free(&wr);
        return NULL; /* degrade to in-process */
    }
    if (wr.outcome == CBM_PROC_CLEAN) {
        /* Clean exit → transfer the worker's response (the common path). If the
         * worker exited clean but wrote no response (a degenerate case, e.g. a
         * self binary that does not act as an index worker), resp is NULL and the
         * caller degrades to the in-process path — a clean run never needs the
         * crash-recovery loop. */
        char *resp = wr.response; /* transfer ownership to caller (may be NULL) */
        wr.response = NULL;
        cbm_index_worker_result_free(&wr);
        if (cbm_mcp_index_response_published(resp)) {
            cbm_mcp_server_notify_index_published(srv);
        }
        return resp;
    }
    if (cancel_requested && atomic_load(cancel_requested)) {
        cbm_proc_outcome_t cancelled_outcome = wr.outcome;
        cbm_index_worker_result_free(&wr);
        return build_worker_failure_response(args, cancelled_outcome);
    }

    /* Crash / hang / nonzero exit → skip-and-continue recovery. Re-run the
     * worker PARALLEL (there are no sequential production runs) with the
     * per-file marker JOURNAL armed; after each failed run the journal's
     * open-S set is the in-flight SUSPECT set. A file is quarantined only
     * when it appears in the suspect sets of TWO CONSECUTIVE failed runs
     * (intersection — a stale or merely unlucky in-flight file rotates out),
     * and only ONE file per round: the OLDEST open S in the intersection
     * (for a hang the oldest still-open file IS the stuck one; for a crash
     * it is the longest-running suspect — the best single deterministic
     * pick). A clean run then indexes the good files and reports the
     * quarantined ones as phase="crash"/"hang" skips via the ordinary
     * Stage-2 skip plumbing. The old design re-ran SINGLE-THREADED to keep
     * one exact marker; at scale that fell into the sequential crawl, went
     * quiet, was killed as a hang mid-pass, and the stale marker got FOUR
     * innocent ms-typescript fixtures quarantined one 15-minute retry at a
     * time. */
    cbm_proc_outcome_t last_outcome = wr.outcome;
    cbm_index_worker_result_free(&wr);

    char marker_path[CBM_SZ_1K];
    char quarantine_path[CBM_SZ_1K];
    supervisor_tmp_path(marker_path, sizeof(marker_path), ".marker");
    supervisor_tmp_path(quarantine_path, sizeof(quarantine_path), ".quarantine");
    (void)remove(marker_path);
    /* Start the quarantine list empty (truncate any stale file). */
    FILE *qinit = cbm_fopen(quarantine_path, "wb");
    if (qinit) {
        (void)fclose(qinit);
    }

    int cap = 100;
    const char *cap_env = getenv("CBM_INDEX_MAX_RESTARTS");
    if (cap_env && cap_env[0]) {
        int v = atoi(cap_env);
        if (v > 0) {
            cap = v;
        }
    }

    char *resp = NULL;
    int quarantined = 0;         /* files pinned + added to the quarantine list so far */
    char **prev_suspects = NULL; /* previous failed round's in-flight set */
    int prev_n = 0;
    for (int i = 0; i < cap; i++) {
        cbm_index_worker_result_t wr2;
        int rc2 = cbm_index_spawn_worker(args, /*single_thread=*/false, marker_path,
                                         quarantine_path, cancel_requested, &wr2);
        if (rc2 != 0) {
            last_outcome = wr2.outcome;
            cbm_index_worker_result_free(&wr2);
            break; /* spawn failed mid-recovery — give up */
        }
        if (wr2.outcome == CBM_PROC_CLEAN && wr2.response) {
            resp = wr2.response; /* transfer ownership to caller */
            wr2.response = NULL;
            cbm_index_worker_result_free(&wr2);
            break; /* good files indexed; quarantined files reported as crash/hang */
        }
        if (wr2.outcome == CBM_PROC_CRASH || wr2.outcome == CBM_PROC_HANG) {
            last_outcome = wr2.outcome;
            cbm_index_worker_result_free(&wr2);
            /* crash vs hang: the phase this file is quarantined under and
             * reported as in skipped[]. A fault signal → "crash"; a
             * no-progress kill → "hang". */
            const char *phase = (last_outcome == CBM_PROC_HANG) ? "hang" : "crash";
            int sus_n = 0;
            char **suspects = supervisor_read_suspects(marker_path, &sus_n);
            (void)remove(marker_path); /* fresh journal for the next re-run */
            if (!suspects || sus_n == 0) {
                supervisor_free_suspects(suspects, sus_n);
                cbm_log_warn("index.supervisor.unattributable", "action", "give_up");
                break;
            }
            if (prev_suspects) {
                /* Two-consecutive-strikes: quarantine the OLDEST open S that
                 * was also in flight in the previous failed round. */
                const char *pick = NULL;
                for (int k = 0; k < sus_n && !pick; k++) {
                    if (supervisor_suspect_contains(prev_suspects, prev_n, suspects[k])) {
                        pick = suspects[k];
                    }
                }
                if (!pick) {
                    /* Disjoint consecutive in-flight sets: the failure is not
                     * attributable to a recurring file (systemic) — stop
                     * rather than quarantine an innocent. */
                    supervisor_free_suspects(suspects, sus_n);
                    cbm_log_warn("index.supervisor.unattributable", "action", "give_up");
                    break;
                }
                if (!supervisor_append_quarantine(quarantine_path, pick, phase)) {
                    cbm_log_warn("index.supervisor.quarantine_write_fail", "path", pick);
                    supervisor_free_suspects(suspects, sus_n);
                    break;
                }
                quarantined++;
                char attempt_buf[MCP_FIELD_SIZE];
                snprintf(attempt_buf, sizeof(attempt_buf), "%d", i + 1);
                cbm_log_warn("index.file_quarantined", "path", pick, "outcome", phase, "attempt",
                             attempt_buf);
            }
            supervisor_free_suspects(prev_suspects, prev_n);
            prev_suspects = suspects;
            prev_n = sus_n;
            continue;
        }
        /* SPAWN_FAILED / nonzero exit / non-fault kill → not a crash we can
         * attribute; stop and report a contained failure. */
        last_outcome = wr2.outcome;
        cbm_index_worker_result_free(&wr2);
        break;
    }
    supervisor_free_suspects(prev_suspects, prev_n);

    (void)remove(marker_path); /* marker no longer needed */

    /* Terminal best-effort-partial: the loop exited WITHOUT a clean run (cap
     * exhausted, or an unattributable failure) but at least one file was already
     * quarantined. Try ONE final PARALLEL spawn with the accumulated quarantine
     * and NO marker — every known-bad file short-circuits, so a clean run yields
     * a PARTIAL index (all good files indexed, all known crashers/hangs reported
     * as skips) rather than a hard failure. Bounded by the same quiet-timeout,
     * so it cannot itself hang. Rare given monotonic progress. */
    if (!resp && quarantined > 0) {
        cbm_index_worker_result_t wrp;
        int rcp = cbm_index_spawn_worker(args, /*single_thread=*/false, NULL, quarantine_path,
                                         cancel_requested, &wrp);
        if (rcp == 0 && wrp.outcome == CBM_PROC_CLEAN && wrp.response) {
            resp = wrp.response; /* transfer ownership to caller */
            wrp.response = NULL;
            char qn[MCP_FIELD_SIZE];
            snprintf(qn, sizeof(qn), "%d", quarantined);
            cbm_log_error("index.supervisor.partial", "quarantined", qn, "outcome",
                          cbm_proc_outcome_str(last_outcome));
        }
        cbm_index_worker_result_free(&wrp);
    }

    (void)remove(quarantine_path);

    if (!resp) {
        resp = build_worker_failure_response(args, last_outcome);
    }
    /* A contained worker failure is a valid tool response, but it did not
     * publish a graph generation. Reuse the response-status authority so only
     * indexed/degraded outcomes invalidate query stores and tool descriptions. */
    if (cbm_mcp_index_response_published(resp)) {
        cbm_mcp_server_notify_index_published(srv);
    }
    return resp;
}

/* Build a minimal {"repo_path": "<root>"} args object (path safely escaped) and
 * run it through index_run_supervised. Shared by the session auto-index (srv
 * present → its cached store is invalidated) and the watcher re-index (srv NULL).
 * Returns the worker's response string (caller frees) or NULL to degrade. */
static char *index_run_supervised_path(cbm_mcp_server_t *srv, const char *root_path) {
    if (!root_path || !root_path[0]) {
        return NULL;
    }
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_strcpy(doc, root, "repo_path", root_path);
    char *args = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    if (!args) {
        return NULL;
    }
    char *resp = index_run_supervised(srv, args);
    free(args);
    return resp;
}

/* Public entry (see mcp.h): watcher and auto-index callers pass their long-lived
 * server so worker publication can defer cached-store invalidation safely. */
char *cbm_mcp_index_run_supervised_path(cbm_mcp_server_t *srv, const char *root_path) {
    return index_run_supervised_path(srv, root_path);
}

bool cbm_path_within_root(const char *root_path, const char *abs_path); /* defined below */

static char *handle_index_repository(cbm_mcp_server_t *srv, const char *args) {
    CBM_PROF_START(prof_index_total);
    CBM_PROF_START(prof_index_args);
    /* Supervisor gate: run the index in a crash/hang-isolating worker subprocess
     * unless this process IS the worker or the kill switch (CBM_INDEX_SUPERVISOR=0)
     * is set. On spawn failure, fall through to the in-process path (degrade). */
    if (cbm_index_supervisor_should_wrap()) {
        char *supervised = index_run_supervised(srv, args);
        if (supervised) {
            return supervised;
        }
    }
    char *repo_path = cbm_mcp_get_string_arg(args, "repo_path");
    char *mode_str = cbm_mcp_get_string_arg(args, "mode");
    char *name_override = cbm_mcp_get_string_arg(args, "name");
    cbm_normalize_path_sep(repo_path);

    if (!repo_path) {
        free(mode_str);
        free(name_override);
        CBM_PROF_END("index_repository", "args", prof_index_args);
        CBM_PROF_END("index_repository", "TOTAL", prof_index_total);
        return cbm_mcp_text_result(
            "{\"error\":\"repo_path is required\","
            "\"hint\":\"Pass the absolute path to the project root directory.\"}", true);
    }

    repo_path = canonicalize_repo_path_if_exists(repo_path);

    /* Optional workspace boundary: when CBM_ALLOWED_ROOT is set (agentic /
     * multi-tenant deployments where repo_path may be influenced by an
     * untrusted caller), refuse to index a path that resolves outside it.
     * Unset by default, so the standard "index the path I gave you" behaviour
     * is unchanged. */
    const char *allowed_root = getenv("CBM_ALLOWED_ROOT");
    if (allowed_root && allowed_root[0] && repo_path &&
        !cbm_path_within_root(allowed_root, repo_path)) {
        free(mode_str);
        free(name_override);
        free(repo_path);
        return cbm_mcp_text_result("repo_path is outside the allowed root", true);
    }

    if (mode_str && strcmp(mode_str, "cross-repo-intelligence") == 0) {
        free(mode_str);
        char *result = handle_cross_repo_mode(repo_path, name_override, args);
        free(name_override);
        free(repo_path);
        CBM_PROF_END("index_repository", "cross_repo_mode", prof_index_args);
        CBM_PROF_END("index_repository", "TOTAL", prof_index_total);
        return result;
    }

    cbm_index_mode_t mode = CBM_MODE_FULL;
    if (mode_str && strcmp(mode_str, "fast") == 0) {
        mode = CBM_MODE_FAST;
    } else if (mode_str && strcmp(mode_str, "moderate") == 0) {
        mode = CBM_MODE_MODERATE;
    }
    free(mode_str);

    bool persistence = cbm_mcp_get_bool_arg(args, "persistence");
    CBM_PROF_END("index_repository", "args", prof_index_args);

    CBM_PROF_START(prof_index_pipeline_new);
    cbm_pipeline_t *p = cbm_pipeline_new(repo_path, NULL, mode);
    CBM_PROF_END("index_repository", "pipeline_new", prof_index_pipeline_new);
    if (!p) {
        free(name_override);
        free(repo_path);
        CBM_PROF_END("index_repository", "TOTAL", prof_index_total);
        return cbm_mcp_text_result(
            "{\"error\":\"failed to create indexing pipeline\","
            "\"hint\":\"Check that repo_path exists and is readable. The directory may be empty or inaccessible.\"}", true);
    }
    CBM_PROF_START(prof_index_pipeline_config);
    if (name_override && name_override[0] && !cbm_pipeline_set_project_name(p, name_override)) {
        cbm_pipeline_free(p);
        free(name_override);
        free(repo_path);
        CBM_PROF_END("index_repository", "pipeline_config", prof_index_pipeline_config);
        CBM_PROF_END("index_repository", "TOTAL", prof_index_total);
        return cbm_mcp_text_result("invalid project name", true);
    }
    free(name_override);
    cbm_pipeline_set_persistence(p, persistence);
    cbm_pipeline_apply_config(p, srv->config);

    char *project_name = heap_strdup(cbm_pipeline_project_name(p));
    CBM_PROF_END("index_repository", "pipeline_config", prof_index_pipeline_config);

    /* Close cached store before indexing because full and fallback paths may replace the DB. */
    CBM_PROF_START(prof_index_close_before);
    if (srv->owns_store && srv->store) {
        cbm_store_close(srv->store);
        srv->store = NULL;
    }
    free(srv->current_project);
    srv->current_project = NULL;
    CBM_PROF_END("index_repository", "close_cached_store_before", prof_index_close_before);

    /* Serialize pipeline runs to prevent concurrent writes.
     * Track active pipeline so signal handler and notifications/cancelled
     * can cancel it mid-run. */
    CBM_PROF_START(prof_index_locked_run);
    cbm_pipeline_lock();
    cbm_mutex_lock(&srv->active_request_lock);
    atomic_store_explicit(&srv->active_pipeline, p, memory_order_release);
    cbm_mutex_unlock(&srv->active_request_lock);
    int rc = cbm_pipeline_run(p);
    bool graph_changed = cbm_pipeline_graph_changed(p);
    cbm_pipeline_publish_kind_t publish_kind = cbm_pipeline_publish_kind(p);
    bool incremental_fallback = cbm_pipeline_incremental_fallback(p);
    const char *publish_reason = cbm_pipeline_publish_reason(p);
    cbm_mutex_lock(&srv->active_request_lock);
    atomic_store_explicit(&srv->active_pipeline, NULL, memory_order_release);
    cbm_mutex_unlock(&srv->active_request_lock);
    /* Refresh the watcher baseline before releasing the pipeline lock. Otherwise
     * a poll that observes the explicit edit can acquire the lock in the gap
     * below and launch a redundant full-mode reindex before the later response
     * bookkeeping reaches cbm_watcher_mark_indexed(). */
    if (rc == 0 && srv->watcher) {
        cbm_watcher_mark_indexed(srv->watcher, project_name, repo_path);
    }
    cbm_pipeline_unlock();
    CBM_PROF_END("index_repository", "pipeline_locked_run", prof_index_locked_run);

    /* Capture the excluded-subtree list (#411) while the pipeline (which owns
     * the strings) is still alive — the response builder copies them into the
     * JSON doc, so they need only outlive that call, not cbm_pipeline_free. */
    CBM_PROF_START(prof_index_excluded);
    char **excluded_dirs = NULL;
    int excluded_count = 0;
    cbm_pipeline_get_excluded(p, &excluded_dirs, &excluded_count);
    CBM_PROF_END("index_repository", "get_excluded", prof_index_excluded);

    /* Capture the per-file skip list (Stage 2 / Track B) while the pipeline
     * still owns the strings; the response builder copies them into the doc. */
    cbm_file_error_t *file_errors = NULL;
    int file_error_count = 0;
    cbm_pipeline_get_file_errors(p, &file_errors, &file_error_count);

    CBM_PROF_START(prof_index_mem_collect);
    cbm_mem_collect(); /* return mimalloc pages to OS after large indexing */
    CBM_PROF_END("index_repository", "post_mem_collect", prof_index_mem_collect);

    /* Invalidate cached store so next query reopens the fresh database */
    if (srv->owns_store && srv->store) {
        cbm_store_close(srv->store);
        srv->store = NULL;
    }
    free(srv->current_project);
    srv->current_project = NULL;

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_obj_add_str(doc, root, "project", project_name);
    yyjson_mut_obj_add_str(doc, root, "publish_kind",
                           cbm_pipeline_publish_kind_name(publish_kind));
    if (publish_reason && publish_reason[0]) {
        yyjson_mut_obj_add_str(doc, root, "publish_reason", publish_reason);
    }
    yyjson_mut_obj_add_bool(doc, root, "graph_changed", graph_changed);
    yyjson_mut_obj_add_bool(doc, root, "incremental_fallback", incremental_fallback);
    add_pipeline_exact_delta_stats(doc, root, cbm_pipeline_exact_delta_stats(p));

    if (rc == 0) {
        CBM_PROF_START(prof_index_resolve_store);
        cbm_store_t *resolved_store = resolve_store(srv, project_name);
        cbm_store_t *owned_writable_store = NULL;
        cbm_store_t *store =
            cbm_mcp_writable_existing_store(resolved_store, &owned_writable_store);
        CBM_PROF_END("index_repository", "resolve_store", prof_index_resolve_store);
        if (store) {
            /* Auto-detect ecosystem and index installed deps from fresh graph.
             * Queries manifest files already indexed by pipeline step 1. */
            CBM_PROF_START(prof_index_deps);
            int dep_owner_rc = CBM_STORE_OK;
            int effective_dep_limit = cbm_mcp_effective_auto_dep_limit(srv, args);
            int deps_reindexed =
                cbm_mcp_auto_index_deps(srv, project_name, repo_path, store,
                                        effective_dep_limit, &dep_owner_rc);
            CBM_PROF_END("index_repository", "dep_auto_index", prof_index_deps);

            if (dep_owner_rc != CBM_STORE_OK) {
                rc = dep_owner_rc;
                yyjson_mut_obj_add_str(
                    doc, root, "error",
                    "failed to refresh file-delta owner metadata after dependency indexing");
            }

            CBM_PROF_START(prof_index_rank_refresh);
            (void)cbm_pagerank_refresh_after_publish(
                store, project_name, srv->config, graph_changed, deps_reindexed,
                cbm_rank_refresh_publish_from_pipeline(publish_kind, incremental_fallback));
            CBM_PROF_END("index_repository", "rank_refresh", prof_index_rank_refresh);
            /* In-process publish must meet the same freshness contract as the
             * supervised worker path (which notifies at all its exits). */
            cbm_mcp_server_notify_index_published(srv);
            CBM_PROF_START(prof_index_counts);
            int nodes = cbm_store_count_nodes(store, project_name);
            int edges = cbm_store_count_edges(store, project_name);
            CBM_PROF_END("index_repository", "count_graph", prof_index_counts);
            yyjson_mut_obj_add_int(doc, root, "nodes", nodes);
            yyjson_mut_obj_add_int(doc, root, "edges", edges);
            if (deps_reindexed > 0)
                yyjson_mut_obj_add_int(doc, root, "dependencies_indexed", deps_reindexed);

            CBM_PROF_START(prof_index_ecosystem);
            cbm_pkg_manager_t eco = cbm_detect_ecosystem(repo_path);
            CBM_PROF_END("index_repository", "detect_ecosystem", prof_index_ecosystem);
            if (eco != CBM_PKG_COUNT)
                yyjson_mut_obj_add_str(doc, root, "detected_ecosystem",
                                       cbm_pkg_manager_str(eco));
            /* Check the canonical SQLite ADR backend first, with legacy-file
             * fallback for installations that have not migrated yet. */
            CBM_PROF_START(prof_index_adr);
            bool adr_exists = project_has_adr(store, project_name, repo_path);
            yyjson_mut_obj_add_bool(doc, root, "adr_present", adr_exists);
            if (!adr_exists) {
                yyjson_mut_obj_add_str(
                    doc, root, "adr_hint",
                    "Project indexed. Consider creating an Architecture Decision Record: "
                    "explore the codebase with get_architecture(aspects=['all']), then use "
                    "manage_adr(mode='update') to persist architectural insights across MCP server runs.");
            }
            CBM_PROF_END("index_repository", "adr_check", prof_index_adr);
        } else if (resolved_store) {
            rc = CBM_STORE_ERR;
            yyjson_mut_obj_add_str(doc, root, "error",
                                   "project store could not be opened read-write");
        }
        if (owned_writable_store) {
            cbm_store_close(owned_writable_store);
        }
        if (rc == 0) {
            /* Full skip/coverage evidence is written while pipeline-owned strings live. */
            char logfile_path[CBM_SZ_1K] = "";
            bool has_logfile = write_skip_logfile(project_name, file_errors, file_error_count,
                                                  logfile_path, sizeof(logfile_path));
            bool degraded = build_index_success_response(
                srv, doc, root, project_name, repo_path, persistence, p, excluded_dirs,
                excluded_count, file_errors, file_error_count,
                has_logfile ? logfile_path : NULL);
            yyjson_mut_obj_add_str(doc, root, "status", degraded ? "degraded" : "indexed");
        } else {
            yyjson_mut_obj_add_str(doc, root, "status", "error");
            yyjson_mut_obj_add_str(
                doc, root, "hint",
                "Indexing completed but post-publish metadata refresh failed; retry indexing "
                "and inspect server logs for rebuild_file_delta_owners_after_deps.");
        }
    } else {
        yyjson_mut_obj_add_str(doc, root, "status", "error");
        yyjson_mut_obj_add_str(doc, root, "hint",
                               "Pipeline failed. Check repo_path exists and contains source files. "
                               "Try mode='fast' for a quicker diagnostic run.");
    }

    if (rc == 0 && publish_kind == CBM_PIPELINE_PUBLISH_INCREMENTAL_OVERLAY &&
        cbm_mcp_overlay_compaction_after_publish(srv)) {
        int compact_max = cbm_mcp_overlay_compaction_max_generations(srv);
        bool started =
            cbm_mcp_server_start_overlay_compaction(srv, project_name, compact_max);
        yyjson_mut_obj_add_str(doc, root, "overlay_compaction_policy",
                               CBM_CONFIG_OVERLAY_COMPACTION_POLICY_AFTER_PUBLISH);
        yyjson_mut_obj_add_int(doc, root, "overlay_compaction_max_generations",
                               compact_max);
        yyjson_mut_obj_add_bool(doc, root, "overlay_compaction_started", started);
        const char *compact_status =
            started ? "started"
                    : (cbm_mcp_server_overlay_compaction_active(srv) ? "already_running"
                                                                      : "not_started");
        yyjson_mut_obj_add_str(doc, root, "overlay_compaction_status",
                               compact_status);
    }

    CBM_PROF_START(prof_index_response_fields);
    if (srv->session_project[0])
        yyjson_mut_obj_add_str(doc, root, "session_project", srv->session_project);
    CBM_PROF_END("index_repository", "response_fields", prof_index_response_fields);

    CBM_PROF_START(prof_index_serialize);
    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    /* Free the pipeline only after the response doc copied the excluded list.
     * Supervised worker: skip the deep free — the process exits right after
     * handing over the response (main.c fast-exits), and piecemeal-freeing a
     * multi-GB graph before process death costs minutes on kernel-scale repos;
     * the OS reclaims it wholesale at exit. In-process paths (tests, kill
     * switch, degrade) still free normally. */
    if (cbm_index_worker_active()) {
        cbm_log_info("index.worker.fast_exit", "skip", "pipeline_free");
    } else {
        cbm_pipeline_free(p);
    }
    free(project_name);
    free(repo_path);

    char *result = cbm_mcp_text_result(json, rc != 0);
    free(json);
    CBM_PROF_END("index_repository", "serialize_cleanup", prof_index_serialize);
    CBM_PROF_END("index_repository", "TOTAL", prof_index_total);
    return result;
}

/* ── get_code_snippet ─────────────────────────────────────────── */

/* Copy a node from an array into a heap-allocated standalone node. */
static void copy_node(const cbm_node_t *src, cbm_node_t *dst) {
    dst->id = src->id;
    dst->project = heap_strdup(src->project);
    dst->label = heap_strdup(src->label);
    dst->name = heap_strdup(src->name);
    dst->qualified_name = heap_strdup(src->qualified_name);
    dst->file_path = heap_strdup(src->file_path);
    dst->start_line = src->start_line;
    dst->end_line = src->end_line;
    dst->properties_json = src->properties_json ? heap_strdup(src->properties_json) : NULL;
}

/* Build a JSON suggestions response for ambiguous or fuzzy results. */
static char *snippet_suggestions(const char *input, cbm_node_t *nodes, int count) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_obj_add_str(doc, root, "status", "ambiguous");

    char msg[CBM_SZ_512];
    snprintf(msg, sizeof(msg),
             "%d matches for \"%s\". Pick a qualified_name from suggestions below, "
             "or use search_graph(name_pattern=\"...\") to narrow results.",
             count, input);
    yyjson_mut_obj_add_str(doc, root, "message", msg);

    yyjson_mut_val *arr = yyjson_mut_arr(doc);
    for (int i = 0; i < count; i++) {
        yyjson_mut_val *s = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_str(doc, s, "qualified_name",
                               nodes[i].qualified_name ? nodes[i].qualified_name : "");
        if (nodes[i].name && nodes[i].name[0])
            yyjson_mut_obj_add_str(doc, s, "name", nodes[i].name);
        if (nodes[i].label && nodes[i].label[0])
            yyjson_mut_obj_add_str(doc, s, "label", nodes[i].label);
        if (nodes[i].file_path && nodes[i].file_path[0])
            yyjson_mut_obj_add_str(doc, s, "file_path", nodes[i].file_path);
        yyjson_mut_arr_append(arr, s);
    }
    yyjson_mut_obj_add_val(doc, root, "suggestions", arr);

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);

    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

/* Enrich a mutable JSON object with key-value pairs from a node's properties_json.
 * Returns the parsed yyjson_doc (caller frees AFTER serialization — zero-copy). */
static yyjson_doc *enrich_node_properties(yyjson_mut_doc *doc, yyjson_mut_val *obj,
                                          const char *properties_json, bool compact,
                                          const char *const fields[], int field_count) {
    if (!properties_json || properties_json[0] == '\0' || (compact && field_count == 0)) {
        return NULL;
    }
    yyjson_doc *props_doc = yyjson_read(properties_json, strlen(properties_json), 0);
    if (!props_doc) {
        return NULL;
    }
    yyjson_val *props_root = yyjson_doc_get_root(props_doc);
    if (!props_root || !yyjson_is_obj(props_root)) {
        yyjson_doc_free(props_doc);
        return NULL;
    }
    yyjson_obj_iter iter;
    yyjson_obj_iter_init(props_root, &iter);
    bool added = false;
    yyjson_val *key;
    while ((key = yyjson_obj_iter_next(&iter))) {
        yyjson_val *val = yyjson_obj_iter_get_val(key);
        const char *k = yyjson_get_str(key);
        if (!k) {
            continue;
        }
        if (sg_field_blocked(k) || (compact && !sg_field_selected(k, fields, field_count))) {
            continue;
        }
        /* Search results flatten node properties into the result object for
         * token economy, so property keys must not overwrite/collide with
         * stable result fields such as source:"project" vs source:"infra". */
        if (yyjson_mut_obj_get(obj, k) != NULL) {
            continue;
        }
        if (yyjson_is_str(val)) {
            yyjson_mut_obj_add_str(doc, obj, k, yyjson_get_str(val));
            added = true;
        } else if (yyjson_is_bool(val)) {
            yyjson_mut_obj_add_bool(doc, obj, k, yyjson_get_bool(val));
            added = true;
        } else if (yyjson_is_int(val)) {
            yyjson_mut_obj_add_int(doc, obj, k, yyjson_get_int(val));
            added = true;
        } else if (yyjson_is_real(val)) {
            yyjson_mut_obj_add_real(doc, obj, k, yyjson_get_real(val));
            added = true;
        }
    }
    if (!added) {
        yyjson_doc_free(props_doc);
        return NULL;
    }
    return props_doc; /* caller frees after serialization */
}

/* True only when abs_path, after realpath/_fullpath resolution (which collapses
 * `..` and resolves symlinks/junctions), stays within root_path. This is the
 * single containment guard every MCP file-read sink must pass before reading a
 * file into a tool response: both snippet and search responses route through
 * it, so an indexed path that escapes the project root — via `..`, a symlink,
 * or a Windows junction — is never read back out. */
/* Canonicalize `path` (resolve symlinks/junctions and `..`) into `out`
 * (>= CBM_SZ_4K bytes); returns true on success. Isolating the per-OS resolver
 * keeps cbm_path_within_root's control flow unconditional: the previous `#ifdef`
 * opened the `if (...) {` brace in one branch and a different one in the other,
 * sharing a single close brace — legal C, but it splits the function's braces
 * across preprocessor branches, which defeats source-level tooling that parses
 * without the preprocessor (and left this function unindexed in the graph). */
static bool resolve_canonical_path(const char *path, char *out, size_t out_sz) {
    /* cbm_canonical_path: realpath on POSIX; wide existence check +
     * GetFullPathNameW on Windows (the old bare _fullpath was ANSI —
     * CJK-locale corruption, #973 — and, unlike POSIX realpath, resolved
     * nonexistent paths too; requiring existence aligns the platforms). */
    if (!cbm_canonical_path(path, out, out_sz)) {
        return false;
    }
#ifdef _WIN32
    cbm_normalize_path_sep(out);
#endif
    return true;
}

bool cbm_path_within_root(const char *root_path, const char *abs_path) {
    if (!root_path || !abs_path) {
        return false;
    }
    char real_root[CBM_SZ_4K];
    char real_file[CBM_SZ_4K];
    if (resolve_canonical_path(root_path, real_root, sizeof(real_root)) &&
        resolve_canonical_path(abs_path, real_file, sizeof(real_file))) {
        size_t root_len = strlen(real_root);
        if (strncmp(real_file, real_root, root_len) == 0 &&
            (real_file[root_len] == '/' || real_file[root_len] == '\0')) {
            return true;
        }
    }
    return false;
}

static bool utf8_is_cont(unsigned char c) {
    return (c & 0xC0) == 0x80;
}

static char *sanitize_utf8_lossy(const char *s) {
    enum {
        UTF8_REPLACEMENT_LEN = 3,
        UTF8_THREE_BYTE_LEN = 3,
        UTF8_FOUR_BYTE_LEN = 4,
        UTF8_FOURTH_BYTE = 3,
    };
    if (!s) {
        return NULL;
    }
    size_t len = strlen(s);
    if (len > (((size_t)-1) - SKIP_ONE) / UTF8_REPLACEMENT_LEN) {
        return NULL;
    }
    char *out = malloc(len * UTF8_REPLACEMENT_LEN + SKIP_ONE);
    if (!out) {
        return NULL;
    }

    const unsigned char *p = (const unsigned char *)s;
    const unsigned char *end = p + len;
    unsigned char *dst = (unsigned char *)out;
    while (p < end) {
        unsigned char c = *p;
        size_t n = 0;
        if (c < 0x80) {
            n = 1;
        } else if (c >= 0xC2 && c <= 0xDF && p + 1 < end && utf8_is_cont(p[1])) {
            n = 2;
        } else if (c == 0xE0 && p + 2 < end && p[1] >= 0xA0 && p[1] <= 0xBF && utf8_is_cont(p[2])) {
            n = UTF8_THREE_BYTE_LEN;
        } else if (c >= 0xE1 && c <= 0xEC && p + 2 < end && utf8_is_cont(p[1]) &&
                   utf8_is_cont(p[2])) {
            n = UTF8_THREE_BYTE_LEN;
        } else if (c == 0xED && p + 2 < end && p[1] >= 0x80 && p[1] <= 0x9F && utf8_is_cont(p[2])) {
            n = UTF8_THREE_BYTE_LEN;
        } else if (c >= 0xEE && c <= 0xEF && p + 2 < end && utf8_is_cont(p[1]) &&
                   utf8_is_cont(p[2])) {
            n = UTF8_THREE_BYTE_LEN;
        } else if (c == 0xF0 && p + UTF8_FOURTH_BYTE < end && p[1] >= 0x90 && p[1] <= 0xBF &&
                   utf8_is_cont(p[2]) && utf8_is_cont(p[UTF8_FOURTH_BYTE])) {
            n = UTF8_FOUR_BYTE_LEN;
        } else if (c >= 0xF1 && c <= 0xF3 && p + UTF8_FOURTH_BYTE < end && utf8_is_cont(p[1]) &&
                   utf8_is_cont(p[2]) && utf8_is_cont(p[UTF8_FOURTH_BYTE])) {
            n = UTF8_FOUR_BYTE_LEN;
        } else if (c == 0xF4 && p + UTF8_FOURTH_BYTE < end && p[1] >= 0x80 && p[1] <= 0x8F &&
                   utf8_is_cont(p[2]) && utf8_is_cont(p[UTF8_FOURTH_BYTE])) {
            n = UTF8_FOUR_BYTE_LEN;
        }

        if (n > 0) {
            memcpy(dst, p, n);
            dst += n;
            p += n;
        } else {
            *dst++ = 0xEF;
            *dst++ = 0xBF;
            *dst++ = 0xBD;
            p++;
        }
    }
    *dst = '\0';
    return out;
}

/* Build an enriched snippet response for a resolved node. */
/* Add a string array to a JSON object (no-op if count == 0). */
static void add_string_array(yyjson_mut_doc *doc, yyjson_mut_val *obj, const char *key,
                             char **strings, int count) {
    if (count <= 0) {
        return;
    }
    yyjson_mut_val *arr = yyjson_mut_arr(doc);
    for (int i = 0; i < count; i++) {
        yyjson_mut_arr_add_str(doc, arr, strings[i]);
    }
    yyjson_mut_obj_add_val(doc, obj, key, arr);
}

/* get_code_snippet coverage note (#963): if the resolved node's file is
 * flagged parse_partial, warn that the graph may under-report this file.
 * Correlated by construction — the result names its file. (An entirely-
 * skipped file cannot appear here: it has no nodes to resolve a snippet
 * from.) */
static void add_snippet_coverage_note(yyjson_mut_doc *doc, yyjson_mut_val *root_obj,
                                      cbm_store_t *store, const cbm_node_t *node) {
    if (!node->file_path || !node->file_path[0] || !node->project) {
        return;
    }
    cbm_coverage_row_t *rows = NULL;
    int count = 0;
    if (cbm_store_coverage_get(store, node->project, &rows, &count) != CBM_STORE_OK) {
        return;
    }
    for (int i = 0; i < count; i++) {
        if (rows[i].rel_path && strcmp(rows[i].rel_path, node->file_path) == 0 && rows[i].kind &&
            strcmp(rows[i].kind, "parse_partial") == 0) {
            char note[CBM_SZ_1K];
            snprintf(note, sizeof(note),
                     "This file was only PARTIALLY indexed — line range(s) %s could not be "
                     "parsed, so constructs there may be missing from the graph (callers/callees "
                     "and search results can under-report this file). The source above is ground "
                     "truth. (best-effort signal)",
                     rows[i].detail && rows[i].detail[0] ? rows[i].detail : "?");
            yyjson_mut_obj_add_strcpy(doc, root_obj, "coverage_note", note);
            break;
        }
    }
    cbm_store_free_coverage(rows, count);
}

static char *build_snippet_response(cbm_mcp_server_t *srv, cbm_node_t *node,
                                    const char *match_method, bool include_neighbors,
                                    cbm_node_t *alternatives, int alt_count,
                                    int max_lines, const char *mode, bool compact) {
    char *root_path = get_project_root(srv, node->project);

    int start = node->start_line > 0 ? node->start_line : SKIP_ONE;
    int end = node->end_line > start ? node->end_line : start + SNIPPET_DEFAULT_LINES;
    int total_lines = end - start + 1;
    bool truncated = false;
    char *source = NULL;
    char *source_tail = NULL;

    /* Build absolute path and verify it's within the project root.
     * Prevents path traversal via crafted file_path (e.g., "../../.ssh/id_rsa"). */
    char *abs_path = NULL;
    if (root_path && node->file_path) {
        size_t apsz = strlen(root_path) + strlen(node->file_path) + MCP_SEPARATOR;
        abs_path = malloc(apsz);
        int path_len = abs_path
                           ? snprintf(abs_path, apsz, "%s/%s", root_path, node->file_path)
                           : CBM_NOT_FOUND;
        bool path_ok = path_len >= 0 && (size_t)path_len < apsz &&
                       cbm_path_within_root(root_path, abs_path);
        if (path_ok) {
            if (mode && strcmp(mode, "signature") == 0) {
                truncated = true;
            } else if (mode && strcmp(mode, "head_tail") == 0 && max_lines > 0 &&
                       total_lines > max_lines) {
                int head_count = (max_lines * CBM_SNIPPET_HEAD_PERCENT) /
                                 CBM_SNIPPET_PERCENT_DENOMINATOR;
                int tail_count = max_lines - head_count;
                if (head_count < 1) head_count = 1;
                if (tail_count < 1) tail_count = 1;
                source = read_file_lines(abs_path, start, start + head_count - 1);
                source_tail = read_file_lines(abs_path, end - tail_count + 1, end);
                truncated = true;
            } else if (max_lines > 0 && total_lines > max_lines) {
                end = start + max_lines - 1;
                source = read_file_lines(abs_path, start, end);
                truncated = true;
            } else {
                source = read_file_lines(abs_path, start, end);
            }
        }   /* end if (path_ok) */
    }   /* end if (root_path && node->file_path) */

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root_obj = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root_obj);

    if (node->name && node->name[0] &&
        (!compact || !ends_with_segment(node->qualified_name, node->name)))
        yyjson_mut_obj_add_str(doc, root_obj, "name", node->name);
    yyjson_mut_obj_add_str(doc, root_obj, "qualified_name",
                           node->qualified_name ? node->qualified_name : "");
    if (node->label && node->label[0])
        yyjson_mut_obj_add_str(doc, root_obj, "label", node->label);

    const char *display_path = "";
    if (abs_path) {
        display_path = abs_path;
    } else if (node->file_path) {
        display_path = node->file_path;
    }
    if (display_path[0])
        yyjson_mut_obj_add_str(doc, root_obj, "file_path", display_path);
    yyjson_mut_obj_add_int(doc, root_obj, "start_line", start);
    yyjson_mut_obj_add_int(doc, root_obj, "end_line", end);

    if (mode && strcmp(mode, "signature") == 0) {
        /* Signature mode: source omitted; signature comes from properties below */
    } else if (mode && strcmp(mode, "head_tail") == 0 && source && source_tail) {
        /* Combine head + marker + tail */
        int omitted = total_lines - max_lines;
        char marker[128];
        snprintf(marker, sizeof(marker), "\n[... %d lines omitted ...]\n", omitted);
        size_t combined_sz = strlen(source) + strlen(marker) + strlen(source_tail) + 1;
        char *combined = malloc(combined_sz);
        if (combined) {
            snprintf(combined, combined_sz, "%s%s%s", source, marker, source_tail);
            char *safe_combined = sanitize_utf8_lossy(combined);
            if (safe_combined) {
                yyjson_mut_obj_add_strcpy(doc, root_obj, "source", safe_combined);
                free(safe_combined);
            } else {
                yyjson_mut_obj_add_strcpy(doc, root_obj, "source", combined);
            }
            free(combined);
        } else {
            /* OOM fallback: output head only */
            yyjson_mut_obj_add_str(doc, root_obj, "source", source);
        }
    } else if (source) {
        char *safe_source = sanitize_utf8_lossy(source);
        if (safe_source) {
            yyjson_mut_obj_add_strcpy(doc, root_obj, "source", safe_source);
            free(safe_source);
        } else {
            yyjson_mut_obj_add_str(doc, root_obj, "source", source);
        }
    } else {
        yyjson_mut_obj_add_str(doc, root_obj, "source", "(source not available)");
    }

    /* Truncation metadata */
    if (truncated) {
        yyjson_mut_obj_add_bool(doc, root_obj, "truncated", true);
        yyjson_mut_obj_add_bool(doc, root_obj, "source_clipped", true);
        yyjson_mut_obj_add_int(doc, root_obj, "clipped_at_lines", max_lines);
        yyjson_mut_obj_add_int(doc, root_obj, "total_lines", total_lines);
    }

    /* match_method — omitted for exact matches */
    if (match_method) {
        yyjson_mut_obj_add_str(doc, root_obj, "match_method", match_method);
    }

    /* Enrich with node properties.
     * props_doc is freed AFTER serialization since yyjson_mut_obj_add_str
     * stores pointers into it (zero-copy). */
    yyjson_doc *props_doc = NULL;
    bool include_properties = !compact || (mode && strcmp(mode, "signature") == 0);
    if (include_properties && node->properties_json && node->properties_json[0] != '\0') {
        props_doc = yyjson_read(node->properties_json, strlen(node->properties_json), 0);
        if (props_doc) {
            yyjson_val *props_root = yyjson_doc_get_root(props_doc);
            if (props_root && yyjson_is_obj(props_root)) {
                yyjson_obj_iter iter;
                yyjson_obj_iter_init(props_root, &iter);
                yyjson_val *key;
                while ((key = yyjson_obj_iter_next(&iter))) {
                    yyjson_val *val = yyjson_obj_iter_get_val(key);
                    const char *k = yyjson_get_str(key);
                    if (!k) {
                        continue;
                    }
                    /* Retain useful metadata without returning large indexing
                     * intermediates or overwriting stable response fields. */
                    if (sg_field_blocked(k) ||
                        (strcmp(k, "source") != 0 && yyjson_mut_obj_get(root_obj, k))) {
                        continue;
                    }
                    if (yyjson_is_str(val)) {
                        const char *sv = yyjson_get_str(val);
                        if (sv && sv[0]) {
                            if (strcmp(k, "source") == 0) {
                                yyjson_mut_obj_add_str(doc, root_obj, "property_source", sv);
                            } else {
                                yyjson_mut_obj_add_str(doc, root_obj, k, sv);
                            }
                        }
                    } else if (yyjson_is_bool(val)) {
                        bool bv = yyjson_get_bool(val);
                        /* compact: omit false booleans (false = absent/default) */
                        if (!compact || bv)
                            yyjson_mut_obj_add_bool(doc, root_obj, k, bv);
                    } else if (yyjson_is_int(val)) {
                        int64_t iv = yyjson_get_int(val);
                        /* compact: omit zero integers (0 = absent/default) */
                        if (!compact || iv != 0)
                            yyjson_mut_obj_add_int(doc, root_obj, k, iv);
                    } else if (yyjson_is_real(val)) {
                        double rv = yyjson_get_real(val);
                        if (!compact || rv != 0.0)
                            yyjson_mut_obj_add_real(doc, root_obj, k, rv);
                    }
                }
            }
        }
    }

    /* Caller/callee counts — store already resolved by calling handler */
    cbm_store_t *store = srv->store;
    int in_deg = 0;
    int out_deg = 0;
    cbm_store_node_degree(store, node->id, &in_deg, &out_deg);
    yyjson_mut_obj_add_int(doc, root_obj, "callers", in_deg);
    yyjson_mut_obj_add_int(doc, root_obj, "callees", out_deg);

    add_snippet_coverage_note(doc, root_obj, store, node);

    char **nb_callers = NULL;
    int nb_caller_count = 0;
    char **nb_callees = NULL;
    int nb_callee_count = 0;
    if (include_neighbors) {
        cbm_store_node_neighbor_names(store, node->id, MCP_DEFAULT_LIMIT, &nb_callers,
                                      &nb_caller_count, &nb_callees, &nb_callee_count);
        add_string_array(doc, root_obj, "caller_names", nb_callers, nb_caller_count);
        add_string_array(doc, root_obj, "callee_names", nb_callees, nb_callee_count);
    }

    /* Alternatives (when auto-resolved from ambiguous) */
    if (alternatives && alt_count > 0) {
        yyjson_mut_val *arr = yyjson_mut_arr(doc);
        for (int i = 0; i < alt_count; i++) {
            yyjson_mut_val *a = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_str(doc, a, "qualified_name",
                                   alternatives[i].qualified_name ? alternatives[i].qualified_name
                                                                  : "");
            if (alternatives[i].file_path && alternatives[i].file_path[0])
                yyjson_mut_obj_add_str(doc, a, "file_path", alternatives[i].file_path);
            yyjson_mut_arr_append(arr, a);
        }
        yyjson_mut_obj_add_val(doc, root_obj, "alternatives", arr);
    }

    /* Provenance tagging: keep "source" reserved for the code body. */
    bool snippet_dep = cbm_is_dep_project(node->project, srv->session_project);
    yyjson_mut_obj_add_str(doc, root_obj, "source_origin",
                           snippet_dep ? "dependency" : "project");
    if (snippet_dep) {
        yyjson_mut_obj_add_bool(doc, root_obj, "read_only", true);
    }

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    yyjson_doc_free(props_doc); /* safe if NULL */
    for (int i = 0; i < nb_caller_count; i++) {
        free(nb_callers[i]);
    }
    for (int i = 0; i < nb_callee_count; i++) {
        free(nb_callees[i]);
    }
    free(nb_callers);
    free(nb_callees);
    free(root_path);
    free(abs_path);
    free(source);
    free(source_tail);

    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

static char *handle_get_code_snippet(cbm_mcp_server_t *srv, const char *args) {
    char *qn = cbm_mcp_get_string_arg(args, "qualified_name");
    char *raw_project = get_store_project_arg(args);
    project_expand_t pe = {0};
    cbm_store_t *store = resolve_project_store(srv, raw_project, &pe);
    char *project = pe.value;
    /* When no project param given, try to parse the project prefix from the
     * qualified name by checking for a matching .db file.  This is Option C:
     * the QN is self-describing, so we can always open the right store even on
     * a cold start (no prior search_graph call).
     * Falls back to the currently-open store's project as a secondary option. */
    const char *eff_project = project;
    if (!eff_project && qn) {
        project = extract_project_from_qn(qn);
        if (project) {
            eff_project = project;
            store = resolve_store(srv, project);
        } else if (srv->current_project && srv->current_project[0]) {
            eff_project = srv->current_project;
        }
    }
    bool cfg_compact_g = cbm_config_get_bool(srv->config, "compact", true);
    bool compact = cbm_mcp_get_bool_arg_default(args, "compact", cfg_compact_g);
    bool auto_resolve = cbm_mcp_get_bool_arg(args, "auto_resolve");
    bool include_neighbors = cbm_mcp_get_bool_arg(args, "include_neighbors");
    int cfg_max_lines = cbm_config_get_int(srv->config, CBM_CONFIG_SNIPPET_MAX_LINES,
                                           CBM_DEFAULT_SNIPPET_MAX_LINES);
    int max_lines = cbm_mcp_get_int_arg(args, "max_lines", cfg_max_lines);
    char *snippet_mode = cbm_mcp_get_string_arg(args, "mode");

    if (!qn) {
        free(project);
        free(snippet_mode);
        return cbm_mcp_text_result(
            "{\"error\":\"qualified_name is required\","
            "\"hint\":\"Pass a symbol qualified name, e.g. {\\\"qualified_name\\\":\\\"myapp.src.main.handle_request\\\"}. "
            "Use search_graph to find qualified names.\"}", true);
    }

    if (snippet_mode && strcmp(snippet_mode, "full") != 0 &&
        strcmp(snippet_mode, "signature") != 0 && strcmp(snippet_mode, "head_tail") != 0) {
        char errbuf[256];
        snprintf(errbuf, sizeof(errbuf),
            "{\"error\":\"invalid mode '%s'\","
            "\"hint\":\"Valid values: full, signature, head_tail\"}", snippet_mode);
        free(qn);
        free(project);
        free(snippet_mode);
        return cbm_mcp_text_result(errbuf, true);
    }

    REQUIRE_STORE_EX(store, project, (free(qn), free(snippet_mode), qn = NULL, snippet_mode = NULL));

    /* eff_project already set via resolve_project_store + QN extraction fallback */

    /* Tier 1: Exact QN match */
    cbm_node_t node = {0};
    int rc = cbm_store_find_node_by_qn(store, eff_project, qn, &node);
    if (rc == CBM_STORE_OK) {
        char *result =
            build_snippet_response(srv, &node, NULL /*exact*/, include_neighbors, NULL, 0,
                                      max_lines, snippet_mode, compact);
        free_node_contents(&node);
        free(qn);
        free(project);
        free(snippet_mode);
        return result;
    }

    /* Tier 2: Suffix match — handles partial QNs ("main.HandleRequest")
     * and short names ("ProcessOrder") via LIKE '%.X'. */
    cbm_node_t *suffix_nodes = NULL;
    int suffix_count = 0;
    cbm_store_find_nodes_by_qn_suffix(store, eff_project, qn, &suffix_nodes, &suffix_count);
    if (suffix_count == 1) {
        copy_node(&suffix_nodes[0], &node);
        cbm_store_free_nodes(suffix_nodes, suffix_count);
        char *result = build_snippet_response(srv, &node, "suffix", include_neighbors, NULL, 0,
                                                     max_lines, snippet_mode, compact);
        free_node_contents(&node);
        free(qn);
        free(project);
        free(snippet_mode);
        return result;
    }

    /* Tier 3: Short name match */
    cbm_node_t *name_nodes = NULL;
    int name_count = 0;
    cbm_store_find_nodes_by_name(store, eff_project, qn, &name_nodes, &name_count);
    if (name_count == 1) {
        copy_node(&name_nodes[0], &node);
        cbm_store_free_nodes(name_nodes, name_count);
        cbm_store_free_nodes(suffix_nodes, suffix_count);
        char *result = build_snippet_response(srv, &node, "name", include_neighbors, NULL, 0,
                                                     max_lines, snippet_mode, compact);
        free_node_contents(&node);
        free(qn);
        free(project);
        free(snippet_mode);
        return result;
    }

    /* Ambiguous: collect candidates from suffix + name tiers (dedup by id) */
    int total_cand = suffix_count + name_count;
    if (total_cand > 0) {
        /* Dedup by node ID */
        cbm_node_t *candidates = calloc((size_t)total_cand, sizeof(cbm_node_t));
        int cand_count = 0;

        for (int i = 0; i < suffix_count; i++) {
            copy_node(&suffix_nodes[i], &candidates[cand_count++]);
        }
        for (int i = 0; i < name_count; i++) {
            bool dup = false;
            for (int j = 0; j < cand_count; j++) {
                if (candidates[j].id == name_nodes[i].id) {
                    dup = true;
                    break;
                }
            }
            if (!dup) {
                copy_node(&name_nodes[i], &candidates[cand_count++]);
            }
        }

        cbm_store_free_nodes(suffix_nodes, suffix_count);
        cbm_store_free_nodes(name_nodes, name_count);

        /* Single candidate after dedup — resolve immediately, not ambiguous */
        if (cand_count == 1) {
            copy_node(&candidates[0], &node);
            free_node_contents(&candidates[0]);
            free(candidates);
            char *result = build_snippet_response(srv, &node, "name", include_neighbors, NULL, 0,
                                                         max_lines, snippet_mode, compact);
            free_node_contents(&node);
            free(qn);
            free(project);
            free(snippet_mode);
            return result;
        }

        /* Auto-resolve: pick best candidate by degree when 2+ candidates */
        if (auto_resolve && cand_count >= 2) {
            /* Find best: highest total degree, prefer non-test files */
            int best_idx = 0;
            int best_deg = -1;
            bool best_is_test = false;
            for (int i = 0; i < cand_count; i++) {
                int in_d = 0;
                int out_d = 0;
                cbm_store_node_degree(store, candidates[i].id, &in_d, &out_d);
                int deg = in_d + out_d;
                bool is_test =
                    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
                    candidates[i].file_path && strstr(candidates[i].file_path, "_test") != NULL;
                if (i == 0 || (best_is_test && !is_test) ||
                    (!best_is_test == !is_test && deg > best_deg) ||
                    (!best_is_test == !is_test && deg == best_deg && candidates[i].qualified_name &&
                     best_idx >= 0 && candidates[best_idx].qualified_name &&
                     strcmp(candidates[i].qualified_name, candidates[best_idx].qualified_name) <
                         0)) {
                    best_idx = i;
                    best_deg = deg;
                    best_is_test = is_test;
                }
            }

            copy_node(&candidates[best_idx], &node);

            /* Build alternatives list (skip the picked one) */
            cbm_node_t *alts = calloc((size_t)(cand_count - 1), sizeof(cbm_node_t));
            int alt_count = 0;
            for (int i = 0; i < cand_count; i++) {
                if (i != best_idx) {
                    copy_node(&candidates[i], &alts[alt_count++]);
                }
            }

            for (int i = 0; i < cand_count; i++) {
                free_node_contents(&candidates[i]);
            }
            free(candidates);

            char *result =
                build_snippet_response(srv, &node, "auto_best", include_neighbors, alts, alt_count,
                                          max_lines, snippet_mode, compact);
            free_node_contents(&node);
            for (int i = 0; i < alt_count; i++) {
                free_node_contents(&alts[i]);
            }
            free(alts);
            free(qn);
            free(project);
            free(snippet_mode);
            return result;
        }

        /* Return suggestions */
        char *result = snippet_suggestions(qn, candidates, cand_count);
        for (int i = 0; i < cand_count; i++) {
            free_node_contents(&candidates[i]);
        }
        free(candidates);
        free(qn);
        free(project);
        free(snippet_mode);
        return result;
    }

    cbm_store_free_nodes(suffix_nodes, suffix_count);
    cbm_store_free_nodes(name_nodes, name_count);

    /* Nothing found */
    {
        char errbuf[512];
        snprintf(errbuf, sizeof(errbuf),
            "{\"error\":\"symbol not found: '%s'\","
            "\"hint\":\"Use search_graph with name_pattern to find the correct qualified_name.\"}", qn);
        free(qn);
        free(project);
        free(snippet_mode);
        return cbm_mcp_text_result(errbuf, true);
    }
}

/* ── search_code v2: graph-augmented code search ─────────────── */

/* Strip non-ASCII bytes to guarantee valid UTF-8 JSON output */
enum { ASCII_MAX = 127 };
static void sanitize_ascii(char *s) {
    for (unsigned char *p = (unsigned char *)s; *p; p++) {
        if (*p > ASCII_MAX) {
            *p = '?';
        }
    }
}

/* Intermediate grep match */
typedef struct {
    char file[CBM_SZ_512];
    int line;
    char content[CBM_SZ_1K];
} grep_match_t;

/* Deduped result: one per containing graph node */
typedef struct {
    int64_t node_id; /* 0 = raw match (no containing node) */
    char node_name[CBM_SZ_256];
    char qualified_name[CBM_SZ_512];
    char label[CBM_SZ_64];
    char file[CBM_SZ_512];
    int start_line;
    int end_line;
    int in_degree;
    int out_degree;
    int score;
    int match_lines[CBM_SZ_64];
    int match_count;
} search_result_t;

/* Score a result for ranking: project source first, vendored last, tests lowest */
enum { SCORE_FUNC = 10, SCORE_ROUTE = 15, SCORE_VENDORED = -50, SCORE_TEST = -5 };
enum { MAX_LINE_SPAN = 999999 };

static int compute_search_score(const search_result_t *r) {
    int score = r->in_degree;
    if (strcmp(r->label, "Function") == 0 || strcmp(r->label, "Method") == 0) {
        score += SCORE_FUNC;
    }
    if (strcmp(r->label, "Route") == 0) {
        score += SCORE_ROUTE;
    }
    if (strstr(r->file, "vendored/") || strstr(r->file, "vendor/") ||
        strstr(r->file, "node_modules/")) {
        score += SCORE_VENDORED;
    }
    /* Penalize test files */
    if (strstr(r->file, "test") || strstr(r->file, "spec") || strstr(r->file, "_test.")) {
        score += SCORE_TEST;
    }
    return score;
}

static int search_result_cmp(const void *a, const void *b) {
    const search_result_t *ra = (const search_result_t *)a;
    const search_result_t *rb = (const search_result_t *)b;
    return rb->score - ra->score; /* descending */
}

typedef enum {
    SEARCH_CODE_SCAN_RECURSIVE_GREP = 0,
    SEARCH_CODE_SCAN_FILELIST_GREP = 1,
    SEARCH_CODE_SCAN_GIT_GREP = 2,
} search_code_scan_mode_t;

/* Return true when git can operate on root_path as a worktree. This is a
 * capability probe, not a .git path check, so it also supports subdirectories
 * and linked worktrees. stderr is redirected to keep MCP stdio clean. */
static bool search_code_git_worktree_available(const char *root_path) {
    char cmd[CBM_SZ_2K];
#ifdef _WIN32
    int n = snprintf(cmd, sizeof(cmd),
                     "git -C \"%s\" rev-parse --is-inside-work-tree 2>NUL", root_path);
#else
    int n = snprintf(cmd, sizeof(cmd),
                     "git -C \"%s\" rev-parse --is-inside-work-tree 2>/dev/null", root_path);
#endif
    if (n < 0 || (size_t)n >= sizeof(cmd)) {
        return false;
    }
    FILE *fp = cbm_popen(cmd, "r");
    if (!fp) {
        return false;
    }
    char line[CBM_SZ_64] = "";
    bool ok = fgets(line, sizeof(line), fp) && strncmp(line, "true", CBM_SZ_4) == 0;
    cbm_pclose(fp);
    return ok;
}

/* Build the grep command string based on scoped vs recursive mode.
 * case_sensitive=false adds -i for case-insensitive matching (grep default is sensitive). */
static bool build_grep_cmd(char *cmd, size_t cmd_sz, bool use_regex, bool case_sensitive,
                           search_code_scan_mode_t scan_mode, const char *file_pattern,
                           const char *tmpfile, const char *filelist, const char *root_path) {
#ifdef _WIN32
    const char *simple_match = use_regex ? "" : " -SimpleMatch";
    const char *case_match = case_sensitive ? " -CaseSensitive" : "";
    int n;
    if (scan_mode == SEARCH_CODE_SCAN_GIT_GREP) {
        const char *git_flag = use_regex ? "-E" : "-F";
        const char *ignore_case = case_sensitive ? "" : " -i";
        n = snprintf(cmd, cmd_sz,
                     "git -C \"%s\" grep -n%s --untracked %s -f \"%s\" -- . 2>NUL",
                     root_path, ignore_case, git_flag, tmpfile);
    } else if (scan_mode == SEARCH_CODE_SCAN_FILELIST_GREP) {
        /* #687: PowerShell consumes newline-delimited literal paths; unlike
         * cmd/xargs, spaces remain part of a single filename. */
        n = snprintf(
            cmd, cmd_sz,
            "powershell -Command \"$pat = Get-Content -Encoding UTF8 -LiteralPath '%s'; "
            "Get-Content -Encoding UTF8 -LiteralPath '%s' | ForEach-Object { "
            "Select-String -LiteralPath $_ -Pattern $pat%s%s -ErrorAction SilentlyContinue } "
            "| ForEach-Object { $_.Path + [char]9 + $_.LineNumber + [char]9 + $_.Line }\"",
            tmpfile, filelist, simple_match, case_match);
    } else if (file_pattern) {
        n = snprintf(
            cmd, cmd_sz,
            "powershell -Command \"Get-ChildItem -Recurse -Path '%s\\*' -Include '%s' -File "
            "-ErrorAction SilentlyContinue | Select-String -Pattern "
            "(Get-Content -Encoding UTF8 -LiteralPath '%s')%s%s -ErrorAction SilentlyContinue "
            "| ForEach-Object { $_.Path + [char]9 + $_.LineNumber + [char]9 + $_.Line }\"",
            root_path, file_pattern, tmpfile, simple_match, case_match);
    } else {
        n = snprintf(
            cmd, cmd_sz,
            "powershell -Command \"Get-ChildItem -Recurse -Path '%s\\*' -File "
            "-ErrorAction SilentlyContinue | Select-String -Pattern "
            "(Get-Content -Encoding UTF8 -LiteralPath '%s')%s%s -ErrorAction SilentlyContinue "
            "| ForEach-Object { $_.Path + [char]9 + $_.LineNumber + [char]9 + $_.Line }\"",
            root_path, tmpfile, simple_match, case_match);
    }
    return n >= 0 && (size_t)n < cmd_sz;
#else
    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
    const char *flag = use_regex ? "-E" : "-F";
    const char *ci_flag = case_sensitive ? "" : " -i";
    int n;
    if (scan_mode == SEARCH_CODE_SCAN_GIT_GREP) {
        n = snprintf(cmd, cmd_sz,
                     "git -C \"%s\" grep -n%s --untracked %s -f \"%s\" -- . 2>/dev/null",
                     root_path, ci_flag, flag, tmpfile);
    } else if (scan_mode == SEARCH_CODE_SCAN_FILELIST_GREP) {
        (void)file_pattern;
        /* #687: filelist is NUL-delimited so spaces remain within one path. */
        n = snprintf(cmd, cmd_sz, "xargs -0 grep -Hn%s %s -f '%s' -- < '%s' 2>/dev/null",
                     ci_flag, flag, tmpfile, filelist);
    } else {
        if (file_pattern) {
            n = snprintf(cmd, cmd_sz, "grep -rn%s %s --include='%s' -f '%s' '%s' 2>/dev/null",
                         ci_flag, flag, file_pattern, tmpfile, root_path);
        } else {
            n = snprintf(cmd, cmd_sz, "grep -rn%s %s -f '%s' '%s' 2>/dev/null", ci_flag, flag,
                         tmpfile, root_path);
        }
    }
    return n >= 0 && (size_t)n < cmd_sz;
#endif
}

/* Build deduplicated file list from search results + raw matches. */
static yyjson_mut_val *build_dedup_files_array(yyjson_mut_doc *doc, search_result_t *sr,
                                               int output_count, grep_match_t *raw, int raw_count) {
    yyjson_mut_val *files_arr = yyjson_mut_arr(doc);
    char *seen_files[CBM_SZ_512];
    int seen_count = 0;
    for (int fi = 0; fi < output_count; fi++) {
        bool dup = false;
        for (int j = 0; j < seen_count; j++) {
            if (strcmp(seen_files[j], sr[fi].file) == 0) {
                dup = true;
                break;
            }
        }
        if (!dup && seen_count < CBM_SZ_512) {
            seen_files[seen_count++] = sr[fi].file;
            yyjson_mut_arr_add_str(doc, files_arr, sr[fi].file);
        }
    }
    for (int fi = 0; fi < raw_count && seen_count < CBM_SZ_512; fi++) {
        bool dup = false;
        for (int j = 0; j < seen_count; j++) {
            if (strcmp(seen_files[j], raw[fi].file) == 0) {
                dup = true;
                break;
            }
        }
        if (!dup) {
            seen_files[seen_count++] = raw[fi].file;
            yyjson_mut_arr_add_str(doc, files_arr, raw[fi].file);
        }
    }
    return files_arr;
}

/* Attach source or context lines to a search result JSON item. */
static void attach_result_source(yyjson_mut_doc *doc, yyjson_mut_val *item, search_result_t *r,
                                 int mode, int context_lines, const char *root_path) {
    enum { MODE_FULL = 1 };
    if (r->start_line <= 0 || r->end_line <= 0) {
        return;
    }
    char abs_path[CBM_SZ_1K];
    snprintf(abs_path, sizeof(abs_path), "%s/%s", root_path, r->file);

    /* Containment: a search result whose indexed path resolves outside the
     * project root (a `..` segment, or a symlink/junction that discovery
     * followed) must not be read back into the response. Same guard the
     * snippet path already uses. */
    if (!cbm_path_within_root(root_path, abs_path)) {
        return;
    }

    if (mode == MODE_FULL) {
        /* Cap each hit's source at a match-anchored window: uncapped
         * whole-symbol dumps ran to 5.7KB × N hits (142KB responses). The
         * complete symbol stays one get_code_snippet call away;
         * source_start/source_truncated make the cut explicit. */
        enum { SC_FULL_MAX_LINES = 60, SC_FULL_LEAD = 5 };
        int s = r->start_line;
        int e = r->end_line;
        bool truncated = false;
        if (e - s + 1 > SC_FULL_MAX_LINES) {
            if (r->match_count > 0 && r->match_lines[0] - SC_FULL_LEAD > s) {
                s = r->match_lines[0] - SC_FULL_LEAD;
            }
            e = s + SC_FULL_MAX_LINES - 1;
            if (e > r->end_line) {
                e = r->end_line;
            }
            truncated = true;
        }
        char *source = read_file_lines(abs_path, s, e);
        if (source) {
            sanitize_ascii(source);
            yyjson_mut_obj_add_strcpy(doc, item, "source", source);
            free(source);
            if (truncated) {
                yyjson_mut_obj_add_int(doc, item, "source_start", s);
                yyjson_mut_obj_add_bool(doc, item, "source_truncated", true);
            }
        }
    } else if (context_lines > 0 && r->match_count > 0) {
        int ctx_start = r->match_lines[0] - context_lines;
        int ctx_end = r->match_lines[r->match_count - SKIP_ONE] + context_lines;
        if (ctx_start < SKIP_ONE) {
            ctx_start = SKIP_ONE;
        }
        char *ctx = read_file_lines(abs_path, ctx_start, ctx_end);
        if (ctx) {
            sanitize_ascii(ctx);
            yyjson_mut_obj_add_strcpy(doc, item, "context", ctx);
            yyjson_mut_obj_add_int(doc, item, "context_start", ctx_start);
            free(ctx);
        }
    }
}

/* Build directory distribution object from search results (top-level dir → count). */
/* Aggregate hits by top-level directory. Shared by the JSON object and the
 * TOON table emission. Returns the number of distinct directories. */
static int aggregate_search_dirs(search_result_t *sr, int sr_count, char dir_names[][CBM_SZ_128],
                                 int *dir_counts, int max_dirs) {
    int dir_n = 0;
    for (int di = 0; di < sr_count; di++) {
        char top[CBM_SZ_128] = "";
        const char *slash = strchr(sr[di].file, '/');
        if (slash) {
            size_t dlen = (size_t)(slash - sr[di].file + SKIP_ONE);
            if (dlen >= sizeof(top)) {
                dlen = sizeof(top) - SKIP_ONE;
            }
            memcpy(top, sr[di].file, dlen);
            top[dlen] = '\0';
        } else {
            snprintf(top, sizeof(top), "%s", sr[di].file);
        }
        int found = CBM_NOT_FOUND;
        for (int d = 0; d < dir_n; d++) {
            if (strcmp(dir_names[d], top) == 0) {
                found = d;
                break;
            }
        }
        if (found >= 0) {
            dir_counts[found]++;
        } else if (dir_n < max_dirs) {
            snprintf(dir_names[dir_n], CBM_SZ_128, "%s", top);
            dir_counts[dir_n] = SKIP_ONE;
            dir_n++;
        }
    }
    return dir_n;
}

static yyjson_mut_val *build_dir_distribution(yyjson_mut_doc *doc, search_result_t *sr,
                                              int sr_count) {
    yyjson_mut_val *dirs = yyjson_mut_obj(doc);
    char dir_names[CBM_SZ_64][CBM_SZ_128];
    int dir_counts[CBM_SZ_64];
    int dir_n = aggregate_search_dirs(sr, sr_count, dir_names, dir_counts, CBM_SZ_64);
    for (int d = 0; d < dir_n; d++) {
        yyjson_mut_val *key = yyjson_mut_strcpy(doc, dir_names[d]);
        yyjson_mut_val *val = yyjson_mut_int(doc, dir_counts[d]);
        yyjson_mut_obj_add(dirs, key, val);
    }
    return dirs;
}

/* TOON emission for compact-mode search results: one row per hit
 * (qn/label/file/lines/matches/degrees — `node` dropped, it duplicates the
 * qn's last segment), a raw[] table for uncorrelated matches, a dirs[]
 * distribution table, and the summary scalars. */
static char *assemble_search_output_toon(search_result_t *sr, int sr_count, grep_match_t *raw,
                                         int raw_count, int gm_count, int limit,
                                         const char *project, bool warn_literal_pipe,
                                         uint64_t elapsed_ms) {
    enum { MAX_RAW = 20, SEARCH_SLOW_MS = 5000 };
    cbm_sb_t sb;
    cbm_sb_init(&sb);

    /* Identify the resolved target even when the result set is empty.  This is
     * intentionally the queried project, which may differ from session context. */
    cbm_toon_scalar_str(&sb, "project", project);

    int output_count = sr_count < limit ? sr_count : limit;
    static const char *const cols[] = {"qn", "label", "file", "lines", "matches", "in", "out"};
    cbm_toon_table_header(&sb, "results", output_count, cols, 7);
    for (int ri = 0; ri < output_count; ri++) {
        search_result_t *r = &sr[ri];
        char lines[CBM_SZ_32];
        if (r->start_line > 0) {
            snprintf(lines, sizeof(lines), "%d-%d", r->start_line,
                     r->end_line > r->start_line ? r->end_line : r->start_line);
        } else {
            lines[0] = '\0';
        }
        /* match line numbers ';'-joined (no comma → no cell quoting) */
        char matches[CBM_SZ_256];
        size_t mpos = 0;
        matches[0] = '\0';
        for (int j = 0; j < r->match_count && mpos + 12 < sizeof(matches); j++) {
            int n = snprintf(matches + mpos, sizeof(matches) - mpos, "%s%d", j > 0 ? ";" : "",
                             r->match_lines[j]);
            if (n < 0) {
                break;
            }
            mpos += (size_t)n;
        }
        cbm_toon_row_begin(&sb);
        cbm_toon_cell_str(&sb, r->qualified_name, true);
        cbm_toon_cell_str(&sb, r->label, false);
        cbm_toon_cell_str(&sb, r->file, false);
        cbm_toon_cell_str(&sb, lines, false);
        cbm_toon_cell_str(&sb, matches, false);
        cbm_toon_cell_int(&sb, r->in_degree, false);
        cbm_toon_cell_int(&sb, r->out_degree, false);
        cbm_toon_row_end(&sb);
    }

    int raw_output = raw_count < MAX_RAW ? raw_count : MAX_RAW;
    if (raw_output > 0) {
        static const char *const rcols[] = {"file", "line", "content"};
        cbm_toon_table_header(&sb, "raw", raw_output, rcols, 3);
        for (int ri = 0; ri < raw_output; ri++) {
            cbm_toon_row_begin(&sb);
            cbm_toon_cell_str(&sb, raw[ri].file, true);
            cbm_toon_cell_int(&sb, raw[ri].line, false);
            cbm_toon_cell_str(&sb, raw[ri].content, false);
            cbm_toon_row_end(&sb);
        }
    }

    char dir_names[CBM_SZ_64][CBM_SZ_128];
    int dir_counts[CBM_SZ_64];
    int dir_n = aggregate_search_dirs(sr, sr_count, dir_names, dir_counts, CBM_SZ_64);
    if (dir_n > 0) {
        static const char *const dcols[] = {"dir", "hits"};
        cbm_toon_table_header(&sb, "dirs", dir_n, dcols, 2);
        for (int d = 0; d < dir_n; d++) {
            cbm_toon_row_begin(&sb);
            cbm_toon_cell_str(&sb, dir_names[d], true);
            cbm_toon_cell_int(&sb, dir_counts[d], false);
            cbm_toon_row_end(&sb);
        }
    }

    cbm_toon_scalar_int(&sb, "total_grep_matches", gm_count);
    cbm_toon_scalar_int(&sb, "total_results", sr_count);
    cbm_toon_scalar_int(&sb, "raw_match_count", raw_count);
    cbm_toon_scalar_int(&sb, "elapsed_ms", (long long)elapsed_ms);
    if (warn_literal_pipe) {
        cbm_toon_scalar_str(&sb, "warning",
                            "pattern contains '|' but regex=false, so it is matched literally "
                            "(not as alternation). Pass regex=true for 'foo|bar' to mean "
                            "'foo OR bar'.");
    }
    if (elapsed_ms >= SEARCH_SLOW_MS) {
        cbm_toon_scalar_str(&sb, "warning_slow",
                            "search was slow; narrow file_pattern/path_filter or use a more "
                            "specific pattern");
    }
    return cbm_sb_finish(&sb);
}

/* Phase 4: assemble JSON output from search results */
static char *assemble_search_output(search_result_t *sr, int sr_count, grep_match_t *raw,
                                    int raw_count, int gm_count, int limit, int mode,
                                    int context_lines, const char *root_path, const char *project,
                                    bool warn_literal_pipe, uint64_t elapsed_ms,
                                    const char *search_scope, int dirty_pending,
                                    int dirty_overlay_ready, const char *dirty_warning,
                                    const cbm_store_overlay_node_view_summary_t *overlay_summary) {
    enum {
        MODE_COMPACT = 0,
        MODE_FULL = 1,
        MODE_FILES = 2,
        SEARCH_SLOW_MS = (int)(CBM_SZ_5 * CBM_MSEC_PER_SEC),
    };

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root_obj = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root_obj);

    /* Report the resolved target rather than session context: explicit project
     * selection remains observable for successful and zero-result searches. */
    yyjson_mut_obj_add_str(doc, root_obj, "project", project);

    int output_count = sr_count < limit ? sr_count : limit;

    if (mode == MODE_FILES) {
        yyjson_mut_obj_add_val(doc, root_obj, "files",
                               build_dedup_files_array(doc, sr, output_count, raw, raw_count));
    } else {
        yyjson_mut_val *results_arr = yyjson_mut_arr(doc);
        for (int ri = 0; ri < output_count; ri++) {
            search_result_t *r = &sr[ri];
            yyjson_mut_val *item = yyjson_mut_obj(doc);

            yyjson_mut_obj_add_str(doc, item, "node", r->node_name);
            yyjson_mut_obj_add_str(doc, item, "qualified_name", r->qualified_name);
            yyjson_mut_obj_add_str(doc, item, "label", r->label);
            yyjson_mut_obj_add_str(doc, item, "file", r->file);
            yyjson_mut_obj_add_int(doc, item, "start_line", r->start_line);
            yyjson_mut_obj_add_int(doc, item, "end_line", r->end_line);
            yyjson_mut_obj_add_int(doc, item, "in_degree", r->in_degree);
            yyjson_mut_obj_add_int(doc, item, "out_degree", r->out_degree);

            yyjson_mut_val *ml = yyjson_mut_arr(doc);
            for (int j = 0; j < r->match_count; j++) {
                yyjson_mut_arr_add_int(doc, ml, r->match_lines[j]);
            }
            yyjson_mut_obj_add_val(doc, item, "match_lines", ml);
            attach_result_source(doc, item, r, mode, context_lines, root_path);
            yyjson_mut_arr_add_val(results_arr, item);
        }
        yyjson_mut_obj_add_val(doc, root_obj, "results", results_arr);

        enum { MAX_RAW = 20 };
        yyjson_mut_val *raw_arr = yyjson_mut_arr(doc);
        int raw_output = raw_count < MAX_RAW ? raw_count : MAX_RAW;
        for (int ri = 0; ri < raw_output; ri++) {
            yyjson_mut_val *item = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_str(doc, item, "file", raw[ri].file);
            yyjson_mut_obj_add_int(doc, item, "line", raw[ri].line);
            yyjson_mut_obj_add_str(doc, item, "content", raw[ri].content);
            yyjson_mut_arr_add_val(raw_arr, item);
        }
        yyjson_mut_obj_add_val(doc, root_obj, "matches", raw_arr);
    }

    yyjson_mut_obj_add_val(doc, root_obj, "directories", build_dir_distribution(doc, sr, sr_count));

    /* Summary stats */
    yyjson_mut_obj_add_int(doc, root_obj, "total_grep_matches", gm_count);
    yyjson_mut_obj_add_int(doc, root_obj, "total_results", sr_count);
    yyjson_mut_obj_add_int(doc, root_obj, "raw_match_count", raw_count);
    yyjson_mut_obj_add_int(doc, root_obj, "elapsed_ms", (int)elapsed_ms);
    yyjson_mut_obj_add_str(doc, root_obj, "search_scope",
                           search_scope ? search_scope : "project_recursive");
    if (sr_count > 0 && gm_count > 0) {
        char ratio[CBM_SZ_32];
        snprintf(ratio, sizeof(ratio), "%.1fx", (double)gm_count / (double)(sr_count + raw_count));
        yyjson_mut_obj_add_strcpy(doc, root_obj, "dedup_ratio", ratio);
    }

    /* Warnings: surface common foot-guns instead of leaving them silent. */
    yyjson_mut_val *warnings = yyjson_mut_arr(doc);
    if (warn_literal_pipe) {
        yyjson_mut_arr_add_strcpy(
            doc, warnings,
            "pattern contains '|' but regex=false, so it is matched literally (not as "
            "alternation). Pass regex=true for 'foo|bar' to mean 'foo OR bar'.");
    }
    if (elapsed_ms >= SEARCH_SLOW_MS) {
        char slow[CBM_SZ_128];
        snprintf(slow, sizeof(slow),
                 "search took %dms (>%ds); narrow file_pattern/path_filter or use a more "
                 "specific pattern",
                 (int)elapsed_ms, SEARCH_SLOW_MS / 1000);
        yyjson_mut_arr_add_strcpy(doc, warnings, slow);
        char ems[CBM_SZ_32];
        snprintf(ems, sizeof(ems), "%d", (int)elapsed_ms);
        cbm_log_warn("search.slow", "elapsed_ms", ems); /* visibility in logs */
    }
    if (yyjson_mut_arr_size(warnings) > 0) {
        yyjson_mut_obj_add_val(doc, root_obj, "warnings", warnings);
    }
    cbm_mcp_add_dirty_file_freshness_counts(doc, root_obj, dirty_pending, dirty_overlay_ready,
                                            dirty_warning);
    add_overlay_active_search_code_freshness(doc, root_obj, overlay_summary);

    char *json = yy_doc_to_str(doc);
    if (json) {
        sanitize_ascii(json);
    }
    yyjson_mut_doc_free(doc);

    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

/* Read grep output from fp, parse file:line:content format, apply path filter,
 * and return a dynamically-allocated grep_match_t array. */
/* Strip root path prefix from a file path. */
static const char *strip_root_prefix(const char *path, const char *root, size_t root_len) {
    if (strncmp(path, root, root_len) != 0) {
        return path;
    }
    const char *p = path + root_len;
    if (*p == '/') {
        p++;
    }
    return p;
}

static grep_match_t *collect_grep_matches(FILE *fp, const char *root_path, size_t root_len,
                                          bool has_path_filter, cbm_regex_t *path_regex,
                                          int grep_limit, int *out_count) {
    int gm_cap = CBM_SZ_64;
    int gm_count = 0;
    grep_match_t *gm = malloc(gm_cap * sizeof(grep_match_t));
    char line[CBM_SZ_2K];

    while (fgets(line, sizeof(line), fp) && gm_count < grep_limit) {
        size_t len = strlen(line);
        while (len > 0 && (line[len - SKIP_ONE] == '\n' || line[len - SKIP_ONE] == '\r')) {
            line[--len] = '\0';
        }
        if (len == 0) {
            continue;
        }

        /* PowerShell output uses tab as delimiter (paths may contain colons
         * on Windows, e.g. C:\dir\file). Unix grep uses colon. */
#ifdef _WIN32
        char sep = '\t';
#else
        char sep = ':';
#endif
        char *sep1 = strchr(line, (unsigned char)sep);
        if (!sep1) {
            continue;
        }
        char *sep2 = strchr(sep1 + SKIP_ONE, (unsigned char)sep);
        if (!sep2) {
            continue;
        }
        *sep1 = '\0';
        *sep2 = '\0';

#ifdef _WIN32
        cbm_normalize_path_sep(line);
#endif
        const char *path = line;
        const char *file = strip_root_prefix(path, root_path, root_len);

        if (has_path_filter && cbm_regexec(path_regex, file, 0, NULL, 0) != CBM_REG_OK) {
            continue;
        }

        safe_grow(gm, gm_count, gm_cap, PAIR_LEN);
        snprintf(gm[gm_count].file, sizeof(gm[0].file), "%s", file);
        gm[gm_count].line = (int)strtol(sep1 + SKIP_ONE, NULL, CBM_DECIMAL_BASE);
        snprintf(gm[gm_count].content, sizeof(gm[0].content), "%s", sep2 + SKIP_ONE);
        sanitize_ascii(gm[gm_count].content);
        gm_count++;
    }

    *out_count = gm_count;
    return gm;
}

/* Find the tightest node containing a line in a file. Returns index or -1. */
static int find_tightest_node(cbm_node_t *nodes, int count, int line) {
    int best = CBM_NOT_FOUND;
    int best_span = MAX_LINE_SPAN;
    for (int j = 0; j < count; j++) {
        if (nodes[j].start_line <= line && nodes[j].end_line >= line) {
            int span = nodes[j].end_line - nodes[j].start_line;
            if (span < best_span) {
                best = j;
                best_span = span;
            }
        }
    }
    return best;
}

/* Add a grep hit to the search result set (merge into existing or create new). */
static bool search_result_matches_node(const search_result_t *r, const cbm_node_t *n) {
    if (!r || !n) {
        return false;
    }
    if (n->id > CBM_STORE_NO_NODE_ID) {
        return r->node_id == n->id;
    }
    if (r->node_id != CBM_STORE_NO_NODE_ID) {
        return false;
    }
    const char *qn = n->qualified_name ? n->qualified_name : "";
    if (qn[0] && strcmp(r->qualified_name, qn) == 0) {
        return true;
    }
    const char *name = n->name ? n->name : "";
    const char *file = n->file_path ? n->file_path : "";
    return qn[0] == '\0' && strcmp(r->node_name, name) == 0 && strcmp(r->file, file) == 0;
}

static void add_to_search_results(search_result_t **sr, int *sr_count, int *sr_cap, cbm_node_t *n,
                                  int line) {
    for (int j = 0; j < *sr_count; j++) {
        if (search_result_matches_node(&(*sr)[j], n)) {
            if ((*sr)[j].match_count < CBM_SZ_64) {
                (*sr)[j].match_lines[(*sr)[j].match_count++] = line;
            }
            return;
        }
    }
    if (*sr_count >= *sr_cap) {
        *sr_cap *= PAIR_LEN;
        *sr = safe_realloc(*sr, *sr_cap * sizeof(search_result_t));
        memset(&(*sr)[*sr_count], 0, (*sr_cap - *sr_count) * sizeof(search_result_t));
    }
    search_result_t *r = &(*sr)[*sr_count];
    r->node_id = n->id;
    snprintf(r->node_name, sizeof(r->node_name), "%s", n->name ? n->name : "");
    snprintf(r->qualified_name, sizeof(r->qualified_name), "%s",
             n->qualified_name ? n->qualified_name : "");
    snprintf(r->label, sizeof(r->label), "%s", n->label ? n->label : "");
    snprintf(r->file, sizeof(r->file), "%s", n->file_path ? n->file_path : "");
    r->start_line = n->start_line;
    r->end_line = n->end_line;
    r->match_lines[0] = line;
    r->match_count = SKIP_ONE;
    (*sr_count)++;
}

/* Match a single grep hit to the tightest containing node, then add to sr or raw. */
static void classify_grep_hit(grep_match_t *hit, cbm_node_t *file_nodes, int file_node_count,
                              search_result_t **sr, int *sr_count, int *sr_cap, grep_match_t **raw,
                              int *raw_count, int *raw_cap) {
    int best = find_tightest_node(file_nodes, file_node_count, hit->line);
    if (best >= 0) {
        add_to_search_results(sr, sr_count, sr_cap, &file_nodes[best], hit->line);
    } else {
        if (*raw_count >= *raw_cap) {
            *raw_cap = (*raw_cap == 0) ? CBM_SZ_32 : *raw_cap * PAIR_LEN;
            *raw = safe_realloc(*raw, *raw_cap * sizeof(grep_match_t));
        }
        if (*raw) {
            (*raw)[(*raw_count)++] = *hit;
        }
    }
}

/* Free a file_nodes array returned from cbm_store_find_nodes_by_file. */
static void free_file_nodes(cbm_node_t *nodes, int count) {
    for (int j = 0; j < count; j++) {
        safe_str_free(&nodes[j].project);
        safe_str_free(&nodes[j].label);
        safe_str_free(&nodes[j].name);
        safe_str_free(&nodes[j].qualified_name);
        safe_str_free(&nodes[j].file_path);
        safe_str_free(&nodes[j].properties_json);
    }
    free(nodes);
}

/* Classify all grep matches file-by-file into search results and raw hits. */
static void classify_all_grep_hits(grep_match_t *gm, int gm_count, cbm_store_t *store,
                                   const char *project, search_result_t **sr, int *sr_count,
                                   int *sr_cap, grep_match_t **raw, int *raw_count, int *raw_cap,
                                   bool use_overlay_view) {
    qsort(gm, gm_count, sizeof(grep_match_t), (int (*)(const void *, const void *))strcmp);
    int i = 0;
    while (i < gm_count) {
        const char *cur_file = gm[i].file;
        int file_start = i;
        while (i < gm_count && strcmp(gm[i].file, cur_file) == 0) {
            i++;
        }
        cbm_node_t *file_nodes = NULL;
        int file_node_count = 0;
        if (store) {
            if (use_overlay_view) {
                cbm_store_find_nodes_by_file_overlay_view(store, project, cur_file, &file_nodes,
                                                          &file_node_count);
            } else {
                cbm_store_find_nodes_by_file(store, project, cur_file, &file_nodes,
                                             &file_node_count);
            }
        }
        for (int mi = file_start; mi < i; mi++) {
            classify_grep_hit(&gm[mi], file_nodes, file_node_count, sr, sr_count, sr_cap, raw,
                              raw_count, raw_cap);
        }
        free_file_nodes(file_nodes, file_node_count);
    }
}


/* Validate shell-safe arguments for search. */
/* Search/grep paths and globs are ALWAYS single-quoted (POSIX sh) or
 * double-/single-quoted (Windows cmd/PowerShell) on the command line, which
 * neutralises '&' — a very common character in real paths (R&D, "Foo & Bar",
 * OneDrive). Accept '&' here while still rejecting every metacharacter that
 * could break out of the quoting (#272). */
static bool validate_search_path_arg(const char *s) {
    if (!s) {
        return false;
    }
    for (const char *p = s; *p; p++) {
        switch (*p) {
        case '\'':
        case '"':
        case ';':
        case '|':
        case '$':
        case '`':
        case '<':
        case '>':
        case '\n':
        case '\r':
#ifndef _WIN32
        case '\\':
#endif
            return false;
        default:
            break;
        }
    }
    return true;
}

static bool validate_search_args(const char *root_path, const char *file_pattern) {
    if (!validate_search_path_arg(root_path)) {
        return false;
    }
    if (file_pattern && !validate_search_path_arg(file_pattern)) {
        return false;
    }
    return true;
}

static bool search_code_file_pattern_matches(const char *file_pattern, const char *rel_path) {
    if (!file_pattern || !file_pattern[0]) {
        return true;
    }
    if (!rel_path) {
        return false;
    }
    if (sqlite3_strglob(file_pattern, rel_path) == 0) {
        return true;
    }
    const char *base = strrchr(rel_path, '/');
#ifdef _WIN32
    const char *backslash = strrchr(rel_path, '\\');
    if (!base || (backslash && backslash > base)) {
        base = backslash;
    }
#endif
    base = base ? base + SKIP_ONE : rel_path;
    return sqlite3_strglob(file_pattern, base) == 0;
}

/* Write an absolute file list from indexed graph files. POSIX uses NUL records
 * for xargs -0; Windows uses newline records for PowerShell Get-Content.
 * Returns true when an indexed file set existed, even if file_pattern matched
 * zero files; that preserves upstream's "indexed scope first" behavior instead
 * of falling through to an unbounded recursive scan. */
static bool write_scoped_filelist(cbm_mcp_server_t *srv, const char *project,
                                  const char *root_path, const char *file_pattern,
                                  const char *filelist) {
    cbm_store_t *pre_store = resolve_store(srv, project);
    if (!pre_store) {
        return false;
    }

    char **indexed_files = NULL;
    int indexed_count = 0;
    int rc = cbm_store_list_files(pre_store, project, &indexed_files, &indexed_count);
    if (rc != CBM_STORE_OK || indexed_count <= 0) {
        free_counted_string_array(indexed_files, indexed_count);
        return false;
    }

    FILE *fl = fopen(filelist, "wb");
    if (!fl) {
        free_counted_string_array(indexed_files, indexed_count);
        return false;
    }

    bool ok = true;
    for (int fi = 0; fi < indexed_count; fi++) {
        const char *rel = indexed_files[fi];
        if (!search_code_file_pattern_matches(file_pattern, rel) ||
            (rel && strpbrk(rel, "\r\n"))) {
            continue;
        }
        char abs_path[CBM_PATH_MAX];
        int n = snprintf(abs_path, sizeof(abs_path), "%s/%s", root_path, rel ? rel : "");
        if (n < 0 || (size_t)n >= sizeof(abs_path)) {
            continue;
        }
        size_t len = strlen(abs_path);
        if (fwrite(abs_path, SKIP_ONE, len, fl) != len ||
#ifdef _WIN32
            fputc('\n', fl) == EOF
#else
            fputc('\0', fl) == EOF
#endif
        ) {
            ok = false;
            break;
        }
    }
    if (fclose(fl) != 0) {
        ok = false;
    }
    free_counted_string_array(indexed_files, indexed_count);
    if (!ok) {
        cbm_unlink(filelist);
    }
    return ok;
}

static bool search_rel_path_is_safe(const char *rel_path) {
    if (!rel_path || !rel_path[0] || rel_path[0] == '/' || rel_path[0] == '\\') {
        return false;
    }
    if (((rel_path[0] >= 'a' && rel_path[0] <= 'z') ||
         (rel_path[0] >= 'A' && rel_path[0] <= 'Z')) &&
        rel_path[1] == ':') {
        return false;
    }

    const char *seg = rel_path;
    for (const char *p = rel_path;; p++) {
        if (*p == '/' || *p == '\\' || *p == '\0') {
            size_t len = (size_t)(p - seg);
            if ((len == SKIP_ONE && seg[0] == '.') ||
                (len == PAIR_LEN && seg[0] == '.' && seg[SKIP_ONE] == '.')) {
                return false;
            }
            if (*p == '\0') {
                break;
            }
            seg = p + SKIP_ONE;
        }
    }
    return true;
}

static bool regex_meta_char(char c) {
    return strchr(".^$*+?()[]{}|\\", c) != NULL;
}

static bool extract_exact_path_filter(const char *filter, char *out, size_t out_sz) {
    if (!filter || !out || out_sz == 0) {
        return false;
    }
    out[0] = '\0';
    size_t len = strlen(filter);
    if (len < CBM_SZ_3 || filter[0] != '^' || filter[len - SKIP_ONE] != '$') {
        return false;
    }

    size_t pos = 0;
    for (size_t i = SKIP_ONE; i + SKIP_ONE < len; i++) {
        char c = filter[i];
        if (c == '\\') {
            if (i + PAIR_LEN > len - SKIP_ONE) {
                return false;
            }
            c = filter[++i];
            if (!regex_meta_char(c)) {
                return false;
            }
        } else if (regex_meta_char(c)) {
            return false;
        }
        if (pos + SKIP_ONE >= out_sz) {
            out[0] = '\0';
            return false;
        }
        out[pos++] = c;
    }
    out[pos] = '\0';
    cbm_normalize_path_sep(out);
    return validate_search_path_arg(out) && search_rel_path_is_safe(out);
}

/* Write pattern to a temp file for grep -f. Returns true on success. */
static bool write_pattern_file(char *tmpfile, int tmpfile_sz, const char *pattern) {
    if (!tmpfile || tmpfile_sz <= 0 || !pattern) {
        return false;
    }
    int n = snprintf(tmpfile, (size_t)tmpfile_sz, "%s/cbm_search_XXXXXX", cbm_tmpdir());
    if (n < 0 || n >= tmpfile_sz) {
        return false;
    }
    int fd = cbm_mkstemp_s(tmpfile, (size_t)tmpfile_sz);
    if (fd < 0) {
        return false;
    }
#ifdef _WIN32
    FILE *tf = _fdopen(fd, "w");
#else
    FILE *tf = fdopen(fd, "w");
#endif
    if (!tf) {
        cbm_close_fd(fd);
        cbm_unlink(tmpfile);
        return false;
    }
    bool ok = fputs(pattern, tf) >= 0 && fputc('\n', tf) != EOF;
    if (fclose(tf) != 0) {
        ok = false;
    }
    if (!ok) {
        cbm_unlink(tmpfile);
        return false;
    }
    return true;
}

/* Compile a path filter regex. Returns true if compiled successfully. */
static bool compile_path_filter(const char *filter, cbm_regex_t *re) {
    if (!filter || !filter[0]) {
        return false;
    }
    return cbm_regcomp(re, filter, CBM_REG_EXTENDED | CBM_REG_NOSUB) == CBM_REG_OK;
}

static char *handle_search_code(cbm_mcp_server_t *srv, const char *args) {
    cbm_mcp_output_format_t response_format = cbm_mcp_response_format(srv, args);
    if (response_format == CBM_MCP_OUTPUT_INVALID) {
        return cbm_mcp_invalid_response_format();
    }
    char *pattern = cbm_mcp_get_string_arg(args, "pattern");
    char *project = get_project_arg(args);
    char *file_pattern = cbm_mcp_get_string_arg(args, "file_pattern");
    char *path_filter = cbm_mcp_get_string_arg(args, "path_filter");
    char exact_filter_path[CBM_PATH_MAX];
    exact_filter_path[0] = '\0';
    bool has_exact_filter_path =
        !file_pattern && extract_exact_path_filter(path_filter, exact_filter_path,
                                                   sizeof(exact_filter_path));
    char *mode_str = cbm_mcp_get_string_arg(args, "mode");
    int context_lines = cbm_mcp_get_int_arg(args, "context", 0);
    int cfg_search_limit_sc = cbm_config_get_int(srv->config, CBM_CONFIG_SEARCH_LIMIT,
                                                CBM_MCP_DEFAULT_SEARCH_LIMIT);
    int limit = cbm_mcp_get_positive_int_arg(args, "limit", cfg_search_limit_sc,
                                             CBM_MCP_DEFAULT_SEARCH_LIMIT);
    bool use_regex = cbm_mcp_get_bool_arg(args, "regex");
    uint64_t search_t0 = cbm_now_ms();
    /* In literal (non-regex) mode a '|' is matched as a byte, not alternation —
     * a common silent 0-match trap; flagged in the result warnings (#282). */
    bool pat_has_pipe = pattern && strchr(pattern, '|') != NULL;

    /* Parse mode: compact (default), full, files.
     * "summary" is NOT valid for source grep — it belongs to graph mode. Warn if passed. */
    enum { MODE_COMPACT, MODE_FULL, MODE_FILES };
    int mode = MODE_COMPACT;
    bool mode_warning = false; /* set if an invalid mode value was passed */
    char mode_warning_msg[256];
    mode_warning_msg[0] = '\0';
    if (mode_str) {
        if (strcmp(mode_str, "full") == 0) {
            mode = MODE_FULL;
        } else if (strcmp(mode_str, "files") == 0) {
            mode = MODE_FILES;
        } else if (strcmp(mode_str, "compact") == 0) {
            mode = MODE_COMPACT; /* explicit compact is fine — it's the default */
        } else {
            /* Unknown mode for source grep — warn and use default (compact) */
            mode_warning = true;
            if (strcmp(mode_str, "summary") == 0) {
                snprintf(mode_warning_msg, sizeof(mode_warning_msg),
                    "mode='summary' is only valid for graph mode (default dispatch), not source grep. "
                    "For source grep use mode='compact' (default), 'full', or 'files'. "
                    "Using mode='compact' for this request.");
            } else {
                snprintf(mode_warning_msg, sizeof(mode_warning_msg),
                    "unknown mode '%s' for source grep; valid values: compact (default), full, files. "
                    "Using mode='compact' for this request.", mode_str);
            }
        }
        free(mode_str);
    }

    cbm_regex_t path_regex;
    bool has_path_filter = compile_path_filter(path_filter, &path_regex);
    free(path_filter);
    path_filter = NULL;

    if (!pattern) {
        if (has_path_filter) {
            cbm_regfree(&path_regex);
        }
        free(project);
        free(file_pattern);
        return cbm_mcp_text_result(
            "{\"error\":\"pattern is required\","
            "\"hint\":\"Pass a text pattern or regex (with regex:true) to search source code.\"}", true);
    }

    /* Use the same project and automatic-indexing authority as graph tools.
     * It joins an in-flight startup index (or performs configured first-use
     * indexing) before a missing store can be reported.  The returned store is
     * retained below for graph annotations, avoiding a second cache lookup. */
    project_expand_t pe = {0};
    cbm_store_t *store = resolve_project_store(srv, project, &pe);
    project = pe.value; /* resolve_project_store consumes the raw argument. */
    REQUIRE_STORE_EX(store, project, {
        if (has_path_filter) {
            cbm_regfree(&path_regex);
        }
        free(pattern);
        free(file_pattern);
    });

    char *root_path = get_project_root_from_store(store, project);
    if (!root_path) {
        if (has_path_filter) {
            cbm_regfree(&path_regex);
        }
        free(pattern);
        free(project);
        free(file_pattern);
        char *_err = build_project_list_error("project not found or not indexed");
        char *_res = cbm_mcp_text_result(_err, true);
        free(_err);
        return _res;
    }

    if (!validate_search_args(root_path, file_pattern)) {
        if (has_path_filter) {
            cbm_regfree(&path_regex);
        }
        free(root_path);
        free(pattern);
        free(project);
        free(file_pattern);
        return cbm_mcp_text_result("path or file_pattern contains invalid characters", true);
    }

    /* issue #283: when regex=true, a syntactically invalid pattern (e.g. an
     * unclosed group) makes the underlying grep fail, which the handler would
     * otherwise report as an empty result set — indistinguishable from a
     * legitimate no-match. Validate the user's regex up front and return an
     * explicit error so callers can tell "broken pattern" from "no matches". */
    if (use_regex) {
        cbm_regex_t probe;
        if (cbm_regcomp(&probe, pattern, CBM_REG_EXTENDED | CBM_REG_NOSUB) != CBM_REG_OK) {
            if (has_path_filter) {
                cbm_regfree(&path_regex);
            }
            free(root_path);
            free(pattern);
            free(project);
            free(file_pattern);
            return cbm_mcp_text_result(
                "invalid regex pattern (regex=true): check for unbalanced (), [], or {}", true);
        }
        cbm_regfree(&probe);
    }

    /* ── Phase 0.5: Multi-word → regex conversion ───────────── */
    /* If pattern contains whitespace and is not already a regex, convert to a
     * regex that matches all words in order: "foo bar baz" → "foo.*bar.*baz".
     * This avoids requiring the exact phrase as a contiguous substring. */
    if (!use_regex && strchr(pattern, ' ')) {
        size_t plen = strlen(pattern);
        /* Worst case: every char is a space → ".*" between each char */
        char *regex_pat = malloc(plen * 3 + 1);
        if (regex_pat) {
            char *dst = regex_pat;
            const char *src = pattern;
            bool in_space = false;
            while (*src) {
                if (*src == ' ' || *src == '\t') {
                    if (!in_space) {
                        *dst++ = '.';
                        *dst++ = '*';
                        in_space = true;
                    }
                } else {
                    /* Escape regex metacharacters from user input */
                    if (strchr("\\^$.|?*+()[]{}", *src)) {
                        *dst++ = '\\';
                    }
                    *dst++ = *src;
                    in_space = false;
                }
                src++;
            }
            *dst = '\0';
            free(pattern);
            pattern = regex_pat;
            use_regex = true;
        }
    }

    /* ── Phase 1: Grep scan ──────────────────────────────────── */
    char tmpfile[CBM_PATH_MAX];
    if (!write_pattern_file(tmpfile, sizeof(tmpfile), pattern)) {
        char errmsg[CBM_SZ_256];
        snprintf(errmsg, sizeof(errmsg), "search failed: cannot create temp file (%s)",
                 strerror(errno));
        if (has_path_filter) {
            cbm_regfree(&path_regex);
        }
        free(root_path);
        free(pattern);
        free(project);
        free(file_pattern);
        return cbm_mcp_text_result(
            "{\"error\":\"search failed: could not create temp file\","
            "\"hint\":\"Check that /tmp is writable and has disk space.\"}", true);
    }

    /* Case-sensitivity: default case-insensitive (grep -i), opt-in case-sensitive. */
    bool case_sensitive = cbm_mcp_get_bool_arg(args, "case_sensitive");

    /* No grep-level match limit — let grep find all matches, then dedup and
     * cap in our code. The -m flag caused results from large vendored files
     * to exhaust the quota before reaching project source files. */
    enum { GREP_MAX_MATCHES = 500 };
    int grep_limit = GREP_MAX_MATCHES;

    /* Default to the full project root so active edits and newly created files
     * are searchable before the next index. Exact literal path_filter values can
     * safely narrow traversal to one file; general regex path_filter stays a
     * post-filter because approximating regexes as filesystem globs is lossy. */
    char filelist[CBM_PATH_MAX];
    int filelist_len = snprintf(filelist, sizeof(filelist), "%s.files", tmpfile);
    if (filelist_len < 0 || (size_t)filelist_len >= sizeof(filelist)) {
        cbm_unlink(tmpfile);
        if (has_path_filter) {
            cbm_regfree(&path_regex);
        }
        free(root_path);
        free(pattern);
        free(project);
        free(file_pattern);
        return cbm_mcp_text_result(
            "{\"error\":\"search failed: temporary file path too long\","
            "\"hint\":\"Use a shorter TMPDIR/TEMP path or project path.\"}", true);
    }
    search_code_scan_mode_t scan_mode = SEARCH_CODE_SCAN_RECURSIVE_GREP;
    char grep_root[CBM_PATH_MAX];
    const char *grep_target = root_path;
    if (has_exact_filter_path) {
        int gn = snprintf(grep_root, sizeof(grep_root), "%s/%s", root_path, exact_filter_path);
        if (gn < 0 || (size_t)gn >= sizeof(grep_root) || !validate_search_path_arg(grep_root)) {
            cbm_unlink(tmpfile);
            free(root_path);
            free(pattern);
            free(project);
            free(file_pattern);
            if (has_path_filter) {
                cbm_regfree(&path_regex);
            }
            return cbm_mcp_text_result(
                "{\"error\":\"search failed: exact path_filter path too long\","
                "\"hint\":\"Use a shorter project path or path_filter.\"}", true);
        }
        grep_target = grep_root;
    } else if (!file_pattern && search_code_git_worktree_available(root_path)) {
        scan_mode = SEARCH_CODE_SCAN_GIT_GREP;
    } else if (file_pattern &&
               write_scoped_filelist(srv, project, root_path, file_pattern, filelist)) {
        scan_mode = SEARCH_CODE_SCAN_FILELIST_GREP;
    }

    char cmd[CBM_SZ_4K];
    if (!build_grep_cmd(cmd, sizeof(cmd), use_regex, case_sensitive, scan_mode, file_pattern,
                        tmpfile, filelist, grep_target)) {
        cbm_unlink(tmpfile);
        if (scan_mode == SEARCH_CODE_SCAN_FILELIST_GREP) {
            cbm_unlink(filelist);
        }
        if (has_path_filter) {
            cbm_regfree(&path_regex);
        }
        free(root_path);
        free(pattern);
        free(project);
        free(file_pattern);
        return cbm_mcp_text_result(
            "{\"error\":\"search failed: grep command too long\","
            "\"hint\":\"Use a shorter project path or file_pattern.\"}", true);
    }

    FILE *fp = cbm_popen(cmd, "r");
    if (!fp) {
        cbm_unlink(tmpfile);
        if (scan_mode == SEARCH_CODE_SCAN_FILELIST_GREP) {
            cbm_unlink(filelist);
        }
        if (has_path_filter) {
            cbm_regfree(&path_regex);
        }
        free(root_path);
        free(pattern);
        free(project);
        free(file_pattern);
        return cbm_mcp_text_result(
            "{\"error\":\"search failed: grep command could not execute\","
            "\"hint\":\"Check that grep is installed and the project root directory exists.\"}", true);
    }

    /* Collect grep matches into array */
    int gm_count = 0;
    grep_match_t *gm = collect_grep_matches(fp, root_path, strlen(root_path), has_path_filter,
                                            &path_regex, grep_limit, &gm_count);
    cbm_pclose(fp);
    cbm_unlink(tmpfile);
    if (scan_mode == SEARCH_CODE_SCAN_FILELIST_GREP) {
        cbm_unlink(filelist);
    }

    /* ── Phase 2+3: Block expansion + graph ranking ──────────── */
    /* Sort grep matches by file for contiguous processing.
     * Then: one SQL query per unique file for nodes, one batch query for all degrees. */

    int sr_cap = CBM_SZ_32;
    int sr_count = 0;
    search_result_t *sr = calloc(sr_cap, sizeof(search_result_t));

    int raw_cap = CBM_SZ_32;
    int raw_count = 0;
    grep_match_t *raw = malloc(raw_cap * sizeof(grep_match_t));

    /* Sort matches by file path for contiguous per-file processing */
    qsort(gm, gm_count, sizeof(grep_match_t), (int (*)(const void *, const void *))strcmp);

    cbm_store_overlay_node_view_summary_t overlay_summary = {0};
    bool overlay_ready_for_code =
        store && project && project[0] &&
        cbm_store_get_overlay_node_view_summary(store, project, &overlay_summary) ==
            CBM_STORE_OK &&
        cbm_store_overlay_node_view_has_ready_rows(&overlay_summary);

    classify_all_grep_hits(gm, gm_count, store, project, &sr, &sr_count, &sr_cap, &raw, &raw_count,
                           &raw_cap, overlay_ready_for_code);

    /* Phase 3: batch degree query — ONE query for all results instead of 2×N */
    if (store && sr_count > 0) {
        int64_t *ids = malloc(sr_count * sizeof(int64_t));
        int *in_degs = malloc(sr_count * sizeof(int));
        int *out_degs = malloc(sr_count * sizeof(int));
        if (ids && in_degs && out_degs) {
            for (int j = 0; j < sr_count; j++) {
                ids[j] = sr[j].node_id;
            }
            if (cbm_store_batch_count_degrees(store, ids, sr_count, "CALLS", in_degs,
                                              out_degs) == CBM_STORE_OK) {
                for (int j = 0; j < sr_count; j++) {
                    sr[j].in_degree = in_degs[j];
                    sr[j].out_degree = out_degs[j];
                }
            }
        }
        free(ids);
        free(in_degs);
        free(out_degs);
    }

    /* Compute scores and sort */
    for (int j = 0; j < sr_count; j++) {
        sr[j].score = compute_search_score(&sr[j]);
    }
    if (sr_count > SKIP_ONE) {
        qsort(sr, sr_count, sizeof(search_result_t), search_result_cmp);
    }

    /* ── Phase 4: Context assembly (extracted helper) ─────────── */

    const char *search_scope = "project_recursive";
    if (has_exact_filter_path) {
        search_scope = "path_filter_exact";
    } else if (scan_mode == SEARCH_CODE_SCAN_GIT_GREP) {
        search_scope = "git_worktree";
    } else if (scan_mode == SEARCH_CODE_SCAN_FILELIST_GREP) {
        search_scope = "indexed_files";
    }
    int dirty_pending = 0;
    int dirty_overlay_ready = 0;
    if (!get_dirty_file_counts(store, project, &dirty_pending, &dirty_overlay_ready) &&
        srv->store && srv->store != store) {
        get_dirty_file_counts(srv->store, project, &dirty_pending, &dirty_overlay_ready);
    }

    bool sc_legacy_json = response_format == CBM_MCP_OUTPUT_JSON;
    bool needs_freshness_json = overlay_ready_for_code || dirty_pending > 0 ||
                                dirty_overlay_ready > 0;
    char *result = NULL;
    if (mode == MODE_COMPACT && !sc_legacy_json && !needs_freshness_json) {
        char *toon_text =
            assemble_search_output_toon(sr, sr_count, raw, raw_count, gm_count, limit, project,
                                        pat_has_pipe && !use_regex, cbm_now_ms() - search_t0);
        result = cbm_mcp_text_result(toon_text ? toon_text : "out of memory",
                                     toon_text == NULL);
        free(toon_text);
    } else {
        result = assemble_search_output(
            sr, sr_count, raw, raw_count, gm_count, limit, mode, context_lines, root_path, project,
            pat_has_pipe && !use_regex, cbm_now_ms() - search_t0, search_scope, dirty_pending,
            dirty_overlay_ready,
            overlay_ready_for_code
                ? "search_code reads live source files and uses active overlay graph annotations "
                  "where ready; pending dirty files may still lack graph metadata until overlay "
                  "or reindex completes."
                : "search_code reads live source files, but graph annotations use canonical graph "
                  "rows; dirty file graph metadata may be absent until overlay or reindex "
                  "completes.",
            overlay_ready_for_code ? &overlay_summary : NULL);
    }
    free(gm);
    free(sr);
    free(raw);
    free(root_path);
    free(pattern);
    free(project);
    free(file_pattern);
    if (has_path_filter) {
        cbm_regfree(&path_regex);
    }

    /* Inject mode warning into the result JSON if an unsupported mode was passed */
    if (mode_warning && result) {
        /* result is a JSON object like {"matches":[...],"count":N}
         * Inject "mode_warning":"..." by appending before the closing brace */
        size_t rlen = strlen(result);
        /* Locate the last '}' to insert before it */
        if (rlen > 0 && result[rlen - 1] == '}') {
            size_t needed = rlen + strlen(mode_warning_msg) + 32;
            char *warned = (char *)malloc(needed);
            if (warned) {
                /* Chop the closing brace, add warning field, re-close */
                memcpy(warned, result, rlen - 1);
                warned[rlen - 1] = '\0';
                /* Check if the existing JSON object has any fields */
                bool has_fields = strchr(result, ':') != NULL;
                if (has_fields) {
                    snprintf(warned + rlen - 1, needed - rlen + 1,
                             ",\"mode_warning\":\"%s\"}", mode_warning_msg);
                } else {
                    snprintf(warned + rlen - 1, needed - rlen + 1,
                             "\"mode_warning\":\"%s\"}", mode_warning_msg);
                }
                free(result);
                result = warned;
            }
        }
    }

    return result;
}

/* ── detect_changes ───────────────────────────────────────────── */

/* Collect BFS seed ids: every symbol DEFINED in a changed file (everything but
 * the structural container labels — those have no CALLS edges). These anchor
 * the multi-source impact traversal. */
static void detect_collect_seeds(cbm_store_t *store, const char *project, const char *file,
                                 int64_t **seeds, int *n, int *cap) {
    cbm_node_t *nodes = NULL;
    int ncount = 0;
    cbm_store_find_nodes_by_file(store, project, file, &nodes, &ncount);
    for (int i = 0; i < ncount; i++) {
        const char *lb = nodes[i].label;
        if (lb && strcmp(lb, "File") != 0 && strcmp(lb, "Folder") != 0 &&
            strcmp(lb, "Project") != 0 && strcmp(lb, "Module") != 0 && strcmp(lb, "Package") != 0 &&
            strcmp(lb, "Section") != 0) {
            if (*n >= *cap) {
                *cap = *cap ? *cap * 2 : 16;
                *seeds = safe_realloc(*seeds, (size_t)*cap * sizeof(int64_t));
            }
            (*seeds)[(*n)++] = nodes[i].id;
        }
    }
    cbm_store_free_nodes(nodes, ncount);
}

/* Module key for the impacted rollup = the first TWO path segments
 * ("src/mcp/mcp.c" -> "src/mcp"), a quotient of the blast radius coarse enough
 * to fit yet specific enough to localize (one segment collapses a whole tree
 * to "src"). Falls back to one segment, then the whole path. */
static void detect_module_of(const char *file, char *out, size_t outsz) {
    if (!file || !file[0]) {
        snprintf(out, outsz, "(root)");
        return;
    }
    const char *s1 = strchr(file, '/');
    if (!s1) {
        snprintf(out, outsz, "%s", file);
        return;
    }
    const char *s2 = strchr(s1 + 1, '/');
    size_t len = s2 ? (size_t)(s2 - file) : strlen(file);
    if (len >= outsz) {
        len = outsz - 1;
    }
    memcpy(out, file, len);
    out[len] = '\0';
}

/* Aggregate the impact set into the 2-segment module rollup. Fills up to
 * DETECT_MODCAP (module, count) pairs; symbols beyond the cap land in
 * *overflow (surfaced as "(other)", never silently dropped). Shared by the
 * tree and json emitters so both encodings carry the same model. */
enum { DETECT_MODCAP = 256 };

static int detect_module_rollup(const cbm_traverse_result_t *impact, char mods[][CBM_SZ_128],
                                int *mcnt, int *overflow) {
    int nmods = 0;
    *overflow = 0;
    for (int i = 0; i < impact->visited_count; i++) {
        char m[CBM_SZ_128];
        detect_module_of(impact->visited[i].node.file_path, m, sizeof(m));
        int j = 0;
        for (; j < nmods; j++) {
            if (strcmp(mods[j], m) == 0) {
                mcnt[j]++;
                break;
            }
        }
        if (j == nmods) {
            if (nmods < DETECT_MODCAP) {
                snprintf(mods[nmods], CBM_SZ_128, "%s", m);
                mcnt[nmods] = 1;
                nmods++;
            } else {
                (*overflow)++;
            }
        }
    }
    return nmods;
}

/* Local formatting helpers keep detect_changes independent of search/trace
 * presentation internals while preserving deterministic grouping. */
static size_t detect_qn_prefix_len(const char *qn) {
    const char *last = qn ? strrchr(qn, '.') : NULL;
    return last ? (size_t)(last - qn) : 0U;
}

static int detect_hop_cmp_qn(const void *left, const void *right) {
    const cbm_node_hop_t *a = (const cbm_node_hop_t *)left;
    const cbm_node_hop_t *b = (const cbm_node_hop_t *)right;
    const char *aqn = a->node.qualified_name ? a->node.qualified_name : "";
    const char *bqn = b->node.qualified_name ? b->node.qualified_name : "";
    int comparison = strcmp(aqn, bqn);
    if (comparison != 0) {
        return comparison;
    }
    if (a->hop != b->hop) {
        return a->hop < b->hop ? -1 : 1;
    }
    return (a->node.id > b->node.id) - (a->node.id < b->node.id);
}

/* Emit the impacted set as a grouped tree leg: rows grouped under their shared
 * (qn-prefix, file), `name label hop` per row. At most `limit` rows are listed
 * (the visited array is hop-ordered, so the closest — highest-signal — impact
 * shows first); impacted_total always carries the exact full count, and
 * `impacted_shown < impacted_total` is the honest truncation signal. The
 * module rollup (emitted by the caller) stays complete regardless. */
static void detect_emit_impacted_tree(cbm_sb_t *sb, cbm_traverse_result_t *tr, int limit) {
    cbm_tree_scalar_int(sb, "impacted_total", tr->visited_count);
    int shown = tr->visited_count < limit ? tr->visited_count : limit;
    /* qn order for stable grouping, but keep hop-closeness: sort by (hop) is
     * lost under qn sort, so group AFTER selecting the nearest `shown` rows —
     * the visited array is already (hop,id)-ordered from the BFS. */
    char hdr[CBM_SZ_128];
    snprintf(hdr, sizeof(hdr),
             "impacted_shown: %d\nimpacted: %d  (rows: name label hop; qn = group prefix + \".\" "
             "+ name; nearest hops first)\n",
             shown, shown);
    cbm_sb_append(sb, hdr);
    if (shown > 1) {
        qsort(tr->visited, (size_t)shown, sizeof(cbm_node_hop_t), detect_hop_cmp_qn);
    }
    char cur_group[CBM_SZ_1K] = "";
    for (int i = 0; i < shown; i++) {
        const char *qn =
            tr->visited[i].node.qualified_name ? tr->visited[i].node.qualified_name : "";
        const char *file = tr->visited[i].node.file_path ? tr->visited[i].node.file_path : "";
        size_t plen = detect_qn_prefix_len(qn);
        char group[CBM_SZ_1K];
        snprintf(group, sizeof(group), "%.*s (%s)", (int)plen, qn, file);
        if (strcmp(group, cur_group) != 0) {
            snprintf(cur_group, sizeof(cur_group), "%s", group);
            cbm_sb_append(sb, group);
            cbm_sb_append(sb, ":\n");
        }
        char row[CBM_SZ_512];
        snprintf(row, sizeof(row), "  %s %s %d\n", plen ? qn + plen + 1 : qn,
                 tr->visited[i].node.label ? tr->visited[i].node.label : "", tr->visited[i].hop);
        cbm_sb_append(sb, row);
    }
    if (shown < tr->visited_count) {
        char more[CBM_SZ_256];
        snprintf(more, sizeof(more),
                 "impacted_omitted: %d  (see impacted_modules for the full rollup; raise 'limit' "
                 "or lower 'depth' to see specifics)\n",
                 tr->visited_count - shown);
        cbm_sb_append(sb, more);
    }
}

static char *handle_detect_changes(cbm_mcp_server_t *srv, const char *args) {
    char *project = get_project_arg(args);
    char *base_branch = cbm_mcp_get_string_arg(args, "base_branch");
    char *since = cbm_mcp_get_string_arg(args, "since");
    char *scope = cbm_mcp_get_string_arg(args, "scope");
    int depth = cbm_mcp_get_int_arg(args, "depth", MCP_DEFAULT_BFS_DEPTH);
    depth = clamp_mcp_depth(depth, "detect_changes");

    /* scope: "files" = just changed files, "symbols" = files + symbols (default) */
    bool want_symbols = !scope || strcmp(scope, "symbols") == 0 || strcmp(scope, "impact") == 0;

    if (!project || project[0] == '\0') {
        char *err = build_missing_project_error();
        char *result = cbm_mcp_text_result(err, true);
        free(err);
        free(project);
        free(base_branch);
        free(since);
        free(scope);
        return result;
    }

    /* `since` (e.g. "HEAD~10", "v0.5.0") is the documented diff base but was
     * previously parsed and never used: it takes precedence over base_branch.
     * Route it through base_branch so the shared shell-arg validation and the
     * existing `<base>...HEAD` (three-dot) diff apply unchanged — `since` thus
     * adopts the same merge-base semantics base_branch already uses. */
    if (since && since[0]) {
        free(base_branch);
        base_branch = since; /* transfer ownership */
        since = NULL;
    }
    free(since); /* no-op after the swap (since is NULL); frees it otherwise */

    if (!base_branch) {
        base_branch = heap_strdup("main");
    }

    /* Reject shell metacharacters, and a leading '-', in the user-supplied
     * branch name. base_branch is spliced into `git diff --name-only
     * "<base>"...HEAD`; a value starting with '-' would be read by git as an
     * option rather than a ref (e.g. `--output=<path>` writes the diff to an
     * arbitrary file). A real git ref never begins with '-'. */
    if (!cbm_validate_shell_arg(base_branch) || base_branch[0] == '-') {
        free(project);
        free(base_branch);
        free(scope);
        return cbm_mcp_text_result("base_branch contains invalid characters", true);
    }

    char *root_path = get_project_root(srv, project);
    if (!root_path) {
        char *err = build_project_list_error_srv(srv, "project not found or not indexed");
        char *res = cbm_mcp_text_result(err, true);
        free(err);
        free(project);
        free(base_branch);
        free(scope);
        return res;
    }

    if (!validate_search_path_arg(root_path)) {
        free(root_path);
        free(project);
        free(base_branch);
        free(scope);
        return cbm_mcp_text_result("project path contains invalid characters", true);
    }

    /* Get changed files via git (-C avoids cd + quoting issues on Windows).
     * Three sources are merged:
     *   1. committed changes vs base   (diff <base>...HEAD)
     *   2. unstaged tracked changes    (diff)
     *   3. untracked + staged-new files (status --porcelain) — these are
     *      invisible to `git diff` and were silently missed before, so a
     *      brand-new file never appeared until a manual re-index (#520).
     * status --porcelain prefixes each path with a 2-char code + space
     * ("?? path", "A  path"); the prefix is stripped when parsing below. */
    char cmd[CBM_SZ_2K];
    int cmd_len;
#ifdef _WIN32
    cmd_len = snprintf(cmd, sizeof(cmd),
                       "git -C \"%s\" diff --name-only \"%s\"...HEAD 2>NUL & "
                       "git -C \"%s\" diff --name-only 2>NUL & "
                       "git --no-optional-locks -C \"%s\" status --porcelain "
                       "--untracked-files=normal 2>NUL",
                       root_path, base_branch, root_path, root_path);
#else
    cmd_len = snprintf(cmd, sizeof(cmd),
                       "{ git -C '%s' diff --name-only '%s'...HEAD 2>/dev/null; "
                       "git -C '%s' diff --name-only 2>/dev/null; "
                       "git --no-optional-locks -C '%s' status --porcelain "
                       "--untracked-files=normal 2>/dev/null; } | sort -u",
                       root_path, base_branch, root_path, root_path);
#endif
    if (cmd_len < 0 || (size_t)cmd_len >= sizeof(cmd)) {
        free(root_path);
        free(project);
        free(base_branch);
        free(scope);
        return cbm_mcp_text_result(
            "git diff command is too long; use a shorter project path or branch name", true);
    }

    FILE *fp = cbm_popen(cmd, "r");
    if (!fp) {
        char errmsg[CBM_SZ_256];
        snprintf(errmsg, sizeof(errmsg),
                 "git diff failed: cannot execute command (%s). Check that git is installed.",
                 strerror(errno));
        free(root_path);
        free(project);
        free(base_branch);
        free(scope);
        return cbm_mcp_text_result(errmsg, true);
    }

    /* resolve_store already called via get_project_root above */
    cbm_store_t *store = srv->store;

    /* Direction of impact. Default inbound = the BLAST RADIUS: the transitive
     * CALLERS of the changed symbols, which may need review. outbound = what
     * the changed code depends on; both = union. */
    char *direction = cbm_mcp_get_string_arg(args, "direction");
    if (!direction) {
        direction = heap_strdup("inbound");
    }
    /* Teaching error, same contract as trace_path: never silently correct an
     * unknown direction — the caller would misread the result's semantics. */
    if (strcmp(direction, "inbound") != 0 && strcmp(direction, "outbound") != 0 &&
        strcmp(direction, "both") != 0) {
        char errbuf[CBM_SZ_256];
        snprintf(errbuf, sizeof(errbuf),
                 "invalid direction \"%s\" — use \"inbound\" (blast radius: transitive callers), "
                 "\"outbound\" (dependencies), or \"both\"",
                 direction);
        free(direction);
        free(root_path);
        free(project);
        free(base_branch);
        free(scope);
        (void)cbm_pclose(fp);
        return cbm_mcp_text_result(errbuf, true);
    }
    cbm_mcp_output_format_t response_format = cbm_mcp_response_format(srv, args);
    if (response_format == CBM_MCP_OUTPUT_INVALID) {
        free(direction);
        free(root_path);
        free(project);
        free(base_branch);
        free(scope);
        (void)cbm_pclose(fp);
        return cbm_mcp_invalid_response_format();
    }
    bool legacy_json = response_format == CBM_MCP_OUTPUT_JSON;

    /* Per-symbol impacted-row display cap (the module rollup stays complete).
     * impacted_total always reports the true count, so this never hides scale. */
    int imp_limit = cbm_mcp_get_int_arg(args, "limit", MCP_DEFAULT_IMPACT_LIMIT);
    if (imp_limit < 1) {
        imp_limit = 1;
    }
    if (imp_limit > MCP_BFS_LIMIT_MAX) {
        imp_limit = MCP_BFS_LIMIT_MAX;
    }

    /* Collect changed file paths into a C array (drives seeds, the rollup, and
     * both output encodings). */
    char **files = NULL;
    int file_count = 0;
    int file_cap = 0;
    int64_t *seeds = NULL;
    int seed_count = 0;
    int seed_cap = 0;

    char line[CBM_SZ_1K];
    while (fgets(line, sizeof(line), fp)) {
        size_t len = strlen(line);
        while (len > 0 && (line[len - SKIP_ONE] == '\n' || line[len - SKIP_ONE] == '\r')) {
            line[--len] = '\0';
        }
        if (len == 0) {
            continue;
        }
        /* Strip the `git status --porcelain` 2-char code + space; for a rename
         * ("R  old -> new") keep the destination path. */
        char *path_line = line;
        if (len > PAIR_LEN && line[PAIR_LEN] == ' ' && strchr(" MADRCU?!", line[0]) &&
            strchr(" MADRCU?!", line[1])) {
            path_line = line + PAIR_LEN + SKIP_ONE;
            char *arrow = strstr(path_line, " -> ");
            if (arrow) {
                enum { ARROW_LEN = 4 };
                path_line = arrow + ARROW_LEN;
            }
        }
        if (path_line[0] == '\0') {
            continue;
        }
        /* Dedup: the three git sources are sorted+unioned on POSIX but not on
         * Windows (separate commands), and a path can repeat. */
        bool dup = false;
        for (int i = 0; i < file_count; i++) {
            if (strcmp(files[i], path_line) == 0) {
                dup = true;
                break;
            }
        }
        if (dup) {
            continue;
        }
        if (file_count >= file_cap) {
            file_cap = file_cap ? file_cap * 2 : 16;
            files = safe_realloc(files, (size_t)file_cap * sizeof(char *));
        }
        files[file_count++] = heap_strdup(path_line);
        if (want_symbols) {
            detect_collect_seeds(store, project, path_line, &seeds, &seed_count, &seed_cap);
        }
    }
    int git_status = cbm_pclose(fp);

    /* merge-base SHA: the exact commit the diff is measured against, so the
     * result is reproducible even as base_branch advances. Best-effort. */
    char merge_base[64] = "";
    {
        char mbcmd[CBM_SZ_2K];
#ifdef _WIN32
        snprintf(mbcmd, sizeof(mbcmd), "git -C \"%s\" merge-base \"%s\" HEAD 2>NUL", root_path,
                 base_branch);
#else
        snprintf(mbcmd, sizeof(mbcmd), "git -C '%s' merge-base '%s' HEAD 2>/dev/null", root_path,
                 base_branch);
#endif
        FILE *mbfp = cbm_popen(mbcmd, "r");
        if (mbfp) {
            if (fgets(merge_base, sizeof(merge_base), mbfp)) {
                size_t l = strlen(merge_base);
                while (l > 0 && (merge_base[l - 1] == '\n' || merge_base[l - 1] == '\r')) {
                    merge_base[--l] = '\0';
                }
            }
            (void)cbm_pclose(mbfp);
        }
    }

    /* The impact traversal: ONE multi-source BFS over all seeds. */
    cbm_traverse_result_t impact = {0};
    bool truncated = false;
    char impact_error[CBM_SZ_256] = "";
    if (want_symbols && seed_count > 0) {
        if (cbm_store_bfs_multi(store, seeds, seed_count, direction, NULL, 0, depth,
                                MCP_BFS_LIMIT_MAX, &impact, &truncated) != CBM_STORE_OK) {
            snprintf(impact_error, sizeof(impact_error),
                     "impact traversal failed: %s; changed_files remains valid, retry after "
                     "reindexing or use scope='files'",
                     cbm_store_error(store));
        }
    }

    bool is_error = (git_status != 0 && file_count == 0) || impact_error[0] != '\0';
    char *out_str = NULL;

    if (!legacy_json) {
        cbm_sb_t sb;
        cbm_sb_init(&sb);
        cbm_tree_scalar_str(&sb, "base", base_branch);
        if (merge_base[0]) {
            cbm_tree_scalar_str(&sb, "merge_base", merge_base);
        }
        cbm_tree_scalar_str(&sb, "direction", direction);
        if (is_error) {
            char hint_buf[CBM_SZ_256];
            if (impact_error[0]) {
                snprintf(hint_buf, sizeof(hint_buf), "%s", impact_error);
            } else {
                snprintf(hint_buf, sizeof(hint_buf),
                         "git diff exited with status %d. Check that branch '%s' exists.",
                         git_status, base_branch);
            }
            cbm_tree_scalar_str(&sb, "hint", hint_buf);
        }
        /* changed files (the git result) */
        char cf[CBM_SZ_64];
        snprintf(cf, sizeof(cf), "changed_files: %d\n", file_count);
        cbm_sb_append(&sb, cf);
        for (int i = 0; i < file_count; i++) {
            cbm_sb_append(&sb, "  ");
            cbm_sb_append(&sb, files[i]);
            cbm_sb_append(&sb, "\n");
        }
        cbm_tree_scalar_int(&sb, "seed_symbols", seed_count);
        if (want_symbols) {
            detect_emit_impacted_tree(&sb, &impact, imp_limit);
            /* module rollup: a quotient view of the blast radius */
            if (impact.visited_count > 0) {
                cbm_sb_append(&sb, "impacted_modules: (rows: module count)\n");
                char (*mods)[CBM_SZ_128] = malloc(DETECT_MODCAP * CBM_SZ_128);
                int *mcnt = malloc(DETECT_MODCAP * sizeof(int));
                if (mods && mcnt) {
                    int overflow = 0;
                    int nmods = detect_module_rollup(&impact, mods, mcnt, &overflow);
                    for (int j = 0; j < nmods; j++) {
                        char mrow[CBM_SZ_256];
                        snprintf(mrow, sizeof(mrow), "  %s %d\n", mods[j], mcnt[j]);
                        cbm_sb_append(&sb, mrow);
                    }
                    if (overflow > 0) {
                        char orow[CBM_SZ_128];
                        snprintf(orow, sizeof(orow), "  (other) %d\n", overflow);
                        cbm_sb_append(&sb, orow);
                    }
                }
                free(mods);
                free(mcnt);
            }
            if (truncated) {
                cbm_tree_scalar_bool(&sb, "truncated", true);
                cbm_tree_scalar_str(&sb, "hint",
                                    "impact hit the safety ceiling — narrow with a lower "
                                    "'depth' or a smaller diff");
            }
        }
        out_str = cbm_sb_finish(&sb);
    } else {
        /* format:"json" = json-stringified tree: same model, structured. */
        yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
        yyjson_mut_val *root_obj = yyjson_mut_obj(doc);
        yyjson_mut_doc_set_root(doc, root_obj);
        yyjson_mut_obj_add_strcpy(doc, root_obj, "base", base_branch);
        if (merge_base[0]) {
            yyjson_mut_obj_add_strcpy(doc, root_obj, "merge_base", merge_base);
        }
        yyjson_mut_obj_add_strcpy(doc, root_obj, "direction", direction);
        yyjson_mut_val *cf = yyjson_mut_arr(doc);
        for (int i = 0; i < file_count; i++) {
            yyjson_mut_arr_add_strcpy(doc, cf, files[i]);
        }
        yyjson_mut_obj_add_val(doc, root_obj, "changed_files", cf);
        yyjson_mut_obj_add_int(doc, root_obj, "seed_symbols", seed_count);
        yyjson_mut_obj_add_int(doc, root_obj, "impacted_total", impact.visited_count);
        int imp_shown = impact.visited_count < imp_limit ? impact.visited_count : imp_limit;
        yyjson_mut_obj_add_int(doc, root_obj, "impacted_shown", imp_shown);
        yyjson_mut_val *imp = yyjson_mut_arr(doc);
        for (int i = 0; i < imp_shown; i++) {
            yyjson_mut_val *o = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_strcpy(
                doc, o, "qn",
                impact.visited[i].node.qualified_name ? impact.visited[i].node.qualified_name : "");
            yyjson_mut_obj_add_strcpy(
                doc, o, "label", impact.visited[i].node.label ? impact.visited[i].node.label : "");
            yyjson_mut_obj_add_strcpy(
                doc, o, "file",
                impact.visited[i].node.file_path ? impact.visited[i].node.file_path : "");
            yyjson_mut_obj_add_int(doc, o, "hop", impact.visited[i].hop);
            yyjson_mut_arr_add_val(imp, o);
        }
        yyjson_mut_obj_add_val(doc, root_obj, "impacted", imp);
        /* Model parity with the tree encoding: the complete module rollup. */
        if (impact.visited_count > 0) {
            char (*mods)[CBM_SZ_128] = malloc(DETECT_MODCAP * CBM_SZ_128);
            int *mcnt = malloc(DETECT_MODCAP * sizeof(int));
            if (mods && mcnt) {
                int overflow = 0;
                int nmods = detect_module_rollup(&impact, mods, mcnt, &overflow);
                yyjson_mut_val *rollup = yyjson_mut_arr(doc);
                for (int j = 0; j < nmods; j++) {
                    yyjson_mut_val *o = yyjson_mut_obj(doc);
                    yyjson_mut_obj_add_strcpy(doc, o, "module", mods[j]);
                    yyjson_mut_obj_add_int(doc, o, "count", mcnt[j]);
                    yyjson_mut_arr_add_val(rollup, o);
                }
                if (overflow > 0) {
                    yyjson_mut_val *o = yyjson_mut_obj(doc);
                    yyjson_mut_obj_add_strcpy(doc, o, "module", "(other)");
                    yyjson_mut_obj_add_int(doc, o, "count", overflow);
                    yyjson_mut_arr_add_val(rollup, o);
                }
                yyjson_mut_obj_add_val(doc, root_obj, "impacted_modules", rollup);
            }
            free(mods);
            free(mcnt);
        }
        yyjson_mut_obj_add_bool(doc, root_obj, "truncated", truncated);
        if (is_error) {
            char hint_buf[CBM_SZ_256];
            if (impact_error[0]) {
                snprintf(hint_buf, sizeof(hint_buf), "%s", impact_error);
            } else {
                snprintf(hint_buf, sizeof(hint_buf),
                         "git diff exited with status %d. Check that branch '%s' exists.",
                         git_status, base_branch);
            }
            yyjson_mut_obj_add_strcpy(doc, root_obj, "hint", hint_buf);
        }
        out_str = yy_doc_to_str(doc);
        yyjson_mut_doc_free(doc);
    }

    cbm_store_traverse_free(&impact);
    for (int i = 0; i < file_count; i++) {
        free(files[i]);
    }
    free(files);
    free(seeds);
    free(direction);
    free(root_path);
    free(project);
    free(base_branch);
    free(scope);

    char *result = cbm_mcp_text_result(out_str, is_error);
    free(out_str);
    return result;
}

/* ── manage_adr ───────────────────────────────────────────────── */

/* ADR "sections" mode: list markdown headers ('#'-prefixed lines) from the
 * ADR content string. */
static void adr_list_sections_from_content(yyjson_mut_doc *doc, yyjson_mut_val *root_obj,
                                           const char *content) {
    yyjson_mut_val *sections = yyjson_mut_arr(doc);
    const char *p = content;
    while (p && *p) {
        const char *eol = strchr(p, '\n');
        size_t linelen = eol ? (size_t)(eol - p) : strlen(p);
        while (linelen > 0 && p[linelen - SKIP_ONE] == '\r') {
            linelen--;
        }
        if (linelen > 0 && p[0] == '#') {
            char hdr[CBM_SZ_1K];
            if (linelen >= sizeof(hdr)) {
                linelen = sizeof(hdr) - SKIP_ONE;
            }
            memcpy(hdr, p, linelen);
            hdr[linelen] = '\0';
            yyjson_mut_arr_add_strcpy(doc, sections, hdr);
        }
        if (!eol) {
            break;
        }
        p = eol + SKIP_ONE;
    }
    yyjson_mut_obj_add_val(doc, root_obj, "sections", sections);
}

/* Read the legacy file-based ADR (<root>/.codebase-memory/adr.md), used by
 * older versions. Returns a heap buffer (caller frees) or NULL if missing/
 * empty. Kept only to migrate old ADRs into the store (#256). */
static char *adr_read_legacy_file(const char *root_path) {
    if (!root_path) {
        return NULL;
    }
    char adr_path[CBM_SZ_4K];
    snprintf(adr_path, sizeof(adr_path), "%s/.codebase-memory/adr.md", root_path);
    FILE *fp = cbm_fopen(adr_path, "r");
    if (!fp) {
        return NULL;
    }
    (void)fseek(fp, 0, SEEK_END);
    long sz = ftell(fp);
    if (sz <= 0) {
        (void)fclose(fp);
        return NULL;
    }
    (void)fseek(fp, 0, SEEK_SET);
    char *buf = malloc((size_t)sz + SKIP_ONE);
    if (!buf) {
        (void)fclose(fp);
        return NULL;
    }
    size_t n = fread(buf, SKIP_ONE, (size_t)sz, fp);
    buf[n] = '\0';
    (void)fclose(fp);
    if (buf[0] == '\0') {
        free(buf);
        return NULL;
    }
    return buf;
}

#define ADR_EMPTY_HINT                                                             \
    "No ADR yet. Create one with manage_adr(mode='update', "                       \
    "content='## PURPOSE\\n...\\n\\n## STACK\\n...\\n\\n## ARCHITECTURE\\n..."     \
    "\\n\\n## PATTERNS\\n...\\n\\n## TRADEOFFS\\n...\\n\\n## PHILOSOPHY\\n...'). " \
    "For guided creation: explore the codebase with get_architecture, "            \
    "then draft and store. Sections: PURPOSE, STACK, ARCHITECTURE, "               \
    "PATTERNS, TRADEOFFS, PHILOSOPHY."

static char *handle_manage_adr(cbm_mcp_server_t *srv, const char *args) {
    char *project = get_project_arg(args);
    char *mode_str = cbm_mcp_get_string_arg(args, "mode");
    char *content = cbm_mcp_get_string_arg(args, "content");

    if (!mode_str) {
        mode_str = heap_strdup("get");
    }

    /* No project specified and no session project → cannot locate an ADR.
     * (resolve_store would fall back to the default in-memory store, which is
     * non-NULL and would mask the missing-project condition.) */
    if ((!project || project[0] == '\0') && srv->session_project[0] == '\0') {
        free(project);
        free(mode_str);
        free(content);
        char *err = build_missing_project_error();
        char *result = cbm_mcp_text_result(err, true);
        free(err);
        return result;
    }
    if (!project || project[0] == '\0') {
        free(project);
        project = heap_strdup(srv->session_project);
    }

    /* ADRs are stored in the SQLite store (project_summaries), the SAME
     * backend the UI /api/adr endpoints use — so writes via the MCP tool and
     * the UI are visible to each other (#256). */
    cbm_store_t *resolved = resolve_store(srv, project);
    if (!resolved) {
        char *err = build_project_list_error_srv(srv, "project not found or not indexed");
        char *res = cbm_mcp_text_result(err, true);
        free(err);
        free(project);
        free(mode_str);
        free(content);
        return res;
    }

    /* resolve_store opens file-backed projects READ-ONLY (query stores must
     * not mutate the DB). manage_adr is the only resolve_store caller that
     * WRITES, so it needs a writable handle. For a file-backed project open a
     * dedicated read-write handle to the same DB file (the project is verified
     * to exist via resolve_store, so cbm_store_open_path won't create a ghost
     * DB). For an in-memory / embedded store (db_path == NULL) the resolved
     * store is already writable — use it directly. */
    cbm_store_t *store = resolved;
    cbm_store_t *owned_rw = NULL;
    const char *resolved_db_path = cbm_store_db_path(resolved);
    if (resolved_db_path) {
        owned_rw = cbm_store_open_path_existing(resolved_db_path);
        if (!owned_rw) {
            char *err = build_project_list_error_srv(srv, "project store could not be opened read-write");
            char *res = cbm_mcp_text_result(err, true);
            free(err);
            free(project);
            free(mode_str);
            free(content);
            return res;
        }
        store = owned_rw;
    }

    /* One-time migration: older versions wrote ADRs to a file at
     * <root>/.codebase-memory/adr.md. If the store has no ADR yet but that
     * legacy file exists, import it so nothing is lost on upgrade. */
    cbm_adr_t adr;
    memset(&adr, 0, sizeof(adr));
    bool have_adr = (cbm_store_adr_get(store, project, &adr) == CBM_STORE_OK);
    if (!have_adr) {
        char *root_path = get_project_root(srv, project);
        char *legacy = adr_read_legacy_file(root_path);
        free(root_path);
        if (legacy) {
            if (cbm_store_adr_store(store, project, legacy) == CBM_STORE_OK) {
                have_adr = (cbm_store_adr_get(store, project, &adr) == CBM_STORE_OK);
            }
            free(legacy);
        }
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root_obj = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root_obj);

    bool is_error = false;
    if ((strcmp(mode_str, "update") == 0 || strcmp(mode_str, "store") == 0) && content) {
        if (cbm_store_adr_store(store, project, content) == CBM_STORE_OK) {
            yyjson_mut_obj_add_str(doc, root_obj, "status", "updated");
        } else {
            yyjson_mut_obj_add_str(doc, root_obj, "status", "write_error");
            is_error = true;
        }
    } else if (strcmp(mode_str, "sections") == 0) {
        adr_list_sections_from_content(doc, root_obj, have_adr ? adr.content : NULL);
    } else { /* get */
        if (have_adr && adr.content) {
            yyjson_mut_obj_add_strcpy(doc, root_obj, "content", adr.content);
        } else {
            yyjson_mut_obj_add_str(doc, root_obj, "content", "");
            yyjson_mut_obj_add_str(doc, root_obj, "status", "no_adr");
            yyjson_mut_obj_add_str(doc, root_obj, "adr_hint", ADR_EMPTY_HINT);
        }
    }

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    if (have_adr) {
        cbm_store_adr_free(&adr);
    }
    if (owned_rw) {
        cbm_store_close(owned_rw);
    }
    free(project);
    free(mode_str);
    free(content);

    char *result = cbm_mcp_text_result(json, is_error);
    free(json);
    return result;
}

/* ── ingest_traces ────────────────────────────────────────────── */

static char *handle_ingest_traces(cbm_mcp_server_t *srv, const char *args) {
    (void)srv;
    /* Parse traces array from JSON args */
    yyjson_doc *adoc = yyjson_read(args, strlen(args), 0);
    int trace_count = 0;

    if (adoc) {
        yyjson_val *aroot = yyjson_doc_get_root(adoc);
        yyjson_val *traces = yyjson_obj_get(aroot, "traces");
        if (traces && yyjson_is_arr(traces)) {
            trace_count = (int)yyjson_arr_size(traces);
        }
        yyjson_doc_free(adoc);
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_obj_add_str(doc, root, "status", "accepted");
    yyjson_mut_obj_add_int(doc, root, "traces_received", trace_count);
    yyjson_mut_obj_add_str(doc, root, "note",
                           "Runtime edge creation from traces not yet implemented");

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);

    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

/* ── index_dependencies ───────────────────────────────────────── */

static char *handle_index_dependencies(cbm_mcp_server_t *srv, const char *args) {
    char *raw_project = get_store_project_arg(args);
    char *pkg_mgr_str = cbm_mcp_get_string_arg(args, "package_manager");

    if (!raw_project) {
        free(pkg_mgr_str);
        return cbm_mcp_text_result("{\"error\":\"project is required\"}", true);
    }

    /* Parse packages[] array */
    yyjson_doc *doc_args = yyjson_read(args, strlen(args), 0);
    yyjson_val *root_args = yyjson_doc_get_root(doc_args);
    yyjson_val *packages_val = yyjson_obj_get(root_args, "packages");
    yyjson_val *source_paths_val = yyjson_obj_get(root_args, "source_paths");

    if (!packages_val || !yyjson_is_arr(packages_val) || yyjson_arr_size(packages_val) == 0) {
        yyjson_doc_free(doc_args);
        free(raw_project);
        free(pkg_mgr_str);
        return cbm_mcp_text_result(
            "{\"error\":\"packages[] is required\"}", true);
    }

    bool has_paths = source_paths_val && yyjson_is_arr(source_paths_val);
    bool has_mgr = pkg_mgr_str != NULL;
    if (!has_paths && !has_mgr) {
        yyjson_doc_free(doc_args);
        free(raw_project);
        free(pkg_mgr_str);
        return cbm_mcp_text_result(
            "{\"error\":\"Either source_paths[] or package_manager is required\"}", true);
    }

    /* DRY: expand "self"/"dep"/path shortcuts */
    project_expand_t pe = {0};
    (void)resolve_project_store(srv, raw_project, &pe);
    char *project = pe.value ? pe.value : raw_project;
    cbm_store_t *resolved_store = resolve_store(srv, project);
    cbm_store_t *owned_writable_store = NULL;
    cbm_store_t *store =
        cbm_mcp_writable_existing_store(resolved_store, &owned_writable_store);
    if (!store) {
        yyjson_doc_free(doc_args);
        free(project);
        free(pkg_mgr_str);
        return cbm_mcp_text_result(
            resolved_store
                ? "{\"error\":\"project store could not be opened read-write\"}"
                : "{\"error\":\"no project loaded\","
                  "\"hint\":\"Run index_repository with repo_path first.\"}",
            true);
    }

    cbm_pkg_manager_t mgr = has_mgr ? cbm_parse_pkg_manager(pkg_mgr_str) : CBM_PKG_CUSTOM;

    /* Get project root for package_manager resolution */
    char *root_path = NULL;
    if (has_mgr) {
        cbm_project_t proj_info;
        if (cbm_store_get_project(store, project, &proj_info) == 0) {
            root_path = heap_strdup(proj_info.root_path);
        }
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_str(doc, root, "status", "ok");
    yyjson_mut_val *pkg_results = yyjson_mut_arr(doc);

    size_t pkg_count = yyjson_arr_size(packages_val);
    for (size_t i = 0; i < pkg_count; i++) {
        yyjson_val *pkg_val = yyjson_arr_get(packages_val, i);
        const char *pkg_name = yyjson_get_str(pkg_val);
        if (!pkg_name) continue;

        yyjson_mut_val *pr = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_str(doc, pr, "name", pkg_name);

        /* Resolve source directory */
        const char *source_dir = NULL;
        char *resolved_path = NULL;

        if (has_paths && i < yyjson_arr_size(source_paths_val)) {
            yyjson_val *sp = yyjson_arr_get(source_paths_val, i);
            source_dir = yyjson_get_str(sp);
        } else if (has_mgr && root_path) {
            cbm_dep_resolved_t resolved = {0};
            if (cbm_resolve_pkg_source(mgr, pkg_name, root_path, &resolved) == 0) {
                resolved_path = heap_strdup(resolved.path);
                source_dir = resolved_path;
                if (resolved.version)
                    yyjson_mut_obj_add_str(doc, pr, "version", resolved.version);
                cbm_dep_resolved_free(&resolved);
            }
        }

        if (!source_dir || !cbm_is_dir(source_dir)) {
            yyjson_mut_obj_add_str(doc, pr, "status", "not_found");
            yyjson_mut_obj_add_str(doc, pr, "hint",
                "Use source_paths[] with the directory containing dep source.");
            yyjson_mut_arr_append(pkg_results, pr);
            free(resolved_path);
            continue;
        }

        /* Run pipeline: flush dep into project db */
        char *dep_proj = cbm_dep_project_name(project, pkg_name);
        cbm_pipeline_t *dp = cbm_pipeline_new(source_dir, NULL, CBM_MODE_DEP);
        if (dp) {
            cbm_pipeline_apply_config(dp, srv->config);
            cbm_pipeline_set_project_name(dp, dep_proj);
            cbm_pipeline_set_flush_store(dp, store);
            int rc = cbm_pipeline_run(dp);
            cbm_pipeline_free(dp);

            if (rc == 0) {
                int nodes = cbm_store_count_nodes(store, dep_proj);
                int edges = cbm_store_count_edges(store, dep_proj);
                yyjson_mut_obj_add_str(doc, pr, "status", "indexed");
                yyjson_mut_obj_add_int(doc, pr, "nodes", nodes);
                yyjson_mut_obj_add_int(doc, pr, "edges", edges);
            } else {
                yyjson_mut_obj_add_str(doc, pr, "status", "index_failed");
            }
        } else {
            yyjson_mut_obj_add_str(doc, pr, "status", "pipeline_failed");
            yyjson_mut_obj_add_str(doc, pr, "hint", "Out of memory or invalid source path.");
        }
        free(dep_proj);
        free(resolved_path);
        yyjson_mut_arr_append(pkg_results, pr);
    }

    yyjson_mut_obj_add_val(doc, root, "packages", pkg_results);
    if (srv->session_project[0])
        yyjson_mut_obj_add_str(doc, root, "session_project", srv->session_project);

    /* Recompute rank views after adding dependency nodes unless the coupled
     * PageRank/LinkRank/node-degree capability is disabled. */
    (void)cbm_pagerank_compute_with_config(store, project, srv->config);
    /* Dependency sub-projects and cross-boundary edges changed the queryable
     * vocabulary (project.dep.* labels/types/patterns). */
    cbm_mcp_server_notify_index_published(srv);

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    yyjson_doc_free(doc_args);
    free(project);
    free(pkg_mgr_str);
    free(root_path);
    if (owned_writable_store) {
        cbm_store_close(owned_writable_store);
    }

    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

/* ── Tool dispatch ────────────────────────────────────────────── */

static char *build_hidden_tools_payload(cbm_mcp_server_t *srv) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    if (!doc) {
        return heap_strdup("{\"error\":\"failed to build hidden tools payload\"}");
    }

    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_val *advanced = yyjson_mut_arr(doc);
    yyjson_mut_val *hidden = yyjson_mut_arr(doc);
    yyjson_mut_val *visible = yyjson_mut_arr(doc);

    for (int i = 0; i < TOOL_COUNT; i++) {
        const char *name = TOOLS[i].name;
        if (is_streamlined_default_tool(name)) {
            continue;
        }

        yyjson_mut_arr_add_str(doc, advanced, name);
        if (cbm_mcp_advanced_tool_visible(srv, name)) {
            yyjson_mut_arr_add_str(doc, visible, name);
        } else {
            yyjson_mut_arr_add_str(doc, hidden, name);
        }
    }

    yyjson_mut_obj_add_val(doc, root, "advanced_tools", advanced);
    yyjson_mut_obj_add_val(doc, root, "hidden_tools", hidden);
    yyjson_mut_obj_add_val(doc, root, "already_visible_tools", visible);
    yyjson_mut_obj_add_bool(doc, root, "revealed", true);
    yyjson_mut_obj_add_str(doc, root, "next_step",
                           "call tools/list again; hidden tools are now advertised for this MCP server process");
    yyjson_mut_obj_add_str(doc, root, "enable_all",
                           "set env CBM_TOOL_MODE=classic or config set tool_mode classic");
    yyjson_mut_obj_add_str(doc, root, "enable_one",
                           "config set tool_<name> true (e.g. tool_index_repository true)");

    yyjson_mut_val *resources = yyjson_mut_arr(doc);
    yyjson_mut_arr_add_str(doc, resources, "codebase://schema");
    yyjson_mut_arr_add_str(doc, resources, "codebase://architecture");
    yyjson_mut_arr_add_str(doc, resources, "codebase://status");
    yyjson_mut_obj_add_val(doc, root, "resources", resources);

    char *out = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return out ? out : heap_strdup("{\"error\":\"failed to serialize hidden tools payload\"}");
}

/* Compatibility spellings accepted by older raw-JSON callers but deliberately
 * omitted from tools/list and CLI help. Keep this list narrow and semantic:
 * each entry must map unambiguously to a current canonical input.
 *
 * History: 3d133a3c added search_in to the former combined search tool;
 * 39539eff split graph and source search into search_graph and search_code.
 * Existing callers/tests still pass search_in="source", which is an exact
 * no-op spelling of search_code's contract. 32a3820c accepted `cypher` before
 * the query_graph schema standardized on `query`; those strings are exact
 * aliases. Other values/keys are ambiguous and must fail with the canonical
 * spelling or tool. Remove an exception only in an announced compatibility
 * break after usage evidence shows it is unused. */
static bool mcp_legacy_argument_is_valid(const char *tool_name, yyjson_val *properties,
                                         yyjson_val *args, const char *key,
                                         bool *out_invalid_search_in) {
    if (out_invalid_search_in) {
        *out_invalid_search_in = false;
    }
    if (!tool_name || !properties || !args || !key) {
        return false;
    }
    yyjson_val *value = yyjson_obj_get(args, key);
    if (yyjson_obj_get(properties, "project") && yyjson_is_str(value) &&
        (strcmp(key, "project_name") == 0 || strcmp(key, "project_id") == 0 ||
         strcmp(key, "projectName") == 0)) {
        return true;
    }
    if (strcmp(tool_name, "search_code") == 0 && strcmp(key, "search_in") == 0) {
        bool is_source = yyjson_is_str(value) && strcmp(yyjson_get_str(value), "source") == 0;
        if (!is_source && out_invalid_search_in) {
            *out_invalid_search_in = true;
        }
        return is_source;
    }
    if (strcmp(tool_name, "query_graph") == 0 && strcmp(key, "cypher") == 0) {
        return yyjson_is_str(value);
    }
    return false;
}

static char *mcp_validate_tool_argument_keys(const char *tool_name, const char *args_json) {
    const char *schema_json = cbm_mcp_tool_input_schema(tool_name);
    if (!schema_json) {
        return NULL; /* unknown-tool handling remains a protocol-level authority */
    }

    const char *effective_args = args_json ? args_json : "{}";
    yyjson_doc *args_doc = yyjson_read(effective_args, strlen(effective_args), 0);
    yyjson_val *args = args_doc ? yyjson_doc_get_root(args_doc) : NULL;
    if (!args || !yyjson_is_obj(args)) {
        yyjson_doc_free(args_doc);
        return cbm_mcp_text_result(
            "tool arguments must be a JSON object; use tools/list or 'cli <tool> --help' "
            "for the supported arguments",
            true);
    }

    yyjson_doc *schema_doc = yyjson_read(schema_json, strlen(schema_json), 0);
    yyjson_val *schema = schema_doc ? yyjson_doc_get_root(schema_doc) : NULL;
    yyjson_val *properties = schema ? yyjson_obj_get(schema, "properties") : NULL;
    if (!properties || !yyjson_is_obj(properties)) {
        yyjson_doc_free(schema_doc);
        yyjson_doc_free(args_doc);
        return cbm_mcp_text_result("tool input schema is unavailable; retry after reinstalling",
                                   true);
    }

    const char *unknown = NULL;
    bool invalid_search_in = false;
    yyjson_obj_iter arg_iter;
    yyjson_obj_iter_init(args, &arg_iter);
    yyjson_val *arg_key;
    while ((arg_key = yyjson_obj_iter_next(&arg_iter)) != NULL) {
        const char *key = yyjson_get_str(arg_key);
        bool accepted = key && yyjson_obj_get(properties, key);
        if (!accepted) {
            accepted =
                mcp_legacy_argument_is_valid(tool_name, properties, args, key, &invalid_search_in);
        }
        if (!accepted) {
            unknown = key ? key : "<invalid>";
            break;
        }
    }

    char *error_result = NULL;
    if (invalid_search_in) {
        error_result = cbm_mcp_text_result(
            "search_code supports source text search directly: omit search_in or use "
            "search_in='source' for legacy callers; use search_graph for graph search",
            true);
    } else if (unknown) {
        char supported[CBM_SZ_1K] = "";
        size_t used = 0;
        yyjson_obj_iter prop_iter;
        yyjson_obj_iter_init(properties, &prop_iter);
        yyjson_val *prop_key;
        while ((prop_key = yyjson_obj_iter_next(&prop_iter)) != NULL) {
            const char *name = yyjson_get_str(prop_key);
            if (!name) {
                continue;
            }
            int written = snprintf(supported + used, sizeof(supported) - used, "%s%s",
                                   used ? ", " : "", name);
            if (written < 0 || (size_t)written >= sizeof(supported) - used) {
                break;
            }
            used += (size_t)written;
        }
        char message[CBM_SZ_2K];
        snprintf(message, sizeof(message),
                 "unknown argument '%.*s' for tool '%.*s'; supported arguments: %s. "
                 "Use tools/list or 'cli %.*s --help' for types and descriptions.",
                 CBM_SZ_256, unknown, CBM_SZ_256, tool_name, supported[0] ? supported : "(none)",
                 CBM_SZ_256, tool_name);
        error_result = cbm_mcp_text_result(message, true);
    }

    yyjson_doc_free(schema_doc);
    yyjson_doc_free(args_doc);
    return error_result;
}

char *cbm_mcp_handle_tool(cbm_mcp_server_t *srv, const char *tool_name, const char *args_json) {
    if (!tool_name) {
        return cbm_mcp_text_result(
            "{\"error\":\"missing tool name\","
            "\"hint\":\"Available tools: search_graph, query_graph, search_code, "
            "trace_path, get_code. "
            "Use tools/list to see all available tools.\"}", true);
    }
    if (srv && !mcp_tool_allowed(srv->tool_profile, tool_name)) {
        char message[CBM_SZ_256];
        snprintf(message, sizeof(message), "tool '%s' is not available in the %s tool profile",
                 tool_name, mcp_tool_profile_name(srv->tool_profile));
        return cbm_mcp_text_result(message, true);
    }
    char *argument_error = mcp_validate_tool_argument_keys(tool_name, args_json);
    if (argument_error) {
        return argument_error;
    }

    /* Streamlined alias: get_code → get_code_snippet handler.
     * (The 3 search tools dispatch by their real names below.) */
    if (strcmp(tool_name, "get_code") == 0) {
        return handle_get_code_snippet(srv, args_json);
    }

    /* Original tool names (classic mode or individually enabled) */
    if (strcmp(tool_name, "list_projects") == 0) {
        return handle_list_projects(srv, args_json);
    }
    if (strcmp(tool_name, "get_graph_schema") == 0) {
        return handle_get_graph_schema(srv, args_json);
    }
    if (strcmp(tool_name, "search_graph") == 0) {
        return handle_search_graph(srv, args_json);
    }
    if (strcmp(tool_name, "query_graph") == 0) {
        return handle_query_graph(srv, args_json);
    }
    if (strcmp(tool_name, "index_status") == 0) {
        return handle_index_status(srv, args_json);
    }
    if (strcmp(tool_name, "check_index_coverage") == 0) {
        return handle_check_index_coverage(srv, args_json);
    }
    if (strcmp(tool_name, "delete_project") == 0) {
        return handle_delete_project(srv, args_json);
    }
    if (strcmp(tool_name, "trace_path") == 0 || strcmp(tool_name, "trace_call_path") == 0) {
        return handle_trace_path(srv, args_json);
    }
    if (strcmp(tool_name, "get_architecture") == 0) {
        return handle_get_architecture(srv, args_json);
    }

    /* Pipeline-dependent tools */
    if (strcmp(tool_name, "index_repository") == 0) {
        return handle_index_repository(srv, args_json);
    }
    if (strcmp(tool_name, "get_code_snippet") == 0) {
        return handle_get_code_snippet(srv, args_json);
    }
    if (strcmp(tool_name, "search_code") == 0) {
        return handle_search_code(srv, args_json);
    }
    if (strcmp(tool_name, "detect_changes") == 0) {
        return handle_detect_changes(srv, args_json);
    }
    if (strcmp(tool_name, "manage_adr") == 0) {
        return handle_manage_adr(srv, args_json);
    }
    if (strcmp(tool_name, "ingest_traces") == 0) {
        return handle_ingest_traces(srv, args_json);
    }
    if (strcmp(tool_name, "index_dependencies") == 0) {
        return handle_index_dependencies(srv, args_json);
    }

    /* _hidden_tools: informational pseudo-tool for progressive disclosure */
    if (strcmp(tool_name, "_hidden_tools") == 0) {
        bool changed = srv && !srv->hidden_tools_revealed;
        char *payload = build_hidden_tools_payload(srv);
        if (srv) {
            srv->hidden_tools_revealed = true;
        }
        if (changed) {
            send_notification(srv, "notifications/tools/list_changed");
        }
        char *result = cbm_mcp_text_result(payload, false);
        free(payload);
        return result;
    }

    char msg[512];
    snprintf(msg, sizeof(msg),
        "{\"error\":\"unknown tool: '%s'\","
        "\"hint\":\"Available tools: search_graph, query_graph, search_code, "
        "trace_path, get_code. "
        "Use tools/list to see all available tools.\"}", tool_name);
    return cbm_mcp_text_result(msg, true);
}

/* ── Session detection + auto-index ────────────────────────────── */

/* Detect session root from CWD (fallback: single indexed project from DB). */
static void detect_session(cbm_mcp_server_t *srv) {
    if (srv->session_detected) {
        return;
    }
    srv->session_detected = true;

    /* 1. Try CWD */
    char cwd[CBM_SZ_1K];
    if (getcwd(cwd, sizeof(cwd)) != NULL) {
        const char *home = cbm_get_home_dir();
        /* Skip useless roots: / and $HOME */
        if (strcmp(cwd, "/") != 0 && (home == NULL || strcmp(cwd, home) != 0)) {
            snprintf(srv->session_root, sizeof(srv->session_root), "%s", cwd);
            cbm_log_info("session.root.cwd", "path", cwd);
        }
    }

    /* Derive project name from path — MUST match cbm_project_name_from_path()
     * (fqn.c:168) which the pipeline uses for db file naming and node project column.
     * Previous code used "last 2 segments" convention which produced different names,
     * breaking expand_project_param() and maybe_auto_index db file checks. */
    if (srv->session_root[0]) {
        char *name = cbm_project_name_from_path(srv->session_root);
        snprintf(srv->session_project, sizeof(srv->session_project), "%s", name);
        free(name);
    }

    /* Validate derived project name — don't create dbs for empty/dot names */
    if (srv->session_project[0] == '\0' ||
        strcmp(srv->session_project, ".") == 0 ||
        strcmp(srv->session_project, "..") == 0) {
        cbm_log_warn("session.invalid_name", "derived", srv->session_project,
                     "cwd", srv->session_root,
                     "hint", "Cannot derive valid project name from CWD");
        srv->session_project[0] = '\0';
        srv->session_root[0] = '\0';
    }
}

void cbm_mcp_server_detect_session(cbm_mcp_server_t *srv) {
    if (!srv) {
        return;
    }
    detect_session(srv);
}

/* auto_watch config: gates background watcher registration (default on).
 * Multi-project users can contain a session to its own project with
 * `config set auto_watch false`. */
static bool auto_watch_enabled(cbm_mcp_server_t *srv) {
    if (!srv->config) {
        return true; /* default on */
    }
    return cbm_config_get_bool(srv->config, CBM_CONFIG_AUTO_WATCH, true);
}

/* Register the session project with the background watcher for ongoing
 * change detection — unless auto_watch is disabled. */
static void register_watcher_if_enabled(cbm_mcp_server_t *srv) {
    if (!srv->watcher || srv->session_project[0] == '\0' || srv->session_root[0] == '\0') {
        return;
    }
    if (!auto_watch_enabled(srv)) {
        cbm_log_info("watcher.register.skipped", "reason", "auto_watch_off", "project",
                     srv->session_project);
        return;
    }
    cbm_watcher_watch(srv->watcher, srv->session_project, srv->session_root);
}

/* Background auto-index thread function */
static void autoindex_thread_release_caches(void) {
    /* Sequential extraction builds a thread-local syntax-kind bitset cache on
     * the calling thread. Worker-pool threads release the same cache at their
     * exit boundary; the auto-index owner must do likewise on every return. */
    cbm_kind_in_set_free_cache();
    cbm_mem_collect();
}

static void *autoindex_thread(void *arg) {
    cbm_mcp_server_t *srv = (cbm_mcp_server_t *)arg;

    cbm_log_info("autoindex.start", "project", srv->session_project, "path", srv->session_root);

    /* #832: prefer the supervised worker subprocess. Indexing the whole session in
     * this long-lived server thread ratchets RSS (mimalloc v3 does not reclaim the
     * pages worker threads abandon at exit); running it in a child that exits hands
     * 100% of that memory back to the OS every cycle. Degrade to the in-process
     * pipeline below when the supervisor is off (kill switch) or the spawn fails. */
    if (cbm_index_supervisor_should_wrap()) {
        char *resp = index_run_supervised_path(srv, srv->session_root);
        if (resp) {
            bool published = cbm_mcp_index_response_published(resp);
            free(resp);
            if (atomic_load(&srv->stop_requested)) {
                cbm_log_info("autoindex.cancelled", "project", srv->session_project);
                autoindex_thread_release_caches();
                return NULL;
            }
            srv->autoindex_failed = !published;
            srv->just_autoindexed = published;
            if (!published) {
                cbm_log_warn("autoindex.err", "msg", "supervised_index_failed");
                autoindex_thread_release_caches();
                return NULL;
            }
            cbm_log_info("autoindex.done", "project", srv->session_project, "mode", "supervised");
            /* Register with watcher for ongoing change detection — gated on
             * auto_watch (#849), same as the in-process branch below. A bare
             * `if (srv->watcher)` would register even when the user set
             * `config set auto_watch false`, since srv->watcher is always set. */
            register_watcher_if_enabled(srv);
            autoindex_thread_release_caches();
            return NULL;
        }
        /* resp == NULL → spawn-failure degrade → fall through to in-process. */
    }

    if (atomic_load(&srv->stop_requested)) {
        cbm_log_info("autoindex.cancelled", "project", srv->session_project);
        autoindex_thread_release_caches();
        return NULL;
    }

    cbm_pipeline_t *p = cbm_pipeline_new(srv->session_root, NULL, CBM_MODE_FULL);
    if (!p) {
        srv->autoindex_failed = true;
        srv->just_autoindexed = false;
        cbm_log_warn("autoindex.err", "msg", "pipeline_create_failed");
        autoindex_thread_release_caches();
        return NULL;
    }
    cbm_pipeline_apply_config(p, srv->config);

    /* Block until any concurrent pipeline finishes */
    cbm_pipeline_lock();
    int rc = cbm_pipeline_run(p);
    bool graph_changed = cbm_pipeline_graph_changed(p);
    cbm_pipeline_publish_kind_t publish_kind = cbm_pipeline_publish_kind(p);
    bool incremental_fallback = cbm_pipeline_incremental_fallback(p);
    cbm_pipeline_unlock();

    cbm_pipeline_free(p);

    srv->autoindex_failed = (rc != 0);
    srv->just_autoindexed = (rc == 0);

    if (rc == 0) {
        /* Re-index dependencies after fresh dump.
         *
         * Open an INDEPENDENT writable handle (overlay_compaction_thread
         * pattern) instead of calling resolve_store(): this still runs on the
         * background auto-index thread while the request thread may be
         * mid-query on srv->store, and resolve_store() mutates srv->store /
         * srv->current_project / srv->owns_store without synchronization —
         * including cbm_store_close(srv->store) and free(srv->current_project),
         * a use-after-free under the store.h one-handle-per-thread contract.
         * The request thread's join in REQUIRE_STORE_EX only covers the
         * store==NULL path, so it does not serialize this case. */
        cbm_store_t *store = NULL;
        char autoindex_db_path[CBM_SZ_1K];
        autoindex_db_path[0] = '\0';
        project_db_path(srv->session_project, autoindex_db_path, sizeof(autoindex_db_path));
        if (autoindex_db_path[0]) {
            store = cbm_store_open_path_existing(autoindex_db_path);
        }
        if (store) {
            int effective_dep_limit = cbm_mcp_effective_auto_dep_limit(srv, NULL);
            int deps_reindexed = cbm_mcp_auto_index_deps(
                srv, srv->session_project, srv->session_root, store,
                effective_dep_limit, NULL);
            (void)cbm_pagerank_refresh_after_publish(
                store, srv->session_project, srv->config, graph_changed, deps_reindexed,
                cbm_rank_refresh_publish_from_pipeline(publish_kind, incremental_fallback));
            cbm_store_close(store);
        }
        cbm_mcp_server_notify_index_published(srv);

        cbm_log_info("autoindex.done", "project", srv->session_project);
        register_watcher_if_enabled(srv);
        if (srv->watcher && auto_watch_enabled(srv)) {
            cbm_watcher_mark_indexed(srv->watcher, srv->session_project, srv->session_root);
        }
    } else {
        cbm_log_warn("autoindex.err", "msg", "pipeline_run_failed");
    }
    autoindex_thread_release_caches();
    return NULL;
}

/* Check if a DB file has actual content (at least 1 node).
 * Returns true if DB exists AND has nodes. Lightweight raw SQLite check. */
static bool db_has_content(const char *db_path) {
    int64_t file_size = cbm_file_size(db_path);
    if (file_size < 0) return false; /* file doesn't exist */
    if (file_size == 0) return false;

    sqlite3 *db = NULL;
    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return false;
    }
    sqlite3_stmt *stmt = NULL;
    bool has = false;
    if (sqlite3_prepare_v2(db, "SELECT 1 FROM nodes LIMIT 1", -1, &stmt, NULL) == SQLITE_OK) {
        has = (sqlite3_step(stmt) == SQLITE_ROW);
        sqlite3_finalize(stmt);
    }
    sqlite3_close(db);
    return has;
}

/* Check if a DB's index is stale by comparing DB file mtime against latest
 * git commit time. If the repo has commits newer than the DB, it's stale.
 * Also stale if DB is older than max_age_seconds (0 = disabled).
 * Returns false on any error (conservative: don't trigger unnecessary reindex). */
static bool db_is_stale(const char *db_path, const char *repo_path, int max_age_seconds) {
    struct stat db_st;
    if (stat(db_path, &db_st) != 0) return false;
    time_t db_mtime = db_st.st_mtime;

    /* Check age-based staleness (configurable, 0 = disabled).
     * Guard against clock skew: only consider stale if now > db_mtime. */
    if (max_age_seconds > 0) {
        time_t now = time(NULL);
        if (now > db_mtime && (now - db_mtime) > max_age_seconds) return true;
    }

    /* Check git HEAD commit time vs DB mtime */
    if (!validate_search_path_arg(repo_path)) return false;
    char cmd[1024];
#ifdef _WIN32
    const char *null_dev = "NUL";
#else
    const char *null_dev = "/dev/null";
#endif
    int cmd_len = snprintf(cmd, sizeof(cmd), "git -C \"%s\" log -1 --format=%%ct HEAD 2>%s",
                           repo_path, null_dev);
    if (cmd_len < 0 || (size_t)cmd_len >= sizeof(cmd)) return false;
    // NOLINTNEXTLINE(bugprone-command-processor,cert-env33-c)
    FILE *fp = cbm_popen(cmd, "r");
    if (!fp) return false;
    char line[64] = {0};
    if (fgets(line, sizeof(line), fp)) {
        long commit_time = strtol(line, NULL, 10);
        cbm_pclose(fp);
        /* Stale if latest commit is newer than DB */
        return commit_time > (long)db_mtime;
    }
    cbm_pclose(fp);
    return false;
}

/* Config keys for reindex behavior */
#define CBM_CONFIG_REINDEX_ON_STARTUP "reindex_on_startup"
#define CBM_CONFIG_REINDEX_STALE_SECONDS "reindex_stale_seconds"

static bool cbm_mcp_reindex_on_startup(cbm_mcp_server_t *srv) {
    return cbm_config_get_effective_bool(srv ? srv->config : NULL,
                                         CBM_CONFIG_REINDEX_ON_STARTUP, false);
}

static int cbm_mcp_reindex_stale_seconds(cbm_mcp_server_t *srv) {
    return cbm_config_get_effective_int(srv ? srv->config : NULL,
                                        CBM_CONFIG_REINDEX_STALE_SECONDS, 0);
}

/* Start auto-indexing if configured and project not yet indexed. */
static void maybe_auto_index(cbm_mcp_server_t *srv) {
    srv->autoindex_block = MCP_AUTOINDEX_BLOCK_NONE;
    srv->autoindex_observed_files = 0;
    srv->autoindex_file_limit = cbm_mcp_auto_index_limit(srv);
    if (srv->session_root[0] == '\0') {
        return; /* no session root detected */
    }

    /* Check if project already has a populated DB */
    bool needs_index = true;
    char db_check[1024] = {0};
    project_db_path(srv->session_project, db_check, sizeof(db_check));
    if (db_check[0]) {
        if (db_has_content(db_check)) {
            /* DB exists and has nodes — check if stale */
            bool reindex_on_startup = cbm_mcp_reindex_on_startup(srv);
            int stale_seconds = cbm_mcp_reindex_stale_seconds(srv);
            bool stale = db_is_stale(db_check, srv->session_root, stale_seconds);

            if (stale && reindex_on_startup) {
                cbm_log_info("autoindex.stale", "reason", "commits_newer_than_index", "project",
                             srv->session_project);
                needs_index = true;
            } else {
                if (stale) {
                    cbm_log_info("autoindex.stale_skipped", "reason", "reindex_on_startup=false",
                                 "hint", "set reindex_on_startup true to auto-update on restart",
                                 "project", srv->session_project);
                } else {
                    cbm_log_info("autoindex.skip", "reason", "already_indexed", "project",
                                 srv->session_project);
                }
                /* Respect auto_watch when registering an existing index (#849). */
                register_watcher_if_enabled(srv);
                needs_index = false;
            }
        } else {
            if (cbm_file_exists(db_check)) {
                /* DB file exists but has 0 nodes — treat as not indexed */
                cbm_log_info("autoindex.empty_db", "reason", "db_exists_but_empty", "project",
                             srv->session_project);
            }
            needs_index = true;
        }
    }

    if (!needs_index) return;

    /* Check auto_index: env var CBM_AUTO_INDEX > config DB > no-config manual.
     * Shared with synchronous first-use indexing so auto_index=false cannot
     * be bypassed by a later search/trace request. */
    bool auto_index = cbm_mcp_auto_index_enabled(srv);

    if (!auto_index) {
        srv->autoindex_block = MCP_AUTOINDEX_BLOCK_DISABLED;
        cbm_log_info("autoindex.skip", "reason", "disabled", "hint",
                     "export CBM_AUTO_INDEX=true  OR  codebase-memory-mcp config set auto_index true");
        return;
    }

    /* Quick file count check to avoid OOM on massive repos. A configured limit
     * of 0 means "no limit", matching the public config registry. */
    if (!cbm_mcp_auto_index_within_limit(srv, srv->session_root)) {
        return;
    }

    /* Launch auto-index in background */
    if (cbm_thread_create(&srv->autoindex_tid, 0, autoindex_thread, srv) == 0) {
        srv->autoindex_active = true;
    } else {
        /* Do not turn a transient thread-launch failure into a user task. The
         * first store-backed request runs the existing synchronous first-use
         * path before REQUIRE_STORE_EX can build an error. */
        cbm_log_warn("autoindex.skip", "reason", "thread_start_failed");
    }
}

/* ── Background update check ──────────────────────────────────── */

#define UPDATE_CHECK_URL "https://api.github.com/repos/DeusData/codebase-memory-mcp/releases/latest"

static void *update_check_thread(void *arg) {
    cbm_mcp_server_t *srv = (cbm_mcp_server_t *)arg;

    int timeout_s = cbm_mcp_update_check_timeout_s(srv);
    if (timeout_s <= 0) {
        return NULL;
    }

    char cmd[CBM_SZ_512];
    int cmd_len = snprintf(cmd, sizeof(cmd),
                           "curl -sf --max-time %d -H 'Accept: application/vnd.github+json' "
                           "'" UPDATE_CHECK_URL "' 2>/dev/null",
                           timeout_s);
    if (cmd_len < 0 || (size_t)cmd_len >= sizeof(cmd)) {
        return NULL;
    }

    FILE *fp = cbm_popen(cmd, "r");
    if (!fp) {
        return NULL;
    }

    char buf[CBM_SZ_4K];
    size_t total = 0;
    while (total < sizeof(buf) - SKIP_ONE) {
        size_t n = fread(buf + total, SKIP_ONE, sizeof(buf) - SKIP_ONE - total, fp);
        if (n == 0) {
            break;
        }
        total += n;
    }
    buf[total] = '\0';
    cbm_pclose(fp);

    /* Parse tag_name from JSON response */
    yyjson_doc *doc = yyjson_read(buf, total, 0);
    if (!doc) {
        return NULL;
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *tag = yyjson_obj_get(root, "tag_name");
    const char *tag_str = yyjson_get_str(tag);

    if (tag_str) {
        const char *current = cbm_cli_get_version();
        if (cbm_compare_versions(tag_str, current) > 0) {
            cbm_mutex_lock(&srv->update_notice_lock);
            snprintf(srv->update_notice, sizeof(srv->update_notice),
                     "Update available: %s -> %s -- run: codebase-memory-mcp update  |  "
                     "Enjoying codebase-memory-mcp? Please leave a star: "
                     "https://github.com/DeusData/codebase-memory-mcp",
                     current, tag_str);
            cbm_mutex_unlock(&srv->update_notice_lock);
            cbm_log_info("update.available", "current", current, "latest", tag_str);
        }
    }

    yyjson_doc_free(doc);
    return NULL;
}

static void start_update_check(cbm_mcp_server_t *srv) {
    if (srv->update_checked) {
        return;
    }
    srv->update_checked = true; /* prevent double-launch */
    if (cbm_thread_create(&srv->update_tid, 0, update_check_thread, srv) == 0) {
        srv->update_thread_active = true;
    }
}

/* Prepend update notice to a tool result, then clear it (one-shot). */
static char *inject_update_notice(cbm_mcp_server_t *srv, char *result_json) {
    if (!srv || !result_json) {
        return result_json;
    }

    char notice[sizeof(srv->update_notice)];
    cbm_mutex_lock(&srv->update_notice_lock);
    snprintf(notice, sizeof(notice), "%s", srv->update_notice);
    cbm_mutex_unlock(&srv->update_notice_lock);

    if (notice[0] == '\0') {
        return result_json;
    }

    /* Parse existing result, prepend notice text, rebuild */
    yyjson_doc *doc = yyjson_read(result_json, strlen(result_json), 0);
    if (!doc) {
        return result_json;
    }

    yyjson_mut_doc *mdoc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_val_mut_copy(mdoc, yyjson_doc_get_root(doc));
    yyjson_doc_free(doc);
    if (!root) {
        yyjson_mut_doc_free(mdoc);
        return result_json;
    }
    yyjson_mut_doc_set_root(mdoc, root);

    /* Find the "content" array */
    yyjson_mut_val *content = yyjson_mut_obj_get(root, "content");
    if (content && yyjson_mut_is_arr(content)) {
        /* Prepend a text content item with the update notice */
        yyjson_mut_val *notice_item = yyjson_mut_obj(mdoc);
        yyjson_mut_obj_add_str(mdoc, notice_item, "type", "text");
        yyjson_mut_obj_add_str(mdoc, notice_item, "text", notice);
        yyjson_mut_arr_prepend(content, notice_item);
    }

    size_t len;
    char *new_json = yyjson_mut_write(mdoc, YYJSON_WRITE_ALLOW_INVALID_UNICODE, &len);
    yyjson_mut_doc_free(mdoc);

    if (new_json) {
        free(result_json);
        cbm_mutex_lock(&srv->update_notice_lock);
        srv->update_notice[0] = '\0'; /* clear — one-shot after successful injection */
        cbm_mutex_unlock(&srv->update_notice_lock);
        return new_json;
    }
    return result_json;
}

/* ── MCP Resources (Phase 10) ─────────────────────────────────── */

static void write_protocol_json(FILE *out, const char *json, bool content_length_framed) {
    if (!out || !json) return;
    CBM_PROF_START(prof_mcp_write);
    if (content_length_framed) {
        (void)fprintf(out, MCP_CONTENT_HEADER " %zu\r\n\r\n%s", strlen(json), json);
    } else {
        (void)fprintf(out, "%s\n", json);
    }
    (void)fflush(out);
    CBM_PROF_END("mcp_write", content_length_framed ? "content_length" : "json_line",
                 prof_mcp_write);
}

/* Send a JSON-RPC notification (no id) to the client's protocol stream.
 * Must match the active transport framing: raw JSON lines for line mode, or
 * Content-Length frames after the client uses Content-Length framing. */
static void send_notification(cbm_mcp_server_t *srv, const char *method) {
    if (!srv || !srv->out_stream) return;
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_str(doc, root, "jsonrpc", "2.0");
    yyjson_mut_obj_add_str(doc, root, "method", method);
    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    if (json) {
        write_protocol_json(srv->out_stream, json, srv->out_content_length_framed);
        free(json);
    }
}

/* Handle resources/list — return 3 resource URIs. */
static char *handle_resources_list(cbm_mcp_server_t *srv) {
    (void)srv;
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_val *arr = yyjson_mut_arr(doc);

    /* Resource 1: schema */
    yyjson_mut_val *r1 = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, r1, "uri", "codebase://schema");
    yyjson_mut_obj_add_str(doc, r1, "name", "Code Graph Schema");
    yyjson_mut_obj_add_str(doc, r1, "description",
        "Node labels (Function, Class, Module, etc.) and edge types (CALLS, IMPORTS, "
        "DEFINES_METHOD, etc.) with counts. Read this before writing Cypher queries "
        "to know valid labels and relationship types.");
    yyjson_mut_obj_add_str(doc, r1, "mimeType", "application/json");
    yyjson_mut_arr_add_val(arr, r1);

    /* Resource 2: architecture */
    yyjson_mut_val *r2 = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, r2, "uri", "codebase://architecture");
    yyjson_mut_obj_add_str(doc, r2, "name", "Architecture Overview");
    yyjson_mut_obj_add_str(doc, r2, "description",
        "Total nodes/edges, top 10 key functions ranked by PageRank (structural "
        "importance), and relationship patterns. Read this first to understand "
        "codebase structure and find important entry points.");
    yyjson_mut_obj_add_str(doc, r2, "mimeType", "application/json");
    yyjson_mut_arr_add_val(arr, r2);

    /* Resource 3: status */
    yyjson_mut_val *r3 = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, r3, "uri", "codebase://status");
    yyjson_mut_obj_add_str(doc, r3, "name", "Index Status");
    yyjson_mut_obj_add_str(doc, r3, "description",
        "Project name, indexing status (ready/empty/not_indexed/indexing), "
        "node/edge counts, PageRank stats, detected ecosystem, dependency list. "
        "Status 'indexing' = in progress, 'not_indexed' includes action_required hint. "
        "Auto-index failure reports detail and fix suggestion.");
    yyjson_mut_obj_add_str(doc, r3, "mimeType", "application/json");
    yyjson_mut_arr_add_val(arr, r3);

    yyjson_mut_obj_add_val(doc, root, "resources", arr);
    char *out = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return out;
}

/* Get the active project name: current_project (from last tool call) or session_project. */
static const char *active_project_name(cbm_mcp_server_t *srv) {
    if (srv->current_project) return srv->current_project;
    return srv->session_project[0] ? srv->session_project : NULL;
}

/* Resolve store for resource handlers. Prefers the currently-open project
 * (set by the most recent tool call) over the session project, so resources
 * reflect data the user is actually querying — not the empty CWD project. */
static cbm_store_t *resolve_resource_store(cbm_mcp_server_t *srv) {
    /* Consume a deferred invalidation first — same contract as resolve_store. */
    reap_stale_store(srv);
    /* 1. Use currently-open project (set by last resolve_store call) */
    if (srv->current_project && srv->store)
        return srv->store;
    /* 2. Fall back to session project */
    const char *proj = srv->session_project[0] ? srv->session_project : NULL;
    if (proj) return resolve_store(srv, proj);
    return srv->store;
}

/* Build schema resource content (reuses inject_context_once logic). */
static void build_resource_schema(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                  cbm_mcp_server_t *srv) {
    cbm_store_t *store = resolve_resource_store(srv);
    const char *proj = active_project_name(srv);

    if (!store) {
        yyjson_mut_obj_add_str(doc, root, "status", "not_indexed");
        return;
    }

    cbm_store_overlay_node_view_summary_t overlay_summary = {0};
    bool used_active_schema = false;
    bool active_schema_failed = false;
    cbm_schema_info_t schema = {0};
    mcp_get_current_schema(store, proj, MCP_CYPHER_MATCH_VOCABULARY, &schema,
                           &overlay_summary, &used_active_schema, &active_schema_failed);

    yyjson_mut_val *label_arr = yyjson_mut_arr(doc);
    for (int i = 0; i < schema.node_label_count; i++) {
        yyjson_mut_val *lbl = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_strcpy(doc, lbl, "label", schema.node_labels[i].label);
        yyjson_mut_obj_add_int(doc, lbl, "count", schema.node_labels[i].count);
        yyjson_mut_arr_add_val(label_arr, lbl);
    }
    yyjson_mut_obj_add_val(doc, root, "node_labels", label_arr);

    yyjson_mut_val *type_arr = yyjson_mut_arr(doc);
    for (int i = 0; i < schema.edge_type_count; i++) {
        yyjson_mut_val *et = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_strcpy(doc, et, "type", schema.edge_types[i].type);
        yyjson_mut_obj_add_int(doc, et, "count", schema.edge_types[i].count);
        yyjson_mut_arr_add_val(type_arr, et);
    }
    yyjson_mut_obj_add_val(doc, root, "edge_types", type_arr);
    cbm_store_schema_free(&schema);

    bool overlay_limitation_reported = false;
    if (used_active_schema) {
        add_overlay_active_schema_freshness(
            doc, root, &overlay_summary, false,
            "codebase://schema used active overlay node and edge rows for node_labels and "
            "edge_types; property-key discovery and relationship patterns are not included in this "
            "resource.");
    } else {
        overlay_limitation_reported = add_canonical_only_overlay_freshness(
            doc, root, store, proj,
            "codebase://schema reads canonical graph schema counts; ready overlay rows are not "
            "included until active schema resource views or compaction are available.");
        if (active_schema_failed && !overlay_limitation_reported) {
            add_response_warning(
                doc, root,
                "codebase://schema could not read the active overlay schema view; canonical "
                "schema counts were returned.");
        }
    }
    int dirty_pending = 0;
    int dirty_overlay_ready = 0;
    if (get_dirty_file_counts(store, proj, &dirty_pending, &dirty_overlay_ready)) {
        add_dirty_file_freshness_counts(doc, root, dirty_pending, dirty_overlay_ready);
        if (used_active_schema && dirty_pending > 0) {
            add_response_warning(doc, root,
                                 "codebase://schema used ready overlay rows, but pending dirty "
                                 "files may still be absent until overlay or reindex completes.");
        } else if (!used_active_schema && !overlay_limitation_reported) {
            add_response_warning(doc, root,
                                 "codebase://schema reads canonical graph schema counts; dirty "
                                 "file changes may be absent until overlay or reindex completes.");
        }
    }
}

/* CBM_CONFIG_KEY_FUNCTIONS_EXCLUDE defined in constants section at top of file */

/* Build a key_functions SQL query with optional exclude patterns.
 * exclude_csv: comma-separated globs from config, or NULL.
 * exclude_arr: NULL-terminated array from tool param, or NULL.
 * Returns a heap-allocated SQL string. Caller must free. */
/* Double single-quotes so a glob→like pattern can be safely interpolated into
 * a SQL string literal. The search path binds LIKE patterns as ?N parameters
 * (safe — sqlite3_bind_text handles quoting), but build_key_functions_sql
 * bakes the NOT LIKE clause into SQL text, so the exclude pattern (which comes
 * from the MCP `exclude` tool arg + the key_functions_exclude config) must be
 * quote-escaped here to prevent breaking out of the '...' literal (SQL
 * injection). Returns a heap string the caller frees; NULL on OOM/NULL input. */
static char *sql_escape_quotes(const char *s) {
    if (!s) {
        return NULL;
    }
    size_t n = 0, quotes = 0;
    for (; s[n]; n++) {
        if (s[n] == '\'') {
            quotes++;
        }
    }
    char *out = malloc(n + quotes + 1);
    if (!out) {
        return NULL;
    }
    size_t j = 0;
    for (size_t i = 0; i < n; i++) {
        if (s[i] == '\'') {
            out[j++] = '\''; /* double the quote */
        }
        out[j++] = s[i];
    }
    out[j] = '\0';
    return out;
}

static char *build_key_functions_sql(const char *exclude_csv, const char **exclude_arr, int limit,
                                     bool path_scoped) {
    char sql[4096];
    int pos = 0;
    pos += snprintf(sql + pos, sizeof(sql) - pos,
        "SELECT n.name, n.qualified_name, n.label, n.file_path, pr.rank "
        "FROM pagerank pr JOIN nodes n ON n.id = pr.node_id "
        "WHERE pr.project = ?1 "
        "AND n.label IN ('Function','Class','Method','Interface') ");
    if (path_scoped) {
        pos += snprintf(sql + pos, sizeof(sql) - pos,
                        "AND (n.file_path = ?2 OR n.file_path LIKE ?3) ");
    }

    /* Apply config-based excludes (comma-separated globs) */
    if (exclude_csv && exclude_csv[0]) {
        char *csv_copy = heap_strdup(exclude_csv);
        char *tok = strtok(csv_copy, ",");
        while (tok && pos < (int)sizeof(sql) - 128) {
            while (*tok == ' ') tok++; /* trim leading space */
            char *like = cbm_glob_to_like(tok);
            if (like) {
                char *safe = sql_escape_quotes(like); /* prevent SQL injection */
                free(like);
                if (safe) {
                    pos += snprintf(sql + pos, sizeof(sql) - pos,
                        "AND n.file_path NOT LIKE '%s' ", safe);
                    free(safe);
                }
            }
            tok = strtok(NULL, ",");
        }
        free(csv_copy);
    }

    /* Apply param-based excludes (array of globs) */
    if (exclude_arr) {
        for (int i = 0; exclude_arr[i] && pos < (int)sizeof(sql) - 128; i++) {
            char *like = cbm_glob_to_like(exclude_arr[i]);
            if (like) {
                char *safe = sql_escape_quotes(like); /* prevent SQL injection */
                free(like);
                if (safe) {
                    pos += snprintf(sql + pos, sizeof(sql) - pos,
                        "AND n.file_path NOT LIKE '%s' ", safe);
                    free(safe);
                }
            }
        }
    }

    snprintf(sql + pos, sizeof(sql) - pos, "ORDER BY pr.rank DESC LIMIT %d",
             limit > 0 ? limit : 25);
    return heap_strdup(sql);
}

/* Build architecture resource content. */
static void build_resource_architecture(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                        cbm_mcp_server_t *srv) {
    cbm_store_t *store = resolve_resource_store(srv);
    const char *proj = active_project_name(srv);

    if (!store) {
        yyjson_mut_obj_add_str(doc, root, "status", "not_indexed");
        return;
    }

    int nodes = cbm_store_count_nodes(store, proj);
    int edges = cbm_store_count_edges(store, proj);
    yyjson_mut_obj_add_int(doc, root, "total_nodes", nodes);
    yyjson_mut_obj_add_int(doc, root, "total_edges", edges);

    const char *resource_aspects[] = {"languages", "entry_points", "routes"};
    cbm_architecture_info_t arch = {0};
    bool arch_loaded = proj && proj[0] &&
                       cbm_store_get_architecture(
                           store, proj, resource_aspects,
                           (int)(sizeof(resource_aspects) / sizeof(resource_aspects[0])), &arch, 0,
                           1.0) == CBM_STORE_OK;
    if (arch_loaded) {
        add_architecture_languages_json(doc, root, &arch);
        add_architecture_entry_points_json(doc, root, &arch);
        add_architecture_routes_json(doc, root, &arch);
    } else if (proj && proj[0]) {
        add_response_warning(doc, root,
                             "codebase://architecture omitted active summary sections because "
                             "architecture summary queries failed.");
    }

    bool active_architecture_reported =
        arch_loaded &&
        add_overlay_active_architecture_freshness(
            doc, root, store, proj, true, true, true, false,
            "codebase://architecture used active overlay node rows for languages, entry_points, "
            "routes, and relationship_patterns; total_nodes, total_edges, and key_functions "
            "remain canonical or stale until active views or compaction are available.");
    bool overlay_limitation_reported =
        !active_architecture_reported &&
        add_canonical_only_overlay_freshness(
            doc, root, store, proj,
            "codebase://architecture reads canonical graph summaries; ready overlay rows are not "
            "included until active architecture resource views or compaction are available.");
    int dirty_pending = 0;
    int dirty_overlay_ready = 0;
    if (get_dirty_file_counts(store, proj, &dirty_pending, &dirty_overlay_ready)) {
        add_dirty_file_freshness_counts(doc, root, dirty_pending, dirty_overlay_ready);
        if (!overlay_limitation_reported) {
            add_response_warning(
                doc, root,
                active_architecture_reported
                    ? "codebase://architecture used active overlay node rows for summary sections; "
                      "dirty file changes outside ready overlays may still be absent from canonical "
                      "summaries until overlay or reindex completes."
                    : "codebase://architecture reads canonical graph summaries; dirty file changes "
                      "may be absent until overlay or reindex completes.");
        }
    }

    /* Key functions by PageRank (top 10), with config-driven exclude patterns */
    struct sqlite3 *db = cbm_store_get_db(store);
    if (db && proj) {
        const char *excl_csv = srv->config
            ? cbm_config_get(srv->config, CBM_CONFIG_KEY_FUNCTIONS_EXCLUDE, "")
            : "";
        int kf_limit = srv->config
            ? cbm_config_get_int(srv->config, CBM_CONFIG_KEY_FUNCTIONS_COUNT, 25)
            : 25;
        char *sql = build_key_functions_sql(excl_csv, NULL, kf_limit, false);
        sqlite3_stmt *stmt = NULL;
        if (!sql) {
            add_response_warning(doc, root, "key_functions omitted: out of memory building SQL");
        } else if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, proj, -1, SQLITE_TRANSIENT);
            yyjson_mut_val *kf_arr = yyjson_mut_arr(doc);
            while (sqlite3_step(stmt) == SQLITE_ROW) {
                yyjson_mut_val *kf = yyjson_mut_obj(doc);
                const char *name = (const char *)sqlite3_column_text(stmt, 0);
                const char *qn = (const char *)sqlite3_column_text(stmt, 1);
                const char *label = (const char *)sqlite3_column_text(stmt, 2);
                const char *fp = (const char *)sqlite3_column_text(stmt, 3);
                double rank = sqlite3_column_double(stmt, 4);
                if (name && !ends_with_segment(qn, name))
                    yyjson_mut_obj_add_strcpy(doc, kf, "name", name);
                if (qn)              yyjson_mut_obj_add_strcpy(doc, kf, "qualified_name", qn);
                if (label && label[0]) yyjson_mut_obj_add_strcpy(doc, kf, "label", label);
                if (fp    && fp[0])    yyjson_mut_obj_add_strcpy(doc, kf, "file_path", fp);
                add_pagerank_val(doc, kf, rank);
                yyjson_mut_arr_add_val(kf_arr, kf);
            }
            yyjson_mut_obj_add_val(doc, root, "key_functions", kf_arr);
            sqlite3_finalize(stmt);
        }
        free(sql);
    }

    /* Relationship patterns from schema. Counts-only: the counts variant
     * collects rel_patterns too (the pattern join sits outside the with_props
     * gates in get_schema_impl), and this resource never displays property
     * keys — so the full json_each discovery would be pure waste here.
     * Overlay-aware via the shared selector, matching every other MCP schema
     * surface. */
    cbm_schema_info_t schema = {0};
    mcp_get_current_schema(store, proj, MCP_CYPHER_MATCH_VOCABULARY, &schema, NULL, NULL,
                           NULL);
    if (schema.rel_pattern_count > 0) {
        yyjson_mut_val *rp_arr = yyjson_mut_arr(doc);
        for (int i = 0; i < schema.rel_pattern_count; i++) {
            char *display = schema_relationship_pattern_text(&schema.rel_patterns[i], false);
            if (display) {
                yyjson_mut_arr_add_strcpy(doc, rp_arr, display);
                free(display);
            }
        }
        yyjson_mut_obj_add_val(doc, root, "relationship_patterns", rp_arr);
    }
    cbm_store_schema_free(&schema);
    cbm_store_architecture_free(&arch);
}

/* Build status resource content. */
static void build_resource_status(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                  cbm_mcp_server_t *srv) {
    cbm_store_t *store = resolve_resource_store(srv);
    const char *proj = active_project_name(srv);

    if (proj) yyjson_mut_obj_add_str(doc, root, "project", proj);

    /* IX-2: Check for indexing-in-progress BEFORE checking store contents */
    if (srv->autoindex_active) {
        yyjson_mut_obj_add_str(doc, root, "status", "indexing");
        yyjson_mut_obj_add_str(doc, root, "hint",
            "Indexing is in progress. Results will be available when status changes to 'ready'. "
            "This typically takes 5-30 seconds depending on project size.");
        return;
    }

    if (!store) {
        yyjson_mut_obj_add_str(doc, root, "status", "not_indexed");
        /* IX-1: Report if auto-index was attempted and failed */
        if (srv->autoindex_failed) {
            yyjson_mut_obj_add_str(doc, root, "detail",
                "Auto-indexing was attempted but failed. Run index_repository explicitly for detailed errors.");
        } else {
            yyjson_mut_obj_add_str(doc, root, "action_required",
                "Call index_repository with repo_path to index this project.");
        }
        return;
    }

    int nodes = cbm_store_count_nodes(store, proj);
    int edges = cbm_store_count_edges(store, proj);
    yyjson_mut_obj_add_str(doc, root, "status", nodes > 0 ? "ready" : "empty");
    if (nodes == 0 && !srv->autoindex_failed) {
        yyjson_mut_obj_add_str(doc, root, "hint",
            "Project store exists but is empty. This may happen if the project has no recognized source files, "
            "or if indexing hasn't completed yet. Try index_repository for explicit indexing.");
    }
    yyjson_mut_obj_add_int(doc, root, "nodes", nodes);
    yyjson_mut_obj_add_int(doc, root, "edges", edges);
    int dirty_pending = 0;
    int dirty_overlay_ready = 0;
    if (get_dirty_file_counts(store, proj, &dirty_pending, &dirty_overlay_ready)) {
        add_dirty_file_freshness_counts(doc, root, dirty_pending, dirty_overlay_ready);
        add_response_warning(doc, root,
                             "codebase://status counts canonical graph rows; dirty file changes "
                             "may be absent until overlay or reindex completes.");
    }
    add_overlay_node_read_view_summary(
        doc, root, store, proj,
        "codebase://status includes overlay_read_view counts, but nodes/edges are canonical "
        "counts while overlay-aware tools may read active overlay rows.");

    /* PageRank stats */
    struct sqlite3 *db = cbm_store_get_db(store);
    if (db && proj) {
        sqlite3_stmt *stmt = NULL;
        if (sqlite3_prepare_v2(db,
                "SELECT COUNT(*), MAX(computed_at) FROM pagerank WHERE project = ?1",
                -1, &stmt, NULL) == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, proj, -1, SQLITE_TRANSIENT);
            if (sqlite3_step(stmt) == SQLITE_ROW) {
                int ranked = sqlite3_column_int(stmt, 0);
                if (ranked > 0) {
                    yyjson_mut_obj_add_int(doc, root, "ranked_nodes", ranked);
                    const char *ts = (const char *)sqlite3_column_text(stmt, 1);
                    if (ts) yyjson_mut_obj_add_strcpy(doc, root, "pagerank_computed_at", ts);
                }
            }
            sqlite3_finalize(stmt);
        }
    }

    /* Detected ecosystem */
    if (srv->session_root[0]) {
        cbm_pkg_manager_t eco = cbm_detect_ecosystem(srv->session_root);
        if (eco != CBM_PKG_COUNT)
            yyjson_mut_obj_add_str(doc, root, "detected_ecosystem",
                                   cbm_pkg_manager_str(eco));
    }

    /* Dependencies — query projects table for dep entries */
    if (db && proj) {
        sqlite3_stmt *stmt = NULL;
        char pattern[512];
        snprintf(pattern, sizeof(pattern), "%s.dep.%%", proj);
        if (sqlite3_prepare_v2(db,
                "SELECT name FROM projects WHERE name LIKE ?1 ORDER BY name",
                -1, &stmt, NULL) == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, pattern, -1, SQLITE_TRANSIENT);
            yyjson_mut_val *dep_arr = yyjson_mut_arr(doc);
            int dep_count = 0;
            while (sqlite3_step(stmt) == SQLITE_ROW) {
                const char *dname = (const char *)sqlite3_column_text(stmt, 0);
                if (dname) {
                    yyjson_mut_val *d = yyjson_mut_obj(doc);
                    yyjson_mut_obj_add_strcpy(doc, d, "name", dname);
                    int dn = cbm_store_count_nodes(store, dname);
                    yyjson_mut_obj_add_int(doc, d, "nodes", dn);
                    yyjson_mut_arr_add_val(dep_arr, d);
                    dep_count++;
                }
            }
            sqlite3_finalize(stmt);
            if (dep_count > 0)
                yyjson_mut_obj_add_val(doc, root, "dependencies", dep_arr);
        }
    }
}

/* Handle resources/read — dispatch by URI.
 * Returns result JSON on success (caller wraps in JSON-RPC response).
 * On error, sets *err_out to a pre-formatted JSON-RPC error and returns NULL. */
static char *handle_resources_read(cbm_mcp_server_t *srv, const char *params_raw,
                                   const cbm_jsonrpc_request_t *req, char **err_out) {
    *err_out = NULL;
    /* Extract URI from params */
    char *uri = NULL;
    if (params_raw) {
        yyjson_doc *pdoc = yyjson_read(params_raw, strlen(params_raw), 0);
        if (pdoc) {
            yyjson_val *u = yyjson_obj_get(yyjson_doc_get_root(pdoc), "uri");
            if (u && yyjson_is_str(u))
                uri = heap_strdup(yyjson_get_str(u));
            yyjson_doc_free(pdoc);
        }
    }
    if (!uri) {
        *err_out = format_request_error(req, CBM_JSONRPC_INVALID_PARAMS, "Missing uri parameter");
        return NULL;
    }

    /* Build resource content — root IS the content object */
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    if (strcmp(uri, "codebase://schema") == 0) {
        build_resource_schema(doc, root, srv);
    } else if (strcmp(uri, "codebase://architecture") == 0) {
        build_resource_architecture(doc, root, srv);
    } else if (strcmp(uri, "codebase://status") == 0) {
        build_resource_status(doc, root, srv);
    } else {
        yyjson_mut_doc_free(doc);
        char msg[512];
        snprintf(
            msg, sizeof(msg),
            "Resource not found: '%s'. "
            "Available resources: codebase://schema, codebase://architecture, codebase://status. "
            "Use resources/list to discover all resources.",
            uri);
        free(uri);
        *err_out = format_request_error(req, CBM_MCP_RESOURCE_NOT_FOUND, msg);
        return NULL;
    }

    /* Format as resources/read response: {contents: [{uri, mimeType, text}]} */
    char *content_json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);

    yyjson_mut_doc *rdoc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *rroot = yyjson_mut_obj(rdoc);
    yyjson_mut_doc_set_root(rdoc, rroot);

    yyjson_mut_val *contents = yyjson_mut_arr(rdoc);
    yyjson_mut_val *item = yyjson_mut_obj(rdoc);
    yyjson_mut_obj_add_strcpy(rdoc, item, "uri", uri);
    yyjson_mut_obj_add_str(rdoc, item, "mimeType", "application/json");
    if (content_json)
        yyjson_mut_obj_add_strcpy(rdoc, item, "text", content_json);
    yyjson_mut_arr_add_val(contents, item);
    yyjson_mut_obj_add_val(rdoc, rroot, "contents", contents);

    char *out = yy_doc_to_str(rdoc);
    yyjson_mut_doc_free(rdoc);
    free(content_json);
    free(uri);
    return out;
}

/* ── Server request handler ───────────────────────────────────── */

char *cbm_mcp_server_handle(cbm_mcp_server_t *srv, const char *line) {
    CBM_PROF_START(prof_mcp_request_total);
    CBM_PROF_START(prof_mcp_parse);
    cbm_jsonrpc_request_t req = {0};
    int parse_status = cbm_jsonrpc_parse(line, &req);
    if (parse_status != 0) {
        CBM_PROF_END("mcp_request", "parse_error", prof_mcp_parse);
        CBM_PROF_END("mcp_request_total", "parse_error", prof_mcp_request_total);
        const char *message = "Internal error";
        if (parse_status == CBM_JSONRPC_PARSE_ERROR) {
            message = "Parse error";
        } else if (parse_status == CBM_JSONRPC_INVALID_REQUEST) {
            message = "Invalid Request";
        }
        char *error = format_request_error(&req, parse_status, message);
        cbm_jsonrpc_request_free(&req);
        return error;
    }
    CBM_PROF_END("mcp_request", "parse", prof_mcp_parse);

    /* Notifications (no id) → handle cancellation, then no response */
    if (!req.has_id) {
        if (req.method && strcmp(req.method, "notifications/cancelled") == 0) {
            /* MCP cancellation: cancel the active pipeline if request ID matches */
            cbm_mutex_lock(&srv->active_request_lock);
            cbm_pipeline_t *active =
                atomic_load_explicit(&srv->active_pipeline, memory_order_acquire);
            if (active && cbm_mcp_cancel_request_matches(req.params_raw, srv->active_request_id,
                                                         srv->active_request_id_str)) {
                cbm_pipeline_cancel(active);
                cbm_log_info("mcp.cancelled", "match", "true");
            }
            cbm_mutex_unlock(&srv->active_request_lock);
        }
        cbm_jsonrpc_request_free(&req);
        CBM_PROF_END("mcp_request_total", "notification", prof_mcp_request_total);
        return NULL;
    }

    struct timespec req_t0;
    cbm_clock_gettime(CLOCK_MONOTONIC, &req_t0);
    char *result_json = NULL;
    bool request_logged = false;

    if (strcmp(req.method, "initialize") == 0) {
        result_json = cbm_mcp_initialize_response_for_profile(req.params_raw, srv->tool_profile);
        detect_session(srv);
        if (srv->tool_profile == CBM_MCP_TOOL_PROFILE_ALL) {
            start_update_check(srv);
            maybe_auto_index(srv);
        }
    } else if (strcmp(req.method, "resources/list") == 0) {
        result_json = handle_resources_list(srv);
    } else if (strcmp(req.method, "resources/read") == 0) {
        /* Resource protocol errors are pre-formatted with the request's exact
         * numeric, string, or null ID and returned through err_out. */
        char *err_out = NULL;
        result_json = handle_resources_read(srv, req.params_raw, &req, &err_out);
        if (err_out) {
            /* Error already formatted as JSON-RPC with correct id — return directly */
            CBM_PROF_END("mcp_request_total", req.method ? req.method : "unknown",
                         prof_mcp_request_total);
            cbm_jsonrpc_request_free(&req);
            return err_out;
        }
    } else if (strcmp(req.method, "ping") == 0) {
        result_json = heap_strdup("{}");
    } else if (strcmp(req.method, "resources/templates/list") == 0) {
        /* Clients probe these on connect even when no templates or prompts are
         * declared and may treat -32601 as a failed connection (#958). */
        result_json = heap_strdup("{\"resourceTemplates\":[]}");
    } else if (strcmp(req.method, "prompts/list") == 0) {
        result_json = cbm_mcp_prompts_list();
    } else if (strcmp(req.method, "prompts/get") == 0) {
        int prompt_error = 0;
        const char *prompt_message = NULL;
        result_json = cbm_mcp_prompt_get(req.params_raw, &prompt_error, &prompt_message);
        if (prompt_error != 0) {
            char *error = format_request_error(&req, prompt_error,
                                               prompt_message ? prompt_message : "Internal error");
            struct timespec error_t1;
            cbm_clock_gettime(CLOCK_MONOTONIC, &error_t1);
            long long error_dur_us =
                ((long long)(error_t1.tv_sec - req_t0.tv_sec) * MCP_S_TO_US) +
                ((long long)(error_t1.tv_nsec - req_t0.tv_nsec) / MCP_MS_TO_US);
            cbm_log_mcp_request(req.method, NULL, true, error_dur_us);
            CBM_PROF_END("mcp_request_total", req.method, prof_mcp_request_total);
            cbm_jsonrpc_request_free(&req);
            return error;
        }
    } else if (strcmp(req.method, "tools/list") == 0) {
        result_json = cbm_mcp_tools_list_page(srv, req.params_raw);
    } else if (strcmp(req.method, "tools/call") == 0) {
        CBM_PROF_START(prof_mcp_tool_params);
        char *tool_name = NULL;
        char *tool_args = NULL;
        const char *params_error = NULL;
        int protocol_error_code =
            parse_tool_call_params(req.params_raw, &tool_name, &tool_args, &params_error);
        CBM_PROF_END("mcp_tool_call", "params", prof_mcp_tool_params);

        char protocol_error[192] = {0};
        if (protocol_error_code != 0) {
            snprintf(protocol_error, sizeof(protocol_error), "%s",
                     params_error ? params_error : "Invalid tools/call params");
        } else if (!mcp_tool_name_is_known(tool_name)) {
            protocol_error_code = CBM_JSONRPC_INVALID_PARAMS;
            snprintf(protocol_error, sizeof(protocol_error), "Unknown tool: %.128s", tool_name);
        }

        if (protocol_error_code != 0) {
            char *err = format_request_error(&req, protocol_error_code, protocol_error);
            struct timespec error_t1;
            cbm_clock_gettime(CLOCK_MONOTONIC, &error_t1);
            long long error_dur_us =
                ((long long)(error_t1.tv_sec - req_t0.tv_sec) * MCP_S_TO_US) +
                ((long long)(error_t1.tv_nsec - req_t0.tv_nsec) / MCP_MS_TO_US);
            cbm_log_mcp_request(req.method, tool_name, true, error_dur_us);
            free(tool_name);
            free(tool_args);
            CBM_PROF_END("mcp_request_total", req.method, prof_mcp_request_total);
            cbm_jsonrpc_request_free(&req);
            return err;
        }

        cbm_mutex_lock(&srv->active_request_lock);
        srv->active_request_id = req.id;
        free(srv->active_request_id_str);
        srv->active_request_id_str = req.id_str ? heap_strdup(req.id_str) : NULL;
        cbm_mutex_unlock(&srv->active_request_lock);

        struct timespec t0;
        cbm_clock_gettime(CLOCK_MONOTONIC, &t0);
        CBM_PROF_START(prof_mcp_tool_execute);
        result_json = cbm_mcp_handle_tool(srv, tool_name, tool_args);
        CBM_PROF_END("mcp_tool_execute", tool_name ? tool_name : "missing_tool",
                     prof_mcp_tool_execute);
        cbm_mutex_lock(&srv->active_request_lock);
        srv->active_request_id = CBM_NOT_FOUND;
        free(srv->active_request_id_str);
        srv->active_request_id_str = NULL;
        cbm_mutex_unlock(&srv->active_request_lock);
        struct timespec t1;
        cbm_clock_gettime(CLOCK_MONOTONIC, &t1);
        long long dur_us = ((long long)(t1.tv_sec - t0.tv_sec) * MCP_S_TO_US) +
                           ((long long)(t1.tv_nsec - t0.tv_nsec) / MCP_MS_TO_US);
        bool is_err = (result_json != NULL) && (strstr(result_json, "\"isError\":true") != NULL);
        cbm_diag_record_query(dur_us, is_err);
        long long request_dur_us = ((long long)(t1.tv_sec - req_t0.tv_sec) * MCP_S_TO_US) +
                                   ((long long)(t1.tv_nsec - req_t0.tv_nsec) / MCP_MS_TO_US);
        cbm_log_mcp_request(req.method, tool_name, is_err, request_dur_us);
        request_logged = true;

        CBM_PROF_START(prof_mcp_inject_notice);
        result_json = inject_update_notice(srv, result_json);
        CBM_PROF_END("mcp_tool_call", "inject_update_notice", prof_mcp_inject_notice);
        free(tool_name);
        free(tool_args);
    } else {
        /* Echo the original id (string or numeric, issue #253) on the error. */
        cbm_jsonrpc_response_t err_resp = {
            .id = req.id,
            .id_str = req.id_str,
            .id_is_null = req.id_is_null,
            .error_code = CBM_JSONRPC_METHOD_NOT_FOUND,
            .error_message = "Method not found",
        };
        char *err = cbm_jsonrpc_format_response(&err_resp);
        CBM_PROF_END("mcp_request_total", req.method ? req.method : "unknown",
                     prof_mcp_request_total);
        struct timespec t1;
        cbm_clock_gettime(CLOCK_MONOTONIC, &t1);
        long long dur_us = ((long long)(t1.tv_sec - req_t0.tv_sec) * MCP_S_TO_US) +
                           ((long long)(t1.tv_nsec - req_t0.tv_nsec) / MCP_MS_TO_US);
        cbm_log_mcp_request(req.method, NULL, true, dur_us);
        cbm_jsonrpc_request_free(&req);
        return err;
    }

    if (!request_logged) {
        struct timespec t1;
        cbm_clock_gettime(CLOCK_MONOTONIC, &t1);
        long long dur_us = ((long long)(t1.tv_sec - req_t0.tv_sec) * MCP_S_TO_US) +
                           ((long long)(t1.tv_nsec - req_t0.tv_nsec) / MCP_MS_TO_US);
        cbm_log_mcp_request(req.method, NULL, false, dur_us);
    }

    cbm_jsonrpc_response_t resp = {
        .id = req.id,
        .id_str = req.id_str,
        .id_is_null = req.id_is_null,
        .result_json = result_json,
    };
    CBM_PROF_START(prof_mcp_response_format);
    char *out = cbm_jsonrpc_format_response(&resp);
    CBM_PROF_END("mcp_request", "format_response", prof_mcp_response_format);
    free(result_json);
    CBM_PROF_END("mcp_request_total", req.method ? req.method : "unknown", prof_mcp_request_total);
    cbm_jsonrpc_request_free(&req);
    return out;
}

/* Best-effort tools/list_changed delivery: correctness never depends on
 * this (query_graph_no_rows_hint self-heals a stale description). Never
 * emit before the client has received at least one tools/list response.
 * The server tracks no explicit MCP `initialize`-handshake-complete state,
 * so "query_graph_tool_description built" — set only inside a served
 * tools/list — is the narrowest existing proxy for that gate. */
static bool mcp_tools_list_already_served(const cbm_mcp_server_t *srv) {
    return srv && srv->query_graph_tool_description != NULL;
}

/* Drain a pending list_changed notification strictly AFTER the response
 * write so notification and response bytes never interleave (single-
 * threaded writes preserved: background publication threads only ever set
 * the flag via cbm_mcp_server_notify_index_published; only the request
 * thread reaches this drain and calls send_notification). The
 * atomic_exchange coalesces any burst of publications into one
 * notification, and a client relist does not itself re-set the flag. */
static void mcp_drain_tools_list_changed(cbm_mcp_server_t *srv) {
    if (mcp_tools_list_already_served(srv) &&
        atomic_exchange(&srv->tools_list_changed_pending, false)) {
        send_notification(srv, "notifications/tools/list_changed");
    }
}

/* Handle a Content-Length-framed message (LSP-style transport).
 * Reads headers, body, processes request, writes framed response. */
static void handle_content_length_frame(cbm_mcp_server_t *srv, FILE *in, FILE *out, char **line,
                                        size_t *cap, int content_len) {
    /* Skip blank line(s) between header and body */
    while (cbm_getline(line, cap, in) > 0) {
        size_t hlen = strlen(*line);
        while (hlen > 0 && ((*line)[hlen - SKIP_ONE] == '\n' || (*line)[hlen - SKIP_ONE] == '\r')) {
            (*line)[--hlen] = '\0';
        }
        if (hlen == 0) {
            break;
        }
    }

    char *body = malloc((size_t)content_len + SKIP_ONE);
    if (!body) {
        return;
    }
    size_t nread = fread(body, SKIP_ONE, (size_t)content_len, in);
    body[nread] = '\0';

    srv->out_content_length_framed = true;
    char *resp = cbm_mcp_server_handle(srv, body);
    free(body);

    if (resp) {
        write_protocol_json(out, resp, true);
        free(resp);
        mcp_drain_tools_list_changed(srv);
    }
}

#ifndef _WIN32
/* Unix 3-phase poll: non-blocking fd check, FILE* buffer peek, blocking poll.
 * Returns: 1 = data ready, 0 = timeout (evicted idle stores), -1 = error/EOF. */
static int poll_for_input_unix(cbm_mcp_server_t *srv, int fd, FILE *in) {
    struct pollfd pfd = {.fd = fd, .events = POLLIN};
    int idle_timeout_s = cbm_mcp_store_idle_timeout_s(srv);
    int pr = poll(&pfd, SKIP_ONE, 0); /* Phase 1: non-blocking */

    if (pr < 0) {
        return CBM_NOT_FOUND;
    }
    if (pr > 0) {
        return SKIP_ONE;
    }

    /* Phase 2: peek FILE* buffer */
    int saved_flags = fcntl(fd, F_GETFL);
    if (saved_flags < 0) {
        /* fcntl failed — fall through to a short blocking poll (see the Phase-3
         * note below on why the interval is bounded, not the full idle timeout) */
        pr = poll(&pfd, SKIP_ONE, MCP_TIMEOUT_MS);
        if (pr < 0) {
            return CBM_NOT_FOUND;
        }
        if (pr == 0) {
            cbm_mcp_server_evict_idle(srv, idle_timeout_s);
            return 0;
        }
        return SKIP_ONE;
    }

    (void)fcntl(fd, F_SETFL, saved_flags | O_NONBLOCK);
    int c = fgetc(in);
    (void)fcntl(fd, F_SETFL, saved_flags);

    if (c == EOF) {
        if (feof(in)) {
            return CBM_NOT_FOUND; /* true EOF */
        }
        clearerr(in);
        /* Phase 3: blocking poll, bounded to a SHORT interval (not the full idle
         * timeout). macOS poll()/select() do NOT report POLLIN/POLLHUP when a
         * FIFO's last writer closes — only read() returns 0 there (verified). A
         * 60s poll would therefore leave the server blocked up to a full idle
         * timeout after stdin EOF (a client that closes the pipe would appear to
         * hang). Waking every MCP_TIMEOUT_MS lets the Phase-2 read() above detect
         * the EOF within ~1s. Idle-store eviction (threshold STORE_IDLE_TIMEOUT_S)
         * is idempotent, so checking it on each short tick is harmless. */
        pr = poll(&pfd, SKIP_ONE, MCP_TIMEOUT_MS);
        if (pr < 0) {
            return CBM_NOT_FOUND;
        }
        if (pr == 0) {
            cbm_mcp_server_evict_idle(srv, idle_timeout_s);
            return 0;
        }
        return SKIP_ONE;
    }

    (void)ungetc(c, in);
    return SKIP_ONE;
}
#endif

/* ── Event loop ───────────────────────────────────────────────── */

int cbm_mcp_server_run(cbm_mcp_server_t *srv, FILE *in, FILE *out) {
    srv->out_stream = out; /* store for sending notifications */
    srv->out_content_length_framed = false;
    char *line = NULL;
    size_t cap = 0;
    int fd = cbm_fileno(in);

    for (;;) {
        if (atomic_load(&srv->stop_requested)) {
            break; /* signal-handler/watchdog shutdown (see request_stop) */
        }
        /* Poll with idle timeout so we can evict unused stores between requests.
         *
         * IMPORTANT: poll() operates on the raw fd, but getline() reads from a
         * buffered FILE*. When a client sends multiple messages in rapid
         * succession, the first getline() call may drain ALL kernel data into
         * libc's internal FILE* buffer. Subsequent poll() calls then see an
         * empty kernel fd and block for store_idle_timeout_s seconds even
         * though the next messages are already in the FILE* buffer.
         *
         * Fix (Unix): use a three-phase approach —
         *   Phase 1: non-blocking poll (timeout=0) to check the kernel fd.
         *   Phase 2: if Phase 1 returns 0, peek the FILE* buffer via fgetc/
         *            ungetc to detect data buffered by a prior getline() call.
         *            The fd is temporarily set O_NONBLOCK so fgetc() returns
         *            immediately (EAGAIN → EOF + ferror) instead of blocking
         *            when the FILE* buffer is empty, which would otherwise
         *            bypass the Phase 3 idle eviction timeout.
         *   Phase 3: only if both phases confirm no data, do blocking poll. */
#ifdef _WIN32
        /* Windows: WaitForSingleObject on stdin handle */
        HANDLE hStdin = (HANDLE)_get_osfhandle(fd);
        int idle_timeout_s = cbm_mcp_store_idle_timeout_s(srv);
        DWORD wr = WaitForSingleObject(hStdin, (DWORD)(idle_timeout_s * MCP_TIMEOUT_MS));
        if (wr == WAIT_FAILED) {
            break;
        }
        if (wr == WAIT_TIMEOUT) {
            cbm_mcp_server_evict_idle(srv, idle_timeout_s);
            continue;
        }
#else
        int pr = poll_for_input_unix(srv, fd, in);
        if (pr < 0) {
            break;
        }
        if (pr == 0) {
            continue; /* timeout — idle stores evicted */
        }
#endif

        if (cbm_getline(&line, &cap, in) <= 0) {
            break;
        }

        /* Trim trailing newline/CR */
        size_t len = strlen(line);
        while (len > 0 && (line[len - SKIP_ONE] == '\n' || line[len - SKIP_ONE] == '\r')) {
            line[--len] = '\0';
        }
        if (len == 0) {
            continue;
        }

        /* Content-Length framing (LSP-style transport) */
        if (strncmp(line, MCP_CONTENT_HEADER, SLEN(MCP_CONTENT_HEADER)) == 0) {
            int content_len = (int)strtol(line + MCP_CONTENT_PREFIX, NULL, CBM_DECIMAL_BASE);
            if (content_len > 0 && content_len <= MCP_DEFAULT_LIMIT * CBM_SZ_1K * CBM_SZ_1K) {
                handle_content_length_frame(srv, in, out, &line, &cap, content_len);
            }
            continue;
        }

        char *resp = cbm_mcp_server_handle(srv, line);
        if (resp) {
            srv->out_content_length_framed = false;
            write_protocol_json(out, resp, false);
            free(resp);
            /* Drain AFTER the response so notification and response bytes
             * never interleave; single-threaded writes preserved
             * (background threads only ever set the pending flag). */
            mcp_drain_tools_list_changed(srv);
        }
    }

    free(line);
    return 0;
}

/* ── cbm_parse_file_uri ──────────────────────────────────────── */

bool cbm_parse_file_uri(const char *uri, char *out_path, int out_size) {
    if (!uri || !out_path || out_size <= 0) {
        if (out_path && out_size > 0) {
            out_path[0] = '\0';
        }
        return false;
    }

    /* Must start with file:// */
    if (strncmp(uri, "file://", SLEN("file://")) != 0) {
        out_path[0] = '\0';
        return false;
    }

    const char *path = uri + MCP_URI_PREFIX;

    /* On Windows, file:///C:/path → /C:/path. Strip leading / before drive letter. */
    if (path[0] == '/' && path[SKIP_ONE] &&
        ((path[SKIP_ONE] >= 'A' && path[SKIP_ONE] <= 'Z') ||
         (path[SKIP_ONE] >= 'a' && path[SKIP_ONE] <= 'z')) &&
        path[PAIR_LEN] == ':') {
        path++; /* skip the leading / */
    }

    snprintf(out_path, out_size, "%s", path);
    return true;
}
