/*
 * mcp.c — MCP server: JSON-RPC 2.0 over stdio with graph tools.
 *
 * Uses yyjson for fast JSON parsing/building.
 * Single-threaded event loop: read line → parse → dispatch → respond.
 */

// operations

#include "foundation/constants.h"

#define SLEN(s) (sizeof(s) - 1)
#define MCP_CONTENT_HEADER "Content-Length:"

enum {
    MCP_FIELD_SIZE = 1040,
    MCP_TIMEOUT_MS = 1000,
    MCP_HALF_SEC_US = 500000,
    MCP_MAX_ROWS = 100,
    MCP_MAX_DEPTH = 15,
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
    MCP_N_DEFAULTS_2 = 2,
    MCP_URI_PREFIX = 7,      /* strlen("file://") */
    MCP_CONTENT_PREFIX = SLEN(MCP_CONTENT_HEADER),
    MCP_RETURN_2 = 2,
};
#define MCP_MS_TO_US 1000LL
#define MCP_S_TO_US 1000000LL

#include "mcp/mcp.h"
#include "store/store.h"
#include <sqlite3.h>
#include "cypher/cypher.h"
#include "discover/discover.h"
#include "pipeline/pipeline.h"
#include "depindex/depindex.h"
#include "pagerank/pagerank.h"
#include "pipeline/pass_cross_repo.h"
#include "git/git_context.h"
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
#include "foundation/str_util.h"
#include "foundation/dump_verify.h"
#include "foundation/compat_regex.h"
#include <sqlite3.h>
#include "pipeline/artifact.h"

#ifdef _WIN32
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
        summary.active_file_tombstones <= 0) {
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
    if (!summary || summary->active_file_tombstones <= 0) {
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
    if (!summary || summary->active_file_tombstones <= 0) {
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
    if (!summary || summary->active_file_tombstones <= 0) {
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
    if (!summary || summary->active_file_tombstones <= 0) {
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
        "query_graph used active overlay node rows for this node-only Cypher query; "
        "relationship, EXISTS, and degree-derived Cypher queries remain canonical until "
        "active Cypher relationship views are available.");
}

static void add_overlay_active_schema_freshness(
    yyjson_mut_doc *doc, yyjson_mut_val *root,
    const cbm_store_overlay_node_view_summary_t *summary, bool include_properties,
    const char *warning) {
    if (!summary || summary->active_file_tombstones <= 0) {
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
    if (!summary || summary->active_file_tombstones <= 0) {
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
        summary.active_file_tombstones <= 0) {
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
        summary.active_file_tombstones <= 0) {
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

/* Auto-index default used by production servers with a config store. Embedded
 * no-config servers stay manual unless CBM_AUTO_INDEX explicitly opts in. */
#define CBM_DEFAULT_AUTO_INDEX_LIMIT 50000

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

/* JSON-RPC 2.0 standard error codes */
#define JSONRPC_PARSE_ERROR (-32700)
#define JSONRPC_METHOD_NOT_FOUND (-32601)

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
    memset(out, 0, sizeof(*out));
    out->id = CBM_NOT_FOUND;

    yyjson_doc *doc = yyjson_read(line, strlen(line), 0);
    if (!doc) {
        return CBM_NOT_FOUND;
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    if (!yyjson_is_obj(root)) {
        yyjson_doc_free(doc);
        return CBM_NOT_FOUND;
    }

    yyjson_val *v_jsonrpc = yyjson_obj_get(root, "jsonrpc");
    yyjson_val *v_method = yyjson_obj_get(root, "method");
    yyjson_val *v_id = yyjson_obj_get(root, "id");
    yyjson_val *v_params = yyjson_obj_get(root, "params");

    if (!v_method || !yyjson_is_str(v_method)) {
        yyjson_doc_free(doc);
        return CBM_NOT_FOUND;
    }

    out->jsonrpc =
        heap_strdup(v_jsonrpc && yyjson_is_str(v_jsonrpc) ? yyjson_get_str(v_jsonrpc) : "2.0");
    out->method = heap_strdup(yyjson_get_str(v_method));

    if (v_id) {
        out->has_id = true;
        if (yyjson_is_int(v_id)) {
            out->id = yyjson_get_int(v_id);
        } else if (yyjson_is_str(v_id)) {
            /* JSON-RPC 2.0 §4 permits string ids (Claude Desktop uses them).
             * Preserve verbatim instead of coercing via strtol (issue #253). */
            out->id_str = heap_strdup(yyjson_get_str(v_id));
        }
    }

    if (v_params) {
        out->params_raw = yyjson_val_write(v_params, 0, NULL);
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
    if (resp->id_str) {
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
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_obj_add_str(doc, root, "jsonrpc", "2.0");
    yyjson_mut_obj_add_int(doc, root, "id", id);

    yyjson_mut_val *err = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_int(doc, err, "code", code);
    yyjson_mut_obj_add_str(doc, err, "message", message);
    yyjson_mut_obj_add_val(doc, root, "error", err);

    char *out = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return out;
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
    yyjson_mut_obj_add_str(doc, item, "text", text);
    yyjson_mut_arr_add_val(content, item);
    yyjson_mut_obj_add_val(doc, root, "content", content);

    if (is_error) {
        yyjson_mut_obj_add_bool(doc, root, "isError", true);
    }

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

/* ── Tool definitions ─────────────────────────────────────────── */

typedef struct {
    const char *name;
    const char *description;
    const char *input_schema; /* JSON string */
} tool_def_t;

static const tool_def_t TOOLS[] = {
    {"index_repository",
     "Index a repository into the knowledge graph. Use for explicit indexing or pre-warming; "
     "graph-backed default tools (search_graph, query_graph, trace_path, get_code) can "
     "auto-index the server CWD or explicit directory paths on first use when auto_index "
     "is enabled. "
     "Special mode 'cross-repo-intelligence': skip extraction, only match Routes/Channels "
     "across projects to create CROSS_HTTP_CALLS/CROSS_ASYNC_CALLS/CROSS_CHANNEL edges. "
     "Requires target_projects param. Ensure target projects have fresh indexes first.",
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
     "\"persistence\":{\"type\":\"boolean\",\"default\":false,\"description\":"
     "\"Write compressed artifact to .codebase-memory/graph.db.zst for team sharing. "
     "Teammates can bootstrap from the artifact instead of full re-indexing.\"}"
     "},\"required\":[\"repo_path\"]}"},

    {"search_graph",
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
     "label/file_path='', degree=0. Saves tokens.\"},"
     "\"include_dependencies\":{\"type\":\"boolean\",\"default\":true,\"description\":\"Include "
     "symbols from dependency sub-projects (marked source=dependency in results). Set false to "
     "scope to project code only. When true, project symbols rank above dependency symbols by "
     "default (config key search_disable_dep_ranking=true reverts to pure relevance).\"},"
     "\"exclude\":{\"type\":\"array\",\"items\":{\"type\":\"string\"},\"description\":\"Glob "
     "patterns for file paths to exclude from results (e.g. [\\\"tests/**\\\",\\\"scripts/**\\\"])."
     "\"}}}"},

    {"query_graph",
     "Execute a Cypher query against the knowledge graph for complex multi-hop patterns, "
     "aggregations, and cross-service analysis. Row scan and output bytes are capped by default "
     "(config keys query_max_rows and query_max_output_bytes). Set max_output_bytes=0 for "
     "unlimited output bytes or add LIMIT. "
     "Dependency sub-project symbols (proj.dep.*) are tagged source:dependency; to rank your own "
     "project's symbols above them, ORDER BY CASE WHEN n.project LIKE '%.dep.%' THEN 1 ELSE 0 END.",
     "{\"type\":\"object\",\"properties\":{\"query\":{\"type\":\"string\",\"description\":\"Cypher "
     "query\"},\"project\":{\"type\":\"string\",\"description\":\"Indexed project name. Omit to "
     "use the MCP server project derived from server CWD.\"},\"max_rows\":{\"type\":\"integer\","
     "\"description\":\"Scan-level row limit. Omit to use query_max_rows config. Set 0 to use "
     "the implementation ceiling. Note: limits nodes scanned, not rows returned. For output size, "
     "use max_output_bytes or add LIMIT to your Cypher query.\"},\"max_output_bytes\":{\"type\":"
     "\"integer\",\"description\":\"Max response size in bytes (configurable via "
     "query_max_output_bytes config key). Set to 0 for unlimited. When exceeded, returns "
     "truncated=true with total_bytes and hint to add LIMIT.\"}},\"required\":[\"query\"]}"},

    {"trace_path",
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
     "trace_max_results config key). Set higher for exhaustive traces. Response includes "
     "callees_total/callers_total for truncation awareness.\"},\"compact\":{\"type\":\"boolean\","
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
     "\"}},\"description\":\"Pass function_name OR qualified_name (at least one required).\"}"},

    {"get_code_snippet",
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

    {"get_graph_schema", "Get the schema of the knowledge graph (node labels, edge types)",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\",\"description\":"
     "\"Indexed project name or repository directory. Omit to use the MCP server project derived "
     "from server CWD; first use may auto-index it.\"}}}"},

    {"get_architecture",
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
     "\"aspects\":{\"type\":"
     "\"array\",\"items\":{\"type\":\"string\"},\"description\":\"Optional sections to include; "
     "omit for the default overview.\"},"
     "\"exclude\":{\"type\":\"array\",\"items\":{\"type\":\"string\"},\"description\":\"Optional "
     "file-path globs to omit from key_functions, e.g. tests/** or vendor/**.\"}}}"},

    {"search_code",
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
     "\"}},\"required\":["
     "\"pattern\"]}"},

    {"list_projects", "List all indexed projects", "{\"type\":\"object\",\"properties\":{}}"},

    {"delete_project", "Delete a project from the index",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\",\"description\":"
     "\"Indexed project name to delete.\"}},\"required\":["
     "\"project\"]}"},

    {"index_status", "Get the indexing status of a project",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\",\"description\":"
     "\"Indexed project name to inspect.\"}},\"required\":["
     "\"project\"]}"},

    {"detect_changes", "Detect code changes and their impact",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\",\"description\":"
     "\"Indexed project name whose repository history should be compared.\"},\"scope\":{\"type\":"
     "\"string\",\"description\":\"Optional path or subsystem scope for impact analysis.\"},"
     "\"depth\":{\"type\":\"integer\",\"default\":2,\"description\":\"Reserved for future "
     "multi-hop impact traversal; current result reports changed files and directly defined "
     "symbols.\"},\"base_branch\":{\"type\":"
     "\"string\",\"default\":\"main\",\"description\":\"Git branch used when since is omitted.\"},"
     "\"since\":{\"type\":\"string\",\"description\":"
     "\"Git ref or tag to compare from (e.g. HEAD~5, v0.5.0). Diffs <ref>...HEAD.\"}},"
     "\"required\":"
     "[\"project\"]}"},

    {"manage_adr", "Create or update Architecture Decision Records",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\",\"description\":"
     "\"Indexed project name whose ADR data should be read or updated.\"},\"mode\":{\"type\":"
     "\"string\",\"enum\":[\"get\",\"update\",\"sections\"],\"description\":\"get returns ADRs, "
     "update writes content, sections returns selected sections.\"},\"content\":{\"type\":\"string\","
     "\"description\":\"ADR markdown/content for update mode.\"},"
     "\"sections\":{\"type\":\"array\",\"items\":{\"type\":\"string\"},\"description\":\"Section "
     "names to return in sections mode.\"}},\"required\":[\"project\"]"
     "}"},

    {"ingest_traces",
     "Accept runtime trace events and report the event count. Graph edge creation from traces is "
     "not yet implemented.",
     "{\"type\":\"object\",\"properties\":{\"traces\":{\"type\":\"array\",\"items\":{\"type\":"
     "\"object\"},\"description\":\"Runtime trace events to merge into the graph.\"},\"project\":{\"type\":"
     "\"string\",\"description\":\"Indexed project name receiving the trace data.\"}},\"required\":[\"traces\",\"project\"]}"},

    {"index_dependencies",
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
    {"get_code",
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

/* Config key for tool visibility mode */
#define CBM_CONFIG_TOOL_MODE "tool_mode"

static void emit_tool(yyjson_mut_doc *doc, yyjson_mut_val *tools, const tool_def_t *t) {
    yyjson_mut_val *tool = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, tool, "name", t->name);
    yyjson_mut_obj_add_str(doc, tool, "description", t->description);
    yyjson_doc *schema_doc = yyjson_read(t->input_schema, strlen(t->input_schema), 0);
    if (schema_doc) {
        yyjson_mut_val *schema = yyjson_val_mut_copy(doc, yyjson_doc_get_root(schema_doc));
        yyjson_mut_obj_add_val(doc, tool, "inputSchema", schema);
        yyjson_doc_free(schema_doc);
    }
    yyjson_mut_arr_add_val(tools, tool);
}

static bool is_streamlined_default_tool(const char *name) {
    return name && (strcmp(name, "search_graph") == 0 ||
                    strcmp(name, "query_graph") == 0 ||
                    strcmp(name, "search_code") == 0 ||
                    strcmp(name, "trace_path") == 0);
}

/* cbm_mcp_tools_list() defined after struct cbm_mcp_server (needs full type) */

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

char *cbm_mcp_initialize_response(const char *params_json) {
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
    yyjson_mut_obj_add_str(doc, impl, "version", "0.10.0");
    yyjson_mut_obj_add_val(doc, root, "serverInfo", impl);

    yyjson_mut_val *caps = yyjson_mut_obj(doc);
    yyjson_mut_val *tools_cap = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_bool(doc, tools_cap, "listChanged", true);
    yyjson_mut_obj_add_val(doc, caps, "tools", tools_cap);
    /* Advertise MCP resources capability — clients can read codebase://schema etc. */
    yyjson_mut_val *res_cap = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_bool(doc, res_cap, "subscribe", false);
    yyjson_mut_obj_add_bool(doc, res_cap, "listChanged", true);
    yyjson_mut_obj_add_val(doc, caps, "resources", res_cap);
    yyjson_mut_obj_add_val(doc, root, "capabilities", caps);

    char *out = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return out;
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
static void notify_resources_updated(cbm_mcp_server_t *srv);
static void send_notification(cbm_mcp_server_t *srv, const char *method);
static char *build_key_functions_sql(const char *exclude_csv, const char **exclude_arr, int limit);
static bool validate_cbm_db_with_timeout(const char *path, int busy_timeout_ms);
static void *overlay_compaction_thread(void *arg);

struct cbm_mcp_server {
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
    bool context_injected; /* true after first _context header sent (Phase 9) */
    bool client_has_resources; /* true if client advertised resources capability */
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
    _Atomic(cbm_pipeline_t *) active_pipeline; /* non-NULL while index_repository runs */
    int64_t active_request_id;       /* JSON-RPC id of the in-progress tool call */
};

static bool cbm_mcp_tool_mode_is_classic(cbm_mcp_server_t *srv) {
    /* Env var keeps script/test overrides independent from the persisted config. */
    char tool_mode_buf[CBM_SZ_64];
    const char *tool_mode = cbm_safe_getenv("CBM_TOOL_MODE", tool_mode_buf,
                                            sizeof(tool_mode_buf), NULL);
    if (tool_mode && tool_mode[0] != '\0') {
        return strcmp(tool_mode, "classic") == 0;
    }
    tool_mode = (srv && srv->config)
        ? cbm_config_get(srv->config, CBM_CONFIG_TOOL_MODE, "streamlined")
        : "streamlined";
    return strcmp(tool_mode, "classic") == 0;
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
        srv && srv->config ? cbm_config_get(srv->config, CBM_CONFIG_INCREMENTAL_REINDEX, "off")
                           : "off";
    return policy && strcmp(policy, "off") != 0;
}

static int cbm_mcp_effective_auto_dep_limit(cbm_mcp_server_t *srv, const char *args_json) {
    bool enabled = cbm_config_get_bool(srv ? srv->config : NULL, CBM_CONFIG_AUTO_INDEX_DEPS, true);
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

static int cbm_mcp_auto_index_limit(cbm_mcp_server_t *srv) {
    return cbm_config_get_effective_int(srv ? srv->config : NULL, CBM_CONFIG_AUTO_INDEX_LIMIT,
                                        CBM_DEFAULT_AUTO_INDEX_LIMIT);
}

static bool cbm_mcp_auto_index_within_limit(cbm_mcp_server_t *srv, const char *root_path) {
    int file_limit = cbm_mcp_auto_index_limit(srv);
    if (file_limit <= 0) {
        return true;
    }
    cbm_discover_opts_t opts = {.mode = CBM_MODE_FULL, .ignore_file = NULL, .max_file_size = 0};
    int count = 0;
    if (cbm_discover_count_bounded(root_path, &opts, file_limit, &count) != 0) {
        cbm_log_warn("autoindex.skip", "reason", "file_count_failed", "path",
                     root_path ? root_path : "");
        return false;
    }
    if (count > file_limit) {
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

char *cbm_mcp_tools_list(cbm_mcp_server_t *srv) {
    bool classic = cbm_mcp_tool_mode_is_classic(srv);
    bool reveal_hidden = (!classic && srv && srv->hidden_tools_revealed);

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_val *tools = yyjson_mut_arr(doc);

    if (!classic) {
        /* Streamlined mode: default surface = focused graph/text tools from
         * TOOLS[] plus fork-only aliases from STREAMLINED_TOOLS[]. Keeping
         * canonical tools in TOOLS[] prevents schema drift between modes. */
        for (int i = 0; i < TOOL_COUNT; i++) {
            if (is_streamlined_default_tool(TOOLS[i].name)) {
                emit_tool(doc, tools, &TOOLS[i]);
            }
        }
        for (int i = 0; i < STREAMLINED_TOOL_COUNT; i++) {
            emit_tool(doc, tools, &STREAMLINED_TOOLS[i]);
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
            if (reveal_hidden || cbm_mcp_tool_config_enabled(srv, TOOLS[i].name)) {
                emit_tool(doc, tools, &TOOLS[i]);
            }
        }

        /* Progressive disclosure: list advanced tools so AI knows they exist.
         * Added as a special tool entry with description explaining how to enable.
         * Default-surface tools (search_graph, query_graph, search_code,
         * trace_path, get_code) are NOT listed here. */
        yyjson_mut_val *hint_tool = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_str(doc, hint_tool, "name", "_hidden_tools");
        yyjson_mut_obj_add_str(doc, hint_tool, "description",
            "Advanced tools are normally hidden in streamlined mode. "
            "Advanced tools: index_repository, get_code_snippet, "
            "get_graph_schema, get_architecture, list_projects, "
            "delete_project, index_status, detect_changes, manage_adr, "
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
        yyjson_mut_obj_add_val(doc, hint_tool, "inputSchema", hint_schema);
        yyjson_mut_arr_add_val(tools, hint_tool);
    } else {
        /* Classic mode: all original tools. trace_path is the upstream-listed
         * name and the single canonical call-tracing tool. */
        for (int i = 0; i < TOOL_COUNT; i++) {
            emit_tool(doc, tools, &TOOLS[i]);
        }
    }

    yyjson_mut_obj_add_val(doc, root, "tools", tools);

    char *out = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return out;
}

cbm_mcp_server_t *cbm_mcp_server_new(const char *store_path) {
    cbm_mcp_server_t *srv = calloc(CBM_ALLOC_ONE, sizeof(*srv));
    if (!srv) {
        return NULL;
    }
    cbm_mutex_init(&srv->update_notice_lock);

    /* If a store_path is given, open that project directly.
     * Otherwise, create an in-memory store for test/embedded use. */
    if (store_path) {
        srv->store = cbm_store_open(store_path);
        srv->current_project = heap_strdup(store_path);
    } else {
        srv->store = cbm_store_open_memory();
    }
    srv->owns_store = true;

    cbm_mutex_init(&srv->overlay_compaction_lock);
    return srv;
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
}

void cbm_mcp_server_set_session_project(cbm_mcp_server_t *srv, const char *name) {
    if (!srv || !name) return;
    snprintf(srv->session_project, sizeof(srv->session_project), "%s", name);
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

void cbm_mcp_server_free(cbm_mcp_server_t *srv) {
    if (!srv) {
        return;
    }
    if (srv->update_thread_active) {
        cbm_thread_join(&srv->update_tid);
    }
    if (srv->autoindex_active) {
        cbm_thread_join(&srv->autoindex_tid);
    }
    (void)cbm_mcp_server_join_overlay_compaction(srv, NULL);
    cbm_mutex_destroy(&srv->overlay_compaction_lock);
    cbm_mutex_destroy(&srv->update_notice_lock);
    if (srv->owns_store && srv->store) {
        cbm_store_close(srv->store);
    }
    free(srv->current_project);
    free(srv);
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

    cbm_mutex_lock(&srv->overlay_compaction_lock);
    if (srv->overlay_compaction_started) {
        cbm_mutex_unlock(&srv->overlay_compaction_lock);
        return false;
    }
    snprintf(srv->overlay_compaction_project, sizeof(srv->overlay_compaction_project), "%s",
             project);
    srv->overlay_compaction_max_generations = max_generations;
    srv->overlay_compaction_rc = CBM_STORE_ERR;
    srv->overlay_compaction_compacted = 0;
    srv->overlay_compaction_finished = false;
    srv->overlay_compaction_started = true;
    cbm_mutex_unlock(&srv->overlay_compaction_lock);

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

    cbm_mutex_lock(&srv->overlay_compaction_lock);
    snprintf(project, sizeof(project), "%s", srv->overlay_compaction_project);
    max_generations = srv->overlay_compaction_max_generations;
    cbm_mutex_unlock(&srv->overlay_compaction_lock);

    int rc = CBM_STORE_ERR;
    int compacted = 0;
    char path[CBM_SZ_1K];
    project_db_path(project, path, sizeof(path));
    if (path[0]) {
        cbm_store_t *store = cbm_store_open_path_query(path);
        if (store) {
            rc = cbm_store_compact_ready_overlay_generations(store, project, max_generations,
                                                             &compacted);
            cbm_store_close(store);
        } else {
            rc = CBM_STORE_NOT_FOUND;
        }
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

    /* Dep projects (e.g., "myapp.dep.pandas") live in the parent project's DB
     * ("myapp.db"), not in a separate "myapp.dep.pandas.db". Extract parent. */
    char parent_buf[1024];
    const char *db_project = parent_project_for_db(project, parent_buf, sizeof(parent_buf));

    /* Already open for this project's DB? */
    if (srv->current_project && strcmp(srv->current_project, db_project) == 0 && srv->store) {
        return srv->store;
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
    if (!validate_cbm_db_with_timeout(path, validate_busy_timeout_ms)) {
        return NULL;
    }
    srv->store = cbm_store_open_path_query(path);
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
        if (cbm_store_get_project(srv->store, project, &proj_verify) != CBM_STORE_OK) {
            cbm_store_close(srv->store);
            srv->store = NULL;
            return NULL;
        }
        /* Register newly-accessed project with watcher (root_path from DB).
         * Validate that it looks like an absolute POSIX, UNC, or Windows-drive
         * path before watching — a retained bad-path DB (#557) may store a
         * numeric/empty root_path that would point the watcher at nothing. */
        if (srv->watcher && root_path_looks_usable(proj_verify.root_path)) {
            cbm_watcher_watch(srv->watcher, project, proj_verify.root_path);
        }
        sync_session_from_open_project(srv, srv->store, db_project, &proj_verify);
        cbm_project_free_fields(&proj_verify);
        srv->owns_store = true;
        free(srv->current_project);
        srv->current_project = heap_strdup(project);
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
                                    bool *truncated) {
    int count = 0;
    int listed = 0;
    size_t offset = 0;
    if (truncated) {
        *truncated = false;
    }
    if (out && out_sz > 0) {
        out[0] = '\0';
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
        count++;

        char project_name[CBM_DIRENT_NAME_MAX];
        size_t project_len = len - MCP_DB_EXT;
        if (project_len >= sizeof(project_name)) {
            if (truncated) {
                *truncated = true;
            }
            continue;
        }
        memcpy(project_name, n, project_len);
        project_name[project_len] = '\0';

        char escaped[CBM_SZ_1K];
        int escaped_len = cbm_json_escape(escaped, (int)sizeof(escaped), project_name);
        if (escaped_len <= 0 && project_name[0] != '\0') {
            if (truncated) {
                *truncated = true;
            }
            continue;
        }

        size_t item_len = (size_t)escaped_len + CBM_QUOTE_PAIR;
        if (listed > 0) {
            item_len += SKIP_ONE;
        }
        if (!out || out_sz == 0 || offset + item_len >= out_sz) {
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
    yyjson_mut_obj_add_val(doc, obj, "git", git);

    cbm_git_context_free(&ctx);
}

/* Build a helpful error listing available projects. Caller must free() result. */
static char *build_project_list_error_srv(cbm_mcp_server_t *srv, const char *reason) {
    char dir_path[1024];
    cache_dir(dir_path, sizeof(dir_path));

    char projects[CBM_SZ_4K] = "";
    bool projects_truncated = false;
    int count = collect_db_project_names(dir_path, projects, sizeof(projects), &projects_truncated);

    /* Optional: session_project and _context fields for richer error context */
    char session_frag[256] = "";
    char context_frag[512] = "";
    if (srv && srv->session_project[0]) {
        snprintf(session_frag, sizeof(session_frag),
                 ",\"session_project\":\"%s\"", srv->session_project);
        /* Include a minimal _context so clients can identify session state */
        bool ctx_enabled = cbm_config_get_bool(srv->config, "context_injection", true);
        if (ctx_enabled && !srv->context_injected) {
            snprintf(context_frag, sizeof(context_frag),
                     ",\"_context\":{\"status\":\"not_indexed\","
                     "\"hint\":\"No project indexed yet. Pass project='/path/to/repo' to index.\"}");
            srv->context_injected = true;  /* one-shot: suppress from future successful responses */
        }
    }

    enum { ERR_BUF_SZ = 6144 };
    char buf[ERR_BUF_SZ];
    if (count > 0) {
        snprintf(buf, sizeof(buf),
                 "{\"error\":\"%s\",\"hint\":\"Use list_projects to see all indexed projects, "
                 "then pass the project name.\",\"available_projects\":[%s],\"count\":%d%s%s%s}",
                 reason, projects, count,
                 projects_truncated ? ",\"available_projects_truncated\":true" : "",
                 session_frag, context_frag);
    } else {
        snprintf(buf, sizeof(buf),
                 "{\"error\":\"%s\",\"hint\":\"No projects indexed yet. "
                 "Call index_repository first.\"%s%s}",
                 reason, session_frag, context_frag);
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
                                cbm_mcp_server_t *srv, cbm_store_t *store) {
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
    bool inject_enabled = cbm_config_get_bool(srv->config, "context_injection", true);
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

    /* Node/edge counts */
    const char *proj = srv->session_project[0] ? srv->session_project : NULL;
    int nodes = cbm_store_count_nodes(store, proj);
    int edges = cbm_store_count_edges(store, proj);
    yyjson_mut_obj_add_int(doc, ctx, "nodes", nodes);
    yyjson_mut_obj_add_int(doc, ctx, "edges", edges);

    /* Schema: node labels + edge types */
    cbm_schema_info_t schema = {0};
    cbm_store_get_schema(store, proj, &schema);
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
    if (db && proj) {
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
    if (db && proj) {
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
        char *kf_sql = build_key_functions_sql(kf_exclude, NULL, kf_cfg_limit);
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

    /* Detected ecosystem */
    if (srv->session_root[0]) {
        cbm_pkg_manager_t eco = cbm_detect_ecosystem(srv->session_root);
        if (eco != CBM_PKG_COUNT) {
            yyjson_mut_obj_add_str(doc, ctx, "detected_ecosystem",
                                   cbm_pkg_manager_str(eco));
        }
    }

    yyjson_mut_obj_add_val(doc, root, "_context", ctx);
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

    /* Open READ-ONLY — never modify foreign databases.
     * Check for 'nodes' table which is the core cbm schema indicator. */
    sqlite3 *db = NULL;
    int rc = sqlite3_open_v2(path, &db, SQLITE_OPEN_READONLY, NULL);
    if (rc != SQLITE_OK) {
        const char *base = strrchr(path, '/');
        base = base ? base + 1 : path;
        cbm_log_warn("db.skip", "file", base, "reason", "sqlite_open_failed");
        if (db) sqlite3_close(db);
        return false;
    }
    sqlite3_busy_timeout(db, busy_timeout_ms);

    sqlite3_stmt *stmt = NULL;
    rc = sqlite3_prepare_v2(db,
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
    sqlite3_close(db);
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

/* Open a .db file briefly, collect node/edge counts and root_path,
 * then append a JSON entry to arr. */
static void build_project_json_entry(yyjson_mut_doc *doc, yyjson_mut_val *arr, const char *dir_path,
                                     const char *name, size_t name_len, int64_t size_bytes) {
    char project_name[CBM_SZ_1K];
    int project_len =
        snprintf(project_name, sizeof(project_name), "%.*s", (int)(name_len - MCP_DB_EXT), name);
    if (project_len <= 0 || (size_t)project_len >= sizeof(project_name)) {
        return;
    }

    char full_path[CBM_SZ_2K];
    int full_path_len = snprintf(full_path, sizeof(full_path), "%s/%s", dir_path, name);
    if (full_path_len <= 0 || (size_t)full_path_len >= sizeof(full_path)) {
        return;
    }

    cbm_store_t *pstore = cbm_store_open_path(full_path);
    int nodes = 0;
    int edges = 0;
    char root_path_buf[CBM_SZ_1K] = "";
    if (pstore) {
        nodes = cbm_store_count_nodes(pstore, project_name);
        edges = cbm_store_count_edges(pstore, project_name);
        cbm_project_t proj = {0};
        if (cbm_store_get_project(pstore, project_name, &proj) == CBM_STORE_OK) {
            if (proj.root_path) {
                snprintf(root_path_buf, sizeof(root_path_buf), "%s", proj.root_path);
            }
            safe_str_free(&proj.name);
            safe_str_free(&proj.indexed_at);
            safe_str_free(&proj.root_path);
        }
        cbm_store_close(pstore);
    }

    yyjson_mut_val *p = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_strcpy(doc, p, "name", project_name);
    yyjson_mut_obj_add_strcpy(doc, p, "root_path", root_path_buf);
    add_git_context_json(doc, p, root_path_buf[0] ? root_path_buf : NULL);
    yyjson_mut_obj_add_int(doc, p, "nodes", nodes);
    yyjson_mut_obj_add_int(doc, p, "edges", edges);
    yyjson_mut_obj_add_int(doc, p, "size_bytes", size_bytes);
    yyjson_mut_arr_add_val(arr, p);
}

/* list_projects: scan cache directory for .db files.
 * Each project is a single .db file — no central registry needed. */
static char *handle_list_projects(cbm_mcp_server_t *srv, const char *args) {
    (void)args;

    char dir_path[CBM_SZ_1K];
    cache_dir(dir_path, sizeof(dir_path));
    int validate_busy_timeout_ms = cbm_mcp_db_validate_busy_timeout_ms(srv);

    cbm_dir_t *d = cbm_opendir(dir_path);

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_val *arr = yyjson_mut_arr(doc);

    if (d) {
        cbm_dirent_t *entry;
        while ((entry = cbm_readdir(d)) != NULL) {
            const char *name = entry->name;
            size_t len = strlen(name);

            if (!is_project_db_file(name, len)) {
                continue;
            }

            /* Extract project name = filename without .db suffix */
            char project_name[CBM_SZ_1K];
            int project_len =
                snprintf(project_name, sizeof(project_name), "%.*s", (int)(len - MCP_DB_EXT),
                         name);
            if (project_len <= 0 || (size_t)project_len >= sizeof(project_name)) {
                continue;
            }

            /* Skip invalid project names (corrupt entries like ..db) */
            if (project_name[0] == '\0' || strcmp(project_name, ".") == 0 ||
                strcmp(project_name, "..") == 0) {
                continue;
            }

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

            build_project_json_entry(doc, arr, dir_path, name, len, (int64_t)st.st_size);
        }
        cbm_closedir(d);
    }

    yyjson_mut_obj_add_val(doc, root, "projects", arr);

    /* Guide user when no projects are indexed */
    if (yyjson_mut_arr_size(arr) == 0) {
        yyjson_mut_obj_add_str(doc, root, "hint",
                               "No projects indexed. Call index_repository(repo_path=...) first.");
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

static char *handle_get_graph_schema(cbm_mcp_server_t *srv, const char *args) {
    char *raw_project = cbm_mcp_get_string_arg(args, "project");
    project_expand_t pe = {0};
    cbm_store_t *store = resolve_project_store(srv, raw_project, &pe);
    char *project = pe.value;
    REQUIRE_STORE(store, project);

    cbm_store_overlay_node_view_summary_t overlay_summary = {0};
    bool overlay_ready =
        cbm_store_get_overlay_node_view_summary(store, project, &overlay_summary) == CBM_STORE_OK &&
        overlay_summary.active_file_tombstones > 0;
    bool used_active_schema = false;
    bool active_schema_failed = false;

    cbm_schema_info_t schema = {0};
    if (overlay_ready && cbm_store_get_schema_overlay_view(store, project, &schema) == CBM_STORE_OK) {
        used_active_schema = true;
    } else {
        active_schema_failed = overlay_ready;
        cbm_store_get_schema(store, project, &schema);
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

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

    /* Check ADR presence */
    cbm_project_t proj_info = {0};
    if (cbm_store_get_project(store, project, &proj_info) == 0 && proj_info.root_path) {
        char adr_path[CBM_SZ_4K];
        int adr_len = snprintf(adr_path, sizeof(adr_path), "%s/.codebase-memory/adr.md",
                               proj_info.root_path);
        bool adr_exists = adr_len > 0 && (size_t)adr_len < sizeof(adr_path) &&
                          cbm_file_exists(adr_path);
        yyjson_mut_obj_add_bool(doc, root, "adr_present", adr_exists);
        if (!adr_exists) {
            yyjson_mut_obj_add_str(
                doc, root, "adr_hint",
                "No ADR found. Use manage_adr(mode='update') to persist architectural "
                "decisions across MCP server runs. Run get_architecture(aspects=['all']) first.");
        }
        cbm_project_free_fields(&proj_info);
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

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    cbm_store_schema_free(&schema);
    free(project);

    char *result = cbm_mcp_text_result(json, false);
    free(json);
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
                    int effective_dep_limit = cbm_mcp_effective_auto_dep_limit(srv, NULL);
                    (void)cbm_mcp_auto_index_deps(srv, srv->session_project,
                                                  srv->session_root, store,
                                                  effective_dep_limit, NULL);
                    cbm_pagerank_compute_with_config(store, srv->session_project, srv->config);
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
                    cbm_pagerank_compute_with_config(store, db_project, srv->config);
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
    BM25_DEFAULT_LIMIT = 100,
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
    if (!summary || summary->active_file_tombstones <= 0) {
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
        "  JOIN active_files af"
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
                         const char *file_pattern, int limit, int offset) {
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

/* Forward declaration — defined later. enrich_node_properties parses the
 * node's properties_json and grafts the parsed values onto the result item.
 * It returns the parsed yyjson_doc which must outlive the serialization
 * because yyjson_mut_obj_add_val uses zero-copy strings into that doc. */
static yyjson_doc *enrich_node_properties(yyjson_mut_doc *doc, yyjson_mut_val *obj,
                                          const char *properties_json);

/* Emit the cbm_store_search results as a JSON "results" array on the doc.
 * Property docs created via enrich_node_properties are collected in
 * *out_pdocs (count in *out_pdoc_count) and must be freed by the caller
 * AFTER serializing doc, since yyjson_mut strings are zero-copy pointers
 * into those parsed docs. The caller also frees out_pdocs itself. */
static void emit_search_results(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                const cbm_search_output_t *out, cbm_store_t *store,
                                const char *relationship, bool include_connected,
                                bool connected_names_authoritative, int offset, int limit,
                                bool compact, const char *session_project,
                                yyjson_doc ***out_pdocs, int *out_pdoc_count) {
    yyjson_doc **pdocs = out->count > 0 ? malloc((size_t)out->count * sizeof(yyjson_doc *)) : NULL;
    int pdoc_count = 0;
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
        yyjson_doc *pdoc = enrich_node_properties(doc, item, sr->node.properties_json);
        if (pdoc && pdocs) {
            pdocs[pdoc_count++] = pdoc;
        }
        yyjson_mut_arr_add_val(results, item);
    }
    yyjson_mut_obj_add_val(doc, root, "results", results);
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

static char *semantic_query_type_error_response(void) {
    return cbm_mcp_text_result(
        "semantic_query must be an array of keyword strings, e.g. "
        "[\"send\",\"pubsub\",\"publish\"], not a single string. Split your query "
        "into individual keywords; each is scored independently via per-keyword "
        "min-cosine.",
        true);
}

/* Append the semantic_query vector-search results onto the doc.  Returns
 * true if semantic_query was provided as a non-array (type error — caller
 * should surface to the user). */
static bool run_semantic_query(yyjson_mut_doc *doc, yyjson_mut_val *root, const char *args,
                               cbm_store_t *store, const char *project, int limit) {
    enum { MAX_KW_SEARCH = 32 };
    yyjson_doc *args_doc = yyjson_read(args, strlen(args), 0);
    yyjson_val *args_root = args_doc ? yyjson_doc_get_root(args_doc) : NULL;
    yyjson_val *sq_val = args_root ? yyjson_obj_get(args_root, "semantic_query") : NULL;
    bool type_error = false;
    if (sq_val && !yyjson_is_arr(sq_val)) {
        type_error = true;
    } else if (sq_val && yyjson_arr_size(sq_val) > 0) {
        if (project && cbm_store_derived_view_is_stale(
                           store, project, CBM_STORE_DERIVED_VIEW_SEMANTIC_EDGES)) {
            add_stale_derived_view_warning(
                doc, root, CBM_STORE_DERIVED_VIEW_SEMANTIC_EDGES,
                "semantic_edges derived view is stale; semantic_results may be stale.");
        }
        const char *keywords[MAX_KW_SEARCH];
        int ki = extract_semantic_keywords(sq_val, keywords, MAX_KW_SEARCH);
        cbm_vector_result_t *vresults = NULL;
        int vcount = 0;
        int sem_limit = limit > 0 ? limit : CBM_SZ_16;
        if (cbm_store_vector_search(store, project, keywords, ki, sem_limit, &vresults, &vcount) ==
                CBM_STORE_OK &&
            vcount > 0) {
            emit_semantic_results(doc, root, vresults, vcount);
            cbm_store_free_vector_results(vresults, vcount);
        }
    }
    if (args_doc) {
        yyjson_doc_free(args_doc);
    }
    return type_error;
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

    bool sq_type_error = run_semantic_query(mdoc, root, args, store, project, limit);
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

/* Convert shell-glob wildcards to POSIX ERE: bare '*' → '.*', bare '?' → '.'
 * "Bare" means not already preceded by '.' or '\'. This lets users pass
 * glob-style patterns like "*tool*" and have them work as ".*tool.*". */
static char *glob_to_regex(const char *glob) {
    size_t len = strlen(glob);
    /* Worst case: every char expands to 2 chars plus NUL */
    char *out = malloc(len * 2 + 1);
    if (!out) return NULL;
    size_t o = 0;
    for (size_t i = 0; i < len; i++) {
        char prev = i > 0 ? glob[i - 1] : 0;
        if (glob[i] == '*' && prev != '.' && prev != '\\') {
            out[o++] = '.';
            out[o++] = '*';
        } else if (glob[i] == '?' && prev != '.' && prev != '\\') {
            out[o++] = '.';
        } else {
            out[o++] = glob[i];
        }
    }
    out[o] = '\0';
    return out;
}

static char *handle_search_graph(cbm_mcp_server_t *srv, const char *args) {
    char *raw_project = cbm_mcp_get_string_arg(args, "project");
    project_expand_t pe = {0};
    cbm_store_t *store = resolve_project_store(srv, raw_project, &pe);
    char *project = pe.value;
    REQUIRE_STORE(store, project);

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
            q_overlay_summary.active_file_tombstones > 0;
        bool q_has_terms = !q_overlay_ready || bm25_query_has_terms(query);
        char *bm25_json =
            q_overlay_ready
                ? bm25_search_overlay_active(store, project, query, q_file_pattern, q_limit,
                                             q_offset, &q_overlay_summary)
                : bm25_search(store, project, query, q_file_pattern, q_limit, q_offset);
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
            char *result = cbm_mcp_text_result(fresh_json ? fresh_json : payload_json, false);
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
    /* F9: pre-validate regex patterns — auto-convert glob wildcards to regex.
     * Users/agents frequently pass *tool* (glob) instead of .*tool.* (regex).
     * On regex compilation failure, try glob_to_regex() conversion before erroring. */
    if (name_pattern) {
        cbm_regex_t re;
        if (cbm_regcomp(&re, name_pattern, CBM_REG_EXTENDED | CBM_REG_NOSUB) != 0) {
            char *converted = glob_to_regex(name_pattern);
            if (converted && cbm_regcomp(&re, converted, CBM_REG_EXTENDED | CBM_REG_NOSUB) == 0) {
                cbm_regfree(&re);
                free(name_pattern);
                name_pattern = converted;
            } else {
                free(converted);
                char errbuf[512];
                snprintf(errbuf, sizeof(errbuf),
                    "{\"error\":\"invalid regex in name_pattern: '%s'\","
                    "\"hint\":\"Use regex syntax: '.*tool.*' instead of '*tool*'\"}", name_pattern);
                free(label); free(name_pattern); free(pe.value);
                return cbm_mcp_text_result(errbuf, true);
            }
        } else {
            cbm_regfree(&re);
        }
    }
    if (qn_pattern) {
        cbm_regex_t re;
        if (cbm_regcomp(&re, qn_pattern, CBM_REG_EXTENDED | CBM_REG_NOSUB) != 0) {
            char *converted = glob_to_regex(qn_pattern);
            if (converted && cbm_regcomp(&re, converted, CBM_REG_EXTENDED | CBM_REG_NOSUB) == 0) {
                cbm_regfree(&re);
                free(qn_pattern);
                qn_pattern = converted;
            } else {
                free(converted);
                char errbuf[512];
                snprintf(errbuf, sizeof(errbuf),
                    "{\"error\":\"invalid regex in qn_pattern: '%s'\","
                    "\"hint\":\"Use regex syntax: '.*tool.*' instead of '*tool*'\"}", qn_pattern);
                free(label); free(name_pattern); free(qn_pattern); free(pe.value);
                return cbm_mcp_text_result(errbuf, true);
            }
        } else {
            cbm_regfree(&re);
        }
    }
    /* NEW: unified pattern — OR search across name AND qualified_name */
    char *unified_pattern = cbm_mcp_get_string_arg(args, "pattern");
    if (unified_pattern) {
        cbm_regex_t re;
        if (cbm_regcomp(&re, unified_pattern, CBM_REG_EXTENDED | CBM_REG_NOSUB) != 0) {
            char *converted = glob_to_regex(unified_pattern);
            if (converted && cbm_regcomp(&re, converted, CBM_REG_EXTENDED | CBM_REG_NOSUB) == 0) {
                cbm_regfree(&re);
                free(unified_pattern);
                unified_pattern = converted;
            } else {
                free(converted);
                char errbuf[512];
                snprintf(errbuf, sizeof(errbuf),
                    "{\"error\":\"invalid regex in pattern: '%s'\","
                    "\"hint\":\"Use regex syntax: '.*tool.*' instead of '*tool*'\"}", unified_pattern);
                free(label); free(name_pattern); free(qn_pattern);
                free(unified_pattern); free(pe.value);
                return cbm_mcp_text_result(errbuf, true);
            }
        } else {
            cbm_regfree(&re);
        }
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

    cbm_search_output_t out = {0};
    cbm_store_overlay_node_view_summary_t overlay_summary = {0};
    bool overlay_ready_for_nodes =
        project && project[0] &&
        cbm_store_get_overlay_node_view_summary(store, project, &overlay_summary) ==
            CBM_STORE_OK &&
        overlay_summary.active_file_tombstones > 0;
    bool overlay_active_edges_requested =
        relationship || include_connected || exclude_entry_points ||
        min_degree >= 0 || max_degree >= 0 ||
        (sort_by && (strcmp(sort_by, "degree") == 0 || strcmp(sort_by, "calls") == 0 ||
                     strcmp(sort_by, "linkrank") == 0));
    bool overlay_search_used = overlay_ready_for_nodes;
    if (overlay_search_used) {
        cbm_store_search_overlay_view(store, &params, &out);
    } else {
        cbm_store_search(store, &params, &out);
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
                            srv->session_project,
                            &props_docs, &props_doc_count);
    }

    /* Auto-context: first response gets full architecture/schema/_context header.
     * Subsequent responses just get session_project. */
    inject_context_once(doc, root, srv, store);
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
            if (eco == CBM_PKG_COUNT) {
                snprintf(hint, sizeof(hint),
                    "No dependency sub-projects indexed, and no recognized build system "
                    "detected in '%s'. Supported: Python/uv (pyproject.toml, requirements.txt), "
                    "Rust/cargo, npm/bun (package.json), Go (go.mod), JVM/Maven/Gradle, "
                    ".NET/NuGet (*.csproj), Ruby/Bundler (Gemfile), PHP/Composer, "
                    "Swift/SPM, Dart/pub, Elixir/Mix, C-Make (Makefile), C-CMake, "
                    "C-Meson, C-Conan, or generic vendor/ directory. "
                    "Re-index after adding a manifest file.",
                    srv->session_root[0] ? srv->session_root : "(unknown project root)");
            } else {
                snprintf(hint, sizeof(hint),
                    "No dependency sub-projects indexed yet for %s build system '%s'. "
                    "Dep scanning runs automatically on index_repository. "
                    "If deps are vendored in vendor/ vendored/ third_party/ etc., "
                    "re-run index_repository(repo_path=\"%s\") to trigger dep discovery.",
                    cbm_pkg_manager_str(eco), cbm_pkg_manager_str(eco),
                    srv->session_root[0] ? srv->session_root : "<repo_path>");
            }
            yyjson_mut_obj_add_strcpy(doc, root, "hint", hint);
        }
    }

    /* Semantic (vector) search: append semantic_results if semantic_query
     * array was provided.  Returns true on type error (non-array value). */
    bool sq_type_error = run_semantic_query(doc, root, args, store, project, limit);
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

static char *handle_query_graph(cbm_mcp_server_t *srv, const char *args) {
    /* B7: schema says "cypher" but handler read "query" — fix to read "cypher" first */
    char *query = cbm_mcp_get_string_arg(args, "cypher");
    if (!query) query = cbm_mcp_get_string_arg(args, "query"); /* backward compat */
    /* CQ-2: use resolve_project_store for "self"/"dep"/path expansion */
    char *raw_project = cbm_mcp_get_string_arg(args, "project");
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

    if (!query) {
        free(project);
        return cbm_mcp_text_result(
            "{\"error\":\"query is required\","
            "\"hint\":\"Pass a Cypher query string, e.g. MATCH (n:Function) RETURN n.name LIMIT 10\"}", true);
    }
    if (!store) {
        char *_err = build_project_list_error("project not found or not indexed");
        char *_res = cbm_mcp_text_result(_err, true);
        free(_err);
        free(project);
        free(query);
        return _res;
    }

    cbm_store_overlay_node_view_summary_t overlay_summary = {0};
    bool overlay_ready =
        project && cbm_store_get_overlay_node_view_summary(store, project, &overlay_summary) ==
                       CBM_STORE_OK &&
        overlay_summary.active_file_tombstones > 0;
    bool used_active_cypher_nodes = false;
    cbm_cypher_result_t result = {0};
    int rc = overlay_ready ? cbm_cypher_execute_active_nodes(store, query, project, max_rows,
                                                             &result,
                                                             &used_active_cypher_nodes)
                           : cbm_cypher_execute(store, query, project, max_rows, &result);

    if (rc < 0) {
        char *err_msg = result.error ? result.error : "query execution failed";
        char *resp = cbm_mcp_text_result(err_msg, true);
        cbm_cypher_result_free(&result);
        free(query);
        free(project);
        return resp;
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    add_query_graph_derived_warnings(doc, root, store, project, query, &result);
    bool overlay_limitation_reported = false;
    if (used_active_cypher_nodes) {
        add_overlay_active_cypher_freshness(doc, root, &overlay_summary);
    } else {
        overlay_limitation_reported = add_canonical_only_overlay_freshness(
            doc, root, store, project,
            "query_graph reads canonical Cypher rows for this query shape; ready overlay rows are "
            "included only for node-only Cypher queries until active relationship views or "
            "compaction are available.");
    }
    int dirty_pending = 0;
    int dirty_overlay_ready = 0;
    if (get_dirty_file_counts(store, project, &dirty_pending, &dirty_overlay_ready)) {
        add_dirty_file_freshness_counts(doc, root, dirty_pending, dirty_overlay_ready);
        if (!used_active_cypher_nodes) {
            add_canonical_only_read_model(doc, root);
        }
        if (!used_active_cypher_nodes && !overlay_limitation_reported) {
            add_response_warning(doc, root,
                                 "query_graph reads canonical graph rows; dirty file changes may "
                                 "be absent until overlay or reindex completes.");
        } else if (used_active_cypher_nodes && dirty_pending > 0) {
            add_response_warning(doc, root,
                                 "query_graph used ready overlay node rows, but pending dirty "
                                 "files may still be absent until overlay or reindex completes.");
        }
    } else if (overlay_limitation_reported) {
        add_canonical_only_read_model(doc, root);
    }

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

    /* CQ-3: Warn when filter params combined with cypher — they're silently ignored */
    {
        char *ignored_label = cbm_mcp_get_string_arg(args, "label");
        if (ignored_label) {
            yyjson_mut_obj_add_str(doc, root, "warning",
                "cypher param present; label, name_pattern, file_pattern, sort_by, and other "
                "filter params are ignored in Cypher mode. Use WHERE clause instead.");
            free(ignored_label);
        }
    }

    char *json = yy_doc_to_str(doc);
    int total_rows = result.row_count;
    yyjson_mut_doc_free(doc);
    cbm_cypher_result_free(&result);
    free(query);
    free(project);

    /* Output truncation: cap response at max_output_bytes */
    if (max_output_bytes > 0 && json) {
        size_t json_len = strlen(json);
        if (json_len > (size_t)max_output_bytes) {
            /* Build a truncated response with metadata */
            char trunc_json[256];
            snprintf(trunc_json, sizeof(trunc_json),
                     "{\"truncated\":true,\"total_bytes\":%lu,\"rows_returned\":%d,"
                     "\"hint\":\"Add LIMIT to your Cypher query\"}",
                     (unsigned long)json_len, total_rows);
            char *res = cbm_mcp_text_result(trunc_json, false);
            free(json);
            return res;
        }
    }

    char *res = cbm_mcp_text_result(json, false);
    free(json);
    return res;
}

static char *handle_index_status(cbm_mcp_server_t *srv, const char *args) {
    char *raw_project = cbm_mcp_get_string_arg(args, "project");
    project_expand_t pe = {0};
    cbm_store_t *store = resolve_project_store(srv, raw_project, &pe);
    char *project = pe.value;
    REQUIRE_STORE(store, project);

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    if (srv->session_project[0])
        yyjson_mut_obj_add_str(doc, root, "session_project", srv->session_project);

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
                add_git_context_json(doc, root, proj_info.root_path);
                cbm_pkg_manager_t eco = cbm_detect_ecosystem(proj_info.root_path);
                if (eco != CBM_PKG_COUNT) {
                    yyjson_mut_obj_add_str(doc, root, "detected_ecosystem",
                                           cbm_pkg_manager_str(eco));
                }
            }
            /* Always free project fields — cbm_store_get_project heap-allocates strings */
            cbm_project_free_fields(&proj_info);
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
    char *name = cbm_mcp_get_string_arg(args, "project");
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

/* Check if an aspect is requested (NULL aspects = all, or array contains "all" or the name). */
static bool aspect_wanted(yyjson_doc *aspects_doc, yyjson_val *aspects_arr, const char *name) {
    if (!aspects_arr) {
        return true; /* no filter = all */
    }
    yyjson_arr_iter iter;
    yyjson_arr_iter_init(aspects_arr, &iter);
    yyjson_val *val;
    while ((val = yyjson_arr_iter_next(&iter)) != NULL) {
        const char *s = yyjson_get_str(val);
        if (s && (strcmp(s, "all") == 0 || strcmp(s, name) == 0)) {
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

static char *handle_get_architecture(cbm_mcp_server_t *srv, const char *args) {
    char *raw_project = cbm_mcp_get_string_arg(args, "project");
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

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    if (srv->session_project[0])
        yyjson_mut_obj_add_str(doc, root, "session_project", srv->session_project);

    if (project) {
        yyjson_mut_obj_add_str(doc, root, "project", project);
    }
    if (path_scoped) {
        yyjson_mut_obj_add_str(doc, root, "path", norm_path);
        yyjson_mut_obj_add_int(doc, root, "root_total_nodes",
                               cbm_store_count_nodes(store, project));
        yyjson_mut_obj_add_int(doc, root, "root_total_edges",
                               cbm_store_count_edges(store, project));
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
            yyjson_mut_arr_add_str(doc, pats, schema.rel_patterns[i]);
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
                char *kf_sql_heap =
                    build_key_functions_sql(excl_csv, (const char **)excl_arr, kf_limit);
                if (!kf_sql_heap) {
                    add_response_warning(doc, root,
                                         "key_functions omitted: out of memory building SQL");
                } else {
                    const char *kf_sql = kf_sql_heap;
                    sqlite3_stmt *kf_stmt = NULL;
                    if (sqlite3_prepare_v2(db, kf_sql, -1, &kf_stmt, NULL) == SQLITE_OK) {
                        if (project) sqlite3_bind_text(kf_stmt, 1, project, -1, SQLITE_TRANSIENT);
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

/* Forward declaration: defined after trace_path */
static void free_node_contents(cbm_node_t *n);
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

/* Pick the best-resolving node among name matches. Sets *ambiguous when the top
 * score is shared by more than one candidate AND degree cannot break the tie
 * (a genuine tie the caller must disambiguate), so resolution never silently
 * traces the wrong same-named node. Degree acts as a final tiebreaker: a node
 * that participates in the graph (has edges) beats an isolated stub. */
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
    int tied_idx[64];
    int tie_count = 0;
    for (int i = 0; i < count && tie_count < 64; i++) {
        if (node_resolution_score(&nodes[i]) == best_score) {
            tied_idx[tie_count++] = i;
        }
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
                               bool include_tests, bool risk_labels, char **exclude_likes) {
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

static char *handle_trace_path(cbm_mcp_server_t *srv, const char *args) {
    char *func_name = cbm_mcp_get_string_arg(args, "function_name");
    char *qn_input = cbm_mcp_get_string_arg(args, "qualified_name"); /* cross-tool chaining */
    char *raw_project = cbm_mcp_get_string_arg(args, "project");
    char *direction = cbm_mcp_get_string_arg(args, "direction");
    char *trace_mode = cbm_mcp_get_string_arg(args, "mode"); /* calls|data_flow|cross_service */
    char *param_name = cbm_mcp_get_string_arg(args, "parameter_name");
    int depth = cbm_mcp_get_int_arg(args, "depth", 3);
    /* F10: clamp depth to minimum 1 — O(1) */
    if (depth < 1) depth = 1;
    int cfg_trace_max = cbm_config_get_int(srv->config, CBM_CONFIG_TRACE_MAX_RESULTS,
                                            CBM_DEFAULT_TRACE_MAX_RESULTS);
    int max_results = cbm_mcp_get_positive_int_arg(args, "max_results", cfg_trace_max,
                                                   CBM_DEFAULT_TRACE_MAX_RESULTS);
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
        overlay_summary.active_file_tombstones > 0;

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

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    /* func_name may be NULL when only qualified_name was passed — use qn_input as fallback */
    yyjson_mut_obj_add_str(doc, root, "function",
        func_name ? func_name : (qn_input ? qn_input : ""));
    yyjson_mut_obj_add_str(doc, root, "direction", effective_direction);

    /* Report candidates when multiple nodes matched but resolution picked one */
    if (node_count > 1) {
        yyjson_mut_val *candidates = yyjson_mut_arr(doc);
        for (int i = 0; i < node_count && i < 5; i++) {
            yyjson_mut_val *c = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_str(doc, c, "qualified_name",
                                   nodes[i].qualified_name ? nodes[i].qualified_name : "");
            if (nodes[i].file_path && nodes[i].file_path[0])
                yyjson_mut_obj_add_str(doc, c, "file_path", nodes[i].file_path);
            yyjson_mut_arr_append(candidates, c);
        }
        yyjson_mut_obj_add_val(doc, root, "candidates", candidates);
        yyjson_mut_obj_add_str(doc, root, "resolved",
                               nodes[sel].qualified_name ? nodes[sel].qualified_name : "");
    }

    /* Extract edge_types here — after all early returns — to avoid memory leaks.
     * free_string_array(NULL) is NULL-safe.
     * Resolution order: explicit edge_types array > mode-based defaults > CALLS. */
    int edge_type_count_user = 0;
    char **edge_types_user = cbm_mcp_get_string_array_arg(args, "edge_types",
                                                           &edge_type_count_user);
    if (edge_type_count_user < 0) {
        yyjson_mut_doc_free(doc);
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

    if (do_outbound) {
        int bfs_rc = overlay_trace_requested
                         ? cbm_store_bfs_overlay_view(store, project, nodes[sel].qualified_name,
                                                      "outbound", edge_types, edge_type_count,
                                                      depth, max_results, &tr_out)
                         : cbm_store_bfs(store, nodes[sel].id, "outbound", edge_types,
                                         edge_type_count, depth, max_results, &tr_out);
        if (bfs_rc == CBM_STORE_OK && overlay_trace_requested) {
            overlay_trace_succeeded = true;
        }

        yyjson_mut_val *callees = yyjson_mut_arr(doc);
        trace_append_nodes(srv, doc, callees, &tr_out, compact, include_tests, risk_labels,
                           exclude_likes);
        yyjson_mut_obj_add_val(doc, root, "callees", callees);
        yyjson_mut_obj_add_int(doc, root, "callees_total", tr_out.visited_count);
    }

    if (do_inbound) {
        int bfs_rc = overlay_trace_requested
                         ? cbm_store_bfs_overlay_view(store, project, nodes[sel].qualified_name,
                                                      "inbound", edge_types, edge_type_count,
                                                      depth, max_results, &tr_in)
                         : cbm_store_bfs(store, nodes[sel].id, "inbound", edge_types,
                                         edge_type_count, depth, max_results, &tr_in);
        if (bfs_rc == CBM_STORE_OK && overlay_trace_requested) {
            overlay_trace_succeeded = true;
        }

        yyjson_mut_val *callers = yyjson_mut_arr(doc);
        trace_append_nodes(srv, doc, callers, &tr_in, compact, include_tests, risk_labels,
                           exclude_likes);
        yyjson_mut_obj_add_val(doc, root, "callers", callers);
        yyjson_mut_obj_add_int(doc, root, "callers_total", tr_in.visited_count);
    }

    add_derived_freshness_warnings(doc, root,
                                   (do_outbound && tr_out.pagerank_stale) ||
                                       (do_inbound && tr_in.pagerank_stale),
                                   (do_outbound && tr_out.linkrank_stale) ||
                                       (do_inbound && tr_in.linkrank_stale),
                                   false);
    int dirty_pending = 0;
    int dirty_overlay_ready = 0;
    if (overlay_trace_succeeded) {
        add_overlay_active_trace_freshness(doc, root, &overlay_summary);
    }
    if (get_dirty_file_counts(store, project, &dirty_pending, &dirty_overlay_ready)) {
        add_dirty_file_freshness_counts(doc, root, dirty_pending, dirty_overlay_ready);
        if (!overlay_trace_succeeded) {
            add_response_warning(doc, root,
                                 "trace_path reads canonical graph rows; dirty file changes may "
                                 "be absent until overlay or reindex completes.");
        }
    }

    if (srv->session_project[0])
        yyjson_mut_obj_add_str(doc, root, "session_project", srv->session_project);

    /* Serialize BEFORE freeing traversal results (yyjson borrows strings) */
    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);

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
    FILE *fp = fopen(path, "r");
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
    cbm_project_t proj = {0};
    if (cbm_store_get_project(store, slug, &proj) != CBM_STORE_OK) {
        free(slug_owned);
        return NULL;
    }
    char *root = heap_strdup(proj.root_path);
    free((void *)proj.name);
    free((void *)proj.indexed_at);
    free((void *)proj.root_path);
    free(slug_owned);
    return root;
}

/* ── index_repository ─────────────────────────────────────────── */

/* Handle mode="cross-repo-intelligence" — extract to reduce complexity. */
static char *handle_cross_repo_mode(const char *repo_path, const char *args) {
    char *project = heap_strdup(cbm_project_name_from_path(repo_path));
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

    int total = result.http_edges + result.async_edges + result.channel_edges + result.grpc_edges +
                result.graphql_edges + result.trpc_edges;
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_str(doc, root, "status", "success");
    yyjson_mut_obj_add_str(doc, root, "mode", "cross-repo-intelligence");
    yyjson_mut_obj_add_strcpy(doc, root, "project", project);
    yyjson_mut_obj_add_int(doc, root, "projects_scanned", result.projects_scanned);
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

static char *handle_index_repository(cbm_mcp_server_t *srv, const char *args) {
    CBM_PROF_START(prof_index_total);
    CBM_PROF_START(prof_index_args);
    char *repo_path = cbm_mcp_get_string_arg(args, "repo_path");
    char *mode_str = cbm_mcp_get_string_arg(args, "mode");
    cbm_normalize_path_sep(repo_path);

    if (!repo_path) {
        free(mode_str);
        CBM_PROF_END("index_repository", "args", prof_index_args);
        CBM_PROF_END("index_repository", "TOTAL", prof_index_total);
        return cbm_mcp_text_result(
            "{\"error\":\"repo_path is required\","
            "\"hint\":\"Pass the absolute path to the project root directory.\"}", true);
    }

    if (mode_str && strcmp(mode_str, "cross-repo-intelligence") == 0) {
        free(mode_str);
        char *result = handle_cross_repo_mode(repo_path, args);
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
        free(repo_path);
        CBM_PROF_END("index_repository", "TOTAL", prof_index_total);
        return cbm_mcp_text_result(
            "{\"error\":\"failed to create indexing pipeline\","
            "\"hint\":\"Check that repo_path exists and is readable. The directory may be empty or inaccessible.\"}", true);
    }
    CBM_PROF_START(prof_index_pipeline_config);
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
    atomic_store_explicit(&srv->active_pipeline, p, memory_order_release);
    int rc = cbm_pipeline_run(p);
    bool graph_changed = cbm_pipeline_graph_changed(p);
    cbm_pipeline_publish_kind_t publish_kind = cbm_pipeline_publish_kind(p);
    const char *publish_reason = cbm_pipeline_publish_reason(p);
    atomic_store_explicit(&srv->active_pipeline, NULL, memory_order_release);
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
    add_pipeline_exact_delta_stats(doc, root, cbm_pipeline_exact_delta_stats(p));

    if (rc == 0) {
        CBM_PROF_START(prof_index_resolve_store);
        cbm_store_t *store = resolve_store(srv, project_name);
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
                cbm_rank_refresh_publish_from_pipeline(publish_kind));
            CBM_PROF_END("index_repository", "rank_refresh", prof_index_rank_refresh);
            /* Explicit indexing just observed the current worktree state. Refresh
             * the watcher baseline so it does not immediately reindex the same
             * dirty status after this response. */
            if (srv->watcher)
                cbm_watcher_mark_indexed(srv->watcher, project_name, repo_path);

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


            /* Check ADR presence and suggest creation if missing */
            CBM_PROF_START(prof_index_adr);
            char adr_path[CBM_SZ_4K];
            int adr_len = snprintf(adr_path, sizeof(adr_path), "%s/.codebase-memory/adr.md",
                                   repo_path);
            bool adr_exists = adr_len > 0 && (size_t)adr_len < sizeof(adr_path) &&
                              cbm_file_exists(adr_path);
            yyjson_mut_obj_add_bool(doc, root, "adr_present", adr_exists);
            if (!adr_exists) {
                yyjson_mut_obj_add_str(
                    doc, root, "adr_hint",
                    "Project indexed. Consider creating an Architecture Decision Record: "
                    "explore the codebase with get_architecture(aspects=['all']), then use "
                    "manage_adr(mode='store') to persist architectural insights across MCP server runs.");
            }
            CBM_PROF_END("index_repository", "adr_check", prof_index_adr);
        }
    }

    yyjson_mut_obj_add_str(doc, root, "status", rc == 0 ? "indexed" : "error");

    /* Surface excluded subtrees (#411) so users know what wasn't indexed.
     * The discover layer collects .gitignore'd / config-excluded directories;
     * emit them as an "excluded" array (copies strings into the JSON doc, so
     * they need only outlive this block — pipeline is freed below). */
    CBM_PROF_START(prof_index_response_fields);
    if (excluded_count > 0 && excluded_dirs) {
        yyjson_mut_val *arr = yyjson_mut_arr(doc);
        for (int i = 0; i < excluded_count; i++) {
            if (excluded_dirs[i] && excluded_dirs[i][0]) {
                yyjson_mut_arr_add_strcpy(doc, arr, excluded_dirs[i]);
            }
        }
        if (yyjson_mut_arr_size(arr) > 0) {
            yyjson_mut_obj_add_val(doc, root, "excluded", arr);
        }
    }

    if (srv->session_project[0])
        yyjson_mut_obj_add_str(doc, root, "session_project", srv->session_project);
    CBM_PROF_END("index_repository", "response_fields", prof_index_response_fields);

    /* Notify resource-capable clients that graph data changed */
    CBM_PROF_START(prof_index_notify);
    if (rc == 0) notify_resources_updated(srv);
    CBM_PROF_END("index_repository", "notify_resources", prof_index_notify);

    CBM_PROF_START(prof_index_serialize);
    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    /* Free the pipeline only after the response doc copied the excluded list. */
    cbm_pipeline_free(p);
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
                                          const char *properties_json) {
    if (!properties_json || properties_json[0] == '\0') {
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
    yyjson_val *key;
    while ((key = yyjson_obj_iter_next(&iter))) {
        yyjson_val *val = yyjson_obj_iter_get_val(key);
        const char *k = yyjson_get_str(key);
        if (!k) {
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
        } else if (yyjson_is_bool(val)) {
            yyjson_mut_obj_add_bool(doc, obj, k, yyjson_get_bool(val));
        } else if (yyjson_is_int(val)) {
            yyjson_mut_obj_add_int(doc, obj, k, yyjson_get_int(val));
        } else if (yyjson_is_real(val)) {
            yyjson_mut_obj_add_real(doc, obj, k, yyjson_get_real(val));
        }
    }
    return props_doc; /* caller frees after serialization */
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
        size_t apsz = strlen(root_path) + strlen(node->file_path) + 2;
        abs_path = malloc(apsz);
        snprintf(abs_path, apsz, "%s/%s", root_path, node->file_path);

        /* Path containment: resolve symlinks/../ and verify file stays within root. */
        char *real_root = mcp_resolve_existing_path(root_path);
        char *real_file = mcp_resolve_existing_path(abs_path);
        bool path_ok = false;
        if (real_root && real_file) {
            size_t root_len = strlen(real_root);
            if (strncmp(real_file, real_root, root_len) == 0 &&
                (real_file[root_len] == '/' || real_file[root_len] == '\\' ||
                 real_file[root_len] == '\0')) {
                path_ok = true;
            }
        }
        free(real_root);
        free(real_file);
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
    if (node->properties_json && node->properties_json[0] != '\0') {
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
    char *raw_project = cbm_mcp_get_string_arg(args, "project");
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
    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
    const char *flag = use_regex ? "-E" : "-F";
    const char *ci_flag = case_sensitive ? "" : " -i";
    int n;
    if (scan_mode == SEARCH_CODE_SCAN_GIT_GREP) {
#ifdef _WIN32
        n = snprintf(cmd, cmd_sz,
                     "git -C \"%s\" grep -n%s --untracked %s -f \"%s\" -- . 2>NUL",
                     root_path, ci_flag, flag, tmpfile);
#else
        n = snprintf(cmd, cmd_sz,
                     "git -C \"%s\" grep -n%s --untracked %s -f \"%s\" -- . 2>/dev/null",
                     root_path, ci_flag, flag, tmpfile);
#endif
    } else if (scan_mode == SEARCH_CODE_SCAN_FILELIST_GREP) {
        (void)file_pattern;
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

    if (mode == MODE_FULL) {
        char *source = read_file_lines(abs_path, r->start_line, r->end_line);
        if (source) {
            sanitize_ascii(source);
            yyjson_mut_obj_add_strcpy(doc, item, "source", source);
            free(source);
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
static yyjson_mut_val *build_dir_distribution(yyjson_mut_doc *doc, search_result_t *sr,
                                              int sr_count) {
    yyjson_mut_val *dirs = yyjson_mut_obj(doc);
    char dir_names[CBM_SZ_64][CBM_SZ_128];
    int dir_counts[CBM_SZ_64];
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
        } else if (dir_n < CBM_SZ_64) {
            snprintf(dir_names[dir_n], sizeof(dir_names[0]), "%s", top);
            dir_counts[dir_n] = SKIP_ONE;
            dir_n++;
        }
    }
    for (int d = 0; d < dir_n; d++) {
        yyjson_mut_val *key = yyjson_mut_strcpy(doc, dir_names[d]);
        yyjson_mut_val *val = yyjson_mut_int(doc, dir_counts[d]);
        yyjson_mut_obj_add(dirs, key, val);
    }
    return dirs;
}

/* Phase 4: assemble JSON output from search results */
static char *assemble_search_output(search_result_t *sr, int sr_count, grep_match_t *raw,
                                    int raw_count, int gm_count, int limit, int mode,
                                    int context_lines, const char *root_path,
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

/* Write a NUL-separated absolute file list from indexed graph files.
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
        if (!search_code_file_pattern_matches(file_pattern, rel)) {
            continue;
        }
        char abs_path[CBM_PATH_MAX];
        int n = snprintf(abs_path, sizeof(abs_path), "%s/%s", root_path, rel ? rel : "");
        if (n < 0 || (size_t)n >= sizeof(abs_path)) {
            continue;
        }
        size_t len = strlen(abs_path);
        if (fwrite(abs_path, SKIP_ONE, len, fl) != len || fputc('\0', fl) == EOF) {
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
    char *pattern = cbm_mcp_get_string_arg(args, "pattern");
    char *project = cbm_mcp_get_string_arg(args, "project");
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

    /* Project: explicit param > session_project fallback > error */
    if (!project && srv->session_project[0]) {
        project = heap_strdup(srv->session_project);
    }
    if (!project) {
        if (has_path_filter) {
            cbm_regfree(&path_regex);
        }
        free(pattern);
        free(file_pattern);
        char *_err = build_project_list_error("project is required");
        char *_res = cbm_mcp_text_result(_err, true);
        free(_err);
        return _res;
    }

    char *root_path = get_project_root(srv, project);
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
    } else if (write_scoped_filelist(srv, project, root_path, file_pattern, filelist)) {
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

    cbm_store_t *store = resolve_store(srv, project);

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
        overlay_summary.active_file_tombstones > 0;

    classify_all_grep_hits(gm, gm_count, store, project, &sr, &sr_count, &sr_cap, &raw, &raw_count,
                           &raw_cap, overlay_ready_for_code);

    /* Phase 3: batch degree query — ONE query for all results instead of 2×N */
    if (store && sr_count > 0) {
        int64_t *ids = malloc(sr_count * sizeof(int64_t));
        int *in_degs = malloc(sr_count * sizeof(int));
        int *out_degs = malloc(sr_count * sizeof(int));
        for (int j = 0; j < sr_count; j++) {
            ids[j] = sr[j].node_id;
        }
        if (cbm_store_batch_count_degrees(store, ids, sr_count, "CALLS", in_degs, out_degs) ==
            CBM_STORE_OK) {
            for (int j = 0; j < sr_count; j++) {
                sr[j].in_degree = in_degs[j];
                sr[j].out_degree = out_degs[j];
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
    char *result =
        assemble_search_output(sr, sr_count, raw, raw_count, gm_count, limit, mode, context_lines,
                               root_path, pat_has_pipe && !use_regex, cbm_now_ms() - search_t0,
                               search_scope, dirty_pending, dirty_overlay_ready,
                               overlay_ready_for_code
                                   ? "search_code reads live source files and uses active overlay "
                                     "graph annotations where ready; pending dirty files may still "
                                     "lack graph metadata until overlay or reindex completes."
                                   : "search_code reads live source files, but graph annotations use "
                                     "canonical graph rows; dirty file graph metadata may be absent "
                                     "until overlay or reindex completes.",
                               overlay_ready_for_code ? &overlay_summary : NULL);
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

/* Find symbols defined in a file and add them to the impacted array. */
static void detect_add_impacted_symbols(cbm_store_t *store, const char *project, const char *file,
                                        yyjson_mut_doc *doc, yyjson_mut_val *impacted) {
    cbm_node_t *nodes = NULL;
    int ncount = 0;
    cbm_store_find_nodes_by_file(store, project, file, &nodes, &ncount);
    for (int i = 0; i < ncount; i++) {
        if (nodes[i].label && strcmp(nodes[i].label, "File") != 0 &&
            strcmp(nodes[i].label, "Folder") != 0 && strcmp(nodes[i].label, "Project") != 0) {
            yyjson_mut_val *item = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_strcpy(doc, item, "name", nodes[i].name ? nodes[i].name : "");
            yyjson_mut_obj_add_strcpy(doc, item, "label", nodes[i].label);
            yyjson_mut_obj_add_strcpy(doc, item, "file", file);
            yyjson_mut_arr_add_val(impacted, item);
        }
    }
    cbm_store_free_nodes(nodes, ncount);
}

static char *handle_detect_changes(cbm_mcp_server_t *srv, const char *args) {
    char *project = cbm_mcp_get_string_arg(args, "project");
    char *base_branch = cbm_mcp_get_string_arg(args, "base_branch");
    char *since = cbm_mcp_get_string_arg(args, "since");
    char *scope = cbm_mcp_get_string_arg(args, "scope");
    int depth = cbm_mcp_get_int_arg(args, "depth", MCP_DEFAULT_BFS_DEPTH);

    /* scope: "files" = just changed files, "symbols" = files + symbols (default) */
    bool want_symbols = !scope || strcmp(scope, "symbols") == 0 || strcmp(scope, "impact") == 0;

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

    /* Reject shell metacharacters in user-supplied branch name */
    if (!cbm_validate_shell_arg(base_branch)) {
        free(project);
        free(base_branch);
        free(scope);
        return cbm_mcp_text_result("base_branch contains invalid characters", true);
    }

    char *root_path = get_project_root(srv, project);
    if (!root_path) {
        free(project);
        free(base_branch);
        return cbm_mcp_text_result(
            "{\"error\":\"project not found\","
            "\"hint\":\"Pass project='/path/to/repo' or project='~/path/to/repo' to specify the project. "
            "Run index_repository with repo_path to index it first, "
            "or use list_projects to see available projects.\"}", true);
    }

    if (!validate_search_path_arg(root_path)) {
        free(root_path);
        free(project);
        free(base_branch);
        free(scope);
        return cbm_mcp_text_result("project path contains invalid characters", true);
    }

    /* Get changed files via git (-C avoids cd + quoting issues on Windows) */
    char cmd[CBM_SZ_2K];
    int cmd_len;
#ifdef _WIN32
    cmd_len = snprintf(cmd, sizeof(cmd),
                       "git -C \"%s\" diff --name-only \"%s\"...HEAD 2>NUL & "
                       "git -C \"%s\" diff --name-only 2>NUL",
                       root_path, base_branch, root_path);
#else
    cmd_len = snprintf(cmd, sizeof(cmd),
                       "{ git -C \"%s\" diff --name-only \"%s\"...HEAD 2>/dev/null; "
                       "git -C \"%s\" diff --name-only 2>/dev/null; } | sort -u",
                       root_path, base_branch, root_path);
#endif
    if (cmd_len < 0 || (size_t)cmd_len >= sizeof(cmd)) {
        free(root_path);
        free(project);
        free(base_branch);
        free(scope);
        return cbm_mcp_text_result(
            "{\"error\":\"git diff failed: command too long\","
            "\"hint\":\"Use shorter project paths or branch names.\"}", true);
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
        return cbm_mcp_text_result(
            "{\"error\":\"git diff failed\","
            "\"hint\":\"Check that git is installed and the project is a git repository.\"}", true);
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root_obj = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root_obj);

    yyjson_mut_val *changed = yyjson_mut_arr(doc);
    yyjson_mut_val *impacted = yyjson_mut_arr(doc);

    /* resolve_store already called via get_project_root above */
    cbm_store_t *store = srv->store;

    char line[CBM_SZ_1K];
    int file_count = 0;

    while (fgets(line, sizeof(line), fp)) {
        size_t len = strlen(line);
        while (len > 0 && (line[len - SKIP_ONE] == '\n' || line[len - SKIP_ONE] == '\r')) {
            line[--len] = '\0';
        }
        if (len == 0) {
            continue;
        }

        yyjson_mut_arr_add_strcpy(doc, changed, line);
        file_count++;

        /* Find symbols defined in this file (only when scope includes symbols). */
        if (want_symbols) {
            detect_add_impacted_symbols(store, project, line, doc, impacted);
        }
    }
    int git_status = cbm_pclose(fp);

    bool is_error = false;
    if (git_status != 0 && file_count == 0) {
        char hint_buf[CBM_SZ_256];
        snprintf(hint_buf, sizeof(hint_buf),
                 "git diff exited with status %d. Check that branch '%s' exists.", git_status,
                 base_branch);
        yyjson_mut_obj_add_strcpy(doc, root_obj, "hint", hint_buf);
        is_error = true;
    }

    yyjson_mut_obj_add_val(doc, root_obj, "changed_files", changed);
    yyjson_mut_obj_add_int(doc, root_obj, "changed_count", file_count);
    yyjson_mut_obj_add_val(doc, root_obj, "impacted_symbols", impacted);
    yyjson_mut_obj_add_int(doc, root_obj, "depth", depth);

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    free(root_path);
    free(project);
    free(base_branch);
    free(scope);

    char *result = cbm_mcp_text_result(json, is_error);
    free(json);
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
    FILE *fp = fopen(adr_path, "r");
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
    char *project = cbm_mcp_get_string_arg(args, "project");
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
        return cbm_mcp_text_result(
            "{\"error\":\"project not found\","
            "\"hint\":\"Pass project='/path/to/repo' or project='~/path/to/repo' to specify the project. "
            "Run index_repository with repo_path to index it first, "
            "or use list_projects to see available projects.\"}", true);
    }
    if (!project || project[0] == '\0') {
        free(project);
        project = heap_strdup(srv->session_project);
    }

    /* ADRs are stored in the SQLite store (project_summaries), the SAME
     * backend the UI /api/adr endpoints use — so writes via the MCP tool and
     * the UI are visible to each other (#256). */
    cbm_store_t *store = resolve_store(srv, project);
    if (!store) {
        free(project);
        free(mode_str);
        free(content);
        return cbm_mcp_text_result(
            "{\"error\":\"project not found\","
            "\"hint\":\"Pass project='/path/to/repo' or project='~/path/to/repo' to specify the project. "
            "Run index_repository with repo_path to index it first, "
            "or use list_projects to see available projects.\"}", true);
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
    char *raw_project = cbm_mcp_get_string_arg(args, "project");
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
    cbm_store_t *store = resolve_store(srv, project);
    if (!store) {
        yyjson_doc_free(doc_args);
        free(project);
        free(pkg_mgr_str);
        return cbm_mcp_text_result(
            "{\"error\":\"no project loaded\","
            "\"hint\":\"Run index_repository with repo_path first.\"}", true);
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

    /* Recompute PageRank after adding dep nodes so relevance sort includes them */
    cbm_pagerank_compute_default(store, project);

    /* Notify resource-capable clients that graph data changed */
    notify_resources_updated(srv);

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    yyjson_doc_free(doc_args);
    free(project);
    free(pkg_mgr_str);
    free(root_path);

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

char *cbm_mcp_handle_tool(cbm_mcp_server_t *srv, const char *tool_name, const char *args_json) {
    if (!tool_name) {
        return cbm_mcp_text_result(
            "{\"error\":\"missing tool name\","
            "\"hint\":\"Available tools: search_graph, query_graph, search_code, "
            "trace_path, get_code. "
            "Use tools/list to see all available tools.\"}", true);
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

/* Background auto-index thread function */
static void *autoindex_thread(void *arg) {
    cbm_mcp_server_t *srv = (cbm_mcp_server_t *)arg;

    cbm_log_info("autoindex.start", "project", srv->session_project, "path", srv->session_root);

    cbm_pipeline_t *p = cbm_pipeline_new(srv->session_root, NULL, CBM_MODE_FULL);
    if (!p) {
        cbm_log_warn("autoindex.err", "msg", "pipeline_create_failed");
        return NULL;
    }
    cbm_pipeline_apply_config(p, srv->config);

    /* Block until any concurrent pipeline finishes */
    cbm_pipeline_lock();
    int rc = cbm_pipeline_run(p);
    bool graph_changed = cbm_pipeline_graph_changed(p);
    cbm_pipeline_publish_kind_t publish_kind = cbm_pipeline_publish_kind(p);
    cbm_pipeline_unlock();

    cbm_pipeline_free(p);

    if (rc == 0) {
        /* Re-index dependencies after fresh dump */
        cbm_store_t *store = resolve_store(srv, srv->session_project);
        if (store) {
            int effective_dep_limit = cbm_mcp_effective_auto_dep_limit(srv, NULL);
            int deps_reindexed = cbm_mcp_auto_index_deps(
                srv, srv->session_project, srv->session_root, store,
                effective_dep_limit, NULL);
            (void)cbm_pagerank_refresh_after_publish(
                store, srv->session_project, srv->config, graph_changed, deps_reindexed,
                cbm_rank_refresh_publish_from_pipeline(publish_kind));
        }

        cbm_log_info("autoindex.done", "project", srv->session_project);
        notify_resources_updated(srv);
        if (srv->watcher) {
            cbm_watcher_mark_indexed(srv->watcher, srv->session_project, srv->session_root);
        }
    } else {
        cbm_log_warn("autoindex.err", "msg", "pipeline_run_failed");
    }
    cbm_mem_collect();
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
                /* Register watcher for live change detection */
                if (srv->watcher) {
                    cbm_watcher_watch(srv->watcher, srv->session_project, srv->session_root);
                }
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

/* Send notifications/resources/list_changed after index operations.
 * Per MCP spec: list_changed is for when the server's resource data changes
 * (we declared listChanged:true in capabilities). notifications/resources/updated
 * is only for per-resource subscriptions (we don't support subscribe). */
static void notify_resources_updated(cbm_mcp_server_t *srv) {
    if (srv->client_has_resources)
        send_notification(srv, "notifications/resources/list_changed");
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
    bool overlay_ready =
        proj && cbm_store_get_overlay_node_view_summary(store, proj, &overlay_summary) ==
                    CBM_STORE_OK &&
        overlay_summary.active_file_tombstones > 0;
    bool used_active_schema = false;
    bool active_schema_failed = false;

    cbm_schema_info_t schema = {0};
    if (overlay_ready &&
        cbm_store_get_schema_counts_overlay_view(store, proj, &schema) == CBM_STORE_OK) {
        used_active_schema = true;
    } else {
        active_schema_failed = overlay_ready;
        cbm_store_get_schema_counts(store, proj, &schema);
    }

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

static char *build_key_functions_sql(const char *exclude_csv,
                                     const char **exclude_arr, int limit) {
    char sql[4096];
    int pos = 0;
    pos += snprintf(sql + pos, sizeof(sql) - pos,
        "SELECT n.name, n.qualified_name, n.label, n.file_path, pr.rank "
        "FROM pagerank pr JOIN nodes n ON n.id = pr.node_id "
        "WHERE pr.project = ?1 "
        "AND n.label IN ('Function','Class','Method','Interface') ");

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
            "and routes; total_nodes, total_edges, key_functions, and relationship_patterns "
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
        char *sql = build_key_functions_sql(excl_csv, NULL, kf_limit);
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

    /* Relationship patterns from schema */
    cbm_schema_info_t schema = {0};
    cbm_store_get_schema(store, proj, &schema);
    if (schema.rel_pattern_count > 0) {
        yyjson_mut_val *rp_arr = yyjson_mut_arr(doc);
        for (int i = 0; i < schema.rel_pattern_count; i++) {
            yyjson_mut_arr_add_strcpy(doc, rp_arr, schema.rel_patterns[i]);
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
                                   int64_t req_id, char **err_out) {
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
        *err_out = cbm_jsonrpc_format_error(req_id, -32602, "Missing uri parameter");
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
        snprintf(msg, sizeof(msg),
            "Resource not found: '%s'. "
            "Available resources: codebase://schema, codebase://architecture, codebase://status. "
            "Use resources/list to discover all resources.",
            uri);
        free(uri);
        *err_out = cbm_jsonrpc_format_error(req_id, -32002, msg);
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
    if (cbm_jsonrpc_parse(line, &req) < 0) {
        CBM_PROF_END("mcp_request", "parse_error", prof_mcp_parse);
        CBM_PROF_END("mcp_request_total", "parse_error", prof_mcp_request_total);
        return cbm_jsonrpc_format_error(0, JSONRPC_PARSE_ERROR, "Parse error");
    }
    CBM_PROF_END("mcp_request", "parse", prof_mcp_parse);

    /* Notifications (no id) → handle cancellation, then no response */
    if (!req.has_id) {
        if (req.method && strcmp(req.method, "notifications/cancelled") == 0) {
            /* MCP cancellation: cancel the active pipeline if request ID matches */
            cbm_pipeline_t *active =
                atomic_load_explicit(&srv->active_pipeline, memory_order_acquire);
            if (active) {
                cbm_pipeline_cancel(active);
                cbm_log_info("mcp.cancelled", "request_id_active",
                             srv->active_request_id > 0 ? "yes" : "none");
            }
        }
        cbm_jsonrpc_request_free(&req);
        CBM_PROF_END("mcp_request_total", "notification", prof_mcp_request_total);
        return NULL;
    }

    char *result_json = NULL;

    if (strcmp(req.method, "initialize") == 0) {
        result_json = cbm_mcp_initialize_response(req.params_raw);
        start_update_check(srv);
        detect_session(srv);
        maybe_auto_index(srv);
    } else if (strcmp(req.method, "resources/list") == 0) {
        result_json = handle_resources_list(srv);
    } else if (strcmp(req.method, "resources/read") == 0) {
        /* handle_resources_read may return a pre-formatted JSON-RPC error (id=0).
         * Detect by checking for NULL result_json — errors are returned via err_out. */
        char *err_out = NULL;
        result_json = handle_resources_read(srv, req.params_raw, req.id, &err_out);
        if (err_out) {
            /* Error already formatted as JSON-RPC with correct id — return directly */
            CBM_PROF_END("mcp_request_total", req.method ? req.method : "unknown",
                         prof_mcp_request_total);
            cbm_jsonrpc_request_free(&req);
            return err_out;
        }
    } else if (strcmp(req.method, "ping") == 0) {
        result_json = heap_strdup("{}");
    } else if (strcmp(req.method, "tools/list") == 0) {
        result_json = cbm_mcp_tools_list(srv);
    } else if (strcmp(req.method, "tools/call") == 0) {
        CBM_PROF_START(prof_mcp_tool_params);
        char *tool_name = req.params_raw ? cbm_mcp_get_tool_name(req.params_raw) : NULL;
        char *tool_args =
            req.params_raw ? cbm_mcp_get_arguments(req.params_raw) : heap_strdup("{}");
        CBM_PROF_END("mcp_tool_call", "params", prof_mcp_tool_params);

        struct timespec t0;
        cbm_clock_gettime(CLOCK_MONOTONIC, &t0);
        CBM_PROF_START(prof_mcp_tool_execute);
        result_json = cbm_mcp_handle_tool(srv, tool_name, tool_args);
        CBM_PROF_END("mcp_tool_execute", tool_name ? tool_name : "missing_tool",
                     prof_mcp_tool_execute);
        struct timespec t1;
        cbm_clock_gettime(CLOCK_MONOTONIC, &t1);
        long long dur_us = ((long long)(t1.tv_sec - t0.tv_sec) * MCP_S_TO_US) +
                           ((long long)(t1.tv_nsec - t0.tv_nsec) / MCP_MS_TO_US);
        bool is_err = (result_json != NULL) && (strstr(result_json, "\"isError\":true") != NULL);
        cbm_diag_record_query(dur_us, is_err);

        CBM_PROF_START(prof_mcp_inject_notice);
        result_json = inject_update_notice(srv, result_json);
        CBM_PROF_END("mcp_tool_call", "inject_update_notice", prof_mcp_inject_notice);
        free(tool_name);
        free(tool_args);
    } else {
        /* Echo the original id (string or numeric, issue #253) on the error. */
        char err_obj[160];
        snprintf(err_obj, sizeof(err_obj), "{\"code\":%d,\"message\":\"Method not found\"}",
                 JSONRPC_METHOD_NOT_FOUND);
        cbm_jsonrpc_response_t err_resp = {
            .id = req.id,
            .id_str = req.id_str,
            .error_json = err_obj,
        };
        char *err = cbm_jsonrpc_format_response(&err_resp);
        CBM_PROF_END("mcp_request_total", req.method ? req.method : "unknown",
                     prof_mcp_request_total);
        cbm_jsonrpc_request_free(&req);
        return err;
    }

    cbm_jsonrpc_response_t resp = {
        .id = req.id,
        .id_str = req.id_str,
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
    }
}

#ifndef _WIN32
/* Unix 3-phase poll: non-blocking fd check, FILE* buffer peek, blocking poll.
 * Returns: 1 = data ready, 0 = timeout (evicted idle stores), -1 = error/EOF. */
static int poll_for_input_unix(cbm_mcp_server_t *srv, int fd, FILE *in) {
    struct pollfd pfd = {.fd = fd, .events = POLLIN};
    int idle_timeout_s = cbm_mcp_store_idle_timeout_s(srv);
    int poll_timeout_ms = idle_timeout_s * MCP_TIMEOUT_MS;
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
        /* fcntl failed — fall through to blocking poll */
        pr = poll(&pfd, SKIP_ONE, poll_timeout_ms);
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
        /* Phase 3: blocking poll */
        pr = poll(&pfd, SKIP_ONE, poll_timeout_ms);
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
