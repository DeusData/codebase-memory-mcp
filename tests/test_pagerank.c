/*
 * test_pagerank.c — Tests for PageRank (node) + LinkRank (edge) ranking.
 *
 * TDD: All tests written BEFORE implementation. They should fail (RED)
 * until the corresponding feature is implemented (GREEN).
 *
 * References:
 *   - igraph test suite: pagerank, multigraph, dangling, complete graph
 *   - NetworkX test suite: test_pagerank, test_dangling, test_empty
 *   - aider repomap: edge weights, file rank distribution
 *   - Kim et al. (2010) LinkRank: edge ranking formula
 */
#include "../src/foundation/compat.h"
#include "../src/foundation/constants.h"
#include "test_framework.h"
#include "test_helpers.h"
#include <store/store.h>
#include <pagerank/pagerank.h>
#include <depindex/depindex.h>
#include <cli/cli.h> /* cbm_config_open/set/get_double for with_config tuning */
#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <sqlite3.h>

/* ── Test helpers ──────────────────────────────────────────── */

static int64_t add_node(cbm_store_t *s, const char *project, const char *name) {
    cbm_node_t n = {0};
    n.project = project;
    n.label = "Function";
    n.name = name;
    n.qualified_name = name;
    n.file_path = "test.c";
    return cbm_store_upsert_node(s, &n);
}

static int64_t add_edge(cbm_store_t *s, const char *project,
                        int64_t src, int64_t dst, const char *type) {
    cbm_edge_t e = {0};
    e.project = project;
    e.source_id = src;
    e.target_id = dst;
    e.type = type;
    return cbm_store_insert_edge(s, &e);
}

static double get_pr(cbm_store_t *s, int64_t node_id) {
    return cbm_pagerank_get(s, node_id);
}

static int count_table_rows(cbm_store_t *s, const char *table) {
    sqlite3 *db = cbm_store_get_db(s);
    if (!db) return -1;
    char sql[CBM_LINE_BUF];
    snprintf(sql, sizeof(sql), "SELECT COUNT(*) FROM %s", table);
    sqlite3_stmt *stmt = NULL;
    int count = 0;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW) count = sqlite3_column_int(stmt, 0);
        sqlite3_finalize(stmt);
    }
    return count;
}

static int get_project_for_row(cbm_store_t *s, const char *table,
                               const char *id_column, int64_t id,
                               char *buf, size_t buf_sz) {
    sqlite3 *db = cbm_store_get_db(s);
    if (!db || !buf || buf_sz == 0) return 0;
    buf[0] = '\0';
    char sql[CBM_LINE_BUF];
    snprintf(sql, sizeof(sql), "SELECT project FROM %s WHERE %s = ?1",
             table, id_column);
    sqlite3_stmt *stmt = NULL;
    int found = 0;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_int64(stmt, 1, id);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            const char *project = (const char *)sqlite3_column_text(stmt, 0);
            snprintf(buf, buf_sz, "%s", project ? project : "");
            found = 1;
        }
        sqlite3_finalize(stmt);
    }
    return found;
}

static double get_lr_by_edge_id(cbm_store_t *s, int64_t edge_id) {
    return cbm_linkrank_get(s, edge_id);
}

static double get_linkrank_in_by_node_id(cbm_store_t *s, int64_t node_id) {
    sqlite3 *db = cbm_store_get_db(s);
    if (!db) return 0.0;
    sqlite3_stmt *stmt = NULL;
    double value = 0.0;
    if (sqlite3_prepare_v2(db,
                           "SELECT COALESCE(linkrank_in, 0.0) "
                           "FROM node_degree WHERE node_id = ?1",
                           -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_int64(stmt, 1, node_id);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            value = sqlite3_column_double(stmt, 0);
        }
        sqlite3_finalize(stmt);
    }
    return value;
}

/* ── 1. Core PageRank tests ──────────────────────────────── */

TEST(pagerank_empty_graph) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "empty", "/tmp/empty");
    int rc = cbm_pagerank_compute_default(s, "empty");
    ASSERT_EQ(rc, 0); /* 0 nodes ranked */
    ASSERT_EQ(count_table_rows(s, "pagerank"), 0);
    cbm_store_close(s);
    PASS();
}

TEST(pagerank_single_node) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "single", "/tmp/single");
    int64_t a = add_node(s, "single", "main");
    int rc = cbm_pagerank_compute_default(s, "single");
    ASSERT_EQ(rc, 1);
    double r = get_pr(s, a);
    ASSERT_TRUE(fabs(r - 1.0) < 0.01);
    cbm_store_close(s);
    PASS();
}

TEST(pagerank_two_nodes_one_edge) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "two", "/tmp/two");
    int64_t a = add_node(s, "two", "caller");
    int64_t b = add_node(s, "two", "callee");
    add_edge(s, "two", a, b, "CALLS");
    cbm_pagerank_compute_default(s, "two");
    double ra = get_pr(s, a);
    double rb = get_pr(s, b);
    ASSERT_TRUE(rb > ra); /* callee gets more rank */
    ASSERT_TRUE(fabs(ra + rb - 1.0) < 0.01);
    cbm_store_close(s);
    PASS();
}

TEST(pagerank_cycle) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "cyc", "/tmp/cyc");
    int64_t a = add_node(s, "cyc", "funcA");
    int64_t b = add_node(s, "cyc", "funcB");
    add_edge(s, "cyc", a, b, "CALLS");
    add_edge(s, "cyc", b, a, "CALLS");
    cbm_pagerank_compute_default(s, "cyc");
    double ra = get_pr(s, a);
    double rb = get_pr(s, b);
    ASSERT_TRUE(fabs(ra - rb) < 0.01); /* symmetric */
    cbm_store_close(s);
    PASS();
}

TEST(pagerank_star_topology) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "star", "/tmp/star");
    int64_t hub = add_node(s, "star", "hub");
    int64_t s1 = add_node(s, "star", "spoke1");
    int64_t s2 = add_node(s, "star", "spoke2");
    int64_t s3 = add_node(s, "star", "spoke3");
    add_edge(s, "star", s1, hub, "CALLS");
    add_edge(s, "star", s2, hub, "CALLS");
    add_edge(s, "star", s3, hub, "CALLS");
    cbm_pagerank_compute_default(s, "star");
    ASSERT_TRUE(get_pr(s, hub) > get_pr(s, s1));
    cbm_store_close(s);
    PASS();
}

TEST(pagerank_edge_weights) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "wt", "/tmp/wt");
    int64_t a = add_node(s, "wt", "source");
    int64_t b = add_node(s, "wt", "called");
    int64_t c = add_node(s, "wt", "used");
    add_edge(s, "wt", a, b, "CALLS");  /* weight 1.0 */
    add_edge(s, "wt", a, c, "USAGE");  /* weight 0.2 */
    cbm_pagerank_compute_default(s, "wt");
    ASSERT_TRUE(get_pr(s, b) > get_pr(s, c));
    cbm_store_close(s);
    PASS();
}

TEST(pagerank_convergence) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "chain", "/tmp/chain");
    int64_t ids[5];
    for (int i = 0; i < 5; i++) {
        char name[8]; snprintf(name, sizeof(name), "n%d", i);
        ids[i] = add_node(s, "chain", name);
    }
    for (int i = 0; i < 4; i++) add_edge(s, "chain", ids[i], ids[i+1], "CALLS");
    int rc = cbm_pagerank_compute_default(s, "chain");
    ASSERT_EQ(rc, 5);
    ASSERT_TRUE(get_pr(s, ids[4]) > get_pr(s, ids[0]));
    cbm_store_close(s);
    PASS();
}

TEST(pagerank_sum_to_one) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "sum", "/tmp/sum");
    int64_t a = add_node(s, "sum", "a");
    int64_t b = add_node(s, "sum", "b");
    int64_t c = add_node(s, "sum", "c");
    add_edge(s, "sum", a, b, "CALLS");
    add_edge(s, "sum", b, c, "CALLS");
    add_edge(s, "sum", c, a, "CALLS");
    cbm_pagerank_compute_default(s, "sum");
    double total = get_pr(s, a) + get_pr(s, b) + get_pr(s, c);
    ASSERT_TRUE(fabs(total - 1.0) < 0.05);
    cbm_store_close(s);
    PASS();
}

TEST(pagerank_stored_in_db) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "db", "/tmp/db");
    add_node(s, "db", "f1");
    add_node(s, "db", "f2");
    cbm_pagerank_compute_default(s, "db");
    ASSERT_EQ(count_table_rows(s, "pagerank"), 2);

    cbm_derived_view_state_t state = {0};
    ASSERT_EQ(cbm_store_get_derived_view_state(s, "db", CBM_STORE_DERIVED_VIEW_PAGERANK,
                                               &state),
              CBM_STORE_OK);
    ASSERT_STR_EQ(state.status, CBM_STORE_DERIVED_STATUS_COMPLETE);
    ASSERT_EQ(state.source_generation, CBM_STORE_DERIVED_GENERATION_UNKNOWN);
    cbm_store_derived_view_state_free_fields(&state);

    ASSERT_EQ(cbm_store_get_derived_view_state(s, "db", CBM_STORE_DERIVED_VIEW_LINKRANK,
                                               &state),
              CBM_STORE_OK);
    ASSERT_STR_EQ(state.status, CBM_STORE_DERIVED_STATUS_COMPLETE);
    cbm_store_derived_view_state_free_fields(&state);

    ASSERT_EQ(cbm_store_get_derived_view_state(s, "db", CBM_STORE_DERIVED_VIEW_NODE_DEGREE,
                                               &state),
              CBM_STORE_OK);
    ASSERT_STR_EQ(state.status, CBM_STORE_DERIVED_STATUS_COMPLETE);
    cbm_store_derived_view_state_free_fields(&state);

    cbm_store_close(s);
    PASS();
}

TEST(pagerank_views_complete_requires_all_rank_views) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "fresh", "/tmp/fresh");
    add_node(s, "fresh", "f1");
    add_node(s, "fresh", "f2");

    ASSERT_FALSE(cbm_pagerank_views_complete(s, "fresh"));
    cbm_pagerank_compute_default(s, "fresh");
    ASSERT_TRUE(cbm_pagerank_views_complete(s, "fresh"));

    ASSERT_EQ(cbm_store_set_derived_view_state(s, "fresh", CBM_STORE_DERIVED_VIEW_LINKRANK,
                                               CBM_STORE_DERIVED_GENERATION_UNKNOWN,
                                               CBM_STORE_DERIVED_STATUS_STALE),
              CBM_STORE_OK);
    ASSERT_FALSE(cbm_pagerank_views_complete(s, "fresh"));

    cbm_store_close(s);
    PASS();
}

TEST(pagerank_refresh_if_needed_repairs_missing_views) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "refresh_missing", "/tmp/refresh_missing");
    int64_t a = add_node(s, "refresh_missing", "a");
    int64_t b = add_node(s, "refresh_missing", "b");
    add_edge(s, "refresh_missing", a, b, "CALLS");

    ASSERT_FALSE(cbm_pagerank_views_complete(s, "refresh_missing"));
    ASSERT_EQ(cbm_pagerank_refresh_if_needed(s, "refresh_missing", NULL, false, 0, false), 2);
    ASSERT_TRUE(cbm_pagerank_views_complete(s, "refresh_missing"));
    ASSERT_EQ(count_table_rows(s, "pagerank"), 2);

    cbm_store_close(s);
    PASS();
}

TEST(pagerank_refresh_if_needed_skips_complete_unchanged_graph) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "refresh_skip", "/tmp/refresh_skip");
    int64_t a = add_node(s, "refresh_skip", "a");
    int64_t b = add_node(s, "refresh_skip", "b");
    add_edge(s, "refresh_skip", a, b, "CALLS");

    ASSERT_EQ(cbm_pagerank_compute_default(s, "refresh_skip"), 2);
    ASSERT_EQ(cbm_pagerank_refresh_if_needed(s, "refresh_skip", NULL, false, 0, false), 0);
    ASSERT_EQ(count_table_rows(s, "pagerank"), 2);

    cbm_store_close(s);
    PASS();
}

TEST(pagerank_refresh_if_needed_recomputes_changed_graph) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "refresh_changed", "/tmp/refresh_changed");
    int64_t a = add_node(s, "refresh_changed", "a");
    ASSERT_EQ(cbm_pagerank_compute_default(s, "refresh_changed"), 1);
    int64_t b = add_node(s, "refresh_changed", "b");
    add_edge(s, "refresh_changed", a, b, "CALLS");

    ASSERT_EQ(cbm_pagerank_refresh_if_needed(s, "refresh_changed", NULL, true, 0, false), 2);
    ASSERT_EQ(count_table_rows(s, "pagerank"), 2);

    cbm_store_close(s);
    PASS();
}

TEST(pagerank_refresh_if_needed_recomputes_reindexed_deps) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "refresh_deps", "/tmp/refresh_deps");
    int64_t app = add_node(s, "refresh_deps", "app");
    ASSERT_EQ(cbm_pagerank_compute_default(s, "refresh_deps"), 1);

    cbm_store_upsert_project(s, "refresh_deps.dep.lib", "/tmp/refresh_dep_lib");
    int64_t dep = add_node(s, "refresh_deps.dep.lib", "dep");
    add_edge(s, "refresh_deps", app, dep, "CALLS");

    ASSERT_EQ(cbm_pagerank_refresh_if_needed(s, "refresh_deps", NULL, false, 1, false), 2);
    ASSERT_TRUE(get_pr(s, dep) > 0.0);

    cbm_store_close(s);
    PASS();
}

TEST(pagerank_disabled_config_clears_rank_views) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "rank_disabled", "/tmp/rank_disabled");
    int64_t a = add_node(s, "rank_disabled", "a");
    int64_t b = add_node(s, "rank_disabled", "b");
    add_edge(s, "rank_disabled", a, b, "CALLS");
    ASSERT_EQ(cbm_pagerank_compute_default(s, "rank_disabled"), 2);
    ASSERT_TRUE(count_table_rows(s, "pagerank") > 0);
    ASSERT_TRUE(count_table_rows(s, "linkrank") > 0);
    ASSERT_TRUE(count_table_rows(s, "node_degree") > 0);

    char tmpdir[CBM_PATH_MAX];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/pr-disabled-XXXXXX");
    ASSERT_TRUE(cbm_mkdtemp(tmpdir) != NULL);
    cbm_config_t *cfg = cbm_config_open(tmpdir);
    ASSERT_NOT_NULL(cfg);
    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_RANK_ENABLED, "false"), 0);
    ASSERT_EQ(cbm_pagerank_refresh_if_needed(s, "rank_disabled", cfg, true, 0, false), 0);
    ASSERT_EQ(count_table_rows(s, "pagerank"), 0);
    ASSERT_EQ(count_table_rows(s, "linkrank"), 0);
    ASSERT_EQ(count_table_rows(s, "node_degree"), 0);
    ASSERT_FALSE(cbm_pagerank_views_complete(s, "rank_disabled"));

    cbm_config_close(cfg);
    th_rmtree(tmpdir);
    cbm_store_close(s);
    PASS();
}

TEST(pagerank_refresh_defer_exact_delta_reindexes_defers_only_with_stale_rank_views) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "refresh_policy", "/tmp/refresh_policy");
    int64_t a = add_node(s, "refresh_policy", "a");
    int64_t b = add_node(s, "refresh_policy", "b");
    add_edge(s, "refresh_policy", a, b, "CALLS");

    char tmpdir[CBM_PATH_MAX];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/pr-refresh-XXXXXX");
    ASSERT_TRUE(cbm_mkdtemp(tmpdir) != NULL);
    cbm_config_t *cfg = cbm_config_open(tmpdir);
    ASSERT_NOT_NULL(cfg);
    ASSERT_EQ(
        cbm_config_set(cfg, CBM_CONFIG_RANK_REFRESH, CBM_RANK_REFRESH_DEFER_EXACT_DELTA_REINDEXES),
        0);

    ASSERT_EQ(cbm_pagerank_compute_default(s, "refresh_policy"), 2);
    int64_t c = add_node(s, "refresh_policy", "c");
    add_edge(s, "refresh_policy", b, c, "CALLS");
    const char *rank_views[] = {CBM_STORE_DERIVED_VIEW_PAGERANK,
                                CBM_STORE_DERIVED_VIEW_LINKRANK,
                                CBM_STORE_DERIVED_VIEW_NODE_DEGREE};
    int rank_view_count = (int)(sizeof(rank_views) / sizeof(rank_views[0]));
    ASSERT_EQ(cbm_store_mark_derived_views_stale(s, "refresh_policy",
                                                 CBM_STORE_DERIVED_GENERATION_UNKNOWN,
                                                 rank_views, rank_view_count),
              CBM_STORE_OK);

    ASSERT_EQ(cbm_pagerank_refresh_if_needed(s, "refresh_policy", cfg, true, 0, true), 0);
    ASSERT_FALSE(cbm_pagerank_views_complete(s, "refresh_policy"));
    ASSERT_EQ(count_table_rows(s, "pagerank"), 2);

    ASSERT_EQ(cbm_pagerank_refresh_if_needed(s, "refresh_policy", cfg, true, 0, false), 3);
    ASSERT_TRUE(cbm_pagerank_views_complete(s, "refresh_policy"));
    ASSERT_EQ(count_table_rows(s, "pagerank"), 3);

    cbm_config_close(cfg);
    th_rmtree(tmpdir);
    cbm_store_close(s);
    PASS();
}

TEST(pagerank_refresh_defer_exact_delta_reindexes_does_not_defer_containment) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "refresh_exact_only", "/tmp/refresh_exact_only");
    int64_t a = add_node(s, "refresh_exact_only", "a");
    int64_t b = add_node(s, "refresh_exact_only", "b");
    add_edge(s, "refresh_exact_only", a, b, "CALLS");

    char tmpdir[CBM_PATH_MAX];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/pr-refresh-exact-only-XXXXXX");
    ASSERT_TRUE(cbm_mkdtemp(tmpdir) != NULL);
    cbm_config_t *cfg = cbm_config_open(tmpdir);
    ASSERT_NOT_NULL(cfg);
    ASSERT_EQ(
        cbm_config_set(cfg, CBM_CONFIG_RANK_REFRESH, CBM_RANK_REFRESH_DEFER_EXACT_DELTA_REINDEXES),
        0);

    ASSERT_EQ(cbm_pagerank_compute_default(s, "refresh_exact_only"), 2);
    int64_t c = add_node(s, "refresh_exact_only", "c");
    add_edge(s, "refresh_exact_only", b, c, "CALLS");
    ASSERT_EQ(cbm_store_mark_rank_derived_views_stale(
                  s, "refresh_exact_only", CBM_STORE_DERIVED_GENERATION_UNKNOWN),
              CBM_STORE_OK);

    ASSERT_EQ(cbm_pagerank_refresh_after_publish(
                  s, "refresh_exact_only", cfg, true, 0,
                  CBM_RANK_REFRESH_PUBLISH_INCREMENTAL_CONTAINMENT),
              3);
    ASSERT_TRUE(cbm_pagerank_views_complete(s, "refresh_exact_only"));
    ASSERT_EQ(count_table_rows(s, "pagerank"), 3);

    cbm_config_close(cfg);
    th_rmtree(tmpdir);
    cbm_store_close(s);
    PASS();
}

TEST(pagerank_refresh_defer_all_incremental_reindexes_defers_containment) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "refresh_incremental", "/tmp/refresh_incremental");
    int64_t a = add_node(s, "refresh_incremental", "a");
    int64_t b = add_node(s, "refresh_incremental", "b");
    add_edge(s, "refresh_incremental", a, b, "CALLS");

    char tmpdir[CBM_PATH_MAX];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/pr-refresh-incremental-XXXXXX");
    ASSERT_TRUE(cbm_mkdtemp(tmpdir) != NULL);
    cbm_config_t *cfg = cbm_config_open(tmpdir);
    ASSERT_NOT_NULL(cfg);
    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_RANK_REFRESH,
                             CBM_RANK_REFRESH_DEFER_ALL_INCREMENTAL_REINDEXES),
              0);

    ASSERT_EQ(cbm_pagerank_compute_default(s, "refresh_incremental"), 2);
    int64_t c = add_node(s, "refresh_incremental", "c");
    add_edge(s, "refresh_incremental", b, c, "CALLS");
    ASSERT_EQ(cbm_store_mark_rank_derived_views_stale(
                  s, "refresh_incremental", CBM_STORE_DERIVED_GENERATION_UNKNOWN),
              CBM_STORE_OK);

    ASSERT_EQ(cbm_pagerank_refresh_after_publish(
                  s, "refresh_incremental", cfg, true, 0,
                  CBM_RANK_REFRESH_PUBLISH_INCREMENTAL_CONTAINMENT),
              0);
    ASSERT_FALSE(cbm_pagerank_views_complete(s, "refresh_incremental"));
    ASSERT_EQ(count_table_rows(s, "pagerank"), 2);

    cbm_config_close(cfg);
    th_rmtree(tmpdir);
    cbm_store_close(s);
    PASS();
}

TEST(pagerank_refresh_defer_all_incremental_reindexes_defers_full_fallback) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "refresh_fallback", "/tmp/refresh_fallback");
    int64_t a = add_node(s, "refresh_fallback", "a");
    int64_t b = add_node(s, "refresh_fallback", "b");
    add_edge(s, "refresh_fallback", a, b, "CALLS");

    char tmpdir[CBM_PATH_MAX];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/pr-refresh-fallback-XXXXXX");
    ASSERT_TRUE(cbm_mkdtemp(tmpdir) != NULL);
    cbm_config_t *cfg = cbm_config_open(tmpdir);
    ASSERT_NOT_NULL(cfg);

    ASSERT_EQ(cbm_rank_refresh_publish_from_pipeline(CBM_PIPELINE_PUBLISH_FULL, false),
              CBM_RANK_REFRESH_PUBLISH_FULL);
    ASSERT_EQ(cbm_rank_refresh_publish_from_pipeline(CBM_PIPELINE_PUBLISH_FULL, true),
              CBM_RANK_REFRESH_PUBLISH_INCREMENTAL_FALLBACK);

    ASSERT_EQ(cbm_pagerank_compute_default(s, "refresh_fallback"), 2);
    int64_t c = add_node(s, "refresh_fallback", "c");
    add_edge(s, "refresh_fallback", b, c, "CALLS");
    ASSERT_EQ(cbm_store_mark_rank_derived_views_stale(
                  s, "refresh_fallback", CBM_STORE_DERIVED_GENERATION_UNKNOWN),
              CBM_STORE_OK);

    ASSERT_EQ(
        cbm_config_set(cfg, CBM_CONFIG_RANK_REFRESH, CBM_RANK_REFRESH_DEFER_EXACT_DELTA_REINDEXES),
        0);
    ASSERT_EQ(cbm_pagerank_refresh_after_publish(
                  s, "refresh_fallback", cfg, true, 0,
                  CBM_RANK_REFRESH_PUBLISH_INCREMENTAL_FALLBACK),
              3);
    ASSERT_TRUE(cbm_pagerank_views_complete(s, "refresh_fallback"));

    int64_t d = add_node(s, "refresh_fallback", "d");
    add_edge(s, "refresh_fallback", c, d, "CALLS");
    ASSERT_EQ(cbm_store_mark_rank_derived_views_stale(
                  s, "refresh_fallback", CBM_STORE_DERIVED_GENERATION_UNKNOWN),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_RANK_REFRESH,
                             CBM_RANK_REFRESH_DEFER_ALL_INCREMENTAL_REINDEXES),
              0);
    ASSERT_EQ(cbm_pagerank_refresh_after_publish(
                  s, "refresh_fallback", cfg, true, 0,
                  CBM_RANK_REFRESH_PUBLISH_INCREMENTAL_FALLBACK),
              0);
    ASSERT_FALSE(cbm_pagerank_views_complete(s, "refresh_fallback"));
    ASSERT_EQ(count_table_rows(s, "pagerank"), 3);

    cbm_config_close(cfg);
    th_rmtree(tmpdir);
    cbm_store_close(s);
    PASS();
}

TEST(pagerank_refresh_default_defers_incremental_when_rank_views_stale) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "refresh_default", "/tmp/refresh_default");
    int64_t a = add_node(s, "refresh_default", "a");
    int64_t b = add_node(s, "refresh_default", "b");
    add_edge(s, "refresh_default", a, b, "CALLS");

    ASSERT_EQ(cbm_pagerank_compute_default(s, "refresh_default"), 2);
    int64_t c = add_node(s, "refresh_default", "c");
    add_edge(s, "refresh_default", b, c, "CALLS");
    ASSERT_EQ(cbm_store_mark_rank_derived_views_stale(
                  s, "refresh_default", CBM_STORE_DERIVED_GENERATION_UNKNOWN),
              CBM_STORE_OK);

    ASSERT_EQ(cbm_pagerank_refresh_after_publish(
                  s, "refresh_default", NULL, true, 0,
                  CBM_RANK_REFRESH_PUBLISH_INCREMENTAL_CONTAINMENT),
              0);
    ASSERT_FALSE(cbm_pagerank_views_complete(s, "refresh_default"));
    ASSERT_EQ(count_table_rows(s, "pagerank"), 2);

    ASSERT_EQ(cbm_pagerank_refresh_after_publish(
                  s, "refresh_default", NULL, true, 1,
                  CBM_RANK_REFRESH_PUBLISH_INCREMENTAL_CONTAINMENT),
              3);
    ASSERT_TRUE(cbm_pagerank_views_complete(s, "refresh_default"));
    ASSERT_EQ(count_table_rows(s, "pagerank"), 3);

    cbm_store_close(s);
    PASS();
}

TEST(pagerank_refresh_invalid_policy_falls_back_to_at_publish) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "refresh_invalid_policy", "/tmp/refresh_invalid_policy");
    int64_t a = add_node(s, "refresh_invalid_policy", "a");
    int64_t b = add_node(s, "refresh_invalid_policy", "b");
    add_edge(s, "refresh_invalid_policy", a, b, "CALLS");

    char tmpdir[CBM_PATH_MAX];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/pr-refresh-bad-XXXXXX");
    ASSERT_TRUE(cbm_mkdtemp(tmpdir) != NULL);
    cbm_config_t *cfg = cbm_config_open(tmpdir);
    ASSERT_NOT_NULL(cfg);
    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_RANK_REFRESH, "bogus"), -1);
    ASSERT_EQ(th_set_raw_config_value(tmpdir, CBM_CONFIG_RANK_REFRESH, "bogus"), 0);

    const char *rank_views[] = {CBM_STORE_DERIVED_VIEW_PAGERANK,
                                CBM_STORE_DERIVED_VIEW_LINKRANK,
                                CBM_STORE_DERIVED_VIEW_NODE_DEGREE};
    int rank_view_count = (int)(sizeof(rank_views) / sizeof(rank_views[0]));
    ASSERT_EQ(cbm_store_mark_derived_views_stale(s, "refresh_invalid_policy",
                                                 CBM_STORE_DERIVED_GENERATION_UNKNOWN,
                                                 rank_views, rank_view_count),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_pagerank_refresh_if_needed(s, "refresh_invalid_policy", cfg, true, 0, true), 2);
    ASSERT_TRUE(cbm_pagerank_views_complete(s, "refresh_invalid_policy"));

    cbm_config_close(cfg);
    th_rmtree(tmpdir);
    cbm_store_close(s);
    PASS();
}

TEST(pagerank_recompute_replaces) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "re", "/tmp/re");
    int64_t a = add_node(s, "re", "f1");
    cbm_pagerank_compute_default(s, "re");
    double r1 = get_pr(s, a);
    cbm_pagerank_compute_default(s, "re");
    ASSERT_EQ(count_table_rows(s, "pagerank"), 1);
    double r2 = get_pr(s, a);
    ASSERT_TRUE(fabs(r1 - r2) < 0.001);
    cbm_store_close(s);
    PASS();
}

TEST(pagerank_full_scope_includes_deps) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "proj", "/tmp/proj");
    cbm_store_upsert_project(s, "proj.dep.lib", "/tmp/lib");
    int64_t a = add_node(s, "proj", "app_main");
    int64_t b = add_node(s, "proj.dep.lib", "lib_func");
    add_edge(s, "proj", a, b, "CALLS");
    int rc = cbm_pagerank_compute(s, "proj", CBM_PAGERANK_DAMPING,
                                  CBM_PAGERANK_EPSILON, CBM_PAGERANK_MAX_ITER,
                                  &CBM_DEFAULT_EDGE_WEIGHTS, CBM_RANK_SCOPE_FULL);
    ASSERT_EQ(rc, 2);
    ASSERT_TRUE(get_pr(s, b) > 0.0);
    cbm_store_close(s);
    PASS();
}

TEST(pagerank_full_scope_preserves_dep_project_attribution) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "proj_attr", "/tmp/proj_attr");
    cbm_store_upsert_project(s, "proj_attr.dep.lib", "/tmp/lib_attr");
    int64_t app = add_node(s, "proj_attr", "app_main");
    int64_t dep_a = add_node(s, "proj_attr.dep.lib", "lib_a");
    int64_t dep_b = add_node(s, "proj_attr.dep.lib", "lib_b");
    add_edge(s, "proj_attr", app, dep_a, "CALLS");
    int64_t dep_edge = add_edge(s, "proj_attr.dep.lib", dep_a, dep_b, "CALLS");

    int rc = cbm_pagerank_compute(s, "proj_attr", CBM_PAGERANK_DAMPING,
                                  CBM_PAGERANK_EPSILON, CBM_PAGERANK_MAX_ITER,
                                  &CBM_DEFAULT_EDGE_WEIGHTS, CBM_RANK_SCOPE_FULL);
    ASSERT_EQ(rc, 3);

    char project_buf[CBM_PATH_MAX];
    ASSERT_TRUE(get_project_for_row(s, "pagerank", "node_id", dep_b,
                                    project_buf, sizeof(project_buf)));
    ASSERT_TRUE(strcmp(project_buf, "proj_attr.dep.lib") == 0);
    ASSERT_TRUE(get_project_for_row(s, "node_degree", "node_id", dep_b,
                                    project_buf, sizeof(project_buf)));
    ASSERT_TRUE(strcmp(project_buf, "proj_attr.dep.lib") == 0);
    ASSERT_TRUE(get_project_for_row(s, "linkrank", "edge_id", dep_edge,
                                    project_buf, sizeof(project_buf)));
    ASSERT_TRUE(strcmp(project_buf, "proj_attr.dep.lib") == 0);

    cbm_store_close(s);
    PASS();
}

TEST(pagerank_project_scope_excludes_deps) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "proj2", "/tmp/proj2");
    cbm_store_upsert_project(s, "proj2.dep.lib", "/tmp/lib2");
    add_node(s, "proj2", "my_func");
    int64_t dep = add_node(s, "proj2.dep.lib", "lib_func");
    int rc = cbm_pagerank_compute(s, "proj2", CBM_PAGERANK_DAMPING,
                                  CBM_PAGERANK_EPSILON, CBM_PAGERANK_MAX_ITER,
                                  &CBM_DEFAULT_EDGE_WEIGHTS, CBM_RANK_SCOPE_PROJECT);
    ASSERT_EQ(rc, 1);
    ASSERT_TRUE(get_pr(s, dep) == 0.0);
    cbm_store_close(s);
    PASS();
}

TEST(pagerank_rank_scope_config_controls_scope_and_clears_stale_rows) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "cfgscope", "/tmp/cfgscope");
    cbm_store_upsert_project(s, "cfgscope.dep.lib", "/tmp/lib");
    int64_t app = add_node(s, "cfgscope", "app_main");
    int64_t dep = add_node(s, "cfgscope.dep.lib", "lib_func");
    add_edge(s, "cfgscope", app, dep, "CALLS");

    char tmpdir[CBM_PATH_MAX];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/pr-scope-XXXXXX");
    ASSERT_TRUE(cbm_mkdtemp(tmpdir) != NULL);
    cbm_config_t *cfg = cbm_config_open(tmpdir);
    ASSERT_NOT_NULL(cfg);

    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_RANK_SCOPE, "full"), 0);
    ASSERT_EQ(cbm_pagerank_compute_with_config(s, "cfgscope", cfg), 2);
    ASSERT_TRUE(get_pr(s, dep) > 0.0);

    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_RANK_SCOPE, "project"), 0);
    ASSERT_EQ(cbm_pagerank_compute_with_config(s, "cfgscope", cfg), 1);
    ASSERT_TRUE(get_pr(s, app) > 0.0);
    ASSERT_TRUE(get_pr(s, dep) == 0.0);

    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_RANK_SCOPE, "invalid"), -1);
    ASSERT_EQ(th_set_raw_config_value(tmpdir, CBM_CONFIG_RANK_SCOPE, "invalid"), 0);
    ASSERT_EQ(cbm_pagerank_compute_with_config(s, "cfgscope", cfg), 2);
    ASSERT_TRUE(get_pr(s, dep) > 0.0);

    cbm_config_close(cfg);
    th_rmtree(tmpdir);
    cbm_store_close(s);
    PASS();
}

TEST(pagerank_dangling_nodes) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "dang", "/tmp/dang");
    int64_t a = add_node(s, "dang", "caller");
    int64_t b = add_node(s, "dang", "leaf");
    add_edge(s, "dang", a, b, "CALLS");
    cbm_pagerank_compute_default(s, "dang");
    ASSERT_TRUE(get_pr(s, b) > 0.0);
    double total = get_pr(s, a) + get_pr(s, b);
    ASSERT_TRUE(fabs(total - 1.0) < 0.05);
    cbm_store_close(s);
    PASS();
}

TEST(pagerank_null_safety) {
    ASSERT_EQ(cbm_pagerank_compute_default(NULL, "x"), -1);
    ASSERT_EQ(cbm_pagerank_compute_default(NULL, NULL), -1);
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_EQ(cbm_pagerank_compute_default(s, NULL), -1);
    ASSERT_EQ(cbm_pagerank_compute_default(s, ""), -1);
    cbm_store_close(s);
    PASS();
}

/* ── 2. Edge cases from igraph/NetworkX ──────────────────── */

TEST(pagerank_self_loop) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "self", "/tmp/self");
    int64_t a = add_node(s, "self", "recursive");
    add_edge(s, "self", a, a, "CALLS");
    int rc = cbm_pagerank_compute_default(s, "self");
    ASSERT_EQ(rc, 1);
    ASSERT_TRUE(fabs(get_pr(s, a) - 1.0) < 0.01);
    cbm_store_close(s);
    PASS();
}

TEST(pagerank_disconnected_components) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "disc", "/tmp/disc");
    int64_t a = add_node(s, "disc", "a");
    int64_t b = add_node(s, "disc", "b");
    int64_t c = add_node(s, "disc", "c");
    int64_t d = add_node(s, "disc", "d");
    add_edge(s, "disc", a, b, "CALLS");
    add_edge(s, "disc", c, d, "CALLS");
    cbm_pagerank_compute_default(s, "disc");
    double total = get_pr(s, a) + get_pr(s, b) + get_pr(s, c) + get_pr(s, d);
    ASSERT_TRUE(fabs(total - 1.0) < 0.05);
    double comp1 = get_pr(s, a) + get_pr(s, b);
    double comp2 = get_pr(s, c) + get_pr(s, d);
    ASSERT_TRUE(fabs(comp1 - comp2) < 0.15);
    cbm_store_close(s);
    PASS();
}

TEST(pagerank_all_dangling_no_edges) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "noedge", "/tmp/noedge");
    int64_t ids[5];
    for (int i = 0; i < 5; i++) {
        char name[16]; snprintf(name, sizeof(name), "n%d", i);
        ids[i] = add_node(s, "noedge", name);
    }
    int rc = cbm_pagerank_compute_default(s, "noedge");
    ASSERT_EQ(rc, 5);
    double expected = 1.0 / 5.0;
    for (int i = 0; i < 5; i++)
        ASSERT_TRUE(fabs(get_pr(s, ids[i]) - expected) < 0.01);
    cbm_store_close(s);
    PASS();
}

TEST(pagerank_complete_graph) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "kn", "/tmp/kn");
    int64_t ids[4];
    for (int i = 0; i < 4; i++) {
        char name[8]; snprintf(name, sizeof(name), "k%d", i);
        ids[i] = add_node(s, "kn", name);
    }
    for (int i = 0; i < 4; i++)
        for (int j = 0; j < 4; j++)
            if (i != j) add_edge(s, "kn", ids[i], ids[j], "CALLS");
    cbm_pagerank_compute_default(s, "kn");
    for (int i = 0; i < 4; i++)
        ASSERT_TRUE(fabs(get_pr(s, ids[i]) - 0.25) < 0.01);
    cbm_store_close(s);
    PASS();
}

TEST(pagerank_multigraph_edges) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "multi", "/tmp/multi");
    int64_t a = add_node(s, "multi", "caller");
    int64_t b = add_node(s, "multi", "callee");
    add_edge(s, "multi", a, b, "CALLS");
    add_edge(s, "multi", a, b, "IMPORTS");
    cbm_pagerank_compute_default(s, "multi");
    ASSERT_TRUE(get_pr(s, b) > get_pr(s, a));
    cbm_store_close(s);
    PASS();
}

TEST(pagerank_large_graph_stability) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "big", "/tmp/big");
    int64_t ids[100];
    for (int i = 0; i < 100; i++) {
        char name[16]; snprintf(name, sizeof(name), "f%d", i);
        ids[i] = add_node(s, "big", name);
    }
    for (int i = 0; i < 99; i++)
        add_edge(s, "big", ids[i], ids[i+1], "CALLS");
    int rc = cbm_pagerank_compute_default(s, "big");
    ASSERT_EQ(rc, 100);
    double total = 0.0;
    for (int i = 0; i < 100; i++) total += get_pr(s, ids[i]);
    ASSERT_TRUE(fabs(total - 1.0) < 0.05);
    ASSERT_TRUE(get_pr(s, ids[99]) > get_pr(s, ids[0]));
    cbm_store_close(s);
    PASS();
}

TEST(pagerank_zero_weight_edges) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "zw", "/tmp/zw");
    int64_t a = add_node(s, "zw", "a");
    int64_t b = add_node(s, "zw", "b");
    add_edge(s, "zw", a, b, "CONFIGURES");
    cbm_edge_weights_t zero_w = CBM_DEFAULT_EDGE_WEIGHTS;
    zero_w.configures = 0.0;
    cbm_pagerank_compute(s, "zw", CBM_PAGERANK_DAMPING, CBM_PAGERANK_EPSILON,
                         CBM_PAGERANK_MAX_ITER, &zero_w, CBM_RANK_SCOPE_FULL);
    ASSERT_TRUE(fabs(get_pr(s, a) - get_pr(s, b)) < 0.01);
    cbm_store_close(s);
    PASS();
}

TEST(pagerank_custom_damping_high) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "hi_d", "/tmp/hi_d");
    int64_t a = add_node(s, "hi_d", "a");
    int64_t b = add_node(s, "hi_d", "b");
    add_edge(s, "hi_d", a, b, "CALLS");
    cbm_pagerank_compute(s, "hi_d", 0.99, CBM_PAGERANK_EPSILON,
                         50, &CBM_DEFAULT_EDGE_WEIGHTS, CBM_RANK_SCOPE_FULL);
    double total = get_pr(s, a) + get_pr(s, b);
    ASSERT_TRUE(fabs(total - 1.0) < 0.05);
    ASSERT_TRUE(get_pr(s, b) > get_pr(s, a));
    cbm_store_close(s);
    PASS();
}

TEST(pagerank_custom_damping_low) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "lo_d", "/tmp/lo_d");
    int64_t a = add_node(s, "lo_d", "a");
    int64_t b = add_node(s, "lo_d", "b");
    add_edge(s, "lo_d", a, b, "CALLS");
    cbm_pagerank_compute(s, "lo_d", 0.1, CBM_PAGERANK_EPSILON,
                         CBM_PAGERANK_MAX_ITER, &CBM_DEFAULT_EDGE_WEIGHTS,
                         CBM_RANK_SCOPE_FULL);
    ASSERT_TRUE(fabs(get_pr(s, a) - get_pr(s, b)) < 0.1);
    cbm_store_close(s);
    PASS();
}

TEST(pagerank_max_iter_zero) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "mi0", "/tmp/mi0");
    add_node(s, "mi0", "a");
    add_node(s, "mi0", "b");
    add_edge(s, "mi0", 1, 2, "CALLS");
    /* max_iter <= 0 resets to default */
    int rc = cbm_pagerank_compute(s, "mi0", CBM_PAGERANK_DAMPING,
                                  CBM_PAGERANK_EPSILON, 0,
                                  &CBM_DEFAULT_EDGE_WEIGHTS, CBM_RANK_SCOPE_FULL);
    ASSERT_TRUE(rc > 0);
    cbm_store_close(s);
    PASS();
}

TEST(pagerank_known_values) {
    /* 3-node cycle: all should get equal rank 1/3 */
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "kv", "/tmp/kv");
    int64_t a = add_node(s, "kv", "a");
    int64_t b = add_node(s, "kv", "b");
    int64_t c = add_node(s, "kv", "c");
    add_edge(s, "kv", a, b, "CALLS");
    add_edge(s, "kv", b, c, "CALLS");
    add_edge(s, "kv", c, a, "CALLS");
    cbm_pagerank_compute_default(s, "kv");
    double expected = 1.0 / 3.0;
    ASSERT_TRUE(fabs(get_pr(s, a) - expected) < 0.01);
    ASSERT_TRUE(fabs(get_pr(s, b) - expected) < 0.01);
    ASSERT_TRUE(fabs(get_pr(s, c) - expected) < 0.01);
    cbm_store_close(s);
    PASS();
}

TEST(pagerank_known_values_asymmetric) {
    /* NetworkX test graph: 6 nodes, node 4 highest rank */
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "nx", "/tmp/nx");
    int64_t n[7];
    for (int i = 1; i <= 6; i++) {
        char name[8]; snprintf(name, sizeof(name), "n%d", i);
        n[i] = add_node(s, "nx", name);
    }
    add_edge(s, "nx", n[1], n[2], "CALLS");
    add_edge(s, "nx", n[1], n[3], "CALLS");
    add_edge(s, "nx", n[3], n[1], "CALLS");
    add_edge(s, "nx", n[3], n[2], "CALLS");
    add_edge(s, "nx", n[3], n[5], "CALLS");
    add_edge(s, "nx", n[4], n[5], "CALLS");
    add_edge(s, "nx", n[4], n[6], "CALLS");
    add_edge(s, "nx", n[5], n[4], "CALLS");
    add_edge(s, "nx", n[5], n[6], "CALLS");
    add_edge(s, "nx", n[6], n[4], "CALLS");
    cbm_pagerank_compute_default(s, "nx");
    ASSERT_TRUE(get_pr(s, n[4]) > get_pr(s, n[1]));
    ASSERT_TRUE(get_pr(s, n[2]) > 0.0); /* dangling node gets rank */
    double total = 0;
    for (int i = 1; i <= 6; i++) total += get_pr(s, n[i]);
    ASSERT_TRUE(fabs(total - 1.0) < 0.05);
    cbm_store_close(s);
    PASS();
}

TEST(pagerank_scope_deps_only) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "sd", "/tmp/sd");
    cbm_store_upsert_project(s, "sd.dep.lib", "/tmp/sdlib");
    int64_t proj_node = add_node(s, "sd", "app");
    int64_t dep_node = add_node(s, "sd.dep.lib", "lib");
    int rc = cbm_pagerank_compute(s, "sd", CBM_PAGERANK_DAMPING,
                                  CBM_PAGERANK_EPSILON, CBM_PAGERANK_MAX_ITER,
                                  &CBM_DEFAULT_EDGE_WEIGHTS, CBM_RANK_SCOPE_DEPS);
    ASSERT_EQ(rc, 1);
    ASSERT_TRUE(get_pr(s, dep_node) > 0.0);
    ASSERT_TRUE(get_pr(s, proj_node) == 0.0);
    cbm_store_close(s);
    PASS();
}

/* ── 3. LinkRank tests ───────────────────────────────────── */

TEST(linkrank_computed_from_pagerank) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "lr", "/tmp/lr");
    add_node(s, "lr", "f1");
    add_node(s, "lr", "f2");
    add_edge(s, "lr", 1, 2, "CALLS");
    cbm_pagerank_compute_default(s, "lr");
    ASSERT_TRUE(count_table_rows(s, "linkrank") > 0);
    cbm_store_close(s);
    PASS();
}

TEST(linkrank_formula_correct) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "lrf", "/tmp/lrf");
    int64_t a = add_node(s, "lrf", "src");
    int64_t b = add_node(s, "lrf", "dst");
    int64_t eid = add_edge(s, "lrf", a, b, "CALLS");
    cbm_pagerank_compute_default(s, "lrf");
    double pra = get_pr(s, a);
    double lr = get_lr_by_edge_id(s, eid);
    /* Single outgoing CALLS (weight 1.0): LR = PR(A) * 1.0 / 1.0 = PR(A) */
    ASSERT_TRUE(fabs(lr - pra) < 0.01);
    cbm_store_close(s);
    PASS();
}

TEST(linkrank_calls_higher_than_usage) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "lrw", "/tmp/lrw");
    int64_t a = add_node(s, "lrw", "src");
    int64_t b = add_node(s, "lrw", "called");
    int64_t c = add_node(s, "lrw", "used");
    int64_t e1 = add_edge(s, "lrw", a, b, "CALLS");
    int64_t e2 = add_edge(s, "lrw", a, c, "USAGE");
    cbm_pagerank_compute_default(s, "lrw");
    ASSERT_TRUE(get_lr_by_edge_id(s, e1) > get_lr_by_edge_id(s, e2));
    cbm_store_close(s);
    PASS();
}

TEST(linkrank_stored_in_db) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "lrs", "/tmp/lrs");
    add_node(s, "lrs", "f1");
    add_node(s, "lrs", "f2");
    add_edge(s, "lrs", 1, 2, "CALLS");
    add_edge(s, "lrs", 2, 1, "IMPORTS");
    cbm_pagerank_compute_default(s, "lrs");
    ASSERT_EQ(count_table_rows(s, "linkrank"), 2);
    cbm_store_close(s);
    PASS();
}

TEST(linkrank_self_loop_edge) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "lrsl", "/tmp/lrsl");
    int64_t a = add_node(s, "lrsl", "recursive");
    int64_t eid = add_edge(s, "lrsl", a, a, "CALLS");
    cbm_pagerank_compute_default(s, "lrsl");
    ASSERT_EQ(count_table_rows(s, "linkrank"), 1);
    ASSERT_TRUE(get_lr_by_edge_id(s, eid) > 0.0);
    cbm_store_close(s);
    PASS();
}

TEST(linkrank_no_edges) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "lrne", "/tmp/lrne");
    add_node(s, "lrne", "isolated");
    cbm_pagerank_compute_default(s, "lrne");
    ASSERT_EQ(count_table_rows(s, "linkrank"), 0);
    cbm_store_close(s);
    PASS();
}

TEST(linkrank_sum_equals_pagerank_sum) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "lrs2", "/tmp/lrs2");
    int64_t a = add_node(s, "lrs2", "a");
    int64_t b = add_node(s, "lrs2", "b");
    int64_t c = add_node(s, "lrs2", "c");
    add_edge(s, "lrs2", a, b, "CALLS");
    add_edge(s, "lrs2", b, c, "CALLS");
    add_edge(s, "lrs2", c, a, "CALLS");
    cbm_pagerank_compute_default(s, "lrs2");
    sqlite3 *db = cbm_store_get_db(s);
    sqlite3_stmt *st = NULL;
    double lr_sum = 0.0;
    sqlite3_prepare_v2(db, "SELECT SUM(rank) FROM linkrank", -1, &st, NULL);
    if (sqlite3_step(st) == SQLITE_ROW) lr_sum = sqlite3_column_double(st, 0);
    sqlite3_finalize(st);
    double pr_sum = get_pr(s, a) + get_pr(s, b) + get_pr(s, c);
    ASSERT_TRUE(fabs(lr_sum - pr_sum) < 0.05);
    cbm_store_close(s);
    PASS();
}

TEST(linkrank_in_matches_incoming_linkrank_sum) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "lrin", "/tmp/lrin");
    int64_t a = add_node(s, "lrin", "a");
    int64_t b = add_node(s, "lrin", "b");
    int64_t c = add_node(s, "lrin", "c");
    int64_t ab = add_edge(s, "lrin", a, b, "CALLS");
    int64_t cb = add_edge(s, "lrin", c, b, "USAGE");
    add_edge(s, "lrin", b, a, "CALLS");

    cbm_pagerank_compute_default(s, "lrin");
    double incoming = get_lr_by_edge_id(s, ab) + get_lr_by_edge_id(s, cb);
    ASSERT_TRUE(fabs(get_linkrank_in_by_node_id(s, b) - incoming) < 1e-9);

    cbm_store_close(s);
    PASS();
}

/* ── 4. Integration: dep scoping ─────────────────────────── */

TEST(pagerank_after_dep_index) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "proj", "/tmp/proj");
    cbm_store_upsert_project(s, "proj.dep.lib", "/tmp/lib");
    int64_t a = add_node(s, "proj", "app_main");
    int64_t b = add_node(s, "proj.dep.lib", "lib_init");
    int64_t c = add_node(s, "proj.dep.lib", "lib_process");
    add_edge(s, "proj", a, b, "CALLS");
    add_edge(s, "proj.dep.lib", b, c, "CALLS");
    int rc = cbm_pagerank_compute_default(s, "proj");
    ASSERT_EQ(rc, 3);
    ASSERT_TRUE(get_pr(s, c) > 0.0);
    double total = get_pr(s, a) + get_pr(s, b) + get_pr(s, c);
    ASSERT_TRUE(fabs(total - 1.0) < 0.05);
    cbm_store_close(s);
    PASS();
}

/* ── 5. Phase 8.5: key_functions in get_architecture ─────── */

TEST(architecture_key_functions_with_pagerank) {
    /* After PR compute, verify key_functions array in architecture response
     * with top nodes by PageRank, correct order. */
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "arch", "/tmp/arch");
    int64_t ids[6];
    ids[0] = add_node(s, "arch", "hub_func");
    ids[1] = add_node(s, "arch", "spoke1");
    ids[2] = add_node(s, "arch", "spoke2");
    ids[3] = add_node(s, "arch", "spoke3");
    ids[4] = add_node(s, "arch", "spoke4");
    ids[5] = add_node(s, "arch", "leaf");
    /* hub_func called by 4 spokes → highest PageRank */
    add_edge(s, "arch", ids[1], ids[0], "CALLS");
    add_edge(s, "arch", ids[2], ids[0], "CALLS");
    add_edge(s, "arch", ids[3], ids[0], "CALLS");
    add_edge(s, "arch", ids[4], ids[0], "CALLS");
    cbm_pagerank_compute_default(s, "arch");
    /* hub_func should have highest rank */
    double hub_pr = get_pr(s, ids[0]);
    double leaf_pr = get_pr(s, ids[5]);
    ASSERT_TRUE(hub_pr > leaf_pr);
    /* Verify key_functions query works (top N by pagerank) */
    sqlite3 *db = cbm_store_get_db(s);
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(db,
        "SELECT n.name, pr.rank FROM nodes n "
        "JOIN pagerank pr ON pr.node_id = n.id "
        "WHERE n.project = 'arch' "
        "ORDER BY pr.rank DESC LIMIT 3", -1, &stmt, NULL);
    ASSERT_EQ(rc, SQLITE_OK);
    /* First result should be hub_func */
    ASSERT_EQ(sqlite3_step(stmt), SQLITE_ROW);
    const char *top_name = (const char *)sqlite3_column_text(stmt, 0);
    ASSERT_STR_EQ(top_name, "hub_func");
    sqlite3_finalize(stmt);
    cbm_store_close(s);
    PASS();
}

TEST(architecture_key_functions_no_pagerank) {
    /* When PageRank not computed, key_functions query returns 0 rows gracefully */
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "nopr", "/tmp/nopr");
    add_node(s, "nopr", "f1");
    /* Do NOT compute pagerank */
    sqlite3 *db = cbm_store_get_db(s);
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(db,
        "SELECT n.name, pr.rank FROM nodes n "
        "JOIN pagerank pr ON pr.node_id = n.id "
        "WHERE n.project = 'nopr' "
        "ORDER BY pr.rank DESC LIMIT 3", -1, &stmt, NULL);
    ASSERT_EQ(rc, SQLITE_OK);
    /* No rows — pagerank table empty for this project */
    ASSERT_EQ(sqlite3_step(stmt), SQLITE_DONE);
    sqlite3_finalize(stmt);
    cbm_store_close(s);
    PASS();
}

/* ── 6. Phase 8.5: config-backed edge weights ────────────── */

TEST(pagerank_config_custom_weights) {
    /* Verify custom edge weights struct produces different rankings */
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "cw", "/tmp/cw");
    int64_t a = add_node(s, "cw", "source");
    int64_t b = add_node(s, "cw", "imported");
    int64_t c = add_node(s, "cw", "called");
    add_edge(s, "cw", a, b, "IMPORTS");
    add_edge(s, "cw", a, c, "CALLS");
    /* Default: CALLS=1.0, IMPORTS=0.3 → c gets more rank */
    cbm_pagerank_compute_default(s, "cw");
    double rc_default = get_pr(s, c);
    double rb_default = get_pr(s, b);
    ASSERT_TRUE(rc_default > rb_default);
    /* Custom: boost IMPORTS to 2.0, drop CALLS to 0.1 */
    cbm_edge_weights_t custom = CBM_DEFAULT_EDGE_WEIGHTS;
    custom.imports = 2.0;
    custom.calls = 0.1;
    cbm_pagerank_compute(s, "cw", CBM_PAGERANK_DAMPING, CBM_PAGERANK_EPSILON,
                         CBM_PAGERANK_MAX_ITER, &custom, CBM_RANK_SCOPE_FULL);
    double rc_custom = get_pr(s, c);
    double rb_custom = get_pr(s, b);
    /* Now imported node should get more rank */
    ASSERT_TRUE(rb_custom > rc_custom);
    cbm_store_close(s);
    PASS();
}

/* ── 7. Phase 8.5: PageRank stats in index_status ────────── */

TEST(pagerank_stats_in_db) {
    /* After compute, verify pagerank table has computed_at timestamp */
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "stats", "/tmp/stats");
    add_node(s, "stats", "f1");
    add_node(s, "stats", "f2");
    add_edge(s, "stats", 1, 2, "CALLS");
    cbm_pagerank_compute_default(s, "stats");
    /* Verify computed_at is set */
    sqlite3 *db = cbm_store_get_db(s);
    sqlite3_stmt *stmt = NULL;
    sqlite3_prepare_v2(db,
        "SELECT COUNT(*), MAX(computed_at) FROM pagerank WHERE project = 'stats'",
        -1, &stmt, NULL);
    ASSERT_EQ(sqlite3_step(stmt), SQLITE_ROW);
    int ranked = sqlite3_column_int(stmt, 0);
    ASSERT_EQ(ranked, 2);
    const char *ts = (const char *)sqlite3_column_text(stmt, 1);
    ASSERT_NOT_NULL(ts);
    ASSERT_TRUE(strlen(ts) >= 10); /* at least YYYY-MM-DD */
    sqlite3_finalize(stmt);
    cbm_store_close(s);
    PASS();
}

/* ── 8. Phase 8.5: API streamlining ──────────────────────── */

TEST(pagerank_conditional_degree_logic) {
    /* Verify pagerank_score is populated on search results when PR is computed.
     * Uses pagerank_get directly since search result integration is tested
     * by the existing sort_by tests. */
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "cd", "/tmp/cd");
    int64_t a = add_node(s, "cd", "func_a");
    int64_t b = add_node(s, "cd", "func_b");
    add_edge(s, "cd", a, b, "CALLS");
    /* Before PR compute: pagerank_get returns 0 */
    ASSERT_TRUE(get_pr(s, a) == 0.0);
    ASSERT_TRUE(get_pr(s, b) == 0.0);
    /* After PR compute: pagerank_get returns > 0 */
    cbm_pagerank_compute_default(s, "cd");
    ASSERT_TRUE(get_pr(s, a) > 0.0);
    ASSERT_TRUE(get_pr(s, b) > 0.0);
    cbm_store_close(s);
    PASS();
}

TEST(pagerank_dep_source_tag_format) {
    /* Verify dep source tagging uses ".dep." detection.
     * cbm_is_dep_project("proj.dep.pandas", "proj") → true
     * cbm_is_dep_project("proj", "proj") → false */
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "dp", "/tmp/dp");
    cbm_store_upsert_project(s, "dp.dep.pandas", "/tmp/pandas");
    add_node(s, "dp", "my_func");
    add_node(s, "dp.dep.pandas", "DataFrame");
    /* Search all: both should be returned with correct source tags */
    cbm_search_params_t params = {0};
    params.limit = 10;
    cbm_search_output_t out = {0};
    cbm_store_search(s, &params, &out);
    ASSERT_TRUE(out.count >= 2);
    /* Verify dep detection helper */
    ASSERT_TRUE(cbm_is_dep_project("dp.dep.pandas", "dp"));
    ASSERT_FALSE(cbm_is_dep_project("dp", "dp"));
    ASSERT_FALSE(cbm_is_dep_project("deputy", "dep"));
    cbm_store_search_free(&out);
    cbm_store_close(s);
    PASS();
}

/* ── 9. Phase 8.5: Edge cases ────────────────────────────── */

TEST(pagerank_config_weight_very_small) {
    /* Very small (near-zero) edge weight should not crash.
     * Ranks should still sum to ~1.0 (valid distribution). */
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "vsm", "/tmp/vsm");
    int64_t a = add_node(s, "vsm", "a");
    int64_t b = add_node(s, "vsm", "b");
    add_edge(s, "vsm", a, b, "CALLS");
    cbm_edge_weights_t small_w = CBM_DEFAULT_EDGE_WEIGHTS;
    small_w.calls = 0.001; /* near-zero weight */
    int rc = cbm_pagerank_compute(s, "vsm", CBM_PAGERANK_DAMPING, CBM_PAGERANK_EPSILON,
                         CBM_PAGERANK_MAX_ITER, &small_w, CBM_RANK_SCOPE_FULL);
    ASSERT_EQ(rc, 2);
    /* Should not crash, ranks should sum to ~1 */
    double total = get_pr(s, a) + get_pr(s, b);
    ASSERT_TRUE(fabs(total - 1.0) < 0.1);
    cbm_store_close(s);
    PASS();
}

/* #21: PageRank damping + epsilon are tunable via config keys
 * (pagerank_damping, pagerank_epsilon) through cbm_pagerank_compute_with_config
 * — previously only max_iter + edge weights were config-exposed; damping/epsilon
 * were hard-coded #defines. This test proves the damping knob changes output. */
TEST(pagerank_damping_epsilon_config_tunable) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "cfg", "/tmp/cfg");
    int64_t hub = add_node(s, "cfg", "hub");
    int64_t s1 = add_node(s, "cfg", "s1");
    int64_t s2 = add_node(s, "cfg", "s2");
    int64_t s3 = add_node(s, "cfg", "s3");
    add_edge(s, "cfg", hub, s1, "CALLS");
    add_edge(s, "cfg", hub, s2, "CALLS");
    add_edge(s, "cfg", hub, s3, "CALLS");
    add_edge(s, "cfg", s1, hub, "CALLS");

    char tmpdir[CBM_PATH_MAX];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/pr-cfg-XXXXXX");
    ASSERT_TRUE(cbm_mkdtemp(tmpdir) != NULL);
    cbm_config_t *cfg = cbm_config_open(tmpdir);
    ASSERT_NOT_NULL(cfg);

    /* Low damping (0.5) → hub rank should differ from high damping (0.99).
     * compute_with_config returns the count of ranked nodes (4) on success. */
    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_PAGERANK_DAMPING, "0.5"), 0);
    ASSERT_EQ(cbm_pagerank_compute_with_config(s, "cfg", cfg), 4);
    double hub_low = get_pr(s, hub);

    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_PAGERANK_DAMPING, "0.99"), 0);
    ASSERT_EQ(cbm_pagerank_compute_with_config(s, "cfg", cfg), 4);
    double hub_high = get_pr(s, hub);

    /* The damping knob must materially change the rank value. */
    ASSERT_TRUE(fabs(hub_low - hub_high) > 1e-6);

    /* Sanity: ranks still sum to ~1 under a custom config. */
    double total = get_pr(s, hub) + get_pr(s, s1) + get_pr(s, s2) + get_pr(s, s3);
    ASSERT_TRUE(fabs(total - 1.0) < 0.05);

    cbm_config_close(cfg);
    th_rmtree(tmpdir);
    cbm_store_close(s);
    PASS();
}

/* Robustness (review finding #44): a NaN damping must NOT corrupt PageRank.
 * Since #21 made damping/epsilon config-tunable and cbm_config_get_double uses
 * strtod (which parses "nan"), a user can `config set pagerank_damping nan`.
 * The range clamp must reject NaN — IEEE-754 makes all NaN comparisons false,
 * so the naive `damping < 0 || damping > 1` form lets NaN through, poisoning
 * every rank and preventing convergence. */
TEST(pagerank_nan_damping_is_clamped) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "nan", "/tmp/nan");
    int64_t a = add_node(s, "nan", "a");
    int64_t b = add_node(s, "nan", "b");
    add_edge(s, "nan", a, b, "CALLS");
    add_edge(s, "nan", b, a, "CALLS");

    int rc = cbm_pagerank_compute(s, "nan", NAN, CBM_PAGERANK_EPSILON,
                                  CBM_PAGERANK_MAX_ITER, NULL, CBM_DEFAULT_RANK_SCOPE);
    ASSERT_EQ(rc, 2);
    double ra = get_pr(s, a);
    double rb = get_pr(s, b);
    ASSERT_TRUE(isfinite(ra)); /* NaN damping must be clamped, not propagated */
    ASSERT_TRUE(isfinite(rb));
    ASSERT_TRUE(fabs((ra + rb) - 1.0) < 0.05);

    cbm_store_close(s);
    PASS();
}

/* #49: out-of-range / nonsensical damping, epsilon, and max_iter must all clamp
 * to safe defaults and NOT corrupt the computation or hang. Guards the
 * config-tunable knobs (#21) against bad user-supplied values. */
TEST(pagerank_invalid_inputs_clamp_cleanly) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "bad", "/tmp/bad");
    int64_t a = add_node(s, "bad", "a");
    int64_t b = add_node(s, "bad", "b");
    add_edge(s, "bad", a, b, "CALLS");
    add_edge(s, "bad", b, a, "CALLS");

    struct { double damping; double epsilon; int max_iter; const char *label; } cases[] = {
        {-0.5, CBM_PAGERANK_EPSILON, 20, "negative damping"},   /* clamp to 0.85 */
        {2.0, CBM_PAGERANK_EPSILON, 20, "damping>1"},           /* clamp to 0.85 */
        {CBM_PAGERANK_DAMPING, -1.0, 20, "negative epsilon"},   /* clamp to 1e-6 */
        {CBM_PAGERANK_DAMPING, 0.0, 20, "zero epsilon"},        /* clamp to 1e-6 */
        {CBM_PAGERANK_DAMPING, CBM_PAGERANK_EPSILON, -5, "neg max_iter"},  /* clamp to 20 */
        {CBM_PAGERANK_DAMPING, CBM_PAGERANK_EPSILON, 0, "zero max_iter"},  /* clamp to 20 */
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        int rc = cbm_pagerank_compute(s, "bad", cases[i].damping, cases[i].epsilon,
                                      cases[i].max_iter, NULL, CBM_DEFAULT_RANK_SCOPE);
        ASSERT_EQ(rc, 2); /* both nodes ranked */
        double ra = get_pr(s, a), rb = get_pr(s, b);
        ASSERT_TRUE(isfinite(ra));
        ASSERT_TRUE(isfinite(rb));
        ASSERT_TRUE(fabs((ra + rb) - 1.0) < 0.05); /* ranks still sum to ~1 */
        (void)cases[i].label;
    }
    cbm_store_close(s);
    PASS();
}

/* ── Suite registration ──────────────────────────────────── */

SUITE(pagerank) {
    /* Core PageRank (14 tests) */
    RUN_TEST(pagerank_empty_graph);
    RUN_TEST(pagerank_single_node);
    RUN_TEST(pagerank_two_nodes_one_edge);
    RUN_TEST(pagerank_cycle);
    RUN_TEST(pagerank_star_topology);
    RUN_TEST(pagerank_edge_weights);
    RUN_TEST(pagerank_convergence);
    RUN_TEST(pagerank_damping_epsilon_config_tunable);
    RUN_TEST(pagerank_nan_damping_is_clamped);
    RUN_TEST(pagerank_invalid_inputs_clamp_cleanly);
    RUN_TEST(pagerank_sum_to_one);
    RUN_TEST(pagerank_stored_in_db);
    RUN_TEST(pagerank_views_complete_requires_all_rank_views);
    RUN_TEST(pagerank_refresh_if_needed_repairs_missing_views);
    RUN_TEST(pagerank_refresh_if_needed_skips_complete_unchanged_graph);
    RUN_TEST(pagerank_refresh_if_needed_recomputes_changed_graph);
    RUN_TEST(pagerank_refresh_if_needed_recomputes_reindexed_deps);
    RUN_TEST(pagerank_disabled_config_clears_rank_views);
    RUN_TEST(pagerank_refresh_defer_exact_delta_reindexes_defers_only_with_stale_rank_views);
    RUN_TEST(pagerank_refresh_defer_exact_delta_reindexes_does_not_defer_containment);
    RUN_TEST(pagerank_refresh_defer_all_incremental_reindexes_defers_containment);
    RUN_TEST(pagerank_refresh_defer_all_incremental_reindexes_defers_full_fallback);
    RUN_TEST(pagerank_refresh_default_defers_incremental_when_rank_views_stale);
    RUN_TEST(pagerank_refresh_invalid_policy_falls_back_to_at_publish);
    RUN_TEST(pagerank_recompute_replaces);
    RUN_TEST(pagerank_full_scope_includes_deps);
    RUN_TEST(pagerank_full_scope_preserves_dep_project_attribution);
    RUN_TEST(pagerank_project_scope_excludes_deps);
    RUN_TEST(pagerank_rank_scope_config_controls_scope_and_clears_stale_rows);
    RUN_TEST(pagerank_dangling_nodes);
    RUN_TEST(pagerank_null_safety);
    /* Edge cases from igraph/NetworkX (13 tests) */
    RUN_TEST(pagerank_self_loop);
    RUN_TEST(pagerank_disconnected_components);
    RUN_TEST(pagerank_all_dangling_no_edges);
    RUN_TEST(pagerank_complete_graph);
    RUN_TEST(pagerank_multigraph_edges);
    RUN_TEST(pagerank_large_graph_stability);
    RUN_TEST(pagerank_zero_weight_edges);
    RUN_TEST(pagerank_custom_damping_high);
    RUN_TEST(pagerank_custom_damping_low);
    RUN_TEST(pagerank_max_iter_zero);
    RUN_TEST(pagerank_known_values);
    RUN_TEST(pagerank_known_values_asymmetric);
    RUN_TEST(pagerank_scope_deps_only);
    /* LinkRank (7 tests) */
    RUN_TEST(linkrank_computed_from_pagerank);
    RUN_TEST(linkrank_formula_correct);
    RUN_TEST(linkrank_calls_higher_than_usage);
    RUN_TEST(linkrank_stored_in_db);
    RUN_TEST(linkrank_self_loop_edge);
    RUN_TEST(linkrank_no_edges);
    RUN_TEST(linkrank_sum_equals_pagerank_sum);
    RUN_TEST(linkrank_in_matches_incoming_linkrank_sum);
    /* Integration (1 test) */
    RUN_TEST(pagerank_after_dep_index);
    /* Phase 8.5: key_functions + config weights + stats + streamlining (7 tests) */
    RUN_TEST(architecture_key_functions_with_pagerank);
    RUN_TEST(architecture_key_functions_no_pagerank);
    RUN_TEST(pagerank_config_custom_weights);
    RUN_TEST(pagerank_stats_in_db);
    RUN_TEST(pagerank_conditional_degree_logic);
    RUN_TEST(pagerank_dep_source_tag_format);
    RUN_TEST(pagerank_config_weight_very_small);
}
