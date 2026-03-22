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
#include "test_framework.h"
#include <store/store.h>
#include <pagerank/pagerank.h>
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
    char sql[64];
    snprintf(sql, sizeof(sql), "SELECT COUNT(*) FROM %s", table);
    sqlite3_stmt *stmt = NULL;
    int count = 0;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW) count = sqlite3_column_int(stmt, 0);
        sqlite3_finalize(stmt);
    }
    return count;
}

static double get_lr_by_edge_id(cbm_store_t *s, int64_t edge_id) {
    return cbm_linkrank_get(s, edge_id);
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
    RUN_TEST(pagerank_sum_to_one);
    RUN_TEST(pagerank_stored_in_db);
    RUN_TEST(pagerank_recompute_replaces);
    RUN_TEST(pagerank_full_scope_includes_deps);
    RUN_TEST(pagerank_project_scope_excludes_deps);
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
    /* Integration (1 test) */
    RUN_TEST(pagerank_after_dep_index);
}
