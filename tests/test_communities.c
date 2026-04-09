/*
 * test_communities.c — Tests for the communities pipeline pass.
 *
 * Verifies Louvain community detection on service-linking edges:
 * creates graph buffer with nodes + edges, runs pass, checks Community
 * nodes and MEMBER_OF edges.
 */
#include "test_framework.h"
#include <pipeline/pipeline_internal.h>
#include <pipeline/servicelink.h>
#include <store/store.h>
#include "graph_buffer/graph_buffer.h"
#include <stdatomic.h>
#include <string.h>
#include <stdlib.h>

/* ── Helper to create pipeline context ──────────────────────── */

static cbm_pipeline_ctx_t make_ctx(cbm_gbuf_t *gb) {
    static atomic_int cancelled;
    atomic_init(&cancelled, 0);
    cbm_pipeline_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.project_name = "test";
    ctx.repo_path = "/tmp";
    ctx.gbuf = gb;
    ctx.cancelled = &cancelled;
    return ctx;
}

/* ── Helper to count Community nodes ────────────────────────── */

static int count_community_nodes(cbm_gbuf_t *gb) {
    const cbm_gbuf_node_t **nodes = NULL;
    int count = 0;
    cbm_gbuf_find_by_label(gb, "Community", &nodes, &count);
    return count;
}

/* ── Helper to count MEMBER_OF edges ────────────────────────── */

static int count_member_of_edges(cbm_gbuf_t *gb) {
    return cbm_gbuf_edge_count_by_type(gb, "MEMBER_OF");
}

/* ── Test: basic — two clusters ─────────────────────────────── */

TEST(communities_basic) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");

    /* Create two clusters of function nodes connected by CALLS edges.
     * Cluster A: f1 <-> f2 <-> f3 <-> f1 (triangle)
     * Cluster B: f4 <-> f5 (pair) */
    int64_t f1 = cbm_gbuf_upsert_node(gb, "Function", "f1", "test.f1", "a.go", 1, 5, "{}");
    int64_t f2 = cbm_gbuf_upsert_node(gb, "Function", "f2", "test.f2", "a.go", 6, 10, "{}");
    int64_t f3 = cbm_gbuf_upsert_node(gb, "Function", "f3", "test.f3", "a.go", 11, 15, "{}");
    int64_t f4 = cbm_gbuf_upsert_node(gb, "Function", "f4", "test.f4", "b.go", 1, 5, "{}");
    int64_t f5 = cbm_gbuf_upsert_node(gb, "Function", "f5", "test.f5", "b.go", 6, 10, "{}");

    /* Triangle edges */
    cbm_gbuf_insert_edge(gb, f1, f2, "CALLS", "{}");
    cbm_gbuf_insert_edge(gb, f2, f3, "CALLS", "{}");
    cbm_gbuf_insert_edge(gb, f1, f3, "CALLS", "{}");

    /* Pair edges */
    cbm_gbuf_insert_edge(gb, f4, f5, "CALLS", "{}");

    cbm_pipeline_ctx_t ctx = make_ctx(gb);
    ASSERT_EQ(cbm_pipeline_pass_communities(&ctx), 0);

    /* Should have 2 Community nodes */
    ASSERT_EQ(count_community_nodes(gb), 2);

    /* Should have 5 MEMBER_OF edges (one per function node) */
    ASSERT_EQ(count_member_of_edges(gb), 5);

    cbm_gbuf_free(gb);
    PASS();
}

/* ── Test: empty — no edges ─────────────────────────────────── */

TEST(communities_empty) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");

    /* No edges at all */
    cbm_pipeline_ctx_t ctx = make_ctx(gb);
    ASSERT_EQ(cbm_pipeline_pass_communities(&ctx), 0);

    /* No Community nodes */
    ASSERT_EQ(count_community_nodes(gb), 0);
    ASSERT_EQ(count_member_of_edges(gb), 0);

    cbm_gbuf_free(gb);
    PASS();
}

/* ── Test: single edge — one community with 2 members ───────── */

TEST(communities_single_edge) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");

    int64_t f1 = cbm_gbuf_upsert_node(gb, "Function", "f1", "test.f1", "a.go", 1, 5, "{}");
    int64_t f2 = cbm_gbuf_upsert_node(gb, "Function", "f2", "test.f2", "a.go", 6, 10, "{}");
    cbm_gbuf_insert_edge(gb, f1, f2, "CALLS", "{}");

    cbm_pipeline_ctx_t ctx = make_ctx(gb);
    ASSERT_EQ(cbm_pipeline_pass_communities(&ctx), 0);

    /* One community with 2 members */
    ASSERT_EQ(count_community_nodes(gb), 1);
    ASSERT_EQ(count_member_of_edges(gb), 2);

    /* Verify the Community node has member_count property */
    const cbm_gbuf_node_t **community_nodes = NULL;
    int ccount = 0;
    cbm_gbuf_find_by_label(gb, "Community", &community_nodes, &ccount);
    ASSERT_EQ(ccount, 1);
    ASSERT_TRUE(strstr(community_nodes[0]->properties_json, "\"member_count\":2") != NULL);

    cbm_gbuf_free(gb);
    PASS();
}

/* ── Test: mixed edge types feed into detection ─────────────── */

TEST(communities_mixed_edge_types) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");

    /* Create nodes */
    int64_t f1 = cbm_gbuf_upsert_node(gb, "Function", "f1", "test.f1", "a.go", 1, 5, "{}");
    int64_t f2 = cbm_gbuf_upsert_node(gb, "Function", "f2", "test.f2", "a.go", 6, 10, "{}");
    int64_t f3 = cbm_gbuf_upsert_node(gb, "Function", "f3", "test.f3", "b.go", 1, 5, "{}");

    /* Mix of edge types — all should be picked up */
    cbm_gbuf_insert_edge(gb, f1, f2, "CALLS", "{}");
    cbm_gbuf_insert_edge(gb, f2, f3, SL_EDGE_KAFKA, "{}");
    cbm_gbuf_insert_edge(gb, f1, f3, SL_EDGE_GRPC, "{}");

    cbm_pipeline_ctx_t ctx = make_ctx(gb);
    ASSERT_EQ(cbm_pipeline_pass_communities(&ctx), 0);

    /* All 3 nodes are interconnected, should form 1 community */
    ASSERT_EQ(count_community_nodes(gb), 1);
    ASSERT_EQ(count_member_of_edges(gb), 3);

    /* Verify member_count is 3 */
    const cbm_gbuf_node_t **community_nodes = NULL;
    int ccount = 0;
    cbm_gbuf_find_by_label(gb, "Community", &community_nodes, &ccount);
    ASSERT_EQ(ccount, 1);
    ASSERT_TRUE(strstr(community_nodes[0]->properties_json, "\"member_count\":3") != NULL);

    cbm_gbuf_free(gb);
    PASS();
}

/* ── Suite entry point ──────────────────────────────────────── */

void suite_communities(void) {
    RUN_TEST(communities_basic);
    RUN_TEST(communities_empty);
    RUN_TEST(communities_single_edge);
    RUN_TEST(communities_mixed_edge_types);
}
