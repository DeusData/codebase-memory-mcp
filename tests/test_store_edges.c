/*
 * test_store_edges.c — Tests for edge CRUD operations.
 *
 * Ported from internal/store/store_test.go (TestEdgeCRUD, TestInsertEdgeBatch,
 * TestFindEdgesByURLPath, etc.)
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include <store/store.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

/* Helper: create a store with project + N nodes (A, B, C, ...) */
static cbm_store_t *setup_store_with_nodes(int n, int64_t *ids) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    char name[8], qn[32];
    for (int i = 0; i < n; i++) {
        snprintf(name, sizeof(name), "%c", 'A' + i);
        snprintf(qn, sizeof(qn), "test.%c", 'A' + i);
        cbm_node_t node = {
            .project = "test", .label = "Function", .name = name, .qualified_name = qn};
        ids[i] = cbm_store_upsert_node(s, &node);
    }
    return s;
}

/* ── Edge CRUD ──────────────────────────────────────────────────── */

TEST(store_edge_insert_find) {
    int64_t ids[2];
    cbm_store_t *s = setup_store_with_nodes(2, ids);

    /* Insert edge */
    cbm_edge_t e = {.project = "test", .source_id = ids[0], .target_id = ids[1], .type = "CALLS"};
    int64_t eid = cbm_store_insert_edge(s, &e);
    ASSERT_GT(eid, 0);

    /* Find by source */
    cbm_edge_t *edges = NULL;
    int count = 0;
    int rc = cbm_store_find_edges_by_source(s, ids[0], &edges, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(count, 1);
    ASSERT_STR_EQ(edges[0].type, "CALLS");
    ASSERT_EQ(edges[0].source_id, ids[0]);
    ASSERT_EQ(edges[0].target_id, ids[1]);
    cbm_store_free_edges(edges, count);

    /* Find by target */
    rc = cbm_store_find_edges_by_target(s, ids[1], &edges, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(count, 1);
    cbm_store_free_edges(edges, count);

    /* Count */
    int ecnt = cbm_store_count_edges(s, "test");
    ASSERT_EQ(ecnt, 1);

    cbm_store_close(s);
    PASS();
}

TEST(store_edge_dedup) {
    int64_t ids[2];
    cbm_store_t *s = setup_store_with_nodes(2, ids);

    /* Insert same edge twice — should not duplicate */
    cbm_edge_t e = {.project = "test", .source_id = ids[0], .target_id = ids[1], .type = "CALLS"};
    cbm_store_insert_edge(s, &e);
    cbm_store_insert_edge(s, &e);

    int ecnt = cbm_store_count_edges(s, "test");
    ASSERT_EQ(ecnt, 1);

    cbm_store_close(s);
    PASS();
}

/* #768 schema half: the edges UNIQUE constraint must discriminate IMPORTS
 * edges by local_name so two named imports from the same specifier coexist
 * as two rows, while re-inserting the SAME (src,tgt,IMPORTS,local_name)
 * still upserts onto one row. Non-IMPORTS uniqueness is unchanged. */
TEST(store_imports_edge_local_name_coexist) {
    int64_t ids[2];
    cbm_store_t *s = setup_store_with_nodes(2, ids);

    cbm_edge_t ea = {.project = "test",
                     .source_id = ids[0],
                     .target_id = ids[1],
                     .type = "IMPORTS",
                     .properties_json = "{\"local_name\":\"A\"}"};
    cbm_edge_t eb = {.project = "test",
                     .source_id = ids[0],
                     .target_id = ids[1],
                     .type = "IMPORTS",
                     .properties_json = "{\"local_name\":\"B\"}"};

    int64_t ida = cbm_store_insert_edge(s, &ea);
    int64_t idb = cbm_store_insert_edge(s, &eb);
    ASSERT_GT(ida, 0);
    ASSERT_GT(idb, 0);
    ASSERT_NEQ(ida, idb); /* distinct local_name -> distinct rows */

    /* Same (src,tgt,IMPORTS,local_name) again -> upsert onto the same row. */
    int64_t ida_again = cbm_store_insert_edge(s, &ea);
    ASSERT_EQ(ida_again, ida);

    cbm_edge_t *edges = NULL;
    int count = 0;
    int rc = cbm_store_find_edges_by_type(s, "test", "IMPORTS", &edges, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(count, 2);
    ASSERT_TRUE(strstr(edges[0].properties_json, "\"local_name\":\"A\"") != NULL ||
                strstr(edges[1].properties_json, "\"local_name\":\"A\"") != NULL);
    ASSERT_TRUE(strstr(edges[0].properties_json, "\"local_name\":\"B\"") != NULL ||
                strstr(edges[1].properties_json, "\"local_name\":\"B\"") != NULL);
    cbm_store_free_edges(edges, count);

    /* Non-IMPORTS dedup is unchanged: differing properties (no local_name)
     * must still upsert onto one row, not fan out via a NULL discriminator. */
    cbm_edge_t c1 = {.project = "test",
                     .source_id = ids[0],
                     .target_id = ids[1],
                     .type = "CALLS",
                     .properties_json = "{}"};
    cbm_edge_t c2 = {.project = "test",
                     .source_id = ids[0],
                     .target_id = ids[1],
                     .type = "CALLS",
                     .properties_json = "{\"weight\":5}"};
    int64_t idc1 = cbm_store_insert_edge(s, &c1);
    int64_t idc2 = cbm_store_insert_edge(s, &c2);
    ASSERT_GT(idc1, 0);
    ASSERT_EQ(idc1, idc2);

    cbm_store_close(s);
    PASS();
}

TEST(store_edge_find_by_source_type) {
    int64_t ids[3];
    cbm_store_t *s = setup_store_with_nodes(3, ids);

    cbm_edge_t e1 = {.project = "test", .source_id = ids[0], .target_id = ids[1], .type = "CALLS"};
    cbm_edge_t e2 = {
        .project = "test", .source_id = ids[0], .target_id = ids[2], .type = "IMPORTS"};
    cbm_store_insert_edge(s, &e1);
    cbm_store_insert_edge(s, &e2);

    cbm_edge_t *edges = NULL;
    int count = 0;
    int rc = cbm_store_find_edges_by_source_type(s, ids[0], "CALLS", &edges, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(count, 1);
    ASSERT_STR_EQ(edges[0].type, "CALLS");
    cbm_store_free_edges(edges, count);

    cbm_store_close(s);
    PASS();
}

TEST(store_edge_find_by_target_type) {
    int64_t ids[3];
    cbm_store_t *s = setup_store_with_nodes(3, ids);

    cbm_edge_t e1 = {.project = "test", .source_id = ids[0], .target_id = ids[2], .type = "CALLS"};
    cbm_edge_t e2 = {
        .project = "test", .source_id = ids[1], .target_id = ids[2], .type = "IMPORTS"};
    cbm_store_insert_edge(s, &e1);
    cbm_store_insert_edge(s, &e2);

    cbm_edge_t *edges = NULL;
    int count = 0;
    int rc = cbm_store_find_edges_by_target_type(s, ids[2], "CALLS", &edges, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(count, 1);
    cbm_store_free_edges(edges, count);

    cbm_store_close(s);
    PASS();
}

TEST(store_edge_find_by_type) {
    int64_t ids[3];
    cbm_store_t *s = setup_store_with_nodes(3, ids);

    cbm_edge_t e1 = {.project = "test", .source_id = ids[0], .target_id = ids[1], .type = "CALLS"};
    cbm_edge_t e2 = {.project = "test", .source_id = ids[1], .target_id = ids[2], .type = "CALLS"};
    cbm_edge_t e3 = {
        .project = "test", .source_id = ids[0], .target_id = ids[2], .type = "IMPORTS"};
    cbm_store_insert_edge(s, &e1);
    cbm_store_insert_edge(s, &e2);
    cbm_store_insert_edge(s, &e3);

    cbm_edge_t *edges = NULL;
    int count = 0;
    int rc = cbm_store_find_edges_by_type(s, "test", "CALLS", &edges, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(count, 2);
    cbm_store_free_edges(edges, count);

    cbm_store_close(s);
    PASS();
}

TEST(store_edge_count_by_type) {
    int64_t ids[3];
    cbm_store_t *s = setup_store_with_nodes(3, ids);

    cbm_edge_t e1 = {.project = "test", .source_id = ids[0], .target_id = ids[1], .type = "CALLS"};
    cbm_edge_t e2 = {.project = "test", .source_id = ids[1], .target_id = ids[2], .type = "CALLS"};
    cbm_edge_t e3 = {
        .project = "test", .source_id = ids[0], .target_id = ids[2], .type = "IMPORTS"};
    cbm_store_insert_edge(s, &e1);
    cbm_store_insert_edge(s, &e2);
    cbm_store_insert_edge(s, &e3);

    int cnt = cbm_store_count_edges_by_type(s, "test", "CALLS");
    ASSERT_EQ(cnt, 2);

    cnt = cbm_store_count_edges_by_type(s, "test", "IMPORTS");
    ASSERT_EQ(cnt, 1);

    cnt = cbm_store_count_edges_by_type(s, "test", "NONEXISTENT");
    ASSERT_EQ(cnt, 0);

    cbm_store_close(s);
    PASS();
}

TEST(store_edge_delete_by_type) {
    int64_t ids[3];
    cbm_store_t *s = setup_store_with_nodes(3, ids);

    cbm_edge_t e1 = {.project = "test", .source_id = ids[0], .target_id = ids[1], .type = "CALLS"};
    cbm_edge_t e2 = {
        .project = "test", .source_id = ids[0], .target_id = ids[2], .type = "IMPORTS"};
    cbm_store_insert_edge(s, &e1);
    cbm_store_insert_edge(s, &e2);

    cbm_store_delete_edges_by_type(s, "test", "CALLS");
    int ecnt = cbm_store_count_edges(s, "test");
    ASSERT_EQ(ecnt, 1);

    cbm_store_close(s);
    PASS();
}

/* ── Edge properties ────────────────────────────────────────────── */

TEST(store_edge_properties_json) {
    int64_t ids[2];
    cbm_store_t *s = setup_store_with_nodes(2, ids);

    cbm_edge_t e = {.project = "test",
                    .source_id = ids[0],
                    .target_id = ids[1],
                    .type = "HTTP_CALLS",
                    .properties_json = "{\"url_path\":\"/api/orders/create\",\"confidence\":0.8}"};
    cbm_store_insert_edge(s, &e);

    cbm_edge_t *edges = NULL;
    int count = 0;
    cbm_store_find_edges_by_source(s, ids[0], &edges, &count);
    ASSERT_EQ(count, 1);
    ASSERT_NOT_NULL(edges[0].properties_json);
    ASSERT(strstr(edges[0].properties_json, "url_path") != NULL);
    ASSERT(strstr(edges[0].properties_json, "/api/orders/create") != NULL);
    cbm_store_free_edges(edges, count);

    cbm_store_close(s);
    PASS();
}

TEST(store_edge_null_properties) {
    int64_t ids[2];
    cbm_store_t *s = setup_store_with_nodes(2, ids);

    cbm_edge_t e = {.project = "test",
                    .source_id = ids[0],
                    .target_id = ids[1],
                    .type = "CALLS",
                    .properties_json = NULL};
    cbm_store_insert_edge(s, &e);

    cbm_edge_t *edges = NULL;
    int count = 0;
    cbm_store_find_edges_by_source(s, ids[0], &edges, &count);
    ASSERT_EQ(count, 1);
    ASSERT_NOT_NULL(edges[0].properties_json);
    ASSERT_STR_EQ(edges[0].properties_json, "{}");
    cbm_store_free_edges(edges, count);

    cbm_store_close(s);
    PASS();
}

/* ── Batch edge insert ──────────────────────────────────────────── */

TEST(store_edge_batch_insert) {
    int64_t ids[10];
    cbm_store_t *s = setup_store_with_nodes(10, ids);

    /* Create edges: each node calls the next */
    cbm_edge_t edges[9];
    for (int i = 0; i < 9; i++) {
        edges[i] = (cbm_edge_t){
            .project = "test", .source_id = ids[i], .target_id = ids[i + 1], .type = "CALLS"};
    }

    int rc = cbm_store_insert_edge_batch(s, edges, 9);
    ASSERT_EQ(rc, CBM_STORE_OK);

    int ecnt = cbm_store_count_edges(s, "test");
    ASSERT_EQ(ecnt, 9);

    /* Re-insert should not duplicate */
    rc = cbm_store_insert_edge_batch(s, edges, 9);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ecnt = cbm_store_count_edges(s, "test");
    ASSERT_EQ(ecnt, 9);

    cbm_store_close(s);
    PASS();
}

TEST(store_edge_batch_empty) {
    cbm_store_t *s = cbm_store_open_memory();
    int rc = cbm_store_insert_edge_batch(s, NULL, 0);
    ASSERT_EQ(rc, CBM_STORE_OK);
    cbm_store_close(s);
    PASS();
}

/* ── Edge cascade on node delete ────────────────────────────────── */

TEST(store_edge_cascade_on_node_delete) {
    int64_t ids[3];
    cbm_store_t *s = setup_store_with_nodes(3, ids);

    /* A→B, A→C */
    cbm_edge_t e1 = {.project = "test", .source_id = ids[0], .target_id = ids[1], .type = "CALLS"};
    cbm_edge_t e2 = {.project = "test", .source_id = ids[0], .target_id = ids[2], .type = "CALLS"};
    cbm_store_insert_edge(s, &e1);
    cbm_store_insert_edge(s, &e2);

    /* Delete node A — edges should cascade */
    cbm_store_delete_nodes_by_project(s, "test");
    int ecnt = cbm_store_count_edges(s, "test");
    ASSERT_EQ(ecnt, 0);

    cbm_store_close(s);
    PASS();
}

/* ── Batch insert with count=0 (non-NULL array) ────────────────── */

TEST(store_edge_batch_insert_zero_count) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    cbm_edge_t dummy = {.project = "test", .source_id = 1, .target_id = 2, .type = "CALLS"};
    int rc = cbm_store_insert_edge_batch(s, &dummy, 0);
    ASSERT_EQ(rc, CBM_STORE_OK);

    int ecnt = cbm_store_count_edges(s, "test");
    ASSERT_EQ(ecnt, 0);

    cbm_store_close(s);
    PASS();
}

/* ── Batch insert stress: 50 edges ─────────────────────────────── */

TEST(store_edge_batch_insert_50) {
    /* Create 51 nodes so we can have 50 A→B edges with distinct targets */
    int64_t ids[51];
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    for (int i = 0; i < 51; i++) {
        char name[8], qn[32];
        snprintf(name, sizeof(name), "N%d", i);
        snprintf(qn, sizeof(qn), "test.N%d", i);
        cbm_node_t node = {
            .project = "test", .label = "Function", .name = name, .qualified_name = qn};
        ids[i] = cbm_store_upsert_node(s, &node);
    }

    /* 50 edges: N0→N1, N1→N2, ..., N49→N50 */
    cbm_edge_t edges[50];
    for (int i = 0; i < 50; i++) {
        edges[i] = (cbm_edge_t){
            .project = "test", .source_id = ids[i], .target_id = ids[i + 1], .type = "CALLS"};
    }

    int rc = cbm_store_insert_edge_batch(s, edges, 50);
    ASSERT_EQ(rc, CBM_STORE_OK);

    int ecnt = cbm_store_count_edges(s, "test");
    ASSERT_EQ(ecnt, 50);

    cbm_store_close(s);
    PASS();
}

/* ── find_edges_by_source with non-existent source ─────────────── */

TEST(store_edge_find_source_nonexistent) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    cbm_edge_t *edges = NULL;
    int count = 0;
    int rc = cbm_store_find_edges_by_source(s, 999999, &edges, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(count, 0);
    cbm_store_free_edges(edges, count);

    cbm_store_close(s);
    PASS();
}

/* ── find_edges_by_target with non-existent target ─────────────── */

TEST(store_edge_find_target_nonexistent) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    cbm_edge_t *edges = NULL;
    int count = 0;
    int rc = cbm_store_find_edges_by_target(s, 999999, &edges, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(count, 0);
    cbm_store_free_edges(edges, count);

    cbm_store_close(s);
    PASS();
}

/* ── find_edges_by_type for non-existent type ──────────────────── */

TEST(store_edge_find_type_nonexistent) {
    int64_t ids[2];
    cbm_store_t *s = setup_store_with_nodes(2, ids);

    cbm_edge_t e = {.project = "test", .source_id = ids[0], .target_id = ids[1], .type = "CALLS"};
    cbm_store_insert_edge(s, &e);

    cbm_edge_t *edges = NULL;
    int count = 0;
    int rc = cbm_store_find_edges_by_type(s, "test", "DOES_NOT_EXIST", &edges, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(count, 0);
    cbm_store_free_edges(edges, count);

    cbm_store_close(s);
    PASS();
}

/* ── find_edges_among (scoped edge fetch for #2039) ──────────────── */

/* A-B-C-D-E path graph (CALLS chain) plus a B->D IMPORTS edge so more than
 * one type is exercised. Sampling {A,B,C} should return only edges whose
 * BOTH endpoints are in that set: A->B and (deliberately excluded) neither
 * C->D nor B->D nor D->E, since D and E are outside the sample. */
static cbm_store_t *setup_among_store(int64_t *ids) {
    int64_t discard[5];
    if (!ids)
        ids = discard;
    cbm_store_t *s = setup_store_with_nodes(5, ids); /* A B C D E */

    cbm_edge_t ab = {.project = "test", .source_id = ids[0], .target_id = ids[1], .type = "CALLS"};
    cbm_edge_t bc = {.project = "test", .source_id = ids[1], .target_id = ids[2], .type = "CALLS"};
    cbm_edge_t cd = {.project = "test", .source_id = ids[2], .target_id = ids[3], .type = "CALLS"};
    cbm_edge_t de = {.project = "test", .source_id = ids[3], .target_id = ids[4], .type = "CALLS"};
    cbm_edge_t bd = {
        .project = "test", .source_id = ids[1], .target_id = ids[3], .type = "IMPORTS"};
    cbm_store_insert_edge(s, &ab);
    cbm_store_insert_edge(s, &bc);
    cbm_store_insert_edge(s, &cd);
    cbm_store_insert_edge(s, &de);
    cbm_store_insert_edge(s, &bd);
    return s;
}

TEST(store_find_edges_among_scopes_to_sample) {
    int64_t ids[5];
    cbm_store_t *s = setup_among_store(ids);

    int64_t sample[3] = {ids[0], ids[1], ids[2]}; /* A, B, C — excludes D, E */
    cbm_edge_t *edges = NULL;
    int count = 0;
    int rc = cbm_store_find_edges_among(s, "test", sample, 3, &edges, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(count, 2); /* A->B, B->C only: C->D/B->D/D->E fall outside the sample */

    bool saw_ab = false, saw_bc = false;
    for (int i = 0; i < count; i++) {
        ASSERT_NOT_NULL(edges[i].type);
        /* No properties column fetched — must stay NULL, not garbage. */
        ASSERT_NULL(edges[i].properties_json);
        if (edges[i].source_id == ids[0] && edges[i].target_id == ids[1] &&
            strcmp(edges[i].type, "CALLS") == 0)
            saw_ab = true;
        if (edges[i].source_id == ids[1] && edges[i].target_id == ids[2] &&
            strcmp(edges[i].type, "CALLS") == 0)
            saw_bc = true;
    }
    ASSERT_TRUE(saw_ab);
    ASSERT_TRUE(saw_bc);

    cbm_store_free_edges(edges, count);
    cbm_store_close(s);
    PASS();
}

TEST(store_find_edges_among_no_internal_edges) {
    int64_t ids[5];
    cbm_store_t *s = setup_among_store(ids);

    /* A and E are both in the graph but never directly connected to each
     * other, and no other sampled node bridges them. */
    int64_t sample[2] = {ids[0], ids[4]};
    cbm_edge_t *edges = NULL;
    int count = 0;
    int rc = cbm_store_find_edges_among(s, "test", sample, 2, &edges, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(count, 0);
    cbm_store_free_edges(edges, count);

    cbm_store_close(s);
    PASS();
}

TEST(store_find_edges_among_invalid_args) {
    int64_t ids[5];
    cbm_store_t *s = setup_among_store(ids);
    int64_t sample[1] = {ids[0]};
    cbm_edge_t *edges = (cbm_edge_t *)0x1; /* poison — must be reset to NULL */
    int count = -1;

    ASSERT_EQ(cbm_store_find_edges_among(NULL, "test", sample, 1, &edges, &count), CBM_STORE_ERR);
    ASSERT_NULL(edges);
    ASSERT_EQ(count, 0);

    edges = (cbm_edge_t *)0x1;
    count = -1;
    ASSERT_EQ(cbm_store_find_edges_among(s, "test", sample, 0, &edges, &count), CBM_STORE_ERR);
    ASSERT_NULL(edges);
    ASSERT_EQ(count, 0);

    edges = (cbm_edge_t *)0x1;
    count = -1;
    ASSERT_EQ(cbm_store_find_edges_among(s, "test", NULL, 1, &edges, &count), CBM_STORE_ERR);
    ASSERT_NULL(edges);
    ASSERT_EQ(count, 0);

    cbm_store_close(s);
    PASS();
}

/* Repeated calls on the same handle must not leak/collide their private TEMP
 * table — each call re-scopes it to a fresh id set. */
TEST(store_find_edges_among_reusable_across_calls) {
    int64_t ids[5];
    cbm_store_t *s = setup_among_store(ids);

    int64_t first[2] = {ids[0], ids[1]};
    cbm_edge_t *e1 = NULL;
    int c1 = 0;
    ASSERT_EQ(cbm_store_find_edges_among(s, "test", first, 2, &e1, &c1), CBM_STORE_OK);
    ASSERT_EQ(c1, 1); /* A->B */
    cbm_store_free_edges(e1, c1);

    int64_t second[2] = {ids[3], ids[4]};
    cbm_edge_t *e2 = NULL;
    int c2 = 0;
    ASSERT_EQ(cbm_store_find_edges_among(s, "test", second, 2, &e2, &c2), CBM_STORE_OK);
    ASSERT_EQ(c2, 1); /* D->E */
    cbm_store_free_edges(e2, c2);

    cbm_store_close(s);
    PASS();
}

/* cbm_store_open_path_query hands out a read-only connection — layout
 * requests go through it. cbm_store_find_edges_among must work there: it
 * only ever writes to the private TEMP schema, never the main DB file. */
static void tse_cleanup_db(const char *db_path) {
    char sidecar[512];
    unlink(db_path);
    snprintf(sidecar, sizeof(sidecar), "%s-wal", db_path);
    unlink(sidecar);
    snprintf(sidecar, sizeof(sidecar), "%s-shm", db_path);
    unlink(sidecar);
}

TEST(store_find_edges_among_readonly_connection) {
    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/cbm_test_edges_among_ro_%d.db", cbm_tmpdir(),
            (int)getpid());
    tse_cleanup_db(db_path);

    cbm_store_t *setup = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(setup);
    ASSERT_EQ(cbm_store_upsert_project(setup, "test", "/tmp/test"), CBM_STORE_OK);
    int64_t ids[3];
    char name[8], qn[32];
    for (int i = 0; i < 3; i++) {
        snprintf(name, sizeof(name), "%c", 'A' + i);
        snprintf(qn, sizeof(qn), "test.%c", 'A' + i);
        cbm_node_t node = {
            .project = "test", .label = "Function", .name = name, .qualified_name = qn};
        ids[i] = cbm_store_upsert_node(setup, &node);
        ASSERT_GT(ids[i], 0);
    }
    cbm_edge_t ab = {.project = "test", .source_id = ids[0], .target_id = ids[1], .type = "CALLS"};
    ASSERT_GT(cbm_store_insert_edge(setup, &ab), 0);
    ASSERT_EQ(cbm_store_seal_for_atomic_publish(setup), CBM_STORE_OK);
    cbm_store_close(setup);

    cbm_store_t *reader = cbm_store_open_path_query(db_path);
    ASSERT_NOT_NULL(reader);

    cbm_edge_t *edges = NULL;
    int count = 0;
    int rc = cbm_store_find_edges_among(reader, "test", ids, 3, &edges, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(edges[0].source_id, ids[0]);
    ASSERT_EQ(edges[0].target_id, ids[1]);
    cbm_store_free_edges(edges, count);

    cbm_store_close(reader);
    tse_cleanup_db(db_path);
    PASS();
}

/* ── count_edges on empty project ──────────────────────────────── */

TEST(store_edge_count_empty_project) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "empty", "/tmp/empty");

    int ecnt = cbm_store_count_edges(s, "empty");
    ASSERT_EQ(ecnt, 0);

    cbm_store_close(s);
    PASS();
}

/* ── count_edges_by_type for missing type ──────────────────────── */

TEST(store_edge_count_by_type_missing) {
    int64_t ids[2];
    cbm_store_t *s = setup_store_with_nodes(2, ids);

    cbm_edge_t e = {.project = "test", .source_id = ids[0], .target_id = ids[1], .type = "CALLS"};
    cbm_store_insert_edge(s, &e);

    int cnt = cbm_store_count_edges_by_type(s, "test", "NEVER_EXISTS");
    ASSERT_EQ(cnt, 0);

    cbm_store_close(s);
    PASS();
}

/* ── delete_edges_by_type preserves other types ────────────────── */

TEST(store_edge_delete_by_type_preserves_others) {
    int64_t ids[3];
    cbm_store_t *s = setup_store_with_nodes(3, ids);

    cbm_edge_t e1 = {.project = "test", .source_id = ids[0], .target_id = ids[1], .type = "CALLS"};
    cbm_edge_t e2 = {
        .project = "test", .source_id = ids[0], .target_id = ids[2], .type = "IMPORTS"};
    cbm_edge_t e3 = {
        .project = "test", .source_id = ids[1], .target_id = ids[2], .type = "HTTP_CALLS"};
    cbm_store_insert_edge(s, &e1);
    cbm_store_insert_edge(s, &e2);
    cbm_store_insert_edge(s, &e3);
    ASSERT_EQ(cbm_store_count_edges(s, "test"), 3);

    /* Delete only CALLS */
    cbm_store_delete_edges_by_type(s, "test", "CALLS");
    ASSERT_EQ(cbm_store_count_edges(s, "test"), 2);

    /* IMPORTS and HTTP_CALLS still exist */
    ASSERT_EQ(cbm_store_count_edges_by_type(s, "test", "IMPORTS"), 1);
    ASSERT_EQ(cbm_store_count_edges_by_type(s, "test", "HTTP_CALLS"), 1);
    ASSERT_EQ(cbm_store_count_edges_by_type(s, "test", "CALLS"), 0);

    cbm_store_close(s);
    PASS();
}

/* ── delete_edges_by_project preserves other projects ──────────── */

TEST(store_edge_delete_by_project_preserves_others) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "alpha", "/tmp/alpha");
    cbm_store_upsert_project(s, "beta", "/tmp/beta");

    /* Create nodes in both projects */
    cbm_node_t na = {
        .project = "alpha", .label = "Function", .name = "A", .qualified_name = "alpha.A"};
    cbm_node_t nb = {
        .project = "alpha", .label = "Function", .name = "B", .qualified_name = "alpha.B"};
    cbm_node_t nc = {
        .project = "beta", .label = "Function", .name = "C", .qualified_name = "beta.C"};
    cbm_node_t nd = {
        .project = "beta", .label = "Function", .name = "D", .qualified_name = "beta.D"};
    int64_t idA = cbm_store_upsert_node(s, &na);
    int64_t idB = cbm_store_upsert_node(s, &nb);
    int64_t idC = cbm_store_upsert_node(s, &nc);
    int64_t idD = cbm_store_upsert_node(s, &nd);

    cbm_edge_t e1 = {.project = "alpha", .source_id = idA, .target_id = idB, .type = "CALLS"};
    cbm_edge_t e2 = {.project = "beta", .source_id = idC, .target_id = idD, .type = "CALLS"};
    cbm_store_insert_edge(s, &e1);
    cbm_store_insert_edge(s, &e2);

    ASSERT_EQ(cbm_store_count_edges(s, "alpha"), 1);
    ASSERT_EQ(cbm_store_count_edges(s, "beta"), 1);

    /* Delete alpha edges only */
    cbm_store_delete_edges_by_project(s, "alpha");
    ASSERT_EQ(cbm_store_count_edges(s, "alpha"), 0);
    ASSERT_EQ(cbm_store_count_edges(s, "beta"), 1);

    cbm_store_close(s);
    PASS();
}

/* ── Edge with special chars in properties_json ────────────────── */

TEST(store_edge_properties_special_chars) {
    int64_t ids[2];
    cbm_store_t *s = setup_store_with_nodes(2, ids);

    /* JSON with unicode, quotes, backslashes */
    const char *json = "{\"desc\":\"line1\\nline2\",\"path\":\"/api/v1/users?q=foo&bar=baz\","
                       "\"emoji\":\"\\u2603\"}";
    cbm_edge_t e = {.project = "test",
                    .source_id = ids[0],
                    .target_id = ids[1],
                    .type = "HTTP_CALLS",
                    .properties_json = json};
    cbm_store_insert_edge(s, &e);

    cbm_edge_t *edges = NULL;
    int count = 0;
    cbm_store_find_edges_by_source(s, ids[0], &edges, &count);
    ASSERT_EQ(count, 1);
    ASSERT_NOT_NULL(edges[0].properties_json);
    /* Round-trip should preserve the JSON string */
    ASSERT(strstr(edges[0].properties_json, "line1\\nline2") != NULL);
    ASSERT(strstr(edges[0].properties_json, "/api/v1/users") != NULL);
    cbm_store_free_edges(edges, count);

    cbm_store_close(s);
    PASS();
}

/* ── Edge with very long type string ───────────────────────────── */

TEST(store_edge_long_type_string) {
    int64_t ids[2];
    cbm_store_t *s = setup_store_with_nodes(2, ids);

    /* 200-char type string */
    char long_type[201];
    memset(long_type, 'X', 200);
    long_type[200] = '\0';

    cbm_edge_t e = {.project = "test", .source_id = ids[0], .target_id = ids[1], .type = long_type};
    int64_t eid = cbm_store_insert_edge(s, &e);
    ASSERT_GT(eid, 0);

    /* Verify it round-trips */
    cbm_edge_t *edges = NULL;
    int count = 0;
    int rc = cbm_store_find_edges_by_source(s, ids[0], &edges, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(count, 1);
    ASSERT_STR_EQ(edges[0].type, long_type);
    cbm_store_free_edges(edges, count);

    cbm_store_close(s);
    PASS();
}

/* ── find_edges_by_source_type with non-existent type ──────────── */

TEST(store_edge_find_source_type_nonexistent) {
    int64_t ids[2];
    cbm_store_t *s = setup_store_with_nodes(2, ids);

    cbm_edge_t e = {.project = "test", .source_id = ids[0], .target_id = ids[1], .type = "CALLS"};
    cbm_store_insert_edge(s, &e);

    cbm_edge_t *edges = NULL;
    int count = 0;
    int rc = cbm_store_find_edges_by_source_type(s, ids[0], "NOPE", &edges, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(count, 0);
    cbm_store_free_edges(edges, count);

    cbm_store_close(s);
    PASS();
}

SUITE(store_edges) {
    RUN_TEST(store_edge_insert_find);
    RUN_TEST(store_edge_dedup);
    RUN_TEST(store_imports_edge_local_name_coexist);
    RUN_TEST(store_edge_find_by_source_type);
    RUN_TEST(store_edge_find_by_target_type);
    RUN_TEST(store_edge_find_by_type);
    RUN_TEST(store_edge_count_by_type);
    RUN_TEST(store_edge_delete_by_type);
    RUN_TEST(store_edge_properties_json);
    RUN_TEST(store_edge_null_properties);
    RUN_TEST(store_edge_batch_insert);
    RUN_TEST(store_edge_batch_empty);
    RUN_TEST(store_edge_cascade_on_node_delete);
    /* Edge case tests */
    RUN_TEST(store_edge_batch_insert_zero_count);
    RUN_TEST(store_edge_batch_insert_50);
    RUN_TEST(store_edge_find_source_nonexistent);
    RUN_TEST(store_edge_find_target_nonexistent);
    RUN_TEST(store_edge_find_type_nonexistent);
    RUN_TEST(store_find_edges_among_scopes_to_sample);
    RUN_TEST(store_find_edges_among_no_internal_edges);
    RUN_TEST(store_find_edges_among_invalid_args);
    RUN_TEST(store_find_edges_among_reusable_across_calls);
    RUN_TEST(store_find_edges_among_readonly_connection);
    RUN_TEST(store_edge_count_empty_project);
    RUN_TEST(store_edge_count_by_type_missing);
    RUN_TEST(store_edge_delete_by_type_preserves_others);
    RUN_TEST(store_edge_delete_by_project_preserves_others);
    RUN_TEST(store_edge_properties_special_chars);
    RUN_TEST(store_edge_long_type_string);
    RUN_TEST(store_edge_find_source_type_nonexistent);
}
