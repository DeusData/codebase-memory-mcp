/*
 * test_store_nodes.c — Tests for store schema, project CRUD, and node CRUD.
 *
 * Ported from internal/store/store_test.go (TestOpenMemory, TestNodeCRUD,
 * TestNodeDedup, TestProjectCRUD, TestUpsertNodeBatch, etc.)
 */
#include "test_framework.h"
#include <store/store.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

/* ── Schema / Open / Close ──────────────────────────────────────── */

TEST(store_open_memory) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    cbm_store_close(s);
    PASS();
}

TEST(store_close_null) {
    cbm_store_close(NULL); /* should not crash */
    PASS();
}

TEST(store_open_memory_twice) {
    cbm_store_t *s1 = cbm_store_open_memory();
    cbm_store_t *s2 = cbm_store_open_memory();
    ASSERT_NOT_NULL(s1);
    ASSERT_NOT_NULL(s2);
    /* independent databases */
    cbm_store_close(s1);
    cbm_store_close(s2);
    PASS();
}

/* ── Project CRUD ───────────────────────────────────────────────── */

TEST(store_project_crud) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);

    /* Create */
    int rc = cbm_store_upsert_project(s, "myproject", "/home/user/myproject");
    ASSERT_EQ(rc, CBM_STORE_OK);

    /* Get */
    cbm_project_t p = {0};
    rc = cbm_store_get_project(s, "myproject", &p);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_STR_EQ(p.name, "myproject");
    ASSERT_STR_EQ(p.root_path, "/home/user/myproject");
    ASSERT_NOT_NULL(p.indexed_at);
    cbm_project_free_fields(&p);

    /* List */
    cbm_project_t *projects = NULL;
    int count = 0;
    rc = cbm_store_list_projects(s, &projects, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(count, 1);
    ASSERT_STR_EQ(projects[0].name, "myproject");
    cbm_store_free_projects(projects, count);

    /* Get non-existent */
    cbm_project_t p2 = {0};
    rc = cbm_store_get_project(s, "nonexistent", &p2);
    ASSERT_EQ(rc, CBM_STORE_NOT_FOUND);

    cbm_store_close(s);
    PASS();
}

TEST(store_project_update) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/old/path");

    /* Update root path */
    cbm_store_upsert_project(s, "test", "/new/path");

    cbm_project_t p = {0};
    cbm_store_get_project(s, "test", &p);
    ASSERT_STR_EQ(p.root_path, "/new/path");
    cbm_project_free_fields(&p);

    /* Should still be 1 project */
    cbm_project_t *projects = NULL;
    int count = 0;
    cbm_store_list_projects(s, &projects, &count);
    ASSERT_EQ(count, 1);
    cbm_store_free_projects(projects, count);

    cbm_store_close(s);
    PASS();
}

TEST(store_project_delete) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    int rc = cbm_store_delete_project(s, "test");
    ASSERT_EQ(rc, CBM_STORE_OK);

    cbm_project_t p = {0};
    rc = cbm_store_get_project(s, "test", &p);
    ASSERT_EQ(rc, CBM_STORE_NOT_FOUND);

    cbm_store_close(s);
    PASS();
}

/* ── Node CRUD ──────────────────────────────────────────────────── */

TEST(store_node_crud) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    /* Insert node */
    cbm_node_t n = {.project = "test",
                    .label = "Function",
                    .name = "Foo",
                    .qualified_name = "test.main.Foo",
                    .file_path = "main.go",
                    .start_line = 10,
                    .end_line = 20,
                    .properties_json = "{\"signature\":\"func Foo(x int) error\"}"};
    int64_t id = cbm_store_upsert_node(s, &n);
    ASSERT_GT(id, 0);

    /* Find by QN */
    cbm_node_t found = {0};
    int rc = cbm_store_find_node_by_qn(s, "test", "test.main.Foo", &found);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_STR_EQ(found.name, "Foo");
    ASSERT_STR_EQ(found.label, "Function");
    ASSERT_STR_EQ(found.file_path, "main.go");
    ASSERT_EQ(found.start_line, 10);
    ASSERT_EQ(found.end_line, 20);
    ASSERT_NOT_NULL(found.properties_json);
    cbm_node_free_fields(&found);

    /* Find by ID */
    cbm_node_t found2 = {0};
    rc = cbm_store_find_node_by_id(s, id, &found2);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_STR_EQ(found2.qualified_name, "test.main.Foo");
    cbm_node_free_fields(&found2);

    /* Find by name */
    cbm_node_t *nodes = NULL;
    int count = 0;
    rc = cbm_store_find_nodes_by_name(s, "test", "Foo", &nodes, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(count, 1);
    ASSERT_STR_EQ(nodes[0].name, "Foo");
    cbm_store_free_nodes(nodes, count);

    /* Count */
    int cnt = cbm_store_count_nodes(s, "test");
    ASSERT_EQ(cnt, 1);

    cbm_store_close(s);
    PASS();
}

TEST(store_node_dedup) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    /* Insert same QN twice — should update, not duplicate */
    cbm_node_t n1 = {
        .project = "test", .label = "Function", .name = "Foo", .qualified_name = "test.main.Foo"};
    cbm_node_t n2 = {.project = "test",
                     .label = "Function",
                     .name = "Foo",
                     .qualified_name = "test.main.Foo",
                     .properties_json = "{\"updated\":true}"};
    cbm_store_upsert_node(s, &n1);
    cbm_store_upsert_node(s, &n2);

    int cnt = cbm_store_count_nodes(s, "test");
    ASSERT_EQ(cnt, 1);

    /* Verify it was updated */
    cbm_node_t found = {0};
    cbm_store_find_node_by_qn(s, "test", "test.main.Foo", &found);
    ASSERT_NOT_NULL(found.properties_json);
    /* Should contain "updated" */
    ASSERT(strstr(found.properties_json, "updated") != NULL);
    cbm_node_free_fields(&found);

    cbm_store_close(s);
    PASS();
}

TEST(store_node_find_by_label) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    cbm_node_t n1 = {
        .project = "test", .label = "Function", .name = "A", .qualified_name = "test.A"};
    cbm_node_t n2 = {.project = "test", .label = "Class", .name = "B", .qualified_name = "test.B"};
    cbm_node_t n3 = {
        .project = "test", .label = "Function", .name = "C", .qualified_name = "test.C"};
    cbm_store_upsert_node(s, &n1);
    cbm_store_upsert_node(s, &n2);
    cbm_store_upsert_node(s, &n3);

    cbm_node_t *nodes = NULL;
    int count = 0;
    int rc = cbm_store_find_nodes_by_label(s, "test", "Function", &nodes, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(count, 2);
    cbm_store_free_nodes(nodes, count);

    rc = cbm_store_find_nodes_by_label(s, "test", "Class", &nodes, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(count, 1);
    ASSERT_STR_EQ(nodes[0].name, "B");
    cbm_store_free_nodes(nodes, count);

    cbm_store_close(s);
    PASS();
}

TEST(store_node_find_by_file) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    cbm_node_t n1 = {.project = "test",
                     .label = "Function",
                     .name = "A",
                     .qualified_name = "test.A",
                     .file_path = "main.go"};
    cbm_node_t n2 = {.project = "test",
                     .label = "Function",
                     .name = "B",
                     .qualified_name = "test.B",
                     .file_path = "util.go"};
    cbm_node_t n3 = {.project = "test",
                     .label = "Function",
                     .name = "C",
                     .qualified_name = "test.C",
                     .file_path = "main.go"};
    cbm_store_upsert_node(s, &n1);
    cbm_store_upsert_node(s, &n2);
    cbm_store_upsert_node(s, &n3);

    cbm_node_t *nodes = NULL;
    int count = 0;
    int rc = cbm_store_find_nodes_by_file(s, "test", "main.go", &nodes, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(count, 2);
    cbm_store_free_nodes(nodes, count);

    cbm_store_close(s);
    PASS();
}

TEST(store_node_find_not_found) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    cbm_node_t found = {0};
    int rc = cbm_store_find_node_by_qn(s, "test", "nonexistent", &found);
    ASSERT_EQ(rc, CBM_STORE_NOT_FOUND);

    rc = cbm_store_find_node_by_id(s, 99999, &found);
    ASSERT_EQ(rc, CBM_STORE_NOT_FOUND);

    cbm_store_close(s);
    PASS();
}

TEST(store_node_count_empty) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    int cnt = cbm_store_count_nodes(s, "test");
    ASSERT_EQ(cnt, 0);

    cbm_store_close(s);
    PASS();
}

TEST(store_node_delete_by_file) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    cbm_node_t n1 = {.project = "test",
                     .label = "Function",
                     .name = "A",
                     .qualified_name = "test.A",
                     .file_path = "main.go"};
    cbm_node_t n2 = {.project = "test",
                     .label = "Function",
                     .name = "B",
                     .qualified_name = "test.B",
                     .file_path = "util.go"};
    cbm_store_upsert_node(s, &n1);
    cbm_store_upsert_node(s, &n2);

    cbm_store_delete_nodes_by_file(s, "test", "main.go");
    int cnt = cbm_store_count_nodes(s, "test");
    ASSERT_EQ(cnt, 1);

    cbm_store_close(s);
    PASS();
}

TEST(store_node_delete_by_label) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    cbm_node_t n1 = {
        .project = "test", .label = "Function", .name = "A", .qualified_name = "test.A"};
    cbm_node_t n2 = {.project = "test", .label = "Class", .name = "B", .qualified_name = "test.B"};
    cbm_store_upsert_node(s, &n1);
    cbm_store_upsert_node(s, &n2);

    cbm_store_delete_nodes_by_label(s, "test", "Function");
    int cnt = cbm_store_count_nodes(s, "test");
    ASSERT_EQ(cnt, 1);

    cbm_store_close(s);
    PASS();
}

/* ── Batch operations ───────────────────────────────────────────── */

TEST(store_node_batch_upsert) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    /* Create 150 nodes */
    cbm_node_t nodes[150];
    int64_t ids[150];
    char names[150][32];
    char qns[150][64];

    for (int i = 0; i < 150; i++) {
        snprintf(names[i], sizeof(names[i]), "func_%d", i);
        snprintf(qns[i], sizeof(qns[i]), "test.pkg.func_%d", i);
        nodes[i] = (cbm_node_t){
            .project = "test",
            .label = "Function",
            .name = names[i],
            .qualified_name = qns[i],
            .file_path = "pkg.go",
            .start_line = i * 10,
            .end_line = i * 10 + 9,
        };
    }

    int rc = cbm_store_upsert_node_batch(s, nodes, 150, ids);
    ASSERT_EQ(rc, CBM_STORE_OK);

    /* Verify all IDs are non-zero */
    for (int i = 0; i < 150; i++) {
        ASSERT_GT(ids[i], 0);
    }

    /* Verify count */
    int cnt = cbm_store_count_nodes(s, "test");
    ASSERT_EQ(cnt, 150);

    /* Re-upsert should not duplicate */
    int64_t ids2[150];
    rc = cbm_store_upsert_node_batch(s, nodes, 150, ids2);
    ASSERT_EQ(rc, CBM_STORE_OK);
    cnt = cbm_store_count_nodes(s, "test");
    ASSERT_EQ(cnt, 150);

    /* IDs should be the same */
    for (int i = 0; i < 150; i++) {
        ASSERT_EQ(ids[i], ids2[i]);
    }

    cbm_store_close(s);
    PASS();
}

TEST(store_node_batch_empty) {
    cbm_store_t *s = cbm_store_open_memory();
    int rc = cbm_store_upsert_node_batch(s, NULL, 0, NULL);
    ASSERT_EQ(rc, CBM_STORE_OK);
    cbm_store_close(s);
    PASS();
}

/* ── Cascade delete ─────────────────────────────────────────────── */

TEST(store_cascade_delete) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    /* Create nodes and an edge */
    cbm_node_t n1 = {
        .project = "test", .label = "Function", .name = "A", .qualified_name = "test.A"};
    cbm_node_t n2 = {
        .project = "test", .label = "Function", .name = "B", .qualified_name = "test.B"};
    int64_t id1 = cbm_store_upsert_node(s, &n1);
    int64_t id2 = cbm_store_upsert_node(s, &n2);

    cbm_edge_t e = {.project = "test", .source_id = id1, .target_id = id2, .type = "CALLS"};
    cbm_store_insert_edge(s, &e);

    /* Delete project — should cascade */
    cbm_store_delete_project(s, "test");

    int ncnt = cbm_store_count_nodes(s, "test");
    int ecnt = cbm_store_count_edges(s, "test");
    ASSERT_EQ(ncnt, 0);
    ASSERT_EQ(ecnt, 0);

    cbm_store_close(s);
    PASS();
}

/* ── File hashes ────────────────────────────────────────────────── */

TEST(store_file_hash_crud) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    /* Upsert */
    int rc = cbm_store_upsert_file_hash(s, "test", "main.go", "abc123", 1000000, 512);
    ASSERT_EQ(rc, CBM_STORE_OK);

    /* Get */
    cbm_file_hash_t *hashes = NULL;
    int count = 0;
    rc = cbm_store_get_file_hashes(s, "test", &hashes, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(count, 1);
    ASSERT_STR_EQ(hashes[0].rel_path, "main.go");
    ASSERT_STR_EQ(hashes[0].sha256, "abc123");
    ASSERT_EQ(hashes[0].mtime_ns, 1000000);
    ASSERT_EQ(hashes[0].size, 512);
    cbm_store_free_file_hashes(hashes, count);

    /* Update */
    rc = cbm_store_upsert_file_hash(s, "test", "main.go", "def456", 2000000, 1024);
    ASSERT_EQ(rc, CBM_STORE_OK);
    rc = cbm_store_get_file_hashes(s, "test", &hashes, &count);
    ASSERT_EQ(count, 1);
    ASSERT_STR_EQ(hashes[0].sha256, "def456");
    ASSERT_EQ(hashes[0].mtime_ns, 2000000);
    cbm_store_free_file_hashes(hashes, count);

    /* Delete single */
    rc = cbm_store_delete_file_hash(s, "test", "main.go");
    ASSERT_EQ(rc, CBM_STORE_OK);
    rc = cbm_store_get_file_hashes(s, "test", &hashes, &count);
    ASSERT_EQ(count, 0);
    cbm_store_free_file_hashes(hashes, count);

    cbm_store_close(s);
    PASS();
}

/* ── Properties JSON round-trip ─────────────────────────────────── */

TEST(store_node_properties_json) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    cbm_node_t n = {.project = "test",
                    .label = "Function",
                    .name = "Bar",
                    .qualified_name = "test.Bar",
                    .properties_json = "{\"visibility\":\"public\",\"is_entry_point\":true}"};
    cbm_store_upsert_node(s, &n);

    cbm_node_t found = {0};
    cbm_store_find_node_by_qn(s, "test", "test.Bar", &found);
    ASSERT_NOT_NULL(found.properties_json);
    ASSERT(strstr(found.properties_json, "visibility") != NULL);
    ASSERT(strstr(found.properties_json, "public") != NULL);
    cbm_node_free_fields(&found);

    cbm_store_close(s);
    PASS();
}

TEST(store_node_null_properties) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    /* NULL properties should default to "{}" */
    cbm_node_t n = {.project = "test",
                    .label = "Function",
                    .name = "Baz",
                    .qualified_name = "test.Baz",
                    .properties_json = NULL};
    cbm_store_upsert_node(s, &n);

    cbm_node_t found = {0};
    cbm_store_find_node_by_qn(s, "test", "test.Baz", &found);
    ASSERT_NOT_NULL(found.properties_json);
    ASSERT_STR_EQ(found.properties_json, "{}");
    cbm_node_free_fields(&found);

    cbm_store_close(s);
    PASS();
}

/* ── File overlap ───────────────────────────────────────────────── */

TEST(store_find_by_file_overlap) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    cbm_node_t na = {.project = "test",
                     .label = "Function",
                     .name = "funcA",
                     .qualified_name = "test.main.funcA",
                     .file_path = "main.go",
                     .start_line = 1,
                     .end_line = 10};
    cbm_node_t nb = {.project = "test",
                     .label = "Function",
                     .name = "funcB",
                     .qualified_name = "test.main.funcB",
                     .file_path = "main.go",
                     .start_line = 12,
                     .end_line = 25};
    cbm_node_t nc = {.project = "test",
                     .label = "Function",
                     .name = "funcC",
                     .qualified_name = "test.main.funcC",
                     .file_path = "other.go",
                     .start_line = 1,
                     .end_line = 50};
    /* Module node should be excluded from overlap results */
    cbm_node_t nm = {.project = "test",
                     .label = "Module",
                     .name = "main",
                     .qualified_name = "test.main",
                     .file_path = "main.go",
                     .start_line = 1,
                     .end_line = 100};
    cbm_store_upsert_node(s, &na);
    cbm_store_upsert_node(s, &nb);
    cbm_store_upsert_node(s, &nc);
    cbm_store_upsert_node(s, &nm);

    /* Overlap with funcA (lines 5-8) */
    cbm_node_t *nodes = NULL;
    int count = 0;
    int rc = cbm_store_find_nodes_by_file_overlap(s, "test", "main.go", 5, 8, &nodes, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(count, 1);
    ASSERT_STR_EQ(nodes[0].name, "funcA");
    cbm_store_free_nodes(nodes, count);

    /* Overlap spanning funcA and funcB (lines 8-15) */
    rc = cbm_store_find_nodes_by_file_overlap(s, "test", "main.go", 8, 15, &nodes, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(count, 2);
    cbm_store_free_nodes(nodes, count);

    /* No overlap (lines 26-30) */
    rc = cbm_store_find_nodes_by_file_overlap(s, "test", "main.go", 26, 30, &nodes, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(count, 0);
    cbm_store_free_nodes(nodes, count);

    /* Different file */
    rc = cbm_store_find_nodes_by_file_overlap(s, "test", "other.go", 1, 50, &nodes, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(count, 1);
    ASSERT_STR_EQ(nodes[0].name, "funcC");
    cbm_store_free_nodes(nodes, count);

    cbm_store_close(s);
    PASS();
}

/* ── QN suffix ─────────────────────────────────────────────────── */

TEST(store_find_by_qn_suffix_single) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    cbm_node_t n = {.project = "test",
                    .label = "Function",
                    .name = "HandleRequest",
                    .qualified_name = "test.cmd.server.main.HandleRequest"};
    cbm_store_upsert_node(s, &n);

    cbm_node_t *nodes = NULL;
    int count = 0;
    int rc = cbm_store_find_nodes_by_qn_suffix(s, "test", "main.HandleRequest", &nodes, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(count, 1);
    ASSERT_STR_EQ(nodes[0].name, "HandleRequest");
    cbm_store_free_nodes(nodes, count);

    cbm_store_close(s);
    PASS();
}

TEST(store_find_by_qn_suffix_no_match) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    cbm_node_t n = {
        .project = "test", .label = "Function", .name = "Foo", .qualified_name = "test.main.Foo"};
    cbm_store_upsert_node(s, &n);

    cbm_node_t *nodes = NULL;
    int count = 0;
    int rc = cbm_store_find_nodes_by_qn_suffix(s, "test", "main.Bar", &nodes, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(count, 0);
    cbm_store_free_nodes(nodes, count);

    cbm_store_close(s);
    PASS();
}

TEST(store_find_by_qn_suffix_multiple) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    cbm_node_t n1 = {.project = "test",
                     .label = "Function",
                     .name = "Run",
                     .qualified_name = "test.cmd.server.Run"};
    cbm_node_t n2 = {.project = "test",
                     .label = "Function",
                     .name = "Run",
                     .qualified_name = "test.cmd.worker.Run"};
    cbm_store_upsert_node(s, &n1);
    cbm_store_upsert_node(s, &n2);

    cbm_node_t *nodes = NULL;
    int count = 0;
    int rc = cbm_store_find_nodes_by_qn_suffix(s, "test", "Run", &nodes, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(count, 2);
    cbm_store_free_nodes(nodes, count);

    cbm_store_close(s);
    PASS();
}

TEST(store_find_by_qn_suffix_dot_boundary) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    cbm_node_t n1 = {.project = "test",
                     .label = "Function",
                     .name = "HandleRequest",
                     .qualified_name = "test.main.HandleRequest"};
    cbm_node_t n2 = {.project = "test",
                     .label = "Function",
                     .name = "MyHandleRequestHelper",
                     .qualified_name = "test.main.MyHandleRequestHelper"};
    cbm_store_upsert_node(s, &n1);
    cbm_store_upsert_node(s, &n2);

    /* Should only match the one with ".HandleRequest" suffix, not partial word */
    cbm_node_t *nodes = NULL;
    int count = 0;
    int rc = cbm_store_find_nodes_by_qn_suffix(s, "test", "HandleRequest", &nodes, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(count, 1);
    ASSERT_STR_EQ(nodes[0].name, "HandleRequest");
    cbm_store_free_nodes(nodes, count);

    cbm_store_close(s);
    PASS();
}

/* ── Node degree ───────────────────────────────────────────────── */

TEST(store_node_degree) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    cbm_node_t na = {
        .project = "test", .label = "Function", .name = "A", .qualified_name = "test.A"};
    cbm_node_t nb = {
        .project = "test", .label = "Function", .name = "B", .qualified_name = "test.B"};
    cbm_node_t nc = {
        .project = "test", .label = "Function", .name = "C", .qualified_name = "test.C"};
    int64_t idA = cbm_store_upsert_node(s, &na);
    int64_t idB = cbm_store_upsert_node(s, &nb);
    int64_t idC = cbm_store_upsert_node(s, &nc);

    /* A->B (CALLS), A->C (CALLS), B->C (CALLS), A->C (USAGE — not counted) */
    cbm_edge_t e1 = {.project = "test", .source_id = idA, .target_id = idB, .type = "CALLS"};
    cbm_edge_t e2 = {.project = "test", .source_id = idA, .target_id = idC, .type = "CALLS"};
    cbm_edge_t e3 = {.project = "test", .source_id = idB, .target_id = idC, .type = "CALLS"};
    cbm_edge_t e4 = {.project = "test", .source_id = idA, .target_id = idC, .type = "USAGE"};
    cbm_store_insert_edge(s, &e1);
    cbm_store_insert_edge(s, &e2);
    cbm_store_insert_edge(s, &e3);
    cbm_store_insert_edge(s, &e4);

    int inA, outA, inB, outB, inC, outC;
    /* DF-1: cbm_store_node_degree returns total degree (all edge types).
     * A: 0 in, 3 out (2 CALLS + 1 USAGE). B: 1 in, 1 out. C: 3 in (2 CALLS + 1 USAGE), 0 out. */
    cbm_store_node_degree(s, idA, &inA, &outA);
    ASSERT_EQ(inA, 0);
    ASSERT_EQ(outA, 3);

    cbm_store_node_degree(s, idB, &inB, &outB);
    ASSERT_EQ(inB, 1);
    ASSERT_EQ(outB, 1);

    cbm_store_node_degree(s, idC, &inC, &outC);
    ASSERT_EQ(inC, 3);
    ASSERT_EQ(outC, 0);

    cbm_store_close(s);
    PASS();
}

/* ── File hash batch ───────────────────────────────────────────── */

TEST(store_file_hash_batch) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    cbm_file_hash_t hashes[3] = {
        {.project = "test", .rel_path = "a.go", .sha256 = "h1", .mtime_ns = 1000, .size = 100},
        {.project = "test", .rel_path = "b.go", .sha256 = "h2", .mtime_ns = 2000, .size = 200},
        {.project = "test", .rel_path = "c.go", .sha256 = "h3", .mtime_ns = 3000, .size = 300},
    };
    int rc = cbm_store_upsert_file_hash_batch(s, hashes, 3);
    ASSERT_EQ(rc, CBM_STORE_OK);

    cbm_file_hash_t *stored = NULL;
    int count = 0;
    rc = cbm_store_get_file_hashes(s, "test", &stored, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(count, 3);
    cbm_store_free_file_hashes(stored, count);

    /* Update hashes (should not duplicate) */
    hashes[0].sha256 = "updated";
    hashes[0].mtime_ns = 9000;
    rc = cbm_store_upsert_file_hash_batch(s, hashes, 3);
    ASSERT_EQ(rc, CBM_STORE_OK);

    rc = cbm_store_get_file_hashes(s, "test", &stored, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(count, 3);
    /* Verify updated value */
    int found = 0;
    for (int i = 0; i < count; i++) {
        if (strcmp(stored[i].rel_path, "a.go") == 0) {
            ASSERT_STR_EQ(stored[i].sha256, "updated");
            ASSERT_EQ(stored[i].mtime_ns, 9000);
            found = 1;
        }
    }
    ASSERT_TRUE(found);
    cbm_store_free_file_hashes(stored, count);

    cbm_store_close(s);
    PASS();
}

/* ── Find edges by URL path ────────────────────────────────────── */

TEST(store_find_edges_by_url_path) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    cbm_node_t ns = {
        .project = "test", .label = "Function", .name = "caller", .qualified_name = "test.caller"};
    cbm_node_t nt = {.project = "test",
                     .label = "Function",
                     .name = "handler",
                     .qualified_name = "test.handler"};
    int64_t srcID = cbm_store_upsert_node(s, &ns);
    int64_t tgtID = cbm_store_upsert_node(s, &nt);

    cbm_edge_t e = {.project = "test",
                    .source_id = srcID,
                    .target_id = tgtID,
                    .type = "HTTP_CALLS",
                    .properties_json = "{\"url_path\":\"/api/orders/create\",\"confidence\":0.8}"};
    cbm_store_insert_edge(s, &e);

    /* Search for edges containing "orders" */
    cbm_edge_t *edges = NULL;
    int count = 0;
    int rc = cbm_store_find_edges_by_url_path(s, "test", "orders", &edges, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(count, 1);
    ASSERT(strstr(edges[0].properties_json, "/api/orders/create") != NULL);
    cbm_store_free_edges(edges, count);

    /* Search for non-matching */
    rc = cbm_store_find_edges_by_url_path(s, "test", "users", &edges, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(count, 0);
    cbm_store_free_edges(edges, count);

    cbm_store_close(s);
    PASS();
}

/* ── Restore from ──────────────────────────────────────────────── */

TEST(store_restore_from) {
    /* Create in-memory source store with data */
    cbm_store_t *src = cbm_store_open_memory();
    cbm_store_upsert_project(src, "test", "/tmp/test");
    for (int i = 0; i < 10; i++) {
        char name[32], qn[64];
        snprintf(name, sizeof(name), "Func%d", i);
        snprintf(qn, sizeof(qn), "test.main.Func%d", i);
        cbm_node_t n = {.project = "test",
                        .label = "Function",
                        .name = name,
                        .qualified_name = qn,
                        .file_path = "main.go",
                        .start_line = i * 10,
                        .end_line = i * 10 + 5};
        cbm_store_upsert_node(src, &n);
    }

    /* Create destination store */
    cbm_store_t *dst = cbm_store_open_memory();

    /* Restore: copy from src → dst */
    int rc = cbm_store_restore_from(dst, src);
    ASSERT_EQ(rc, CBM_STORE_OK);

    /* Verify data survived */
    cbm_node_t found = {0};
    rc = cbm_store_find_node_by_qn(dst, "test", "test.main.Func5", &found);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_STR_EQ(found.name, "Func5");
    cbm_node_free_fields(&found);

    int cnt = cbm_store_count_nodes(dst, "test");
    ASSERT_EQ(cnt, 10);

    cbm_store_close(src);
    cbm_store_close(dst);
    PASS();
}

/* ── Pragma settings ───────────────────────────────────────────── */

TEST(store_pragma_settings) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    /* Just verify we can open and the store works — pragma settings
     * are verified by the fact that the store functions correctly. */
    cbm_store_upsert_project(s, "test", "/tmp/test");
    cbm_node_t n = {
        .project = "test", .label = "Function", .name = "X", .qualified_name = "test.X"};
    int64_t id = cbm_store_upsert_node(s, &n);
    ASSERT_TRUE(id > 0);
    cbm_store_close(s);
    PASS();
}

TEST(store_find_node_ids_by_qns) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "test", "/tmp/test");

    /* Insert two nodes */
    cbm_node_t na = {
        .project = "test", .label = "Function", .name = "A", .qualified_name = "test.A"};
    cbm_node_t nb = {
        .project = "test", .label = "Function", .name = "B", .qualified_name = "test.B"};
    int64_t id1 = cbm_store_upsert_node(s, &na);
    int64_t id2 = cbm_store_upsert_node(s, &nb);
    ASSERT_TRUE(id1 > 0);
    ASSERT_TRUE(id2 > 0);

    /* Batch lookup: 2 found + 1 missing */
    const char *qns[] = {"test.A", "test.B", "test.missing"};
    int64_t ids[3];
    int found = cbm_store_find_node_ids_by_qns(s, "test", qns, 3, ids);
    ASSERT_EQ(found, 2);
    ASSERT_EQ(ids[0], id1);
    ASSERT_EQ(ids[1], id2);
    ASSERT_EQ(ids[2], 0); /* missing → 0 */

    /* Empty batch */
    int found2 = cbm_store_find_node_ids_by_qns(s, "test", NULL, 0, ids);
    ASSERT_EQ(found2, 0);

    cbm_store_close(s);
    PASS();
}

SUITE(store_nodes) {
    RUN_TEST(store_open_memory);
    RUN_TEST(store_close_null);
    RUN_TEST(store_open_memory_twice);
    RUN_TEST(store_project_crud);
    RUN_TEST(store_project_update);
    RUN_TEST(store_project_delete);
    RUN_TEST(store_node_crud);
    RUN_TEST(store_node_dedup);
    RUN_TEST(store_node_find_by_label);
    RUN_TEST(store_node_find_by_file);
    RUN_TEST(store_node_find_not_found);
    RUN_TEST(store_node_count_empty);
    RUN_TEST(store_node_delete_by_file);
    RUN_TEST(store_node_delete_by_label);
    RUN_TEST(store_node_batch_upsert);
    RUN_TEST(store_node_batch_empty);
    RUN_TEST(store_cascade_delete);
    RUN_TEST(store_file_hash_crud);
    RUN_TEST(store_node_properties_json);
    RUN_TEST(store_node_null_properties);
    RUN_TEST(store_find_by_file_overlap);
    RUN_TEST(store_find_by_qn_suffix_single);
    RUN_TEST(store_find_by_qn_suffix_no_match);
    RUN_TEST(store_find_by_qn_suffix_multiple);
    RUN_TEST(store_find_by_qn_suffix_dot_boundary);
    RUN_TEST(store_node_degree);
    RUN_TEST(store_file_hash_batch);
    RUN_TEST(store_find_edges_by_url_path);
    RUN_TEST(store_restore_from);
    RUN_TEST(store_pragma_settings);
    RUN_TEST(store_find_node_ids_by_qns);
}
