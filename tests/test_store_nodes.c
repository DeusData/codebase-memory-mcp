/*
 * test_store_nodes.c — Tests for store schema, project CRUD, and node CRUD.
 *
 * Ported from internal/store/store_test.go (TestOpenMemory, TestNodeCRUD,
 * TestNodeDedup, TestProjectCRUD, TestUpsertNodeBatch, etc.)
 */
#include "test_framework.h"
#include "test_graph_diff.h"
#include "test_helpers.h"
#include "test_sqlite_helpers.h"
#include <foundation/compat.h>
#include <foundation/compat_fs.h>
#include <foundation/constants.h>
#include <store/store.h>
#include <sqlite3.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

/* ── Schema / Open / Close ──────────────────────────────────────── */

enum { STORE_TEST_SQLITE_AUTO_LEN = -1 };
enum {
    STORE_TEST_BIND_PROJECT = 1,
    STORE_TEST_BIND_GENERATION = 2,
    STORE_TEST_BIND_STATUS = 3,
    STORE_TEST_BIND_REPO_FINGERPRINT = 4,
    STORE_TEST_BIND_CONFIG_FINGERPRINT = 5,
    STORE_TEST_BIND_COMPLETED_STATE = 6,
};
enum {
    STORE_TEST_COMPLETED_NULL = 0,
    STORE_TEST_COMPLETED_SET = 1,
};
typedef enum {
    STORE_TEST_OVERLAY_NODES,
    STORE_TEST_OVERLAY_EDGES,
    STORE_TEST_OVERLAY_TOMBSTONES,
} store_test_overlay_table_t;
enum {
    STORE_TEST_OVERLAY_ROW_CONTEXT = 0,
    STORE_TEST_OVERLAY_ROW_OWNED = 1,
};

static const char STORE_TEST_INVALID_DERIVED_STATUS[] = "fresh-ish";
static const char *const STORE_TEST_GRAPH_DERIVED_VIEWS[] = {
    CBM_STORE_DERIVED_VIEW_PAGERANK,        CBM_STORE_DERIVED_VIEW_LINKRANK,
    CBM_STORE_DERIVED_VIEW_NODE_DEGREE,     CBM_STORE_DERIVED_VIEW_SEMANTIC_EDGES,
    CBM_STORE_DERIVED_VIEW_ROUTES,          CBM_STORE_DERIVED_VIEW_ARCHITECTURE,
};

static int store_publish_helper_file_delta(cbm_store_t *s, int64_t generation);
static int store_publish_old_main_delta(cbm_store_t *s, int64_t generation);

static int store_count_index_generation(cbm_store_t *s, const char *project, int64_t generation,
                                        const char *status, const char *repo_fingerprint,
                                        const char *config_fingerprint, int completed_state) {
    sqlite3_stmt *stmt = NULL;
    int count = CBM_STORE_ERR;
    sqlite3 *db = cbm_store_get_db(s);
    const char *sql = "SELECT COUNT(*) FROM index_generations "
                      "WHERE project = ?1 AND generation = ?2 AND status = ?3 "
                      "AND repo_fingerprint = ?4 AND config_fingerprint = ?5 "
                      "AND ((?6 = 0 AND completed_at IS NULL) OR "
                      "(?6 = 1 AND completed_at IS NOT NULL)) AND started_at <> ''";
    if (!db || sqlite3_prepare_v2(db, sql, STORE_TEST_SQLITE_AUTO_LEN, &stmt, NULL) !=
                   SQLITE_OK) {
        return CBM_STORE_ERR;
    }
    sqlite3_bind_text(stmt, STORE_TEST_BIND_PROJECT, project, STORE_TEST_SQLITE_AUTO_LEN,
                      SQLITE_STATIC);
    sqlite3_bind_int64(stmt, STORE_TEST_BIND_GENERATION, generation);
    sqlite3_bind_text(stmt, STORE_TEST_BIND_STATUS, status, STORE_TEST_SQLITE_AUTO_LEN,
                      SQLITE_STATIC);
    sqlite3_bind_text(stmt, STORE_TEST_BIND_REPO_FINGERPRINT, repo_fingerprint,
                      STORE_TEST_SQLITE_AUTO_LEN, SQLITE_STATIC);
    sqlite3_bind_text(stmt, STORE_TEST_BIND_CONFIG_FINGERPRINT, config_fingerprint,
                      STORE_TEST_SQLITE_AUTO_LEN, SQLITE_STATIC);
    sqlite3_bind_int(stmt, STORE_TEST_BIND_COMPLETED_STATE, completed_state);
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        count = sqlite3_column_int(stmt, 0);
    }
    sqlite3_finalize(stmt);
    return count;
}

static int store_count_overlay_generation_row(cbm_store_t *s, const char *project,
                                              int64_t overlay_generation,
                                              int64_t base_generation, const char *status) {
    sqlite3_stmt *stmt = NULL;
    int count = CBM_STORE_ERR;
    sqlite3 *db = cbm_store_get_db(s);
    const char *sql = "SELECT COUNT(*) FROM overlay_generations "
                      "WHERE project = ?1 AND overlay_generation = ?2 "
                      "AND base_generation = ?3 AND status = ?4 "
                      "AND created_at <> '' AND updated_at <> ''";
    if (!db || sqlite3_prepare_v2(db, sql, STORE_TEST_SQLITE_AUTO_LEN, &stmt, NULL) !=
                   SQLITE_OK) {
        return CBM_STORE_ERR;
    }
    sqlite3_bind_text(stmt, 1, project, STORE_TEST_SQLITE_AUTO_LEN, SQLITE_STATIC);
    sqlite3_bind_int64(stmt, 2, overlay_generation);
    sqlite3_bind_int64(stmt, 3, base_generation);
    sqlite3_bind_text(stmt, 4, status, STORE_TEST_SQLITE_AUTO_LEN, SQLITE_STATIC);
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        count = sqlite3_column_int(stmt, 0);
    }
    sqlite3_finalize(stmt);
    return count;
}

static const char *store_overlay_count_sql(store_test_overlay_table_t table) {
    switch (table) {
    case STORE_TEST_OVERLAY_NODES:
        return "SELECT COUNT(*) FROM overlay_nodes WHERE project = ?1 "
               "AND overlay_generation = ?2 AND rel_path = ?3";
    case STORE_TEST_OVERLAY_EDGES:
        return "SELECT COUNT(*) FROM overlay_edges WHERE project = ?1 "
               "AND overlay_generation = ?2 AND rel_path = ?3";
    case STORE_TEST_OVERLAY_TOMBSTONES:
        return "SELECT COUNT(*) FROM overlay_tombstones WHERE project = ?1 "
               "AND overlay_generation = ?2 AND rel_path = ?3";
    }
    return NULL;
}

static const char *store_overlay_owned_count_sql(store_test_overlay_table_t table) {
    switch (table) {
    case STORE_TEST_OVERLAY_NODES:
        return "SELECT COUNT(*) FROM overlay_nodes WHERE project = ?1 "
               "AND overlay_generation = ?2 AND rel_path = ?3 AND owned = ?4";
    case STORE_TEST_OVERLAY_EDGES:
        return "SELECT COUNT(*) FROM overlay_edges WHERE project = ?1 "
               "AND overlay_generation = ?2 AND rel_path = ?3 AND owned = ?4";
    case STORE_TEST_OVERLAY_TOMBSTONES:
        return NULL;
    }
    return NULL;
}

static int store_count_overlay_rows(cbm_store_t *s, store_test_overlay_table_t table,
                                    const char *project,
                                    int64_t overlay_generation, const char *rel_path) {
    sqlite3_stmt *stmt = NULL;
    int count = CBM_STORE_ERR;
    sqlite3 *db = cbm_store_get_db(s);
    const char *sql = store_overlay_count_sql(table);
    if (!sql || !db ||
        sqlite3_prepare_v2(db, sql, STORE_TEST_SQLITE_AUTO_LEN, &stmt, NULL) != SQLITE_OK) {
        return CBM_STORE_ERR;
    }
    sqlite3_bind_text(stmt, 1, project, STORE_TEST_SQLITE_AUTO_LEN, SQLITE_STATIC);
    sqlite3_bind_int64(stmt, 2, overlay_generation);
    sqlite3_bind_text(stmt, 3, rel_path, STORE_TEST_SQLITE_AUTO_LEN, SQLITE_STATIC);
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        count = sqlite3_column_int(stmt, 0);
    }
    sqlite3_finalize(stmt);
    return count;
}

static int store_count_overlay_owned_rows(cbm_store_t *s, store_test_overlay_table_t table,
                                          const char *project,
                                          int64_t overlay_generation, const char *rel_path,
                                          int owned) {
    sqlite3_stmt *stmt = NULL;
    int count = CBM_STORE_ERR;
    sqlite3 *db = cbm_store_get_db(s);
    const char *sql = store_overlay_owned_count_sql(table);
    if (!sql || !db ||
        sqlite3_prepare_v2(db, sql, STORE_TEST_SQLITE_AUTO_LEN, &stmt, NULL) != SQLITE_OK) {
        return CBM_STORE_ERR;
    }
    sqlite3_bind_text(stmt, 1, project, STORE_TEST_SQLITE_AUTO_LEN, SQLITE_STATIC);
    sqlite3_bind_int64(stmt, 2, overlay_generation);
    sqlite3_bind_text(stmt, 3, rel_path, STORE_TEST_SQLITE_AUTO_LEN, SQLITE_STATIC);
    sqlite3_bind_int(stmt, 4, owned);
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        count = sqlite3_column_int(stmt, 0);
    }
    sqlite3_finalize(stmt);
    return count;
}

static int store_count_overlay_fts_matches(cbm_store_t *s, const char *project,
                                           int64_t overlay_generation, const char *rel_path,
                                           const char *query) {
    sqlite3_stmt *stmt = NULL;
    int count = CBM_STORE_ERR;
    sqlite3 *db = cbm_store_get_db(s);
    const char *sql =
        "SELECT COUNT(*) FROM " CBM_STORE_DERIVED_VIEW_NODES_FTS_OVERLAY
        " JOIN overlay_nodes n"
        "   ON n.id = " CBM_STORE_DERIVED_VIEW_NODES_FTS_OVERLAY ".rowid"
        " WHERE " CBM_STORE_DERIVED_VIEW_NODES_FTS_OVERLAY " MATCH ?1"
        "   AND n.project = ?2 AND n.overlay_generation = ?3 AND n.rel_path = ?4";
    if (!db || sqlite3_prepare_v2(db, sql, STORE_TEST_SQLITE_AUTO_LEN, &stmt, NULL) !=
                   SQLITE_OK) {
        return CBM_STORE_ERR;
    }
    sqlite3_bind_text(stmt, 1, query, STORE_TEST_SQLITE_AUTO_LEN, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, project, STORE_TEST_SQLITE_AUTO_LEN, SQLITE_STATIC);
    sqlite3_bind_int64(stmt, 3, overlay_generation);
    sqlite3_bind_text(stmt, 4, rel_path, STORE_TEST_SQLITE_AUTO_LEN, SQLITE_STATIC);
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        count = sqlite3_column_int(stmt, 0);
    }
    sqlite3_finalize(stmt);
    return count;
}

static int store_count_overlay_fts_raw_matches(cbm_store_t *s, const char *query) {
    sqlite3_stmt *stmt = NULL;
    int count = CBM_STORE_ERR;
    sqlite3 *db = cbm_store_get_db(s);
    const char *sql =
        "SELECT COUNT(*) FROM " CBM_STORE_DERIVED_VIEW_NODES_FTS_OVERLAY
        " WHERE " CBM_STORE_DERIVED_VIEW_NODES_FTS_OVERLAY " MATCH ?1";
    if (!db || sqlite3_prepare_v2(db, sql, STORE_TEST_SQLITE_AUTO_LEN, &stmt, NULL) !=
                   SQLITE_OK) {
        return CBM_STORE_ERR;
    }
    sqlite3_bind_text(stmt, 1, query, STORE_TEST_SQLITE_AUTO_LEN, SQLITE_STATIC);
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        count = sqlite3_column_int(stmt, 0);
    }
    sqlite3_finalize(stmt);
    return count;
}

static int store_count_metadata_owners(cbm_store_t *s, int edge, const char *project,
                                       const char *rel_path) {
    sqlite3_stmt *stmt = NULL;
    int count = CBM_STORE_ERR;
    const char *sql = edge ? "SELECT COUNT(*) FROM edge_owners WHERE project = ?1 AND rel_path = ?2"
                           : "SELECT COUNT(*) FROM node_owners WHERE project = ?1 AND rel_path = ?2";
    sqlite3 *db = cbm_store_get_db(s);
    if (!db || sqlite3_prepare_v2(db, sql, STORE_TEST_SQLITE_AUTO_LEN, &stmt, NULL) !=
                   SQLITE_OK) {
        return CBM_STORE_ERR;
    }
    sqlite3_bind_text(stmt, 1, project, STORE_TEST_SQLITE_AUTO_LEN, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, rel_path, STORE_TEST_SQLITE_AUTO_LEN, SQLITE_STATIC);
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        count = sqlite3_column_int(stmt, 0);
    }
    sqlite3_finalize(stmt);
    return count;
}

static void store_free_string_array(char **items, int count) {
    if (!items) {
        return;
    }
    for (int i = 0; i < count; i++) {
        free(items[i]);
    }
    free(items);
}

static int store_node_qn_exists(cbm_store_t *s, const char *project, const char *qn) {
    cbm_node_t node = {0};
    int rc = cbm_store_find_node_by_qn(s, project, qn, &node);
    if (rc == CBM_STORE_OK) {
        cbm_node_free_fields(&node);
        return 1;
    }
    return 0;
}

static int store_count_derived_view_state(cbm_store_t *s, const char *project,
                                          const char *view_name, int64_t generation,
                                          const char *status) {
    sqlite3_stmt *stmt = NULL;
    int count = CBM_STORE_ERR;
    sqlite3 *db = cbm_store_get_db(s);
    const char *sql = "SELECT COUNT(*) FROM derived_view_state "
                      "WHERE project = ?1 AND view_name = ?2 AND source_generation = ?3 "
                      "AND status = ?4";
    if (!db || sqlite3_prepare_v2(db, sql, STORE_TEST_SQLITE_AUTO_LEN, &stmt, NULL) !=
                   SQLITE_OK) {
        return CBM_STORE_ERR;
    }
    sqlite3_bind_text(stmt, 1, project, STORE_TEST_SQLITE_AUTO_LEN, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, view_name, STORE_TEST_SQLITE_AUTO_LEN, SQLITE_STATIC);
    sqlite3_bind_int64(stmt, 3, generation);
    sqlite3_bind_text(stmt, 4, status, STORE_TEST_SQLITE_AUTO_LEN, SQLITE_STATIC);
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        count = sqlite3_column_int(stmt, 0);
    }
    sqlite3_finalize(stmt);
    return count;
}

static int store_count_stale_graph_derived_views(cbm_store_t *s, const char *project,
                                                 int64_t generation) {
    int total = 0;
    for (size_t i = 0; i < sizeof(STORE_TEST_GRAPH_DERIVED_VIEWS) /
                               sizeof(STORE_TEST_GRAPH_DERIVED_VIEWS[0]);
         i++) {
        int count =
            store_count_derived_view_state(s, project, STORE_TEST_GRAPH_DERIVED_VIEWS[i],
                                           generation, CBM_STORE_DERIVED_STATUS_STALE);
        if (count < 0) {
            return count;
        }
        total += count;
    }
    return total;
}

static int store_graph_derived_view_count(void) {
    return (int)(sizeof(STORE_TEST_GRAPH_DERIVED_VIEWS) /
                 sizeof(STORE_TEST_GRAPH_DERIVED_VIEWS[0]));
}

static int store_string_array_contains(char **items, int count, const char *needle) {
    for (int i = 0; i < count; i++) {
        if (strcmp(items[i], needle) == 0) {
            return 1;
        }
    }
    return 0;
}

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

TEST(store_exact_delta_metadata_schema) {
    static const char *tables[] = {
        "index_generations", "file_state",       "node_owners",        "edge_owners",
        "symbol_exports",   "import_refs",      "derived_view_state",  "overlay_generations",
        "overlay_nodes",    "overlay_edges",    "overlay_tombstones",  "overlay_file_hashes",
        "overlay_file_state", "overlay_symbol_exports", "overlay_import_refs",
        "overlay_delta_meta",
    };
    static const char *indexes[] = {
        "idx_file_state_hash",       "idx_node_owners_path",      "idx_node_owners_node_id",
        "idx_edge_owners_path",      "idx_edge_owners_edge_id",   "idx_symbol_exports_path",
        "idx_symbol_exports_node_id", "idx_import_refs_target",
        "idx_derived_view_state_status", "idx_overlay_generations_status",
        "idx_overlay_nodes_project_gen", "idx_overlay_nodes_project_gen_qn",
        "idx_overlay_edges_project_gen", "idx_overlay_tombstones_project_gen",
        "idx_overlay_file_state_project_gen", "idx_overlay_import_refs_target",
        "idx_overlay_symbol_exports_path", "idx_overlay_delta_meta_project_gen",
    };

    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    sqlite3 *db = cbm_store_get_db(s);
    ASSERT_NOT_NULL(db);

    for (size_t i = 0; i < sizeof(tables) / sizeof(tables[0]); i++) {
        ASSERT_TRUE(cbm_test_sqlite_object_exists(db, "table", tables[i]));
    }
    for (size_t i = 0; i < sizeof(indexes) / sizeof(indexes[0]); i++) {
        ASSERT_TRUE(cbm_test_sqlite_object_exists(db, "index", indexes[i]));
    }

    cbm_store_close(s);
    PASS();
}

TEST(store_open_path_query_does_not_create_missing_db) {
    char path[CBM_SZ_256];
    int n = snprintf(path, sizeof(path), "%s/cbm_store_query_missing_XXXXXX", cbm_tmpdir());
    ASSERT_GT(n, 0);
    ASSERT_LT(n, (int)sizeof(path));
    int fd = cbm_mkstemp_s(path, sizeof(path));
    ASSERT_GT(fd, -1);
    cbm_close_fd(fd);
    ASSERT_EQ(cbm_unlink(path), 0);

    cbm_store_t *s = cbm_store_open_path_query(path);
    ASSERT_NULL(s);

    FILE *probe = fopen(path, "rb");
    ASSERT_NULL(probe);
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

TEST(store_project_reads_reset_cached_statements) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "myproject", "/home/user/myproject"), CBM_STORE_OK);

    cbm_project_t p = {0};
    ASSERT_EQ(cbm_store_get_project(s, "myproject", &p), CBM_STORE_OK);
    cbm_project_free_fields(&p);

    cbm_project_t *projects = NULL;
    int count = 0;
    ASSERT_EQ(cbm_store_list_projects(s, &projects, &count), CBM_STORE_OK);
    cbm_store_free_projects(projects, count);

    ASSERT_EQ(cbm_store_drop_indexes(s), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_create_indexes(s), CBM_STORE_OK);

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

typedef struct {
    int count;
    int saw_a;
    int saw_c;
    int saw_other_project;
} store_visit_nodes_by_label_ctx_t;

static int store_visit_nodes_by_label_cb(const char *label, const char *name,
                                         const char *qualified_name, const char *file_path,
                                         void *userdata) {
    store_visit_nodes_by_label_ctx_t *ctx = (store_visit_nodes_by_label_ctx_t *)userdata;
    if (!ctx || !label || !name || !qualified_name || !file_path) {
        return CBM_STORE_ERR;
    }
    ctx->count++;
    if (strcmp(label, "Function") != 0) {
        return CBM_STORE_ERR;
    }
    if (strcmp(name, "A") == 0 && strcmp(qualified_name, "test.A") == 0 &&
        strcmp(file_path, "main.go") == 0) {
        ctx->saw_a = 1;
    }
    if (strcmp(name, "C") == 0 && strcmp(qualified_name, "test.C") == 0 &&
        strcmp(file_path, "util.go") == 0) {
        ctx->saw_c = 1;
    }
    if (strcmp(qualified_name, "other.A") == 0) {
        ctx->saw_other_project = 1;
    }
    return CBM_STORE_OK;
}

TEST(store_visit_nodes_by_label_identity_rows) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");
    cbm_store_upsert_project(s, "other", "/tmp/other");

    cbm_node_t n1 = {.project = "test",
                     .label = "Function",
                     .name = "A",
                     .qualified_name = "test.A",
                     .file_path = "main.go",
                     .properties_json = "{\"ignored\":true}"};
    cbm_node_t n2 = {.project = "test",
                     .label = "Class",
                     .name = "B",
                     .qualified_name = "test.B",
                     .file_path = "main.go"};
    cbm_node_t n3 = {.project = "test",
                     .label = "Function",
                     .name = "C",
                     .qualified_name = "test.C",
                     .file_path = "util.go"};
    cbm_node_t n4 = {.project = "other",
                     .label = "Function",
                     .name = "A",
                     .qualified_name = "other.A",
                     .file_path = "main.go"};
    cbm_store_upsert_node(s, &n1);
    cbm_store_upsert_node(s, &n2);
    cbm_store_upsert_node(s, &n3);
    cbm_store_upsert_node(s, &n4);

    store_visit_nodes_by_label_ctx_t ctx = {0};
    int rc = cbm_store_visit_nodes_by_label(s, "test", "Function",
                                            store_visit_nodes_by_label_cb, &ctx);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(ctx.count, 2);
    ASSERT_EQ(ctx.saw_a, 1);
    ASSERT_EQ(ctx.saw_c, 1);
    ASSERT_EQ(ctx.saw_other_project, 0);

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

TEST(store_file_hash_upsert_rejects_null_required_fields) {
    /* Pins the API contract that `cbm_store_upsert_file_hash` returns
     * CBM_STORE_ERR (not silent OK) when a NOT NULL column would receive
     * SQL NULL. This is the failure mode that
     * `pipeline_incremental.c:persist_hashes` checks for and logs as
     * `incremental.persist_hash_failed`. If this contract ever changes
     * (e.g. the schema relaxes NOT NULL on rel_path or sha256), the
     * downstream warning becomes silent and the orphaned-node bug class
     * can re-emerge. Track that change here, not just in the consumer. */
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "test", "/tmp/test");

    /* Sanity: a fully-valid upsert returns OK. */
    int rc = cbm_store_upsert_file_hash(s, "test", "main.go", "abc123", 1000000, 512);
    ASSERT_EQ(rc, CBM_STORE_OK);

    /* NULL sha256 violates NOT NULL on file_hashes.sha256 → must return ERR. */
    rc = cbm_store_upsert_file_hash(s, "test", "other.go", NULL, 2000000, 1024);
    ASSERT_EQ(rc, CBM_STORE_ERR);

    /* NULL rel_path violates NOT NULL on file_hashes.rel_path → must return ERR. */
    rc = cbm_store_upsert_file_hash(s, "test", NULL, "deadbeef", 3000000, 2048);
    ASSERT_EQ(rc, CBM_STORE_ERR);

    /* NULL project violates NOT NULL on file_hashes.project → must return ERR. */
    rc = cbm_store_upsert_file_hash(s, NULL, "third.go", "cafebabe", 4000000, 4096);
    ASSERT_EQ(rc, CBM_STORE_ERR);

    /* The valid row from earlier must still be present — partial-failure
     * policy: a single bad upsert does not corrupt or remove other rows. */
    cbm_file_hash_t *hashes = NULL;
    int count = 0;
    cbm_store_get_file_hashes(s, "test", &hashes, &count);
    ASSERT_EQ(count, 1);
    ASSERT_STR_EQ(hashes[0].rel_path, "main.go");
    cbm_store_free_file_hashes(hashes, count);

    cbm_store_close(s);
    PASS();
}

TEST(store_file_state_crud) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "test", "/tmp/test");

    cbm_file_state_t state = {
        .project = "test",
        .rel_path = "main.go",
        .content_hash = "content-a",
        .git_oid = "git-a",
        .mtime_ns = 1000000,
        .size = 512,
        .language = "go",
        .pass_fingerprint = "pass-a",
        .generation = 1,
        .indexed_at = "2026-03-14T00:00:00Z",
    };
    int rc = cbm_store_upsert_file_state(s, &state);
    ASSERT_EQ(rc, CBM_STORE_OK);

    cbm_file_state_t got = {0};
    rc = cbm_store_get_file_state(s, "test", "main.go", &got);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_STR_EQ(got.content_hash, "content-a");
    ASSERT_STR_EQ(got.git_oid, "git-a");
    ASSERT_EQ(got.mtime_ns, 1000000);
    ASSERT_EQ(got.size, 512);
    ASSERT_STR_EQ(got.language, "go");
    ASSERT_STR_EQ(got.pass_fingerprint, "pass-a");
    ASSERT_EQ(got.generation, 1);
    ASSERT_STR_EQ(got.indexed_at, "2026-03-14T00:00:00Z");
    cbm_store_file_state_free_fields(&got);

    state.content_hash = "content-b";
    state.git_oid = "";
    state.mtime_ns = 2000000;
    state.size = 1024;
    state.language = "c";
    state.pass_fingerprint = "pass-b";
    state.generation = 2;
    state.indexed_at = "2026-03-15T00:00:00Z";
    rc = cbm_store_upsert_file_state(s, &state);
    ASSERT_EQ(rc, CBM_STORE_OK);

    rc = cbm_store_get_file_state(s, "test", "main.go", &got);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_STR_EQ(got.content_hash, "content-b");
    ASSERT_STR_EQ(got.git_oid, "");
    ASSERT_EQ(got.mtime_ns, 2000000);
    ASSERT_EQ(got.size, 1024);
    ASSERT_STR_EQ(got.language, "c");
    ASSERT_STR_EQ(got.pass_fingerprint, "pass-b");
    ASSERT_EQ(got.generation, 2);
    cbm_store_file_state_free_fields(&got);

    rc = cbm_store_delete_file_state(s, "test", "main.go");
    ASSERT_EQ(rc, CBM_STORE_OK);
    rc = cbm_store_get_file_state(s, "test", "main.go", &got);
    ASSERT_EQ(rc, CBM_STORE_NOT_FOUND);

    cbm_store_close(s);
    PASS();
}

TEST(store_file_state_get_resets_cached_statement) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    cbm_file_state_t state = {
        .project = "test",
        .rel_path = "main.go",
        .content_hash = "content-a",
        .git_oid = "",
        .mtime_ns = 1000000,
        .size = 512,
        .language = "go",
        .pass_fingerprint = "pass-a",
        .generation = 1,
        .indexed_at = "2026-03-14T00:00:00Z",
    };
    ASSERT_EQ(cbm_store_upsert_file_state(s, &state), CBM_STORE_OK);

    cbm_file_state_t got = {0};
    ASSERT_EQ(cbm_store_get_file_state(s, "test", "main.go", &got), CBM_STORE_OK);
    cbm_store_file_state_free_fields(&got);

    ASSERT_EQ(cbm_store_drop_indexes(s), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_create_indexes(s), CBM_STORE_OK);

    cbm_store_close(s);
    PASS();
}

TEST(store_index_generation_reservation_monotonic) {
    enum { FIRST_GENERATION = 1, SECOND_GENERATION = 2 };
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    int64_t generation = 0;
    ASSERT_EQ(cbm_store_reserve_index_generation(s, "test", "repo-a", "config-a", &generation),
              CBM_STORE_OK);
    ASSERT_EQ(generation, FIRST_GENERATION);
    ASSERT_EQ(store_count_index_generation(s, "test", FIRST_GENERATION,
                                           CBM_STORE_INDEX_STATUS_RESERVED, "repo-a",
                                           "config-a", STORE_TEST_COMPLETED_NULL),
              1);

    ASSERT_EQ(cbm_store_reserve_index_generation(s, "test", NULL, NULL, &generation),
              CBM_STORE_OK);
    ASSERT_EQ(generation, SECOND_GENERATION);
    ASSERT_EQ(store_count_index_generation(s, "test", SECOND_GENERATION,
                                           CBM_STORE_INDEX_STATUS_RESERVED, "", "",
                                           STORE_TEST_COMPLETED_NULL),
              1);

    cbm_store_close(s);
    PASS();
}

TEST(store_index_generation_reservation_requires_project) {
    enum { NO_RESERVED_GENERATION = 0, FIRST_GENERATION = 1, SENTINEL_GENERATION = 99 };
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);

    int64_t generation = SENTINEL_GENERATION;
    ASSERT_EQ(cbm_store_reserve_index_generation(s, "missing", "repo-a", "config-a",
                                                 &generation),
              CBM_STORE_ERR);
    ASSERT_EQ(generation, NO_RESERVED_GENERATION);
    ASSERT_EQ(store_count_index_generation(s, "missing", FIRST_GENERATION,
                                           CBM_STORE_INDEX_STATUS_RESERVED, "repo-a", "config-a",
                                           STORE_TEST_COMPLETED_NULL),
              0);

    cbm_store_close(s);
    PASS();
}

TEST(store_index_generation_finish_complete) {
    enum { FIRST_GENERATION = 1 };
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    int64_t generation = 0;
    ASSERT_EQ(cbm_store_reserve_index_generation(s, "test", "repo-a", "config-a", &generation),
              CBM_STORE_OK);
    ASSERT_EQ(generation, FIRST_GENERATION);
    ASSERT_EQ(cbm_store_finish_index_generation(s, "test", generation,
                                                CBM_STORE_INDEX_STATUS_COMPLETE),
              CBM_STORE_OK);
    ASSERT_EQ(store_count_index_generation(s, "test", FIRST_GENERATION,
                                           CBM_STORE_INDEX_STATUS_COMPLETE, "repo-a",
                                           "config-a", STORE_TEST_COMPLETED_SET),
              1);
    ASSERT_EQ(store_count_index_generation(s, "test", FIRST_GENERATION,
                                           CBM_STORE_INDEX_STATUS_RESERVED, "repo-a",
                                           "config-a", STORE_TEST_COMPLETED_NULL),
              0);
    ASSERT_EQ(cbm_store_finish_index_generation(s, "test", generation,
                                                CBM_STORE_INDEX_STATUS_COMPLETE),
              CBM_STORE_NOT_FOUND);

    cbm_store_close(s);
    PASS();
}

TEST(store_latest_complete_index_generation_ignores_reserved_and_failed) {
    enum { FIRST_GENERATION = 1, SECOND_GENERATION = 2, THIRD_GENERATION = 3 };
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    int64_t latest = -1;
    ASSERT_EQ(cbm_store_latest_complete_index_generation(s, "test", &latest), CBM_STORE_OK);
    ASSERT_EQ(latest, 0);

    int64_t generation = 0;
    ASSERT_EQ(cbm_store_reserve_index_generation(s, "test", NULL, NULL, &generation),
              CBM_STORE_OK);
    ASSERT_EQ(generation, FIRST_GENERATION);
    ASSERT_EQ(cbm_store_latest_complete_index_generation(s, "test", &latest), CBM_STORE_OK);
    ASSERT_EQ(latest, 0);

    ASSERT_EQ(cbm_store_finish_index_generation(s, "test", generation,
                                                CBM_STORE_INDEX_STATUS_COMPLETE),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_store_latest_complete_index_generation(s, "test", &latest), CBM_STORE_OK);
    ASSERT_EQ(latest, FIRST_GENERATION);

    ASSERT_EQ(cbm_store_reserve_index_generation(s, "test", NULL, NULL, &generation),
              CBM_STORE_OK);
    ASSERT_EQ(generation, SECOND_GENERATION);
    ASSERT_EQ(cbm_store_finish_index_generation(s, "test", generation,
                                                CBM_STORE_INDEX_STATUS_FAILED),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_store_latest_complete_index_generation(s, "test", &latest), CBM_STORE_OK);
    ASSERT_EQ(latest, FIRST_GENERATION);

    ASSERT_EQ(cbm_store_reserve_index_generation(s, "test", NULL, NULL, &generation),
              CBM_STORE_OK);
    ASSERT_EQ(generation, THIRD_GENERATION);
    ASSERT_EQ(cbm_store_finish_index_generation(s, "test", generation,
                                                CBM_STORE_INDEX_STATUS_COMPLETE),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_store_latest_complete_index_generation(s, "test", &latest), CBM_STORE_OK);
    ASSERT_EQ(latest, THIRD_GENERATION);

    cbm_store_close(s);
    PASS();
}

TEST(store_index_generation_finish_failed_and_invalid_status) {
    enum { FIRST_GENERATION = 1 };
    const char *invalid_status = "invalid-status";
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    int64_t generation = 0;
    ASSERT_EQ(cbm_store_reserve_index_generation(s, "test", NULL, NULL, &generation),
              CBM_STORE_OK);
    ASSERT_EQ(generation, FIRST_GENERATION);
    ASSERT_EQ(cbm_store_finish_index_generation(s, "test", generation, invalid_status),
              CBM_STORE_ERR);
    ASSERT_EQ(store_count_index_generation(s, "test", FIRST_GENERATION,
                                           CBM_STORE_INDEX_STATUS_RESERVED, "", "",
                                           STORE_TEST_COMPLETED_NULL),
              1);

    ASSERT_EQ(cbm_store_finish_index_generation(s, "test", generation,
                                                CBM_STORE_INDEX_STATUS_FAILED),
              CBM_STORE_OK);
    ASSERT_EQ(store_count_index_generation(s, "test", FIRST_GENERATION,
                                           CBM_STORE_INDEX_STATUS_FAILED, "", "",
                                           STORE_TEST_COMPLETED_SET),
              1);

    cbm_store_close(s);
    PASS();
}

TEST(store_overlay_generation_reservation_status_and_counts) {
    enum { FIRST_OVERLAY = 1, SECOND_OVERLAY = 2, BASE_GENERATION = 9 };
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    int64_t overlay_generation = 0;
    ASSERT_EQ(cbm_store_reserve_overlay_generation(s, "test", BASE_GENERATION,
                                                   &overlay_generation),
              CBM_STORE_OK);
    ASSERT_EQ(overlay_generation, FIRST_OVERLAY);
    ASSERT_EQ(store_count_overlay_generation_row(s, "test", FIRST_OVERLAY, BASE_GENERATION,
                                                 CBM_STORE_OVERLAY_STATUS_RESERVED),
              1);

    int count = -1;
    ASSERT_EQ(cbm_store_count_overlay_generations(s, "test", NULL, &count), CBM_STORE_OK);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(cbm_store_count_overlay_generations(s, "test",
                                                  CBM_STORE_OVERLAY_STATUS_RESERVED, &count),
              CBM_STORE_OK);
    ASSERT_EQ(count, 1);

    ASSERT_EQ(cbm_store_set_overlay_generation_status(s, "test", overlay_generation,
                                                      CBM_STORE_OVERLAY_STATUS_READY),
              CBM_STORE_OK);
    ASSERT_EQ(store_count_overlay_generation_row(s, "test", FIRST_OVERLAY, BASE_GENERATION,
                                                 CBM_STORE_OVERLAY_STATUS_READY),
              1);
    ASSERT_EQ(cbm_store_count_overlay_generations(s, "test",
                                                  CBM_STORE_OVERLAY_STATUS_RESERVED, &count),
              CBM_STORE_OK);
    ASSERT_EQ(count, 0);
    ASSERT_EQ(cbm_store_count_overlay_generations(s, "test", CBM_STORE_OVERLAY_STATUS_READY,
                                                  &count),
              CBM_STORE_OK);
    ASSERT_EQ(count, 1);

    ASSERT_EQ(cbm_store_reserve_overlay_generation(s, "test", BASE_GENERATION,
                                                   &overlay_generation),
              CBM_STORE_OK);
    ASSERT_EQ(overlay_generation, SECOND_OVERLAY);
    ASSERT_EQ(cbm_store_count_overlay_generations(s, "test", NULL, &count), CBM_STORE_OK);
    ASSERT_EQ(count, 2);

    cbm_store_close(s);
    PASS();
}

TEST(store_overlay_generation_rejects_invalid_inputs) {
    enum { SENTINEL_GENERATION = 42 };
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    int64_t overlay_generation = SENTINEL_GENERATION;
    ASSERT_EQ(cbm_store_reserve_overlay_generation(s, "missing", 0, &overlay_generation),
              CBM_STORE_ERR);
    ASSERT_EQ(overlay_generation, 0);
    ASSERT_EQ(cbm_store_reserve_overlay_generation(s, "test", -1, &overlay_generation),
              CBM_STORE_ERR);
    ASSERT_EQ(overlay_generation, 0);

    ASSERT_EQ(cbm_store_reserve_overlay_generation(s, "test", 0, &overlay_generation),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_store_set_overlay_generation_status(s, "test", overlay_generation,
                                                      "almost_ready"),
              CBM_STORE_ERR);
    ASSERT_EQ(cbm_store_set_overlay_generation_status(s, "test", overlay_generation + 1,
                                                      CBM_STORE_OVERLAY_STATUS_READY),
              CBM_STORE_NOT_FOUND);

    int count = SENTINEL_GENERATION;
    ASSERT_EQ(cbm_store_count_overlay_generations(s, "test", "almost_ready", &count),
              CBM_STORE_ERR);
    ASSERT_EQ(count, 0);

    cbm_store_close(s);
    PASS();
}

TEST(store_claim_ready_overlay_generation_claims_oldest_once) {
    enum { BASE_GENERATION = 9 };
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    int64_t first = 0;
    int64_t second = 0;
    ASSERT_EQ(cbm_store_reserve_overlay_generation(s, "test", BASE_GENERATION, &first),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_store_reserve_overlay_generation(s, "test", BASE_GENERATION + 1,
                                                   &second),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_store_set_overlay_generation_status(s, "test", first,
                                                      CBM_STORE_OVERLAY_STATUS_READY),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_store_set_overlay_generation_status(s, "test", second,
                                                      CBM_STORE_OVERLAY_STATUS_READY),
              CBM_STORE_OK);

    int64_t claimed = 0;
    int64_t base = 0;
    ASSERT_EQ(cbm_store_claim_ready_overlay_generation(s, "test", &claimed, &base),
              CBM_STORE_OK);
    ASSERT_EQ(claimed, first);
    ASSERT_EQ(base, BASE_GENERATION);
    ASSERT_EQ(store_count_overlay_generation_row(s, "test", first, BASE_GENERATION,
                                                 CBM_STORE_OVERLAY_STATUS_COMPACTING),
              1);

    ASSERT_EQ(cbm_store_claim_ready_overlay_generation(s, "test", &claimed, &base),
              CBM_STORE_OK);
    ASSERT_EQ(claimed, second);
    ASSERT_EQ(base, BASE_GENERATION + 1);
    ASSERT_EQ(store_count_overlay_generation_row(s, "test", second, BASE_GENERATION + 1,
                                                 CBM_STORE_OVERLAY_STATUS_COMPACTING),
              1);

    claimed = -1;
    base = -1;
    ASSERT_EQ(cbm_store_claim_ready_overlay_generation(s, "test", &claimed, &base),
              CBM_STORE_NOT_FOUND);
    ASSERT_EQ(claimed, 0);
    ASSERT_EQ(base, 0);

    cbm_store_close(s);
    PASS();
}

TEST(store_claim_ready_overlay_generation_ignores_nonready_and_validates_outputs) {
    enum { BASE_GENERATION = 3, SENTINEL = 42 };
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    int64_t reserved = 0;
    int64_t failed = 0;
    ASSERT_EQ(cbm_store_reserve_overlay_generation(s, "test", BASE_GENERATION,
                                                   &reserved),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_store_reserve_overlay_generation(s, "test", BASE_GENERATION,
                                                   &failed),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_store_set_overlay_generation_status(s, "test", failed,
                                                      CBM_STORE_OVERLAY_STATUS_FAILED),
              CBM_STORE_OK);

    int64_t claimed = SENTINEL;
    int64_t base = SENTINEL;
    ASSERT_EQ(cbm_store_claim_ready_overlay_generation(s, "test", &claimed, &base),
              CBM_STORE_NOT_FOUND);
    ASSERT_EQ(claimed, 0);
    ASSERT_EQ(base, 0);
    ASSERT_EQ(cbm_store_claim_ready_overlay_generation(s, "test", NULL, &base),
              CBM_STORE_ERR);
    ASSERT_EQ(base, 0);
    claimed = SENTINEL;
    ASSERT_EQ(cbm_store_claim_ready_overlay_generation(s, "test", &claimed, NULL),
              CBM_STORE_ERR);
    ASSERT_EQ(claimed, 0);

    cbm_store_close(s);
    PASS();
}

TEST(store_overlay_file_delta_publish_rows_and_tombstone) {
    enum { BASE_GENERATION = 3 };
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    int64_t overlay_generation = 0;
    ASSERT_EQ(cbm_store_reserve_overlay_generation(s, "test", BASE_GENERATION,
                                                   &overlay_generation),
              CBM_STORE_OK);

    cbm_node_t context_nodes[] = {
        {.project = "test",
         .label = "Folder",
         .name = "src",
         .qualified_name = "test.src",
         .file_path = "src",
         .properties_json = "{}"},
    };
    cbm_node_t nodes[] = {
        {.project = "test",
         .label = "Function",
         .name = "main",
         .qualified_name = "test.main",
         .file_path = "main.go",
         .properties_json = "{}"},
        {.project = "test",
         .label = "Function",
         .name = "helper",
         .qualified_name = "test.helper",
         .file_path = "main.go",
         .properties_json = "{}"},
    };
    cbm_store_delta_edge_t edges[] = {
        {.source_qn = "test.main",
         .target_qn = "test.helper",
         .type = "CALLS",
         .properties_json = "{}",
         .derived_kind = CBM_STORE_DERIVED_KIND_DIRECT},
    };
    cbm_store_file_delta_t delta = {.project = "test",
                                    .rel_path = "main.go",
                                    .generation = BASE_GENERATION,
                                    .context_nodes = context_nodes,
                                    .context_node_count = 1,
                                    .nodes = nodes,
                                    .node_count = 2,
                                    .edges = edges,
                                    .edge_count = 1};

    ASSERT_EQ(cbm_store_publish_overlay_file_delta(s, &delta, overlay_generation), CBM_STORE_OK);
    ASSERT_EQ(store_count_overlay_rows(s, STORE_TEST_OVERLAY_NODES, "test", overlay_generation,
                                       "main.go"),
              3);
    ASSERT_EQ(store_count_overlay_owned_rows(s, STORE_TEST_OVERLAY_NODES, "test",
                                             overlay_generation, "main.go",
                                             STORE_TEST_OVERLAY_ROW_OWNED),
              2);
    ASSERT_EQ(store_count_overlay_owned_rows(s, STORE_TEST_OVERLAY_NODES, "test",
                                             overlay_generation, "main.go",
                                             STORE_TEST_OVERLAY_ROW_CONTEXT),
              1);
    ASSERT_EQ(store_count_overlay_rows(s, STORE_TEST_OVERLAY_EDGES, "test", overlay_generation,
                                       "main.go"),
              1);
    ASSERT_EQ(store_count_overlay_rows(s, STORE_TEST_OVERLAY_TOMBSTONES, "test",
                                       overlay_generation, "main.go"),
              1);
    ASSERT_EQ(store_count_overlay_fts_matches(s, "test", overlay_generation, "main.go",
                                              "helper"),
              1);

    int count = -1;
    ASSERT_EQ(cbm_store_count_overlay_generations(s, "test", CBM_STORE_OVERLAY_STATUS_READY,
                                                  &count),
              CBM_STORE_OK);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(cbm_store_count_nodes(s, "test"), 0);
    ASSERT_EQ(cbm_store_count_edges(s, "test"), 0);

    ASSERT_EQ(cbm_store_publish_overlay_file_delta(s, &delta, overlay_generation), CBM_STORE_OK);
    ASSERT_EQ(store_count_overlay_rows(s, STORE_TEST_OVERLAY_NODES, "test", overlay_generation,
                                       "main.go"),
              3);
    ASSERT_EQ(store_count_overlay_rows(s, STORE_TEST_OVERLAY_EDGES, "test", overlay_generation,
                                       "main.go"),
              1);
    ASSERT_EQ(store_count_overlay_rows(s, STORE_TEST_OVERLAY_TOMBSTONES, "test",
                                       overlay_generation, "main.go"),
              1);
    ASSERT_EQ(store_count_overlay_fts_matches(s, "test", overlay_generation, "main.go",
                                              "helper"),
              1);

    cbm_node_t replacement_nodes[] = {
        {.project = "test",
         .label = "Function",
         .name = "replacement",
         .qualified_name = "test.replacement",
         .file_path = "main.go",
         .properties_json = "{}"},
    };
    cbm_store_file_delta_t replacement_delta = {.project = "test",
                                                .rel_path = "main.go",
                                                .generation = BASE_GENERATION,
                                                .context_nodes = context_nodes,
                                                .context_node_count = 1,
                                                .nodes = replacement_nodes,
                                                .node_count = 1};
    ASSERT_EQ(cbm_store_publish_overlay_file_delta(s, &replacement_delta, overlay_generation),
              CBM_STORE_OK);
    ASSERT_EQ(store_count_overlay_rows(s, STORE_TEST_OVERLAY_NODES, "test", overlay_generation,
                                       "main.go"),
              2);
    ASSERT_EQ(store_count_overlay_fts_matches(s, "test", overlay_generation, "main.go",
                                              "helper"),
              0);
    ASSERT_EQ(store_count_overlay_fts_matches(s, "test", overlay_generation, "main.go",
                                              "replacement"),
              1);

    cbm_node_t helper_nodes[] = {
        {.project = "test",
         .label = "Function",
         .name = "other",
         .qualified_name = "test.other",
         .file_path = "helper.go",
         .properties_json = "{}"},
    };
    cbm_store_file_delta_t helper_delta = {.project = "test",
                                           .rel_path = "helper.go",
                                           .generation = BASE_GENERATION,
                                           .context_nodes = context_nodes,
                                           .context_node_count = 1,
                                           .nodes = helper_nodes,
                                           .node_count = 1};
    ASSERT_EQ(cbm_store_publish_overlay_file_delta(s, &helper_delta, overlay_generation),
              CBM_STORE_OK);
    ASSERT_EQ(store_count_overlay_rows(s, STORE_TEST_OVERLAY_NODES, "test", overlay_generation,
                                       "helper.go"),
              2);
    ASSERT_EQ(store_count_overlay_owned_rows(s, STORE_TEST_OVERLAY_NODES, "test",
                                             overlay_generation, "helper.go",
                                             STORE_TEST_OVERLAY_ROW_CONTEXT),
              1);

    cbm_store_close(s);
    PASS();
}

TEST(store_delete_project_clears_overlay_fts) {
    enum { BASE_GENERATION = 3 };
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    int64_t overlay_generation = 0;
    ASSERT_EQ(cbm_store_reserve_overlay_generation(s, "test", BASE_GENERATION,
                                                   &overlay_generation),
              CBM_STORE_OK);

    cbm_node_t nodes[] = {
        {.project = "test",
         .label = "Function",
         .name = "needle_symbol",
         .qualified_name = "test.needle_symbol",
         .file_path = "main.go",
         .properties_json = "{}"},
    };
    cbm_store_file_delta_t delta = {.project = "test",
                                    .rel_path = "main.go",
                                    .generation = BASE_GENERATION,
                                    .nodes = nodes,
                                    .node_count = 1};

    ASSERT_EQ(cbm_store_publish_overlay_file_delta(s, &delta, overlay_generation), CBM_STORE_OK);
    ASSERT_EQ(store_count_overlay_fts_raw_matches(s, "needle"), 1);

    ASSERT_EQ(cbm_store_delete_project(s, "test"), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);
    ASSERT_EQ(store_count_overlay_fts_raw_matches(s, "needle"), 0);

    int count = -1;
    ASSERT_EQ(cbm_store_count_overlay_generations(s, "test", NULL, &count), CBM_STORE_OK);
    ASSERT_EQ(count, 0);
    cbm_store_close(s);
    PASS();
}

TEST(store_overlay_file_delta_publish_rejects_invalid_delta_without_rows) {
    enum { BASE_GENERATION = 1 };
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    int64_t overlay_generation = 0;
    ASSERT_EQ(cbm_store_reserve_overlay_generation(s, "test", BASE_GENERATION,
                                                   &overlay_generation),
              CBM_STORE_OK);

    cbm_node_t bad_node = {.project = "test",
                           .label = "Function",
                           .name = "bad",
                           .qualified_name = NULL,
                           .file_path = "bad.go",
                           .properties_json = "{}"};
    cbm_store_file_delta_t bad_delta = {.project = "test",
                                        .rel_path = "bad.go",
                                        .generation = BASE_GENERATION,
                                        .nodes = &bad_node,
                                        .node_count = 1};

    ASSERT_EQ(cbm_store_publish_overlay_file_delta(s, &bad_delta, overlay_generation),
              CBM_STORE_ERR);
    ASSERT_EQ(store_count_overlay_rows(s, STORE_TEST_OVERLAY_NODES, "test", overlay_generation,
                                       "bad.go"),
              0);
    ASSERT_EQ(store_count_overlay_rows(s, STORE_TEST_OVERLAY_EDGES, "test", overlay_generation,
                                       "bad.go"),
              0);
    ASSERT_EQ(store_count_overlay_rows(s, STORE_TEST_OVERLAY_TOMBSTONES, "test",
                                       overlay_generation, "bad.go"),
              0);
    int count = -1;
    ASSERT_EQ(cbm_store_count_overlay_generations(s, "test",
                                                  CBM_STORE_OVERLAY_STATUS_RESERVED, &count),
              CBM_STORE_OK);
    ASSERT_EQ(count, 1);

    cbm_store_close(s);
    PASS();
}

TEST(store_overlay_file_delta_batch_rolls_back_all_files) {
    enum { BASE_GENERATION = 1 };
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    int64_t overlay_generation = 0;
    ASSERT_EQ(cbm_store_reserve_overlay_generation(s, "test", BASE_GENERATION,
                                                   &overlay_generation),
              CBM_STORE_OK);

    cbm_node_t good_node = {.project = "test",
                            .label = "Function",
                            .name = "good",
                            .qualified_name = "test.good",
                            .file_path = "good.go",
                            .properties_json = "{}"};
    cbm_store_file_delta_t good_delta = {.project = "test",
                                         .rel_path = "good.go",
                                         .generation = BASE_GENERATION,
                                         .nodes = &good_node,
                                         .node_count = 1};
    cbm_node_t bad_node = {.project = "test",
                           .label = "Function",
                           .name = "bad",
                           .qualified_name = NULL,
                           .file_path = "bad.go",
                           .properties_json = "{}"};
    cbm_store_file_delta_t bad_delta = {.project = "test",
                                        .rel_path = "bad.go",
                                        .generation = BASE_GENERATION,
                                        .nodes = &bad_node,
                                        .node_count = 1};
    const cbm_store_file_delta_t *deltas[] = {&good_delta, &bad_delta};

    ASSERT_EQ(cbm_store_publish_overlay_file_delta_batch(s, deltas, CBM_SZ_2,
                                                         overlay_generation),
              CBM_STORE_ERR);
    ASSERT_EQ(store_count_overlay_rows(s, STORE_TEST_OVERLAY_NODES, "test", overlay_generation,
                                       "good.go"),
              0);
    ASSERT_EQ(store_count_overlay_rows(s, STORE_TEST_OVERLAY_TOMBSTONES, "test",
                                       overlay_generation, "good.go"),
              0);
    ASSERT_EQ(store_count_overlay_generation_row(s, "test", overlay_generation, BASE_GENERATION,
                                                 CBM_STORE_OVERLAY_STATUS_RESERVED),
              1);

    cbm_store_close(s);
    PASS();
}

TEST(store_overlay_file_delta_publish_rejects_failed_generation) {
    enum { BASE_GENERATION = 1 };
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    int64_t overlay_generation = 0;
    ASSERT_EQ(cbm_store_reserve_overlay_generation(s, "test", BASE_GENERATION,
                                                   &overlay_generation),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_store_set_overlay_generation_status(s, "test", overlay_generation,
                                                      CBM_STORE_OVERLAY_STATUS_FAILED),
              CBM_STORE_OK);

    cbm_node_t node = {.project = "test",
                       .label = "Function",
                       .name = "blocked",
                       .qualified_name = "test.blocked",
                       .file_path = "blocked.go",
                       .properties_json = "{}"};
    cbm_store_file_delta_t delta = {.project = "test",
                                    .rel_path = "blocked.go",
                                    .generation = BASE_GENERATION,
                                    .nodes = &node,
                                    .node_count = 1};

    ASSERT_EQ(cbm_store_publish_overlay_file_delta(s, &delta, overlay_generation),
              CBM_STORE_ERR);
    ASSERT_EQ(store_count_overlay_rows(s, STORE_TEST_OVERLAY_NODES, "test", overlay_generation,
                                       "blocked.go"),
              0);
    ASSERT_EQ(store_count_overlay_rows(s, STORE_TEST_OVERLAY_TOMBSTONES, "test",
                                       overlay_generation, "blocked.go"),
              0);

    cbm_store_close(s);
    PASS();
}

TEST(store_overlay_node_view_summary_counts_latest_ready_overlay) {
    enum { BASE_GENERATION = 1 };
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    cbm_node_t old_main = {.project = "test",
                           .label = "Function",
                           .name = "old_main",
                           .qualified_name = "test.old_main",
                           .file_path = "main.go",
                           .properties_json = "{}"};
    cbm_node_t stable = {.project = "test",
                         .label = "Function",
                         .name = "stable",
                         .qualified_name = "test.stable",
                         .file_path = "stable.go",
                         .properties_json = "{}"};
    ASSERT_GT(cbm_store_upsert_node(s, &old_main), 0);
    ASSERT_GT(cbm_store_upsert_node(s, &stable), 0);

    cbm_store_overlay_node_view_summary_t summary = {0};
    ASSERT_EQ(cbm_store_get_overlay_node_view_summary(s, "test", &summary), CBM_STORE_OK);
    ASSERT_EQ(summary.overlay_ready_generations, 0);
    ASSERT_EQ(summary.active_file_tombstones, 0);
    ASSERT_EQ(summary.canonical_nodes_visible, 2);
    ASSERT_EQ(summary.overlay_owned_nodes_visible, 0);
    ASSERT_EQ(summary.total_nodes_visible, 2);

    int64_t first_overlay = 0;
    ASSERT_EQ(cbm_store_reserve_overlay_generation(s, "test", BASE_GENERATION, &first_overlay),
              CBM_STORE_OK);
    cbm_node_t first_nodes[] = {
        {.project = "test",
         .label = "Function",
         .name = "new_main",
         .qualified_name = "test.new_main",
         .file_path = "main.go",
         .properties_json = "{}"},
        {.project = "test",
         .label = "Function",
         .name = "new_helper",
         .qualified_name = "test.new_helper",
         .file_path = "main.go",
         .properties_json = "{}"},
    };
    cbm_store_file_delta_t first_delta = {.project = "test",
                                          .rel_path = "main.go",
                                          .generation = BASE_GENERATION,
                                          .nodes = first_nodes,
                                          .node_count = 2};
    ASSERT_EQ(cbm_store_publish_overlay_file_delta(s, &first_delta, first_overlay),
              CBM_STORE_OK);

    ASSERT_EQ(cbm_store_get_overlay_node_view_summary(s, "test", &summary), CBM_STORE_OK);
    ASSERT_EQ(summary.overlay_ready_generations, 1);
    ASSERT_EQ(summary.active_file_tombstones, 1);
    ASSERT_EQ(summary.canonical_nodes_visible, 1);
    ASSERT_EQ(summary.overlay_owned_nodes_visible, 2);
    ASSERT_EQ(summary.total_nodes_visible, 3);

    int64_t second_overlay = 0;
    ASSERT_EQ(cbm_store_reserve_overlay_generation(s, "test", BASE_GENERATION, &second_overlay),
              CBM_STORE_OK);
    cbm_node_t second_node = {.project = "test",
                              .label = "Function",
                              .name = "newer_main",
                              .qualified_name = "test.newer_main",
                              .file_path = "main.go",
                              .properties_json = "{}"};
    cbm_store_file_delta_t second_delta = {.project = "test",
                                           .rel_path = "main.go",
                                           .generation = BASE_GENERATION,
                                           .nodes = &second_node,
                                           .node_count = 1};
    ASSERT_EQ(cbm_store_publish_overlay_file_delta(s, &second_delta, second_overlay),
              CBM_STORE_OK);

    ASSERT_EQ(cbm_store_get_overlay_node_view_summary(s, "test", &summary), CBM_STORE_OK);
    ASSERT_EQ(summary.overlay_ready_generations, 1);
    ASSERT_EQ(summary.active_file_tombstones, 1);
    ASSERT_EQ(summary.canonical_nodes_visible, 1);
    ASSERT_EQ(summary.overlay_owned_nodes_visible, 1);
    ASSERT_EQ(summary.total_nodes_visible, 2);
    ASSERT_EQ(store_count_overlay_rows(s, STORE_TEST_OVERLAY_NODES, "test", first_overlay,
                                       "main.go"),
              0);
    ASSERT_EQ(store_count_overlay_rows(s, STORE_TEST_OVERLAY_TOMBSTONES, "test",
                                       first_overlay, "main.go"),
              0);

    cbm_store_close(s);
    PASS();
}

TEST(store_overlay_publish_prunes_superseded_file_rows_and_fts) {
    enum { BASE_GENERATION = 1 };
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    int64_t first_overlay = 0;
    ASSERT_EQ(cbm_store_reserve_overlay_generation(s, "test", BASE_GENERATION, &first_overlay),
              CBM_STORE_OK);
    cbm_node_t stale_main = {.project = "test",
                             .label = "Function",
                             .name = "stale_symbol",
                             .qualified_name = "test.stale_symbol",
                             .file_path = "main.go",
                             .properties_json = "{}"};
    cbm_node_t helper = {.project = "test",
                         .label = "Function",
                         .name = "helper_symbol",
                         .qualified_name = "test.helper_symbol",
                         .file_path = "helper.go",
                         .properties_json = "{}"};
    cbm_store_file_delta_t first_main = {.project = "test",
                                         .rel_path = "main.go",
                                         .generation = BASE_GENERATION,
                                         .nodes = &stale_main,
                                         .node_count = 1};
    cbm_store_file_delta_t first_helper = {.project = "test",
                                           .rel_path = "helper.go",
                                           .generation = BASE_GENERATION,
                                           .nodes = &helper,
                                           .node_count = 1};
    const cbm_store_file_delta_t *first_deltas[] = {&first_main, &first_helper};
    ASSERT_EQ(cbm_store_publish_overlay_file_delta_batch(s, first_deltas, CBM_SZ_2,
                                                         first_overlay),
              CBM_STORE_OK);
    ASSERT_EQ(store_count_overlay_fts_raw_matches(s, "stale"), 1);
    ASSERT_EQ(store_count_overlay_fts_raw_matches(s, "helper"), 1);

    int64_t second_overlay = 0;
    ASSERT_EQ(cbm_store_reserve_overlay_generation(s, "test", BASE_GENERATION, &second_overlay),
              CBM_STORE_OK);
    cbm_node_t fresh_main = {.project = "test",
                             .label = "Function",
                             .name = "fresh_symbol",
                             .qualified_name = "test.fresh_symbol",
                             .file_path = "main.go",
                             .properties_json = "{}"};
    cbm_store_file_delta_t second_main = {.project = "test",
                                          .rel_path = "main.go",
                                          .generation = BASE_GENERATION,
                                          .nodes = &fresh_main,
                                          .node_count = 1};
    ASSERT_EQ(cbm_store_publish_overlay_file_delta(s, &second_main, second_overlay),
              CBM_STORE_OK);

    ASSERT_EQ(store_count_overlay_rows(s, STORE_TEST_OVERLAY_NODES, "test", first_overlay,
                                       "main.go"),
              0);
    ASSERT_EQ(store_count_overlay_rows(s, STORE_TEST_OVERLAY_TOMBSTONES, "test",
                                       first_overlay, "main.go"),
              0);
    ASSERT_EQ(store_count_overlay_rows(s, STORE_TEST_OVERLAY_NODES, "test", first_overlay,
                                       "helper.go"),
              1);
    ASSERT_EQ(store_count_overlay_rows(s, STORE_TEST_OVERLAY_TOMBSTONES, "test",
                                       first_overlay, "helper.go"),
              1);
    ASSERT_EQ(store_count_overlay_fts_raw_matches(s, "stale"), 0);
    ASSERT_EQ(store_count_overlay_fts_raw_matches(s, "helper"), 1);
    ASSERT_EQ(store_count_overlay_fts_raw_matches(s, "fresh"), 1);

    int count = -1;
    ASSERT_EQ(cbm_store_count_overlay_generations(s, "test", CBM_STORE_OVERLAY_STATUS_READY,
                                                  &count),
              CBM_STORE_OK);
    ASSERT_EQ(count, 2);
    ASSERT_EQ(store_count_overlay_generation_row(s, "test", first_overlay, BASE_GENERATION,
                                                 CBM_STORE_OVERLAY_STATUS_READY),
              1);
    ASSERT_EQ(store_count_overlay_generation_row(s, "test", second_overlay, BASE_GENERATION,
                                                 CBM_STORE_OVERLAY_STATUS_READY),
              1);

    cbm_store_close(s);
    PASS();
}

TEST(store_compact_overlay_generation_promotes_metadata_and_cleans_overlay) {
    enum { BASE_GENERATION = 1, COMPACT_GENERATION = 2 };
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    int64_t generation = 0;
    ASSERT_EQ(cbm_store_reserve_index_generation(s, "test", NULL, NULL, &generation),
              CBM_STORE_OK);
    ASSERT_EQ(generation, BASE_GENERATION);
    ASSERT_EQ(store_publish_helper_file_delta(s, generation), CBM_STORE_OK);
    ASSERT_EQ(store_publish_old_main_delta(s, generation), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_finish_index_generation(s, "test", generation,
                                                CBM_STORE_INDEX_STATUS_COMPLETE),
              CBM_STORE_OK);

    int64_t overlay_generation = 0;
    ASSERT_EQ(cbm_store_reserve_overlay_generation(s, "test", BASE_GENERATION,
                                                   &overlay_generation),
              CBM_STORE_OK);
    cbm_node_t new_nodes[1] = {{.project = "test",
                                .label = "Function",
                                .name = "New",
                                .qualified_name = "test.main.New",
                                .file_path = "main.go",
                                .properties_json = "{}"}};
    cbm_store_delta_edge_t edges[1] = {{.source_qn = "test.main.New",
                                        .target_qn = "test.helper.Helper",
                                        .type = "CALLS",
                                        .properties_json = "{}",
                                        .derived_kind = CBM_STORE_DERIVED_KIND_DIRECT}};
    cbm_file_hash_t hash = {.project = "test",
                            .rel_path = "main.go",
                            .sha256 = "overlay-main-hash",
                            .mtime_ns = 2,
                            .size = 20};
    cbm_file_state_t state = {.project = "test",
                              .rel_path = "main.go",
                              .content_hash = "overlay-main-content",
                              .git_oid = "overlay-oid",
                              .mtime_ns = 2,
                              .size = 20,
                              .language = "c",
                              .pass_fingerprint = "pass-overlay",
                              .generation = BASE_GENERATION,
                              .indexed_at = "2026-06-30T00:02:00Z"};
    cbm_store_symbol_export_t exports[1] = {
        {.qualified_name = "test.main.New", .node_id = CBM_STORE_NO_NODE_ID}};
    cbm_store_import_ref_t imports[1] = {{.import_text = "test.helper",
                                          .local_name = "Helper",
                                          .target_qn = "test.helper.Helper"}};
    cbm_store_file_delta_t overlay_delta = {
        .project = "test",
        .rel_path = "main.go",
        .generation = BASE_GENERATION,
        .file_hash = &hash,
        .file_state = &state,
        .nodes = new_nodes,
        .node_count = 1,
        .edges = edges,
        .edge_count = 1,
        .exports = exports,
        .export_count = 1,
        .imports = imports,
        .import_count = 1,
        .derived_view_name = CBM_STORE_DERIVED_VIEW_NODES_FTS,
        .derived_status = CBM_STORE_DERIVED_STATUS_COMPLETE};
    ASSERT_EQ(cbm_store_publish_overlay_file_delta(s, &overlay_delta, overlay_generation),
              CBM_STORE_OK);
    cbm_dirty_file_state_t dirty = {.project = "test",
                                    .rel_path = "main.go",
                                    .observed_hash = "overlay-main-content",
                                    .observed_generation = overlay_generation,
                                    .source = CBM_STORE_DIRTY_SOURCE_EXPLICIT_REINDEX,
                                    .status = CBM_STORE_DIRTY_STATUS_OVERLAY_READY};
    ASSERT_EQ(cbm_store_upsert_dirty_file(s, &dirty), CBM_STORE_OK);

    ASSERT_EQ(cbm_store_reserve_index_generation(s, "test", NULL, NULL, &generation),
              CBM_STORE_OK);
    ASSERT_EQ(generation, COMPACT_GENERATION);
    int64_t claimed_overlay = 0;
    int64_t claimed_base = 0;
    ASSERT_EQ(cbm_store_claim_ready_overlay_generation(s, "test", &claimed_overlay,
                                                       &claimed_base),
              CBM_STORE_OK);
    ASSERT_EQ(claimed_overlay, overlay_generation);
    ASSERT_EQ(claimed_base, BASE_GENERATION);
    ASSERT_EQ(cbm_store_compact_overlay_generation(s, "test", overlay_generation, generation),
              CBM_STORE_OK);

    ASSERT_EQ(store_node_qn_exists(s, "test", "test.main.Old"), 0);
    ASSERT_EQ(store_node_qn_exists(s, "test", "test.main.New"), 1);
    ASSERT_EQ(store_node_qn_exists(s, "test", "test.helper.Helper"), 1);
    ASSERT_EQ(cbm_store_count_edges(s, "test"), 1);
    ASSERT_EQ(store_count_metadata_owners(s, 0, "test", "main.go"), 1);
    ASSERT_EQ(store_count_metadata_owners(s, 1, "test", "main.go"), 1);

    cbm_file_state_t got = {0};
    ASSERT_EQ(cbm_store_get_file_state(s, "test", "main.go", &got), CBM_STORE_OK);
    ASSERT_STR_EQ(got.content_hash, "overlay-main-content");
    ASSERT_STR_EQ(got.pass_fingerprint, "pass-overlay");
    ASSERT_EQ(got.generation, COMPACT_GENERATION);
    cbm_store_file_state_free_fields(&got);

    char **items = NULL;
    int count = 0;
    ASSERT_EQ(cbm_store_list_symbol_exports_by_file(s, "test", "main.go", &items, &count),
              CBM_STORE_OK);
    ASSERT_EQ(count, 1);
    ASSERT_STR_EQ(items[0], "test.main.New");
    store_free_string_array(items, count);
    items = NULL;
    ASSERT_EQ(cbm_store_list_import_ref_paths_by_target(s, "test", "test.helper.Helper",
                                                        &items, &count),
              CBM_STORE_OK);
    ASSERT_EQ(store_string_array_contains(items, count, "main.go"), 1);
    store_free_string_array(items, count);

    int pending = -1;
    int overlay_ready = -1;
    ASSERT_EQ(cbm_store_count_dirty_files(s, "test", &pending, &overlay_ready), CBM_STORE_OK);
    ASSERT_EQ(pending, 0);
    ASSERT_EQ(overlay_ready, 0);
    ASSERT_EQ(store_count_overlay_rows(s, STORE_TEST_OVERLAY_NODES, "test", overlay_generation,
                                       "main.go"),
              0);
    ASSERT_EQ(store_count_overlay_rows(s, STORE_TEST_OVERLAY_TOMBSTONES, "test",
                                       overlay_generation, "main.go"),
              0);
    ASSERT_EQ(store_count_overlay_generation_row(s, "test", overlay_generation,
                                                 BASE_GENERATION,
                                                 CBM_STORE_OVERLAY_STATUS_READY),
              0);
    ASSERT_EQ(store_count_index_generation(s, "test", COMPACT_GENERATION,
                                           CBM_STORE_INDEX_STATUS_COMPLETE, "", "",
                                           STORE_TEST_COMPLETED_SET),
              1);

    cbm_store_close(s);
    PASS();
}

TEST(store_compact_overlay_generation_promotes_delete_only_tombstone) {
    enum { BASE_GENERATION = 1, COMPACT_GENERATION = 2 };
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    int64_t generation = 0;
    ASSERT_EQ(cbm_store_reserve_index_generation(s, "test", NULL, NULL, &generation),
              CBM_STORE_OK);
    ASSERT_EQ(generation, BASE_GENERATION);
    ASSERT_EQ(store_publish_helper_file_delta(s, generation), CBM_STORE_OK);
    ASSERT_EQ(store_publish_old_main_delta(s, generation), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_finish_index_generation(s, "test", generation,
                                                CBM_STORE_INDEX_STATUS_COMPLETE),
              CBM_STORE_OK);

    int64_t overlay_generation = 0;
    ASSERT_EQ(cbm_store_reserve_overlay_generation(s, "test", BASE_GENERATION,
                                                   &overlay_generation),
              CBM_STORE_OK);
    cbm_store_file_delta_t delete_delta = {
        .project = "test",
        .rel_path = "main.go",
        .generation = BASE_GENERATION,
        .derived_view_name = CBM_STORE_DERIVED_VIEW_NODES_FTS,
        .derived_status = CBM_STORE_DERIVED_STATUS_COMPLETE};
    ASSERT_EQ(cbm_store_publish_overlay_file_delta(s, &delete_delta, overlay_generation),
              CBM_STORE_OK);
    cbm_dirty_file_state_t dirty = {.project = "test",
                                    .rel_path = "main.go",
                                    .observed_generation = overlay_generation,
                                    .source = CBM_STORE_DIRTY_SOURCE_EXPLICIT_REINDEX,
                                    .status = CBM_STORE_DIRTY_STATUS_OVERLAY_READY};
    ASSERT_EQ(cbm_store_upsert_dirty_file(s, &dirty), CBM_STORE_OK);

    ASSERT_EQ(cbm_store_reserve_index_generation(s, "test", NULL, NULL, &generation),
              CBM_STORE_OK);
    ASSERT_EQ(generation, COMPACT_GENERATION);
    ASSERT_EQ(cbm_store_compact_overlay_generation(s, "test", overlay_generation, generation),
              CBM_STORE_OK);

    ASSERT_EQ(store_node_qn_exists(s, "test", "test.main.Old"), 0);
    ASSERT_EQ(store_node_qn_exists(s, "test", "test.helper.Helper"), 1);
    ASSERT_EQ(store_count_metadata_owners(s, 0, "test", "main.go"), 0);
    ASSERT_EQ(store_count_metadata_owners(s, 1, "test", "main.go"), 0);
    cbm_file_state_t got = {0};
    ASSERT_EQ(cbm_store_get_file_state(s, "test", "main.go", &got), CBM_STORE_NOT_FOUND);
    ASSERT_EQ(store_count_derived_view_state(s, "test", CBM_STORE_DERIVED_VIEW_NODES_FTS,
                                             COMPACT_GENERATION,
                                             CBM_STORE_DERIVED_STATUS_STALE),
              1);
    int pending = -1;
    int overlay_ready = -1;
    ASSERT_EQ(cbm_store_count_dirty_files(s, "test", &pending, &overlay_ready), CBM_STORE_OK);
    ASSERT_EQ(pending, 0);
    ASSERT_EQ(overlay_ready, 0);
    ASSERT_EQ(store_count_overlay_rows(s, STORE_TEST_OVERLAY_TOMBSTONES, "test",
                                       overlay_generation, "main.go"),
              0);
    ASSERT_EQ(store_count_overlay_generation_row(s, "test", overlay_generation,
                                                 BASE_GENERATION,
                                                 CBM_STORE_OVERLAY_STATUS_READY),
              0);
    ASSERT_EQ(store_count_index_generation(s, "test", COMPACT_GENERATION,
                                           CBM_STORE_INDEX_STATUS_COMPLETE, "", "",
                                           STORE_TEST_COMPLETED_SET),
              1);

    cbm_store_close(s);
    PASS();
}

TEST(store_find_nodes_by_file_overlay_view_returns_latest_ready_rows) {
    enum { BASE_GENERATION = 1 };
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    cbm_node_t old_main = {.project = "test",
                           .label = "Function",
                           .name = "old_main",
                           .qualified_name = "test.old_main",
                           .file_path = "main.go",
                           .properties_json = "{}"};
    cbm_node_t stable = {.project = "test",
                         .label = "Function",
                         .name = "stable",
                         .qualified_name = "test.stable",
                         .file_path = "stable.go",
                         .properties_json = "{}"};
    ASSERT_GT(cbm_store_upsert_node(s, &old_main), 0);
    ASSERT_GT(cbm_store_upsert_node(s, &stable), 0);

    cbm_node_t *nodes = NULL;
    int count = -1;
    ASSERT_EQ(cbm_store_find_nodes_by_file_overlay_view(s, "test", "main.go", &nodes, &count),
              CBM_STORE_OK);
    ASSERT_EQ(count, 1);
    ASSERT_GT(nodes[0].id, CBM_STORE_NO_NODE_ID);
    ASSERT_STR_EQ(nodes[0].name, "old_main");
    cbm_store_free_nodes(nodes, count);

    int64_t first_overlay = 0;
    ASSERT_EQ(cbm_store_reserve_overlay_generation(s, "test", BASE_GENERATION, &first_overlay),
              CBM_STORE_OK);
    cbm_node_t first_nodes[] = {
        {.project = "test",
         .label = "Function",
         .name = "new_main",
         .qualified_name = "test.new_main",
         .file_path = "main.go",
         .properties_json = "{}"},
        {.project = "test",
         .label = "Function",
         .name = "new_helper",
         .qualified_name = "test.new_helper",
         .file_path = "main.go",
         .properties_json = "{}"},
    };
    cbm_store_file_delta_t first_delta = {.project = "test",
                                          .rel_path = "main.go",
                                          .generation = BASE_GENERATION,
                                          .nodes = first_nodes,
                                          .node_count = 2};
    ASSERT_EQ(cbm_store_publish_overlay_file_delta(s, &first_delta, first_overlay),
              CBM_STORE_OK);

    nodes = NULL;
    count = -1;
    ASSERT_EQ(cbm_store_find_nodes_by_file_overlay_view(s, "test", "main.go", &nodes, &count),
              CBM_STORE_OK);
    ASSERT_EQ(count, 2);
    ASSERT_EQ(nodes[0].id, CBM_STORE_NO_NODE_ID);
    ASSERT_EQ(nodes[1].id, CBM_STORE_NO_NODE_ID);
    ASSERT_STR_EQ(nodes[0].name, "new_helper");
    ASSERT_STR_EQ(nodes[1].name, "new_main");
    cbm_store_free_nodes(nodes, count);

    int64_t second_overlay = 0;
    ASSERT_EQ(cbm_store_reserve_overlay_generation(s, "test", BASE_GENERATION, &second_overlay),
              CBM_STORE_OK);
    cbm_node_t second_node = {.project = "test",
                              .label = "Function",
                              .name = "newer_main",
                              .qualified_name = "test.newer_main",
                              .file_path = "main.go",
                              .properties_json = "{}"};
    cbm_store_file_delta_t second_delta = {.project = "test",
                                           .rel_path = "main.go",
                                           .generation = BASE_GENERATION,
                                           .nodes = &second_node,
                                           .node_count = 1};
    ASSERT_EQ(cbm_store_publish_overlay_file_delta(s, &second_delta, second_overlay),
              CBM_STORE_OK);

    nodes = NULL;
    count = -1;
    ASSERT_EQ(cbm_store_find_nodes_by_file_overlay_view(s, "test", "main.go", &nodes, &count),
              CBM_STORE_OK);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(nodes[0].id, CBM_STORE_NO_NODE_ID);
    ASSERT_STR_EQ(nodes[0].name, "newer_main");
    cbm_store_free_nodes(nodes, count);

    nodes = NULL;
    count = -1;
    ASSERT_EQ(cbm_store_find_nodes_by_file_overlay_view(s, "test", "stable.go", &nodes, &count),
              CBM_STORE_OK);
    ASSERT_EQ(count, 1);
    ASSERT_GT(nodes[0].id, CBM_STORE_NO_NODE_ID);
    ASSERT_STR_EQ(nodes[0].name, "stable");
    cbm_store_free_nodes(nodes, count);

    cbm_store_close(s);
    PASS();
}

TEST(store_search_overlay_view_without_ready_overlay_matches_canonical_search) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    cbm_node_t old_main = {.project = "test",
                           .label = "Function",
                           .name = "old_main",
                           .qualified_name = "test.old_main",
                           .file_path = "main.go",
                           .properties_json = "{}"};
    ASSERT_GT(cbm_store_upsert_node(s, &old_main), 0);

    cbm_search_params_t params = {.project = "test",
                                  .pattern = "main",
                                  .sort_by = "name",
                                  .limit = 10};
    cbm_search_output_t active = {0};
    ASSERT_EQ(cbm_store_search_overlay_view(s, &params, &active), CBM_STORE_OK);
    ASSERT_EQ(active.total, 1);
    ASSERT_EQ(active.count, 1);
    ASSERT_GT(active.results[0].node.id, CBM_STORE_NO_NODE_ID);
    ASSERT_STR_EQ(active.results[0].node.name, "old_main");
    cbm_store_search_free(&active);

    cbm_store_close(s);
    PASS();
}

TEST(store_search_overlay_view_matches_full_rebuild_oracle) {
    enum { BASE_GENERATION = 1 };
    cbm_store_t *live = cbm_store_open_memory();
    cbm_store_t *oracle = cbm_store_open_memory();
    ASSERT_NOT_NULL(live);
    ASSERT_NOT_NULL(oracle);
    ASSERT_EQ(cbm_store_upsert_project(live, "test", "/tmp/test"), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_upsert_project(oracle, "test", "/tmp/test"), CBM_STORE_OK);

    cbm_node_t old_main = {.project = "test",
                           .label = "Function",
                           .name = "old_main",
                           .qualified_name = "test.old_main",
                           .file_path = "main.go",
                           .properties_json = "{}"};
    cbm_node_t stable = {.project = "test",
                         .label = "Function",
                         .name = "stable",
                         .qualified_name = "test.stable",
                         .file_path = "stable.go",
                         .properties_json = "{}"};
    ASSERT_GT(cbm_store_upsert_node(live, &old_main), 0);
    ASSERT_GT(cbm_store_upsert_node(live, &stable), 0);
    ASSERT_GT(cbm_store_upsert_node(oracle, &stable), 0);

    int64_t first_overlay = 0;
    ASSERT_EQ(cbm_store_reserve_overlay_generation(live, "test", BASE_GENERATION,
                                                   &first_overlay),
              CBM_STORE_OK);
    cbm_node_t first_main = {.project = "test",
                             .label = "Function",
                             .name = "new_main",
                             .qualified_name = "test.new_main",
                             .file_path = "main.go",
                             .properties_json = "{}"};
    cbm_store_file_delta_t first_delta = {.project = "test",
                                          .rel_path = "main.go",
                                          .generation = BASE_GENERATION,
                                          .nodes = &first_main,
                                          .node_count = 1};
    ASSERT_EQ(cbm_store_publish_overlay_file_delta(live, &first_delta, first_overlay),
              CBM_STORE_OK);

    int64_t second_overlay = 0;
    ASSERT_EQ(cbm_store_reserve_overlay_generation(live, "test", BASE_GENERATION,
                                                   &second_overlay),
              CBM_STORE_OK);
    cbm_node_t newer_main = {.project = "test",
                             .label = "Function",
                             .name = "newer_main",
                             .qualified_name = "test.newer_main",
                             .file_path = "main.go",
                             .properties_json = "{}"};
    cbm_store_file_delta_t second_delta = {.project = "test",
                                           .rel_path = "main.go",
                                           .generation = BASE_GENERATION,
                                           .nodes = &newer_main,
                                           .node_count = 1};
    ASSERT_EQ(cbm_store_publish_overlay_file_delta(live, &second_delta, second_overlay),
              CBM_STORE_OK);
    ASSERT_GT(cbm_store_upsert_node(oracle, &newer_main), 0);

    cbm_search_params_t params = {.project = "test",
                                  .pattern = "main|stable",
                                  .sort_by = "name",
                                  .limit = 10,
                                  .min_degree = 0};
    cbm_search_output_t active = {0};
    cbm_search_output_t expected = {0};
    ASSERT_EQ(cbm_store_search_overlay_view(live, &params, &active), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_search(oracle, &params, &expected), CBM_STORE_OK);
    ASSERT_EQ(active.total, expected.total);
    ASSERT_EQ(active.count, expected.count);
    ASSERT_EQ(active.count, 2);
    ASSERT_STR_EQ(active.results[0].node.name, expected.results[0].node.name);
    ASSERT_STR_EQ(active.results[1].node.name, expected.results[1].node.name);
    ASSERT_STR_EQ(active.results[0].node.name, "newer_main");
    ASSERT_STR_EQ(active.results[1].node.name, "stable");
    ASSERT_EQ(active.results[0].node.id, CBM_STORE_NO_NODE_ID);
    ASSERT_GT(active.results[1].node.id, CBM_STORE_NO_NODE_ID);

    cbm_store_search_free(&active);
    cbm_store_search_free(&expected);
    cbm_store_close(live);
    cbm_store_close(oracle);
    PASS();
}

TEST(store_search_overlay_view_uses_active_relationship_edges) {
    enum { BASE_GENERATION = 1 };
    cbm_store_t *live = cbm_store_open_memory();
    cbm_store_t *oracle = cbm_store_open_memory();
    ASSERT_NOT_NULL(live);
    ASSERT_NOT_NULL(oracle);
    ASSERT_EQ(cbm_store_upsert_project(live, "test", "/tmp/test"), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_upsert_project(oracle, "test", "/tmp/test"), CBM_STORE_OK);

    cbm_node_t old_main = {.project = "test",
                           .label = "Function",
                           .name = "old_main",
                           .qualified_name = "test.old_main",
                           .file_path = "main.go",
                           .properties_json = "{}"};
    cbm_node_t stable = {.project = "test",
                         .label = "Function",
                         .name = "stable",
                         .qualified_name = "test.stable",
                         .file_path = "stable.go",
                         .properties_json = "{}"};
    int64_t old_main_id = cbm_store_upsert_node(live, &old_main);
    int64_t stable_id = cbm_store_upsert_node(live, &stable);
    ASSERT_GT(old_main_id, 0);
    ASSERT_GT(stable_id, 0);
    cbm_edge_t old_edge = {.project = "test",
                           .source_id = old_main_id,
                           .target_id = stable_id,
                           .type = "CALLS"};
    ASSERT_GT(cbm_store_insert_edge(live, &old_edge), 0);
    ASSERT_GT(cbm_store_upsert_node(oracle, &stable), 0);

    int64_t overlay_generation = 0;
    ASSERT_EQ(cbm_store_reserve_overlay_generation(live, "test", BASE_GENERATION,
                                                   &overlay_generation),
              CBM_STORE_OK);
    cbm_node_t new_main = {.project = "test",
                           .label = "Function",
                           .name = "new_main",
                           .qualified_name = "test.new_main",
                           .file_path = "main.go",
                           .properties_json = "{}"};
    cbm_store_delta_edge_t new_edge = {.source_qn = "test.new_main",
                                       .target_qn = "test.stable",
                                       .type = "CALLS",
                                       .properties_json = "{}",
                                       .derived_kind = CBM_STORE_DERIVED_KIND_DIRECT};
    cbm_store_file_delta_t delta = {.project = "test",
                                    .rel_path = "main.go",
                                    .generation = BASE_GENERATION,
                                    .nodes = &new_main,
                                    .node_count = 1,
                                    .edges = &new_edge,
                                    .edge_count = 1};
    ASSERT_EQ(cbm_store_publish_overlay_file_delta(live, &delta, overlay_generation),
              CBM_STORE_OK);
    int64_t new_main_id = cbm_store_upsert_node(oracle, &new_main);
    ASSERT_GT(new_main_id, 0);
    int64_t oracle_stable_id = 0;
    cbm_node_t oracle_stable = {0};
    ASSERT_EQ(cbm_store_find_node_by_qn(oracle, "test", "test.stable", &oracle_stable),
              CBM_STORE_OK);
    oracle_stable_id = oracle_stable.id;
    cbm_node_free_fields(&oracle_stable);
    cbm_edge_t oracle_edge = {.project = "test",
                              .source_id = new_main_id,
                              .target_id = oracle_stable_id,
                              .type = "CALLS"};
    ASSERT_GT(cbm_store_insert_edge(oracle, &oracle_edge), 0);

    cbm_search_params_t params = {.project = "test",
                                  .relationship = "CALLS",
                                  .sort_by = "name",
                                  .limit = 10,
                                  .include_connected = true,
                                  .min_degree = 0,
                                  .max_degree = -1};
    cbm_search_output_t active = {0};
    cbm_search_output_t expected = {0};
    ASSERT_EQ(cbm_store_search_overlay_view(live, &params, &active), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_search(oracle, &params, &expected), CBM_STORE_OK);
    ASSERT_EQ(active.total, expected.total);
    ASSERT_EQ(active.count, expected.count);
    ASSERT_EQ(active.count, 2);
    ASSERT_STR_EQ(active.results[0].node.name, expected.results[0].node.name);
    ASSERT_STR_EQ(active.results[1].node.name, expected.results[1].node.name);
    ASSERT_STR_EQ(active.results[0].node.name, "new_main");
    ASSERT_STR_EQ(active.results[1].node.name, "stable");
    ASSERT_EQ(active.results[0].node.id, CBM_STORE_NO_NODE_ID);
    ASSERT_GT(active.results[1].in_degree, 0);
    ASSERT_GT(active.results[0].out_degree, 0);
    ASSERT_EQ(active.results[0].connected_count, 1);
    ASSERT_EQ(active.results[1].connected_count, 1);
    ASSERT_STR_EQ(active.results[0].connected_names[0], "stable");
    ASSERT_STR_EQ(active.results[1].connected_names[0], "new_main");

    cbm_node_t *active_names = NULL;
    int active_name_count = 0;
    ASSERT_EQ(cbm_store_find_nodes_by_name_overlay_view(live, "test", "new_main",
                                                        &active_names, &active_name_count),
              CBM_STORE_OK);
    ASSERT_EQ(active_name_count, 1);
    ASSERT_STR_EQ(active_names[0].qualified_name, "test.new_main");
    cbm_store_free_nodes(active_names, active_name_count);
    active_names = NULL;
    active_name_count = 0;
    ASSERT_EQ(cbm_store_find_nodes_by_name_overlay_view(live, "test", "old_main",
                                                        &active_names, &active_name_count),
              CBM_STORE_OK);
    ASSERT_EQ(active_name_count, 0);
    cbm_store_free_nodes(active_names, active_name_count);

    cbm_node_t *active_functions = NULL;
    int active_function_count = 0;
    ASSERT_EQ(cbm_store_find_nodes_by_label_overlay_view(live, "test", "Function",
                                                         &active_functions,
                                                         &active_function_count),
              CBM_STORE_OK);
    ASSERT_EQ(active_function_count, 2);
    ASSERT_STR_EQ(active_functions[0].name, "new_main");
    ASSERT_STR_EQ(active_functions[1].name, "stable");
    cbm_store_free_nodes(active_functions, active_function_count);
    active_functions = NULL;
    active_function_count = 0;
    ASSERT_EQ(cbm_store_find_nodes_by_label_overlay_view(live, "test", NULL, &active_functions,
                                                         &active_function_count),
              CBM_STORE_OK);
    ASSERT_EQ(active_function_count, 2);
    cbm_store_free_nodes(active_functions, active_function_count);
    active_functions = NULL;
    active_function_count = 0;
    ASSERT_EQ(cbm_store_find_nodes_by_label_overlay_view_limited(live, "test", NULL, 1,
                                                                 &active_functions,
                                                                 &active_function_count),
              CBM_STORE_OK);
    ASSERT_EQ(active_function_count, 1);
    cbm_store_free_nodes(active_functions, active_function_count);

    const char *edge_types[] = {"CALLS"};
    cbm_traverse_result_t active_trace = {0};
    ASSERT_EQ(cbm_store_bfs_overlay_view(live, "test", "test.new_main", "outbound",
                                         edge_types, 1, 1, 10, &active_trace),
              CBM_STORE_OK);
    ASSERT_EQ(active_trace.visited_count, 1);
    ASSERT_STR_EQ(active_trace.root.name, "new_main");
    ASSERT_STR_EQ(active_trace.visited[0].node.name, "stable");
    cbm_store_traverse_free(&active_trace);

    cbm_store_search_free(&active);
    cbm_store_search_free(&expected);
    cbm_store_close(live);
    cbm_store_close(oracle);
    PASS();
}

TEST(store_schema_counts_overlay_view_uses_active_nodes_and_edges) {
    enum { BASE_GENERATION = 1 };
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    cbm_node_t old_main = {.project = "test",
                           .label = "Function",
                           .name = "old_main",
                           .qualified_name = "test.old_main",
                           .file_path = "main.go",
                           .properties_json = "{\"old_role\":true}"};
    cbm_node_t stable = {.project = "test",
                         .label = "Class",
                         .name = "stable",
                         .qualified_name = "test.stable",
                         .file_path = "stable.go",
                         .properties_json = "{\"stable_role\":true}"};
    int64_t old_main_id = cbm_store_upsert_node(s, &old_main);
    int64_t stable_id = cbm_store_upsert_node(s, &stable);
    ASSERT_GT(old_main_id, 0);
    ASSERT_GT(stable_id, 0);
    cbm_edge_t old_edge = {.project = "test",
                           .source_id = old_main_id,
                           .target_id = stable_id,
                           .type = "CALLS",
                           .properties_json = "{\"old_edge\":true}"};
    ASSERT_GT(cbm_store_insert_edge(s, &old_edge), 0);

    cbm_schema_info_t schema = {0};
    ASSERT_EQ(cbm_store_get_schema_counts(s, "test", &schema), CBM_STORE_OK);
    ASSERT_EQ(schema.rel_pattern_count, 1);
    ASSERT_STR_EQ(schema.rel_patterns[0], "(Function)-[CALLS]->(Class) [1x]");
    cbm_store_schema_free(&schema);

    int64_t overlay_generation = 0;
    ASSERT_EQ(cbm_store_reserve_overlay_generation(s, "test", BASE_GENERATION,
                                                   &overlay_generation),
              CBM_STORE_OK);
    cbm_node_t new_main = {.project = "test",
                           .label = "Route",
                           .name = "/fresh",
                           .qualified_name = "test.route.fresh",
                           .file_path = "main.go",
                           .properties_json = "{\"fresh_role\":true}"};
    cbm_store_delta_edge_t new_edge = {.source_qn = "test.route.fresh",
                                       .target_qn = "test.stable",
                                       .type = "HANDLES",
                                       .properties_json = "{\"fresh_edge\":true}",
                                       .derived_kind = CBM_STORE_DERIVED_KIND_DIRECT};
    cbm_store_file_delta_t delta = {.project = "test",
                                    .rel_path = "main.go",
                                    .generation = BASE_GENERATION,
                                    .nodes = &new_main,
                                    .node_count = 1,
                                    .edges = &new_edge,
                                    .edge_count = 1};
    ASSERT_EQ(cbm_store_publish_overlay_file_delta(s, &delta, overlay_generation),
              CBM_STORE_OK);

    ASSERT_EQ(cbm_store_get_schema_counts_overlay_view(s, "test", &schema), CBM_STORE_OK);
    ASSERT_EQ(schema.node_label_count, 2);
    ASSERT_EQ(schema.edge_type_count, 1);
    int route_count = CBM_NOT_FOUND;
    int class_count = CBM_NOT_FOUND;
    int function_count = CBM_NOT_FOUND;
    for (int i = 0; i < schema.node_label_count; i++) {
        if (strcmp(schema.node_labels[i].label, "Route") == 0) {
            route_count = schema.node_labels[i].count;
        } else if (strcmp(schema.node_labels[i].label, "Class") == 0) {
            class_count = schema.node_labels[i].count;
        } else if (strcmp(schema.node_labels[i].label, "Function") == 0) {
            function_count = schema.node_labels[i].count;
        }
    }
    int handles_count = CBM_NOT_FOUND;
    int calls_count = CBM_NOT_FOUND;
    for (int i = 0; i < schema.edge_type_count; i++) {
        if (strcmp(schema.edge_types[i].type, "HANDLES") == 0) {
            handles_count = schema.edge_types[i].count;
        } else if (strcmp(schema.edge_types[i].type, "CALLS") == 0) {
            calls_count = schema.edge_types[i].count;
        }
    }
    ASSERT_EQ(route_count, 1);
    ASSERT_EQ(class_count, 1);
    ASSERT_EQ(function_count, CBM_NOT_FOUND);
    ASSERT_EQ(handles_count, 1);
    ASSERT_EQ(calls_count, CBM_NOT_FOUND);
    ASSERT_EQ(schema.rel_pattern_count, 1);
    ASSERT_STR_EQ(schema.rel_patterns[0], "(Route)-[HANDLES]->(Class) [1x]");
    cbm_store_schema_free(&schema);

    ASSERT_EQ(cbm_store_get_schema_overlay_view(s, "test", &schema), CBM_STORE_OK);
    bool saw_fresh_node_prop = false;
    bool saw_old_node_prop = false;
    bool saw_stable_node_prop = false;
    for (int i = 0; i < schema.node_label_count; i++) {
        for (int j = 0; j < schema.node_labels[i].property_count; j++) {
            const char *prop = schema.node_labels[i].properties[j];
            if (strcmp(schema.node_labels[i].label, "Route") == 0 &&
                strcmp(prop, "fresh_role") == 0) {
                saw_fresh_node_prop = true;
            }
            if (strcmp(prop, "old_role") == 0) {
                saw_old_node_prop = true;
            }
            if (strcmp(schema.node_labels[i].label, "Class") == 0 &&
                strcmp(prop, "stable_role") == 0) {
                saw_stable_node_prop = true;
            }
        }
    }
    bool saw_fresh_edge_prop = false;
    bool saw_old_edge_prop = false;
    for (int i = 0; i < schema.edge_type_count; i++) {
        for (int j = 0; j < schema.edge_types[i].property_count; j++) {
            const char *prop = schema.edge_types[i].properties[j];
            if (strcmp(schema.edge_types[i].type, "HANDLES") == 0 &&
                strcmp(prop, "fresh_edge") == 0) {
                saw_fresh_edge_prop = true;
            }
            if (strcmp(prop, "old_edge") == 0) {
                saw_old_edge_prop = true;
            }
        }
    }
    ASSERT(saw_fresh_node_prop);
    ASSERT(saw_stable_node_prop);
    ASSERT(!saw_old_node_prop);
    ASSERT(saw_fresh_edge_prop);
    ASSERT(!saw_old_edge_prop);
    ASSERT_EQ(schema.rel_pattern_count, 1);
    ASSERT_STR_EQ(schema.rel_patterns[0], "(Route)-[HANDLES]->(Class) [1x]");
    cbm_store_schema_free(&schema);

    cbm_store_close(s);
    PASS();
}

TEST(store_owner_metadata_crud) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "test", "/tmp/test");

    cbm_node_t nodes[2] = {
        {.project = "test",
         .label = "Function",
         .name = "main",
         .qualified_name = "test.main",
         .file_path = "main.go",
         .properties_json = "{}"},
        {.project = "test",
         .label = "Function",
         .name = "helper",
         .qualified_name = "test.helper",
         .file_path = "helper.go",
         .properties_json = "{}"},
    };
    int64_t main_id = cbm_store_upsert_node(s, &nodes[0]);
    int64_t helper_id = cbm_store_upsert_node(s, &nodes[1]);
    ASSERT_GT(main_id, 0);
    ASSERT_GT(helper_id, 0);

    cbm_edge_t edge = {.project = "test",
                       .source_id = main_id,
                       .target_id = helper_id,
                       .type = "CALLS",
                       .properties_json = "{}"};
    int64_t edge_id = cbm_store_insert_edge(s, &edge);
    ASSERT_GT(edge_id, 0);

    ASSERT_EQ(cbm_store_upsert_node_owner(s, "test", main_id, "main.go", 1), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_upsert_edge_owner(s, "test", edge_id, "main.go", NULL, 1),
              CBM_STORE_OK);
    ASSERT_EQ(store_count_metadata_owners(s, 0, "test", "main.go"), 1);
    ASSERT_EQ(store_count_metadata_owners(s, 1, "test", "main.go"), 1);

    ASSERT_EQ(cbm_store_upsert_node_owner(s, "test", main_id, "renamed.go", 2),
              CBM_STORE_OK);
    ASSERT_EQ(store_count_metadata_owners(s, 0, "test", "main.go"), 0);
    ASSERT_EQ(store_count_metadata_owners(s, 0, "test", "renamed.go"), 1);

    ASSERT_EQ(cbm_store_delete_edge_owners_by_file(s, "test", "main.go"), CBM_STORE_OK);
    ASSERT_EQ(store_count_metadata_owners(s, 1, "test", "main.go"), 0);
    ASSERT_EQ(store_count_metadata_owners(s, 0, "test", "renamed.go"), 1);

    ASSERT_EQ(cbm_store_delete_node_owners_by_file(s, "test", "renamed.go"), CBM_STORE_OK);
    ASSERT_EQ(store_count_metadata_owners(s, 0, "test", "renamed.go"), 0);

    cbm_store_close(s);
    PASS();
}

TEST(store_rebuild_file_delta_owners_derives_from_graph) {
    enum { TEST_GENERATION = 7 };
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    cbm_node_t nodes[7] = {
        {.project = "test",
         .label = "Function",
         .name = "main",
         .qualified_name = "test.main",
         .file_path = "main.go",
         .properties_json = "{}"},
        {.project = "test",
         .label = "Function",
         .name = "helper",
         .qualified_name = "test.helper",
         .file_path = "helper.go",
         .properties_json = "{}"},
        {.project = "test",
         .label = "Package",
         .name = "pkg",
         .qualified_name = "test.pkg",
         .file_path = "",
         .properties_json = "{}"},
        {.project = "test",
         .label = "File",
         .name = "main.go",
         .qualified_name = "test.main.__file__",
         .file_path = "main.go",
         .properties_json = "{}"},
        {.project = "test",
         .label = "File",
         .name = "helper.go",
         .qualified_name = "test.helper.__file__",
         .file_path = "helper.go",
         .properties_json = "{}"},
        {.project = "test",
         .label = "Folder",
         .name = "src",
         .qualified_name = "test.src",
         .file_path = "src",
         .properties_json = "{}"},
        {.project = "test",
         .label = "File",
         .name = "main.go",
         .qualified_name = "test.src.main.__file__",
         .file_path = "src/main.go",
         .properties_json = "{}"},
    };
    int64_t main_id = cbm_store_upsert_node(s, &nodes[0]);
    int64_t helper_id = cbm_store_upsert_node(s, &nodes[1]);
    int64_t package_id = cbm_store_upsert_node(s, &nodes[2]);
    int64_t main_file_id = cbm_store_upsert_node(s, &nodes[3]);
    int64_t helper_file_id = cbm_store_upsert_node(s, &nodes[4]);
    int64_t folder_id = cbm_store_upsert_node(s, &nodes[5]);
    int64_t nested_file_id = cbm_store_upsert_node(s, &nodes[6]);
    ASSERT_GT(main_id, 0);
    ASSERT_GT(helper_id, 0);
    ASSERT_GT(package_id, 0);
    ASSERT_GT(main_file_id, 0);
    ASSERT_GT(helper_file_id, 0);
    ASSERT_GT(folder_id, 0);
    ASSERT_GT(nested_file_id, 0);

    cbm_edge_t direct_edge = {.project = "test",
                              .source_id = main_id,
                              .target_id = helper_id,
                              .type = "CALLS",
                              .properties_json = "{}"};
    cbm_edge_t target_fallback_edge = {.project = "test",
                                       .source_id = package_id,
                                       .target_id = helper_id,
                                       .type = "CONTAINS",
                                       .properties_json = "{}"};
    cbm_edge_t structural_edge = {.project = "test",
                                  .source_id = folder_id,
                                  .target_id = nested_file_id,
                                  .type = "CONTAINS_FILE",
                                  .properties_json = "{}"};
    int64_t direct_edge_id = cbm_store_insert_edge(s, &direct_edge);
    int64_t fallback_edge_id = cbm_store_insert_edge(s, &target_fallback_edge);
    int64_t structural_edge_id = cbm_store_insert_edge(s, &structural_edge);
    ASSERT_GT(direct_edge_id, 0);
    ASSERT_GT(fallback_edge_id, 0);
    ASSERT_GT(structural_edge_id, 0);

    cbm_file_state_t same_stem_c_state = {.project = "test",
                                          .rel_path = "src/pipeline/pipeline.c",
                                          .content_hash = "hash-c",
                                          .git_oid = "",
                                          .mtime_ns = 1,
                                          .size = 2,
                                          .language = "C",
                                          .pass_fingerprint = "test",
                                          .generation = TEST_GENERATION,
                                          .indexed_at = "2026-07-02T00:00:00Z"};
    cbm_file_state_t same_stem_h_state = {.project = "test",
                                          .rel_path = "src/pipeline/pipeline.h",
                                          .content_hash = "hash-h",
                                          .git_oid = "",
                                          .mtime_ns = 1,
                                          .size = 2,
                                          .language = "C",
                                          .pass_fingerprint = "test",
                                          .generation = TEST_GENERATION,
                                          .indexed_at = "2026-07-02T00:00:00Z"};
    ASSERT_EQ(cbm_store_upsert_file_state(s, &same_stem_c_state), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_upsert_file_state(s, &same_stem_h_state), CBM_STORE_OK);

    cbm_node_t same_stem_nodes[] = {
        {.project = "test",
         .label = "Module",
         .name = "src/pipeline/pipeline.c",
         .qualified_name = "test.src.pipeline.pipeline",
         .file_path = "src/pipeline/pipeline.c",
         .properties_json = "{}"},
        {.project = "test",
         .label = "Function",
         .name = "cbm_pipeline_run",
         .qualified_name = "test.src.pipeline.pipeline.cbm_pipeline_run",
         .file_path = "src/pipeline/pipeline.c",
         .properties_json = "{}"},
        {.project = "test",
         .label = "Function",
         .name = "cbm_pipeline_mode",
         .qualified_name = "test.src.pipeline.pipeline.cbm_pipeline_mode",
         .file_path = "src/pipeline/pipeline.h",
         .properties_json = "{}"},
        /* Historical File-node QN collision: pipeline.c and pipeline.h both
         * map to test.src.pipeline.pipeline.__file__; file_state must still
         * allow ownership for src/pipeline/pipeline.c. */
        {.project = "test",
         .label = "File",
         .name = "pipeline.h",
         .qualified_name = "test.src.pipeline.pipeline.__file__",
         .file_path = "src/pipeline/pipeline.h",
         .properties_json = "{}"},
    };
    int64_t same_stem_module_id = cbm_store_upsert_node(s, &same_stem_nodes[0]);
    int64_t same_stem_c_fn_id = cbm_store_upsert_node(s, &same_stem_nodes[1]);
    int64_t same_stem_h_fn_id = cbm_store_upsert_node(s, &same_stem_nodes[2]);
    int64_t same_stem_file_id = cbm_store_upsert_node(s, &same_stem_nodes[3]);
    ASSERT_GT(same_stem_module_id, 0);
    ASSERT_GT(same_stem_c_fn_id, 0);
    ASSERT_GT(same_stem_h_fn_id, 0);
    ASSERT_GT(same_stem_file_id, 0);
    cbm_edge_t same_stem_call = {.project = "test",
                                 .source_id = same_stem_module_id,
                                 .target_id = same_stem_h_fn_id,
                                 .type = "CALLS",
                                 .properties_json = "{}"};
    int64_t same_stem_call_id = cbm_store_insert_edge(s, &same_stem_call);
    ASSERT_GT(same_stem_call_id, 0);

    ASSERT_EQ(cbm_store_upsert_node_owner(s, "test", main_id, "stale.go", TEST_GENERATION - 1),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_store_upsert_edge_owner(s, "test", direct_edge_id, "stale.go", NULL,
                                          TEST_GENERATION - 1),
              CBM_STORE_OK);
    ASSERT_EQ(store_count_metadata_owners(s, 0, "test", "stale.go"), 1);
    ASSERT_EQ(store_count_metadata_owners(s, 1, "test", "stale.go"), 1);

    ASSERT_EQ(cbm_store_rebuild_file_delta_owners(s, "test", TEST_GENERATION), CBM_STORE_OK);

    int node_owners = 0;
    int edge_owners = 0;
    ASSERT_EQ(cbm_store_count_file_delta_owners(s, "test", "main.go", &node_owners,
                                                &edge_owners),
              CBM_STORE_OK);
    ASSERT_EQ(node_owners, 2);
    ASSERT_EQ(edge_owners, 1);

    ASSERT_EQ(cbm_store_count_file_delta_owners(s, "test", "helper.go", &node_owners,
                                                &edge_owners),
              CBM_STORE_OK);
    ASSERT_EQ(node_owners, 2);
    ASSERT_EQ(edge_owners, 1);
    ASSERT_EQ(cbm_store_count_file_delta_owners(s, "test", "src", &node_owners, &edge_owners),
              CBM_STORE_OK);
    ASSERT_EQ(node_owners, 0);
    ASSERT_EQ(edge_owners, 0);
    ASSERT_EQ(cbm_store_count_file_delta_owners(s, "test", "src/main.go", &node_owners,
                                                &edge_owners),
              CBM_STORE_OK);
    ASSERT_EQ(node_owners, 1);
    ASSERT_EQ(edge_owners, 1);
    cbm_store_inbound_edge_t *inbound = NULL;
    int inbound_count = 0;
    ASSERT_EQ(cbm_store_list_file_delta_inbound_edges(s, "test", "src/main.go", &inbound,
                                                      &inbound_count),
              CBM_STORE_OK);
    ASSERT_EQ(inbound_count, 1);
    ASSERT_STR_EQ(inbound[0].source_qn, "test.src");
    ASSERT_STR_EQ(inbound[0].target_qn, "test.src.main.__file__");
    ASSERT_STR_EQ(inbound[0].type, "CONTAINS_FILE");
    ASSERT_STR_EQ(inbound[0].source_rel_path, "");
    ASSERT_STR_EQ(inbound[0].target_rel_path, "src/main.go");
    ASSERT_STR_EQ(inbound[0].edge_rel_path, "src/main.go");
    cbm_store_free_inbound_edges(inbound, inbound_count);

    ASSERT_EQ(cbm_store_count_file_delta_owners(s, "test", "src/pipeline/pipeline.c",
                                                &node_owners, &edge_owners),
              CBM_STORE_OK);
    ASSERT_EQ(node_owners, 2);
    ASSERT_EQ(edge_owners, 1);
    ASSERT_EQ(cbm_store_count_file_delta_owners(s, "test", "src/pipeline/pipeline.h",
                                                &node_owners, &edge_owners),
              CBM_STORE_OK);
    ASSERT_EQ(node_owners, 2);
    ASSERT_EQ(edge_owners, 0);

    inbound = NULL;
    inbound_count = 0;
    ASSERT_EQ(cbm_store_list_file_delta_inbound_edges(s, "test", "src/pipeline/pipeline.h",
                                                      &inbound, &inbound_count),
              CBM_STORE_OK);
    ASSERT_EQ(inbound_count, 1);
    ASSERT_STR_EQ(inbound[0].source_qn, "test.src.pipeline.pipeline");
    ASSERT_STR_EQ(inbound[0].target_qn, "test.src.pipeline.pipeline.cbm_pipeline_mode");
    ASSERT_STR_EQ(inbound[0].type, "CALLS");
    ASSERT_STR_EQ(inbound[0].source_rel_path, "src/pipeline/pipeline.c");
    ASSERT_STR_EQ(inbound[0].target_rel_path, "src/pipeline/pipeline.h");
    ASSERT_STR_EQ(inbound[0].edge_rel_path, "src/pipeline/pipeline.c");
    cbm_store_free_inbound_edges(inbound, inbound_count);

    ASSERT_EQ(store_count_metadata_owners(s, 0, "test", "stale.go"), 0);
    ASSERT_EQ(store_count_metadata_owners(s, 1, "test", "stale.go"), 0);

    cbm_store_close(s);
    PASS();
}

TEST(store_import_export_metadata_crud) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    cbm_node_t node = {.project = "test",
                       .label = "Function",
                       .name = "Helper",
                       .qualified_name = "test.lib.Helper",
                       .file_path = "lib.go",
                       .properties_json = "{}"};
    int64_t node_id = cbm_store_upsert_node(s, &node);
    ASSERT_GT(node_id, 0);

    ASSERT_EQ(cbm_store_upsert_symbol_export(s, "test", "test.lib.Helper", "lib.go", node_id, 1),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_store_upsert_symbol_export(s, "test", "test.lib.Unresolved", "lib.go",
                                             CBM_STORE_NO_NODE_ID, 1),
              CBM_STORE_OK);

    char **items = NULL;
    int count = 0;
    ASSERT_EQ(cbm_store_list_symbol_exports_by_file(s, "test", "lib.go", &items, &count),
              CBM_STORE_OK);
    ASSERT_EQ(count, 2);
    ASSERT_STR_EQ(items[0], "test.lib.Helper");
    ASSERT_STR_EQ(items[1], "test.lib.Unresolved");
    store_free_string_array(items, count);

    ASSERT_EQ(cbm_store_upsert_import_ref(s, "test", "caller.go", "test.lib", "Helper",
                                          "test.lib.Helper", 1),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_store_upsert_import_ref(s, "test", "other.go", "test.other", "Other",
                                          "test.other.Other", 1),
              CBM_STORE_OK);

    items = NULL;
    count = 0;
    ASSERT_EQ(cbm_store_list_import_ref_paths_by_target(s, "test", "test.lib.Helper", &items,
                                                       &count),
              CBM_STORE_OK);
    ASSERT_EQ(count, 1);
    ASSERT_STR_EQ(items[0], "caller.go");
    store_free_string_array(items, count);

    items = NULL;
    count = 0;
    ASSERT_EQ(cbm_store_list_import_ref_paths_for_export_file(s, "test", "lib.go", &items,
                                                             &count),
              CBM_STORE_OK);
    ASSERT_EQ(count, 1);
    ASSERT_STR_EQ(items[0], "caller.go");
    store_free_string_array(items, count);

    ASSERT_EQ(cbm_store_upsert_import_ref(s, "test", "caller.go", "test.lib", "Helper",
                                          "test.lib.Renamed", 2),
              CBM_STORE_OK);
    items = NULL;
    count = 0;
    ASSERT_EQ(cbm_store_list_import_ref_paths_by_target(s, "test", "test.lib.Helper", &items,
                                                       &count),
              CBM_STORE_OK);
    ASSERT_EQ(count, 0);
    store_free_string_array(items, count);

    items = NULL;
    count = 0;
    ASSERT_EQ(cbm_store_list_import_ref_paths_by_target(s, "test", "test.lib.Renamed", &items,
                                                       &count),
              CBM_STORE_OK);
    ASSERT_EQ(count, 1);
    ASSERT_STR_EQ(items[0], "caller.go");
    store_free_string_array(items, count);

    ASSERT_EQ(cbm_store_delete_import_refs_by_file(s, "test", "caller.go"), CBM_STORE_OK);
    items = NULL;
    count = 0;
    ASSERT_EQ(cbm_store_list_import_ref_paths_by_target(s, "test", "test.lib.Renamed", &items,
                                                       &count),
              CBM_STORE_OK);
    ASSERT_EQ(count, 0);
    store_free_string_array(items, count);

    ASSERT_EQ(cbm_store_delete_symbol_exports_by_file(s, "test", "lib.go"), CBM_STORE_OK);
    items = NULL;
    count = 0;
    ASSERT_EQ(cbm_store_list_symbol_exports_by_file(s, "test", "lib.go", &items, &count),
              CBM_STORE_OK);
    ASSERT_EQ(count, 0);
    store_free_string_array(items, count);

    cbm_store_close(s);
    PASS();
}

TEST(store_file_delta_affected_paths_from_exports_and_imports) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    ASSERT_EQ(cbm_store_upsert_symbol_export(s, "test", "test.lib.Kept", "lib.go",
                                             CBM_STORE_NO_NODE_ID, 1),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_store_upsert_symbol_export(s, "test", "test.lib.Removed", "lib.go",
                                             CBM_STORE_NO_NODE_ID, 1),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_store_upsert_import_ref(s, "test", "caller.go", "test.lib", "Kept",
                                          "test.lib.Kept", 1),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_store_upsert_import_ref(s, "test", "caller.go", "test.lib", "KeptAgain",
                                          "test.lib.Kept", 1),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_store_upsert_import_ref(s, "test", "removed_user.go", "test.lib", "Removed",
                                          "test.lib.Removed", 1),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_store_upsert_import_ref(s, "test", "new_user.go", "test.lib", "New",
                                          "test.lib.New", 1),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_store_upsert_import_ref(s, "test", "unrelated.go", "test.other", "Other",
                                          "test.other.Other", 1),
              CBM_STORE_OK);

    const char *new_exports[] = {"test.lib.Kept", "test.lib.New"};
    char **paths = NULL;
    int count = 0;
    ASSERT_EQ(cbm_store_list_file_delta_affected_paths(s, "test", "lib.go", new_exports, 2,
                                                       &paths, &count),
              CBM_STORE_OK);
    ASSERT_EQ(count, 4);
    ASSERT_EQ(store_string_array_contains(paths, count, "lib.go"), 1);
    ASSERT_EQ(store_string_array_contains(paths, count, "caller.go"), 1);
    ASSERT_EQ(store_string_array_contains(paths, count, "removed_user.go"), 1);
    ASSERT_EQ(store_string_array_contains(paths, count, "new_user.go"), 1);
    ASSERT_EQ(store_string_array_contains(paths, count, "unrelated.go"), 0);
    store_free_string_array(paths, count);

    cbm_store_close(s);
    PASS();
}

TEST(store_file_delta_affected_paths_high_fanout_dedupes) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_upsert_symbol_export(s, "test", "test.lib.Hot", "hot.go",
                                             CBM_STORE_NO_NODE_ID, 1),
              CBM_STORE_OK);

    for (int i = 0; i < CBM_SZ_16; i++) {
        char rel_path[CBM_SZ_64];
        char local_name[CBM_SZ_64];
        snprintf(rel_path, sizeof(rel_path), "fan_%02d.go", i);
        snprintf(local_name, sizeof(local_name), "Hot%d", i);
        ASSERT_EQ(cbm_store_upsert_import_ref(s, "test", rel_path, "test.lib", local_name,
                                              "test.lib.Hot", 1),
                  CBM_STORE_OK);
        ASSERT_EQ(cbm_store_upsert_import_ref(s, "test", rel_path, "test.lib", "HotDuplicate",
                                              "test.lib.Hot", 1),
                  CBM_STORE_OK);
    }

    char **paths = NULL;
    int count = 0;
    ASSERT_EQ(cbm_store_list_file_delta_affected_paths(s, "test", "hot.go", NULL, 0, &paths,
                                                       &count),
              CBM_STORE_OK);
    ASSERT_EQ(count, CBM_SZ_16 + 1);
    ASSERT_EQ(store_string_array_contains(paths, count, "hot.go"), 1);
    ASSERT_EQ(store_string_array_contains(paths, count, "fan_00.go"), 1);
    ASSERT_EQ(store_string_array_contains(paths, count, "fan_15.go"), 1);
    store_free_string_array(paths, count);

    cbm_store_close(s);
    PASS();
}

TEST(store_file_delta_publish_rolls_back_on_failure) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    cbm_node_t old_nodes[1] = {{.project = "test",
                                .label = "Function",
                                .name = "Old",
                                .qualified_name = "test.main.Old",
                                .file_path = "main.go",
                                .properties_json = "{}"}};
    cbm_file_hash_t old_hash = {
        .project = "test", .rel_path = "main.go", .sha256 = "old-hash", .mtime_ns = 1, .size = 10};
    cbm_file_state_t old_state = {.project = "test",
                                  .rel_path = "main.go",
                                  .content_hash = "old-content",
                                  .git_oid = "old-oid",
                                  .mtime_ns = 1,
                                  .size = 10,
                                  .language = "c",
                                  .pass_fingerprint = "pass-a",
                                  .generation = 1,
                                  .indexed_at = "2026-06-30T00:00:00Z"};
    cbm_store_symbol_export_t old_exports[1] = {
        {.qualified_name = "test.main.Old", .node_id = CBM_STORE_NO_NODE_ID}};
    cbm_store_file_delta_t old_delta = {.project = "test",
                                        .rel_path = "main.go",
                                        .generation = 1,
                                        .file_hash = &old_hash,
                                        .file_state = &old_state,
                                        .nodes = old_nodes,
                                        .node_count = 1,
                                        .exports = old_exports,
                                        .export_count = 1,
                                        .derived_view_name = CBM_STORE_DERIVED_VIEW_NODES_FTS,
                                        .derived_status = CBM_STORE_DERIVED_STATUS_COMPLETE};
    ASSERT_EQ(cbm_store_publish_file_delta(s, &old_delta), CBM_STORE_OK);

    cbm_node_t new_nodes[1] = {{.project = "test",
                                .label = "Function",
                                .name = "New",
                                .qualified_name = "test.main.New",
                                .file_path = "main.go",
                                .properties_json = "{}"}};
    cbm_store_delta_edge_t bad_edges[1] = {{.source_qn = "test.main.New",
                                            .target_qn = "test.main.Missing",
                                            .type = "CALLS",
                                            .properties_json = "{}"}};
    cbm_file_hash_t new_hash = {
        .project = "test", .rel_path = "main.go", .sha256 = "new-hash", .mtime_ns = 2, .size = 20};
    cbm_file_state_t new_state = {.project = "test",
                                  .rel_path = "main.go",
                                  .content_hash = "new-content",
                                  .git_oid = "new-oid",
                                  .mtime_ns = 2,
                                  .size = 20,
                                  .language = "c",
                                  .pass_fingerprint = "pass-b",
                                  .generation = 2,
                                  .indexed_at = "2026-06-30T00:01:00Z"};
    cbm_store_file_delta_t bad_delta = {.project = "test",
                                        .rel_path = "main.go",
                                        .generation = 2,
                                        .file_hash = &new_hash,
                                        .file_state = &new_state,
                                        .nodes = new_nodes,
                                        .node_count = 1,
                                        .edges = bad_edges,
                                        .edge_count = 1,
                                        .derived_view_name = CBM_STORE_DERIVED_VIEW_NODES_FTS,
                                        .derived_status = CBM_STORE_DERIVED_STATUS_COMPLETE};
    ASSERT_EQ(cbm_store_publish_file_delta(s, &bad_delta), CBM_STORE_NOT_FOUND);

    ASSERT_EQ(store_node_qn_exists(s, "test", "test.main.Old"), 1);
    ASSERT_EQ(store_node_qn_exists(s, "test", "test.main.New"), 0);
    ASSERT_EQ(store_count_metadata_owners(s, 0, "test", "main.go"), 1);
    ASSERT_EQ(cbm_store_count_edges(s, "test"), 0);

    cbm_file_hash_t *hashes = NULL;
    int count = 0;
    ASSERT_EQ(cbm_store_get_file_hashes(s, "test", &hashes, &count), CBM_STORE_OK);
    ASSERT_EQ(count, 1);
    ASSERT_STR_EQ(hashes[0].sha256, "old-hash");
    cbm_store_free_file_hashes(hashes, count);

    cbm_file_state_t got = {0};
    ASSERT_EQ(cbm_store_get_file_state(s, "test", "main.go", &got), CBM_STORE_OK);
    ASSERT_STR_EQ(got.content_hash, "old-content");
    ASSERT_EQ(got.generation, 1);
    cbm_store_file_state_free_fields(&got);

    char **exports = NULL;
    count = 0;
    ASSERT_EQ(cbm_store_list_symbol_exports_by_file(s, "test", "main.go", &exports, &count),
              CBM_STORE_OK);
    ASSERT_EQ(count, 1);
    ASSERT_STR_EQ(exports[0], "test.main.Old");
    store_free_string_array(exports, count);
    ASSERT_EQ(store_count_derived_view_state(s, "test", CBM_STORE_DERIVED_VIEW_NODES_FTS, 1,
                                             CBM_STORE_DERIVED_STATUS_COMPLETE),
              1);

    cbm_store_close(s);
    PASS();
}

static int store_publish_helper_file_delta_named(cbm_store_t *s, int64_t generation,
                                                const char *name, const char *qualified_name,
                                                const char *sha256, const char *content_hash) {
    cbm_node_t nodes[1] = {{.project = "test",
                            .label = "Function",
                            .name = name,
                            .qualified_name = qualified_name,
                            .file_path = "helper.go",
                            .properties_json = "{}"}};
    cbm_file_hash_t hash = {.project = "test",
                            .rel_path = "helper.go",
                            .sha256 = sha256,
                            .mtime_ns = 1,
                            .size = 10};
    cbm_file_state_t state = {.project = "test",
                              .rel_path = "helper.go",
                              .content_hash = content_hash,
                              .git_oid = "",
                              .mtime_ns = 1,
                              .size = 10,
                              .language = "c",
                              .pass_fingerprint = "pass-a",
                              .generation = generation,
                              .indexed_at = "2026-06-30T00:00:00Z"};
    cbm_store_symbol_export_t exports[1] = {
        {.qualified_name = qualified_name, .node_id = CBM_STORE_NO_NODE_ID}};
    cbm_store_file_delta_t delta = {.project = "test",
                                    .rel_path = "helper.go",
                                    .generation = generation,
                                    .file_hash = &hash,
                                    .file_state = &state,
                                    .nodes = nodes,
                                    .node_count = 1,
                                    .exports = exports,
                                    .export_count = 1};
    return cbm_store_publish_file_delta(s, &delta);
}

static int store_publish_helper_file_delta(cbm_store_t *s, int64_t generation) {
    return store_publish_helper_file_delta_named(s, generation, "Helper", "test.helper.Helper",
                                                "helper-hash", "helper-content");
}

static int store_publish_new_helper_file_delta(cbm_store_t *s, int64_t generation) {
    return store_publish_helper_file_delta_named(s, generation, "NewHelper",
                                                "test.helper.NewHelper", "new-helper-hash",
                                                "new-helper-content");
}

static int store_publish_old_main_delta(cbm_store_t *s, int64_t generation) {
    cbm_node_t nodes[1] = {{.project = "test",
                            .label = "Function",
                            .name = "Old",
                            .qualified_name = "test.main.Old",
                            .file_path = "main.go",
                            .properties_json = "{}"}};
    cbm_file_hash_t hash = {.project = "test",
                            .rel_path = "main.go",
                            .sha256 = "old-main-hash",
                            .mtime_ns = 1,
                            .size = 10};
    cbm_file_state_t state = {.project = "test",
                              .rel_path = "main.go",
                              .content_hash = "old-main-content",
                              .git_oid = "",
                              .mtime_ns = 1,
                              .size = 10,
                              .language = "c",
                              .pass_fingerprint = "pass-a",
                              .generation = generation,
                              .indexed_at = "2026-06-30T00:00:00Z"};
    cbm_store_symbol_export_t exports[1] = {
        {.qualified_name = "test.main.Old", .node_id = CBM_STORE_NO_NODE_ID}};
    cbm_store_file_delta_t delta = {.project = "test",
                                    .rel_path = "main.go",
                                    .generation = generation,
                                    .file_hash = &hash,
                                    .file_state = &state,
                                    .nodes = nodes,
                                    .node_count = 1,
                                    .exports = exports,
                                    .export_count = 1};
    return cbm_store_publish_file_delta(s, &delta);
}

static int store_publish_new_main_delta_target(cbm_store_t *s, int64_t generation,
                                               const char *target_qn) {
    cbm_node_t nodes[1] = {{.project = "test",
                            .label = "Function",
                            .name = "New",
                            .qualified_name = "test.main.New",
                            .file_path = "main.go",
                            .properties_json = "{}"}};
    cbm_store_delta_edge_t edges[1] = {{.source_qn = "test.main.New",
                                        .target_qn = target_qn,
                                        .type = "CALLS",
                                        .properties_json = "{}"}};
    cbm_file_hash_t hash = {.project = "test",
                            .rel_path = "main.go",
                            .sha256 = "new-main-hash",
                            .mtime_ns = 2,
                            .size = 20};
    cbm_file_state_t state = {.project = "test",
                              .rel_path = "main.go",
                              .content_hash = "new-main-content",
                              .git_oid = "",
                              .mtime_ns = 2,
                              .size = 20,
                              .language = "c",
                              .pass_fingerprint = "pass-b",
                              .generation = generation,
                              .indexed_at = "2026-06-30T00:01:00Z"};
    cbm_store_symbol_export_t exports[1] = {
        {.qualified_name = "test.main.New", .node_id = CBM_STORE_NO_NODE_ID}};
    cbm_store_import_ref_t imports[1] = {
        {.import_text = "test.helper", .local_name = "Helper", .target_qn = target_qn}};
    cbm_store_file_delta_t delta = {.project = "test",
                                    .rel_path = "main.go",
                                    .generation = generation,
                                    .file_hash = &hash,
                                    .file_state = &state,
                                    .nodes = nodes,
                                    .node_count = 1,
                                    .edges = edges,
                                    .edge_count = 1,
                                    .exports = exports,
                                    .export_count = 1,
                                    .imports = imports,
                                    .import_count = 1,
                                    .derived_view_name = CBM_STORE_DERIVED_VIEW_NODES_FTS,
                                    .derived_status = CBM_STORE_DERIVED_STATUS_COMPLETE};
    return cbm_store_publish_file_delta(s, &delta);
}

static int store_publish_new_main_delta(cbm_store_t *s, int64_t generation) {
    return store_publish_new_main_delta_target(s, generation, "test.helper.Helper");
}

static int store_publish_new_main_to_new_helper_delta(cbm_store_t *s, int64_t generation) {
    return store_publish_new_main_delta_target(s, generation, "test.helper.NewHelper");
}

static int store_publish_bad_main_delta(cbm_store_t *s, int64_t generation) {
    cbm_node_t nodes[1] = {{.project = "test",
                            .label = "Function",
                            .name = "New",
                            .qualified_name = "test.main.New",
                            .file_path = "main.go",
                            .properties_json = "{}"}};
    cbm_store_delta_edge_t edges[1] = {{.source_qn = "test.main.New",
                                        .target_qn = "test.main.Missing",
                                        .type = "CALLS",
                                        .properties_json = "{}"}};
    cbm_file_hash_t hash = {.project = "test",
                            .rel_path = "main.go",
                            .sha256 = "bad-main-hash",
                            .mtime_ns = 2,
                            .size = 20};
    cbm_file_state_t state = {.project = "test",
                              .rel_path = "main.go",
                              .content_hash = "bad-main-content",
                              .git_oid = "",
                              .mtime_ns = 2,
                              .size = 20,
                              .language = "c",
                              .pass_fingerprint = "pass-b",
                              .generation = generation,
                              .indexed_at = "2026-06-30T00:01:00Z"};
    cbm_store_file_delta_t delta = {.project = "test",
                                    .rel_path = "main.go",
                                    .generation = generation,
                                    .file_hash = &hash,
                                    .file_state = &state,
                                    .nodes = nodes,
                                    .node_count = 1,
                                    .edges = edges,
                                    .edge_count = 1,
                                    .derived_view_name = CBM_STORE_DERIVED_VIEW_NODES_FTS,
                                    .derived_status = CBM_STORE_DERIVED_STATUS_COMPLETE};
    return cbm_store_publish_file_delta(s, &delta);
}

TEST(store_file_delta_publish_matches_fresh_final_graph) {
    enum {
        BASE_GENERATION = 1,
        FINAL_GENERATION = 2,
        EXPECTED_FINAL_NODES = 2,
        EXPECTED_FINAL_EDGES = 1,
    };
    cbm_store_t *delta_store = cbm_store_open_memory();
    cbm_store_t *fresh_store = cbm_store_open_memory();
    ASSERT_NOT_NULL(delta_store);
    ASSERT_NOT_NULL(fresh_store);
    ASSERT_EQ(cbm_store_upsert_project(delta_store, "test", "/tmp/test"), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_upsert_project(fresh_store, "test", "/tmp/test"), CBM_STORE_OK);

    int64_t generation = 0;
    ASSERT_EQ(cbm_store_reserve_index_generation(delta_store, "test", NULL, NULL, &generation),
              CBM_STORE_OK);
    ASSERT_EQ(generation, BASE_GENERATION);
    ASSERT_EQ(store_publish_helper_file_delta(delta_store, generation), CBM_STORE_OK);
    ASSERT_EQ(store_publish_old_main_delta(delta_store, generation), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_finish_index_generation(delta_store, "test", generation,
                                                CBM_STORE_INDEX_STATUS_COMPLETE),
              CBM_STORE_OK);

    ASSERT_EQ(cbm_store_reserve_index_generation(delta_store, "test", NULL, NULL, &generation),
              CBM_STORE_OK);
    ASSERT_EQ(generation, FINAL_GENERATION);
    ASSERT_EQ(store_publish_new_main_delta(delta_store, generation), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_finish_index_generation(delta_store, "test", generation,
                                                CBM_STORE_INDEX_STATUS_COMPLETE),
              CBM_STORE_OK);

    ASSERT_EQ(cbm_store_reserve_index_generation(fresh_store, "test", NULL, NULL, &generation),
              CBM_STORE_OK);
    ASSERT_EQ(generation, BASE_GENERATION);
    ASSERT_EQ(store_publish_helper_file_delta(fresh_store, generation), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_finish_index_generation(fresh_store, "test", generation,
                                                CBM_STORE_INDEX_STATUS_COMPLETE),
              CBM_STORE_OK);

    ASSERT_EQ(cbm_store_reserve_index_generation(fresh_store, "test", NULL, NULL, &generation),
              CBM_STORE_OK);
    ASSERT_EQ(generation, FINAL_GENERATION);
    ASSERT_EQ(store_publish_new_main_delta(fresh_store, generation), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_finish_index_generation(fresh_store, "test", generation,
                                                CBM_STORE_INDEX_STATUS_COMPLETE),
              CBM_STORE_OK);

    ASSERT_EQ(cbm_store_count_nodes(delta_store, "test"), cbm_store_count_nodes(fresh_store, "test"));
    ASSERT_EQ(cbm_store_count_edges(delta_store, "test"), cbm_store_count_edges(fresh_store, "test"));
    ASSERT_EQ(cbm_store_count_nodes(delta_store, "test"), EXPECTED_FINAL_NODES);
    ASSERT_EQ(cbm_store_count_edges(delta_store, "test"), EXPECTED_FINAL_EDGES);
    ASSERT_EQ(store_node_qn_exists(delta_store, "test", "test.main.Old"), 0);
    ASSERT_EQ(store_node_qn_exists(fresh_store, "test", "test.main.Old"), 0);
    ASSERT_EQ(store_node_qn_exists(delta_store, "test", "test.main.New"), 1);
    ASSERT_EQ(store_node_qn_exists(fresh_store, "test", "test.main.New"), 1);
    ASSERT_EQ(store_node_qn_exists(delta_store, "test", "test.helper.Helper"), 1);
    ASSERT_EQ(store_node_qn_exists(fresh_store, "test", "test.helper.Helper"), 1);

    cbm_file_state_t got = {0};
    ASSERT_EQ(cbm_store_get_file_state(delta_store, "test", "main.go", &got), CBM_STORE_OK);
    ASSERT_STR_EQ(got.content_hash, "new-main-content");
    ASSERT_EQ(got.generation, FINAL_GENERATION);
    cbm_store_file_state_free_fields(&got);
    ASSERT_EQ(cbm_store_get_file_state(fresh_store, "test", "main.go", &got), CBM_STORE_OK);
    ASSERT_STR_EQ(got.content_hash, "new-main-content");
    ASSERT_EQ(got.generation, FINAL_GENERATION);
    cbm_store_file_state_free_fields(&got);

    char **items = NULL;
    int count = 0;
    ASSERT_EQ(cbm_store_list_symbol_exports_by_file(delta_store, "test", "main.go", &items,
                                                    &count),
              CBM_STORE_OK);
    ASSERT_EQ(count, 1);
    ASSERT_STR_EQ(items[0], "test.main.New");
    store_free_string_array(items, count);
    items = NULL;
    count = 0;
    ASSERT_EQ(cbm_store_list_import_ref_paths_by_target(delta_store, "test",
                                                        "test.helper.Helper", &items, &count),
              CBM_STORE_OK);
    ASSERT_EQ(count, 1);
    ASSERT_STR_EQ(items[0], "main.go");
    store_free_string_array(items, count);

    char *tmp = th_mktempdir("cbm_delta_graph_diff");
    ASSERT_NOT_NULL(tmp);
    const char *delta_db = TH_PATH(tmp, "delta.db");
    const char *fresh_db = TH_PATH(tmp, "fresh.db");
    ASSERT_EQ(cbm_store_dump_to_file(delta_store, delta_db), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_dump_to_file(fresh_store, fresh_db), CBM_STORE_OK);
    char diff_err[CBM_SZ_8K] = {0};
    ASSERT_EQ(cbm_test_compare_canonical_graphs(delta_db, fresh_db, "test", diff_err,
                                                sizeof(diff_err)),
              0);

    cbm_store_close(delta_store);
    cbm_store_close(fresh_store);
    th_cleanup(tmp);
    PASS();
}

TEST(store_file_delta_graph_noop_refreshes_metadata_only) {
    enum {
        BASE_GENERATION = 1,
        FINAL_GENERATION = 2,
    };
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);
    int64_t generation = 0;
    ASSERT_EQ(cbm_store_reserve_index_generation(s, "test", NULL, NULL, &generation),
              CBM_STORE_OK);
    ASSERT_EQ(generation, BASE_GENERATION);
    ASSERT_EQ(store_publish_helper_file_delta(s, BASE_GENERATION), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_finish_index_generation(s, "test", BASE_GENERATION,
                                                CBM_STORE_INDEX_STATUS_COMPLETE),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_store_delete_symbol_exports_by_file(s, "test", "helper.go"), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_reserve_index_generation(s, "test", NULL, NULL, &generation),
              CBM_STORE_OK);
    ASSERT_EQ(generation, FINAL_GENERATION);

    cbm_node_t same_nodes[1] = {{.project = "test",
                                 .label = "Function",
                                 .name = "Helper",
                                 .qualified_name = "test.helper.Helper",
                                 .file_path = "helper.go",
                                 .properties_json = "{}"}};
    cbm_file_hash_t hash = {.project = "test",
                            .rel_path = "helper.go",
                            .sha256 = "helper-hash-v2",
                            .mtime_ns = 2,
                            .size = 20};
    cbm_file_state_t state = {.project = "test",
                              .rel_path = "helper.go",
                              .content_hash = "helper-content-v2",
                              .git_oid = "",
                              .mtime_ns = 2,
                              .size = 20,
                              .language = "c",
                              .pass_fingerprint = "pass-a",
                              .generation = FINAL_GENERATION,
                              .indexed_at = "2026-06-30T00:02:00Z"};
    cbm_store_symbol_export_t exports[1] = {
        {.qualified_name = "test.helper.Helper", .node_id = CBM_STORE_NO_NODE_ID}};
    cbm_store_file_delta_t same_delta = {.project = "test",
                                         .rel_path = "helper.go",
                                         .generation = FINAL_GENERATION,
                                         .file_hash = &hash,
                                         .file_state = &state,
                                         .nodes = same_nodes,
                                         .node_count = 1,
                                         .exports = exports,
                                         .export_count = 1};
    const cbm_store_file_delta_t *same_deltas[] = {&same_delta};
    bool graph_equal = false;
    ASSERT_EQ(cbm_store_file_delta_batch_graph_equal(s, same_deltas, 1, &graph_equal),
              CBM_STORE_OK);
    ASSERT_TRUE(graph_equal);
    ASSERT_EQ(cbm_store_refresh_file_delta_metadata_batch_complete(s, same_deltas, 1),
              CBM_STORE_OK);
    ASSERT_EQ(store_node_qn_exists(s, "test", "test.helper.Helper"), 1);
    ASSERT_EQ(cbm_store_count_edges(s, "test"), 0);
    char **restored_exports = NULL;
    int restored_export_count = 0;
    ASSERT_EQ(cbm_store_list_symbol_exports_by_file(s, "test", "helper.go", &restored_exports,
                                                    &restored_export_count),
              CBM_STORE_OK);
    ASSERT_EQ(restored_export_count, 1);
    store_free_string_array(restored_exports, restored_export_count);

    cbm_file_state_t got = {0};
    ASSERT_EQ(cbm_store_get_file_state(s, "test", "helper.go", &got), CBM_STORE_OK);
    ASSERT_STR_EQ(got.content_hash, "helper-content-v2");
    ASSERT_EQ(got.generation, FINAL_GENERATION);
    cbm_store_file_state_free_fields(&got);

    cbm_node_t changed_nodes[1] = {{.project = "test",
                                    .label = "Function",
                                    .name = "Renamed",
                                    .qualified_name = "test.helper.Renamed",
                                    .file_path = "helper.go",
                                    .properties_json = "{}"}};
    cbm_store_symbol_export_t changed_exports[1] = {
        {.qualified_name = "test.helper.Renamed", .node_id = CBM_STORE_NO_NODE_ID}};
    cbm_store_file_delta_t changed_delta = same_delta;
    changed_delta.nodes = changed_nodes;
    changed_delta.exports = changed_exports;
    const cbm_store_file_delta_t *changed_deltas[] = {&changed_delta};
    graph_equal = true;
    ASSERT_EQ(cbm_store_file_delta_batch_graph_equal(s, changed_deltas, 1, &graph_equal),
              CBM_STORE_OK);
    ASSERT_FALSE(graph_equal);

    cbm_store_close(s);
    PASS();
}

TEST(store_file_delta_publish_failure_finishes_generation_failed) {
    enum { BASE_GENERATION = 1, FAILED_GENERATION = 2 };
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    int64_t generation = 0;
    ASSERT_EQ(cbm_store_reserve_index_generation(s, "test", NULL, NULL, &generation),
              CBM_STORE_OK);
    ASSERT_EQ(generation, BASE_GENERATION);
    ASSERT_EQ(store_publish_helper_file_delta(s, generation), CBM_STORE_OK);
    ASSERT_EQ(store_publish_old_main_delta(s, generation), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_finish_index_generation(s, "test", generation,
                                                CBM_STORE_INDEX_STATUS_COMPLETE),
              CBM_STORE_OK);

    ASSERT_EQ(cbm_store_reserve_index_generation(s, "test", NULL, NULL, &generation),
              CBM_STORE_OK);
    ASSERT_EQ(generation, FAILED_GENERATION);
    ASSERT_EQ(store_publish_bad_main_delta(s, generation), CBM_STORE_NOT_FOUND);
    ASSERT_EQ(cbm_store_finish_index_generation(s, "test", generation,
                                                CBM_STORE_INDEX_STATUS_FAILED),
              CBM_STORE_OK);

    ASSERT_EQ(store_node_qn_exists(s, "test", "test.main.Old"), 1);
    ASSERT_EQ(store_node_qn_exists(s, "test", "test.main.New"), 0);
    ASSERT_EQ(store_count_index_generation(s, "test", FAILED_GENERATION,
                                           CBM_STORE_INDEX_STATUS_FAILED, "", "",
                                           STORE_TEST_COMPLETED_SET),
              1);
    ASSERT_EQ(store_count_index_generation(s, "test", FAILED_GENERATION,
                                           CBM_STORE_INDEX_STATUS_RESERVED, "", "",
                                           STORE_TEST_COMPLETED_NULL),
              0);

    cbm_store_close(s);
    PASS();
}

TEST(store_file_delta_publish_multifile_generation) {
    enum {
        BASE_GENERATION = 1,
        FINAL_GENERATION = 2,
        EXPECTED_FINAL_NODES = 2,
        EXPECTED_FINAL_EDGES = 1,
    };
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    int64_t generation = 0;
    ASSERT_EQ(cbm_store_reserve_index_generation(s, "test", NULL, NULL, &generation),
              CBM_STORE_OK);
    ASSERT_EQ(generation, BASE_GENERATION);
    ASSERT_EQ(store_publish_helper_file_delta(s, generation), CBM_STORE_OK);
    ASSERT_EQ(store_publish_old_main_delta(s, generation), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_finish_index_generation(s, "test", generation,
                                                CBM_STORE_INDEX_STATUS_COMPLETE),
              CBM_STORE_OK);

    ASSERT_EQ(cbm_store_reserve_index_generation(s, "test", NULL, NULL, &generation),
              CBM_STORE_OK);
    ASSERT_EQ(generation, FINAL_GENERATION);
    ASSERT_EQ(store_publish_new_helper_file_delta(s, generation), CBM_STORE_OK);
    ASSERT_EQ(store_publish_new_main_to_new_helper_delta(s, generation), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_finish_index_generation(s, "test", generation,
                                                CBM_STORE_INDEX_STATUS_COMPLETE),
              CBM_STORE_OK);

    ASSERT_EQ(cbm_store_count_nodes(s, "test"), EXPECTED_FINAL_NODES);
    ASSERT_EQ(cbm_store_count_edges(s, "test"), EXPECTED_FINAL_EDGES);
    ASSERT_EQ(store_node_qn_exists(s, "test", "test.helper.Helper"), 0);
    ASSERT_EQ(store_node_qn_exists(s, "test", "test.helper.NewHelper"), 1);
    ASSERT_EQ(store_node_qn_exists(s, "test", "test.main.Old"), 0);
    ASSERT_EQ(store_node_qn_exists(s, "test", "test.main.New"), 1);

    cbm_file_state_t got = {0};
    ASSERT_EQ(cbm_store_get_file_state(s, "test", "helper.go", &got), CBM_STORE_OK);
    ASSERT_STR_EQ(got.content_hash, "new-helper-content");
    ASSERT_EQ(got.generation, FINAL_GENERATION);
    cbm_store_file_state_free_fields(&got);
    ASSERT_EQ(cbm_store_get_file_state(s, "test", "main.go", &got), CBM_STORE_OK);
    ASSERT_STR_EQ(got.content_hash, "new-main-content");
    ASSERT_EQ(got.generation, FINAL_GENERATION);
    cbm_store_file_state_free_fields(&got);

    char **items = NULL;
    int count = 0;
    ASSERT_EQ(cbm_store_list_import_ref_paths_by_target(s, "test", "test.helper.NewHelper",
                                                        &items, &count),
              CBM_STORE_OK);
    ASSERT_EQ(count, 1);
    ASSERT_STR_EQ(items[0], "main.go");
    store_free_string_array(items, count);
    ASSERT_EQ(store_count_index_generation(s, "test", FINAL_GENERATION,
                                           CBM_STORE_INDEX_STATUS_COMPLETE, "", "",
                                           STORE_TEST_COMPLETED_SET),
              1);
    ASSERT_EQ(store_count_stale_graph_derived_views(s, "test", FINAL_GENERATION),
              store_graph_derived_view_count());

    cbm_store_close(s);
    PASS();
}

TEST(store_file_delta_batch_publish_rolls_back_all_files) {
    enum { BASE_GENERATION = 1, FAILED_GENERATION = 2, BATCH_DELTA_COUNT = 2 };
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    int64_t generation = 0;
    ASSERT_EQ(cbm_store_reserve_index_generation(s, "test", NULL, NULL, &generation),
              CBM_STORE_OK);
    ASSERT_EQ(generation, BASE_GENERATION);
    ASSERT_EQ(store_publish_helper_file_delta(s, generation), CBM_STORE_OK);
    ASSERT_EQ(store_publish_old_main_delta(s, generation), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_finish_index_generation(s, "test", generation,
                                                CBM_STORE_INDEX_STATUS_COMPLETE),
              CBM_STORE_OK);

    ASSERT_EQ(cbm_store_reserve_index_generation(s, "test", NULL, NULL, &generation),
              CBM_STORE_OK);
    ASSERT_EQ(generation, FAILED_GENERATION);

    cbm_node_t helper_nodes[1] = {{.project = "test",
                                   .label = "Function",
                                   .name = "NewHelper",
                                   .qualified_name = "test.helper.NewHelper",
                                   .file_path = "helper.go",
                                   .properties_json = "{}"}};
    cbm_file_hash_t helper_hash = {.project = "test",
                                   .rel_path = "helper.go",
                                   .sha256 = "new-helper-hash",
                                   .mtime_ns = 2,
                                   .size = 20};
    cbm_file_state_t helper_state = {.project = "test",
                                     .rel_path = "helper.go",
                                     .content_hash = "new-helper-content",
                                     .git_oid = "",
                                     .mtime_ns = 2,
                                     .size = 20,
                                     .language = "c",
                                     .pass_fingerprint = "pass-b",
                                     .generation = generation,
                                     .indexed_at = "2026-06-30T00:01:00Z"};
    cbm_store_symbol_export_t helper_exports[1] = {
        {.qualified_name = "test.helper.NewHelper", .node_id = CBM_STORE_NO_NODE_ID}};
    cbm_store_file_delta_t helper_delta = {.project = "test",
                                           .rel_path = "helper.go",
                                           .generation = generation,
                                           .file_hash = &helper_hash,
                                           .file_state = &helper_state,
                                           .nodes = helper_nodes,
                                           .node_count = 1,
                                           .exports = helper_exports,
                                           .export_count = 1};

    cbm_node_t main_nodes[1] = {{.project = "test",
                                 .label = "Function",
                                 .name = "New",
                                 .qualified_name = "test.main.New",
                                 .file_path = "main.go",
                                 .properties_json = "{}"}};
    cbm_store_delta_edge_t bad_edges[1] = {{.source_qn = "test.main.New",
                                            .target_qn = "test.main.Missing",
                                            .type = "CALLS",
                                            .properties_json = "{}"}};
    cbm_file_hash_t main_hash = {.project = "test",
                                 .rel_path = "main.go",
                                 .sha256 = "bad-main-hash",
                                 .mtime_ns = 2,
                                 .size = 20};
    cbm_file_state_t main_state = {.project = "test",
                                   .rel_path = "main.go",
                                   .content_hash = "bad-main-content",
                                   .git_oid = "",
                                   .mtime_ns = 2,
                                   .size = 20,
                                   .language = "c",
                                   .pass_fingerprint = "pass-b",
                                   .generation = generation,
                                   .indexed_at = "2026-06-30T00:01:00Z"};
    cbm_store_file_delta_t bad_main_delta = {.project = "test",
                                             .rel_path = "main.go",
                                             .generation = generation,
                                             .file_hash = &main_hash,
                                             .file_state = &main_state,
                                             .nodes = main_nodes,
                                             .node_count = 1,
                                             .edges = bad_edges,
                                             .edge_count = 1};
    const cbm_store_file_delta_t *deltas[BATCH_DELTA_COUNT] = {&helper_delta, &bad_main_delta};
    ASSERT_EQ(cbm_store_publish_file_delta_batch(s, deltas, BATCH_DELTA_COUNT),
              CBM_STORE_NOT_FOUND);
    ASSERT_EQ(cbm_store_finish_index_generation(s, "test", generation,
                                                CBM_STORE_INDEX_STATUS_FAILED),
              CBM_STORE_OK);

    ASSERT_EQ(store_node_qn_exists(s, "test", "test.helper.Helper"), 1);
    ASSERT_EQ(store_node_qn_exists(s, "test", "test.helper.NewHelper"), 0);
    ASSERT_EQ(store_node_qn_exists(s, "test", "test.main.Old"), 1);
    ASSERT_EQ(store_node_qn_exists(s, "test", "test.main.New"), 0);
    ASSERT_EQ(cbm_store_count_edges(s, "test"), 0);

    cbm_file_state_t got = {0};
    ASSERT_EQ(cbm_store_get_file_state(s, "test", "helper.go", &got), CBM_STORE_OK);
    ASSERT_STR_EQ(got.content_hash, "helper-content");
    ASSERT_EQ(got.generation, BASE_GENERATION);
    cbm_store_file_state_free_fields(&got);
    ASSERT_EQ(store_count_index_generation(s, "test", FAILED_GENERATION,
                                           CBM_STORE_INDEX_STATUS_FAILED, "", "",
                                           STORE_TEST_COMPLETED_SET),
              1);

    cbm_store_close(s);
    PASS();
}

TEST(store_file_delta_batch_complete_marks_graph_views_stale) {
    enum { BATCH_GENERATION = 1, BATCH_DELTA_COUNT = 1 };
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    int64_t generation = 0;
    ASSERT_EQ(cbm_store_reserve_index_generation(s, "test", NULL, NULL, &generation),
              CBM_STORE_OK);
    ASSERT_EQ(generation, BATCH_GENERATION);

    cbm_node_t helper_nodes[1] = {{.project = "test",
                                   .label = "Function",
                                   .name = "Helper",
                                   .qualified_name = "test.helper.Helper",
                                   .file_path = "helper.go",
                                   .properties_json = "{}"}};
    cbm_file_hash_t helper_hash = {.project = "test",
                                   .rel_path = "helper.go",
                                   .sha256 = "helper-hash",
                                   .mtime_ns = 1,
                                   .size = 10};
    cbm_file_state_t helper_state = {.project = "test",
                                     .rel_path = "helper.go",
                                     .content_hash = "helper-content",
                                     .git_oid = "",
                                     .mtime_ns = 1,
                                     .size = 10,
                                     .language = "c",
                                     .pass_fingerprint = "pass-a",
                                     .generation = BATCH_GENERATION,
                                     .indexed_at = "2026-06-30T00:00:00Z"};
    cbm_store_symbol_export_t helper_exports[1] = {
        {.qualified_name = "test.helper.Helper", .node_id = CBM_STORE_NO_NODE_ID}};
    cbm_store_file_delta_t helper_delta = {.project = "test",
                                           .rel_path = "helper.go",
                                           .generation = BATCH_GENERATION,
                                           .file_hash = &helper_hash,
                                           .file_state = &helper_state,
                                           .nodes = helper_nodes,
                                           .node_count = 1,
                                           .exports = helper_exports,
                                           .export_count = 1,
                                           .derived_view_name = CBM_STORE_DERIVED_VIEW_NODES_FTS,
                                           .derived_status = CBM_STORE_DERIVED_STATUS_COMPLETE};
    const cbm_store_file_delta_t *deltas[BATCH_DELTA_COUNT] = {&helper_delta};
    ASSERT_EQ(cbm_store_publish_file_delta_batch_complete(s, deltas, BATCH_DELTA_COUNT),
              CBM_STORE_OK);

    ASSERT_EQ(store_node_qn_exists(s, "test", "test.helper.Helper"), 1);
    ASSERT_EQ(store_count_index_generation(s, "test", BATCH_GENERATION,
                                           CBM_STORE_INDEX_STATUS_COMPLETE, "", "",
                                           STORE_TEST_COMPLETED_SET),
              1);
    ASSERT_EQ(store_count_derived_view_state(s, "test", CBM_STORE_DERIVED_VIEW_NODES_FTS,
                                             BATCH_GENERATION,
                                             CBM_STORE_DERIVED_STATUS_COMPLETE),
              1);
    ASSERT_EQ(store_count_stale_graph_derived_views(s, "test", BATCH_GENERATION),
              store_graph_derived_view_count());

    cbm_store_close(s);
    PASS();
}

TEST(store_file_delta_batch_complete_rolls_back_when_generation_missing) {
    enum { BASE_GENERATION = 1, MISSING_GENERATION = 2, BATCH_DELTA_COUNT = 1 };
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    int64_t generation = 0;
    ASSERT_EQ(cbm_store_reserve_index_generation(s, "test", NULL, NULL, &generation),
              CBM_STORE_OK);
    ASSERT_EQ(generation, BASE_GENERATION);
    ASSERT_EQ(store_publish_helper_file_delta(s, generation), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_finish_index_generation(s, "test", generation,
                                                CBM_STORE_INDEX_STATUS_COMPLETE),
              CBM_STORE_OK);

    cbm_node_t helper_nodes[1] = {{.project = "test",
                                   .label = "Function",
                                   .name = "NewHelper",
                                   .qualified_name = "test.helper.NewHelper",
                                   .file_path = "helper.go",
                                   .properties_json = "{}"}};
    cbm_file_hash_t helper_hash = {.project = "test",
                                   .rel_path = "helper.go",
                                   .sha256 = "new-helper-hash",
                                   .mtime_ns = 2,
                                   .size = 20};
    cbm_file_state_t helper_state = {.project = "test",
                                     .rel_path = "helper.go",
                                     .content_hash = "new-helper-content",
                                     .git_oid = "",
                                     .mtime_ns = 2,
                                     .size = 20,
                                     .language = "c",
                                     .pass_fingerprint = "pass-b",
                                     .generation = MISSING_GENERATION,
                                     .indexed_at = "2026-06-30T00:01:00Z"};
    cbm_store_symbol_export_t helper_exports[1] = {
        {.qualified_name = "test.helper.NewHelper", .node_id = CBM_STORE_NO_NODE_ID}};
    cbm_store_file_delta_t helper_delta = {.project = "test",
                                           .rel_path = "helper.go",
                                           .generation = MISSING_GENERATION,
                                           .file_hash = &helper_hash,
                                           .file_state = &helper_state,
                                           .nodes = helper_nodes,
                                           .node_count = 1,
                                           .exports = helper_exports,
                                           .export_count = 1};
    const cbm_store_file_delta_t *deltas[BATCH_DELTA_COUNT] = {&helper_delta};
    ASSERT_EQ(cbm_store_publish_file_delta_batch_complete(s, deltas, BATCH_DELTA_COUNT),
              CBM_STORE_NOT_FOUND);

    ASSERT_EQ(store_node_qn_exists(s, "test", "test.helper.Helper"), 1);
    ASSERT_EQ(store_node_qn_exists(s, "test", "test.helper.NewHelper"), 0);
    cbm_file_state_t got = {0};
    ASSERT_EQ(cbm_store_get_file_state(s, "test", "helper.go", &got), CBM_STORE_OK);
    ASSERT_STR_EQ(got.content_hash, "helper-content");
    ASSERT_EQ(got.generation, BASE_GENERATION);
    cbm_store_file_state_free_fields(&got);
    ASSERT_EQ(store_count_index_generation(s, "test", MISSING_GENERATION,
                                           CBM_STORE_INDEX_STATUS_COMPLETE, "", "",
                                           STORE_TEST_COMPLETED_SET),
              0);

    cbm_store_close(s);
    PASS();
}

TEST(store_file_delta_publish_commits_graph_and_metadata) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    cbm_node_t old_nodes[1] = {{.project = "test",
                                .label = "Function",
                                .name = "Old",
                                .qualified_name = "test.main.Old",
                                .file_path = "main.go",
                                .properties_json = "{}"}};
    cbm_file_hash_t old_hash = {
        .project = "test", .rel_path = "main.go", .sha256 = "old-hash", .mtime_ns = 1, .size = 10};
    cbm_store_file_delta_t old_delta = {.project = "test",
                                        .rel_path = "main.go",
                                        .generation = 1,
                                        .file_hash = &old_hash,
                                        .nodes = old_nodes,
                                        .node_count = 1};
    ASSERT_EQ(cbm_store_publish_file_delta(s, &old_delta), CBM_STORE_OK);

    cbm_node_t nodes[2] = {
        {.project = "test",
         .label = "Function",
         .name = "New",
         .qualified_name = "test.main.New",
         .file_path = "main.go",
         .properties_json = "{}"},
        {.project = "test",
         .label = "Function",
         .name = "Helper",
         .qualified_name = "test.main.Helper",
         .file_path = "main.go",
         .properties_json = "{}"},
    };
    cbm_store_delta_edge_t edges[1] = {{.source_qn = "test.main.New",
                                        .target_qn = "test.main.Helper",
                                        .type = "CALLS",
                                        .properties_json = "{}"}};
    cbm_file_hash_t hash = {
        .project = "test", .rel_path = "main.go", .sha256 = "new-hash", .mtime_ns = 2, .size = 20};
    cbm_file_state_t state = {.project = "test",
                              .rel_path = "main.go",
                              .content_hash = "new-content",
                              .git_oid = "new-oid",
                              .mtime_ns = 2,
                              .size = 20,
                              .language = "c",
                              .pass_fingerprint = "pass-b",
                              .generation = 2,
                              .indexed_at = "2026-06-30T00:01:00Z"};
    cbm_store_symbol_export_t exports[1] = {
        {.qualified_name = "test.main.New", .node_id = CBM_STORE_NO_NODE_ID}};
    cbm_store_import_ref_t imports[1] = {
        {.import_text = "test.main", .local_name = "New", .target_qn = "test.main.New"}};
    cbm_store_file_delta_t delta = {.project = "test",
                                    .rel_path = "main.go",
                                    .generation = 2,
                                    .file_hash = &hash,
                                    .file_state = &state,
                                    .nodes = nodes,
                                    .node_count = 2,
                                    .edges = edges,
                                    .edge_count = 1,
                                    .exports = exports,
                                    .export_count = 1,
                                    .imports = imports,
                                    .import_count = 1,
                                    .derived_view_name = CBM_STORE_DERIVED_VIEW_NODES_FTS,
                                    .derived_status = CBM_STORE_DERIVED_STATUS_COMPLETE};
    ASSERT_EQ(cbm_store_publish_file_delta(s, &delta), CBM_STORE_OK);

    ASSERT_EQ(store_node_qn_exists(s, "test", "test.main.Old"), 0);
    ASSERT_EQ(store_node_qn_exists(s, "test", "test.main.New"), 1);
    ASSERT_EQ(store_node_qn_exists(s, "test", "test.main.Helper"), 1);
    ASSERT_EQ(cbm_store_count_edges(s, "test"), 1);
    ASSERT_EQ(store_count_metadata_owners(s, 0, "test", "main.go"), 2);
    ASSERT_EQ(store_count_metadata_owners(s, 1, "test", "main.go"), 1);

    cbm_file_hash_t *hashes = NULL;
    int count = 0;
    ASSERT_EQ(cbm_store_get_file_hashes(s, "test", &hashes, &count), CBM_STORE_OK);
    ASSERT_EQ(count, 1);
    ASSERT_STR_EQ(hashes[0].sha256, "new-hash");
    cbm_store_free_file_hashes(hashes, count);

    cbm_file_state_t got = {0};
    ASSERT_EQ(cbm_store_get_file_state(s, "test", "main.go", &got), CBM_STORE_OK);
    ASSERT_STR_EQ(got.content_hash, "new-content");
    ASSERT_EQ(got.generation, 2);
    cbm_store_file_state_free_fields(&got);

    char **items = NULL;
    count = 0;
    ASSERT_EQ(cbm_store_list_symbol_exports_by_file(s, "test", "main.go", &items, &count),
              CBM_STORE_OK);
    ASSERT_EQ(count, 1);
    ASSERT_STR_EQ(items[0], "test.main.New");
    store_free_string_array(items, count);

    items = NULL;
    count = 0;
    ASSERT_EQ(cbm_store_list_import_ref_paths_by_target(s, "test", "test.main.New", &items,
                                                       &count),
              CBM_STORE_OK);
    ASSERT_EQ(count, 1);
    ASSERT_STR_EQ(items[0], "main.go");
    store_free_string_array(items, count);
    ASSERT_EQ(store_count_derived_view_state(s, "test", CBM_STORE_DERIVED_VIEW_NODES_FTS, 2,
                                             CBM_STORE_DERIVED_STATUS_COMPLETE),
              1);
    ASSERT_EQ(store_count_stale_graph_derived_views(s, "test", 2),
              store_graph_derived_view_count());

    cbm_store_close(s);
    PASS();
}

TEST(store_file_delta_delete_cleans_graph_and_metadata) {
    enum {
        BASE_GENERATION = 1,
        DELETE_GENERATION = 2,
    };
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);
    ASSERT_EQ(store_publish_helper_file_delta(s, BASE_GENERATION), CBM_STORE_OK);
    ASSERT_EQ(store_publish_new_main_delta(s, BASE_GENERATION), CBM_STORE_OK);
    ASSERT_EQ(store_node_qn_exists(s, "test", "test.helper.Helper"), 1);
    ASSERT_EQ(store_node_qn_exists(s, "test", "test.main.New"), 1);
    ASSERT_EQ(cbm_store_count_edges(s, "test"), 1);

    ASSERT_EQ(cbm_store_delete_file_delta(s, "test", "helper.go", DELETE_GENERATION,
                                          CBM_STORE_DERIVED_VIEW_NODES_FTS),
              CBM_STORE_OK);

    ASSERT_EQ(store_node_qn_exists(s, "test", "test.helper.Helper"), 0);
    ASSERT_EQ(store_node_qn_exists(s, "test", "test.main.New"), 1);
    ASSERT_EQ(cbm_store_count_edges(s, "test"), 0);
    ASSERT_EQ(store_count_metadata_owners(s, 0, "test", "helper.go"), 0);
    ASSERT_EQ(store_count_metadata_owners(s, 1, "test", "helper.go"), 0);
    ASSERT_EQ(store_count_metadata_owners(s, 0, "test", "main.go"), 1);
    ASSERT_EQ(store_count_metadata_owners(s, 1, "test", "main.go"), 0);

    cbm_file_state_t deleted_state = {0};
    ASSERT_EQ(cbm_store_get_file_state(s, "test", "helper.go", &deleted_state),
              CBM_STORE_NOT_FOUND);
    ASSERT_EQ(store_count_derived_view_state(s, "test", CBM_STORE_DERIVED_VIEW_NODES_FTS,
                                             DELETE_GENERATION, CBM_STORE_DERIVED_STATUS_STALE),
              1);
    ASSERT_EQ(store_count_stale_graph_derived_views(s, "test", DELETE_GENERATION),
              store_graph_derived_view_count());

    cbm_file_hash_t *hashes = NULL;
    int hash_count = 0;
    ASSERT_EQ(cbm_store_get_file_hashes(s, "test", &hashes, &hash_count), CBM_STORE_OK);
    ASSERT_EQ(hash_count, 1);
    ASSERT_STR_EQ(hashes[0].rel_path, "main.go");
    cbm_store_free_file_hashes(hashes, hash_count);

    cbm_store_close(s);
    PASS();
}

TEST(store_file_delta_delete_complete_finishes_generation) {
    enum {
        BASE_GENERATION = 1,
        DELETE_GENERATION = 2,
    };
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    int64_t generation = 0;
    ASSERT_EQ(cbm_store_reserve_index_generation(s, "test", NULL, NULL, &generation),
              CBM_STORE_OK);
    ASSERT_EQ(generation, BASE_GENERATION);
    ASSERT_EQ(store_publish_helper_file_delta(s, BASE_GENERATION), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_finish_index_generation(s, "test", BASE_GENERATION,
                                                CBM_STORE_INDEX_STATUS_COMPLETE),
              CBM_STORE_OK);

    generation = 0;
    ASSERT_EQ(cbm_store_reserve_index_generation(s, "test", NULL, NULL, &generation),
              CBM_STORE_OK);
    ASSERT_EQ(generation, DELETE_GENERATION);
    ASSERT_EQ(cbm_store_delete_file_delta_complete(s, "test", "helper.go", generation,
                                                   CBM_STORE_DERIVED_VIEW_NODES_FTS),
              CBM_STORE_OK);

    ASSERT_EQ(store_node_qn_exists(s, "test", "test.helper.Helper"), 0);
    ASSERT_EQ(store_count_index_generation(s, "test", DELETE_GENERATION,
                                           CBM_STORE_INDEX_STATUS_COMPLETE, "", "",
                                           STORE_TEST_COMPLETED_SET),
              1);

    cbm_store_close(s);
    PASS();
}

TEST(store_derived_view_state_public_api) {
    enum {
        STALE_GENERATION = 5,
        COMPLETE_GENERATION = 6,
    };
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    const char *views[] = {CBM_STORE_DERIVED_VIEW_PAGERANK,
                           CBM_STORE_DERIVED_VIEW_NODE_DEGREE,
                           CBM_STORE_DERIVED_VIEW_SEMANTIC_EDGES};
    ASSERT_EQ(cbm_store_mark_derived_views_stale(s, "test", STALE_GENERATION, views,
                                                 (int)(sizeof(views) / sizeof(views[0]))),
              CBM_STORE_OK);
    ASSERT_EQ(store_count_derived_view_state(s, "test", CBM_STORE_DERIVED_VIEW_PAGERANK,
                                             STALE_GENERATION, CBM_STORE_DERIVED_STATUS_STALE),
              1);
    ASSERT_EQ(store_count_derived_view_state(s, "test", CBM_STORE_DERIVED_VIEW_NODE_DEGREE,
                                             STALE_GENERATION, CBM_STORE_DERIVED_STATUS_STALE),
              1);
    ASSERT_EQ(store_count_derived_view_state(s, "test", CBM_STORE_DERIVED_VIEW_SEMANTIC_EDGES,
                                             STALE_GENERATION, CBM_STORE_DERIVED_STATUS_STALE),
              1);
    cbm_derived_view_state_t got = {0};
    ASSERT_EQ(cbm_store_get_derived_view_state(s, "test", CBM_STORE_DERIVED_VIEW_NODE_DEGREE,
                                               &got),
              CBM_STORE_OK);
    ASSERT_TRUE(cbm_store_derived_view_is_stale(s, "test",
                                                CBM_STORE_DERIVED_VIEW_NODE_DEGREE));
    ASSERT_STR_EQ(got.project, "test");
    ASSERT_STR_EQ(got.view_name, CBM_STORE_DERIVED_VIEW_NODE_DEGREE);
    ASSERT_EQ(got.source_generation, STALE_GENERATION);
    ASSERT_STR_EQ(got.status, CBM_STORE_DERIVED_STATUS_STALE);
    ASSERT_NOT_NULL(got.computed_at);
    cbm_store_derived_view_state_free_fields(&got);

    ASSERT_EQ(cbm_store_set_derived_view_state(s, "test", CBM_STORE_DERIVED_VIEW_PAGERANK,
                                               COMPLETE_GENERATION,
                                               CBM_STORE_DERIVED_STATUS_COMPLETE),
              CBM_STORE_OK);
    ASSERT_EQ(store_count_derived_view_state(s, "test", CBM_STORE_DERIVED_VIEW_PAGERANK,
                                             STALE_GENERATION, CBM_STORE_DERIVED_STATUS_STALE),
              0);
    ASSERT_EQ(store_count_derived_view_state(s, "test", CBM_STORE_DERIVED_VIEW_PAGERANK,
                                             COMPLETE_GENERATION,
                                             CBM_STORE_DERIVED_STATUS_COMPLETE),
              1);
    ASSERT_EQ(cbm_store_get_derived_view_state(s, "test", CBM_STORE_DERIVED_VIEW_PAGERANK,
                                               &got),
              CBM_STORE_OK);
    ASSERT_EQ(got.source_generation, COMPLETE_GENERATION);
    ASSERT_STR_EQ(got.status, CBM_STORE_DERIVED_STATUS_COMPLETE);
    cbm_store_derived_view_state_free_fields(&got);
    ASSERT_FALSE(cbm_store_derived_view_is_stale(s, "test",
                                                 CBM_STORE_DERIVED_VIEW_PAGERANK));

    ASSERT_EQ(cbm_store_set_derived_view_state(s, "test", CBM_STORE_DERIVED_VIEW_ARCHITECTURE,
                                               CBM_STORE_DERIVED_GENERATION_UNKNOWN,
                                               CBM_STORE_DERIVED_STATUS_COMPLETE),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_store_get_derived_view_state(s, "test", CBM_STORE_DERIVED_VIEW_ARCHITECTURE,
                                               &got),
              CBM_STORE_OK);
    ASSERT_EQ(got.source_generation, CBM_STORE_DERIVED_GENERATION_UNKNOWN);
    ASSERT_STR_EQ(got.status, CBM_STORE_DERIVED_STATUS_COMPLETE);
    cbm_store_derived_view_state_free_fields(&got);

    const char *complete_views[] = {CBM_STORE_DERIVED_VIEW_ROUTES,
                                    CBM_STORE_DERIVED_VIEW_ARCHITECTURE};
    ASSERT_EQ(cbm_store_mark_derived_views_complete(
                  s, "test", COMPLETE_GENERATION, complete_views,
                  (int)(sizeof(complete_views) / sizeof(complete_views[0]))),
              CBM_STORE_OK);
    ASSERT_EQ(store_count_derived_view_state(s, "test", CBM_STORE_DERIVED_VIEW_ROUTES,
                                             COMPLETE_GENERATION,
                                             CBM_STORE_DERIVED_STATUS_COMPLETE),
              1);
    ASSERT_EQ(store_count_derived_view_state(s, "test", CBM_STORE_DERIVED_VIEW_ARCHITECTURE,
                                             COMPLETE_GENERATION,
                                             CBM_STORE_DERIVED_STATUS_COMPLETE),
              1);

    ASSERT_EQ(cbm_store_set_derived_view_state(s, "test", CBM_STORE_DERIVED_VIEW_LINKRANK,
                                               COMPLETE_GENERATION,
                                               STORE_TEST_INVALID_DERIVED_STATUS),
              CBM_STORE_ERR);
    ASSERT_EQ(store_count_derived_view_state(s, "test", CBM_STORE_DERIVED_VIEW_LINKRANK,
                                             COMPLETE_GENERATION,
                                             CBM_STORE_DERIVED_STATUS_COMPLETE),
              0);
    ASSERT_EQ(cbm_store_get_derived_view_state(s, "test", CBM_STORE_DERIVED_VIEW_LINKRANK,
                                               &got),
              CBM_STORE_NOT_FOUND);
    ASSERT_EQ(cbm_store_mark_derived_views_stale(s, "test", COMPLETE_GENERATION, NULL, 0),
              CBM_STORE_OK);

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

    const char *mixed_qns[] = {"test.A", NULL, "test.B", "test.A", "other.C"};
    int64_t mixed_ids[5];
    int mixed_found = cbm_store_find_node_ids_by_qns(s, "test", mixed_qns, 5, mixed_ids);
    ASSERT_EQ(mixed_found, 3);
    ASSERT_EQ(mixed_ids[0], id1);
    ASSERT_EQ(mixed_ids[1], 0);
    ASSERT_EQ(mixed_ids[2], id2);
    ASSERT_EQ(mixed_ids[3], id1);
    ASSERT_EQ(mixed_ids[4], 0);

    /* Empty batch */
    int found2 = cbm_store_find_node_ids_by_qns(s, "test", NULL, 0, ids);
    ASSERT_EQ(found2, 0);

    cbm_store_close(s);
    PASS();
}

/* ── Integrity check tests ──────────────────────────────────────── */

TEST(store_integrity_clean) {
    /* A fresh store with correct data should pass integrity check */
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "test-proj", "/tmp/test");
    ASSERT_TRUE(cbm_store_check_integrity(s));
    cbm_store_close(s);
    PASS();
}

TEST(store_integrity_empty) {
    /* An empty store (no project rows) should pass — 0 rows is fine */
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_TRUE(cbm_store_check_integrity(s));
    cbm_store_close(s);
    PASS();
}

TEST(store_integrity_corrupt_bad_path) {
    /* Simulate corruption: root_path is a numeric string (not a real path).
     * This matches the real corruption where node IDs ended up in root_path. */
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    sqlite3 *db = cbm_store_get_db(s);
    sqlite3_exec(db,
                 "INSERT INTO projects (name, indexed_at, root_path) "
                 "VALUES ('some-project', '2024-01-01', '826');",
                 NULL, NULL, NULL);
    ASSERT_FALSE(cbm_store_check_integrity(s));
    cbm_store_close(s);
    PASS();
}

TEST(store_integrity_windows_lowercase_drive_issue367) {
    /* Windows drive letters may be lower- or upper-case; a lowercase drive
     * path must NOT be treated as corrupt. Previously the check only accepted
     * 'A'..'Z', so "c:/repo" was flagged and the DB auto-deleted (#227/#367). */
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "lc-drive", "c:/Users/dev/repo");
    ASSERT_TRUE(cbm_store_check_integrity(s));
    cbm_store_close(s);
    PASS();
}

TEST(store_integrity_multiple_project_rows_allowed) {
    /* Dependency projects are stored in the parent DB, so a valid store may
     * contain more than one projects row. Row count alone is not corruption. */
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    sqlite3 *db = cbm_store_get_db(s);
    for (int i = 0; i < 10; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "INSERT INTO projects (name, indexed_at, root_path) "
                 "VALUES ('proj-%d', '2024-01-01', '/tmp/%d');",
                 i, i);
        sqlite3_exec(db, sql, NULL, NULL, NULL);
    }
    ASSERT_TRUE(cbm_store_check_integrity(s));
    cbm_store_close(s);
    PASS();
}

TEST(store_integrity_null_check) {
    /* NULL store should return false (not crash) */
    ASSERT_FALSE(cbm_store_check_integrity(NULL));
    PASS();
}

TEST(store_integrity_full_path_only_classification) {
    /* The _full variant must classify a bad root_path (with an otherwise-fine
     * projects table) as a path-only defect so callers can retain the DB
     * (#557), while genuine corruption (too many rows) is NOT path-only. */
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    bool path_only = true;

    /* Clean DB: passes, path_only stays false. */
    cbm_store_upsert_project(s, "clean-proj", "/tmp/clean");
    path_only = true;
    ASSERT_TRUE(cbm_store_check_integrity_full(s, &path_only));
    ASSERT_FALSE(path_only);

    /* Bad root_path, single row: fails, path_only == true (retain-eligible). */
    sqlite3 *db = cbm_store_get_db(s);
    sqlite3_exec(db, "DELETE FROM projects;", NULL, NULL, NULL);
    sqlite3_exec(db,
                 "INSERT INTO projects (name, indexed_at, root_path) "
                 "VALUES ('bad-path-proj', '2024-01-01', '6860');",
                 NULL, NULL, NULL);
    path_only = false;
    ASSERT_FALSE(cbm_store_check_integrity_full(s, &path_only));
    ASSERT_TRUE(path_only);

    /* Many valid project rows are allowed and are not a path-only failure. */
    sqlite3_exec(db, "DELETE FROM projects;", NULL, NULL, NULL);
    for (int i = 0; i < 10; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "INSERT INTO projects (name, indexed_at, root_path) "
                 "VALUES ('proj-%d', '2024-01-01', '/tmp/%d');",
                 i, i);
        sqlite3_exec(db, sql, NULL, NULL, NULL);
    }
    path_only = true;
    ASSERT_TRUE(cbm_store_check_integrity_full(s, &path_only));
    ASSERT_FALSE(path_only);

    cbm_store_close(s);
    PASS();
}

/* ── Edge case: NULL / empty field handling ────────────────────── */

TEST(store_node_null_project) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);

    /* Upsert with NULL project — should fail gracefully */
    cbm_node_t n = {
        .project = NULL, .label = "Function", .name = "Foo", .qualified_name = "null.Foo"};
    int64_t id = cbm_store_upsert_node(s, &n);
    /* Either returns error or silently succeeds; must not crash */
    (void)id;

    cbm_store_close(s);
    PASS();
}

TEST(store_node_null_qn) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    /* Upsert with NULL qualified_name */
    cbm_node_t n = {.project = "test", .label = "Function", .name = "Bar", .qualified_name = NULL};
    int64_t id = cbm_store_upsert_node(s, &n);
    /* Must not crash regardless of return value */
    (void)id;

    cbm_store_close(s);
    PASS();
}

TEST(store_node_empty_strings) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    /* Upsert with all fields as empty strings */
    cbm_node_t n = {.project = "test",
                    .label = "",
                    .name = "",
                    .qualified_name = "",
                    .file_path = "",
                    .start_line = 0,
                    .end_line = 0,
                    .properties_json = ""};
    int64_t id = cbm_store_upsert_node(s, &n);
    /* Should succeed — empty strings are valid */
    ASSERT_GT(id, 0);

    cbm_node_t found = {0};
    int rc = cbm_store_find_node_by_qn(s, "test", "", &found);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_STR_EQ(found.name, "");
    ASSERT_STR_EQ(found.label, "");
    cbm_node_free_fields(&found);

    cbm_store_close(s);
    PASS();
}

/* ── Edge case: not-found lookups ──────────────────────────────── */

TEST(store_find_by_id_not_found) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    cbm_node_t found = {0};
    int rc = cbm_store_find_node_by_id(s, 999999, &found);
    ASSERT_EQ(rc, CBM_STORE_NOT_FOUND);

    /* Negative ID should also be not found */
    rc = cbm_store_find_node_by_id(s, -1, &found);
    ASSERT_EQ(rc, CBM_STORE_NOT_FOUND);

    cbm_store_close(s);
    PASS();
}

TEST(store_find_by_qn_not_found) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    /* Insert a node so the store is non-empty */
    cbm_node_t n = {
        .project = "test", .label = "Function", .name = "Exists", .qualified_name = "test.Exists"};
    cbm_store_upsert_node(s, &n);

    /* Search for a non-existent QN */
    cbm_node_t found = {0};
    int rc = cbm_store_find_node_by_qn(s, "test", "test.DoesNotExist", &found);
    ASSERT_EQ(rc, CBM_STORE_NOT_FOUND);

    /* Wrong project */
    rc = cbm_store_find_node_by_qn(s, "other-project", "test.Exists", &found);
    ASSERT_EQ(rc, CBM_STORE_NOT_FOUND);

    cbm_store_close(s);
    PASS();
}

/* ── Edge case: cross-project lookups ──────────────────────────── */

TEST(store_find_by_qn_any_cross_project) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "proj-a", "/tmp/a");
    cbm_store_upsert_project(s, "proj-b", "/tmp/b");

    cbm_node_t na = {.project = "proj-a",
                     .label = "Function",
                     .name = "SharedFunc",
                     .qualified_name = "proj-a.main.SharedFunc"};
    cbm_node_t nb = {.project = "proj-b",
                     .label = "Class",
                     .name = "Widget",
                     .qualified_name = "proj-b.pkg.Widget"};
    cbm_store_upsert_node(s, &na);
    cbm_store_upsert_node(s, &nb);

    /* find_node_by_qn_any finds without project filter */
    cbm_node_t found = {0};
    int rc = cbm_store_find_node_by_qn_any(s, "proj-a.main.SharedFunc", &found);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_STR_EQ(found.name, "SharedFunc");
    ASSERT_STR_EQ(found.project, "proj-a");
    cbm_node_free_fields(&found);

    rc = cbm_store_find_node_by_qn_any(s, "proj-b.pkg.Widget", &found);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_STR_EQ(found.name, "Widget");
    ASSERT_STR_EQ(found.project, "proj-b");
    cbm_node_free_fields(&found);

    /* Non-existent QN */
    rc = cbm_store_find_node_by_qn_any(s, "nonexistent.Nope", &found);
    ASSERT_EQ(rc, CBM_STORE_NOT_FOUND);

    cbm_store_close(s);
    PASS();
}

TEST(store_find_by_name_any_cross_project) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "proj-a", "/tmp/a");
    cbm_store_upsert_project(s, "proj-b", "/tmp/b");

    /* Same name in two projects */
    cbm_node_t na = {.project = "proj-a",
                     .label = "Function",
                     .name = "Init",
                     .qualified_name = "proj-a.main.Init"};
    cbm_node_t nb = {.project = "proj-b",
                     .label = "Function",
                     .name = "Init",
                     .qualified_name = "proj-b.main.Init"};
    cbm_node_t nc = {.project = "proj-b",
                     .label = "Function",
                     .name = "Other",
                     .qualified_name = "proj-b.main.Other"};
    cbm_store_upsert_node(s, &na);
    cbm_store_upsert_node(s, &nb);
    cbm_store_upsert_node(s, &nc);

    /* find_nodes_by_name_any should find both "Init" across projects */
    cbm_node_t *nodes = NULL;
    int count = 0;
    int rc = cbm_store_find_nodes_by_name_any(s, "Init", &nodes, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(count, 2);
    cbm_store_free_nodes(nodes, count);

    /* Name that doesn't exist */
    rc = cbm_store_find_nodes_by_name_any(s, "Nonexistent", &nodes, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(count, 0);
    cbm_store_free_nodes(nodes, count);

    cbm_store_close(s);
    PASS();
}

/* ── Edge case: empty result sets ──────────────────────────────── */

TEST(store_find_by_file_no_match) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    cbm_node_t n = {.project = "test",
                    .label = "Function",
                    .name = "Foo",
                    .qualified_name = "test.Foo",
                    .file_path = "main.go"};
    cbm_store_upsert_node(s, &n);

    /* Search for a file that has no nodes */
    cbm_node_t *nodes = NULL;
    int count = 0;
    int rc = cbm_store_find_nodes_by_file(s, "test", "nonexistent.go", &nodes, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(count, 0);
    cbm_store_free_nodes(nodes, count);

    cbm_store_close(s);
    PASS();
}

/* ── Edge case: batch upsert boundary ─────────────────────────── */

TEST(store_node_batch_upsert_zero) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    /* Batch upsert with count=0 should succeed, do nothing */
    int rc = cbm_store_upsert_node_batch(s, NULL, 0, NULL);
    ASSERT_EQ(rc, CBM_STORE_OK);

    int cnt = cbm_store_count_nodes(s, "test");
    ASSERT_EQ(cnt, 0);

    cbm_store_close(s);
    PASS();
}

TEST(store_node_batch_upsert_100) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    cbm_node_t nodes[100];
    int64_t ids[100];
    char names[100][32];
    char qns[100][64];

    for (int i = 0; i < 100; i++) {
        snprintf(names[i], sizeof(names[i]), "stress_%d", i);
        snprintf(qns[i], sizeof(qns[i]), "test.stress.stress_%d", i);
        nodes[i] = (cbm_node_t){.project = "test",
                                .label = "Function",
                                .name = names[i],
                                .qualified_name = qns[i],
                                .file_path = "stress.go",
                                .start_line = i,
                                .end_line = i + 1};
    }

    int rc = cbm_store_upsert_node_batch(s, nodes, 100, ids);
    ASSERT_EQ(rc, CBM_STORE_OK);

    /* All IDs should be positive */
    for (int i = 0; i < 100; i++)
        ASSERT_GT(ids[i], 0);

    /* IDs should all be unique */
    for (int i = 0; i < 100; i++)
        for (int j = i + 1; j < 100; j++)
            ASSERT_NEQ(ids[i], ids[j]);

    int cnt = cbm_store_count_nodes(s, "test");
    ASSERT_EQ(cnt, 100);

    /* Verify a few random lookups */
    cbm_node_t found = {0};
    rc = cbm_store_find_node_by_qn(s, "test", "test.stress.stress_0", &found);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_STR_EQ(found.name, "stress_0");
    cbm_node_free_fields(&found);

    rc = cbm_store_find_node_by_qn(s, "test", "test.stress.stress_99", &found);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_STR_EQ(found.name, "stress_99");
    cbm_node_free_fields(&found);

    cbm_store_close(s);
    PASS();
}

/* ── Edge case: delete then verify remaining ──────────────────── */

TEST(store_delete_by_label_verify_remaining) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    cbm_node_t n1 = {
        .project = "test", .label = "Function", .name = "FuncA", .qualified_name = "test.FuncA"};
    cbm_node_t n2 = {
        .project = "test", .label = "Class", .name = "ClassB", .qualified_name = "test.ClassB"};
    cbm_node_t n3 = {
        .project = "test", .label = "Function", .name = "FuncC", .qualified_name = "test.FuncC"};
    cbm_node_t n4 = {
        .project = "test", .label = "Method", .name = "MethodD", .qualified_name = "test.MethodD"};
    cbm_store_upsert_node(s, &n1);
    cbm_store_upsert_node(s, &n2);
    cbm_store_upsert_node(s, &n3);
    cbm_store_upsert_node(s, &n4);

    /* Delete all Functions */
    int rc = cbm_store_delete_nodes_by_label(s, "test", "Function");
    ASSERT_EQ(rc, CBM_STORE_OK);

    int cnt = cbm_store_count_nodes(s, "test");
    ASSERT_EQ(cnt, 2);

    /* Class and Method should remain */
    cbm_node_t found = {0};
    rc = cbm_store_find_node_by_qn(s, "test", "test.ClassB", &found);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_STR_EQ(found.label, "Class");
    cbm_node_free_fields(&found);

    rc = cbm_store_find_node_by_qn(s, "test", "test.MethodD", &found);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_STR_EQ(found.label, "Method");
    cbm_node_free_fields(&found);

    /* Deleted ones should be gone */
    rc = cbm_store_find_node_by_qn(s, "test", "test.FuncA", &found);
    ASSERT_EQ(rc, CBM_STORE_NOT_FOUND);
    rc = cbm_store_find_node_by_qn(s, "test", "test.FuncC", &found);
    ASSERT_EQ(rc, CBM_STORE_NOT_FOUND);

    cbm_store_close(s);
    PASS();
}

TEST(store_delete_by_file_verify_remaining) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    cbm_node_t n1 = {.project = "test",
                     .label = "Function",
                     .name = "A",
                     .qualified_name = "test.A",
                     .file_path = "delete_me.go"};
    cbm_node_t n2 = {.project = "test",
                     .label = "Function",
                     .name = "B",
                     .qualified_name = "test.B",
                     .file_path = "keep_me.go"};
    cbm_node_t n3 = {.project = "test",
                     .label = "Function",
                     .name = "C",
                     .qualified_name = "test.C",
                     .file_path = "delete_me.go"};
    cbm_store_upsert_node(s, &n1);
    cbm_store_upsert_node(s, &n2);
    cbm_store_upsert_node(s, &n3);

    int rc = cbm_store_delete_nodes_by_file(s, "test", "delete_me.go");
    ASSERT_EQ(rc, CBM_STORE_OK);

    int cnt = cbm_store_count_nodes(s, "test");
    ASSERT_EQ(cnt, 1);

    /* Only keep_me.go node should remain */
    cbm_node_t found = {0};
    rc = cbm_store_find_node_by_qn(s, "test", "test.B", &found);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_STR_EQ(found.file_path, "keep_me.go");
    cbm_node_free_fields(&found);

    /* Deleted nodes should be gone */
    rc = cbm_store_find_node_by_qn(s, "test", "test.A", &found);
    ASSERT_EQ(rc, CBM_STORE_NOT_FOUND);
    rc = cbm_store_find_node_by_qn(s, "test", "test.C", &found);
    ASSERT_EQ(rc, CBM_STORE_NOT_FOUND);

    cbm_store_close(s);
    PASS();
}

/* ── Edge case: upsert dedup with field changes ───────────────── */

TEST(store_node_upsert_updates_fields) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    /* Insert initial node */
    cbm_node_t n1 = {.project = "test",
                     .label = "Function",
                     .name = "MyFunc",
                     .qualified_name = "test.MyFunc",
                     .file_path = "old.go",
                     .start_line = 1,
                     .end_line = 10,
                     .properties_json = "{\"version\":1}"};
    int64_t id1 = cbm_store_upsert_node(s, &n1);
    ASSERT_GT(id1, 0);

    /* Upsert same QN with changed fields */
    cbm_node_t n2 = {.project = "test",
                     .label = "Method",
                     .name = "MyFunc",
                     .qualified_name = "test.MyFunc",
                     .file_path = "new.go",
                     .start_line = 50,
                     .end_line = 60,
                     .properties_json = "{\"version\":2}"};
    int64_t id2 = cbm_store_upsert_node(s, &n2);
    ASSERT_EQ(id1, id2); /* Same ID — updated, not duplicated */

    /* Count should still be 1 */
    int cnt = cbm_store_count_nodes(s, "test");
    ASSERT_EQ(cnt, 1);

    /* Verify fields were updated */
    cbm_node_t found = {0};
    int rc = cbm_store_find_node_by_id(s, id1, &found);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_STR_EQ(found.label, "Method");
    ASSERT_STR_EQ(found.file_path, "new.go");
    ASSERT_EQ(found.start_line, 50);
    ASSERT_EQ(found.end_line, 60);
    ASSERT(strstr(found.properties_json, "version") != NULL);
    ASSERT(strstr(found.properties_json, "2") != NULL);
    cbm_node_free_fields(&found);

    cbm_store_close(s);
    PASS();
}

/* ── Edge case: long qualified name ───────────────────────────── */

TEST(store_node_long_qn) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    /* Build a 1200-char qualified name */
    char long_qn[1201];
    memset(long_qn, 'a', 1200);
    long_qn[0] = 't'; /* make it look like a dotted path */
    for (int i = 50; i < 1200; i += 50)
        long_qn[i] = '.';
    long_qn[1200] = '\0';

    cbm_node_t n = {.project = "test",
                    .label = "Function",
                    .name = "LongName",
                    .qualified_name = long_qn,
                    .file_path = "big.go"};
    int64_t id = cbm_store_upsert_node(s, &n);
    ASSERT_GT(id, 0);

    /* Should be retrievable by QN */
    cbm_node_t found = {0};
    int rc = cbm_store_find_node_by_qn(s, "test", long_qn, &found);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_STR_EQ(found.qualified_name, long_qn);
    ASSERT_STR_EQ(found.name, "LongName");
    cbm_node_free_fields(&found);

    cbm_store_close(s);
    PASS();
}

/* ── Edge case: properties JSON with special characters ────────── */

TEST(store_node_properties_special_chars) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    /* JSON with quotes, backslashes, unicode, newlines */
    const char *props = "{\"desc\":\"line1\\nline2\","
                        "\"path\":\"C:\\\\Users\\\\test\","
                        "\"emoji\":\"\\u2603\","
                        "\"nested\":{\"key\":\"val with \\\"quotes\\\"\"}}";

    cbm_node_t n = {.project = "test",
                    .label = "Function",
                    .name = "SpecialFunc",
                    .qualified_name = "test.SpecialFunc",
                    .properties_json = props};
    int64_t id = cbm_store_upsert_node(s, &n);
    ASSERT_GT(id, 0);

    cbm_node_t found = {0};
    int rc = cbm_store_find_node_by_id(s, id, &found);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_NOT_NULL(found.properties_json);
    /* Round-trip should preserve the JSON exactly */
    ASSERT_STR_EQ(found.properties_json, props);
    cbm_node_free_fields(&found);

    cbm_store_close(s);
    PASS();
}

/* ── Edge case: delete from non-existent project/file ─────────── */

TEST(store_delete_nodes_nonexistent) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    /* Insert one node */
    cbm_node_t n = {.project = "test",
                    .label = "Function",
                    .name = "Survivor",
                    .qualified_name = "test.Survivor",
                    .file_path = "main.go"};
    cbm_store_upsert_node(s, &n);

    /* Delete by non-existent file — should succeed but delete nothing */
    int rc = cbm_store_delete_nodes_by_file(s, "test", "ghost.go");
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(cbm_store_count_nodes(s, "test"), 1);

    /* Delete by non-existent label */
    rc = cbm_store_delete_nodes_by_label(s, "test", "Interface");
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(cbm_store_count_nodes(s, "test"), 1);

    /* Delete by non-existent project */
    rc = cbm_store_delete_nodes_by_project(s, "no-such-project");
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(cbm_store_count_nodes(s, "test"), 1);

    cbm_store_close(s);
    PASS();
}

/* ── Edge case: count nodes for unknown project ───────────────── */

TEST(store_count_nodes_unknown_project) {
    cbm_store_t *s = cbm_store_open_memory();
    /* No project created — count should be 0 */
    int cnt = cbm_store_count_nodes(s, "ghost-project");
    ASSERT_EQ(cnt, 0);
    cbm_store_close(s);
    PASS();
}

SUITE(store_nodes) {
    RUN_TEST(store_open_memory);
    RUN_TEST(store_close_null);
    RUN_TEST(store_open_memory_twice);
    RUN_TEST(store_exact_delta_metadata_schema);
    RUN_TEST(store_open_path_query_does_not_create_missing_db);
    RUN_TEST(store_integrity_clean);
    RUN_TEST(store_integrity_empty);
    RUN_TEST(store_integrity_corrupt_bad_path);
    RUN_TEST(store_integrity_windows_lowercase_drive_issue367);
    RUN_TEST(store_integrity_multiple_project_rows_allowed);
    RUN_TEST(store_integrity_null_check);
    RUN_TEST(store_integrity_full_path_only_classification);
    RUN_TEST(store_project_crud);
    RUN_TEST(store_project_reads_reset_cached_statements);
    RUN_TEST(store_project_update);
    RUN_TEST(store_project_delete);
    RUN_TEST(store_node_crud);
    RUN_TEST(store_node_dedup);
    RUN_TEST(store_node_find_by_label);
    RUN_TEST(store_visit_nodes_by_label_identity_rows);
    RUN_TEST(store_node_find_by_file);
    RUN_TEST(store_node_find_not_found);
    RUN_TEST(store_node_count_empty);
    RUN_TEST(store_node_delete_by_file);
    RUN_TEST(store_node_delete_by_label);
    RUN_TEST(store_node_batch_upsert);
    RUN_TEST(store_node_batch_empty);
    RUN_TEST(store_cascade_delete);
    RUN_TEST(store_file_hash_crud);
    RUN_TEST(store_file_hash_upsert_rejects_null_required_fields);
    RUN_TEST(store_file_state_crud);
    RUN_TEST(store_file_state_get_resets_cached_statement);
    RUN_TEST(store_index_generation_reservation_monotonic);
    RUN_TEST(store_index_generation_reservation_requires_project);
    RUN_TEST(store_index_generation_finish_complete);
    RUN_TEST(store_latest_complete_index_generation_ignores_reserved_and_failed);
    RUN_TEST(store_index_generation_finish_failed_and_invalid_status);
    RUN_TEST(store_overlay_generation_reservation_status_and_counts);
    RUN_TEST(store_overlay_generation_rejects_invalid_inputs);
    RUN_TEST(store_claim_ready_overlay_generation_claims_oldest_once);
    RUN_TEST(store_claim_ready_overlay_generation_ignores_nonready_and_validates_outputs);
    RUN_TEST(store_overlay_file_delta_publish_rows_and_tombstone);
    RUN_TEST(store_delete_project_clears_overlay_fts);
    RUN_TEST(store_overlay_file_delta_publish_rejects_invalid_delta_without_rows);
    RUN_TEST(store_overlay_file_delta_batch_rolls_back_all_files);
    RUN_TEST(store_overlay_file_delta_publish_rejects_failed_generation);
    RUN_TEST(store_overlay_node_view_summary_counts_latest_ready_overlay);
    RUN_TEST(store_overlay_publish_prunes_superseded_file_rows_and_fts);
    RUN_TEST(store_compact_overlay_generation_promotes_metadata_and_cleans_overlay);
    RUN_TEST(store_compact_overlay_generation_promotes_delete_only_tombstone);
    RUN_TEST(store_find_nodes_by_file_overlay_view_returns_latest_ready_rows);
    RUN_TEST(store_search_overlay_view_without_ready_overlay_matches_canonical_search);
    RUN_TEST(store_search_overlay_view_matches_full_rebuild_oracle);
    RUN_TEST(store_search_overlay_view_uses_active_relationship_edges);
    RUN_TEST(store_schema_counts_overlay_view_uses_active_nodes_and_edges);
    RUN_TEST(store_owner_metadata_crud);
    RUN_TEST(store_rebuild_file_delta_owners_derives_from_graph);
    RUN_TEST(store_import_export_metadata_crud);
    RUN_TEST(store_file_delta_affected_paths_from_exports_and_imports);
    RUN_TEST(store_file_delta_affected_paths_high_fanout_dedupes);
    RUN_TEST(store_file_delta_publish_rolls_back_on_failure);
    RUN_TEST(store_file_delta_publish_matches_fresh_final_graph);
    RUN_TEST(store_file_delta_graph_noop_refreshes_metadata_only);
    RUN_TEST(store_file_delta_publish_failure_finishes_generation_failed);
    RUN_TEST(store_file_delta_publish_multifile_generation);
    RUN_TEST(store_file_delta_batch_publish_rolls_back_all_files);
    RUN_TEST(store_file_delta_batch_complete_marks_graph_views_stale);
    RUN_TEST(store_file_delta_batch_complete_rolls_back_when_generation_missing);
    RUN_TEST(store_file_delta_publish_commits_graph_and_metadata);
    RUN_TEST(store_file_delta_delete_cleans_graph_and_metadata);
    RUN_TEST(store_file_delta_delete_complete_finishes_generation);
    RUN_TEST(store_derived_view_state_public_api);
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
    RUN_TEST(store_node_null_project);
    RUN_TEST(store_node_null_qn);
    RUN_TEST(store_node_empty_strings);
    RUN_TEST(store_find_by_id_not_found);
    RUN_TEST(store_find_by_qn_not_found);
    RUN_TEST(store_find_by_qn_any_cross_project);
    RUN_TEST(store_find_by_name_any_cross_project);
    RUN_TEST(store_find_by_file_no_match);
    RUN_TEST(store_node_batch_upsert_zero);
    RUN_TEST(store_node_batch_upsert_100);
    RUN_TEST(store_delete_by_label_verify_remaining);
    RUN_TEST(store_delete_by_file_verify_remaining);
    RUN_TEST(store_node_upsert_updates_fields);
    RUN_TEST(store_node_long_qn);
    RUN_TEST(store_node_properties_special_chars);
    RUN_TEST(store_delete_nodes_nonexistent);
    RUN_TEST(store_count_nodes_unknown_project);
}
