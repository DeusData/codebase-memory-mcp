#include "../src/foundation/compat.h"
#include "test_framework.h"
#include <pipeline/servicelink.h>
#include <store/store.h>
#include <sqlite3.h>
#include <string.h>
#include <stdio.h>

/* ── Helpers ────────────────────────────────────────────────────── */

static void rm_rf(const char *path) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", path);
    (void)system(cmd);
}

/* Mini helper: count rows from protocol_endpoints for a given project */
static int count_endpoints(const char *db_path, const char *project) {
    cbm_store_t *s = cbm_store_open_path_query(db_path);
    if (!s) return -1;

    char sql[512];
    snprintf(sql, sizeof(sql),
        "SELECT COUNT(*) FROM protocol_endpoints WHERE project = '%s';", project);

    sqlite3_stmt *stmt = NULL;
    int count = 0;
    if (sqlite3_prepare_v2(cbm_store_get_db(s), sql, -1, &stmt, NULL) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            count = sqlite3_column_int(stmt, 0);
        }
        sqlite3_finalize(stmt);
    }
    cbm_store_close(s);
    return count;
}

/* ── Tests ──────────────────────────────────────────────────────── */

TEST(persist_endpoints_creates_table) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/ep-persist-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) { SKIP("cbm_mkdtemp failed"); }

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/test.db", tmpdir);

    /* Create a store so the DB exists with base schema */
    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);
    cbm_store_close(s);

    /* Persist some endpoints via cbm_persist_endpoints */
    cbm_sl_endpoint_list_t *list = cbm_sl_endpoint_list_new();
    sl_register_endpoint(list, "testproj", "graphql", "producer",
                         "getUser", "resolver.getUser", "src/r.ts", "{}");

    int rc = cbm_persist_endpoints(db_path, "testproj", list);
    ASSERT_EQ(rc, 1);

    int cnt = count_endpoints(db_path, "testproj");
    ASSERT_EQ(cnt, 1);

    cbm_sl_endpoint_list_free(list);
    rm_rf(tmpdir);
    PASS();
}

TEST(persist_endpoints_roundtrip) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/ep-rt-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) { SKIP("cbm_mkdtemp failed"); }

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/test.db", tmpdir);

    /* Create base schema */
    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);
    cbm_store_close(s);

    /* Persist 2 endpoints */
    cbm_sl_endpoint_list_t *list = cbm_sl_endpoint_list_new();
    sl_register_endpoint(list, "proj", "pubsub", "producer",
                         "order.created", "svc.OrderService.create", "src/order.ts", "{}");
    sl_register_endpoint(list, "proj", "pubsub", "consumer",
                         "order.created", "svc.Listener.onOrder", "src/listen.ts", "{}");

    cbm_persist_endpoints(db_path, "proj", list);
    cbm_sl_endpoint_list_free(list);

    /* Query back and verify */
    s = cbm_store_open_path_query(db_path);
    ASSERT_NOT_NULL(s);
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(cbm_store_get_db(s),
        "SELECT protocol, role, identifier, node_qn FROM protocol_endpoints "
        "WHERE project='proj' ORDER BY role;", -1, &stmt, NULL);
    ASSERT_EQ(rc, SQLITE_OK);

    /* First row: consumer (alphabetical order) */
    ASSERT_EQ(sqlite3_step(stmt), SQLITE_ROW);
    ASSERT_STR_EQ((const char *)sqlite3_column_text(stmt, 1), "consumer");
    ASSERT_STR_EQ((const char *)sqlite3_column_text(stmt, 2), "order.created");

    /* Second row: producer */
    ASSERT_EQ(sqlite3_step(stmt), SQLITE_ROW);
    ASSERT_STR_EQ((const char *)sqlite3_column_text(stmt, 1), "producer");

    sqlite3_finalize(stmt);
    cbm_store_close(s);
    rm_rf(tmpdir);
    PASS();
}

TEST(persist_endpoints_replaces_on_reindex) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/ep-repl-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) { SKIP("cbm_mkdtemp failed"); }

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/test.db", tmpdir);

    /* Create base schema */
    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);
    cbm_store_close(s);

    /* First index: 2 endpoints */
    cbm_sl_endpoint_list_t *list = cbm_sl_endpoint_list_new();
    sl_register_endpoint(list, "proj", "kafka", "producer",
                         "topicA", "fn1", "a.ts", "");
    sl_register_endpoint(list, "proj", "kafka", "producer",
                         "topicB", "fn2", "b.ts", "");
    cbm_persist_endpoints(db_path, "proj", list);
    cbm_sl_endpoint_list_free(list);

    ASSERT_EQ(count_endpoints(db_path, "proj"), 2);

    /* Simulate re-index: persist replaces old endpoints */
    list = cbm_sl_endpoint_list_new();
    sl_register_endpoint(list, "proj", "kafka", "consumer",
                         "topicC", "fn3", "c.ts", "");
    cbm_persist_endpoints(db_path, "proj", list);
    cbm_sl_endpoint_list_free(list);

    ASSERT_EQ(count_endpoints(db_path, "proj"), 1);

    rm_rf(tmpdir);
    PASS();
}

TEST(persist_endpoints_multiple_protocols) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/ep-multi-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) { SKIP("cbm_mkdtemp failed"); }

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/test.db", tmpdir);

    /* Create base schema */
    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);
    cbm_store_close(s);

    cbm_sl_endpoint_list_t *list = cbm_sl_endpoint_list_new();
    sl_register_endpoint(list, "proj", "graphql", "producer",
                         "getUser", "r.getUser", "r.ts", "");
    sl_register_endpoint(list, "proj", "pubsub", "consumer",
                         "order.created", "l.onOrder", "l.ts", "");
    cbm_persist_endpoints(db_path, "proj", list);
    cbm_sl_endpoint_list_free(list);

    /* Query by protocol */
    s = cbm_store_open_path_query(db_path);
    sqlite3_stmt *stmt = NULL;
    sqlite3_prepare_v2(cbm_store_get_db(s),
        "SELECT COUNT(*) FROM protocol_endpoints WHERE protocol='graphql';",
        -1, &stmt, NULL);
    sqlite3_step(stmt);
    ASSERT_EQ(sqlite3_column_int(stmt, 0), 1);
    sqlite3_finalize(stmt);

    sqlite3_prepare_v2(cbm_store_get_db(s),
        "SELECT COUNT(*) FROM protocol_endpoints WHERE protocol='pubsub';",
        -1, &stmt, NULL);
    sqlite3_step(stmt);
    ASSERT_EQ(sqlite3_column_int(stmt, 0), 1);
    sqlite3_finalize(stmt);

    cbm_store_close(s);
    rm_rf(tmpdir);
    PASS();
}

SUITE(endpoint_persistence) {
    RUN_TEST(persist_endpoints_creates_table);
    RUN_TEST(persist_endpoints_roundtrip);
    RUN_TEST(persist_endpoints_replaces_on_reindex);
    RUN_TEST(persist_endpoints_multiple_protocols);
}
