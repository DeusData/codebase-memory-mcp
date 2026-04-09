#include "../src/foundation/compat.h"
#include "test_framework.h"
#include <pipeline/servicelink.h>
#include <sqlite3.h>
#include <stdio.h>
#include <string.h>

/* Forward declaration — defined in pass_crossrepolinks.c */
int cbm_cross_project_link(const char *cache_dir);

static void rm_rf(const char *path) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", path);
    (void)system(cmd);
}

/* Helper: create a project .db with protocol_endpoints */
static void create_project_db(const char *dir, const char *name,
                               const char *inserts[], int insert_count) {
    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/%s.db", dir, name);

    sqlite3 *db = NULL;
    sqlite3_open(db_path, &db);
    sqlite3_exec(db,
        "CREATE TABLE IF NOT EXISTS protocol_endpoints ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  project TEXT NOT NULL, protocol TEXT NOT NULL, role TEXT NOT NULL,"
        "  identifier TEXT NOT NULL, node_qn TEXT NOT NULL, file_path TEXT NOT NULL,"
        "  extra TEXT DEFAULT '{}', UNIQUE(project,protocol,role,identifier,node_qn));",
        NULL, NULL, NULL);

    for (int i = 0; i < insert_count; i++) {
        sqlite3_exec(db, inserts[i], NULL, NULL, NULL);
    }
    sqlite3_close(db);
}

/* Helper: count rows in _crosslinks.db */
static int count_crosslinks(const char *cache_dir, const char *where_clause) {
    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/_crosslinks.db", cache_dir);

    sqlite3 *db = NULL;
    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        return -1;
    }
    char sql[512];
    if (where_clause && where_clause[0]) {
        snprintf(sql, sizeof(sql), "SELECT COUNT(*) FROM cross_links WHERE %s;", where_clause);
    } else {
        snprintf(sql, sizeof(sql), "SELECT COUNT(*) FROM cross_links;");
    }
    sqlite3_stmt *stmt = NULL;
    int count = 0;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            count = sqlite3_column_int(stmt, 0);
        }
        sqlite3_finalize(stmt);
    }
    sqlite3_close(db);
    return count;
}

/* Helper: get confidence of first matching crosslink */
static double get_crosslink_confidence(const char *cache_dir,
                                        const char *producer_project,
                                        const char *consumer_project) {
    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/_crosslinks.db", cache_dir);

    sqlite3 *db = NULL;
    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        return -1.0;
    }
    char sql[512];
    snprintf(sql, sizeof(sql),
        "SELECT confidence FROM cross_links WHERE producer_project='%s' AND consumer_project='%s' LIMIT 1;",
        producer_project, consumer_project);
    sqlite3_stmt *stmt = NULL;
    double conf = -1.0;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            conf = sqlite3_column_double(stmt, 0);
        }
        sqlite3_finalize(stmt);
    }
    sqlite3_close(db);
    return conf;
}

/* ── Tests ──────────────────────────────────────────────────────── */

TEST(cross_link_exact_match) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/xl-exact-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) { SKIP("cbm_mkdtemp failed"); }

    const char *api_inserts[] = {
        "INSERT INTO protocol_endpoints (project,protocol,role,identifier,node_qn,file_path) "
        "VALUES ('main-api','graphql','producer','getUser','r.UserResolver.getUser','src/r.ts');"
    };
    const char *app_inserts[] = {
        "INSERT INTO protocol_endpoints (project,protocol,role,identifier,node_qn,file_path) "
        "VALUES ('mobile-app','graphql','consumer','getUser','hooks.useGetUser','src/hooks/u.ts');"
    };

    create_project_db(tmpdir, "main-api", api_inserts, 1);
    create_project_db(tmpdir, "mobile-app", app_inserts, 1);

    int links = cbm_cross_project_link(tmpdir);
    ASSERT_EQ(links, 1);
    ASSERT_EQ(count_crosslinks(tmpdir, NULL), 1);

    double conf = get_crosslink_confidence(tmpdir, "main-api", "mobile-app");
    ASSERT_FLOAT_EQ(conf, 0.95, 0.01);

    rm_rf(tmpdir);
    PASS();
}

TEST(cross_link_normalized_match) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/xl-norm-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) { SKIP("cbm_mkdtemp failed"); }

    const char *api_inserts[] = {
        "INSERT INTO protocol_endpoints (project,protocol,role,identifier,node_qn,file_path) "
        "VALUES ('svc-a','pubsub','producer','orderCreated','svc.publish','src/pub.ts');"
    };
    const char *app_inserts[] = {
        "INSERT INTO protocol_endpoints (project,protocol,role,identifier,node_qn,file_path) "
        "VALUES ('svc-b','pubsub','consumer','order_created','svc.listen','src/sub.ts');"
    };

    create_project_db(tmpdir, "svc-a", api_inserts, 1);
    create_project_db(tmpdir, "svc-b", app_inserts, 1);

    int links = cbm_cross_project_link(tmpdir);
    ASSERT_EQ(links, 1);

    double conf = get_crosslink_confidence(tmpdir, "svc-a", "svc-b");
    ASSERT_FLOAT_EQ(conf, 0.85, 0.01);

    rm_rf(tmpdir);
    PASS();
}

TEST(cross_link_same_project_ignored) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/xl-same-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) { SKIP("cbm_mkdtemp failed"); }

    const char *inserts[] = {
        "INSERT INTO protocol_endpoints (project,protocol,role,identifier,node_qn,file_path) "
        "VALUES ('myproj','kafka','producer','events','fn1','a.ts');",
        "INSERT INTO protocol_endpoints (project,protocol,role,identifier,node_qn,file_path) "
        "VALUES ('myproj','kafka','consumer','events','fn2','b.ts');"
    };

    create_project_db(tmpdir, "myproj", inserts, 2);

    int links = cbm_cross_project_link(tmpdir);
    ASSERT_EQ(links, 0);

    rm_rf(tmpdir);
    PASS();
}

TEST(cross_link_no_match) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/xl-nomatch-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) { SKIP("cbm_mkdtemp failed"); }

    const char *a_inserts[] = {
        "INSERT INTO protocol_endpoints (project,protocol,role,identifier,node_qn,file_path) "
        "VALUES ('svc-a','kafka','producer','topicA','fn1','a.ts');"
    };
    const char *b_inserts[] = {
        "INSERT INTO protocol_endpoints (project,protocol,role,identifier,node_qn,file_path) "
        "VALUES ('svc-b','kafka','consumer','topicB','fn2','b.ts');"
    };

    create_project_db(tmpdir, "svc-a", a_inserts, 1);
    create_project_db(tmpdir, "svc-b", b_inserts, 1);

    int links = cbm_cross_project_link(tmpdir);
    ASSERT_EQ(links, 0);

    rm_rf(tmpdir);
    PASS();
}

TEST(cross_link_multiple_protocols) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/xl-multi-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) { SKIP("cbm_mkdtemp failed"); }

    const char *api_inserts[] = {
        "INSERT INTO protocol_endpoints (project,protocol,role,identifier,node_qn,file_path) "
        "VALUES ('api','graphql','producer','getUser','r.getUser','r.ts');",
        "INSERT INTO protocol_endpoints (project,protocol,role,identifier,node_qn,file_path) "
        "VALUES ('api','pubsub','consumer','order.created','l.onOrder','l.ts');"
    };
    const char *app_inserts[] = {
        "INSERT INTO protocol_endpoints (project,protocol,role,identifier,node_qn,file_path) "
        "VALUES ('app','graphql','consumer','getUser','h.useGetUser','h.ts');"
    };
    const char *svc_inserts[] = {
        "INSERT INTO protocol_endpoints (project,protocol,role,identifier,node_qn,file_path) "
        "VALUES ('order-svc','pubsub','producer','order.created','s.create','s.ts');"
    };

    create_project_db(tmpdir, "api", api_inserts, 2);
    create_project_db(tmpdir, "app", app_inserts, 1);
    create_project_db(tmpdir, "order-svc", svc_inserts, 1);

    int links = cbm_cross_project_link(tmpdir);
    ASSERT_EQ(links, 2);  /* graphql: api->app, pubsub: order-svc->api */

    ASSERT_EQ(count_crosslinks(tmpdir, "protocol='graphql'"), 1);
    ASSERT_EQ(count_crosslinks(tmpdir, "protocol='pubsub'"), 1);

    rm_rf(tmpdir);
    PASS();
}

TEST(cross_link_missing_table_skipped) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/xl-miss-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) { SKIP("cbm_mkdtemp failed"); }

    const char *a_inserts[] = {
        "INSERT INTO protocol_endpoints (project,protocol,role,identifier,node_qn,file_path) "
        "VALUES ('svc-a','kafka','producer','events','fn1','a.ts');"
    };
    create_project_db(tmpdir, "svc-a", a_inserts, 1);

    /* Create an empty DB (no protocol_endpoints table) */
    char old_db[512];
    snprintf(old_db, sizeof(old_db), "%s/old-project.db", tmpdir);
    sqlite3 *db = NULL;
    sqlite3_open(old_db, &db);
    sqlite3_exec(db, "CREATE TABLE nodes (id INTEGER PRIMARY KEY);", NULL, NULL, NULL);
    sqlite3_close(db);

    /* Should not crash, just skip the old DB */
    int links = cbm_cross_project_link(tmpdir);
    ASSERT_GTE(links, 0);  /* no consumers anywhere, so 0 links */

    rm_rf(tmpdir);
    PASS();
}

SUITE(cross_project_links) {
    RUN_TEST(cross_link_exact_match);
    RUN_TEST(cross_link_normalized_match);
    RUN_TEST(cross_link_same_project_ignored);
    RUN_TEST(cross_link_no_match);
    RUN_TEST(cross_link_multiple_protocols);
    RUN_TEST(cross_link_missing_table_skipped);
}
