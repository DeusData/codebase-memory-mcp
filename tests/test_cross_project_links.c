#include "../src/foundation/compat.h"
#include "test_framework.h"
#include <pipeline/pass_cross_repo.h>
#include <pipeline/servicelink.h>
#include <store/store.h>
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

/* A single endpoint fixture row. */
typedef struct {
    const char *project;
    const char *protocol;
    const char *role;
    const char *identifier;
    const char *node_qn;
    const char *file_path;
    const char *extra; /* may be NULL → "{}" */
} ep_fixture_t;

/* Create a project DB at <dir>/<name>.db with full schema (nodes + edges +
 * protocol_endpoints). For each endpoint, insert a node with the given
 * qualified_name (so cbm_store_find_node_by_qn resolves it during linking)
 * and a row into protocol_endpoints. */
static void create_project_db(const char *dir, const char *name,
                              const ep_fixture_t *eps, int ep_count) {
    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/%s.db", dir, name);

    cbm_store_t *store = cbm_store_open_path(db_path);
    if (!store) return;

    cbm_store_upsert_project(store, name, dir);

    /* Ensure protocol_endpoints exists (cbm_store_open_path doesn't create it
     * — it's created lazily by cbm_persist_endpoints). */
    cbm_store_exec(store,
        "CREATE TABLE IF NOT EXISTS protocol_endpoints ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  project TEXT NOT NULL, protocol TEXT NOT NULL, role TEXT NOT NULL,"
        "  identifier TEXT NOT NULL, node_qn TEXT NOT NULL, file_path TEXT NOT NULL,"
        "  extra TEXT DEFAULT '{}', UNIQUE(project,protocol,role,identifier,node_qn));");

    sqlite3_stmt *ins = NULL;
    sqlite3_prepare_v2(cbm_store_get_db(store),
        "INSERT INTO protocol_endpoints (project,protocol,role,identifier,node_qn,file_path,extra) "
        "VALUES (?,?,?,?,?,?,?);", -1, &ins, NULL);

    for (int i = 0; i < ep_count; i++) {
        const ep_fixture_t *e = &eps[i];

        cbm_node_t n = {
            .project = e->project,
            .label = "Function",
            .name = e->node_qn,
            .qualified_name = e->node_qn,
            .file_path = e->file_path,
        };
        cbm_store_upsert_node(store, &n);

        if (ins) {
            sqlite3_bind_text(ins, 1, e->project, -1, SQLITE_STATIC);
            sqlite3_bind_text(ins, 2, e->protocol, -1, SQLITE_STATIC);
            sqlite3_bind_text(ins, 3, e->role, -1, SQLITE_STATIC);
            sqlite3_bind_text(ins, 4, e->identifier, -1, SQLITE_STATIC);
            sqlite3_bind_text(ins, 5, e->node_qn, -1, SQLITE_STATIC);
            sqlite3_bind_text(ins, 6, e->file_path, -1, SQLITE_STATIC);
            sqlite3_bind_text(ins, 7, e->extra ? e->extra : "{}", -1, SQLITE_STATIC);
            sqlite3_step(ins);
            sqlite3_reset(ins);
        }
    }
    if (ins) sqlite3_finalize(ins);
    cbm_store_close(store);
}

/* Count edges of `edge_type` in <dir>/<project>.db. */
static int count_edges_by_type(const char *dir, const char *project, const char *edge_type) {
    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/%s.db", dir, project);
    cbm_store_t *s = cbm_store_open_path_query(db_path);
    if (!s) return -1;
    int count = cbm_store_count_edges_by_type(s, project, edge_type);
    cbm_store_close(s);
    return count;
}

/* Read confidence from the first edge of `edge_type` in a project's DB by
 * scanning the properties JSON for `"confidence":<value>`. Returns -1.0 on
 * miss. */
static double get_edge_confidence(const char *dir, const char *project,
                                  const char *edge_type) {
    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/%s.db", dir, project);
    cbm_store_t *s = cbm_store_open_path_query(db_path);
    if (!s) return -1.0;

    cbm_edge_t *edges = NULL;
    int count = 0;
    cbm_store_find_edges_by_type(s, project, edge_type, &edges, &count);
    double conf = -1.0;
    if (count > 0 && edges[0].properties_json) {
        const char *p = strstr(edges[0].properties_json, "\"confidence\":");
        if (p) {
            conf = strtod(p + strlen("\"confidence\":"), NULL);
        }
    }
    cbm_store_free_edges(edges, count);
    cbm_store_close(s);
    return conf;
}

/* Returns 1 if a MessagingChannel node with QN __channel__<proto>__<id> exists
 * in <dir>/<project>.db, else 0. */
static int channel_node_exists(const char *dir, const char *project,
                               const char *protocol, const char *identifier) {
    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/%s.db", dir, project);
    cbm_store_t *s = cbm_store_open_path_query(db_path);
    if (!s) return 0;

    char qn[512];
    snprintf(qn, sizeof(qn), "__channel__%s__%s", protocol, identifier);
    cbm_node_t n = {0};
    int found = (cbm_store_find_node_by_qn(s, project, qn, &n) == 0);
    if (found) cbm_node_free_fields(&n);
    cbm_store_close(s);
    return found;
}

/* Returns the source/target node ids of the first edge of `edge_type` in
 * <dir>/<project>.db, or 0/0 if missing. Also copies properties_json into
 * props_buf (truncated to bufsz-1). */
static void get_first_edge_ends(const char *dir, const char *project,
                                const char *edge_type,
                                int64_t *out_src, int64_t *out_tgt,
                                char *props_buf, int props_bufsz) {
    *out_src = 0;
    *out_tgt = 0;
    if (props_buf && props_bufsz > 0) props_buf[0] = '\0';

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/%s.db", dir, project);
    cbm_store_t *s = cbm_store_open_path_query(db_path);
    if (!s) return;

    cbm_edge_t *edges = NULL;
    int count = 0;
    cbm_store_find_edges_by_type(s, project, edge_type, &edges, &count);
    if (count > 0) {
        *out_src = edges[0].source_id;
        *out_tgt = edges[0].target_id;
        if (props_buf && props_bufsz > 0 && edges[0].properties_json) {
            snprintf(props_buf, (size_t)props_bufsz, "%s", edges[0].properties_json);
        }
    }
    cbm_store_free_edges(edges, count);
    cbm_store_close(s);
}

/* Returns the local node id of a MessagingChannel anchor in <dir>/<project>.db,
 * or 0 if absent. */
static int64_t get_channel_node_id(const char *dir, const char *project,
                                   const char *protocol, const char *identifier) {
    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/%s.db", dir, project);
    cbm_store_t *s = cbm_store_open_path_query(db_path);
    if (!s) return 0;

    char qn[512];
    snprintf(qn, sizeof(qn), "__channel__%s__%s", protocol, identifier);
    cbm_node_t n = {0};
    int64_t id = 0;
    if (cbm_store_find_node_by_qn(s, project, qn, &n) == 0) {
        id = n.id;
        cbm_node_free_fields(&n);
    }
    cbm_store_close(s);
    return id;
}

/* Returns the local node id of a function-style node in <dir>/<project>.db,
 * or 0 if absent. */
static int64_t get_node_id_by_qn(const char *dir, const char *project, const char *qn) {
    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/%s.db", dir, project);
    cbm_store_t *s = cbm_store_open_path_query(db_path);
    if (!s) return 0;
    cbm_node_t n = {0};
    int64_t id = 0;
    if (cbm_store_find_node_by_qn(s, project, qn, &n) == 0) {
        id = n.id;
        cbm_node_free_fields(&n);
    }
    cbm_store_close(s);
    return id;
}

/* ── Tests ──────────────────────────────────────────────────────── */

TEST(cross_link_exact_match) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/xl-exact-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) { SKIP("cbm_mkdtemp failed"); }

    const ep_fixture_t prod_ep = {
        "main-api", "kafka", "producer", "user.created",
        "svc.UserService.publishCreated", "src/svc.ts", NULL
    };
    const ep_fixture_t cons_ep = {
        "consumer-svc", "kafka", "consumer", "user.created",
        "svc.Listener.onUserCreated", "src/listen.ts", NULL
    };

    create_project_db(tmpdir, "main-api", &prod_ep, 1);
    create_project_db(tmpdir, "consumer-svc", &cons_ep, 1);

    int links = cbm_cross_project_link(tmpdir);
    ASSERT_EQ(links, 1);

    /* Bidirectional: one edge in producer DB, one in consumer DB */
    ASSERT_EQ(count_edges_by_type(tmpdir, "main-api", CBM_EDGE_CROSS_KAFKA_CALLS), 1);
    ASSERT_EQ(count_edges_by_type(tmpdir, "consumer-svc", CBM_EDGE_CROSS_KAFKA_CALLS), 1);

    /* Anchor MessagingChannel nodes exist in both DBs */
    ASSERT_TRUE(channel_node_exists(tmpdir, "main-api", "kafka", "user.created"));
    ASSERT_TRUE(channel_node_exists(tmpdir, "consumer-svc", "kafka", "user.created"));

    /* Producer edge: function -> channel */
    int64_t prod_fn_id = get_node_id_by_qn(tmpdir, "main-api",
                                           "svc.UserService.publishCreated");
    int64_t prod_chan_id = get_channel_node_id(tmpdir, "main-api", "kafka", "user.created");
    ASSERT_GT(prod_fn_id, 0);
    ASSERT_GT(prod_chan_id, 0);
    int64_t fwd_src = 0, fwd_tgt = 0;
    char fwd_props[1024];
    get_first_edge_ends(tmpdir, "main-api", CBM_EDGE_CROSS_KAFKA_CALLS,
                        &fwd_src, &fwd_tgt, fwd_props, sizeof(fwd_props));
    ASSERT_EQ(fwd_src, prod_fn_id);
    ASSERT_EQ(fwd_tgt, prod_chan_id);
    ASSERT_TRUE(strstr(fwd_props, "\"target_project\":\"consumer-svc\"") != NULL);
    ASSERT_TRUE(strstr(fwd_props, "\"target_function\":\"svc.Listener.onUserCreated\"") != NULL);

    /* Consumer edge: channel -> function */
    int64_t cons_fn_id = get_node_id_by_qn(tmpdir, "consumer-svc",
                                           "svc.Listener.onUserCreated");
    int64_t cons_chan_id = get_channel_node_id(tmpdir, "consumer-svc", "kafka",
                                               "user.created");
    ASSERT_GT(cons_fn_id, 0);
    ASSERT_GT(cons_chan_id, 0);
    int64_t rev_src = 0, rev_tgt = 0;
    char rev_props[1024];
    get_first_edge_ends(tmpdir, "consumer-svc", CBM_EDGE_CROSS_KAFKA_CALLS,
                        &rev_src, &rev_tgt, rev_props, sizeof(rev_props));
    ASSERT_EQ(rev_src, cons_chan_id);
    ASSERT_EQ(rev_tgt, cons_fn_id);
    ASSERT_TRUE(strstr(rev_props, "\"source_project\":\"main-api\"") != NULL);
    ASSERT_TRUE(strstr(rev_props, "\"source_function\":\"svc.UserService.publishCreated\"") != NULL);

    double conf = get_edge_confidence(tmpdir, "main-api", CBM_EDGE_CROSS_KAFKA_CALLS);
    ASSERT_FLOAT_EQ(conf, 0.95, 0.01);

    rm_rf(tmpdir);
    PASS();
}

TEST(cross_link_normalized_match) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/xl-norm-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) { SKIP("cbm_mkdtemp failed"); }

    const ep_fixture_t prod_ep = {
        "svc-a", "pubsub", "producer", "orderCreated",
        "svc.publish", "src/pub.ts", NULL
    };
    const ep_fixture_t cons_ep = {
        "svc-b", "pubsub", "consumer", "order_created",
        "svc.listen", "src/sub.ts", NULL
    };

    create_project_db(tmpdir, "svc-a", &prod_ep, 1);
    create_project_db(tmpdir, "svc-b", &cons_ep, 1);

    int links = cbm_cross_project_link(tmpdir);
    ASSERT_EQ(links, 1);

    ASSERT_EQ(count_edges_by_type(tmpdir, "svc-a", CBM_EDGE_CROSS_PUBSUB_CALLS), 1);
    ASSERT_EQ(count_edges_by_type(tmpdir, "svc-b", CBM_EDGE_CROSS_PUBSUB_CALLS), 1);

    double conf = get_edge_confidence(tmpdir, "svc-a", CBM_EDGE_CROSS_PUBSUB_CALLS);
    ASSERT_FLOAT_EQ(conf, 0.85, 0.01);

    rm_rf(tmpdir);
    PASS();
}

TEST(cross_link_same_project_ignored) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/xl-same-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) { SKIP("cbm_mkdtemp failed"); }

    const ep_fixture_t eps[] = {
        {"myproj", "kafka", "producer", "events", "myproj.fn1", "a.ts", NULL},
        {"myproj", "kafka", "consumer", "events", "myproj.fn2", "b.ts", NULL},
    };
    create_project_db(tmpdir, "myproj", eps, 2);

    int links = cbm_cross_project_link(tmpdir);
    ASSERT_EQ(links, 0);
    ASSERT_EQ(count_edges_by_type(tmpdir, "myproj", CBM_EDGE_CROSS_KAFKA_CALLS), 0);

    rm_rf(tmpdir);
    PASS();
}

TEST(cross_link_no_match) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/xl-nomatch-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) { SKIP("cbm_mkdtemp failed"); }

    const ep_fixture_t prod_ep = {
        "svc-a", "kafka", "producer", "topicA", "svca.fn1", "a.ts", NULL
    };
    const ep_fixture_t cons_ep = {
        "svc-b", "kafka", "consumer", "topicB", "svcb.fn2", "b.ts", NULL
    };
    create_project_db(tmpdir, "svc-a", &prod_ep, 1);
    create_project_db(tmpdir, "svc-b", &cons_ep, 1);

    int links = cbm_cross_project_link(tmpdir);
    ASSERT_EQ(links, 0);
    ASSERT_EQ(count_edges_by_type(tmpdir, "svc-a", CBM_EDGE_CROSS_KAFKA_CALLS), 0);
    ASSERT_EQ(count_edges_by_type(tmpdir, "svc-b", CBM_EDGE_CROSS_KAFKA_CALLS), 0);

    rm_rf(tmpdir);
    PASS();
}

TEST(cross_link_multiple_protocols) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/xl-multi-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) { SKIP("cbm_mkdtemp failed"); }

    const ep_fixture_t api_eps[] = {
        {"api", "kafka", "producer", "user.created", "api.publish", "p.ts", NULL},
        {"api", "pubsub", "consumer", "order.created", "api.onOrder", "l.ts", NULL},
    };
    const ep_fixture_t app_eps[] = {
        {"app", "kafka", "consumer", "user.created", "app.useUser", "h.ts", NULL},
    };
    const ep_fixture_t svc_eps[] = {
        {"order-svc", "pubsub", "producer", "order.created", "svc.create", "s.ts", NULL},
    };

    create_project_db(tmpdir, "api", api_eps, 2);
    create_project_db(tmpdir, "app", app_eps, 1);
    create_project_db(tmpdir, "order-svc", svc_eps, 1);

    int links = cbm_cross_project_link(tmpdir);
    ASSERT_EQ(links, 2); /* kafka: api->app, pubsub: order-svc->api */

    /* kafka: producer api emits one outbound edge; consumer app emits one. */
    ASSERT_EQ(count_edges_by_type(tmpdir, "api", CBM_EDGE_CROSS_KAFKA_CALLS), 1);
    ASSERT_EQ(count_edges_by_type(tmpdir, "app", CBM_EDGE_CROSS_KAFKA_CALLS), 1);
    /* pubsub: producer order-svc emits one; consumer api emits one. */
    ASSERT_EQ(count_edges_by_type(tmpdir, "order-svc", CBM_EDGE_CROSS_PUBSUB_CALLS), 1);
    ASSERT_EQ(count_edges_by_type(tmpdir, "api", CBM_EDGE_CROSS_PUBSUB_CALLS), 1);

    rm_rf(tmpdir);
    PASS();
}

TEST(cross_link_missing_table_skipped) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/xl-miss-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) { SKIP("cbm_mkdtemp failed"); }

    const ep_fixture_t prod_ep = {
        "svc-a", "kafka", "producer", "events", "svca.fn1", "a.ts", NULL
    };
    create_project_db(tmpdir, "svc-a", &prod_ep, 1);

    /* DB without protocol_endpoints — opening via cbm_store_open_path will
     * still create base schema (nodes/edges/projects), but we drop the
     * endpoint table to simulate a project that was indexed before
     * messaging support landed. */
    char old_db[512];
    snprintf(old_db, sizeof(old_db), "%s/old-project.db", tmpdir);
    cbm_store_t *s = cbm_store_open_path(old_db);
    cbm_store_exec(s, "DROP TABLE IF EXISTS protocol_endpoints;");
    cbm_store_close(s);

    /* Should not crash, just skip the old DB. */
    int links = cbm_cross_project_link(tmpdir);
    ASSERT_GTE(links, 0);

    rm_rf(tmpdir);
    PASS();
}

TEST(cross_link_http_protocol_skipped) {
    /* http endpoints are owned by upstream's Route-QN matcher and must be
     * skipped by the messaging matcher — no CROSS_HTTP_CALLS edges should
     * be emitted from protocol_endpoints rows here. */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/xl-skip-http-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) { SKIP("cbm_mkdtemp failed"); }

    const ep_fixture_t prod_ep = {
        "projA", "http", "producer", "POST /v1/score", "projA.call", "c.js", NULL
    };
    const ep_fixture_t cons_ep = {
        "projB", "http", "consumer", "POST /v1/score", "projB.score", "r.js", NULL
    };
    create_project_db(tmpdir, "projA", &prod_ep, 1);
    create_project_db(tmpdir, "projB", &cons_ep, 1);

    int links = cbm_cross_project_link(tmpdir);
    ASSERT_EQ(links, 0);
    ASSERT_EQ(count_edges_by_type(tmpdir, "projA", CBM_EDGE_CROSS_HTTP_CALLS), 0);
    ASSERT_EQ(count_edges_by_type(tmpdir, "projB", CBM_EDGE_CROSS_HTTP_CALLS), 0);

    rm_rf(tmpdir);
    PASS();
}

TEST(cross_link_unresolved_qn_skipped) {
    /* If a producer endpoint references a node_qn that doesn't exist in the
     * project's nodes table, the matcher logs a warning and skips emission
     * (resolve_node_id returns 0). */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/xl-unres-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) { SKIP("cbm_mkdtemp failed"); }

    /* Insert endpoint row but no matching node — by manually opening DB
     * after create_project_db and inserting an extra protocol_endpoints
     * row whose node_qn doesn't exist as a node. */
    const ep_fixture_t prod_ep = {
        "svc-a", "kafka", "producer", "events", "svca.real", "a.ts", NULL
    };
    create_project_db(tmpdir, "svc-a", &prod_ep, 1);

    /* Consumer side: real endpoint row, but node_qn doesn't have a node row. */
    char b_db[512];
    snprintf(b_db, sizeof(b_db), "%s/svc-b.db", tmpdir);
    cbm_store_t *bs = cbm_store_open_path(b_db);
    cbm_store_upsert_project(bs, "svc-b", tmpdir);
    cbm_store_exec(bs,
        "CREATE TABLE IF NOT EXISTS protocol_endpoints ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  project TEXT NOT NULL, protocol TEXT NOT NULL, role TEXT NOT NULL,"
        "  identifier TEXT NOT NULL, node_qn TEXT NOT NULL, file_path TEXT NOT NULL,"
        "  extra TEXT DEFAULT '{}', UNIQUE(project,protocol,role,identifier,node_qn));");
    cbm_store_exec(bs,
        "INSERT INTO protocol_endpoints (project,protocol,role,identifier,node_qn,file_path) "
        "VALUES ('svc-b','kafka','consumer','events','svcb.ghost','b.ts');");
    cbm_store_close(bs);

    int links = cbm_cross_project_link(tmpdir);
    ASSERT_EQ(links, 0); /* consumer node unresolved → emit returns 0 */
    ASSERT_EQ(count_edges_by_type(tmpdir, "svc-a", CBM_EDGE_CROSS_KAFKA_CALLS), 0);
    ASSERT_EQ(count_edges_by_type(tmpdir, "svc-b", CBM_EDGE_CROSS_KAFKA_CALLS), 0);

    rm_rf(tmpdir);
    PASS();
}

TEST(cross_link_idempotent_rerun) {
    /* Running the matcher twice must produce the same edge counts (wipe of
     * stale messaging CROSS_* edges before re-emission) and reuse the
     * existing MessagingChannel anchor nodes. */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/xl-idem-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) { SKIP("cbm_mkdtemp failed"); }

    const ep_fixture_t prod_ep = {
        "svc-a", "sqs", "producer", "events", "svca.pub", "p.ts", NULL
    };
    const ep_fixture_t cons_ep = {
        "svc-b", "sqs", "consumer", "events", "svcb.sub", "s.ts", NULL
    };
    create_project_db(tmpdir, "svc-a", &prod_ep, 1);
    create_project_db(tmpdir, "svc-b", &cons_ep, 1);

    int links1 = cbm_cross_project_link(tmpdir);
    ASSERT_EQ(links1, 1);
    int64_t chan_a_first = get_channel_node_id(tmpdir, "svc-a", "sqs", "events");
    int64_t chan_b_first = get_channel_node_id(tmpdir, "svc-b", "sqs", "events");
    ASSERT_GT(chan_a_first, 0);
    ASSERT_GT(chan_b_first, 0);

    int links2 = cbm_cross_project_link(tmpdir);
    ASSERT_EQ(links2, 1);

    ASSERT_EQ(count_edges_by_type(tmpdir, "svc-a", CBM_EDGE_CROSS_SQS_CALLS), 1);
    ASSERT_EQ(count_edges_by_type(tmpdir, "svc-b", CBM_EDGE_CROSS_SQS_CALLS), 1);

    /* Anchor nodes reused (same id), not duplicated. */
    ASSERT_EQ(get_channel_node_id(tmpdir, "svc-a", "sqs", "events"), chan_a_first);
    ASSERT_EQ(get_channel_node_id(tmpdir, "svc-b", "sqs", "events"), chan_b_first);

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
    RUN_TEST(cross_link_http_protocol_skipped);
    RUN_TEST(cross_link_unresolved_qn_skipped);
    RUN_TEST(cross_link_idempotent_rerun);
}
