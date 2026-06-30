#include "test_framework.h"
#include "graph_buffer/graph_buffer.h"
#include "pipeline/pipeline_internal.h"
#include "service_patterns.h"

#include <stdbool.h>
#include <stdint.h>

static int has_data_flow(cbm_gbuf_t *gb, int64_t source_id, int64_t target_id) {
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_source_type(gb, source_id, "DATA_FLOWS", &edges, &count);
    for (int i = 0; i < count; i++) {
        if (edges[i]->target_id == target_id) {
            return 1;
        }
    }
    return 0;
}

static int count_handles_to(cbm_gbuf_t *gb, int64_t target_id) {
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_target_type(gb, target_id, "HANDLES", &edges, &count);
    return count;
}

static bool has_handle(cbm_gbuf_t *gb, int64_t source_id, int64_t target_id) {
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_target_type(gb, target_id, "HANDLES", &edges, &count);
    for (int i = 0; i < count; i++) {
        if (edges[i]->source_id == source_id) {
            return true;
        }
    }
    return false;
}

TEST(infrascan_http_route_literal_guard_rejects_filesystem_paths) {
    ASSERT_FALSE(cbm_service_pattern_is_http_route_literal("/etc/crio/crio.conf", "requests.get"));
    ASSERT_FALSE(
        cbm_service_pattern_is_http_route_literal("/root/.aws/credentials", "requests.get"));
    ASSERT_FALSE(cbm_service_pattern_is_http_route_literal("/var/run/app.json", "requests.get"));
    ASSERT_FALSE(cbm_service_pattern_is_http_route_literal("/locations/", "str.split"));
    ASSERT_FALSE(cbm_service_pattern_is_http_route_literal("/api", "os.path.join"));
    ASSERT_FALSE(cbm_service_pattern_is_http_route_literal(NULL, "requests.get"));
    ASSERT_FALSE(cbm_service_pattern_is_http_route_literal("", "requests.get"));
    /* CLI slash-command syntax: ':' mid-segment is not a route param
     * (autorun's "/ar:allow", "/ar:a" etc. — not HTTP routes). */
    ASSERT_FALSE(cbm_service_pattern_is_http_route_literal("/ar:allow", "app.command"));
    ASSERT_FALSE(cbm_service_pattern_is_http_route_literal("/ar:a", "app.command"));
    ASSERT_FALSE(cbm_service_pattern_is_http_route_literal("/gh:pr", "app.command"));
    /* Filesystem paths with document/source extensions are never routes. */
    ASSERT_FALSE(cbm_service_pattern_is_http_route_literal("/new/file.txt", "open"));
    ASSERT_FALSE(cbm_service_pattern_is_http_route_literal("/fake/path.pdf", "open"));
    ASSERT_FALSE(cbm_service_pattern_is_http_route_literal("/Users/test/plans/foo.md", "open"));
    /* Filesystem roots. */
    ASSERT_FALSE(cbm_service_pattern_is_http_route_literal("/usr/bin/uv", "subprocess"));
    ASSERT_FALSE(cbm_service_pattern_is_http_route_literal("/home/user/.claude/plans/bar.md", "open"));
    ASSERT_FALSE(cbm_service_pattern_is_http_route_literal("/tmp/alpha", "requests.get"));
    ASSERT_FALSE(cbm_service_pattern_is_http_route_literal("/Users/dev/project", "requests.get"));
    /* Whitespace: command/description strings are not routes. */
    ASSERT_FALSE(cbm_service_pattern_is_http_route_literal("/autorun test task description", "run"));
    /* Positive controls: real routes must still pass. */
    ASSERT_TRUE(cbm_service_pattern_is_http_route_literal("/api/orders", "requests.get"));
    ASSERT_TRUE(cbm_service_pattern_is_http_route_literal("/users/:id", "app.route"));
    ASSERT_TRUE(cbm_service_pattern_is_http_route_literal("/teams/:team/users/:id", "app.route"));
    ASSERT_TRUE(cbm_service_pattern_is_http_route_literal("/items/{id}", "router.get"));
    ASSERT_TRUE(cbm_service_pattern_is_http_route_literal("https://orders.example/api/orders",
                                                          "requests.get"));
    PASS();
}

TEST(infrascan_service_pattern_match_uses_qn_boundaries) {
    ASSERT_EQ(cbm_service_pattern_match(
                  "proj.plugins.autorun.tests.test_plugin._dispatch"),
              CBM_SVC_NONE);
    ASSERT_EQ(cbm_service_pattern_match("proj.myrequests.client.get"), CBM_SVC_NONE);

    ASSERT_EQ(cbm_service_pattern_match("proj.gin.router.GET"), CBM_SVC_ROUTE_REG);
    ASSERT_EQ(cbm_service_pattern_match("proj.express.router.get"), CBM_SVC_ROUTE_REG);
    ASSERT_EQ(cbm_service_pattern_match("proj.venv.requests.api.get"), CBM_SVC_HTTP);
    ASSERT_EQ(cbm_service_pattern_match("proj.service.requests_get"), CBM_SVC_HTTP);
    ASSERT_EQ(cbm_service_pattern_match("proj.GuzzleHttp.Client.get"), CBM_SVC_HTTP);
    PASS();
}

TEST(infrascan_route_nodes_skip_bad_http_url_paths) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp/cbm_infrascan_route_guard");
    ASSERT_NOT_NULL(gb);
    int64_t caller =
        cbm_gbuf_upsert_node(gb, "Function", "client", "test.client", "client.py", 1, 3, "{}");
    int64_t fs_callee =
        cbm_gbuf_upsert_node(gb, "Function", "requests.get", "requests.get", "", 0, 0, "{}");
    int64_t split_callee =
        cbm_gbuf_upsert_node(gb, "Function", "str.split", "str.split", "", 0, 0, "{}");
    int64_t empty_callee =
        cbm_gbuf_upsert_node(gb, "Function", "requests.post", "requests.post", "", 0, 0, "{}");
    int64_t long_callee =
        cbm_gbuf_upsert_node(gb, "Function", "requests.put", "requests.put", "", 0, 0, "{}");
    ASSERT_GT(caller, 0);
    ASSERT_GT(fs_callee, 0);
    ASSERT_GT(split_callee, 0);
    ASSERT_GT(empty_callee, 0);
    ASSERT_GT(long_callee, 0);

    cbm_gbuf_insert_edge(gb, caller, fs_callee, "HTTP_CALLS",
                         "{\"callee\":\"requests.get\",\"url_path\":\"/etc/crio/crio.conf\","
                         "\"method\":\"GET\"}");
    cbm_gbuf_insert_edge(gb, caller, split_callee, "HTTP_CALLS",
                         "{\"callee\":\"str.split\",\"url_path\":\"/locations/\","
                         "\"method\":\"ANY\"}");
    cbm_gbuf_insert_edge(gb, caller, empty_callee, "HTTP_CALLS",
                         "{\"callee\":\"requests.get\",\"method\":\"GET\"}");
    char long_path[CBM_SZ_1K];
    const char route_prefix[] = "/api/";
    memset(long_path, 'a', sizeof(long_path));
    memcpy(long_path, route_prefix, sizeof(route_prefix) - 1);
    long_path[sizeof(long_path) - 1] = '\0';
    char long_props[CBM_SZ_2K];
    int n = snprintf(long_props, sizeof(long_props),
                     "{\"callee\":\"requests.put\",\"url_path\":\"%s\",\"method\":\"PUT\"}",
                     long_path);
    ASSERT_GT(n, 0);
    ASSERT_LT((size_t)n, sizeof(long_props));
    cbm_gbuf_insert_edge(gb, caller, long_callee, "HTTP_CALLS", long_props);

    cbm_pipeline_create_route_nodes(gb);

    ASSERT_NULL(cbm_gbuf_find_by_qn(gb, "__route__GET__/etc/crio/crio.conf"));
    ASSERT_NULL(cbm_gbuf_find_by_qn(gb, "__route__ANY__/locations/"));
    ASSERT_NULL(cbm_gbuf_find_by_qn(gb, "__route__GET__"));
    const cbm_gbuf_node_t **routes = NULL;
    int route_count = 0;
    ASSERT_EQ(cbm_gbuf_find_by_label(gb, "Route", &routes, &route_count), 0);
    ASSERT_EQ(route_count, 0);

    cbm_gbuf_free(gb);
    PASS();
}

TEST(infrascan_http_calls_join_matching_handler_route) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp/cbm_infrascan_route_join");
    ASSERT_NOT_NULL(gb);

    int64_t route = cbm_gbuf_upsert_node(gb, "Route", "/api/orders", "__route__GET__/api/orders",
                                         "", 0, 0, "{\"method\":\"GET\"}");
    int64_t handler = cbm_gbuf_upsert_node(gb, "Function", "get_orders", "test.get_orders",
                                           "server.py", 1, 3, "{}");
    int64_t client =
        cbm_gbuf_upsert_node(gb, "Function", "client", "test.client", "client.py", 1, 3, "{}");
    int64_t bad_route =
        cbm_gbuf_upsert_node(gb, "Route", "/etc/crio/crio.conf",
                             "__route__GET__/etc/crio/crio.conf", "", 0, 0, "{\"method\":\"GET\"}");
    int64_t bad_handler = cbm_gbuf_upsert_node(gb, "Function", "bad_handler", "test.bad_handler",
                                               "server.py", 5, 7, "{}");
    int64_t bad_client = cbm_gbuf_upsert_node(gb, "Function", "bad_client", "test.bad_client",
                                              "client.py", 5, 7, "{}");
    ASSERT_GT(route, 0);
    ASSERT_GT(handler, 0);
    ASSERT_GT(client, 0);
    ASSERT_GT(bad_route, 0);
    ASSERT_GT(bad_handler, 0);
    ASSERT_GT(bad_client, 0);

    cbm_gbuf_insert_edge(gb, handler, route, "HANDLES", "{\"handler\":\"test.get_orders\"}");
    cbm_gbuf_insert_edge(gb, client, route, "HTTP_CALLS",
                         "{\"callee\":\"requests.get\",\"url_path\":\"/api/orders\","
                         "\"method\":\"GET\"}");
    cbm_gbuf_insert_edge(gb, bad_handler, bad_route, "HANDLES",
                         "{\"handler\":\"test.bad_handler\"}");
    cbm_gbuf_insert_edge(gb, bad_client, bad_route, "HTTP_CALLS",
                         "{\"callee\":\"requests.get\",\"url_path\":\"/etc/crio/crio.conf\","
                         "\"method\":\"GET\"}");

    cbm_pipeline_create_route_nodes(gb);

    ASSERT_TRUE(has_data_flow(gb, client, handler));
    ASSERT_FALSE(has_data_flow(gb, bad_client, bad_handler));

    cbm_gbuf_free(gb);
    PASS();
}

TEST(infrascan_infra_match_does_not_expand_root_handlers_to_external_paths) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp/cbm_infrascan_infra_match");
    ASSERT_NOT_NULL(gb);

    int64_t root_route =
        cbm_gbuf_upsert_node(gb, "Route", "/", "__route__GET__/", "api/server.py", 0, 0,
                             "{\"method\":\"GET\"}");
    int64_t root_handler = cbm_gbuf_upsert_node(gb, "Function", "root", "test.root",
                                                "api/server.py", 1, 3, "{}");
    int64_t external =
        cbm_gbuf_upsert_node(gb, "Route", "https://github.com/pre-commit/pre-commit-hooks",
                             "__route__infra__https://github.com/pre-commit/pre-commit-hooks",
                             ".pre-commit-config.yaml", 0, 0, "{\"source\":\"infra\"}");
    int64_t api_root = cbm_gbuf_upsert_node(gb, "Route", "https://api.example.com/",
                                            "__route__infra__https://api.example.com/",
                                            "deploy.yaml", 0, 0, "{\"source\":\"infra\"}");
    ASSERT_GT(root_route, 0);
    ASSERT_GT(root_handler, 0);
    ASSERT_GT(external, 0);
    ASSERT_GT(api_root, 0);

    cbm_gbuf_insert_edge(gb, root_handler, root_route, "HANDLES", "{\"handler\":\"test.root\"}");

    cbm_pipeline_create_route_nodes(gb);

    ASSERT_EQ(count_handles_to(gb, external), 0);
    ASSERT_EQ(count_handles_to(gb, api_root), 1);

    cbm_gbuf_free(gb);
    PASS();
}

TEST(infrascan_infra_match_uses_all_matching_handler_routes) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp/cbm_infrascan_infra_match_all");
    ASSERT_NOT_NULL(gb);

    int64_t route_a = cbm_gbuf_upsert_node(gb, "Route", "/orders", "__route__GET__/orders",
                                           "services/orders/api.py", 0, 0,
                                           "{\"method\":\"GET\"}");
    int64_t route_b = cbm_gbuf_upsert_node(gb, "Route", "/orders", "__route__POST__/orders",
                                           "services/orders/admin.py", 0, 0,
                                           "{\"method\":\"POST\"}");
    int64_t handler_a = cbm_gbuf_upsert_node(gb, "Function", "list_orders", "test.list_orders",
                                             "services/orders/api.py", 1, 3, "{}");
    int64_t handler_b = cbm_gbuf_upsert_node(gb, "Function", "create_order", "test.create_order",
                                             "services/orders/admin.py", 1, 3, "{}");
    int64_t infra =
        cbm_gbuf_upsert_node(gb, "Route", "https://orders.example.com/orders",
                             "__route__infra__https://orders.example.com/orders", "deploy.yaml",
                             0, 0, "{\"source\":\"infra\"}");
    ASSERT_GT(route_a, 0);
    ASSERT_GT(route_b, 0);
    ASSERT_GT(handler_a, 0);
    ASSERT_GT(handler_b, 0);
    ASSERT_GT(infra, 0);

    cbm_gbuf_insert_edge(gb, handler_a, route_a, "HANDLES", "{\"handler\":\"test.list_orders\"}");
    cbm_gbuf_insert_edge(gb, handler_b, route_b, "HANDLES", "{\"handler\":\"test.create_order\"}");

    cbm_pipeline_create_route_nodes(gb);

    ASSERT_TRUE(has_handle(gb, handler_a, infra));
    ASSERT_TRUE(has_handle(gb, handler_b, infra));

    cbm_gbuf_free(gb);
    PASS();
}

TEST(infrascan_prefix_bridge_uses_all_registrars_not_first_edge) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp/cbm_infrascan_prefix_bridge");
    ASSERT_NOT_NULL(gb);

    int64_t prefix =
        cbm_gbuf_upsert_node(gb, "Route", "/api", "__route__ANY__/api", "svc/router.py", 0, 0,
                             "{\"method\":\"ANY\"}");
    int64_t users_registrar = cbm_gbuf_upsert_node(gb, "Function", "include_users",
                                                   "test.svc.users.router.include_users",
                                                   "svc/api/users/router.py", 1, 3, "{}");
    int64_t orders_registrar = cbm_gbuf_upsert_node(gb, "Function", "include_orders",
                                                    "test.svc.orders.router.include_orders",
                                                    "svc/api/orders/router.py", 1, 3, "{}");
    int64_t users_handler = cbm_gbuf_upsert_node(
        gb, "Function", "list_users", "test.svc.users.handlers.list_users",
        "svc/api/users/handlers.py", 10, 12, "{\"route_path\":\"/users\"}");
    int64_t orders_handler = cbm_gbuf_upsert_node(
        gb, "Function", "list_orders", "test.svc.orders.handlers.list_orders",
        "svc/api/orders/handlers.py", 10, 12, "{\"route_path\":\"/orders\"}");
    ASSERT_GT(prefix, 0);
    ASSERT_GT(users_registrar, 0);
    ASSERT_GT(orders_registrar, 0);
    ASSERT_GT(users_handler, 0);
    ASSERT_GT(orders_handler, 0);

    cbm_gbuf_insert_edge(gb, users_registrar, prefix, "CALLS", "{}");
    cbm_gbuf_insert_edge(gb, orders_registrar, prefix, "CALLS", "{}");

    cbm_pipeline_create_route_nodes(gb);

    ASSERT_TRUE(has_handle(gb, users_handler, prefix));
    ASSERT_TRUE(has_handle(gb, orders_handler, prefix));

    cbm_gbuf_free(gb);
    PASS();
}

SUITE(infrascan) {
    RUN_TEST(infrascan_http_route_literal_guard_rejects_filesystem_paths);
    RUN_TEST(infrascan_service_pattern_match_uses_qn_boundaries);
    RUN_TEST(infrascan_route_nodes_skip_bad_http_url_paths);
    RUN_TEST(infrascan_http_calls_join_matching_handler_route);
    RUN_TEST(infrascan_infra_match_does_not_expand_root_handlers_to_external_paths);
    RUN_TEST(infrascan_infra_match_uses_all_matching_handler_routes);
    RUN_TEST(infrascan_prefix_bridge_uses_all_registrars_not_first_edge);
}
