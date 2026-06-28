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
    ASSERT_GT(caller, 0);
    ASSERT_GT(fs_callee, 0);
    ASSERT_GT(split_callee, 0);
    ASSERT_GT(empty_callee, 0);

    cbm_gbuf_insert_edge(gb, caller, fs_callee, "HTTP_CALLS",
                         "{\"callee\":\"requests.get\",\"url_path\":\"/etc/crio/crio.conf\","
                         "\"method\":\"GET\"}");
    cbm_gbuf_insert_edge(gb, caller, split_callee, "HTTP_CALLS",
                         "{\"callee\":\"str.split\",\"url_path\":\"/locations/\","
                         "\"method\":\"ANY\"}");
    cbm_gbuf_insert_edge(gb, caller, empty_callee, "HTTP_CALLS",
                         "{\"callee\":\"requests.get\",\"method\":\"GET\"}");

    cbm_pipeline_create_route_nodes(gb);

    ASSERT_NULL(cbm_gbuf_find_by_qn(gb, "__route__GET__/etc/crio/crio.conf"));
    ASSERT_NULL(cbm_gbuf_find_by_qn(gb, "__route__ANY__/locations/"));
    ASSERT_NULL(cbm_gbuf_find_by_qn(gb, "__route__GET__"));

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

SUITE(infrascan) {
    RUN_TEST(infrascan_http_route_literal_guard_rejects_filesystem_paths);
    RUN_TEST(infrascan_service_pattern_match_uses_qn_boundaries);
    RUN_TEST(infrascan_route_nodes_skip_bad_http_url_paths);
    RUN_TEST(infrascan_http_calls_join_matching_handler_route);
}
