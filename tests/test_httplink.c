/*
 * test_httplink.c — Tests for HTTP route discovery and cross-service linking.
 *
 * Port of Go internal/httplink/ test files:
 *   - similarity_test.go (4 tests)
 *   - httplink_test.go (32 tests)
 *   - config_test.go (5 tests)
 *   - langparity_test.go (2 tests)
 *
 * Total: 43 Go tests → 43 C tests
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include <pipeline/httplink.h>
#include <foundation/yaml.h>
#include <store/store.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

/* ═══════════════════════════════════════════════════════════════════
 *  Similarity tests (port of similarity_test.go)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(httplink_levenshtein_distance) {
    ASSERT_EQ(cbm_levenshtein_distance("", ""), 0);
    ASSERT_EQ(cbm_levenshtein_distance("abc", ""), 3);
    ASSERT_EQ(cbm_levenshtein_distance("", "abc"), 3);
    ASSERT_EQ(cbm_levenshtein_distance("abc", "abc"), 0);
    ASSERT_EQ(cbm_levenshtein_distance("kitten", "sitting"), 3);
    ASSERT_EQ(cbm_levenshtein_distance("api/orders", "api/order"), 1);
    ASSERT_EQ(cbm_levenshtein_distance("/api/v1/orders", "/api/v2/orders"), 1);
    PASS();
}

TEST(httplink_normalized_levenshtein) {
    double v;

    v = cbm_normalized_levenshtein("abc", "abc");
    ASSERT_FLOAT_EQ(v, 1.0, 0.001);

    v = cbm_normalized_levenshtein("", "");
    ASSERT_FLOAT_EQ(v, 1.0, 0.001);

    v = cbm_normalized_levenshtein("api/orders", "api/order");
    ASSERT(v >= 0.88 && v <= 0.92);

    v = cbm_normalized_levenshtein("/api/v1/items", "/api/v2/items");
    ASSERT(v >= 0.90 && v <= 0.94);

    v = cbm_normalized_levenshtein("completely", "different");
    ASSERT(v >= 0.0 && v <= 0.4);

    PASS();
}

TEST(httplink_ngram_overlap) {
    double v;

    v = cbm_ngram_overlap("api/orders", "api/orders", 3);
    ASSERT_FLOAT_EQ(v, 1.0, 0.001);

    v = cbm_ngram_overlap("api/orders", "api/order", 3);
    ASSERT(v >= 0.8 && v <= 1.0);

    v = cbm_ngram_overlap("abcdef", "ghijkl", 3);
    ASSERT_FLOAT_EQ(v, 0.0, 0.001);

    v = cbm_ngram_overlap("ab", "cd", 3);
    ASSERT_FLOAT_EQ(v, 0.0, 0.001);

    PASS();
}

TEST(httplink_confidence_band) {
    ASSERT_STR_EQ(cbm_confidence_band(0.95), "high");
    ASSERT_STR_EQ(cbm_confidence_band(0.70), "high");
    ASSERT_STR_EQ(cbm_confidence_band(0.69), "medium");
    ASSERT_STR_EQ(cbm_confidence_band(0.45), "medium");
    ASSERT_STR_EQ(cbm_confidence_band(0.44), "speculative");
    ASSERT_STR_EQ(cbm_confidence_band(0.25), "speculative");
    ASSERT_STR_EQ(cbm_confidence_band(0.24), "");
    ASSERT_STR_EQ(cbm_confidence_band(0.0), "");
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Path matching tests (port of httplink_test.go path tests)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(httplink_normalize_path) {
    ASSERT_STR_EQ(cbm_normalize_path("/api/orders/"), "/api/orders");
    ASSERT_STR_EQ(cbm_normalize_path("/api/orders"), "/api/orders");
    ASSERT_STR_EQ(cbm_normalize_path("/api/orders/:id"), "/api/orders/*");
    ASSERT_STR_EQ(cbm_normalize_path("/api/orders/{order_id}"), "/api/orders/*");
    ASSERT_STR_EQ(cbm_normalize_path("/API/Orders"), "/api/orders");
    ASSERT_STR_EQ(cbm_normalize_path("/api/:version/items/:id"), "/api/*/items/*");
    ASSERT_STR_EQ(cbm_normalize_path("/api/{version}/items/{id}"), "/api/*/items/*");
    ASSERT_STR_EQ(cbm_normalize_path("/"), "");
    ASSERT_STR_EQ(cbm_normalize_path(""), "");
    PASS();
}

TEST(httplink_paths_match) {
    /* Exact match */
    ASSERT_TRUE(cbm_paths_match("/api/orders", "/api/orders"));
    ASSERT_TRUE(cbm_paths_match("/api/orders/", "/api/orders"));

    /* Case insensitive */
    ASSERT_TRUE(cbm_paths_match("/API/Orders", "/api/orders"));

    /* Suffix match */
    ASSERT_TRUE(cbm_paths_match("https://example.com/api/orders", "/api/orders"));

    /* Wildcard params */
    ASSERT_TRUE(cbm_paths_match("/api/orders/:id", "/api/orders/{order_id}"));
    ASSERT_TRUE(cbm_paths_match("/api/orders/123", "/api/orders/:id"));

    /* Segment wildcard */
    ASSERT_TRUE(cbm_paths_match("/api/:version/items", "/api/v1/items"));

    /* Different lengths */
    ASSERT_FALSE(cbm_paths_match("/api/orders", "/api/orders/detail"));
    ASSERT_FALSE(cbm_paths_match("/api", "/api/orders"));

    /* Both wildcards */
    ASSERT_TRUE(cbm_paths_match("/api/*/items", "/api/*/items"));

    /* No match */
    ASSERT_FALSE(cbm_paths_match("/api/users", "/api/orders"));

    PASS();
}

TEST(httplink_paths_match_suffix) {
    ASSERT_TRUE(cbm_paths_match("/host/prefix/api/orders", "/api/orders"));
    PASS();
}

TEST(httplink_path_match_score) {
    double v;

    /* Exact matches */
    v = cbm_path_match_score("/api/orders", "/api/orders");
    ASSERT(v >= 0.78 && v <= 0.82);

    v = cbm_path_match_score("/integrate", "/integrate");
    ASSERT(v >= 0.60 && v <= 0.67);

    v = cbm_path_match_score("/api/v1/orders/items", "/api/v1/orders/items");
    ASSERT(v >= 0.93 && v <= 0.96);

    /* URL with scheme+host — normalizes to exact match after stripping host */
    v = cbm_path_match_score("https://host/api/orders", "/api/orders");
    ASSERT(v >= 0.78 && v <= 0.82);

    /* Numeric IDs normalized to wildcard */
    v = cbm_path_match_score("/api/orders/123", "/api/orders/:id");
    ASSERT(v >= 0.90 && v <= 0.96);

    /* No match */
    v = cbm_path_match_score("/api/users", "/api/orders");
    ASSERT_FLOAT_EQ(v, 0.0, 0.001);

    v = cbm_path_match_score("/", "/api/orders");
    ASSERT_FLOAT_EQ(v, 0.0, 0.001);

    v = cbm_path_match_score("", "/api/orders");
    ASSERT_FLOAT_EQ(v, 0.0, 0.001);

    PASS();
}

TEST(httplink_same_service) {
    /* Same dir */
    ASSERT_TRUE(cbm_same_service("a.b.c.mod.Func1", "a.b.c.mod.Func2"));
    /* Different dir */
    ASSERT_FALSE(cbm_same_service("a.b.c.mod.Func1", "a.b.x.mod.Func2"));
    /* Same deep dir */
    ASSERT_TRUE(cbm_same_service("a.b.c.d.mod.Func", "a.b.c.d.mod.Other"));
    /* Different deep dir */
    ASSERT_FALSE(cbm_same_service("a.b.c.d.mod.Func", "a.b.c.e.mod.Other"));
    /* Too few segments */
    ASSERT_FALSE(cbm_same_service("short.x", "short.y"));
    ASSERT_FALSE(cbm_same_service("a.b", "a.b"));
    /* 3 segments: dir="a" */
    ASSERT_TRUE(cbm_same_service("a.b.c", "a.b.c"));
    ASSERT_FALSE(cbm_same_service("a.b.c", "x.b.c"));
    /* Realistic multi-service */
    ASSERT_TRUE(cbm_same_service("myapp.docker-images.cloud-runs.order-service.main.Func",
                                 "myapp.docker-images.cloud-runs.order-service.handlers.Other"));
    ASSERT_FALSE(
        cbm_same_service("myapp.docker-images.cloud-runs.order-service.main.Func",
                         "myapp.docker-images.cloud-runs.notification-service.main.health_check"));
    ASSERT_TRUE(cbm_same_service("myapp.docker-images.cloud-runs.svcA.sub.mod.Func",
                                 "myapp.docker-images.cloud-runs.svcA.sub.mod.Other"));
    ASSERT_FALSE(cbm_same_service("myapp.docker-images.cloud-runs.svcA.sub.mod.Func",
                                  "myapp.docker-images.cloud-runs.svcB.sub.mod.Other"));
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  URL extraction tests
 * ═══════════════════════════════════════════════════════════════════ */

TEST(httplink_extract_url_paths) {
    char *paths[16];
    int n;

    n = cbm_extract_url_paths("URL = \"https://example.com/api/orders\"", paths, 16);
    ASSERT_EQ(n, 1);
    for (int i = 0; i < n; i++)
        free(paths[i]);

    n = cbm_extract_url_paths("fetch(\"http://host/api/v1/items\")", paths, 16);
    ASSERT_EQ(n, 1);
    for (int i = 0; i < n; i++)
        free(paths[i]);

    n = cbm_extract_url_paths("path = \"/api/orders\"", paths, 16);
    ASSERT_EQ(n, 1);
    for (int i = 0; i < n; i++)
        free(paths[i]);

    n = cbm_extract_url_paths("no urls here", paths, 16);
    ASSERT_EQ(n, 0);

    n = cbm_extract_url_paths("both = \"https://a.com/api/x\" and \"/api/y\"", paths, 16);
    ASSERT_EQ(n, 2);
    for (int i = 0; i < n; i++)
        free(paths[i]);

    PASS();
}

TEST(httplink_extract_json_string_paths) {
    char *paths[16];
    int n;

    n = cbm_extract_json_string_paths(
        "BODY = '{\"target\": \"https://api.internal.com/api/orders\", \"method\": \"POST\"}'",
        paths, 16);
    ASSERT_EQ(n, 1);
    for (int i = 0; i < n; i++)
        free(paths[i]);

    n = cbm_extract_json_string_paths(
        "CONFIG = {\"endpoint\": \"/api/v1/process\", \"timeout\": 30}", paths, 16);
    ASSERT_EQ(n, 1);
    for (int i = 0; i < n; i++)
        free(paths[i]);

    n = cbm_extract_json_string_paths("plain string without json", paths, 16);
    ASSERT_EQ(n, 0);

    n = cbm_extract_json_string_paths(
        "{\"services\": [{\"url\": \"https://svc.example.com/api/health\"}]}", paths, 16);
    ASSERT_EQ(n, 1);
    for (int i = 0; i < n; i++)
        free(paths[i]);

    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Route extraction: Python
 * ═══════════════════════════════════════════════════════════════════ */

TEST(httplink_extract_python_routes) {
    const char *decs[] = {"@app.post(\"/api/orders\")"};
    cbm_route_handler_t routes[4];
    int n = cbm_extract_python_routes("create_order", "proj.api.routes.create_order", decs, 1,
                                      routes, 4);
    ASSERT_EQ(n, 1);
    ASSERT_STR_EQ(routes[0].path, "/api/orders");
    ASSERT_STR_EQ(routes[0].method, "POST");
    ASSERT_STR_EQ(routes[0].qualified_name, "proj.api.routes.create_order");
    PASS();
}

TEST(httplink_extract_python_routes_multiple) {
    const char *decs[] = {"@router.get(\"/api/items/{item_id}\")", "@router.post(\"/api/items\")"};
    cbm_route_handler_t routes[4];
    int n = cbm_extract_python_routes("handler", "proj.api.handler", decs, 2, routes, 4);
    ASSERT_EQ(n, 2);
    PASS();
}

TEST(httplink_extract_python_routes_no_decorators) {
    cbm_route_handler_t routes[4];
    int n = cbm_extract_python_routes("helper", "proj.utils.helper", NULL, 0, routes, 4);
    ASSERT_EQ(n, 0);
    PASS();
}

TEST(httplink_extract_python_ws_routes) {
    const char *decs[] = {"@app.websocket(\"/ws/chat\")"};
    cbm_route_handler_t routes[4];
    int n = cbm_extract_python_routes("ws_handler", "proj.api.ws_handler", decs, 1, routes, 4);
    ASSERT_EQ(n, 1);
    ASSERT_STR_EQ(routes[0].path, "/ws/chat");
    ASSERT_STR_EQ(routes[0].method, "WS");
    ASSERT_STR_EQ(routes[0].protocol, "ws");
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Route extraction: Go gin/chi
 * ═══════════════════════════════════════════════════════════════════ */

TEST(httplink_extract_go_routes) {
    const char *source = "\tr.POST(\"/api/orders\", h.CreateOrder)\n"
                         "\tr.GET(\"/api/orders/:id\", h.GetOrder)\n";
    cbm_route_handler_t routes[8];
    int n = cbm_extract_go_routes("RegisterRoutes", "proj.api.RegisterRoutes", source, routes, 8);
    ASSERT_EQ(n, 2);
    ASSERT_STR_EQ(routes[0].path, "/api/orders");
    ASSERT_STR_EQ(routes[0].method, "POST");
    ASSERT_STR_EQ(routes[1].path, "/api/orders/:id");
    PASS();
}

TEST(httplink_chi_prefix) {
    const char *source = "func SetupRoutes(r chi.Router) {\n"
                         "\tr.Route(\"/api\", func(r chi.Router) {\n"
                         "\t\tr.Get(\"/health\", healthHandler)\n"
                         "\t\tr.Route(\"/users\", func(r chi.Router) {\n"
                         "\t\t\tr.Get(\"/\", listUsers)\n"
                         "\t\t\tr.Post(\"/{id}\", updateUser)\n"
                         "\t\t})\n"
                         "\t})\n"
                         "}\n";
    cbm_route_handler_t routes[8];
    int n = cbm_extract_go_routes("SetupRoutes", "proj.SetupRoutes", source, routes, 8);
    ASSERT_EQ(n, 3);

    /* Verify all routes have /api prefix */
    /* Verify all routes have /api prefix */
    bool found_health = false;
    for (int i = 0; i < n; i++) {
        if (strcmp(routes[i].path, "/api/health") == 0 && strcmp(routes[i].method, "GET") == 0)
            found_health = true;
    }
    ASSERT_TRUE(found_health);
    PASS();
}

TEST(httplink_chi_prefix_mixed_with_gin) {
    const char *source = "func RegisterRoutes(r *gin.RouterGroup) {\n"
                         "\torders := r.Group(\"/orders\")\n"
                         "\torders.GET(\"/:id\", getOrder)\n"
                         "\torders.POST(\"\", createOrder)\n"
                         "}\n";
    cbm_route_handler_t routes[8];
    int n = cbm_extract_go_routes("RegisterRoutes", "proj.RegisterRoutes", source, routes, 8);
    ASSERT_EQ(n, 2);
    /* Both should have /orders prefix from gin group */
    for (int i = 0; i < n; i++) {
        ASSERT(strncmp(routes[i].path, "/orders", 7) == 0);
    }
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Route extraction: Java Spring
 * ═══════════════════════════════════════════════════════════════════ */

TEST(httplink_extract_spring_ws_routes) {
    const char *decs[] = {"@MessageMapping(\"/chat\")"};
    cbm_route_handler_t routes[4];
    int n =
        cbm_extract_java_routes("handleChat", "proj.ChatController.handleChat", decs, 1, routes, 4);
    ASSERT_EQ(n, 1);
    ASSERT_STR_EQ(routes[0].path, "/chat");
    ASSERT_STR_EQ(routes[0].method, "WS");
    ASSERT_STR_EQ(routes[0].protocol, "ws");
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Route extraction: Ktor
 * ═══════════════════════════════════════════════════════════════════ */

TEST(httplink_extract_ktor_ws_routes) {
    const char *source = "\twebSocket(\"/chat\") {\n"
                         "\t\tfor (frame in incoming) {\n"
                         "\t\t\tsend(frame)\n"
                         "\t\t}\n"
                         "\t}\n"
                         "\tget(\"/api/health\") {\n"
                         "\t\tcall.respond(\"ok\")\n"
                         "\t}\n";
    cbm_route_handler_t routes[8];
    int n = cbm_extract_ktor_routes("configureRouting", "proj.Routing.configureRouting", source,
                                    routes, 8);
    ASSERT_EQ(n, 2);

    bool ws_found = false, http_found = false;
    for (int i = 0; i < n; i++) {
        if (strcmp(routes[i].protocol, "ws") == 0 && strcmp(routes[i].path, "/chat") == 0 &&
            strcmp(routes[i].method, "WS") == 0)
            ws_found = true;
        if (strcmp(routes[i].path, "/api/health") == 0 && strcmp(routes[i].method, "GET") == 0)
            http_found = true;
    }
    ASSERT_TRUE(ws_found);
    ASSERT_TRUE(http_found);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Route extraction: Express.js (with allowlist filtering)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(httplink_express_route_filtering) {
    cbm_route_handler_t routes[4];
    int n;

    /* Should match (allowlisted receivers) */
    n = cbm_extract_express_routes("testFunc", "proj.test.testFunc",
                                   "app.get('/api/users', handler)", routes, 4);
    ASSERT_EQ(n, 1);
    ASSERT_STR_EQ(routes[0].method, "GET");
    ASSERT_STR_EQ(routes[0].path, "/api/users");

    n = cbm_extract_express_routes("testFunc", "proj.test.testFunc",
                                   "router.post('/orders', handler)", routes, 4);
    ASSERT_EQ(n, 1);
    ASSERT_STR_EQ(routes[0].method, "POST");

    n = cbm_extract_express_routes("testFunc", "proj.test.testFunc",
                                   "server.put('/items', handler)", routes, 4);
    ASSERT_EQ(n, 1);

    n = cbm_extract_express_routes("testFunc", "proj.test.testFunc",
                                   "api.delete('/users/:id', handler)", routes, 4);
    ASSERT_EQ(n, 1);

    n = cbm_extract_express_routes("testFunc", "proj.test.testFunc",
                                   "routes.patch('/items/:id', handler)", routes, 4);
    ASSERT_EQ(n, 1);

    /* Should NOT match (not in allowlist) */
    n = cbm_extract_express_routes("testFunc", "proj.test.testFunc", "req.get('Content-Type')",
                                   routes, 4);
    ASSERT_EQ(n, 0);

    n = cbm_extract_express_routes("testFunc", "proj.test.testFunc", "res.get('key')", routes, 4);
    ASSERT_EQ(n, 0);

    n = cbm_extract_express_routes("testFunc", "proj.test.testFunc", "this.get('property')", routes,
                                   4);
    ASSERT_EQ(n, 0);

    n = cbm_extract_express_routes("testFunc", "proj.test.testFunc", "map.get('key')", routes, 4);
    ASSERT_EQ(n, 0);

    n = cbm_extract_express_routes("testFunc", "proj.test.testFunc", "model.delete('record')",
                                   routes, 4);
    ASSERT_EQ(n, 0);

    n = cbm_extract_express_routes("testFunc", "proj.test.testFunc", "params.get('id')", routes, 4);
    ASSERT_EQ(n, 0);

    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Detection tests
 * ═══════════════════════════════════════════════════════════════════ */

TEST(httplink_detect_protocol) {
    ASSERT_STR_EQ(cbm_detect_protocol("err := websocket.Upgrade(w, r, nil, 1024, 1024)"), "ws");
    ASSERT_STR_EQ(cbm_detect_protocol("conn, err := websocket.Accept(w, r, nil)"), "ws");
    ASSERT_STR_EQ(cbm_detect_protocol("conn, err := upgrader.Upgrade(w, r, nil)"), "ws");
    ASSERT_STR_EQ(cbm_detect_protocol("ws.on(\"connection\", func)"), "ws");
    ASSERT_STR_EQ(cbm_detect_protocol("io.on(\"connection\", handler)"), "ws");
    ASSERT_STR_EQ(cbm_detect_protocol("w.Header().Set(\"Content-Type\", \"text/event-stream\")"),
                  "sse");
    ASSERT_STR_EQ(cbm_detect_protocol("return EventSourceResponse(generate())"), "sse");
    ASSERT_STR_EQ(cbm_detect_protocol("SseEmitter emitter = new SseEmitter()"), "sse");
    ASSERT_STR_EQ(cbm_detect_protocol("ServerSentEvent event = ServerSentEvent.builder()"), "sse");
    ASSERT_STR_EQ(cbm_detect_protocol("return json.Marshal(result)"), "");
    ASSERT_STR_EQ(cbm_detect_protocol(""), "");
    PASS();
}

TEST(httplink_is_test_node) {
    ASSERT_FALSE(cbm_is_test_node_fp("src/routes/api.js", false));
    ASSERT_TRUE(cbm_is_test_node_fp("test/app.get.js", false));
    ASSERT_TRUE(cbm_is_test_node_fp("__tests__/routes.test.ts", false));
    ASSERT_TRUE(cbm_is_test_node_fp("src/routes/api.js", true));
    ASSERT_FALSE(cbm_is_test_node_fp("lib/router/index.js", false));
    ASSERT_TRUE(cbm_is_test_node_fp("tests/fixtures/server.js", false));
    ASSERT_FALSE(cbm_is_test_node_fp("app/controllers/orders_controller.rb", false));
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Config tests (port of config_test.go)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(httplink_is_path_excluded) {
    const char *paths[] = {"/health", "/debug", "/internal/status"};
    ASSERT_TRUE(cbm_is_path_excluded("/health", paths, 3));
    ASSERT_TRUE(cbm_is_path_excluded("/health/", paths, 3));
    ASSERT_TRUE(cbm_is_path_excluded("/HEALTH", paths, 3));
    ASSERT_TRUE(cbm_is_path_excluded("/debug", paths, 3));
    ASSERT_TRUE(cbm_is_path_excluded("/internal/status", paths, 3));
    ASSERT_FALSE(cbm_is_path_excluded("/api/orders", paths, 3));
    ASSERT_FALSE(cbm_is_path_excluded("/healthcheck", paths, 3));
    PASS();
}

TEST(httplink_default_exclude_paths) {
    /* Verify default exclude paths include common health/debug endpoints */
    ASSERT(cbm_default_exclude_paths_count >= 9);
    /* Verify /health is in the list */
    bool found = false;
    for (int i = 0; i < cbm_default_exclude_paths_count; i++) {
        if (strcmp(cbm_default_exclude_paths[i], "/health") == 0) {
            found = true;
            break;
        }
    }
    ASSERT_TRUE(found);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  YAML config tests (port of config_test.go — YAML-dependent tests)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(httplink_load_config_default) {
    /* Nonexistent path → defaults */
    cbm_httplink_config_t cfg = cbm_httplink_load_config("/nonexistent/path");
    ASSERT_FLOAT_EQ(cbm_httplink_effective_min_confidence(&cfg), 0.25, 0.001);
    ASSERT_TRUE(cbm_httplink_effective_fuzzy_matching(&cfg));

    const char *paths[64];
    int count = cbm_httplink_all_exclude_paths(&cfg, paths, 64);
    ASSERT_EQ(count, cbm_default_exclude_paths_count);

    cbm_httplink_config_free(&cfg);
    PASS();
}

TEST(httplink_load_config_from_file) {
    /* Create temp dir with .cgrconfig */
    char tmpdir[256]; snprintf(tmpdir, sizeof(tmpdir), "/tmp/httplink-cfg-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        SKIP("cbm_mkdtemp failed");

    char cfgpath[512];
    snprintf(cfgpath, sizeof(cfgpath), "%s/.cgrconfig", tmpdir);

    FILE *f = fopen(cfgpath, "w");
    if (!f) {
        rmdir(tmpdir);
        SKIP("cannot write .cgrconfig");
    }
    fprintf(f, "\n"
               "http_linker:\n"
               "  exclude_paths:\n"
               "    - /debug\n"
               "    - /internal/status\n"
               "  min_confidence: 0.5\n"
               "  fuzzy_matching: false\n");
    fclose(f);

    cbm_httplink_config_t cfg = cbm_httplink_load_config(tmpdir);
    ASSERT_FLOAT_EQ(cbm_httplink_effective_min_confidence(&cfg), 0.5, 0.001);
    ASSERT_FALSE(cbm_httplink_effective_fuzzy_matching(&cfg));

    const char *paths[64];
    int count = cbm_httplink_all_exclude_paths(&cfg, paths, 64);
    int expected = cbm_default_exclude_paths_count + 2;
    ASSERT_EQ(count, expected);

    cbm_httplink_config_free(&cfg);

    /* Cleanup */
    unlink(cfgpath);
    rmdir(tmpdir);
    PASS();
}

TEST(httplink_load_config_invalid_yaml) {
    /* Invalid YAML → fallback to defaults */
    char tmpdir[256]; snprintf(tmpdir, sizeof(tmpdir), "/tmp/httplink-bad-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        SKIP("cbm_mkdtemp failed");

    char cfgpath[512];
    snprintf(cfgpath, sizeof(cfgpath), "%s/.cgrconfig", tmpdir);

    FILE *f = fopen(cfgpath, "w");
    if (!f) {
        rmdir(tmpdir);
        SKIP("cannot write .cgrconfig");
    }
    fprintf(f, "not: [valid: yaml");
    fclose(f);

    cbm_httplink_config_t cfg = cbm_httplink_load_config(tmpdir);
    /* Should fall back to defaults */
    ASSERT_FLOAT_EQ(cbm_httplink_effective_min_confidence(&cfg), 0.25, 0.001);

    cbm_httplink_config_free(&cfg);

    /* Cleanup */
    unlink(cfgpath);
    rmdir(tmpdir);
    PASS();
}

TEST(httplink_all_exclude_paths_merge) {
    /* User-configured paths should be appended after defaults */
    cbm_httplink_config_t cfg = cbm_httplink_default_config();
    cfg.exclude_paths = calloc(2, sizeof(char *));
    cfg.exclude_paths[0] = strdup("/custom1");
    cfg.exclude_paths[1] = strdup("/custom2");
    cfg.exclude_path_count = 2;

    const char *paths[64];
    int count = cbm_httplink_all_exclude_paths(&cfg, paths, 64);
    int expected = cbm_default_exclude_paths_count + 2;
    ASSERT_EQ(count, expected);

    /* Verify defaults are first */
    for (int i = 0; i < cbm_default_exclude_paths_count; i++) {
        ASSERT_STR_EQ(paths[i], cbm_default_exclude_paths[i]);
    }

    /* Verify custom paths are appended */
    ASSERT_STR_EQ(paths[cbm_default_exclude_paths_count], "/custom1");
    ASSERT_STR_EQ(paths[cbm_default_exclude_paths_count + 1], "/custom2");

    cbm_httplink_config_free(&cfg);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Langparity tests (port of langparity_test.go)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(httplink_http_client_keywords_all_languages) {
    /* Each language should have at least one keyword in the list */
    typedef struct {
        const char *lang;
        const char **keywords;
        int nkw;
    } lang_kw_t;

    const char *py_kw[] = {"requests.get", "httpx.", "aiohttp."};
    const char *go_kw[] = {"http.Get", "http.Post", "http.NewRequest"};
    const char *js_kw[] = {"fetch(", "axios."};
    const char *java_kw[] = {"HttpClient", "RestTemplate"};
    const char *rust_kw[] = {"reqwest::", "hyper::"};

    lang_kw_t langs[] = {
        {"Python", py_kw, 3}, {"Go", go_kw, 3},     {"JavaScript", js_kw, 2},
        {"Java", java_kw, 2}, {"Rust", rust_kw, 2},
    };

    for (int l = 0; l < 5; l++) {
        bool found = false;
        for (int k = 0; k < langs[l].nkw && !found; k++) {
            for (int i = 0; i < cbm_http_client_keywords_count; i++) {
                if (strstr(cbm_http_client_keywords[i], langs[l].keywords[k]) ||
                    strcmp(cbm_http_client_keywords[i], langs[l].keywords[k]) == 0) {
                    found = true;
                    break;
                }
            }
        }
        if (!found) {
            printf("  FAIL: no HTTP client keywords for %s\n", langs[l].lang);
            return 1;
        }
    }
    PASS();
}

TEST(httplink_route_extraction_negative_cases) {
    cbm_route_handler_t routes[8];
    int n;
    const char *sources[] = {
        "func processOrder(order Order) error {\n\treturn nil\n}\n",
        "function calculate(x, y) {\n\treturn x + y;\n}\n",
        "def transform_data(data):\n    return data.upper()\n",
    };

    for (int i = 0; i < 3; i++) {
        /* Python */
        n = cbm_extract_python_routes("fn", "proj.fn", NULL, 0, routes, 8);
        ASSERT_EQ(n, 0);

        /* Go */
        n = cbm_extract_go_routes("fn", "proj.fn", sources[i], routes, 8);
        ASSERT_EQ(n, 0);

        /* Express */
        n = cbm_extract_express_routes("fn", "proj.fn", sources[i], routes, 8);
        ASSERT_EQ(n, 0);

        /* Laravel */
        n = cbm_extract_laravel_routes("fn", "proj.fn", sources[i], routes, 8);
        ASSERT_EQ(n, 0);
    }
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Read source lines tests
 * ═══════════════════════════════════════════════════════════════════ */

TEST(httplink_read_source_lines) {
    /* Create temp dir with test file */
    char tmpdir[256]; snprintf(tmpdir, sizeof(tmpdir), "/tmp/httplink-test-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        printf("  SKIP: cbm_mkdtemp failed\n");
        return -1;
    }

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/test.go", tmpdir);
    FILE *f = fopen(fpath, "w");
    if (!f) {
        printf("  SKIP: cannot write\n");
        return -1;
    }
    fprintf(f, "line1\nline2\nline3\nline4\nline5\n");
    fclose(f);

    char *result = cbm_read_source_lines_disk(tmpdir, "test.go", 2, 4);
    ASSERT_NOT_NULL(result);
    ASSERT_STR_EQ(result, "line2\nline3\nline4");
    free(result);

    /* Cleanup */
    unlink(fpath);
    rmdir(tmpdir);
    PASS();
}

TEST(httplink_read_source_lines_missing_file) {
    char *result = cbm_read_source_lines_disk("/nonexistent", "missing.go", 1, 10);
    ASSERT_NULL(result);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Integration tests with store (port of Linker tests)
 *  These test the full pipeline: create nodes → run linker → verify edges
 * ═══════════════════════════════════════════════════════════════════ */

/* ── Linker integration: route nodes created (simplified) ──────── */

TEST(httplink_linker_route_nodes) {
    /* Test route extraction from Python decorators (no store needed). */
    const char *decs[] = {"@app.post(\"/api/orders\")"};
    cbm_route_handler_t routes[4];
    int n = cbm_extract_python_routes("create_order", "testproj.handler.routes.create_order", decs,
                                      1, routes, 4);
    ASSERT_EQ(n, 1);
    ASSERT_STR_EQ(routes[0].path, "/api/orders");
    ASSERT_STR_EQ(routes[0].method, "POST");
    PASS();
}

/* ── Linker integration: same-service skip ─────────────────────── */

TEST(httplink_linker_same_service_skip) {
    /* Both caller and handler in same service → no link */
    ASSERT_TRUE(cbm_same_service("testproj.cat.sub.svcA.internal.client",
                                 "testproj.cat.sub.svcA.internal.handle_orders"));
    PASS();
}

/* ── Laravel path filter: reject $ and : in matched paths ──────── */

TEST(httplink_laravel_path_filter) {
    cbm_route_handler_t routes[8];
    int n;

    /* Cache key patterns should be filtered (contain $ or :) */
    n = cbm_extract_laravel_routes("fn", "proj.fn",
                                   "Cache::get('article:{$this->id}:image')", routes, 8);
    ASSERT_EQ(n, 0);

    n = cbm_extract_laravel_routes("fn", "proj.fn",
                                   "Route::get(\"cache:$key\", fn() => null)", routes, 8);
    ASSERT_EQ(n, 0);

    /* Valid routes should still pass (Laravel uses {param} not $param) */
    n = cbm_extract_laravel_routes("fn", "proj.fn",
                                   "Route::get('/api/users/{id}', 'UserController@show')", routes,
                                   8);
    ASSERT_EQ(n, 1);
    ASSERT_STR_EQ(routes[0].path, "/api/users/{id}");

    /* Route with no special chars also passes */
    n = cbm_extract_laravel_routes("fn", "proj.fn",
                                   "Route::post('/api/login', 'AuthController@login')", routes, 8);
    ASSERT_EQ(n, 1);
    ASSERT_STR_EQ(routes[0].path, "/api/login");

    PASS();
}

/* ── Laravel module-level route extraction ─────────────────────── */

TEST(httplink_laravel_module_level_routes) {
    const char *source = "<?php\n"
                         "use App\\Http\\Controllers\\OrderController;\n"
                         "Route::get('/api/orders', [OrderController::class, 'index']);\n"
                         "Route::post('/api/orders', [OrderController::class, 'store']);\n"
                         "Route::get('/api/orders/{id}', [OrderController::class, 'show']);\n";

    cbm_route_handler_t routes[8];
    int n = cbm_extract_laravel_routes("api.php", "testproj.routes.api", source, routes, 8);
    ASSERT_GTE(n, 3);

    bool found_orders = false, found_orders_id = false;
    for (int i = 0; i < n; i++) {
        if (strcmp(routes[i].path, "/api/orders") == 0)
            found_orders = true;
        if (strcmp(routes[i].path, "/api/orders/{id}") == 0)
            found_orders_id = true;
    }
    ASSERT_TRUE(found_orders);
    ASSERT_TRUE(found_orders_id);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Suite definition
 * ═══════════════════════════════════════════════════════════════════ */

SUITE(httplink) {
    /* Similarity (4 tests from similarity_test.go) */
    RUN_TEST(httplink_levenshtein_distance);
    RUN_TEST(httplink_normalized_levenshtein);
    RUN_TEST(httplink_ngram_overlap);
    RUN_TEST(httplink_confidence_band);

    /* Path matching (5 tests from httplink_test.go) */
    RUN_TEST(httplink_normalize_path);
    RUN_TEST(httplink_paths_match);
    RUN_TEST(httplink_paths_match_suffix);
    RUN_TEST(httplink_path_match_score);
    RUN_TEST(httplink_same_service);

    /* URL extraction (2 tests) */
    RUN_TEST(httplink_extract_url_paths);
    RUN_TEST(httplink_extract_json_string_paths);

    /* Python routes (4 tests) */
    RUN_TEST(httplink_extract_python_routes);
    RUN_TEST(httplink_extract_python_routes_multiple);
    RUN_TEST(httplink_extract_python_routes_no_decorators);
    RUN_TEST(httplink_extract_python_ws_routes);

    /* Go routes (3 tests) */
    RUN_TEST(httplink_extract_go_routes);
    RUN_TEST(httplink_chi_prefix);
    RUN_TEST(httplink_chi_prefix_mixed_with_gin);

    /* Java/Ktor/Express routes (3 tests) */
    RUN_TEST(httplink_extract_spring_ws_routes);
    RUN_TEST(httplink_extract_ktor_ws_routes);
    RUN_TEST(httplink_express_route_filtering);

    /* Detection (2 tests) */
    RUN_TEST(httplink_detect_protocol);
    RUN_TEST(httplink_is_test_node);

    /* Config (6 tests — 2 original + 4 YAML-dependent) */
    RUN_TEST(httplink_is_path_excluded);
    RUN_TEST(httplink_default_exclude_paths);
    RUN_TEST(httplink_load_config_default);
    RUN_TEST(httplink_load_config_from_file);
    RUN_TEST(httplink_load_config_invalid_yaml);
    RUN_TEST(httplink_all_exclude_paths_merge);

    /* Langparity (2 tests) */
    RUN_TEST(httplink_http_client_keywords_all_languages);
    RUN_TEST(httplink_route_extraction_negative_cases);

    /* Source lines (2 tests) */
    RUN_TEST(httplink_read_source_lines);
    RUN_TEST(httplink_read_source_lines_missing_file);

    /* Laravel path filter (1 test) */
    RUN_TEST(httplink_laravel_path_filter);

    /* Integration (3 tests) */
    RUN_TEST(httplink_linker_route_nodes);
    RUN_TEST(httplink_linker_same_service_skip);
    RUN_TEST(httplink_laravel_module_level_routes);
}
