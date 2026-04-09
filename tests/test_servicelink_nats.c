/*
 * test_servicelink_nats.c — Tests for NATS protocol linking.
 *
 * Creates synthetic source files (.go, .py, .js, .ts, .rs),
 * builds a graph buffer with nodes, runs the NATS linker, and verifies
 * that NATS_CALLS edges are created with correct properties.
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include <pipeline/servicelink.h>
/* httplink.h removed — functions now in servicelink.h */
#include <store/store.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>
#include "graph_buffer/graph_buffer.h"
#include <stdatomic.h>

/* ── Helpers ─────────────────────────────────────────────────────── */

/* Recursive remove */
static void rm_rf(const char *path) {
    char cmd[1024];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", path);
    (void)system(cmd);
}

/* Write a synthetic file at repo_path/rel_path with given content */
static void write_file(const char *repo_path, const char *rel_path, const char *content) {
    char full_path[1024];
    snprintf(full_path, sizeof(full_path), "%s/%s", repo_path, rel_path);

    /* Create parent directories */
    char dir[1024];
    snprintf(dir, sizeof(dir), "%s", full_path);
    char *last_slash = strrchr(dir, '/');
    if (last_slash) {
        *last_slash = '\0';
        char mkdir_cmd[1080];
        snprintf(mkdir_cmd, sizeof(mkdir_cmd), "mkdir -p '%s'", dir);
        (void)system(mkdir_cmd);
    }

    FILE *f = fopen(full_path, "w");
    if (f) {
        fputs(content, f);
        fclose(f);
    }
}

/* Create a pipeline context for testing */
static cbm_pipeline_ctx_t make_ctx(cbm_gbuf_t *gb, const char *repo_path) {
    static atomic_int cancelled;
    atomic_init(&cancelled, 0);
    cbm_pipeline_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.project_name = "test";
    ctx.repo_path = repo_path;
    ctx.gbuf = gb;
    ctx.cancelled = &cancelled;
    return ctx;
}

/* Count NATS_CALLS edges */
static int count_nats_edges(cbm_gbuf_t *gb) {
    return cbm_gbuf_edge_count_by_type(gb, "NATS_CALLS");
}

/* Check if a NATS_CALLS edge has given identifier */
static bool has_nats_edge_with_identifier(cbm_gbuf_t *gb, const char *identifier) {
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_type(gb, "NATS_CALLS", &edges, &count);
    char needle[256];
    snprintf(needle, sizeof(needle), "\"identifier\":\"%s\"", identifier);
    for (int i = 0; i < count; i++) {
        if (edges[i]->properties_json && strstr(edges[i]->properties_json, needle))
            return true;
    }
    return false;
}

/* Check if a NATS_CALLS edge has given confidence band */
static bool has_nats_edge_with_band(cbm_gbuf_t *gb, const char *band) {
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_type(gb, "NATS_CALLS", &edges, &count);
    char needle[64];
    snprintf(needle, sizeof(needle), "\"confidence_band\":\"%s\"", band);
    for (int i = 0; i < count; i++) {
        if (edges[i]->properties_json && strstr(edges[i]->properties_json, needle))
            return true;
    }
    return false;
}

/* ── External: nats_subject_match declared in servicelink_nats.c ── */
extern int nats_subject_match(const char *pattern, const char *subject);

/* ═══════════════════════════════════════════════════════════════════
 *  Test 1: Go nc.Publish + nc.Subscribe → edge (exact match)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(test_nats_go_publish_subscribe) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_nats_t1_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Go publisher */
    const char *pub_src =
        "package main\n"
        "\n"
        "func publishOrder() {\n"
        "    nc.Publish(\"orders.new\", data)\n"
        "}\n";

    write_file(tmpdir, "publisher/main.go", pub_src);

    /* Go subscriber */
    const char *sub_src =
        "package main\n"
        "\n"
        "func subscribeOrders() {\n"
        "    nc.Subscribe(\"orders.new\", func(msg *nats.Msg) {})\n"
        "}\n";

    write_file(tmpdir, "consumer/main.go", sub_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Function", "publishOrder",
        "test.publisher.main.publishOrder",
        "publisher/main.go", 3, 5, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t sub_id = cbm_gbuf_upsert_node(gb, "Function", "subscribeOrders",
        "test.consumer.main.subscribeOrders",
        "consumer/main.go", 3, 5, NULL);
    ASSERT_GT(sub_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_nats(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_nats_edges(gb), 0);
    ASSERT_TRUE(has_nats_edge_with_identifier(gb, "orders.new"));
    ASSERT_TRUE(has_nats_edge_with_band(gb, "high"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 2: Python nc.publish + nc.subscribe → edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(test_nats_python_publish_subscribe) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_nats_t2_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Python publisher */
    const char *pub_src =
        "import nats\n"
        "\n"
        "async def publish_event():\n"
        "    await nc.publish('events.user.created', b'data')\n";

    write_file(tmpdir, "publisher/pub.py", pub_src);

    /* Python subscriber */
    const char *sub_src =
        "import nats\n"
        "\n"
        "async def subscribe_events():\n"
        "    await nc.subscribe('events.user.created', cb=handler)\n";

    write_file(tmpdir, "consumer/sub.py", sub_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Function", "publish_event",
        "test.publisher.pub.publish_event",
        "publisher/pub.py", 3, 4, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t sub_id = cbm_gbuf_upsert_node(gb, "Function", "subscribe_events",
        "test.consumer.sub.subscribe_events",
        "consumer/sub.py", 3, 4, NULL);
    ASSERT_GT(sub_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_nats(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_nats_edges(gb), 0);
    ASSERT_TRUE(has_nats_edge_with_identifier(gb, "events.user.created"));
    ASSERT_TRUE(has_nats_edge_with_band(gb, "high"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 3: Node.js nc.publish + nc.subscribe → edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(test_nats_node_publish_subscribe) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_nats_t3_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Node.js publisher */
    const char *pub_src =
        "async function publishMetric() {\n"
        "  nc.publish('metrics.cpu', sc.encode('data'));\n"
        "}\n";

    write_file(tmpdir, "publisher/pub.ts", pub_src);

    /* Node.js subscriber */
    const char *sub_src =
        "async function subscribeMetrics() {\n"
        "  const sub = nc.subscribe('metrics.cpu');\n"
        "}\n";

    write_file(tmpdir, "consumer/sub.ts", sub_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Function", "publishMetric",
        "test.publisher.pub.publishMetric",
        "publisher/pub.ts", 1, 3, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t sub_id = cbm_gbuf_upsert_node(gb, "Function", "subscribeMetrics",
        "test.consumer.sub.subscribeMetrics",
        "consumer/sub.ts", 1, 3, NULL);
    ASSERT_GT(sub_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_nats(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_nats_edges(gb), 0);
    ASSERT_TRUE(has_nats_edge_with_identifier(gb, "metrics.cpu"));
    ASSERT_TRUE(has_nats_edge_with_band(gb, "high"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 4: Wildcard * matches exactly one token
 * ═══════════════════════════════════════════════════════════════════ */

TEST(test_nats_wildcard_star) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_nats_t4_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Publisher publishes to "orders.us" */
    const char *pub_src =
        "package main\n"
        "\n"
        "func publishOrder() {\n"
        "    nc.Publish(\"orders.us\", data)\n"
        "}\n";

    write_file(tmpdir, "publisher/main.go", pub_src);

    /* Subscriber subscribes to "orders.*" (wildcard: one token) */
    const char *sub_src =
        "package main\n"
        "\n"
        "func subscribeOrders() {\n"
        "    nc.Subscribe(\"orders.*\", func(msg *nats.Msg) {})\n"
        "}\n";

    write_file(tmpdir, "consumer/main.go", sub_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Function", "publishOrder",
        "test.publisher.main.publishOrder",
        "publisher/main.go", 3, 5, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t sub_id = cbm_gbuf_upsert_node(gb, "Function", "subscribeOrders",
        "test.consumer.main.subscribeOrders",
        "consumer/main.go", 3, 5, NULL);
    ASSERT_GT(sub_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_nats(&ctx);

    /* orders.* should match orders.us */
    ASSERT_GT(links, 0);
    ASSERT_GT(count_nats_edges(gb), 0);
    ASSERT_TRUE(has_nats_edge_with_identifier(gb, "orders.*"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 5: Wildcard > matches one or more trailing tokens
 * ═══════════════════════════════════════════════════════════════════ */

TEST(test_nats_wildcard_gt) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_nats_t5_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Publisher publishes to "events.user.created" */
    const char *pub_src =
        "package main\n"
        "\n"
        "func publishEvent() {\n"
        "    nc.Publish(\"events.user.created\", data)\n"
        "}\n";

    write_file(tmpdir, "publisher/main.go", pub_src);

    /* Subscriber subscribes to "events.>" (wildcard: 1+ trailing tokens) */
    const char *sub_src =
        "package main\n"
        "\n"
        "func subscribeEvents() {\n"
        "    nc.Subscribe(\"events.>\", func(msg *nats.Msg) {})\n"
        "}\n";

    write_file(tmpdir, "consumer/main.go", sub_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Function", "publishEvent",
        "test.publisher.main.publishEvent",
        "publisher/main.go", 3, 5, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t sub_id = cbm_gbuf_upsert_node(gb, "Function", "subscribeEvents",
        "test.consumer.main.subscribeEvents",
        "consumer/main.go", 3, 5, NULL);
    ASSERT_GT(sub_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_nats(&ctx);

    /* events.> should match events.user.created */
    ASSERT_GT(links, 0);
    ASSERT_GT(count_nats_edges(gb), 0);
    ASSERT_TRUE(has_nats_edge_with_identifier(gb, "events.>"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 6: Request-Reply — nc.Request creates a consumer edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(test_nats_request_reply) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_nats_t6_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Service that subscribes (responder) */
    const char *responder_src =
        "package main\n"
        "\n"
        "func handleRequest() {\n"
        "    nc.Subscribe(\"api.greet\", func(msg *nats.Msg) {\n"
        "        msg.Respond([]byte(\"hello\"))\n"
        "    })\n"
        "}\n";

    write_file(tmpdir, "responder/main.go", responder_src);

    /* Client that requests (caller) */
    const char *caller_src =
        "package main\n"
        "\n"
        "func callGreet() {\n"
        "    resp, _ := nc.Request(\"api.greet\", []byte(\"world\"))\n"
        "}\n";

    write_file(tmpdir, "caller/main.go", caller_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t responder_id = cbm_gbuf_upsert_node(gb, "Function", "handleRequest",
        "test.responder.main.handleRequest",
        "responder/main.go", 3, 7, NULL);
    ASSERT_GT(responder_id, 0);

    int64_t caller_id = cbm_gbuf_upsert_node(gb, "Function", "callGreet",
        "test.caller.main.callGreet",
        "caller/main.go", 3, 5, NULL);
    ASSERT_GT(caller_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_nats(&ctx);

    /* nc.Request is treated as consumer — should match the subscriber's Publish */
    /* Both subscribe to "api.greet", so the responder (subscriber) and caller
     * (request) should not self-link but should create cross-edges.
     * The Request caller becomes a consumer matched against the responder who
     * is also a consumer — but both match same subject from different nodes. */
    /* Actually: the responder is a subscriber (consumer), the caller is a
     * request (consumer). We need a publisher for edges. But Request is
     * treated as a consumer that calls a subject. The subscriber is also
     * a consumer. For a link to form, we need a pub-sub pair.
     * Let's add a publisher to the responder side to make this test meaningful. */

    /* The test verifies that Request creates a consumer entry.
     * Since both are consumers and neither is a publisher, there should be
     * no edges. But let's verify the Request pattern is detected by adding
     * a publisher node. */

    /* Actually, re-reading the spec: Request is treated as consumer (caller).
     * Subscribe is treated as consumer. For an edge, we need pub+sub.
     * Let's verify by checking that the nats link count reflects the
     * actual pub/sub matching. */
    /* No publisher node exists → no edges expected from consumer-consumer.
     * But the test is about verifying Request is detected. Let me restructure. */

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);

    /* Restructured test: publisher + Request consumer */
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_nats_t6b_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Publisher that publishes to "api.greet" */
    const char *pub_src =
        "package main\n"
        "\n"
        "func publishGreet() {\n"
        "    nc.Publish(\"api.greet\", data)\n"
        "}\n";

    write_file(tmpdir, "publisher/main.go", pub_src);

    /* Client that uses Request (treated as consumer) */
    const char *req_src =
        "package main\n"
        "\n"
        "func callGreet() {\n"
        "    resp, _ := nc.Request(\"api.greet\", []byte(\"world\"))\n"
        "}\n";

    write_file(tmpdir, "caller/main.go", req_src);

    gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Function", "publishGreet",
        "test.publisher.main.publishGreet",
        "publisher/main.go", 3, 5, NULL);
    ASSERT_GT(pub_id, 0);

    caller_id = cbm_gbuf_upsert_node(gb, "Function", "callGreet",
        "test.caller.main.callGreet",
        "caller/main.go", 3, 5, NULL);
    ASSERT_GT(caller_id, 0);

    ctx = make_ctx(gb, tmpdir);
    links = cbm_servicelink_nats(&ctx);

    /* Request("api.greet") consumer should match Publish("api.greet") producer */
    ASSERT_GT(links, 0);
    ASSERT_GT(count_nats_edges(gb), 0);
    ASSERT_TRUE(has_nats_edge_with_identifier(gb, "api.greet"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 7: No match — different subjects produce no edges
 * ═══════════════════════════════════════════════════════════════════ */

TEST(test_nats_no_match) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_nats_t7_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Publisher to "orders.new" */
    const char *pub_src =
        "package main\n"
        "\n"
        "func publishOrder() {\n"
        "    nc.Publish(\"orders.new\", data)\n"
        "}\n";

    write_file(tmpdir, "publisher/main.go", pub_src);

    /* Subscriber to "payments.processed" */
    const char *sub_src =
        "package main\n"
        "\n"
        "func subscribePayments() {\n"
        "    nc.Subscribe(\"payments.processed\", func(msg *nats.Msg) {})\n"
        "}\n";

    write_file(tmpdir, "consumer/main.go", sub_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    cbm_gbuf_upsert_node(gb, "Function", "publishOrder",
        "test.publisher.main.publishOrder",
        "publisher/main.go", 3, 5, NULL);

    cbm_gbuf_upsert_node(gb, "Function", "subscribePayments",
        "test.consumer.main.subscribePayments",
        "consumer/main.go", 3, 5, NULL);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_nats(&ctx);

    /* "orders.new" should NOT match "payments.processed" */
    ASSERT_EQ(links, 0);
    ASSERT_EQ(count_nats_edges(gb), 0);

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 8: Unit tests for nats_subject_match() function
 * ═══════════════════════════════════════════════════════════════════ */

TEST(test_nats_subject_match_unit) {
    /* Exact match */
    ASSERT_EQ(nats_subject_match("order.created", "order.created"), 1);

    /* * matches exactly one token */
    ASSERT_EQ(nats_subject_match("order.*", "order.created"), 1);
    ASSERT_EQ(nats_subject_match("*.created", "order.created"), 1);

    /* * does NOT match zero or multiple tokens */
    ASSERT_EQ(nats_subject_match("order.*", "order.created.us"), 0);
    ASSERT_EQ(nats_subject_match("order.*", "order"), 0);

    /* > matches one or more trailing tokens */
    ASSERT_EQ(nats_subject_match("order.>", "order.created"), 1);
    ASSERT_EQ(nats_subject_match("order.>", "order.created.us"), 1);
    ASSERT_EQ(nats_subject_match("order.>", "order.created.us.east"), 1);

    /* > does NOT match zero tokens (key difference from AMQP #) */
    ASSERT_EQ(nats_subject_match("order.>", "order"), 0);

    /* > must be last token — only works at end */
    ASSERT_EQ(nats_subject_match(">.order", "something.order"), 0);

    /* Combined wildcards */
    ASSERT_EQ(nats_subject_match("*.*.us", "order.created.us"), 1);
    ASSERT_EQ(nats_subject_match("*.*.us", "order.created.eu"), 0);

    /* No match */
    ASSERT_EQ(nats_subject_match("order.created", "order.updated"), 0);
    ASSERT_EQ(nats_subject_match("order.created", "payment.created"), 0);

    /* NULL handling */
    ASSERT_EQ(nats_subject_match(NULL, "order"), 0);
    ASSERT_EQ(nats_subject_match("order", NULL), 0);

    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test: Class node with NATS publisher → detected
 * ═══════════════════════════════════════════════════════════════════ */

TEST(nats_class_node_publisher) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_nats_cls_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    const char *pub_src =
        "class OrderPublisher {\n"
        "  async publish(order) {\n"
        "    nc.publish('orders.created', JSON.stringify(order));\n"
        "  }\n"
        "}\n";
    write_file(tmpdir, "publishers/order.ts", pub_src);

    const char *sub_src =
        "function handleOrders() {\n"
        "  nc.subscribe('orders.created', (msg) => {});\n"
        "}\n";
    write_file(tmpdir, "handlers/order.ts", sub_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);
    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Class", "OrderPublisher",
        "test.publishers.order.OrderPublisher", "publishers/order.ts", 1, 5, NULL);
    ASSERT_GT(pub_id, 0);
    int64_t sub_id = cbm_gbuf_upsert_node(gb, "Function", "handleOrders",
        "test.handlers.order.handleOrders", "handlers/order.ts", 1, 3, NULL);
    ASSERT_GT(sub_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_nats(&ctx);
    ASSERT_GT(links, 0);
    ASSERT_GT(cbm_gbuf_edge_count_by_type(gb, "NATS_CALLS"), 0);

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Suite definition
 * ═══════════════════════════════════════════════════════════════════ */

SUITE(servicelink_nats) {
    RUN_TEST(test_nats_go_publish_subscribe);
    RUN_TEST(test_nats_python_publish_subscribe);
    RUN_TEST(test_nats_node_publish_subscribe);
    RUN_TEST(test_nats_wildcard_star);
    RUN_TEST(test_nats_wildcard_gt);
    RUN_TEST(test_nats_request_reply);
    RUN_TEST(test_nats_no_match);
    RUN_TEST(test_nats_subject_match_unit);
    RUN_TEST(nats_class_node_publisher);
}
