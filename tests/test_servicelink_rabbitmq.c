/*
 * test_servicelink_rabbitmq.c — Tests for RabbitMQ/AMQP protocol linking.
 *
 * Creates synthetic source files (.py, .go, .java, .js, .ts, .rs),
 * builds a graph buffer with nodes, runs the RabbitMQ linker, and verifies
 * that AMQP_CALLS edges are created with correct properties.
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

/* Count AMQP_CALLS edges */
static int count_amqp_edges(cbm_gbuf_t *gb) {
    return cbm_gbuf_edge_count_by_type(gb, "AMQP_CALLS");
}

/* Check if an AMQP_CALLS edge has given confidence band */
static bool has_amqp_edge_with_band(cbm_gbuf_t *gb, const char *band) {
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_type(gb, "AMQP_CALLS", &edges, &count);
    char needle[64];
    snprintf(needle, sizeof(needle), "\"confidence_band\":\"%s\"", band);
    for (int i = 0; i < count; i++) {
        if (edges[i]->properties_json && strstr(edges[i]->properties_json, needle))
            return true;
    }
    return false;
}

/* Check if an AMQP_CALLS edge has given identifier */
static bool has_amqp_edge_with_identifier(cbm_gbuf_t *gb, const char *identifier) {
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_type(gb, "AMQP_CALLS", &edges, &count);
    char needle[256];
    snprintf(needle, sizeof(needle), "\"identifier\":\"%s\"", identifier);
    for (int i = 0; i < count; i++) {
        if (edges[i]->properties_json && strstr(edges[i]->properties_json, needle))
            return true;
    }
    return false;
}

/* Check if an AMQP_CALLS edge has given exchange in extra JSON */
static bool has_amqp_edge_with_exchange(cbm_gbuf_t *gb, const char *exchange) {
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_type(gb, "AMQP_CALLS", &edges, &count);
    char needle[256];
    snprintf(needle, sizeof(needle), "\"exchange\":\"%s\"", exchange);
    for (int i = 0; i < count; i++) {
        if (edges[i]->properties_json && strstr(edges[i]->properties_json, needle))
            return true;
    }
    return false;
}

/* ── External: amqp_topic_match declared in servicelink_rabbitmq.c ── */
extern int amqp_topic_match(const char *pattern, const char *subject);

/* ═══════════════════════════════════════════════════════════════════
 *  Test 1: Python basic_publish + basic_consume → edge (direct exchange)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(rabbitmq_python_direct) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_rmq_t1_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Python publisher: default exchange, routing_key = queue name */
    const char *pub_src =
        "import pika\n"
        "\n"
        "def send_order():\n"
        "    channel.basic_publish(exchange='', routing_key='order_queue',\n"
        "                         body='order data')\n";

    write_file(tmpdir, "publisher/send.py", pub_src);

    /* Python consumer */
    const char *sub_src =
        "import pika\n"
        "\n"
        "def handle_order():\n"
        "    channel.basic_consume(queue='order_queue',\n"
        "                         on_message_callback=callback)\n";

    write_file(tmpdir, "consumer/recv.py", sub_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Function", "send_order",
        "test.publisher.send.send_order",
        "publisher/send.py", 3, 5, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t sub_id = cbm_gbuf_upsert_node(gb, "Function", "handle_order",
        "test.consumer.recv.handle_order",
        "consumer/recv.py", 3, 5, NULL);
    ASSERT_GT(sub_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_rabbitmq(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_amqp_edges(gb), 0);
    ASSERT_TRUE(has_amqp_edge_with_band(gb, "high"));
    ASSERT_TRUE(has_amqp_edge_with_identifier(gb, "order_queue"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 2: Java @RabbitListener + rabbitTemplate.convertAndSend → edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(rabbitmq_java_template) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_rmq_t2_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Java publisher */
    const char *pub_src =
        "public class OrderPublisher {\n"
        "    public void publish() {\n"
        "        rabbitTemplate.convertAndSend(\"order-exchange\", \"order.created\", msg);\n"
        "    }\n"
        "}\n";

    write_file(tmpdir, "src/main/java/OrderPublisher.java", pub_src);

    /* Java consumer */
    const char *sub_src =
        "public class OrderConsumer {\n"
        "    @RabbitListener(queues = \"order.created\")\n"
        "    public void handle(String msg) {\n"
        "        System.out.println(msg);\n"
        "    }\n"
        "}\n";

    write_file(tmpdir, "src/main/java/OrderConsumer.java", sub_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Method", "publish",
        "test.OrderPublisher.publish",
        "src/main/java/OrderPublisher.java", 2, 4, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t sub_id = cbm_gbuf_upsert_node(gb, "Method", "handle",
        "test.OrderConsumer.handle",
        "src/main/java/OrderConsumer.java", 2, 5, NULL);
    ASSERT_GT(sub_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_rabbitmq(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_amqp_edges(gb), 0);
    ASSERT_TRUE(has_amqp_edge_with_identifier(gb, "order.created"));
    ASSERT_TRUE(has_amqp_edge_with_exchange(gb, "order-exchange"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 3: Go ch.Publish + ch.Consume → edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(rabbitmq_go_publish_consume) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_rmq_t3_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Go publisher */
    const char *pub_src =
        "package main\n"
        "\n"
        "func publishEvent() {\n"
        "    ch.Publish(\"events\", \"event.new\", false, false, amqp.Publishing{Body: body})\n"
        "}\n";

    write_file(tmpdir, "publisher/main.go", pub_src);

    /* Go consumer */
    const char *sub_src =
        "package main\n"
        "\n"
        "func consumeEvents() {\n"
        "    msgs, _ := ch.Consume(\"event.new\", \"\", true, false, false, false, nil)\n"
        "}\n";

    write_file(tmpdir, "consumer/main.go", sub_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Function", "publishEvent",
        "test.publisher.main.publishEvent",
        "publisher/main.go", 3, 5, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t sub_id = cbm_gbuf_upsert_node(gb, "Function", "consumeEvents",
        "test.consumer.main.consumeEvents",
        "consumer/main.go", 3, 5, NULL);
    ASSERT_GT(sub_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_rabbitmq(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_amqp_edges(gb), 0);
    ASSERT_TRUE(has_amqp_edge_with_identifier(gb, "event.new"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 4: Node.js channel.publish + channel.consume → edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(rabbitmq_nodejs_publish_consume) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_rmq_t4_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Node.js publisher */
    const char *pub_src =
        "async function publishNotification() {\n"
        "  channel.publish('notifications', 'notify.email', Buffer.from('hello'));\n"
        "}\n";

    write_file(tmpdir, "publisher/notify.js", pub_src);

    /* Node.js consumer */
    const char *sub_src =
        "async function consumeNotifications() {\n"
        "  channel.consume('notify.email', (msg) => {\n"
        "    console.log(msg.content.toString());\n"
        "  });\n"
        "}\n";

    write_file(tmpdir, "consumer/handler.js", sub_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Function", "publishNotification",
        "test.publisher.notify.publishNotification",
        "publisher/notify.js", 1, 3, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t sub_id = cbm_gbuf_upsert_node(gb, "Function", "consumeNotifications",
        "test.consumer.handler.consumeNotifications",
        "consumer/handler.js", 1, 5, NULL);
    ASSERT_GT(sub_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_rabbitmq(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_amqp_edges(gb), 0);
    ASSERT_TRUE(has_amqp_edge_with_identifier(gb, "notify.email"));
    ASSERT_TRUE(has_amqp_edge_with_exchange(gb, "notifications"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 5: AMQP topic wildcard: order.* matches order.created → edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(rabbitmq_topic_star_match) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_rmq_t5_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Publisher with topic wildcard pattern in routing_key */
    const char *pub_src =
        "package main\n"
        "\n"
        "func publishOrder() {\n"
        "    ch.Publish(\"topic-exchange\", \"order.*\", false, false, amqp.Publishing{Body: body})\n"
        "}\n";

    write_file(tmpdir, "publisher/main.go", pub_src);

    /* Consumer listening on queue named "order.created" */
    const char *sub_src =
        "package main\n"
        "\n"
        "func consumeOrders() {\n"
        "    msgs, _ := ch.Consume(\"order.created\", \"\", true, false, false, false, nil)\n"
        "}\n";

    write_file(tmpdir, "consumer/main.go", sub_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Function", "publishOrder",
        "test.publisher.main.publishOrder",
        "publisher/main.go", 3, 5, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t sub_id = cbm_gbuf_upsert_node(gb, "Function", "consumeOrders",
        "test.consumer.main.consumeOrders",
        "consumer/main.go", 3, 5, NULL);
    ASSERT_GT(sub_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_rabbitmq(&ctx);

    /* order.* should match order.created → topic match */
    ASSERT_GT(links, 0);
    ASSERT_GT(count_amqp_edges(gb), 0);
    ASSERT_TRUE(has_amqp_edge_with_identifier(gb, "order.created"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 6: AMQP topic wildcard: order.# matches order.created.us → edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(rabbitmq_topic_hash_match) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_rmq_t6_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Publisher with # wildcard */
    const char *pub_src =
        "async function publishOrder() {\n"
        "  channel.publish('topic-exchange', 'order.#', Buffer.from('data'));\n"
        "}\n";

    write_file(tmpdir, "publisher/pub.js", pub_src);

    /* Consumer for order.created.us */
    const char *sub_src =
        "async function consumeOrders() {\n"
        "  channel.consume('order.created.us', (msg) => {});\n"
        "}\n";

    write_file(tmpdir, "consumer/sub.js", sub_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Function", "publishOrder",
        "test.publisher.pub.publishOrder",
        "publisher/pub.js", 1, 3, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t sub_id = cbm_gbuf_upsert_node(gb, "Function", "consumeOrders",
        "test.consumer.sub.consumeOrders",
        "consumer/sub.js", 1, 3, NULL);
    ASSERT_GT(sub_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_rabbitmq(&ctx);

    /* order.# should match order.created.us */
    ASSERT_GT(links, 0);
    ASSERT_GT(count_amqp_edges(gb), 0);
    ASSERT_TRUE(has_amqp_edge_with_identifier(gb, "order.created.us"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 7: AMQP topic wildcard: order.* does NOT match order.created.us
 * ═══════════════════════════════════════════════════════════════════ */

TEST(rabbitmq_topic_star_no_multi_segment) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_rmq_t7_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Publisher with * wildcard (matches one word only) */
    const char *pub_src =
        "package main\n"
        "\n"
        "func publishOrder() {\n"
        "    ch.Publish(\"topic-exchange\", \"order.*\", false, false, amqp.Publishing{Body: body})\n"
        "}\n";

    write_file(tmpdir, "publisher/main.go", pub_src);

    /* Consumer for multi-segment queue name */
    const char *sub_src =
        "package main\n"
        "\n"
        "func consumeOrders() {\n"
        "    msgs, _ := ch.Consume(\"order.created.us\", \"\", true, false, false, false, nil)\n"
        "}\n";

    write_file(tmpdir, "consumer/main.go", sub_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Function", "publishOrder",
        "test.publisher.main.publishOrder",
        "publisher/main.go", 3, 5, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t sub_id = cbm_gbuf_upsert_node(gb, "Function", "consumeOrders",
        "test.consumer.main.consumeOrders",
        "consumer/main.go", 3, 5, NULL);
    ASSERT_GT(sub_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_rabbitmq(&ctx);

    /* order.* should NOT match order.created.us (3 segments vs pattern expects 2) */
    ASSERT_EQ(links, 0);
    ASSERT_EQ(count_amqp_edges(gb), 0);

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 8: Fanout exchange — all consumers match
 * ═══════════════════════════════════════════════════════════════════ */

TEST(rabbitmq_fanout_all_consumers) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_rmq_t8_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Publisher to fanout exchange */
    const char *pub_src =
        "import pika\n"
        "\n"
        "def broadcast():\n"
        "    channel.basic_publish(exchange='logs-fanout', routing_key='ignored',\n"
        "                         body='broadcast msg')\n";

    write_file(tmpdir, "publisher/broadcast.py", pub_src);

    /* Consumer A */
    const char *sub_a_src =
        "import pika\n"
        "\n"
        "def consumer_a():\n"
        "    channel.basic_consume(queue='queue_a',\n"
        "                         on_message_callback=cb)\n";

    write_file(tmpdir, "consumer/a.py", sub_a_src);

    /* Consumer B */
    const char *sub_b_src =
        "import pika\n"
        "\n"
        "def consumer_b():\n"
        "    channel.basic_consume(queue='queue_b',\n"
        "                         on_message_callback=cb)\n";

    write_file(tmpdir, "consumer/b.py", sub_b_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Function", "broadcast",
        "test.publisher.broadcast.broadcast",
        "publisher/broadcast.py", 3, 5, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t sub_a_id = cbm_gbuf_upsert_node(gb, "Function", "consumer_a",
        "test.consumer.a.consumer_a",
        "consumer/a.py", 3, 5, NULL);
    ASSERT_GT(sub_a_id, 0);

    int64_t sub_b_id = cbm_gbuf_upsert_node(gb, "Function", "consumer_b",
        "test.consumer.b.consumer_b",
        "consumer/b.py", 3, 5, NULL);
    ASSERT_GT(sub_b_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_rabbitmq(&ctx);

    /* Fanout: both consumers should receive edges */
    ASSERT_EQ(links, 2);
    ASSERT_EQ(count_amqp_edges(gb), 2);

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 9: Default exchange (routing_key = queue name) → exact match
 * ═══════════════════════════════════════════════════════════════════ */

TEST(rabbitmq_default_exchange) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_rmq_t9_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Node.js publisher using sendToQueue (default exchange) */
    const char *pub_src =
        "async function sendTask() {\n"
        "  channel.sendToQueue('task_queue', Buffer.from('work'));\n"
        "}\n";

    write_file(tmpdir, "publisher/send.ts", pub_src);

    /* Node.js consumer */
    const char *sub_src =
        "async function processTask() {\n"
        "  channel.consume('task_queue', (msg) => {\n"
        "    console.log(msg.content.toString());\n"
        "  });\n"
        "}\n";

    write_file(tmpdir, "consumer/process.ts", sub_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Function", "sendTask",
        "test.publisher.send.sendTask",
        "publisher/send.ts", 1, 3, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t sub_id = cbm_gbuf_upsert_node(gb, "Function", "processTask",
        "test.consumer.process.processTask",
        "consumer/process.ts", 1, 5, NULL);
    ASSERT_GT(sub_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_rabbitmq(&ctx);

    /* Default exchange: routing_key "task_queue" = queue name "task_queue" */
    ASSERT_GT(links, 0);
    ASSERT_GT(count_amqp_edges(gb), 0);
    ASSERT_TRUE(has_amqp_edge_with_identifier(gb, "task_queue"));
    ASSERT_TRUE(has_amqp_edge_with_band(gb, "high"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 10: Self-link prevention (same node publishes and consumes)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(rabbitmq_no_self_link) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_rmq_t10_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Single function that both publishes and consumes */
    const char *src =
        "import pika\n"
        "\n"
        "def relay():\n"
        "    channel.basic_publish(exchange='', routing_key='relay_queue',\n"
        "                         body='data')\n"
        "    channel.basic_consume(queue='relay_queue',\n"
        "                         on_message_callback=cb)\n";

    write_file(tmpdir, "relay.py", src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t id = cbm_gbuf_upsert_node(gb, "Function", "relay",
        "test.relay.relay", "relay.py", 3, 7, NULL);
    ASSERT_GT(id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_rabbitmq(&ctx);

    /* Same node: should NOT create self-link */
    ASSERT_EQ(links, 0);
    ASSERT_EQ(count_amqp_edges(gb), 0);

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 11: No match (different queues, no binding)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(rabbitmq_no_match_different_queues) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_rmq_t11_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Publisher to "orders" queue via default exchange */
    const char *pub_src =
        "import pika\n"
        "\n"
        "def send_order():\n"
        "    channel.basic_publish(exchange='', routing_key='orders',\n"
        "                         body='order')\n";

    write_file(tmpdir, "pub.py", pub_src);

    /* Consumer on "payments" queue */
    const char *sub_src =
        "import pika\n"
        "\n"
        "def handle_payment():\n"
        "    channel.basic_consume(queue='payments',\n"
        "                         on_message_callback=cb)\n";

    write_file(tmpdir, "sub.py", sub_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    cbm_gbuf_upsert_node(gb, "Function", "send_order",
        "test.pub.send_order", "pub.py", 3, 5, NULL);

    cbm_gbuf_upsert_node(gb, "Function", "handle_payment",
        "test.sub.handle_payment", "sub.py", 3, 5, NULL);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_rabbitmq(&ctx);

    /* "orders" publisher should NOT match "payments" consumer */
    ASSERT_EQ(links, 0);
    ASSERT_EQ(count_amqp_edges(gb), 0);

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 12: Empty graph buffer (no crash)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(rabbitmq_empty_graph) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_rmq_t12_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_rabbitmq(&ctx);

    ASSERT_EQ(links, 0);
    ASSERT_EQ(count_amqp_edges(gb), 0);

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 13: Rust basic_publish + basic_consume → edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(rabbitmq_rust_publish_consume) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_rmq_t13_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Rust publisher */
    const char *pub_src =
        "async fn publish_event() {\n"
        "    channel.basic_publish(\"\", \"rust_queue\", BasicPublishOptions::default(), payload, props).await;\n"
        "}\n";

    write_file(tmpdir, "publisher/main.rs", pub_src);

    /* Rust consumer */
    const char *sub_src =
        "async fn consume_events() {\n"
        "    let consumer = channel.basic_consume(\"rust_queue\", \"consumer_tag\", opts, table).await;\n"
        "}\n";

    write_file(tmpdir, "consumer/main.rs", sub_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Function", "publish_event",
        "test.publisher.main.publish_event",
        "publisher/main.rs", 1, 3, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t sub_id = cbm_gbuf_upsert_node(gb, "Function", "consume_events",
        "test.consumer.main.consume_events",
        "consumer/main.rs", 1, 3, NULL);
    ASSERT_GT(sub_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_rabbitmq(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_amqp_edges(gb), 0);
    ASSERT_TRUE(has_amqp_edge_with_identifier(gb, "rust_queue"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 14: Unit test for amqp_topic_match function
 * ═══════════════════════════════════════════════════════════════════ */

TEST(rabbitmq_topic_match_unit) {
    /* Exact match */
    ASSERT_EQ(amqp_topic_match("order.created", "order.created"), 1);

    /* * matches one word */
    ASSERT_EQ(amqp_topic_match("order.*", "order.created"), 1);
    ASSERT_EQ(amqp_topic_match("*.created", "order.created"), 1);

    /* * does NOT match zero or multiple words */
    ASSERT_EQ(amqp_topic_match("order.*", "order.created.us"), 0);
    ASSERT_EQ(amqp_topic_match("order.*", "order"), 0);

    /* # matches zero or more words */
    ASSERT_EQ(amqp_topic_match("order.#", "order.created"), 1);
    ASSERT_EQ(amqp_topic_match("order.#", "order.created.us"), 1);
    ASSERT_EQ(amqp_topic_match("order.#", "order"), 1);
    ASSERT_EQ(amqp_topic_match("#", "anything.goes.here"), 1);
    ASSERT_EQ(amqp_topic_match("#", "single"), 1);

    /* Combined */
    ASSERT_EQ(amqp_topic_match("*.*.us", "order.created.us"), 1);
    ASSERT_EQ(amqp_topic_match("*.*.us", "order.created.eu"), 0);

    /* No match */
    ASSERT_EQ(amqp_topic_match("order.created", "order.updated"), 0);
    ASSERT_EQ(amqp_topic_match("order.created", "payment.created"), 0);

    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test: Class node with RabbitMQ publisher → detected
 * ═══════════════════════════════════════════════════════════════════ */

TEST(rabbitmq_class_node_publisher) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_amqp_cls_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    const char *pub_src =
        "class EventPublisher {\n"
        "  async publish(event) {\n"
        "    channel.basicPublish('events', 'order.created', Buffer.from(JSON.stringify(event)));\n"
        "  }\n"
        "}\n";
    write_file(tmpdir, "publishers/event.ts", pub_src);

    const char *sub_src =
        "function consumeEvents() {\n"
        "  channel.basicConsume('order-events-queue', (msg) => {});\n"
        "}\n";
    write_file(tmpdir, "consumers/event.ts", sub_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);
    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Class", "EventPublisher",
        "test.publishers.event.EventPublisher", "publishers/event.ts", 1, 5, NULL);
    ASSERT_GT(pub_id, 0);
    int64_t sub_id = cbm_gbuf_upsert_node(gb, "Function", "consumeEvents",
        "test.consumers.event.consumeEvents", "consumers/event.ts", 1, 3, NULL);
    ASSERT_GT(sub_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_rabbitmq(&ctx);
    ASSERT_GTE(links, 0);

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Suite definition
 * ═══════════════════════════════════════════════════════════════════ */

SUITE(servicelink_rabbitmq) {
    RUN_TEST(rabbitmq_python_direct);
    RUN_TEST(rabbitmq_java_template);
    RUN_TEST(rabbitmq_go_publish_consume);
    RUN_TEST(rabbitmq_nodejs_publish_consume);
    RUN_TEST(rabbitmq_topic_star_match);
    RUN_TEST(rabbitmq_topic_hash_match);
    RUN_TEST(rabbitmq_topic_star_no_multi_segment);
    RUN_TEST(rabbitmq_fanout_all_consumers);
    RUN_TEST(rabbitmq_default_exchange);
    RUN_TEST(rabbitmq_no_self_link);
    RUN_TEST(rabbitmq_no_match_different_queues);
    RUN_TEST(rabbitmq_empty_graph);
    RUN_TEST(rabbitmq_rust_publish_consume);
    RUN_TEST(rabbitmq_topic_match_unit);
    RUN_TEST(rabbitmq_class_node_publisher);
}
