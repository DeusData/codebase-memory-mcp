/*
 * test_servicelink_kafka.c — Tests for Kafka protocol linking.
 *
 * Creates synthetic source files (.go, .py, .java, .js, .ts, .rs),
 * builds a graph buffer with nodes, runs the Kafka linker, and verifies
 * that KAFKA_CALLS edges are created with correct confidence.
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
static void rm_rf_kafka(const char *path) {
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

/* Count KAFKA_CALLS edges */
static int count_kafka_edges(cbm_gbuf_t *gb) {
    return cbm_gbuf_edge_count_by_type(gb, "KAFKA_CALLS");
}

/* Check if a KAFKA_CALLS edge has given confidence band */
static bool has_kafka_edge_with_band(cbm_gbuf_t *gb, const char *band) {
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_type(gb, "KAFKA_CALLS", &edges, &count);
    char needle[64];
    snprintf(needle, sizeof(needle), "\"confidence_band\":\"%s\"", band);
    for (int i = 0; i < count; i++) {
        if (edges[i]->properties_json && strstr(edges[i]->properties_json, needle))
            return true;
    }
    return false;
}

/* Check if a KAFKA_CALLS edge has given identifier */
static bool has_kafka_edge_with_identifier(cbm_gbuf_t *gb, const char *identifier) {
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_type(gb, "KAFKA_CALLS", &edges, &count);
    char needle[256];
    snprintf(needle, sizeof(needle), "\"identifier\":\"%s\"", identifier);
    for (int i = 0; i < count; i++) {
        if (edges[i]->properties_json && strstr(edges[i]->properties_json, needle))
            return true;
    }
    return false;
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 1: Go kafka.Writer producer + kafka.NewReader consumer → edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(kafka_go_writer_reader) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_kafka_t1_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Go producer using kafka.Writer */
    const char *producer_src =
        "package main\n"
        "\n"
        "func publishOrder() {\n"
        "    w := &kafka.Writer{Topic: \"order-events\"}\n"
        "    w.WriteMessages(ctx, kafka.Message{Value: data})\n"
        "}\n";

    write_file(tmpdir, "producer/main.go", producer_src);

    /* Go consumer using kafka.NewReader */
    const char *consumer_src =
        "package main\n"
        "\n"
        "func consumeOrders() {\n"
        "    r := kafka.NewReader(kafka.ReaderConfig{Topic: \"order-events\"})\n"
        "    msg, _ := r.ReadMessage(ctx)\n"
        "}\n";

    write_file(tmpdir, "consumer/main.go", consumer_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t prod_id = cbm_gbuf_upsert_node(gb, "Function", "publishOrder",
        "test.producer.main.publishOrder", "producer/main.go", 3, 6, NULL);
    ASSERT_GT(prod_id, 0);

    int64_t cons_id = cbm_gbuf_upsert_node(gb, "Function", "consumeOrders",
        "test.consumer.main.consumeOrders", "consumer/main.go", 3, 6, NULL);
    ASSERT_GT(cons_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_kafka(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_kafka_edges(gb), 0);
    ASSERT_TRUE(has_kafka_edge_with_band(gb, "high"));
    ASSERT_TRUE(has_kafka_edge_with_identifier(gb, "order-events"));

    cbm_gbuf_free(gb);
    rm_rf_kafka(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 2: Java @KafkaListener consumer detection
 * ═══════════════════════════════════════════════════════════════════ */

TEST(kafka_java_listener) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_kafka_t2_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Java producer using kafkaTemplate */
    const char *producer_src =
        "package com.example;\n"
        "\n"
        "public class OrderProducer {\n"
        "    public void sendOrder() {\n"
        "        kafkaTemplate.send(\"user-notifications\", payload);\n"
        "    }\n"
        "}\n";

    write_file(tmpdir, "src/OrderProducer.java", producer_src);

    /* Java consumer using @KafkaListener */
    const char *consumer_src =
        "package com.example;\n"
        "\n"
        "public class NotificationConsumer {\n"
        "    @KafkaListener(topics = \"user-notifications\")\n"
        "    public void onMessage(String msg) {\n"
        "        System.out.println(msg);\n"
        "    }\n"
        "}\n";

    write_file(tmpdir, "src/NotificationConsumer.java", consumer_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t prod_id = cbm_gbuf_upsert_node(gb, "Method", "sendOrder",
        "test.OrderProducer.sendOrder", "src/OrderProducer.java", 4, 6, NULL);
    ASSERT_GT(prod_id, 0);

    int64_t cons_id = cbm_gbuf_upsert_node(gb, "Method", "onMessage",
        "test.NotificationConsumer.onMessage", "src/NotificationConsumer.java", 4, 7, NULL);
    ASSERT_GT(cons_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_kafka(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_kafka_edges(gb), 0);
    ASSERT_TRUE(has_kafka_edge_with_identifier(gb, "user-notifications"));

    cbm_gbuf_free(gb);
    rm_rf_kafka(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 3: Java kafkaTemplate.send producer detection
 * ═══════════════════════════════════════════════════════════════════ */

TEST(kafka_java_template_send) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_kafka_t3_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Java producer */
    const char *producer_src =
        "package com.example;\n"
        "\n"
        "public class EventPublisher {\n"
        "    public void publish() {\n"
        "        kafkaTemplate.send(\"audit-log\", event);\n"
        "    }\n"
        "}\n";

    write_file(tmpdir, "src/EventPublisher.java", producer_src);

    /* Java consumer using consumer.subscribe */
    const char *consumer_src =
        "package com.example;\n"
        "\n"
        "public class AuditConsumer {\n"
        "    public void start() {\n"
        "        consumer.subscribe(Arrays.asList(\"audit-log\"));\n"
        "        consumer.poll(Duration.ofMillis(100));\n"
        "    }\n"
        "}\n";

    write_file(tmpdir, "src/AuditConsumer.java", consumer_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t prod_id = cbm_gbuf_upsert_node(gb, "Method", "publish",
        "test.EventPublisher.publish", "src/EventPublisher.java", 4, 6, NULL);
    ASSERT_GT(prod_id, 0);

    int64_t cons_id = cbm_gbuf_upsert_node(gb, "Method", "start",
        "test.AuditConsumer.start", "src/AuditConsumer.java", 4, 7, NULL);
    ASSERT_GT(cons_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_kafka(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_kafka_edges(gb), 0);
    ASSERT_TRUE(has_kafka_edge_with_identifier(gb, "audit-log"));

    cbm_gbuf_free(gb);
    rm_rf_kafka(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 4: Python producer.send + KafkaConsumer → edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(kafka_python_producer_consumer) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_kafka_t4_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Python producer */
    const char *producer_src =
        "from kafka import KafkaProducer\n"
        "\n"
        "def publish_event():\n"
        "    producer = KafkaProducer(bootstrap_servers='localhost:9092')\n"
        "    producer.send('payment-events', value=data)\n";

    write_file(tmpdir, "publisher.py", producer_src);

    /* Python consumer */
    const char *consumer_src =
        "from kafka import KafkaConsumer\n"
        "\n"
        "def consume_payments():\n"
        "    consumer = KafkaConsumer('payment-events',\n"
        "                            bootstrap_servers='localhost:9092')\n"
        "    for msg in consumer:\n"
        "        process(msg)\n";

    write_file(tmpdir, "consumer.py", consumer_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t prod_id = cbm_gbuf_upsert_node(gb, "Function", "publish_event",
        "test.publisher.publish_event", "publisher.py", 3, 5, NULL);
    ASSERT_GT(prod_id, 0);

    int64_t cons_id = cbm_gbuf_upsert_node(gb, "Function", "consume_payments",
        "test.consumer.consume_payments", "consumer.py", 3, 7, NULL);
    ASSERT_GT(cons_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_kafka(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_kafka_edges(gb), 0);
    ASSERT_TRUE(has_kafka_edge_with_identifier(gb, "payment-events"));

    cbm_gbuf_free(gb);
    rm_rf_kafka(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 5: Node.js producer.send({topic:...}) + consumer.subscribe
 * ═══════════════════════════════════════════════════════════════════ */

TEST(kafka_nodejs_producer_consumer) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_kafka_t5_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Node.js producer */
    const char *producer_src =
        "const { Kafka } = require('kafkajs');\n"
        "\n"
        "async function sendMessage() {\n"
        "  await producer.send({topic: 'user-signups', messages: [{value: 'hello'}]});\n"
        "}\n";

    write_file(tmpdir, "producer.js", producer_src);

    /* Node.js consumer */
    const char *consumer_src =
        "const { Kafka } = require('kafkajs');\n"
        "\n"
        "async function startConsumer() {\n"
        "  await consumer.subscribe({topic: 'user-signups', fromBeginning: true});\n"
        "  await consumer.run({eachMessage: async ({message}) => { console.log(message); }});\n"
        "}\n";

    write_file(tmpdir, "consumer.js", consumer_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t prod_id = cbm_gbuf_upsert_node(gb, "Function", "sendMessage",
        "test.producer.sendMessage", "producer.js", 3, 5, NULL);
    ASSERT_GT(prod_id, 0);

    int64_t cons_id = cbm_gbuf_upsert_node(gb, "Function", "startConsumer",
        "test.consumer.startConsumer", "consumer.js", 3, 6, NULL);
    ASSERT_GT(cons_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_kafka(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_kafka_edges(gb), 0);
    ASSERT_TRUE(has_kafka_edge_with_identifier(gb, "user-signups"));

    cbm_gbuf_free(gb);
    rm_rf_kafka(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 6: Multi-topic: 2 different topics, 2 producers, 2 consumers → 2 edges
 * ═══════════════════════════════════════════════════════════════════ */

TEST(kafka_multi_topic_no_cross_match) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_kafka_t6_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Producer A sends to topic-a */
    const char *prod_a_src =
        "from kafka import KafkaProducer\n"
        "\n"
        "def send_a():\n"
        "    producer.send('topic-alpha', value=b'data-a')\n";

    write_file(tmpdir, "prod_a.py", prod_a_src);

    /* Producer B sends to topic-b */
    const char *prod_b_src =
        "from kafka import KafkaProducer\n"
        "\n"
        "def send_b():\n"
        "    producer.send('topic-beta', value=b'data-b')\n";

    write_file(tmpdir, "prod_b.py", prod_b_src);

    /* Consumer A subscribes to topic-a */
    const char *cons_a_src =
        "from kafka import KafkaConsumer\n"
        "\n"
        "def consume_a():\n"
        "    c = KafkaConsumer('topic-alpha')\n";

    write_file(tmpdir, "cons_a.py", cons_a_src);

    /* Consumer B subscribes to topic-b */
    const char *cons_b_src =
        "from kafka import KafkaConsumer\n"
        "\n"
        "def consume_b():\n"
        "    c = KafkaConsumer('topic-beta')\n";

    write_file(tmpdir, "cons_b.py", cons_b_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pa = cbm_gbuf_upsert_node(gb, "Function", "send_a",
        "test.prod_a.send_a", "prod_a.py", 3, 4, NULL);
    int64_t pb = cbm_gbuf_upsert_node(gb, "Function", "send_b",
        "test.prod_b.send_b", "prod_b.py", 3, 4, NULL);
    int64_t ca = cbm_gbuf_upsert_node(gb, "Function", "consume_a",
        "test.cons_a.consume_a", "cons_a.py", 3, 4, NULL);
    int64_t cb = cbm_gbuf_upsert_node(gb, "Function", "consume_b",
        "test.cons_b.consume_b", "cons_b.py", 3, 4, NULL);
    ASSERT_GT(pa, 0);
    ASSERT_GT(pb, 0);
    ASSERT_GT(ca, 0);
    ASSERT_GT(cb, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_kafka(&ctx);

    /* Exactly 2 edges: topic-alpha and topic-beta, no cross-match */
    ASSERT_EQ(links, 2);
    ASSERT_EQ(count_kafka_edges(gb), 2);
    ASSERT_TRUE(has_kafka_edge_with_identifier(gb, "topic-alpha"));
    ASSERT_TRUE(has_kafka_edge_with_identifier(gb, "topic-beta"));

    cbm_gbuf_free(gb);
    rm_rf_kafka(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 7: Self-link prevention (producer and consumer in same node)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(kafka_no_self_link) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_kafka_t7_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Go file that both produces and consumes from same topic in same function */
    const char *src =
        "package main\n"
        "\n"
        "func relay() {\n"
        "    w := &kafka.Writer{Topic: \"relay-topic\"}\n"
        "    r := kafka.NewReader(kafka.ReaderConfig{Topic: \"relay-topic\"})\n"
        "}\n";

    write_file(tmpdir, "relay.go", src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t id = cbm_gbuf_upsert_node(gb, "Function", "relay",
        "test.relay.relay", "relay.go", 3, 6, NULL);
    ASSERT_GT(id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_kafka(&ctx);

    /* Same node is both producer and consumer — no self-link */
    ASSERT_EQ(links, 0);
    ASSERT_EQ(count_kafka_edges(gb), 0);

    cbm_gbuf_free(gb);
    rm_rf_kafka(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 8: No match (producer on topic "A", consumer on topic "B")
 * ═══════════════════════════════════════════════════════════════════ */

TEST(kafka_no_match_different_topics) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_kafka_t8_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Producer sends to "orders" */
    const char *producer_src =
        "from kafka import KafkaProducer\n"
        "\n"
        "def send_order():\n"
        "    producer.send('orders', value=b'order')\n";

    write_file(tmpdir, "producer.py", producer_src);

    /* Consumer subscribes to "payments" */
    const char *consumer_src =
        "from kafka import KafkaConsumer\n"
        "\n"
        "def consume_payments():\n"
        "    c = KafkaConsumer('payments')\n";

    write_file(tmpdir, "consumer.py", consumer_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    cbm_gbuf_upsert_node(gb, "Function", "send_order",
        "test.producer.send_order", "producer.py", 3, 4, NULL);
    cbm_gbuf_upsert_node(gb, "Function", "consume_payments",
        "test.consumer.consume_payments", "consumer.py", 3, 4, NULL);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_kafka(&ctx);

    /* Different topics — no match */
    ASSERT_EQ(links, 0);
    ASSERT_EQ(count_kafka_edges(gb), 0);

    cbm_gbuf_free(gb);
    rm_rf_kafka(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 9: Rust FutureRecord::to producer + consumer.subscribe
 * ═══════════════════════════════════════════════════════════════════ */

TEST(kafka_rust_producer_consumer) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_kafka_t9_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Rust producer */
    const char *producer_src =
        "use rdkafka::producer::FutureProducer;\n"
        "\n"
        "async fn publish() {\n"
        "    let record = FutureRecord::to(\"metrics-stream\").payload(&data);\n"
        "    producer.send(record, Duration::from_secs(5)).await;\n"
        "}\n";

    write_file(tmpdir, "src/producer.rs", producer_src);

    /* Rust consumer */
    const char *consumer_src =
        "use rdkafka::consumer::StreamConsumer;\n"
        "\n"
        "fn start_consumer() {\n"
        "    consumer.subscribe(&[\"metrics-stream\"]);\n"
        "}\n";

    write_file(tmpdir, "src/consumer.rs", consumer_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t prod_id = cbm_gbuf_upsert_node(gb, "Function", "publish",
        "test.src.producer.publish", "src/producer.rs", 3, 6, NULL);
    ASSERT_GT(prod_id, 0);

    int64_t cons_id = cbm_gbuf_upsert_node(gb, "Function", "start_consumer",
        "test.src.consumer.start_consumer", "src/consumer.rs", 3, 5, NULL);
    ASSERT_GT(cons_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_kafka(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_kafka_edges(gb), 0);
    ASSERT_TRUE(has_kafka_edge_with_identifier(gb, "metrics-stream"));

    cbm_gbuf_free(gb);
    rm_rf_kafka(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 10: Multiple languages producing to same topic → consumer matches
 * ═══════════════════════════════════════════════════════════════════ */

TEST(kafka_multi_language_same_topic) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_kafka_t10_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Go producer */
    const char *go_prod_src =
        "package main\n"
        "\n"
        "func sendEvent() {\n"
        "    w := &kafka.Writer{Topic: \"shared-events\"}\n"
        "    w.WriteMessages(ctx, msg)\n"
        "}\n";

    write_file(tmpdir, "go_producer.go", go_prod_src);

    /* Python producer */
    const char *py_prod_src =
        "from kafka import KafkaProducer\n"
        "\n"
        "def send_event():\n"
        "    producer.send('shared-events', value=b'data')\n";

    write_file(tmpdir, "py_producer.py", py_prod_src);

    /* Java consumer */
    const char *java_cons_src =
        "package com.example;\n"
        "\n"
        "public class EventListener {\n"
        "    @KafkaListener(topics = \"shared-events\")\n"
        "    public void handle(String msg) {}\n"
        "}\n";

    write_file(tmpdir, "EventListener.java", java_cons_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t go_id = cbm_gbuf_upsert_node(gb, "Function", "sendEvent",
        "test.go_producer.sendEvent", "go_producer.go", 3, 6, NULL);
    int64_t py_id = cbm_gbuf_upsert_node(gb, "Function", "send_event",
        "test.py_producer.send_event", "py_producer.py", 3, 4, NULL);
    int64_t java_id = cbm_gbuf_upsert_node(gb, "Method", "handle",
        "test.EventListener.handle", "EventListener.java", 4, 5, NULL);
    ASSERT_GT(go_id, 0);
    ASSERT_GT(py_id, 0);
    ASSERT_GT(java_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_kafka(&ctx);

    /* Consumer matches at least one producer (both produce same topic) */
    ASSERT_GT(links, 0);
    ASSERT_GT(count_kafka_edges(gb), 0);
    ASSERT_TRUE(has_kafka_edge_with_identifier(gb, "shared-events"));

    cbm_gbuf_free(gb);
    rm_rf_kafka(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 11: Empty graph buffer (no crash)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(kafka_empty_graph) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_kafka_t11_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_kafka(&ctx);

    ASSERT_EQ(links, 0);
    ASSERT_EQ(count_kafka_edges(gb), 0);

    cbm_gbuf_free(gb);
    rm_rf_kafka(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 12: TypeScript producer + consumer
 * ═══════════════════════════════════════════════════════════════════ */

TEST(kafka_typescript_producer_consumer) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_kafka_t12_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* TypeScript producer */
    const char *producer_src =
        "import { Kafka } from 'kafkajs';\n"
        "\n"
        "async function produce() {\n"
        "  await producer.send({topic: 'ts-events', messages: [{value: 'test'}]});\n"
        "}\n";

    write_file(tmpdir, "producer.ts", producer_src);

    /* TypeScript consumer */
    const char *consumer_src =
        "import { Kafka } from 'kafkajs';\n"
        "\n"
        "async function consume() {\n"
        "  await consumer.subscribe({topics: ['ts-events']});\n"
        "}\n";

    write_file(tmpdir, "consumer.ts", consumer_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t prod_id = cbm_gbuf_upsert_node(gb, "Function", "produce",
        "test.producer.produce", "producer.ts", 3, 5, NULL);
    ASSERT_GT(prod_id, 0);

    int64_t cons_id = cbm_gbuf_upsert_node(gb, "Function", "consume",
        "test.consumer.consume", "consumer.ts", 3, 5, NULL);
    ASSERT_GT(cons_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_kafka(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_kafka_edges(gb), 0);
    ASSERT_TRUE(has_kafka_edge_with_identifier(gb, "ts-events"));

    cbm_gbuf_free(gb);
    rm_rf_kafka(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test: Class node with Kafka producer/consumer → detected
 * ═══════════════════════════════════════════════════════════════════ */

TEST(kafka_class_node_producer) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_kafka_cls_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    const char *class_src =
        "class OrderProducer {\n"
        "  async produce() {\n"
        "    await producer.send({\n"
        "      topic: 'order-events',\n"
        "      messages: [{ value: JSON.stringify(order) }],\n"
        "    });\n"
        "  }\n"
        "}\n";
    write_file(tmpdir, "producers/order.ts", class_src);

    const char *consumer_src =
        "class OrderConsumer {\n"
        "  async consume() {\n"
        "    await consumer.subscribe({ topic: 'order-events' });\n"
        "  }\n"
        "}\n";
    write_file(tmpdir, "consumers/order.ts", consumer_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);
    int64_t prod_id = cbm_gbuf_upsert_node(gb, "Class", "OrderProducer",
        "test.producers.order.OrderProducer", "producers/order.ts", 1, 8, NULL);
    ASSERT_GT(prod_id, 0);
    int64_t cons_id = cbm_gbuf_upsert_node(gb, "Class", "OrderConsumer",
        "test.consumers.order.OrderConsumer", "consumers/order.ts", 1, 5, NULL);
    ASSERT_GT(cons_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_kafka(&ctx);
    ASSERT_GT(links, 0);
    ASSERT_GT(cbm_gbuf_edge_count_by_type(gb, "KAFKA_CALLS"), 0);

    cbm_gbuf_free(gb);
    rm_rf_kafka(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Suite
 * ═══════════════════════════════════════════════════════════════════ */

SUITE(servicelink_kafka) {
    RUN_TEST(kafka_go_writer_reader);
    RUN_TEST(kafka_java_listener);
    RUN_TEST(kafka_java_template_send);
    RUN_TEST(kafka_python_producer_consumer);
    RUN_TEST(kafka_nodejs_producer_consumer);
    RUN_TEST(kafka_multi_topic_no_cross_match);
    RUN_TEST(kafka_no_self_link);
    RUN_TEST(kafka_no_match_different_topics);
    RUN_TEST(kafka_rust_producer_consumer);
    RUN_TEST(kafka_multi_language_same_topic);
    RUN_TEST(kafka_empty_graph);
    RUN_TEST(kafka_typescript_producer_consumer);
    RUN_TEST(kafka_class_node_producer);
}
