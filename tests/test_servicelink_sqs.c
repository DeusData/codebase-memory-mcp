/*
 * test_servicelink_sqs.c — Tests for SQS protocol linking.
 *
 * Creates synthetic source files (.py, .go, .java, .js, .ts, .tf),
 * builds a graph buffer with nodes, runs the SQS linker, and verifies
 * that SQS_CALLS edges are created with correct confidence bands.
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

/* Count SQS_CALLS edges */
static int count_sqs_edges(cbm_gbuf_t *gb) {
    return cbm_gbuf_edge_count_by_type(gb, "SQS_CALLS");
}

/* Check if an SQS_CALLS edge has a given confidence band */
static bool has_sqs_edge_with_band(cbm_gbuf_t *gb, const char *band) {
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_type(gb, "SQS_CALLS", &edges, &count);
    char needle[64];
    snprintf(needle, sizeof(needle), "\"confidence_band\":\"%s\"", band);
    for (int i = 0; i < count; i++) {
        if (edges[i]->properties_json && strstr(edges[i]->properties_json, needle))
            return true;
    }
    return false;
}

/* Check if an SQS_CALLS edge has a given identifier */
static bool has_sqs_edge_with_identifier(cbm_gbuf_t *gb, const char *identifier) {
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_type(gb, "SQS_CALLS", &edges, &count);
    char needle[256];
    snprintf(needle, sizeof(needle), "\"identifier\":\"%s\"", identifier);
    for (int i = 0; i < count; i++) {
        if (edges[i]->properties_json && strstr(edges[i]->properties_json, needle))
            return true;
    }
    return false;
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 1: Python boto3 send_message + receive_message → edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(sqs_python_send_receive) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_sqs_t1_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Python producer: send_message */
    const char *producer_src =
        "import boto3\n"
        "\n"
        "def send_order():\n"
        "    sqs = boto3.client('sqs')\n"
        "    sqs.send_message(QueueUrl='https://sqs.us-east-1.amazonaws.com/123456789/order-events', MessageBody='hello')\n";

    write_file(tmpdir, "producer/sender.py", producer_src);

    /* Python consumer: receive_message */
    const char *consumer_src =
        "import boto3\n"
        "\n"
        "def poll_orders():\n"
        "    sqs = boto3.client('sqs')\n"
        "    sqs.receive_message(QueueUrl='https://sqs.us-east-1.amazonaws.com/123456789/order-events')\n";

    write_file(tmpdir, "consumer/receiver.py", consumer_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t prod_id = cbm_gbuf_upsert_node(gb, "Function", "send_order",
        "test.producer.sender.send_order", "producer/sender.py", 3, 5, NULL);
    ASSERT_GT(prod_id, 0);

    int64_t cons_id = cbm_gbuf_upsert_node(gb, "Function", "poll_orders",
        "test.consumer.receiver.poll_orders", "consumer/receiver.py", 3, 5, NULL);
    ASSERT_GT(cons_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_sqs(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_sqs_edges(gb), 0);
    ASSERT_TRUE(has_sqs_edge_with_band(gb, "high"));
    ASSERT_TRUE(has_sqs_edge_with_identifier(gb, "order-events"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 2: Queue name extraction from full URL
 * ═══════════════════════════════════════════════════════════════════ */

TEST(sqs_queue_name_from_url) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_sqs_t2_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Python producer with full URL */
    const char *producer_src =
        "def send():\n"
        "    sqs.send_message(QueueUrl='https://sqs.eu-west-1.amazonaws.com/999888777/payment-processing', MessageBody='pay')\n";

    write_file(tmpdir, "send.py", producer_src);

    /* Python consumer with same full URL */
    const char *consumer_src =
        "def recv():\n"
        "    sqs.receive_message(QueueUrl='https://sqs.eu-west-1.amazonaws.com/999888777/payment-processing')\n";

    write_file(tmpdir, "recv.py", consumer_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t prod_id = cbm_gbuf_upsert_node(gb, "Function", "send",
        "test.send.send", "send.py", 1, 2, NULL);
    ASSERT_GT(prod_id, 0);

    int64_t cons_id = cbm_gbuf_upsert_node(gb, "Function", "recv",
        "test.recv.recv", "recv.py", 1, 2, NULL);
    ASSERT_GT(cons_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_sqs(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_TRUE(has_sqs_edge_with_identifier(gb, "payment-processing"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 3: Queue name extraction from ARN (Terraform event source)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(sqs_queue_name_from_arn) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_sqs_t3_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Python producer sending to a queue by URL */
    const char *producer_src =
        "def publish():\n"
        "    sqs.send_message(QueueUrl='https://sqs.us-east-1.amazonaws.com/111222333/notifications', MessageBody='msg')\n";

    write_file(tmpdir, "publish.py", producer_src);

    /* Terraform Lambda event source with ARN */
    const char *tf_src =
        "resource \"aws_lambda_event_source_mapping\" \"sqs_trigger\" {\n"
        "  event_source_arn = \"arn:aws:sqs:us-east-1:111222333:notifications\"\n"
        "  function_name   = aws_lambda_function.processor.arn\n"
        "}\n";

    write_file(tmpdir, "infra/main.tf", tf_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t prod_id = cbm_gbuf_upsert_node(gb, "Function", "publish",
        "test.publish.publish", "publish.py", 1, 2, NULL);
    ASSERT_GT(prod_id, 0);

    int64_t cons_id = cbm_gbuf_upsert_node(gb, "Module", "sqs_trigger",
        "test.infra.main.sqs_trigger", "infra/main.tf", 1, 4, NULL);
    ASSERT_GT(cons_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_sqs(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_TRUE(has_sqs_edge_with_identifier(gb, "notifications"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 4: Java @SqsListener consumer detection
 * ═══════════════════════════════════════════════════════════════════ */

TEST(sqs_java_listener) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_sqs_t4_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Java producer: sendMessage with queueUrl */
    const char *producer_src =
        "public class OrderPublisher {\n"
        "    public void publish() {\n"
        "        sqsClient.sendMessage(SendMessageRequest.builder()\n"
        "            .queueUrl(\"https://sqs.us-east-1.amazonaws.com/123/order-queue\")\n"
        "            .messageBody(\"order\").build());\n"
        "    }\n"
        "}\n";

    write_file(tmpdir, "src/OrderPublisher.java", producer_src);

    /* Java consumer: @SqsListener */
    const char *consumer_src =
        "import io.awspring.cloud.sqs.annotation.SqsListener;\n"
        "\n"
        "public class OrderConsumer {\n"
        "    @SqsListener(\"order-queue\")\n"
        "    public void handleOrder(String message) {\n"
        "        System.out.println(message);\n"
        "    }\n"
        "}\n";

    write_file(tmpdir, "src/OrderConsumer.java", consumer_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t prod_id = cbm_gbuf_upsert_node(gb, "Method", "publish",
        "test.OrderPublisher.publish", "src/OrderPublisher.java", 2, 6, NULL);
    ASSERT_GT(prod_id, 0);

    int64_t cons_id = cbm_gbuf_upsert_node(gb, "Method", "handleOrder",
        "test.OrderConsumer.handleOrder", "src/OrderConsumer.java", 4, 7, NULL);
    ASSERT_GT(cons_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_sqs(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_sqs_edges(gb), 0);
    ASSERT_TRUE(has_sqs_edge_with_identifier(gb, "order-queue"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 5: Java sqsClient.sendMessage producer detection
 * ═══════════════════════════════════════════════════════════════════ */

TEST(sqs_java_send_message) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_sqs_t5_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Java producer: amazonSQS.sendMessage("url", ...) */
    const char *producer_src =
        "public class LegacySender {\n"
        "    public void send() {\n"
        "        amazonSQS.sendMessage(\"https://sqs.us-east-1.amazonaws.com/123/legacy-queue\", body);\n"
        "    }\n"
        "}\n";

    write_file(tmpdir, "src/LegacySender.java", producer_src);

    /* Python consumer on same queue name */
    const char *consumer_src =
        "def consume():\n"
        "    sqs.receive_message(QueueUrl='https://sqs.us-east-1.amazonaws.com/456/legacy-queue')\n";

    write_file(tmpdir, "consumer.py", consumer_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t prod_id = cbm_gbuf_upsert_node(gb, "Method", "send",
        "test.LegacySender.send", "src/LegacySender.java", 2, 4, NULL);
    ASSERT_GT(prod_id, 0);

    int64_t cons_id = cbm_gbuf_upsert_node(gb, "Function", "consume",
        "test.consumer.consume", "consumer.py", 1, 2, NULL);
    ASSERT_GT(cons_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_sqs(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_TRUE(has_sqs_edge_with_identifier(gb, "legacy-queue"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 6: Go SDK SendMessage + ReceiveMessage → edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(sqs_go_send_receive) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_sqs_t6_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Go producer */
    const char *producer_src =
        "package main\n"
        "\n"
        "func sendEvent() {\n"
        "    _, err := sqsClient.SendMessage(ctx, &sqs.SendMessageInput{\n"
        "        QueueUrl:    aws.String(\"https://sqs.us-east-1.amazonaws.com/123/event-bus\"),\n"
        "        MessageBody: aws.String(\"event\"),\n"
        "    })\n"
        "}\n";

    write_file(tmpdir, "producer/send.go", producer_src);

    /* Go consumer */
    const char *consumer_src =
        "package main\n"
        "\n"
        "func pollEvents() {\n"
        "    result, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{\n"
        "        QueueUrl:            aws.String(\"https://sqs.us-east-1.amazonaws.com/123/event-bus\"),\n"
        "        MaxNumberOfMessages: 10,\n"
        "    })\n"
        "}\n";

    write_file(tmpdir, "consumer/poll.go", consumer_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t prod_id = cbm_gbuf_upsert_node(gb, "Function", "sendEvent",
        "test.producer.send.sendEvent", "producer/send.go", 3, 8, NULL);
    ASSERT_GT(prod_id, 0);

    int64_t cons_id = cbm_gbuf_upsert_node(gb, "Function", "pollEvents",
        "test.consumer.poll.pollEvents", "consumer/poll.go", 3, 8, NULL);
    ASSERT_GT(cons_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_sqs(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_sqs_edges(gb), 0);
    ASSERT_TRUE(has_sqs_edge_with_band(gb, "high"));
    ASSERT_TRUE(has_sqs_edge_with_identifier(gb, "event-bus"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 7: Node.js SendMessageCommand + receiveMessage → edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(sqs_nodejs_send_receive) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_sqs_t7_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Node.js producer: SendMessageCommand */
    const char *producer_src =
        "const { SQSClient, SendMessageCommand } = require('@aws-sdk/client-sqs');\n"
        "\n"
        "async function publishTask() {\n"
        "  const sqs = new SQSClient({});\n"
        "  await sqs.send(new SendMessageCommand({\n"
        "    QueueUrl: 'https://sqs.us-east-1.amazonaws.com/123/task-queue',\n"
        "    MessageBody: JSON.stringify({ task: 'process' }),\n"
        "  }));\n"
        "}\n";

    write_file(tmpdir, "publisher.js", producer_src);

    /* Node.js consumer: receiveMessage */
    const char *consumer_src =
        "const { SQSClient, ReceiveMessageCommand } = require('@aws-sdk/client-sqs');\n"
        "\n"
        "async function consumeTask() {\n"
        "  const sqs = new SQSClient({});\n"
        "  const result = await sqs.send(new ReceiveMessageCommand({\n"
        "    QueueUrl: 'https://sqs.us-east-1.amazonaws.com/123/task-queue',\n"
        "    MaxNumberOfMessages: 5,\n"
        "  }));\n"
        "}\n";

    write_file(tmpdir, "consumer.js", consumer_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t prod_id = cbm_gbuf_upsert_node(gb, "Function", "publishTask",
        "test.publisher.publishTask", "publisher.js", 3, 9, NULL);
    ASSERT_GT(prod_id, 0);

    int64_t cons_id = cbm_gbuf_upsert_node(gb, "Function", "consumeTask",
        "test.consumer.consumeTask", "consumer.js", 3, 9, NULL);
    ASSERT_GT(cons_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_sqs(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_sqs_edges(gb), 0);
    ASSERT_TRUE(has_sqs_edge_with_identifier(gb, "task-queue"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 8: Multi-queue — 2 different queues, no cross-match
 * ═══════════════════════════════════════════════════════════════════ */

TEST(sqs_multi_queue_no_cross) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_sqs_t8_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Producer sends to queue-alpha */
    const char *producer_src =
        "def send_alpha():\n"
        "    sqs.send_message(QueueUrl='https://sqs.us-east-1.amazonaws.com/123/queue-alpha', MessageBody='a')\n";

    write_file(tmpdir, "prod_alpha.py", producer_src);

    /* Consumer receives from queue-beta */
    const char *consumer_src =
        "def recv_beta():\n"
        "    sqs.receive_message(QueueUrl='https://sqs.us-east-1.amazonaws.com/123/queue-beta')\n";

    write_file(tmpdir, "cons_beta.py", consumer_src);

    /* Producer sends to queue-beta */
    const char *producer2_src =
        "def send_beta():\n"
        "    sqs.send_message(QueueUrl='https://sqs.us-east-1.amazonaws.com/123/queue-beta', MessageBody='b')\n";

    write_file(tmpdir, "prod_beta.py", producer2_src);

    /* Consumer receives from queue-alpha */
    const char *consumer2_src =
        "def recv_alpha():\n"
        "    sqs.receive_message(QueueUrl='https://sqs.us-east-1.amazonaws.com/123/queue-alpha')\n";

    write_file(tmpdir, "cons_alpha.py", consumer2_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pa = cbm_gbuf_upsert_node(gb, "Function", "send_alpha",
        "test.prod_alpha.send_alpha", "prod_alpha.py", 1, 2, NULL);
    int64_t cb = cbm_gbuf_upsert_node(gb, "Function", "recv_beta",
        "test.cons_beta.recv_beta", "cons_beta.py", 1, 2, NULL);
    int64_t pb = cbm_gbuf_upsert_node(gb, "Function", "send_beta",
        "test.prod_beta.send_beta", "prod_beta.py", 1, 2, NULL);
    int64_t ca = cbm_gbuf_upsert_node(gb, "Function", "recv_alpha",
        "test.cons_alpha.recv_alpha", "cons_alpha.py", 1, 2, NULL);

    ASSERT_GT(pa, 0);
    ASSERT_GT(cb, 0);
    ASSERT_GT(pb, 0);
    ASSERT_GT(ca, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_sqs(&ctx);

    /* Should have exactly 2 edges: alpha→alpha, beta→beta */
    ASSERT_EQ(links, 2);
    ASSERT_EQ(count_sqs_edges(gb), 2);
    ASSERT_TRUE(has_sqs_edge_with_identifier(gb, "queue-alpha"));
    ASSERT_TRUE(has_sqs_edge_with_identifier(gb, "queue-beta"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 9: Self-link prevention
 * ═══════════════════════════════════════════════════════════════════ */

TEST(sqs_no_self_link) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_sqs_t9_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Single Python file that both sends and receives from same queue */
    const char *src =
        "def process():\n"
        "    sqs.send_message(QueueUrl='https://sqs.us-east-1.amazonaws.com/123/self-queue', MessageBody='x')\n"
        "    sqs.receive_message(QueueUrl='https://sqs.us-east-1.amazonaws.com/123/self-queue')\n";

    write_file(tmpdir, "processor.py", src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t id = cbm_gbuf_upsert_node(gb, "Function", "process",
        "test.processor.process", "processor.py", 1, 3, NULL);
    ASSERT_GT(id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_sqs(&ctx);

    /* Same node is both producer and consumer — should NOT create self-link */
    ASSERT_EQ(links, 0);
    ASSERT_EQ(count_sqs_edges(gb), 0);

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 10: No match — sender to "A", receiver from "B"
 * ═══════════════════════════════════════════════════════════════════ */

TEST(sqs_no_match_different_queues) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_sqs_t10_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Producer sends to "orders" */
    const char *producer_src =
        "def send_orders():\n"
        "    sqs.send_message(QueueUrl='https://sqs.us-east-1.amazonaws.com/123/orders', MessageBody='ord')\n";

    write_file(tmpdir, "send.py", producer_src);

    /* Consumer receives from "payments" */
    const char *consumer_src =
        "def recv_payments():\n"
        "    sqs.receive_message(QueueUrl='https://sqs.us-east-1.amazonaws.com/123/payments')\n";

    write_file(tmpdir, "recv.py", consumer_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t prod_id = cbm_gbuf_upsert_node(gb, "Function", "send_orders",
        "test.send.send_orders", "send.py", 1, 2, NULL);
    ASSERT_GT(prod_id, 0);

    int64_t cons_id = cbm_gbuf_upsert_node(gb, "Function", "recv_payments",
        "test.recv.recv_payments", "recv.py", 1, 2, NULL);
    ASSERT_GT(cons_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_sqs(&ctx);

    /* Different queue names — no match */
    ASSERT_EQ(links, 0);
    ASSERT_EQ(count_sqs_edges(gb), 0);

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 11: Empty graph buffer (no crash)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(sqs_empty_graph) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_sqs_t11_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_sqs(&ctx);

    ASSERT_EQ(links, 0);
    ASSERT_EQ(count_sqs_edges(gb), 0);

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 12: TypeScript producer + consumer
 * ═══════════════════════════════════════════════════════════════════ */

TEST(sqs_typescript_send_receive) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_sqs_t12_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* TypeScript producer */
    const char *producer_src =
        "import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs';\n"
        "\n"
        "export async function enqueue(): Promise<void> {\n"
        "  const client = new SQSClient({});\n"
        "  await client.send(new SendMessageCommand({\n"
        "    QueueUrl: 'https://sqs.us-east-1.amazonaws.com/123/ts-queue',\n"
        "    MessageBody: 'hello',\n"
        "  }));\n"
        "}\n";

    write_file(tmpdir, "enqueue.ts", producer_src);

    /* TypeScript consumer */
    const char *consumer_src =
        "import { SQSClient, ReceiveMessageCommand } from '@aws-sdk/client-sqs';\n"
        "\n"
        "export async function dequeue(): Promise<void> {\n"
        "  const client = new SQSClient({});\n"
        "  const res = await client.send(new ReceiveMessageCommand({\n"
        "    QueueUrl: 'https://sqs.us-east-1.amazonaws.com/123/ts-queue',\n"
        "    MaxNumberOfMessages: 10,\n"
        "  }));\n"
        "}\n";

    write_file(tmpdir, "dequeue.ts", consumer_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t prod_id = cbm_gbuf_upsert_node(gb, "Function", "enqueue",
        "test.enqueue.enqueue", "enqueue.ts", 3, 9, NULL);
    ASSERT_GT(prod_id, 0);

    int64_t cons_id = cbm_gbuf_upsert_node(gb, "Function", "dequeue",
        "test.dequeue.dequeue", "dequeue.ts", 3, 9, NULL);
    ASSERT_GT(cons_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_sqs(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_TRUE(has_sqs_edge_with_identifier(gb, "ts-queue"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test: Class node with SQS sender → detected
 * ═══════════════════════════════════════════════════════════════════ */

TEST(sqs_class_node_sender) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_sqs_cls_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    const char *sender_src =
        "class NotificationSender {\n"
        "  async send() {\n"
        "    await sqs.sendMessage({\n"
        "      QueueUrl: 'https://sqs.eu-west-1.amazonaws.com/123/notifications',\n"
        "      MessageBody: JSON.stringify(msg),\n"
        "    });\n"
        "  }\n"
        "}\n";
    write_file(tmpdir, "senders/notification.ts", sender_src);

    const char *receiver_src =
        "function processNotifications() {\n"
        "  sqs.receiveMessage({\n"
        "    QueueUrl: 'https://sqs.eu-west-1.amazonaws.com/123/notifications',\n"
        "  });\n"
        "}\n";
    write_file(tmpdir, "receivers/notification.ts", receiver_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);
    int64_t sender_id = cbm_gbuf_upsert_node(gb, "Class", "NotificationSender",
        "test.senders.notification.NotificationSender", "senders/notification.ts", 1, 8, NULL);
    ASSERT_GT(sender_id, 0);
    int64_t recv_id = cbm_gbuf_upsert_node(gb, "Function", "processNotifications",
        "test.receivers.notification.processNotifications", "receivers/notification.ts", 1, 5, NULL);
    ASSERT_GT(recv_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_sqs(&ctx);
    ASSERT_GT(links, 0);
    ASSERT_GT(cbm_gbuf_edge_count_by_type(gb, "SQS_CALLS"), 0);

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Suite definition
 * ═══════════════════════════════════════════════════════════════════ */

SUITE(servicelink_sqs) {
    RUN_TEST(sqs_python_send_receive);
    RUN_TEST(sqs_queue_name_from_url);
    RUN_TEST(sqs_queue_name_from_arn);
    RUN_TEST(sqs_java_listener);
    RUN_TEST(sqs_java_send_message);
    RUN_TEST(sqs_go_send_receive);
    RUN_TEST(sqs_nodejs_send_receive);
    RUN_TEST(sqs_multi_queue_no_cross);
    RUN_TEST(sqs_no_self_link);
    RUN_TEST(sqs_no_match_different_queues);
    RUN_TEST(sqs_empty_graph);
    RUN_TEST(sqs_typescript_send_receive);
    RUN_TEST(sqs_class_node_sender);
}
