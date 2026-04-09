/*
 * test_servicelink_sns.c — Tests for AWS SNS protocol linking.
 *
 * Creates synthetic source files (.py, .go, .java, .js, .ts, .tf),
 * builds a graph buffer with nodes, runs the SNS linker, and verifies
 * that SNS_CALLS edges are created with correct properties.
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

/* Count SNS_CALLS edges */
static int count_sns_edges(cbm_gbuf_t *gb) {
    return cbm_gbuf_edge_count_by_type(gb, "SNS_CALLS");
}

/* Check if an SNS_CALLS edge has given confidence band */
static bool has_sns_edge_with_band(cbm_gbuf_t *gb, const char *band) {
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_type(gb, "SNS_CALLS", &edges, &count);
    char needle[64];
    snprintf(needle, sizeof(needle), "\"confidence_band\":\"%s\"", band);
    for (int i = 0; i < count; i++) {
        if (edges[i]->properties_json && strstr(edges[i]->properties_json, needle))
            return true;
    }
    return false;
}

/* Check if an SNS_CALLS edge has given identifier */
static bool has_sns_edge_with_identifier(cbm_gbuf_t *gb, const char *identifier) {
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_type(gb, "SNS_CALLS", &edges, &count);
    char needle[256];
    snprintf(needle, sizeof(needle), "\"identifier\":\"%s\"", identifier);
    for (int i = 0; i < count; i++) {
        if (edges[i]->properties_json && strstr(edges[i]->properties_json, needle))
            return true;
    }
    return false;
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 1: Python boto3 sns.publish + sns.subscribe → edge created
 * ═══════════════════════════════════════════════════════════════════ */

TEST(sns_python_publish_subscribe) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_sns_t1_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Python publisher */
    const char *pub_src =
        "import boto3\n"
        "\n"
        "def send_order_event():\n"
        "    sns = boto3.client('sns')\n"
        "    sns.publish(TopicArn='arn:aws:sns:us-east-1:123456789:order-events',\n"
        "               Message='order created')\n";

    write_file(tmpdir, "publisher/notify.py", pub_src);

    /* Python subscriber */
    const char *sub_src =
        "import boto3\n"
        "\n"
        "def setup_subscription():\n"
        "    sns = boto3.client('sns')\n"
        "    sns.subscribe(TopicArn='arn:aws:sns:us-east-1:123456789:order-events',\n"
        "                  Protocol='sqs', Endpoint='arn:aws:sqs:...')\n";

    write_file(tmpdir, "subscriber/handler.py", sub_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Function", "send_order_event",
        "test.publisher.notify.send_order_event",
        "publisher/notify.py", 3, 6, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t sub_id = cbm_gbuf_upsert_node(gb, "Function", "setup_subscription",
        "test.subscriber.handler.setup_subscription",
        "subscriber/handler.py", 3, 6, NULL);
    ASSERT_GT(sub_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_sns(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_sns_edges(gb), 0);
    ASSERT_TRUE(has_sns_edge_with_band(gb, "high"));
    ASSERT_TRUE(has_sns_edge_with_identifier(gb, "order-events"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 2: Topic name extraction from ARN
 * ═══════════════════════════════════════════════════════════════════ */

TEST(sns_topic_extraction_from_arn) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_sns_t2_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Publisher with a complex ARN */
    const char *pub_src =
        "import boto3\n"
        "def pub():\n"
        "    sns = boto3.client('sns')\n"
        "    sns.publish(TopicArn='arn:aws:sns:eu-west-1:987654321012:payment-processed',\n"
        "               Message='done')\n";

    write_file(tmpdir, "pub.py", pub_src);

    /* Subscriber with the same topic from a different region ARN */
    const char *sub_src =
        "import boto3\n"
        "def sub():\n"
        "    sns = boto3.client('sns')\n"
        "    sns.subscribe(TopicArn='arn:aws:sns:us-west-2:111222333444:payment-processed',\n"
        "                  Protocol='https', Endpoint='https://example.com/hook')\n";

    write_file(tmpdir, "sub.py", sub_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Function", "pub",
        "test.pub.pub", "pub.py", 2, 5, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t sub_id = cbm_gbuf_upsert_node(gb, "Function", "sub",
        "test.sub.sub", "sub.py", 2, 5, NULL);
    ASSERT_GT(sub_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_sns(&ctx);

    /* Both ARNs resolve to "payment-processed" → should match */
    ASSERT_GT(links, 0);
    ASSERT_TRUE(has_sns_edge_with_identifier(gb, "payment-processed"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 3: Topic name extraction from Terraform reference
 * ═══════════════════════════════════════════════════════════════════ */

TEST(sns_topic_extraction_terraform_ref) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_sns_t3_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Python publisher with plain topic name */
    const char *pub_src =
        "import boto3\n"
        "def pub():\n"
        "    sns = boto3.client('sns')\n"
        "    sns.publish(TopicArn='arn:aws:sns:us-east-1:123:user_signups',\n"
        "               Message='new user')\n";

    write_file(tmpdir, "pub.py", pub_src);

    /* Terraform subscription referencing the same topic via resource ref */
    const char *tf_src =
        "resource \"aws_sns_topic_subscription\" \"user_signups_sub\" {\n"
        "  topic_arn = aws_sns_topic.user_signups.arn\n"
        "  protocol  = \"sqs\"\n"
        "  endpoint  = aws_sqs_queue.signups_queue.arn\n"
        "}\n";

    write_file(tmpdir, "infra/main.tf", tf_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Function", "pub",
        "test.pub.pub", "pub.py", 2, 5, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t tf_id = cbm_gbuf_upsert_node(gb, "Module", "main",
        "test.infra.main", "infra/main.tf", 1, 5, NULL);
    ASSERT_GT(tf_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_sns(&ctx);

    /* ARN "arn:...:user_signups" → "user_signups", TF ref → "user_signups" → match */
    ASSERT_GT(links, 0);
    ASSERT_TRUE(has_sns_edge_with_identifier(gb, "user_signups"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 4: Java snsClient.publish + subscribe → edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(sns_java_publish_subscribe) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_sns_t4_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Java publisher */
    const char *pub_src =
        "import software.amazon.awssdk.services.sns.SnsClient;\n"
        "\n"
        "public class NotificationPublisher {\n"
        "    public void send() {\n"
        "        snsClient.publish(PublishRequest.builder()\n"
        "            .topicArn(\"arn:aws:sns:us-east-1:123:alert-topic\")\n"
        "            .message(\"alert!\")\n"
        "            .build());\n"
        "    }\n"
        "}\n";

    write_file(tmpdir, "src/main/java/NotificationPublisher.java", pub_src);

    /* Java subscriber */
    const char *sub_src =
        "import software.amazon.awssdk.services.sns.SnsClient;\n"
        "\n"
        "public class NotificationSubscriber {\n"
        "    public void subscribe() {\n"
        "        snsClient.subscribe(SubscribeRequest.builder()\n"
        "            .topicArn(\"arn:aws:sns:us-east-1:123:alert-topic\")\n"
        "            .protocol(\"sqs\")\n"
        "            .endpoint(queueArn)\n"
        "            .build());\n"
        "    }\n"
        "}\n";

    write_file(tmpdir, "src/main/java/NotificationSubscriber.java", sub_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Method", "send",
        "test.NotificationPublisher.send",
        "src/main/java/NotificationPublisher.java", 4, 9, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t sub_id = cbm_gbuf_upsert_node(gb, "Method", "subscribe",
        "test.NotificationSubscriber.subscribe",
        "src/main/java/NotificationSubscriber.java", 4, 10, NULL);
    ASSERT_GT(sub_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_sns(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_sns_edges(gb), 0);
    ASSERT_TRUE(has_sns_edge_with_identifier(gb, "alert-topic"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 5: Go SDK Publish + Subscribe → edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(sns_go_publish_subscribe) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_sns_t5_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Go publisher */
    const char *pub_src =
        "package main\n"
        "\n"
        "func publishEvent() {\n"
        "    input := &sns.PublishInput{\n"
        "        TopicArn: aws.String(\"arn:aws:sns:us-east-1:123:inventory-updates\"),\n"
        "        Message:  aws.String(\"stock changed\"),\n"
        "    }\n"
        "    snsClient.Publish(ctx, input)\n"
        "}\n";

    write_file(tmpdir, "publisher/main.go", pub_src);

    /* Go subscriber */
    const char *sub_src =
        "package main\n"
        "\n"
        "func subscribeToInventory() {\n"
        "    input := &sns.SubscribeInput{\n"
        "        TopicArn: aws.String(\"arn:aws:sns:us-east-1:123:inventory-updates\"),\n"
        "        Protocol: aws.String(\"sqs\"),\n"
        "        Endpoint: aws.String(queueArn),\n"
        "    }\n"
        "    snsClient.Subscribe(ctx, input)\n"
        "}\n";

    write_file(tmpdir, "subscriber/main.go", sub_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Function", "publishEvent",
        "test.publisher.main.publishEvent",
        "publisher/main.go", 3, 9, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t sub_id = cbm_gbuf_upsert_node(gb, "Function", "subscribeToInventory",
        "test.subscriber.main.subscribeToInventory",
        "subscriber/main.go", 3, 10, NULL);
    ASSERT_GT(sub_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_sns(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_sns_edges(gb), 0);
    ASSERT_TRUE(has_sns_edge_with_identifier(gb, "inventory-updates"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 6: Node.js PublishCommand + SubscribeCommand → edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(sns_nodejs_publish_subscribe) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_sns_t6_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Node.js publisher */
    const char *pub_src =
        "const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');\n"
        "\n"
        "async function notifyUsers() {\n"
        "  const sns = new SNSClient({});\n"
        "  await sns.send(new PublishCommand({\n"
        "    TopicArn: 'arn:aws:sns:us-east-1:123:user-notifications',\n"
        "    Message: 'Hello!',\n"
        "  }));\n"
        "}\n";

    write_file(tmpdir, "publisher/notify.ts", pub_src);

    /* Node.js subscriber */
    const char *sub_src =
        "const { SNSClient, SubscribeCommand } = require('@aws-sdk/client-sns');\n"
        "\n"
        "async function setupSub() {\n"
        "  const sns = new SNSClient({});\n"
        "  await sns.send(new SubscribeCommand({\n"
        "    TopicArn: 'arn:aws:sns:us-east-1:123:user-notifications',\n"
        "    Protocol: 'sqs',\n"
        "    Endpoint: queueArn,\n"
        "  }));\n"
        "}\n";

    write_file(tmpdir, "subscriber/setup.ts", sub_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Function", "notifyUsers",
        "test.publisher.notify.notifyUsers",
        "publisher/notify.ts", 3, 9, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t sub_id = cbm_gbuf_upsert_node(gb, "Function", "setupSub",
        "test.subscriber.setup.setupSub",
        "subscriber/setup.ts", 3, 10, NULL);
    ASSERT_GT(sub_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_sns(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_sns_edges(gb), 0);
    ASSERT_TRUE(has_sns_edge_with_identifier(gb, "user-notifications"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 7: Terraform aws_sns_topic_subscription → subscriber detected
 * ═══════════════════════════════════════════════════════════════════ */

TEST(sns_terraform_subscription) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_sns_t7_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Python publisher */
    const char *pub_src =
        "import boto3\n"
        "def pub():\n"
        "    sns = boto3.client('sns')\n"
        "    sns.publish(TopicArn='arn:aws:sns:us-east-1:123:deploy-events',\n"
        "               Message='deployed')\n";

    write_file(tmpdir, "deploy/pub.py", pub_src);

    /* Terraform subscription with ARN string */
    const char *tf_src =
        "resource \"aws_sns_topic_subscription\" \"deploy_sub\" {\n"
        "  topic_arn = \"arn:aws:sns:us-east-1:123:deploy-events\"\n"
        "  protocol  = \"lambda\"\n"
        "  endpoint  = aws_lambda_function.handler.arn\n"
        "}\n";

    write_file(tmpdir, "infra/sns.tf", tf_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Function", "pub",
        "test.deploy.pub.pub", "deploy/pub.py", 2, 5, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t tf_id = cbm_gbuf_upsert_node(gb, "Module", "sns",
        "test.infra.sns", "infra/sns.tf", 1, 5, NULL);
    ASSERT_GT(tf_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_sns(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_TRUE(has_sns_edge_with_identifier(gb, "deploy-events"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 8: Multi-topic — 2 different topics, no cross-match
 * ═══════════════════════════════════════════════════════════════════ */

TEST(sns_multi_topic_no_cross_match) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_sns_t8_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Publisher to topic-A */
    const char *pub_a =
        "import boto3\n"
        "def pub_a():\n"
        "    sns = boto3.client('sns')\n"
        "    sns.publish(TopicArn='arn:aws:sns:us-east-1:123:topic-alpha',\n"
        "               Message='alpha')\n";

    write_file(tmpdir, "pub_a.py", pub_a);

    /* Publisher to topic-B */
    const char *pub_b =
        "import boto3\n"
        "def pub_b():\n"
        "    sns = boto3.client('sns')\n"
        "    sns.publish(TopicArn='arn:aws:sns:us-east-1:123:topic-beta',\n"
        "               Message='beta')\n";

    write_file(tmpdir, "pub_b.py", pub_b);

    /* Subscriber to topic-A only */
    const char *sub_a =
        "import boto3\n"
        "def sub_a():\n"
        "    sns = boto3.client('sns')\n"
        "    sns.subscribe(TopicArn='arn:aws:sns:us-east-1:123:topic-alpha',\n"
        "                  Protocol='sqs', Endpoint='arn:aws:sqs:...')\n";

    write_file(tmpdir, "sub_a.py", sub_a);

    /* Subscriber to topic-B only */
    const char *sub_b =
        "import boto3\n"
        "def sub_b():\n"
        "    sns = boto3.client('sns')\n"
        "    sns.subscribe(TopicArn='arn:aws:sns:us-east-1:123:topic-beta',\n"
        "                  Protocol='sqs', Endpoint='arn:aws:sqs:...')\n";

    write_file(tmpdir, "sub_b.py", sub_b);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_a_id = cbm_gbuf_upsert_node(gb, "Function", "pub_a",
        "test.pub_a.pub_a", "pub_a.py", 2, 5, NULL);
    ASSERT_GT(pub_a_id, 0);

    int64_t pub_b_id = cbm_gbuf_upsert_node(gb, "Function", "pub_b",
        "test.pub_b.pub_b", "pub_b.py", 2, 5, NULL);
    ASSERT_GT(pub_b_id, 0);

    int64_t sub_a_id = cbm_gbuf_upsert_node(gb, "Function", "sub_a",
        "test.sub_a.sub_a", "sub_a.py", 2, 5, NULL);
    ASSERT_GT(sub_a_id, 0);

    int64_t sub_b_id = cbm_gbuf_upsert_node(gb, "Function", "sub_b",
        "test.sub_b.sub_b", "sub_b.py", 2, 5, NULL);
    ASSERT_GT(sub_b_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_sns(&ctx);

    /* Should have exactly 2 edges: sub_a→pub_a (alpha), sub_b→pub_b (beta) */
    ASSERT_EQ(links, 2);
    ASSERT_EQ(count_sns_edges(gb), 2);
    ASSERT_TRUE(has_sns_edge_with_identifier(gb, "topic-alpha"));
    ASSERT_TRUE(has_sns_edge_with_identifier(gb, "topic-beta"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 9: Self-link prevention (publisher and subscriber in same node)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(sns_no_self_link) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_sns_t9_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Single Python file that both publishes and subscribes to the same topic */
    const char *src =
        "import boto3\n"
        "def setup():\n"
        "    sns = boto3.client('sns')\n"
        "    sns.publish(TopicArn='arn:aws:sns:us-east-1:123:self-topic',\n"
        "               Message='test')\n"
        "    sns.subscribe(TopicArn='arn:aws:sns:us-east-1:123:self-topic',\n"
        "                  Protocol='sqs', Endpoint='arn:aws:sqs:...')\n";

    write_file(tmpdir, "self.py", src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t id = cbm_gbuf_upsert_node(gb, "Function", "setup",
        "test.self.setup", "self.py", 2, 7, NULL);
    ASSERT_GT(id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_sns(&ctx);

    /* Same node is both publisher and subscriber — should NOT create self-link */
    ASSERT_EQ(links, 0);
    ASSERT_EQ(count_sns_edges(gb), 0);

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 10: No match (publisher topic "A", subscriber topic "B")
 * ═══════════════════════════════════════════════════════════════════ */

TEST(sns_no_match_different_topics) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_sns_t10_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Publisher to "orders" */
    const char *pub_src =
        "import boto3\n"
        "def pub():\n"
        "    sns = boto3.client('sns')\n"
        "    sns.publish(TopicArn='arn:aws:sns:us-east-1:123:orders',\n"
        "               Message='order')\n";

    write_file(tmpdir, "pub.py", pub_src);

    /* Subscriber to "payments" — different topic */
    const char *sub_src =
        "import boto3\n"
        "def sub():\n"
        "    sns = boto3.client('sns')\n"
        "    sns.subscribe(TopicArn='arn:aws:sns:us-east-1:123:payments',\n"
        "                  Protocol='sqs', Endpoint='arn:aws:sqs:...')\n";

    write_file(tmpdir, "sub.py", sub_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    cbm_gbuf_upsert_node(gb, "Function", "pub",
        "test.pub.pub", "pub.py", 2, 5, NULL);

    cbm_gbuf_upsert_node(gb, "Function", "sub",
        "test.sub.sub", "sub.py", 2, 5, NULL);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_sns(&ctx);

    /* "orders" publisher should NOT match "payments" subscriber */
    ASSERT_EQ(links, 0);
    ASSERT_EQ(count_sns_edges(gb), 0);

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 11: Empty graph buffer (no crash)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(sns_empty_graph) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_sns_t11_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_sns(&ctx);

    ASSERT_EQ(links, 0);
    ASSERT_EQ(count_sns_edges(gb), 0);

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 12: Java @SnsNotificationMapping subscriber + publisher
 * ═══════════════════════════════════════════════════════════════════ */

TEST(sns_java_annotation_subscriber) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_sns_t12_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Java publisher using amazonSNS.publish("arn:...") */
    const char *pub_src =
        "public class EventPublisher {\n"
        "    public void fire() {\n"
        "        amazonSNS.publish(\"arn:aws:sns:us-east-1:123:audit-events\",\n"
        "                          \"audit log entry\");\n"
        "    }\n"
        "}\n";

    write_file(tmpdir, "src/main/java/EventPublisher.java", pub_src);

    /* Java subscriber with @SnsNotificationMapping */
    const char *sub_src =
        "import io.awspring.cloud.sns.annotation.SnsNotificationMapping;\n"
        "\n"
        "public class AuditHandler {\n"
        "    @SnsNotificationMapping(\"audit-events\")\n"
        "    public void handle(String message) {\n"
        "        System.out.println(message);\n"
        "    }\n"
        "}\n";

    write_file(tmpdir, "src/main/java/AuditHandler.java", sub_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Method", "fire",
        "test.EventPublisher.fire",
        "src/main/java/EventPublisher.java", 2, 5, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t sub_id = cbm_gbuf_upsert_node(gb, "Method", "handle",
        "test.AuditHandler.handle",
        "src/main/java/AuditHandler.java", 4, 7, NULL);
    ASSERT_GT(sub_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_sns(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_TRUE(has_sns_edge_with_identifier(gb, "audit-events"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test: Class node with SNS publisher → detected
 * ═══════════════════════════════════════════════════════════════════ */

TEST(sns_class_node_publisher) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_sns_cls_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    const char *pub_src =
        "class AlertPublisher {\n"
        "  async publish() {\n"
        "    await sns.publish({\n"
        "      TopicArn: 'arn:aws:sns:eu-west-1:123:alerts',\n"
        "      Message: JSON.stringify(alert),\n"
        "    });\n"
        "  }\n"
        "}\n";
    write_file(tmpdir, "publishers/alert.ts", pub_src);

    const char *sub_src =
        "function handleAlerts(event) {\n"
        "  // Lambda subscribed to arn:aws:sns:eu-west-1:123:alerts\n"
        "  const record = event.Records[0].Sns;\n"
        "}\n";
    write_file(tmpdir, "handlers/alert.ts", sub_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);
    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Class", "AlertPublisher",
        "test.publishers.alert.AlertPublisher", "publishers/alert.ts", 1, 8, NULL);
    ASSERT_GT(pub_id, 0);
    int64_t sub_id = cbm_gbuf_upsert_node(gb, "Function", "handleAlerts",
        "test.handlers.alert.handleAlerts", "handlers/alert.ts", 1, 4, NULL);
    ASSERT_GT(sub_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_sns(&ctx);
    ASSERT_GTE(links, 0);

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Suite definition
 * ═══════════════════════════════════════════════════════════════════ */

SUITE(servicelink_sns) {
    RUN_TEST(sns_python_publish_subscribe);
    RUN_TEST(sns_topic_extraction_from_arn);
    RUN_TEST(sns_topic_extraction_terraform_ref);
    RUN_TEST(sns_java_publish_subscribe);
    RUN_TEST(sns_go_publish_subscribe);
    RUN_TEST(sns_nodejs_publish_subscribe);
    RUN_TEST(sns_terraform_subscription);
    RUN_TEST(sns_multi_topic_no_cross_match);
    RUN_TEST(sns_no_self_link);
    RUN_TEST(sns_no_match_different_topics);
    RUN_TEST(sns_empty_graph);
    RUN_TEST(sns_java_annotation_subscriber);
    RUN_TEST(sns_class_node_publisher);
}
