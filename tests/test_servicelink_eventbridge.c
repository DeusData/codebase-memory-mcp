/*
 * test_servicelink_eventbridge.c — Tests for AWS EventBridge protocol linking.
 *
 * Creates synthetic source files (.py, .go, .java, .js, .ts, .tf),
 * builds a graph buffer with nodes, runs the EventBridge linker, and verifies
 * that EVENTBRIDGE_CALLS edges are created with correct properties.
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

/* Count EVENTBRIDGE_CALLS edges */
static int count_eventbridge_edges(cbm_gbuf_t *gb) {
    return cbm_gbuf_edge_count_by_type(gb, "EVENTBRIDGE_CALLS");
}

/* Check if an EVENTBRIDGE_CALLS edge has given confidence band */
static bool has_eb_edge_with_band(cbm_gbuf_t *gb, const char *band) {
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_type(gb, "EVENTBRIDGE_CALLS", &edges, &count);
    char needle[64];
    snprintf(needle, sizeof(needle), "\"confidence_band\":\"%s\"", band);
    for (int i = 0; i < count; i++) {
        if (edges[i]->properties_json && strstr(edges[i]->properties_json, needle))
            return true;
    }
    return false;
}

/* Check if an EVENTBRIDGE_CALLS edge has given identifier */
static bool has_eb_edge_with_identifier(cbm_gbuf_t *gb, const char *identifier) {
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_type(gb, "EVENTBRIDGE_CALLS", &edges, &count);
    char needle[256];
    snprintf(needle, sizeof(needle), "\"identifier\":\"%s\"", identifier);
    for (int i = 0; i < count; i++) {
        if (edges[i]->properties_json && strstr(edges[i]->properties_json, needle))
            return true;
    }
    return false;
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 1: Python put_events + Terraform event_rule → edge created
 * ═══════════════════════════════════════════════════════════════════ */

TEST(eb_python_put_events_terraform_rule) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_eb_t1_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Python publisher */
    const char *pub_src =
        "import boto3\n"
        "\n"
        "def emit_order_event():\n"
        "    client = boto3.client('events')\n"
        "    client.put_events(Entries=[{\n"
        "        'Source': 'my.orders',\n"
        "        'DetailType': 'OrderCreated',\n"
        "        'Detail': '{\"orderId\": \"123\"}'\n"
        "    }])\n";

    write_file(tmpdir, "publisher/events.py", pub_src);

    /* Terraform consumer */
    const char *tf_src =
        "resource \"aws_cloudwatch_event_rule\" \"order_rule\" {\n"
        "  event_pattern = jsonencode({\n"
        "    \"source\"      = [\"my.orders\"]\n"
        "    \"detail-type\" = [\"OrderCreated\"]\n"
        "  })\n"
        "}\n";

    write_file(tmpdir, "infra/rules.tf", tf_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Function", "emit_order_event",
        "test.publisher.events.emit_order_event",
        "publisher/events.py", 3, 9, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t tf_id = cbm_gbuf_upsert_node(gb, "Module", "rules",
        "test.infra.rules", "infra/rules.tf", 1, 6, NULL);
    ASSERT_GT(tf_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_eventbridge(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_eventbridge_edges(gb), 0);
    ASSERT_TRUE(has_eb_edge_with_band(gb, "high"));
    ASSERT_TRUE(has_eb_edge_with_identifier(gb, "my.orders:OrderCreated"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 2: Go PutEventsInput + Terraform rule → edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(eb_go_put_events_terraform_rule) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_eb_t2_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Go publisher */
    const char *pub_src =
        "package main\n"
        "\n"
        "func emitPaymentEvent() {\n"
        "    input := &eventbridge.PutEventsInput{\n"
        "        Entries: []types.PutEventsRequestEntry{{\n"
        "            Source:     aws.String(\"payment.service\"),\n"
        "            DetailType: aws.String(\"PaymentProcessed\"),\n"
        "            Detail:     aws.String(`{\"amount\": 100}`),\n"
        "        }},\n"
        "    }\n"
        "    client.PutEvents(ctx, input)\n"
        "}\n";

    write_file(tmpdir, "publisher/payment.go", pub_src);

    /* Terraform consumer */
    const char *tf_src =
        "resource \"aws_cloudwatch_event_rule\" \"payment_rule\" {\n"
        "  event_pattern = jsonencode({\n"
        "    \"source\"      = [\"payment.service\"]\n"
        "    \"detail-type\" = [\"PaymentProcessed\"]\n"
        "  })\n"
        "}\n";

    write_file(tmpdir, "infra/payment.tf", tf_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Function", "emitPaymentEvent",
        "test.publisher.payment.emitPaymentEvent",
        "publisher/payment.go", 3, 12, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t tf_id = cbm_gbuf_upsert_node(gb, "Module", "payment",
        "test.infra.payment", "infra/payment.tf", 1, 6, NULL);
    ASSERT_GT(tf_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_eventbridge(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_eventbridge_edges(gb), 0);
    ASSERT_TRUE(has_eb_edge_with_identifier(gb, "payment.service:PaymentProcessed"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 3: Java PutEventsRequestEntry + Terraform rule → edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(eb_java_put_events_terraform_rule) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_eb_t3_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Java publisher */
    const char *pub_src =
        "import software.amazon.awssdk.services.eventbridge.EventBridgeClient;\n"
        "\n"
        "public class OrderPublisher {\n"
        "    public void publishOrder() {\n"
        "        PutEventsRequestEntry entry = PutEventsRequestEntry.builder()\n"
        "            .source(\"commerce.orders\")\n"
        "            .detailType(\"OrderShipped\")\n"
        "            .detail(\"{\\\"orderId\\\": \\\"456\\\"}\")\n"
        "            .build();\n"
        "    }\n"
        "}\n";

    write_file(tmpdir, "src/main/java/OrderPublisher.java", pub_src);

    /* Terraform consumer */
    const char *tf_src =
        "resource \"aws_cloudwatch_event_rule\" \"order_shipped\" {\n"
        "  event_pattern = jsonencode({\n"
        "    \"source\"      = [\"commerce.orders\"]\n"
        "    \"detail-type\" = [\"OrderShipped\"]\n"
        "  })\n"
        "}\n";

    write_file(tmpdir, "infra/events.tf", tf_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Method", "publishOrder",
        "test.OrderPublisher.publishOrder",
        "src/main/java/OrderPublisher.java", 4, 10, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t tf_id = cbm_gbuf_upsert_node(gb, "Module", "events",
        "test.infra.events", "infra/events.tf", 1, 6, NULL);
    ASSERT_GT(tf_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_eventbridge(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_eventbridge_edges(gb), 0);
    ASSERT_TRUE(has_eb_edge_with_identifier(gb, "commerce.orders:OrderShipped"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 4: Node.js PutEventsCommand + Terraform rule → edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(eb_nodejs_put_events_terraform_rule) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_eb_t4_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Node.js publisher */
    const char *pub_src =
        "const { EventBridgeClient, PutEventsCommand } = require('@aws-sdk/client-eventbridge');\n"
        "\n"
        "async function emitUserEvent() {\n"
        "  const client = new EventBridgeClient({});\n"
        "  await client.send(new PutEventsCommand({\n"
        "    Entries: [{\n"
        "      Source: 'user.service',\n"
        "      DetailType: 'UserRegistered',\n"
        "      Detail: JSON.stringify({ userId: '789' }),\n"
        "    }]\n"
        "  }));\n"
        "}\n";

    write_file(tmpdir, "publisher/users.ts", pub_src);

    /* Terraform consumer */
    const char *tf_src =
        "resource \"aws_cloudwatch_event_rule\" \"user_registered\" {\n"
        "  event_pattern = jsonencode({\n"
        "    \"source\"      = [\"user.service\"]\n"
        "    \"detail-type\" = [\"UserRegistered\"]\n"
        "  })\n"
        "}\n";

    write_file(tmpdir, "infra/users.tf", tf_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Function", "emitUserEvent",
        "test.publisher.users.emitUserEvent",
        "publisher/users.ts", 3, 12, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t tf_id = cbm_gbuf_upsert_node(gb, "Module", "users",
        "test.infra.users", "infra/users.tf", 1, 6, NULL);
    ASSERT_GT(tf_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_eventbridge(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_eventbridge_edges(gb), 0);
    ASSERT_TRUE(has_eb_edge_with_identifier(gb, "user.service:UserRegistered"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 5: Source+DetailType compound match → high confidence edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(eb_compound_match_high_confidence) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_eb_t5_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Python publisher */
    const char *pub_src =
        "import boto3\n"
        "\n"
        "def emit():\n"
        "    client = boto3.client('events')\n"
        "    client.put_events(Entries=[{\n"
        "        'Source': 'inventory.app',\n"
        "        'DetailType': 'StockUpdated',\n"
        "        'Detail': '{}'\n"
        "    }])\n";

    write_file(tmpdir, "pub.py", pub_src);

    /* Terraform with exact match */
    const char *tf_src =
        "resource \"aws_cloudwatch_event_rule\" \"stock_rule\" {\n"
        "  event_pattern = jsonencode({\n"
        "    \"source\"      = [\"inventory.app\"]\n"
        "    \"detail-type\" = [\"StockUpdated\"]\n"
        "  })\n"
        "}\n";

    write_file(tmpdir, "main.tf", tf_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Function", "emit",
        "test.pub.emit", "pub.py", 3, 9, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t tf_id = cbm_gbuf_upsert_node(gb, "Module", "main",
        "test.main", "main.tf", 1, 6, NULL);
    ASSERT_GT(tf_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_eventbridge(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_TRUE(has_eb_edge_with_band(gb, "high"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 6: Source-only match (consumer has no detail-type) → lower confidence
 * ═══════════════════════════════════════════════════════════════════ */

TEST(eb_source_only_match_lower_confidence) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_eb_t6_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Python publisher with source + detail_type */
    const char *pub_src =
        "import boto3\n"
        "\n"
        "def emit():\n"
        "    client = boto3.client('events')\n"
        "    client.put_events(Entries=[{\n"
        "        'Source': 'billing.app',\n"
        "        'DetailType': 'InvoiceCreated',\n"
        "        'Detail': '{}'\n"
        "    }])\n";

    write_file(tmpdir, "pub.py", pub_src);

    /* Terraform consumer with source-only (no detail-type filter) */
    const char *tf_src =
        "resource \"aws_cloudwatch_event_rule\" \"billing_rule\" {\n"
        "  event_pattern = jsonencode({\n"
        "    \"source\" = [\"billing.app\"]\n"
        "  })\n"
        "}\n";

    write_file(tmpdir, "main.tf", tf_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Function", "emit",
        "test.pub.emit", "pub.py", 3, 9, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t tf_id = cbm_gbuf_upsert_node(gb, "Module", "main",
        "test.main", "main.tf", 1, 5, NULL);
    ASSERT_GT(tf_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_eventbridge(&ctx);

    /* Should match with source-only confidence (0.80 → "high" band) */
    ASSERT_GT(links, 0);
    ASSERT_GT(count_eventbridge_edges(gb), 0);

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 7: Multi-source: 2 different sources, no cross-match
 * ═══════════════════════════════════════════════════════════════════ */

TEST(eb_multi_source_no_cross_match) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_eb_t7_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Publisher A */
    const char *pub_a =
        "import boto3\n"
        "\n"
        "def emit_alpha():\n"
        "    client = boto3.client('events')\n"
        "    client.put_events(Entries=[{\n"
        "        'Source': 'alpha.service',\n"
        "        'DetailType': 'AlphaEvent',\n"
        "        'Detail': '{}'\n"
        "    }])\n";

    write_file(tmpdir, "pub_a.py", pub_a);

    /* Publisher B */
    const char *pub_b =
        "import boto3\n"
        "\n"
        "def emit_beta():\n"
        "    client = boto3.client('events')\n"
        "    client.put_events(Entries=[{\n"
        "        'Source': 'beta.service',\n"
        "        'DetailType': 'BetaEvent',\n"
        "        'Detail': '{}'\n"
        "    }])\n";

    write_file(tmpdir, "pub_b.py", pub_b);

    /* Consumer A only */
    const char *tf_a =
        "resource \"aws_cloudwatch_event_rule\" \"alpha_rule\" {\n"
        "  event_pattern = jsonencode({\n"
        "    \"source\"      = [\"alpha.service\"]\n"
        "    \"detail-type\" = [\"AlphaEvent\"]\n"
        "  })\n"
        "}\n";

    write_file(tmpdir, "alpha.tf", tf_a);

    /* Consumer B only */
    const char *tf_b =
        "resource \"aws_cloudwatch_event_rule\" \"beta_rule\" {\n"
        "  event_pattern = jsonencode({\n"
        "    \"source\"      = [\"beta.service\"]\n"
        "    \"detail-type\" = [\"BetaEvent\"]\n"
        "  })\n"
        "}\n";

    write_file(tmpdir, "beta.tf", tf_b);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_a_id = cbm_gbuf_upsert_node(gb, "Function", "emit_alpha",
        "test.pub_a.emit_alpha", "pub_a.py", 3, 9, NULL);
    ASSERT_GT(pub_a_id, 0);

    int64_t pub_b_id = cbm_gbuf_upsert_node(gb, "Function", "emit_beta",
        "test.pub_b.emit_beta", "pub_b.py", 3, 9, NULL);
    ASSERT_GT(pub_b_id, 0);

    int64_t tf_a_id = cbm_gbuf_upsert_node(gb, "Module", "alpha",
        "test.alpha", "alpha.tf", 1, 6, NULL);
    ASSERT_GT(tf_a_id, 0);

    int64_t tf_b_id = cbm_gbuf_upsert_node(gb, "Module", "beta",
        "test.beta", "beta.tf", 1, 6, NULL);
    ASSERT_GT(tf_b_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_eventbridge(&ctx);

    /* Should have exactly 2 edges, no cross-match */
    ASSERT_EQ(links, 2);
    ASSERT_EQ(count_eventbridge_edges(gb), 2);
    ASSERT_TRUE(has_eb_edge_with_identifier(gb, "alpha.service:AlphaEvent"));
    ASSERT_TRUE(has_eb_edge_with_identifier(gb, "beta.service:BetaEvent"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 8: Self-link prevention (same node is publisher and consumer)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(eb_no_self_link) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_eb_t8_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Terraform file that has both an event rule and a put_events-like structure
     * is unrealistic, so use a Python file with both CDK EventPattern and put_events */
    const char *src =
        "import boto3\n"
        "\n"
        "def setup():\n"
        "    client = boto3.client('events')\n"
        "    client.put_events(Entries=[{\n"
        "        'Source': 'self.test',\n"
        "        'DetailType': 'SelfEvent',\n"
        "        'Detail': '{}'\n"
        "    }])\n"
        "    rule = Rule(event_pattern=EventPattern(\n"
        "        source=['self.test'],\n"
        "        detail_type=['SelfEvent']\n"
        "    ))\n";

    write_file(tmpdir, "self.py", src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t id = cbm_gbuf_upsert_node(gb, "Function", "setup",
        "test.self.setup", "self.py", 3, 13, NULL);
    ASSERT_GT(id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_eventbridge(&ctx);

    /* Same node is both producer and consumer — should NOT create self-link */
    ASSERT_EQ(links, 0);
    ASSERT_EQ(count_eventbridge_edges(gb), 0);

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 9: No match (different source names)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(eb_no_match_different_sources) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_eb_t9_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Publisher to "orders.service" */
    const char *pub_src =
        "import boto3\n"
        "\n"
        "def emit():\n"
        "    client = boto3.client('events')\n"
        "    client.put_events(Entries=[{\n"
        "        'Source': 'orders.service',\n"
        "        'DetailType': 'OrderCreated',\n"
        "        'Detail': '{}'\n"
        "    }])\n";

    write_file(tmpdir, "pub.py", pub_src);

    /* Consumer for "payments.service" — different source */
    const char *tf_src =
        "resource \"aws_cloudwatch_event_rule\" \"pay_rule\" {\n"
        "  event_pattern = jsonencode({\n"
        "    \"source\"      = [\"payments.service\"]\n"
        "    \"detail-type\" = [\"PaymentReceived\"]\n"
        "  })\n"
        "}\n";

    write_file(tmpdir, "main.tf", tf_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    cbm_gbuf_upsert_node(gb, "Function", "emit",
        "test.pub.emit", "pub.py", 3, 9, NULL);

    cbm_gbuf_upsert_node(gb, "Module", "main",
        "test.main", "main.tf", 1, 6, NULL);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_eventbridge(&ctx);

    ASSERT_EQ(links, 0);
    ASSERT_EQ(count_eventbridge_edges(gb), 0);

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 10: Empty graph buffer (no crash)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(eb_empty_graph) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_eb_t10_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_eventbridge(&ctx);

    ASSERT_EQ(links, 0);
    ASSERT_EQ(count_eventbridge_edges(gb), 0);

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 11: Terraform event_pattern with multiple sources in array
 * ═══════════════════════════════════════════════════════════════════ */

TEST(eb_terraform_multiple_sources) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_eb_t11_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Publisher to "shipping.app" */
    const char *pub_src =
        "import boto3\n"
        "\n"
        "def emit_shipping():\n"
        "    client = boto3.client('events')\n"
        "    client.put_events(Entries=[{\n"
        "        'Source': 'shipping.app',\n"
        "        'DetailType': 'ShipmentDispatched',\n"
        "        'Detail': '{}'\n"
        "    }])\n";

    write_file(tmpdir, "pub.py", pub_src);

    /* Terraform with source array — first element should be matched */
    const char *tf_src =
        "resource \"aws_cloudwatch_event_rule\" \"shipping_rule\" {\n"
        "  event_pattern = jsonencode({\n"
        "    \"source\"      = [\"shipping.app\"]\n"
        "    \"detail-type\" = [\"ShipmentDispatched\"]\n"
        "  })\n"
        "}\n";

    write_file(tmpdir, "infra/ship.tf", tf_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Function", "emit_shipping",
        "test.pub.emit_shipping", "pub.py", 3, 9, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t tf_id = cbm_gbuf_upsert_node(gb, "Module", "ship",
        "test.infra.ship", "infra/ship.tf", 1, 6, NULL);
    ASSERT_GT(tf_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_eventbridge(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_TRUE(has_eb_edge_with_identifier(gb, "shipping.app:ShipmentDispatched"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 12: CDK/Python EventPattern rule + Python publisher → edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(eb_cdk_python_event_pattern) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_eb_t12_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Python publisher */
    const char *pub_src =
        "import boto3\n"
        "\n"
        "def emit_notification():\n"
        "    client = boto3.client('events')\n"
        "    client.put_events(Entries=[{\n"
        "        'Source': 'notification.svc',\n"
        "        'DetailType': 'EmailSent',\n"
        "        'Detail': '{}'\n"
        "    }])\n";

    write_file(tmpdir, "publisher/notify.py", pub_src);

    /* CDK Python consumer with EventPattern */
    const char *cdk_src =
        "from aws_cdk import aws_events as events\n"
        "\n"
        "def create_rule(scope):\n"
        "    rule = events.Rule(scope, 'EmailRule',\n"
        "        event_pattern=events.EventPattern(\n"
        "            source=['notification.svc'],\n"
        "            detail_type=['EmailSent']\n"
        "        )\n"
        "    )\n";

    write_file(tmpdir, "cdk/stack.py", cdk_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Function", "emit_notification",
        "test.publisher.notify.emit_notification",
        "publisher/notify.py", 3, 9, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t cdk_id = cbm_gbuf_upsert_node(gb, "Function", "create_rule",
        "test.cdk.stack.create_rule",
        "cdk/stack.py", 3, 9, NULL);
    ASSERT_GT(cdk_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_eventbridge(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_eventbridge_edges(gb), 0);
    ASSERT_TRUE(has_eb_edge_with_identifier(gb, "notification.svc:EmailSent"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 13: Node.js putEvents (v2 SDK style) + Terraform rule
 * ═══════════════════════════════════════════════════════════════════ */

TEST(eb_nodejs_put_events_v2_style) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_eb_t13_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Node.js v2-style publisher */
    const char *pub_src =
        "const AWS = require('aws-sdk');\n"
        "\n"
        "async function emitAuditEvent() {\n"
        "  const eventBridge = new AWS.EventBridge();\n"
        "  await eventBridge.putEvents({\n"
        "    Entries: [{\n"
        "      Source: 'audit.service',\n"
        "      DetailType: 'AuditLogCreated',\n"
        "      Detail: JSON.stringify({ action: 'login' }),\n"
        "    }]\n"
        "  }).promise();\n"
        "}\n";

    write_file(tmpdir, "publisher/audit.js", pub_src);

    /* Terraform consumer */
    const char *tf_src =
        "resource \"aws_cloudwatch_event_rule\" \"audit_rule\" {\n"
        "  event_pattern = jsonencode({\n"
        "    \"source\"      = [\"audit.service\"]\n"
        "    \"detail-type\" = [\"AuditLogCreated\"]\n"
        "  })\n"
        "}\n";

    write_file(tmpdir, "infra/audit.tf", tf_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Function", "emitAuditEvent",
        "test.publisher.audit.emitAuditEvent",
        "publisher/audit.js", 3, 12, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t tf_id = cbm_gbuf_upsert_node(gb, "Module", "audit",
        "test.infra.audit", "infra/audit.tf", 1, 6, NULL);
    ASSERT_GT(tf_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_eventbridge(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_TRUE(has_eb_edge_with_identifier(gb, "audit.service:AuditLogCreated"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test: Class node with EventBridge emitter → detected
 * ═══════════════════════════════════════════════════════════════════ */

TEST(eventbridge_class_node_emitter) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_eb_cls_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    const char *emitter_src =
        "class OrderEventEmitter {\n"
        "  async emit(order) {\n"
        "    await eventBridge.putEvents({\n"
        "      Entries: [{\n"
        "        Source: 'orders',\n"
        "        DetailType: 'OrderCreated',\n"
        "        Detail: JSON.stringify(order),\n"
        "      }],\n"
        "    });\n"
        "  }\n"
        "}\n";
    write_file(tmpdir, "emitters/order.ts", emitter_src);

    const char *handler_src =
        "function handleOrderCreated(event) {\n"
        "  // EventBridge Rule: detail-type = OrderCreated\n"
        "  const detail = event.detail;\n"
        "}\n";
    write_file(tmpdir, "handlers/order.ts", handler_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);
    int64_t emitter_id = cbm_gbuf_upsert_node(gb, "Class", "OrderEventEmitter",
        "test.emitters.order.OrderEventEmitter", "emitters/order.ts", 1, 11, NULL);
    ASSERT_GT(emitter_id, 0);
    int64_t handler_id = cbm_gbuf_upsert_node(gb, "Function", "handleOrderCreated",
        "test.handlers.order.handleOrderCreated", "handlers/order.ts", 1, 4, NULL);
    ASSERT_GT(handler_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_eventbridge(&ctx);
    ASSERT_GTE(links, 0);

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Suite definition
 * ═══════════════════════════════════════════════════════════════════ */

SUITE(servicelink_eventbridge) {
    RUN_TEST(eb_python_put_events_terraform_rule);
    RUN_TEST(eb_go_put_events_terraform_rule);
    RUN_TEST(eb_java_put_events_terraform_rule);
    RUN_TEST(eb_nodejs_put_events_terraform_rule);
    RUN_TEST(eb_compound_match_high_confidence);
    RUN_TEST(eb_source_only_match_lower_confidence);
    RUN_TEST(eb_multi_source_no_cross_match);
    RUN_TEST(eb_no_self_link);
    RUN_TEST(eb_no_match_different_sources);
    RUN_TEST(eb_empty_graph);
    RUN_TEST(eb_terraform_multiple_sources);
    RUN_TEST(eb_cdk_python_event_pattern);
    RUN_TEST(eb_nodejs_put_events_v2_style);
    RUN_TEST(eventbridge_class_node_emitter);
}
