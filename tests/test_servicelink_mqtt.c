/*
 * test_servicelink_mqtt.c — Tests for MQTT protocol linking.
 *
 * Creates synthetic source files (.py, .go, .js, .ts, .rs, .c),
 * builds a graph buffer with nodes, runs the MQTT linker, and verifies
 * that MQTT_CALLS edges are created with correct properties.
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

/* Count MQTT_CALLS edges */
static int count_mqtt_edges(cbm_gbuf_t *gb) {
    return cbm_gbuf_edge_count_by_type(gb, "MQTT_CALLS");
}

/* Check if an MQTT_CALLS edge has given identifier */
static bool has_mqtt_edge_with_identifier(cbm_gbuf_t *gb, const char *identifier) {
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_type(gb, "MQTT_CALLS", &edges, &count);
    char needle[256];
    snprintf(needle, sizeof(needle), "\"identifier\":\"%s\"", identifier);
    for (int i = 0; i < count; i++) {
        if (edges[i]->properties_json && strstr(edges[i]->properties_json, needle))
            return true;
    }
    return false;
}

/* Check if an MQTT_CALLS edge has given confidence band */
static bool has_mqtt_edge_with_band(cbm_gbuf_t *gb, const char *band) {
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_type(gb, "MQTT_CALLS", &edges, &count);
    char needle[64];
    snprintf(needle, sizeof(needle), "\"confidence_band\":\"%s\"", band);
    for (int i = 0; i < count; i++) {
        if (edges[i]->properties_json && strstr(edges[i]->properties_json, needle))
            return true;
    }
    return false;
}

/* ── External: mqtt_topic_match declared in servicelink_mqtt.c ── */
extern int mqtt_topic_match(const char *pattern, const char *subject);

/* ═══════════════════════════════════════════════════════════════════
 *  Test 1: Python paho-mqtt publish + subscribe → edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(mqtt_python_publish_subscribe) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_mqtt_t1_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Python publisher */
    const char *pub_src =
        "import paho.mqtt.client as mqtt\n"
        "\n"
        "def send_temperature():\n"
        "    client.publish('sensor/temp', payload='25.3')\n";

    write_file(tmpdir, "publisher/temp.py", pub_src);

    /* Python subscriber */
    const char *sub_src =
        "import paho.mqtt.client as mqtt\n"
        "\n"
        "def on_temp():\n"
        "    client.subscribe('sensor/temp')\n";

    write_file(tmpdir, "subscriber/handler.py", sub_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Function", "send_temperature",
        "test.publisher.temp.send_temperature",
        "publisher/temp.py", 3, 4, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t sub_id = cbm_gbuf_upsert_node(gb, "Function", "on_temp",
        "test.subscriber.handler.on_temp",
        "subscriber/handler.py", 3, 4, NULL);
    ASSERT_GT(sub_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_mqtt(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_mqtt_edges(gb), 0);
    ASSERT_TRUE(has_mqtt_edge_with_identifier(gb, "sensor/temp"));
    ASSERT_TRUE(has_mqtt_edge_with_band(gb, "high"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 2: Go Paho publish + subscribe → edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(mqtt_go_publish_subscribe) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_mqtt_t2_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Go publisher */
    const char *pub_src =
        "package main\n"
        "\n"
        "func publishStatus() {\n"
        "    token := client.Publish(\"device/status\", 0, false, payload)\n"
        "}\n";

    write_file(tmpdir, "publisher/status.go", pub_src);

    /* Go subscriber */
    const char *sub_src =
        "package main\n"
        "\n"
        "func subscribeStatus() {\n"
        "    token := client.Subscribe(\"device/status\", 0, callback)\n"
        "}\n";

    write_file(tmpdir, "subscriber/handler.go", sub_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Function", "publishStatus",
        "test.publisher.status.publishStatus",
        "publisher/status.go", 3, 5, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t sub_id = cbm_gbuf_upsert_node(gb, "Function", "subscribeStatus",
        "test.subscriber.handler.subscribeStatus",
        "subscriber/handler.go", 3, 5, NULL);
    ASSERT_GT(sub_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_mqtt(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_mqtt_edges(gb), 0);
    ASSERT_TRUE(has_mqtt_edge_with_identifier(gb, "device/status"));
    ASSERT_TRUE(has_mqtt_edge_with_band(gb, "high"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 3: Node.js mqtt.js publish + subscribe → edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(mqtt_node_publish_subscribe) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_mqtt_t3_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Node.js publisher */
    const char *pub_src =
        "const mqtt = require('mqtt');\n"
        "\n"
        "function sendAlert() {\n"
        "  client.publish('alerts/fire', 'building-A');\n"
        "}\n";

    write_file(tmpdir, "publisher/alert.js", pub_src);

    /* Node.js subscriber */
    const char *sub_src =
        "const mqtt = require('mqtt');\n"
        "\n"
        "function onAlert() {\n"
        "  client.subscribe('alerts/fire');\n"
        "}\n";

    write_file(tmpdir, "subscriber/handler.js", sub_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Function", "sendAlert",
        "test.publisher.alert.sendAlert",
        "publisher/alert.js", 3, 5, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t sub_id = cbm_gbuf_upsert_node(gb, "Function", "onAlert",
        "test.subscriber.handler.onAlert",
        "subscriber/handler.js", 3, 5, NULL);
    ASSERT_GT(sub_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_mqtt(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_mqtt_edges(gb), 0);
    ASSERT_TRUE(has_mqtt_edge_with_identifier(gb, "alerts/fire"));
    ASSERT_TRUE(has_mqtt_edge_with_band(gb, "high"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 4: '+' single-level wildcard matching
 * ═══════════════════════════════════════════════════════════════════ */

TEST(mqtt_wildcard_plus) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_mqtt_t4_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Publisher to specific topic */
    const char *pub_src =
        "package main\n"
        "\n"
        "func publishTemp() {\n"
        "    client.Publish(\"sensor/temp\", 0, false, payload)\n"
        "}\n";

    write_file(tmpdir, "publisher/temp.go", pub_src);

    /* Subscriber with + wildcard */
    const char *sub_src =
        "package main\n"
        "\n"
        "func subscribeAll() {\n"
        "    client.Subscribe(\"sensor/+\", 0, callback)\n"
        "}\n";

    write_file(tmpdir, "subscriber/handler.go", sub_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Function", "publishTemp",
        "test.publisher.temp.publishTemp",
        "publisher/temp.go", 3, 5, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t sub_id = cbm_gbuf_upsert_node(gb, "Function", "subscribeAll",
        "test.subscriber.handler.subscribeAll",
        "subscriber/handler.go", 3, 5, NULL);
    ASSERT_GT(sub_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_mqtt(&ctx);

    /* sensor/+ should match sensor/temp */
    ASSERT_GT(links, 0);
    ASSERT_GT(count_mqtt_edges(gb), 0);
    ASSERT_TRUE(has_mqtt_edge_with_identifier(gb, "sensor/+"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 5: '#' multi-level wildcard matching
 * ═══════════════════════════════════════════════════════════════════ */

TEST(mqtt_wildcard_hash) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_mqtt_t5_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Publisher to deep topic */
    const char *pub_src =
        "const mqtt = require('mqtt');\n"
        "\n"
        "function sendReading() {\n"
        "  client.publish('home/living/temp/celsius', '22.5');\n"
        "}\n";

    write_file(tmpdir, "publisher/reading.js", pub_src);

    /* Subscriber with # wildcard */
    const char *sub_src =
        "const mqtt = require('mqtt');\n"
        "\n"
        "function onHome() {\n"
        "  client.subscribe('home/#');\n"
        "}\n";

    write_file(tmpdir, "subscriber/handler.js", sub_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Function", "sendReading",
        "test.publisher.reading.sendReading",
        "publisher/reading.js", 3, 5, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t sub_id = cbm_gbuf_upsert_node(gb, "Function", "onHome",
        "test.subscriber.handler.onHome",
        "subscriber/handler.js", 3, 5, NULL);
    ASSERT_GT(sub_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_mqtt(&ctx);

    /* home/# should match home/living/temp/celsius */
    ASSERT_GT(links, 0);
    ASSERT_GT(count_mqtt_edges(gb), 0);
    ASSERT_TRUE(has_mqtt_edge_with_identifier(gb, "home/#"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 6: No match — different topics, no edges
 * ═══════════════════════════════════════════════════════════════════ */

TEST(mqtt_no_match) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_mqtt_t6_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Publisher on one topic */
    const char *pub_src =
        "import paho.mqtt.client as mqtt\n"
        "\n"
        "def send_temp():\n"
        "    client.publish('sensor/temp', payload='25')\n";

    write_file(tmpdir, "publisher/temp.py", pub_src);

    /* Subscriber on completely different topic */
    const char *sub_src =
        "import paho.mqtt.client as mqtt\n"
        "\n"
        "def on_humidity():\n"
        "    client.subscribe('weather/humidity')\n";

    write_file(tmpdir, "subscriber/handler.py", sub_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    cbm_gbuf_upsert_node(gb, "Function", "send_temp",
        "test.publisher.temp.send_temp",
        "publisher/temp.py", 3, 4, NULL);

    cbm_gbuf_upsert_node(gb, "Function", "on_humidity",
        "test.subscriber.handler.on_humidity",
        "subscriber/handler.py", 3, 4, NULL);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_mqtt(&ctx);

    ASSERT_EQ(links, 0);
    ASSERT_EQ(count_mqtt_edges(gb), 0);

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 7: Unit tests for mqtt_topic_match() function
 * ═══════════════════════════════════════════════════════════════════ */

TEST(mqtt_topic_match_unit) {
    /* Exact match */
    ASSERT_EQ(mqtt_topic_match("sensor/temp", "sensor/temp"), 1);
    ASSERT_EQ(mqtt_topic_match("a/b/c", "a/b/c"), 1);

    /* + matches exactly one level */
    ASSERT_EQ(mqtt_topic_match("sensor/+", "sensor/temp"), 1);
    ASSERT_EQ(mqtt_topic_match("+/temp", "sensor/temp"), 1);
    ASSERT_EQ(mqtt_topic_match("sensor/+/data", "sensor/temp/data"), 1);

    /* + does NOT match zero or multiple levels */
    ASSERT_EQ(mqtt_topic_match("sensor/+", "sensor/temp/data"), 0);
    ASSERT_EQ(mqtt_topic_match("sensor/+", "sensor"), 0);

    /* # matches zero or more remaining levels */
    ASSERT_EQ(mqtt_topic_match("sensor/#", "sensor/temp"), 1);
    ASSERT_EQ(mqtt_topic_match("sensor/#", "sensor/temp/data"), 1);
    ASSERT_EQ(mqtt_topic_match("sensor/#", "sensor"), 1);
    ASSERT_EQ(mqtt_topic_match("#", "anything/goes/here"), 1);
    ASSERT_EQ(mqtt_topic_match("#", "single"), 1);

    /* Combined wildcards */
    ASSERT_EQ(mqtt_topic_match("+/+/data", "sensor/temp/data"), 1);
    ASSERT_EQ(mqtt_topic_match("+/+/data", "sensor/temp/info"), 0);

    /* No match */
    ASSERT_EQ(mqtt_topic_match("sensor/temp", "sensor/humidity"), 0);
    ASSERT_EQ(mqtt_topic_match("sensor/temp", "device/temp"), 0);

    /* Edge cases */
    ASSERT_EQ(mqtt_topic_match("a/b", "a/b/c"), 0);
    ASSERT_EQ(mqtt_topic_match("a/b/c", "a/b"), 0);

    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test: Class node with MQTT publisher → detected
 * ═══════════════════════════════════════════════════════════════════ */

TEST(mqtt_class_node_publisher) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_mqtt_cls_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    const char *pub_src =
        "class SensorPublisher {\n"
        "  publish(reading) {\n"
        "    client.publish('sensors/temperature', JSON.stringify(reading));\n"
        "  }\n"
        "}\n";
    write_file(tmpdir, "publishers/sensor.ts", pub_src);

    const char *sub_src =
        "function monitorTemperature() {\n"
        "  client.subscribe('sensors/temperature', (err) => {});\n"
        "}\n";
    write_file(tmpdir, "monitors/sensor.ts", sub_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);
    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Class", "SensorPublisher",
        "test.publishers.sensor.SensorPublisher", "publishers/sensor.ts", 1, 5, NULL);
    ASSERT_GT(pub_id, 0);
    int64_t sub_id = cbm_gbuf_upsert_node(gb, "Function", "monitorTemperature",
        "test.monitors.sensor.monitorTemperature", "monitors/sensor.ts", 1, 3, NULL);
    ASSERT_GT(sub_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_mqtt(&ctx);
    ASSERT_GT(links, 0);
    ASSERT_GT(cbm_gbuf_edge_count_by_type(gb, "MQTT_CALLS"), 0);

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Suite definition
 * ═══════════════════════════════════════════════════════════════════ */

SUITE(servicelink_mqtt) {
    RUN_TEST(mqtt_python_publish_subscribe);
    RUN_TEST(mqtt_go_publish_subscribe);
    RUN_TEST(mqtt_node_publish_subscribe);
    RUN_TEST(mqtt_wildcard_plus);
    RUN_TEST(mqtt_wildcard_hash);
    RUN_TEST(mqtt_no_match);
    RUN_TEST(mqtt_topic_match_unit);
    RUN_TEST(mqtt_class_node_publisher);
}
