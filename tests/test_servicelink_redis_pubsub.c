/*
 * test_servicelink_redis_pubsub.c — Tests for Redis Pub/Sub protocol linking.
 *
 * Creates synthetic source files (.py, .go, .js, .ts),
 * builds a graph buffer with nodes, runs the Redis Pub/Sub linker, and verifies
 * that REDIS_PUBSUB_CALLS edges are created with correct properties.
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

/* Count REDIS_PUBSUB_CALLS edges */
static int count_redis_edges(cbm_gbuf_t *gb) {
    return cbm_gbuf_edge_count_by_type(gb, "REDIS_PUBSUB_CALLS");
}

/* Check if a REDIS_PUBSUB_CALLS edge has given identifier */
static bool has_redis_edge_with_identifier(cbm_gbuf_t *gb, const char *identifier) {
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_type(gb, "REDIS_PUBSUB_CALLS", &edges, &count);
    char needle[256];
    snprintf(needle, sizeof(needle), "\"identifier\":\"%s\"", identifier);
    for (int i = 0; i < count; i++) {
        if (edges[i]->properties_json && strstr(edges[i]->properties_json, needle))
            return true;
    }
    return false;
}

/* Check if a REDIS_PUBSUB_CALLS edge has given confidence band */
static bool has_redis_edge_with_band(cbm_gbuf_t *gb, const char *band) {
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_type(gb, "REDIS_PUBSUB_CALLS", &edges, &count);
    char needle[64];
    snprintf(needle, sizeof(needle), "\"confidence_band\":\"%s\"", band);
    for (int i = 0; i < count; i++) {
        if (edges[i]->properties_json && strstr(edges[i]->properties_json, needle))
            return true;
    }
    return false;
}

/* ── External: redis_glob_match declared in servicelink_redis_pubsub.c ── */
extern int redis_glob_match(const char *pattern, const char *subject);

/* ═══════════════════════════════════════════════════════════════════
 *  Test 1: Python redis.publish + pubsub.subscribe → edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(redis_python_publish_subscribe) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_redis_t1_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Python publisher */
    const char *pub_src =
        "import redis\n"
        "\n"
        "def send_event():\n"
        "    r.publish('events', 'hello world')\n";

    write_file(tmpdir, "publisher/send.py", pub_src);

    /* Python subscriber */
    const char *sub_src =
        "import redis\n"
        "\n"
        "def listen_events():\n"
        "    pubsub.subscribe('events')\n";

    write_file(tmpdir, "consumer/listen.py", sub_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Function", "send_event",
        "test.publisher.send.send_event",
        "publisher/send.py", 3, 4, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t sub_id = cbm_gbuf_upsert_node(gb, "Function", "listen_events",
        "test.consumer.listen.listen_events",
        "consumer/listen.py", 3, 4, NULL);
    ASSERT_GT(sub_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_redis_pubsub(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_redis_edges(gb), 0);
    ASSERT_TRUE(has_redis_edge_with_band(gb, "high"));
    ASSERT_TRUE(has_redis_edge_with_identifier(gb, "events"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 2: Go Publish + Subscribe → edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(redis_go_publish_subscribe) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_redis_t2_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Go publisher */
    const char *pub_src =
        "package main\n"
        "\n"
        "func publishOrder() {\n"
        "    rdb.Publish(ctx, \"orders\", payload)\n"
        "}\n";

    write_file(tmpdir, "publisher/main.go", pub_src);

    /* Go subscriber */
    const char *sub_src =
        "package main\n"
        "\n"
        "func subscribeOrders() {\n"
        "    sub := rdb.Subscribe(ctx, \"orders\")\n"
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
    int links = cbm_servicelink_redis_pubsub(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_redis_edges(gb), 0);
    ASSERT_TRUE(has_redis_edge_with_identifier(gb, "orders"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 3: Node.js publish + subscribe → edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(redis_node_publish_subscribe) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_redis_t3_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Node.js publisher */
    const char *pub_src =
        "async function sendNotification() {\n"
        "  await redis.publish('notifications', JSON.stringify(data));\n"
        "}\n";

    write_file(tmpdir, "publisher/notify.js", pub_src);

    /* Node.js subscriber */
    const char *sub_src =
        "async function listenNotifications() {\n"
        "  await subscriber.subscribe('notifications', (msg) => {\n"
        "    console.log(msg);\n"
        "  });\n"
        "}\n";

    write_file(tmpdir, "consumer/handler.js", sub_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Function", "sendNotification",
        "test.publisher.notify.sendNotification",
        "publisher/notify.js", 1, 3, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t sub_id = cbm_gbuf_upsert_node(gb, "Function", "listenNotifications",
        "test.consumer.handler.listenNotifications",
        "consumer/handler.js", 1, 5, NULL);
    ASSERT_GT(sub_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_redis_pubsub(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_redis_edges(gb), 0);
    ASSERT_TRUE(has_redis_edge_with_identifier(gb, "notifications"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 4: PSUBSCRIBE glob with * — matches any characters
 * ═══════════════════════════════════════════════════════════════════ */

TEST(redis_psubscribe_glob) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_redis_t4_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Python publisher to specific channel */
    const char *pub_src =
        "import redis\n"
        "\n"
        "def publish_news():\n"
        "    r.publish('news.sports', 'goal scored')\n";

    write_file(tmpdir, "publisher/news.py", pub_src);

    /* Python subscriber with glob pattern */
    const char *sub_src =
        "import redis\n"
        "\n"
        "def listen_all_news():\n"
        "    pubsub.psubscribe('news.*')\n";

    write_file(tmpdir, "consumer/all_news.py", sub_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Function", "publish_news",
        "test.publisher.news.publish_news",
        "publisher/news.py", 3, 4, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t sub_id = cbm_gbuf_upsert_node(gb, "Function", "listen_all_news",
        "test.consumer.all_news.listen_all_news",
        "consumer/all_news.py", 3, 4, NULL);
    ASSERT_GT(sub_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_redis_pubsub(&ctx);

    /* news.* should match news.sports (Redis glob, * matches any chars) */
    ASSERT_GT(links, 0);
    ASSERT_GT(count_redis_edges(gb), 0);
    ASSERT_TRUE(has_redis_edge_with_identifier(gb, "news.*"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 5: PSUBSCRIBE glob with ? — single character match
 * ═══════════════════════════════════════════════════════════════════ */

TEST(redis_psubscribe_question_mark) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_redis_t5_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Go publisher */
    const char *pub_src =
        "package main\n"
        "\n"
        "func publishToShard() {\n"
        "    rdb.Publish(ctx, \"shard.3\", payload)\n"
        "}\n";

    write_file(tmpdir, "publisher/main.go", pub_src);

    /* Go subscriber with ? glob */
    const char *sub_src =
        "package main\n"
        "\n"
        "func subscribeAllShards() {\n"
        "    sub := rdb.PSubscribe(ctx, \"shard.?\")\n"
        "}\n";

    write_file(tmpdir, "consumer/main.go", sub_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Function", "publishToShard",
        "test.publisher.main.publishToShard",
        "publisher/main.go", 3, 5, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t sub_id = cbm_gbuf_upsert_node(gb, "Function", "subscribeAllShards",
        "test.consumer.main.subscribeAllShards",
        "consumer/main.go", 3, 5, NULL);
    ASSERT_GT(sub_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_redis_pubsub(&ctx);

    /* shard.? should match shard.3 (? matches one char) */
    ASSERT_GT(links, 0);
    ASSERT_GT(count_redis_edges(gb), 0);
    ASSERT_TRUE(has_redis_edge_with_identifier(gb, "shard.?"));

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 6: No match — different channels, no edges
 * ═══════════════════════════════════════════════════════════════════ */

TEST(redis_no_match) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_redis_t6_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Publisher on channel "orders" */
    const char *pub_src =
        "import redis\n"
        "\n"
        "def send_order():\n"
        "    r.publish('orders', 'order data')\n";

    write_file(tmpdir, "publisher/send.py", pub_src);

    /* Subscriber on channel "payments" — no match */
    const char *sub_src =
        "import redis\n"
        "\n"
        "def listen_payments():\n"
        "    pubsub.subscribe('payments')\n";

    write_file(tmpdir, "consumer/listen.py", sub_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Function", "send_order",
        "test.publisher.send.send_order",
        "publisher/send.py", 3, 4, NULL);
    ASSERT_GT(pub_id, 0);

    int64_t sub_id = cbm_gbuf_upsert_node(gb, "Function", "listen_payments",
        "test.consumer.listen.listen_payments",
        "consumer/listen.py", 3, 4, NULL);
    ASSERT_GT(sub_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_redis_pubsub(&ctx);

    /* Different channels: no edges */
    ASSERT_EQ(links, 0);
    ASSERT_EQ(count_redis_edges(gb), 0);

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 7: Unit tests for redis_glob_match() directly
 * ═══════════════════════════════════════════════════════════════════ */

TEST(redis_glob_match_unit) {
    /* Exact match */
    ASSERT_EQ(redis_glob_match("hello", "hello"), 1);
    ASSERT_EQ(redis_glob_match("hello", "world"), 0);

    /* * matches zero or more characters */
    ASSERT_EQ(redis_glob_match("news.*", "news.sports"), 1);
    ASSERT_EQ(redis_glob_match("news.*", "news."), 1);
    ASSERT_EQ(redis_glob_match("*", "anything"), 1);
    ASSERT_EQ(redis_glob_match("*", ""), 1);
    ASSERT_EQ(redis_glob_match("h*o", "hello"), 1);
    ASSERT_EQ(redis_glob_match("h*o", "ho"), 1);
    ASSERT_EQ(redis_glob_match("h*o", "hx"), 0);

    /* ? matches exactly one character */
    ASSERT_EQ(redis_glob_match("shard.?", "shard.3"), 1);
    ASSERT_EQ(redis_glob_match("shard.?", "shard."), 0);
    ASSERT_EQ(redis_glob_match("shard.?", "shard.12"), 0);
    ASSERT_EQ(redis_glob_match("?", "a"), 1);
    ASSERT_EQ(redis_glob_match("?", ""), 0);

    /* [charset] character class */
    ASSERT_EQ(redis_glob_match("channel.[abc]", "channel.a"), 1);
    ASSERT_EQ(redis_glob_match("channel.[abc]", "channel.b"), 1);
    ASSERT_EQ(redis_glob_match("channel.[abc]", "channel.d"), 0);

    /* Escaped characters */
    ASSERT_EQ(redis_glob_match("hello\\*", "hello*"), 1);
    ASSERT_EQ(redis_glob_match("hello\\*", "helloX"), 0);

    /* Complex patterns */
    ASSERT_EQ(redis_glob_match("user.*.events", "user.123.events"), 1);
    ASSERT_EQ(redis_glob_match("user.*.events", "user..events"), 1);
    ASSERT_EQ(redis_glob_match("user.*.events", "user.events"), 0);

    /* NULL safety */
    ASSERT_EQ(redis_glob_match(NULL, "hello"), 0);
    ASSERT_EQ(redis_glob_match("hello", NULL), 0);

    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test: Class node with Redis pub/sub publisher → detected
 * ═══════════════════════════════════════════════════════════════════ */

TEST(redis_pubsub_class_node_publisher) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_redis_cls_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    const char *pub_src =
        "class CacheInvalidator {\n"
        "  invalidate(key) {\n"
        "    redis.publish('cache-invalidation', JSON.stringify({ key }));\n"
        "  }\n"
        "}\n";
    write_file(tmpdir, "services/cache.ts", pub_src);

    const char *sub_src =
        "function listenInvalidations() {\n"
        "  redis.subscribe('cache-invalidation', (msg) => {});\n"
        "}\n";
    write_file(tmpdir, "listeners/cache.ts", sub_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);
    int64_t pub_id = cbm_gbuf_upsert_node(gb, "Class", "CacheInvalidator",
        "test.services.cache.CacheInvalidator", "services/cache.ts", 1, 5, NULL);
    ASSERT_GT(pub_id, 0);
    int64_t sub_id = cbm_gbuf_upsert_node(gb, "Function", "listenInvalidations",
        "test.listeners.cache.listenInvalidations", "listeners/cache.ts", 1, 3, NULL);
    ASSERT_GT(sub_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_redis_pubsub(&ctx);
    ASSERT_GT(links, 0);
    ASSERT_GT(cbm_gbuf_edge_count_by_type(gb, "REDIS_PUBSUB_CALLS"), 0);

    cbm_gbuf_free(gb);
    rm_rf(tmpdir);
    PASS();
}

/* ── Test suite ──────────────────────────────────────────────────── */

SUITE(servicelink_redis_pubsub) {
    RUN_TEST(redis_python_publish_subscribe);
    RUN_TEST(redis_go_publish_subscribe);
    RUN_TEST(redis_node_publish_subscribe);
    RUN_TEST(redis_psubscribe_glob);
    RUN_TEST(redis_psubscribe_question_mark);
    RUN_TEST(redis_no_match);
    RUN_TEST(redis_glob_match_unit);
    RUN_TEST(redis_pubsub_class_node_publisher);
}
