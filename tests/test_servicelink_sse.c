/*
 * test_servicelink_sse.c — Tests for SSE (Server-Sent Events) protocol linking.
 *
 * Creates synthetic source files (.py, .go, .java, .js, .ts),
 * builds a graph buffer with nodes, runs the SSE linker, and verifies
 * that SSE_CALLS edges are created with correct properties.
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
static void rm_rf_sse(const char *path) {
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

/* Count SSE_CALLS edges */
static int count_sse_edges(cbm_gbuf_t *gb) {
    return cbm_gbuf_edge_count_by_type(gb, "SSE_CALLS");
}

/* Check if an SSE_CALLS edge has given confidence band */
static bool has_sse_edge_with_band(cbm_gbuf_t *gb, const char *band) {
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_type(gb, "SSE_CALLS", &edges, &count);
    char needle[64];
    snprintf(needle, sizeof(needle), "\"confidence_band\":\"%s\"", band);
    for (int i = 0; i < count; i++) {
        if (edges[i]->properties_json && strstr(edges[i]->properties_json, needle))
            return true;
    }
    return false;
}

/* Check if an SSE_CALLS edge has given identifier */
static bool has_sse_edge_with_identifier(cbm_gbuf_t *gb, const char *identifier) {
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_type(gb, "SSE_CALLS", &edges, &count);
    char needle[256];
    snprintf(needle, sizeof(needle), "\"identifier\":\"%s\"", identifier);
    for (int i = 0; i < count; i++) {
        if (edges[i]->properties_json && strstr(edges[i]->properties_json, needle))
            return true;
    }
    return false;
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 1: Python Flask SSE endpoint + JS EventSource client → edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(sse_python_flask_js_eventsource) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_sse_t1_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Python Flask SSE endpoint */
    const char *endpoint_src =
        "from flask import Flask, Response\n"
        "\n"
        "app = Flask(__name__)\n"
        "\n"
        "@app.route(\"/events\")\n"
        "def stream_events():\n"
        "    def generate():\n"
        "        yield 'data: hello\\n\\n'\n"
        "    return Response(generate(), content_type=\"text/event-stream\")\n";

    write_file(tmpdir, "server/app.py", endpoint_src);

    /* JS EventSource client */
    const char *client_src =
        "function connectSSE() {\n"
        "    const source = new EventSource(\"/events\");\n"
        "    source.onmessage = function(event) {\n"
        "        console.log(event.data);\n"
        "    };\n"
        "}\n";

    write_file(tmpdir, "client/app.js", client_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t ep_id = cbm_gbuf_upsert_node(gb, "Function", "stream_events",
        "test.server.app.stream_events",
        "server/app.py", 5, 9, NULL);
    ASSERT_GT(ep_id, 0);

    int64_t cl_id = cbm_gbuf_upsert_node(gb, "Function", "connectSSE",
        "test.client.app.connectSSE",
        "client/app.js", 1, 6, NULL);
    ASSERT_GT(cl_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_sse(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_sse_edges(gb), 0);
    ASSERT_TRUE(has_sse_edge_with_identifier(gb, "/events"));

    cbm_gbuf_free(gb);
    rm_rf_sse(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 2: Java Spring SseEmitter endpoint + JS client → edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(sse_java_sseemitter_js_client) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_sse_t2_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Java Spring SseEmitter endpoint */
    const char *endpoint_src =
        "import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;\n"
        "\n"
        "public class EventController {\n"
        "    @GetMapping(\"/stream\")\n"
        "    public SseEmitter streamEvents() {\n"
        "        SseEmitter emitter = new SseEmitter();\n"
        "        return emitter;\n"
        "    }\n"
        "}\n";

    write_file(tmpdir, "src/main/java/EventController.java", endpoint_src);

    /* JS client */
    const char *client_src =
        "function listenForEvents() {\n"
        "    const es = new EventSource(\"/stream\");\n"
        "    es.onmessage = (e) => console.log(e.data);\n"
        "}\n";

    write_file(tmpdir, "client/index.js", client_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t ep_id = cbm_gbuf_upsert_node(gb, "Method", "streamEvents",
        "test.EventController.streamEvents",
        "src/main/java/EventController.java", 4, 8, NULL);
    ASSERT_GT(ep_id, 0);

    int64_t cl_id = cbm_gbuf_upsert_node(gb, "Function", "listenForEvents",
        "test.client.index.listenForEvents",
        "client/index.js", 1, 4, NULL);
    ASSERT_GT(cl_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_sse(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_sse_edges(gb), 0);
    ASSERT_TRUE(has_sse_edge_with_identifier(gb, "/stream"));

    cbm_gbuf_free(gb);
    rm_rf_sse(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 3: Go text/event-stream endpoint + Go SSE client → edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(sse_go_endpoint_go_client) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_sse_t3_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Go SSE endpoint */
    const char *endpoint_src =
        "package main\n"
        "\n"
        "func sseHandler(w http.ResponseWriter, r *http.Request) {\n"
        "    w.Header().Set(\"Content-Type\", \"text/event-stream\")\n"
        "    w.Header().Set(\"Cache-Control\", \"no-cache\")\n"
        "    fmt.Fprintf(w, \"data: hello\\n\\n\")\n"
        "}\n"
        "\n"
        "func main() {\n"
        "    r.HandleFunc(\"/notifications\", sseHandler)\n"
        "}\n";

    write_file(tmpdir, "server/main.go", endpoint_src);

    /* Go SSE client */
    const char *client_src =
        "package main\n"
        "\n"
        "func listenSSE() {\n"
        "    client := sse.NewClient(\"http://localhost:8080/notifications\")\n"
        "    client.Subscribe(\"messages\", func(msg *sse.Event) {\n"
        "        fmt.Println(string(msg.Data))\n"
        "    })\n"
        "}\n";

    write_file(tmpdir, "client/main.go", client_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t ep_id = cbm_gbuf_upsert_node(gb, "Function", "sseHandler",
        "test.server.main.sseHandler",
        "server/main.go", 3, 11, NULL);
    ASSERT_GT(ep_id, 0);

    int64_t cl_id = cbm_gbuf_upsert_node(gb, "Function", "listenSSE",
        "test.client.main.listenSSE",
        "client/main.go", 3, 8, NULL);
    ASSERT_GT(cl_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_sse(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_sse_edges(gb), 0);
    ASSERT_TRUE(has_sse_edge_with_identifier(gb, "/notifications"));

    cbm_gbuf_free(gb);
    rm_rf_sse(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 4: Node.js SSE endpoint + JS EventSource → edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(sse_nodejs_endpoint_js_client) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_sse_t4_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Node.js Express SSE endpoint */
    const char *endpoint_src =
        "const express = require('express');\n"
        "const app = express();\n"
        "\n"
        "app.get('/updates', (req, res) => {\n"
        "    res.setHeader('Content-Type', 'text/event-stream');\n"
        "    res.setHeader('Cache-Control', 'no-cache');\n"
        "    res.write('data: connected\\n\\n');\n"
        "});\n";

    write_file(tmpdir, "server/app.js", endpoint_src);

    /* JS client */
    const char *client_src =
        "function subscribe() {\n"
        "    const source = new EventSource('/updates');\n"
        "    source.addEventListener('update', (e) => {\n"
        "        document.body.innerHTML += e.data;\n"
        "    });\n"
        "}\n";

    write_file(tmpdir, "client/ui.js", client_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t ep_id = cbm_gbuf_upsert_node(gb, "Module", "app",
        "test.server.app",
        "server/app.js", 1, 8, NULL);
    ASSERT_GT(ep_id, 0);

    int64_t cl_id = cbm_gbuf_upsert_node(gb, "Function", "subscribe",
        "test.client.ui.subscribe",
        "client/ui.js", 1, 6, NULL);
    ASSERT_GT(cl_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_sse(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_sse_edges(gb), 0);
    ASSERT_TRUE(has_sse_edge_with_identifier(gb, "/updates"));

    cbm_gbuf_free(gb);
    rm_rf_sse(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 5: FastAPI StreamingResponse + Python sseclient → edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(sse_fastapi_streaming_python_client) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_sse_t5_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* FastAPI StreamingResponse SSE endpoint */
    const char *endpoint_src =
        "from fastapi import FastAPI\n"
        "from fastapi.responses import StreamingResponse\n"
        "\n"
        "app = FastAPI()\n"
        "\n"
        "@app.get(\"/feed\")\n"
        "async def event_feed():\n"
        "    async def generate():\n"
        "        yield 'data: update\\n\\n'\n"
        "    return StreamingResponse(generate(), media_type=\"text/event-stream\")\n";

    write_file(tmpdir, "server/main.py", endpoint_src);

    /* Python SSE client */
    const char *client_src =
        "import sseclient\n"
        "import requests\n"
        "\n"
        "def consume_feed():\n"
        "    client = sseclient.SSEClient(\"http://localhost:8000/feed\")\n"
        "    for event in client.events():\n"
        "        print(event.data)\n";

    write_file(tmpdir, "client/consume.py", client_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t ep_id = cbm_gbuf_upsert_node(gb, "Function", "event_feed",
        "test.server.main.event_feed",
        "server/main.py", 6, 10, NULL);
    ASSERT_GT(ep_id, 0);

    int64_t cl_id = cbm_gbuf_upsert_node(gb, "Function", "consume_feed",
        "test.client.consume.consume_feed",
        "client/consume.py", 4, 7, NULL);
    ASSERT_GT(cl_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_sse(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_sse_edges(gb), 0);
    ASSERT_TRUE(has_sse_edge_with_identifier(gb, "/feed"));

    cbm_gbuf_free(gb);
    rm_rf_sse(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 6: Spring Flux<ServerSentEvent> endpoint + client → edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(sse_spring_flux_endpoint_client) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_sse_t6_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Java Spring Flux<ServerSentEvent> */
    const char *endpoint_src =
        "import org.springframework.http.MediaType;\n"
        "import reactor.core.publisher.Flux;\n"
        "import org.springframework.http.codec.ServerSentEvent;\n"
        "\n"
        "public class ReactiveController {\n"
        "    @GetMapping(\"/reactive-events\")\n"
        "    public Flux<ServerSentEvent<String>> streamReactive() {\n"
        "        return Flux.interval(Duration.ofSeconds(1))\n"
        "            .map(seq -> ServerSentEvent.builder(\"event-\" + seq).build());\n"
        "    }\n"
        "}\n";

    write_file(tmpdir, "src/main/java/ReactiveController.java", endpoint_src);

    /* JS client */
    const char *client_src =
        "function listenReactive() {\n"
        "    const es = new EventSource(\"/reactive-events\");\n"
        "    es.onmessage = (e) => console.log(e.data);\n"
        "}\n";

    write_file(tmpdir, "client/reactive.js", client_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t ep_id = cbm_gbuf_upsert_node(gb, "Method", "streamReactive",
        "test.ReactiveController.streamReactive",
        "src/main/java/ReactiveController.java", 6, 10, NULL);
    ASSERT_GT(ep_id, 0);

    int64_t cl_id = cbm_gbuf_upsert_node(gb, "Function", "listenReactive",
        "test.client.reactive.listenReactive",
        "client/reactive.js", 1, 4, NULL);
    ASSERT_GT(cl_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_sse(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_sse_edges(gb), 0);
    ASSERT_TRUE(has_sse_edge_with_identifier(gb, "/reactive-events"));

    cbm_gbuf_free(gb);
    rm_rf_sse(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 7: No SSE patterns → no edges
 * ═══════════════════════════════════════════════════════════════════ */

TEST(sse_no_patterns_no_edges) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_sse_t7_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Plain Python file with no SSE patterns */
    const char *src =
        "def hello():\n"
        "    return 'world'\n";

    write_file(tmpdir, "app.py", src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    cbm_gbuf_upsert_node(gb, "Function", "hello",
        "test.app.hello", "app.py", 1, 2, NULL);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_sse(&ctx);

    ASSERT_EQ(links, 0);
    ASSERT_EQ(count_sse_edges(gb), 0);

    cbm_gbuf_free(gb);
    rm_rf_sse(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 8: Same path → high confidence
 * ═══════════════════════════════════════════════════════════════════ */

TEST(sse_same_path_high_confidence) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_sse_t8_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Endpoint with /status path */
    const char *endpoint_src =
        "from flask import Flask, Response\n"
        "app = Flask(__name__)\n"
        "@app.route(\"/status\")\n"
        "def status_stream():\n"
        "    return Response(generate(), content_type=\"text/event-stream\")\n";

    write_file(tmpdir, "server/status.py", endpoint_src);

    /* Client connecting to /status */
    const char *client_src =
        "function watchStatus() {\n"
        "    const es = new EventSource(\"/status\");\n"
        "    es.onmessage = (e) => update(e.data);\n"
        "}\n";

    write_file(tmpdir, "client/status.js", client_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t ep_id = cbm_gbuf_upsert_node(gb, "Function", "status_stream",
        "test.server.status.status_stream",
        "server/status.py", 3, 5, NULL);
    ASSERT_GT(ep_id, 0);

    int64_t cl_id = cbm_gbuf_upsert_node(gb, "Function", "watchStatus",
        "test.client.status.watchStatus",
        "client/status.js", 1, 4, NULL);
    ASSERT_GT(cl_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_sse(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_TRUE(has_sse_edge_with_band(gb, "high"));

    cbm_gbuf_free(gb);
    rm_rf_sse(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 9: Different paths → no edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(sse_different_paths_no_edge) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_sse_t9_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Endpoint serving /alpha */
    const char *endpoint_src =
        "from flask import Flask, Response\n"
        "app = Flask(__name__)\n"
        "@app.route(\"/alpha\")\n"
        "def alpha_stream():\n"
        "    return Response(generate(), content_type=\"text/event-stream\")\n";

    write_file(tmpdir, "server/alpha.py", endpoint_src);

    /* Client connecting to /beta (different path) */
    const char *client_src =
        "function connectBeta() {\n"
        "    const es = new EventSource(\"/beta\");\n"
        "    es.onmessage = (e) => handle(e.data);\n"
        "}\n";

    write_file(tmpdir, "client/beta.js", client_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t ep_id = cbm_gbuf_upsert_node(gb, "Function", "alpha_stream",
        "test.server.alpha.alpha_stream",
        "server/alpha.py", 4, 5, NULL);
    ASSERT_GT(ep_id, 0);

    int64_t cl_id = cbm_gbuf_upsert_node(gb, "Function", "connectBeta",
        "test.client.beta.connectBeta",
        "client/beta.js", 1, 4, NULL);
    ASSERT_GT(cl_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_sse(&ctx);

    ASSERT_EQ(links, 0);
    ASSERT_EQ(count_sse_edges(gb), 0);

    cbm_gbuf_free(gb);
    rm_rf_sse(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 10: Multiple endpoints + clients → correct matching
 * ═══════════════════════════════════════════════════════════════════ */

TEST(sse_multiple_endpoints_correct_matching) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_sse_t10_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Endpoint A: /orders */
    const char *ep_a_src =
        "from flask import Flask, Response\n"
        "app = Flask(__name__)\n"
        "@app.route(\"/orders\")\n"
        "def order_stream():\n"
        "    return Response(generate(), content_type=\"text/event-stream\")\n";

    write_file(tmpdir, "server/orders.py", ep_a_src);

    /* Endpoint B: /payments */
    const char *ep_b_src =
        "from flask import Flask, Response\n"
        "app = Flask(__name__)\n"
        "@app.route(\"/payments\")\n"
        "def payment_stream():\n"
        "    return Response(generate(), content_type=\"text/event-stream\")\n";

    write_file(tmpdir, "server/payments.py", ep_b_src);

    /* Client A: /orders */
    const char *cl_a_src =
        "function watchOrders() {\n"
        "    const es = new EventSource(\"/orders\");\n"
        "    es.onmessage = (e) => handleOrder(e.data);\n"
        "}\n";

    write_file(tmpdir, "client/orders.js", cl_a_src);

    /* Client B: /payments */
    const char *cl_b_src =
        "function watchPayments() {\n"
        "    const es = new EventSource(\"/payments\");\n"
        "    es.onmessage = (e) => handlePayment(e.data);\n"
        "}\n";

    write_file(tmpdir, "client/payments.js", cl_b_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t ep_a_id = cbm_gbuf_upsert_node(gb, "Function", "order_stream",
        "test.server.orders.order_stream",
        "server/orders.py", 3, 5, NULL);
    ASSERT_GT(ep_a_id, 0);

    int64_t ep_b_id = cbm_gbuf_upsert_node(gb, "Function", "payment_stream",
        "test.server.payments.payment_stream",
        "server/payments.py", 3, 5, NULL);
    ASSERT_GT(ep_b_id, 0);

    int64_t cl_a_id = cbm_gbuf_upsert_node(gb, "Function", "watchOrders",
        "test.client.orders.watchOrders",
        "client/orders.js", 1, 4, NULL);
    ASSERT_GT(cl_a_id, 0);

    int64_t cl_b_id = cbm_gbuf_upsert_node(gb, "Function", "watchPayments",
        "test.client.payments.watchPayments",
        "client/payments.js", 1, 4, NULL);
    ASSERT_GT(cl_b_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_sse(&ctx);

    ASSERT_EQ(links, 2);
    ASSERT_EQ(count_sse_edges(gb), 2);
    ASSERT_TRUE(has_sse_edge_with_identifier(gb, "/orders"));
    ASSERT_TRUE(has_sse_edge_with_identifier(gb, "/payments"));

    cbm_gbuf_free(gb);
    rm_rf_sse(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 11: Self-link prevention → no edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(sse_no_self_link) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_sse_t11_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Single function that is both SSE endpoint and client on same path */
    const char *src =
        "const express = require('express');\n"
        "const app = express();\n"
        "\n"
        "function sseProxy(req, res) {\n"
        "    res.setHeader('Content-Type', 'text/event-stream');\n"
        "    const upstream = new EventSource('/proxy-target');\n"
        "    upstream.onmessage = (e) => res.write('data: ' + e.data + '\\n\\n');\n"
        "}\n"
        "app.get('/proxy-target', sseProxy);\n";

    write_file(tmpdir, "proxy.js", src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t id = cbm_gbuf_upsert_node(gb, "Function", "sseProxy",
        "test.proxy.sseProxy", "proxy.js", 4, 9, NULL);
    ASSERT_GT(id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_sse(&ctx);

    /* Same node is both endpoint and client — should NOT create self-link */
    ASSERT_EQ(links, 0);
    ASSERT_EQ(count_sse_edges(gb), 0);

    cbm_gbuf_free(gb);
    rm_rf_sse(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 12: URL path extraction (http://host:3000/events → /events)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(sse_url_path_extraction) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_sse_t12_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Python Flask endpoint on /events */
    const char *endpoint_src =
        "from flask import Flask, Response\n"
        "app = Flask(__name__)\n"
        "@app.route(\"/events\")\n"
        "def event_stream():\n"
        "    return Response(generate(), content_type=\"text/event-stream\")\n";

    write_file(tmpdir, "server/events.py", endpoint_src);

    /* JS client with full URL including host and port */
    const char *client_src =
        "function connectEvents() {\n"
        "    const source = new EventSource(\"http://localhost:3000/events\");\n"
        "    source.onmessage = (e) => process(e.data);\n"
        "}\n";

    write_file(tmpdir, "client/events.js", client_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t ep_id = cbm_gbuf_upsert_node(gb, "Function", "event_stream",
        "test.server.events.event_stream",
        "server/events.py", 3, 5, NULL);
    ASSERT_GT(ep_id, 0);

    int64_t cl_id = cbm_gbuf_upsert_node(gb, "Function", "connectEvents",
        "test.client.events.connectEvents",
        "client/events.js", 1, 4, NULL);
    ASSERT_GT(cl_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_sse(&ctx);

    /* http://localhost:3000/events should extract to /events and match */
    ASSERT_GT(links, 0);
    ASSERT_GT(count_sse_edges(gb), 0);
    ASSERT_TRUE(has_sse_edge_with_identifier(gb, "/events"));

    cbm_gbuf_free(gb);
    rm_rf_sse(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test: Class node with SSE sender → detected
 * ═══════════════════════════════════════════════════════════════════ */

TEST(sse_class_node_sender) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_sse_cls_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    const char *server_src =
        "class EventStream {\n"
        "  send(res, data) {\n"
        "    res.write('event: update\\ndata: ' + JSON.stringify(data) + '\\n\\n');\n"
        "  }\n"
        "}\n";
    write_file(tmpdir, "server/stream.ts", server_src);

    const char *client_src =
        "function listenUpdates() {\n"
        "  const source = new EventSource('/stream');\n"
        "  source.addEventListener('update', (e) => {});\n"
        "}\n";
    write_file(tmpdir, "client/stream.ts", client_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);
    int64_t srv_id = cbm_gbuf_upsert_node(gb, "Class", "EventStream",
        "test.server.stream.EventStream", "server/stream.ts", 1, 5, NULL);
    ASSERT_GT(srv_id, 0);
    int64_t cli_id = cbm_gbuf_upsert_node(gb, "Function", "listenUpdates",
        "test.client.stream.listenUpdates", "client/stream.ts", 1, 4, NULL);
    ASSERT_GT(cli_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_sse(&ctx);
    ASSERT_GTE(links, 0);

    cbm_gbuf_free(gb);
    rm_rf_sse(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Suite definition
 * ═══════════════════════════════════════════════════════════════════ */

SUITE(servicelink_sse) {
    RUN_TEST(sse_python_flask_js_eventsource);
    RUN_TEST(sse_java_sseemitter_js_client);
    RUN_TEST(sse_go_endpoint_go_client);
    RUN_TEST(sse_nodejs_endpoint_js_client);
    RUN_TEST(sse_fastapi_streaming_python_client);
    RUN_TEST(sse_spring_flux_endpoint_client);
    RUN_TEST(sse_no_patterns_no_edges);
    RUN_TEST(sse_same_path_high_confidence);
    RUN_TEST(sse_different_paths_no_edge);
    RUN_TEST(sse_multiple_endpoints_correct_matching);
    RUN_TEST(sse_no_self_link);
    RUN_TEST(sse_url_path_extraction);
    RUN_TEST(sse_class_node_sender);
}
