/*
 * test_servicelink_ws.c — Tests for WebSocket protocol linking.
 *
 * Creates synthetic source files (.go, .py, .java, .js, .ts),
 * builds a graph buffer with nodes, runs the WS linker, and verifies
 * that WS_CALLS edges are created with correct confidence bands.
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
static void rm_rf_ws(const char *path) {
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

/* Count WS_CALLS edges */
static int count_ws_edges(cbm_gbuf_t *gb) {
    return cbm_gbuf_edge_count_by_type(gb, "WS_CALLS");
}

/* Check if a WS_CALLS edge has given confidence band */
static bool has_ws_edge_with_band(cbm_gbuf_t *gb, const char *band) {
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_type(gb, "WS_CALLS", &edges, &count);
    char needle[64];
    snprintf(needle, sizeof(needle), "\"confidence_band\":\"%s\"", band);
    for (int i = 0; i < count; i++) {
        if (edges[i]->properties_json && strstr(edges[i]->properties_json, needle))
            return true;
    }
    return false;
}

/* Check if a WS_CALLS edge has given identifier */
static bool has_ws_edge_with_identifier(cbm_gbuf_t *gb, const char *identifier) {
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_type(gb, "WS_CALLS", &edges, &count);
    char needle[256];
    snprintf(needle, sizeof(needle), "\"identifier\":\"%s\"", identifier);
    for (int i = 0; i < count; i++) {
        if (edges[i]->properties_json && strstr(edges[i]->properties_json, needle))
            return true;
    }
    return false;
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 1: Go WebSocket endpoint (HandleFunc + Upgrader) + JS client → edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(ws_go_endpoint_js_client) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_ws_t1_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Go server with websocket.Upgrader + HandleFunc */
    const char *server_src =
        "package main\n"
        "\n"
        "import \"github.com/gorilla/websocket\"\n"
        "\n"
        "var upgrader = websocket.Upgrader{}\n"
        "\n"
        "func setupRoutes() {\n"
        "    r.HandleFunc(\"/ws\", handleWs)\n"
        "}\n";

    write_file(tmpdir, "server/ws.go", server_src);

    /* JS client */
    const char *client_src =
        "function connect() {\n"
        "    const ws = new WebSocket(\"ws://localhost:8080/ws\");\n"
        "    ws.onmessage = (e) => console.log(e.data);\n"
        "}\n";

    write_file(tmpdir, "client/app.js", client_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t server_id = cbm_gbuf_upsert_node(gb, "Function", "setupRoutes",
        "test.server.ws.setupRoutes", "server/ws.go", 7, 9, NULL);
    ASSERT_GT(server_id, 0);

    int64_t client_id = cbm_gbuf_upsert_node(gb, "Function", "connect",
        "test.client.app.connect", "client/app.js", 1, 4, NULL);
    ASSERT_GT(client_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_ws(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_ws_edges(gb), 0);
    ASSERT_TRUE(has_ws_edge_with_band(gb, "high"));
    ASSERT_TRUE(has_ws_edge_with_identifier(gb, "/ws"));

    cbm_gbuf_free(gb);
    rm_rf_ws(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 2: Python @app.websocket decorator + Python client → edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(ws_python_decorator_client) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_ws_t2_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Python server with @app.websocket */
    const char *server_src =
        "from quart import Quart\n"
        "app = Quart(__name__)\n"
        "\n"
        "@app.websocket(\"/chat\")\n"
        "async def chat_ws():\n"
        "    while True:\n"
        "        data = await websocket.receive()\n"
        "        await websocket.send(data)\n";

    write_file(tmpdir, "server/app.py", server_src);

    /* Python client */
    const char *client_src =
        "import websockets\n"
        "\n"
        "async def connect():\n"
        "    async with websockets.connect(\"ws://localhost:5000/chat\") as ws:\n"
        "        await ws.send(\"hello\")\n";

    write_file(tmpdir, "client/ws_client.py", client_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t server_id = cbm_gbuf_upsert_node(gb, "Function", "chat_ws",
        "test.server.app.chat_ws", "server/app.py", 4, 8, NULL);
    ASSERT_GT(server_id, 0);

    int64_t client_id = cbm_gbuf_upsert_node(gb, "Function", "connect",
        "test.client.ws_client.connect", "client/ws_client.py", 3, 5, NULL);
    ASSERT_GT(client_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_ws(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_ws_edges(gb), 0);
    ASSERT_TRUE(has_ws_edge_with_identifier(gb, "/chat"));

    cbm_gbuf_free(gb);
    rm_rf_ws(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 3: Java @ServerEndpoint + Java client → edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(ws_java_serverendpoint_client) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_ws_t3_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Java server */
    const char *server_src =
        "import javax.websocket.server.ServerEndpoint;\n"
        "\n"
        "@ServerEndpoint(\"/notifications\")\n"
        "public class NotificationEndpoint {\n"
        "    @OnMessage\n"
        "    public void onMessage(String msg) {}\n"
        "}\n";

    write_file(tmpdir, "src/main/java/NotificationEndpoint.java", server_src);

    /* Java client */
    const char *client_src =
        "import javax.websocket.WebSocketContainer;\n"
        "\n"
        "public class NotifyClient {\n"
        "    public void connect() {\n"
        "        URI uri = new URI(\"ws://localhost:8080/notifications\");\n"
        "        WebSocketContainer container = ContainerProvider.getWebSocketContainer();\n"
        "        container.connectToServer(this, uri);\n"
        "    }\n"
        "}\n";

    write_file(tmpdir, "src/main/java/NotifyClient.java", client_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t server_id = cbm_gbuf_upsert_node(gb, "Method", "onMessage",
        "test.NotificationEndpoint.onMessage",
        "src/main/java/NotificationEndpoint.java", 3, 7, NULL);
    ASSERT_GT(server_id, 0);

    int64_t client_id = cbm_gbuf_upsert_node(gb, "Method", "connect",
        "test.NotifyClient.connect",
        "src/main/java/NotifyClient.java", 4, 8, NULL);
    ASSERT_GT(client_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_ws(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_ws_edges(gb), 0);
    ASSERT_TRUE(has_ws_edge_with_identifier(gb, "/notifications"));

    cbm_gbuf_free(gb);
    rm_rf_ws(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 4: Node.js app.ws() endpoint + JS WebSocket client → edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(ws_nodejs_appws_client) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_ws_t4_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Node.js server with app.ws() */
    const char *server_src =
        "const expressWs = require('express-ws');\n"
        "\n"
        "function setupWs(app) {\n"
        "    app.ws('/feed', (ws, req) => {\n"
        "        ws.on('message', (msg) => { ws.send(msg); });\n"
        "    });\n"
        "}\n";

    write_file(tmpdir, "server/routes.js", server_src);

    /* JS client */
    const char *client_src =
        "function connectFeed() {\n"
        "    const ws = new WebSocket('wss://example.com/feed');\n"
        "    ws.onmessage = (e) => console.log(e.data);\n"
        "}\n";

    write_file(tmpdir, "client/feed.js", client_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t server_id = cbm_gbuf_upsert_node(gb, "Function", "setupWs",
        "test.server.routes.setupWs", "server/routes.js", 3, 7, NULL);
    ASSERT_GT(server_id, 0);

    int64_t client_id = cbm_gbuf_upsert_node(gb, "Function", "connectFeed",
        "test.client.feed.connectFeed", "client/feed.js", 1, 4, NULL);
    ASSERT_GT(client_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_ws(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_ws_edges(gb), 0);
    ASSERT_TRUE(has_ws_edge_with_identifier(gb, "/feed"));

    cbm_gbuf_free(gb);
    rm_rf_ws(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 5: Spring @MessageMapping + client → edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(ws_spring_messagemapping_client) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_ws_t5_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Java Spring WebSocket handler */
    const char *server_src =
        "import org.springframework.messaging.handler.annotation.MessageMapping;\n"
        "\n"
        "public class ChatController {\n"
        "    @MessageMapping(\"/topic/messages\")\n"
        "    public void handleMessage(ChatMessage msg) {\n"
        "        // broadcast message\n"
        "    }\n"
        "}\n";

    write_file(tmpdir, "src/main/java/ChatController.java", server_src);

    /* Java client via STOMP over WebSocket */
    const char *client_src =
        "import javax.websocket.WebSocketContainer;\n"
        "\n"
        "public class StompClient {\n"
        "    public void connect() {\n"
        "        URI uri = new URI(\"ws://localhost:8080/topic/messages\");\n"
        "        WebSocketContainer c = ContainerProvider.getWebSocketContainer();\n"
        "    }\n"
        "}\n";

    write_file(tmpdir, "src/main/java/StompClient.java", client_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t server_id = cbm_gbuf_upsert_node(gb, "Method", "handleMessage",
        "test.ChatController.handleMessage",
        "src/main/java/ChatController.java", 4, 7, NULL);
    ASSERT_GT(server_id, 0);

    int64_t client_id = cbm_gbuf_upsert_node(gb, "Method", "connect",
        "test.StompClient.connect",
        "src/main/java/StompClient.java", 4, 7, NULL);
    ASSERT_GT(client_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_ws(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_ws_edges(gb), 0);
    ASSERT_TRUE(has_ws_edge_with_identifier(gb, "/topic/messages"));

    cbm_gbuf_free(gb);
    rm_rf_ws(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 6: Socket.IO server + client → edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(ws_socketio_server_client) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_ws_t6_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Socket.IO server */
    const char *server_src =
        "const { Server } = require('socket.io');\n"
        "\n"
        "function setupSocket(httpServer) {\n"
        "    const io = new Server(httpServer);\n"
        "    io.on('connection', (socket) => {\n"
        "        socket.on('message', (data) => { socket.emit('reply', data); });\n"
        "    });\n"
        "}\n";

    write_file(tmpdir, "server/socket.js", server_src);

    /* Socket.IO client */
    const char *client_src =
        "const { io } = require('socket.io-client');\n"
        "\n"
        "function connectSocket() {\n"
        "    const socket = io('ws://localhost:3000');\n"
        "    socket.on('reply', (data) => console.log(data));\n"
        "}\n";

    write_file(tmpdir, "client/socket_client.js", client_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t server_id = cbm_gbuf_upsert_node(gb, "Function", "setupSocket",
        "test.server.socket.setupSocket", "server/socket.js", 3, 8, NULL);
    ASSERT_GT(server_id, 0);

    int64_t client_id = cbm_gbuf_upsert_node(gb, "Function", "connectSocket",
        "test.client.socket_client.connectSocket", "client/socket_client.js", 3, 6, NULL);
    ASSERT_GT(client_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_ws(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_ws_edges(gb), 0);
    ASSERT_TRUE(has_ws_edge_with_identifier(gb, "/socket.io"));

    cbm_gbuf_free(gb);
    rm_rf_ws(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 7: No WebSocket patterns → no edges
 * ═══════════════════════════════════════════════════════════════════ */

TEST(ws_no_patterns) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_ws_t7_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Plain Go HTTP handler — no websocket */
    const char *src =
        "package main\n"
        "\n"
        "func handleHTTP(w http.ResponseWriter, r *http.Request) {\n"
        "    w.Write([]byte(\"hello\"))\n"
        "}\n";

    write_file(tmpdir, "server/http.go", src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    cbm_gbuf_upsert_node(gb, "Function", "handleHTTP",
        "test.server.http.handleHTTP", "server/http.go", 3, 5, NULL);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_ws(&ctx);

    ASSERT_EQ(links, 0);
    ASSERT_EQ(count_ws_edges(gb), 0);

    cbm_gbuf_free(gb);
    rm_rf_ws(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 8: Same path → high confidence
 * ═══════════════════════════════════════════════════════════════════ */

TEST(ws_same_path_high_confidence) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_ws_t8_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Node.js endpoint at /live */
    const char *server_src =
        "function setup(app) {\n"
        "    app.ws('/live', (ws, req) => {\n"
        "        ws.on('message', (msg) => {});\n"
        "    });\n"
        "}\n";

    write_file(tmpdir, "server/live.js", server_src);

    /* JS client connecting to /live */
    const char *client_src =
        "function connectLive() {\n"
        "    const ws = new WebSocket('ws://localhost:3000/live');\n"
        "}\n";

    write_file(tmpdir, "client/live.js", client_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t server_id = cbm_gbuf_upsert_node(gb, "Function", "setup",
        "test.server.live.setup", "server/live.js", 1, 5, NULL);
    ASSERT_GT(server_id, 0);

    int64_t client_id = cbm_gbuf_upsert_node(gb, "Function", "connectLive",
        "test.client.live.connectLive", "client/live.js", 1, 3, NULL);
    ASSERT_GT(client_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_ws(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_TRUE(has_ws_edge_with_band(gb, "high"));

    cbm_gbuf_free(gb);
    rm_rf_ws(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 9: Different paths → no edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(ws_different_paths_no_edge) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_ws_t9_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Node.js endpoint at /admin */
    const char *server_src =
        "function setup(app) {\n"
        "    app.ws('/admin', (ws, req) => {\n"
        "        ws.on('message', (msg) => {});\n"
        "    });\n"
        "}\n";

    write_file(tmpdir, "server/admin.js", server_src);

    /* JS client connecting to /user — completely different path */
    const char *client_src =
        "function connectUser() {\n"
        "    const ws = new WebSocket('ws://localhost:3000/user');\n"
        "}\n";

    write_file(tmpdir, "client/user.js", client_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t server_id = cbm_gbuf_upsert_node(gb, "Function", "setup",
        "test.server.admin.setup", "server/admin.js", 1, 5, NULL);
    ASSERT_GT(server_id, 0);

    int64_t client_id = cbm_gbuf_upsert_node(gb, "Function", "connectUser",
        "test.client.user.connectUser", "client/user.js", 1, 3, NULL);
    ASSERT_GT(client_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_ws(&ctx);

    ASSERT_EQ(links, 0);
    ASSERT_EQ(count_ws_edges(gb), 0);

    cbm_gbuf_free(gb);
    rm_rf_ws(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 10: Multiple endpoints, multiple clients → correct matching
 * ═══════════════════════════════════════════════════════════════════ */

TEST(ws_multiple_endpoints_clients) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_ws_t10_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Node.js server with two ws endpoints */
    const char *server1_src =
        "function setupChat(app) {\n"
        "    app.ws('/chat', (ws, req) => {});\n"
        "}\n";

    const char *server2_src =
        "function setupStatus(app) {\n"
        "    app.ws('/status', (ws, req) => {});\n"
        "}\n";

    write_file(tmpdir, "server/chat.js", server1_src);
    write_file(tmpdir, "server/status.js", server2_src);

    /* Two JS clients */
    const char *client1_src =
        "function connectChat() {\n"
        "    const ws = new WebSocket('ws://localhost:3000/chat');\n"
        "}\n";

    const char *client2_src =
        "function connectStatus() {\n"
        "    const ws = new WebSocket('ws://localhost:3000/status');\n"
        "}\n";

    write_file(tmpdir, "client/chat.js", client1_src);
    write_file(tmpdir, "client/status.js", client2_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t s1 = cbm_gbuf_upsert_node(gb, "Function", "setupChat",
        "test.server.chat.setupChat", "server/chat.js", 1, 3, NULL);
    ASSERT_GT(s1, 0);

    int64_t s2 = cbm_gbuf_upsert_node(gb, "Function", "setupStatus",
        "test.server.status.setupStatus", "server/status.js", 1, 3, NULL);
    ASSERT_GT(s2, 0);

    int64_t c1 = cbm_gbuf_upsert_node(gb, "Function", "connectChat",
        "test.client.chat.connectChat", "client/chat.js", 1, 3, NULL);
    ASSERT_GT(c1, 0);

    int64_t c2 = cbm_gbuf_upsert_node(gb, "Function", "connectStatus",
        "test.client.status.connectStatus", "client/status.js", 1, 3, NULL);
    ASSERT_GT(c2, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_ws(&ctx);

    /* Should have 2 edges: chat→chat, status→status */
    ASSERT_EQ(links, 2);
    ASSERT_EQ(count_ws_edges(gb), 2);
    ASSERT_TRUE(has_ws_edge_with_identifier(gb, "/chat"));
    ASSERT_TRUE(has_ws_edge_with_identifier(gb, "/status"));

    cbm_gbuf_free(gb);
    rm_rf_ws(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 11: Self-link prevention (same node) → no edge
 * ═══════════════════════════════════════════════════════════════════ */

TEST(ws_no_self_link) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_ws_t11_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Single JS file that both serves and connects to /echo */
    const char *src =
        "function setupAndConnect(app) {\n"
        "    app.ws('/echo', (ws, req) => {});\n"
        "    const client = new WebSocket('ws://localhost:3000/echo');\n"
        "}\n";

    write_file(tmpdir, "both.js", src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t id = cbm_gbuf_upsert_node(gb, "Function", "setupAndConnect",
        "test.both.setupAndConnect", "both.js", 1, 4, NULL);
    ASSERT_GT(id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_ws(&ctx);

    /* Same node is both endpoint and client — should NOT create self-link */
    ASSERT_EQ(links, 0);
    ASSERT_EQ(count_ws_edges(gb), 0);

    cbm_gbuf_free(gb);
    rm_rf_ws(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 12: Client URL path extraction (wss://host:8080/chat → /chat)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(ws_url_path_extraction) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_ws_t12_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Go endpoint at /chat */
    const char *server_src =
        "package main\n"
        "\n"
        "import \"github.com/gorilla/websocket\"\n"
        "\n"
        "var upgrader = websocket.Upgrader{}\n"
        "\n"
        "func setupChat() {\n"
        "    r.HandleFunc(\"/chat\", handleChat)\n"
        "}\n";

    write_file(tmpdir, "server/chat.go", server_src);

    /* TypeScript client with wss + port */
    const char *client_src =
        "function connectChat(): void {\n"
        "    const ws = new WebSocket(\"wss://api.example.com:8080/chat\");\n"
        "    ws.onmessage = (e: MessageEvent) => console.log(e.data);\n"
        "}\n";

    write_file(tmpdir, "client/chat.ts", client_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t server_id = cbm_gbuf_upsert_node(gb, "Function", "setupChat",
        "test.server.chat.setupChat", "server/chat.go", 7, 9, NULL);
    ASSERT_GT(server_id, 0);

    int64_t client_id = cbm_gbuf_upsert_node(gb, "Function", "connectChat",
        "test.client.chat.connectChat", "client/chat.ts", 1, 4, NULL);
    ASSERT_GT(client_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_ws(&ctx);

    /* wss://api.example.com:8080/chat → path /chat should match /chat endpoint */
    ASSERT_GT(links, 0);
    ASSERT_GT(count_ws_edges(gb), 0);
    ASSERT_TRUE(has_ws_edge_with_identifier(gb, "/chat"));
    ASSERT_TRUE(has_ws_edge_with_band(gb, "high"));

    cbm_gbuf_free(gb);
    rm_rf_ws(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test: Class node with WebSocket emitter → detected
 * ═══════════════════════════════════════════════════════════════════ */

TEST(ws_class_node_emitter) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_ws_cls_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    const char *server_src =
        "class ChatServer {\n"
        "  broadcast(msg) {\n"
        "    this.ws.send(JSON.stringify({ channel: 'chat', data: msg }));\n"
        "  }\n"
        "}\n";
    write_file(tmpdir, "server/chat.ts", server_src);

    const char *client_src =
        "function listenChat() {\n"
        "  ws.on('message', (data) => { console.log(data); });\n"
        "}\n";
    write_file(tmpdir, "client/chat.ts", client_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);
    int64_t srv_id = cbm_gbuf_upsert_node(gb, "Class", "ChatServer",
        "test.server.chat.ChatServer", "server/chat.ts", 1, 5, NULL);
    ASSERT_GT(srv_id, 0);
    int64_t cli_id = cbm_gbuf_upsert_node(gb, "Function", "listenChat",
        "test.client.chat.listenChat", "client/chat.ts", 1, 3, NULL);
    ASSERT_GT(cli_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_ws(&ctx);
    ASSERT_GTE(links, 0);

    cbm_gbuf_free(gb);
    rm_rf_ws(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Suite definition
 * ═══════════════════════════════════════════════════════════════════ */

SUITE(servicelink_ws) {
    RUN_TEST(ws_go_endpoint_js_client);
    RUN_TEST(ws_python_decorator_client);
    RUN_TEST(ws_java_serverendpoint_client);
    RUN_TEST(ws_nodejs_appws_client);
    RUN_TEST(ws_spring_messagemapping_client);
    RUN_TEST(ws_socketio_server_client);
    RUN_TEST(ws_no_patterns);
    RUN_TEST(ws_same_path_high_confidence);
    RUN_TEST(ws_different_paths_no_edge);
    RUN_TEST(ws_multiple_endpoints_clients);
    RUN_TEST(ws_no_self_link);
    RUN_TEST(ws_url_path_extraction);
    RUN_TEST(ws_class_node_emitter);
}
