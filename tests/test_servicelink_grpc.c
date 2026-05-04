/*
 * test_servicelink_grpc.c — Tests for gRPC protocol linking.
 *
 * Creates synthetic source files (.proto, .go, .py, .java, .js, etc.),
 * builds a graph buffer with nodes, runs the gRPC linker, and verifies
 * that GRPC_CALLS edges are created with correct confidence bands.
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
static void rm_rf_grpc(const char *path) {
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

/* Check if any GRPC_CALLS edge exists */
static int count_grpc_edges(cbm_gbuf_t *gb) {
    return cbm_gbuf_edge_count_by_type(gb, "GRPC_CALLS");
}

/* Check if a GRPC_CALLS edge has given confidence band */
static bool has_grpc_edge_with_band(cbm_gbuf_t *gb, const char *band) {
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_type(gb, "GRPC_CALLS", &edges, &count);
    char needle[64];
    snprintf(needle, sizeof(needle), "\"confidence_band\":\"%s\"", band);
    for (int i = 0; i < count; i++) {
        if (edges[i]->properties_json && strstr(edges[i]->properties_json, needle))
            return true;
    }
    return false;
}

/* Check if a GRPC_CALLS edge has given identifier */
static bool has_grpc_edge_with_identifier(cbm_gbuf_t *gb, const char *identifier) {
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_type(gb, "GRPC_CALLS", &edges, &count);
    char needle[256];
    snprintf(needle, sizeof(needle), "\"identifier\":\"%s\"", identifier);
    for (int i = 0; i < count; i++) {
        if (edges[i]->properties_json && strstr(edges[i]->properties_json, needle))
            return true;
    }
    return false;
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 1: Proto file service definitions → producers
 * ═══════════════════════════════════════════════════════════════════ */

TEST(grpc_proto_service_definitions) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_grpc_t1_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Write a .proto file with two rpc methods */
    const char *proto_src =
        "syntax = \"proto3\";\n"
        "package myapp;\n"
        "\n"
        "service OrderService {\n"
        "  rpc CreateOrder(CreateOrderRequest) returns (CreateOrderResponse);\n"
        "  rpc GetOrder(GetOrderRequest) returns (Order);\n"
        "}\n";

    write_file(tmpdir, "proto/order.proto", proto_src);

    /* Write a Go client that calls CreateOrder */
    const char *go_client_src =
        "package main\n"
        "\n"
        "func placeOrder() {\n"
        "    conn, _ := grpc.Dial(\"localhost:50051\")\n"
        "    client := pb.NewOrderServiceClient(conn)\n"
        "    resp, _ := client.CreateOrder(ctx, req)\n"
        "}\n";

    write_file(tmpdir, "cmd/client/main.go", go_client_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    /* Create nodes for the proto file and Go client */
    int64_t proto_id = cbm_gbuf_upsert_node(gb, "Module", "order",
        "test.proto.order", "proto/order.proto", 1, 8, NULL);
    ASSERT_GT(proto_id, 0);

    int64_t client_id = cbm_gbuf_upsert_node(gb, "Function", "placeOrder",
        "test.cmd.client.main.placeOrder", "cmd/client/main.go", 3, 7, NULL);
    ASSERT_GT(client_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_grpc(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_grpc_edges(gb), 0);

    /* The client calls OrderService.* which should match the proto definition */
    ASSERT_TRUE(has_grpc_edge_with_band(gb, "high"));

    cbm_gbuf_free(gb);
    rm_rf_grpc(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 2: Go server registration → producer, Go client → consumer
 * ═══════════════════════════════════════════════════════════════════ */

TEST(grpc_go_server_client_match) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_grpc_t2_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Go server that registers a service */
    const char *server_src =
        "package main\n"
        "\n"
        "func main() {\n"
        "    s := grpc.NewServer()\n"
        "    pb.RegisterUserServiceServer(s, &userServer{})\n"
        "    s.Serve(lis)\n"
        "}\n";

    write_file(tmpdir, "server/main.go", server_src);

    /* Go client that creates a client for the same service */
    const char *client_src =
        "package main\n"
        "\n"
        "func getUser() {\n"
        "    conn, _ := grpc.Dial(addr)\n"
        "    client := pb.NewUserServiceClient(conn)\n"
        "    user, _ := client.GetUser(ctx, req)\n"
        "}\n";

    write_file(tmpdir, "client/main.go", client_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t server_id = cbm_gbuf_upsert_node(gb, "Function", "main",
        "test.server.main.main", "server/main.go", 3, 7, NULL);
    ASSERT_GT(server_id, 0);

    int64_t client_id = cbm_gbuf_upsert_node(gb, "Function", "getUser",
        "test.client.main.getUser", "client/main.go", 3, 7, NULL);
    ASSERT_GT(client_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_grpc(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_grpc_edges(gb), 0);
    ASSERT_TRUE(has_grpc_edge_with_band(gb, "high"));

    cbm_gbuf_free(gb);
    rm_rf_grpc(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 3: Python servicer + stub matching
 * ═══════════════════════════════════════════════════════════════════ */

TEST(grpc_python_servicer_stub) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_grpc_t3_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Python server implementing servicer */
    const char *server_src =
        "import grpc\n"
        "from proto import payment_pb2_grpc\n"
        "\n"
        "class PaymentServicer(payment_pb2_grpc.PaymentServiceServicer):\n"
        "    def ProcessPayment(self, request, context):\n"
        "        return payment_pb2.PaymentResponse(status='ok')\n";

    write_file(tmpdir, "services/payment_server.py", server_src);

    /* Python client using stub */
    const char *client_src =
        "import grpc\n"
        "from proto import payment_pb2_grpc\n"
        "\n"
        "def make_payment():\n"
        "    channel = grpc.insecure_channel('localhost:50051')\n"
        "    stub = payment_pb2_grpc.PaymentStub(channel)\n"
        "    response = stub.ProcessPayment(request)\n";

    write_file(tmpdir, "clients/payment_client.py", client_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t server_id = cbm_gbuf_upsert_node(gb, "Method", "ProcessPayment",
        "test.services.payment_server.PaymentServicer.ProcessPayment",
        "services/payment_server.py", 4, 6, NULL);
    ASSERT_GT(server_id, 0);

    int64_t client_id = cbm_gbuf_upsert_node(gb, "Function", "make_payment",
        "test.clients.payment_client.make_payment",
        "clients/payment_client.py", 4, 7, NULL);
    ASSERT_GT(client_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_grpc(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_grpc_edges(gb), 0);

    cbm_gbuf_free(gb);
    rm_rf_grpc(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 4: Java server + client (extends ImplBase + newBlockingStub)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(grpc_java_server_client) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_grpc_t4_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Java server */
    const char *server_src =
        "package com.example;\n"
        "\n"
        "public class InventoryServiceImpl extends InventoryGrpc.InventoryImplBase {\n"
        "    @Override\n"
        "    public void checkStock(CheckStockRequest req,\n"
        "                           StreamObserver<StockResponse> resp) {\n"
        "        resp.onNext(StockResponse.newBuilder().build());\n"
        "        resp.onCompleted();\n"
        "    }\n"
        "}\n";

    write_file(tmpdir, "src/main/java/InventoryServiceImpl.java", server_src);

    /* Java client */
    const char *client_src =
        "package com.example;\n"
        "\n"
        "public class InventoryClient {\n"
        "    public void check() {\n"
        "        var stub = InventoryGrpc.newBlockingStub(channel);\n"
        "        var resp = stub.checkStock(req);\n"
        "    }\n"
        "}\n";

    write_file(tmpdir, "src/main/java/InventoryClient.java", client_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t server_id = cbm_gbuf_upsert_node(gb, "Method", "checkStock",
        "test.InventoryServiceImpl.checkStock",
        "src/main/java/InventoryServiceImpl.java", 3, 10, NULL);
    ASSERT_GT(server_id, 0);

    int64_t client_id = cbm_gbuf_upsert_node(gb, "Method", "check",
        "test.InventoryClient.check",
        "src/main/java/InventoryClient.java", 4, 7, NULL);
    ASSERT_GT(client_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_grpc(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_grpc_edges(gb), 0);
    ASSERT_TRUE(has_grpc_edge_with_band(gb, "high"));

    cbm_gbuf_free(gb);
    rm_rf_grpc(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 5: Node.js server.addService + client
 * ═══════════════════════════════════════════════════════════════════ */

TEST(grpc_nodejs_server_client) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_grpc_t5_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Node.js server */
    const char *server_src =
        "const grpc = require('@grpc/grpc-js');\n"
        "\n"
        "function startServer() {\n"
        "  const server = new grpc.Server();\n"
        "  server.addService(NotificationService.service, {\n"
        "    sendNotification: sendNotification,\n"
        "  });\n"
        "  server.bindAsync('0.0.0.0:50051', creds, () => {});\n"
        "}\n";

    write_file(tmpdir, "notification/server.js", server_src);

    /* Node.js client */
    const char *client_src =
        "const grpc = require('@grpc/grpc-js');\n"
        "\n"
        "function notify() {\n"
        "  const client = new NotificationClient('localhost:50051', creds);\n"
        "  client.sendNotification(msg, callback);\n"
        "}\n";

    write_file(tmpdir, "gateway/client.js", client_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t server_id = cbm_gbuf_upsert_node(gb, "Function", "startServer",
        "test.notification.server.startServer",
        "notification/server.js", 3, 9, NULL);
    ASSERT_GT(server_id, 0);

    int64_t client_id = cbm_gbuf_upsert_node(gb, "Function", "notify",
        "test.gateway.client.notify",
        "gateway/client.js", 3, 6, NULL);
    ASSERT_GT(client_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_grpc(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_grpc_edges(gb), 0);
    ASSERT_TRUE(has_grpc_edge_with_band(gb, "high"));

    cbm_gbuf_free(gb);
    rm_rf_grpc(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 6: Proto definitions → multiple services with multiple methods
 * ═══════════════════════════════════════════════════════════════════ */

TEST(grpc_proto_multiple_services) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_grpc_t6_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    const char *proto_src =
        "syntax = \"proto3\";\n"
        "\n"
        "service AuthService {\n"
        "  rpc Login(LoginRequest) returns (LoginResponse);\n"
        "  rpc Logout(LogoutRequest) returns (LogoutResponse);\n"
        "  rpc RefreshToken(RefreshRequest) returns (TokenResponse);\n"
        "}\n"
        "\n"
        "service UserService {\n"
        "  rpc GetUser(GetUserRequest) returns (User);\n"
        "  rpc UpdateUser(UpdateUserRequest) returns (User);\n"
        "}\n";

    write_file(tmpdir, "proto/services.proto", proto_src);

    /* Go client that uses AuthService */
    const char *client_src =
        "package main\n"
        "\n"
        "func authenticate() {\n"
        "    client := pb.NewAuthServiceClient(conn)\n"
        "    resp, _ := client.Login(ctx, req)\n"
        "}\n";

    write_file(tmpdir, "cmd/auth_client.go", client_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t proto_id = cbm_gbuf_upsert_node(gb, "Module", "services",
        "test.proto.services", "proto/services.proto", 1, 12, NULL);
    ASSERT_GT(proto_id, 0);

    int64_t client_id = cbm_gbuf_upsert_node(gb, "Function", "authenticate",
        "test.cmd.auth_client.authenticate", "cmd/auth_client.go", 3, 6, NULL);
    ASSERT_GT(client_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_grpc(&ctx);

    /* Should have at least 1 link (AuthService client → proto) */
    ASSERT_GT(links, 0);
    ASSERT_GT(count_grpc_edges(gb), 0);

    cbm_gbuf_free(gb);
    rm_rf_grpc(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 7: Method-only match (lower confidence)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(grpc_method_only_match) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_grpc_t7_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Proto with GetOrder method */
    const char *proto_src =
        "syntax = \"proto3\";\n"
        "service OrderService {\n"
        "  rpc GetOrder(GetOrderRequest) returns (Order);\n"
        "}\n";

    write_file(tmpdir, "proto/order.proto", proto_src);

    /* Go code that calls client.GetOrder() without NewOrderServiceClient pattern */
    const char *go_src =
        "package main\n"
        "\n"
        "func fetchOrder() {\n"
        "    resp, _ := client.GetOrder(ctx, req)\n"
        "}\n";

    write_file(tmpdir, "handlers/order.go", go_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t proto_id = cbm_gbuf_upsert_node(gb, "Module", "order",
        "test.proto.order", "proto/order.proto", 1, 4, NULL);
    ASSERT_GT(proto_id, 0);

    int64_t handler_id = cbm_gbuf_upsert_node(gb, "Function", "fetchOrder",
        "test.handlers.order.fetchOrder", "handlers/order.go", 3, 5, NULL);
    ASSERT_GT(handler_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_grpc(&ctx);

    /* Should have a medium-confidence match (method-only) */
    ASSERT_GT(links, 0);
    ASSERT_GT(count_grpc_edges(gb), 0);
    ASSERT_TRUE(has_grpc_edge_with_band(gb, "medium"));

    cbm_gbuf_free(gb);
    rm_rf_grpc(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 8: No match (unrelated services)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(grpc_no_match_unrelated) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_grpc_t8_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Proto defines OrderService */
    const char *proto_src =
        "syntax = \"proto3\";\n"
        "service OrderService {\n"
        "  rpc CreateOrder(Req) returns (Resp);\n"
        "}\n";

    write_file(tmpdir, "proto/order.proto", proto_src);

    /* Go client calls a completely different service */
    const char *go_src =
        "package main\n"
        "\n"
        "func fetchPayment() {\n"
        "    client := pb.NewPaymentServiceClient(conn)\n"
        "}\n";

    write_file(tmpdir, "cmd/pay.go", go_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    cbm_gbuf_upsert_node(gb, "Module", "order",
        "test.proto.order", "proto/order.proto", 1, 4, NULL);

    cbm_gbuf_upsert_node(gb, "Function", "fetchPayment",
        "test.cmd.pay.fetchPayment", "cmd/pay.go", 3, 5, NULL);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_grpc(&ctx);

    /* PaymentService client should NOT match OrderService proto */
    ASSERT_EQ(links, 0);
    ASSERT_EQ(count_grpc_edges(gb), 0);

    cbm_gbuf_free(gb);
    rm_rf_grpc(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 9: C# server + client
 * ═══════════════════════════════════════════════════════════════════ */

TEST(grpc_csharp_server_client) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_grpc_t9_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* C# server */
    const char *server_src =
        "using Grpc.Core;\n"
        "\n"
        "public class CatalogServiceImpl : CatalogGrpc.CatalogBase\n"
        "{\n"
        "    public override Task<ProductReply> GetProduct(ProductRequest req, ServerCallContext ctx)\n"
        "    {\n"
        "        return Task.FromResult(new ProductReply());\n"
        "    }\n"
        "}\n";

    write_file(tmpdir, "Services/CatalogService.cs", server_src);

    /* C# client */
    const char *client_src =
        "using Grpc.Core;\n"
        "\n"
        "public class CatalogClient\n"
        "{\n"
        "    public void GetProduct()\n"
        "    {\n"
        "        var client = new CatalogGrpc.CatalogClient(channel);\n"
        "    }\n"
        "}\n";

    write_file(tmpdir, "Clients/CatalogClient.cs", client_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t server_id = cbm_gbuf_upsert_node(gb, "Method", "GetProduct",
        "test.Services.CatalogServiceImpl.GetProduct",
        "Services/CatalogService.cs", 3, 9, NULL);
    ASSERT_GT(server_id, 0);

    int64_t client_id = cbm_gbuf_upsert_node(gb, "Method", "GetProduct",
        "test.Clients.CatalogClient.GetProduct",
        "Clients/CatalogClient.cs", 5, 8, NULL);
    ASSERT_GT(client_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_grpc(&ctx);

    /* CatalogGrpc client → CatalogGrpc server */
    ASSERT_GT(links, 0);
    ASSERT_GT(count_grpc_edges(gb), 0);

    cbm_gbuf_free(gb);
    rm_rf_grpc(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 10: Empty graph buffer (no crash)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(grpc_empty_graph) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_grpc_t10_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_grpc(&ctx);

    ASSERT_EQ(links, 0);
    ASSERT_EQ(count_grpc_edges(gb), 0);

    cbm_gbuf_free(gb);
    rm_rf_grpc(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 11: Self-link prevention (producer and consumer in same node)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(grpc_no_self_link) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_grpc_t11_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Go file that both registers and creates a client for same service */
    const char *src =
        "package main\n"
        "\n"
        "func main() {\n"
        "    pb.RegisterTestServiceServer(s, &impl{})\n"
        "    client := pb.NewTestServiceClient(conn)\n"
        "}\n";

    write_file(tmpdir, "main.go", src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t id = cbm_gbuf_upsert_node(gb, "Function", "main",
        "test.main.main", "main.go", 3, 6, NULL);
    ASSERT_GT(id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_grpc(&ctx);

    /* Same node is both producer and consumer — should NOT create self-link */
    ASSERT_EQ(links, 0);
    ASSERT_EQ(count_grpc_edges(gb), 0);

    cbm_gbuf_free(gb);
    rm_rf_grpc(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 12: Rust server + client (tonic patterns)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(grpc_rust_server_client) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_grpc_t12_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Rust server (tonic) */
    const char *server_src =
        "use tonic::{Request, Response, Status};\n"
        "\n"
        "impl Greeter for MyGreeter {\n"
        "    async fn say_hello(&self, request: Request<HelloRequest>)\n"
        "        -> Result<Response<HelloReply>, Status> {\n"
        "        Ok(Response::new(HelloReply { message: \"Hello\".into() }))\n"
        "    }\n"
        "}\n";

    write_file(tmpdir, "src/server.rs", server_src);

    /* Rust client (tonic) */
    const char *client_src =
        "use tonic::transport::Channel;\n"
        "\n"
        "async fn greet() {\n"
        "    let client = GreeterClient::connect(\"http://[::1]:50051\").await.unwrap();\n"
        "    let response = client.say_hello(request).await.unwrap();\n"
        "}\n";

    write_file(tmpdir, "src/client.rs", client_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t server_id = cbm_gbuf_upsert_node(gb, "Method", "say_hello",
        "test.src.server.MyGreeter.say_hello",
        "src/server.rs", 3, 8, NULL);
    ASSERT_GT(server_id, 0);

    int64_t client_id = cbm_gbuf_upsert_node(gb, "Function", "greet",
        "test.src.client.greet",
        "src/client.rs", 3, 6, NULL);
    ASSERT_GT(client_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_grpc(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_grpc_edges(gb), 0);
    ASSERT_TRUE(has_grpc_edge_with_band(gb, "high"));

    cbm_gbuf_free(gb);
    rm_rf_grpc(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 13: @GrpcService annotation (Java/Spring Boot)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(grpc_java_annotation) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_grpc_t13_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Java server with @GrpcService annotation */
    const char *server_src =
        "import net.devh.boot.grpc.server.service.GrpcService;\n"
        "\n"
        "@GrpcService\n"
        "public class ShippingService extends ShippingGrpc.ShippingImplBase {\n"
        "    public void trackShipment(TrackRequest req, StreamObserver resp) {}\n"
        "}\n";

    write_file(tmpdir, "src/main/java/ShippingService.java", server_src);

    /* Java client */
    const char *client_src =
        "public class ShippingClient {\n"
        "    public void track() {\n"
        "        var stub = ShippingGrpc.newBlockingStub(channel);\n"
        "    }\n"
        "}\n";

    write_file(tmpdir, "src/main/java/ShippingClient.java", client_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t server_id = cbm_gbuf_upsert_node(gb, "Method", "trackShipment",
        "test.ShippingService.trackShipment",
        "src/main/java/ShippingService.java", 3, 6, NULL);
    ASSERT_GT(server_id, 0);

    int64_t client_id = cbm_gbuf_upsert_node(gb, "Method", "track",
        "test.ShippingClient.track",
        "src/main/java/ShippingClient.java", 2, 4, NULL);
    ASSERT_GT(client_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_grpc(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_grpc_edges(gb), 0);
    ASSERT_TRUE(has_grpc_edge_with_band(gb, "high"));

    cbm_gbuf_free(gb);
    rm_rf_grpc(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 14: Identifier matching helper edge cases
 * ═══════════════════════════════════════════════════════════════════ */

TEST(grpc_edge_has_identifier) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_grpc_t14_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Proto with named methods */
    const char *proto_src =
        "syntax = \"proto3\";\n"
        "service SearchService {\n"
        "  rpc Search(SearchRequest) returns (SearchResponse);\n"
        "  rpc Suggest(SuggestRequest) returns (SuggestResponse);\n"
        "}\n";

    write_file(tmpdir, "proto/search.proto", proto_src);

    /* Go client that creates SearchService client */
    const char *client_src =
        "package main\n"
        "func doSearch() {\n"
        "    c := pb.NewSearchServiceClient(conn)\n"
        "}\n";

    write_file(tmpdir, "cmd/search.go", client_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    cbm_gbuf_upsert_node(gb, "Module", "search",
        "test.proto.search", "proto/search.proto", 1, 5, NULL);

    cbm_gbuf_upsert_node(gb, "Function", "doSearch",
        "test.cmd.search.doSearch", "cmd/search.go", 2, 4, NULL);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_grpc(&ctx);

    ASSERT_GT(links, 0);
    /* Verify the edge contains the service name in the identifier */
    ASSERT_TRUE(has_grpc_edge_with_identifier(gb, "SearchService.*"));

    cbm_gbuf_free(gb);
    rm_rf_grpc(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test: Class node with gRPC client → detected
 * ═══════════════════════════════════════════════════════════════════ */

TEST(grpc_class_node_client) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_grpc_cls_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    const char *proto_src =
        "syntax = \"proto3\";\n"
        "service OrderService {\n"
        "  rpc GetOrder (GetOrderRequest) returns (Order);\n"
        "}\n";
    write_file(tmpdir, "proto/order.proto", proto_src);

    const char *class_src =
        "class OrderClient {\n"
        "  constructor() {\n"
        "    this.client = new OrderServiceClient('localhost:50051', grpc.credentials.createInsecure());\n"
        "  }\n"
        "}\n";
    write_file(tmpdir, "clients/order.ts", class_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);
    int64_t proto_id = cbm_gbuf_upsert_node(gb, "Module", "order_proto",
        "test.proto.order", "proto/order.proto", 1, 4, NULL);
    ASSERT_GT(proto_id, 0);
    int64_t class_id = cbm_gbuf_upsert_node(gb, "Class", "OrderClient",
        "test.clients.order.OrderClient", "clients/order.ts", 1, 5, NULL);
    ASSERT_GT(class_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_grpc(&ctx);
    ASSERT_GT(links, 0);
    ASSERT_GT(cbm_gbuf_edge_count_by_type(gb, "GRPC_CALLS"), 0);

    cbm_gbuf_free(gb);
    rm_rf_grpc(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Suite definition
 * ═══════════════════════════════════════════════════════════════════ */

SUITE(servicelink_grpc) {
    RUN_TEST(grpc_proto_service_definitions);
    RUN_TEST(grpc_go_server_client_match);
    RUN_TEST(grpc_python_servicer_stub);
    RUN_TEST(grpc_java_server_client);
    RUN_TEST(grpc_nodejs_server_client);
    RUN_TEST(grpc_proto_multiple_services);
    RUN_TEST(grpc_method_only_match);
    RUN_TEST(grpc_no_match_unrelated);
    RUN_TEST(grpc_csharp_server_client);
    RUN_TEST(grpc_empty_graph);
    RUN_TEST(grpc_no_self_link);
    RUN_TEST(grpc_rust_server_client);
    RUN_TEST(grpc_java_annotation);
    RUN_TEST(grpc_edge_has_identifier);
    RUN_TEST(grpc_class_node_client);
}
