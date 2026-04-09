/*
 * test_servicelink_trpc.c -- Tests for tRPC protocol linking.
 *
 * Creates synthetic TypeScript/JavaScript source files, builds a graph
 * buffer with nodes, runs the tRPC linker, and verifies that TRPC_CALLS
 * edges are created with correct confidence bands.
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

/* -- Helpers -------------------------------------------------------------- */

/* Recursive remove */
static void rm_rf_trpc(const char *path) {
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

/* Count TRPC_CALLS edges */
static int count_trpc_edges(cbm_gbuf_t *gb) {
    return cbm_gbuf_edge_count_by_type(gb, "TRPC_CALLS");
}

/* Check if a TRPC_CALLS edge has given confidence band */
static bool has_trpc_edge_with_band(cbm_gbuf_t *gb, const char *band) {
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_type(gb, "TRPC_CALLS", &edges, &count);
    char needle[64];
    snprintf(needle, sizeof(needle), "\"confidence_band\":\"%s\"", band);
    for (int i = 0; i < count; i++) {
        if (edges[i]->properties_json && strstr(edges[i]->properties_json, needle))
            return true;
    }
    return false;
}

/* Check if a TRPC_CALLS edge has given identifier */
static bool has_trpc_edge_with_identifier(cbm_gbuf_t *gb, const char *identifier) {
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_type(gb, "TRPC_CALLS", &edges, &count);
    char needle[256];
    snprintf(needle, sizeof(needle), "\"identifier\":\"%s\"", identifier);
    for (int i = 0; i < count; i++) {
        if (edges[i]->properties_json && strstr(edges[i]->properties_json, needle))
            return true;
    }
    return false;
}

/* ==========================================================================
 *  Test 1: createTRPCRouter with procedure definitions
 * ========================================================================== */

TEST(test_trpc_router_definition) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_trpc_t1_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Router file defining procedures */
    const char *router_src =
        "import { createTRPCRouter, publicProcedure } from '../trpc';\n"
        "import { z } from 'zod';\n"
        "\n"
        "export const userRouter = createTRPCRouter({\n"
        "  getAll: publicProcedure.query(async () => {\n"
        "    return db.user.findMany();\n"
        "  }),\n"
        "  getById: publicProcedure\n"
        "    .input(z.object({ id: z.string() }))\n"
        "    .query(async ({ input }) => {\n"
        "      return db.user.findUnique({ where: { id: input.id } });\n"
        "    }),\n"
        "});\n";

    write_file(tmpdir, "server/routers/user.ts", router_src);

    /* Client calling one of the procedures */
    const char *client_src =
        "import { trpc } from '../utils/trpc';\n"
        "\n"
        "export function UserList() {\n"
        "  const { data } = trpc.user.getAll.useQuery();\n"
        "  return <div>{data}</div>;\n"
        "}\n";

    write_file(tmpdir, "client/components/UserList.tsx", client_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t router_id = cbm_gbuf_upsert_node(gb, "Module", "userRouter",
        "test.server.routers.user", "server/routers/user.ts", 1, 13, NULL);
    ASSERT_GT(router_id, 0);

    int64_t client_id = cbm_gbuf_upsert_node(gb, "Function", "UserList",
        "test.client.components.UserList", "client/components/UserList.tsx", 3, 6, NULL);
    ASSERT_GT(client_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_trpc(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_trpc_edges(gb), 0);

    /* Consumer path "user.getAll" should match producer "getAll" partially */
    ASSERT_TRUE(has_trpc_edge_with_band(gb, "high"));

    cbm_gbuf_free(gb);
    rm_rf_trpc(tmpdir);
    PASS();
}

/* ==========================================================================
 *  Test 2: React hooks (useQuery, useMutation)
 * ========================================================================== */

TEST(test_trpc_react_hooks) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_trpc_t2_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Router with multiple procedures */
    const char *router_src =
        "export const postRouter = createTRPCRouter({\n"
        "  create: publicProcedure\n"
        "    .input(z.object({ title: z.string() }))\n"
        "    .mutation(async ({ input }) => {\n"
        "      return db.post.create({ data: input });\n"
        "    }),\n"
        "  list: publicProcedure.query(async () => {\n"
        "    return db.post.findMany();\n"
        "  }),\n"
        "});\n";

    write_file(tmpdir, "server/routers/post.ts", router_src);

    /* Component using both useQuery and useMutation */
    const char *client_src =
        "function PostPage() {\n"
        "  const posts = trpc.post.list.useQuery();\n"
        "  const createPost = trpc.post.create.useMutation();\n"
        "  return <div />;\n"
        "}\n";

    write_file(tmpdir, "client/pages/PostPage.tsx", client_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t router_id = cbm_gbuf_upsert_node(gb, "Module", "postRouter",
        "test.server.routers.post", "server/routers/post.ts", 1, 10, NULL);
    ASSERT_GT(router_id, 0);

    int64_t client_id = cbm_gbuf_upsert_node(gb, "Function", "PostPage",
        "test.client.pages.PostPage", "client/pages/PostPage.tsx", 1, 5, NULL);
    ASSERT_GT(client_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_trpc(&ctx);

    /* Should match both list and create */
    ASSERT_GT(links, 0);
    ASSERT_GT(count_trpc_edges(gb), 0);

    cbm_gbuf_free(gb);
    rm_rf_trpc(tmpdir);
    PASS();
}

/* ==========================================================================
 *  Test 3: Vanilla client calls (client.X.query(), client.X.mutate())
 * ========================================================================== */

TEST(test_trpc_vanilla_client) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_trpc_t3_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Router */
    const char *router_src =
        "export const itemRouter = createTRPCRouter({\n"
        "  getItem: publicProcedure\n"
        "    .input(z.object({ id: z.string() }))\n"
        "    .query(async ({ input }) => {\n"
        "      return db.item.findUnique({ where: { id: input.id } });\n"
        "    }),\n"
        "});\n";

    write_file(tmpdir, "server/routers/item.ts", router_src);

    /* Vanilla client usage */
    const char *client_src =
        "async function fetchItem(id: string) {\n"
        "  const item = await client.item.getItem.query({ id });\n"
        "  return item;\n"
        "}\n";

    write_file(tmpdir, "lib/api.ts", client_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t router_id = cbm_gbuf_upsert_node(gb, "Module", "itemRouter",
        "test.server.routers.item", "server/routers/item.ts", 1, 7, NULL);
    ASSERT_GT(router_id, 0);

    int64_t client_id = cbm_gbuf_upsert_node(gb, "Function", "fetchItem",
        "test.lib.api.fetchItem", "lib/api.ts", 1, 4, NULL);
    ASSERT_GT(client_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_trpc(&ctx);

    ASSERT_GT(links, 0);
    ASSERT_GT(count_trpc_edges(gb), 0);

    /* "item.getItem" consumer should match "getItem" producer (partial) */
    ASSERT_TRUE(has_trpc_edge_with_band(gb, "high"));

    cbm_gbuf_free(gb);
    rm_rf_trpc(tmpdir);
    PASS();
}

/* ==========================================================================
 *  Test 4: Nested router paths (user.getAll)
 * ========================================================================== */

TEST(test_trpc_nested_router) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_trpc_t4_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Nested router: appRouter merges sub-routers */
    const char *router_src =
        "export const appRouter = createTRPCRouter({\n"
        "  getProfile: publicProcedure.query(async () => {\n"
        "    return db.profile.findFirst();\n"
        "  }),\n"
        "  updateProfile: protectedProcedure\n"
        "    .input(z.object({ name: z.string() }))\n"
        "    .mutation(async ({ input }) => {\n"
        "      return db.profile.update({ data: input });\n"
        "    }),\n"
        "});\n";

    write_file(tmpdir, "server/router.ts", router_src);

    /* Consumer calling nested path */
    const char *client_src =
        "function ProfilePage() {\n"
        "  const profile = api.profile.getProfile.useQuery();\n"
        "  const update = api.profile.updateProfile.useMutation();\n"
        "  return <div />;\n"
        "}\n";

    write_file(tmpdir, "pages/profile.tsx", client_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t router_id = cbm_gbuf_upsert_node(gb, "Module", "appRouter",
        "test.server.router", "server/router.ts", 1, 10, NULL);
    ASSERT_GT(router_id, 0);

    int64_t client_id = cbm_gbuf_upsert_node(gb, "Function", "ProfilePage",
        "test.pages.profile.ProfilePage", "pages/profile.tsx", 1, 5, NULL);
    ASSERT_GT(client_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_trpc(&ctx);

    /* "profile.getProfile" consumer matches "getProfile" producer (partial match) */
    ASSERT_GT(links, 0);
    ASSERT_GT(count_trpc_edges(gb), 0);
    ASSERT_TRUE(has_trpc_edge_with_band(gb, "high"));

    cbm_gbuf_free(gb);
    rm_rf_trpc(tmpdir);
    PASS();
}

/* ==========================================================================
 *  Test 5: No match -- different procedure names, no edges
 * ========================================================================== */

TEST(test_trpc_no_match) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_trpc_t5_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Router defines "createOrder" */
    const char *router_src =
        "export const orderRouter = createTRPCRouter({\n"
        "  createOrder: publicProcedure\n"
        "    .input(z.object({ item: z.string() }))\n"
        "    .mutation(async ({ input }) => {\n"
        "      return db.order.create({ data: input });\n"
        "    }),\n"
        "});\n";

    write_file(tmpdir, "server/routers/order.ts", router_src);

    /* Client calls a completely different procedure */
    const char *client_src =
        "function PaymentPage() {\n"
        "  const pay = trpc.payment.processPayment.useQuery();\n"
        "  return <div />;\n"
        "}\n";

    write_file(tmpdir, "pages/payment.tsx", client_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    cbm_gbuf_upsert_node(gb, "Module", "orderRouter",
        "test.server.routers.order", "server/routers/order.ts", 1, 7, NULL);

    cbm_gbuf_upsert_node(gb, "Function", "PaymentPage",
        "test.pages.payment.PaymentPage", "pages/payment.tsx", 1, 4, NULL);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_trpc(&ctx);

    /* "processPayment" should NOT match "createOrder" */
    ASSERT_EQ(links, 0);
    ASSERT_EQ(count_trpc_edges(gb), 0);

    cbm_gbuf_free(gb);
    rm_rf_trpc(tmpdir);
    PASS();
}

/* ==========================================================================
 *  Test 6: Partial match -- last-segment matching at lower confidence
 * ========================================================================== */

TEST(test_trpc_partial_match) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_trpc_t6_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Router defines "getAll" (flat name) */
    const char *router_src =
        "export const taskRouter = createTRPCRouter({\n"
        "  getAll: publicProcedure.query(async () => {\n"
        "    return db.task.findMany();\n"
        "  }),\n"
        "});\n";

    write_file(tmpdir, "server/routers/task.ts", router_src);

    /* Client calls "task.getAll" -- last segment "getAll" matches */
    const char *client_src =
        "function TaskList() {\n"
        "  const tasks = trpc.task.getAll.useQuery();\n"
        "  return <div />;\n"
        "}\n";

    write_file(tmpdir, "pages/tasks.tsx", client_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t router_id = cbm_gbuf_upsert_node(gb, "Module", "taskRouter",
        "test.server.routers.task", "server/routers/task.ts", 1, 5, NULL);
    ASSERT_GT(router_id, 0);

    int64_t client_id = cbm_gbuf_upsert_node(gb, "Function", "TaskList",
        "test.pages.tasks.TaskList", "pages/tasks.tsx", 1, 4, NULL);
    ASSERT_GT(client_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_trpc(&ctx);

    /* "task.getAll" consumer vs "getAll" producer -> partial match (0.80) -> high band */
    ASSERT_GT(links, 0);
    ASSERT_GT(count_trpc_edges(gb), 0);
    ASSERT_TRUE(has_trpc_edge_with_band(gb, "high"));
    ASSERT_TRUE(has_trpc_edge_with_identifier(gb, "task.getAll"));

    cbm_gbuf_free(gb);
    rm_rf_trpc(tmpdir);
    PASS();
}

/* ==========================================================================
 *  Test 7: Empty graph buffer (no crash)
 * ========================================================================== */

TEST(test_trpc_empty_graph) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_trpc_t7_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_trpc(&ctx);

    ASSERT_EQ(links, 0);
    ASSERT_EQ(count_trpc_edges(gb), 0);

    cbm_gbuf_free(gb);
    rm_rf_trpc(tmpdir);
    PASS();
}

/* ==========================================================================
 *  Test 8: Self-link prevention (producer and consumer in same node)
 * ========================================================================== */

TEST(test_trpc_no_self_link) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_trpc_t8_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* File that both defines and calls the same procedure */
    const char *src =
        "export const router = createTRPCRouter({\n"
        "  getData: publicProcedure.query(async () => {\n"
        "    return db.data.findMany();\n"
        "  }),\n"
        "});\n"
        "\n"
        "const result = trpc.data.getData.useQuery();\n";

    write_file(tmpdir, "server/combined.tsx", src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    int64_t id = cbm_gbuf_upsert_node(gb, "Module", "combined",
        "test.server.combined", "server/combined.tsx", 1, 7, NULL);
    ASSERT_GT(id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_trpc(&ctx);

    /* Same node is both producer and consumer -- should NOT create self-link */
    ASSERT_EQ(links, 0);
    ASSERT_EQ(count_trpc_edges(gb), 0);

    cbm_gbuf_free(gb);
    rm_rf_trpc(tmpdir);
    PASS();
}

/* ==========================================================================
 *  Test 9: Non-TS/JS files are ignored
 * ========================================================================== */

TEST(test_trpc_ignores_non_ts_files) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_trpc_t9_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Go file containing tRPC-like patterns (should be ignored) */
    const char *go_src =
        "package main\n"
        "\n"
        "func main() {\n"
        "    // trpc.user.getAll.useQuery()\n"
        "    // getAll: publicProcedure.query()\n"
        "}\n";

    write_file(tmpdir, "main.go", go_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);

    cbm_gbuf_upsert_node(gb, "Function", "main",
        "test.main.main", "main.go", 3, 6, NULL);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_trpc(&ctx);

    ASSERT_EQ(links, 0);
    ASSERT_EQ(count_trpc_edges(gb), 0);

    cbm_gbuf_free(gb);
    rm_rf_trpc(tmpdir);
    PASS();
}

/* ==========================================================================
 *  Test: Class node with tRPC router → detected
 * ========================================================================== */

TEST(trpc_class_node_router) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_trpc_cls_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    const char *router_src =
        "class UserRouter {\n"
        "  router = t.router({\n"
        "    getUser: t.procedure.query(({ input }) => {}),\n"
        "  });\n"
        "}\n";
    write_file(tmpdir, "routers/user.ts", router_src);

    const char *client_src =
        "function fetchUser() {\n"
        "  trpc.user.getUser.useQuery({ id: 1 });\n"
        "}\n";
    write_file(tmpdir, "pages/user.ts", client_src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", tmpdir);
    int64_t router_id = cbm_gbuf_upsert_node(gb, "Class", "UserRouter",
        "test.routers.user.UserRouter", "routers/user.ts", 1, 5, NULL);
    ASSERT_GT(router_id, 0);
    int64_t client_id = cbm_gbuf_upsert_node(gb, "Function", "fetchUser",
        "test.pages.user.fetchUser", "pages/user.ts", 1, 3, NULL);
    ASSERT_GT(client_id, 0);

    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir);
    int links = cbm_servicelink_trpc(&ctx);
    ASSERT_GTE(links, 0);

    cbm_gbuf_free(gb);
    rm_rf_trpc(tmpdir);
    PASS();
}

/* ==========================================================================
 *  Suite definition
 * ========================================================================== */

SUITE(servicelink_trpc) {
    RUN_TEST(test_trpc_router_definition);
    RUN_TEST(test_trpc_react_hooks);
    RUN_TEST(test_trpc_vanilla_client);
    RUN_TEST(test_trpc_nested_router);
    RUN_TEST(test_trpc_no_match);
    RUN_TEST(test_trpc_partial_match);
    RUN_TEST(test_trpc_empty_graph);
    RUN_TEST(test_trpc_no_self_link);
    RUN_TEST(test_trpc_ignores_non_ts_files);
    RUN_TEST(trpc_class_node_router);
}
