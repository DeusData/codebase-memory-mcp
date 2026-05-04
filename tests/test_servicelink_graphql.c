/*
 * test_servicelink_graphql.c — Tests for GraphQL cross-service protocol linking.
 *
 * Tests cover:
 *   - SDL definition scanning (.graphql files)
 *   - Resolver detection (decorators, Go gqlgen, JS resolver objects)
 *   - Client call detection (gql tag, useQuery hooks, apollo client, .execute)
 *   - End-to-end matching with correct confidence bands
 *   - Name normalization (camelCase <-> snake_case matching)
 *   - Fuzzy matching via normalized Levenshtein
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

/* ── Helpers ───────────────────────────────────────────────────── */

/* Write a file with the given content. Creates parent dir if needed. */
static int write_file(const char *path, const char *content) {
    FILE *f = fopen(path, "w");
    if (!f) {
        return -1;
    }
    fputs(content, f);
    fclose(f);
    return 0;
}

/* Recursive rmdir helper (removes files and subdirs) */
static void rm_rf(const char *path) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", path);
    (void)system(cmd);
}

/* Create a pipeline context for testing with a graph buffer and repo path */
static cbm_pipeline_ctx_t make_test_ctx(cbm_gbuf_t *gbuf, const char *repo_path) {
    cbm_pipeline_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.project_name = "test";
    ctx.repo_path = repo_path;
    ctx.gbuf = gbuf;

    /* Provide a non-NULL cancelled flag (not cancelled) */
    static atomic_int not_cancelled;
    atomic_init(&not_cancelled, 0);
    ctx.cancelled = &not_cancelled;

    return ctx;
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 1: SDL file scanning — Query fields become producers
 * ═══════════════════════════════════════════════════════════════════ */

TEST(graphql_sdl_query_fields) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/gql-sdl-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        SKIP("cbm_mkdtemp failed");
    }

    /* Write a .graphql SDL file */
    char schema_path[512];
    snprintf(schema_path, sizeof(schema_path), "%s/schema.graphql", tmpdir);
    write_file(schema_path, "type Query {\n"
                            "  getUser(id: ID!): User\n"
                            "  listOrders(limit: Int): [Order]\n"
                            "}\n"
                            "\n"
                            "type Mutation {\n"
                            "  createUser(input: CreateUserInput!): User\n"
                            "}\n");

    /* Write a client .ts file that uses these operations */
    char client_path[512];
    snprintf(client_path, sizeof(client_path), "%s/client.ts", tmpdir);
    write_file(client_path, "const GET_USER = gql`\n"
                            "  query getUser($id: ID!) {\n"
                            "    getUser(id: $id) { name email }\n"
                            "  }\n"
                            "`;\n"
                            "\n"
                            "function UserComponent() {\n"
                            "  const { data } = useQuery(GET_USER);\n"
                            "}\n");

    /* Create graph buffer and add nodes */
    cbm_gbuf_t *gbuf = cbm_gbuf_new("test", tmpdir);
    ASSERT_NOT_NULL(gbuf);

    /* SDL file node spans the entire file */
    int64_t schema_id = cbm_gbuf_upsert_node(gbuf, "Module", "schema",
                                              "test.schema.graphql", "schema.graphql",
                                              1, 9, "{}");
    ASSERT_NEQ(schema_id, 0);

    /* Client function node */
    int64_t client_id = cbm_gbuf_upsert_node(gbuf, "Function", "UserComponent",
                                              "test.client.UserComponent", "client.ts",
                                              1, 9, "{}");
    ASSERT_NEQ(client_id, 0);

    /* Run the linker */
    cbm_pipeline_ctx_t ctx = make_test_ctx(gbuf, tmpdir);
    int result = cbm_servicelink_graphql(&ctx);
    ASSERT_GTE(result, 0);

    /* Check that GRAPHQL_CALLS edges were created */
    int edge_count = cbm_gbuf_edge_count_by_type(gbuf, "GRAPHQL_CALLS");
    ASSERT_GT(edge_count, 0);

    cbm_gbuf_free(gbuf);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 2: gql tagged template client call detection
 * ═══════════════════════════════════════════════════════════════════ */

TEST(graphql_gql_tag_detection) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/gql-tag-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        SKIP("cbm_mkdtemp failed");
    }

    /* Schema file with a Query field */
    char schema_path[512];
    snprintf(schema_path, sizeof(schema_path), "%s/schema.graphql", tmpdir);
    write_file(schema_path, "type Query {\n"
                            "  fetchPosts(limit: Int): [Post]\n"
                            "}\n");

    /* Client file with gql` query fetchPosts ... ` */
    char client_path[512];
    snprintf(client_path, sizeof(client_path), "%s/posts.ts", tmpdir);
    write_file(client_path, "const FETCH_POSTS = gql`\n"
                            "  query fetchPosts($limit: Int) {\n"
                            "    fetchPosts(limit: $limit) { id title }\n"
                            "  }\n"
                            "`;\n");

    cbm_gbuf_t *gbuf = cbm_gbuf_new("test", tmpdir);
    ASSERT_NOT_NULL(gbuf);

    int64_t schema_id = cbm_gbuf_upsert_node(gbuf, "Module", "schema",
                                              "test.schema", "schema.graphql",
                                              1, 3, "{}");
    ASSERT_NEQ(schema_id, 0);

    int64_t client_id = cbm_gbuf_upsert_node(gbuf, "Function", "fetchPostsQuery",
                                              "test.posts.fetchPostsQuery", "posts.ts",
                                              1, 5, "{}");
    ASSERT_NEQ(client_id, 0);

    cbm_pipeline_ctx_t ctx = make_test_ctx(gbuf, tmpdir);
    int result = cbm_servicelink_graphql(&ctx);
    ASSERT_GTE(result, 0);

    /* Should have found the link: fetchPosts consumer -> fetchPosts producer */
    int edge_count = cbm_gbuf_edge_count_by_type(gbuf, "GRAPHQL_CALLS");
    ASSERT_EQ(edge_count, 1);

    cbm_gbuf_free(gbuf);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 3: useQuery / useMutation hook detection
 * ═══════════════════════════════════════════════════════════════════ */

TEST(graphql_use_hooks) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/gql-hook-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        SKIP("cbm_mkdtemp failed");
    }

    /* Schema */
    char schema_path[512];
    snprintf(schema_path, sizeof(schema_path), "%s/schema.graphql", tmpdir);
    write_file(schema_path, "type Query {\n"
                            "  getProfile(id: ID!): Profile\n"
                            "}\n"
                            "type Mutation {\n"
                            "  updateProfile(input: ProfileInput!): Profile\n"
                            "}\n");

    /* React component using hooks */
    char comp_path[512];
    snprintf(comp_path, sizeof(comp_path), "%s/Profile.tsx", tmpdir);
    write_file(comp_path, "const GET_PROFILE = gql`query getProfile { ... }`;\n"
                          "const UPDATE_PROFILE = gql`mutation updateProfile { ... }`;\n"
                          "\n"
                          "function ProfileComponent() {\n"
                          "  const { data } = useQuery(GET_PROFILE);\n"
                          "  const [update] = useMutation(UPDATE_PROFILE);\n"
                          "}\n");

    cbm_gbuf_t *gbuf = cbm_gbuf_new("test", tmpdir);
    ASSERT_NOT_NULL(gbuf);

    cbm_gbuf_upsert_node(gbuf, "Module", "schema",
                          "test.schema", "schema.graphql",
                          1, 6, "{}");
    cbm_gbuf_upsert_node(gbuf, "Function", "ProfileComponent",
                          "test.Profile.ProfileComponent", "Profile.tsx",
                          1, 7, "{}");

    cbm_pipeline_ctx_t ctx = make_test_ctx(gbuf, tmpdir);
    int result = cbm_servicelink_graphql(&ctx);
    ASSERT_GTE(result, 0);

    /* Should find links for both getProfile and updateProfile */
    int edge_count = cbm_gbuf_edge_count_by_type(gbuf, "GRAPHQL_CALLS");
    ASSERT_GTE(edge_count, 1);

    cbm_gbuf_free(gbuf);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 4: Go gqlgen resolver detection
 * ═══════════════════════════════════════════════════════════════════ */

TEST(graphql_go_resolver) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/gql-gores-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        SKIP("cbm_mkdtemp failed");
    }

    /* Go resolver file */
    char resolver_path[512];
    snprintf(resolver_path, sizeof(resolver_path), "%s/resolver.go", tmpdir);
    write_file(resolver_path,
               "package graph\n"
               "\n"
               "func (r *queryResolver) GetUser(ctx context.Context, id string) (*User, error) {\n"
               "    return r.userService.FindByID(ctx, id)\n"
               "}\n"
               "\n"
               "func (r *mutationResolver) CreateUser(ctx context.Context, input NewUser) (*User, error) {\n"
               "    return r.userService.Create(ctx, input)\n"
               "}\n");

    /* Client calling getUser via gql */
    char client_path[512];
    snprintf(client_path, sizeof(client_path), "%s/client.ts", tmpdir);
    write_file(client_path, "const query = gql`query GetUser($id: ID!) {\n"
                            "  getUser(id: $id) { name email }\n"
                            "}`;\n");

    cbm_gbuf_t *gbuf = cbm_gbuf_new("test", tmpdir);
    ASSERT_NOT_NULL(gbuf);

    int64_t resolver_id = cbm_gbuf_upsert_node(gbuf, "Method", "GetUser",
                                                "test.resolver.GetUser", "resolver.go",
                                                1, 9, "{}");
    ASSERT_NEQ(resolver_id, 0);

    int64_t client_id = cbm_gbuf_upsert_node(gbuf, "Function", "fetchUser",
                                              "test.client.fetchUser", "client.ts",
                                              1, 3, "{}");
    ASSERT_NEQ(client_id, 0);

    cbm_pipeline_ctx_t ctx = make_test_ctx(gbuf, tmpdir);
    int result = cbm_servicelink_graphql(&ctx);
    ASSERT_GTE(result, 0);

    /* Should link GetUser consumer to GetUser resolver producer */
    int edge_count = cbm_gbuf_edge_count_by_type(gbuf, "GRAPHQL_CALLS");
    ASSERT_EQ(edge_count, 1);

    cbm_gbuf_free(gbuf);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 5: Python client.execute detection
 * ═══════════════════════════════════════════════════════════════════ */

TEST(graphql_python_execute) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/gql-pyexec-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        SKIP("cbm_mkdtemp failed");
    }

    /* Schema */
    char schema_path[512];
    snprintf(schema_path, sizeof(schema_path), "%s/schema.graphql", tmpdir);
    write_file(schema_path, "type Query {\n"
                            "  searchProducts(term: String!): [Product]\n"
                            "}\n");

    /* Python client */
    char py_path[512];
    snprintf(py_path, sizeof(py_path), "%s/client.py", tmpdir);
    write_file(py_path, "def search_products(client, term):\n"
                        "    result = client.execute(\"query searchProducts($term: String!) {\n"
                        "        searchProducts(term: $term) { id name price }\n"
                        "    }\")\n"
                        "    return result\n");

    cbm_gbuf_t *gbuf = cbm_gbuf_new("test", tmpdir);
    ASSERT_NOT_NULL(gbuf);

    cbm_gbuf_upsert_node(gbuf, "Module", "schema",
                          "test.schema", "schema.graphql",
                          1, 3, "{}");
    cbm_gbuf_upsert_node(gbuf, "Function", "search_products",
                          "test.client.search_products", "client.py",
                          1, 5, "{}");

    cbm_pipeline_ctx_t ctx = make_test_ctx(gbuf, tmpdir);
    int result = cbm_servicelink_graphql(&ctx);
    ASSERT_GTE(result, 0);

    /* Should find searchProducts link */
    int edge_count = cbm_gbuf_edge_count_by_type(gbuf, "GRAPHQL_CALLS");
    ASSERT_EQ(edge_count, 1);

    cbm_gbuf_free(gbuf);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 6: NestJS @Query/@Mutation decorator detection
 * ═══════════════════════════════════════════════════════════════════ */

TEST(graphql_decorator_resolvers) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/gql-decor-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        SKIP("cbm_mkdtemp failed");
    }

    /* NestJS-style resolver */
    char resolver_path[512];
    snprintf(resolver_path, sizeof(resolver_path), "%s/user.resolver.ts", tmpdir);
    write_file(resolver_path,
               "@Resolver()\n"
               "export class UserResolver {\n"
               "  @Query('getUser')\n"
               "  async getUser(@Args('id') id: string) {\n"
               "    return this.userService.findOne(id);\n"
               "  }\n"
               "\n"
               "  @Mutation('createUser')\n"
               "  async createUser(@Args('input') input: CreateUserInput) {\n"
               "    return this.userService.create(input);\n"
               "  }\n"
               "}\n");

    /* Client using gql tags */
    char client_path[512];
    snprintf(client_path, sizeof(client_path), "%s/userClient.ts", tmpdir);
    write_file(client_path, "const q = gql`query getUser($id: ID!) {\n"
                            "  getUser(id: $id) { name }\n"
                            "}`;\n");

    cbm_gbuf_t *gbuf = cbm_gbuf_new("test", tmpdir);
    ASSERT_NOT_NULL(gbuf);

    cbm_gbuf_upsert_node(gbuf, "Method", "getUser",
                          "test.user.resolver.getUser", "user.resolver.ts",
                          1, 12, "{}");
    cbm_gbuf_upsert_node(gbuf, "Function", "userClient",
                          "test.userClient.userClient", "userClient.ts",
                          1, 3, "{}");

    cbm_pipeline_ctx_t ctx = make_test_ctx(gbuf, tmpdir);
    int result = cbm_servicelink_graphql(&ctx);
    ASSERT_GTE(result, 0);

    /* Decorator resolver should match gql tag consumer */
    int edge_count = cbm_gbuf_edge_count_by_type(gbuf, "GRAPHQL_CALLS");
    ASSERT_GTE(edge_count, 1);

    cbm_gbuf_free(gbuf);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 7: Resolver object pattern (Apollo Server style)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(graphql_resolver_object) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/gql-robj-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        SKIP("cbm_mkdtemp failed");
    }

    /* Apollo Server resolver map */
    char resolver_path[512];
    snprintf(resolver_path, sizeof(resolver_path), "%s/resolvers.js", tmpdir);
    write_file(resolver_path,
               "const resolvers = {\n"
               "  Query: {\n"
               "    getBooks: (parent, args) => books,\n"
               "    getAuthor: (parent, args) => findAuthor(args.id),\n"
               "  },\n"
               "  Mutation: {\n"
               "    addBook: (parent, args) => createBook(args),\n"
               "  },\n"
               "};\n");

    /* Client */
    char client_path[512];
    snprintf(client_path, sizeof(client_path), "%s/bookClient.ts", tmpdir);
    write_file(client_path, "const q = gql`query getBooks {\n"
                            "  getBooks { title author }\n"
                            "}`;\n");

    cbm_gbuf_t *gbuf = cbm_gbuf_new("test", tmpdir);
    ASSERT_NOT_NULL(gbuf);

    cbm_gbuf_upsert_node(gbuf, "Function", "resolvers",
                          "test.resolvers.resolvers", "resolvers.js",
                          1, 9, "{}");
    cbm_gbuf_upsert_node(gbuf, "Function", "bookQuery",
                          "test.bookClient.bookQuery", "bookClient.ts",
                          1, 3, "{}");

    cbm_pipeline_ctx_t ctx = make_test_ctx(gbuf, tmpdir);
    int result = cbm_servicelink_graphql(&ctx);
    ASSERT_GTE(result, 0);

    int edge_count = cbm_gbuf_edge_count_by_type(gbuf, "GRAPHQL_CALLS");
    ASSERT_GTE(edge_count, 1);

    cbm_gbuf_free(gbuf);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 8: No producers — should create zero edges
 * ═══════════════════════════════════════════════════════════════════ */

TEST(graphql_no_producers) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/gql-noprod-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        SKIP("cbm_mkdtemp failed");
    }

    /* Only a client file, no schema or resolvers */
    char client_path[512];
    snprintf(client_path, sizeof(client_path), "%s/orphan.ts", tmpdir);
    write_file(client_path, "const q = gql`query FetchStuff { stuff { id } }`;\n");

    cbm_gbuf_t *gbuf = cbm_gbuf_new("test", tmpdir);
    ASSERT_NOT_NULL(gbuf);

    cbm_gbuf_upsert_node(gbuf, "Function", "orphan",
                          "test.orphan.orphan", "orphan.ts",
                          1, 1, "{}");

    cbm_pipeline_ctx_t ctx = make_test_ctx(gbuf, tmpdir);
    int result = cbm_servicelink_graphql(&ctx);
    ASSERT_EQ(result, 0);

    int edge_count = cbm_gbuf_edge_count_by_type(gbuf, "GRAPHQL_CALLS");
    ASSERT_EQ(edge_count, 0);

    cbm_gbuf_free(gbuf);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 9: No consumers — should create zero edges
 * ═══════════════════════════════════════════════════════════════════ */

TEST(graphql_no_consumers) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/gql-nocons-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        SKIP("cbm_mkdtemp failed");
    }

    /* Only a schema, no client code */
    char schema_path[512];
    snprintf(schema_path, sizeof(schema_path), "%s/schema.graphql", tmpdir);
    write_file(schema_path, "type Query {\n"
                            "  hello: String\n"
                            "}\n");

    cbm_gbuf_t *gbuf = cbm_gbuf_new("test", tmpdir);
    ASSERT_NOT_NULL(gbuf);

    cbm_gbuf_upsert_node(gbuf, "Module", "schema",
                          "test.schema", "schema.graphql",
                          1, 3, "{}");

    cbm_pipeline_ctx_t ctx = make_test_ctx(gbuf, tmpdir);
    int result = cbm_servicelink_graphql(&ctx);
    ASSERT_EQ(result, 0);

    int edge_count = cbm_gbuf_edge_count_by_type(gbuf, "GRAPHQL_CALLS");
    ASSERT_EQ(edge_count, 0);

    cbm_gbuf_free(gbuf);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 10: Normalized name matching (camelCase <-> snake_case)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(graphql_normalized_matching) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/gql-norm-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        SKIP("cbm_mkdtemp failed");
    }

    /* Schema uses camelCase */
    char schema_path[512];
    snprintf(schema_path, sizeof(schema_path), "%s/schema.graphql", tmpdir);
    write_file(schema_path, "type Query {\n"
                            "  getUserProfile(id: ID!): Profile\n"
                            "}\n");

    /* Python client uses snake_case operation name (but as a query it matches
     * when normalized: get_user_profile -> getuserprofile == getuserprofile) */
    char py_path[512];
    snprintf(py_path, sizeof(py_path), "%s/client.py", tmpdir);
    write_file(py_path, "result = client.execute(\"query get_user_profile($id: ID!) {\n"
                        "    getUserProfile(id: $id) { name }\n"
                        "}\")\n");

    cbm_gbuf_t *gbuf = cbm_gbuf_new("test", tmpdir);
    ASSERT_NOT_NULL(gbuf);

    cbm_gbuf_upsert_node(gbuf, "Module", "schema",
                          "test.schema", "schema.graphql",
                          1, 3, "{}");
    cbm_gbuf_upsert_node(gbuf, "Function", "call_graphql",
                          "test.client.call_graphql", "client.py",
                          1, 3, "{}");

    cbm_pipeline_ctx_t ctx = make_test_ctx(gbuf, tmpdir);
    int result = cbm_servicelink_graphql(&ctx);
    ASSERT_GTE(result, 0);

    /* Normalized match should create an edge */
    int edge_count = cbm_gbuf_edge_count_by_type(gbuf, "GRAPHQL_CALLS");
    ASSERT_EQ(edge_count, 1);

    cbm_gbuf_free(gbuf);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 11: Multiple operations in one file
 * ═══════════════════════════════════════════════════════════════════ */

TEST(graphql_multiple_operations) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/gql-multi-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        SKIP("cbm_mkdtemp failed");
    }

    /* Schema with multiple fields */
    char schema_path[512];
    snprintf(schema_path, sizeof(schema_path), "%s/schema.graphql", tmpdir);
    write_file(schema_path, "type Query {\n"
                            "  listUsers: [User]\n"
                            "  getProduct(id: ID!): Product\n"
                            "  searchItems(term: String!): [Item]\n"
                            "}\n");

    /* Client with multiple gql tags */
    char client_path[512];
    snprintf(client_path, sizeof(client_path), "%s/api.ts", tmpdir);
    write_file(client_path,
               "const LIST_USERS = gql`query listUsers { listUsers { id name } }`;\n"
               "const GET_PRODUCT = gql`query getProduct($id: ID!) { getProduct(id: $id) { name } }`;\n"
               "const SEARCH = gql`query searchItems($term: String!) { searchItems(term: $term) { id } }`;\n");

    cbm_gbuf_t *gbuf = cbm_gbuf_new("test", tmpdir);
    ASSERT_NOT_NULL(gbuf);

    cbm_gbuf_upsert_node(gbuf, "Module", "schema",
                          "test.schema", "schema.graphql",
                          1, 5, "{}");
    cbm_gbuf_upsert_node(gbuf, "Function", "apiQueries",
                          "test.api.apiQueries", "api.ts",
                          1, 3, "{}");

    cbm_pipeline_ctx_t ctx = make_test_ctx(gbuf, tmpdir);
    int result = cbm_servicelink_graphql(&ctx);
    ASSERT_GTE(result, 0);

    /* Multiple operations between same node pair get merged into one edge
     * by gbuf dedup on (source_id, target_id, type). Verify at least 1 edge. */
    int edge_count = cbm_gbuf_edge_count_by_type(gbuf, "GRAPHQL_CALLS");
    ASSERT_GTE(edge_count, 1);

    cbm_gbuf_free(gbuf);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 12: apolloClient.query detection
 * ═══════════════════════════════════════════════════════════════════ */

TEST(graphql_apollo_client) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/gql-apollo-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        SKIP("cbm_mkdtemp failed");
    }

    /* Schema */
    char schema_path[512];
    snprintf(schema_path, sizeof(schema_path), "%s/schema.graphql", tmpdir);
    write_file(schema_path, "type Query {\n"
                            "  dashboard: DashboardData\n"
                            "}\n");

    /* Client with apolloClient.query pattern */
    char client_path[512];
    snprintf(client_path, sizeof(client_path), "%s/dashboard.ts", tmpdir);
    write_file(client_path, "const DASHBOARD_QUERY = gql`query dashboard { ... }`;\n"
                            "async function loadDashboard() {\n"
                            "  const result = await apolloClient.query({ query: DASHBOARD_QUERY });\n"
                            "}\n");

    cbm_gbuf_t *gbuf = cbm_gbuf_new("test", tmpdir);
    ASSERT_NOT_NULL(gbuf);

    cbm_gbuf_upsert_node(gbuf, "Module", "schema",
                          "test.schema", "schema.graphql",
                          1, 3, "{}");
    cbm_gbuf_upsert_node(gbuf, "Function", "loadDashboard",
                          "test.dashboard.loadDashboard", "dashboard.ts",
                          1, 4, "{}");

    cbm_pipeline_ctx_t ctx = make_test_ctx(gbuf, tmpdir);
    int result = cbm_servicelink_graphql(&ctx);
    ASSERT_GTE(result, 0);

    /* gql tag should match; apollo .query may also match */
    int edge_count = cbm_gbuf_edge_count_by_type(gbuf, "GRAPHQL_CALLS");
    ASSERT_GTE(edge_count, 1);

    cbm_gbuf_free(gbuf);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 13: Empty graph buffer — should return 0 links gracefully
 * ═══════════════════════════════════════════════════════════════════ */

TEST(graphql_empty_graph) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/gql-empty-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        SKIP("cbm_mkdtemp failed");
    }

    cbm_gbuf_t *gbuf = cbm_gbuf_new("test", tmpdir);
    ASSERT_NOT_NULL(gbuf);

    cbm_pipeline_ctx_t ctx = make_test_ctx(gbuf, tmpdir);
    int result = cbm_servicelink_graphql(&ctx);
    ASSERT_EQ(result, 0);

    int edge_count = cbm_gbuf_edge_count_by_type(gbuf, "GRAPHQL_CALLS");
    ASSERT_EQ(edge_count, 0);

    cbm_gbuf_free(gbuf);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 14: Subscription type
 * ═══════════════════════════════════════════════════════════════════ */

TEST(graphql_subscription) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/gql-sub-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        SKIP("cbm_mkdtemp failed");
    }

    /* Schema with subscription */
    char schema_path[512];
    snprintf(schema_path, sizeof(schema_path), "%s/schema.graphql", tmpdir);
    write_file(schema_path, "type Subscription {\n"
                            "  onMessageReceived: Message\n"
                            "}\n");

    /* Client subscribing */
    char client_path[512];
    snprintf(client_path, sizeof(client_path), "%s/sub.ts", tmpdir);
    write_file(client_path, "const SUB = gql`subscription onMessageReceived {\n"
                            "  onMessageReceived { id body sender }\n"
                            "}`;\n");

    cbm_gbuf_t *gbuf = cbm_gbuf_new("test", tmpdir);
    ASSERT_NOT_NULL(gbuf);

    cbm_gbuf_upsert_node(gbuf, "Module", "schema",
                          "test.schema", "schema.graphql",
                          1, 3, "{}");
    cbm_gbuf_upsert_node(gbuf, "Function", "subscribeFn",
                          "test.sub.subscribeFn", "sub.ts",
                          1, 3, "{}");

    cbm_pipeline_ctx_t ctx = make_test_ctx(gbuf, tmpdir);
    int result = cbm_servicelink_graphql(&ctx);
    ASSERT_GTE(result, 0);

    int edge_count = cbm_gbuf_edge_count_by_type(gbuf, "GRAPHQL_CALLS");
    ASSERT_EQ(edge_count, 1);

    cbm_gbuf_free(gbuf);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 15: Confidence bands — exact vs normalized vs fuzzy
 * ═══════════════════════════════════════════════════════════════════ */

TEST(graphql_confidence_bands) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/gql-conf-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        SKIP("cbm_mkdtemp failed");
    }

    /* Schema with a field */
    char schema_path[512];
    snprintf(schema_path, sizeof(schema_path), "%s/schema.graphql", tmpdir);
    write_file(schema_path, "type Query {\n"
                            "  getOrderDetails(id: ID!): OrderDetails\n"
                            "}\n");

    /* Client with exact match */
    char exact_path[512];
    snprintf(exact_path, sizeof(exact_path), "%s/exact.ts", tmpdir);
    write_file(exact_path, "const Q = gql`query getOrderDetails { ... }`;\n");

    cbm_gbuf_t *gbuf = cbm_gbuf_new("test", tmpdir);
    ASSERT_NOT_NULL(gbuf);

    int64_t schema_id = cbm_gbuf_upsert_node(gbuf, "Module", "schema",
                                              "test.schema", "schema.graphql",
                                              1, 3, "{}");
    int64_t exact_id = cbm_gbuf_upsert_node(gbuf, "Function", "exactFn",
                                             "test.exact.exactFn", "exact.ts",
                                             1, 1, "{}");
    ASSERT_NEQ(schema_id, 0);
    ASSERT_NEQ(exact_id, 0);

    cbm_pipeline_ctx_t ctx = make_test_ctx(gbuf, tmpdir);
    int result = cbm_servicelink_graphql(&ctx);
    ASSERT_GTE(result, 0);

    /* Verify edge was created */
    int edge_count = cbm_gbuf_edge_count_by_type(gbuf, "GRAPHQL_CALLS");
    ASSERT_EQ(edge_count, 1);

    /* Verify the edge has high confidence (exact match = 0.95) */
    const cbm_gbuf_edge_t **edges = NULL;
    int ecount = 0;
    cbm_gbuf_find_edges_by_type(gbuf, "GRAPHQL_CALLS", &edges, &ecount);
    ASSERT_EQ(ecount, 1);
    ASSERT_NOT_NULL(edges[0]->properties_json);

    /* The properties should contain "high" confidence band */
    ASSERT_NOT_NULL(strstr(edges[0]->properties_json, "\"high\""));

    cbm_gbuf_free(gbuf);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 16: gql tag with operation name different from field name → matched via field
 * ═══════════════════════════════════════════════════════════════════ */

TEST(graphql_field_name_extraction) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/gql-field-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        SKIP("cbm_mkdtemp failed");
    }

    /* Schema file: field is "formatMessage" */
    char schema_path[512];
    snprintf(schema_path, sizeof(schema_path), "%s/schema.graphql", tmpdir);
    write_file(schema_path, "type Query {\n"
                            "  formatMessage(params: FormatMessageParams!): FormatMessageResult\n"
                            "}\n");

    /* Client file: operation name is "formatNotification", field is "formatMessage" */
    char client_path[512];
    snprintf(client_path, sizeof(client_path), "%s/notify.ts", tmpdir);
    write_file(client_path, "async function sendNotification() {\n"
                            "  const result = await gateway.request(\n"
                            "    gql`\n"
                            "      query formatNotification($params: FormatMessageParams!) {\n"
                            "        formatMessage(params: $params) {\n"
                            "          subject\n"
                            "          body\n"
                            "        }\n"
                            "      }\n"
                            "    `\n"
                            "  );\n"
                            "}\n");

    cbm_gbuf_t *gbuf = cbm_gbuf_new("test", tmpdir);
    ASSERT_NOT_NULL(gbuf);

    int64_t schema_id = cbm_gbuf_upsert_node(gbuf, "Module", "schema",
                                              "test.schema", "schema.graphql",
                                              1, 3, "{}");
    ASSERT_NEQ(schema_id, 0);

    int64_t client_id = cbm_gbuf_upsert_node(gbuf, "Function", "sendNotification",
                                              "test.notify.sendNotification", "notify.ts",
                                              1, 12, "{}");
    ASSERT_NEQ(client_id, 0);

    cbm_pipeline_ctx_t ctx = make_test_ctx(gbuf, tmpdir);
    int result = cbm_servicelink_graphql(&ctx);
    ASSERT_GTE(result, 0);

    /* Should have found the link via field name "formatMessage" (not operation name) */
    int edge_count = cbm_gbuf_edge_count_by_type(gbuf, "GRAPHQL_CALLS");
    ASSERT_EQ(edge_count, 1);

    cbm_gbuf_free(gbuf);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 17: Class node with gql tag → detected as consumer
 * ═══════════════════════════════════════════════════════════════════ */

TEST(graphql_class_node_consumer) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/gql-class-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        SKIP("cbm_mkdtemp failed");
    }

    char schema_path[512];
    snprintf(schema_path, sizeof(schema_path), "%s/schema.graphql", tmpdir);
    write_file(schema_path, "type Query {\n"
                            "  getUser(id: ID!): User\n"
                            "}\n");

    char client_path[512];
    snprintf(client_path, sizeof(client_path), "%s/UserService.ts", tmpdir);
    write_file(client_path, "class UserService {\n"
                            "  static query = gql`\n"
                            "    query getUser($id: ID!) {\n"
                            "      getUser(id: $id) { name email }\n"
                            "    }\n"
                            "  `;\n"
                            "}\n");

    cbm_gbuf_t *gbuf = cbm_gbuf_new("test", tmpdir);
    ASSERT_NOT_NULL(gbuf);

    int64_t schema_id = cbm_gbuf_upsert_node(gbuf, "Module", "schema",
                                              "test.schema", "schema.graphql",
                                              1, 3, "{}");
    ASSERT_NEQ(schema_id, 0);

    int64_t class_id = cbm_gbuf_upsert_node(gbuf, "Class", "UserService",
                                              "test.UserService", "UserService.ts",
                                              1, 7, "{}");
    ASSERT_NEQ(class_id, 0);

    cbm_pipeline_ctx_t ctx = make_test_ctx(gbuf, tmpdir);
    int result = cbm_servicelink_graphql(&ctx);
    ASSERT_GTE(result, 0);

    int edge_count = cbm_gbuf_edge_count_by_type(gbuf, "GRAPHQL_CALLS");
    ASSERT_EQ(edge_count, 1);

    cbm_gbuf_free(gbuf);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 18: Variable node with gql tag → detected as consumer
 * ═══════════════════════════════════════════════════════════════════ */

TEST(graphql_variable_node_consumer) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/gql-var-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        SKIP("cbm_mkdtemp failed");
    }

    char schema_path[512];
    snprintf(schema_path, sizeof(schema_path), "%s/schema.graphql", tmpdir);
    write_file(schema_path, "type Query {\n"
                            "  listPosts(limit: Int): [Post]\n"
                            "}\n");

    char client_path[512];
    snprintf(client_path, sizeof(client_path), "%s/queries.ts", tmpdir);
    write_file(client_path, "const LIST_POSTS = gql`\n"
                            "  query listPosts($limit: Int) {\n"
                            "    listPosts(limit: $limit) { id title }\n"
                            "  }\n"
                            "`;\n");

    cbm_gbuf_t *gbuf = cbm_gbuf_new("test", tmpdir);
    ASSERT_NOT_NULL(gbuf);

    int64_t schema_id = cbm_gbuf_upsert_node(gbuf, "Module", "schema",
                                              "test.schema", "schema.graphql",
                                              1, 3, "{}");
    ASSERT_NEQ(schema_id, 0);

    int64_t var_id = cbm_gbuf_upsert_node(gbuf, "Variable", "LIST_POSTS",
                                            "test.queries.LIST_POSTS", "queries.ts",
                                            1, 5, "{}");
    ASSERT_NEQ(var_id, 0);

    cbm_pipeline_ctx_t ctx = make_test_ctx(gbuf, tmpdir);
    int result = cbm_servicelink_graphql(&ctx);
    ASSERT_GTE(result, 0);

    int edge_count = cbm_gbuf_edge_count_by_type(gbuf, "GRAPHQL_CALLS");
    ASSERT_EQ(edge_count, 1);

    cbm_gbuf_free(gbuf);
    rm_rf(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Suite definition
 * ═══════════════════════════════════════════════════════════════════ */

SUITE(servicelink_graphql) {
    RUN_TEST(graphql_sdl_query_fields);
    RUN_TEST(graphql_gql_tag_detection);
    RUN_TEST(graphql_use_hooks);
    RUN_TEST(graphql_go_resolver);
    RUN_TEST(graphql_python_execute);
    RUN_TEST(graphql_decorator_resolvers);
    RUN_TEST(graphql_resolver_object);
    RUN_TEST(graphql_no_producers);
    RUN_TEST(graphql_no_consumers);
    RUN_TEST(graphql_normalized_matching);
    RUN_TEST(graphql_multiple_operations);
    RUN_TEST(graphql_apollo_client);
    RUN_TEST(graphql_empty_graph);
    RUN_TEST(graphql_subscription);
    RUN_TEST(graphql_confidence_bands);
    RUN_TEST(graphql_field_name_extraction);
    RUN_TEST(graphql_class_node_consumer);
    RUN_TEST(graphql_variable_node_consumer);
}
