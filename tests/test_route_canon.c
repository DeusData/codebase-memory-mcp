/*
 * test_route_canon.c — Unit tests for cbm_route_canon_path().
 *
 * Verifies that framework-specific route-parameter placeholder syntaxes
 * (":id", "{id}", "<id>", "${id}") collapse to a single "{}" token so that a
 * client call site and a server handler rendezvous on the same Route QN
 * regardless of the language/framework that produced each side.
 * See src/pipeline/pass_route_nodes.c.
 */
#include "test_framework.h"
#include "pipeline/pipeline_internal.h"
#include <yyjson/yyjson.h>

#include <string.h>

TEST(route_canon_static_unchanged) {
    char b[128];
    ASSERT_STR_EQ(cbm_route_canon_path("/products/categories", b, sizeof(b)),
                  "/products/categories");
    PASS();
}

TEST(route_canon_colon_param) {
    char b[128];
    ASSERT_STR_EQ(cbm_route_canon_path("/players/:id", b, sizeof(b)), "/players/{}");
    PASS();
}

TEST(route_canon_brace_param) {
    char b[128];
    ASSERT_STR_EQ(cbm_route_canon_path("/players/{id}", b, sizeof(b)), "/players/{}");
    PASS();
}

/* The core invariant: Axum "{id}" and a JS client ":id" must converge. */
TEST(route_canon_colon_and_brace_converge) {
    char a[128];
    char c[128];
    cbm_route_canon_path("/clients/{id}/authorized-users", a, sizeof(a));
    cbm_route_canon_path("/clients/:clientId/authorized-users", c, sizeof(c));
    ASSERT_STR_EQ(a, c);
    ASSERT_STR_EQ(a, "/clients/{}/authorized-users");
    PASS();
}

/* Parameter names are intentionally discarded ("{id}" == ":requestId"). */
TEST(route_canon_param_name_agnostic) {
    char a[128];
    char c[128];
    cbm_route_canon_path("/link-requests/{id}/status", a, sizeof(a));
    cbm_route_canon_path("/link-requests/:requestId/status", c, sizeof(c));
    ASSERT_STR_EQ(a, c);
    PASS();
}

TEST(route_canon_angle_param) {
    char b[128];
    ASSERT_STR_EQ(cbm_route_canon_path("/users/<int:id>", b, sizeof(b)), "/users/{}");
    PASS();
}

TEST(route_canon_template_interpolation) {
    char b[128];
    ASSERT_STR_EQ(cbm_route_canon_path("/players/${playerId}", b, sizeof(b)), "/players/{}");
    PASS();
}

TEST(route_canon_multiple_params) {
    char b[128];
    ASSERT_STR_EQ(cbm_route_canon_path("/orders/{id}/items/{itemIndex}", b, sizeof(b)),
                  "/orders/{}/items/{}");
    PASS();
}

/* A ':' that is not at a segment start is literal, not a route parameter. */
TEST(route_canon_colon_mid_segment_is_literal) {
    char b[128];
    ASSERT_STR_EQ(cbm_route_canon_path("/a/b:c", b, sizeof(b)), "/a/b:c");
    PASS();
}

TEST(route_canon_null_and_empty) {
    char b[8];
    ASSERT_STR_EQ(cbm_route_canon_path("", b, sizeof(b)), "");
    ASSERT_STR_EQ(cbm_route_canon_path(NULL, b, sizeof(b)), "");
    PASS();
}

/* A tight output buffer must still yield a bounded, NUL-terminated string. */
TEST(route_canon_truncation_safe) {
    char b[6];
    const char *r = cbm_route_canon_path("/players/:id", b, sizeof(b));
    ASSERT(strlen(r) < sizeof(b));
    PASS();
}

TEST(route_identity_http_default_is_canonical_json) {
    char qn[CBM_ROUTE_QN_SIZE];
    char props[CBM_SZ_256];
    ASSERT_TRUE(cbm_pipeline_build_service_route_identity(
        "/players/:id", CBM_SVC_HTTP, NULL, NULL, "arg_url", qn, sizeof(qn), props,
        sizeof(props)));
    ASSERT_STR_EQ(qn, "__route__ANY__/players/{}");
    ASSERT_NOT_NULL(strstr(props, "\"method\":\"" CBM_ROUTE_DEFAULT_METHOD "\""));
    ASSERT_NOT_NULL(strstr(props, "\"source\":\"arg_url\""));
    yyjson_doc *doc = yyjson_read(props, strlen(props), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_doc_free(doc);
    PASS();
}

TEST(route_identity_source_is_escaped_json) {
    char qn[CBM_ROUTE_QN_SIZE];
    char props[CBM_SZ_256];
    ASSERT_TRUE(cbm_pipeline_build_service_route_identity(
        "/api/orders", CBM_SVC_HTTP, "GET", NULL, "decor\"ator", qn, sizeof(qn), props,
        sizeof(props)));
    ASSERT_STR_EQ(qn, "__route__GET__/api/orders");
    ASSERT_NOT_NULL(strstr(props, "\"source\":\"decor\\\"ator\""));
    yyjson_doc *doc = yyjson_read(props, strlen(props), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_doc_free(doc);
    PASS();
}

TEST(route_identity_async_default_is_canonical_json) {
    char qn[CBM_ROUTE_QN_SIZE];
    char props[CBM_SZ_256];
    ASSERT_TRUE(cbm_pipeline_build_service_route_identity(
        "orders.created", CBM_SVC_ASYNC, NULL, NULL, NULL, qn, sizeof(qn), props,
        sizeof(props)));
    ASSERT_STR_EQ(qn, "__route__" CBM_ROUTE_DEFAULT_ASYNC_BROKER "__orders.created");
    ASSERT_STR_EQ(props, "{\"broker\":\"" CBM_ROUTE_DEFAULT_ASYNC_BROKER "\"}");
    PASS();
}

TEST(route_identity_rejects_unknown_service_kind) {
    char qn[CBM_ROUTE_QN_SIZE];
    char props[CBM_SZ_256];
    ASSERT_FALSE(cbm_pipeline_build_service_route_identity(
        "/api/orders", CBM_SVC_CONFIG, NULL, NULL, NULL, qn, sizeof(qn), props,
        sizeof(props)));
    PASS();
}

SUITE(route_canon) {
    RUN_TEST(route_canon_static_unchanged);
    RUN_TEST(route_canon_colon_param);
    RUN_TEST(route_canon_brace_param);
    RUN_TEST(route_canon_colon_and_brace_converge);
    RUN_TEST(route_canon_param_name_agnostic);
    RUN_TEST(route_canon_angle_param);
    RUN_TEST(route_canon_template_interpolation);
    RUN_TEST(route_canon_multiple_params);
    RUN_TEST(route_canon_colon_mid_segment_is_literal);
    RUN_TEST(route_canon_null_and_empty);
    RUN_TEST(route_canon_truncation_safe);
    RUN_TEST(route_identity_http_default_is_canonical_json);
    RUN_TEST(route_identity_source_is_escaped_json);
    RUN_TEST(route_identity_async_default_is_canonical_json);
    RUN_TEST(route_identity_rejects_unknown_service_kind);
}
