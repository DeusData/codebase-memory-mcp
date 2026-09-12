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
#include "service_patterns.h"

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

/* ── Go 1.22 ServeMux "METHOD /path" pattern splitting ─────────── */

TEST(go_mux_split_method_pattern) {
    const char *method = NULL;
    ASSERT_STR_EQ(cbm_go_split_mux_pattern("GET /users/{id}", &method), "/users/{id}");
    ASSERT_STR_EQ(method, "GET");
    PASS();
}

TEST(go_mux_split_host_pattern) {
    /* Host prefix is skipped; "{$}" stays for canonicalization. */
    const char *method = NULL;
    ASSERT_STR_EQ(cbm_go_split_mux_pattern("GET example.com/{$}", &method), "/{$}");
    ASSERT_STR_EQ(method, "GET");
    PASS();
}

TEST(go_mux_split_wildcard_tail) {
    const char *method = NULL;
    ASSERT_STR_EQ(cbm_go_split_mux_pattern("POST /orders/{id...}", &method), "/orders/{id...}");
    ASSERT_STR_EQ(method, "POST");
    PASS();
}

TEST(go_mux_split_plain_path_is_null) {
    /* No leading method → not a method-qualified pattern. */
    const char *method = NULL;
    ASSERT(cbm_go_split_mux_pattern("/legacy", &method) == NULL);
    ASSERT(method == NULL);
    PASS();
}

TEST(go_mux_split_prose_is_null) {
    /* A space between method and '/' beyond the separator means prose, and an
     * unknown method never splits. */
    const char *method = NULL;
    ASSERT(cbm_go_split_mux_pattern("GET the file /tmp/x", &method) == NULL);
    ASSERT(cbm_go_split_mux_pattern("FETCH /x", &method) == NULL);
    ASSERT(cbm_go_split_mux_pattern("GET ", &method) == NULL);
    ASSERT(cbm_go_split_mux_pattern("GETTER /x", &method) == NULL);
    PASS();
}

TEST(go_mux_split_canon_converges) {
    /* End-to-end invariant: the split path canonicalizes to the same QN body a
     * client ":id" call site produces. */
    const char *method = NULL;
    const char *p = cbm_go_split_mux_pattern("GET /users/{id}", &method);
    char a[128];
    char c[128];
    cbm_route_canon_path(p, a, sizeof(a));
    cbm_route_canon_path("/users/:id", c, sizeof(c));
    ASSERT_STR_EQ(a, c);
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
    RUN_TEST(go_mux_split_method_pattern);
    RUN_TEST(go_mux_split_host_pattern);
    RUN_TEST(go_mux_split_wildcard_tail);
    RUN_TEST(go_mux_split_plain_path_is_null);
    RUN_TEST(go_mux_split_prose_is_null);
    RUN_TEST(go_mux_split_canon_converges);
}
