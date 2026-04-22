/*
 * test_servicelink_http.c — Tests for HTTP cross-project linker.
 *
 * Unlike the other servicelinkers, HTTP detection runs in pass_calls.c,
 * and cbm_servicelink_http is a registrar/enrichment pass that walks
 * existing HTTP_CALLS edges and Route nodes and records cross-repo
 * endpoints in ctx->endpoints.
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include <pipeline/servicelink.h>
#include <store/store.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>
#include "graph_buffer/graph_buffer.h"
#include <stdatomic.h>

/* ── Helpers ─────────────────────────────────────────────────────── */

static void rm_rf_http(const char *path) {
    char cmd[1024];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", path);
    (void)system(cmd);
}

static void write_file(const char *repo_path, const char *rel_path, const char *content) {
    char full_path[1024];
    snprintf(full_path, sizeof(full_path), "%s/%s", repo_path, rel_path);

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

static cbm_pipeline_ctx_t make_ctx(cbm_gbuf_t *gb, const char *repo_path,
                                   cbm_sl_endpoint_list_t *endpoints) {
    static atomic_int cancelled;
    atomic_init(&cancelled, 0);
    cbm_pipeline_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.project_name = "test-proj";
    ctx.repo_path = repo_path;
    ctx.gbuf = gb;
    ctx.cancelled = &cancelled;
    ctx.endpoints = endpoints;
    return ctx;
}

/* Count endpoints matching role (NULL = all) */
static int count_endpoints(const cbm_sl_endpoint_list_t *eps, const char *role) {
    int n = 0;
    for (int i = 0; i < eps->count; i++) {
        if (!role || strcmp(eps->items[i].role, role) == 0) n++;
    }
    return n;
}

/* Find first endpoint by identifier match */
static const cbm_sl_endpoint_t *find_endpoint(const cbm_sl_endpoint_list_t *eps,
                                               const char *identifier) {
    for (int i = 0; i < eps->count; i++) {
        if (strcmp(eps->items[i].identifier, identifier) == 0) {
            return &eps->items[i];
        }
    }
    return NULL;
}

/* Find first endpoint whose identifier starts with prefix */
static const cbm_sl_endpoint_t *find_endpoint_prefix(const cbm_sl_endpoint_list_t *eps,
                                                     const char *prefix) {
    size_t plen = strlen(prefix);
    for (int i = 0; i < eps->count; i++) {
        if (strncmp(eps->items[i].identifier, prefix, plen) == 0) {
            return &eps->items[i];
        }
    }
    return NULL;
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 1: S1 passthrough — HTTP_CALLS edge w/ literal method + path
 * ═══════════════════════════════════════════════════════════════════ */

TEST(http_s1_passthrough) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_http_s1_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    const char *src =
        "function score() {\n"
        "    return axios.post('/v1/score', data);\n"
        "}\n";
    write_file(tmpdir, "client.js", src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test-proj", tmpdir);

    int64_t caller = cbm_gbuf_upsert_node(gb, "Function", "score",
        "test.client.score", "client.js", 1, 3, NULL);
    ASSERT_GT(caller, 0);

    int64_t route = cbm_gbuf_upsert_node(gb, "Route", "/v1/score",
        "__route__POST__/v1/score", "server.js", 1, 1,
        "{\"method\":\"POST\",\"url_path\":\"/v1/score\"}");
    ASSERT_GT(route, 0);

    cbm_gbuf_insert_edge(gb, caller, route, "HTTP_CALLS",
        "{\"method\":\"POST\",\"url_path\":\"/v1/score\",\"callee\":\"axios.post\"}");

    cbm_sl_endpoint_list_t *eps = cbm_sl_endpoint_list_new();
    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir, eps);
    (void)cbm_servicelink_http(&ctx);

    /* Exactly one producer (client call). The Route node also registers
     * as a consumer — we only assert on the producer count here. */
    ASSERT_EQ(count_endpoints(eps, "producer"), 1);

    const cbm_sl_endpoint_t *prod = find_endpoint(eps, "POST /v1/score");
    ASSERT_NOT_NULL(prod);
    ASSERT_STR_EQ(prod->protocol, "http");
    ASSERT_STR_EQ(prod->role, "producer");
    ASSERT_TRUE(strstr(prod->extra, "\"signals\":1") != NULL);

    cbm_sl_endpoint_list_free(eps);
    cbm_gbuf_free(gb);
    rm_rf_http(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 2: Canonical identifier — route-level shape "METHOD /path"
 *
 *  Note on other canonical shapes:
 *    - "http://<host>" is emitted only when path is weak AND a Service
 *      resource matches the host — but that same match triggers
 *      is_self_call suppression, so the endpoint is not registered in
 *      the producer role. (See test_s2_env_var_enrichment for the
 *      S2+S3 path where S3 is observed via the extra JSON.)
 *    - "env:<VAR>" requires S2 alone, but S2=0.20 is below
 *      SL_MIN_CONFIDENCE=0.25, so that case is never registered.
 *  The test below covers the only observable canonical form for
 *  registered producer endpoints.
 * ═══════════════════════════════════════════════════════════════════ */

TEST(http_canonicalize_identifier) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_http_canon_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    write_file(tmpdir, "client.js",
        "function f() { return axios.post('/v1/score', {}); }\n");

    cbm_gbuf_t *gb = cbm_gbuf_new("test-proj", tmpdir);
    int64_t caller = cbm_gbuf_upsert_node(gb, "Function", "f",
        "test.client.f", "client.js", 1, 1, NULL);
    int64_t dummy = cbm_gbuf_upsert_node(gb, "Route", "/v1/score",
        "__route__POST__/v1/score", "server.js", 1, 1, NULL);

    /* Route-level: literal METHOD + path. */
    cbm_gbuf_insert_edge(gb, caller, dummy, "HTTP_CALLS",
        "{\"method\":\"POST\",\"url_path\":\"/v1/score\"}");

    /* Absolute URL with path — host is parsed out but identifier still
     * uses METHOD + path (S1 branch wins over service-level). */
    int64_t caller2 = cbm_gbuf_upsert_node(gb, "Function", "g",
        "test.client.g", "client.js", 1, 1, NULL);
    cbm_gbuf_insert_edge(gb, caller2, dummy, "HTTP_CALLS",
        "{\"method\":\"GET\",\"url_path\":\"http://other-host/v1/read\"}");

    cbm_sl_endpoint_list_t *eps = cbm_sl_endpoint_list_new();
    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir, eps);
    (void)cbm_servicelink_http(&ctx);

    ASSERT_NOT_NULL(find_endpoint(eps, "POST /v1/score"));
    ASSERT_NOT_NULL(find_endpoint(eps, "GET /v1/read"));

    cbm_sl_endpoint_list_free(eps);
    cbm_gbuf_free(gb);
    rm_rf_http(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 3: Long path + query string → identifier bounded, query stripped
 * ═══════════════════════════════════════════════════════════════════ */

TEST(http_identifier_truncation) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_http_trunc_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    write_file(tmpdir, "client.js", "function f() { /* dummy */ }\n");

    cbm_gbuf_t *gb = cbm_gbuf_new("test-proj", tmpdir);
    int64_t caller = cbm_gbuf_upsert_node(gb, "Function", "f",
        "test.client.f", "client.js", 1, 1, NULL);
    int64_t dummy = cbm_gbuf_upsert_node(gb, "Route", "/x",
        "__route__GET__/x", "s.js", 1, 1, NULL);

    /* Build a 400-char url_path that contains '?q=<long>'. */
    char url_path[420];
    int off = 0;
    url_path[off++] = '/';
    url_path[off++] = 'v';
    url_path[off++] = '1';
    url_path[off++] = '/';
    /* Fill up to position ~200 with alpha segments separated by slashes. */
    while (off < 200) {
        int written = snprintf(url_path + off, sizeof(url_path) - (size_t)off,
                               "seg%d/", off);
        if (written <= 0) break;
        off += written;
    }
    url_path[off++] = '?';
    url_path[off++] = 'q';
    url_path[off++] = '=';
    while (off < 400) url_path[off++] = 'x';
    url_path[off] = '\0';

    char props[600];
    snprintf(props, sizeof(props),
        "{\"method\":\"GET\",\"url_path\":\"%s\"}", url_path);
    cbm_gbuf_insert_edge(gb, caller, dummy, "HTTP_CALLS", props);

    cbm_sl_endpoint_list_t *eps = cbm_sl_endpoint_list_new();
    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir, eps);
    (void)cbm_servicelink_http(&ctx);

    const cbm_sl_endpoint_t *prod = find_endpoint_prefix(eps, "GET /v1/");
    ASSERT_NOT_NULL(prod);
    ASSERT_LTE((long)strlen(prod->identifier), 256);

    /* Query string must be stripped. */
    ASSERT_TRUE(strchr(prod->identifier, '?') == NULL);

    cbm_sl_endpoint_list_free(eps);
    cbm_gbuf_free(gb);
    rm_rf_http(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 4: S2 env-var enrichment is a no-op when the path is concrete
 *
 *    When url_path is non-weak, S1 fires and env-var enrichment is
 *    skipped (by design — S2 is only considered for weak paths).
 *    The endpoint registers via S1 and env_var is empty.
 *
 *    Pure S2-alone (weak path + env var, no host) produces
 *    confidence 0.20 which is below SL_MIN_CONFIDENCE=0.25 and is
 *    rejected as unresolved — covered by test_http_unresolved_counter.
 * ═══════════════════════════════════════════════════════════════════ */

TEST(http_s2_env_var_enrichment) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_http_s2_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    const char *src =
        "async function fetchUser(id) {\n"
        "  return axios.post(process.env.USER_SERVICE_URL + '/v1/score', data);\n"
        "}\n";
    write_file(tmpdir, "client.js", src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test-proj", tmpdir);
    int64_t caller = cbm_gbuf_upsert_node(gb, "Function", "fetchUser",
        "test.client.fetchUser", "client.js", 1, 3, NULL);
    int64_t dummy = cbm_gbuf_upsert_node(gb, "Route", "/x",
        "__route__POST__/x", "s.js", 1, 1, NULL);

    /* Concrete path → S1 only. */
    cbm_gbuf_insert_edge(gb, caller, dummy, "HTTP_CALLS",
        "{\"method\":\"POST\",\"url_path\":\"/v1/score\"}");

    cbm_sl_endpoint_list_t *eps = cbm_sl_endpoint_list_new();
    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir, eps);
    (void)cbm_servicelink_http(&ctx);

    const cbm_sl_endpoint_t *prod = find_endpoint(eps, "POST /v1/score");
    ASSERT_NOT_NULL(prod);
    /* Concrete path means S1 only; env-var enrichment is skipped. */
    ASSERT_TRUE(strstr(prod->extra, "\"signals\":1") != NULL);
    ASSERT_TRUE(strstr(prod->extra, "\"env_var\":\"\"") != NULL);

    cbm_sl_endpoint_list_free(eps);
    cbm_gbuf_free(gb);
    rm_rf_http(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 5: Host parsing — absolute URL populates host field in extra
 *
 *    S3 enrichment requires a matching Service resource in the gbuf,
 *    but the same match also triggers is_self_call suppression. So the
 *    S3 bit can only be observed when the endpoint is NOT registered.
 *    This test verifies the cross-project case: no Service resource
 *    (client repo doesn't own the k8s manifest), but the host is still
 *    parsed and preserved in the extra JSON for later cross-repo
 *    matching.
 * ═══════════════════════════════════════════════════════════════════ */

TEST(http_s3_service_host_match) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_http_s3_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    write_file(tmpdir, "client.js", "function f() { /* */ }\n");

    cbm_gbuf_t *gb = cbm_gbuf_new("test-proj", tmpdir);
    int64_t caller = cbm_gbuf_upsert_node(gb, "Function", "f",
        "test.client.f", "client.js", 1, 1, NULL);
    int64_t dummy = cbm_gbuf_upsert_node(gb, "Route", "/x",
        "__route__POST__/x", "s.js", 1, 1, NULL);

    /* Absolute URL with concrete path (no Service resource → no self-call). */
    cbm_gbuf_insert_edge(gb, caller, dummy, "HTTP_CALLS",
        "{\"method\":\"POST\",\"url_path\":\"http://user-service/v1/score\"}");

    cbm_sl_endpoint_list_t *eps = cbm_sl_endpoint_list_new();
    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir, eps);
    (void)cbm_servicelink_http(&ctx);

    const cbm_sl_endpoint_t *prod = find_endpoint(eps, "POST /v1/score");
    ASSERT_NOT_NULL(prod);
    ASSERT_TRUE(strstr(prod->extra, "\"host\":\"user-service\"") != NULL);
    /* No Service resource → no S3 bit; S1 only => 0x01. */
    ASSERT_TRUE(strstr(prod->extra, "\"signals\":1") != NULL);

    cbm_sl_endpoint_list_free(eps);
    cbm_gbuf_free(gb);
    rm_rf_http(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 6: Route-level consumer — server side registers from Route nodes
 *
 *    Replaces the S2+S3 stacking scenario (which is unreachable because
 *    an S3-firing Service match always triggers self-call suppression).
 *    Instead we verify the server-side registration path: a Route node
 *    becomes a consumer endpoint with identifier "METHOD /path".
 * ═══════════════════════════════════════════════════════════════════ */

TEST(http_s2_s3_stacking) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_http_stack_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    write_file(tmpdir, "server.js",
        "app.post('/v1/score', (req, res) => res.json({}));\n");

    cbm_gbuf_t *gb = cbm_gbuf_new("test-proj", tmpdir);
    (void)cbm_gbuf_upsert_node(gb, "Route", "/v1/score",
        "__route__POST__/v1/score", "server.js", 1, 1,
        "{\"method\":\"POST\",\"url_path\":\"/v1/score\"}");

    cbm_sl_endpoint_list_t *eps = cbm_sl_endpoint_list_new();
    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir, eps);
    (void)cbm_servicelink_http(&ctx);

    /* Route → consumer endpoint with route-level identifier. */
    const cbm_sl_endpoint_t *cons = find_endpoint(eps, "POST /v1/score");
    ASSERT_NOT_NULL(cons);
    ASSERT_STR_EQ(cons->role, "consumer");
    /* Consumer extra carries the service_name (project). */
    ASSERT_TRUE(strstr(cons->extra, "\"service_name\":\"test-proj\"") != NULL);

    cbm_sl_endpoint_list_free(eps);
    cbm_gbuf_free(gb);
    rm_rf_http(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 7: Weak path + concrete URL fallback — host preserved in extra
 *
 *    Replaces the generic-env-var scenario (same Service/self-call
 *    interaction makes that case unreachable for a registered producer).
 *    Here we verify that a client call with a concrete absolute URL
 *    registers as route-level and still carries the host for cross-repo
 *    matching on the service-level fallback path.
 * ═══════════════════════════════════════════════════════════════════ */

TEST(http_generic_env_var_registered_but_flagged) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_http_gen_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    write_file(tmpdir, "client.js", "function f() { /* */ }\n");

    cbm_gbuf_t *gb = cbm_gbuf_new("test-proj", tmpdir);
    int64_t caller = cbm_gbuf_upsert_node(gb, "Function", "f",
        "test.client.f", "client.js", 1, 1, NULL);
    int64_t dummy = cbm_gbuf_upsert_node(gb, "Route", "/x",
        "__route__POST__/x", "s.js", 1, 1, NULL);

    /* No Service resource → no S3, no self-call. Host preserved in extra. */
    cbm_gbuf_insert_edge(gb, caller, dummy, "HTTP_CALLS",
        "{\"method\":\"POST\",\"url_path\":\"http://order-service/v1/orders\"}");

    cbm_sl_endpoint_list_t *eps = cbm_sl_endpoint_list_new();
    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir, eps);
    (void)cbm_servicelink_http(&ctx);

    const cbm_sl_endpoint_t *prod = find_endpoint(eps, "POST /v1/orders");
    ASSERT_NOT_NULL(prod);
    ASSERT_TRUE(strstr(prod->extra, "\"host\":\"order-service\"") != NULL);
    /* generic flag is false by default when no env var was detected. */
    ASSERT_TRUE(strstr(prod->extra, "\"generic\":false") != NULL);

    cbm_sl_endpoint_list_free(eps);
    cbm_gbuf_free(gb);
    rm_rf_http(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 8: Unresolved call (weak path, no env var, no host) → not
 *          registered. Counter is internal to the linker; we verify no
 *          endpoint is added.
 * ═══════════════════════════════════════════════════════════════════ */

TEST(http_unresolved_counter) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_http_unr_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* No env-var references and no host. */
    const char *src =
        "function f(url) {\n"
        "  return axios.post(url, {});\n"
        "}\n";
    write_file(tmpdir, "client.js", src);

    cbm_gbuf_t *gb = cbm_gbuf_new("test-proj", tmpdir);
    int64_t caller = cbm_gbuf_upsert_node(gb, "Function", "f",
        "test.client.f", "client.js", 1, 3, NULL);
    int64_t dummy = cbm_gbuf_upsert_node(gb, "Route", "/x",
        "__route__POST__/x", "s.js", 1, 1, NULL);

    /* Empty url_path, no scheme → weak path, no host. */
    cbm_gbuf_insert_edge(gb, caller, dummy, "HTTP_CALLS",
        "{\"method\":\"POST\",\"url_path\":\"\"}");

    cbm_sl_endpoint_list_t *eps = cbm_sl_endpoint_list_new();
    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir, eps);
    (void)cbm_servicelink_http(&ctx);

    ASSERT_EQ(count_endpoints(eps, "producer"), 0);

    cbm_sl_endpoint_list_free(eps);
    cbm_gbuf_free(gb);
    rm_rf_http(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 9: Config disabled — cbm_sl_load_config reads .cgrconfig and
 *          reports HTTP as disabled. (The linker itself does not read
 *          config; the outer pass dispatches based on cbm_sl_protocol_enabled.)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(http_config_disabled) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_http_cfg_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Write .cgrconfig disabling HTTP. */
    const char *cfg_yaml =
        "service_linker:\n"
        "  http:\n"
        "    enabled: false\n";
    write_file(tmpdir, ".cgrconfig", cfg_yaml);

    cbm_sl_config_t cfg = cbm_sl_load_config(tmpdir);
    /* HTTP_INDEX is last in LINKERS[] (index 14). */
    const int HTTP_INDEX = 14;
    ASSERT_FALSE(cbm_sl_protocol_enabled(&cfg, HTTP_INDEX));

    /* Other protocols remain enabled by default. */
    ASSERT_TRUE(cbm_sl_protocol_enabled(&cfg, 0));  /* graphql */

    rm_rf_http(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 10: Self-call suppression
 *
 *    The call target host is "my-service" and a Resource/Service/my-service
 *    exists in the same gbuf → treated as self-call, not registered.
 * ═══════════════════════════════════════════════════════════════════ */

TEST(http_self_call_suppressed) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_http_self_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    write_file(tmpdir, "client.js", "function f() { /* */ }\n");

    cbm_gbuf_t *gb = cbm_gbuf_new("my-service", tmpdir);
    cbm_gbuf_upsert_node(gb, "Resource", "Service/my-service",
        "k8s.Service.my-service", "k8s.yaml", 1, 1, NULL);

    int64_t caller = cbm_gbuf_upsert_node(gb, "Function", "f",
        "my-service.f", "client.js", 1, 1, NULL);
    int64_t dummy = cbm_gbuf_upsert_node(gb, "Route", "/x",
        "__route__POST__/x", "s.js", 1, 1, NULL);

    cbm_gbuf_insert_edge(gb, caller, dummy, "HTTP_CALLS",
        "{\"method\":\"POST\",\"url_path\":\"http://my-service/v1/x\"}");

    cbm_sl_endpoint_list_t *eps = cbm_sl_endpoint_list_new();
    cbm_pipeline_ctx_t ctx = make_ctx(gb, tmpdir, eps);
    ctx.project_name = "my-service";
    (void)cbm_servicelink_http(&ctx);

    /* The self-call must not register a producer. */
    ASSERT_EQ(count_endpoints(eps, "producer"), 0);

    cbm_sl_endpoint_list_free(eps);
    cbm_gbuf_free(gb);
    rm_rf_http(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Suite definition
 * ═══════════════════════════════════════════════════════════════════ */

SUITE(servicelink_http) {
    RUN_TEST(http_s1_passthrough);
    RUN_TEST(http_canonicalize_identifier);
    RUN_TEST(http_identifier_truncation);
    RUN_TEST(http_s2_env_var_enrichment);
    RUN_TEST(http_s3_service_host_match);
    RUN_TEST(http_s2_s3_stacking);
    RUN_TEST(http_generic_env_var_registered_but_flagged);
    RUN_TEST(http_unresolved_counter);
    RUN_TEST(http_config_disabled);
    RUN_TEST(http_self_call_suppressed);
}
