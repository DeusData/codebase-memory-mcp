/*
 * repro_issue523.c — Reproduce-first case for issue #523.
 *
 * BUG: "cross-repo-intelligence returns 0 edges for a byte-identical call/route"
 *
 * Root cause (pass_calls.c::resolve_single_call):
 *
 *   When a Python client uses `import requests` and calls
 *   `requests.get("/api/orders/{id}")`, the `requests` package is an external
 *   pip dependency whose source is NOT present in the indexed tree.
 *   `cbm_registry_resolve` resolves the callee name to a candidate QN
 *   containing "requests", but `cbm_gbuf_find_by_qn(ctx->gbuf, res.qualified_name)`
 *   returns NULL — the node does not exist in the graph because `requests` was
 *   never indexed.  The guard at pass_calls.c::resolve_single_call line ~406:
 *
 *       const cbm_gbuf_node_t *target_node = cbm_gbuf_find_by_qn(ctx->gbuf, res.qualified_name);
 *       if (!target_node || source_node->id == target_node->id)
 *           return 0;          ← call is SILENTLY DROPPED
 *
 *   causes the call to be silently dropped before it ever reaches
 *   `emit_classified_edge` / `emit_http_async_edge`.  No HTTP_CALLS edge is
 *   created in the client project DB.
 *
 *   Without an HTTP_CALLS edge in the client DB, `match_http_routes` in
 *   pass_cross_repo.c finds nothing to iterate over, and `cbm_cross_repo_match`
 *   returns http_edges == 0 — even when the server project has a perfectly
 *   matching Route node (byte-identical path, correct method) and a HANDLES
 *   edge pointing to the handler function.
 *
 * Expected (correct) behaviour:
 *   A call to an external HTTP client library (e.g. `requests.get`) with a
 *   URL/path first argument MUST produce an HTTP_CALLS edge in the client
 *   project DB, even when the library's source is not indexed.  The linker
 *   should detect the service-pattern match on the resolved QN substring
 *   ("requests") and emit the edge before consulting the node graph.
 *   Subsequently, `cbm_cross_repo_match` must produce at least one
 *   CROSS_HTTP_CALLS edge linking the client caller to the server route handler
 *   when the client url_path (canonicalized) matches the server Route QN.
 *
 * Actual (buggy) behaviour:
 *   cbm_cross_repo_match returns http_edges == 0.  The assertion below is RED.
 *
 * Companion: pass_calls.c (sequential path) and pass_parallel.c (parallel path)
 * both share the same guard; fixing one requires fixing both.
 *
 * Note on parallel pipeline:
 *   HTTP_CALLS edges are produced on BOTH the sequential (< 50 files) and
 *   parallel (>= 50 files) pipeline paths, so this test uses a small fixture
 *   (< 50 files) and exercises the sequential path.  The parallel path has the
 *   same root cause and is covered by the same fix (pass_parallel.c::
 *   finalize_and_emit has an identical unindexed-node guard).
 */

#include "test_framework.h"
#include "repro_harness.h"
#include "pipeline/pass_cross_repo.h"

#include <stdio.h>
#include <string.h>

/* ── Fixture files ───────────────────────────────────────────────────────── */

/*
 * CLIENT SERVICE (order-client):
 *   Uses the real `requests` library imported at the top of the file.
 *   The `requests` package is NOT present in the indexed tree (no vendored
 *   source, no stub) — this is exactly the real-world multi-service scenario.
 *   The caller function `fetch_order` makes a GET request to the byte-identical
 *   path "/api/orders/{id}" that the server registers.
 *
 * WHY this triggers the bug:
 *   cbm_registry_resolve("requests.get", …) returns a candidate QN that
 *   contains "requests" (service-pattern match → CBM_SVC_HTTP), BUT
 *   cbm_gbuf_find_by_qn returns NULL for that QN because no `requests` node
 *   was ever inserted into the graph buffer.  resolve_single_call returns 0,
 *   the call is dropped, and no HTTP_CALLS edge is created.
 */
static const RFile client_files[] = {
    {"client/orders.py",
     "import requests\n"
     "\n"
     "\n"
     "BASE_URL = \"http://order-service:8080\"\n"
     "\n"
     "\n"
     "def fetch_order(order_id):\n"
     "    \"\"\"Fetch a single order from the order service.\"\"\"\n"
     "    return requests.get(\"/api/orders/{id}\", params={\"id\": order_id})\n"
     "\n"
     "\n"
     "def list_orders():\n"
     "    \"\"\"Fetch all orders from the order service.\"\"\"\n"
     "    return requests.get(\"/api/orders\")\n"},
};
enum { N_CLIENT_FILES = (int)(sizeof(client_files) / sizeof(client_files[0])) };

/*
 * SERVER SERVICE (order-service):
 *   A minimal Flask application that defines the route handler for the path
 *   the client calls.  The path "/api/orders/{id}" is byte-identical to the
 *   client's call argument.  Flask uses `{id}` parameter syntax; the extractor
 *   mints a Route node with QN `__route__GET__/api/orders/{}` (canonicalized
 *   via cbm_route_canon_path).  A HANDLES edge links the Route to `get_order`.
 */
static const RFile server_files[] = {
    {"server/app.py", "from flask import Flask, jsonify\n"
                      "\n"
                      "app = Flask(__name__)\n"
                      "\n"
                      "\n"
                      "@app.get(\"/api/orders/{id}\")\n"
                      "def get_order(order_id):\n"
                      "    \"\"\"Return a single order by id.\"\"\"\n"
                      "    return jsonify({\"id\": order_id, \"status\": \"ok\"})\n"
                      "\n"
                      "\n"
                      "@app.get(\"/api/orders\")\n"
                      "def list_orders():\n"
                      "    \"\"\"Return all orders.\"\"\"\n"
                      "    return jsonify({\"orders\": []})\n"},
};
enum { N_SERVER_FILES = (int)(sizeof(server_files) / sizeof(server_files[0])) };

/* ── Reproduction test ───────────────────────────────────────────────────── */

/*
 * TEST: repro_issue523_crossrepo_http_calls_edge
 *
 * Steps:
 *   1. Index the CLIENT service — expect HTTP_CALLS >= 1 (currently 0: RED
 *      because unindexed `requests` causes the call to be dropped).
 *   2. Index the SERVER service — expect Route nodes >= 1 (this side is GREEN;
 *      Flask decorator extraction is correct).
 *   3. Run cbm_cross_repo_match(client_project, [server_project], 1).
 *   4. Assert result.http_edges >= 1 — this is the cross-repo edge count.
 *      Currently 0 because step 1 yields no HTTP_CALLS to match.
 *
 * The assertion at step 4 is the canonical RED line.  Steps 1 and 3 are
 * diagnostic: step 1 prints the http_calls count so the fix can be verified
 * independently; step 3 fails fast if the server was not indexed correctly.
 */
TEST(repro_issue523_crossrepo_http_calls_edge) {
    /* ── Index client service ─────────────────────────────────── */
    RProj client;
    cbm_store_t *client_store = rh_index_files(&client, client_files, N_CLIENT_FILES);
    ASSERT_NOT_NULL(client_store);

    int client_http = rh_count_edges(client_store, client.project, "HTTP_CALLS");
    fprintf(stderr,
            "  [523] client HTTP_CALLS=%d  "
            "(expected>=1; 0=bug: requests not indexed → call dropped)\n",
            client_http);

    cbm_store_close(client_store);
    client_store = NULL; /* re-opened inside cbm_cross_repo_match via cache dir */

    /* ── Index server service ─────────────────────────────────── */
    RProj server;
    cbm_store_t *server_store = rh_index_files(&server, server_files, N_SERVER_FILES);
    ASSERT_NOT_NULL(server_store);

    int server_routes = rh_count_label(server_store, server.project, "Route");
    fprintf(stderr, "  [523] server Route nodes=%d  (expected>=2; 0=extractor broken)\n",
            server_routes);
    /* Server-side extraction is correct — if this fails the test environment is
     * broken, not the cross-repo linker.  Fail fast with a clear message. */
    if (server_routes < 1) {
        cbm_store_close(server_store);
        rh_cleanup(&client, NULL);
        rh_cleanup(&server, server_store);
        FAIL("server route extraction broken — test environment issue, not issue #523");
    }

    cbm_store_close(server_store);
    server_store = NULL; /* re-opened bidirectionally inside cbm_cross_repo_match */

    /* ── Cross-repo match ─────────────────────────────────────── */
    /*
     * cbm_cross_repo_match opens both project DBs from the cache directory
     * (the same $HOME/.cache/codebase-memory-mcp/<project>.db paths that
     * rh_open_indexed wrote).  It iterates HTTP_CALLS edges in the client DB
     * and looks for matching Route QNs in the server DB.
     *
     * Correct: http_edges >= 1 (at least one edge for /api/orders/{id}).
     * Buggy:   http_edges == 0 (no HTTP_CALLS in client → nothing to match).
     */
    const char *server_project = server.project;
    cbm_cross_repo_result_t result = cbm_cross_repo_match(client.project, &server_project, 1);

    fprintf(stderr,
            "  [523] cross_repo http_edges=%d  "
            "(expected>=1; 0=bug confirmed: issue #523)\n",
            result.http_edges);

    /* ── Cleanup ──────────────────────────────────────────────── */
    rh_cleanup(&client, NULL);
    rh_cleanup(&server, NULL);

    /*
     * WHY RED: result.http_edges == 0 on current code.
     *
     * The root cause is in resolve_single_call (pass_calls.c ~line 405):
     *   cbm_gbuf_find_by_qn returns NULL for the `requests` QN (not indexed).
     *   The function returns 0 before reaching emit_classified_edge.
     *   No HTTP_CALLS edge is written to the client DB.
     *   match_http_routes in pass_cross_repo.c finds no HTTP_CALLS to iterate.
     *   cbm_cross_repo_match returns http_edges = 0.
     *
     * The fix must allow emit_http_async_edge to fire for service-pattern
     * matches even when the resolved target node is absent from the graph buffer
     * (i.e., skip the cbm_gbuf_find_by_qn guard for CBM_SVC_HTTP / CBM_SVC_ASYNC
     * calls, or create a synthetic stub node so the guard passes).
     */
    ASSERT_GTE(result.http_edges, 1);

    PASS();
}

/* ── #523 follow-up: cross-repo matcher behaviors ────────────────────────────
 *
 * The canonical repro above covers the client-emission root cause. These three
 * cover the pass_cross_repo.c matching behaviors added alongside it:
 *   (A) scheme/host-stripped full-URL match,
 *   (B) concrete-path vs templated-route fuzzy match,
 *   (C) provider-side reverse run + dedup idempotence.
 */

/* (A) Client calls a FULL URL (scheme+host+port); server route is a bare path.
 * cr_url_path() must strip the scheme+authority so the paths match. */
static const RFile a_client_files[] = {
    {"client/orders.py", "import requests\n"
                         "\n"
                         "\n"
                         "def list_orders():\n"
                         "    return requests.get(\"http://order-service:8080/api/orders\")\n"},
};
static const RFile a_server_files[] = {
    {"server/app.py", "from flask import Flask, jsonify\n"
                      "\n"
                      "app = Flask(__name__)\n"
                      "\n"
                      "\n"
                      "@app.get(\"/api/orders\")\n"
                      "def list_orders():\n"
                      "    return jsonify({\"orders\": []})\n"},
};

TEST(repro_issue523_crossrepo_scheme_stripped_url_match) {
    RProj client;
    cbm_store_t *cs = rh_index_files(&client, a_client_files, 1);
    ASSERT_NOT_NULL(cs);
    cbm_store_close(cs);

    RProj server;
    cbm_store_t *ss = rh_index_files(&server, a_server_files, 1);
    ASSERT_NOT_NULL(ss);
    cbm_store_close(ss);

    const char *sp = server.project;
    cbm_cross_repo_result_t r = cbm_cross_repo_match(client.project, &sp, 1);
    fprintf(stderr, "  [523-A] scheme-stripped http_edges=%d (expected>=1)\n", r.http_edges);

    rh_cleanup(&client, NULL);
    rh_cleanup(&server, NULL);
    ASSERT_GTE(r.http_edges, 1);
    PASS();
}

/* (B) Client calls a CONCRETE path ("/api/orders/42"); server registers a
 * TEMPLATED route ("/api/orders/{id}" -> canonical "{}"). The exact QN lookup
 * misses; find_route_handler_fuzzy must segment-match concrete vs template. */
static const RFile b_client_files[] = {
    {"client/orders.py", "import requests\n"
                         "\n"
                         "\n"
                         "def fetch_order():\n"
                         "    return requests.get(\"http://order-service:8080/api/orders/42\")\n"},
};
static const RFile b_server_files[] = {
    {"server/app.py", "from flask import Flask, jsonify\n"
                      "\n"
                      "app = Flask(__name__)\n"
                      "\n"
                      "\n"
                      "@app.get(\"/api/orders/{id}\")\n"
                      "def get_order(order_id):\n"
                      "    return jsonify({\"id\": order_id})\n"},
};

TEST(repro_issue523_crossrepo_template_fuzzy_match) {
    RProj client;
    cbm_store_t *cs = rh_index_files(&client, b_client_files, 1);
    ASSERT_NOT_NULL(cs);
    cbm_store_close(cs);

    RProj server;
    cbm_store_t *ss = rh_index_files(&server, b_server_files, 1);
    ASSERT_NOT_NULL(ss);
    cbm_store_close(ss);

    const char *sp = server.project;
    cbm_cross_repo_result_t r = cbm_cross_repo_match(client.project, &sp, 1);
    fprintf(stderr, "  [523-B] concrete->template http_edges=%d (expected>=1)\n", r.http_edges);

    rh_cleanup(&client, NULL);
    rh_cleanup(&server, NULL);
    ASSERT_GTE(r.http_edges, 1);
    PASS();
}

/* (C) Reverse direction + dedup idempotence. Run the match from the PROVIDER
 * side (server as source): the server has no outbound HTTP_CALLS, so only the
 * reverse pass can discover the client's call. Running twice must not inflate:
 * the second run inserts no new rows, and the client DB holds exactly one
 * CROSS_HTTP_CALLS edge. */
TEST(repro_issue523_crossrepo_reverse_and_dedup) {
    RProj client;
    cbm_store_t *cs = rh_index_files(&client, b_client_files, 1);
    ASSERT_NOT_NULL(cs);
    cbm_store_close(cs);

    RProj server;
    cbm_store_t *ss = rh_index_files(&server, b_server_files, 1);
    ASSERT_NOT_NULL(ss);
    cbm_store_close(ss);

    /* Run from the provider side: source = server, target = client. */
    const char *cp = client.project;
    cbm_cross_repo_result_t r1 = cbm_cross_repo_match(server.project, &cp, 1);
    fprintf(stderr, "  [523-C] provider-side run1 http_edges=%d (expected>=1)\n", r1.http_edges);
    ASSERT_GTE(r1.http_edges, 1);

    /* Second identical run must insert no new rows (dedup idempotence). */
    cbm_cross_repo_result_t r2 = cbm_cross_repo_match(server.project, &cp, 1);
    fprintf(stderr, "  [523-C] provider-side run2 http_edges=%d (expected 0, no new rows)\n",
            r2.http_edges);
    ASSERT_EQ(r2.http_edges, 0);

    /* Exactly one CROSS_HTTP_CALLS row persists in the client DB. */
    cbm_store_t *cs2 = cbm_store_open_path(client.dbpath);
    ASSERT_NOT_NULL(cs2);
    int rows = rh_count_edges(cs2, client.project, "CROSS_HTTP_CALLS");
    fprintf(stderr, "  [523-C] client CROSS_HTTP_CALLS rows=%d (expected 1)\n", rows);
    cbm_store_close(cs2);

    rh_cleanup(&client, NULL);
    rh_cleanup(&server, NULL);
    ASSERT_EQ(rows, 1);
    PASS();
}

/* ── Suite ───────────────────────────────────────────────────────────────── */
SUITE(repro_issue523) {
    RUN_TEST(repro_issue523_crossrepo_http_calls_edge);
    RUN_TEST(repro_issue523_crossrepo_scheme_stripped_url_match);
    RUN_TEST(repro_issue523_crossrepo_template_fuzzy_match);
    RUN_TEST(repro_issue523_crossrepo_reverse_and_dedup);
}
