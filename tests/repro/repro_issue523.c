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

/* ── Cross-repo matcher gaps (distilled from PR #536, same issue #523) ───── */
/*
 * The pass_calls half of #523 (HTTP_CALLS edges for unindexed client libs) is
 * fixed; these cases cover the remaining pass_cross_repo.c matcher gaps:
 *
 *   1. url_path can be a FULL URL ("http://host:8080/v2/orders") — stored raw
 *      from the call's first string arg. cbm_route_canon_path never strips
 *      scheme/authority, so the built route QN embeds the host and the exact
 *      lookup misses.  → cr_url_path()
 *   2. A CONCRETE client path ("/v2/orders/123") never exact-matches a
 *      templated route QN ("__route__GET__/v2/orders/{}").
 *      → find_route_handler_fuzzy() / cr_path_matches_template()
 *   3. match_http_routes only ran src→tgt: matching initiated from the
 *      provider side finds nothing (the provider has no outbound HTTP_CALLS).
 *      → reverse-direction run in cbm_cross_repo_match()
 *   4. Worse, a provider-side run DESTROYED existing links: delete_cross_edges
 *      wiped the provider's reverse edges and — without reverse matching —
 *      nothing recreated them. → the reverse-direction run restores them; row
 *      dedup itself needs no guard (the edges table upserts on the
 *      (source, target, type) UNIQUE key).
 */

/* Index a one-file client and a one-file server project through the production
 * pipeline. Closes both stores again — cbm_cross_repo_match re-opens the DBs
 * from the cache dir. Returns false when either project failed to index or the
 * PRECONDITIONS (client HTTP_CALLS edge, server Route node) are missing, so a
 * broken fixture fails RED instead of vacuously passing. */
static bool cr536_setup(RProj *client, const char *client_py, RProj *server,
                        const char *server_py) {
    memset(client, 0, sizeof(*client));
    memset(server, 0, sizeof(*server));
    cbm_store_t *cs = rh_index(client, "client/orders.py", client_py);
    if (!cs) {
        return false;
    }
    int client_http = rh_count_edges(cs, client->project, "HTTP_CALLS");
    cbm_store_close(cs);

    cbm_store_t *ss = rh_index(server, "server/app.py", server_py);
    if (!ss) {
        return false;
    }
    int server_routes = rh_count_label(ss, server->project, "Route");
    cbm_store_close(ss);

    fprintf(stderr, "  [523] precondition: client HTTP_CALLS=%d server Routes=%d (both >=1)\n",
            client_http, server_routes);
    return client_http >= 1 && server_routes >= 1;
}

/* Count CROSS_HTTP_CALLS rows in a project DB (re-opened from the cache dir,
 * because cbm_cross_repo_match wrote through its own store handles). */
static int cr536_count_cross(RProj *proj) {
    cbm_store_t *st = cbm_store_open_path(proj->dbpath);
    if (!st) {
        return -1;
    }
    int n = rh_count_edges(st, proj->project, "CROSS_HTTP_CALLS");
    cbm_store_close(st);
    return n;
}

/*
 * TEST: url_path stored as a full URL must match a bare-path route.
 * WHY RED on unfixed code: the route QN is built from the raw url_path, so the
 * lookup key is "__route__GET__http://order-api.internal:8080/v2/orders" and
 * never matches the server's "__route__GET__/v2/orders".
 */
TEST(repro_issue523_scheme_stripped_url_match) {
    RProj client, server;
    bool ok = cr536_setup(&client,
                          "import requests\n"
                          "\n"
                          "\n"
                          "def fetch_orders():\n"
                          "    \"\"\"Fetch all orders via the service base URL.\"\"\"\n"
                          "    return requests.get(\"http://order-api.internal:8080/v2/orders\")\n",
                          &server,
                          "from flask import Flask, jsonify\n"
                          "\n"
                          "app = Flask(__name__)\n"
                          "\n"
                          "\n"
                          "@app.get(\"/v2/orders\")\n"
                          "def list_orders():\n"
                          "    \"\"\"Return all orders.\"\"\"\n"
                          "    return jsonify({\"orders\": []})\n");
    if (!ok) {
        rh_cleanup(&client, NULL);
        rh_cleanup(&server, NULL);
        FAIL("fixture precondition failed (client HTTP_CALLS / server Route missing)");
    }

    const char *tgt = server.project;
    cbm_cross_repo_result_t r = cbm_cross_repo_match(client.project, &tgt, 1);
    fprintf(stderr, "  [523] full-URL url_path: http_edges=%d (expected>=1)\n", r.http_edges);

    rh_cleanup(&client, NULL);
    rh_cleanup(&server, NULL);
    ASSERT_GTE(r.http_edges, 1);
    PASS();
}

/*
 * TEST: a concrete client path must fuzzy-match a templated server route.
 * WHY RED on unfixed code: "__route__GET__/v2/orders/123" exact-lookup misses
 * "__route__GET__/v2/orders/{}" and there is no template fallback.
 */
TEST(repro_issue523_template_fuzzy_match) {
    RProj client, server;
    bool ok = cr536_setup(&client,
                          "import requests\n"
                          "\n"
                          "\n"
                          "def fetch_order():\n"
                          "    \"\"\"Fetch one concrete order.\"\"\"\n"
                          "    return requests.get(\"/v2/orders/123\")\n",
                          &server,
                          "from flask import Flask, jsonify\n"
                          "\n"
                          "app = Flask(__name__)\n"
                          "\n"
                          "\n"
                          "@app.get(\"/v2/orders/{order_id}\")\n"
                          "def get_order(order_id):\n"
                          "    \"\"\"Return one order by id.\"\"\"\n"
                          "    return jsonify({\"id\": order_id})\n");
    if (!ok) {
        rh_cleanup(&client, NULL);
        rh_cleanup(&server, NULL);
        FAIL("fixture precondition failed (client HTTP_CALLS / server Route missing)");
    }

    const char *tgt = server.project;
    cbm_cross_repo_result_t r = cbm_cross_repo_match(client.project, &tgt, 1);
    fprintf(stderr, "  [523] concrete-vs-template: http_edges=%d (expected>=1)\n", r.http_edges);

    rh_cleanup(&client, NULL);
    rh_cleanup(&server, NULL);
    ASSERT_GTE(r.http_edges, 1);
    PASS();
}

/*
 * TEST: matching initiated from the PROVIDER side must still find the link.
 * WHY RED on unfixed code: match_http_routes only scans the source project's
 * HTTP_CALLS — the server has none, so the provider-side run reports 0.
 */
TEST(repro_issue523_reverse_direction_match) {
    RProj client, server;
    bool ok = cr536_setup(&client,
                          "import requests\n"
                          "\n"
                          "\n"
                          "def fetch_status():\n"
                          "    \"\"\"Poll the status endpoint.\"\"\"\n"
                          "    return requests.get(\"/v2/status\")\n",
                          &server,
                          "from flask import Flask, jsonify\n"
                          "\n"
                          "app = Flask(__name__)\n"
                          "\n"
                          "\n"
                          "@app.get(\"/v2/status\")\n"
                          "def get_status():\n"
                          "    \"\"\"Return service status.\"\"\"\n"
                          "    return jsonify({\"status\": \"ok\"})\n");
    if (!ok) {
        rh_cleanup(&client, NULL);
        rh_cleanup(&server, NULL);
        FAIL("fixture precondition failed (client HTTP_CALLS / server Route missing)");
    }

    /* Provider indexed the match run: server is SRC, client is TGT. */
    const char *tgt = client.project;
    cbm_cross_repo_result_t r = cbm_cross_repo_match(server.project, &tgt, 1);
    int client_cross = cr536_count_cross(&client);
    fprintf(stderr, "  [523] provider-side run: http_edges=%d client CROSS_HTTP_CALLS=%d\n",
            r.http_edges, client_cross);

    rh_cleanup(&client, NULL);
    rh_cleanup(&server, NULL);
    ASSERT_GTE(r.http_edges, 1);
    ASSERT_GTE(client_cross, 1);
    PASS();
}

/*
 * TEST: the matcher must CONVERGE — running it from either side, repeatedly,
 * leaves exactly one CROSS_HTTP_CALLS row in each DB (no losses, no dupes).
 * WHY RED on unfixed code: run 2 (provider side) starts with
 * delete_cross_edges(server), wiping the reverse edge run 1 wrote into the
 * server DB — and, lacking reverse-direction matching, recreates nothing: a
 * provider-side run permanently DESTROYS the recorded link (server count 0).
 * Duplicate rows are impossible either way (the edges table upserts on the
 * (source, target, type) UNIQUE key); the run-3 assertions pin that too.
 */
TEST(repro_issue523_idempotent_cross_edges) {
    RProj client, server;
    bool ok = cr536_setup(&client,
                          "import requests\n"
                          "\n"
                          "\n"
                          "def fetch_health():\n"
                          "    \"\"\"Poll the health endpoint.\"\"\"\n"
                          "    return requests.get(\"/v2/health\")\n",
                          &server,
                          "from flask import Flask, jsonify\n"
                          "\n"
                          "app = Flask(__name__)\n"
                          "\n"
                          "\n"
                          "@app.get(\"/v2/health\")\n"
                          "def get_health():\n"
                          "    \"\"\"Return health.\"\"\"\n"
                          "    return jsonify({\"health\": \"ok\"})\n");
    if (!ok) {
        rh_cleanup(&client, NULL);
        rh_cleanup(&server, NULL);
        FAIL("fixture precondition failed (client HTTP_CALLS / server Route missing)");
    }

    /* Run 1: consumer side — creates fwd row (client DB) + rev row (server DB). */
    const char *tgt_srv = server.project;
    cbm_cross_repo_result_t r1 = cbm_cross_repo_match(client.project, &tgt_srv, 1);
    int client_1 = cr536_count_cross(&client);
    int server_1 = cr536_count_cross(&server);

    /* Run 2: provider side — must re-find the same pair, not destroy it. */
    const char *tgt_cli = client.project;
    cbm_cross_repo_result_t r2 = cbm_cross_repo_match(server.project, &tgt_cli, 1);
    int client_2 = cr536_count_cross(&client);
    int server_2 = cr536_count_cross(&server);

    /* Run 3: consumer side again — convergence, still exactly one row each. */
    cbm_cross_repo_result_t r3 = cbm_cross_repo_match(client.project, &tgt_srv, 1);
    int client_3 = cr536_count_cross(&client);
    int server_3 = cr536_count_cross(&server);

    fprintf(stderr,
            "  [523] convergence: run1 http=%d (client=%d server=%d) "
            "run2 http=%d (client=%d server=%d) run3 http=%d (client=%d server=%d) "
            "— every count must be 1\n",
            r1.http_edges, client_1, server_1, r2.http_edges, client_2, server_2, r3.http_edges,
            client_3, server_3);

    rh_cleanup(&client, NULL);
    rh_cleanup(&server, NULL);

    ASSERT_EQ(client_1, 1);
    ASSERT_EQ(server_1, 1);
    /* The regression: on unfixed code the provider-side run leaves server_2==0. */
    ASSERT_EQ(client_2, 1);
    ASSERT_EQ(server_2, 1);
    ASSERT_EQ(client_3, 1);
    ASSERT_EQ(server_3, 1);
    PASS();
}

/* ── Suite ───────────────────────────────────────────────────────────────── */
SUITE(repro_issue523) {
    RUN_TEST(repro_issue523_crossrepo_http_calls_edge);
    RUN_TEST(repro_issue523_scheme_stripped_url_match);
    RUN_TEST(repro_issue523_template_fuzzy_match);
    RUN_TEST(repro_issue523_reverse_direction_match);
    RUN_TEST(repro_issue523_idempotent_cross_edges);
}
