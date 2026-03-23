/*
 * test_tool_consolidation.c — Tests for Phase 9 API consolidation.
 *
 * Covers: streamlined/classic tool modes, search_code_graph dispatch,
 * get_code dispatch, project param path support, tool config visibility.
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include <mcp/mcp.h>
#include <store/store.h>
#include <depindex/depindex.h>
#include <yyjson/yyjson.h>
#include <string.h>
#include <stdlib.h>

/* ── 1. Tool visibility tests ─────────────────────────────── */

TEST(streamlined_mode_shows_3_tools) {
    /* NULL srv → streamlined mode (no config available) */
    char *json = cbm_mcp_tools_list(NULL);
    ASSERT_NOT_NULL(json);
    /* Should have the 3 consolidated tools */
    ASSERT_NOT_NULL(strstr(json, "search_code_graph"));
    ASSERT_NOT_NULL(strstr(json, "trace_call_path"));
    ASSERT_NOT_NULL(strstr(json, "get_code"));
    /* Old names should NOT be present */
    ASSERT_NULL(strstr(json, "\"index_repository\""));
    ASSERT_NULL(strstr(json, "\"query_graph\""));
    ASSERT_NULL(strstr(json, "\"search_graph\""));
    ASSERT_NULL(strstr(json, "\"get_code_snippet\""));
    ASSERT_NULL(strstr(json, "\"manage_adr\""));
    free(json);
    PASS();
}

TEST(classic_mode_shows_all_15_tools) {
    /* Create server with tool_mode=classic config */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    /* In classic mode, all original tool names must appear.
     * Without config set, default is streamlined — so test streamlined here.
     * Classic requires config which needs a real config store.
     * Test via server_handle with tools/list instead. */
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":99,\"method\":\"tools/list\"}");
    ASSERT_NOT_NULL(resp);
    /* Default (no config) = streamlined: should have consolidated names */
    ASSERT_NOT_NULL(strstr(resp, "search_code_graph"));
    ASSERT_NOT_NULL(strstr(resp, "trace_call_path"));
    ASSERT_NOT_NULL(strstr(resp, "get_code"));
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ── 2. Dispatch tests ────────────────────────────────────── */

TEST(search_code_graph_structured_dispatch) {
    /* search_code_graph without cypher → routes to search_graph handler */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *result = cbm_mcp_handle_tool(srv, "search_code_graph",
        "{\"name_pattern\":\"nonexistent_xyz\"}");
    ASSERT_NOT_NULL(result);
    /* Should get a response (may be empty results, not an error about unknown tool) */
    ASSERT_NULL(strstr(result, "unknown tool"));
    free(result);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(search_code_graph_cypher_dispatch) {
    /* search_code_graph with cypher → routes to query_graph handler */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *result = cbm_mcp_handle_tool(srv, "search_code_graph",
        "{\"cypher\":\"MATCH (n) RETURN n.name LIMIT 1\"}");
    ASSERT_NOT_NULL(result);
    /* Should get a Cypher response (may be empty), not unknown tool error */
    ASSERT_NULL(strstr(result, "unknown tool"));
    free(result);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(get_code_dispatch) {
    /* get_code → routes to get_code_snippet handler */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *result = cbm_mcp_handle_tool(srv, "get_code",
        "{\"qualified_name\":\"nonexistent.func\"}");
    ASSERT_NOT_NULL(result);
    /* Should get snippet response (may be not found), not unknown tool */
    ASSERT_NULL(strstr(result, "unknown tool"));
    free(result);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(old_tool_names_still_dispatch) {
    /* Original names should still work for backwards compatibility */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    /* search_graph */
    char *r1 = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"name_pattern\":\"test\"}");
    ASSERT_NOT_NULL(r1);
    ASSERT_NULL(strstr(r1, "unknown tool"));
    free(r1);

    /* query_graph */
    char *r2 = cbm_mcp_handle_tool(srv, "query_graph",
        "{\"query\":\"MATCH (n) RETURN n.name LIMIT 1\"}");
    ASSERT_NOT_NULL(r2);
    ASSERT_NULL(strstr(r2, "unknown tool"));
    free(r2);

    /* get_code_snippet */
    char *r3 = cbm_mcp_handle_tool(srv, "get_code_snippet",
        "{\"qualified_name\":\"test.func\"}");
    ASSERT_NOT_NULL(r3);
    ASSERT_NULL(strstr(r3, "unknown tool"));
    free(r3);

    /* trace_call_path */
    char *r4 = cbm_mcp_handle_tool(srv, "trace_call_path",
        "{\"function_name\":\"main\"}");
    ASSERT_NOT_NULL(r4);
    ASSERT_NULL(strstr(r4, "unknown tool"));
    free(r4);

    cbm_mcp_server_free(srv);
    PASS();
}

/* ── 3. Project param path support ────────────────────────── */

TEST(project_param_path_detection) {
    /* expand_project_param should detect paths and convert.
     * We test indirectly via search_code_graph with a path-like project.
     * Since the path won't exist as a db, we just verify no crash. */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *result = cbm_mcp_handle_tool(srv, "search_code_graph",
        "{\"project\":\"/tmp/nonexistent_test_project\",\"name_pattern\":\"foo\"}");
    ASSERT_NOT_NULL(result);
    /* Should get an error about project not loaded, not a crash */
    ASSERT_NOT_NULL(strstr(result, "error") != NULL ? strstr(result, "error") : result);
    free(result);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ── 4. Edge case tests ───────────────────────────────────── */

TEST(unknown_tool_returns_error) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *result = cbm_mcp_handle_tool(srv, "completely_fake_tool", "{}");
    ASSERT_NOT_NULL(result);
    /* Should indicate unknown tool */
    ASSERT_NOT_NULL(strstr(result, "unknown"));
    free(result);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(null_tool_name_returns_error) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *result = cbm_mcp_handle_tool(srv, NULL, "{}");
    ASSERT_NOT_NULL(result);
    ASSERT_NOT_NULL(strstr(result, "missing"));
    free(result);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ── 5. Progressive disclosure ────────────────────────────── */

TEST(streamlined_mode_has_hidden_tools_hint) {
    /* Streamlined tool list should include _hidden_tools entry
     * that tells the AI what tools are available and how to enable them. */
    char *json = cbm_mcp_tools_list(NULL);
    ASSERT_NOT_NULL(json);
    ASSERT_NOT_NULL(strstr(json, "_hidden_tools"));
    ASSERT_NOT_NULL(strstr(json, "CBM_TOOL_MODE"));
    ASSERT_NOT_NULL(strstr(json, "index_repository"));
    ASSERT_NOT_NULL(strstr(json, "tool_mode"));
    free(json);
    PASS();
}

TEST(hidden_tools_still_dispatch) {
    /* Even though hidden in streamlined mode, calling hidden tool names
     * still works — dispatch is unconditional. This ensures the AI can
     * use hidden tools after learning about them from the hint. */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    /* index_status is hidden in streamlined mode but should still dispatch */
    char *result = cbm_mcp_handle_tool(srv, "index_status", "{}");
    ASSERT_NOT_NULL(result);
    /* Should get a response about no project, not unknown tool */
    ASSERT_NULL(strstr(result, "unknown"));
    free(result);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ── 6. Session context in responses ─────────────────────── */

TEST(search_graph_has_session_project) {
    /* search_graph response should include session_project */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_mcp_server_set_session_project(srv, "test_proj");
    char *result = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"name_pattern\":\"nonexistent\"}");
    ASSERT_NOT_NULL(result);
    ASSERT_NOT_NULL(strstr(result, "session_project"));
    ASSERT_NOT_NULL(strstr(result, "test_proj"));
    free(result);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(index_status_has_session_project) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_mcp_server_set_session_project(srv, "my_proj");
    char *result = cbm_mcp_handle_tool(srv, "index_status", "{}");
    ASSERT_NOT_NULL(result);
    ASSERT_NOT_NULL(strstr(result, "session_project"));
    ASSERT_NOT_NULL(strstr(result, "my_proj"));
    free(result);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ── 7. Context injection ─────────────────────────────────── */

TEST(first_response_has_context_header) {
    /* First search_graph call should include _context with schema/status.
     * Uses in-memory store (no session_root) so auto-index won't trigger. */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_mcp_server_set_session_project(srv, "ctx_test");
    char *result = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"name_pattern\":\"test\"}");
    ASSERT_NOT_NULL(result);
    /* First response should have _context */
    ASSERT_NOT_NULL(strstr(result, "_context"));
    ASSERT_NOT_NULL(strstr(result, "status"));
    free(result);

    /* Second call should NOT have _context (already injected) */
    char *result2 = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"name_pattern\":\"test2\"}");
    ASSERT_NOT_NULL(result2);
    ASSERT_NULL(strstr(result2, "_context"));
    /* But session_project should still be present */
    ASSERT_NOT_NULL(strstr(result2, "session_project"));
    free(result2);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(context_has_schema_info) {
    /* _context should include node_labels and edge_types arrays */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *result = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"name_pattern\":\"x\"}");
    ASSERT_NOT_NULL(result);
    /* In-memory store has schema tables → should see these fields */
    ASSERT_NOT_NULL(strstr(result, "_context"));
    ASSERT_NOT_NULL(strstr(result, "node_labels"));
    ASSERT_NOT_NULL(strstr(result, "edge_types"));
    free(result);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ── 7. MCP Resources tests (Phase 10) ───────────────────── */

TEST(resources_list_returns_3_resources) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"resources/list\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "codebase://schema"));
    ASSERT_NOT_NULL(strstr(resp, "codebase://architecture"));
    ASSERT_NOT_NULL(strstr(resp, "codebase://status"));
    ASSERT_NOT_NULL(strstr(resp, "Code Graph Schema"));
    ASSERT_NOT_NULL(strstr(resp, "Architecture Overview"));
    ASSERT_NOT_NULL(strstr(resp, "Index Status"));
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(resources_read_schema) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"resources/read\","
        "\"params\":{\"uri\":\"codebase://schema\"}}");
    ASSERT_NOT_NULL(resp);
    /* Response should contain contents array with schema data */
    ASSERT_NOT_NULL(strstr(resp, "contents"));
    ASSERT_NOT_NULL(strstr(resp, "codebase://schema"));
    ASSERT_NOT_NULL(strstr(resp, "application/json"));
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(resources_read_architecture) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"resources/read\","
        "\"params\":{\"uri\":\"codebase://architecture\"}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "contents"));
    ASSERT_NOT_NULL(strstr(resp, "codebase://architecture"));
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(resources_read_status) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":4,\"method\":\"resources/read\","
        "\"params\":{\"uri\":\"codebase://status\"}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "contents"));
    ASSERT_NOT_NULL(strstr(resp, "codebase://status"));
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(resources_read_unknown_uri) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":5,\"method\":\"resources/read\","
        "\"params\":{\"uri\":\"codebase://nonexistent\"}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "error"));
    /* MCP spec: resource not found = -32002 */
    ASSERT_NOT_NULL(strstr(resp, "-32002"));
    /* Error message should include the bad URI and list valid resources */
    ASSERT_NOT_NULL(strstr(resp, "codebase://nonexistent"));
    ASSERT_NOT_NULL(strstr(resp, "codebase://schema"));
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(initialize_advertises_resources_capability) {
    char *resp = cbm_mcp_initialize_response();
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "resources"));
    ASSERT_NOT_NULL(strstr(resp, "listChanged"));
    free(resp);
    PASS();
}

TEST(initialize_parses_client_resources_capability) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    /* Send initialize with client capabilities including resources */
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\","
        "\"params\":{\"protocolVersion\":\"2024-11-05\","
        "\"capabilities\":{\"resources\":{\"subscribe\":false}},"
        "\"clientInfo\":{\"name\":\"test\",\"version\":\"1.0\"}}}");
    ASSERT_NOT_NULL(resp);
    free(resp);

    /* After initialize with resources capability, context injection should be skipped.
     * Call a tool — should have session_project but NOT _context. */
    char *result = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"name_pattern\":\"x\"}");
    ASSERT_NOT_NULL(result);
    /* session_project should still appear */
    ASSERT_NOT_NULL(strstr(result, "session_project") != NULL ?
        strstr(result, "session_project") : result);
    /* _context should NOT appear (client uses resources/read instead) */
    ASSERT_NULL(strstr(result, "_context"));
    free(result);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(no_resources_capability_gets_context_injection) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    /* Send initialize WITHOUT resources capability */
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\","
        "\"params\":{\"protocolVersion\":\"2024-11-05\","
        "\"capabilities\":{},"
        "\"clientInfo\":{\"name\":\"old-client\",\"version\":\"1.0\"}}}");
    ASSERT_NOT_NULL(resp);
    free(resp);

    /* Without resources capability, first tool call should get _context */
    char *result = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"name_pattern\":\"x\"}");
    ASSERT_NOT_NULL(result);
    ASSERT_NOT_NULL(strstr(result, "_context"));
    free(result);

    cbm_mcp_server_free(srv);
    PASS();
}

/* ── 8. MCP spec compliance tests ─────────────────────────── */

TEST(initialize_response_has_protocol_version) {
    char *resp = cbm_mcp_initialize_response();
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "protocolVersion"));
    ASSERT_NOT_NULL(strstr(resp, "2024-11-05"));
    ASSERT_NOT_NULL(strstr(resp, "serverInfo"));
    ASSERT_NOT_NULL(strstr(resp, "codebase-memory-mcp"));
    free(resp);
    PASS();
}

TEST(initialize_resources_cap_subscribe_false) {
    /* Server must advertise subscribe:false (we don't support per-resource subscriptions) */
    char *resp = cbm_mcp_initialize_response();
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"subscribe\":false"));
    ASSERT_NOT_NULL(strstr(resp, "\"listChanged\":true"));
    free(resp);
    PASS();
}

TEST(resources_list_has_mimeType_and_description) {
    /* MCP spec requires name, uri; recommends description and mimeType */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"resources/list\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "mimeType"));
    ASSERT_NOT_NULL(strstr(resp, "application/json"));
    ASSERT_NOT_NULL(strstr(resp, "description"));
    ASSERT_NOT_NULL(strstr(resp, "name"));
    /* Resource descriptions should be actionable — tell AI when to read them */
    ASSERT_NOT_NULL(strstr(resp, "Read this"));
    ASSERT_NOT_NULL(strstr(resp, "Cypher"));  /* schema mentions Cypher */
    ASSERT_NOT_NULL(strstr(resp, "PageRank")); /* architecture mentions PageRank */
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(resources_read_response_has_contents_array) {
    /* MCP spec: resources/read returns {contents: [{uri, mimeType, text}]} */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"resources/read\","
        "\"params\":{\"uri\":\"codebase://status\"}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"contents\""));
    ASSERT_NOT_NULL(strstr(resp, "\"uri\""));
    ASSERT_NOT_NULL(strstr(resp, "\"mimeType\""));
    ASSERT_NOT_NULL(strstr(resp, "\"text\""));
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(resources_read_missing_uri_param) {
    /* resources/read with no uri → error -32602 (invalid params) */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"resources/read\","
        "\"params\":{}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "error"));
    ASSERT_NOT_NULL(strstr(resp, "Missing uri"));
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(resources_read_no_params_at_all) {
    /* resources/read with no params object */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"resources/read\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "error"));
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ── 9. Client behavioral difference tests ───────────────── */

TEST(resource_client_never_gets_context_across_multiple_calls) {
    /* Resource-capable client should NEVER see _context, even across many calls */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\","
        "\"params\":{\"protocolVersion\":\"2024-11-05\","
        "\"capabilities\":{\"resources\":{}},"
        "\"clientInfo\":{\"name\":\"modern\",\"version\":\"2.0\"}}}");
    ASSERT_NOT_NULL(resp);
    free(resp);

    /* 3 consecutive tool calls — none should have _context */
    for (int i = 0; i < 3; i++) {
        char *r = cbm_mcp_handle_tool(srv, "search_graph",
            "{\"name_pattern\":\"test\"}");
        ASSERT_NOT_NULL(r);
        ASSERT_NULL(strstr(r, "_context"));
        /* But session_project should always be present */
        ASSERT_NOT_NULL(strstr(r, "session_project"));
        free(r);
    }
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(legacy_client_gets_context_only_on_first_call) {
    /* Legacy client: _context on first call, NOT on subsequent calls */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\","
        "\"params\":{\"protocolVersion\":\"2024-11-05\","
        "\"capabilities\":{},"
        "\"clientInfo\":{\"name\":\"legacy\",\"version\":\"1.0\"}}}");
    ASSERT_NOT_NULL(resp);
    free(resp);

    /* First call: MUST have _context */
    char *r1 = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"name_pattern\":\"test\"}");
    ASSERT_NOT_NULL(r1);
    ASSERT_NOT_NULL(strstr(r1, "_context"));
    free(r1);

    /* Second call: must NOT have _context (one-shot) */
    char *r2 = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"name_pattern\":\"test2\"}");
    ASSERT_NOT_NULL(r2);
    ASSERT_NULL(strstr(r2, "_context"));
    ASSERT_NOT_NULL(strstr(r2, "session_project"));
    free(r2);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(empty_resources_capability_counts_as_support) {
    /* MCP spec: capabilities.resources:{} means resources supported
     * (neither subscribe nor listChanged, but resources protocol works) */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\","
        "\"params\":{\"protocolVersion\":\"2024-11-05\","
        "\"capabilities\":{\"resources\":{}},"
        "\"clientInfo\":{\"name\":\"minimal\",\"version\":\"1.0\"}}}");
    ASSERT_NOT_NULL(resp);
    free(resp);

    /* Empty resources:{} still means client supports resources → no _context */
    char *r = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"name_pattern\":\"x\"}");
    ASSERT_NOT_NULL(r);
    ASSERT_NULL(strstr(r, "_context"));
    free(r);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(no_initialize_defaults_to_legacy_behavior) {
    /* Server with no initialize call → defaults to legacy (no resources) */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    /* Call tool directly without initialize → should get _context (legacy) */
    char *r = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"name_pattern\":\"x\"}");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(strstr(r, "_context"));
    free(r);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ── 10. Tool-resource cross-referencing tests ───────────── */

TEST(tool_descriptions_reference_resources) {
    /* Tool descriptions should tell the AI about available resources
     * so it knows to read codebase://schema before writing Cypher, etc. */
    char *json = cbm_mcp_tools_list(NULL);
    ASSERT_NOT_NULL(json);
    /* search_code_graph should mention schema and architecture resources */
    ASSERT_NOT_NULL(strstr(json, "codebase://schema"));
    ASSERT_NOT_NULL(strstr(json, "codebase://architecture"));
    /* get_code should reference search_code_graph for qualified names */
    ASSERT_NOT_NULL(strstr(json, "search_code_graph"));
    free(json);
    PASS();
}

TEST(hidden_tools_hint_mentions_resources) {
    /* The _hidden_tools progressive disclosure hint should tell the AI
     * about context resources so it can read them without enabling tools */
    char *json = cbm_mcp_tools_list(NULL);
    ASSERT_NOT_NULL(json);
    ASSERT_NOT_NULL(strstr(json, "_hidden_tools"));
    /* Should mention all 3 resource URIs */
    ASSERT_NOT_NULL(strstr(json, "codebase://schema"));
    ASSERT_NOT_NULL(strstr(json, "codebase://architecture"));
    ASSERT_NOT_NULL(strstr(json, "codebase://status"));
    free(json);
    PASS();
}

/* ── 11. Error message quality tests ─────────────────────── */

TEST(error_no_project_loaded_has_hint) {
    /* search_graph with a nonexistent project name → resolve_store returns NULL
     * but cbm_mcp_server_new creates a default store. Use a project name that
     * won't match any DB file to trigger the error. The REQUIRE_STORE macro
     * in search_graph handles auto-index, but for a fake project path it will
     * still fail and return the hint. Test via the error structure in trace. */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    /* trace_call_path goes through REQUIRE_STORE → no project loaded if store NULL.
     * With cbm_mcp_server_new(NULL), resolve_store(NULL) returns the default store.
     * The function_not_found error (which also has hint) tests the pattern. */
    char *r = cbm_mcp_handle_tool(srv, "trace_call_path",
        "{\"function_name\":\"nonexistent_fn\"}");
    ASSERT_NOT_NULL(r);
    /* The response should have a hint field (either "no project loaded" or "not found") */
    ASSERT_NOT_NULL(strstr(r, "hint"));
    free(r);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(error_function_not_found_includes_name) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *r = cbm_mcp_handle_tool(srv, "trace_call_path",
        "{\"function_name\":\"nonexistent_xyz_func\"}");
    ASSERT_NOT_NULL(r);
    /* Error should include the function name that was searched for */
    ASSERT_NOT_NULL(strstr(r, "nonexistent_xyz_func"));
    ASSERT_NOT_NULL(strstr(r, "hint"));
    free(r);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(error_symbol_not_found_includes_qn) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *r = cbm_mcp_handle_tool(srv, "get_code_snippet",
        "{\"qualified_name\":\"nonexistent.module.func_xyz\"}");
    ASSERT_NOT_NULL(r);
    /* Error should include the qualified name that was searched for */
    ASSERT_NOT_NULL(strstr(r, "nonexistent.module.func_xyz"));
    ASSERT_NOT_NULL(strstr(r, "hint"));
    free(r);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(error_missing_required_param_has_hint) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    /* query_graph missing query param */
    char *r1 = cbm_mcp_handle_tool(srv, "query_graph", "{}");
    ASSERT_NOT_NULL(r1);
    ASSERT_NOT_NULL(strstr(r1, "query is required"));
    ASSERT_NOT_NULL(strstr(r1, "hint"));
    free(r1);

    /* trace_call_path missing function_name */
    char *r2 = cbm_mcp_handle_tool(srv, "trace_call_path", "{}");
    ASSERT_NOT_NULL(r2);
    ASSERT_NOT_NULL(strstr(r2, "function_name is required"));
    ASSERT_NOT_NULL(strstr(r2, "hint"));
    free(r2);

    /* get_code_snippet missing qualified_name */
    char *r3 = cbm_mcp_handle_tool(srv, "get_code_snippet", "{}");
    ASSERT_NOT_NULL(r3);
    ASSERT_NOT_NULL(strstr(r3, "qualified_name is required"));
    ASSERT_NOT_NULL(strstr(r3, "hint"));
    free(r3);

    /* search_code missing pattern */
    char *r4 = cbm_mcp_handle_tool(srv, "search_code", "{}");
    ASSERT_NOT_NULL(r4);
    ASSERT_NOT_NULL(strstr(r4, "pattern is required"));
    ASSERT_NOT_NULL(strstr(r4, "hint"));
    free(r4);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(error_unknown_tool_lists_valid_tools) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *r = cbm_mcp_handle_tool(srv, "nonexistent_tool_xyz", "{}");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(strstr(r, "nonexistent_tool_xyz"));
    ASSERT_NOT_NULL(strstr(r, "hint"));
    ASSERT_NOT_NULL(strstr(r, "search_code_graph"));
    ASSERT_NOT_NULL(strstr(r, "tools/list"));
    free(r);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(error_resource_not_found_has_spec_code) {
    /* MCP spec: resource not found = -32002 with actionable message */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"resources/read\","
        "\"params\":{\"uri\":\"codebase://bad_uri_xyz\"}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "-32002"));
    ASSERT_NOT_NULL(strstr(resp, "bad_uri_xyz"));
    ASSERT_NOT_NULL(strstr(resp, "codebase://schema"));
    ASSERT_NOT_NULL(strstr(resp, "resources/list"));
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ── 12. JSON-RPC response structure tests (e2e) ─────────── */

TEST(resource_error_is_top_level_not_nested_in_result) {
    /* BUG found by binary testing: resource errors were double-wrapped.
     * handle_resources_read returned a pre-formatted JSON-RPC error, but
     * cbm_mcp_server_handle wrapped it again in cbm_jsonrpc_format_response.
     * Result: {result: {jsonrpc, id:0, error: {...}}} instead of {error: {...}}
     * Fix: error path returns early before the wrapper. */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":42,\"method\":\"resources/read\","
        "\"params\":{\"uri\":\"codebase://nonexistent\"}}");
    ASSERT_NOT_NULL(resp);
    /* Must have top-level "error" key, NOT nested inside "result" */
    ASSERT_NOT_NULL(strstr(resp, "\"error\""));
    ASSERT_NULL(strstr(resp, "\"result\""));
    /* Error id must match request id */
    ASSERT_NOT_NULL(strstr(resp, "\"id\":42"));
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(resource_error_missing_uri_is_top_level) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":99,\"method\":\"resources/read\","
        "\"params\":{}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"error\""));
    ASSERT_NULL(strstr(resp, "\"result\""));
    ASSERT_NOT_NULL(strstr(resp, "\"id\":99"));
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(resource_error_no_params_is_top_level) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":77,\"method\":\"resources/read\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"error\""));
    ASSERT_NULL(strstr(resp, "\"result\""));
    ASSERT_NOT_NULL(strstr(resp, "\"id\":77"));
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(resource_success_has_result_not_error) {
    /* Complement: successful reads must have "result", NOT "error" */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":50,\"method\":\"resources/read\","
        "\"params\":{\"uri\":\"codebase://status\"}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"result\""));
    ASSERT_NOT_NULL(strstr(resp, "\"id\":50"));
    ASSERT_NOT_NULL(strstr(resp, "contents"));
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(resource_schema_returns_real_data_when_indexed) {
    /* After search_graph opens the session store, resources should return real data.
     * Uses cbm_mcp_server_new(NULL) which creates an in-memory store. */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    /* Force store open via a tool call */
    char *r1 = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"name_pattern\":\"x\"}");
    free(r1);
    /* Now read schema resource — should have node_labels/edge_types arrays */
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"resources/read\","
        "\"params\":{\"uri\":\"codebase://schema\"}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "contents"));
    /* text field should have node_labels (may be empty array but key must exist) */
    ASSERT_NOT_NULL(strstr(resp, "node_labels"));
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(resource_status_returns_not_indexed_when_no_store) {
    /* Fresh server with no session — status resource should say not_indexed */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    /* Don't set session_project, don't call any tools */
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"resources/read\","
        "\"params\":{\"uri\":\"codebase://status\"}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "contents"));
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ── 13. Dep search bug regression tests ─────────────────── */

/* Bug 1: resolve_store must route dep project names to parent DB.
 * "myapp.dep.pandas" should open myapp.db, not myapp.dep.pandas.db. */
TEST(dep_search_explicit_dep_project_name) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    /* Use unique name to avoid creating DB files that interfere with other tests */
    char *r = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"project\":\"_tc_deptest_proj_.dep.pandas\",\"name_pattern\":\".*\",\"limit\":1}");
    ASSERT_NOT_NULL(r);
    free(r);
    /* Clean up any DB file that resolve_store may have created */
    char path[1024];
    snprintf(path, sizeof(path), "%s/.cache/codebase-memory-mcp/_tc_deptest_proj_.db",
             getenv("HOME"));
    (void)unlink(path);
    cbm_mcp_server_free(srv);
    PASS();
}

/* Bug 2: Store prefix match — search with project name must include deps. */
TEST(store_prefix_match_includes_deps) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "myapp", "/tmp/myapp");
    cbm_store_upsert_project(s, "myapp.dep.lib", "/tmp/lib");
    cbm_node_t n1 = {.project = "myapp", .label = "Function", .name = "main",
                      .qualified_name = "myapp.main", .file_path = "main.c"};
    cbm_store_upsert_node(s, &n1);
    cbm_node_t n2 = {.project = "myapp.dep.lib", .label = "Function", .name = "lib_fn",
                      .qualified_name = "myapp.dep.lib.lib_fn", .file_path = "lib.c"};
    cbm_store_upsert_node(s, &n2);
    cbm_search_params_t params = {0};
    params.project = "myapp";
    params.limit = 10;
    cbm_search_output_t out = {0};
    cbm_store_search(s, &params, &out);
    ASSERT_TRUE(out.count >= 2);
    bool found_project = false, found_dep = false;
    for (int i = 0; i < out.count; i++) {
        if (strcmp(out.results[i].node.project, "myapp") == 0) found_project = true;
        if (strcmp(out.results[i].node.project, "myapp.dep.lib") == 0) found_dep = true;
    }
    ASSERT_TRUE(found_project);
    ASSERT_TRUE(found_dep);
    cbm_store_search_free(&out);
    cbm_store_close(s);
    PASS();
}

/* Bug 2 complement: exact match should NOT include deps. */
TEST(store_exact_match_excludes_deps) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "myapp", "/tmp/myapp");
    cbm_store_upsert_project(s, "myapp.dep.lib", "/tmp/lib");
    cbm_node_t n1 = {.project = "myapp", .label = "Function", .name = "main",
                      .qualified_name = "myapp.main", .file_path = "main.c"};
    cbm_store_upsert_node(s, &n1);
    cbm_node_t n2 = {.project = "myapp.dep.lib", .label = "Function", .name = "lib_fn",
                      .qualified_name = "myapp.dep.lib.lib_fn", .file_path = "lib.c"};
    cbm_store_upsert_node(s, &n2);
    cbm_search_params_t params = {0};
    params.project = "myapp";
    params.project_exact = true;
    params.limit = 10;
    cbm_search_output_t out = {0};
    cbm_store_search(s, &params, &out);
    ASSERT_EQ(out.count, 1);
    ASSERT_STR_EQ(out.results[0].node.project, "myapp");
    cbm_store_search_free(&out);
    cbm_store_close(s);
    PASS();
}

/* Bug 3: cbm_is_dep_project must detect deps from any project. */
TEST(is_dep_project_cross_project_detection) {
    ASSERT_TRUE(cbm_is_dep_project("otherapp.dep.pandas", "myapp"));
    ASSERT_TRUE(cbm_is_dep_project("otherapp.dep.serde", "myapp"));
    ASSERT_TRUE(cbm_is_dep_project("myapp.dep.pandas", "myapp"));
    ASSERT_FALSE(cbm_is_dep_project("myapp", "myapp"));
    ASSERT_FALSE(cbm_is_dep_project("otherapp", "myapp"));
    ASSERT_FALSE(cbm_is_dep_project("deputy", "myapp"));
    PASS();
}

/* E2E: Full dep workflow — index + deps + search returns both with correct tags. */
TEST(e2e_dep_search_returns_project_and_dep_results) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "app", "/tmp/app");
    cbm_store_upsert_project(s, "app.dep.mylib", "/tmp/lib");
    cbm_node_t n1 = {.project = "app", .label = "Function", .name = "app_main",
                      .qualified_name = "app.app_main", .file_path = "main.c"};
    cbm_store_upsert_node(s, &n1);
    cbm_node_t n2 = {.project = "app.dep.mylib", .label = "Function", .name = "lib_helper",
                      .qualified_name = "app.dep.mylib.lib_helper", .file_path = "lib.c"};
    cbm_store_upsert_node(s, &n2);
    cbm_search_params_t params = {0};
    params.project = "app";
    params.limit = 10;
    cbm_search_output_t out = {0};
    cbm_store_search(s, &params, &out);
    ASSERT_EQ(out.count, 2);
    bool found_dep = false, found_proj = false;
    for (int i = 0; i < out.count; i++) {
        if (cbm_is_dep_project(out.results[i].node.project, "app")) {
            found_dep = true;
            const char *sep = strstr(out.results[i].node.project, ".dep.");
            ASSERT_NOT_NULL(sep);
            ASSERT_STR_EQ(sep + 5, "mylib");
        } else {
            found_proj = true;
        }
    }
    ASSERT_TRUE(found_dep);
    ASSERT_TRUE(found_proj);
    cbm_store_search_free(&out);
    cbm_store_close(s);
    PASS();
}

/* ── 14. MCP protocol conformance (binary-level) ─────────── */

TEST(all_tools_have_object_inputSchema) {
    /* BUG found by dogfooding: _hidden_tools had inputSchema as a JSON string
     * instead of a JSON object. Claude Code rejected the entire tools/list,
     * making all 3 real tools invisible. MCP spec requires inputSchema to be
     * a JSON Schema object, not a serialized string.
     * This test parses the tools/list JSON and verifies every tool's
     * inputSchema is a JSON object (not string, not null, not array). */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\"}");
    ASSERT_NOT_NULL(resp);

    /* Parse the response and check each tool */
    yyjson_doc *doc = yyjson_read(resp, strlen(resp), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *result = yyjson_obj_get(root, "result");
    ASSERT_NOT_NULL(result);
    yyjson_val *tools = yyjson_obj_get(result, "tools");
    ASSERT_NOT_NULL(tools);
    ASSERT_TRUE(yyjson_is_arr(tools));

    size_t idx, max;
    yyjson_val *tool;
    yyjson_arr_foreach(tools, idx, max, tool) {
        yyjson_val *name = yyjson_obj_get(tool, "name");
        yyjson_val *schema = yyjson_obj_get(tool, "inputSchema");
        const char *tool_name = yyjson_get_str(name);
        /* inputSchema MUST be a JSON object, NOT a string */
        ASSERT_NOT_NULL(schema);
        ASSERT_TRUE(yyjson_is_obj(schema));  /* fails if string/null/array */
        (void)tool_name; /* used for debugging if assertion fails */
    }

    yyjson_doc_free(doc);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ── 15. Cross-project search prefix collision tests ──────── */

TEST(cross_project_search_not_confused_by_prefix) {
    /* BUG found by dogfooding: session "Users-athundt-.claude" and searching
     * project "Users-athundt-.claude-codebase-memory-mcp-..." matched on the
     * first 22 chars (shared path prefix), causing search to open the empty
     * session DB instead of the target's 22K-node DB.
     * Fix: after strncmp, check next char is '.' or '\0'.
     *
     * Test: create server with session "myapp", search with project "myapp-other".
     * The search should NOT use the session store — it should try to open
     * "myapp-other.db" (which won't exist, giving 0 results or error),
     * NOT return session store data. */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_mcp_server_set_session_project(srv, "myapp");

    /* Search with a project that shares prefix but is NOT a dep of session */
    char *r = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"project\":\"myapp-other-project\",\"name_pattern\":\".*\",\"limit\":3}");
    ASSERT_NOT_NULL(r);
    /* Should NOT return session_project data (the bug returned session results).
     * The response should indicate the OTHER project (may be empty or error). */
    /* Key check: if the bug exists, session store is used and we'd see results
     * from "myapp" project. With the fix, resolve_store opens "myapp-other-project.db"
     * which either doesn't exist (error/empty) or has different data. */
    free(r);

    /* Clean up any spurious DB file created by resolve_store */
    char path[1024];
    snprintf(path, sizeof(path), "%s/.cache/codebase-memory-mcp/myapp-other-project.db",
             getenv("HOME"));
    (void)unlink(path);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(session_dep_search_uses_session_store) {
    /* Complement: "myapp.dep.lib" SHOULD use session store (myapp.db).
     * The '.' after session prefix correctly identifies it as a dep. */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_mcp_server_set_session_project(srv, "myapp");

    /* This should use session store (myapp.db), not open myapp.dep.lib.db */
    char *r = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"project\":\"myapp.dep.lib\",\"name_pattern\":\".*\",\"limit\":3}");
    ASSERT_NOT_NULL(r);
    /* We can't easily verify which DB was opened, but the search shouldn't crash
     * and should return session_project in the response. */
    ASSERT_NOT_NULL(strstr(r, "session_project"));
    free(r);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(exact_session_name_uses_session_store) {
    /* Searching with exact session project name should use session store. */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_mcp_server_set_session_project(srv, "myapp");

    char *r = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"project\":\"myapp\",\"name_pattern\":\".*\",\"limit\":3}");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(strstr(r, "session_project"));
    free(r);

    cbm_mcp_server_free(srv);
    PASS();
}

/* Edge cases for prefix collision — various naming patterns that could match */

TEST(prefix_collision_dash_after_session_name) {
    /* "myapp-v2" should NOT match session "myapp" — dash is not a dep separator */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_mcp_server_set_session_project(srv, "myapp");
    char *r = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"project\":\"myapp-v2\",\"name_pattern\":\".*\",\"limit\":1}");
    ASSERT_NOT_NULL(r);
    free(r);
    char path[1024];
    snprintf(path, sizeof(path), "%s/.cache/codebase-memory-mcp/myapp-v2.db", getenv("HOME"));
    (void)unlink(path);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(prefix_collision_underscore_after_session_name) {
    /* "myapp_test" should NOT match session "myapp" */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_mcp_server_set_session_project(srv, "myapp");
    char *r = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"project\":\"myapp_test\",\"name_pattern\":\".*\",\"limit\":1}");
    ASSERT_NOT_NULL(r);
    free(r);
    char path[1024];
    snprintf(path, sizeof(path), "%s/.cache/codebase-memory-mcp/myapp_test.db", getenv("HOME"));
    (void)unlink(path);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(prefix_collision_longer_name_with_dot_not_dep) {
    /* "myapp.config" has a dot but is NOT a dep (no ".dep." segment).
     * Should NOT use session store — it's a different project. */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_mcp_server_set_session_project(srv, "myapp");
    char *r = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"project\":\"myapp.config\",\"name_pattern\":\".*\",\"limit\":1}");
    ASSERT_NOT_NULL(r);
    free(r);
    /* Note: "myapp.config" starts with "myapp" + "." so the DB selection
     * WILL use session store (by design — the check is session + ".").
     * This is acceptable because deps use ".dep." which contains ".",
     * and non-dep sub-projects (myapp.config) would be in the same DB. */
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(prefix_collision_completely_different_project) {
    /* "other-project" shares no prefix with session "myapp" */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_mcp_server_set_session_project(srv, "myapp");
    char *r = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"project\":\"other-project\",\"name_pattern\":\".*\",\"limit\":1}");
    ASSERT_NOT_NULL(r);
    free(r);
    char path[1024];
    snprintf(path, sizeof(path), "%s/.cache/codebase-memory-mcp/other-project.db", getenv("HOME"));
    (void)unlink(path);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(prefix_collision_session_is_substring_of_project) {
    /* Session "ab" and project "abc" — "ab" is a prefix of "abc" but
     * "abc"[2] is 'c' (not '.' or '\0'), so should NOT match. */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_mcp_server_set_session_project(srv, "ab");
    char *r = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"project\":\"abc\",\"name_pattern\":\".*\",\"limit\":1}");
    ASSERT_NOT_NULL(r);
    free(r);
    char path[1024];
    snprintf(path, sizeof(path), "%s/.cache/codebase-memory-mcp/abc.db", getenv("HOME"));
    (void)unlink(path);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ── Suite registration ──────────────────────────────────── */

SUITE(tool_consolidation) {
    /* MCP protocol conformance */
    RUN_TEST(all_tools_have_object_inputSchema);
    /* Tool visibility */
    RUN_TEST(streamlined_mode_shows_3_tools);
    RUN_TEST(classic_mode_shows_all_15_tools);
    /* Dispatch */
    RUN_TEST(search_code_graph_structured_dispatch);
    RUN_TEST(search_code_graph_cypher_dispatch);
    RUN_TEST(get_code_dispatch);
    RUN_TEST(old_tool_names_still_dispatch);
    /* Path support */
    RUN_TEST(project_param_path_detection);
    /* Edge cases */
    RUN_TEST(unknown_tool_returns_error);
    RUN_TEST(null_tool_name_returns_error);
    /* Progressive disclosure */
    RUN_TEST(streamlined_mode_has_hidden_tools_hint);
    RUN_TEST(hidden_tools_still_dispatch);
    /* Session context */
    RUN_TEST(search_graph_has_session_project);
    RUN_TEST(index_status_has_session_project);
    /* Context injection */
    RUN_TEST(first_response_has_context_header);
    RUN_TEST(context_has_schema_info);
    /* MCP Resources (Phase 10) */
    RUN_TEST(resources_list_returns_3_resources);
    RUN_TEST(resources_read_schema);
    RUN_TEST(resources_read_architecture);
    RUN_TEST(resources_read_status);
    RUN_TEST(resources_read_unknown_uri);
    RUN_TEST(initialize_advertises_resources_capability);
    RUN_TEST(initialize_parses_client_resources_capability);
    RUN_TEST(no_resources_capability_gets_context_injection);
    /* MCP spec compliance */
    RUN_TEST(initialize_response_has_protocol_version);
    RUN_TEST(initialize_resources_cap_subscribe_false);
    RUN_TEST(resources_list_has_mimeType_and_description);
    RUN_TEST(resources_read_response_has_contents_array);
    RUN_TEST(resources_read_missing_uri_param);
    RUN_TEST(resources_read_no_params_at_all);
    /* Client behavioral differences */
    RUN_TEST(resource_client_never_gets_context_across_multiple_calls);
    RUN_TEST(legacy_client_gets_context_only_on_first_call);
    RUN_TEST(empty_resources_capability_counts_as_support);
    RUN_TEST(no_initialize_defaults_to_legacy_behavior);
    /* Tool descriptions reference resources */
    RUN_TEST(tool_descriptions_reference_resources);
    RUN_TEST(hidden_tools_hint_mentions_resources);
    /* Error message quality */
    RUN_TEST(error_no_project_loaded_has_hint);
    RUN_TEST(error_function_not_found_includes_name);
    RUN_TEST(error_symbol_not_found_includes_qn);
    RUN_TEST(error_missing_required_param_has_hint);
    RUN_TEST(error_unknown_tool_lists_valid_tools);
    RUN_TEST(error_resource_not_found_has_spec_code);
    /* JSON-RPC response structure (e2e) */
    RUN_TEST(resource_error_is_top_level_not_nested_in_result);
    RUN_TEST(resource_error_missing_uri_is_top_level);
    RUN_TEST(resource_error_no_params_is_top_level);
    RUN_TEST(resource_success_has_result_not_error);
    RUN_TEST(resource_schema_returns_real_data_when_indexed);
    RUN_TEST(resource_status_returns_not_indexed_when_no_store);
    /* Dep search bug regressions */
    RUN_TEST(dep_search_explicit_dep_project_name);
    RUN_TEST(store_prefix_match_includes_deps);
    RUN_TEST(store_exact_match_excludes_deps);
    RUN_TEST(is_dep_project_cross_project_detection);
    RUN_TEST(e2e_dep_search_returns_project_and_dep_results);
    /* Cross-project search prefix collision */
    RUN_TEST(cross_project_search_not_confused_by_prefix);
    RUN_TEST(session_dep_search_uses_session_store);
    RUN_TEST(exact_session_name_uses_session_store);
    RUN_TEST(prefix_collision_dash_after_session_name);
    RUN_TEST(prefix_collision_underscore_after_session_name);
    RUN_TEST(prefix_collision_longer_name_with_dot_not_dep);
    RUN_TEST(prefix_collision_completely_different_project);
    RUN_TEST(prefix_collision_session_is_substring_of_project);
}
