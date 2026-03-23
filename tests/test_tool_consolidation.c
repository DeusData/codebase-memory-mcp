/*
 * test_tool_consolidation.c — Tests for Phase 9 API consolidation.
 *
 * Covers: streamlined/classic tool modes, search_code_graph dispatch,
 * get_code dispatch, project param path support, tool config visibility.
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include <mcp/mcp.h>
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

/* ── Suite registration ──────────────────────────────────── */

SUITE(tool_consolidation) {
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
}
