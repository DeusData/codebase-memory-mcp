/*
 * test_mcp.c — Tests for the MCP server module.
 *
 * Covers: JSON-RPC parsing, MCP protocol, tool dispatch, tool handlers.
 */
#include "../src/foundation/compat.h"
#include "../src/foundation/compat_fs.h" /* cbm_unlink / cbm_rmdir */
#include "test_framework.h"
#include <mcp/mcp.h>
#include <store/store.h>
#include <yyjson/yyjson.h>
#include <string.h>
#include <stdlib.h>

/* ══════════════════════════════════════════════════════════════════
 *  JSON-RPC PARSING
 * ══════════════════════════════════════════════════════════════════ */

TEST(jsonrpc_parse_request) {
    const char *line = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\","
                       "\"params\":{\"capabilities\":{}}}";
    cbm_jsonrpc_request_t req = {0};
    int rc = cbm_jsonrpc_parse(line, &req);
    ASSERT_EQ(rc, 0);
    ASSERT_STR_EQ(req.jsonrpc, "2.0");
    ASSERT_STR_EQ(req.method, "initialize");
    ASSERT_EQ(req.id, 1);
    ASSERT_TRUE(req.has_id);
    ASSERT_NOT_NULL(req.params_raw);
    cbm_jsonrpc_request_free(&req);
    PASS();
}

TEST(jsonrpc_parse_notification) {
    const char *line = "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}";
    cbm_jsonrpc_request_t req = {0};
    int rc = cbm_jsonrpc_parse(line, &req);
    ASSERT_EQ(rc, 0);
    ASSERT_STR_EQ(req.method, "notifications/initialized");
    ASSERT_FALSE(req.has_id);
    cbm_jsonrpc_request_free(&req);
    PASS();
}

TEST(jsonrpc_parse_invalid) {
    cbm_jsonrpc_request_t req = {0};
    int rc = cbm_jsonrpc_parse("not json", &req);
    ASSERT_EQ(rc, -1);
    cbm_jsonrpc_request_free(&req);
    PASS();
}

TEST(jsonrpc_parse_tools_call) {
    const char *line = "{\"jsonrpc\":\"2.0\",\"id\":42,\"method\":\"tools/call\","
                       "\"params\":{\"name\":\"search_graph\","
                       "\"arguments\":{\"label\":\"Function\",\"limit\":5}}}";
    cbm_jsonrpc_request_t req = {0};
    int rc = cbm_jsonrpc_parse(line, &req);
    ASSERT_EQ(rc, 0);
    ASSERT_STR_EQ(req.method, "tools/call");
    ASSERT_EQ(req.id, 42);
    ASSERT_NOT_NULL(req.params_raw);
    cbm_jsonrpc_request_free(&req);
    PASS();
}

/* issue #253: JSON-RPC 2.0 §4 permits string ids (Claude Desktop sends them
 * for "initialize"). Previously strtol-coerced to 0; must be preserved. */
TEST(jsonrpc_parse_string_id_issue253) {
    const char *line = "{\"jsonrpc\":\"2.0\",\"id\":\"init-abc\",\"method\":\"initialize\"}";
    cbm_jsonrpc_request_t req = {0};
    int rc = cbm_jsonrpc_parse(line, &req);
    ASSERT_EQ(rc, 0);
    ASSERT_TRUE(req.has_id);
    ASSERT_NOT_NULL(req.id_str);
    ASSERT_STR_EQ(req.id_str, "init-abc");
    cbm_jsonrpc_request_free(&req);

    /* A purely non-numeric string would have become 0 under strtol. */
    const char *line2 = "{\"jsonrpc\":\"2.0\",\"id\":\"xyz\",\"method\":\"ping\"}";
    cbm_jsonrpc_request_t req2 = {0};
    ASSERT_EQ(cbm_jsonrpc_parse(line2, &req2), 0);
    ASSERT_NOT_NULL(req2.id_str);
    ASSERT_STR_EQ(req2.id_str, "xyz");
    cbm_jsonrpc_request_free(&req2);
    PASS();
}

/* issue #253: the response must echo the string id verbatim, not as a number. */
TEST(jsonrpc_format_response_string_id_issue253) {
    cbm_jsonrpc_response_t resp = {
        .id_str = "init-abc",
        .result_json = "{\"ok\":true}",
    };
    char *json = cbm_jsonrpc_format_response(&resp);
    ASSERT_NOT_NULL(json);
    ASSERT_NOT_NULL(strstr(json, "\"id\":\"init-abc\""));
    /* Must NOT have coerced to a numeric id. */
    ASSERT_NULL(strstr(json, "\"id\":0"));
    free(json);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  JSON-RPC FORMATTING
 * ══════════════════════════════════════════════════════════════════ */

TEST(jsonrpc_format_response) {
    cbm_jsonrpc_response_t resp = {
        .id = 1,
        .result_json = "{\"name\":\"codebase-memory-mcp\"}",
    };
    char *json = cbm_jsonrpc_format_response(&resp);
    ASSERT_NOT_NULL(json);
    /* Should contain jsonrpc, id, and result */
    ASSERT_NOT_NULL(strstr(json, "\"jsonrpc\":\"2.0\""));
    ASSERT_NOT_NULL(strstr(json, "\"id\":1"));
    ASSERT_NOT_NULL(strstr(json, "\"result\""));
    free(json);
    PASS();
}

TEST(jsonrpc_format_error) {
    char *json = cbm_jsonrpc_format_error(5, -32600, "Invalid Request");
    ASSERT_NOT_NULL(json);
    ASSERT_NOT_NULL(strstr(json, "\"id\":5"));
    ASSERT_NOT_NULL(strstr(json, "\"error\""));
    ASSERT_NOT_NULL(strstr(json, "-32600"));
    ASSERT_NOT_NULL(strstr(json, "Invalid Request"));
    free(json);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  MCP PROTOCOL HELPERS
 * ══════════════════════════════════════════════════════════════════ */

TEST(mcp_initialize_response) {
    /* Default (no params): returns latest supported version */
    char *json = cbm_mcp_initialize_response(NULL);
    ASSERT_NOT_NULL(json);
    ASSERT_NOT_NULL(strstr(json, "codebase-memory-mcp"));
    ASSERT_NOT_NULL(strstr(json, "capabilities"));
    ASSERT_NOT_NULL(strstr(json, "tools"));
    ASSERT_NOT_NULL(strstr(json, "2025-11-25"));
    free(json);

    /* Client requests a supported version: server echoes it */
    json = cbm_mcp_initialize_response("{\"protocolVersion\":\"2024-11-05\"}");
    ASSERT_NOT_NULL(json);
    ASSERT_NOT_NULL(strstr(json, "2024-11-05"));
    free(json);

    json = cbm_mcp_initialize_response("{\"protocolVersion\":\"2025-06-18\"}");
    ASSERT_NOT_NULL(json);
    ASSERT_NOT_NULL(strstr(json, "2025-06-18"));
    free(json);

    /* Client requests unknown version: server returns its latest */
    json = cbm_mcp_initialize_response("{\"protocolVersion\":\"9999-01-01\"}");
    ASSERT_NOT_NULL(json);
    ASSERT_NOT_NULL(strstr(json, "2025-11-25"));
    free(json);
    PASS();
}

TEST(mcp_tools_list) {
    char *json = cbm_mcp_tools_list();
    ASSERT_NOT_NULL(json);
    /* Should contain all 14 tools */
    ASSERT_NOT_NULL(strstr(json, "index_repository"));
    ASSERT_NOT_NULL(strstr(json, "search_graph"));
    ASSERT_NOT_NULL(strstr(json, "query_graph"));
    ASSERT_NOT_NULL(strstr(json, "trace_path"));
    ASSERT_NOT_NULL(strstr(json, "get_code_snippet"));
    ASSERT_NOT_NULL(strstr(json, "get_graph_schema"));
    ASSERT_NOT_NULL(strstr(json, "get_architecture"));
    ASSERT_NOT_NULL(strstr(json, "search_code"));
    ASSERT_NOT_NULL(strstr(json, "list_projects"));
    ASSERT_NOT_NULL(strstr(json, "delete_project"));
    ASSERT_NOT_NULL(strstr(json, "index_status"));
    ASSERT_NOT_NULL(strstr(json, "detect_changes"));
    ASSERT_NOT_NULL(strstr(json, "manage_adr"));
    ASSERT_NOT_NULL(strstr(json, "ingest_traces"));
    free(json);
    PASS();
}

TEST(mcp_tools_array_schemas_have_items) {
    /* VS Code 1.112+ rejects array schemas without "items" (see
     * https://github.com/microsoft/vscode/issues/248810).
     * Walk every tool's inputSchema and verify that every "type":"array"
     * property also contains "items". */
    char *json = cbm_mcp_tools_list();
    ASSERT_NOT_NULL(json);

    /* Scan for all occurrences of "type":"array" — each must be followed
     * by "items" before the next closing brace of that property. */
    const char *p = json;
    while ((p = strstr(p, "\"type\":\"array\"")) != NULL) {
        /* Find the enclosing '}' for this property object */
        const char *end = strchr(p, '}');
        ASSERT_NOT_NULL(end);
        /* "items" must appear between p and end */
        size_t span = (size_t)(end - p);
        char *segment = malloc(span + 1);
        memcpy(segment, p, span);
        segment[span] = '\0';
        ASSERT_NOT_NULL(strstr(segment, "\"items\"")); /* array missing items */
        free(segment);
        p = end;
    }

    free(json);
    PASS();
}

TEST(mcp_text_result) {
    char *json = cbm_mcp_text_result("{\"total\":5}", false);
    ASSERT_NOT_NULL(json);
    ASSERT_NOT_NULL(strstr(json, "\"type\":\"text\""));
    /* The text value is JSON-escaped inside the "text" field */
    ASSERT_NOT_NULL(strstr(json, "total"));
    ASSERT_NULL(strstr(json, "\"isError\":true"));
    free(json);
    PASS();
}

TEST(mcp_text_result_error) {
    char *json = cbm_mcp_text_result("something failed", true);
    ASSERT_NOT_NULL(json);
    ASSERT_NOT_NULL(strstr(json, "\"isError\":true"));
    ASSERT_NOT_NULL(strstr(json, "something failed"));
    free(json);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  ARGUMENT EXTRACTION
 * ══════════════════════════════════════════════════════════════════ */

TEST(mcp_get_tool_name) {
    const char *params = "{\"name\":\"search_graph\",\"arguments\":{\"label\":\"Function\"}}";
    char *name = cbm_mcp_get_tool_name(params);
    ASSERT_NOT_NULL(name);
    ASSERT_STR_EQ(name, "search_graph");
    free(name);
    PASS();
}

TEST(mcp_get_arguments) {
    const char *params =
        "{\"name\":\"search_graph\",\"arguments\":{\"label\":\"Function\",\"limit\":5}}";
    char *args = cbm_mcp_get_arguments(params);
    ASSERT_NOT_NULL(args);
    ASSERT_NOT_NULL(strstr(args, "\"label\":\"Function\""));
    ASSERT_NOT_NULL(strstr(args, "\"limit\":5"));
    free(args);
    PASS();
}

TEST(mcp_get_string_arg) {
    const char *args = "{\"label\":\"Function\",\"name_pattern\":\".*Order.*\"}";
    char *val = cbm_mcp_get_string_arg(args, "label");
    ASSERT_NOT_NULL(val);
    ASSERT_STR_EQ(val, "Function");
    free(val);

    val = cbm_mcp_get_string_arg(args, "name_pattern");
    ASSERT_NOT_NULL(val);
    ASSERT_STR_EQ(val, ".*Order.*");
    free(val);

    val = cbm_mcp_get_string_arg(args, "nonexistent");
    ASSERT_NULL(val);
    PASS();
}

TEST(mcp_get_int_arg) {
    const char *args = "{\"limit\":10,\"offset\":5}";
    int val = cbm_mcp_get_int_arg(args, "limit", 0);
    ASSERT_EQ(val, 10);
    val = cbm_mcp_get_int_arg(args, "offset", 0);
    ASSERT_EQ(val, 5);
    val = cbm_mcp_get_int_arg(args, "missing", 42);
    ASSERT_EQ(val, 42);
    PASS();
}

TEST(mcp_get_bool_arg) {
    const char *args = "{\"include_connected\":true,\"regex\":false}";
    bool val = cbm_mcp_get_bool_arg(args, "include_connected");
    ASSERT_TRUE(val);
    val = cbm_mcp_get_bool_arg(args, "regex");
    ASSERT_FALSE(val);
    val = cbm_mcp_get_bool_arg(args, "missing");
    ASSERT_FALSE(val);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  SERVER HANDLE — PROTOCOL FLOW
 * ══════════════════════════════════════════════════════════════════ */

TEST(server_handle_initialize) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\","
                                   "\"params\":{\"capabilities\":{}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"id\":1"));
    ASSERT_NOT_NULL(strstr(resp, "codebase-memory-mcp"));
    ASSERT_NOT_NULL(strstr(resp, "capabilities"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(server_handle_initialized_notification) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    /* Notification has no id → no response */
    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}");
    ASSERT_NULL(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(server_handle_tools_list) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"id\":2"));
    ASSERT_NOT_NULL(strstr(resp, "search_graph"));
    ASSERT_NOT_NULL(strstr(resp, "query_graph"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(server_handle_unknown_method) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"unknown/method\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"error\""));
    ASSERT_NOT_NULL(strstr(resp, "-32601")); /* Method not found */
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  TOOL HANDLERS (via server_handle)
 * ══════════════════════════════════════════════════════════════════ */

/* Helper: create a server with an in-memory store populated with test data */
static cbm_mcp_server_t *setup_mcp_with_data(void) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL); /* NULL = in-memory */
    return srv;
}

TEST(tool_list_projects_empty) {
    cbm_mcp_server_t *srv = setup_mcp_with_data();

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":10,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"list_projects\",\"arguments\":{}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"id\":10"));
    /* Should return a result (possibly empty list) */
    ASSERT_NOT_NULL(strstr(resp, "\"result\""));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_get_graph_schema_empty) {
    cbm_mcp_server_t *srv = setup_mcp_with_data();

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":11,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"get_graph_schema\",\"arguments\":{}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"result\""));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_unknown_tool) {
    cbm_mcp_server_t *srv = setup_mcp_with_data();

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":12,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"nonexistent_tool\",\"arguments\":{}}}");
    ASSERT_NOT_NULL(resp);
    /* Should return result with isError */
    ASSERT_NOT_NULL(strstr(resp, "isError"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_search_graph_basic) {
    cbm_mcp_server_t *srv = setup_mcp_with_data();

    /* search_graph with no project → should work on empty store */
    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":13,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"search_graph\","
                                   "\"arguments\":{\"label\":\"Function\",\"limit\":10}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"result\""));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

/* Forward declarations for helpers defined later in this file */
static cbm_mcp_server_t *setup_snippet_server(char *tmp_dir, size_t tmp_sz);
static void cleanup_snippet_dir(const char *tmp_dir);
static char *extract_text_content(const char *mcp_result);

TEST(tool_search_graph_includes_node_properties) {
    /* search_graph results must surface each node's properties_json
     * payload so callers don't have to round-trip through get_code_snippet
     * just to read them. The setup_snippet_server inserts HandleRequest
     * with a signature/return_type/is_exported property blob; this test
     * pins that those keys reach the MCP response. */
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":42,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"search_graph\","
             "\"arguments\":{\"project\":\"test-project\",\"label\":\"Function\","
             "\"name_pattern\":\"HandleRequest\",\"limit\":5}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    /* Properties from HandleRequest's properties_json must appear. */
    ASSERT_NOT_NULL(strstr(inner, "signature"));
    ASSERT_NOT_NULL(strstr(inner, "func HandleRequest"));
    ASSERT_NOT_NULL(strstr(inner, "is_exported"));
    free(inner);
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

TEST(tool_query_graph_basic) {
    cbm_mcp_server_t *srv = setup_mcp_with_data();

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":14,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"query_graph\","
             "\"arguments\":{\"query\":\"MATCH (f:Function) RETURN f.name\"}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"result\""));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_index_status_no_project) {
    cbm_mcp_server_t *srv = setup_mcp_with_data();

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":15,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"index_status\",\"arguments\":{}}}");
    ASSERT_NOT_NULL(resp);
    /* Should return error or empty status */
    ASSERT_NOT_NULL(strstr(resp, "\"result\""));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  TOOL HANDLERS WITH DATA
 * ══════════════════════════════════════════════════════════════════ */

TEST(tool_trace_call_path_not_found) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":20,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"trace_call_path\","
                                   "\"arguments\":{\"function_name\":\"NonExistent\","
                                   "\"project\":\"nonexistent\"}}}");
    ASSERT_NOT_NULL(resp);
    /* Should return error about project not found */
    ASSERT_NOT_NULL(strstr(resp, "not found"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_trace_missing_function_name) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":21,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"trace_call_path\","
                                   "\"arguments\":{}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "required"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_delete_project_not_found) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":22,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"delete_project\","
                                   "\"arguments\":{\"project\":\"nonexistent\"}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "not_found"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_get_architecture_empty) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":24,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"get_architecture\","
                                   "\"arguments\":{\"project\":\"nonexistent\"}}}");
    ASSERT_NOT_NULL(resp);
    /* No store for nonexistent project — should return project error */
    ASSERT_TRUE(strstr(resp, "not found") || strstr(resp, "not indexed"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

/* Regression for #281: handle_get_architecture must actually call
 * cbm_store_get_architecture and surface its sections. Before the fix
 * only label/edge histograms were emitted regardless of which aspects
 * were requested. The store-side arch_entry_points query reads
 * properties.is_entry_point on Function nodes, so we tag one node and
 * assert the resulting JSON surfaces an "entry_points" array containing
 * the tagged function — which is impossible without the wiring. */
TEST(tool_get_architecture_emits_populated_sections) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    cbm_store_t *st = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(st);

    const char *proj = "arch-test";
    cbm_mcp_server_set_project(srv, proj);
    cbm_store_upsert_project(st, proj, "/tmp/arch-test");

    cbm_node_t main_fn = {0};
    main_fn.project = proj;
    main_fn.label = "Function";
    main_fn.name = "main";
    main_fn.qualified_name = "arch-test.cmd.main";
    main_fn.file_path = "cmd/main.go";
    main_fn.start_line = 1;
    main_fn.end_line = 3;
    main_fn.properties_json = "{\"is_entry_point\":true}";
    ASSERT_GT(cbm_store_upsert_node(st, &main_fn), 0);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":91,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"get_architecture\","
             "\"arguments\":{\"project\":\"arch-test\",\"aspects\":[\"all\"]}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);

    /* The handler always emits node/edge counts and schema histograms;
     * those existed before #281. The "entry_points" array only appears
     * when cbm_store_get_architecture is actually called and its result
     * is serialized — which is exactly what #281 wires up. */
    ASSERT_NOT_NULL(strstr(inner, "\"entry_points\""));
    ASSERT_NOT_NULL(strstr(inner, "main"));

    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_query_graph_missing_query) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":23,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"query_graph\","
                                   "\"arguments\":{}}}");
    ASSERT_NOT_NULL(resp);
    /* Should return error about missing query */
    ASSERT_NOT_NULL(strstr(resp, "required"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  PIPELINE-DEPENDENT TOOL HANDLERS
 * ══════════════════════════════════════════════════════════════════ */

TEST(tool_index_repository_missing_path) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":30,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"index_repository\","
                                   "\"arguments\":{}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "required"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_get_code_snippet_missing_qn) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":31,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"get_code_snippet\","
                                   "\"arguments\":{}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "required"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_get_code_snippet_not_found) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":32,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"get_code_snippet\","
                                   "\"arguments\":{\"qualified_name\":\"nonexistent.func\","
                                   "\"project\":\"nonexistent\"}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "not found"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_search_code_missing_pattern) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":33,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"search_code\","
                                   "\"arguments\":{}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "required"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_search_code_no_project) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":34,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"search_code\","
                                   "\"arguments\":{\"pattern\":\"func main\","
                                   "\"project\":\"nonexistent\"}}}");
    ASSERT_NOT_NULL(resp);
    /* No project indexed → error */
    ASSERT_TRUE(strstr(resp, "not found") || strstr(resp, "not indexed") ||
                strstr(resp, "required"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(search_code_multi_word) {
    char tmp[512];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    /* Multi-word query "HandleRequest error" — should find the line
     * "func HandleRequest() error {" via regex conversion. */
    char req[512];
    snprintf(req, sizeof(req),
             "{\"jsonrpc\":\"2.0\",\"id\":90,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"search_code\","
             "\"arguments\":{\"pattern\":\"HandleRequest error\","
             "\"project\":\"test-project\"}}}");

    char *resp = cbm_mcp_server_handle(srv, req);
    ASSERT_NOT_NULL(resp);
    /* Should find at least one result (not zero) */
    ASSERT_TRUE(strstr(resp, "HandleRequest") != NULL);
    /* Should NOT contain an error about "not found" */
    ASSERT_TRUE(strstr(resp, "\"isError\":true") == NULL);
    free(resp);

    cleanup_snippet_dir(tmp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* issue #283: search_code with regex=true and a syntactically invalid pattern
 * must return an explicit error, not an empty result indistinguishable from a
 * legitimate no-match. */
TEST(search_code_invalid_regex_errors_issue283) {
    char tmp[512];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    /* Unclosed group under regex=true → must be flagged as an error. */
    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":91,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"search_code\","
                                   "\"arguments\":{\"pattern\":\"func(\",\"regex\":true,"
                                   "\"project\":\"test-project\"}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"isError\":true"));
    ASSERT_NOT_NULL(strstr(resp, "invalid regex"));
    free(resp);

    /* Same pattern as a literal (regex=false) must NOT error. */
    resp = cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":92,\"method\":\"tools/call\","
                                      "\"params\":{\"name\":\"search_code\","
                                      "\"arguments\":{\"pattern\":\"func(\",\"regex\":false,"
                                      "\"project\":\"test-project\"}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_TRUE(strstr(resp, "invalid regex") == NULL);
    free(resp);

    cleanup_snippet_dir(tmp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* issue #282: a literal '|' under regex=false is a silent 0-match trap. It must
 * now be surfaced as a warning (and the result carries elapsed_ms). */
TEST(search_code_literal_pipe_warns_issue282) {
    char tmp[512];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":93,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"search_code\","
                                   "\"arguments\":{\"pattern\":\"HandleRequest|Nope\","
                                   "\"regex\":false,\"project\":\"test-project\"}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "warnings"));   /* surfaced, not silent */
    ASSERT_NOT_NULL(strstr(resp, "regex=true")); /* the hint names the fix */
    ASSERT_NOT_NULL(strstr(resp, "elapsed_ms")); /* timing is reported */
    free(resp);

    cleanup_snippet_dir(tmp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* issue #272: '&' in a path / file_pattern is neutralised by the command's
 * quoting and must no longer be rejected as "invalid characters". */
TEST(search_code_ampersand_accepted_issue272) {
    char tmp[512];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":94,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"search_code\","
                                   "\"arguments\":{\"pattern\":\"HandleRequest\","
                                   "\"file_pattern\":\"*R&D*.go\",\"project\":\"test-project\"}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_TRUE(strstr(resp, "invalid characters") == NULL);
    free(resp);

    cleanup_snippet_dir(tmp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_detect_changes_no_project) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":35,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"detect_changes\","
                                   "\"arguments\":{}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "not found"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_manage_adr_no_project) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":36,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"manage_adr\","
                                   "\"arguments\":{}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "not found"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

/* Regression test for use-after-free in handle_manage_adr (get path).
 * MUST FAIL before fix: free(buf) is called before yy_doc_to_str serializes doc,
 * so result field is missing or contains garbage. MUST PASS after fix. */
TEST(tool_manage_adr_get_with_existing_adr) {
    /* Create a temp directory with .codebase-memory/adr.md */
    char tmp_dir[256];
    snprintf(tmp_dir, sizeof(tmp_dir), "/tmp/cbm-adr-test-XXXXXX");
    if (!cbm_mkdtemp(tmp_dir)) {
        PASS(); /* skip if mkdtemp fails */
    }

    char adr_dir[512];
    snprintf(adr_dir, sizeof(adr_dir), "%s/.codebase-memory", tmp_dir);
    cbm_mkdir(adr_dir);

    char adr_path[512];
    snprintf(adr_path, sizeof(adr_path), "%s/adr.md", adr_dir);
    FILE *fp = fopen(adr_path, "w");
    ASSERT_NOT_NULL(fp);
    fputs("## PURPOSE\nTest ADR content for regression test.\n\n"
          "## STACK\nC, SQLite.\n\n"
          "## ARCHITECTURE\nMCP server.\n",
          fp);
    fclose(fp);

    /* Create server and register the project */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(st);
    cbm_store_upsert_project(st, "test-adr-uaf", tmp_dir);
    cbm_mcp_server_set_project(srv, "test-adr-uaf");

    /* Call manage_adr via full JSON-RPC path to exercise cbm_jsonrpc_format_response.
     * The bug: free(buf) before yy_doc_to_str causes garbage JSON; format_response
     * then fails to parse the result and omits the "result" field entirely. */
    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":99,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"manage_adr\","
             "\"arguments\":{\"project\":\"test-adr-uaf\",\"mode\":\"get\"}}}");
    ASSERT_NOT_NULL(resp);
    /* JSON-RPC response must include a "result" field (absent when use-after-free) */
    ASSERT_NOT_NULL(strstr(resp, "\"result\""));
    /* ADR content must appear in response */
    ASSERT_NOT_NULL(strstr(resp, "PURPOSE"));
    /* Must not be an error */
    ASSERT_NULL(strstr(resp, "isError"));
    free(resp);

    /* Clean up */
    cbm_mcp_server_free(srv);
    remove(adr_path);
    rmdir(adr_dir);
    rmdir(tmp_dir);
    PASS();
}

/* issue #256: manage_adr (MCP) and the UI /api/adr endpoints must share ONE
 * backend. A manage_adr(update) write must be readable via cbm_store_adr_get
 * (the exact API the UI's /api/adr GET uses). */
TEST(tool_manage_adr_unified_backend_issue256) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(st);
    cbm_store_upsert_project(st, "adr-unify", "/tmp/adr-unify");
    cbm_mcp_server_set_project(srv, "adr-unify");

    /* Write via the MCP tool. */
    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":120,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"manage_adr\",\"arguments\":{\"project\":\"adr-unify\","
             "\"mode\":\"update\",\"content\":\"## PURPOSE\\nUnified ADR backend.\\n\"}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "updated"));
    free(resp);

    /* Read DIRECTLY via the store API the UI /api/adr uses — must see it. */
    cbm_adr_t adr;
    memset(&adr, 0, sizeof(adr));
    ASSERT_EQ(cbm_store_adr_get(st, "adr-unify", &adr), CBM_STORE_OK);
    ASSERT_NOT_NULL(adr.content);
    ASSERT_NOT_NULL(strstr(adr.content, "Unified ADR backend."));
    cbm_store_adr_free(&adr);

    /* And manage_adr(get) round-trips the same content. */
    resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":121,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"manage_adr\",\"arguments\":{\"project\":\"adr-unify\","
             "\"mode\":\"get\"}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "Unified ADR backend."));
    ASSERT_NULL(strstr(resp, "isError"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_ingest_traces_basic) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":37,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"ingest_traces\","
             "\"arguments\":{\"traces\":[{\"caller\":\"a\",\"callee\":\"b\"}]}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "accepted"));
    ASSERT_NOT_NULL(strstr(resp, "traces_received"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_ingest_traces_empty) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":38,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"ingest_traces\","
                                   "\"arguments\":{\"traces\":[]}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "accepted"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  IDLE STORE EVICTION
 * ══════════════════════════════════════════════════════════════════ */

TEST(store_idle_eviction) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    cbm_mcp_server_set_project(srv, "test-evict");

    /* Trigger resolve_store via a tool call to set store_last_used */
    char *resp = cbm_mcp_handle_tool(srv, "get_graph_schema", "{\"project\":\"test-evict\"}");
    free(resp);

    ASSERT_TRUE(cbm_mcp_server_has_cached_store(srv));

    /* Evict with 0s timeout → should evict immediately */
    cbm_mcp_server_evict_idle(srv, 0);
    ASSERT_FALSE(cbm_mcp_server_has_cached_store(srv));

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(store_idle_no_eviction_within_timeout) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    cbm_mcp_server_set_project(srv, "test-evict");

    char *resp = cbm_mcp_handle_tool(srv, "get_graph_schema", "{\"project\":\"test-evict\"}");
    free(resp);

    ASSERT_TRUE(cbm_mcp_server_has_cached_store(srv));

    /* Evict with large timeout → should NOT evict */
    cbm_mcp_server_evict_idle(srv, 99999);
    ASSERT_TRUE(cbm_mcp_server_has_cached_store(srv));

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(store_idle_evict_protects_initial_store) {
    /* Evicting with NULL server should not crash */
    cbm_mcp_server_evict_idle(NULL, 0);

    /* Evicting server whose store was never accessed via a named project
     * should NOT evict the initial in-memory store (store_last_used == 0). */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_TRUE(cbm_mcp_server_has_cached_store(srv));
    cbm_mcp_server_evict_idle(srv, 0);
    ASSERT_TRUE(cbm_mcp_server_has_cached_store(srv));

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(store_idle_evict_access_resets_timer) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    cbm_mcp_server_set_project(srv, "test-evict");

    /* First access */
    char *resp = cbm_mcp_handle_tool(srv, "get_graph_schema", "{\"project\":\"test-evict\"}");
    free(resp);

    /* Second access (resets timer) */
    resp = cbm_mcp_handle_tool(srv, "get_graph_schema", "{\"project\":\"test-evict\"}");
    free(resp);

    ASSERT_TRUE(cbm_mcp_server_has_cached_store(srv));

    /* With large timeout, store should survive */
    cbm_mcp_server_evict_idle(srv, 99999);
    ASSERT_TRUE(cbm_mcp_server_has_cached_store(srv));

    /* With 0 timeout, store should be evicted */
    cbm_mcp_server_evict_idle(srv, 0);
    ASSERT_FALSE(cbm_mcp_server_has_cached_store(srv));

    cbm_mcp_server_free(srv);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  URI HELPERS
 * ══════════════════════════════════════════════════════════════════ */

TEST(parse_file_uri_unix) {
    char path[256];
    ASSERT_TRUE(cbm_parse_file_uri("file:///home/user/project", path, sizeof(path)));
    ASSERT_STR_EQ(path, "/home/user/project");

    ASSERT_TRUE(cbm_parse_file_uri("file:///tmp/test", path, sizeof(path)));
    ASSERT_STR_EQ(path, "/tmp/test");

    ASSERT_TRUE(cbm_parse_file_uri("file:///", path, sizeof(path)));
    ASSERT_STR_EQ(path, "/");
    PASS();
}

TEST(parse_file_uri_windows) {
    char path[256];
    /* Windows drive letter — leading / stripped */
    ASSERT_TRUE(cbm_parse_file_uri("file:///C:/Users/project", path, sizeof(path)));
    ASSERT_STR_EQ(path, "C:/Users/project");

    ASSERT_TRUE(cbm_parse_file_uri("file:///D:/Projects/myapp", path, sizeof(path)));
    ASSERT_STR_EQ(path, "D:/Projects/myapp");
    PASS();
}

TEST(parse_file_uri_invalid) {
    char path[256];
    /* Non-file URI */
    ASSERT_FALSE(cbm_parse_file_uri("https://example.com", path, sizeof(path)));
    ASSERT_STR_EQ(path, "");

    /* Empty string */
    ASSERT_FALSE(cbm_parse_file_uri("", path, sizeof(path)));
    ASSERT_STR_EQ(path, "");

    /* NULL */
    ASSERT_FALSE(cbm_parse_file_uri(NULL, path, sizeof(path)));
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  SNIPPET TESTS — Port of internal/tools/snippet_test.go
 * ══════════════════════════════════════════════════════════════════ */

#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

/* Create an MCP server pre-populated with nodes/edges matching Go testSnippetServer.
 * Writes a source file to tmp_dir/project/main.go.
 * Caller must free the server with cbm_mcp_server_free and
 * unlink the source file + rmdir manually. */
static cbm_mcp_server_t *setup_snippet_server(char *tmp_dir, size_t tmp_sz) {
    /* Create temp dir */
    snprintf(tmp_dir, tmp_sz, "/tmp/cbm_snippet_test_XXXXXX");
    if (!cbm_mkdtemp(tmp_dir))
        return NULL;

    char proj_dir[512];
    snprintf(proj_dir, sizeof(proj_dir), "%s/project", tmp_dir);
    cbm_mkdir(proj_dir);

    /* Write sample source file */
    char src_path[512];
    snprintf(src_path, sizeof(src_path), "%s/main.go", proj_dir);
    FILE *fp = fopen(src_path, "w");
    if (!fp)
        return NULL;
    fprintf(fp, "package main\n"
                "\n"
                "func HandleRequest() error {\n"
                "\treturn nil\n"
                "}\n"
                "\n"
                "func ProcessOrder(id int) {\n"
                "\t// process\n"
                "}\n"
                "\n"
                "func Run() {\n"
                "\t// server\n"
                "}\n");
    fclose(fp);

    /* Create server with in-memory store */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    if (!srv)
        return NULL;

    cbm_store_t *st = cbm_mcp_server_store(srv);
    if (!st) {
        cbm_mcp_server_free(srv);
        return NULL;
    }

    const char *proj_name = "test-project";
    cbm_mcp_server_set_project(srv, proj_name);
    cbm_store_upsert_project(st, proj_name, proj_dir);

    /* Create nodes */
    cbm_node_t n_hr = {0};
    n_hr.project = proj_name;
    n_hr.label = "Function";
    n_hr.name = "HandleRequest";
    n_hr.qualified_name = "test-project.cmd.server.main.HandleRequest";
    n_hr.file_path = "main.go";
    n_hr.start_line = 3;
    n_hr.end_line = 5;
    n_hr.properties_json = "{\"signature\":\"func HandleRequest() error\","
                           "\"return_type\":\"error\","
                           "\"is_exported\":true}";
    int64_t id_hr = cbm_store_upsert_node(st, &n_hr);

    cbm_node_t n_po = {0};
    n_po.project = proj_name;
    n_po.label = "Function";
    n_po.name = "ProcessOrder";
    n_po.qualified_name = "test-project.cmd.server.main.ProcessOrder";
    n_po.file_path = "main.go";
    n_po.start_line = 7;
    n_po.end_line = 9;
    n_po.properties_json = "{\"signature\":\"func ProcessOrder(id int)\"}";
    int64_t id_po = cbm_store_upsert_node(st, &n_po);

    cbm_node_t n_run1 = {0};
    n_run1.project = proj_name;
    n_run1.label = "Function";
    n_run1.name = "Run";
    n_run1.qualified_name = "test-project.cmd.server.Run";
    n_run1.file_path = "main.go";
    n_run1.start_line = 11;
    n_run1.end_line = 13;
    int64_t id_run1 = cbm_store_upsert_node(st, &n_run1);

    cbm_node_t n_run2 = {0};
    n_run2.project = proj_name;
    n_run2.label = "Function";
    n_run2.name = "Run";
    n_run2.qualified_name = "test-project.cmd.worker.Run";
    n_run2.file_path = "main.go";
    n_run2.start_line = 11;
    n_run2.end_line = 13;
    cbm_store_upsert_node(st, &n_run2);

    /* Create edges: HandleRequest -> ProcessOrder, HandleRequest -> Run1 */
    cbm_edge_t e1 = {.project = proj_name, .source_id = id_hr, .target_id = id_po, .type = "CALLS"};
    cbm_store_insert_edge(st, &e1);

    cbm_edge_t e2 = {
        .project = proj_name, .source_id = id_hr, .target_id = id_run1, .type = "CALLS"};
    cbm_store_insert_edge(st, &e2);
    (void)id_run1; /* run1 used for edge above */

    return srv;
}

/* Cleanup temp files created by setup_snippet_server */
static void cleanup_snippet_dir(const char *tmp_dir) {
    char path[512];
    snprintf(path, sizeof(path), "%s/project/main.go", tmp_dir);
    unlink(path);
    snprintf(path, sizeof(path), "%s/project", tmp_dir);
    rmdir(path);
    rmdir(tmp_dir);
}

/* Extract the inner "text" value from an MCP tool result JSON.
 * The MCP envelope is: {"content":[{"type":"text","text":"<inner json>"}]}
 * This returns the unescaped inner JSON. Caller must free. */
static char *extract_text_content(const char *mcp_result) {
    if (!mcp_result)
        return NULL;
    yyjson_doc *doc = yyjson_read(mcp_result, strlen(mcp_result), 0);
    if (!doc)
        return strdup(mcp_result); /* fallback */
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *content = yyjson_obj_get(root, "content");
    if (!content) {
        /* Handle JSON-RPC wrapper: {"jsonrpc":...,"result":{"content":[...]}} */
        yyjson_val *rpc_result = yyjson_obj_get(root, "result");
        if (rpc_result) {
            content = yyjson_obj_get(rpc_result, "content");
        }
    }
    if (!content || !yyjson_is_arr(content)) {
        yyjson_doc_free(doc);
        return strdup(mcp_result);
    }
    yyjson_val *item = yyjson_arr_get(content, 0);
    if (!item) {
        yyjson_doc_free(doc);
        return strdup(mcp_result);
    }
    yyjson_val *text = yyjson_obj_get(item, "text");
    const char *str = yyjson_get_str(text);
    char *result = str ? strdup(str) : strdup(mcp_result);
    yyjson_doc_free(doc);
    return result;
}

/* Call get_code_snippet and extract inner text content.
 * Caller must free returned string. */
static char *call_snippet(cbm_mcp_server_t *srv, const char *args_json) {
    char *raw = cbm_mcp_handle_tool(srv, "get_code_snippet", args_json);
    char *text = extract_text_content(raw);
    free(raw);
    return text;
}

/* ── TestSnippet_ExactQN ──────────────────────────────────────── */

TEST(snippet_exact_qn) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *resp =
        call_snippet(srv, "{\"qualified_name\":\"test-project.cmd.server.main.HandleRequest\","
                          "\"project\":\"test-project\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"name\":\"HandleRequest\""));
    ASSERT_NOT_NULL(strstr(resp, "\"source\""));
    /* Exact match should NOT have match_method */
    ASSERT_NULL(strstr(resp, "\"match_method\""));
    /* Enriched properties */
    ASSERT_NOT_NULL(strstr(resp, "\"signature\":\"func HandleRequest() error\""));
    ASSERT_NOT_NULL(strstr(resp, "\"return_type\":\"error\""));
    /* Caller/callee counts: 0 callers, 2 callees */
    ASSERT_NOT_NULL(strstr(resp, "\"callers\":0"));
    ASSERT_NOT_NULL(strstr(resp, "\"callees\":2"));
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

/* ── TestSnippet_QNSuffix ─────────────────────────────────────── */

TEST(snippet_qn_suffix) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *resp = call_snippet(srv, "{\"qualified_name\":\"main.HandleRequest\","
                                   "\"project\":\"test-project\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"name\":\"HandleRequest\""));
    ASSERT_NOT_NULL(strstr(resp, "\"match_method\":\"suffix\""));
    ASSERT_NOT_NULL(strstr(resp, "\"source\""));
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

/* ── TestSnippet_UniqueShortName ──────────────────────────────── */

TEST(snippet_unique_short_name) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    /* "ProcessOrder" is unique — suffix tier matches (QN ends with .ProcessOrder) */
    char *resp = call_snippet(srv, "{\"qualified_name\":\"ProcessOrder\","
                                   "\"project\":\"test-project\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"name\":\"ProcessOrder\""));
    ASSERT_NOT_NULL(strstr(resp, "\"match_method\":\"suffix\""));
    ASSERT_NOT_NULL(strstr(resp, "\"source\""));
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

/* ── TestSnippet_NameTier ─────────────────────────────────────── */

TEST(snippet_name_tier) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    /* "HandleRequest" — suffix tier finds it (QN ends with .HandleRequest) */
    char *resp = call_snippet(srv, "{\"qualified_name\":\"HandleRequest\","
                                   "\"project\":\"test-project\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"name\":\"HandleRequest\""));
    ASSERT_NOT_NULL(strstr(resp, "\"match_method\":\"suffix\""));
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

/* ── TestSnippet_AmbiguousShortName ───────────────────────────── */

TEST(snippet_ambiguous_short_name) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    /* "Run" matches 2 nodes — should return suggestions */
    char *resp = call_snippet(srv, "{\"qualified_name\":\"Run\","
                                   "\"project\":\"test-project\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"status\":\"ambiguous\""));
    ASSERT_NOT_NULL(strstr(resp, "\"message\""));
    ASSERT_NOT_NULL(strstr(resp, "\"suggestions\""));
    /* Must NOT have "error" key */
    ASSERT_NULL(strstr(resp, "\"error\""));
    /* Must NOT have "source" */
    ASSERT_NULL(strstr(resp, "\"source\""));
    /* Should have at least 2 suggestions with qualified_name */
    ASSERT_NOT_NULL(strstr(resp, "test-project.cmd.server.Run"));
    ASSERT_NOT_NULL(strstr(resp, "test-project.cmd.worker.Run"));
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

/* ── TestSnippet_NotFound ─────────────────────────────────────── */

TEST(snippet_not_found) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *resp = call_snippet(srv, "{\"qualified_name\":\"CompletelyNonexistentFunctionXYZ123\","
                                   "\"project\":\"test-project\"}");
    ASSERT_NOT_NULL(resp);
    /* Should return error or suggestions */
    ASSERT_TRUE(strstr(resp, "not found") || strstr(resp, "suggestions"));
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

/* ── TestSnippet_FuzzySuggestions ─────────────────────────────── */

TEST(snippet_fuzzy_suggestions) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    /* "Handle" is not an exact QN or suffix — should get not-found guidance */
    char *resp = call_snippet(srv, "{\"qualified_name\":\"Handle\","
                                   "\"project\":\"test-project\"}");
    ASSERT_NOT_NULL(resp);
    /* Should guide user to search_graph */
    ASSERT_NOT_NULL(strstr(resp, "search_graph"));
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

/* ── TestSnippet_EnrichedProperties ───────────────────────────── */

TEST(snippet_enriched_properties) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *resp =
        call_snippet(srv, "{\"qualified_name\":\"test-project.cmd.server.main.HandleRequest\","
                          "\"project\":\"test-project\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"signature\""));
    ASSERT_NOT_NULL(strstr(resp, "\"return_type\""));
    ASSERT_NOT_NULL(strstr(resp, "\"is_exported\":true"));
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

/* ── TestSnippet_FuzzyLastSegment ─────────────────────────────── */

TEST(snippet_fuzzy_last_segment) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    /* "auth.handlers.HandleRequest" — suffix match should find HandleRequest */
    char *resp = call_snippet(srv, "{\"qualified_name\":\"auth.handlers.HandleRequest\","
                                   "\"project\":\"test-project\"}");
    ASSERT_NOT_NULL(resp);
    /* Should either find it via suffix or guide to search_graph */
    ASSERT_TRUE(strstr(resp, "HandleRequest") != NULL || strstr(resp, "search_graph") != NULL);
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

/* ── TestSnippet_AutoResolve_Default ──────────────────────────── */

TEST(snippet_auto_resolve_default) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    /* "Run" is ambiguous (2 candidates). Without auto_resolve → suggestions */
    char *resp = call_snippet(srv, "{\"qualified_name\":\"Run\","
                                   "\"project\":\"test-project\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"status\":\"ambiguous\""));
    ASSERT_NULL(strstr(resp, "\"source\""));
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

/* ── TestSnippet_AutoResolve_Enabled ──────────────────────────── */

TEST(snippet_auto_resolve_enabled) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    /* "Run" — suffix match should find candidates or guide to search */
    char *resp = call_snippet(srv, "{\"qualified_name\":\"Run\","
                                   "\"project\":\"test-project\"}");
    ASSERT_NOT_NULL(resp);
    /* "Run" matches multiple nodes via suffix → should get suggestions or source */
    ASSERT_TRUE(strstr(resp, "Run") != NULL);
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

/* ── TestSnippet_IncludeNeighbors_Default ─────────────────────── */

TEST(snippet_include_neighbors_default) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *resp =
        call_snippet(srv, "{\"qualified_name\":\"test-project.cmd.server.main.HandleRequest\","
                          "\"project\":\"test-project\"}");
    ASSERT_NOT_NULL(resp);
    /* Without include_neighbors → NO caller_names/callee_names */
    ASSERT_NULL(strstr(resp, "\"caller_names\""));
    ASSERT_NULL(strstr(resp, "\"callee_names\""));
    /* But should still have counts */
    ASSERT_NOT_NULL(strstr(resp, "\"callers\""));
    ASSERT_NOT_NULL(strstr(resp, "\"callees\""));
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

/* ── TestSnippet_IncludeNeighbors_Enabled ─────────────────────── */

TEST(snippet_include_neighbors_enabled) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *resp =
        call_snippet(srv, "{\"qualified_name\":\"test-project.cmd.server.main.HandleRequest\","
                          "\"include_neighbors\":true,\"project\":\"test-project\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"source\""));
    /* HandleRequest has 0 callers → no caller_names array */
    ASSERT_NULL(strstr(resp, "\"caller_names\""));
    /* HandleRequest has 2 callees: ProcessOrder and Run */
    ASSERT_NOT_NULL(strstr(resp, "\"callee_names\""));
    ASSERT_NOT_NULL(strstr(resp, "ProcessOrder"));
    ASSERT_NOT_NULL(strstr(resp, "Run"));
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  JSON-RPC PARSING — EDGE CASES
 * ══════════════════════════════════════════════════════════════════ */

TEST(jsonrpc_parse_empty_string) {
    cbm_jsonrpc_request_t req = {0};
    int rc = cbm_jsonrpc_parse("", &req);
    ASSERT_EQ(rc, -1);
    cbm_jsonrpc_request_free(&req);
    PASS();
}

TEST(jsonrpc_parse_missing_jsonrpc_field) {
    /* jsonrpc field absent — parser defaults to "2.0" if method present */
    const char *line = "{\"id\":1,\"method\":\"initialize\",\"params\":{}}";
    cbm_jsonrpc_request_t req = {0};
    int rc = cbm_jsonrpc_parse(line, &req);
    ASSERT_EQ(rc, 0);
    ASSERT_STR_EQ(req.jsonrpc, "2.0");
    ASSERT_STR_EQ(req.method, "initialize");
    ASSERT_TRUE(req.has_id);
    cbm_jsonrpc_request_free(&req);
    PASS();
}

TEST(jsonrpc_parse_missing_method) {
    /* method is required — should fail */
    const char *line = "{\"jsonrpc\":\"2.0\",\"id\":1,\"params\":{}}";
    cbm_jsonrpc_request_t req = {0};
    int rc = cbm_jsonrpc_parse(line, &req);
    ASSERT_EQ(rc, -1);
    cbm_jsonrpc_request_free(&req);
    PASS();
}

TEST(jsonrpc_parse_string_id) {
    /* JSON-RPC §4: string and numeric ids are distinct. A string id is
     * preserved verbatim (issue #253), never coerced to a number. */
    const char *line = "{\"jsonrpc\":\"2.0\",\"id\":\"99\",\"method\":\"tools/list\"}";
    cbm_jsonrpc_request_t req = {0};
    int rc = cbm_jsonrpc_parse(line, &req);
    ASSERT_EQ(rc, 0);
    ASSERT_TRUE(req.has_id);
    ASSERT_NOT_NULL(req.id_str);
    ASSERT_STR_EQ(req.id_str, "99");
    ASSERT_STR_EQ(req.method, "tools/list");
    cbm_jsonrpc_request_free(&req);
    PASS();
}

TEST(jsonrpc_parse_no_params) {
    /* Request with no params field — params_raw should be NULL */
    const char *line = "{\"jsonrpc\":\"2.0\",\"id\":5,\"method\":\"tools/list\"}";
    cbm_jsonrpc_request_t req = {0};
    int rc = cbm_jsonrpc_parse(line, &req);
    ASSERT_EQ(rc, 0);
    ASSERT_NULL(req.params_raw);
    ASSERT_EQ(req.id, 5);
    cbm_jsonrpc_request_free(&req);
    PASS();
}

TEST(jsonrpc_parse_extra_whitespace) {
    /* Leading/trailing whitespace and internal spacing in JSON */
    const char *line = "  { \"jsonrpc\" : \"2.0\" , \"id\" : 7 , \"method\" : \"ping\" }  ";
    cbm_jsonrpc_request_t req = {0};
    int rc = cbm_jsonrpc_parse(line, &req);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(req.id, 7);
    ASSERT_STR_EQ(req.method, "ping");
    cbm_jsonrpc_request_free(&req);
    PASS();
}

TEST(jsonrpc_parse_array_not_object) {
    /* JSON array at root — not a valid JSON-RPC request */
    cbm_jsonrpc_request_t req = {0};
    int rc = cbm_jsonrpc_parse("[1,2,3]", &req);
    ASSERT_EQ(rc, -1);
    cbm_jsonrpc_request_free(&req);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  ARGUMENT EXTRACTION — EDGE CASES
 * ══════════════════════════════════════════════════════════════════ */

TEST(mcp_get_string_arg_empty_json) {
    /* Empty JSON string — yyjson_read fails → NULL */
    char *val = cbm_mcp_get_string_arg("", "key");
    ASSERT_NULL(val);
    PASS();
}

TEST(mcp_get_string_arg_empty_object) {
    /* Valid JSON with no keys → NULL for any key */
    char *val = cbm_mcp_get_string_arg("{}", "key");
    ASSERT_NULL(val);
    PASS();
}

TEST(mcp_get_string_arg_nested_value) {
    /* Value is an object, not a string → should return NULL */
    const char *args = "{\"config\":{\"nested\":true},\"name\":\"hello\"}";
    char *val = cbm_mcp_get_string_arg(args, "config");
    ASSERT_NULL(val); /* not a string type */
    val = cbm_mcp_get_string_arg(args, "name");
    ASSERT_NOT_NULL(val);
    ASSERT_STR_EQ(val, "hello");
    free(val);
    PASS();
}

TEST(mcp_get_string_arg_int_value) {
    /* Value is an integer, not a string → NULL */
    char *val = cbm_mcp_get_string_arg("{\"count\":42}", "count");
    ASSERT_NULL(val);
    PASS();
}

TEST(mcp_get_int_arg_empty_json) {
    int val = cbm_mcp_get_int_arg("", "key", 99);
    ASSERT_EQ(val, 99);
    PASS();
}

TEST(mcp_get_int_arg_string_value) {
    /* Value is a string, not int → should return default */
    int val = cbm_mcp_get_int_arg("{\"limit\":\"ten\"}", "limit", 5);
    ASSERT_EQ(val, 5);
    PASS();
}

TEST(mcp_get_int_arg_bool_value) {
    /* Value is a bool, not int → default */
    int val = cbm_mcp_get_int_arg("{\"flag\":true}", "flag", -1);
    ASSERT_EQ(val, -1);
    PASS();
}

TEST(mcp_get_bool_arg_empty_json) {
    bool val = cbm_mcp_get_bool_arg("", "key");
    ASSERT_FALSE(val);
    PASS();
}

TEST(mcp_get_bool_arg_int_value) {
    /* Value is int 1, not bool → should return false */
    bool val = cbm_mcp_get_bool_arg("{\"flag\":1}", "flag");
    ASSERT_FALSE(val);
    PASS();
}

TEST(mcp_get_tool_name_empty_json) {
    char *name = cbm_mcp_get_tool_name("");
    ASSERT_NULL(name);
    PASS();
}

TEST(mcp_get_tool_name_missing_name) {
    char *name = cbm_mcp_get_tool_name("{\"arguments\":{}}");
    ASSERT_NULL(name);
    PASS();
}

TEST(mcp_get_arguments_empty_json) {
    char *args = cbm_mcp_get_arguments("");
    ASSERT_NULL(args);
    PASS();
}

TEST(mcp_get_arguments_no_arguments_key) {
    /* No "arguments" key → returns "{}" */
    char *args = cbm_mcp_get_arguments("{\"name\":\"tool\"}");
    ASSERT_NOT_NULL(args);
    ASSERT_STR_EQ(args, "{}");
    free(args);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  FILE URI PARSING — EDGE CASES
 * ══════════════════════════════════════════════════════════════════ */

TEST(parse_file_uri_http_scheme) {
    char path[256];
    ASSERT_FALSE(cbm_parse_file_uri("http://example.com/path", path, sizeof(path)));
    ASSERT_STR_EQ(path, "");
    PASS();
}

TEST(parse_file_uri_ftp_scheme) {
    char path[256];
    ASSERT_FALSE(cbm_parse_file_uri("ftp://server/file.txt", path, sizeof(path)));
    ASSERT_STR_EQ(path, "");
    PASS();
}

TEST(parse_file_uri_buffer_too_small) {
    char path[5]; /* only 5 bytes — path gets truncated */
    ASSERT_TRUE(cbm_parse_file_uri("file:///usr/local/bin", path, sizeof(path)));
    /* snprintf truncates to 4 chars + NUL */
    ASSERT_EQ(strlen(path), 4);
    ASSERT_STR_EQ(path, "/usr");
    PASS();
}

TEST(parse_file_uri_spaces_in_path) {
    char path[256];
    ASSERT_TRUE(cbm_parse_file_uri("file:///home/user/my%20project", path, sizeof(path)));
    /* Raw percent-encoding is preserved (not decoded) */
    ASSERT_STR_EQ(path, "/home/user/my%20project");
    PASS();
}

TEST(parse_file_uri_null_out_path) {
    /* NULL out_path — should not crash */
    ASSERT_FALSE(cbm_parse_file_uri("file:///tmp", NULL, 256));
    PASS();
}

TEST(parse_file_uri_zero_size) {
    char path[256] = "garbage";
    /* out_size=0 → should fail safely */
    ASSERT_FALSE(cbm_parse_file_uri("file:///tmp", path, 0));
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  SERVER HANDLE — EDGE CASES
 * ══════════════════════════════════════════════════════════════════ */

TEST(server_handle_invalid_json) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    char *resp = cbm_mcp_server_handle(srv, "this is not json at all");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"error\""));
    ASSERT_NOT_NULL(strstr(resp, "-32700")); /* Parse error */
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(server_handle_empty_object) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    /* Valid JSON but no method field → parse error */
    char *resp = cbm_mcp_server_handle(srv, "{}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"error\""));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(server_handle_tools_call_missing_name) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    /* tools/call with no tool name in params */
    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":50,\"method\":\"tools/call\","
                                   "\"params\":{\"arguments\":{}}}");
    ASSERT_NOT_NULL(resp);
    /* Should return error about unknown/missing tool */
    ASSERT_NOT_NULL(strstr(resp, "\"id\":50"));
    ASSERT_TRUE(strstr(resp, "error") || strstr(resp, "isError") || strstr(resp, "unknown"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  POLL/GETLINE FILE* BUFFERING FIX
 * ══════════════════════════════════════════════════════════════════ */

#ifndef _WIN32
#include <unistd.h>
#include <signal.h>

/* Signal handler used by alarm() to abort the test if it hangs */
static void alarm_handler(int sig) {
    (void)sig;
    /* Writing to stderr is async-signal-safe */
    const char msg[] = "FAIL: mcp_server_run_rapid_messages timed out (>5s)\n";
    write(STDERR_FILENO, msg, sizeof(msg) - 1);
    _exit(1);
}

TEST(mcp_server_run_rapid_messages) {
    /* Simulate a client sending initialize + notifications/initialized +
     * tools/list all at once (no delays), which exercises the FILE*
     * buffering fix: the first getline() over-reads kernel data into the
     * libc buffer; without the fix, subsequent poll() calls block for 60s.
     *
     * We use alarm(5) to abort the test process if the server hangs. */
    int fds[2];
    ASSERT_EQ(pipe(fds), 0);

    /* Write all 3 messages to the write end in one shot */
    const char *msgs = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\","
                       "\"params\":{\"protocolVersion\":\"2025-11-25\",\"capabilities\":{}}}\n"
                       "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n"
                       "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\",\"params\":{}}\n";
    ssize_t written = write(fds[1], msgs, strlen(msgs));
    ASSERT_TRUE(written > 0);
    close(fds[1]); /* EOF signals end of input to the server */

    FILE *in_fp = fdopen(fds[0], "r");
    ASSERT_NOT_NULL(in_fp);

    FILE *out_fp = tmpfile();
    ASSERT_NOT_NULL(out_fp);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    /* Install alarm to fail the test if cbm_mcp_server_run blocks */
    signal(SIGALRM, alarm_handler);
    alarm(5);

    int rc = cbm_mcp_server_run(srv, in_fp, out_fp);

    alarm(0); /* cancel alarm */
    signal(SIGALRM, SIG_DFL);

    ASSERT_EQ(rc, 0);

    /* Verify both responses are present:
     *   id:1 — initialize response
     *   id:2 — tools/list response (notifications/initialized produces none)
     * and that the tools list payload is included. */
    rewind(out_fp);
    char buf[4096] = {0};
    size_t nread = fread(buf, 1, sizeof(buf) - 1, out_fp);
    ASSERT_TRUE(nread > 0);
    ASSERT_NOT_NULL(strstr(buf, "\"id\":1"));
    ASSERT_NOT_NULL(strstr(buf, "\"id\":2"));
    ASSERT_NOT_NULL(strstr(buf, "tools"));

    cbm_mcp_server_free(srv);
    fclose(out_fp);
    /* in_fp already EOF; fclose cleans up */
    fclose(in_fp);
    PASS();
}
#endif /* !_WIN32 */

/* Issue #235: passing an unrecognised project name to a tool crashed the
 * binary with a buffer overflow while building the "available_projects"
 * error list — collect_db_project_names overflowed projects[CBM_SZ_4K] via
 * an unsigned underflow on (out_sz - offset) once the listed names exceeded
 * the buffer. Fill a temp cache dir with enough long-named .db files to
 * exceed 4 KB, then hit the bad-project path. Under ASan a regression aborts
 * here; the fixed bounds-check keeps it clean and returns a normal error. */
#define ISSUE235_DBNAME(buf, dir, i)                                                         \
    snprintf((buf), sizeof(buf),                                                             \
             "%s/proj_%02d_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" \
             "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.db",                      \
             (dir), (i))
TEST(tool_bad_project_name_no_overflow_issue235) {
    char cache[256];
    snprintf(cache, sizeof(cache), "/tmp/cbm-badproj-XXXXXX");
    if (!cbm_mkdtemp(cache)) {
        PASS(); /* skip if mkdtemp fails */
    }

    const char *saved = getenv("CBM_CACHE_DIR");
    char *saved_copy = saved ? strdup(saved) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    /* 40 * ~130-char names overflows the 4 KB available-projects buffer. */
    enum { ISSUE235_N = 40 };
    for (int i = 0; i < ISSUE235_N; i++) {
        char name[512];
        ISSUE235_DBNAME(name, cache, i);
        FILE *fp = fopen(name, "w");
        if (fp) {
            fputc('x', fp);
            fclose(fp);
        }
    }

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":"
             "\"search_graph\",\"arguments\":{\"label\":\"Function\","
             "\"project\":\"definitely-not-a-real-project-xyz\"}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "not found"));
    free(resp);
    cbm_mcp_server_free(srv);

    if (saved_copy) {
        cbm_setenv("CBM_CACHE_DIR", saved_copy, 1);
        free(saved_copy);
    } else {
        cbm_unsetenv("CBM_CACHE_DIR");
    }
    for (int i = 0; i < ISSUE235_N; i++) {
        char name[512];
        ISSUE235_DBNAME(name, cache, i);
        cbm_unlink(name);
    }
    cbm_rmdir(cache);
    PASS();
}
#undef ISSUE235_DBNAME

/* ══════════════════════════════════════════════════════════════════
 *  REPO_MAP — P4 token-budgeted, seed-aware query tool
 *  (pai/p4-repo-map-query-tool test plan; pinned ACs: AC2, AC6; AC7 tool
 *  legs in scope). All fixtures use an in-memory store pre-opened via
 *  cbm_mcp_server_set_project so resolve_store's "already open" shortcut
 *  serves the fixture data without touching disk (mirrors
 *  tool_get_architecture_emits_populated_sections above).
 * ══════════════════════════════════════════════════════════════════ */

/* Create an in-memory server with `project` pre-registered (both as the
 * server's current_project — so resolve_store never hits disk — and as a
 * row in the `projects` table, so verify_project_indexed passes). */
static cbm_mcp_server_t *rm_setup_server(const char *project) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    cbm_mcp_server_set_project(srv, project);
    cbm_store_upsert_project(st, project, "/tmp/repo-map-test");
    return srv;
}

/* Upsert a scored Function/Method/Class fixture node. `signature`, when
 * non-NULL, is stored verbatim as the "signature" property (repo_map
 * renders it directly — see rm_render rule documented on handle_repo_map).
 * Returns the node id. */
static int64_t rm_add_node(cbm_store_t *st, const char *project, const char *label,
                           const char *name, const char *qn, const char *file_path,
                           double importance, const char *signature) {
    char props[512];
    if (signature) {
        snprintf(props, sizeof(props), "{\"importance\":%.6f,\"signature\":\"%s\"}", importance,
                 signature);
    } else {
        snprintf(props, sizeof(props), "{\"importance\":%.6f}", importance);
    }
    cbm_node_t n = {0};
    n.project = project;
    n.label = label;
    n.name = name;
    n.qualified_name = qn;
    n.file_path = file_path;
    n.start_line = 1;
    n.end_line = 2;
    n.properties_json = props;
    return cbm_store_upsert_node(st, &n);
}

/* Upsert a Function/Method/Class fixture node with NO "importance" key —
 * the exact shape of every pre-P3 index (spec AC7's score-absence case). */
static int64_t rm_add_node_no_score(cbm_store_t *st, const char *project, const char *label,
                                    const char *name, const char *qn, const char *file_path) {
    cbm_node_t n = {0};
    n.project = project;
    n.label = label;
    n.name = name;
    n.qualified_name = qn;
    n.file_path = file_path;
    n.start_line = 1;
    n.end_line = 2;
    n.properties_json = "{}";
    return cbm_store_upsert_node(st, &n);
}

static void rm_add_edge(cbm_store_t *st, const char *project, int64_t src, int64_t dst,
                        const char *type) {
    cbm_edge_t e = {0};
    e.project = project;
    e.source_id = src;
    e.target_id = dst;
    e.type = type;
    e.properties_json = "{}";
    cbm_store_insert_edge(st, &e);
}

/* Three plain, unconnected symbols with distinct importance — the smallest
 * fixture that has a well-defined global ranking. */
static void rm_add_simple_fixture(cbm_store_t *st, const char *project) {
    rm_add_node(st, project, "Function", "A", "qA", "pkg/a.go", 50.0, "A() error");
    rm_add_node(st, project, "Function", "B", "qB", "pkg/b.go", 40.0, "B() error");
    rm_add_node(st, project, "Function", "C", "qC", "pkg/c.go", 30.0, "C() error");
}

enum { RM_FANOUT_COUNT = 30 };

/* 30 symbols in one file, descending importance, with a real-shaped
 * signature — used to exercise the token-budget binary search (AC2a). */
static void rm_add_budget_fixture(cbm_store_t *st, const char *project) {
    char name[64];
    char qn[96];
    char sig[96];
    for (int i = 0; i < RM_FANOUT_COUNT; i++) {
        snprintf(name, sizeof(name), "f%02d", i);
        snprintf(qn, sizeof(qn), "q.f%02d", i);
        snprintf(sig, sizeof(sig), "f%02d(a, b, c int) (int, error)", i);
        rm_add_node(st, project, "Function", name, qn, "pkg/file.go",
                   (double)(RM_FANOUT_COUNT - i), sig);
    }
}

/* Call repo_map and return the extracted inner JSON text (caller frees). */
static char *rm_call(cbm_mcp_server_t *srv, const char *args_json) {
    char *raw = cbm_mcp_handle_tool(srv, "repo_map", args_json);
    char *text = extract_text_content(raw);
    free(raw);
    return text;
}

/* Read an integer field out of a repo_map response's inner JSON text. -1 if absent. */
static long rm_json_int(const char *json, const char *key) {
    if (!json) {
        return -1;
    }
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    if (!doc) {
        return -1;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *val = root ? yyjson_obj_get(root, key) : NULL;
    long result = (val && yyjson_is_int(val)) ? (long)yyjson_get_int(val) : -1;
    yyjson_doc_free(doc);
    return result;
}

/* Read a string field out of a repo_map response's inner JSON text (caller frees). NULL if absent. */
static char *rm_json_str(const char *json, const char *key) {
    if (!json) {
        return NULL;
    }
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    if (!doc) {
        return NULL;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *val = root ? yyjson_obj_get(root, key) : NULL;
    char *result = (val && yyjson_is_str(val)) ? strdup(yyjson_get_str(val)) : NULL;
    yyjson_doc_free(doc);
    return result;
}

/* ── Row 1: registration + dispatch ──────────────────────────────── */

TEST(repo_map_registered_in_tools_list) {
    char *json = cbm_mcp_tools_list();
    ASSERT_NOT_NULL(json);
    const char *p = strstr(json, "\"repo_map\"");
    ASSERT_NOT_NULL(p);
    ASSERT_NOT_NULL(strstr(p, "\"project\""));
    ASSERT_NOT_NULL(strstr(p, "\"seed_anchors\""));
    ASSERT_NOT_NULL(strstr(p, "\"token_budget\""));
    free(json);
    PASS();
}

TEST(repo_map_dispatchable_via_full_jsonrpc) {
    cbm_mcp_server_t *srv = rm_setup_server("rm-dispatch");
    cbm_store_t *st = cbm_mcp_server_store(srv);
    rm_add_node(st, "rm-dispatch", "Function", "Foo", "rm-dispatch.Foo", "pkg/foo.go", 5.0,
               "Foo() error");

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":500,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"repo_map\",\"arguments\":{\"project\":\"rm-dispatch\"}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"id\":500"));
    ASSERT_NULL(strstr(resp, "\"isError\":true"));
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "\"map\""));
    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ── Row 2: AC2a token-budget fit ────────────────────────────────── */

TEST(repo_map_budget_default_applies_when_absent) {
    cbm_mcp_server_t *srv = rm_setup_server("rm-budget-default");
    cbm_store_t *st = cbm_mcp_server_store(srv);
    rm_add_budget_fixture(st, "rm-budget-default");

    char *text = rm_call(srv, "{\"project\":\"rm-budget-default\"}");
    ASSERT_NOT_NULL(text);
    ASSERT_EQ(rm_json_int(text, "budget"), 1600);
    long est = rm_json_int(text, "estimated_tokens");
    ASSERT_TRUE(est >= 0 && est <= 1600);
    free(text);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(repo_map_budget_fits_and_uses_available_space) {
    cbm_mcp_server_t *srv = rm_setup_server("rm-budget-tight");
    cbm_store_t *st = cbm_mcp_server_store(srv);
    rm_add_budget_fixture(st, "rm-budget-tight");

    char *text = rm_call(srv, "{\"project\":\"rm-budget-tight\",\"token_budget\":100}");
    ASSERT_NOT_NULL(text);
    long est = rm_json_int(text, "estimated_tokens");
    long cnt = rm_json_int(text, "symbol_count");
    ASSERT_TRUE(est >= 0 && est <= 100);
    /* Content is plentiful (30 lines >> budget) — the binary search must
     * converge UP to close to the ceiling, not truncate to a tiny result. */
    ASSERT_TRUE(est >= 50);
    ASSERT_TRUE(cnt > 0 && cnt < RM_FANOUT_COUNT);
    free(text);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(repo_map_budget_tiny_no_overshoot_no_hang) {
    cbm_mcp_server_t *srv = rm_setup_server("rm-budget-tiny");
    cbm_store_t *st = cbm_mcp_server_store(srv);
    rm_add_budget_fixture(st, "rm-budget-tiny");

    char *text = rm_call(srv, "{\"project\":\"rm-budget-tiny\",\"token_budget\":64}");
    ASSERT_NOT_NULL(text);
    long est = rm_json_int(text, "estimated_tokens");
    long cnt = rm_json_int(text, "symbol_count");
    ASSERT_TRUE(est >= 0 && est <= 64);
    ASSERT_TRUE(cnt >= 0 && cnt < RM_FANOUT_COUNT);
    free(text);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(repo_map_budget_larger_than_whole_map_returns_everything) {
    cbm_mcp_server_t *srv = rm_setup_server("rm-budget-huge");
    cbm_store_t *st = cbm_mcp_server_store(srv);
    rm_add_budget_fixture(st, "rm-budget-huge");

    char *text = rm_call(srv, "{\"project\":\"rm-budget-huge\",\"token_budget\":100000}");
    ASSERT_NOT_NULL(text);
    ASSERT_EQ(rm_json_int(text, "symbol_count"), RM_FANOUT_COUNT);
    ASSERT_NOT_NULL(strstr(text, "f00("));
    ASSERT_NOT_NULL(strstr(text, "f29("));
    free(text);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(repo_map_budget_zero_or_negative_is_input_error) {
    cbm_mcp_server_t *srv = rm_setup_server("rm-budget-bad");
    cbm_store_t *st = cbm_mcp_server_store(srv);
    rm_add_budget_fixture(st, "rm-budget-bad");

    char *raw = cbm_mcp_handle_tool(srv, "repo_map",
                                    "{\"project\":\"rm-budget-bad\",\"token_budget\":0}");
    ASSERT_NOT_NULL(raw);
    ASSERT_NOT_NULL(strstr(raw, "\"isError\":true"));
    /* Discriminating: must be the tool's own budget validation, not the
     * dispatch-level "unknown tool" error (red-boundary trivial-pass guard). */
    ASSERT_NOT_NULL(strstr(raw, "token_budget"));
    ASSERT_NULL(strstr(raw, "unknown tool"));
    free(raw);

    raw = cbm_mcp_handle_tool(srv, "repo_map",
                              "{\"project\":\"rm-budget-bad\",\"token_budget\":-5}");
    ASSERT_NOT_NULL(raw);
    ASSERT_NOT_NULL(strstr(raw, "\"isError\":true"));
    ASSERT_NOT_NULL(strstr(raw, "token_budget"));
    ASSERT_NULL(strstr(raw, "unknown tool"));
    free(raw);

    cbm_mcp_server_free(srv);
    PASS();
}

/* ── Row 3: AC2b seed-boost ranking ──────────────────────────────── */

/* Two clusters: a HIGH-raw-importance "distant" trio with no relation to the
 * seed, and a MODEST-raw-importance "seed" cluster (S + 4 CALLS/USAGE
 * neighbours — deliberately > REPO_MAP_WEAK_NEIGHBOR_THRESHOLD so the widen
 * path (row 4) does not fire here). */
static void rm_add_seed_boost_fixture(cbm_store_t *st, const char *project) {
    rm_add_node(st, project, "Function", "D0", "proj.D0", "pkg/distant.go", 100.0, "D0() error");
    rm_add_node(st, project, "Function", "D1", "proj.D1", "pkg/distant.go", 90.0, "D1() error");
    rm_add_node(st, project, "Function", "D2", "proj.D2", "pkg/distant.go", 80.0, "D2() error");

    int64_t s = rm_add_node(st, project, "Function", "S", "proj.S", "pkg/seed.go", 5.0,
                            "S() error");
    int64_t n1 =
        rm_add_node(st, project, "Function", "N1", "proj.N1", "pkg/seed_n.go", 4.0, "N1() error");
    int64_t n2 =
        rm_add_node(st, project, "Function", "N2", "proj.N2", "pkg/seed_n.go", 4.0, "N2() error");
    int64_t n3 =
        rm_add_node(st, project, "Function", "N3", "proj.N3", "pkg/seed_n.go", 3.0, "N3() error");
    int64_t n4 =
        rm_add_node(st, project, "Function", "N4", "proj.N4", "pkg/seed_n.go", 3.0, "N4() error");

    rm_add_edge(st, project, s, n1, "CALLS");
    rm_add_edge(st, project, s, n2, "CALLS");
    rm_add_edge(st, project, n3, s, "CALLS"); /* inbound direction too */
    rm_add_edge(st, project, s, n4, "USAGE");
}

TEST(repo_map_seed_boost_ranks_neighbourhood_above_distant) {
    cbm_mcp_server_t *srv = rm_setup_server("rm-seed-boost");
    cbm_store_t *st = cbm_mcp_server_store(srv);
    rm_add_seed_boost_fixture(st, "rm-seed-boost");

    /* Seeded: S's neighbourhood must outrank the distant high-importance cluster. */
    char *seeded =
        rm_call(srv, "{\"project\":\"rm-seed-boost\",\"seed_anchors\":[\"S\"],"
                     "\"token_budget\":100000}");
    ASSERT_NOT_NULL(seeded);
    const char *s_pos = strstr(seeded, "S() error");
    const char *d0_pos = strstr(seeded, "D0() error");
    ASSERT_NOT_NULL(s_pos);
    ASSERT_NOT_NULL(d0_pos);
    ASSERT_TRUE(s_pos < d0_pos);

    /* No seeds: inversion — raw importance dominates, distant wins. */
    char *global = rm_call(srv, "{\"project\":\"rm-seed-boost\",\"token_budget\":100000}");
    ASSERT_NOT_NULL(global);
    const char *g_s_pos = strstr(global, "S() error");
    const char *g_d0_pos = strstr(global, "D0() error");
    ASSERT_NOT_NULL(g_s_pos);
    ASSERT_NOT_NULL(g_d0_pos);
    ASSERT_TRUE(g_d0_pos < g_s_pos);

    free(seeded);
    free(global);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(repo_map_tight_budget_seed_crowds_out_distant) {
    cbm_mcp_server_t *srv = rm_setup_server("rm-seed-tight");
    cbm_store_t *st = cbm_mcp_server_store(srv);
    rm_add_seed_boost_fixture(st, "rm-seed-tight");

    /* Budget fits only the top ~2 lines of the seeded ranking — since the
     * whole 5-member seed cluster outranks the distant trio, none of the
     * distant symbols can appear at all. */
    char *text =
        rm_call(srv, "{\"project\":\"rm-seed-tight\",\"seed_anchors\":[\"S\"],"
                     "\"token_budget\":15}");
    ASSERT_NOT_NULL(text);
    /* Positive discriminator first: the top seeded symbol IS in the map
     * (guards the red-boundary trivial pass where an error response also
     * "contains no D0"). */
    ASSERT_NOT_NULL(strstr(text, "S() error"));
    ASSERT_NULL(strstr(text, "D0() error"));
    ASSERT_NULL(strstr(text, "D1() error"));
    ASSERT_NULL(strstr(text, "D2() error"));
    free(text);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(repo_map_seed_by_file_path) {
    cbm_mcp_server_t *srv = rm_setup_server("rm-seed-file");
    cbm_store_t *st = cbm_mcp_server_store(srv);
    rm_add_seed_boost_fixture(st, "rm-seed-file");

    char *text = rm_call(srv, "{\"project\":\"rm-seed-file\",\"seed_anchors\":[\"pkg/seed.go\"],"
                             "\"token_budget\":100000}");
    ASSERT_NOT_NULL(text);
    const char *s_pos = strstr(text, "S() error");
    const char *d0_pos = strstr(text, "D0() error");
    ASSERT_NOT_NULL(s_pos);
    ASSERT_NOT_NULL(d0_pos);
    ASSERT_TRUE(s_pos < d0_pos);
    ASSERT_NOT_NULL(strstr(text, "\"mode\":\"seeded\""));
    free(text);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(repo_map_seed_resolves_multiple_nodes) {
    cbm_mcp_server_t *srv = rm_setup_server("rm-seed-multi");
    cbm_store_t *st = cbm_mcp_server_store(srv);
    const char *project = "rm-seed-multi";

    rm_add_node(st, project, "Function", "Hi", "proj.pkgA.Hi", "pkg/a.go", 50.0, "Hi() error");
    rm_add_node(st, project, "Function", "Dup", "proj.pkgB.Dup", "pkg/b.go", 2.0, "DupB() error");
    rm_add_node(st, project, "Function", "Dup", "proj.pkgC.Dup", "pkg/c.go", 2.0, "DupC() error");

    char *text = rm_call(srv, "{\"project\":\"rm-seed-multi\",\"seed_anchors\":[\"Dup\"],"
                             "\"token_budget\":100000}");
    ASSERT_NOT_NULL(text);
    ASSERT_NOT_NULL(strstr(text, "\"seed_anchors_resolved\":1"));
    const char *dupb_pos = strstr(text, "DupB() error");
    const char *dupc_pos = strstr(text, "DupC() error");
    const char *hi_pos = strstr(text, "Hi() error");
    ASSERT_NOT_NULL(dupb_pos);
    ASSERT_NOT_NULL(dupc_pos);
    ASSERT_NOT_NULL(hi_pos);
    /* Both nodes resolved by the shared name "Dup" are boosted (2*50=100)
     * above the unrelated higher-raw-importance "Hi" (50). */
    ASSERT_TRUE(dupb_pos < hi_pos);
    ASSERT_TRUE(dupc_pos < hi_pos);
    free(text);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ── Row 4: AC2b sharpening — weak-seed widen ────────────────────── */

TEST(repo_map_weak_seed_triggers_widen_walk) {
    /* Builder's chosen widen rule (also documented on handle_repo_map in
     * mcp.c): when a seed's 1-hop CALLS|USAGE neighbourhood has
     * <= REPO_MAP_WEAK_NEIGHBOR_THRESHOLD (3) members, widen via (a)
     * file-of-symbol seeding — every other symbol defined in the seed's own
     * file — and (b) one more hop from the 1-hop neighbours (2-hop
     * expansion). Both widened sets get REPO_MAP_WIDEN_BOOST (25x) vs the
     * direct seed/1-hop REPO_MAP_SEED_BOOST (50x). Here W has exactly ONE
     * 1-hop neighbour (W1), so widen fires. */
    cbm_mcp_server_t *srv = rm_setup_server("rm-weak-seed");
    cbm_store_t *st = cbm_mcp_server_store(srv);
    const char *project = "rm-weak-seed";

    int64_t w = rm_add_node(st, project, "Function", "W", "rm-weak-seed.W", "pkg/weak.go", 1.0,
                            "W() error");
    int64_t w1 = rm_add_node(st, project, "Function", "W1", "rm-weak-seed.W1", "pkg/other.go",
                             1.0, "W1() error");
    rm_add_node(st, project, "Function", "WSibling", "rm-weak-seed.WSibling", "pkg/weak.go", 1.0,
               "WSibling() error");
    int64_t w1a = rm_add_node(st, project, "Function", "W1a", "rm-weak-seed.W1a",
                              "pkg/other2.go", 1.0, "W1a() error");
    /* Raw importance between the widen boost (1*25=25) and the strong seed
     * boost (1*50=50) — proves the widen-boosted symbols outrank a *higher*
     * raw-importance unrelated symbol, not just "small graph, all fits". */
    rm_add_node(st, project, "Function", "Filler", "rm-weak-seed.Filler", "pkg/filler.go", 10.0,
               "Filler() error");

    rm_add_edge(st, project, w, w1, "CALLS");
    rm_add_edge(st, project, w1, w1a, "CALLS");

    char *text = rm_call(srv, "{\"project\":\"rm-weak-seed\",\"seed_anchors\":[\"W\"],"
                             "\"token_budget\":100000}");
    ASSERT_NOT_NULL(text);
    const char *wsib_pos = strstr(text, "WSibling() error");
    const char *w1a_pos = strstr(text, "W1a() error");
    const char *filler_pos = strstr(text, "Filler() error");
    ASSERT_NOT_NULL(wsib_pos); /* file-of-symbol widen member present */
    ASSERT_NOT_NULL(w1a_pos);  /* 2-hop widen member present */
    ASSERT_NOT_NULL(filler_pos);
    ASSERT_TRUE(wsib_pos < filler_pos);
    ASSERT_TRUE(w1a_pos < filler_pos);
    free(text);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ── Row 5: AC2c empty/unusable seeds → global map ───────────────── */

TEST(repo_map_no_seed_and_empty_seed_and_all_unresolvable_yield_identical_global_map) {
    cbm_mcp_server_t *srv = rm_setup_server("rm-empty-seed");
    cbm_store_t *st = cbm_mcp_server_store(srv);
    rm_add_simple_fixture(st, "rm-empty-seed");

    char *no_seed = rm_call(srv, "{\"project\":\"rm-empty-seed\",\"token_budget\":100000}");
    char *empty_arr = rm_call(
        srv, "{\"project\":\"rm-empty-seed\",\"seed_anchors\":[],\"token_budget\":100000}");
    char *all_unresolvable =
        rm_call(srv, "{\"project\":\"rm-empty-seed\",\"seed_anchors\":[\"nonexistent_xyz\"],"
                     "\"token_budget\":100000}");
    ASSERT_NOT_NULL(no_seed);
    ASSERT_NOT_NULL(empty_arr);
    ASSERT_NOT_NULL(all_unresolvable);

    char *no_seed_map = rm_json_str(no_seed, "map");
    char *empty_map = rm_json_str(empty_arr, "map");
    char *unresolvable_map = rm_json_str(all_unresolvable, "map");
    ASSERT_NOT_NULL(no_seed_map);
    ASSERT_NOT_NULL(empty_map);
    ASSERT_NOT_NULL(unresolvable_map);
    ASSERT_STR_EQ(no_seed_map, empty_map);
    ASSERT_STR_EQ(no_seed_map, unresolvable_map);

    ASSERT_NOT_NULL(strstr(no_seed, "\"mode\":\"global\""));
    ASSERT_NOT_NULL(strstr(empty_arr, "\"mode\":\"global\""));
    ASSERT_NOT_NULL(strstr(all_unresolvable, "\"mode\":\"global\""));

    free(no_seed_map);
    free(empty_map);
    free(unresolvable_map);
    free(no_seed);
    free(empty_arr);
    free(all_unresolvable);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(repo_map_mixed_resolvable_and_unresolvable_seeds_uses_seeded_mode) {
    cbm_mcp_server_t *srv = rm_setup_server("rm-mixed-seed");
    cbm_store_t *st = cbm_mcp_server_store(srv);
    rm_add_simple_fixture(st, "rm-mixed-seed");

    char *text =
        rm_call(srv, "{\"project\":\"rm-mixed-seed\",\"seed_anchors\":[\"nonexistent_xyz\","
                     "\"A\"],\"token_budget\":100000}");
    ASSERT_NOT_NULL(text);
    ASSERT_NOT_NULL(strstr(text, "\"mode\":\"seeded\""));
    ASSERT_NOT_NULL(strstr(text, "\"seed_anchors_requested\":2"));
    ASSERT_NOT_NULL(strstr(text, "\"seed_anchors_resolved\":1"));
    free(text);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ── Row 8: spec AC7 score-absence gating ────────────────────────── */

TEST(repo_map_unscored_project_returns_explicit_gate_error) {
    cbm_mcp_server_t *srv = rm_setup_server("rm-unscored");
    cbm_store_t *st = cbm_mcp_server_store(srv);
    rm_add_node_no_score(st, "rm-unscored", "Function", "Foo", "qFoo", "pkg/foo.go");
    rm_add_node_no_score(st, "rm-unscored", "Function", "Bar", "qBar", "pkg/bar.go");

    char *raw = cbm_mcp_handle_tool(srv, "repo_map", "{\"project\":\"rm-unscored\"}");
    ASSERT_NOT_NULL(raw);
    ASSERT_NOT_NULL(strstr(raw, "\"isError\":true"));
    char *inner = extract_text_content(raw);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "unscored"));
    free(inner);
    free(raw);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(repo_map_partial_score_population_is_not_gated) {
    /* One Function with no importance (pre-P3 leftover) alongside one Class
     * WITH a persisted score — spec AC7's "partial population" sub-case.
     * Documented behaviour: the gate fires only when NO node is scored at
     * all; a partially-scored project proceeds (unscored nodes rank as 0 —
     * not a crash, not a silently-unranked map). */
    cbm_mcp_server_t *srv = rm_setup_server("rm-partial-score");
    cbm_store_t *st = cbm_mcp_server_store(srv);
    rm_add_node_no_score(st, "rm-partial-score", "Function", "Unscored", "qU", "pkg/u.go");
    rm_add_node(st, "rm-partial-score", "Class", "Scored", "qS", "pkg/s.go", 10.0, "Scored()");

    char *raw = cbm_mcp_handle_tool(srv, "repo_map", "{\"project\":\"rm-partial-score\"}");
    ASSERT_NOT_NULL(raw);
    ASSERT_NULL(strstr(raw, "\"isError\":true"));
    char *inner = extract_text_content(raw);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "Scored()"));
    free(inner);
    free(raw);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ── Row 9: spec AC7 graceful input/error paths ──────────────────── */

TEST(repo_map_missing_project_is_error_no_crash) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    char *raw = cbm_mcp_handle_tool(srv, "repo_map", "{}");
    ASSERT_NOT_NULL(raw);
    ASSERT_NOT_NULL(strstr(raw, "\"isError\":true"));
    /* Discriminating: must be the tool's own project-missing error, not
     * dispatch-level "unknown tool" (red-boundary trivial-pass guard). */
    ASSERT_NULL(strstr(raw, "unknown tool"));
    free(raw);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(repo_map_unknown_project_require_store_error) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    char *raw = cbm_mcp_handle_tool(srv, "repo_map", "{\"project\":\"totally-unknown-xyz\"}");
    ASSERT_NOT_NULL(raw);
    ASSERT_TRUE(strstr(raw, "not found") != NULL || strstr(raw, "not indexed") != NULL);
    free(raw);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(repo_map_indexed_false_project_verify_indexed_error) {
    /* current_project pre-set (resolve_store's cache shortcut returns a
     * non-NULL store) but no row was ever upserted into `projects` for this
     * name — exercises verify_project_indexed's error path specifically,
     * distinct from REQUIRE_STORE's file-not-found path above. */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    cbm_mcp_server_set_project(srv, "ghost-project");

    char *raw = cbm_mcp_handle_tool(srv, "repo_map", "{\"project\":\"ghost-project\"}");
    ASSERT_NOT_NULL(raw);
    ASSERT_NOT_NULL(strstr(raw, "not indexed"));
    free(raw);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(repo_map_malformed_seed_anchors_string_coerced) {
    cbm_mcp_server_t *srv = rm_setup_server("rm-malformed-1");
    cbm_store_t *st = cbm_mcp_server_store(srv);
    rm_add_simple_fixture(st, "rm-malformed-1");

    /* seed_anchors as a bare string instead of an array — coerced into a
     * single-element list (documented leniency, not an error). */
    char *raw = cbm_mcp_handle_tool(srv, "repo_map",
                                    "{\"project\":\"rm-malformed-1\",\"seed_anchors\":\"A\"}");
    ASSERT_NOT_NULL(raw);
    ASSERT_NULL(strstr(raw, "\"isError\":true"));
    char *inner = extract_text_content(raw);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "\"mode\":\"seeded\""));
    free(inner);
    free(raw);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(repo_map_malformed_seed_anchors_non_string_elements_skipped) {
    cbm_mcp_server_t *srv = rm_setup_server("rm-malformed-2");
    cbm_store_t *st = cbm_mcp_server_store(srv);
    rm_add_simple_fixture(st, "rm-malformed-2");

    char *raw = cbm_mcp_handle_tool(
        srv, "repo_map", "{\"project\":\"rm-malformed-2\",\"seed_anchors\":[1,2,\"A\"]}");
    ASSERT_NOT_NULL(raw);
    ASSERT_NULL(strstr(raw, "\"isError\":true"));
    char *inner = extract_text_content(raw);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "\"mode\":\"seeded\""));
    ASSERT_NOT_NULL(strstr(inner, "\"seed_anchors_resolved\":1"));
    free(inner);
    free(raw);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(repo_map_absurdly_long_seed_list_capped_no_hang) {
    cbm_mcp_server_t *srv = rm_setup_server("rm-malformed-3");
    cbm_store_t *st = cbm_mcp_server_store(srv);
    rm_add_simple_fixture(st, "rm-malformed-3");

    /* 200 bogus seed names — well past REPO_MAP_MAX_SEEDS (50). Must not
     * hang or crash; excess entries are silently dropped. */
    char buf[4096] = "{\"project\":\"rm-malformed-3\",\"seed_anchors\":[";
    size_t pos = strlen(buf);
    for (int i = 0; i < 200; i++) {
        char frag[32];
        int n = snprintf(frag, sizeof(frag), "%s\"bogus%d\"", i > 0 ? "," : "", i);
        if (n < 0 || pos + (size_t)n >= sizeof(buf) - 4) {
            break;
        }
        memcpy(buf + pos, frag, (size_t)n);
        pos += (size_t)n;
    }
    memcpy(buf + pos, "]}", 3);

    char *raw = cbm_mcp_handle_tool(srv, "repo_map", buf);
    ASSERT_NOT_NULL(raw);
    ASSERT_NULL(strstr(raw, "\"isError\":true"));
    free(raw);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ── Row 10: signature-level rendering, no bodies ────────────────── */

TEST(repo_map_renders_signature_level_no_body_leak) {
    cbm_mcp_server_t *srv = rm_setup_server("rm-render");
    cbm_store_t *st = cbm_mcp_server_store(srv);
    const char *project = "rm-render";

    cbm_node_t n = {0};
    n.project = project;
    n.label = "Function";
    n.name = "Foo";
    n.qualified_name = "rm-render.Foo";
    n.file_path = "pkg/foo.go";
    n.start_line = 1;
    n.end_line = 5;
    n.properties_json = "{\"importance\":10.0,\"signature\":\"Foo(x int) error\","
                        "\"body_preview\":\"BODY_MARKER_DO_NOT_LEAK do_something()\"}";
    ASSERT_GT(cbm_store_upsert_node(st, &n), 0);

    char *text = rm_call(srv, "{\"project\":\"rm-render\"}");
    ASSERT_NOT_NULL(text);
    ASSERT_NOT_NULL(strstr(text, "pkg/foo.go: Foo(x int) error"));
    ASSERT_NULL(strstr(text, "BODY_MARKER_DO_NOT_LEAK"));
    free(text);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ── Row 11: determinism ──────────────────────────────────────────── */

TEST(repo_map_deterministic_byte_identical_across_calls) {
    cbm_mcp_server_t *srv = rm_setup_server("rm-determinism");
    cbm_store_t *st = cbm_mcp_server_store(srv);
    const char *project = "rm-determinism";
    /* Two tied scores force the tie-break rule (qualified_name ASC) to do
     * the work — the case where sort stability actually matters. */
    rm_add_node(st, project, "Function", "Tie1", "qTie1", "pkg/x.go", 5.0, "Tie1() error");
    rm_add_node(st, project, "Function", "Tie2", "qTie2", "pkg/x.go", 5.0, "Tie2() error");
    rm_add_node(st, project, "Function", "Other", "qOther", "pkg/y.go", 3.0, "Other() error");

    char *first = rm_call(srv, "{\"project\":\"rm-determinism\",\"token_budget\":100000}");
    char *second = rm_call(srv, "{\"project\":\"rm-determinism\",\"token_budget\":100000}");
    ASSERT_NOT_NULL(first);
    ASSERT_NOT_NULL(second);
    /* Positive discriminator: a real map with the tied pair rendered in
     * tie-break order (guards the red-boundary trivial pass where two
     * identical error strings compare equal). */
    const char *tie1_pos = strstr(first, "Tie1() error");
    const char *tie2_pos = strstr(first, "Tie2() error");
    ASSERT_NOT_NULL(tie1_pos);
    ASSERT_NOT_NULL(tie2_pos);
    ASSERT_TRUE(tie1_pos < tie2_pos); /* qualified_name ASC: qTie1 < qTie2 */
    ASSERT_STR_EQ(first, second);
    free(first);
    free(second);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ── Row 12: no cross-project leakage ────────────────────────────── */

TEST(repo_map_no_cross_project_leakage) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    cbm_store_upsert_project(st, "projA", "/tmp/projA");
    cbm_store_upsert_project(st, "projB", "/tmp/projB");
    rm_add_node(st, "projA", "Function", "FnA", "projA.FnA", "pkg/a.go", 10.0, "FnA() error");
    rm_add_node(st, "projB", "Function", "FnB", "projB.FnB", "pkg/b.go", 10.0, "FnB() error");

    cbm_mcp_server_set_project(srv, "projA");
    char *text = rm_call(srv, "{\"project\":\"projA\",\"token_budget\":100000}");
    ASSERT_NOT_NULL(text);
    ASSERT_NOT_NULL(strstr(text, "FnA() error"));
    ASSERT_NULL(strstr(text, "FnB() error"));
    ASSERT_NULL(strstr(text, "pkg/b.go"));
    free(text);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ── Row 13: memory/lifecycle hygiene ─────────────────────────────── */

TEST(repo_map_repeated_calls_stable_no_leak_surface) {
    cbm_mcp_server_t *srv = rm_setup_server("rm-repeat");
    cbm_store_t *st = cbm_mcp_server_store(srv);
    rm_add_simple_fixture(st, "rm-repeat");

    char *baseline = rm_call(srv, "{\"project\":\"rm-repeat\",\"token_budget\":100000}");
    ASSERT_NOT_NULL(baseline);
    /* Positive discriminator: baseline is a real map, not a repeated
     * identical error response (red-boundary trivial-pass guard). */
    ASSERT_NOT_NULL(strstr(baseline, "A() error"));
    for (int i = 0; i < 20; i++) {
        char *text = rm_call(srv, "{\"project\":\"rm-repeat\",\"token_budget\":100000}");
        ASSERT_NOT_NULL(text);
        ASSERT_STR_EQ(baseline, text);
        free(text);
    }
    ASSERT_TRUE(cbm_mcp_server_has_cached_store(srv));
    free(baseline);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  SUITE
 * ══════════════════════════════════════════════════════════════════ */

SUITE(mcp) {
    /* JSON-RPC parsing */
    RUN_TEST(jsonrpc_parse_request);
    RUN_TEST(jsonrpc_parse_notification);
    RUN_TEST(jsonrpc_parse_invalid);
    RUN_TEST(jsonrpc_parse_tools_call);
    RUN_TEST(jsonrpc_parse_string_id_issue253);
    RUN_TEST(jsonrpc_format_response_string_id_issue253);

    /* JSON-RPC parsing — edge cases */
    RUN_TEST(jsonrpc_parse_empty_string);
    RUN_TEST(jsonrpc_parse_missing_jsonrpc_field);
    RUN_TEST(jsonrpc_parse_missing_method);
    RUN_TEST(jsonrpc_parse_string_id);
    RUN_TEST(jsonrpc_parse_no_params);
    RUN_TEST(jsonrpc_parse_extra_whitespace);
    RUN_TEST(jsonrpc_parse_array_not_object);

    /* JSON-RPC formatting */
    RUN_TEST(jsonrpc_format_response);
    RUN_TEST(jsonrpc_format_error);

    /* MCP protocol helpers */
    RUN_TEST(mcp_initialize_response);
    RUN_TEST(mcp_tools_list);
    RUN_TEST(mcp_tools_array_schemas_have_items);
    RUN_TEST(mcp_text_result);
    RUN_TEST(mcp_text_result_error);

    /* Argument extraction */
    RUN_TEST(mcp_get_tool_name);
    RUN_TEST(mcp_get_arguments);
    RUN_TEST(mcp_get_string_arg);
    RUN_TEST(mcp_get_int_arg);
    RUN_TEST(mcp_get_bool_arg);

    /* Argument extraction — edge cases */
    RUN_TEST(mcp_get_string_arg_empty_json);
    RUN_TEST(mcp_get_string_arg_empty_object);
    RUN_TEST(mcp_get_string_arg_nested_value);
    RUN_TEST(mcp_get_string_arg_int_value);
    RUN_TEST(mcp_get_int_arg_empty_json);
    RUN_TEST(mcp_get_int_arg_string_value);
    RUN_TEST(mcp_get_int_arg_bool_value);
    RUN_TEST(mcp_get_bool_arg_empty_json);
    RUN_TEST(mcp_get_bool_arg_int_value);
    RUN_TEST(mcp_get_tool_name_empty_json);
    RUN_TEST(mcp_get_tool_name_missing_name);
    RUN_TEST(mcp_get_arguments_empty_json);
    RUN_TEST(mcp_get_arguments_no_arguments_key);

    /* Server protocol handling */
    RUN_TEST(server_handle_initialize);
    RUN_TEST(server_handle_initialized_notification);
    RUN_TEST(server_handle_tools_list);
    RUN_TEST(server_handle_unknown_method);

    /* Server handle — edge cases */
    RUN_TEST(server_handle_invalid_json);
    RUN_TEST(server_handle_empty_object);
    RUN_TEST(server_handle_tools_call_missing_name);

    /* Tool handlers */
    RUN_TEST(tool_list_projects_empty);
    RUN_TEST(tool_get_graph_schema_empty);
    RUN_TEST(tool_unknown_tool);
    RUN_TEST(tool_search_graph_basic);
    RUN_TEST(tool_search_graph_includes_node_properties);
    RUN_TEST(tool_query_graph_basic);
    RUN_TEST(tool_index_status_no_project);

    /* Tool handlers with validation */
    RUN_TEST(tool_trace_call_path_not_found);
    RUN_TEST(tool_trace_missing_function_name);
    RUN_TEST(tool_delete_project_not_found);
    RUN_TEST(tool_get_architecture_empty);
    RUN_TEST(tool_get_architecture_emits_populated_sections);
    RUN_TEST(tool_query_graph_missing_query);

    /* Pipeline-dependent tool handlers */
    RUN_TEST(tool_index_repository_missing_path);
    RUN_TEST(tool_get_code_snippet_missing_qn);
    RUN_TEST(tool_get_code_snippet_not_found);
    RUN_TEST(tool_search_code_missing_pattern);
    RUN_TEST(tool_search_code_no_project);
    RUN_TEST(search_code_multi_word);
    RUN_TEST(search_code_invalid_regex_errors_issue283);
    RUN_TEST(search_code_literal_pipe_warns_issue282);
    RUN_TEST(search_code_ampersand_accepted_issue272);
    RUN_TEST(tool_detect_changes_no_project);
    RUN_TEST(tool_manage_adr_no_project);
    RUN_TEST(tool_manage_adr_get_with_existing_adr);
    RUN_TEST(tool_manage_adr_unified_backend_issue256);
    RUN_TEST(tool_ingest_traces_basic);
    RUN_TEST(tool_ingest_traces_empty);

    /* Idle store eviction */
    RUN_TEST(store_idle_eviction);
    RUN_TEST(store_idle_no_eviction_within_timeout);
    RUN_TEST(store_idle_evict_protects_initial_store);
    RUN_TEST(store_idle_evict_access_resets_timer);

    /* URI helpers */
    RUN_TEST(parse_file_uri_unix);
    RUN_TEST(parse_file_uri_windows);
    RUN_TEST(parse_file_uri_invalid);

    /* URI helpers — edge cases */
    RUN_TEST(parse_file_uri_http_scheme);
    RUN_TEST(parse_file_uri_ftp_scheme);
    RUN_TEST(parse_file_uri_buffer_too_small);
    RUN_TEST(parse_file_uri_spaces_in_path);
    RUN_TEST(parse_file_uri_null_out_path);
    RUN_TEST(parse_file_uri_zero_size);

    /* Poll/getline FILE* buffering fix */
#ifndef _WIN32
    RUN_TEST(mcp_server_run_rapid_messages);
#endif

    /* Snippet resolution (port of snippet_test.go) */
    RUN_TEST(snippet_exact_qn);
    RUN_TEST(snippet_qn_suffix);
    RUN_TEST(snippet_unique_short_name);
    RUN_TEST(snippet_name_tier);
    RUN_TEST(snippet_ambiguous_short_name);
    RUN_TEST(snippet_not_found);
    RUN_TEST(snippet_fuzzy_suggestions);
    RUN_TEST(snippet_enriched_properties);
    RUN_TEST(snippet_fuzzy_last_segment);
    RUN_TEST(snippet_auto_resolve_default);
    RUN_TEST(snippet_auto_resolve_enabled);
    RUN_TEST(snippet_include_neighbors_default);
    RUN_TEST(snippet_include_neighbors_enabled);
    RUN_TEST(tool_bad_project_name_no_overflow_issue235);

    /* repo_map (P4 token-budgeted, seed-aware query tool) */
    RUN_TEST(repo_map_registered_in_tools_list);
    RUN_TEST(repo_map_dispatchable_via_full_jsonrpc);
    RUN_TEST(repo_map_budget_default_applies_when_absent);
    RUN_TEST(repo_map_budget_fits_and_uses_available_space);
    RUN_TEST(repo_map_budget_tiny_no_overshoot_no_hang);
    RUN_TEST(repo_map_budget_larger_than_whole_map_returns_everything);
    RUN_TEST(repo_map_budget_zero_or_negative_is_input_error);
    RUN_TEST(repo_map_seed_boost_ranks_neighbourhood_above_distant);
    RUN_TEST(repo_map_tight_budget_seed_crowds_out_distant);
    RUN_TEST(repo_map_seed_by_file_path);
    RUN_TEST(repo_map_seed_resolves_multiple_nodes);
    RUN_TEST(repo_map_weak_seed_triggers_widen_walk);
    RUN_TEST(repo_map_no_seed_and_empty_seed_and_all_unresolvable_yield_identical_global_map);
    RUN_TEST(repo_map_mixed_resolvable_and_unresolvable_seeds_uses_seeded_mode);
    RUN_TEST(repo_map_unscored_project_returns_explicit_gate_error);
    RUN_TEST(repo_map_partial_score_population_is_not_gated);
    RUN_TEST(repo_map_missing_project_is_error_no_crash);
    RUN_TEST(repo_map_unknown_project_require_store_error);
    RUN_TEST(repo_map_indexed_false_project_verify_indexed_error);
    RUN_TEST(repo_map_malformed_seed_anchors_string_coerced);
    RUN_TEST(repo_map_malformed_seed_anchors_non_string_elements_skipped);
    RUN_TEST(repo_map_absurdly_long_seed_list_capped_no_hang);
    RUN_TEST(repo_map_renders_signature_level_no_body_leak);
    RUN_TEST(repo_map_deterministic_byte_identical_across_calls);
    RUN_TEST(repo_map_no_cross_project_leakage);
    RUN_TEST(repo_map_repeated_calls_stable_no_leak_surface);
}
