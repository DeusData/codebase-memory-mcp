/*
 * test_mcp.c — Tests for the MCP server module.
 *
 * Covers: JSON-RPC parsing, MCP protocol, tool dispatch, tool handlers.
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include <mcp/mcp.h>
#include <pipeline/pipeline.h>
#include <store/store.h>
#include <yyjson/yyjson.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

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
    /* Should contain all public tools */
    ASSERT_NOT_NULL(strstr(json, "index_repository"));
    ASSERT_NOT_NULL(strstr(json, "search_graph"));
    ASSERT_NOT_NULL(strstr(json, "query_graph"));
    ASSERT_NOT_NULL(strstr(json, "trace_call_path"));
    ASSERT_NOT_NULL(strstr(json, "get_code_snippet"));
    ASSERT_NOT_NULL(strstr(json, "get_graph_schema"));
    ASSERT_NOT_NULL(strstr(json, "get_architecture"));
    ASSERT_NOT_NULL(strstr(json, "get_key_symbols"));
    ASSERT_NOT_NULL(strstr(json, "get_impact_analysis"));
    ASSERT_NOT_NULL(strstr(json, "get_architecture_summary"));
    ASSERT_NOT_NULL(strstr(json, "explore"));
    ASSERT_NOT_NULL(strstr(json, "understand"));
    ASSERT_NOT_NULL(strstr(json, "prepare_change"));
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
    ASSERT_NOT_NULL(strstr(resp, "explore"));
    ASSERT_NOT_NULL(strstr(resp, "understand"));
    ASSERT_NOT_NULL(strstr(resp, "prepare_change"));
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

TEST(tool_get_architecture_summary_missing_project) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":25,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"get_architecture_summary\","
                                   "\"arguments\":{}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "project is required"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

static cbm_mcp_server_t *setup_arch_summary_server(char *tmp_dir, size_t tmp_sz) {
    snprintf(tmp_dir, tmp_sz, "/tmp/cbm_mcp_arch_XXXXXX");
    if (!cbm_mkdtemp(tmp_dir)) {
        return NULL;
    }

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    if (!srv) {
        rmdir(tmp_dir);
        return NULL;
    }

    cbm_store_t *st = cbm_mcp_server_store(srv);
    if (!st) {
        cbm_mcp_server_free(srv);
        rmdir(tmp_dir);
        return NULL;
    }

    char *proj_name = cbm_project_name_from_path(tmp_dir);
    if (!proj_name) {
        cbm_mcp_server_free(srv);
        rmdir(tmp_dir);
        return NULL;
    }

    cbm_mcp_server_set_project(srv, proj_name);
    cbm_store_upsert_project(st, proj_name, tmp_dir);

    int64_t prev_fn_id = 0;
    for (int i = 0; i < 24; i++) {
        char file_name[64];
        char file_qn[128];
        char fn_name[32];
        char fn_qn[160];

        snprintf(file_name, sizeof(file_name), "pkg/file%02d.go", i);
        snprintf(file_qn, sizeof(file_qn), "%s.pkg.file%02d", proj_name, i);
        snprintf(fn_name, sizeof(fn_name), "Fn%02d", i);
        snprintf(fn_qn, sizeof(fn_qn), "%s.pkg.file%02d.%s", proj_name, i, fn_name);

        cbm_node_t file = {.project = proj_name,
                           .label = "File",
                           .name = file_name,
                           .qualified_name = file_qn,
                           .file_path = file_name};
        cbm_store_upsert_node(st, &file);

        cbm_node_t fn = {.project = proj_name,
                         .label = "Function",
                         .name = fn_name,
                         .qualified_name = fn_qn,
                         .file_path = file_name,
                         .start_line = 1,
                         .end_line = 40 + i};
        int64_t fn_id = cbm_store_upsert_node(st, &fn);
        if (prev_fn_id > 0) {
            cbm_edge_t edge = {
                .project = proj_name, .source_id = prev_fn_id, .target_id = fn_id, .type = "CALLS"};
            cbm_store_insert_edge(st, &edge);
        }
        prev_fn_id = fn_id;
    }

    free(proj_name);
    return srv;
}

static void cleanup_arch_summary_server(char *tmp_dir, cbm_mcp_server_t *srv) {
    cbm_mcp_server_free(srv);
    if (tmp_dir && tmp_dir[0]) {
        rmdir(tmp_dir);
    }
}

static cbm_mcp_server_t *setup_pagerank_server(void) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    if (!srv) {
        return NULL;
    }

    cbm_store_t *st = cbm_mcp_server_store(srv);
    if (!st) {
        cbm_mcp_server_free(srv);
        return NULL;
    }

    cbm_store_upsert_project(st, "test-rank", "/tmp/test-rank");
    cbm_mcp_server_set_project(srv, "test-rank");

    cbm_node_t nodes[] = {
        {.project = "test-rank", .label = "Function", .name = "Root", .qualified_name = "test-rank.Root"},
        {.project = "test-rank", .label = "Function", .name = "Small", .qualified_name = "test-rank.Small"},
        {.project = "test-rank", .label = "Function", .name = "Hub", .qualified_name = "test-rank.Hub"},
        {.project = "test-rank", .label = "Function", .name = "Leaf", .qualified_name = "test-rank.Leaf"},
        {.project = "test-rank", .label = "Function", .name = "CallerB", .qualified_name = "test-rank.CallerB"},
        {.project = "test-rank", .label = "Function", .name = "CallerC", .qualified_name = "test-rank.CallerC"},
    };
    int64_t ids[6];
    for (int i = 0; i < 6; i++) {
        ids[i] = cbm_store_upsert_node(st, &nodes[i]);
    }

    cbm_edge_t edges[] = {
        {.project = "test-rank", .source_id = ids[0], .target_id = ids[1], .type = "CALLS"},
        {.project = "test-rank", .source_id = ids[1], .target_id = ids[2], .type = "CALLS"},
        {.project = "test-rank", .source_id = ids[4], .target_id = ids[2], .type = "CALLS"},
        {.project = "test-rank", .source_id = ids[5], .target_id = ids[2], .type = "CALLS"},
    };
    for (int i = 0; i < 4; i++) {
        cbm_store_insert_edge(st, &edges[i]);
    }

    if (cbm_store_compute_pagerank(st, "test-rank", 20, 0.85) != CBM_STORE_OK) {
        cbm_mcp_server_free(srv);
        return NULL;
    }
    return srv;
}

static cbm_mcp_server_t *setup_truncation_server(void) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    if (!srv) {
        return NULL;
    }

    cbm_store_t *st = cbm_mcp_server_store(srv);
    if (!st) {
        cbm_mcp_server_free(srv);
        return NULL;
    }

    cbm_store_upsert_project(st, "test-budget", "/tmp/test-budget");
    cbm_mcp_server_set_project(srv, "test-budget");

    const char *sig =
        "{\"signature\":\"func BudgetedOperation(alpha int, beta int, gamma int, delta int, "
        "epsilon int, zeta int, eta int, theta int, iota int) string\"}";
    const char *names[] = {"Root", "A", "B", "C", "D", "E"};
    int64_t ids[6] = {0};

    for (int i = 0; i < 6; i++) {
        char qn[128];
        snprintf(qn, sizeof(qn), "test-budget.%s", names[i]);
        cbm_node_t node = {
            .project = "test-budget",
            .label = "Function",
            .name = names[i],
            .qualified_name = qn,
            .file_path = "pkg/budget.go",
            .start_line = 10 + (i * 5),
            .end_line = 13 + (i * 5),
            .properties_json = sig,
        };
        ids[i] = cbm_store_upsert_node(st, &node);
    }

    for (int i = 0; i < 5; i++) {
        cbm_edge_t edge = {
            .project = "test-budget", .source_id = ids[i], .target_id = ids[i + 1], .type = "CALLS"};
        cbm_store_insert_edge(st, &edge);
    }

    return srv;
}

static cbm_mcp_server_t *setup_impact_server(void) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    if (!srv) {
        return NULL;
    }

    cbm_store_t *st = cbm_mcp_server_store(srv);
    if (!st) {
        cbm_mcp_server_free(srv);
        return NULL;
    }

    cbm_store_upsert_project(st, "impact", "/tmp/impact");
    cbm_mcp_server_set_project(srv, "impact");

    cbm_node_t nodes[] = {
        {.project = "impact",
         .label = "Function",
         .name = "ProcessOrder",
         .qualified_name = "impact.service.ProcessOrder",
         .file_path = "app/services/order_service.php"},
        {.project = "impact",
         .label = "Method",
         .name = "HandleOrder",
         .qualified_name = "impact.controller.OrderController.HandleOrder",
         .file_path = "app/controllers/OrderController.php"},
        {.project = "impact",
         .label = "Function",
         .name = "CliEntry",
         .qualified_name = "impact.cli.CliEntry",
         .file_path = "app/cli/order_cli.php",
         .properties_json = "{\"is_entry_point\":true}"},
        {.project = "impact",
         .label = "Route",
         .name = "POST /orders",
         .qualified_name = "impact.route.post_orders",
         .file_path = "routes/api.php"},
        {.project = "impact",
         .label = "Function",
         .name = "CheckoutApi",
         .qualified_name = "impact.http.CheckoutApi",
         .file_path = "app/http/CheckoutApi.php"},
        {.project = "impact",
         .label = "Function",
         .name = "OrderWebhook",
         .qualified_name = "impact.jobs.OrderWebhook",
         .file_path = "app/jobs/OrderWebhook.php"},
        {.project = "impact",
         .label = "Function",
         .name = "BrowserFlow",
         .qualified_name = "impact.ui.BrowserFlow",
         .file_path = "app/ui/browser_flow.php"},
        {.project = "impact",
         .label = "Function",
         .name = "ProcessOrderTest",
         .qualified_name = "impact.tests.ProcessOrderTest",
         .file_path = "tests/process_order_test.php"},
        {.project = "impact",
         .label = "Function",
         .name = "Duplicate",
         .qualified_name = "impact.core.Duplicate",
         .file_path = "app/core/duplicate.php"},
        {.project = "impact",
         .label = "Function",
         .name = "Duplicate",
         .qualified_name = "impact.tests.Duplicate",
         .file_path = "tests/duplicate_test.php"},
        {.project = "impact",
         .label = "Function",
         .name = "CoreCallerA",
         .qualified_name = "impact.core.CoreCallerA",
         .file_path = "app/core/core_caller_a.php"},
        {.project = "impact",
         .label = "Function",
         .name = "CoreCallerB",
         .qualified_name = "impact.core.CoreCallerB",
         .file_path = "app/core/core_caller_b.php"},
        {.project = "impact",
         .label = "Function",
         .name = "TestCaller",
         .qualified_name = "impact.tests.TestCaller",
         .file_path = "tests/test_caller.php"},
        {.project = "impact",
         .label = "Function",
         .name = "TestCaller2",
         .qualified_name = "impact.tests.TestCaller2",
         .file_path = "tests/test_caller_two.php"},
        {.project = "impact",
         .label = "Function",
         .name = "TestCaller3",
         .qualified_name = "impact.tests.TestCaller3",
         .file_path = "tests/test_caller_three.php"},
    };

    enum {
        ID_PROCESS_ORDER,
        ID_HANDLE_ORDER,
        ID_CLI_ENTRY,
        ID_ROUTE,
        ID_CHECKOUT_API,
        ID_ORDER_WEBHOOK,
        ID_BROWSER_FLOW,
        ID_PROCESS_ORDER_TEST,
        ID_DUPLICATE_PROD,
        ID_DUPLICATE_TEST,
        ID_CORE_CALLER_A,
        ID_CORE_CALLER_B,
        ID_TEST_CALLER,
        ID_TEST_CALLER_2,
        ID_TEST_CALLER_3,
        ID_COUNT
    };
    int64_t ids[ID_COUNT] = {0};
    for (int i = 0; i < ID_COUNT; i++) {
        ids[i] = cbm_store_upsert_node(st, &nodes[i]);
    }

    cbm_edge_t edges[] = {
        {.project = "impact",
         .source_id = ids[ID_HANDLE_ORDER],
         .target_id = ids[ID_PROCESS_ORDER],
         .type = "CALLS"},
        {.project = "impact",
         .source_id = ids[ID_CLI_ENTRY],
         .target_id = ids[ID_PROCESS_ORDER],
         .type = "CALLS"},
        {.project = "impact",
         .source_id = ids[ID_PROCESS_ORDER_TEST],
         .target_id = ids[ID_PROCESS_ORDER],
         .type = "CALLS"},
        {.project = "impact",
         .source_id = ids[ID_HANDLE_ORDER],
         .target_id = ids[ID_ROUTE],
         .type = "HANDLES"},
        {.project = "impact",
         .source_id = ids[ID_CHECKOUT_API],
         .target_id = ids[ID_ROUTE],
         .type = "HTTP_CALLS"},
        {.project = "impact",
         .source_id = ids[ID_ORDER_WEBHOOK],
         .target_id = ids[ID_ROUTE],
         .type = "ASYNC_CALLS"},
        {.project = "impact",
         .source_id = ids[ID_BROWSER_FLOW],
         .target_id = ids[ID_CHECKOUT_API],
         .type = "CALLS"},
        {.project = "impact",
         .source_id = ids[ID_CORE_CALLER_A],
         .target_id = ids[ID_DUPLICATE_PROD],
         .type = "CALLS"},
        {.project = "impact",
         .source_id = ids[ID_CORE_CALLER_B],
         .target_id = ids[ID_DUPLICATE_PROD],
         .type = "CALLS"},
        {.project = "impact",
         .source_id = ids[ID_TEST_CALLER],
         .target_id = ids[ID_DUPLICATE_TEST],
         .type = "CALLS"},
        {.project = "impact",
         .source_id = ids[ID_TEST_CALLER_2],
         .target_id = ids[ID_DUPLICATE_TEST],
         .type = "CALLS"},
        {.project = "impact",
         .source_id = ids[ID_TEST_CALLER_3],
         .target_id = ids[ID_DUPLICATE_TEST],
         .type = "CALLS"},
    };
    const int edge_count = (int)(sizeof(edges) / sizeof(edges[0]));
    for (int i = 0; i < edge_count; i++) {
        cbm_store_insert_edge(st, &edges[i]);
    }

    if (cbm_store_compute_pagerank(st, "impact", 20, 0.85) != CBM_STORE_OK) {
        cbm_mcp_server_free(srv);
        return NULL;
    }

    return srv;
}

TEST(tool_get_architecture_summary_truncated) {
    char tmp_dir[256];
    cbm_mcp_server_t *srv = setup_arch_summary_server(tmp_dir, sizeof(tmp_dir));
    ASSERT_NOT_NULL(srv);
    char *proj_name = cbm_project_name_from_path(tmp_dir);
    ASSERT_NOT_NULL(proj_name);

    char req[1024];
    snprintf(req, sizeof(req),
             "{\"jsonrpc\":\"2.0\",\"id\":26,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"get_architecture_summary\","
              "\"arguments\":{\"project\":\"%s\",\"max_tokens\":1}}}",
             proj_name);

    char *resp = cbm_mcp_server_handle(srv, req);
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "## Project:"));
    ASSERT_NOT_NULL(strstr(resp, "_Truncated at max_tokens._"));
    free(resp);
    free(proj_name);

    cleanup_arch_summary_server(tmp_dir, srv);
    PASS();
}

TEST(tool_get_architecture_summary_project_path_alias) {
    char tmp_dir[256];
    cbm_mcp_server_t *srv = setup_arch_summary_server(tmp_dir, sizeof(tmp_dir));
    ASSERT_NOT_NULL(srv);

    char req[1024];
    snprintf(req, sizeof(req),
             "{\"jsonrpc\":\"2.0\",\"id\":27,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"get_architecture_summary\","
             "\"arguments\":{\"project_path\":\"%s\",\"max_tokens\":64}}}",
             tmp_dir);

    char *resp = cbm_mcp_server_handle(srv, req);
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "## Project:"));
    free(resp);

    cleanup_arch_summary_server(tmp_dir, srv);
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
    ASSERT_TRUE(strstr(resp, "not found") || strstr(resp, "not indexed") || strstr(resp, "required"));
    free(resp);

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

    cbm_node_t n_run3 = {0};
    n_run3.project = proj_name;
    n_run3.label = "Function";
    n_run3.name = "Run";
    n_run3.qualified_name = "test-project.api.server.Run";
    n_run3.file_path = "main.go";
    n_run3.start_line = 11;
    n_run3.end_line = 13;
    cbm_store_upsert_node(st, &n_run3);

    /* Create edges: HandleRequest -> ProcessOrder, HandleRequest -> Run1 */
    cbm_edge_t e1 = {.project = proj_name, .source_id = id_hr, .target_id = id_po, .type = "CALLS"};
    cbm_store_insert_edge(st, &e1);

    cbm_edge_t e2 = {
        .project = proj_name, .source_id = id_hr, .target_id = id_run1, .type = "CALLS"};
    cbm_store_insert_edge(st, &e2);
    (void)id_run1; /* run1 used for edge above */

    if (cbm_store_compute_pagerank(st, proj_name, 20, 0.85) != CBM_STORE_OK) {
        cbm_mcp_server_free(srv);
        return NULL;
    }

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

TEST(tool_search_graph_ranked_pagerank) {
    cbm_mcp_server_t *srv = setup_pagerank_server();
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(srv, "search_graph",
                                    "{\"project\":\"test-rank\",\"label\":\"Function\",\"limit\":10}");
    ASSERT_NOT_NULL(raw);
    char *text = extract_text_content(raw);
    ASSERT_NOT_NULL(text);
    ASSERT_NOT_NULL(strstr(text, "\"pagerank\""));
    ASSERT_NOT_NULL(strstr(text, "\"name\":\"Hub\""));
    ASSERT_NOT_NULL(strstr(text, "\"name\":\"Small\""));
    ASSERT_TRUE(strstr(text, "\"name\":\"Hub\"") < strstr(text, "\"name\":\"Small\""));
    free(text);
    free(raw);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_get_key_symbols_ranked) {
    cbm_mcp_server_t *srv = setup_pagerank_server();
    ASSERT_NOT_NULL(srv);

    char *raw =
        cbm_mcp_handle_tool(srv, "get_key_symbols", "{\"project\":\"test-rank\",\"limit\":3}");
    ASSERT_NOT_NULL(raw);
    char *text = extract_text_content(raw);
    ASSERT_NOT_NULL(text);
    ASSERT_NOT_NULL(strstr(text, "\"results\""));
    ASSERT_NOT_NULL(strstr(text, "\"pagerank\""));
    ASSERT_NOT_NULL(strstr(text, "\"name\":\"Hub\""));
    ASSERT_TRUE(strstr(text, "\"name\":\"Hub\"") < strstr(text, "\"name\":\"Small\""));
    free(text);
    free(raw);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_trace_call_path_ranked_pagerank) {
    cbm_mcp_server_t *srv = setup_pagerank_server();
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(
        srv, "trace_call_path",
        "{\"project\":\"test-rank\",\"function_name\":\"Root\",\"direction\":\"outbound\",\"depth\":3}");
    ASSERT_NOT_NULL(raw);
    char *text = extract_text_content(raw);
    ASSERT_NOT_NULL(text);
    ASSERT_NOT_NULL(strstr(text, "\"callees\""));
    ASSERT_NOT_NULL(strstr(text, "\"pagerank\""));
    ASSERT_NOT_NULL(strstr(text, "\"name\":\"Hub\""));
    ASSERT_NOT_NULL(strstr(text, "\"name\":\"Small\""));
    ASSERT_TRUE(strstr(text, "\"name\":\"Hub\"") < strstr(text, "\"name\":\"Small\""));
    free(text);
    free(raw);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_search_graph_max_tokens_truncates) {
    cbm_mcp_server_t *srv = setup_pagerank_server();
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(
        srv, "search_graph",
        "{\"project\":\"test-rank\",\"label\":\"Function\",\"limit\":10,\"max_tokens\":1}");
    ASSERT_NOT_NULL(raw);
    char *text = extract_text_content(raw);
    ASSERT_NOT_NULL(text);
    ASSERT_NOT_NULL(strstr(text, "\"truncated\":true"));
    ASSERT_NOT_NULL(strstr(text, "\"shown\""));
    ASSERT_NOT_NULL(strstr(text, "\"total_results\""));
    ASSERT_NOT_NULL(strstr(text, "\"name\":\"Hub\""));
    free(text);
    free(raw);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_trace_call_path_max_tokens_truncates) {
    cbm_mcp_server_t *srv = setup_pagerank_server();
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(
        srv, "trace_call_path",
        "{\"project\":\"test-rank\",\"function_name\":\"Root\",\"direction\":\"outbound\","
        "\"depth\":3,\"max_tokens\":1}");
    ASSERT_NOT_NULL(raw);
    char *text = extract_text_content(raw);
    ASSERT_NOT_NULL(text);
    ASSERT_NOT_NULL(strstr(text, "\"truncated\":true"));
    ASSERT_NOT_NULL(strstr(text, "\"shown\""));
    ASSERT_NOT_NULL(strstr(text, "\"total_results\""));
    ASSERT_NOT_NULL(strstr(text, "\"callees\""));
    free(text);
    free(raw);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_search_graph_long_signature_budget_respected) {
    cbm_mcp_server_t *srv = setup_truncation_server();
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(
        srv, "search_graph",
        "{\"project\":\"test-budget\",\"label\":\"Function\",\"limit\":10,\"max_tokens\":100}");
    ASSERT_NOT_NULL(raw);
    char *text = extract_text_content(raw);
    ASSERT_NOT_NULL(text);
    ASSERT_NOT_NULL(strstr(text, "\"truncated\":true"));
    ASSERT_NOT_NULL(strstr(text, "\"shown\":1"));
    free(text);
    free(raw);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_trace_call_path_chain_shows_omitted_count) {
    cbm_mcp_server_t *srv = setup_truncation_server();
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(
        srv, "trace_call_path",
        "{\"project\":\"test-budget\",\"function_name\":\"Root\",\"direction\":\"outbound\","
        "\"depth\":5,\"max_tokens\":100}");
    ASSERT_NOT_NULL(raw);
    char *text = extract_text_content(raw);
    ASSERT_NOT_NULL(text);
    ASSERT_NOT_NULL(strstr(text, "\"truncated\":true"));
    ASSERT_NOT_NULL(strstr(text, "\"callees_chain\":\""));
    ASSERT_NOT_NULL(strstr(text, "more) ->"));
    free(text);
    free(raw);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_query_graph_max_tokens_truncates) {
    char tmp_dir[256];
    cbm_mcp_server_t *srv = setup_arch_summary_server(tmp_dir, sizeof(tmp_dir));
    ASSERT_NOT_NULL(srv);
    char *proj_name = cbm_project_name_from_path(tmp_dir);
    ASSERT_NOT_NULL(proj_name);

    char args[1024];
    snprintf(args, sizeof(args),
             "{\"project\":\"%s\",\"query\":\"MATCH (f:Function) RETURN f.name, f.qualified_name, "
             "f.file_path\",\"max_tokens\":1}",
             proj_name);

    char *raw = cbm_mcp_handle_tool(srv, "query_graph", args);
    ASSERT_NOT_NULL(raw);
    char *text = extract_text_content(raw);
    ASSERT_NOT_NULL(text);
    ASSERT_NOT_NULL(strstr(text, "\"truncated\":true"));
    ASSERT_NOT_NULL(strstr(text, "\"shown\""));
    ASSERT_NOT_NULL(strstr(text, "\"total_results\""));
    ASSERT_NOT_NULL(strstr(text, "\"columns\""));
    free(text);
    free(raw);
    free(proj_name);

    cleanup_arch_summary_server(tmp_dir, srv);
    PASS();
}

TEST(tool_get_impact_analysis_basic) {
    cbm_mcp_server_t *srv = setup_impact_server();
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(
        srv, "get_impact_analysis",
        "{\"project\":\"impact\",\"symbol\":\"ProcessOrder\",\"depth\":4}");
    ASSERT_NOT_NULL(raw);
    char *text = extract_text_content(raw);
    ASSERT_NOT_NULL(text);
    ASSERT_NOT_NULL(strstr(text, "\"symbol\":\"ProcessOrder\""));
    ASSERT_NOT_NULL(strstr(text, "\"qualified_name\":\"impact.service.ProcessOrder\""));
    ASSERT_NOT_NULL(strstr(text, "\"risk_score\":\"high\""));
    ASSERT_NOT_NULL(strstr(
        text,
        "\"summary\":\"2 direct callers, 2 route/entry points, 1 affected tests, 1 transitive impacts\""));
    ASSERT_NOT_NULL(strstr(text, "\"affected_tests\":["));
    free(text);
    free(raw);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_get_impact_analysis_missing_symbol) {
    cbm_mcp_server_t *srv = setup_impact_server();
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(
        srv, "get_impact_analysis", "{\"project\":\"impact\",\"symbol\":\"MissingSymbol\"}");
    ASSERT_NOT_NULL(raw);
    ASSERT_NOT_NULL(strstr(raw, "search_graph(name_pattern"));
    free(raw);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_get_impact_analysis_ambiguous_symbol_picks_top_match) {
    cbm_mcp_server_t *srv = setup_impact_server();
    ASSERT_NOT_NULL(srv);

    char *raw =
        cbm_mcp_handle_tool(srv, "get_impact_analysis",
                            "{\"project\":\"impact\",\"symbol\":\"Duplicate\",\"depth\":2}");
    ASSERT_NOT_NULL(raw);
    char *text = extract_text_content(raw);
    ASSERT_NOT_NULL(text);
    ASSERT_NOT_NULL(strstr(text, "\"qualified_name\":\"impact.core.Duplicate\""));
    ASSERT_NOT_NULL(strstr(text, "\"file\":\"app/core/duplicate.php\""));
    free(text);
    free(raw);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_get_impact_analysis_include_tests_false) {
    cbm_mcp_server_t *srv = setup_impact_server();
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(
        srv, "get_impact_analysis",
        "{\"project\":\"impact\",\"symbol\":\"ProcessOrder\",\"depth\":4,\"include_tests\":false}");
    ASSERT_NOT_NULL(raw);
    char *text = extract_text_content(raw);
    ASSERT_NOT_NULL(text);
    ASSERT_NOT_NULL(strstr(text, "\"affected_tests\":[]"));
    ASSERT_NOT_NULL(
        strstr(text, "\"summary\":\"2 direct callers, 2 route/entry points, 1 transitive impacts\""));
    free(text);
    free(raw);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_get_impact_analysis_max_tokens_truncates) {
    cbm_mcp_server_t *srv = setup_impact_server();
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(
        srv, "get_impact_analysis",
        "{\"project\":\"impact\",\"symbol\":\"ProcessOrder\",\"depth\":4,\"max_tokens\":1}");
    ASSERT_NOT_NULL(raw);
    char *text = extract_text_content(raw);
    ASSERT_NOT_NULL(text);
    ASSERT_NOT_NULL(strstr(text, "\"truncated\":true"));
    ASSERT_NOT_NULL(strstr(text, "\"total_results\""));
    ASSERT_NOT_NULL(strstr(text, "\"shown\""));
    ASSERT_NOT_NULL(strstr(text, "\"impact\""));
    free(text);
    free(raw);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_get_impact_analysis_route_and_entry_point_typing) {
    cbm_mcp_server_t *srv = setup_impact_server();
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(
        srv, "get_impact_analysis",
        "{\"project\":\"impact\",\"symbol\":\"ProcessOrder\",\"depth\":4}");
    ASSERT_NOT_NULL(raw);
    char *text = extract_text_content(raw);
    ASSERT_NOT_NULL(text);
    ASSERT_NOT_NULL(
        strstr(text, "\"name\":\"CliEntry\",\"file\":\"app/cli/order_cli.php\",\"type\":\"entry_point\""));
    ASSERT_NOT_NULL(
        strstr(text, "\"name\":\"POST /orders\",\"file\":\"routes/api.php\",\"type\":\"route\""));
    free(text);
    free(raw);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_explore_basic) {
    cbm_mcp_server_t *srv = setup_impact_server();
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(srv, "explore",
                                    "{\"project\":\"impact\",\"area\":\"Order\"}");
    ASSERT_NOT_NULL(raw);
    char *text = extract_text_content(raw);
    ASSERT_NOT_NULL(text);
    ASSERT_NOT_NULL(strstr(text, "\"matches\""));
    ASSERT_NOT_NULL(strstr(text, "\"dependencies\""));
    ASSERT_NOT_NULL(strstr(text, "\"hotspots\""));
    ASSERT_NOT_NULL(strstr(text, "\"entry_points\""));
    ASSERT_NOT_NULL(strstr(text, "ProcessOrder"));
    ASSERT_NOT_NULL(strstr(text, "CliEntry"));
    free(text);
    free(raw);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_explore_max_tokens_truncates) {
    cbm_mcp_server_t *srv = setup_impact_server();
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(srv, "explore",
                                    "{\"project\":\"impact\",\"area\":\"Order\",\"max_tokens\":1}");
    ASSERT_NOT_NULL(raw);
    char *text = extract_text_content(raw);
    ASSERT_NOT_NULL(text);
    ASSERT_NOT_NULL(strstr(text, "\"truncated\":true"));
    ASSERT_NOT_NULL(strstr(text, "\"total_results\""));
    ASSERT_NOT_NULL(strstr(text, "\"shown\""));
    free(text);
    free(raw);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_understand_exact_short_name_autopicks_best_non_test) {
    cbm_mcp_server_t *srv = setup_impact_server();
    ASSERT_NOT_NULL(srv);

    char *raw =
        cbm_mcp_handle_tool(srv, "understand", "{\"project\":\"impact\",\"symbol\":\"Duplicate\"}");
    ASSERT_NOT_NULL(raw);
    char *text = extract_text_content(raw);
    ASSERT_NOT_NULL(text);
    ASSERT_NOT_NULL(strstr(text, "\"qualified_name\":\"impact.core.Duplicate\""));
    ASSERT_NOT_NULL(strstr(text, "\"alternatives\""));
    ASSERT_NOT_NULL(strstr(text, "impact.tests.Duplicate"));
    free(text);
    free(raw);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_understand_qualified_name_resolution) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(
        srv, "understand",
        "{\"project\":\"test-project\",\"symbol\":\"test-project.cmd.server.main.ProcessOrder\"}");
    ASSERT_NOT_NULL(raw);
    char *text = extract_text_content(raw);
    ASSERT_NOT_NULL(text);
    ASSERT_NOT_NULL(
        strstr(text, "\"qualified_name\":\"test-project.cmd.server.main.ProcessOrder\""));
    ASSERT_NOT_NULL(strstr(text, "\"definition\""));
    ASSERT_NOT_NULL(strstr(text, "\"source\""));
    ASSERT_NOT_NULL(strstr(text, "func ProcessOrder(id int)"));
    free(text);
    free(raw);

    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

TEST(tool_understand_suffix_ambiguity_returns_suggestions) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *raw =
        cbm_mcp_handle_tool(srv, "understand", "{\"project\":\"test-project\",\"symbol\":\"server.Run\"}");
    ASSERT_NOT_NULL(raw);
    char *text = extract_text_content(raw);
    ASSERT_NOT_NULL(text);
    ASSERT_NOT_NULL(strstr(text, "\"status\":\"ambiguous\""));
    ASSERT_NOT_NULL(strstr(text, "test-project.cmd.server.Run"));
    ASSERT_NOT_NULL(strstr(text, "test-project.api.server.Run"));
    free(text);
    free(raw);

    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

TEST(tool_understand_max_tokens_truncates) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(
        srv, "understand",
        "{\"project\":\"test-project\",\"symbol\":\"test-project.cmd.server.main.HandleRequest\","
        "\"max_tokens\":1}");
    ASSERT_NOT_NULL(raw);
    char *text = extract_text_content(raw);
    ASSERT_NOT_NULL(text);
    ASSERT_NOT_NULL(strstr(text, "\"truncated\":true"));
    ASSERT_NOT_NULL(strstr(text, "\"definition\""));
    ASSERT_NOT_NULL(strstr(text, "\"shown\""));
    free(text);
    free(raw);

    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

TEST(tool_prepare_change_basic) {
    cbm_mcp_server_t *srv = setup_impact_server();
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(
        srv, "prepare_change", "{\"project\":\"impact\",\"symbol\":\"ProcessOrder\"}");
    ASSERT_NOT_NULL(raw);
    char *text = extract_text_content(raw);
    ASSERT_NOT_NULL(text);
    ASSERT_NOT_NULL(strstr(text, "\"review_scope\""));
    ASSERT_NOT_NULL(strstr(text, "\"risk_score\":\"high\""));
    ASSERT_NOT_NULL(strstr(text, "\"must_review\""));
    ASSERT_NOT_NULL(strstr(text, "app/services/order_service.php"));
    free(text);
    free(raw);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_prepare_change_include_tests_false) {
    cbm_mcp_server_t *srv = setup_impact_server();
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(
        srv, "prepare_change",
        "{\"project\":\"impact\",\"symbol\":\"ProcessOrder\",\"include_tests\":false}");
    ASSERT_NOT_NULL(raw);
    char *text = extract_text_content(raw);
    ASSERT_NOT_NULL(text);
    ASSERT_NOT_NULL(strstr(text, "\"affected_tests\":[]"));
    ASSERT_NOT_NULL(strstr(text, "\"summary\":\"2 direct callers, 2 route/entry points, 1 transitive impacts\""));
    ASSERT_NULL(strstr(text, "\"review_scope\":{\"must_review\":[\"app/services/order_service.php\"],\"should_review\":[\"app/ui/browser_flow.php\"],\"tests\""));
    free(text);
    free(raw);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_prepare_change_max_tokens_truncates) {
    cbm_mcp_server_t *srv = setup_impact_server();
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(
        srv, "prepare_change",
        "{\"project\":\"impact\",\"symbol\":\"ProcessOrder\",\"max_tokens\":1}");
    ASSERT_NOT_NULL(raw);
    char *text = extract_text_content(raw);
    ASSERT_NOT_NULL(text);
    ASSERT_NOT_NULL(strstr(text, "\"truncated\":true"));
    ASSERT_NOT_NULL(strstr(text, "\"review_scope\""));
    ASSERT_NOT_NULL(strstr(text, "\"shown\""));
    free(text);
    free(raw);

    cbm_mcp_server_free(srv);
    PASS();
}

/* ── Error-path tests for compound tools ──────────────────────── */

TEST(tool_explore_missing_project) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(srv, "explore",
                                    "{\"project\":\"nonexistent\",\"area\":\"foo\"}");
    ASSERT_NOT_NULL(raw);
    ASSERT_NOT_NULL(strstr(raw, "isError"));
    ASSERT_NOT_NULL(strstr(raw, "not found"));
    free(raw);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_explore_no_matches) {
    cbm_mcp_server_t *srv = setup_impact_server();
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(srv, "explore",
                                    "{\"project\":\"impact\",\"area\":\"zzzznonexistent\"}");
    ASSERT_NOT_NULL(raw);
    char *text = extract_text_content(raw);
    ASSERT_NOT_NULL(text);
    /* Should return valid JSON with empty arrays, not an error */
    ASSERT_NOT_NULL(strstr(text, "\"matches\""));
    ASSERT_NOT_NULL(strstr(text, "\"hotspots\""));
    ASSERT_NULL(strstr(text, "isError"));
    free(text);
    free(raw);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_understand_missing_project) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(srv, "understand",
                                    "{\"project\":\"nonexistent\",\"symbol\":\"Foo\"}");
    ASSERT_NOT_NULL(raw);
    ASSERT_NOT_NULL(strstr(raw, "isError"));
    ASSERT_NOT_NULL(strstr(raw, "not found"));
    free(raw);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_understand_missing_symbol) {
    cbm_mcp_server_t *srv = setup_impact_server();
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(srv, "understand",
                                    "{\"project\":\"impact\",\"symbol\":\"ZZZNoSuchSymbol\"}");
    ASSERT_NOT_NULL(raw);
    ASSERT_NOT_NULL(strstr(raw, "isError"));
    ASSERT_NOT_NULL(strstr(raw, "not found"));
    free(raw);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_prepare_change_missing_project) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(srv, "prepare_change",
                                    "{\"project\":\"nonexistent\",\"symbol\":\"Foo\"}");
    ASSERT_NOT_NULL(raw);
    ASSERT_NOT_NULL(strstr(raw, "isError"));
    ASSERT_NOT_NULL(strstr(raw, "not found"));
    free(raw);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_prepare_change_missing_symbol) {
    cbm_mcp_server_t *srv = setup_impact_server();
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(srv, "prepare_change",
                                    "{\"project\":\"impact\",\"symbol\":\"ZZZNoSuchSymbol\"}");
    ASSERT_NOT_NULL(raw);
    ASSERT_NOT_NULL(strstr(raw, "isError"));
    ASSERT_NOT_NULL(strstr(raw, "not found"));
    free(raw);

    cbm_mcp_server_free(srv);
    PASS();
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
    /* JSON-RPC spec allows string IDs; parser converts via strtol */
    const char *line = "{\"jsonrpc\":\"2.0\",\"id\":\"99\",\"method\":\"tools/list\"}";
    cbm_jsonrpc_request_t req = {0};
    int rc = cbm_jsonrpc_parse(line, &req);
    ASSERT_EQ(rc, 0);
    ASSERT_TRUE(req.has_id);
    ASSERT_EQ(req.id, 99);
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
    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":50,\"method\":\"tools/call\","
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
    const char *msgs =
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\","
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

/* ══════════════════════════════════════════════════════════════════
 *  SUITE
 * ══════════════════════════════════════════════════════════════════ */

SUITE(mcp) {
    /* JSON-RPC parsing */
    RUN_TEST(jsonrpc_parse_request);
    RUN_TEST(jsonrpc_parse_notification);
    RUN_TEST(jsonrpc_parse_invalid);
    RUN_TEST(jsonrpc_parse_tools_call);

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
    RUN_TEST(tool_search_graph_ranked_pagerank);
    RUN_TEST(tool_search_graph_max_tokens_truncates);
    RUN_TEST(tool_search_graph_long_signature_budget_respected);
    RUN_TEST(tool_query_graph_basic);
    RUN_TEST(tool_index_status_no_project);

    /* Tool handlers with validation */
    RUN_TEST(tool_trace_call_path_not_found);
    RUN_TEST(tool_trace_missing_function_name);
    RUN_TEST(tool_delete_project_not_found);
    RUN_TEST(tool_get_architecture_empty);
    RUN_TEST(tool_get_architecture_summary_missing_project);
    RUN_TEST(tool_get_architecture_summary_truncated);
    RUN_TEST(tool_get_architecture_summary_project_path_alias);
    RUN_TEST(tool_get_key_symbols_ranked);
    RUN_TEST(tool_trace_call_path_ranked_pagerank);
    RUN_TEST(tool_trace_call_path_max_tokens_truncates);
    RUN_TEST(tool_trace_call_path_chain_shows_omitted_count);
    RUN_TEST(tool_query_graph_missing_query);
    RUN_TEST(tool_query_graph_max_tokens_truncates);
    RUN_TEST(tool_get_impact_analysis_basic);
    RUN_TEST(tool_get_impact_analysis_missing_symbol);
    RUN_TEST(tool_get_impact_analysis_ambiguous_symbol_picks_top_match);
    RUN_TEST(tool_get_impact_analysis_include_tests_false);
    RUN_TEST(tool_get_impact_analysis_max_tokens_truncates);
    RUN_TEST(tool_get_impact_analysis_route_and_entry_point_typing);
    RUN_TEST(tool_explore_basic);
    RUN_TEST(tool_explore_max_tokens_truncates);
    RUN_TEST(tool_understand_exact_short_name_autopicks_best_non_test);
    RUN_TEST(tool_understand_qualified_name_resolution);
    RUN_TEST(tool_understand_suffix_ambiguity_returns_suggestions);
    RUN_TEST(tool_understand_max_tokens_truncates);
    RUN_TEST(tool_prepare_change_basic);
    RUN_TEST(tool_prepare_change_include_tests_false);
    RUN_TEST(tool_prepare_change_max_tokens_truncates);
    RUN_TEST(tool_explore_missing_project);
    RUN_TEST(tool_explore_no_matches);
    RUN_TEST(tool_understand_missing_project);
    RUN_TEST(tool_understand_missing_symbol);
    RUN_TEST(tool_prepare_change_missing_project);
    RUN_TEST(tool_prepare_change_missing_symbol);

    /* Pipeline-dependent tool handlers */
    RUN_TEST(tool_index_repository_missing_path);
    RUN_TEST(tool_get_code_snippet_missing_qn);
    RUN_TEST(tool_get_code_snippet_not_found);
    RUN_TEST(tool_search_code_missing_pattern);
    RUN_TEST(tool_search_code_no_project);
    RUN_TEST(tool_detect_changes_no_project);
    RUN_TEST(tool_manage_adr_no_project);
    RUN_TEST(tool_manage_adr_get_with_existing_adr);
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
}
