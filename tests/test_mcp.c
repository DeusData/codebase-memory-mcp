/*
 * test_mcp.c — Tests for the MCP server module.
 *
 * Covers: JSON-RPC parsing, MCP protocol, tool dispatch, tool handlers.
 */
#include "../src/foundation/compat.h"
#include "../src/foundation/compat_fs.h" /* cbm_unlink / cbm_rmdir */
#include "../src/foundation/constants.h"
#include "../src/foundation/log.h"
#include "test_framework.h"
#include "test_helpers.h"
#include <cli/cli.h>
#include <mcp/index_supervisor.h> /* spawn-count hook — #845 in-process guard */
#include <mcp/mcp.h>
#include <pipeline/pipeline.h>
#include <store/store.h>
#include <watcher/watcher.h>
#include <yyjson/yyjson.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include <sys/stat.h> /* chmod / stat for read-only query reproductions */
#ifdef _WIN32
#include <direct.h>
#define cbm_chdir _chdir
#define cbm_getcwd _getcwd
#else
#include <sys/wait.h> /* waitpid — #845 fork+alarm harness */
#include <unistd.h>
#define cbm_chdir chdir
#define cbm_getcwd getcwd
#endif

static char mcp_log_buf[4096];

static void mcp_capture_log(const char *line) {
    snprintf(mcp_log_buf, sizeof(mcp_log_buf), "%s", line ? line : "");
}

static bool response_contains_json_fragment(const char *response, const char *fragment) {
    if (!response || !fragment) {
        return false;
    }
    if (strstr(response, fragment)) {
        return true;
    }

    char escaped[512];
    size_t out = 0;
    for (size_t i = 0; fragment[i] && out + 2 < sizeof(escaped); i++) {
        if (fragment[i] == '"') {
            escaped[out++] = '\\';
        }
        escaped[out++] = fragment[i];
    }
    escaped[out] = '\0';
    return strstr(response, escaped) != NULL;
}

static void restore_cache_dir(const char *saved_copy) {
    if (saved_copy) {
        cbm_setenv("CBM_CACHE_DIR", saved_copy, 1);
    } else {
        cbm_unsetenv("CBM_CACHE_DIR");
    }
}

static void cleanup_project_db(const char *cache, const char *project) {
    if (!cache || !project) {
        return;
    }

    char path[CBM_SZ_4K];
    snprintf(path, sizeof(path), "%s/%s.db", cache, project);
    cbm_unlink(path);
    snprintf(path, sizeof(path), "%s/%s.db-wal", cache, project);
    cbm_unlink(path);
    snprintf(path, sizeof(path), "%s/%s.db-shm", cache, project);
    cbm_unlink(path);
}

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
    cbm_cli_set_version("9.8.7-test");

    /* Default (no params): returns latest supported version */
    char *json = cbm_mcp_initialize_response(NULL);
    ASSERT_NOT_NULL(json);
    ASSERT_NOT_NULL(strstr(json, "codebase-memory-mcp"));
    ASSERT_NOT_NULL(strstr(json, "\"version\":\"9.8.7-test\""));
    ASSERT_NOT_NULL(strstr(json, "capabilities"));
    ASSERT_NOT_NULL(strstr(json, "tools"));
    ASSERT_NOT_NULL(strstr(json, "\"listChanged\":false"));
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
    cbm_cli_set_version("dev");
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

TEST(mcp_tools_list_latest_metadata) {
    char *json = cbm_mcp_tools_list();
    ASSERT_NOT_NULL(json);
    ASSERT_NOT_NULL(strstr(json, "\"title\":\"Search graph\""));
    ASSERT_NOT_NULL(strstr(json, "\"title\":\"Index repository\""));
    ASSERT_NOT_NULL(strstr(json, "\"outputSchema\":{\"type\":\"object\""));
    ASSERT_NOT_NULL(strstr(json, "\"additionalProperties\":true"));
    free(json);
    PASS();
}

TEST(mcp_index_repository_declares_name_override_issue571) {
    char *json = cbm_mcp_tools_list();
    ASSERT_NOT_NULL(json);
    ASSERT_NOT_NULL(strstr(json, "\"index_repository\""));
    ASSERT_NOT_NULL(strstr(json, "\"name\":{\"type\":\"string\""));
    ASSERT_NOT_NULL(strstr(json, "Non-ASCII bytes are encoded"));
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

TEST(mcp_ingest_traces_items_disallow_additional_properties_issue731) {
    char *json = cbm_mcp_tools_list();
    ASSERT_NOT_NULL(json);

    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *root = yyjson_doc_get_root(doc);
    ASSERT_NOT_NULL(root);
    yyjson_val *tools = yyjson_obj_get(root, "tools");
    ASSERT_NOT_NULL(tools);
    ASSERT_TRUE(yyjson_is_arr(tools));

    yyjson_val *tool;
    yyjson_arr_iter iter;
    yyjson_arr_iter_init(tools, &iter);
    yyjson_val *ingest_traces = NULL;
    while ((tool = yyjson_arr_iter_next(&iter)) != NULL) {
        yyjson_val *name = yyjson_obj_get(tool, "name");
        if (name && yyjson_is_str(name) && strcmp(yyjson_get_str(name), "ingest_traces") == 0) {
            ingest_traces = tool;
            break;
        }
    }
    ASSERT_NOT_NULL(ingest_traces);

    yyjson_val *input_schema = yyjson_obj_get(ingest_traces, "inputSchema");
    ASSERT_NOT_NULL(input_schema);
    yyjson_val *properties = yyjson_obj_get(input_schema, "properties");
    ASSERT_NOT_NULL(properties);
    yyjson_val *traces = yyjson_obj_get(properties, "traces");
    ASSERT_NOT_NULL(traces);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(traces, "type")), "array");
    yyjson_val *items = yyjson_obj_get(traces, "items");
    ASSERT_NOT_NULL(items);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(items, "type")), "object");
    yyjson_val *item_properties = yyjson_obj_get(items, "properties");
    ASSERT_NOT_NULL(item_properties);
    yyjson_val *caller = yyjson_obj_get(item_properties, "caller");
    ASSERT_NOT_NULL(caller);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(caller, "type")), "string");
    yyjson_val *callee = yyjson_obj_get(item_properties, "callee");
    ASSERT_NOT_NULL(callee);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(callee, "type")), "string");
    yyjson_val *count = yyjson_obj_get(item_properties, "count");
    ASSERT_NOT_NULL(count);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(count, "type")), "integer");
    yyjson_val *additional_properties = yyjson_obj_get(items, "additionalProperties");
    ASSERT_NOT_NULL(additional_properties);
    ASSERT_TRUE(yyjson_is_bool(additional_properties));
    ASSERT_FALSE(yyjson_get_bool(additional_properties));

    yyjson_doc_free(doc);
    free(json);
    PASS();
}

/* Guard for PR #560 (schema enum): the get_architecture aspects items schema
 * must carry an enum of the valid tokens — including the new "overview" —
 * mirroring VALID_ASPECTS in mcp.c. Parsed structurally like
 * mcp_ingest_traces_items_disallow_additional_properties_issue731. */
TEST(mcp_get_architecture_aspects_schema_enum_pr560) {
    char *json = cbm_mcp_tools_list();
    ASSERT_NOT_NULL(json);

    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *root = yyjson_doc_get_root(doc);
    ASSERT_NOT_NULL(root);
    yyjson_val *tools = yyjson_obj_get(root, "tools");
    ASSERT_NOT_NULL(tools);
    ASSERT_TRUE(yyjson_is_arr(tools));

    yyjson_val *tool;
    yyjson_arr_iter iter;
    yyjson_arr_iter_init(tools, &iter);
    yyjson_val *get_arch = NULL;
    while ((tool = yyjson_arr_iter_next(&iter)) != NULL) {
        yyjson_val *name = yyjson_obj_get(tool, "name");
        if (name && yyjson_is_str(name) && strcmp(yyjson_get_str(name), "get_architecture") == 0) {
            get_arch = tool;
            break;
        }
    }
    ASSERT_NOT_NULL(get_arch);

    yyjson_val *input_schema = yyjson_obj_get(get_arch, "inputSchema");
    ASSERT_NOT_NULL(input_schema);
    yyjson_val *properties = yyjson_obj_get(input_schema, "properties");
    ASSERT_NOT_NULL(properties);
    yyjson_val *aspects = yyjson_obj_get(properties, "aspects");
    ASSERT_NOT_NULL(aspects);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(aspects, "type")), "array");
    yyjson_val *items = yyjson_obj_get(aspects, "items");
    ASSERT_NOT_NULL(items);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(items, "type")), "string");
    yyjson_val *enum_arr = yyjson_obj_get(items, "enum");
    ASSERT_NOT_NULL(enum_arr);
    ASSERT_TRUE(yyjson_is_arr(enum_arr));

    /* The enum must be exactly the valid-token set — no more, no less. */
    static const char *expected[] = {"all",        "overview",   "structure", "dependencies",
                                     "routes",     "languages",  "packages",  "entry_points",
                                     "hotspots",   "boundaries", "layers",    "file_tree",
                                     "clusters"};
    size_t expected_count = sizeof(expected) / sizeof(expected[0]);
    ASSERT_EQ(yyjson_arr_size(enum_arr), expected_count);
    for (size_t i = 0; i < expected_count; i++) {
        bool found = false;
        yyjson_val *ev;
        yyjson_arr_iter eiter;
        yyjson_arr_iter_init(enum_arr, &eiter);
        while ((ev = yyjson_arr_iter_next(&eiter)) != NULL) {
            if (yyjson_is_str(ev) && strcmp(yyjson_get_str(ev), expected[i]) == 0) {
                found = true;
                break;
            }
        }
        ASSERT_TRUE(found);
    }

    yyjson_doc_free(doc);
    free(json);
    PASS();
}

TEST(mcp_text_result) {
    char *json = cbm_mcp_text_result("{\"total\":5}", false);
    ASSERT_NOT_NULL(json);
    ASSERT_NOT_NULL(strstr(json, "\"type\":\"text\""));
    /* The text value is JSON-escaped inside the "text" field */
    ASSERT_NOT_NULL(strstr(json, "total"));
    ASSERT_NOT_NULL(strstr(json, "\"structuredContent\":{\"total\":5}"));
    ASSERT_NOT_NULL(strstr(json, "\"isError\":false"));
    ASSERT_NULL(strstr(json, "\"isError\":true"));
    free(json);
    PASS();
}

TEST(mcp_text_result_skips_structured_content_for_plain_text) {
    char *json = cbm_mcp_text_result("plain text", false);
    ASSERT_NOT_NULL(json);
    ASSERT_NULL(strstr(json, "\"structuredContent\""));
    ASSERT_NOT_NULL(strstr(json, "\"isError\":false"));
    free(json);
    PASS();
}

TEST(mcp_cancel_matches_request_id) {
    ASSERT_TRUE(cbm_mcp_cancel_request_matches("{\"requestId\":7}", 7, NULL));
    ASSERT_FALSE(cbm_mcp_cancel_request_matches("{\"requestId\":8}", 7, NULL));
    ASSERT_TRUE(cbm_mcp_cancel_request_matches("{\"requestId\":\"call-1\"}", -1, "call-1"));
    ASSERT_FALSE(cbm_mcp_cancel_request_matches("{\"requestId\":\"call-2\"}", -1, "call-1"));
    ASSERT_FALSE(cbm_mcp_cancel_request_matches("{\"requestId\":7}", -1, "7"));
    ASSERT_FALSE(cbm_mcp_cancel_request_matches("{}", 7, NULL));
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

TEST(server_handle_tools_list_paginates) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":200,\"method\":\"tools/list\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"id\":200"));
    ASSERT_NOT_NULL(strstr(resp, "\"nextCursor\":\"8\""));
    ASSERT_NOT_NULL(strstr(resp, "index_repository"));
    ASSERT_NULL(strstr(resp, "manage_adr"));
    free(resp);

    resp = cbm_mcp_server_handle(
        srv,
        "{\"jsonrpc\":\"2.0\",\"id\":201,\"method\":\"tools/list\",\"params\":{\"cursor\":\"8\"}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"id\":201"));
    ASSERT_NULL(strstr(resp, "\"nextCursor\""));
    ASSERT_NOT_NULL(strstr(resp, "manage_adr"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(server_handle_logs_request_without_params) {
    mcp_log_buf[0] = '\0';
    CBMLogLevel prev_level = cbm_log_get_level();
    cbm_log_set_level(CBM_LOG_DEBUG);
    cbm_log_set_format(CBM_LOG_FORMAT_TEXT);
    cbm_log_set_sink_ex(mcp_capture_log, CBM_LOG_SINK_REPLACE);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":210,\"method\":\"tools/list\","
                                   "\"params\":{\"token\":\"secret\"}}");
    ASSERT_NOT_NULL(resp);
    free(resp);
    cbm_mcp_server_free(srv);

    cbm_log_set_sink(NULL);
    cbm_log_set_level(prev_level);

    ASSERT_NOT_NULL(strstr(mcp_log_buf, "msg=mcp.request"));
    ASSERT_NOT_NULL(strstr(mcp_log_buf, "protocol=jsonrpc"));
    ASSERT_NOT_NULL(strstr(mcp_log_buf, "method=tools/list"));
    ASSERT_NOT_NULL(strstr(mcp_log_buf, "status=ok"));
    ASSERT_NULL(strstr(mcp_log_buf, "token"));
    ASSERT_NULL(strstr(mcp_log_buf, "secret"));
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

TEST(tool_search_graph_query_honors_file_pattern_issue552) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(st);

    const char *proj = "issue-552";
    cbm_mcp_server_set_project(srv, proj);
    cbm_store_upsert_project(st, proj, "/tmp/issue-552");

    cbm_node_t lib_status = {0};
    lib_status.project = proj;
    lib_status.label = "Function";
    lib_status.name = "status";
    lib_status.qualified_name = "issue-552.src.lib.status";
    lib_status.file_path = "src/lib/status.c";
    lib_status.start_line = 1;
    lib_status.end_line = 3;
    ASSERT_GT(cbm_store_upsert_node(st, &lib_status), 0);

    cbm_node_t component_status = {0};
    component_status.project = proj;
    component_status.label = "Function";
    component_status.name = "status";
    component_status.qualified_name = "issue-552.src.components.status";
    component_status.file_path = "src/components/status.c";
    component_status.start_line = 1;
    component_status.end_line = 3;
    ASSERT_GT(cbm_store_upsert_node(st, &component_status), 0);

    cbm_store_exec(st, "INSERT INTO nodes_fts(nodes_fts) VALUES('delete-all');");
    ASSERT_EQ(cbm_store_exec(st,
                             "INSERT INTO nodes_fts(rowid, name, qualified_name, label, "
                             "file_path) "
                             "SELECT id, cbm_camel_split(name), qualified_name, label, file_path "
                             "FROM nodes;"),
              CBM_STORE_OK);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":552,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"search_graph\","
                                   "\"arguments\":{\"project\":\"issue-552\",\"query\":\"status\","
                                   "\"file_pattern\":\"src/lib/*\",\"limit\":10}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "\"search_mode\":\"bm25\""));
    ASSERT_NOT_NULL(strstr(inner, "\"file_path\":\"src/lib/status.c\""));
    ASSERT_NULL(strstr(inner, "src/components/status.c"));

    free(inner);
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

TEST(tool_index_status_includes_git_metadata) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":16,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"index_status\","
                                   "\"arguments\":{\"project\":\"test-project\"}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "\"root_path\""));
    ASSERT_NOT_NULL(strstr(inner, "\"git\""));
    ASSERT_NOT_NULL(strstr(inner, "\"is_git\":false"));
    ASSERT_NOT_NULL(strstr(inner, "\"root_exists\":true"));

    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
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

/* Regression: two same-named definitions with equal rank must be reported
 * ambiguous, not silently traced (trace_path previously took nodes[0]). */
TEST(tool_trace_call_path_ambiguous) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    const char *proj = "amb-proj";
    cbm_mcp_server_set_project(srv, proj);
    cbm_store_upsert_project(st, proj, "/tmp/amb");
    cbm_node_t a = {.project = proj,
                    .label = "Function",
                    .name = "amb",
                    .qualified_name = "amb-proj.a.amb",
                    .file_path = "a.c",
                    .start_line = 10,
                    .end_line = 20};
    cbm_node_t b = {.project = proj,
                    .label = "Function",
                    .name = "amb",
                    .qualified_name = "amb-proj.b.amb",
                    .file_path = "b.c",
                    .start_line = 10,
                    .end_line = 20}; /* equal span -> genuine tie */
    ASSERT_GT(cbm_store_upsert_node(st, &a), 0);
    ASSERT_GT(cbm_store_upsert_node(st, &b), 0);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":61,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"trace_call_path\","
             "\"arguments\":{\"function_name\":\"amb\",\"project\":\"amb-proj\"}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "ambiguous"));
    ASSERT_NOT_NULL(strstr(inner, "suggestions"));
    ASSERT_NULL(strstr(inner, "\"callees\""));
    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* Regression: when same-named nodes differ in rank, trace must pick the real
 * definition (callable, larger body) — NOT nodes[0]. The Module is inserted
 * first; if trace took nodes[0] the outbound trace would be empty. */
TEST(tool_trace_call_path_prefers_definition) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    const char *proj = "pref-proj";
    cbm_mcp_server_set_project(srv, proj);
    cbm_store_upsert_project(st, proj, "/tmp/pref");
    /* nodes[0]: the WRONG match (a Module, tiny span), inserted first. */
    cbm_node_t wrong = {.project = proj,
                        .label = "Module",
                        .name = "dup",
                        .qualified_name = "pref-proj.dup",
                        .file_path = "dup.x",
                        .start_line = 1,
                        .end_line = 1};
    /* the real definition: a Function with a body. */
    cbm_node_t def = {.project = proj,
                      .label = "Function",
                      .name = "dup",
                      .qualified_name = "pref-proj.src.dup",
                      .file_path = "src/dup.c",
                      .start_line = 10,
                      .end_line = 50};
    cbm_node_t callee = {.project = proj,
                         .label = "Function",
                         .name = "callee",
                         .qualified_name = "pref-proj.src.callee",
                         .file_path = "src/dup.c",
                         .start_line = 60,
                         .end_line = 70};
    ASSERT_GT(cbm_store_upsert_node(st, &wrong), 0);
    int64_t id_def = cbm_store_upsert_node(st, &def);
    int64_t id_callee = cbm_store_upsert_node(st, &callee);
    ASSERT_GT(id_def, 0);
    ASSERT_GT(id_callee, 0);
    cbm_edge_t e = {.project = proj, .source_id = id_def, .target_id = id_callee, .type = "CALLS"};
    cbm_store_insert_edge(st, &e);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":62,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"trace_call_path\",\"arguments\":{\"function_name\":\"dup\","
             "\"project\":\"pref-proj\",\"direction\":\"outbound\"}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NULL(strstr(inner, "ambiguous"));
    /* picked the Function definition -> its outbound CALLS edge to "callee" shows */
    ASSERT_NOT_NULL(strstr(inner, "callee"));
    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* Reproduce-first (#650, distilled): two GENUINELY-DIFFERENT same-named functions
 * whose bodies differ in length score differently, so the old exact-tie check did
 * not flag them ambiguous — and bfs_union_same_name (#546) then merged the caller
 * sets of both into one confidently-conflated answer (the mirror of #546's under-
 * report). The fix: 2+ real callable defs => ambiguous (disambiguate), never union
 * distinct symbols. RED before the pick_resolved_node real_def_count rule (response
 * merged callerA+callerB), GREEN after (response is ambiguous, no "callers"). */
TEST(tool_trace_call_path_distinct_defs_not_over_unioned) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    const char *proj = "ou-proj";
    cbm_mcp_server_set_project(srv, proj);
    cbm_store_upsert_project(st, proj, "/tmp/ou");
    /* two unrelated real definitions of "dupreal", DIFFERENT body spans */
    cbm_node_t da = {.project = proj, .label = "Function", .name = "dupreal",
                     .qualified_name = "ou-proj.a.dupreal", .file_path = "a.c",
                     .start_line = 10, .end_line = 20}; /* span 10 */
    cbm_node_t db = {.project = proj, .label = "Function", .name = "dupreal",
                     .qualified_name = "ou-proj.b.dupreal", .file_path = "b.c",
                     .start_line = 10, .end_line = 40}; /* span 30 (no tie) */
    cbm_node_t ca = {.project = proj, .label = "Function", .name = "callerA",
                     .qualified_name = "ou-proj.a.callerA", .file_path = "a.c",
                     .start_line = 30, .end_line = 40};
    cbm_node_t cb = {.project = proj, .label = "Function", .name = "callerB",
                     .qualified_name = "ou-proj.b.callerB", .file_path = "b.c",
                     .start_line = 50, .end_line = 60};
    int64_t id_da = cbm_store_upsert_node(st, &da);
    int64_t id_db = cbm_store_upsert_node(st, &db);
    int64_t id_ca = cbm_store_upsert_node(st, &ca);
    int64_t id_cb = cbm_store_upsert_node(st, &cb);
    ASSERT_GT(id_da, 0);
    ASSERT_GT(id_db, 0);
    ASSERT_GT(id_ca, 0);
    ASSERT_GT(id_cb, 0);
    cbm_edge_t ea = {.project = proj, .source_id = id_ca, .target_id = id_da, .type = "CALLS"};
    cbm_edge_t eb = {.project = proj, .source_id = id_cb, .target_id = id_db, .type = "CALLS"};
    cbm_store_insert_edge(st, &ea);
    cbm_store_insert_edge(st, &eb);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":63,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"trace_call_path\",\"arguments\":{\"function_name\":\"dupreal\","
             "\"project\":\"ou-proj\",\"direction\":\"inbound\"}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    /* distinct symbols must be disambiguated, not merged into one caller set */
    ASSERT_NOT_NULL(strstr(inner, "ambiguous"));
    ASSERT_NOT_NULL(strstr(inner, "suggestions"));
    ASSERT_NULL(strstr(inner, "\"callers\""));
    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* Guard that the ambiguity gate does NOT regress the #546 fix: a real .ts
 * implementation plus a body-less ambient .d.ts stub is ONE logical symbol
 * (one real callable def + a fragment), so it must stay non-ambiguous and the
 * caller sets from both nodes must be unioned. */
TEST(tool_trace_call_path_dts_stub_unions_with_impl) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    const char *proj = "dts-proj";
    cbm_mcp_server_set_project(srv, proj);
    cbm_store_upsert_project(st, proj, "/tmp/dts");
    cbm_node_t impl = {.project = proj, .label = "Function", .name = "sym546",
                       .qualified_name = "dts-proj.impl.sym546", .file_path = "src/sym.ts",
                       .start_line = 10, .end_line = 30}; /* real body */
    cbm_node_t stub = {.project = proj, .label = "Function", .name = "sym546",
                       .qualified_name = "dts-proj.stub.sym546", .file_path = "types/sym.d.ts",
                       .start_line = 5, .end_line = 5}; /* body-less ambient decl */
    cbm_node_t crel = {.project = proj, .label = "Function", .name = "callerRel",
                       .qualified_name = "dts-proj.callerRel", .file_path = "src/rel.ts",
                       .start_line = 1, .end_line = 8};
    cbm_node_t cali = {.project = proj, .label = "Function", .name = "callerAlias",
                       .qualified_name = "dts-proj.callerAlias", .file_path = "src/ali.ts",
                       .start_line = 1, .end_line = 8};
    int64_t id_impl = cbm_store_upsert_node(st, &impl);
    int64_t id_stub = cbm_store_upsert_node(st, &stub);
    int64_t id_crel = cbm_store_upsert_node(st, &crel);
    int64_t id_cali = cbm_store_upsert_node(st, &cali);
    ASSERT_GT(id_impl, 0);
    ASSERT_GT(id_stub, 0);
    ASSERT_GT(id_crel, 0);
    ASSERT_GT(id_cali, 0);
    /* callers split by import style: relative -> impl, path-alias -> stub */
    cbm_edge_t er = {.project = proj, .source_id = id_crel, .target_id = id_impl, .type = "CALLS"};
    cbm_edge_t el = {.project = proj, .source_id = id_cali, .target_id = id_stub, .type = "CALLS"};
    cbm_store_insert_edge(st, &er);
    cbm_store_insert_edge(st, &el);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":64,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"trace_call_path\",\"arguments\":{\"function_name\":\"sym546\","
             "\"project\":\"dts-proj\",\"direction\":\"inbound\"}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NULL(strstr(inner, "ambiguous"));
    /* union across impl + stub: BOTH callers appear (this is the #546 fix) */
    ASSERT_NOT_NULL(strstr(inner, "callerRel"));
    ASSERT_NOT_NULL(strstr(inner, "callerAlias"));
    free(inner);
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

/* Distills PR #560 (overview subset): "overview" must expand to a compact
 * subset — every aspect EXCEPT file_tree. Before the fix, "overview" was not
 * registered in either aspect gate (want_aspect in store.c, aspect_wanted in
 * mcp.c), so aspects=["overview"] silently degraded to just
 * {total_nodes,total_edges}. RED on unfixed code: no "entry_points" key. */
TEST(tool_get_architecture_overview_compact_subset_pr560) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    cbm_store_t *st = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(st);

    const char *proj = "arch560";
    cbm_mcp_server_set_project(srv, proj);
    cbm_store_upsert_project(st, proj, "/tmp/arch560");

    cbm_node_t main_fn = {0};
    main_fn.project = proj;
    main_fn.label = "Function";
    main_fn.name = "main";
    main_fn.qualified_name = "arch560.cmd.main";
    main_fn.file_path = "cmd/main.go";
    main_fn.start_line = 1;
    main_fn.end_line = 3;
    main_fn.properties_json = "{\"is_entry_point\":true}";
    ASSERT_GT(cbm_store_upsert_node(st, &main_fn), 0);

    /* A File node so the file_tree aspect has real content — makes the
     * "overview drops file_tree" assertion below non-vacuous. */
    cbm_node_t file_node = {.project = proj,
                            .label = "File",
                            .name = "main.go",
                            .qualified_name = "arch560.cmd.main.go",
                            .file_path = "cmd/main.go"};
    ASSERT_GT(cbm_store_upsert_node(st, &file_node), 0);

    /* Sanity: with "all", both entry_points and file_tree surface. */
    char *resp_all = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":560,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"get_architecture\","
             "\"arguments\":{\"project\":\"arch560\",\"aspects\":[\"all\"]}}}");
    ASSERT_NOT_NULL(resp_all);
    char *inner_all = extract_text_content(resp_all);
    ASSERT_NOT_NULL(inner_all);
    ASSERT_NOT_NULL(strstr(inner_all, "\"entry_points\""));
    ASSERT_NOT_NULL(strstr(inner_all, "\"file_tree\""));
    free(inner_all);
    free(resp_all);

    /* "overview": substantive content (entry_points, node_labels) but NO
     * file_tree section. */
    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":561,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"get_architecture\","
             "\"arguments\":{\"project\":\"arch560\",\"aspects\":[\"overview\"]}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "\"entry_points\""));
    ASSERT_NOT_NULL(strstr(inner, "\"node_labels\""));
    ASSERT_NULL(strstr(inner, "\"file_tree\""));

    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* Distills PR #560 (server-side validation): unknown aspect tokens must be
 * rejected with an isError result listing the valid values. Before the fix
 * the JSON-Schema accepted any string and both aspect gates simply never
 * matched, so a typo like "bogus_aspect" produced a silent near-empty payload
 * with isError:false. RED on unfixed code: no isError, no "Unknown aspect". */
TEST(tool_get_architecture_rejects_unknown_aspect_pr560) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    cbm_store_t *st = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(st);

    const char *proj = "arch560v";
    cbm_mcp_server_set_project(srv, proj);
    cbm_store_upsert_project(st, proj, "/tmp/arch560v");

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":562,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"get_architecture\","
             "\"arguments\":{\"project\":\"arch560v\",\"aspects\":[\"bogus_aspect\"]}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"isError\":true"));
    ASSERT_NOT_NULL(strstr(resp, "Unknown aspect 'bogus_aspect'"));
    /* The error must teach the valid vocabulary, including the new token. */
    ASSERT_NOT_NULL(strstr(resp, "overview"));
    ASSERT_NOT_NULL(strstr(resp, "file_tree"));

    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* Reproduce-first for #640: query handlers must accept the `project_name`
 * alias, not only the canonical `project` key. list_projects surfaces the field
 * as "name" and the error hint says "pass the project name", so a caller
 * naturally passes `project_name`. With no alias, the handler reads key
 * "project" -> NULL -> resolve_store bails before opening any .db -> "project
 * not found or not indexed" even though the project is indexed. Mirrors
 * tool_get_architecture_emits_populated_sections but with the alias key. */
TEST(tool_get_architecture_accepts_project_name_alias_issue640) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    cbm_store_t *st = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(st);

    const char *proj = "alias640";
    cbm_mcp_server_set_project(srv, proj);
    cbm_store_upsert_project(st, proj, "/tmp/alias640");

    cbm_node_t main_fn = {0};
    main_fn.project = proj;
    main_fn.label = "Function";
    main_fn.name = "main";
    main_fn.qualified_name = "alias640.cmd.main";
    main_fn.file_path = "cmd/main.go";
    main_fn.start_line = 1;
    main_fn.end_line = 3;
    main_fn.properties_json = "{\"is_entry_point\":true}";
    ASSERT_GT(cbm_store_upsert_node(st, &main_fn), 0);

    /* Caller passes `project_name` (the natural guess) instead of `project`. */
    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":640,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"get_architecture\","
             "\"arguments\":{\"project_name\":\"alias640\",\"aspects\":[\"all\"]}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);

    /* RED before the alias: inner is the "project not found" error.
     * GREEN after: the alias resolves and architecture sections surface. */
    ASSERT_NULL(strstr(inner, "project not found"));
    ASSERT_NOT_NULL(strstr(inner, "\"entry_points\""));

    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* Reproduce-first for #640: the alias must apply across query handlers, not
 * just get_architecture. search_graph with `project_name` must resolve too. */
TEST(tool_search_graph_accepts_project_name_alias_issue640) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    cbm_store_t *st = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(st);

    const char *proj = "alias640b";
    cbm_mcp_server_set_project(srv, proj);
    cbm_store_upsert_project(st, proj, "/tmp/alias640b");

    cbm_node_t fn = {0};
    fn.project = proj;
    fn.label = "Function";
    fn.name = "WidgetHandler";
    fn.qualified_name = "alias640b.svc.WidgetHandler";
    fn.file_path = "svc/widget.go";
    fn.start_line = 1;
    fn.end_line = 2;
    ASSERT_GT(cbm_store_upsert_node(st, &fn), 0);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":641,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"search_graph\","
             "\"arguments\":{\"project_name\":\"alias640b\",\"name_pattern\":\"Widget.*\"}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);

    ASSERT_NULL(strstr(inner, "project not found"));
    ASSERT_NOT_NULL(strstr(inner, "WidgetHandler"));

    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* Regression for #604: path scopes architecture totals and content. */
TEST(tool_get_architecture_path_scoping) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    cbm_store_t *st = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(st);

    const char *proj = "arch-path";
    cbm_mcp_server_set_project(srv, proj);
    cbm_store_upsert_project(st, proj, "/tmp/arch-path");

    cbm_node_t pkg_global = {.project = proj,
                             .label = "Package",
                             .name = "Django",
                             .qualified_name = "arch-path.Django",
                             .file_path = "vendor/django/__init__.py"};
    cbm_store_upsert_node(st, &pkg_global);

    cbm_node_t pkg_local = {.project = proj,
                            .label = "Package",
                            .name = "hoa",
                            .qualified_name = "arch-path.hoa",
                            .file_path = "apps/hoa/main.go"};
    cbm_store_upsert_node(st, &pkg_local);

    cbm_node_t f_hoa = {.project = proj,
                        .label = "File",
                        .name = "main.go",
                        .qualified_name = "arch-path.apps.hoa.main.go",
                        .file_path = "apps/hoa/main.go"};
    cbm_store_upsert_node(st, &f_hoa);

    cbm_node_t f_other = {.project = proj,
                          .label = "File",
                          .name = "other.go",
                          .qualified_name = "arch-path.other.go",
                          .file_path = "lib/other.go"};
    cbm_store_upsert_node(st, &f_other);

    char *resp_root = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":92,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"get_architecture\","
             "\"arguments\":{\"project\":\"arch-path\",\"aspects\":[\"packages\"]}}}");
    ASSERT_NOT_NULL(resp_root);
    char *inner_root = extract_text_content(resp_root);
    ASSERT_NOT_NULL(inner_root);
    ASSERT_NOT_NULL(strstr(inner_root, "Django"));

    char *resp_scoped =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":93,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"get_architecture\","
                                   "\"arguments\":{\"project\":\"arch-path\",\"path\":\"apps/hoa\","
                                   "\"aspects\":[\"packages\"]}}}");
    ASSERT_NOT_NULL(resp_scoped);
    char *inner_scoped = extract_text_content(resp_scoped);
    ASSERT_NOT_NULL(inner_scoped);

    ASSERT_NOT_NULL(strstr(inner_scoped, "root_total_nodes"));
    ASSERT_NOT_NULL(strstr(inner_scoped, "scoped_total_nodes"));
    ASSERT_NOT_NULL(strstr(inner_scoped, "\"path\""));
    ASSERT_NOT_NULL(strstr(inner_scoped, "hoa"));
    ASSERT_NULL(strstr(inner_scoped, "Django"));

    int root_nodes = 0;
    int scoped_nodes = 0;
    const char *rt = strstr(inner_scoped, "\"root_total_nodes\":");
    const char *stn = strstr(inner_scoped, "\"scoped_total_nodes\":");
    if (rt) {
        sscanf(rt, "\"root_total_nodes\":%d", &root_nodes);
    }
    if (stn) {
        sscanf(stn, "\"scoped_total_nodes\":%d", &scoped_nodes);
    }
    ASSERT_TRUE(root_nodes > scoped_nodes);
    ASSERT_TRUE(scoped_nodes > 0);

    free(inner_scoped);
    free(resp_scoped);
    free(inner_root);
    free(resp_root);
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

/* Reproduce-first (#687): scoped content search over a repo whose ROOT PATH
 * contains a space. write_scoped_filelist emits "<root>/<file>" records that the
 * Unix pipeline pipes to grep via xargs. With plain `xargs` (newline-split) the
 * space splits one path into several bogus args -> grep finds nothing ->
 * total_grep_matches == 0 (RED on the unfixed code). The fix writes NUL-separated
 * records + uses `xargs -0`, so the path stays a single argument -> match found
 * (GREEN). On Windows the scoped path uses PowerShell Get-Content -LiteralPath,
 * which already handles spaces, so this asserts correct behavior there too. */
TEST(search_code_scoped_path_with_spaces_issue687) {
    char tmp[512];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_srch_space_XXXXXX");
    if (!cbm_mkdtemp(tmp)) {
        FAIL("cbm_mkdtemp failed");
    }

    /* Project root deliberately contains a space. */
    char proj_dir[640];
    snprintf(proj_dir, sizeof(proj_dir), "%s/my project", tmp);
    cbm_mkdir(proj_dir);

    char src_path[768];
    snprintf(src_path, sizeof(src_path), "%s/main.go", proj_dir);
    FILE *fp = fopen(src_path, "w");
    if (!fp) {
        rmdir(proj_dir);
        rmdir(tmp);
        FAIL("cannot write source file under spaced path");
    }
    fprintf(fp, "package main\n\nfunc HandleRequest() error {\n\treturn nil\n}\n");
    fclose(fp);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(st);
    const char *proj = "space-search";
    cbm_mcp_server_set_project(srv, proj);
    cbm_store_upsert_project(st, proj, proj_dir);

    /* A node so the file is "indexed" (cbm_store_list_files -> scoped grep path)
     * and the grep hit classifies to a result. */
    cbm_node_t n = {.project = proj,
                    .label = "Function",
                    .name = "HandleRequest",
                    .qualified_name = "space-search.main.HandleRequest",
                    .file_path = "main.go",
                    .start_line = 3,
                    .end_line = 5};
    ASSERT_GT(cbm_store_upsert_node(st, &n), 0);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":94,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"search_code\","
             "\"arguments\":{\"pattern\":\"HandleRequest\",\"project\":\"space-search\"}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);

    /* grep must have found the match despite the space in the root path. */
    int grep_matches = -1;
    const char *g = strstr(inner, "\"total_grep_matches\":");
    if (g) {
        sscanf(g, "\"total_grep_matches\":%d", &grep_matches);
    }
    ASSERT_TRUE(grep_matches > 0);

    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    unlink(src_path);
    rmdir(proj_dir);
    rmdir(tmp);
    PASS();
}

/* Shared fixture for the path_filter prefilter tests (PR #756 distilled):
 * a project with two indexed files that both contain the search pattern —
 * src/handler.go (inside the filter) and vendor/other.go (outside it). */
static cbm_mcp_server_t *setup_prefilter_server(char *tmp, size_t tmp_sz, char *src_path,
                                                size_t src_sz, char *vendor_path, size_t vendor_sz) {
    snprintf(tmp, tmp_sz, "/tmp/cbm_srch_pref_XXXXXX");
    if (!cbm_mkdtemp(tmp)) {
        return NULL;
    }
    char dir[640];
    snprintf(dir, sizeof(dir), "%s/src", tmp);
    cbm_mkdir(dir);
    snprintf(dir, sizeof(dir), "%s/vendor", tmp);
    cbm_mkdir(dir);

    snprintf(src_path, src_sz, "%s/src/handler.go", tmp);
    snprintf(vendor_path, vendor_sz, "%s/vendor/other.go", tmp);
    FILE *fp = fopen(src_path, "w");
    if (!fp) {
        return NULL;
    }
    fprintf(fp, "package main\n\nfunc HandleRequest() error {\n\treturn nil\n}\n");
    fclose(fp);
    fp = fopen(vendor_path, "w");
    if (!fp) {
        return NULL;
    }
    fprintf(fp, "package vendored\n\nfunc HandleRequest() error {\n\treturn nil\n}\n");
    fclose(fp);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    if (!srv) {
        return NULL;
    }
    cbm_store_t *st = cbm_mcp_server_store(srv);
    const char *proj = "prefilter-search";
    cbm_mcp_server_set_project(srv, proj);
    cbm_store_upsert_project(st, proj, tmp);

    cbm_node_t n1 = {.project = proj,
                     .label = "Function",
                     .name = "HandleRequest",
                     .qualified_name = "prefilter-search.main.HandleRequest",
                     .file_path = "src/handler.go",
                     .start_line = 3,
                     .end_line = 5};
    cbm_node_t n2 = {.project = proj,
                     .label = "Function",
                     .name = "HandleRequest",
                     .qualified_name = "prefilter-search.vendored.HandleRequest",
                     .file_path = "vendor/other.go",
                     .start_line = 3,
                     .end_line = 5};
    if (cbm_store_upsert_node(st, &n1) <= 0 || cbm_store_upsert_node(st, &n2) <= 0) {
        cbm_mcp_server_free(srv);
        return NULL;
    }
    return srv;
}

static void cleanup_prefilter_dir(const char *tmp, const char *src_path, const char *vendor_path) {
    char dir[640];
    unlink(src_path);
    unlink(vendor_path);
    snprintf(dir, sizeof(dir), "%s/src", tmp);
    rmdir(dir);
    snprintf(dir, sizeof(dir), "%s/vendor", tmp);
    rmdir(dir);
    rmdir(tmp);
}

/* PR #756 (distilled): scoped search_code prefilters the indexed filelist by
 * path_filter before grep runs. POSITIVE invariant guard: a path_filter that
 * matches the file containing the hit must still return that hit (guards
 * against over-filtering — the prefilter predicate must stay IDENTICAL to the
 * post-grep filter in collect_grep_matches), and files outside the filter
 * stay excluded. Green on pre-prefilter main too (the post-grep filter alone
 * produced the same results): the change is results-preserving perf-only. */
TEST(search_code_path_filter_prefilter_keeps_matches) {
    char tmp[512], src_path[768], vendor_path[768];
    cbm_mcp_server_t *srv = setup_prefilter_server(tmp, sizeof(tmp), src_path, sizeof(src_path),
                                                   vendor_path, sizeof(vendor_path));
    ASSERT_NOT_NULL(srv);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":95,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"search_code\","
             "\"arguments\":{\"pattern\":\"HandleRequest\",\"project\":\"prefilter-search\","
             "\"path_filter\":\"^src/\"}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_TRUE(strstr(resp, "\"isError\":true") == NULL);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);

    /* The in-filter hit is returned; the out-of-filter file is not. */
    ASSERT_NOT_NULL(strstr(inner, "src/handler.go"));
    ASSERT_TRUE(strstr(inner, "vendor/other.go") == NULL);

    /* Exactly the one in-filter grep match survives (same count before and
     * after the prefilter — predicate identity). */
    int grep_matches = -1;
    const char *g = strstr(inner, "\"total_grep_matches\":");
    if (g) {
        sscanf(g, "\"total_grep_matches\":%d", &grep_matches);
    }
    ASSERT_EQ(grep_matches, 1);

    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_prefilter_dir(tmp, src_path, vendor_path);
    PASS();
}

/* PR #756 (distilled): path_filter matching ZERO indexed files. With the
 * prefilter the scoped filelist has 0 records, and handle_search_code now
 * skips the grep subprocess entirely (xargs on an empty filelist is
 * platform-dependent: GNU execs grep once with no operands, BSD skips) and
 * returns the empty result directly. Must be a clean zero-result response —
 * no error. Green on pre-prefilter main too (there the full filelist is
 * grepped and the post-grep filter drops every hit — an empty filelist is
 * unreachable on main): guards the edge the prefilter introduces. */
TEST(search_code_path_filter_matches_nothing) {
    char tmp[512], src_path[768], vendor_path[768];
    cbm_mcp_server_t *srv = setup_prefilter_server(tmp, sizeof(tmp), src_path, sizeof(src_path),
                                                   vendor_path, sizeof(vendor_path));
    ASSERT_NOT_NULL(srv);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":96,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"search_code\","
             "\"arguments\":{\"pattern\":\"HandleRequest\",\"project\":\"prefilter-search\","
             "\"path_filter\":\"^no_such_dir/\"}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_TRUE(strstr(resp, "\"isError\":true") == NULL);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);

    int grep_matches = -1;
    const char *g = strstr(inner, "\"total_grep_matches\":");
    if (g) {
        sscanf(g, "\"total_grep_matches\":%d", &grep_matches);
    }
    ASSERT_EQ(grep_matches, 0);
    int results = -1;
    const char *r = strstr(inner, "\"total_results\":");
    if (r) {
        sscanf(r, "\"total_results\":%d", &results);
    }
    ASSERT_EQ(results, 0);
    ASSERT_TRUE(strstr(inner, "handler.go") == NULL);
    ASSERT_TRUE(strstr(inner, "other.go") == NULL);

    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_prefilter_dir(tmp, src_path, vendor_path);
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
    ASSERT_NOT_NULL(strstr(resp, "missing required argument: project"));
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
    ASSERT_NOT_NULL(strstr(resp, "missing required argument: project"));
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
    ASSERT_NULL(strstr(resp, "\"isError\":true"));
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
    ASSERT_NULL(strstr(resp, "\"isError\":true"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_index_repository_reports_store_backed_adr) {
    char tmp_dir[256];
    snprintf(tmp_dir, sizeof(tmp_dir), "/tmp/cbm-index-adr-test-XXXXXX");
    if (!cbm_mkdtemp(tmp_dir)) {
        PASS();
    }
    char cache[256];
    snprintf(cache, sizeof(cache), "/tmp/cbm-index-adr-cache-XXXXXX");
    if (!cbm_mkdtemp(cache)) {
        cbm_rmdir(tmp_dir);
        PASS();
    }

    const char *saved = getenv("CBM_CACHE_DIR");
    char *saved_copy = saved ? strdup(saved) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    char src_path[512];
    snprintf(src_path, sizeof(src_path), "%s/main.py", tmp_dir);
    FILE *fp = fopen(src_path, "w");
    ASSERT_NOT_NULL(fp);
    fputs("def main():\n    return 'ok'\n", fp);
    fclose(fp);

    char *project = cbm_project_name_from_path(tmp_dir);
    ASSERT_NOT_NULL(project);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    char args[1024];
    snprintf(args, sizeof(args), "{\"repo_path\":\"%s\",\"mode\":\"fast\"}", tmp_dir);
    char *resp = cbm_mcp_handle_tool(srv, "index_repository", args);
    ASSERT_NOT_NULL(resp);
    ASSERT(response_contains_json_fragment(resp, "\"status\":\"indexed\""));
    free(resp);

    char update_args[2048];
    snprintf(update_args, sizeof(update_args),
             "{\"project\":\"%s\",\"mode\":\"update\",\"content\":\"## PURPOSE\\n"
             "Store-backed ADR metadata.\\n\"}",
             project);
    resp = cbm_mcp_handle_tool(srv, "manage_adr", update_args);
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "updated"));
    free(resp);

    resp = cbm_mcp_handle_tool(srv, "index_repository", args);
    ASSERT_NOT_NULL(resp);
    ASSERT(response_contains_json_fragment(resp, "\"status\":\"indexed\""));
    ASSERT(response_contains_json_fragment(resp, "\"adr_present\":true"));
    ASSERT_NULL(strstr(resp, "adr_hint"));
    free(resp);

    char get_args[512];
    snprintf(get_args, sizeof(get_args), "{\"project\":\"%s\",\"mode\":\"get\"}", project);
    resp = cbm_mcp_handle_tool(srv, "manage_adr", get_args);
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "Store-backed ADR metadata."));
    ASSERT_NULL(strstr(resp, "no_adr"));
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_project_db(cache, project);
    restore_cache_dir(saved_copy);
    free(saved_copy);
    free(project);
    remove(src_path);
    cbm_rmdir(cache);
    cbm_rmdir(tmp_dir);
    PASS();
}

TEST(tool_index_repository_dot_uses_absolute_project_key_and_preserves_adr) {
    char tmp_dir[256];
    snprintf(tmp_dir, sizeof(tmp_dir), "/tmp/cbm-index-dot-adr-test-XXXXXX");
    if (!cbm_mkdtemp(tmp_dir)) {
        PASS();
    }
    char cache[256];
    snprintf(cache, sizeof(cache), "/tmp/cbm-index-dot-cache-XXXXXX");
    if (!cbm_mkdtemp(cache)) {
        cbm_rmdir(tmp_dir);
        PASS();
    }

    const char *saved = getenv("CBM_CACHE_DIR");
    char *saved_copy = saved ? strdup(saved) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    char src_path[512];
    snprintf(src_path, sizeof(src_path), "%s/main.py", tmp_dir);
    FILE *fp = fopen(src_path, "w");
    ASSERT_NOT_NULL(fp);
    fputs("def main():\n    return helper()\n\ndef helper():\n    return 1\n", fp);
    fclose(fp);

    char old_cwd[CBM_SZ_4K];
    ASSERT_NOT_NULL(cbm_getcwd(old_cwd, sizeof(old_cwd)));

    char *project = cbm_project_name_from_path(tmp_dir);
    ASSERT_NOT_NULL(project);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    ASSERT_EQ(cbm_chdir(tmp_dir), 0);
    char *resp =
        cbm_mcp_handle_tool(srv, "index_repository", "{\"repo_path\":\".\",\"mode\":\"fast\"}");
    ASSERT_EQ(cbm_chdir(old_cwd), 0);
    ASSERT_NOT_NULL(resp);
    if (!response_contains_json_fragment(resp, "\"status\":\"indexed\"")) {
        free(resp);
        cbm_mcp_server_free(srv);
        cleanup_project_db(cache, project);
        restore_cache_dir(saved_copy);
        free(saved_copy);
        free(project);
        remove(src_path);
        cbm_rmdir(cache);
        cbm_rmdir(tmp_dir);
        PASS();
    }
    ASSERT_NOT_NULL(strstr(resp, project));
    ASSERT(!response_contains_json_fragment(resp, "\"project\":\"root\""));
    free(resp);

    char update_args[2048];
    snprintf(update_args, sizeof(update_args),
             "{\"project\":\"%s\",\"mode\":\"update\",\"content\":\"## PURPOSE\\n"
             "Dot-path ADR marker.\\n\"}",
             project);
    resp = cbm_mcp_handle_tool(srv, "manage_adr", update_args);
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "updated"));
    free(resp);

    ASSERT_EQ(cbm_chdir(tmp_dir), 0);
    resp = cbm_mcp_handle_tool(srv, "index_repository", "{\"repo_path\":\".\",\"mode\":\"fast\"}");
    ASSERT_EQ(cbm_chdir(old_cwd), 0);
    ASSERT_NOT_NULL(resp);
    ASSERT(response_contains_json_fragment(resp, "\"status\":\"indexed\""));
    ASSERT_NOT_NULL(strstr(resp, project));
    ASSERT(response_contains_json_fragment(resp, "\"adr_present\":true"));
    ASSERT(!response_contains_json_fragment(resp, "\"project\":\"root\""));
    free(resp);

    char get_args[512];
    snprintf(get_args, sizeof(get_args), "{\"project\":\"%s\",\"mode\":\"get\"}", project);
    resp = cbm_mcp_handle_tool(srv, "manage_adr", get_args);
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "Dot-path ADR marker."));
    ASSERT_NULL(strstr(resp, "no_adr"));
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_project_db(cache, project);
    restore_cache_dir(saved_copy);
    free(saved_copy);
    free(project);
    remove(src_path);
    cbm_rmdir(cache);
    cbm_rmdir(tmp_dir);
    PASS();
}

TEST(tool_manage_adr_not_found_rich_error) {
    char cache[256];
    snprintf(cache, sizeof(cache), "/tmp/cbm-adr-missing-cache-XXXXXX");
    if (!cbm_mkdtemp(cache)) {
        PASS();
    }

    const char *saved = getenv("CBM_CACHE_DIR");
    char *saved_copy = saved ? strdup(saved) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    char *resp =
        cbm_mcp_handle_tool(srv, "manage_adr",
                            "{\"project\":\"cbm-no-such-project-zzz\",\"mode\":\"get\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "or not indexed"));
    ASSERT_NOT_NULL(strstr(resp, "hint"));
    free(resp);

    cbm_mcp_server_free(srv);
    restore_cache_dir(saved_copy);
    free(saved_copy);
    cbm_rmdir(cache);
    PASS();
}

TEST(tool_manage_adr_get_accepts_abs_path) {
    char tmp_dir[256];
    snprintf(tmp_dir, sizeof(tmp_dir), "/tmp/cbm-adr-abspath-XXXXXX");
    if (!cbm_mkdtemp(tmp_dir)) {
        PASS();
    }
    char cache[256];
    snprintf(cache, sizeof(cache), "/tmp/cbm-adr-abspath-cache-XXXXXX");
    if (!cbm_mkdtemp(cache)) {
        cbm_rmdir(tmp_dir);
        PASS();
    }

    const char *saved = getenv("CBM_CACHE_DIR");
    char *saved_copy = saved ? strdup(saved) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    char src_path[512];
    snprintf(src_path, sizeof(src_path), "%s/main.py", tmp_dir);
    FILE *fp = fopen(src_path, "w");
    ASSERT_NOT_NULL(fp);
    fputs("def main():\n    return 'ok'\n", fp);
    fclose(fp);

    char *project = cbm_project_name_from_path(tmp_dir);
    ASSERT_NOT_NULL(project);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    char args[1024];
    snprintf(args, sizeof(args), "{\"repo_path\":\"%s\",\"mode\":\"fast\"}", tmp_dir);
    char *resp = cbm_mcp_handle_tool(srv, "index_repository", args);
    ASSERT_NOT_NULL(resp);
    ASSERT(response_contains_json_fragment(resp, "\"status\":\"indexed\""));
    free(resp);

    char update_args[2048];
    snprintf(update_args, sizeof(update_args),
             "{\"project\":\"%s\",\"mode\":\"update\",\"content\":\"## PURPOSE\\n"
             "Abs-path normalization test.\\n\"}",
             project);
    resp = cbm_mcp_handle_tool(srv, "manage_adr", update_args);
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "updated"));
    free(resp);

    char get_args[512];
    snprintf(get_args, sizeof(get_args), "{\"project\":\"%s\",\"mode\":\"get\"}", tmp_dir);
    resp = cbm_mcp_handle_tool(srv, "manage_adr", get_args);
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "Abs-path normalization test."));
    ASSERT_NULL(strstr(resp, "or not indexed"));
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_project_db(cache, project);
    restore_cache_dir(saved_copy);
    free(saved_copy);
    free(project);
    remove(src_path);
    cbm_rmdir(cache);
    cbm_rmdir(tmp_dir);
    PASS();
}

TEST(tool_manage_adr_get_accepts_symlink_path) {
#ifdef _WIN32
    PASS();
#else
    char tmp_dir[256];
    snprintf(tmp_dir, sizeof(tmp_dir), "/tmp/cbm-adr-realpath-XXXXXX");
    if (!cbm_mkdtemp(tmp_dir)) {
        PASS();
    }
    char cache[256];
    snprintf(cache, sizeof(cache), "/tmp/cbm-adr-realpath-cache-XXXXXX");
    if (!cbm_mkdtemp(cache)) {
        cbm_rmdir(tmp_dir);
        PASS();
    }

    char link_path[320];
    snprintf(link_path, sizeof(link_path), "%s-link", tmp_dir);
    (void)unlink(link_path);
    if (symlink(tmp_dir, link_path) != 0) {
        cbm_rmdir(cache);
        cbm_rmdir(tmp_dir);
        PASS();
    }

    const char *saved = getenv("CBM_CACHE_DIR");
    char *saved_copy = saved ? strdup(saved) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    char src_path[512];
    snprintf(src_path, sizeof(src_path), "%s/main.py", tmp_dir);
    FILE *fp = fopen(src_path, "w");
    ASSERT_NOT_NULL(fp);
    fputs("def main():\n    return 'ok'\n", fp);
    fclose(fp);

    char *project = cbm_project_name_from_path(tmp_dir);
    ASSERT_NOT_NULL(project);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    char args[1024];
    snprintf(args, sizeof(args), "{\"repo_path\":\"%s\",\"mode\":\"fast\"}", link_path);
    char *resp = cbm_mcp_handle_tool(srv, "index_repository", args);
    ASSERT_NOT_NULL(resp);
    ASSERT(response_contains_json_fragment(resp, "\"status\":\"indexed\""));
    ASSERT_NOT_NULL(strstr(resp, project));
    free(resp);

    char update_args[2048];
    snprintf(update_args, sizeof(update_args),
             "{\"project\":\"%s\",\"mode\":\"update\",\"content\":\"## PURPOSE\\n"
             "Symlink-path normalization test.\\n\"}",
             project);
    resp = cbm_mcp_handle_tool(srv, "manage_adr", update_args);
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "updated"));
    free(resp);

    char get_args[512];
    snprintf(get_args, sizeof(get_args), "{\"project\":\"%s\",\"mode\":\"get\"}", link_path);
    resp = cbm_mcp_handle_tool(srv, "manage_adr", get_args);
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "Symlink-path normalization test."));
    ASSERT_NULL(strstr(resp, "or not indexed"));
    ASSERT_NULL(strstr(resp, "no_adr"));
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_project_db(cache, project);
    restore_cache_dir(saved_copy);
    free(saved_copy);
    free(project);
    remove(src_path);
    unlink(link_path);
    cbm_rmdir(cache);
    cbm_rmdir(tmp_dir);
    PASS();
#endif
}

TEST(tool_detect_changes_not_found_rich_error) {
    char cache[256];
    snprintf(cache, sizeof(cache), "/tmp/cbm-detect-missing-cache-XXXXXX");
    if (!cbm_mkdtemp(cache)) {
        PASS();
    }

    const char *saved = getenv("CBM_CACHE_DIR");
    char *saved_copy = saved ? strdup(saved) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    char *resp =
        cbm_mcp_handle_tool(srv, "detect_changes", "{\"project\":\"cbm-no-such-project-zzz\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "or not indexed"));
    ASSERT_NOT_NULL(strstr(resp, "hint"));
    free(resp);

    cbm_mcp_server_free(srv);
    restore_cache_dir(saved_copy);
    free(saved_copy);
    cbm_rmdir(cache);
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

static bool is_valid_json_response(const char *json) {
    if (!json) {
        return false;
    }
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    if (!doc) {
        return false;
    }
    yyjson_doc_free(doc);
    return true;
}

static bool snippet_source_has_replacement(const char *json) {
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    if (!doc) {
        return false;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *source = yyjson_obj_get(root, "source");
    const char *source_str = yyjson_get_str(source);
    bool found = source_str && strstr(source_str, "\xEF\xBF\xBD");
    yyjson_doc_free(doc);
    return found;
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

/* ── TestSnippet_SourceInvalidUtf8 ────────────────────────────── */

TEST(snippet_source_invalid_utf8) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char src_path[512];
    snprintf(src_path, sizeof(src_path), "%s/project/main.go", tmp);
    FILE *fp = fopen(src_path, "wb");
    ASSERT_NOT_NULL(fp);
    const unsigned char source[] = {
        'p',  'a',  'c', 'k', 'a', 'g',  'e',  ' ',  'm',  'a',  'i',  'n', '\n', '\n',
        'f',  'u',  'n', 'c', ' ', 'H',  'a',  'n',  'd',  'l',  'e',  'R', 'e',  'q',
        'u',  'e',  's', 't', '(', ')',  ' ',  'e',  'r',  'r',  'o',  'r', ' ',  '{',
        '\n', '\t', '/', '/', ' ', 0xC0, 0xD4, 0xB7, 0xC2, '\n', '\t', 'r', 'e',  't',
        'u',  'r',  'n', ' ', 'n', 'i',  'l',  '\n', '}',  '\n'};
    ASSERT_EQ(fwrite(source, 1, sizeof(source), fp), sizeof(source));
    ASSERT_EQ(fclose(fp), 0);

    char *raw =
        cbm_mcp_handle_tool(srv, "get_code_snippet",
                            "{\"qualified_name\":\"test-project.cmd.server.main.HandleRequest\","
                            "\"project\":\"test-project\"}");
    ASSERT_TRUE(is_valid_json_response(raw));
    char *resp = extract_text_content(raw);
    ASSERT_NOT_NULL(resp);
    ASSERT_TRUE(is_valid_json_response(resp));
    ASSERT_NULL(strstr(resp, "\xC0\xD4"));
    ASSERT_NOT_NULL(strstr(resp, "HandleRequest"));
    ASSERT_NOT_NULL(strstr(resp, "return nil"));
    ASSERT_TRUE(snippet_source_has_replacement(resp));

    free(resp);
    free(raw);
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

    /* 40 * ~120-char names overflows the 4 KB available-projects buffer.
     * collect_db_project_names advertises each db's INTERNAL project name
     * (#704), so the fixture must hold valid dbs with long internal names —
     * not stub files — for the bounds-check path to actually be exercised. */
    enum { ISSUE235_N = 40 };
    for (int i = 0; i < ISSUE235_N; i++) {
        char name[512];
        ISSUE235_DBNAME(name, cache, i);
        char iname[256];
        snprintf(iname, sizeof(iname),
                 "proj_%02d_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                 "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                 i);
        cbm_store_t *st = cbm_store_open_path(name);
        if (st) {
            cbm_store_upsert_project(st, iname, cache);
            cbm_store_close(st);
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
        char side[540];
        snprintf(side, sizeof(side), "%s-wal", name);
        cbm_unlink(side);
        snprintf(side, sizeof(side), "%s-shm", name);
        cbm_unlink(side);
    }
    cbm_rmdir(cache);
    PASS();
}
#undef ISSUE235_DBNAME

/* Issue #235 (follow-up): with many long-named projects indexed,
 * collect_db_project_names overflowed projects[CBM_SZ_4K] and truncated the
 * LAST name MID-TOKEN, then clamped offset to out_sz-1 — emitting malformed,
 * unterminated JSON like
 *   ...,"available_projects":["a",...,"vjson_49_bbb],"count":50}
 * (unclosed string + unclosed array). build_project_list_error wrapped that
 * invalid body into the tool error, so a "project not found" reply was NOT
 * valid JSON once enough projects were indexed.
 *
 * Reproduce-first: fill an isolated cache dir with enough long INTERNAL-named
 * dbs to overflow the 4 KB buffer, hit the bad-project path, then assert the
 * ERROR BODY (the inner MCP text content) parses as valid JSON and that
 * available_projects is a JSON array whose length == count. RED on the
 * truncating code (yyjson_read returns NULL on the mid-token cut); GREEN after
 * the element-boundary fix, which only ever writes whole "name" tokens. */
#define BADPROJ_JSON_DBNAME(buf, dir, i)                                                      \
    snprintf((buf), sizeof(buf),                                                              \
             "%s/vjson_%02d_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" \
             "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.db",                       \
             (dir), (i))
TEST(tool_bad_project_error_valid_json_issue235) {
    char cache[256];
    snprintf(cache, sizeof(cache), "/tmp/cbm-badproj-vjson-XXXXXX");
    if (!cbm_mkdtemp(cache)) {
        PASS(); /* skip if mkdtemp fails */
    }

    const char *saved = getenv("CBM_CACHE_DIR");
    char *saved_copy = saved ? strdup(saved) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    /* 50 * ~120-char INTERNAL names >> 4 KB → the available_projects buffer
     * overflows and the last name is cut mid-token on the unfixed code. */
    enum { BADPROJ_N = 50 };
    for (int i = 0; i < BADPROJ_N; i++) {
        char name[512];
        BADPROJ_JSON_DBNAME(name, cache, i);
        char iname[256];
        snprintf(iname, sizeof(iname),
                 "vjson_%02d_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                 "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                 i);
        cbm_store_t *st = cbm_store_open_path(name);
        if (st) {
            cbm_store_upsert_project(st, iname, cache);
            cbm_store_close(st);
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

    /* The inner MCP text content is the error body built by
     * build_project_list_error. Capture its validity BEFORE cleanup so a RED
     * failure still restores the environment. */
    char *body = extract_text_content(resp);
    bool body_valid = false;
    bool aps_ok = false; /* available_projects is an array whose len == count */
    if (body) {
        yyjson_doc *bdoc = yyjson_read(body, strlen(body), 0);
        if (bdoc) {
            body_valid = true;
            yyjson_val *broot = yyjson_doc_get_root(bdoc);
            yyjson_val *aps = yyjson_obj_get(broot, "available_projects");
            yyjson_val *cnt = yyjson_obj_get(broot, "count");
            if (aps && yyjson_is_arr(aps) && cnt && yyjson_is_int(cnt)) {
                aps_ok = (yyjson_arr_size(aps) == (size_t)yyjson_get_int(cnt));
            }
            yyjson_doc_free(bdoc);
        }
    }
    free(body);
    free(resp);
    cbm_mcp_server_free(srv);

    if (saved_copy) {
        cbm_setenv("CBM_CACHE_DIR", saved_copy, 1);
        free(saved_copy);
    } else {
        cbm_unsetenv("CBM_CACHE_DIR");
    }
    for (int i = 0; i < BADPROJ_N; i++) {
        char name[512];
        BADPROJ_JSON_DBNAME(name, cache, i);
        cbm_unlink(name);
        char side[540];
        snprintf(side, sizeof(side), "%s-wal", name);
        cbm_unlink(side);
        snprintf(side, sizeof(side), "%s-shm", name);
        cbm_unlink(side);
    }
    cbm_rmdir(cache);

    /* RED on the unfixed code: mid-token truncation → invalid JSON body. */
    ASSERT_TRUE(body_valid);
    ASSERT_TRUE(aps_ok);
    PASS();
}
#undef BADPROJ_JSON_DBNAME

/* ── #704: project resolution must key on the db's INTERNAL project name ──
 *
 * Issue #704: project resolution is registry-less and filename-addressed.
 * resolve_store() opens <cache>/<passed>.db and then requires the internal
 * `projects.name` row to equal the passed name; list_projects /
 * collect_db_project_names derive the advertised name from the .db FILENAME.
 * When a db's filename != its internal name (a legacy '.'-vs-'-' username
 * twin, or a copied/renamed file) it shows up in list_projects under the
 * filename, but every query returns "project not found" — node rows are
 * tagged with the INTERNAL name, so neither the filename nor the resolve
 * path lines up. The fix makes list + resolve both key on the INTERNAL name.
 *
 * Reproduce-first fixture in an isolated CBM_CACHE_DIR:
 *   - alpha704.db  : filename == internal name "alpha704"   (control / fast path)
 *   - gamma704.db  : internal name "beta704"                (DRIFT: built as
 *                    beta704.db then renamed → filename != internal name)
 *   - ghost704.db  : 0-byte file                            (ghost / unresolvable)
 *
 * RED on buggy code / GREEN on the fix:
 *   A. list_projects advertises "beta704" (internal), NOT "gamma704" (filename),
 *      and NOT "ghost704" (0-byte filtered).
 *   B. search_graph(project="beta704") resolves via the cache-dir scan and
 *      returns the node — not the "project not found" error.
 *   C. control project "alpha704" still resolves on the fast path.
 *   D. the 0-byte ghost is not resolvable.
 *   E. addressing the drifted db by its FILENAME ("gamma704") stays not-found
 *      (we key on the internal name, never the file on disk).
 */

/* Create a file-backed project db at <dir>/<filename> whose INTERNAL project
 * name is `internal` (which may differ from the filename), holding one
 * Function node named `fn`. Returns true on success. */
static bool issue704_make_db(const char *dir, const char *filename, const char *internal,
                             const char *fn) {
    char path[700];
    snprintf(path, sizeof(path), "%s/%s", dir, filename);
    cbm_store_t *st = cbm_store_open_path(path);
    if (!st) {
        return false;
    }
    bool ok = (cbm_store_upsert_project(st, internal, dir) == CBM_STORE_OK);
    if (ok) {
        char qn[256];
        snprintf(qn, sizeof(qn), "%s.%s", internal, fn);
        cbm_node_t n = {0};
        n.project = internal;
        n.label = "Function";
        n.name = fn;
        n.qualified_name = qn;
        n.file_path = "main.go";
        n.start_line = 1;
        n.end_line = 2;
        ok = (cbm_store_upsert_node(st, &n) > 0);
    }
    cbm_store_close(st);
    return ok;
}

TEST(tool_resolve_store_by_internal_name_issue704) {
    char cache[256];
    snprintf(cache, sizeof(cache), "/tmp/cbm-issue704-XXXXXX");
    if (!cbm_mkdtemp(cache)) {
        PASS(); /* skip if mkdtemp fails — not a #704 signal */
    }

    const char *saved = getenv("CBM_CACHE_DIR");
    char *saved_copy = saved ? strdup(saved) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    /* (1) control: filename == internal name */
    ASSERT_TRUE(issue704_make_db(cache, "alpha704.db", "alpha704", "alphaFunc704"));

    /* (2) DRIFT: build beta704.db (internal "beta704") then rename the file to
     *     gamma704.db, so filename "gamma704" != internal "beta704". */
    ASSERT_TRUE(issue704_make_db(cache, "beta704.db", "beta704", "betaFunc704"));
    char beta_path[700];
    char gamma_path[700];
    snprintf(beta_path, sizeof(beta_path), "%s/beta704.db", cache);
    snprintf(gamma_path, sizeof(gamma_path), "%s/gamma704.db", cache);
    ASSERT_EQ(rename(beta_path, gamma_path), 0);

    /* (3) ghost: 0-byte db file */
    char ghost_path[700];
    snprintf(ghost_path, sizeof(ghost_path), "%s/ghost704.db", cache);
    FILE *gp = fopen(ghost_path, "w");
    ASSERT_NOT_NULL(gp);
    fclose(gp);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    /* ── A: list_projects reports INTERNAL names; filters the ghost ── */
    char *list =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"list_projects\",\"arguments\":{}}}");
    ASSERT_NOT_NULL(list);
    ASSERT_NOT_NULL(strstr(list, "alpha704")); /* control */
    ASSERT_NOT_NULL(strstr(list, "beta704"));  /* internal name of drifted db (RED before) */
    ASSERT_NULL(strstr(list, "gamma704"));     /* filename must NOT be advertised (RED before) */
    ASSERT_NULL(strstr(list, "ghost704"));     /* 0-byte ghost filtered (RED before) */
    free(list);

    /* ── B: the drifted project resolves by its INTERNAL name ──────── */
    char *q_beta = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"search_graph\",\"arguments\":{"
             "\"project\":\"beta704\",\"name_pattern\":\"betaFunc704\",\"limit\":5}}}");
    ASSERT_NOT_NULL(q_beta);
    ASSERT_NOT_NULL(strstr(q_beta, "betaFunc704")); /* resolved + returned node (RED before) */
    ASSERT_NULL(strstr(q_beta, "not found"));       /* not the not-found error */
    free(q_beta);

    /* ── C: control project still resolves on the fast path ────────── */
    char *q_alpha = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"search_graph\",\"arguments\":{"
             "\"project\":\"alpha704\",\"name_pattern\":\"alphaFunc704\",\"limit\":5}}}");
    ASSERT_NOT_NULL(q_alpha);
    ASSERT_NOT_NULL(strstr(q_alpha, "alphaFunc704"));
    free(q_alpha);

    /* ── D: the 0-byte ghost is NOT resolvable ─────────────────────── */
    char *q_ghost = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":4,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"search_graph\",\"arguments\":{"
             "\"project\":\"ghost704\",\"name_pattern\":\".*\",\"limit\":5}}}");
    ASSERT_NOT_NULL(q_ghost);
    ASSERT_NOT_NULL(strstr(q_ghost, "not found"));
    free(q_ghost);

    /* ── E: addressing the drifted db by its FILENAME stays not-found ── */
    char *q_gamma = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":5,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"search_graph\",\"arguments\":{"
             "\"project\":\"gamma704\",\"name_pattern\":\".*\",\"limit\":5}}}");
    ASSERT_NOT_NULL(q_gamma);
    ASSERT_NOT_NULL(strstr(q_gamma, "not found"));
    free(q_gamma);

    cbm_mcp_server_free(srv);

    /* ── cleanup ───────────────────────────────────────────────────── */
    if (saved_copy) {
        cbm_setenv("CBM_CACHE_DIR", saved_copy, 1);
        free(saved_copy);
    } else {
        cbm_unsetenv("CBM_CACHE_DIR");
    }
    char a_path[700];
    snprintf(a_path, sizeof(a_path), "%s/alpha704.db", cache);
    char corrupt_path[720];
    snprintf(corrupt_path, sizeof(corrupt_path), "%s.corrupt", ghost_path);
    cbm_unlink(a_path);
    cbm_unlink(gamma_path);
    cbm_unlink(ghost_path);
    cbm_unlink(corrupt_path); /* ghost may be quarantined by resolve_store */
    char side[740];
    snprintf(side, sizeof(side), "%s-wal", a_path);
    cbm_unlink(side);
    snprintf(side, sizeof(side), "%s-shm", a_path);
    cbm_unlink(side);
    snprintf(side, sizeof(side), "%s-wal", gamma_path);
    cbm_unlink(side);
    snprintf(side, sizeof(side), "%s-shm", gamma_path);
    cbm_unlink(side);
    cbm_rmdir(cache);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  QUERY STORE READ-ONLY  (data-integrity reproductions)
 *
 *  Bug: query tools resolve the project store via resolve_store() ->
 *  cbm_store_open_path_query(), which opens the DB SQLITE_OPEN_READWRITE
 *  and runs configure_pragmas() with the WRITE pragmas
 *  (journal_mode=WAL + wal_checkpoint + synchronous). Two consequences:
 *    (a) read-only query tools MUTATE the on-disk DB (write pragmas), and
 *    (b) query tools FAIL outright on a read-only DB file / filesystem
 *        (the READWRITE open returns CANTOPEN -> resolve_store NULL ->
 *        "project not found").
 *  Both tests below are written reproduce-first and are RED on the
 *  unfixed code, GREEN once query opens are READONLY with read-only
 *  pragmas.
 * ══════════════════════════════════════════════════════════════════ */

#define ROQ_PROJECT "cbm-roq-test"

/* Whole-file byte snapshot. Returns malloc'd buffer (caller frees) and
 * writes the length to *out_len. Returns NULL on failure. */
static unsigned char *roq_read_file_bytes(const char *path, long *out_len) {
    *out_len = 0;
    FILE *fp = fopen(path, "rb");
    if (!fp) {
        return NULL;
    }
    if (fseek(fp, 0, SEEK_END) != 0) {
        fclose(fp);
        return NULL;
    }
    long sz = ftell(fp);
    if (sz < 0) {
        fclose(fp);
        return NULL;
    }
    rewind(fp);
    unsigned char *buf = malloc((size_t)sz > 0 ? (size_t)sz : 1);
    if (!buf) {
        fclose(fp);
        return NULL;
    }
    size_t got = fread(buf, 1, (size_t)sz, fp);
    fclose(fp);
    if (got != (size_t)sz) {
        free(buf);
        return NULL;
    }
    *out_len = sz;
    return buf;
}

static int roq_file_exists(const char *path) {
    struct stat st;
    return (stat(path, &st) == 0) ? 1 : 0;
}

/* ── (a) NO-MUTATION ──────────────────────────────────────────────────
 *
 * readonly_query_does_not_mutate_db
 *
 * Create a real project DB, convert it to rollback (DELETE) journal mode
 * on disk, snapshot its exact bytes, run search_graph through the server,
 * then re-snapshot. The buggy query path runs `PRAGMA journal_mode=WAL`,
 * which rewrites the file header (1,1 -> 2,2) and spawns a -wal sidecar —
 * so the snapshots differ. The fixed READONLY path runs no write pragma,
 * so the file is byte-identical.
 *
 * The DELETE-mode fixture is what makes the mutation OBSERVABLE: on an
 * already-WAL file `journal_mode=WAL` is a silent no-op, so we deliberately
 * stage the DB in rollback mode (the same technique repro_issue557 uses to
 * plant a deterministic trigger).
 *
 * WHY RED on unfixed code:
 *   journal_mode=WAL rewrites the header -> memcmp(before, after) != 0 and
 *   a -wal file is created while the cached store is open. Both assertions
 *   that demand "unchanged" fire.
 * ─────────────────────────────────────────────────────────────────── */
TEST(readonly_query_does_not_mutate_db) {
    char tmp_cache[512];
    snprintf(tmp_cache, sizeof(tmp_cache), "%s/cbm_roq_a_XXXXXX", cbm_tmpdir());
    if (!cbm_mkdtemp(tmp_cache)) {
        ASSERT_NOT_NULL(NULL); /* setup failure */
    }
    const char *saved = getenv("CBM_CACHE_DIR");
    char *saved_copy = saved ? strdup(saved) : NULL;
    cbm_setenv("CBM_CACHE_DIR", tmp_cache, 1);

    char db_path[700];
    snprintf(db_path, sizeof(db_path), "%s/%s.db", tmp_cache, ROQ_PROJECT);
    char wal_path[730];
    char shm_path[730];
    snprintf(wal_path, sizeof(wal_path), "%s-wal", db_path);
    snprintf(shm_path, sizeof(shm_path), "%s-shm", db_path);

    /* Build the DB and flip it to rollback journal mode on disk. */
    cbm_store_t *setup = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(setup);
    ASSERT_EQ(cbm_store_upsert_project(setup, ROQ_PROJECT, "/tmp/roq"), CBM_STORE_OK);
    cbm_node_t node = {.project = ROQ_PROJECT,
                       .label = "Function",
                       .name = "ReadOnlyProbe",
                       .qualified_name = "roq.mod.ReadOnlyProbe",
                       .file_path = "mod.c"};
    ASSERT_TRUE(cbm_store_upsert_node(setup, &node) > 0);
    ASSERT_EQ(cbm_store_exec(setup, "PRAGMA journal_mode=DELETE;"), 0);
    cbm_store_close(setup);

    /* Snapshot BEFORE any query. */
    long before_len = 0;
    unsigned char *before = roq_read_file_bytes(db_path, &before_len);
    ASSERT_NOT_NULL(before);

    /* Run a query tool through the server (the resolve_store path). */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char args[512];
    snprintf(args, sizeof(args), "{\"project\":\"%s\",\"name_pattern\":\".*ReadOnlyProbe.*\"}",
             ROQ_PROJECT);
    char *resp = cbm_mcp_handle_tool(srv, "search_graph", args);

    /* Capture sidecar state WHILE the cached store is still open (the buggy
     * RW+WAL open creates -wal here; on close it would be removed again). */
    int wal_while_open = roq_file_exists(wal_path);
    int query_ok = (resp && strstr(resp, "ReadOnlyProbe") != NULL);
    int query_failed = (resp && (strstr(resp, "not found") || strstr(resp, "not indexed")));

    cbm_mcp_server_free(srv); /* closes the store; header change is persisted */

    long after_len = 0;
    unsigned char *after = roq_read_file_bytes(db_path, &after_len);

    int identical = (before && after && before_len == after_len &&
                     memcmp(before, after, (size_t)before_len) == 0);

    if (resp) {
        free(resp);
    }
    free(before);
    free(after);
    cbm_unlink(db_path);
    cbm_unlink(wal_path);
    cbm_unlink(shm_path);
    cbm_rmdir(tmp_cache);
    if (saved_copy) {
        cbm_setenv("CBM_CACHE_DIR", saved_copy, 1);
        free(saved_copy);
    } else {
        cbm_unsetenv("CBM_CACHE_DIR");
    }

    ASSERT_TRUE(query_ok);        /* read path ran and returned the node */
    ASSERT_FALSE(query_failed);   /* not the "project not found" path */
    ASSERT_TRUE(identical);       /* RED on buggy code: WAL pragma rewrote header */
    ASSERT_FALSE(wal_while_open); /* RED on buggy code: RW+WAL open spawned -wal */
    PASS();
}

/* ── (b) READ-ONLY FILESYSTEM ─────────────────────────────────────────
 *
 * readonly_query_succeeds_on_readonly_fs
 *
 * Create a real project DB (left in WAL journal mode, as the indexer
 * writes it), then chmod the CONTAINING DIRECTORY to 0555 (read-only) to
 * simulate a read-only mount / immutable media, then run search_graph.
 *
 * Note on why the directory (not just the file) must be read-only: SQLite's
 * unix VFS auto-downgrades a failed O_RDWR main-db open to O_RDONLY, so a
 * 0444 *file* alone does NOT surface the bug — the connection silently
 * becomes read-only and, with a writable dir, still creates the WAL -shm
 * and reads. The genuine read-only-FS symptom is the WAL write-pragma
 * (journal_mode=WAL) being unable to create the -shm/-wal sidecars in a
 * read-only directory.
 *
 * WHY RED on unfixed code:
 *   cbm_store_open_path_query() runs configure_pragmas(.., false) which
 *   executes `PRAGMA journal_mode = WAL`. In a read-only directory the WAL
 *   wal-index (-shm) cannot be created, so the pragma errors ->
 *   configure_pragmas fails -> the open returns NULL -> resolve_store()
 *   returns NULL -> the handler emits "project not found or not indexed".
 *
 * GREEN on fixed code:
 *   the READONLY open skips the WAL write-pragma; the plain READONLY open
 *   of a WAL-mode DB in a read-only dir still needs -shm, so it fails and
 *   the immutable-URI fallback (file:..?immutable=1) reads the main DB
 *   file directly and the query returns the node. (This is the test that
 *   exercises the immutable fallback path.)
 * ─────────────────────────────────────────────────────────────────── */
TEST(readonly_query_succeeds_on_readonly_fs) {
    char tmp_cache[512];
    snprintf(tmp_cache, sizeof(tmp_cache), "%s/cbm_roq_b_XXXXXX", cbm_tmpdir());
    if (!cbm_mkdtemp(tmp_cache)) {
        ASSERT_NOT_NULL(NULL); /* setup failure */
    }
    const char *saved = getenv("CBM_CACHE_DIR");
    char *saved_copy = saved ? strdup(saved) : NULL;
    cbm_setenv("CBM_CACHE_DIR", tmp_cache, 1);

    char db_path[700];
    snprintf(db_path, sizeof(db_path), "%s/%s.db", tmp_cache, ROQ_PROJECT);
    char wal_path[730];
    char shm_path[730];
    snprintf(wal_path, sizeof(wal_path), "%s-wal", db_path);
    snprintf(shm_path, sizeof(shm_path), "%s-shm", db_path);

    /* Build the DB in its natural WAL journal mode and ensure it is cleanly
     * checkpointed (no -wal frames) so the immutable fallback can read all
     * data from the main file. */
    cbm_store_t *setup = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(setup);
    ASSERT_EQ(cbm_store_upsert_project(setup, ROQ_PROJECT, "/tmp/roq"), CBM_STORE_OK);
    cbm_node_t node = {.project = ROQ_PROJECT,
                       .label = "Function",
                       .name = "ReadOnlyProbe",
                       .qualified_name = "roq.mod.ReadOnlyProbe",
                       .file_path = "mod.c"};
    ASSERT_TRUE(cbm_store_upsert_node(setup, &node) > 0);
    (void)cbm_store_checkpoint(setup); /* fold WAL frames into the main file */
    cbm_store_close(setup);            /* clean close removes -wal/-shm */

    /* Make the containing directory read-only (simulate a read-only mount).
     * SQLite can still traverse + read files, but cannot create -shm/-wal. */
    ASSERT_EQ(chmod(tmp_cache, 0555), 0);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char args[512];
    snprintf(args, sizeof(args), "{\"project\":\"%s\",\"name_pattern\":\".*ReadOnlyProbe.*\"}",
             ROQ_PROJECT);
    char *resp = cbm_mcp_handle_tool(srv, "search_graph", args);

    int query_ok = (resp && strstr(resp, "ReadOnlyProbe") != NULL);
    int query_failed = (resp && (strstr(resp, "not found") || strstr(resp, "not indexed")));

    if (resp) {
        free(resp);
    }
    cbm_mcp_server_free(srv);

    /* Restore write permission on the dir BEFORE unlink (cannot remove dir
     * entries while the directory is read-only). */
    chmod(tmp_cache, 0755);
    cbm_unlink(db_path);
    cbm_unlink(wal_path);
    cbm_unlink(shm_path);
    cbm_rmdir(tmp_cache);
    if (saved_copy) {
        cbm_setenv("CBM_CACHE_DIR", saved_copy, 1);
        free(saved_copy);
    } else {
        cbm_unsetenv("CBM_CACHE_DIR");
    }

    ASSERT_FALSE(query_failed); /* RED on buggy code: WAL pragma fails on RO dir */
    ASSERT_TRUE(query_ok);      /* RED on buggy code: no node returned */
    PASS();
}

#undef ROQ_PROJECT

/* ══════════════════════════════════════════════════════════════════
 *  #845 — supervisor gate must not wrap embedders of cbm_mcp_handle_tool
 * ══════════════════════════════════════════════════════════════════ */

/* Child-side check: index a tiny fixture and verify it ran IN-PROCESS.
 * Distinct exit codes so the parent can report the exact failure mode. */
enum {
    IDX845_OK = 0,
    IDX845_SPAWNED = 41,     /* a worker subprocess was spawned — the #845 bug */
    IDX845_NO_RESULT = 42,   /* handle_tool returned NULL */
    IDX845_NOT_INDEXED = 43, /* response lacks status=indexed */
};

static int idx845_index_inprocess_check(const char *repo_dir) {
    int spawns_before = cbm_index_supervisor_spawn_count();

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    if (!srv) {
        return IDX845_NO_RESULT;
    }
    char args[1024];
    snprintf(args, sizeof(args), "{\"repo_path\":\"%s\",\"mode\":\"fast\"}", repo_dir);
    char *resp = cbm_mcp_handle_tool(srv, "index_repository", args);

    int code = IDX845_OK;
    if (cbm_index_supervisor_spawn_count() != spawns_before) {
        code = IDX845_SPAWNED;
    } else if (!resp) {
        code = IDX845_NO_RESULT;
    } else if (!response_contains_json_fragment(resp, "\"status\":\"indexed\"")) {
        code = IDX845_NOT_INDEXED;
    }
    free(resp);
    cbm_mcp_server_free(srv);
    return code;
}

TEST(index_supervisor_gate_requires_marked_host_issue845) {
    /* #845: index_repository via cbm_mcp_handle_tool from an EMBEDDER (this test
     * binary) must index IN-PROCESS even with CBM_INDEX_SUPERVISOR unset. The
     * supervisor gate may only wrap a process that called
     * cbm_index_supervisor_mark_host() — i.e. the real binary's main(). Before
     * the fix, should_wrap() was true for ANY embedder: the gate resolved the
     * CURRENT binary (this test runner!) and spawned
     * '<test-runner> cli --index-worker index_repository …', which a test binary
     * interprets as suite-filter args → it re-runs test suites in the child →
     * recursive spawn chains (observed 11-min hangs; kernel VM-map load during
     * the 2026-07-04 host panics).
     *
     * POSIX: run the call in a forked child under alarm(20) so the pre-fix
     * recursive behaviour cannot hang the runner; the child reports via exit
     * code. Windows: no fork — run in-process (safe once the gate is fixed; the
     * pre-fix redness is demonstrated on POSIX). */
    char tmp_dir[256];
    snprintf(tmp_dir, sizeof(tmp_dir), "/tmp/cbm-idx845-repo-XXXXXX");
    if (!cbm_mkdtemp(tmp_dir)) {
        PASS();
    }
    char cache[256];
    snprintf(cache, sizeof(cache), "/tmp/cbm-idx845-cache-XXXXXX");
    if (!cbm_mkdtemp(cache)) {
        cbm_rmdir(tmp_dir);
        PASS();
    }

    const char *saved_cache = getenv("CBM_CACHE_DIR");
    char *saved_cache_copy = saved_cache ? strdup(saved_cache) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    /* The point of the guard: NO kill switch. The gate itself must keep an
     * unmarked host in-process. Save + restore the ambient value. */
    const char *saved_sv = getenv("CBM_INDEX_SUPERVISOR");
    char *saved_sv_copy = saved_sv ? strdup(saved_sv) : NULL;
    cbm_unsetenv("CBM_INDEX_SUPERVISOR");

    char src_path[512];
    snprintf(src_path, sizeof(src_path), "%s/main.py", tmp_dir);
    FILE *fp = fopen(src_path, "w");
    ASSERT_NOT_NULL(fp);
    fputs("def main():\n    return 'ok'\n", fp);
    fclose(fp);

    int code = -1;
    bool signalled = false;
    int sig = 0;
#ifdef _WIN32
    code = idx845_index_inprocess_check(tmp_dir);
#else
    fflush(NULL);
    pid_t pid = fork();
    if (pid == 0) {
        alarm(20); /* pre-fix spawn chain must die here, not hang the runner */
        _exit(idx845_index_inprocess_check(tmp_dir));
    }
    ASSERT_TRUE(pid > 0);
    int status = 0;
    (void)waitpid(pid, &status, 0);
    if (WIFEXITED(status)) {
        code = WEXITSTATUS(status);
    } else if (WIFSIGNALED(status)) {
        signalled = true;
        sig = WTERMSIG(status);
    }
#endif

    /* Restore env BEFORE asserting so a red run doesn't leak state. */
    if (saved_sv_copy) {
        cbm_setenv("CBM_INDEX_SUPERVISOR", saved_sv_copy, 1);
        free(saved_sv_copy);
    } else {
        cbm_unsetenv("CBM_INDEX_SUPERVISOR");
    }
    char *project = cbm_project_name_from_path(tmp_dir);
    cleanup_project_db(cache, project);
    free(project);
    restore_cache_dir(saved_cache_copy);
    free(saved_cache_copy);
    remove(src_path);
    cbm_rmdir(cache);
    cbm_rmdir(tmp_dir);

    if (signalled) {
        printf("    child killed by signal %d (alarm => recursive spawn chain hang)\n", sig);
    } else if (code != IDX845_OK) {
        printf("    child exit code %d (41=worker spawned, 42=no result, 43=not indexed)\n",
               code);
    }
    ASSERT_FALSE(signalled);
    ASSERT_EQ(code, IDX845_OK);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  AUTO_WATCH GATE  (distilled from PR #625)
 *
 *  Background watcher registration on session connect is gated by the
 *  `auto_watch` config key (default TRUE = existing behavior).
 * ══════════════════════════════════════════════════════════════════ */

/* Drive the already-indexed connect path (initialize → maybe_auto_index →
 * watcher registration) and return the resulting watch count.
 * auto_watch_value: NULL leaves the key unset (exercises the default),
 * otherwise the key is set to that value before initialize.
 * Returns a negative code on fixture setup failure. */
static int auto_watch_connect_watch_count(const char *auto_watch_value) {
    char cache[256];
    snprintf(cache, sizeof(cache), "/tmp/cbm-autowatch-cache-XXXXXX");
    if (!cbm_mkdtemp(cache)) {
        return -1;
    }

    char repodir[512];
    snprintf(repodir, sizeof(repodir), "%s/repo", cache);
    if (th_mkdir_p(repodir) != 0) {
        th_rmtree(cache);
        return -2;
    }

    /* Same derivation detect_session uses on the cwd — realpath-based, so
     * the name matches even where /tmp is a symlink (macOS). */
    char *project = cbm_project_name_from_path(repodir);
    if (!project) {
        th_rmtree(cache);
        return -3;
    }

    /* Pre-create <cache>/<project>.db so maybe_auto_index takes the
     * "already indexed" branch — the watcher-registration site under test. */
    char db_path[1024];
    snprintf(db_path, sizeof(db_path), "%s/%s.db", cache, project);
    if (th_write_file(db_path, "") != 0) {
        free(project);
        th_rmtree(cache);
        return -4;
    }
    free(project);

    const char *saved = getenv("CBM_CACHE_DIR");
    char *saved_copy = saved ? strdup(saved) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    char old_cwd[1024];
    if (!cbm_getcwd(old_cwd, sizeof(old_cwd)) || cbm_chdir(repodir) != 0) {
        restore_cache_dir(saved_copy);
        free(saved_copy);
        th_rmtree(cache);
        return -5;
    }

    int count = -6;
    cbm_config_t *cfg = cbm_config_open(cache);
    cbm_store_t *wstore = cbm_store_open_memory();
    cbm_watcher_t *watcher = wstore ? cbm_watcher_new(wstore, NULL, NULL) : NULL;
    if (cfg && watcher) {
        if (auto_watch_value) {
            cbm_config_set(cfg, CBM_CONFIG_AUTO_WATCH, auto_watch_value);
        }

        cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
        if (srv) {
            cbm_mcp_server_set_watcher(srv, watcher);
            cbm_mcp_server_set_config(srv, cfg);
            char *resp = cbm_mcp_server_handle(
                srv, "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{}}");
            free(resp);
            count = cbm_watcher_watch_count(watcher);
            cbm_mcp_server_free(srv);
        }
    }

    if (watcher) {
        cbm_watcher_free(watcher);
    }
    if (wstore) {
        cbm_store_close(wstore);
    }
    if (cfg) {
        cbm_config_close(cfg);
    }

    (void)cbm_chdir(old_cwd);
    restore_cache_dir(saved_copy);
    free(saved_copy);
    th_rmtree(cache);
    return count;
}

/* Default (key unset) → watcher registered on connect. Guards the
 * no-behavior-change promise of the auto_watch gate: existing users keep
 * background auto-sync without touching config. */
TEST(mcp_auto_watch_default_registers_watcher_on_connect) {
    int count = auto_watch_connect_watch_count(NULL);
    if (count < 0) {
        PASS(); /* fixture setup failed (tmpdir/cwd unavailable) — skip */
    }
    ASSERT_EQ(count, 1);
    PASS();
}

/* auto_watch=false → NO watcher registered on connect. RED on pre-gate code
 * (registration was unconditional and the key did not exist). */
TEST(mcp_auto_watch_false_skips_watcher_on_connect) {
    int count = auto_watch_connect_watch_count("false");
    if (count < 0) {
        PASS(); /* fixture setup failed (tmpdir/cwd unavailable) — skip */
    }
    ASSERT_EQ(count, 0);
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
    RUN_TEST(mcp_tools_list_latest_metadata);
    RUN_TEST(mcp_index_repository_declares_name_override_issue571);
    RUN_TEST(mcp_tools_array_schemas_have_items);
    RUN_TEST(mcp_ingest_traces_items_disallow_additional_properties_issue731);
    RUN_TEST(mcp_get_architecture_aspects_schema_enum_pr560);
    RUN_TEST(mcp_text_result);
    RUN_TEST(mcp_text_result_skips_structured_content_for_plain_text);
    RUN_TEST(mcp_cancel_matches_request_id);
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
    RUN_TEST(server_handle_tools_list_paginates);
    RUN_TEST(server_handle_logs_request_without_params);
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
    RUN_TEST(tool_search_graph_query_honors_file_pattern_issue552);
    RUN_TEST(tool_query_graph_basic);
    RUN_TEST(tool_index_status_no_project);
    RUN_TEST(tool_index_status_includes_git_metadata);

    /* Tool handlers with validation */
    RUN_TEST(tool_trace_call_path_not_found);
    RUN_TEST(tool_trace_missing_function_name);
    RUN_TEST(tool_trace_call_path_ambiguous);
    RUN_TEST(tool_trace_call_path_prefers_definition);
    RUN_TEST(tool_trace_call_path_distinct_defs_not_over_unioned);
    RUN_TEST(tool_trace_call_path_dts_stub_unions_with_impl);
    RUN_TEST(tool_delete_project_not_found);
    RUN_TEST(tool_get_architecture_empty);
    RUN_TEST(tool_get_architecture_emits_populated_sections);
    RUN_TEST(tool_get_architecture_overview_compact_subset_pr560);
    RUN_TEST(tool_get_architecture_rejects_unknown_aspect_pr560);
    RUN_TEST(tool_get_architecture_accepts_project_name_alias_issue640);
    RUN_TEST(tool_search_graph_accepts_project_name_alias_issue640);
    RUN_TEST(tool_get_architecture_path_scoping);
    RUN_TEST(tool_query_graph_missing_query);

    /* Pipeline-dependent tool handlers */
    RUN_TEST(tool_index_repository_missing_path);
    RUN_TEST(tool_get_code_snippet_missing_qn);
    RUN_TEST(tool_get_code_snippet_not_found);
    RUN_TEST(tool_search_code_missing_pattern);
    RUN_TEST(tool_search_code_no_project);
    RUN_TEST(search_code_multi_word);
    RUN_TEST(search_code_scoped_path_with_spaces_issue687);
    RUN_TEST(search_code_path_filter_prefilter_keeps_matches);
    RUN_TEST(search_code_path_filter_matches_nothing);
    RUN_TEST(search_code_invalid_regex_errors_issue283);
    RUN_TEST(search_code_literal_pipe_warns_issue282);
    RUN_TEST(search_code_ampersand_accepted_issue272);
    RUN_TEST(tool_detect_changes_no_project);
    RUN_TEST(tool_manage_adr_no_project);
    RUN_TEST(tool_manage_adr_get_with_existing_adr);
    RUN_TEST(tool_manage_adr_unified_backend_issue256);
    RUN_TEST(tool_index_repository_reports_store_backed_adr);
    RUN_TEST(tool_index_repository_dot_uses_absolute_project_key_and_preserves_adr);
    RUN_TEST(index_supervisor_gate_requires_marked_host_issue845);
    RUN_TEST(tool_manage_adr_not_found_rich_error);
    RUN_TEST(tool_manage_adr_get_accepts_abs_path);
    RUN_TEST(tool_manage_adr_get_accepts_symlink_path);
    RUN_TEST(tool_detect_changes_not_found_rich_error);
    RUN_TEST(tool_ingest_traces_basic);
    RUN_TEST(tool_ingest_traces_empty);

    /* Query store read-only (data integrity) */
    RUN_TEST(readonly_query_does_not_mutate_db);
    RUN_TEST(readonly_query_succeeds_on_readonly_fs);

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
    RUN_TEST(snippet_source_invalid_utf8);
    RUN_TEST(tool_bad_project_name_no_overflow_issue235);
    RUN_TEST(tool_bad_project_error_valid_json_issue235);
    RUN_TEST(tool_resolve_store_by_internal_name_issue704);

    /* auto_watch gate (distilled from PR #625) */
    RUN_TEST(mcp_auto_watch_default_registers_watcher_on_connect);
    RUN_TEST(mcp_auto_watch_false_skips_watcher_on_connect);
}
