/*
 * test_mcp.c — Tests for the MCP server module.
 *
 * Covers: JSON-RPC parsing, MCP protocol, tool dispatch, tool handlers.
 */
#include "../src/foundation/compat.h"
#include "../src/foundation/compat_fs.h" /* cbm_unlink / cbm_rmdir */
#include "../src/foundation/constants.h"
#include "test_helpers.h"
#include "test_framework.h"
#include <cli/cli.h>
#include <depindex/depindex.h>
#include <mcp/mcp.h>
#include <pipeline/pipeline.h>
#include <store/store.h>
#include <sqlite3.h>
#include <yyjson/yyjson.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>

static bool test_file_exists_mcp(const char *path) {
    FILE *fp = fopen(path, "rb");
    if (!fp) {
        return false;
    }
    fclose(fp);
    return true;
}

static bool has_stale_freshness_view(const char *json, const char *view_name) {
    return json && view_name && strstr(json, "\"freshness\"") &&
           strstr(json, "\"state\":\"stale_with_warning\"") &&
           strstr(json, "\"stale_views\"") && strstr(json, view_name);
}

static bool has_dirty_freshness_counts(const char *json, int pending, int overlay_ready) {
    char pending_buf[CBM_SZ_64];
    char overlay_buf[CBM_SZ_64];
    snprintf(pending_buf, sizeof(pending_buf), "\"dirty_files_pending\":%d", pending);
    snprintf(overlay_buf, sizeof(overlay_buf), "\"dirty_files_overlay_ready\":%d",
             overlay_ready);
    return json && strstr(json, "\"freshness\"") &&
           strstr(json, "\"state\":\"dirty_with_warning\"") &&
           strstr(json, "\"stale_scope\":\"dirty_files\"") &&
           strstr(json, pending_buf) && strstr(json, overlay_buf);
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
    char *json = cbm_mcp_tools_list(NULL);
    ASSERT_NOT_NULL(json);
    /* §4b: when srv=NULL (no config), cbm_mcp_tools_list defaults to "streamlined"
     * mode and emits five user-facing tools plus _hidden_tools. Canonical
     * tools (including trace_path) come from TOOLS[] so classic and
     * streamlined schemas cannot drift; get_code is the concise alias from
     * STREAMLINED_TOOLS[]. The old search_code_graph mega-tool has been
     * deleted. */
    ASSERT_NOT_NULL(strstr(json, "search_graph"));
    ASSERT_NOT_NULL(strstr(json, "query_graph"));
    ASSERT_NOT_NULL(strstr(json, "search_code"));
    ASSERT_NOT_NULL(strstr(json, "trace_path"));
    ASSERT_NOT_NULL(strstr(json, "get_code"));
    /* The deleted mega-tool must NOT appear */
    ASSERT_NULL(strstr(json, "search_code_graph"));
    /* Hidden classic tools should NOT appear as top-level tool entries */
    ASSERT_NULL(strstr(json, "\"index_repository\""));
    free(json);
    PASS();
}

TEST(mcp_tools_list_classic_mode) {
    /* Classic mode (CBM_TOOL_MODE=classic) emits the original 15 split tools,
     * not the streamlined consolidated set. The env var is read at call time
     * (src/mcp/mcp.c:870), so set it, capture the list, then unset it BEFORE any
     * ASSERT — a failed assert must not leak the classic setting into sibling
     * tests (which expect the streamlined default). */
    cbm_setenv("CBM_TOOL_MODE", "classic", 1);
    char *json = cbm_mcp_tools_list(NULL);
    cbm_unsetenv("CBM_TOOL_MODE");
    ASSERT_NOT_NULL(json);
    /* Classic split tools are present (TOOLS[] in mcp.c). */
    ASSERT_NOT_NULL(strstr(json, "\"index_repository\""));
    ASSERT_NOT_NULL(strstr(json, "\"search_graph\""));
    ASSERT_NOT_NULL(strstr(json, "\"query_graph\""));
    /* The streamlined-only consolidated tool + progressive-disclosure hint are
     * NOT emitted in classic mode. */
    ASSERT_NULL(strstr(json, "\"search_code_graph\""));
    ASSERT_NULL(strstr(json, "_hidden_tools"));
    free(json);
    PASS();
}

TEST(mcp_tools_array_schemas_have_items) {
    /* VS Code 1.112+ rejects array schemas without "items" (see
     * https://github.com/microsoft/vscode/issues/248810).
     * Walk every tool's inputSchema and verify that every "type":"array"
     * property also contains "items". */
    cbm_mcp_server_t *srv_tmp = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv_tmp);
    char *json = cbm_mcp_tools_list(srv_tmp);
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
    cbm_mcp_server_free(srv_tmp);
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
    /* §4b: streamlined mode default surface — 5 split tools */
    ASSERT_NOT_NULL(strstr(resp, "search_graph"));
    ASSERT_NOT_NULL(strstr(resp, "trace_path"));
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

TEST(tool_list_projects_includes_tmp_prefixed_project) {
    char cache[256];
    snprintf(cache, sizeof(cache), "/tmp/cbm-list-tmp-XXXXXX");
    if (!cbm_mkdtemp(cache)) {
        PASS(); /* skip if mkdtemp fails */
    }

    const char *saved = getenv("CBM_CACHE_DIR");
    char *saved_copy = saved ? strdup(saved) : NULL;
    const char *saved_auto = getenv("CBM_AUTO_INDEX");
    char *saved_auto_copy = saved_auto ? strdup(saved_auto) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);
    cbm_setenv("CBM_AUTO_INDEX", "false", 1);

    char db_path[512];
    int db_len = snprintf(db_path, sizeof(db_path), "%s/tmp-valid-project.db", cache);
    ASSERT_TRUE(db_len > 0 && (size_t)db_len < sizeof(db_path));
    cbm_store_t *store = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(store);
    ASSERT_EQ(cbm_store_upsert_project(store, "tmp-valid-project", "/tmp/valid-project"),
              CBM_STORE_OK);
    cbm_store_close(store);

    cbm_mcp_server_t *srv = setup_mcp_with_data();
    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":10,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"list_projects\",\"arguments\":{}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "tmp-valid-project"));
    free(resp);
    cbm_mcp_server_free(srv);

    if (saved_copy) {
        cbm_setenv("CBM_CACHE_DIR", saved_copy, 1);
        free(saved_copy);
    } else {
        cbm_unsetenv("CBM_CACHE_DIR");
    }
    if (saved_auto_copy) {
        cbm_setenv("CBM_AUTO_INDEX", saved_auto_copy, 1);
        free(saved_auto_copy);
    } else {
        cbm_unsetenv("CBM_AUTO_INDEX");
    }
    cbm_unlink(db_path);
    char wal[512];
    char shm[512];
    int wal_len = snprintf(wal, sizeof(wal), "%s-wal", db_path);
    int shm_len = snprintf(shm, sizeof(shm), "%s-shm", db_path);
    if (wal_len > 0 && (size_t)wal_len < sizeof(wal)) {
        cbm_unlink(wal);
    }
    if (shm_len > 0 && (size_t)shm_len < sizeof(shm)) {
        cbm_unlink(shm);
    }
    cbm_rmdir(cache);
    PASS();
}

TEST(resolve_store_quarantines_structurally_corrupt_db) {
    char cache[256];
    snprintf(cache, sizeof(cache), "/tmp/cbm-corrupt-quarantine-XXXXXX");
    if (!cbm_mkdtemp(cache)) {
        PASS(); /* skip if mkdtemp fails */
    }

    const char *saved = getenv("CBM_CACHE_DIR");
    char *saved_copy = saved ? strdup(saved) : NULL;
    const char *saved_auto = getenv("CBM_AUTO_INDEX");
    char *saved_auto_copy = saved_auto ? strdup(saved_auto) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);
    cbm_setenv("CBM_AUTO_INDEX", "false", 1);

    char db_path[512];
    int db_len = snprintf(db_path, sizeof(db_path), "%s/corrupt-project.db", cache);
    ASSERT_TRUE(db_len > 0 && (size_t)db_len < sizeof(db_path));
    cbm_store_t *store = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(store);
    sqlite3 *db = cbm_store_get_db(store);
    ASSERT_NOT_NULL(db);
    ASSERT_EQ(sqlite3_exec(db, "DROP TABLE projects;", NULL, NULL, NULL), SQLITE_OK);
    cbm_store_close(store);

    char quarantine[512];
    int quarantine_len = snprintf(quarantine, sizeof(quarantine), "%s.corrupt", db_path);
    ASSERT_TRUE(quarantine_len > 0 && (size_t)quarantine_len < sizeof(quarantine));

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":"
             "\"search_graph\",\"arguments\":{\"project\":\"corrupt-project\","
             "\"pattern\":\"anything\"}}}");
    ASSERT_NOT_NULL(resp);
    free(resp);
    cbm_mcp_server_free(srv);

    ASSERT_FALSE(test_file_exists_mcp(db_path));
    ASSERT_TRUE(test_file_exists_mcp(quarantine));

    if (saved_copy) {
        cbm_setenv("CBM_CACHE_DIR", saved_copy, 1);
        free(saved_copy);
    } else {
        cbm_unsetenv("CBM_CACHE_DIR");
    }
    if (saved_auto_copy) {
        cbm_setenv("CBM_AUTO_INDEX", saved_auto_copy, 1);
        free(saved_auto_copy);
    } else {
        cbm_unsetenv("CBM_AUTO_INDEX");
    }
    cbm_unlink(quarantine);
    cbm_rmdir(cache);
    PASS();
}

TEST(resolve_store_leaves_foreign_sqlite_db_untouched) {
    char cache[256];
    snprintf(cache, sizeof(cache), "/tmp/cbm-foreign-db-XXXXXX");
    if (!cbm_mkdtemp(cache)) {
        PASS(); /* skip if mkdtemp fails */
    }

    const char *saved = getenv("CBM_CACHE_DIR");
    char *saved_copy = saved ? strdup(saved) : NULL;
    const char *saved_auto = getenv("CBM_AUTO_INDEX");
    char *saved_auto_copy = saved_auto ? strdup(saved_auto) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);
    cbm_setenv("CBM_AUTO_INDEX", "false", 1);

    char db_path[512];
    int db_len = snprintf(db_path, sizeof(db_path), "%s/foreign-project.db", cache);
    ASSERT_TRUE(db_len > 0 && (size_t)db_len < sizeof(db_path));
    sqlite3 *foreign_db = NULL;
    ASSERT_EQ(sqlite3_open(db_path, &foreign_db), SQLITE_OK);
    ASSERT_EQ(sqlite3_exec(foreign_db, "CREATE TABLE user_data(id INTEGER PRIMARY KEY);", NULL,
                           NULL, NULL),
              SQLITE_OK);
    sqlite3_close(foreign_db);

    char quarantine[512];
    char wal[512];
    char shm[512];
    int quarantine_len = snprintf(quarantine, sizeof(quarantine), "%s.corrupt", db_path);
    int wal_len = snprintf(wal, sizeof(wal), "%s-wal", db_path);
    int shm_len = snprintf(shm, sizeof(shm), "%s-shm", db_path);
    ASSERT_TRUE(quarantine_len > 0 && (size_t)quarantine_len < sizeof(quarantine));
    ASSERT_TRUE(wal_len > 0 && (size_t)wal_len < sizeof(wal));
    ASSERT_TRUE(shm_len > 0 && (size_t)shm_len < sizeof(shm));

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":"
             "\"search_graph\",\"arguments\":{\"project\":\"foreign-project\","
             "\"pattern\":\"anything\"}}}");
    ASSERT_NOT_NULL(resp);
    free(resp);
    cbm_mcp_server_free(srv);

    ASSERT_TRUE(test_file_exists_mcp(db_path));
    ASSERT_FALSE(test_file_exists_mcp(quarantine));
    ASSERT_FALSE(test_file_exists_mcp(wal));
    ASSERT_FALSE(test_file_exists_mcp(shm));

    if (saved_copy) {
        cbm_setenv("CBM_CACHE_DIR", saved_copy, 1);
        free(saved_copy);
    } else {
        cbm_unsetenv("CBM_CACHE_DIR");
    }
    if (saved_auto_copy) {
        cbm_setenv("CBM_AUTO_INDEX", saved_auto_copy, 1);
        free(saved_auto_copy);
    } else {
        cbm_unsetenv("CBM_AUTO_INDEX");
    }
    cbm_unlink(db_path);
    cbm_rmdir(cache);
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
    const char *scan = inner;
    int source_keys = 0;
    while ((scan = strstr(scan, "\"source\"")) != NULL) {
        source_keys++;
        scan += strlen("\"source\"");
    }
    ASSERT_EQ(source_keys, 1);
    free(inner);
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

TEST(tool_search_graph_warns_on_stale_pagerank_view) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(st);
    ASSERT_EQ(cbm_store_upsert_project(st, "test", "/tmp/test"), CBM_STORE_OK);
    cbm_mcp_server_set_project(srv, "test");

    cbm_node_t node = {.project = "test",
                       .label = "Function",
                       .name = "Handle",
                       .qualified_name = "test.Handle",
                       .file_path = "handle.c"};
    int64_t id = cbm_store_upsert_node(st, &node);
    ASSERT_TRUE(id > 0);
    char rank_sql[256];
    snprintf(rank_sql, sizeof(rank_sql),
             "INSERT INTO pagerank(project,node_id,rank,computed_at) "
             "VALUES('test',%lld,0.9,'2026-06-30T00:00:00Z')",
             (long long)id);
    ASSERT_EQ(cbm_store_exec(st, rank_sql), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_set_derived_view_state(st, "test", CBM_STORE_DERIVED_VIEW_PAGERANK,
                                               CBM_STORE_DERIVED_GENERATION_UNKNOWN,
                                               CBM_STORE_DERIVED_STATUS_STALE),
              CBM_STORE_OK);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":43,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"search_graph\","
             "\"arguments\":{\"project\":\"test\",\"label\":\"Function\",\"limit\":5}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "\"warnings\""));
    ASSERT_NOT_NULL(strstr(inner, "pagerank derived view is stale"));
    ASSERT(has_stale_freshness_view(inner, CBM_STORE_DERIVED_VIEW_PAGERANK));
    ASSERT_NULL(strstr(inner, "\"pagerank\":"));

    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_search_graph_warns_on_stale_route_view) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(st);
    const char *proj = "search-route-stale";
    ASSERT_EQ(cbm_store_upsert_project(st, proj, "/tmp/search-route-stale"), CBM_STORE_OK);
    cbm_mcp_server_set_project(srv, proj);

    cbm_node_t route = {.project = proj,
                        .label = "Route",
                        .name = "/api/status",
                        .qualified_name = "__route__/api/status",
                        .file_path = "src/status.c"};
    ASSERT_GT(cbm_store_upsert_node(st, &route), 0);
    ASSERT_EQ(cbm_store_set_derived_view_state(st, proj, CBM_STORE_DERIVED_VIEW_ROUTES,
                                               CBM_STORE_DERIVED_GENERATION_UNKNOWN,
                                               CBM_STORE_DERIVED_STATUS_STALE),
              CBM_STORE_OK);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":44,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"search_graph\","
             "\"arguments\":{\"project\":\"search-route-stale\",\"label\":\"Route\",\"limit\":5}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "\"warnings\""));
    ASSERT_NOT_NULL(strstr(inner, "routes derived view is stale"));
    ASSERT(has_stale_freshness_view(inner, CBM_STORE_DERIVED_VIEW_ROUTES));

    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_search_graph_reports_dirty_metadata_without_hiding_canonical_rows) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(st);
    const char *proj = "dirty-metadata";
    ASSERT_EQ(cbm_store_upsert_project(st, proj, "/tmp/dirty-metadata"), CBM_STORE_OK);
    cbm_mcp_server_set_project(srv, proj);

    cbm_node_t node = {.project = proj,
                       .label = "Function",
                       .name = "StillVisible",
                       .qualified_name = "dirty.StillVisible",
                       .file_path = "src/dirty.c"};
    ASSERT_GT(cbm_store_upsert_node(st, &node), 0);

    cbm_dirty_file_state_t dirty = {.project = proj,
                                    .rel_path = "src/dirty.c",
                                    .observed_hash = "dirty-hash",
                                    .observed_generation = 7,
                                    .source = CBM_STORE_DIRTY_SOURCE_GIT_STATUS,
                                    .status = CBM_STORE_DIRTY_STATUS_PENDING};
    ASSERT_EQ(cbm_store_upsert_dirty_file(st, &dirty), CBM_STORE_OK);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":145,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"search_graph\","
             "\"arguments\":{\"project\":\"dirty-metadata\",\"label\":\"Function\","
             "\"name_pattern\":\"StillVisible\",\"limit\":5}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "StillVisible"));
    ASSERT_NOT_NULL(strstr(inner, "\"warnings\""));
    ASSERT_NOT_NULL(strstr(inner, "project has dirty files"));
    ASSERT_TRUE(has_dirty_freshness_counts(inner, 1, 0));

    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

static bool mcp_test_upsert_fts_node(cbm_store_t *st, const char *project, const char *label,
                                     const char *name, const char *qualified_name,
                                     const char *file_path) {
    cbm_node_t node = {0};
    node.project = project;
    node.label = label;
    node.name = name;
    node.qualified_name = qualified_name;
    node.file_path = file_path;
    node.start_line = 1;
    node.end_line = 3;
    return cbm_store_upsert_node(st, &node) > 0;
}

static int mcp_test_rebuild_nodes_fts(cbm_store_t *st) {
    return cbm_store_rebuild_nodes_fts(st);
}

TEST(tool_search_graph_query_sees_file_delta_fts_updates) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(st);

    const char *proj = "fts-delta";
    ASSERT_EQ(cbm_store_upsert_project(st, proj, "/tmp/fts-delta"), CBM_STORE_OK);
    cbm_mcp_server_set_project(srv, proj);

    cbm_node_t old_node = {.project = proj,
                           .label = "Function",
                           .name = "obsolete",
                           .qualified_name = "fts-delta.obsolete",
                           .file_path = "src/status.c",
                           .start_line = 1,
                           .end_line = 3};
    cbm_store_file_delta_t old_delta = {.project = proj,
                                        .rel_path = "src/status.c",
                                        .generation = 1,
                                        .nodes = &old_node,
                                        .node_count = 1,
                                        .derived_view_name = CBM_STORE_DERIVED_VIEW_NODES_FTS,
                                        .derived_status = CBM_STORE_DERIVED_STATUS_COMPLETE};
    ASSERT_EQ(cbm_store_publish_file_delta(st, &old_delta), CBM_STORE_OK);

    cbm_node_t new_node = {.project = proj,
                           .label = "Function",
                           .name = "freshmarker",
                           .qualified_name = "fts-delta.freshmarker",
                           .file_path = "src/status.c",
                           .start_line = 1,
                           .end_line = 3};
    cbm_store_file_delta_t new_delta = {.project = proj,
                                        .rel_path = "src/status.c",
                                        .generation = 2,
                                        .nodes = &new_node,
                                        .node_count = 1,
                                        .derived_view_name = CBM_STORE_DERIVED_VIEW_NODES_FTS,
                                        .derived_status = CBM_STORE_DERIVED_STATUS_COMPLETE};
    ASSERT_EQ(cbm_store_publish_file_delta(st, &new_delta), CBM_STORE_OK);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":554,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"search_graph\","
             "\"arguments\":{\"project\":\"fts-delta\",\"query\":\"freshmarker\",\"limit\":5}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "\"search_mode\":\"bm25\""));
    ASSERT_NOT_NULL(strstr(inner, "freshmarker"));
    ASSERT_NULL(strstr(inner, "obsolete"));
    free(inner);
    free(resp);

    resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":555,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"search_graph\","
             "\"arguments\":{\"project\":\"fts-delta\",\"query\":\"obsolete\",\"limit\":5}}}");
    ASSERT_NOT_NULL(resp);
    inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "\"search_mode\":\"bm25\""));
    ASSERT_NULL(strstr(inner, "obsolete"));
    ASSERT_NULL(strstr(inner, "freshmarker"));

    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
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

    ASSERT_TRUE(mcp_test_upsert_fts_node(st, proj, "Function", "status",
                                         "issue-552.src.lib.status", "src/lib/status.c"));
    ASSERT_TRUE(mcp_test_upsert_fts_node(st, proj, "Function", "status",
                                         "issue-552.src.components.status",
                                         "src/components/status.c"));
    ASSERT_EQ(mcp_test_rebuild_nodes_fts(st), CBM_STORE_OK);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":552,\"method\":\"tools/call\","
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

TEST(tool_search_graph_query_uses_search_limit_config) {
    char *tmp = th_mktempdir("cbm_mcp_bm25_limit");
    ASSERT_NOT_NULL(tmp);
    char cfg_dir[512];
    int n = snprintf(cfg_dir, sizeof(cfg_dir), "%s", tmp);
    ASSERT_TRUE(n > 0 && (size_t)n < sizeof(cfg_dir));

    cbm_config_t *cfg = cbm_config_open(cfg_dir);
    ASSERT_NOT_NULL(cfg);
    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_SEARCH_LIMIT, "1"), 0);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_mcp_server_set_config(srv, cfg);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(st);

    const char *proj = "bm25-limit";
    cbm_mcp_server_set_project(srv, proj);
    cbm_store_upsert_project(st, proj, "/tmp/bm25-limit");

    ASSERT_TRUE(mcp_test_upsert_fts_node(st, proj, "Function", "status_ready",
                                         "bm25-limit.src.status_ready",
                                         "src/status_ready.c"));
    ASSERT_TRUE(mcp_test_upsert_fts_node(st, proj, "Function", "status_pending",
                                         "bm25-limit.src.status_pending",
                                         "src/status_pending.c"));
    ASSERT_EQ(mcp_test_rebuild_nodes_fts(st), CBM_STORE_OK);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":554,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"search_graph\","
             "\"arguments\":{\"project\":\"bm25-limit\",\"query\":\"status\"}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);

    yyjson_doc *doc = yyjson_read(inner, strlen(inner), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *root = yyjson_doc_get_root(doc);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(root, "search_mode")), "bm25");
    ASSERT_TRUE(yyjson_get_bool(yyjson_obj_get(root, "has_more")));
    yyjson_val *results = yyjson_obj_get(root, "results");
    ASSERT_NOT_NULL(results);
    ASSERT_EQ(yyjson_arr_size(results), 1);

    yyjson_doc_free(doc);
    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    cbm_config_close(cfg);
    th_rmtree(cfg_dir);
    PASS();
}

TEST(tool_search_graph_query_rejects_bad_semantic_query) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(st);

    const char *proj = "bm25-semantic";
    cbm_mcp_server_set_project(srv, proj);
    cbm_store_upsert_project(st, proj, "/tmp/bm25-semantic");

    ASSERT_TRUE(mcp_test_upsert_fts_node(st, proj, "Function", "publish_status",
                                         "bm25-semantic.src.publish_status", "src/status.c"));
    ASSERT_EQ(mcp_test_rebuild_nodes_fts(st), CBM_STORE_OK);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":553,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"search_graph\","
             "\"arguments\":{\"project\":\"bm25-semantic\",\"query\":\"status\","
             "\"semantic_query\":\"publish\"}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "semantic_query must be an array"));
    ASSERT_NULL(strstr(inner, "\"search_mode\":\"bm25\""));

    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_search_graph_semantic_query_warns_on_stale_semantic_view) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(st);

    const char *proj = "semantic-stale";
    ASSERT_EQ(cbm_store_upsert_project(st, proj, "/tmp/semantic-stale"), CBM_STORE_OK);
    cbm_mcp_server_set_project(srv, proj);
    ASSERT_EQ(cbm_store_set_derived_view_state(st, proj, CBM_STORE_DERIVED_VIEW_SEMANTIC_EDGES,
                                               CBM_STORE_DERIVED_GENERATION_UNKNOWN,
                                               CBM_STORE_DERIVED_STATUS_STALE),
              CBM_STORE_OK);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":48,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"search_graph\","
             "\"arguments\":{\"project\":\"semantic-stale\","
             "\"semantic_query\":[\"publish\"]}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "\"warnings\""));
    ASSERT_NOT_NULL(strstr(inner, "semantic_edges derived view is stale"));
    ASSERT(has_stale_freshness_view(inner, CBM_STORE_DERIVED_VIEW_SEMANTIC_EDGES));

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

TEST(tool_query_graph_warns_on_stale_route_view) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(st);

    const char *proj = "query-route-stale";
    ASSERT_EQ(cbm_store_upsert_project(st, proj, "/tmp/query-route-stale"), CBM_STORE_OK);
    cbm_mcp_server_set_project(srv, proj);
    ASSERT_EQ(cbm_store_set_derived_view_state(st, proj, CBM_STORE_DERIVED_VIEW_ROUTES,
                                               CBM_STORE_DERIVED_GENERATION_UNKNOWN,
                                               CBM_STORE_DERIVED_STATUS_STALE),
              CBM_STORE_OK);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":114,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"query_graph\","
             "\"arguments\":{\"project\":\"query-route-stale\","
             "\"query\":\"MATCH (r:Route) RETURN r.name LIMIT 5\"}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "\"warnings\""));
    ASSERT_NOT_NULL(strstr(inner, "routes derived view is stale"));
    ASSERT(has_stale_freshness_view(inner, CBM_STORE_DERIVED_VIEW_ROUTES));

    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_query_graph_warns_when_broad_query_returns_stale_route) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(st);

    const char *proj = "query-route-result-stale";
    ASSERT_EQ(cbm_store_upsert_project(st, proj, "/tmp/query-route-result-stale"), CBM_STORE_OK);
    cbm_mcp_server_set_project(srv, proj);
    cbm_node_t route = {.project = proj,
                        .label = "Route",
                        .name = "/api/status",
                        .qualified_name = "__route__GET__/api/status",
                        .file_path = "src/status.ts"};
    ASSERT_GT(cbm_store_upsert_node(st, &route), 0);
    ASSERT_EQ(cbm_store_set_derived_view_state(st, proj, CBM_STORE_DERIVED_VIEW_ROUTES,
                                               CBM_STORE_DERIVED_GENERATION_UNKNOWN,
                                               CBM_STORE_DERIVED_STATUS_STALE),
              CBM_STORE_OK);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":115,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"query_graph\","
             "\"arguments\":{\"project\":\"query-route-result-stale\","
             "\"query\":\"MATCH (n) RETURN n.label LIMIT 5\"}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "\"warnings\""));
    ASSERT_NOT_NULL(strstr(inner, "routes derived view is stale"));

    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_query_graph_warns_on_stale_semantic_edges) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(st);

    const char *proj = "query-semantic-stale";
    ASSERT_EQ(cbm_store_upsert_project(st, proj, "/tmp/query-semantic-stale"), CBM_STORE_OK);
    cbm_mcp_server_set_project(srv, proj);
    ASSERT_EQ(cbm_store_set_derived_view_state(st, proj, CBM_STORE_DERIVED_VIEW_SEMANTIC_EDGES,
                                               CBM_STORE_DERIVED_GENERATION_UNKNOWN,
                                               CBM_STORE_DERIVED_STATUS_STALE),
              CBM_STORE_OK);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":115,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"query_graph\","
             "\"arguments\":{\"project\":\"query-semantic-stale\","
             "\"query\":\"MATCH (a)-[:SEMANTICALLY_RELATED]->(b) "
             "RETURN a.name, b.name LIMIT 5\"}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "\"warnings\""));
    ASSERT_NOT_NULL(strstr(inner, "semantic_edges derived view is stale"));
    ASSERT(has_stale_freshness_view(inner, CBM_STORE_DERIVED_VIEW_SEMANTIC_EDGES));

    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_query_graph_warns_on_stale_similarity_edges) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(st);

    const char *proj = "query-similarity-stale";
    ASSERT_EQ(cbm_store_upsert_project(st, proj, "/tmp/query-similarity-stale"), CBM_STORE_OK);
    cbm_mcp_server_set_project(srv, proj);
    ASSERT_EQ(cbm_store_set_derived_view_state(st, proj, CBM_STORE_DERIVED_VIEW_SEMANTIC_EDGES,
                                               CBM_STORE_DERIVED_GENERATION_UNKNOWN,
                                               CBM_STORE_DERIVED_STATUS_STALE),
              CBM_STORE_OK);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":116,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"query_graph\","
             "\"arguments\":{\"project\":\"query-similarity-stale\","
             "\"query\":\"MATCH (a)-[:SIMILAR_TO]->(b) RETURN a.name, b.name LIMIT 5\"}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "\"warnings\""));
    ASSERT_NOT_NULL(strstr(inner, "semantic_edges derived view is stale"));
    ASSERT(has_stale_freshness_view(inner, CBM_STORE_DERIVED_VIEW_SEMANTIC_EDGES));

    free(inner);
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

TEST(tool_trace_path_not_found) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":20,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"trace_path\","
                                   "\"arguments\":{\"function_name\":\"NonExistent\","
                                   "\"project\":\"nonexistent\"}}}");
    ASSERT_NOT_NULL(resp);
    /* Should return error about project not found */
    ASSERT_NOT_NULL(strstr(resp, "not found"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_trace_call_path_alias_dispatches) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":20,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"trace_call_path\","
                                   "\"arguments\":{\"function_name\":\"NonExistent\","
                                   "\"project\":\"nonexistent\"}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "not found"));
    ASSERT_NULL(strstr(resp, "unknown tool"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_trace_missing_function_name) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":21,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"trace_path\","
                                   "\"arguments\":{}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "required"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

/* Regression: two same-named definitions with equal rank must be reported
 * ambiguous, not silently traced (trace_path previously took nodes[0]). */
TEST(tool_trace_path_ambiguous) {
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
             "\"params\":{\"name\":\"trace_path\","
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
TEST(tool_trace_path_prefers_definition) {
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
             "\"params\":{\"name\":\"trace_path\",\"arguments\":{\"function_name\":\"dup\","
             "\"project\":\"pref-proj\",\"direction\":\"outbound\"}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NULL(strstr(inner, "ambiguous"));
    /* picked the Function definition -> its outbound CALLS edge to "callee" shows */
    ASSERT_NOT_NULL(strstr(inner, "callee"));
    free(inner);
    free(resp);

    resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":63,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"trace_path\",\"arguments\":{\"function_name\":\"dup\","
             "\"project\":\"pref-proj\",\"direction\":\"outbound\",\"max_results\":0}}}");
    ASSERT_NOT_NULL(resp);
    inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "callee"));
    free(inner);
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_trace_path_warns_on_stale_rank_views) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(st);
    const char *proj = "trace-stale";
    cbm_mcp_server_set_project(srv, proj);
    ASSERT_EQ(cbm_store_upsert_project(st, proj, "/tmp/trace-stale"), CBM_STORE_OK);

    cbm_node_t root = {.project = proj,
                       .label = "Function",
                       .name = "root",
                       .qualified_name = "trace-stale.root",
                       .file_path = "root.c",
                       .start_line = 1,
                       .end_line = 10};
    cbm_node_t callee = {.project = proj,
                         .label = "Function",
                         .name = "callee",
                         .qualified_name = "trace-stale.callee",
                         .file_path = "callee.c",
                         .start_line = 11,
                         .end_line = 20};
    int64_t root_id = cbm_store_upsert_node(st, &root);
    int64_t callee_id = cbm_store_upsert_node(st, &callee);
    ASSERT_GT(root_id, 0);
    ASSERT_GT(callee_id, 0);
    cbm_edge_t edge = {.project = proj,
                       .source_id = root_id,
                       .target_id = callee_id,
                       .type = "CALLS"};
    ASSERT_GT(cbm_store_insert_edge(st, &edge), 0);

    const char *stale_views[] = {CBM_STORE_DERIVED_VIEW_PAGERANK,
                                 CBM_STORE_DERIVED_VIEW_LINKRANK};
    ASSERT_EQ(cbm_store_mark_derived_views_stale(st, proj, CBM_STORE_DERIVED_GENERATION_UNKNOWN,
                                                 stale_views,
                                                 (int)(sizeof(stale_views) / sizeof(stale_views[0]))),
              CBM_STORE_OK);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":64,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"trace_path\","
             "\"arguments\":{\"function_name\":\"root\",\"project\":\"trace-stale\","
             "\"direction\":\"outbound\"}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "callee"));
    ASSERT_NOT_NULL(strstr(inner, "\"warnings\""));
    ASSERT_NOT_NULL(strstr(inner, "pagerank derived view is stale"));
    ASSERT_NOT_NULL(strstr(inner, "linkrank derived view is stale"));
    ASSERT(has_stale_freshness_view(inner, CBM_STORE_DERIVED_VIEW_PAGERANK));
    ASSERT(has_stale_freshness_view(inner, CBM_STORE_DERIVED_VIEW_LINKRANK));

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

TEST(tool_get_architecture_warns_on_stale_derived_views) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(st);

    const char *proj = "arch-stale";
    ASSERT_EQ(cbm_store_upsert_project(st, proj, "/tmp/arch-stale"), CBM_STORE_OK);
    cbm_mcp_server_set_project(srv, proj);

    cbm_node_t fn = {.project = proj,
                     .label = "Function",
                     .name = "Run",
                     .qualified_name = "arch-stale.Run",
                     .file_path = "run.c"};
    int64_t id = cbm_store_upsert_node(st, &fn);
    ASSERT_GT(id, 0);
    char rank_sql[256];
    snprintf(rank_sql, sizeof(rank_sql),
             "INSERT INTO pagerank(project,node_id,rank,computed_at) "
             "VALUES('arch-stale',%lld,0.9,'2026-06-30T00:00:00Z')",
             (long long)id);
    ASSERT_EQ(cbm_store_exec(st, rank_sql), CBM_STORE_OK);
    const char *stale_views[] = {CBM_STORE_DERIVED_VIEW_PAGERANK,
                                 CBM_STORE_DERIVED_VIEW_ROUTES,
                                 CBM_STORE_DERIVED_VIEW_ARCHITECTURE};
    ASSERT_EQ(cbm_store_mark_derived_views_stale(st, proj, CBM_STORE_DERIVED_GENERATION_UNKNOWN,
                                                 stale_views,
                                                 (int)(sizeof(stale_views) / sizeof(stale_views[0]))),
              CBM_STORE_OK);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":94,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"get_architecture\","
             "\"arguments\":{\"project\":\"arch-stale\",\"aspects\":[\"all\"]}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "\"warnings\""));
    ASSERT_NOT_NULL(strstr(inner, "architecture derived view is stale"));
    ASSERT_NOT_NULL(strstr(inner, "routes derived view is stale"));
    ASSERT(has_stale_freshness_view(inner, CBM_STORE_DERIVED_VIEW_ARCHITECTURE));
    ASSERT(has_stale_freshness_view(inner, CBM_STORE_DERIVED_VIEW_ROUTES));
    ASSERT(has_stale_freshness_view(inner, CBM_STORE_DERIVED_VIEW_PAGERANK));
    ASSERT_NOT_NULL(strstr(inner, "key_functions were omitted"));
    ASSERT_NULL(strstr(inner, "\"key_functions\""));

    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

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

    char *resp_scoped = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":93,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"get_architecture\","
             "\"arguments\":{\"project\":\"arch-path\",\"path\":\"apps/hoa\","
             "\"aspects\":[\"packages\"]}}}");
    ASSERT_NOT_NULL(resp_scoped);
    char *inner_scoped = extract_text_content(resp_scoped);
    ASSERT_NOT_NULL(inner_scoped);

    ASSERT_NOT_NULL(strstr(inner_scoped, "\"path\""));
    ASSERT_NOT_NULL(strstr(inner_scoped, "root_total_nodes"));
    ASSERT_NOT_NULL(strstr(inner_scoped, "scoped_total_nodes"));
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

TEST(tool_index_repository_auto_index_deps_arg_disables_deps) {
    char *repo_tmp = th_mktempdir("cbm_mcp_dep_arg_repo");
    ASSERT_NOT_NULL(repo_tmp);
    char repo[CBM_PATH_MAX];
    int n = snprintf(repo, sizeof(repo), "%s", repo_tmp);
    ASSERT(n >= 0 && (size_t)n < sizeof(repo));

    char *cache_tmp = th_mktempdir("cbm_mcp_dep_arg_cache");
    ASSERT_NOT_NULL(cache_tmp);
    char cache[CBM_PATH_MAX];
    n = snprintf(cache, sizeof(cache), "%s", cache_tmp);
    ASSERT(n >= 0 && (size_t)n < sizeof(cache));

    const char *saved = getenv("CBM_CACHE_DIR");
    char *saved_copy = saved ? cbm_strdup(saved) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    cbm_config_t *cfg = cbm_config_open(cache);
    ASSERT_NOT_NULL(cfg);
    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_AUTO_INDEX_DEPS, "true"), 0);
    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_AUTO_DEP_LIMIT, "5"), 0);

    char vendor_dir[CBM_PATH_MAX];
    n = snprintf(vendor_dir, sizeof(vendor_dir), "%s/vendor/libdep", repo);
    ASSERT(n >= 0 && (size_t)n < sizeof(vendor_dir));
    ASSERT_EQ(th_mkdir_p(vendor_dir), 0);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "Makefile"), "all:\n\tcc main.c\n"), 0);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "main.c"), "int main(void) { return 0; }\n"), 0);
    ASSERT_EQ(th_write_file(TH_PATH(vendor_dir, "lib.c"), "int libdep(void) { return 1; }\n"), 0);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_mcp_server_set_config(srv, cfg);

    char req[CBM_SZ_4K];
    n = snprintf(req, sizeof(req),
                 "{\"jsonrpc\":\"2.0\",\"id\":42,\"method\":\"tools/call\","
                 "\"params\":{\"name\":\"index_repository\","
                 "\"arguments\":{\"repo_path\":\"%s\",\"mode\":\"fast\","
                 "\"auto_index_deps\":false}}}",
                 repo);
    ASSERT(n >= 0 && (size_t)n < sizeof(req));
    char *resp = cbm_mcp_server_handle(srv, req);
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "indexed"));
    ASSERT_NULL(strstr(resp, "dependencies_indexed"));
    free(resp);

    cbm_mcp_server_free(srv);
    cbm_config_close(cfg);
    if (saved_copy) {
        cbm_setenv("CBM_CACHE_DIR", saved_copy, 1);
        free(saved_copy);
    } else {
        cbm_unsetenv("CBM_CACHE_DIR");
    }
    th_cleanup(repo);
    th_cleanup(cache);
    PASS();
}

TEST(tool_index_repository_auto_dep_limit_arg_caps_deps) {
    char *repo_tmp = th_mktempdir("cbm_mcp_dep_limit_repo");
    ASSERT_NOT_NULL(repo_tmp);
    char repo[CBM_PATH_MAX];
    int n = snprintf(repo, sizeof(repo), "%s", repo_tmp);
    ASSERT(n >= 0 && (size_t)n < sizeof(repo));

    char *cache_tmp = th_mktempdir("cbm_mcp_dep_limit_cache");
    ASSERT_NOT_NULL(cache_tmp);
    char cache[CBM_PATH_MAX];
    n = snprintf(cache, sizeof(cache), "%s", cache_tmp);
    ASSERT(n >= 0 && (size_t)n < sizeof(cache));

    const char *saved = getenv("CBM_CACHE_DIR");
    char *saved_copy = saved ? cbm_strdup(saved) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    cbm_config_t *cfg = cbm_config_open(cache);
    ASSERT_NOT_NULL(cfg);
    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_AUTO_INDEX_DEPS, "true"), 0);
    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_AUTO_DEP_LIMIT, "5"), 0);

    ASSERT_EQ(th_write_file(TH_PATH(repo, "Makefile"), "all:\n\tcc main.c\n"), 0);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "main.c"), "int main(void) { return 0; }\n"), 0);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "vendor/liba/liba.c"), "int liba(void) { return 1; }\n"), 0);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "vendor/libb/libb.c"), "int libb(void) { return 2; }\n"), 0);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_mcp_server_set_config(srv, cfg);

    char req[CBM_SZ_4K];
    n = snprintf(req, sizeof(req),
                 "{\"jsonrpc\":\"2.0\",\"id\":43,\"method\":\"tools/call\","
                 "\"params\":{\"name\":\"index_repository\","
                 "\"arguments\":{\"repo_path\":\"%s\",\"mode\":\"fast\","
                 "\"auto_dep_limit\":1}}}",
                 repo);
    ASSERT(n >= 0 && (size_t)n < sizeof(req));
    char *resp = cbm_mcp_server_handle(srv, req);
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "indexed"));
    ASSERT_NOT_NULL(strstr(resp, "\\\"dependencies_indexed\\\":1"));
    free(resp);

    cbm_mcp_server_free(srv);
    cbm_config_close(cfg);
    if (saved_copy) {
        cbm_setenv("CBM_CACHE_DIR", saved_copy, 1);
        free(saved_copy);
    } else {
        cbm_unsetenv("CBM_CACHE_DIR");
    }
    th_cleanup(repo);
    th_cleanup(cache);
    PASS();
}

TEST(tool_index_repository_reports_incremental_containment_reason) {
    char *repo_tmp = th_mktempdir("cbm_mcp_publish_reason_repo");
    if (!repo_tmp) {
        PASS();
    }
    char repo[CBM_PATH_MAX];
    int n = snprintf(repo, sizeof(repo), "%s", repo_tmp);
    ASSERT(n >= 0 && (size_t)n < sizeof(repo));

    char *cache_tmp = th_mktempdir("cbm_mcp_publish_reason_cache");
    ASSERT_NOT_NULL(cache_tmp);
    char cache[CBM_PATH_MAX];
    n = snprintf(cache, sizeof(cache), "%s", cache_tmp);
    ASSERT(n >= 0 && (size_t)n < sizeof(cache));

    const char *saved = getenv("CBM_CACHE_DIR");
    char *saved_copy = saved ? cbm_strdup(saved) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    cbm_config_t *cfg = cbm_config_open(cache);
    ASSERT_NOT_NULL(cfg);
    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_INCREMENTAL_REINDEX, "always"), 0);

    ASSERT_EQ(th_write_file(TH_PATH(repo, "go.mod"), "module example.com/pubreason\n\ngo 1.22\n"),
              0);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "main.go"),
                            "package main\n\nfunc main() {\n\tHelper()\n\tLeaf()\n}\n"),
              0);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "helper.go"),
                            "package main\n\nfunc Helper() int {\n\treturn 1\n}\n"),
              0);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "leaf.go"),
                            "package main\n\nfunc Leaf() int {\n\treturn 2\n}\n"),
              0);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_mcp_server_set_config(srv, cfg);

    char req[CBM_SZ_4K];
    n = snprintf(req, sizeof(req),
                 "{\"jsonrpc\":\"2.0\",\"id\":41,\"method\":\"tools/call\","
                 "\"params\":{\"name\":\"index_repository\","
                 "\"arguments\":{\"repo_path\":\"%s\",\"mode\":\"fast\"}}}",
                 repo);
    ASSERT(n >= 0 && (size_t)n < sizeof(req));
    char *resp = cbm_mcp_server_handle(srv, req);
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "indexed"));
    free(resp);

    char *project = cbm_project_name_from_path(repo);
    ASSERT_NOT_NULL(project);
    n = snprintf(req, sizeof(req),
                 "{\"jsonrpc\":\"2.0\",\"id\":411,\"method\":\"tools/call\","
                 "\"params\":{\"name\":\"search_graph\","
                 "\"arguments\":{\"project\":\"%s\",\"query\":\"Helper\",\"limit\":5}}}",
                 project);
    ASSERT(n >= 0 && (size_t)n < sizeof(req));
    resp = cbm_mcp_server_handle(srv, req);
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "\"search_mode\":\"bm25\""));
    ASSERT_NOT_NULL(strstr(inner, "Helper"));
    free(inner);
    free(resp);
    free(project);

    ASSERT_EQ(th_write_file(TH_PATH(repo, "main.go"),
                            "package main\n\nfunc main() {\n\tHelper()\n\tLeaf()\n}\n\n"
                            "func NewMain() int {\n\treturn 11\n}\n"),
              0);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "helper.go"),
                            "package main\n\nfunc Helper() int {\n\treturn 3\n}\n\n"
                            "func NewHelper() int {\n\treturn 13\n}\n"),
              0);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "leaf.go"),
                            "package main\n\nfunc Leaf() int {\n\treturn 5\n}\n\n"
                            "func NewLeaf() int {\n\treturn 17\n}\n"),
              0);

    n = snprintf(req, sizeof(req),
                 "{\"jsonrpc\":\"2.0\",\"id\":42,\"method\":\"tools/call\","
                 "\"params\":{\"name\":\"index_repository\","
                 "\"arguments\":{\"repo_path\":\"%s\",\"mode\":\"fast\"}}}",
                 repo);
    ASSERT(n >= 0 && (size_t)n < sizeof(req));
    resp = cbm_mcp_server_handle(srv, req);
    ASSERT_NOT_NULL(resp);
    inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "\"publish_kind\":\"incremental_containment\""));
    ASSERT_NOT_NULL(strstr(inner, "\"publish_reason\":\"changed_batch_too_large\""));
    ASSERT_NOT_NULL(strstr(inner, "\"exact_delta\""));
    ASSERT_NOT_NULL(strstr(inner, "\"changed_paths\":3"));

    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    cbm_config_close(cfg);
    if (saved_copy) {
        cbm_setenv("CBM_CACHE_DIR", saved_copy, 1);
        free(saved_copy);
    } else {
        cbm_unsetenv("CBM_CACHE_DIR");
    }
    th_cleanup(repo);
    th_cleanup(cache);
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

TEST(search_code_limit_zero_uses_config_default) {
    char tmp[512];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":190,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"search_code\","
             "\"arguments\":{\"pattern\":\"HandleRequest\","
             "\"project\":\"test-project\",\"limit\":0}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    yyjson_doc *doc = yyjson_read(inner, strlen(inner), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *results = yyjson_obj_get(root, "results");
    ASSERT_NOT_NULL(results);
    ASSERT_GT(yyjson_arr_size(results), 0);

    yyjson_doc_free(doc);
    free(inner);
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

TEST(search_code_exact_path_filter_scopes_traversal) {
    char tmp[512];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":95,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"search_code\","
                                   "\"arguments\":{\"pattern\":\"HandleRequest\","
                                   "\"path_filter\":\"^main\\\\.go$\","
                                   "\"project\":\"test-project\"}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "\"search_scope\":\"path_filter_exact\""));
    ASSERT_NOT_NULL(strstr(inner, "HandleRequest"));
    ASSERT_NULL(strstr(inner, "\"isError\":true"));

    free(inner);
    free(resp);
    cleanup_snippet_dir(tmp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(search_code_git_worktree_scope_includes_untracked_source) {
    char tmp[512];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char proj_dir[512];
    snprintf(proj_dir, sizeof(proj_dir), "%s/project", tmp);
    char cmd[CBM_SZ_1K];
#ifdef _WIN32
    int n = snprintf(cmd, sizeof(cmd), "git -C \"%s\" init -q >NUL 2>NUL", proj_dir);
#else
    int n = snprintf(cmd, sizeof(cmd), "git -C \"%s\" init -q >/dev/null 2>/dev/null", proj_dir);
#endif
    ASSERT(n >= 0 && (size_t)n < sizeof(cmd));
    if (system(cmd) != 0) {
        cbm_mcp_server_free(srv);
        th_rmtree(tmp);
        FAIL("git init failed for search_code git worktree test");
    }

    char extra_path[512];
    snprintf(extra_path, sizeof(extra_path), "%s/active_edit.go", proj_dir);
    ASSERT_EQ(th_write_file(extra_path, "package main\nfunc UntrackedNeedle() {}\n"), 0);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":96,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"search_code\","
                                   "\"arguments\":{\"pattern\":\"UntrackedNeedle\","
                                   "\"project\":\"test-project\"}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "\"search_scope\":\"git_worktree\""));
    ASSERT_NOT_NULL(strstr(inner, "UntrackedNeedle"));
    ASSERT_NULL(strstr(inner, "\"isError\":true"));

    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    th_rmtree(tmp);
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
    cbm_unlink(adr_path);
    cbm_rmdir(adr_dir);
    cbm_rmdir(tmp_dir);
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
                           "\"is_exported\":true,"
                           "\"source\":\"infra\"}";
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
    cbm_unlink(path);
    snprintf(path, sizeof(path), "%s/project", tmp_dir);
    cbm_rmdir(path);
    cbm_rmdir(tmp_dir);
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

static int count_substr_mcp(const char *s, const char *needle) {
    int count = 0;
    if (!s || !needle) return 0;
    size_t nlen = strlen(needle);
    if (nlen == 0) return 0;
    while ((s = strstr(s, needle)) != NULL) {
        count++;
        s += nlen;
    }
    return count;
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
    /* compact: name omitted when it equals last segment of qualified_name */
    ASSERT_NULL(strstr(resp, "\"name\":\"HandleRequest\""));
    ASSERT_NOT_NULL(strstr(resp, "\"qualified_name\":\"test-project.cmd.server.main.HandleRequest\""));
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

TEST(snippet_source_key_is_code_body_only) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *resp =
        call_snippet(srv, "{\"qualified_name\":\"test-project.cmd.server.main.HandleRequest\","
                          "\"project\":\"test-project\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_EQ(count_substr_mcp(resp, "\"source\":"), 1);
    ASSERT_NOT_NULL(strstr(resp, "\"source_origin\":\"project\""));
    ASSERT_NOT_NULL(strstr(resp, "\"property_source\":\"infra\""));

    yyjson_doc *doc = yyjson_read(resp, strlen(resp), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *root = yyjson_doc_get_root(doc);
    const char *source = yyjson_get_str(yyjson_obj_get(root, "source"));
    ASSERT_NOT_NULL(source);
    ASSERT_NOT_NULL(strstr(source, "func HandleRequest() error"));
    yyjson_doc_free(doc);

    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

TEST(snippet_invalid_mode_errors) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *resp =
        call_snippet(srv, "{\"qualified_name\":\"test-project.cmd.server.main.HandleRequest\","
                          "\"project\":\"test-project\",\"mode\":\"compact\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"error\":\"invalid mode 'compact'\""));
    ASSERT_NOT_NULL(strstr(resp, "Valid values: full, signature, head_tail"));
    ASSERT_NULL(strstr(resp, "func HandleRequest() error"));

    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

/* ── TestSnippet_CompactFalse: name present when compact=false ── */

TEST(snippet_compact_false_name_present) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    /* compact=false: name must be present even when it equals last segment of QN */
    char *resp = call_snippet(srv, "{\"qualified_name\":\"test-project.cmd.server.main.HandleRequest\","
                                   "\"project\":\"test-project\","
                                   "\"compact\":false}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"name\":\"HandleRequest\""));
    ASSERT_NOT_NULL(strstr(resp, "\"qualified_name\":\"test-project.cmd.server.main.HandleRequest\""));
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
    /* compact: name omitted when it equals last segment of qualified_name */
    ASSERT_NULL(strstr(resp, "\"name\":\"HandleRequest\""));
    ASSERT_NOT_NULL(strstr(resp, "HandleRequest")); /* present in qualified_name */
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
    /* compact: name omitted when it equals last segment of qualified_name */
    ASSERT_NULL(strstr(resp, "\"name\":\"ProcessOrder\""));
    ASSERT_NOT_NULL(strstr(resp, "ProcessOrder")); /* present in qualified_name */
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
    /* compact: name omitted when it equals last segment of qualified_name */
    ASSERT_NULL(strstr(resp, "\"name\":\"HandleRequest\""));
    ASSERT_NOT_NULL(strstr(resp, "HandleRequest")); /* present in qualified_name */
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

enum { MCP_STDIO_TEST_TIMEOUT_SECONDS = 5 };

/* Signal handler used by alarm() to abort the test if it hangs */
static void alarm_handler(int sig) {
    (void)sig;
    /* Writing to stderr is async-signal-safe */
    const char msg[] = "FAIL: mcp_server_run_rapid_messages timed out (>5s)\n";
    write(STDERR_FILENO, msg, sizeof(msg) - 1);
    _exit(1);
}

static bool append_content_length_frame(char **dst, size_t *remaining, const char *json) {
    if (!dst || !*dst || !remaining || !json) return false;
    size_t len = strlen(json);
    int n = snprintf(*dst, *remaining, "Content-Length: %zu\r\n\r\n%s", len, json);
    if (n < 0 || (size_t)n >= *remaining) return false;
    *dst += n;
    *remaining -= (size_t)n;
    return true;
}

TEST(mcp_server_run_rapid_messages) {
    /* Simulate a client sending initialize + notifications/initialized +
     * tools/list all at once (no delays), which exercises the FILE*
     * buffering fix: the first getline() over-reads kernel data into the
     * libc buffer; without the fix, subsequent poll() calls block for 60s.
     *
     * We use alarm() to abort the test process if the server hangs. */
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
    alarm(MCP_STDIO_TEST_TIMEOUT_SECONDS);

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

TEST(mcp_stdio_output_has_only_jsonrpc_messages) {
    int fds[2];
    ASSERT_EQ(pipe(fds), 0);

    const char *msgs = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\","
                       "\"params\":{\"protocolVersion\":\"2025-11-25\",\"capabilities\":{}}}\n"
                       "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\",\"params\":{}}\n";
    ssize_t written = write(fds[1], msgs, strlen(msgs));
    ASSERT_TRUE(written > 0);
    close(fds[1]);

    FILE *in_fp = fdopen(fds[0], "r");
    ASSERT_NOT_NULL(in_fp);
    FILE *out_fp = tmpfile();
    ASSERT_NOT_NULL(out_fp);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    signal(SIGALRM, alarm_handler);
    alarm(MCP_STDIO_TEST_TIMEOUT_SECONDS);
    int rc = cbm_mcp_server_run(srv, in_fp, out_fp);
    alarm(0);
    signal(SIGALRM, SIG_DFL);
    ASSERT_EQ(rc, 0);

    ASSERT_EQ(fseek(out_fp, 0, SEEK_END), 0);
    long out_len = ftell(out_fp);
    ASSERT_TRUE(out_len > 0);
    rewind(out_fp);

    char *buf = malloc((size_t)out_len + 1);
    ASSERT_NOT_NULL(buf);
    size_t nread = fread(buf, 1, (size_t)out_len, out_fp);
    ASSERT_EQ(nread, (size_t)out_len);
    buf[nread] = '\0';

    int jsonrpc_lines = 0;
    char *line = buf;
    while (line && *line) {
        char *next = strchr(line, '\n');
        if (next) {
            *next = '\0';
        }
        if (*line != '\0') {
            ASSERT_EQ(line[0], '{');
            ASSERT_NOT_NULL(strstr(line, "\"jsonrpc\":\"2.0\""));
            size_t line_len = strlen(line);
            while (line_len > 0 && (line[line_len - 1] == '\r' || line[line_len - 1] == ' ' ||
                                    line[line_len - 1] == '\t')) {
                line_len--;
            }
            ASSERT_TRUE(line_len > 0);
            ASSERT_EQ(line[line_len - 1], '}');
            jsonrpc_lines++;
        }
        line = next ? next + 1 : NULL;
    }
    ASSERT_EQ(jsonrpc_lines, 2);

    free(buf);
    cbm_mcp_server_free(srv);
    fclose(out_fp);
    fclose(in_fp);
    PASS();
}

TEST(mcp_hidden_tools_reveal_sends_list_changed) {
    int fds[2];
    ASSERT_EQ(pipe(fds), 0);

    const char *msgs =
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\",\"params\":{}}\n"
        "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\","
        "\"params\":{\"name\":\"_hidden_tools\",\"arguments\":{}}}\n"
        "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"tools/list\",\"params\":{}}\n";
    ssize_t written = write(fds[1], msgs, strlen(msgs));
    ASSERT_TRUE(written > 0);
    close(fds[1]);

    FILE *in_fp = fdopen(fds[0], "r");
    ASSERT_NOT_NULL(in_fp);
    FILE *out_fp = tmpfile();
    ASSERT_NOT_NULL(out_fp);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    signal(SIGALRM, alarm_handler);
    alarm(MCP_STDIO_TEST_TIMEOUT_SECONDS);
    int rc = cbm_mcp_server_run(srv, in_fp, out_fp);
    alarm(0);
    signal(SIGALRM, SIG_DFL);
    ASSERT_EQ(rc, 0);

    ASSERT_EQ(fseek(out_fp, 0, SEEK_END), 0);
    long out_len = ftell(out_fp);
    ASSERT_TRUE(out_len > 0);
    rewind(out_fp);

    char *buf = malloc((size_t)out_len + 1);
    ASSERT_NOT_NULL(buf);
    size_t nread = fread(buf, 1, (size_t)out_len, out_fp);
    buf[nread] = '\0';

    ASSERT_NOT_NULL(strstr(buf, "\"id\":1"));
    ASSERT_NOT_NULL(strstr(buf, "\"id\":2"));
    ASSERT_NOT_NULL(strstr(buf, "\"id\":3"));
    ASSERT_NOT_NULL(strstr(buf, "notifications/tools/list_changed"));
    ASSERT_EQ(count_substr_mcp(buf, "\"name\":\"index_repository\""), 1);
    ASSERT_EQ(count_substr_mcp(buf, "\"name\":\"get_architecture\""), 1);

    free(buf);
    cbm_mcp_server_free(srv);
    fclose(out_fp);
    fclose(in_fp);
    PASS();
}

TEST(mcp_hidden_tools_reveal_frames_list_changed) {
    int fds[2];
    ASSERT_EQ(pipe(fds), 0);

    enum { FRAME_BUF_SIZE = CBM_SZ_2K };
    char msgs[FRAME_BUF_SIZE];
    char *cursor = msgs;
    size_t remaining = sizeof(msgs);
    ASSERT_TRUE(append_content_length_frame(
        &cursor, &remaining,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\",\"params\":{}}"));
    ASSERT_TRUE(append_content_length_frame(
        &cursor, &remaining,
        "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\","
        "\"params\":{\"name\":\"_hidden_tools\",\"arguments\":{}}}"));
    ASSERT_TRUE(append_content_length_frame(
        &cursor, &remaining,
        "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"tools/list\",\"params\":{}}"));

    size_t msg_len = (size_t)(cursor - msgs);
    ssize_t written = write(fds[1], msgs, msg_len);
    ASSERT_TRUE(written == (ssize_t)msg_len);
    close(fds[1]);

    FILE *in_fp = fdopen(fds[0], "r");
    ASSERT_NOT_NULL(in_fp);
    FILE *out_fp = tmpfile();
    ASSERT_NOT_NULL(out_fp);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    signal(SIGALRM, alarm_handler);
    alarm(MCP_STDIO_TEST_TIMEOUT_SECONDS);
    int rc = cbm_mcp_server_run(srv, in_fp, out_fp);
    alarm(0);
    signal(SIGALRM, SIG_DFL);
    ASSERT_EQ(rc, 0);

    ASSERT_EQ(fseek(out_fp, 0, SEEK_END), 0);
    long out_len = ftell(out_fp);
    ASSERT_TRUE(out_len > 0);
    rewind(out_fp);

    char *buf = malloc((size_t)out_len + 1);
    ASSERT_NOT_NULL(buf);
    size_t nread = fread(buf, 1, (size_t)out_len, out_fp);
    buf[nread] = '\0';

    ASSERT_EQ(count_substr_mcp(buf, "Content-Length:"), 4);
    ASSERT_NOT_NULL(strstr(buf, "notifications/tools/list_changed"));
    ASSERT_EQ(count_substr_mcp(buf, "\"name\":\"index_repository\""), 1);
    ASSERT_EQ(count_substr_mcp(buf, "\"name\":\"get_architecture\""), 1);

    free(buf);
    cbm_mcp_server_free(srv);
    fclose(out_fp);
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
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    yyjson_doc *doc = yyjson_read(inner, strlen(inner), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *root = yyjson_doc_get_root(doc);
    ASSERT_NOT_NULL(root);
    ASSERT_EQ((int)yyjson_get_int(yyjson_obj_get(root, "count")), ISSUE235_N);
    ASSERT_TRUE(yyjson_get_bool(yyjson_obj_get(root, "available_projects_truncated")));
    yyjson_val *projects = yyjson_obj_get(root, "available_projects");
    ASSERT_TRUE(yyjson_is_arr(projects));
    ASSERT_TRUE((int)yyjson_arr_size(projects) < ISSUE235_N);
    yyjson_doc_free(doc);
    free(inner);
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
    RUN_TEST(mcp_tools_list_classic_mode);
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
    RUN_TEST(tool_list_projects_includes_tmp_prefixed_project);
    RUN_TEST(resolve_store_quarantines_structurally_corrupt_db);
    RUN_TEST(resolve_store_leaves_foreign_sqlite_db_untouched);
    RUN_TEST(tool_get_graph_schema_empty);
    RUN_TEST(tool_unknown_tool);
    RUN_TEST(tool_search_graph_basic);
    RUN_TEST(tool_search_graph_includes_node_properties);
    RUN_TEST(tool_search_graph_warns_on_stale_pagerank_view);
    RUN_TEST(tool_search_graph_warns_on_stale_route_view);
    RUN_TEST(tool_search_graph_reports_dirty_metadata_without_hiding_canonical_rows);
    RUN_TEST(tool_search_graph_query_sees_file_delta_fts_updates);
    RUN_TEST(tool_search_graph_query_honors_file_pattern_issue552);
    RUN_TEST(tool_search_graph_query_uses_search_limit_config);
    RUN_TEST(tool_search_graph_query_rejects_bad_semantic_query);
    RUN_TEST(tool_search_graph_semantic_query_warns_on_stale_semantic_view);
    RUN_TEST(tool_query_graph_basic);
    RUN_TEST(tool_query_graph_warns_on_stale_route_view);
    RUN_TEST(tool_query_graph_warns_when_broad_query_returns_stale_route);
    RUN_TEST(tool_query_graph_warns_on_stale_semantic_edges);
    RUN_TEST(tool_query_graph_warns_on_stale_similarity_edges);
    RUN_TEST(tool_index_status_no_project);
    RUN_TEST(tool_index_status_includes_git_metadata);

    /* Tool handlers with validation */
    RUN_TEST(tool_trace_path_not_found);
    RUN_TEST(tool_trace_call_path_alias_dispatches);
    RUN_TEST(tool_trace_missing_function_name);
    RUN_TEST(tool_trace_path_ambiguous);
    RUN_TEST(tool_trace_path_prefers_definition);
    RUN_TEST(tool_trace_path_warns_on_stale_rank_views);
    RUN_TEST(tool_delete_project_not_found);
    RUN_TEST(tool_get_architecture_empty);
    RUN_TEST(tool_get_architecture_emits_populated_sections);
    RUN_TEST(tool_get_architecture_warns_on_stale_derived_views);
    RUN_TEST(tool_get_architecture_path_scoping);
    RUN_TEST(tool_query_graph_missing_query);

    /* Pipeline-dependent tool handlers */
    RUN_TEST(tool_index_repository_missing_path);
    RUN_TEST(tool_index_repository_auto_index_deps_arg_disables_deps);
    RUN_TEST(tool_index_repository_auto_dep_limit_arg_caps_deps);
    RUN_TEST(tool_index_repository_reports_incremental_containment_reason);
    RUN_TEST(tool_get_code_snippet_missing_qn);
    RUN_TEST(tool_get_code_snippet_not_found);
    RUN_TEST(tool_search_code_missing_pattern);
    RUN_TEST(tool_search_code_no_project);
    RUN_TEST(search_code_multi_word);
    RUN_TEST(search_code_limit_zero_uses_config_default);
    RUN_TEST(search_code_invalid_regex_errors_issue283);
    RUN_TEST(search_code_literal_pipe_warns_issue282);
    RUN_TEST(search_code_ampersand_accepted_issue272);
    RUN_TEST(search_code_exact_path_filter_scopes_traversal);
    RUN_TEST(search_code_git_worktree_scope_includes_untracked_source);
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
    RUN_TEST(mcp_stdio_output_has_only_jsonrpc_messages);
    RUN_TEST(mcp_hidden_tools_reveal_sends_list_changed);
    RUN_TEST(mcp_hidden_tools_reveal_frames_list_changed);
#endif

    /* Snippet resolution (port of snippet_test.go) */
    RUN_TEST(snippet_exact_qn);
    RUN_TEST(snippet_source_key_is_code_body_only);
    RUN_TEST(snippet_invalid_mode_errors);
    RUN_TEST(snippet_compact_false_name_present);
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
}
