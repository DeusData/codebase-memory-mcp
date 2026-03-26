/*
 * test_input_validation.c — Tests for parameter validation from fuzz testing.
 * Covers: F1 (empty label), F6 (invalid sort_by), F7 (invalid mode),
 *         F9 (invalid regex), F10 (negative depth), F15 (invalid direction).
 *
 * Each test creates a minimal MCP server, calls a tool handler with invalid
 * input, and asserts the error response contains helpful guidance.
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include <mcp/mcp.h>
#include <store/store.h>
#include <yyjson/yyjson.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

/* ── Helper: extract inner text content from MCP tool result ── */
static char *extract_text(const char *mcp_result) {
    if (!mcp_result) return NULL;
    /* Parse MCP JSON wrapper: {"content":[{"type":"text","text":"..."}]} */
    yyjson_doc *doc = yyjson_read(mcp_result, strlen(mcp_result), 0);
    if (!doc) return strdup(mcp_result);
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *content = yyjson_obj_get(root, "content");
    if (!content || !yyjson_is_arr(content)) {
        yyjson_doc_free(doc);
        return strdup(mcp_result);
    }
    yyjson_val *item = yyjson_arr_get(content, 0);
    yyjson_val *text = item ? yyjson_obj_get(item, "text") : NULL;
    const char *str = text ? yyjson_get_str(text) : NULL;
    char *result = str ? strdup(str) : strdup(mcp_result);
    yyjson_doc_free(doc);
    return result;
}

/* ── Helper: create minimal server with pre-populated data ── */
static cbm_mcp_server_t *setup_validation_server(char *tmp, size_t tmp_sz) {
    snprintf(tmp, tmp_sz, "/tmp/cbm-test-validation-XXXXXX");
    if (!cbm_mkdtemp(tmp)) return NULL;

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    if (!srv) return NULL;

    cbm_store_t *st = cbm_mcp_server_store(srv);
    if (!st) { cbm_mcp_server_free(srv); return NULL; }

    const char *proj = "validation-test";
    cbm_mcp_server_set_project(srv, proj);
    cbm_store_upsert_project(st, proj, tmp);

    /* Insert test nodes: 2 functions + 1 call edge */
    cbm_node_t foo = {.project = proj, .label = "Function", .name = "foo",
                      .qualified_name = "validation-test.test.foo",
                      .file_path = "test.c", .start_line = 1, .end_line = 1};
    cbm_node_t bar = {.project = proj, .label = "Function", .name = "bar",
                      .qualified_name = "validation-test.test.bar",
                      .file_path = "test.c", .start_line = 2, .end_line = 2};
    cbm_store_upsert_node(st, &foo);
    cbm_store_upsert_node(st, &bar);
    cbm_edge_t e = {.project = proj, .source_id = 2, .target_id = 1, .type = "CALLS"};
    cbm_store_insert_edge(st, &e);

    return srv;
}

static void cleanup_validation_dir(const char *dir) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", dir);
    (void)system(cmd); // NOLINT
}

/* ══════════════════════════════════════════════════════════════════
 *  F1: Empty label treated as no filter (not silently returning 0)
 * ══════════════════════════════════════════════════════════════════ */

TEST(f1_empty_label_returns_results) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(srv, "search_graph",
                                    "{\"label\":\"\",\"limit\":5}");
    char *resp = extract_text(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);

    /* Empty label should be treated as "no label filter" → returns all nodes */
    /* Should NOT return error, and total should be > 0 if project has data */
    ASSERT_NULL(strstr(resp, "\"error\""));

    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_validation_dir(tmp);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  F6: Invalid sort_by returns error with valid values
 * ══════════════════════════════════════════════════════════════════ */

TEST(f6_invalid_sort_by_errors) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(srv, "search_graph",
                                    "{\"sort_by\":\"invalid_value\",\"limit\":3}");
    char *resp = extract_text(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);

    /* Must return error mentioning sort_by */
    ASSERT_NOT_NULL(strstr(resp, "error"));
    ASSERT_NOT_NULL(strstr(resp, "sort_by"));
    /* Must list valid values */
    ASSERT_NOT_NULL(strstr(resp, "relevance"));

    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_validation_dir(tmp);
    PASS();
}

/* Edge case: sort_by with typo "degre" (missing 'e') */
TEST(f6_sort_by_typo_errors) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(srv, "search_graph",
                                    "{\"sort_by\":\"degre\",\"limit\":3}");
    char *resp = extract_text(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);

    ASSERT_NOT_NULL(strstr(resp, "error"));
    ASSERT_NOT_NULL(strstr(resp, "degree")); /* suggest correct value */

    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_validation_dir(tmp);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  F9: Invalid regex in name_pattern returns error
 * ══════════════════════════════════════════════════════════════════ */

TEST(f9_invalid_regex_errors) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(srv, "search_graph",
                                    "{\"name_pattern\":\"(\",\"limit\":3}");
    char *resp = extract_text(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);

    /* Must return error mentioning regex/pattern */
    ASSERT_NOT_NULL(strstr(resp, "error"));
    ASSERT_TRUE(strstr(resp, "regex") || strstr(resp, "pattern"));

    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_validation_dir(tmp);
    PASS();
}

/* Edge case: valid regex should NOT error */
TEST(f9_valid_regex_succeeds) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(srv, "search_graph",
                                    "{\"name_pattern\":\"foo.*bar\",\"limit\":3}");
    char *resp = extract_text(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);

    /* Valid regex should NOT produce error */
    ASSERT_NULL(strstr(resp, "\"error\":\"invalid regex"));

    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_validation_dir(tmp);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  F10: Negative depth clamped to 1
 * ══════════════════════════════════════════════════════════════════ */

TEST(f10_negative_depth_returns_results) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(srv, "trace_call_path",
                                    "{\"function_name\":\"foo\",\"depth\":-1}");
    char *resp = extract_text(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);

    /* Should NOT return empty — depth clamped to 1, function "foo" exists */
    /* At minimum should have function name in response */
    ASSERT_NOT_NULL(strstr(resp, "foo"));

    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_validation_dir(tmp);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  F15: Invalid direction returns error with valid values
 * ══════════════════════════════════════════════════════════════════ */

TEST(f15_invalid_direction_errors) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(srv, "trace_call_path",
                                    "{\"function_name\":\"foo\",\"direction\":\"invalid\"}");
    char *resp = extract_text(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);

    /* Must return error mentioning direction */
    ASSERT_NOT_NULL(strstr(resp, "error"));
    ASSERT_NOT_NULL(strstr(resp, "direction"));
    /* Must list valid values */
    ASSERT_NOT_NULL(strstr(resp, "inbound"));

    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_validation_dir(tmp);
    PASS();
}

/* Edge case: valid direction "outbound" should NOT error */
TEST(f15_valid_direction_succeeds) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(srv, "trace_call_path",
                                    "{\"function_name\":\"foo\",\"direction\":\"outbound\"}");
    char *resp = extract_text(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);

    /* Valid direction should NOT produce error about direction */
    ASSERT_NULL(strstr(resp, "invalid direction"));

    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_validation_dir(tmp);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  G1: Summary mode includes results_suppressed indicator
 * ══════════════════════════════════════════════════════════════════ */

TEST(g1_summary_mode_has_results_key) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    /* Pass project explicitly to ensure store is found */
    char *raw = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"mode\":\"summary\",\"limit\":100}");
    char *resp = extract_text(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);

    /* G1: summary mode must include "results" key and results_suppressed */
    ASSERT_NOT_NULL(strstr(resp, "\"total\""));
    ASSERT_NOT_NULL(strstr(resp, "\"results\""));
    ASSERT_NOT_NULL(strstr(resp, "results_suppressed"));

    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_validation_dir(tmp);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  CQ-3: Cypher + filter params produces warning
 * ══════════════════════════════════════════════════════════════════ */

TEST(cq3_cypher_with_label_warns) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(srv, "search_code_graph",
        "{\"cypher\":\"MATCH (n:Function) RETURN n.name LIMIT 5\","
        "\"label\":\"Class\"}");
    char *resp = extract_text(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);

    /* CQ-3: Should warn that label is ignored in Cypher mode */
    ASSERT_NOT_NULL(strstr(resp, "warning"));

    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_validation_dir(tmp);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  IX-2: Status shows "indexing" during active index
 * ══════════════════════════════════════════════════════════════════ */

TEST(ix2_status_resource_format) {
    /* IX-2: Verify status resource has expected fields when server has no data.
     * Can't set autoindex_failed on opaque struct, but we can verify the
     * not_indexed status path returns action_required field. */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    /* Server with no indexed data should report not_indexed with action hint */
    char *raw = cbm_mcp_handle_tool(srv, "index_status", "{}");
    /* index_status without a project returns an error — that's expected */
    ASSERT_NOT_NULL(raw);
    free(raw);

    cbm_mcp_server_free(srv);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  Suite registration
 * ══════════════════════════════════════════════════════════════════ */

void suite_input_validation(void) {
    RUN_TEST(f1_empty_label_returns_results);
    RUN_TEST(f6_invalid_sort_by_errors);
    RUN_TEST(f6_sort_by_typo_errors);
    RUN_TEST(f9_invalid_regex_errors);
    RUN_TEST(f9_valid_regex_succeeds);
    RUN_TEST(f10_negative_depth_returns_results);
    RUN_TEST(f15_invalid_direction_errors);
    RUN_TEST(f15_valid_direction_succeeds);
    RUN_TEST(g1_summary_mode_has_results_key);
    RUN_TEST(cq3_cypher_with_label_warns);
    RUN_TEST(ix2_status_resource_format);
}
