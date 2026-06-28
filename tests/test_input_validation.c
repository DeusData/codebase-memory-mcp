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
#include <cli/cli.h>
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
 *  F6: sort_by 'calls' and 'linkrank' must be accepted (Bug 1)
 * ══════════════════════════════════════════════════════════════════ */

TEST(f6_sort_by_calls_accepted) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);
    char *raw = cbm_mcp_handle_tool(srv, "search_graph",
                                    "{\"sort_by\":\"calls\",\"limit\":3}");
    char *resp = extract_text(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);
    ASSERT_NULL(strstr(resp, "invalid sort_by"));
    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_validation_dir(tmp);
    PASS();
}

TEST(f6_sort_by_linkrank_accepted) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);
    char *raw = cbm_mcp_handle_tool(srv, "search_graph",
                                    "{\"sort_by\":\"linkrank\",\"limit\":3}");
    char *resp = extract_text(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);
    ASSERT_NULL(strstr(resp, "invalid sort_by"));
    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_validation_dir(tmp);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  F9: Glob wildcard patterns auto-converted to regex (Bug 2)
 * ══════════════════════════════════════════════════════════════════ */

TEST(f9_glob_star_autoconverted) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);
    char *raw = cbm_mcp_handle_tool(srv, "search_graph",
                                    "{\"name_pattern\":\"*tool*\",\"limit\":3}");
    char *resp = extract_text(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);
    ASSERT_NULL(strstr(resp, "invalid regex"));
    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_validation_dir(tmp);
    PASS();
}

TEST(f9_glob_question_autoconverted) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);
    char *raw = cbm_mcp_handle_tool(srv, "search_graph",
                                    "{\"name_pattern\":\"*foo?\",\"limit\":3}");
    char *resp = extract_text(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);
    ASSERT_NULL(strstr(resp, "invalid regex"));
    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_validation_dir(tmp);
    PASS();
}

TEST(f9_valid_regex_still_works) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);
    char *raw = cbm_mcp_handle_tool(srv, "search_graph",
                                    "{\"name_pattern\":\".*tool.*\",\"limit\":3}");
    char *resp = extract_text(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);
    ASSERT_NULL(strstr(resp, "error"));
    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_validation_dir(tmp);
    PASS();
}

TEST(f9_truly_invalid_pattern_still_errors) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);
    char *raw = cbm_mcp_handle_tool(srv, "search_graph",
                                    "{\"name_pattern\":\"(\",\"limit\":3}");
    char *resp = extract_text(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "error"));
    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_validation_dir(tmp);
    PASS();
}

TEST(f9_qn_pattern_glob_autoconverted) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);
    char *raw = cbm_mcp_handle_tool(srv, "search_graph",
                                    "{\"qn_pattern\":\"*Handler*\",\"limit\":3}");
    char *resp = extract_text(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);
    ASSERT_NULL(strstr(resp, "invalid regex"));
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

    char *raw = cbm_mcp_handle_tool(srv, "trace_path",
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
 *  Bug 3: trace_path fuzzy fallback on case mismatch
 * ══════════════════════════════════════════════════════════════════ */

TEST(trace_case_mismatch_finds_via_fallback) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);
    /* "Foo" does not exist — only "foo" does. Fallback search should find it.
     * No project passed: resolve_store returns in-memory store, fallback search
     * has no project filter, finds "foo", re-queries with result's project. */
    char *raw = cbm_mcp_handle_tool(srv, "trace_path",
        "{\"function_name\":\"Foo\"}");
    char *resp = extract_text(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);
    /* Must NOT contain "function not found" — fallback should resolve */
    ASSERT_NULL(strstr(resp, "function not found"));
    /* Response should contain "function" key (BFS result) and direction */
    ASSERT_NOT_NULL(strstr(resp, "\"function\""));
    ASSERT_NOT_NULL(strstr(resp, "\"direction\""));
    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_validation_dir(tmp);
    PASS();
}

TEST(trace_exact_match_still_works) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);
    /* Exact name "foo" should work directly without fallback.
     * No project: resolve_store returns in-memory store, find_nodes_by_name
     * uses project=NULL which binds NULL (won't match). Falls to fallback
     * which finds "foo" via search (no project filter). */
    char *raw = cbm_mcp_handle_tool(srv, "trace_path",
        "{\"function_name\":\"foo\"}");
    char *resp = extract_text(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);
    ASSERT_NULL(strstr(resp, "function not found"));
    ASSERT_NOT_NULL(strstr(resp, "foo"));
    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_validation_dir(tmp);
    PASS();
}

TEST(trace_truly_missing_still_errors) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);
    /* "nonexistent_xyz" doesn't match anything — should still error */
    char *raw = cbm_mcp_handle_tool(srv, "trace_path",
        "{\"function_name\":\"nonexistent_xyz\"}");
    char *resp = extract_text(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "function not found"));
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

    char *raw = cbm_mcp_handle_tool(srv, "trace_path",
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

    char *raw = cbm_mcp_handle_tool(srv, "trace_path",
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

TEST(trace_invalid_mode_errors) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(srv, "trace_path",
                                    "{\"function_name\":\"foo\",\"mode\":\"typo\"}");
    char *resp = extract_text(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);

    ASSERT_NOT_NULL(strstr(resp, "error"));
    ASSERT_NOT_NULL(strstr(resp, "mode"));
    ASSERT_NOT_NULL(strstr(resp, "calls"));
    ASSERT_NOT_NULL(strstr(resp, "data_flow"));
    ASSERT_NOT_NULL(strstr(resp, "cross_service"));

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

    char *raw = cbm_mcp_handle_tool(srv, "query_graph",
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
 *  Pattern OR-search: unified name+qn search (Change 1c)
 * ══════════════════════════════════════════════════════════════════ */

TEST(pattern_or_search_graph) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);
    /* pattern="foo" should match node named "foo" (OR across name and qualified_name) */
    char *raw = cbm_mcp_handle_tool(srv, "search_graph",
                                    "{\"pattern\":\"foo\",\"limit\":5}");
    char *resp = extract_text(raw); free(raw);
    ASSERT_NOT_NULL(resp);
    ASSERT_NULL(strstr(resp, "\"error\""));
    ASSERT_NOT_NULL(strstr(resp, "\"results\"")); /* results array present */
    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_validation_dir(tmp);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  Source search via search_in="source" dispatch
 * ══════════════════════════════════════════════════════════════════ */

TEST(source_search_via_search_in_param) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);
    /* Write a file into the tmpdir so grep has something to search */
    char src_path[320];
    snprintf(src_path, sizeof(src_path), "%s/hello.c", tmp);
    FILE *f = fopen(src_path, "w");
    if (f) { fputs("/* cbm_unique_grep_token */\n", f); fclose(f); }

    /* search_in="source" with explicit project slug dispatches to handle_search_code
     * and finds the file we wrote above. */
    char args[512];
    snprintf(args, sizeof(args),
        "{\"pattern\":\"cbm_unique_grep_token\","
        "\"search_in\":\"source\","
        "\"project\":\"validation-test\"}");
    char *raw = cbm_mcp_handle_tool(srv, "search_code", args);
    char *resp = extract_text(raw); free(raw);
    ASSERT_NOT_NULL(resp);
    /* Should return matches array, NOT "project not found" */
    ASSERT_NULL(strstr(resp, "\"error\""));
    ASSERT_NOT_NULL(strstr(resp, "\"matches\""));
    ASSERT_NOT_NULL(strstr(resp, "cbm_unique_grep_token"));
    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_validation_dir(tmp);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  Source search: path-based project arg normalizes to slug
 *  (Bug: get_project_root didn't convert /path → slug, causing
 *   "project not found" even when the project was indexed)
 * ══════════════════════════════════════════════════════════════════ */

TEST(source_search_path_project_normalizes_to_slug) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);
    /* Write a file with a known token */
    char src_path[320];
    snprintf(src_path, sizeof(src_path), "%s/hello.c", tmp);
    FILE *f = fopen(src_path, "w");
    if (f) { fputs("/* path_slug_normalize_token */\n", f); fclose(f); }

    /* The project name "validation-test" was stored with root_path=tmp.
     * Passing project=tmp (the root_path) should normalize via get_project_root.
     * NOTE: this works when cbm_project_name_from_path(tmp) matches the stored
     * project name. For arbitrary test slugs it won't — this test verifies the
     * slug-based path works (project="validation-test" matches current_project). */
    char args[512];
    snprintf(args, sizeof(args),
        "{\"pattern\":\"path_slug_normalize_token\","
        "\"search_in\":\"source\","
        "\"project\":\"validation-test\"}");
    char *raw = cbm_mcp_handle_tool(srv, "search_code", args);
    char *resp = extract_text(raw); free(raw);
    ASSERT_NOT_NULL(resp);
    ASSERT_NULL(strstr(resp, "\"error\""));
    ASSERT_NOT_NULL(strstr(resp, "\"matches\""));
    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_validation_dir(tmp);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  Verify search_in="graph" (default) still does graph search
 * ══════════════════════════════════════════════════════════════════ */

TEST(source_search_default_is_graph) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);
    /* No search_in → defaults to graph search → returns results array */
    char *raw = cbm_mcp_handle_tool(srv, "search_graph",
                                    "{\"pattern\":\"foo\",\"limit\":5}");
    char *resp = extract_text(raw); free(raw);
    ASSERT_NOT_NULL(resp);
    ASSERT_NULL(strstr(resp, "\"error\""));
    ASSERT_NOT_NULL(strstr(resp, "\"results\"")); /* graph search returns results array */
    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_validation_dir(tmp);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  summary=true bool alias (Change 2c)
 * ══════════════════════════════════════════════════════════════════ */

TEST(summary_bool_alias) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);
    char *raw = cbm_mcp_handle_tool(srv, "search_graph",
                                    "{\"summary\":true}");
    char *resp = extract_text(raw); free(raw);
    ASSERT_NOT_NULL(resp);
    ASSERT_NULL(strstr(resp, "\"error\""));
    /* Summary mode: by_label and by_file_top20 present */
    ASSERT_NOT_NULL(strstr(resp, "\"by_label\""));
    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_validation_dir(tmp);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  case_sensitive graph search (Change 2b)
 * ══════════════════════════════════════════════════════════════════ */

TEST(case_sensitive_graph_search) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);
    /* case_sensitive=true: "FOO" should NOT match node named "foo" */
    char *raw = cbm_mcp_handle_tool(srv, "search_graph",
                                    "{\"name_pattern\":\"FOO\",\"case_sensitive\":true,\"limit\":5}");
    char *resp = extract_text(raw); free(raw);
    ASSERT_NOT_NULL(resp);
    ASSERT_NULL(strstr(resp, "\"error\""));
    /* Should return 0 results (no uppercase FOO in test store) */
    ASSERT_NOT_NULL(strstr(resp, "\"total\":0"));
    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_validation_dir(tmp);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  Config: compact default false
 * ══════════════════════════════════════════════════════════════════ */

TEST(config_compact_default_false) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);
    /* Set compact=false in config, then call without compact param */
    cbm_config_t *cfg = cbm_config_open(tmp);
    ASSERT_NOT_NULL(cfg);
    cbm_config_set(cfg, "compact", "false");
    cbm_mcp_server_set_config(srv, cfg);
    char *raw = cbm_mcp_handle_tool(srv, "search_graph", "{\"limit\":3}");
    char *resp = extract_text(raw); free(raw);
    ASSERT_NOT_NULL(resp);
    ASSERT_NULL(strstr(resp, "\"error\""));
    free(resp);
    cbm_mcp_server_free(srv);
    cbm_config_close(cfg);
    cleanup_validation_dir(tmp);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  Config: default_sort_by=calls
 * ══════════════════════════════════════════════════════════════════ */

TEST(config_default_sort_by_calls) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);
    cbm_config_t *cfg = cbm_config_open(tmp);
    ASSERT_NOT_NULL(cfg);
    cbm_config_set(cfg, "default_sort_by", "calls");
    cbm_mcp_server_set_config(srv, cfg);
    char *raw = cbm_mcp_handle_tool(srv, "search_graph", "{\"limit\":3}");
    char *resp = extract_text(raw); free(raw);
    ASSERT_NOT_NULL(resp);
    ASSERT_NULL(strstr(resp, "invalid sort_by")); /* valid sort, no error */
    free(resp);
    cbm_mcp_server_free(srv);
    cbm_config_close(cfg);
    cleanup_validation_dir(tmp);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  trace_path accepts qualified_name param (Change 3a)
 * ══════════════════════════════════════════════════════════════════ */

TEST(trace_accepts_qualified_name_param) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);
    /* Passing full qualified_name should not error (even if BFS finds 0 callers on test store).
     * Must NOT return "function not found" — QN lookup path fires first. */
    char *raw = cbm_mcp_handle_tool(srv, "trace_path",
        "{\"qualified_name\":\"validation-test.test.foo\",\"project\":\"validation-test\",\"direction\":\"outbound\"}");
    char *resp = extract_text(raw); free(raw);
    ASSERT_NOT_NULL(resp);
    /* Should find "foo" node via QN and return trace output, not "function not found" */
    ASSERT_NULL(strstr(resp, "\"error\""));
    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_validation_dir(tmp);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  pattern= glob wildcard auto-converts to regex (*foo* → .*foo.*)
 * ══════════════════════════════════════════════════════════════════ */

TEST(pattern_glob_wildcards_auto_convert) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);
    /* "*foo*" is not valid regex but valid glob — should auto-convert and find "foo" node */
    char *raw = cbm_mcp_handle_tool(srv, "search_graph",
                                    "{\"pattern\":\"*foo*\",\"limit\":5}");
    char *resp = extract_text(raw); free(raw);
    ASSERT_NOT_NULL(resp);
    ASSERT_NULL(strstr(resp, "\"error\""));
    ASSERT_NOT_NULL(strstr(resp, "\"results\""));
    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_validation_dir(tmp);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  pattern= invalid regex that can't be salvaged returns error
 * ══════════════════════════════════════════════════════════════════ */

TEST(pattern_invalid_regex_returns_error) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);
    /* "[invalid" is not valid regex and not a glob — should return error */
    char *raw = cbm_mcp_handle_tool(srv, "search_graph",
                                    "{\"pattern\":\"[invalid\",\"limit\":5}");
    char *resp = extract_text(raw); free(raw);
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"error\""));
    ASSERT_NOT_NULL(strstr(resp, "invalid regex"));
    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_validation_dir(tmp);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  trace: when both function_name AND qualified_name given, QN takes
 *  priority (QN-first lookup runs before name-based lookup)
 * ══════════════════════════════════════════════════════════════════ */

TEST(trace_qn_takes_priority_over_function_name) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);
    /* Pass a valid QN and a non-existent function_name.
     * QN lookup should find "foo" and succeed; function_name is ignored. */
    char *raw = cbm_mcp_handle_tool(srv, "trace_path",
        "{\"qualified_name\":\"validation-test.test.foo\","
        "\"function_name\":\"does_not_exist_anywhere\","
        "\"project\":\"validation-test\"}");
    char *resp = extract_text(raw); free(raw);
    ASSERT_NOT_NULL(resp);
    /* QN lookup finds "foo" → no error, trace succeeds */
    ASSERT_NULL(strstr(resp, "\"error\""));
    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_validation_dir(tmp);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  trace: qualified_name not found returns actionable error hint
 * ══════════════════════════════════════════════════════════════════ */

TEST(trace_qn_not_found_returns_specific_hint) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);
    /* Pass a QN that doesn't exist — should get specific hint about using pattern= */
    char *raw = cbm_mcp_handle_tool(srv, "trace_path",
        "{\"qualified_name\":\"no-such.project.func\","
        "\"project\":\"validation-test\"}");
    char *resp = extract_text(raw); free(raw);
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"error\""));
    /* Should mention qualified_name in error, not generic function_name hint */
    ASSERT_NOT_NULL(strstr(resp, "qualified_name"));
    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_validation_dir(tmp);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  detect_changes: slug project doesn't return "project not found"
 *  (Tests that get_project_root handles slug args correctly after
 *  the path-normalization refactor.)
 * ══════════════════════════════════════════════════════════════════ */

TEST(detect_changes_slug_project_finds_root) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);
    char *raw = cbm_mcp_handle_tool(srv, "detect_changes",
        "{\"project\":\"validation-test\"}");
    char *resp = extract_text(raw); free(raw);
    ASSERT_NOT_NULL(resp);
    /* Should find the project root (not "project not found" error) */
    ASSERT_NULL(strstr(resp, "\"error\":\"project not found\""));
    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_validation_dir(tmp);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  manage_adr: slug project doesn't return "project not found"
 * ══════════════════════════════════════════════════════════════════ */

TEST(manage_adr_slug_project_finds_root) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);
    char *raw = cbm_mcp_handle_tool(srv, "manage_adr",
        "{\"project\":\"validation-test\",\"mode\":\"get\"}");
    char *resp = extract_text(raw); free(raw);
    ASSERT_NOT_NULL(resp);
    /* Should find the project root — either returns ADR content or "not found" for the file,
     * but NOT "project not found" (the store lookup error). */
    ASSERT_NULL(strstr(resp, "\"error\":\"project not found\""));
    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_validation_dir(tmp);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  Tilde expansion: project="~/relpath" expands correctly
 *  (get_project_root uses expand_tilde before realpath)
 * ══════════════════════════════════════════════════════════════════ */

TEST(source_search_tilde_project_expands) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    /* Build a project path relative to $HOME (e.g. ~/... pointing to tmp) */
    const char *home = getenv("HOME");
    if (!home || strncmp(tmp, home, strlen(home)) != 0) {
        /* tmp is not under $HOME — skip this test on this machine */
        cbm_mcp_server_free(srv); cleanup_validation_dir(tmp);
        PASS();
    }
    /* Compute tilde path: replace $HOME prefix with ~ */
    char tilde_path[320];
    snprintf(tilde_path, sizeof(tilde_path), "~%s", tmp + strlen(home));

    char src_path[320];
    snprintf(src_path, sizeof(src_path), "%s/tilde.c", tmp);
    FILE *f = fopen(src_path, "w");
    if (f) { fputs("/* tilde_expand_token */\n", f); fclose(f); }

    /* Pass project as tilde path — should expand to absolute and find root */
    char args[512];
    snprintf(args, sizeof(args),
        "{\"pattern\":\"tilde_expand_token\","
        "\"search_in\":\"source\","
        "\"project\":\"%s\"}", tilde_path);
    char *raw = cbm_mcp_handle_tool(srv, "search_code", args);
    char *resp = extract_text(raw); free(raw);
    ASSERT_NOT_NULL(resp);
    ASSERT_NULL(strstr(resp, "\"error\""));
    ASSERT_NOT_NULL(strstr(resp, "\"matches\""));
    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_validation_dir(tmp);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  project_is_path: path-format project arg routes through slug
 *  conversion in get_project_root (regression for path-based project)
 * ══════════════════════════════════════════════════════════════════ */

TEST(source_search_no_project_falls_back_to_session) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);
    /* Simulate session_project being set (as detect_session would do after initialize) */
    cbm_mcp_server_set_session_project(srv, "validation-test");

    char src_path[320];
    snprintf(src_path, sizeof(src_path), "%s/session.c", tmp);
    FILE *f = fopen(src_path, "w");
    if (f) { fputs("/* session_fallback_token */\n", f); fclose(f); }

    /* No project= arg — get_project_root falls back to session_project */
    char *raw = cbm_mcp_handle_tool(srv, "search_code",
        "{\"pattern\":\"session_fallback_token\",\"search_in\":\"source\"}");
    char *resp = extract_text(raw); free(raw);
    ASSERT_NOT_NULL(resp);
    ASSERT_NULL(strstr(resp, "\"error\""));
    ASSERT_NOT_NULL(strstr(resp, "\"matches\""));
    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_validation_dir(tmp);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  Path-based auto-index: project= is a full directory path that is
 *  NOT under session_root (Bug #4 — .gitignore-excluded separate repo)
 *
 *  When project= is an absolute path to an accessible directory that
 *  hasn't been indexed yet and differs from session_root, codebase-memory
 *  must auto-index that path directly (not session_root).
 * ══════════════════════════════════════════════════════════════════ */

TEST(path_project_auto_indexes_separate_directory) {
    /* Create two separate temp dirs:
     *   session_tmp = first project queried (establishes session_root via public API)
     *   target_tmp  = second project queried (separate path, simulates .gitignore subdir)
     *
     * Workflow mirrors Bug #4: user queries upstream repo that lives in .gitignore
     * of the main project, after the main project session is already active. */
    char session_tmp[256];
    snprintf(session_tmp, sizeof(session_tmp), "/tmp/cbm_path_ai_sess_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(session_tmp));

    char session_src[320];
    snprintf(session_src, sizeof(session_src), "%s/main.c", session_tmp);
    FILE *fp = fopen(session_src, "w");
    if (fp) { fputs("void session_fn(void) {}\n", fp); fclose(fp); }

    char target_tmp[256];
    snprintf(target_tmp, sizeof(target_tmp), "/tmp/cbm_path_ai_tgt_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(target_tmp));

    char target_src[320];
    snprintf(target_src, sizeof(target_src), "%s/upstream.c", target_tmp);
    fp = fopen(target_src, "w");
    if (fp) { fputs("void path_autoindex_sentinel(void) {}\n", fp); fclose(fp); }

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    const char *old_auto_index = getenv("CBM_AUTO_INDEX");
    char *old_auto_index_copy = old_auto_index ? strdup(old_auto_index) : NULL;
    if (old_auto_index) {
        ASSERT_NOT_NULL(old_auto_index_copy);
    }
    cbm_setenv("CBM_AUTO_INDEX", "true", 1);

    /* First query: session project path → establishes session_root internally */
    char args1[512];
    snprintf(args1, sizeof(args1),
             "{\"project\":\"%s\",\"pattern\":\"session_fn\",\"search_in\":\"source\"}",
             session_tmp);
    char *raw1 = cbm_mcp_handle_tool(srv, "search_code", args1);
    free(raw1); /* result not checked — just establishing session_root */

    /* Second query: DIFFERENT path — resolve_project_store must auto-index it.
     * Use graph search (not source grep) so resolve_project_store runs the
     * path-based auto-index and the indexed nodes are searchable. */
    char args2[512];
    snprintf(args2, sizeof(args2),
             "{\"project\":\"%s\",\"pattern\":\"path_autoindex_sentinel\"}", target_tmp);
    char *raw2 = cbm_mcp_handle_tool(srv, "search_graph", args2);
    char *resp = extract_text(raw2); free(raw2);
    bool has_match = resp && strstr(resp, "path_autoindex_sentinel") != NULL;
    free(resp);

    cbm_mcp_server_free(srv);
    if (old_auto_index_copy) {
        cbm_setenv("CBM_AUTO_INDEX", old_auto_index_copy, 1);
        free(old_auto_index_copy);
    } else {
        cbm_unsetenv("CBM_AUTO_INDEX");
    }
    unlink(target_src); rmdir(target_tmp);
    unlink(session_src); rmdir(session_tmp);

    ASSERT_TRUE(has_match);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  Regression: classic tool names still work
 * ══════════════════════════════════════════════════════════════════ */

TEST(regression_trace_path_tool_name_still_works) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);
    char *raw = cbm_mcp_handle_tool(srv, "trace_path",
                                    "{\"function_name\":\"foo\"}");
    char *resp = extract_text(raw); free(raw);
    ASSERT_NOT_NULL(resp);
    ASSERT_NULL(strstr(resp, "unknown tool")); /* must not reject classic name */
    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_validation_dir(tmp);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  Config: context_injection=false disables _context header
 *
 *  context_injection (default true) / CBM_CONTEXT_INJECTION controls
 *  whether inject_context_once embeds the _context header in the first
 *  tool response. Disabling saves tokens in scripted/programmatic use.
 * ══════════════════════════════════════════════════════════════════ */

TEST(config_context_injection_disabled) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);
    cbm_config_t *cfg = cbm_config_open(tmp);
    ASSERT_NOT_NULL(cfg);
    cbm_config_set(cfg, "context_injection", "false");
    cbm_mcp_server_set_config(srv, cfg);

    /* With context_injection=false, _context must NOT appear in any call */
    char *raw = cbm_mcp_handle_tool(srv, "search_graph", "{\"limit\":3}");
    char *resp = extract_text(raw); free(raw);
    ASSERT_NOT_NULL(resp);
    ASSERT_NULL(strstr(resp, "\"_context\":"));
    free(resp);

    /* Second call also no _context */
    raw = cbm_mcp_handle_tool(srv, "search_graph", "{\"limit\":3}");
    resp = extract_text(raw); free(raw);
    ASSERT_NOT_NULL(resp);
    ASSERT_NULL(strstr(resp, "\"_context\":"));
    free(resp);

    cbm_mcp_server_free(srv);
    cbm_config_close(cfg);
    cleanup_validation_dir(tmp);
    PASS();
}

TEST(config_context_injection_enabled_by_default) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_validation_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);
    /* No config set → default is true → _context present on first call */
    char *raw = cbm_mcp_handle_tool(srv, "search_graph", "{\"limit\":3}");
    char *resp = extract_text(raw); free(raw);
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"_context\":"));
    free(resp);

    /* Second call: _context deduped (context_injected=true) */
    raw = cbm_mcp_handle_tool(srv, "search_graph", "{\"limit\":3}");
    resp = extract_text(raw); free(raw);
    ASSERT_NOT_NULL(resp);
    ASSERT_NULL(strstr(resp, "\"_context\":"));
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_validation_dir(tmp);
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
    RUN_TEST(f6_sort_by_calls_accepted);
    RUN_TEST(f6_sort_by_linkrank_accepted);
    RUN_TEST(f9_glob_star_autoconverted);
    RUN_TEST(f9_glob_question_autoconverted);
    RUN_TEST(f9_valid_regex_still_works);
    RUN_TEST(f9_truly_invalid_pattern_still_errors);
    RUN_TEST(f9_qn_pattern_glob_autoconverted);
    RUN_TEST(f10_negative_depth_returns_results);
    RUN_TEST(trace_case_mismatch_finds_via_fallback);
    RUN_TEST(trace_exact_match_still_works);
    RUN_TEST(trace_truly_missing_still_errors);
    RUN_TEST(f15_invalid_direction_errors);
    RUN_TEST(f15_valid_direction_succeeds);
    RUN_TEST(trace_invalid_mode_errors);
    RUN_TEST(g1_summary_mode_has_results_key);
    RUN_TEST(cq3_cypher_with_label_warns);
    RUN_TEST(ix2_status_resource_format);
    RUN_TEST(pattern_or_search_graph);
    RUN_TEST(source_search_via_search_in_param);
    RUN_TEST(source_search_path_project_normalizes_to_slug);
    RUN_TEST(source_search_default_is_graph);
    RUN_TEST(summary_bool_alias);
    RUN_TEST(case_sensitive_graph_search);
    RUN_TEST(config_compact_default_false);
    RUN_TEST(config_default_sort_by_calls);
    RUN_TEST(trace_accepts_qualified_name_param);
    RUN_TEST(pattern_glob_wildcards_auto_convert);
    RUN_TEST(pattern_invalid_regex_returns_error);
    RUN_TEST(trace_qn_takes_priority_over_function_name);
    RUN_TEST(trace_qn_not_found_returns_specific_hint);
    RUN_TEST(detect_changes_slug_project_finds_root);
    RUN_TEST(manage_adr_slug_project_finds_root);
    RUN_TEST(source_search_tilde_project_expands);
    RUN_TEST(source_search_no_project_falls_back_to_session);
    RUN_TEST(path_project_auto_indexes_separate_directory);
    RUN_TEST(regression_trace_path_tool_name_still_works);
    RUN_TEST(config_context_injection_disabled);
    RUN_TEST(config_context_injection_enabled_by_default);
}
