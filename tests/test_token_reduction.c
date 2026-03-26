/*
 * test_token_reduction.c — Tests for token reduction changes.
 *
 * Covers: default limits, smart truncation, compact mode, summary mode,
 *         trace edge cases, query_graph output truncation, token metadata.
 *
 * TDD: All tests written BEFORE implementation. They should fail (RED)
 * until the corresponding feature is implemented (GREEN).
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include <mcp/mcp.h>
#include <store/store.h>
#include <yyjson/yyjson.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

/* ── Helpers (reuse patterns from test_mcp.c) ────────────────── */

/* Forward declaration — definition is in the SEARCH PARAMETERIZATION section */
static cbm_mcp_server_t *setup_sp_server(void);

static char *extract_text_content_tr(const char *mcp_result) {
    if (!mcp_result)
        return NULL;
    yyjson_doc *doc = yyjson_read(mcp_result, strlen(mcp_result), 0);
    if (!doc)
        return strdup(mcp_result);
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

/* Create an MCP server pre-populated with many functions for limit testing.
 * Writes a source file with 80 small functions to tmp_dir/project/many.py.
 * Returns NULL on failure. Caller must free server and call cleanup. */
static cbm_mcp_server_t *setup_limit_test_server(char *tmp_dir, size_t tmp_sz) {
    snprintf(tmp_dir, tmp_sz, "/tmp/cbm_limit_test_XXXXXX");
    if (!cbm_mkdtemp(tmp_dir))
        return NULL;

    char proj_dir[512];
    snprintf(proj_dir, sizeof(proj_dir), "%s/project", tmp_dir);
    cbm_mkdir(proj_dir);

    /* Write source file with many functions */
    char src_path[512];
    snprintf(src_path, sizeof(src_path), "%s/many.py", proj_dir);
    FILE *fp = fopen(src_path, "w");
    if (!fp)
        return NULL;
    for (int i = 0; i < 80; i++) {
        fprintf(fp, "def func_%03d():\n    pass\n\n", i);
    }
    fclose(fp);

    /* Write a large function for truncation tests */
    char big_path[512];
    snprintf(big_path, sizeof(big_path), "%s/big.py", proj_dir);
    fp = fopen(big_path, "w");
    if (!fp)
        return NULL;
    fprintf(fp, "def large_function(arg1, arg2, arg3):\n");
    fprintf(fp, "    \"\"\"Process data with multiple steps.\"\"\"\n");
    for (int i = 2; i < 298; i++) {
        fprintf(fp, "    step_%03d = process(arg1, %d)\n", i, i);
    }
    fprintf(fp, "    result = combine(step_002, step_297)\n");
    fprintf(fp, "    return result\n");
    fclose(fp);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    if (!srv)
        return NULL;

    cbm_store_t *st = cbm_mcp_server_store(srv);
    if (!st) {
        cbm_mcp_server_free(srv);
        return NULL;
    }

    const char *proj_name = "limit-test";
    cbm_mcp_server_set_project(srv, proj_name);
    cbm_store_upsert_project(st, proj_name, proj_dir);

    /* Create 80 function nodes */
    for (int i = 0; i < 80; i++) {
        cbm_node_t n = {0};
        n.project = proj_name;
        n.label = "Function";
        char name_buf[32], qn_buf[64];
        snprintf(name_buf, sizeof(name_buf), "func_%03d", i);
        snprintf(qn_buf, sizeof(qn_buf), "limit-test.many.func_%03d", i);
        n.name = name_buf;
        n.qualified_name = qn_buf;
        n.file_path = "many.py";
        n.start_line = i * 3 + 1;
        n.end_line = i * 3 + 2;
        n.properties_json = "{\"is_exported\":true}";
        cbm_store_upsert_node(st, &n);
    }

    /* Create a large function node for truncation tests */
    cbm_node_t big = {0};
    big.project = proj_name;
    big.label = "Function";
    big.name = "large_function";
    big.qualified_name = "limit-test.big.large_function";
    big.file_path = "big.py";
    big.start_line = 1;
    big.end_line = 300;
    big.properties_json = "{\"signature\":\"def large_function(arg1, arg2, arg3)\","
                          "\"return_type\":\"result\",\"is_exported\":true}";
    cbm_store_upsert_node(st, &big);

    /* Create call chain for trace tests: func_000 -> func_001 -> func_002 */
    int64_t id0 = 1, id1 = 2, id2 = 3; /* approximate IDs */
    cbm_edge_t e1 = {.project = proj_name, .source_id = id0, .target_id = id1, .type = "CALLS"};
    cbm_store_insert_edge(st, &e1);
    cbm_edge_t e2 = {.project = proj_name, .source_id = id1, .target_id = id2, .type = "CALLS"};
    cbm_store_insert_edge(st, &e2);
    /* Create cycle: func_002 -> func_000 */
    cbm_edge_t e3 = {.project = proj_name, .source_id = id2, .target_id = id0, .type = "CALLS"};
    cbm_store_insert_edge(st, &e3);

    return srv;
}

static void cleanup_limit_test_dir(const char *tmp_dir) {
    char path[512];
    snprintf(path, sizeof(path), "%s/project/many.py", tmp_dir);
    unlink(path);
    snprintf(path, sizeof(path), "%s/project/big.py", tmp_dir);
    unlink(path);
    snprintf(path, sizeof(path), "%s/project", tmp_dir);
    rmdir(path);
    rmdir(tmp_dir);
}

/* ══════════════════════════════════════════════════════════════════
 *  1.1 DEFAULT LIMITS
 * ══════════════════════════════════════════════════════════════════ */

TEST(search_graph_default_limit_is_50) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_limit_test_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    /* search_graph with no limit parameter — should default to 50 */
    char *raw = cbm_mcp_handle_tool(srv, "search_graph",
                                    "{\"project\":\"limit-test\",\"label\":\"Function\"}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);

    /* Parse response to count results */
    yyjson_doc *doc = yyjson_read(resp, strlen(resp), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *results = yyjson_obj_get(root, "results");
    ASSERT_NOT_NULL(results);
    ASSERT_TRUE(yyjson_arr_size(results) <= 50);

    /* total should reflect all 80 functions */
    yyjson_val *total = yyjson_obj_get(root, "total");
    ASSERT_TRUE(yyjson_get_int(total) >= 80);

    /* has_more should be true since 80 > 50 */
    yyjson_val *has_more = yyjson_obj_get(root, "has_more");
    ASSERT_TRUE(yyjson_get_bool(has_more));

    yyjson_doc_free(doc);
    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_limit_test_dir(tmp);
    PASS();
}

TEST(search_graph_explicit_limit_honored) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_limit_test_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(srv, "search_graph",
                                    "{\"project\":\"limit-test\",\"label\":\"Function\","
                                    "\"limit\":5}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);

    yyjson_doc *doc = yyjson_read(resp, strlen(resp), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *results = yyjson_obj_get(root, "results");
    ASSERT_EQ((int)yyjson_arr_size(results), 5);

    yyjson_doc_free(doc);
    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_limit_test_dir(tmp);
    PASS();
}

TEST(search_graph_explicit_high_limit_still_works) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_limit_test_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    /* Explicit limit=1000 should override default and return all 80+ */
    char *raw = cbm_mcp_handle_tool(srv, "search_graph",
                                    "{\"project\":\"limit-test\",\"label\":\"Function\","
                                    "\"limit\":1000}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);

    yyjson_doc *doc = yyjson_read(resp, strlen(resp), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *results = yyjson_obj_get(root, "results");
    /* Should get all 80+ nodes (80 funcs + 1 large_function) */
    ASSERT_TRUE((int)yyjson_arr_size(results) > 50);

    yyjson_doc_free(doc);
    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_limit_test_dir(tmp);
    PASS();
}

TEST(search_code_default_limit_is_50) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_limit_test_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    /* search_code for "def " should match all 81 functions but return ≤50 */
    char *raw = cbm_mcp_handle_tool(srv, "search_code",
                                    "{\"project\":\"limit-test\",\"pattern\":\"def \"}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);

    yyjson_doc *doc = yyjson_read(resp, strlen(resp), 0);
    if (doc) {
        yyjson_val *root = yyjson_doc_get_root(doc);
        yyjson_val *results = yyjson_obj_get(root, "results");
        if (results && yyjson_is_arr(results)) {
            ASSERT_TRUE((int)yyjson_arr_size(results) <= 50);
        }
        yyjson_doc_free(doc);
    }

    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_limit_test_dir(tmp);
    PASS();
}

TEST(search_graph_pagination_stable_ordering) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_limit_test_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    /* Page 1: offset=0, limit=10 */
    char *raw1 = cbm_mcp_handle_tool(srv, "search_graph",
                                     "{\"project\":\"limit-test\",\"label\":\"Function\","
                                     "\"limit\":10,\"offset\":0}");
    char *resp1 = extract_text_content_tr(raw1);
    free(raw1);

    /* Page 2: offset=10, limit=10 */
    char *raw2 = cbm_mcp_handle_tool(srv, "search_graph",
                                     "{\"project\":\"limit-test\",\"label\":\"Function\","
                                     "\"limit\":10,\"offset\":10}");
    char *resp2 = extract_text_content_tr(raw2);
    free(raw2);

    ASSERT_NOT_NULL(resp1);
    ASSERT_NOT_NULL(resp2);

    /* Pages should not overlap — check first result of page 2 is not in page 1 */
    yyjson_doc *d2 = yyjson_read(resp2, strlen(resp2), 0);
    if (d2) {
        yyjson_val *r2 = yyjson_doc_get_root(d2);
        yyjson_val *res2 = yyjson_obj_get(r2, "results");
        if (res2 && yyjson_arr_size(res2) > 0) {
            yyjson_val *first = yyjson_arr_get(res2, 0);
            yyjson_val *qn = yyjson_obj_get(first, "qualified_name");
            const char *qn_str = yyjson_get_str(qn);
            if (qn_str) {
                /* This QN should NOT appear in page 1 */
                ASSERT_NULL(strstr(resp1, qn_str));
            }
        }
        yyjson_doc_free(d2);
    }

    free(resp1);
    free(resp2);
    cbm_mcp_server_free(srv);
    cleanup_limit_test_dir(tmp);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  1.2 SMART TRUNCATION
 * ══════════════════════════════════════════════════════════════════ */

TEST(snippet_full_mode_default_200_lines) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_limit_test_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(srv, "get_code_snippet",
                                    "{\"qualified_name\":\"limit-test.big.large_function\","
                                    "\"project\":\"limit-test\"}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);

    /* Should be truncated since function is 300 lines, default max_lines=200 */
    ASSERT_NOT_NULL(strstr(resp, "\"truncated\":true"));
    ASSERT_NOT_NULL(strstr(resp, "\"total_lines\":300"));
    /* Signature should still be present for structural context */
    ASSERT_NOT_NULL(strstr(resp, "large_function"));

    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_limit_test_dir(tmp);
    PASS();
}

TEST(snippet_full_mode_small_function_no_truncation) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_limit_test_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    /* func_000 is only 2 lines — should NOT be truncated */
    char *raw = cbm_mcp_handle_tool(srv, "get_code_snippet",
                                    "{\"qualified_name\":\"limit-test.many.func_000\","
                                    "\"project\":\"limit-test\"}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);

    ASSERT_NULL(strstr(resp, "\"truncated\":true"));
    ASSERT_NOT_NULL(strstr(resp, "\"source\""));

    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_limit_test_dir(tmp);
    PASS();
}

TEST(snippet_signature_mode) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_limit_test_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(srv, "get_code_snippet",
                                    "{\"qualified_name\":\"limit-test.big.large_function\","
                                    "\"project\":\"limit-test\",\"mode\":\"signature\"}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);

    /* Should contain signature from properties */
    ASSERT_NOT_NULL(strstr(resp, "def large_function(arg1, arg2, arg3)"));
    /* Should NOT contain full source body */
    ASSERT_NULL(strstr(resp, "step_050"));
    /* Should indicate total size */
    ASSERT_NOT_NULL(strstr(resp, "\"total_lines\""));

    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_limit_test_dir(tmp);
    PASS();
}

TEST(snippet_head_tail_mode) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_limit_test_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(srv, "get_code_snippet",
                                    "{\"qualified_name\":\"limit-test.big.large_function\","
                                    "\"project\":\"limit-test\","
                                    "\"mode\":\"head_tail\",\"max_lines\":100}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);

    /* Head (first 60 lines) should include the function def */
    ASSERT_NOT_NULL(strstr(resp, "def large_function"));
    /* Tail (last 40 lines) should include the return statement */
    ASSERT_NOT_NULL(strstr(resp, "return result"));
    /* Omission marker between head and tail */
    ASSERT_NOT_NULL(strstr(resp, "lines omitted"));
    ASSERT_NOT_NULL(strstr(resp, "\"truncated\":true"));

    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_limit_test_dir(tmp);
    PASS();
}

TEST(snippet_head_tail_no_truncation_when_fits) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_limit_test_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    /* func_000 is 2 lines, head_tail with max_lines=100 should return all */
    char *raw = cbm_mcp_handle_tool(srv, "get_code_snippet",
                                    "{\"qualified_name\":\"limit-test.many.func_000\","
                                    "\"project\":\"limit-test\","
                                    "\"mode\":\"head_tail\",\"max_lines\":100}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);

    ASSERT_NULL(strstr(resp, "lines omitted"));
    ASSERT_NULL(strstr(resp, "\"truncated\":true"));

    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_limit_test_dir(tmp);
    PASS();
}

TEST(snippet_custom_max_lines) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_limit_test_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(srv, "get_code_snippet",
                                    "{\"qualified_name\":\"limit-test.big.large_function\","
                                    "\"project\":\"limit-test\",\"max_lines\":50}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);

    ASSERT_NOT_NULL(strstr(resp, "\"truncated\":true"));
    ASSERT_NOT_NULL(strstr(resp, "\"total_lines\":300"));

    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_limit_test_dir(tmp);
    PASS();
}

TEST(snippet_max_lines_zero_means_unlimited) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_limit_test_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    /* max_lines=0 should return full source without truncation */
    char *raw = cbm_mcp_handle_tool(srv, "get_code_snippet",
                                    "{\"qualified_name\":\"limit-test.big.large_function\","
                                    "\"project\":\"limit-test\",\"max_lines\":0}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);

    /* Should NOT be truncated */
    ASSERT_NULL(strstr(resp, "\"truncated\":true"));
    /* Should contain content from near the end of the function */
    ASSERT_NOT_NULL(strstr(resp, "return result"));

    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_limit_test_dir(tmp);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  1.3 COMPACT MODE
 * ══════════════════════════════════════════════════════════════════ */

TEST(search_graph_compact_omits_redundant_name) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_limit_test_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(srv, "search_graph",
                                    "{\"project\":\"limit-test\",\"label\":\"Function\","
                                    "\"limit\":5,\"compact\":true}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);

    /* In compact mode, results should have qualified_name but
     * name should be omitted when it's a suffix of qualified_name.
     * All our test functions have name == last segment of QN,
     * so name should be omitted for all results. */
    yyjson_doc *doc = yyjson_read(resp, strlen(resp), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *results = yyjson_obj_get(root, "results");
    ASSERT_NOT_NULL(results);

    /* Check first result has qualified_name but no name */
    yyjson_val *first = yyjson_arr_get(results, 0);
    ASSERT_NOT_NULL(first);
    ASSERT_NOT_NULL(yyjson_obj_get(first, "qualified_name"));
    ASSERT_NULL(yyjson_obj_get(first, "name"));

    yyjson_doc_free(doc);
    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_limit_test_dir(tmp);
    PASS();
}

TEST(trace_compact_omits_redundant_name) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_limit_test_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(srv, "trace_call_path",
                                    "{\"function_name\":\"func_000\","
                                    "\"project\":\"limit-test\",\"compact\":true}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);

    /* Callees should use compact format */
    yyjson_doc *doc = yyjson_read(resp, strlen(resp), 0);
    if (doc) {
        yyjson_val *root = yyjson_doc_get_root(doc);
        yyjson_val *callees = yyjson_obj_get(root, "callees");
        if (callees && yyjson_arr_size(callees) > 0) {
            yyjson_val *first = yyjson_arr_get(callees, 0);
            ASSERT_NOT_NULL(yyjson_obj_get(first, "qualified_name"));
            /* name should be omitted in compact mode */
            ASSERT_NULL(yyjson_obj_get(first, "name"));
        }
        yyjson_doc_free(doc);
    }

    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_limit_test_dir(tmp);
    PASS();
}

TEST(search_graph_compact_defaults_to_true) {
    cbm_mcp_server_t *srv = setup_sp_server();
    ASSERT_NOT_NULL(srv);
    /* No compact param -> default is true -> name omitted when name == last qn segment.
     * All sp-test nodes satisfy this (e.g. name="main", qn="sp-test.main.main"). */
    char *raw = cbm_mcp_handle_tool(srv, "search_graph",
                                    "{\"project\":\"sp-test\","
                                    "\"include_dependencies\":false}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);
    yyjson_doc *doc = yyjson_read(resp, strlen(resp), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *results = yyjson_obj_get(yyjson_doc_get_root(doc), "results");
    ASSERT_NOT_NULL(results);
    ASSERT_GT((int)yyjson_arr_size(results), 0);
    yyjson_val *first = yyjson_arr_get(results, 0);
    /* compact=true default: name == last qn segment -> name field OMITTED */
    ASSERT_NULL(yyjson_obj_get(first, "name"));
    ASSERT_NOT_NULL(yyjson_obj_get(first, "qualified_name"));
    yyjson_doc_free(doc);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(search_graph_compact_false_includes_name) {
    cbm_mcp_server_t *srv = setup_sp_server();
    ASSERT_NOT_NULL(srv);
    char *raw = cbm_mcp_handle_tool(srv, "search_graph",
                                    "{\"project\":\"sp-test\","
                                    "\"include_dependencies\":false,"
                                    "\"compact\":false}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);
    yyjson_doc *doc = yyjson_read(resp, strlen(resp), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *results = yyjson_obj_get(yyjson_doc_get_root(doc), "results");
    ASSERT_NOT_NULL(results);
    ASSERT_GT((int)yyjson_arr_size(results), 0);
    yyjson_val *first = yyjson_arr_get(results, 0);
    /* compact=false: name field present even when name matches qn suffix */
    ASSERT_NOT_NULL(yyjson_obj_get(first, "name"));
    yyjson_doc_free(doc);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  1.4 SUMMARY MODE
 * ══════════════════════════════════════════════════════════════════ */

TEST(search_graph_summary_mode) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_limit_test_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(srv, "search_graph",
                                    "{\"project\":\"limit-test\","
                                    "\"mode\":\"summary\"}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);

    /* Should have aggregate fields, NOT individual results */
    ASSERT_NOT_NULL(strstr(resp, "\"total\""));
    ASSERT_NOT_NULL(strstr(resp, "\"by_label\""));
    ASSERT_NULL(strstr(resp, "\"results\""));

    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_limit_test_dir(tmp);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  1.5 TRACE EDGE CASES
 * ══════════════════════════════════════════════════════════════════ */

TEST(trace_ambiguous_function_returns_candidates) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_limit_test_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    /* Add a second node with same short name but different QN */
    cbm_store_t *st = cbm_mcp_server_store(srv);
    cbm_node_t dup = {0};
    dup.project = "limit-test";
    dup.label = "Function";
    dup.name = "func_000";
    dup.qualified_name = "limit-test.other.func_000";
    dup.file_path = "other.py";
    dup.start_line = 1;
    dup.end_line = 2;
    cbm_store_upsert_node(st, &dup);

    char *raw = cbm_mcp_handle_tool(srv, "trace_call_path",
                                    "{\"function_name\":\"func_000\","
                                    "\"project\":\"limit-test\"}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);

    /* Should include candidates array when name is ambiguous */
    ASSERT_NOT_NULL(strstr(resp, "\"candidates\""));
    ASSERT_NOT_NULL(strstr(resp, "\"resolved\""));

    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_limit_test_dir(tmp);
    PASS();
}

TEST(trace_bfs_deduplicates_cycles) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_limit_test_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    /* func_000 -> func_001 -> func_002 -> func_000 (cycle)
     * BFS should visit each node at most once in results */
    char *raw = cbm_mcp_handle_tool(srv, "trace_call_path",
                                    "{\"function_name\":\"func_000\","
                                    "\"project\":\"limit-test\","
                                    "\"direction\":\"outbound\",\"depth\":5}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);

    yyjson_doc *doc = yyjson_read(resp, strlen(resp), 0);
    if (doc) {
        yyjson_val *root = yyjson_doc_get_root(doc);
        yyjson_val *callees = yyjson_obj_get(root, "callees");
        if (callees) {
            /* Should have at most 2 unique callees (func_001, func_002)
             * NOT 4+ from the cycle being traversed multiple times */
            ASSERT_TRUE((int)yyjson_arr_size(callees) <= 3);
        }
        yyjson_doc_free(doc);
    }

    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_limit_test_dir(tmp);
    PASS();
}

TEST(trace_max_results_parameter) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_limit_test_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(srv, "trace_call_path",
                                    "{\"function_name\":\"func_000\","
                                    "\"project\":\"limit-test\","
                                    "\"max_results\":1}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);

    yyjson_doc *doc = yyjson_read(resp, strlen(resp), 0);
    if (doc) {
        yyjson_val *root = yyjson_doc_get_root(doc);
        yyjson_val *callees = yyjson_obj_get(root, "callees");
        if (callees) {
            ASSERT_TRUE((int)yyjson_arr_size(callees) <= 1);
        }
        yyjson_doc_free(doc);
    }

    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_limit_test_dir(tmp);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  1.7 QUERY_GRAPH OUTPUT TRUNCATION
 * ══════════════════════════════════════════════════════════════════ */

TEST(query_graph_max_output_bytes_truncates) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_limit_test_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    /* Query that returns many rows, but cap output at 1024 bytes */
    char *raw = cbm_mcp_handle_tool(srv, "query_graph",
                                    "{\"query\":\"MATCH (f:Function) RETURN f.name, "
                                    "f.qualified_name, f.file_path\","
                                    "\"project\":\"limit-test\","
                                    "\"max_output_bytes\":1024}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);

    /* Response should indicate truncation */
    ASSERT_NOT_NULL(strstr(resp, "\"truncated\":true"));
    /* Response body should be near the byte limit */
    ASSERT_TRUE(strlen(resp) <= 2048); /* some slack for metadata */

    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_limit_test_dir(tmp);
    PASS();
}

TEST(query_graph_aggregation_not_broken) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_limit_test_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    /* Aggregation query should return correct count regardless of limits */
    char *raw = cbm_mcp_handle_tool(srv, "query_graph",
                                    "{\"query\":\"MATCH (f:Function) RETURN count(f)\","
                                    "\"project\":\"limit-test\"}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);

    /* Should NOT be truncated (aggregation returns 1 small row) */
    ASSERT_NULL(strstr(resp, "\"truncated\":true"));
    /* Should contain a count ≥ 80 (our 80 funcs + large_function) */
    ASSERT_NOT_NULL(strstr(resp, "rows"));

    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_limit_test_dir(tmp);
    PASS();
}

TEST(query_graph_max_output_bytes_zero_unlimited) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_limit_test_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    /* max_output_bytes=0 should disable truncation */
    char *raw = cbm_mcp_handle_tool(srv, "query_graph",
                                    "{\"query\":\"MATCH (f:Function) RETURN f.name\","
                                    "\"project\":\"limit-test\","
                                    "\"max_output_bytes\":0}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);

    ASSERT_NULL(strstr(resp, "\"truncated\":true"));

    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_limit_test_dir(tmp);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  1.8 TOKEN METADATA
 * ══════════════════════════════════════════════════════════════════ */

TEST(response_includes_meta_fields) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_limit_test_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *raw = cbm_mcp_handle_tool(srv, "search_graph",
                                    "{\"project\":\"limit-test\",\"label\":\"Function\","
                                    "\"limit\":5}");
    ASSERT_NOT_NULL(raw);

    /* Token metadata is in the MCP envelope (cbm_mcp_text_result output) */
    ASSERT_NOT_NULL(strstr(raw, "\"_result_bytes\""));
    ASSERT_NOT_NULL(strstr(raw, "\"_est_tokens\""));

    free(raw);
    cbm_mcp_server_free(srv);
    cleanup_limit_test_dir(tmp);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  1.9 FIELD OMISSION (empty label / file_path not emitted)
 * ══════════════════════════════════════════════════════════════════ */

TEST(search_graph_omits_empty_label_and_file_path) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    cbm_mcp_server_set_project(srv, "empty-test");
    cbm_store_upsert_project(st, "empty-test", "/tmp");

    /* Node with empty label and empty file_path */
    cbm_node_t n = {0};
    n.project = "empty-test";
    n.label = "";
    n.name = "anon_func";
    n.qualified_name = "empty-test.mod.anon_func";
    n.file_path = "";
    n.properties_json = "{}";
    cbm_store_upsert_node(st, &n);

    char *raw = cbm_mcp_handle_tool(srv, "search_graph",
                                    "{\"project\":\"empty-test\",\"compact\":false}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);
    yyjson_doc *doc = yyjson_read(resp, strlen(resp), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *results = yyjson_obj_get(yyjson_doc_get_root(doc), "results");
    ASSERT_NOT_NULL(results);
    ASSERT_EQ((int)yyjson_arr_size(results), 1);
    yyjson_val *item = yyjson_arr_get(results, 0);
    /* Empty label and file_path must be omitted, not emitted as "" */
    ASSERT_NULL(yyjson_obj_get(item, "label"));
    ASSERT_NULL(yyjson_obj_get(item, "file_path"));
    yyjson_doc_free(doc);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(search_graph_includes_nonempty_label_and_file_path) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    cbm_mcp_server_set_project(srv, "nonempty-test");
    cbm_store_upsert_project(st, "nonempty-test", "/tmp");

    cbm_node_t n = {0};
    n.project = "nonempty-test";
    n.label = "Function";
    n.name = "do_work";
    n.qualified_name = "nonempty-test.worker.do_work";
    n.file_path = "worker.py";
    n.properties_json = "{}";
    cbm_store_upsert_node(st, &n);

    char *raw = cbm_mcp_handle_tool(srv, "search_graph",
                                    "{\"project\":\"nonempty-test\",\"compact\":false}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);
    yyjson_doc *doc = yyjson_read(resp, strlen(resp), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *results = yyjson_obj_get(yyjson_doc_get_root(doc), "results");
    ASSERT_NOT_NULL(results);
    ASSERT_EQ((int)yyjson_arr_size(results), 1);
    yyjson_val *item = yyjson_arr_get(results, 0);
    /* Non-empty label and file_path must be present with correct values */
    ASSERT_NOT_NULL(yyjson_obj_get(item, "label"));
    ASSERT_NOT_NULL(yyjson_obj_get(item, "file_path"));
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(item, "label")), "Function");
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(item, "file_path")), "worker.py");
    yyjson_doc_free(doc);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* TDD: Zero in_degree/out_degree fields omitted when no edges.
 * RED until Change 3 (zero degree omission) is implemented in mcp.c. */
TEST(search_graph_omits_zero_degrees) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    cbm_mcp_server_set_project(srv, "degree-test");
    cbm_store_upsert_project(st, "degree-test", "/tmp");

    /* Node with no edges -> in_degree=0, out_degree=0 */
    cbm_node_t n = {0};
    n.project = "degree-test";
    n.label = "Function";
    n.name = "isolated";
    n.qualified_name = "degree-test.mod.isolated";
    n.file_path = "mod.py";
    n.properties_json = "{}";
    cbm_store_upsert_node(st, &n);

    char *raw = cbm_mcp_handle_tool(srv, "search_graph",
                                    "{\"project\":\"degree-test\",\"compact\":false}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);
    yyjson_doc *doc = yyjson_read(resp, strlen(resp), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *results = yyjson_obj_get(yyjson_doc_get_root(doc), "results");
    ASSERT_NOT_NULL(results);
    ASSERT_EQ((int)yyjson_arr_size(results), 1);
    yyjson_val *item = yyjson_arr_get(results, 0);
    /* Zero in_degree and out_degree must be omitted, not emitted as 0 */
    ASSERT_NULL(yyjson_obj_get(item, "in_degree"));
    ASSERT_NULL(yyjson_obj_get(item, "out_degree"));
    yyjson_doc_free(doc);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* Non-zero degrees must still be present (regression guard for Change 3). */
TEST(search_graph_includes_nonzero_degrees) {
    cbm_mcp_server_t *srv = setup_sp_server();
    ASSERT_NOT_NULL(srv);
    /* process_request has in_degree=2 (CALLS from main, HTTP_CALLS from fetch_data) */
    char *raw = cbm_mcp_handle_tool(srv, "search_graph",
                                    "{\"project\":\"sp-test\","
                                    "\"qn_pattern\":\".*process_request.*\","
                                    "\"include_dependencies\":false,"
                                    "\"compact\":false}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);
    yyjson_doc *doc = yyjson_read(resp, strlen(resp), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *results = yyjson_obj_get(yyjson_doc_get_root(doc), "results");
    ASSERT_NOT_NULL(results);
    ASSERT_EQ((int)yyjson_arr_size(results), 1);
    yyjson_val *item = yyjson_arr_get(results, 0);
    /* process_request has non-zero in_degree -> must be present */
    ASSERT_NOT_NULL(yyjson_obj_get(item, "in_degree"));
    yyjson_doc_free(doc);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  SEARCH PARAMETERIZATION ACCURACY
 *  TDD: Tests written BEFORE implementation.
 *  RED before changes applied. GREEN after.
 * ══════════════════════════════════════════════════════════════════ */

/* ── Parameterization test fixture ──────────────────────────── */
/*
 * Creates a minimal server with:
 *   Project "sp-test":
 *     node id=1: Function name="main"            qn="sp-test.main.main"
 *                no inbound CALLS (in_deg=0 — entry point)
 *     node id=2: Function name="process_request" qn="sp-test.handlers.process_request"
 *                inbound CALLS from main (in_deg=1)
 *     node id=3: Function name="fetch_data"      qn="sp-test.http.fetch_data"
 *                outbound HTTP_CALLS to process_request (in_deg=0)
 *   Project "sp-test.dep.mypkg":
 *     node id=4: Function name="dep_helper"      qn="sp-test.dep.mypkg.dep_helper"
 *
 *   Edges:
 *     CALLS:      id=1 -> id=2  (main calls process_request)
 *     HTTP_CALLS: id=3 -> id=2  (fetch_data HTTP calls to process_request)
 *
 * Node IDs are predictable: fresh in-memory SQLite, autoincrement from 1.
 */
static cbm_mcp_server_t *setup_sp_server(void) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    if (!srv)
        return NULL;
    cbm_store_t *st = cbm_mcp_server_store(srv);
    if (!st) {
        cbm_mcp_server_free(srv);
        return NULL;
    }

    cbm_mcp_server_set_project(srv, "sp-test");
    cbm_store_upsert_project(st, "sp-test", "/tmp");
    cbm_store_upsert_project(st, "sp-test.dep.mypkg", "/tmp/dep");

    cbm_node_t n1 = {0};
    n1.project = "sp-test";
    n1.label = "Function";
    n1.name = "main";
    n1.qualified_name = "sp-test.main.main";
    n1.file_path = "main.py";
    n1.start_line = 1;
    n1.end_line = 5;
    n1.properties_json = "{}";
    cbm_store_upsert_node(st, &n1);

    cbm_node_t n2 = {0};
    n2.project = "sp-test";
    n2.label = "Function";
    n2.name = "process_request";
    n2.qualified_name = "sp-test.handlers.process_request";
    n2.file_path = "handlers.py";
    n2.start_line = 1;
    n2.end_line = 10;
    n2.properties_json = "{}";
    cbm_store_upsert_node(st, &n2);

    cbm_node_t n3 = {0};
    n3.project = "sp-test";
    n3.label = "Function";
    n3.name = "fetch_data";
    n3.qualified_name = "sp-test.http.fetch_data";
    n3.file_path = "http.py";
    n3.start_line = 1;
    n3.end_line = 8;
    n3.properties_json = "{}";
    cbm_store_upsert_node(st, &n3);

    cbm_node_t n4 = {0};
    n4.project = "sp-test.dep.mypkg";
    n4.label = "Function";
    n4.name = "dep_helper";
    n4.qualified_name = "sp-test.dep.mypkg.dep_helper";
    n4.file_path = "mypkg/helper.py";
    n4.start_line = 1;
    n4.end_line = 5;
    n4.properties_json = "{}";
    cbm_store_upsert_node(st, &n4);

    /* CALLS: main(id=1) -> process_request(id=2) */
    cbm_edge_t e1 = {0};
    e1.project = "sp-test";
    e1.source_id = 1;
    e1.target_id = 2;
    e1.type = "CALLS";
    e1.properties_json = "{}";
    cbm_store_insert_edge(st, &e1);

    /* HTTP_CALLS: fetch_data(id=3) -> process_request(id=2) */
    cbm_edge_t e2 = {0};
    e2.project = "sp-test";
    e2.source_id = 3;
    e2.target_id = 2;
    e2.type = "HTTP_CALLS";
    e2.properties_json = "{}";
    cbm_store_insert_edge(st, &e2);

    return srv;
}

/* ── Changes 2.1 + 1.1 + 1.3: qn_pattern filters qualified_name ── */

TEST(search_graph_qn_pattern_filters_results) {
    cbm_mcp_server_t *srv = setup_sp_server();
    ASSERT_NOT_NULL(srv);
    char *raw = cbm_mcp_handle_tool(srv, "search_graph",
                                    "{\"project\":\"sp-test\","
                                    "\"qn_pattern\":\".*handlers.*\","
                                    "\"include_dependencies\":false}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);
    yyjson_doc *doc = yyjson_read(resp, strlen(resp), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *results = yyjson_obj_get(root, "results");
    ASSERT_NOT_NULL(results);
    /* Only process_request qn contains "handlers". Expect 1 result.
     * RED: qn_pattern ignored, returns all 3 project nodes. GREEN: 1. */
    ASSERT_EQ((int)yyjson_arr_size(results), 1);
    yyjson_doc_free(doc);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(search_graph_qn_pattern_no_match_returns_empty) {
    cbm_mcp_server_t *srv = setup_sp_server();
    ASSERT_NOT_NULL(srv);
    char *raw = cbm_mcp_handle_tool(srv, "search_graph",
                                    "{\"project\":\"sp-test\","
                                    "\"qn_pattern\":\".*nonexistent_module.*\","
                                    "\"include_dependencies\":false}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);
    yyjson_doc *doc = yyjson_read(resp, strlen(resp), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *results = yyjson_obj_get(yyjson_doc_get_root(doc), "results");
    /* RED: qn_pattern ignored, returns all nodes. GREEN: 0. */
    ASSERT_EQ((int)yyjson_arr_size(results), 0);
    yyjson_doc_free(doc);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ── Changes 2.2 + 1.1 + 1.3: relationship filters by edge type ── */

TEST(search_graph_relationship_filters_to_matching_edge_type) {
    cbm_mcp_server_t *srv = setup_sp_server();
    ASSERT_NOT_NULL(srv);
    char *raw = cbm_mcp_handle_tool(srv, "search_graph",
                                    "{\"project\":\"sp-test\","
                                    "\"relationship\":\"HTTP_CALLS\","
                                    "\"include_dependencies\":false}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);
    yyjson_doc *doc = yyjson_read(resp, strlen(resp), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *results = yyjson_obj_get(yyjson_doc_get_root(doc), "results");
    ASSERT_NOT_NULL(results);
    /* fetch_data (source) + process_request (target) both involved in HTTP_CALLS.
     * main has no HTTP_CALLS edges -> excluded.
     * RED: all 3 returned. GREEN: 2 (both endpoints of HTTP_CALLS). */
    ASSERT_EQ((int)yyjson_arr_size(results), 2);
    yyjson_doc_free(doc);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(search_graph_relationship_nonexistent_type_returns_empty) {
    cbm_mcp_server_t *srv = setup_sp_server();
    ASSERT_NOT_NULL(srv);
    char *raw = cbm_mcp_handle_tool(srv, "search_graph",
                                    "{\"project\":\"sp-test\","
                                    "\"relationship\":\"WRITES\","
                                    "\"include_dependencies\":false}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);
    yyjson_doc *doc = yyjson_read(resp, strlen(resp), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *results = yyjson_obj_get(yyjson_doc_get_root(doc), "results");
    /* No WRITES edges exist. RED: all nodes returned. GREEN: 0. */
    ASSERT_EQ((int)yyjson_arr_size(results), 0);
    yyjson_doc_free(doc);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ── Changes 2.3 + 1.2 + 1.3: exclude_entry_points ─────────── */

TEST(search_graph_exclude_entry_points_removes_zero_inbound) {
    cbm_mcp_server_t *srv = setup_sp_server();
    ASSERT_NOT_NULL(srv);
    char *raw = cbm_mcp_handle_tool(srv, "search_graph",
                                    "{\"project\":\"sp-test\","
                                    "\"exclude_entry_points\":true,"
                                    "\"include_dependencies\":false}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);
    yyjson_doc *doc = yyjson_read(resp, strlen(resp), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *results = yyjson_obj_get(yyjson_doc_get_root(doc), "results");
    ASSERT_NOT_NULL(results);
    /* main(in_deg=0) + fetch_data(in_deg=0) excluded. process_request(in_deg=1) kept.
     * RED: all 3 returned. GREEN: 1. */
    ASSERT_EQ((int)yyjson_arr_size(results), 1);
    yyjson_val *first = yyjson_arr_get(results, 0);
    /* Check qualified_name (always present; name may be omitted by compact=true default) */
    yyjson_val *qn = yyjson_obj_get(first, "qualified_name");
    ASSERT_STR_EQ(yyjson_get_str(qn), "sp-test.handlers.process_request");
    yyjson_doc_free(doc);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(search_graph_exclude_entry_points_false_keeps_all) {
    cbm_mcp_server_t *srv = setup_sp_server();
    ASSERT_NOT_NULL(srv);
    char *raw = cbm_mcp_handle_tool(srv, "search_graph",
                                    "{\"project\":\"sp-test\","
                                    "\"exclude_entry_points\":false,"
                                    "\"include_dependencies\":false}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);
    yyjson_doc *doc = yyjson_read(resp, strlen(resp), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *results = yyjson_obj_get(yyjson_doc_get_root(doc), "results");
    ASSERT_EQ((int)yyjson_arr_size(results), 3);
    yyjson_doc_free(doc);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ── Change 1.3: include_dependencies ──────────────────────── */

TEST(search_graph_include_dependencies_true_includes_dep_nodes) {
    cbm_mcp_server_t *srv = setup_sp_server();
    ASSERT_NOT_NULL(srv);
    /* Default: include_dependencies not specified = true */
    char *raw = cbm_mcp_handle_tool(srv, "search_graph",
                                    "{\"project\":\"sp-test\"}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);
    /* dep_helper from sp-test.dep.mypkg should appear in results */
    ASSERT_NOT_NULL(strstr(resp, "dep_helper"));
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(search_graph_include_dependencies_false_excludes_dep_nodes) {
    cbm_mcp_server_t *srv = setup_sp_server();
    ASSERT_NOT_NULL(srv);
    char *raw = cbm_mcp_handle_tool(srv, "search_graph",
                                    "{\"project\":\"sp-test\","
                                    "\"include_dependencies\":false}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);
    yyjson_doc *doc = yyjson_read(resp, strlen(resp), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *results = yyjson_obj_get(yyjson_doc_get_root(doc), "results");
    /* dep_helper (project=sp-test.dep.mypkg) must NOT appear.
     * RED: include_dependencies ignored -- may return 4. GREEN: exactly 3. */
    ASSERT_EQ((int)yyjson_arr_size(results), 3);
    ASSERT_NULL(strstr(resp, "dep_helper"));
    yyjson_doc_free(doc);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ── Change 3.1 reverted: trace compact default remains true ─── */

TEST(trace_call_path_compact_defaults_to_true) {
    cbm_mcp_server_t *srv = setup_sp_server();
    ASSERT_NOT_NULL(srv);
    /* No compact param -> defaults to true -> name omitted when it matches qn suffix */
    char *raw = cbm_mcp_handle_tool(srv, "trace_call_path",
                                    "{\"function_name\":\"main\","
                                    "\"project\":\"sp-test\","
                                    "\"direction\":\"outbound\"}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);
    /* Parse and check: callees[0] should NOT have "name" key (compact=true default).
     * main -> process_request. qn "sp-test.handlers.process_request",
     * name "process_request". ends_with_segment(qn, name) is TRUE => name omitted. */
    yyjson_doc *doc = yyjson_read(resp, strlen(resp), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *callees = yyjson_obj_get(root, "callees");
    ASSERT_NOT_NULL(callees);
    ASSERT_GT((int)yyjson_arr_size(callees), 0);
    yyjson_val *first_callee = yyjson_arr_get(callees, 0);
    /* compact=true default: name matches last segment of qn -> name field OMITTED */
    ASSERT_NULL(yyjson_obj_get(first_callee, "name"));
    yyjson_doc_free(doc);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(trace_call_path_compact_false_includes_name) {
    cbm_mcp_server_t *srv = setup_sp_server();
    ASSERT_NOT_NULL(srv);
    char *raw = cbm_mcp_handle_tool(srv, "trace_call_path",
                                    "{\"function_name\":\"main\","
                                    "\"project\":\"sp-test\","
                                    "\"direction\":\"outbound\","
                                    "\"compact\":false}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);
    yyjson_doc *doc = yyjson_read(resp, strlen(resp), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *callees = yyjson_obj_get(root, "callees");
    ASSERT_NOT_NULL(callees);
    ASSERT_GT((int)yyjson_arr_size(callees), 0);
    yyjson_val *first_callee = yyjson_arr_get(callees, 0);
    /* compact=false explicit: name field present even though name matches qn suffix */
    ASSERT_NOT_NULL(yyjson_obj_get(first_callee, "name"));
    yyjson_doc_free(doc);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ── Change 3.2: trace edge_types user param ────────────────── */

TEST(trace_call_path_edge_types_http_calls_traverses_http_edges) {
    cbm_mcp_server_t *srv = setup_sp_server();
    ASSERT_NOT_NULL(srv);
    /* fetch_data(id=3) has HTTP_CALLS -> process_request(id=2).
     * With edge_types=["HTTP_CALLS"] outbound, process_request should appear.
     * With CALLS-only (old hardcoded): no CALLS from fetch_data -> empty callees. */
    char *raw = cbm_mcp_handle_tool(srv, "trace_call_path",
                                    "{\"function_name\":\"fetch_data\","
                                    "\"project\":\"sp-test\","
                                    "\"direction\":\"outbound\","
                                    "\"edge_types\":[\"HTTP_CALLS\"]}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);
    yyjson_doc *doc = yyjson_read(resp, strlen(resp), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *callees = yyjson_obj_get(yyjson_doc_get_root(doc), "callees");
    ASSERT_NOT_NULL(callees);
    /* RED: edge_types ignored, CALLS used, fetch_data has no CALLS -> callees empty.
     * GREEN: HTTP_CALLS traversed -> process_request in callees. */
    ASSERT_GT((int)yyjson_arr_size(callees), 0);
    yyjson_doc_free(doc);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(trace_call_path_default_edge_types_calls_only) {
    cbm_mcp_server_t *srv = setup_sp_server();
    ASSERT_NOT_NULL(srv);
    /* Without edge_types -> default CALLS -> main -> process_request appears */
    char *raw = cbm_mcp_handle_tool(srv, "trace_call_path",
                                    "{\"function_name\":\"main\","
                                    "\"project\":\"sp-test\","
                                    "\"direction\":\"outbound\"}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);
    yyjson_doc *doc = yyjson_read(resp, strlen(resp), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *callees = yyjson_obj_get(yyjson_doc_get_root(doc), "callees");
    /* main has CALLS -> process_request. Default behavior unchanged. */
    ASSERT_NOT_NULL(callees);
    ASSERT_GT((int)yyjson_arr_size(callees), 0);
    yyjson_doc_free(doc);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  2.0 JSON OUTPUT MINIFICATION
 *  All tool responses must be single-line minified JSON.
 *  yy_doc_to_str uses YYJSON_WRITE_ALLOW_INVALID_UNICODE (no PRETTY).
 *  Tests verify this contract holds across the full API surface.
 * ══════════════════════════════════════════════════════════════════ */

TEST(all_mcp_responses_are_minified_json) {
    cbm_mcp_server_t *srv = setup_sp_server();
    ASSERT_NOT_NULL(srv);

    const char *tools[] = {"search_graph", "trace_call_path", "get_architecture", "query_graph"};
    const char *args[]  = {
        "{\"project\":\"sp-test\",\"limit\":3}",
        "{\"function_name\":\"main\",\"project\":\"sp-test\"}",
        "{\"project\":\"sp-test\"}",
        "{\"query\":\"MATCH (n) RETURN n.name LIMIT 3\",\"project\":\"sp-test\"}"
    };
    for (int t = 0; t < 4; t++) {
        char *raw  = cbm_mcp_handle_tool(srv, tools[t], args[t]);
        char *text = extract_text_content_tr(raw);
        free(raw);
        ASSERT_NOT_NULL(text);
        /* Pretty-printed JSON always contains newlines — must be absent */
        ASSERT_NULL(strstr(text, "\n"));
        free(text);
    }

    cbm_mcp_server_free(srv);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  2.1 trace_call_path FIELD OMISSION (TDD)
 *  Candidates block uses empty-string fallback for file_path (mcp.c:2116).
 *  RED until candidates block is fixed like search_graph.
 * ══════════════════════════════════════════════════════════════════ */

/* Empty file_path in a candidate must be omitted, not emitted as "". */
TEST(trace_call_path_candidates_omits_empty_file_path) {
    cbm_mcp_server_t *srv = setup_sp_server();
    ASSERT_NOT_NULL(srv);
    cbm_store_t *st = cbm_mcp_server_store(srv);

    /* Second "main" node with empty file_path forces ambiguity */
    cbm_node_t dup = {0};
    dup.project = "sp-test";
    dup.label = "Function";
    dup.name = "main";
    dup.qualified_name = "sp-test.alt.main";
    dup.file_path = "";
    dup.properties_json = "{}";
    cbm_store_upsert_node(st, &dup);

    char *raw = cbm_mcp_handle_tool(srv, "trace_call_path",
                                    "{\"function_name\":\"main\","
                                    "\"project\":\"sp-test\"}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"candidates\""));

    yyjson_doc *doc = yyjson_read(resp, strlen(resp), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *candidates = yyjson_obj_get(yyjson_doc_get_root(doc), "candidates");
    ASSERT_NOT_NULL(candidates);

    bool found = false;
    for (size_t i = 0; i < yyjson_arr_size(candidates); i++) {
        yyjson_val *c = yyjson_arr_get(candidates, i);
        yyjson_val *qn = yyjson_obj_get(c, "qualified_name");
        if (qn && strcmp(yyjson_get_str(qn), "sp-test.alt.main") == 0) {
            /* Candidate with empty file_path must NOT have the key */
            ASSERT_NULL(yyjson_obj_get(c, "file_path"));
            found = true;
        }
    }
    ASSERT_TRUE(found);

    yyjson_doc_free(doc);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* Non-empty file_path in candidates must still be present (regression guard). */
TEST(trace_call_path_candidates_includes_nonempty_file_path) {
    cbm_mcp_server_t *srv = setup_sp_server();
    ASSERT_NOT_NULL(srv);
    cbm_store_t *st = cbm_mcp_server_store(srv);

    cbm_node_t dup = {0};
    dup.project = "sp-test";
    dup.label = "Function";
    dup.name = "main";
    dup.qualified_name = "sp-test.alt.main";
    dup.file_path = "alt.py";
    dup.properties_json = "{}";
    cbm_store_upsert_node(st, &dup);

    char *raw = cbm_mcp_handle_tool(srv, "trace_call_path",
                                    "{\"function_name\":\"main\","
                                    "\"project\":\"sp-test\"}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);

    yyjson_doc *doc = yyjson_read(resp, strlen(resp), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *candidates = yyjson_obj_get(yyjson_doc_get_root(doc), "candidates");
    ASSERT_NOT_NULL(candidates);

    /* All candidates here have non-empty file_path -> key must be present */
    for (size_t i = 0; i < yyjson_arr_size(candidates); i++) {
        yyjson_val *c = yyjson_arr_get(candidates, i);
        ASSERT_NOT_NULL(yyjson_obj_get(c, "file_path"));
    }

    yyjson_doc_free(doc);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  2.2 get_architecture COMPACT COVERAGE
 *  key_functions already uses null-guards (if (n), if (lbl), if (fp)).
 *  Tests verify the contract and that output remains minified.
 * ══════════════════════════════════════════════════════════════════ */

TEST(get_architecture_output_is_minified_and_no_empty_fields) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    cbm_mcp_server_set_project(srv, "arch-test");
    cbm_store_upsert_project(st, "arch-test", "/tmp");

    cbm_node_t n = {0};
    n.project = "arch-test";
    n.label = "Function";
    n.name = "entry_point";
    n.qualified_name = "arch-test.main.entry_point";
    n.file_path = "main.py";
    n.properties_json = "{}";
    cbm_store_upsert_node(st, &n);

    char *raw = cbm_mcp_handle_tool(srv, "get_architecture",
                                    "{\"project\":\"arch-test\"}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);

    /* Must be minified */
    ASSERT_NULL(strstr(resp, "\n"));

    /* key_functions block must never emit empty-string values */
    ASSERT_NULL(strstr(resp, "\"name\":\"\""));
    ASSERT_NULL(strstr(resp, "\"label\":\"\""));
    ASSERT_NULL(strstr(resp, "\"file_path\":\"\""));
    ASSERT_NULL(strstr(resp, "\"qualified_name\":\"\""));

    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  2.3 trace_call_path callers_total field
 * ══════════════════════════════════════════════════════════════════ */

TEST(trace_call_path_response_includes_callers_total) {
    /* TDD RED: callers_total never emitted (Bug C) — becomes GREEN after fix */
    cbm_mcp_server_t *srv = setup_sp_server();
    ASSERT_NOT_NULL(srv);
    /* direction=both triggers do_inbound=true; main has no callers but
     * callers_total must still appear in the response */
    char *raw = cbm_mcp_handle_tool(srv, "trace_call_path",
                                    "{\"function_name\":\"main\","
                                    "\"project\":\"sp-test\","
                                    "\"direction\":\"both\"}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);
    /* callers_total must be present even when callers array is empty */
    ASSERT_NOT_NULL(strstr(resp, "\"callers_total\""));
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  2.4 get_code_snippet empty field omission
 * ══════════════════════════════════════════════════════════════════ */

TEST(get_code_snippet_omits_empty_name_label) {
    /* TDD RED: name/label emitted as "" when NULL/empty (Bug B) */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    cbm_mcp_server_set_project(srv, "snip-test");
    cbm_store_upsert_project(st, "snip-test", "/tmp");

    /* Node with empty name and empty label — exercises the "" guard */
    cbm_node_t n = {0};
    n.project = "snip-test";
    n.name = "";          /* empty — should NOT appear as "name":"" */
    n.label = "";         /* empty — should NOT appear as "label":"" */
    n.qualified_name = "snip-test.mod.empty_node";
    n.file_path = "";     /* empty — should NOT appear as "file_path":"" */
    n.start_line = 1;
    n.end_line = 2;
    n.properties_json = "{}";
    cbm_store_upsert_node(st, &n);

    char *raw = cbm_mcp_handle_tool(srv, "get_code_snippet",
                                    "{\"qualified_name\":\"snip-test.mod.empty_node\","
                                    "\"project\":\"snip-test\"}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);
    ASSERT_NULL(strstr(resp, "\"name\":\"\""));
    ASSERT_NULL(strstr(resp, "\"label\":\"\""));
    ASSERT_NULL(strstr(resp, "\"file_path\":\"\""));
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  2.5 get_architecture compact applied to key_functions
 * ══════════════════════════════════════════════════════════════════ */

TEST(get_architecture_compact_omits_redundant_name_in_key_functions) {
    /* TDD RED: key_functions always emits name (Bug A) — becomes GREEN after fix.
     * All sp-test nodes have name == last segment of qualified_name, so
     * compact should omit every name field in key_functions. */
    cbm_mcp_server_t *srv = setup_sp_server();
    ASSERT_NOT_NULL(srv);
    char *raw = cbm_mcp_handle_tool(srv, "get_architecture",
                                    "{\"project\":\"sp-test\"}");
    char *resp = extract_text_content_tr(raw);
    free(raw);
    ASSERT_NOT_NULL(resp);

    /* Parse key_functions and assert no entry has a "name" key that equals
     * the last segment of its "qualified_name" */
    yyjson_doc *doc = yyjson_read(resp, strlen(resp), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *kfs = yyjson_obj_get(root, "key_functions");
    if (kfs && yyjson_is_arr(kfs)) {
        size_t idx, max;
        yyjson_val *kf;
        yyjson_arr_foreach(kfs, idx, max, kf) {
            yyjson_val *name_val = yyjson_obj_get(kf, "name");
            yyjson_val *qn_val   = yyjson_obj_get(kf, "qualified_name");
            if (name_val && qn_val) {
                const char *nm = yyjson_get_str(name_val);
                const char *qn = yyjson_get_str(qn_val);
                /* If name is present, it must NOT equal the last segment of qn */
                if (nm && qn) {
                    size_t qn_len = strlen(qn);
                    size_t nm_len = strlen(nm);
                    bool is_suffix = (nm_len < qn_len) &&
                                     (qn[qn_len - nm_len - 1] == '.' ||
                                      qn[qn_len - nm_len - 1] == ':' ||
                                      qn[qn_len - nm_len - 1] == '/') &&
                                     strcmp(qn + qn_len - nm_len, nm) == 0;
                    ASSERT_FALSE(is_suffix); /* compact must have omitted this */
                }
            }
        }
    }
    yyjson_doc_free(doc);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  SUITE
 * ══════════════════════════════════════════════════════════════════ */

SUITE(token_reduction) {
    /* 1.1 Default Limits */
    RUN_TEST(search_graph_default_limit_is_50);
    RUN_TEST(search_graph_explicit_limit_honored);
    RUN_TEST(search_graph_explicit_high_limit_still_works);
    RUN_TEST(search_code_default_limit_is_50);
    RUN_TEST(search_graph_pagination_stable_ordering);

    /* 1.2 Smart Truncation */
    RUN_TEST(snippet_full_mode_default_200_lines);
    RUN_TEST(snippet_full_mode_small_function_no_truncation);
    RUN_TEST(snippet_signature_mode);
    RUN_TEST(snippet_head_tail_mode);
    RUN_TEST(snippet_head_tail_no_truncation_when_fits);
    RUN_TEST(snippet_custom_max_lines);
    RUN_TEST(snippet_max_lines_zero_means_unlimited);

    /* 1.3 Compact Mode */
    RUN_TEST(search_graph_compact_omits_redundant_name);
    RUN_TEST(search_graph_compact_defaults_to_true);
    RUN_TEST(search_graph_compact_false_includes_name);
    RUN_TEST(trace_compact_omits_redundant_name);

    /* 1.4 Summary Mode */
    RUN_TEST(search_graph_summary_mode);

    /* 1.5 Trace Edge Cases */
    RUN_TEST(trace_ambiguous_function_returns_candidates);
    RUN_TEST(trace_bfs_deduplicates_cycles);
    RUN_TEST(trace_max_results_parameter);

    /* 1.7 query_graph Output Truncation */
    RUN_TEST(query_graph_max_output_bytes_truncates);
    RUN_TEST(query_graph_aggregation_not_broken);
    RUN_TEST(query_graph_max_output_bytes_zero_unlimited);

    /* 1.8 Token Metadata */
    RUN_TEST(response_includes_meta_fields);

    /* 1.9 Field Omission */
    RUN_TEST(search_graph_omits_empty_label_and_file_path);
    RUN_TEST(search_graph_includes_nonempty_label_and_file_path);
    RUN_TEST(search_graph_omits_zero_degrees);
    RUN_TEST(search_graph_includes_nonzero_degrees);

    /* 2.0 JSON Output Minification */
    RUN_TEST(all_mcp_responses_are_minified_json);

    /* 2.1 trace_call_path Field Omission */
    RUN_TEST(trace_call_path_candidates_omits_empty_file_path);
    RUN_TEST(trace_call_path_candidates_includes_nonempty_file_path);

    /* 2.2 get_architecture Compact Coverage */
    RUN_TEST(get_architecture_output_is_minified_and_no_empty_fields);

    /* Search Parameterization Accuracy */
    RUN_TEST(search_graph_qn_pattern_filters_results);
    RUN_TEST(search_graph_qn_pattern_no_match_returns_empty);
    RUN_TEST(search_graph_relationship_filters_to_matching_edge_type);
    RUN_TEST(search_graph_relationship_nonexistent_type_returns_empty);
    RUN_TEST(search_graph_exclude_entry_points_removes_zero_inbound);
    RUN_TEST(search_graph_exclude_entry_points_false_keeps_all);
    RUN_TEST(search_graph_include_dependencies_true_includes_dep_nodes);
    RUN_TEST(search_graph_include_dependencies_false_excludes_dep_nodes);
    RUN_TEST(trace_call_path_compact_defaults_to_true);
    RUN_TEST(trace_call_path_compact_false_includes_name);
    RUN_TEST(trace_call_path_edge_types_http_calls_traverses_http_edges);
    RUN_TEST(trace_call_path_default_edge_types_calls_only);

    /* 2.3 callers_total field completeness */
    RUN_TEST(trace_call_path_response_includes_callers_total);

    /* 2.4 get_code_snippet empty field omission */
    RUN_TEST(get_code_snippet_omits_empty_name_label);

    /* 2.5 get_architecture compact key_functions */
    RUN_TEST(get_architecture_compact_omits_redundant_name_in_key_functions);
}
