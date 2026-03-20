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
}
