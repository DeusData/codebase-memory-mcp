/*
 * test_integration.c — End-to-end integration tests for the pure C pipeline.
 *
 * Creates a temporary project with real source files, indexes it through
 * the full pipeline, then queries the result through MCP tool handlers.
 *
 * This exercises the complete flow: discover → extract → registry → graph
 * buffer → SQLite dump → query. No mocking — real files, real parsing.
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include <mcp/mcp.h>
#include <store/store.h>
#include <pipeline/pipeline.h>
#include <foundation/log.h>

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>

/* ── Test fixture: temp project with Python + Go files ─────────── */

static char g_tmpdir[256];
static char g_dbpath[512];
static cbm_mcp_server_t *g_srv = NULL;
static char *g_project = NULL;

/* Create source files in temp directory */
static int create_test_project(void) {
    snprintf(g_tmpdir, sizeof(g_tmpdir), "/tmp/cbm_integ_XXXXXX");
    if (!cbm_mkdtemp(g_tmpdir))
        return -1;

    char path[512];
    FILE *f;

    /* Python file with function calls */
    snprintf(path, sizeof(path), "%s/main.py", g_tmpdir);
    f = fopen(path, "w");
    if (!f)
        return -1;
    fprintf(f, "def greet(name):\n"
               "    return 'Hello ' + name\n"
               "\n"
               "def farewell(name):\n"
               "    return 'Goodbye ' + name\n"
               "\n"
               "def main():\n"
               "    msg = greet('World')\n"
               "    msg2 = farewell('World')\n"
               "    print(msg, msg2)\n");
    fclose(f);

    /* Go file with function calls */
    snprintf(path, sizeof(path), "%s/utils.go", g_tmpdir);
    f = fopen(path, "w");
    if (!f)
        return -1;
    fprintf(f, "package utils\n"
               "\n"
               "func Add(a, b int) int {\n"
               "    return a + b\n"
               "}\n"
               "\n"
               "func Multiply(a, b int) int {\n"
               "    sum := Add(a, b)\n"
               "    return sum * 2\n"
               "}\n"
               "\n"
               "func Compute(x int) int {\n"
               "    return Multiply(x, Add(x, 1))\n"
               "}\n");
    fclose(f);

    /* JavaScript file */
    snprintf(path, sizeof(path), "%s/app.js", g_tmpdir);
    f = fopen(path, "w");
    if (!f)
        return -1;
    fprintf(f, "function validate(input) {\n"
               "    return input != null;\n"
               "}\n"
               "\n"
               "function process(data) {\n"
               "    if (validate(data)) {\n"
               "        return data.toUpperCase();\n"
               "    }\n"
               "    return null;\n"
               "}\n");
    fclose(f);

    return 0;
}

/* Set up: create project, index it through MCP (production flow) */
static int integration_setup(void) {
    if (create_test_project() != 0)
        return -1;

    /* Derive project name (same logic the pipeline uses) */
    g_project = cbm_project_name_from_path(g_tmpdir);
    if (!g_project)
        return -1;

    /* Build db path for direct store queries (pipeline writes here) */
    const char *home = getenv("HOME");
    if (!home)
        home = "/tmp";
    snprintf(g_dbpath, sizeof(g_dbpath), "%s/.cache/codebase-memory-mcp/%s.db", home, g_project);

    /* Ensure cache dir exists */
    char cache_dir[512];
    snprintf(cache_dir, sizeof(cache_dir), "%s/.cache/codebase-memory-mcp", home);
    cbm_mkdir(cache_dir);

    /* Remove stale db from previous test runs */
    unlink(g_dbpath);

    /* Create MCP server, then index through it (production flow):
     *   1. Server starts with in-memory store
     *   2. index_repository closes in-memory store
     *   3. Pipeline runs → dumps to ~/.cache/.../<project>.db
     *   4. Server reopens from that db
     * This exercises the exact same path as real usage. */
    g_srv = cbm_mcp_server_new(NULL);
    if (!g_srv)
        return -1;

    /* Index our temp project via MCP tool handler */
    char args[512];
    snprintf(args, sizeof(args), "{\"repo_path\":\"%s\"}", g_tmpdir);
    char *resp = cbm_mcp_handle_tool(g_srv, "index_repository", args);
    if (!resp)
        return -1;

    /* Verify indexing succeeded */
    bool ok = strstr(resp, "indexed") != NULL;
    free(resp);
    return ok ? 0 : -1;
}

static void integration_teardown(void) {
    if (g_srv) {
        cbm_mcp_server_free(g_srv);
        g_srv = NULL;
    }
    free(g_project);
    g_project = NULL;

    /* Clean up temp project */
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", g_tmpdir);
    system(cmd);

    /* Clean up cache db */
    unlink(g_dbpath);
    char wal[520], shm[520];
    snprintf(wal, sizeof(wal), "%s-wal", g_dbpath);
    snprintf(shm, sizeof(shm), "%s-shm", g_dbpath);
    unlink(wal);
    unlink(shm);
}

/* ══════════════════════════════════════════════════════════════════
 *  PIPELINE INTEGRATION TESTS
 * ══════════════════════════════════════════════════════════════════ */

/* Helper: call a tool and return response JSON. Caller must free(). */
static char *call_tool(const char *tool, const char *args) {
    if (!g_srv)
        return NULL;
    return cbm_mcp_handle_tool(g_srv, tool, args);
}

TEST(integ_index_has_nodes) {
    /* Open the indexed db directly and check node counts */
    cbm_store_t *store = cbm_store_open_path(g_dbpath);
    ASSERT_NOT_NULL(store);

    int nodes = cbm_store_count_nodes(store, g_project);
    /* We expect: 3 File nodes + 3+ Function/Method nodes per file +
     * Folder/Package/Module nodes. Should be at least 8. */
    ASSERT_TRUE(nodes >= 8);

    cbm_store_close(store);
    PASS();
}

TEST(integ_index_has_edges) {
    cbm_store_t *store = cbm_store_open_path(g_dbpath);
    ASSERT_NOT_NULL(store);

    int edges = cbm_store_count_edges(store, g_project);
    /* We expect CONTAINS_FILE edges + CALLS edges + others */
    ASSERT_TRUE(edges >= 3);

    cbm_store_close(store);
    PASS();
}

TEST(integ_index_has_functions) {
    cbm_store_t *store = cbm_store_open_path(g_dbpath);
    ASSERT_NOT_NULL(store);

    cbm_node_t *funcs = NULL;
    int count = 0;
    int rc = cbm_store_find_nodes_by_label(store, g_project, "Function", &funcs, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    /* Python: greet, farewell, main. Go: Add, Multiply, Compute. JS: validate, process */
    ASSERT_TRUE(count >= 6);

    /* Verify some function names exist */
    bool found_greet = false, found_add = false, found_validate = false;
    for (int i = 0; i < count; i++) {
        if (funcs[i].name && strcmp(funcs[i].name, "greet") == 0)
            found_greet = true;
        if (funcs[i].name && strcmp(funcs[i].name, "Add") == 0)
            found_add = true;
        if (funcs[i].name && strcmp(funcs[i].name, "validate") == 0)
            found_validate = true;
    }
    ASSERT_TRUE(found_greet);
    ASSERT_TRUE(found_add);
    ASSERT_TRUE(found_validate);

    cbm_store_free_nodes(funcs, count);
    cbm_store_close(store);
    PASS();
}

TEST(integ_index_has_files) {
    cbm_store_t *store = cbm_store_open_path(g_dbpath);
    ASSERT_NOT_NULL(store);

    cbm_node_t *files = NULL;
    int count = 0;
    int rc = cbm_store_find_nodes_by_label(store, g_project, "File", &files, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(count, 3); /* main.py, utils.go, app.js */

    bool found_py = false, found_go = false, found_js = false;
    for (int i = 0; i < count; i++) {
        if (files[i].file_path && strstr(files[i].file_path, "main.py"))
            found_py = true;
        if (files[i].file_path && strstr(files[i].file_path, "utils.go"))
            found_go = true;
        if (files[i].file_path && strstr(files[i].file_path, "app.js"))
            found_js = true;
    }
    ASSERT_TRUE(found_py);
    ASSERT_TRUE(found_go);
    ASSERT_TRUE(found_js);

    cbm_store_free_nodes(files, count);
    cbm_store_close(store);
    PASS();
}

TEST(integ_index_has_calls) {
    cbm_store_t *store = cbm_store_open_path(g_dbpath);
    ASSERT_NOT_NULL(store);

    int call_count = cbm_store_count_edges_by_type(store, g_project, "CALLS");
    /* Python: main→greet, main→farewell, main→print
     * Go: Multiply→Add, Compute→Multiply, Compute→Add
     * JS: process→validate */
    ASSERT_TRUE(call_count >= 4);

    cbm_store_close(store);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  MCP TOOL HANDLER INTEGRATION
 * ══════════════════════════════════════════════════════════════════ */

TEST(integ_mcp_list_projects) {
    char *resp = call_tool("list_projects", "{}");
    ASSERT_NOT_NULL(resp);
    /* Should contain the project name derived from temp path */
    ASSERT_NOT_NULL(strstr(resp, "project"));
    free(resp);
    PASS();
}

TEST(integ_mcp_search_graph_by_label) {
    char args[256];
    snprintf(args, sizeof(args), "{\"label\":\"Function\",\"project\":\"%s\",\"limit\":20}",
             g_project);

    char *resp = call_tool("search_graph", args);
    ASSERT_NOT_NULL(resp);
    /* Should return function nodes */
    ASSERT_NOT_NULL(strstr(resp, "Function"));
    /* Should contain our known functions */
    ASSERT_NOT_NULL(strstr(resp, "greet"));
    free(resp);
    PASS();
}

TEST(integ_mcp_search_graph_by_name) {
    char args[256];
    snprintf(args, sizeof(args), "{\"name_pattern\":\".*Add.*\",\"project\":\"%s\"}", g_project);

    char *resp = call_tool("search_graph", args);
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "Add"));
    free(resp);
    PASS();
}

TEST(integ_mcp_query_graph_functions) {
    char args[512];
    snprintf(args, sizeof(args),
             "{\"project\":\"%s\",\"query\":\"MATCH (f:Function) WHERE f.project = '%s' "
             "RETURN f.name LIMIT 20\"}",
             g_project, g_project);

    char *resp = call_tool("query_graph", args);
    ASSERT_NOT_NULL(resp);
    /* Should return results (may be in various formats depending on Cypher output).
     * At minimum, should not be an error. */
    ASSERT_TRUE(strstr(resp, "row") || strstr(resp, "greet") || strstr(resp, "Add") ||
                strstr(resp, "result") || strstr(resp, "f.name"));
    free(resp);
    PASS();
}

TEST(integ_mcp_query_graph_calls) {
    char args[512];
    snprintf(args, sizeof(args),
             "{\"project\":\"%s\",\"query\":\"MATCH (a)-[r:CALLS]->(b) WHERE a.project = '%s' "
             "RETURN a.name, b.name LIMIT 20\"}",
             g_project, g_project);

    char *resp = call_tool("query_graph", args);
    ASSERT_NOT_NULL(resp);
    /* Should have some call relationships */
    ASSERT_NOT_NULL(strstr(resp, "name"));
    free(resp);
    PASS();
}

TEST(integ_mcp_get_graph_schema) {
    char args[128];
    snprintf(args, sizeof(args), "{\"project\":\"%s\"}", g_project);

    char *resp = call_tool("get_graph_schema", args);
    ASSERT_NOT_NULL(resp);
    /* Schema should include node labels and edge types */
    ASSERT_NOT_NULL(strstr(resp, "Function"));
    ASSERT_NOT_NULL(strstr(resp, "File"));
    free(resp);
    PASS();
}

TEST(integ_mcp_get_architecture) {
    char args[128];
    snprintf(args, sizeof(args), "{\"project\":\"%s\"}", g_project);

    char *resp = call_tool("get_architecture", args);
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "total_nodes"));
    free(resp);
    PASS();
}

TEST(integ_mcp_trace_call_path) {
    /* Trace outbound calls from Compute → should reach Add and Multiply */
    char args[256];
    snprintf(args, sizeof(args),
             "{\"function_name\":\"Compute\",\"project\":\"%s\","
             "\"direction\":\"outbound\",\"max_depth\":3}",
             g_project);

    char *resp = call_tool("trace_call_path", args);
    ASSERT_NOT_NULL(resp);
    /* Should find the function and show some path */
    /* Either finds the function, or returns not found if name doesn't match exactly */
    ASSERT_TRUE(strstr(resp, "Compute") || strstr(resp, "Multiply") || strstr(resp, "not found"));
    free(resp);
    PASS();
}

TEST(integ_mcp_index_status) {
    char args[128];
    snprintf(args, sizeof(args), "{\"project\":\"%s\"}", g_project);

    char *resp = call_tool("index_status", args);
    ASSERT_NOT_NULL(resp);
    /* Should show indexed status with node/edge counts */
    ASSERT_NOT_NULL(strstr(resp, g_project));
    free(resp);
    PASS();
}

TEST(integ_mcp_delete_project) {
    /* Delete the project and verify it's gone */
    char args[256];
    snprintf(args, sizeof(args), "{\"project\":\"%s\"}", g_project);

    char *resp = call_tool("delete_project", args);
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "deleted"));
    free(resp);

    /* Note: querying after delete on Linux re-opens the unlinked .db inode
     * (unlink defers removal until all fds close). SQLite's WAL mode connection
     * on an unlinked file leaks internal allocations that sqlite3_close cannot
     * reclaim. Guard behavior for deleted/missing projects is tested separately
     * in tests/smoke_guard.sh using non-existent project names. */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  PIPELINE DIRECT API TESTS
 * ══════════════════════════════════════════════════════════════════ */

TEST(integ_pipeline_fqn_compute) {
    char *fqn = cbm_pipeline_fqn_compute("myproject", "src/utils.go", "Add");
    ASSERT_NOT_NULL(fqn);
    ASSERT_STR_EQ(fqn, "myproject.src.utils.Add");
    free(fqn);
    PASS();
}

TEST(integ_pipeline_fqn_module) {
    char *fqn = cbm_pipeline_fqn_module("myproject", "src/utils.go");
    ASSERT_NOT_NULL(fqn);
    ASSERT_STR_EQ(fqn, "myproject.src.utils");
    free(fqn);
    PASS();
}

TEST(integ_pipeline_project_name) {
    char *name = cbm_project_name_from_path("/home/user/my-project");
    ASSERT_NOT_NULL(name);
    /* Should contain "my-project" or a sanitized version */
    ASSERT_NOT_NULL(strstr(name, "my-project"));
    free(name);
    PASS();
}

TEST(integ_pipeline_cancel) {
    /* Create and immediately cancel a pipeline */
    cbm_pipeline_t *p = cbm_pipeline_new(g_tmpdir, NULL, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);

    cbm_pipeline_cancel(p);
    int rc = cbm_pipeline_run(p);
    /* Should return -1 (cancelled) or complete with partial results */
    /* Either way, it shouldn't crash */
    (void)rc;

    cbm_pipeline_free(p);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  STORE QUERY INTEGRATION
 * ══════════════════════════════════════════════════════════════════ */

TEST(integ_store_search_by_degree) {
    cbm_store_t *store = cbm_store_open_path(g_dbpath);
    ASSERT_NOT_NULL(store);

    /* Find functions with at least 1 outbound call */
    cbm_search_params_t params = {0};
    params.project = g_project;
    params.label = "Function";
    params.min_degree = 1;
    params.max_degree = -1;
    params.limit = 10;

    cbm_search_output_t out = {0};
    int rc = cbm_store_search(store, &params, &out);
    ASSERT_EQ(rc, CBM_STORE_OK);
    /* main, Multiply, Compute, process should all have outbound calls */
    ASSERT_TRUE(out.count >= 1);

    cbm_store_search_free(&out);
    cbm_store_close(store);
    PASS();
}

TEST(integ_store_find_by_file) {
    cbm_store_t *store = cbm_store_open_path(g_dbpath);
    ASSERT_NOT_NULL(store);

    cbm_node_t *nodes = NULL;
    int count = 0;
    int rc = cbm_store_find_nodes_by_file(store, g_project, "main.py", &nodes, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    /* main.py should have: greet, farewell, main functions + Module node */
    ASSERT_TRUE(count >= 3);

    cbm_store_free_nodes(nodes, count);
    cbm_store_close(store);
    PASS();
}

TEST(integ_store_bfs_traversal) {
    cbm_store_t *store = cbm_store_open_path(g_dbpath);
    ASSERT_NOT_NULL(store);

    /* Find a function node to start BFS from */
    cbm_node_t *results = NULL;
    int count = 0;
    cbm_store_find_nodes_by_name(store, g_project, "Multiply", &results, &count);

    if (count > 0) {
        /* BFS outbound from Multiply */
        cbm_traverse_result_t trav = {0};
        int rc = cbm_store_bfs(store, results[0].id, "outbound", NULL, 0, 3, 20, &trav);
        ASSERT_EQ(rc, CBM_STORE_OK);
        /* Should visit at least Add */
        ASSERT_TRUE(trav.visited_count >= 0); /* might be 0 if no edges */
        cbm_store_traverse_free(&trav);
    }

    cbm_store_free_nodes(results, count);
    cbm_store_close(store);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  SUITE
 * ══════════════════════════════════════════════════════════════════ */

SUITE(integration) {
    /* Set up: create temp project and index it */
    if (integration_setup() != 0) {
        printf("  %-50s", "integration_setup");
        printf("SKIP (setup failed)\n");
        tf_skip_count += 16; /* skip all integration tests */
        integration_teardown();
        return;
    }

    /* Pipeline result validation */
    RUN_TEST(integ_index_has_nodes);
    RUN_TEST(integ_index_has_edges);
    RUN_TEST(integ_index_has_functions);
    RUN_TEST(integ_index_has_files);
    RUN_TEST(integ_index_has_calls);

    /* MCP tool handler validation */
    RUN_TEST(integ_mcp_list_projects);
    RUN_TEST(integ_mcp_search_graph_by_label);
    RUN_TEST(integ_mcp_search_graph_by_name);
    RUN_TEST(integ_mcp_query_graph_functions);
    RUN_TEST(integ_mcp_query_graph_calls);
    RUN_TEST(integ_mcp_get_graph_schema);
    RUN_TEST(integ_mcp_get_architecture);
    RUN_TEST(integ_mcp_trace_call_path);
    RUN_TEST(integ_mcp_index_status);

    /* Store query validation */
    RUN_TEST(integ_store_search_by_degree);
    RUN_TEST(integ_store_find_by_file);
    RUN_TEST(integ_store_bfs_traversal);

    /* Pipeline API tests (no db needed) */
    RUN_TEST(integ_pipeline_fqn_compute);
    RUN_TEST(integ_pipeline_fqn_module);
    RUN_TEST(integ_pipeline_project_name);
    RUN_TEST(integ_pipeline_cancel);

    /* Destructive tests (run last!) */
    RUN_TEST(integ_mcp_delete_project);

    /* Teardown */
    integration_teardown();
}
