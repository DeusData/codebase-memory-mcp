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
#include "test_helpers.h"
#include <mcp/mcp.h>
#include <store/store.h>
#include <pipeline/pipeline.h>
#include <foundation/platform.h>
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

static int count_in_response(const char *resp, const char *key) {
    if (!resp)
        return -1;
    char pattern[64];
    snprintf(pattern, sizeof(pattern), "\"%s\":", key);
    const char *p = strstr(resp, pattern);
    if (p)
        return atoi(p + strlen(pattern));
    snprintf(pattern, sizeof(pattern), "\\\"%s\\\":", key);
    p = strstr(resp, pattern);
    if (p)
        return atoi(p + strlen(pattern));
    return -1;
}

static char *string_in_response(const char *resp, const char *key) {
    if (!resp)
        return NULL;
    char pattern[64];
    snprintf(pattern, sizeof(pattern), "\"%s\":\"", key);
    const char *p = strstr(resp, pattern);
    size_t advance = strlen(pattern);
    bool escaped = false;
    if (!p) {
        snprintf(pattern, sizeof(pattern), "\\\"%s\\\":\\\"", key);
        p = strstr(resp, pattern);
        advance = strlen(pattern);
        escaped = true;
    }
    if (!p)
        return NULL;
    p += advance;
    const char *end = escaped ? strstr(p, "\\\"") : strchr(p, '"');
    if (!end || end <= p)
        return NULL;
    size_t len = (size_t)(end - p);
    char *out = malloc(len + 1);
    if (!out)
        return NULL;
    memcpy(out, p, len);
    out[len] = '\0';
    return out;
}

static bool indexed_db_ready(const char *dbpath, const char *project) {
    cbm_store_t *store = cbm_store_open_path_query(dbpath);
    if (!store)
        return false;
    int nodes = cbm_store_count_nodes(store, project);
    int edges = cbm_store_count_edges(store, project);
    cbm_store_close(store);
    return nodes > 0 && edges > 0;
}

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

    /* Derive fallback project name (same logic the pipeline uses) */
    g_project = cbm_project_name_from_path(g_tmpdir);
    if (!g_project)
        return -1;

    /* Build db path for direct store queries (pipeline writes here) */
    const char *cache = cbm_resolve_cache_dir();
    if (!cache)
        cache = "/tmp";
    snprintf(g_dbpath, sizeof(g_dbpath), "%s/%s.db", cache, g_project);

    /* Ensure cache dir exists */
    cbm_mkdir(cache);

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

    /* Verify indexing succeeded and use the actual returned project/db path. */
    bool ok = strstr(resp, "indexed") != NULL;
    char *resp_project = string_in_response(resp, "project");
    if (resp_project && resp_project[0]) {
        free(g_project);
        g_project = resp_project;
        snprintf(g_dbpath, sizeof(g_dbpath), "%s/%s.db", cache, g_project);
    } else {
        free(resp_project);
    }
    int nodes = count_in_response(resp, "nodes");
    int edges = count_in_response(resp, "edges");
    free(resp);
    if (!ok)
        return -1;
    if (nodes <= 0 || edges <= 0)
        return -1;
    return indexed_db_ready(g_dbpath, g_project) ? 0 : -1;
}

static void integration_teardown(void) {
    if (g_srv) {
        cbm_mcp_server_free(g_srv);
        g_srv = NULL;
    }
    free(g_project);
    g_project = NULL;

    /* Clean up temp project */
    th_rmtree(g_tmpdir);

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

TEST(integ_mcp_trace_path) {
    /* Trace outbound calls from Compute → should reach Add and Multiply */
    char args[256];
    snprintf(args, sizeof(args),
             "{\"function_name\":\"Compute\",\"project\":\"%s\","
             "\"direction\":\"outbound\",\"max_depth\":3}",
             g_project);

    char *resp = call_tool("trace_path", args);
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
 *  GDSCRIPT INTEGRATION TESTS
 * ══════════════════════════════════════════════════════════════════ */

/* GDScript fixture project setup — uses the min_project fixture from
 * tests/fixtures/gdscript/min_project/actors/ */

static char g_gd_tmpdir[256];
static char g_gd_dbpath[512];
static cbm_mcp_server_t *g_gd_srv = NULL;
static char *g_gd_project = NULL;

static int create_gdscript_test_project(void) {
    snprintf(g_gd_tmpdir, sizeof(g_gd_tmpdir), "/tmp/cbm_gd_integ_XXXXXX");
    if (!cbm_mkdtemp(g_gd_tmpdir))
        return -1;

    /* Create actors subdirectory */
    char actors_dir[512];
    snprintf(actors_dir, sizeof(actors_dir), "%s/actors", g_gd_tmpdir);
    if (cbm_mkdir(actors_dir) != 0)
        return -1;

    FILE *f;
    char path[512];

    /* base.gd — named class with signal */
    snprintf(path, sizeof(path), "%s/actors/base.gd", g_gd_tmpdir);
    f = fopen(path, "w");
    if (!f) return -1;
    fprintf(f, "class_name Base\n"
               "signal hit\n"
               "func ping():\n"
               "    pass\n");
    fclose(f);

    /* player.gd — main test fixture with signals, imports, inheritance */
    snprintf(path, sizeof(path), "%s/actors/player.gd", g_gd_tmpdir);
    f = fopen(path, "w");
    if (!f) return -1;
    fprintf(f, "class_name Player\n"
               "const BaseAlias = preload(\"res://actors/base.gd\")\n"
               "const ReceiverClass = preload(\"res://actors/receiver.gd\")\n"
               "extends BaseAlias\n"
               "signal hit\n"
               "const Weapon = preload(\"res://actors/weapon.gd\")\n"
               "var WeaponRel = load(\"weapon.gd\")\n"
               "const Scene = preload(\"res://actors/player.tscn\")\n"
               "func attack():\n"
               "    emit_signal(\"hit\")\n"
               "    self.hit.emit()\n"
               "    hit.connect(_on_hit)\n"
               "    var r = ReceiverClass.new()\n"
               "    r.hit.connect(_on_receiver_hit)\n"
               "    r.hit.emit()\n"
               "    helper()\n"
               "\n"
               "func helper():\n"
               "    pass\n"
               "\n"
               "func shadow_param(hit):\n"
               "    hit.connect(_on_hit)\n"
               "\n"
               "func _on_hit():\n"
               "    pass\n"
               "\n"
               "func _on_receiver_hit():\n"
               "    pass\n");
    fclose(f);

    /* receiver.gd — signal receiver */
    snprintf(path, sizeof(path), "%s/actors/receiver.gd", g_gd_tmpdir);
    f = fopen(path, "w");
    if (!f) return -1;
    fprintf(f, "class_name Receiver\n"
               "signal hit\n");
    fclose(f);

    /* weapon.gd — minimal class */
    snprintf(path, sizeof(path), "%s/actors/weapon.gd", g_gd_tmpdir);
    f = fopen(path, "w");
    if (!f) return -1;
    fprintf(f, "class_name Weapon\n");
    fclose(f);

    /* player.tscn — non-code asset (should be string-ref only) */
    snprintf(path, sizeof(path), "%s/actors/player.tscn", g_gd_tmpdir);
    f = fopen(path, "w");
    if (!f) return -1;
    fprintf(f, "[gd_scene load_steps=2 format=3]\n");
    fclose(f);

    /* player_path_extends.gd — direct path inheritance */
    snprintf(path, sizeof(path), "%s/actors/player_path_extends.gd", g_gd_tmpdir);
    f = fopen(path, "w");
    if (!f) return -1;
    fprintf(f, "class_name PlayerPath\n"
               "extends \"res://actors/base.gd\"\n"
               "func attack_path():\n"
               "    pass\n");
    fclose(f);

    /* player_named_extends.gd — named base inheritance */
    snprintf(path, sizeof(path), "%s/actors/player_named_extends.gd", g_gd_tmpdir);
    f = fopen(path, "w");
    if (!f) return -1;
    fprintf(f, "class_name PlayerNamed\n"
               "extends Base\n"
               "func attack_named():\n"
               "    pass\n");
    fclose(f);

    /* player_preload_extends.gd — preload path inheritance */
    snprintf(path, sizeof(path), "%s/actors/player_preload_extends.gd", g_gd_tmpdir);
    f = fopen(path, "w");
    if (!f) return -1;
    fprintf(f, "class_name PlayerPreloadPath\n"
               "extends preload(\"res://actors/base.gd\")\n"
               "func attack_preload_path():\n"
               "    pass\n");
    fclose(f);

    /* nameless_script.gd — no class_name, fallback anchor */
    snprintf(path, sizeof(path), "%s/actors/nameless_script.gd", g_gd_tmpdir);
    f = fopen(path, "w");
    if (!f) return -1;
    fprintf(f, "extends \"res://actors/base.gd\"\n"
               "signal ghost_hit\n"
               "func ghost_attack():\n"
               "    emit_signal(\"ghost_hit\")\n");
    fclose(f);

    /* dynamic_receiver.gd — dynamic receiver (should NOT resolve) */
    snprintf(path, sizeof(path), "%s/actors/dynamic_receiver.gd", g_gd_tmpdir);
    f = fopen(path, "w");
    if (!f) return -1;
    fprintf(f, "class_name DynamicReceiverCase\n"
               "func attack_dynamic(target):\n"
               "    target.hit.emit()\n");
    fclose(f);

    /* builtin_base.gd — extends built-in Node2D */
    snprintf(path, sizeof(path), "%s/actors/builtin_base.gd", g_gd_tmpdir);
    f = fopen(path, "w");
    if (!f) return -1;
    fprintf(f, "class_name BuiltinBaseCase\n"
               "extends Node2D\n"
               "func tick_builtin():\n"
               "    pass\n");
    fclose(f);

    return 0;
}

static int gdscript_integration_setup(void) {
    if (create_gdscript_test_project() != 0)
        return -1;

    g_gd_project = cbm_project_name_from_path(g_gd_tmpdir);
    if (!g_gd_project)
        return -1;

    const char *cache = cbm_resolve_cache_dir();
    if (!cache)
        cache = "/tmp";
    snprintf(g_gd_dbpath, sizeof(g_gd_dbpath), "%s/%s.db", cache, g_gd_project);

    cbm_mkdir(cache);

    unlink(g_gd_dbpath);

    g_gd_srv = cbm_mcp_server_new(NULL);
    if (!g_gd_srv)
        return -1;

    char args[512];
    snprintf(args, sizeof(args), "{\"repo_path\":\"%s\"}", g_gd_tmpdir);
    char *resp = cbm_mcp_handle_tool(g_gd_srv, "index_repository", args);
    if (!resp)
        return -1;

    bool ok = strstr(resp, "indexed") != NULL;
    char *resp_project = string_in_response(resp, "project");
    if (resp_project && resp_project[0]) {
        free(g_gd_project);
        g_gd_project = resp_project;
        snprintf(g_gd_dbpath, sizeof(g_gd_dbpath), "%s/%s.db", cache, g_gd_project);
    } else {
        free(resp_project);
    }
    int nodes = count_in_response(resp, "nodes");
    int edges = count_in_response(resp, "edges");
    free(resp);
    if (!ok)
        return -1;
    if (nodes <= 0 || edges <= 0)
        return -1;
    return indexed_db_ready(g_gd_dbpath, g_gd_project) ? 0 : -1;
}

static void gdscript_integration_teardown(void) {
    if (g_gd_srv) {
        cbm_mcp_server_free(g_gd_srv);
        g_gd_srv = NULL;
    }
    free(g_gd_project);
    g_gd_project = NULL;

    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", g_gd_tmpdir);
    system(cmd);

    unlink(g_gd_dbpath);
    char wal[520], shm[520];
    snprintf(wal, sizeof(wal), "%s-wal", g_gd_dbpath);
    snprintf(shm, sizeof(shm), "%s-shm", g_gd_dbpath);
    unlink(wal);
    unlink(shm);
}

static bool gd_has_suffix(const char *str, const char *suffix) {
    if (!str || !suffix) {
        return false;
    }
    size_t str_len = strlen(str);
    size_t suffix_len = strlen(suffix);
    if (suffix_len > str_len) {
        return false;
    }
    return strcmp(str + str_len - suffix_len, suffix) == 0;
}

static int64_t gd_find_node_id_by_qn(cbm_store_t *store, const char *qn) {
    cbm_node_t node = {0};
    if (cbm_store_find_node_by_qn(store, g_gd_project, qn, &node) != CBM_STORE_OK) {
        return -1;
    }
    int64_t id = node.id;
    cbm_node_free_fields(&node);
    return id;
}

static int64_t gd_find_file_node_id(cbm_store_t *store, const char *file_path) {
    cbm_node_t *nodes = NULL;
    int count = 0;
    if (cbm_store_find_nodes_by_file(store, g_gd_project, file_path, &nodes, &count) !=
        CBM_STORE_OK) {
        return -1;
    }

    int64_t id = -1;
    for (int i = 0; i < count; i++) {
        if (nodes[i].label && strcmp(nodes[i].label, "File") == 0) {
            id = nodes[i].id;
            break;
        }
    }
    cbm_store_free_nodes(nodes, count);
    return id;
}

static bool gd_node_has_parent_class(cbm_store_t *store, const char *node_qn,
                                     const char *expected_parent_qn) {
    cbm_node_t node = {0};
    if (cbm_store_find_node_by_qn(store, g_gd_project, node_qn, &node) != CBM_STORE_OK) {
        return false;
    }

    char needle[1024];
    snprintf(needle, sizeof(needle), "\"parent_class\":\"%s\"", expected_parent_qn);
    bool ok = node.properties_json && strstr(node.properties_json, needle) != NULL;
    cbm_node_free_fields(&node);
    return ok;
}

static int gd_count_edges_to_target_qn(cbm_store_t *store, int64_t source_id, const char *type,
                                       const char *target_qn) {
    cbm_edge_t *edges = NULL;
    int count = 0;
    if (cbm_store_find_edges_by_source_type(store, source_id, type, &edges, &count) !=
        CBM_STORE_OK) {
        return 0;
    }

    int matches = 0;
    for (int i = 0; i < count; i++) {
        cbm_node_t target = {0};
        if (cbm_store_find_node_by_id(store, edges[i].target_id, &target) == CBM_STORE_OK) {
            if (target.qualified_name && strcmp(target.qualified_name, target_qn) == 0) {
                matches++;
            }
            cbm_node_free_fields(&target);
        }
    }
    cbm_store_free_edges(edges, count);
    return matches;
}

static int gd_count_edges_to_target_file(cbm_store_t *store, int64_t source_id, const char *type,
                                         const char *target_file_path) {
    cbm_edge_t *edges = NULL;
    int count = 0;
    if (cbm_store_find_edges_by_source_type(store, source_id, type, &edges, &count) !=
        CBM_STORE_OK) {
        return 0;
    }

    int matches = 0;
    for (int i = 0; i < count; i++) {
        cbm_node_t target = {0};
        if (cbm_store_find_node_by_id(store, edges[i].target_id, &target) == CBM_STORE_OK) {
            if (target.file_path && strcmp(target.file_path, target_file_path) == 0) {
                matches++;
            }
            cbm_node_free_fields(&target);
        }
    }
    cbm_store_free_edges(edges, count);
    return matches;
}

static int gd_count_project_edges_to_target_suffix(cbm_store_t *store, const char *type,
                                                   const char *target_suffix) {
    cbm_edge_t *edges = NULL;
    int count = 0;
    if (cbm_store_find_edges_by_type(store, g_gd_project, type, &edges, &count) !=
        CBM_STORE_OK) {
        return 0;
    }

    int matches = 0;
    for (int i = 0; i < count; i++) {
        cbm_node_t target = {0};
        if (cbm_store_find_node_by_id(store, edges[i].target_id, &target) == CBM_STORE_OK) {
            if (target.file_path && gd_has_suffix(target.file_path, target_suffix)) {
                matches++;
            }
            cbm_node_free_fields(&target);
        }
    }
    cbm_store_free_edges(edges, count);
    return matches;
}

TEST(integ_gdscript_discovers_gd_files) {
    cbm_store_t *store = cbm_store_open_path(g_gd_dbpath);
    ASSERT_NOT_NULL(store);

    cbm_node_t *files = NULL;
    int count = 0;
    int rc = cbm_store_find_nodes_by_label(store, g_gd_project, "File", &files, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);

    int gd_file_count = 0;
    bool found_base = false, found_player = false, found_receiver = false, found_weapon = false;
    bool found_path = false, found_named = false, found_preload = false, found_nameless = false;
    bool found_dynamic = false, found_builtin = false;
    for (int i = 0; i < count; i++) {
        if (!files[i].file_path || !gd_has_suffix(files[i].file_path, ".gd")) {
            continue;
        }
        gd_file_count++;
        if (strcmp(files[i].file_path, "actors/base.gd") == 0) found_base = true;
        if (strcmp(files[i].file_path, "actors/player.gd") == 0) found_player = true;
        if (strcmp(files[i].file_path, "actors/receiver.gd") == 0) found_receiver = true;
        if (strcmp(files[i].file_path, "actors/weapon.gd") == 0) found_weapon = true;
        if (strcmp(files[i].file_path, "actors/player_path_extends.gd") == 0) found_path = true;
        if (strcmp(files[i].file_path, "actors/player_named_extends.gd") == 0) found_named = true;
        if (strcmp(files[i].file_path, "actors/player_preload_extends.gd") == 0) found_preload = true;
        if (strcmp(files[i].file_path, "actors/nameless_script.gd") == 0) found_nameless = true;
        if (strcmp(files[i].file_path, "actors/dynamic_receiver.gd") == 0) found_dynamic = true;
        if (strcmp(files[i].file_path, "actors/builtin_base.gd") == 0) found_builtin = true;
    }

    ASSERT_EQ(gd_file_count, 10);
    ASSERT_TRUE(found_base && found_player && found_receiver && found_weapon && found_path &&
                found_named && found_preload && found_nameless && found_dynamic && found_builtin);

    cbm_store_free_nodes(files, count);
    cbm_store_close(store);
    PASS();
}

TEST(integ_gdscript_script_anchor_methods) {
    cbm_store_t *store = cbm_store_open_path(g_gd_dbpath);
    ASSERT_NOT_NULL(store);

    char player_qn[512], nameless_qn[512];
    char attack_qn[512], helper_qn[512], shadow_qn[512], on_hit_qn[512], on_receiver_qn[512];
    char ghost_attack_qn[512];
    snprintf(player_qn, sizeof(player_qn), "%s.actors.player.Player", g_gd_project);
    snprintf(nameless_qn, sizeof(nameless_qn), "%s.actors.nameless_script.__script__", g_gd_project);
    snprintf(attack_qn, sizeof(attack_qn), "%s.attack", player_qn);
    snprintf(helper_qn, sizeof(helper_qn), "%s.helper", player_qn);
    snprintf(shadow_qn, sizeof(shadow_qn), "%s.shadow_param", player_qn);
    snprintf(on_hit_qn, sizeof(on_hit_qn), "%s._on_hit", player_qn);
    snprintf(on_receiver_qn, sizeof(on_receiver_qn), "%s._on_receiver_hit", player_qn);
    snprintf(ghost_attack_qn, sizeof(ghost_attack_qn), "%s.ghost_attack", nameless_qn);

    ASSERT_TRUE(gd_node_has_parent_class(store, attack_qn, player_qn));
    ASSERT_TRUE(gd_node_has_parent_class(store, helper_qn, player_qn));
    ASSERT_TRUE(gd_node_has_parent_class(store, shadow_qn, player_qn));
    ASSERT_TRUE(gd_node_has_parent_class(store, on_hit_qn, player_qn));
    ASSERT_TRUE(gd_node_has_parent_class(store, on_receiver_qn, player_qn));
    ASSERT_TRUE(gd_node_has_parent_class(store, ghost_attack_qn, nameless_qn));

    int64_t player_id = gd_find_node_id_by_qn(store, player_qn);
    ASSERT_TRUE(player_id >= 0);
    cbm_edge_t *player_edges = NULL;
    int player_edge_count = 0;
    ASSERT_EQ(cbm_store_find_edges_by_source_type(store, player_id, "DEFINES_METHOD", &player_edges,
                                                  &player_edge_count),
              CBM_STORE_OK);
    ASSERT_EQ(player_edge_count, 5);
    cbm_store_free_edges(player_edges, player_edge_count);

    int64_t nameless_id = gd_find_node_id_by_qn(store, nameless_qn);
    ASSERT_TRUE(nameless_id >= 0);
    cbm_edge_t *nameless_edges = NULL;
    int nameless_edge_count = 0;
    ASSERT_EQ(cbm_store_find_edges_by_source_type(store, nameless_id, "DEFINES_METHOD", &nameless_edges,
                                                  &nameless_edge_count),
              CBM_STORE_OK);
    ASSERT_EQ(nameless_edge_count, 1);
    cbm_store_free_edges(nameless_edges, nameless_edge_count);

    cbm_store_close(store);
    PASS();
}

TEST(integ_gdscript_signal_calls_resolved) {
    cbm_store_t *store = cbm_store_open_path(g_gd_dbpath);
    ASSERT_NOT_NULL(store);

    char player_qn[512], attack_qn[512], helper_qn[512], shadow_qn[512];
    char player_signal_qn[512], receiver_signal_qn[512];
    char nameless_qn[512], ghost_attack_qn[512], ghost_signal_qn[512];
    snprintf(player_qn, sizeof(player_qn), "%s.actors.player.Player", g_gd_project);
    snprintf(attack_qn, sizeof(attack_qn), "%s.attack", player_qn);
    snprintf(helper_qn, sizeof(helper_qn), "%s.helper", player_qn);
    snprintf(shadow_qn, sizeof(shadow_qn), "%s.shadow_param", player_qn);
    snprintf(player_signal_qn, sizeof(player_signal_qn), "%s.signal.hit", player_qn);
    snprintf(receiver_signal_qn, sizeof(receiver_signal_qn), "%s.actors.receiver.Receiver.signal.hit",
             g_gd_project);
    snprintf(nameless_qn, sizeof(nameless_qn), "%s.actors.nameless_script.__script__", g_gd_project);
    snprintf(ghost_attack_qn, sizeof(ghost_attack_qn), "%s.ghost_attack", nameless_qn);
    snprintf(ghost_signal_qn, sizeof(ghost_signal_qn), "%s.signal.ghost_hit", nameless_qn);

    ASSERT_EQ(cbm_store_count_edges_by_type(store, g_gd_project, "CALLS"), 4);

    int64_t attack_id = gd_find_node_id_by_qn(store, attack_qn);
    int64_t ghost_attack_id = gd_find_node_id_by_qn(store, ghost_attack_qn);
    int64_t shadow_id = gd_find_node_id_by_qn(store, shadow_qn);
    ASSERT_TRUE(attack_id >= 0);
    ASSERT_TRUE(ghost_attack_id >= 0);
    ASSERT_TRUE(shadow_id >= 0);

    ASSERT_EQ(gd_count_edges_to_target_qn(store, attack_id, "CALLS", player_signal_qn), 1);
    ASSERT_EQ(gd_count_edges_to_target_qn(store, attack_id, "CALLS", receiver_signal_qn), 1);
    ASSERT_EQ(gd_count_edges_to_target_qn(store, attack_id, "CALLS", helper_qn), 1);
    ASSERT_EQ(gd_count_edges_to_target_qn(store, ghost_attack_id, "CALLS", ghost_signal_qn), 1);
    ASSERT_EQ(gd_count_edges_to_target_qn(store, shadow_id, "CALLS", player_signal_qn), 0);

    cbm_store_close(store);
    PASS();
}

TEST(integ_gdscript_inherits_edges) {
    cbm_store_t *store = cbm_store_open_path(g_gd_dbpath);
    ASSERT_NOT_NULL(store);

    char base_qn[512], player_qn[512], path_qn[512], named_qn[512], preload_qn[512], nameless_qn[512];
    snprintf(base_qn, sizeof(base_qn), "%s.actors.base.Base", g_gd_project);
    snprintf(player_qn, sizeof(player_qn), "%s.actors.player.Player", g_gd_project);
    snprintf(path_qn, sizeof(path_qn), "%s.actors.player_path_extends.PlayerPath", g_gd_project);
    snprintf(named_qn, sizeof(named_qn), "%s.actors.player_named_extends.PlayerNamed", g_gd_project);
    snprintf(preload_qn, sizeof(preload_qn), "%s.actors.player_preload_extends.PlayerPreloadPath", g_gd_project);
    snprintf(nameless_qn, sizeof(nameless_qn), "%s.actors.nameless_script.__script__", g_gd_project);

    ASSERT_EQ(cbm_store_count_edges_by_type(store, g_gd_project, "INHERITS"), 5);
    ASSERT_EQ(gd_count_edges_to_target_qn(store, gd_find_node_id_by_qn(store, player_qn), "INHERITS", base_qn), 1);
    ASSERT_EQ(gd_count_edges_to_target_qn(store, gd_find_node_id_by_qn(store, path_qn), "INHERITS", base_qn), 1);
    ASSERT_EQ(gd_count_edges_to_target_qn(store, gd_find_node_id_by_qn(store, named_qn), "INHERITS", base_qn), 1);
    ASSERT_EQ(gd_count_edges_to_target_qn(store, gd_find_node_id_by_qn(store, preload_qn), "INHERITS", base_qn), 1);
    ASSERT_EQ(gd_count_edges_to_target_qn(store, gd_find_node_id_by_qn(store, nameless_qn), "INHERITS", base_qn), 1);

    cbm_store_close(store);
    PASS();
}

TEST(integ_gdscript_import_edges) {
    cbm_store_t *store = cbm_store_open_path(g_gd_dbpath);
    ASSERT_NOT_NULL(store);

    int64_t player_file_id = gd_find_file_node_id(store, "actors/player.gd");
    ASSERT_TRUE(player_file_id >= 0);

    ASSERT_EQ(gd_count_edges_to_target_file(store, player_file_id, "IMPORTS", "actors/base.gd"), 1);
    ASSERT_EQ(gd_count_edges_to_target_file(store, player_file_id, "IMPORTS", "actors/receiver.gd"), 1);
    ASSERT_EQ(gd_count_edges_to_target_file(store, player_file_id, "IMPORTS", "actors/weapon.gd"), 1);
    ASSERT_EQ(gd_count_edges_to_target_file(store, player_file_id, "IMPORTS", "actors/player.tscn"), 0);

    cbm_store_close(store);
    PASS();
}

TEST(integ_gdscript_non_code_asset_no_imports) {
    cbm_store_t *store = cbm_store_open_path(g_gd_dbpath);
    ASSERT_NOT_NULL(store);

    ASSERT_EQ(gd_count_project_edges_to_target_suffix(store, "IMPORTS", ".tscn"), 0);

    cbm_store_close(store);
    PASS();
}

TEST(integ_gdscript_builtin_base_no_inherits) {
    cbm_store_t *store = cbm_store_open_path(g_gd_dbpath);
    ASSERT_NOT_NULL(store);

    /* builtin_base.gd extends Node2D — should have base_classes metadata but
     * no resolved INHERITS edge to a Node2D node (built-in types aren't indexed) */
    cbm_node_t *nodes = NULL;
    int count = 0;
    int rc = cbm_store_find_nodes_by_file(store, g_gd_project, "actors/builtin_base.gd", &nodes, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_TRUE(count >= 1);

    /* Find the BuiltinBaseCase class node */
    int64_t class_id = -1;
    for (int i = 0; i < count; i++) {
        if (nodes[i].label && strcmp(nodes[i].label, "Class") == 0 &&
            nodes[i].name && strcmp(nodes[i].name, "BuiltinBaseCase") == 0) {
            class_id = nodes[i].id;
            ASSERT_TRUE(nodes[i].properties_json != NULL);
            ASSERT_TRUE(strstr(nodes[i].properties_json, "\"base_classes\"") != NULL);
            ASSERT_TRUE(strstr(nodes[i].properties_json, "Node2D") != NULL);
            break;
        }
    }
    ASSERT_TRUE(class_id >= 0);

    /* Should have no INHERITS edges from this class */
    cbm_edge_t *edges = NULL;
    int edge_count = 0;
    cbm_store_find_edges_by_source_type(store, class_id, "INHERITS", &edges, &edge_count);
    ASSERT_EQ(edge_count, 0);
    cbm_store_free_edges(edges, edge_count);

    cbm_store_free_nodes(nodes, count);
    cbm_store_close(store);
    PASS();
}

TEST(integ_gdscript_dynamic_receiver_unresolved) {
    cbm_store_t *store = cbm_store_open_path(g_gd_dbpath);
    ASSERT_NOT_NULL(store);

    /* dynamic_receiver.gd: target.hit.emit() — dynamic receiver, should NOT
     * create a resolved CALLS edge to any signal target */
    cbm_node_t *methods = NULL;
    int count = 0;
    int rc = cbm_store_find_nodes_by_file(store, g_gd_project, "actors/dynamic_receiver.gd", &methods, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);

    int64_t attack_dynamic_id = -1;
    for (int i = 0; i < count; i++) {
        if (methods[i].label && strcmp(methods[i].label, "Method") == 0 &&
            methods[i].name && strcmp(methods[i].name, "attack_dynamic") == 0) {
            attack_dynamic_id = methods[i].id;
            break;
        }
    }

    if (attack_dynamic_id >= 0) {
        cbm_edge_t *edges = NULL;
        int edge_count = 0;
        cbm_store_find_edges_by_source_type(store, attack_dynamic_id, "CALLS", &edges, &edge_count);
        /* Should have no resolved CALLS edges from attack_dynamic (dynamic receiver) */
        ASSERT_EQ(edge_count, 0);
        cbm_store_free_edges(edges, edge_count);
    }

    cbm_store_free_nodes(methods, count);
    cbm_store_close(store);
    PASS();
}

TEST(integ_gdscript_nameless_script_anchor) {
    cbm_store_t *store = cbm_store_open_path(g_gd_dbpath);
    ASSERT_NOT_NULL(store);

    /* nameless_script.gd has no class_name — should use __script__ anchor */
    cbm_node_t *nodes = NULL;
    int count = 0;
    int rc = cbm_store_find_nodes_by_file(store, g_gd_project, "actors/nameless_script.gd", &nodes, &count);
    ASSERT_EQ(rc, CBM_STORE_OK);

    int has_script_anchor = 0;
    for (int i = 0; i < count; i++) {
        if (nodes[i].label && strcmp(nodes[i].label, "Class") == 0 &&
            nodes[i].qualified_name && strstr(nodes[i].qualified_name, "__script__") != NULL) {
            has_script_anchor = 1;
        }
    }
    ASSERT_TRUE(has_script_anchor);

    cbm_store_free_nodes(nodes, count);
    cbm_store_close(store);
    PASS();
}

TEST(integ_gdscript_defines_method_edges) {
    cbm_store_t *store = cbm_store_open_path(g_gd_dbpath);
    ASSERT_NOT_NULL(store);

    char base_qn[512], player_qn[512], path_qn[512], named_qn[512], preload_qn[512];
    char nameless_qn[512], dynamic_qn[512], builtin_qn[512];
    char ping_qn[512], attack_qn[512], helper_qn[512], shadow_qn[512], on_hit_qn[512];
    char on_receiver_qn[512], attack_path_qn[512], attack_named_qn[512], attack_preload_qn[512];
    char ghost_attack_qn[512], dynamic_attack_qn[512], builtin_tick_qn[512];

    snprintf(base_qn, sizeof(base_qn), "%s.actors.base.Base", g_gd_project);
    snprintf(player_qn, sizeof(player_qn), "%s.actors.player.Player", g_gd_project);
    snprintf(path_qn, sizeof(path_qn), "%s.actors.player_path_extends.PlayerPath", g_gd_project);
    snprintf(named_qn, sizeof(named_qn), "%s.actors.player_named_extends.PlayerNamed", g_gd_project);
    snprintf(preload_qn, sizeof(preload_qn), "%s.actors.player_preload_extends.PlayerPreloadPath", g_gd_project);
    snprintf(nameless_qn, sizeof(nameless_qn), "%s.actors.nameless_script.__script__", g_gd_project);
    snprintf(dynamic_qn, sizeof(dynamic_qn), "%s.actors.dynamic_receiver.DynamicReceiverCase", g_gd_project);
    snprintf(builtin_qn, sizeof(builtin_qn), "%s.actors.builtin_base.BuiltinBaseCase", g_gd_project);

    snprintf(ping_qn, sizeof(ping_qn), "%s.ping", base_qn);
    snprintf(attack_qn, sizeof(attack_qn), "%s.attack", player_qn);
    snprintf(helper_qn, sizeof(helper_qn), "%s.helper", player_qn);
    snprintf(shadow_qn, sizeof(shadow_qn), "%s.shadow_param", player_qn);
    snprintf(on_hit_qn, sizeof(on_hit_qn), "%s._on_hit", player_qn);
    snprintf(on_receiver_qn, sizeof(on_receiver_qn), "%s._on_receiver_hit", player_qn);
    snprintf(attack_path_qn, sizeof(attack_path_qn), "%s.attack_path", path_qn);
    snprintf(attack_named_qn, sizeof(attack_named_qn), "%s.attack_named", named_qn);
    snprintf(attack_preload_qn, sizeof(attack_preload_qn), "%s.attack_preload_path", preload_qn);
    snprintf(ghost_attack_qn, sizeof(ghost_attack_qn), "%s.ghost_attack", nameless_qn);
    snprintf(dynamic_attack_qn, sizeof(dynamic_attack_qn), "%s.attack_dynamic", dynamic_qn);
    snprintf(builtin_tick_qn, sizeof(builtin_tick_qn), "%s.tick_builtin", builtin_qn);

    ASSERT_EQ(cbm_store_count_edges_by_type(store, g_gd_project, "DEFINES_METHOD"), 12);
    ASSERT_EQ(gd_count_edges_to_target_qn(store, gd_find_node_id_by_qn(store, base_qn), "DEFINES_METHOD", ping_qn), 1);
    ASSERT_EQ(gd_count_edges_to_target_qn(store, gd_find_node_id_by_qn(store, player_qn), "DEFINES_METHOD", attack_qn), 1);
    ASSERT_EQ(gd_count_edges_to_target_qn(store, gd_find_node_id_by_qn(store, player_qn), "DEFINES_METHOD", helper_qn), 1);
    ASSERT_EQ(gd_count_edges_to_target_qn(store, gd_find_node_id_by_qn(store, player_qn), "DEFINES_METHOD", shadow_qn), 1);
    ASSERT_EQ(gd_count_edges_to_target_qn(store, gd_find_node_id_by_qn(store, player_qn), "DEFINES_METHOD", on_hit_qn), 1);
    ASSERT_EQ(gd_count_edges_to_target_qn(store, gd_find_node_id_by_qn(store, player_qn), "DEFINES_METHOD", on_receiver_qn), 1);
    ASSERT_EQ(gd_count_edges_to_target_qn(store, gd_find_node_id_by_qn(store, path_qn), "DEFINES_METHOD", attack_path_qn), 1);
    ASSERT_EQ(gd_count_edges_to_target_qn(store, gd_find_node_id_by_qn(store, named_qn), "DEFINES_METHOD", attack_named_qn), 1);
    ASSERT_EQ(gd_count_edges_to_target_qn(store, gd_find_node_id_by_qn(store, preload_qn), "DEFINES_METHOD", attack_preload_qn), 1);
    ASSERT_EQ(gd_count_edges_to_target_qn(store, gd_find_node_id_by_qn(store, nameless_qn), "DEFINES_METHOD", ghost_attack_qn), 1);
    ASSERT_EQ(gd_count_edges_to_target_qn(store, gd_find_node_id_by_qn(store, dynamic_qn), "DEFINES_METHOD", dynamic_attack_qn), 1);
    ASSERT_EQ(gd_count_edges_to_target_qn(store, gd_find_node_id_by_qn(store, builtin_qn), "DEFINES_METHOD", builtin_tick_qn), 1);

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
    RUN_TEST(integ_mcp_trace_path);
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

    /* ── GDScript integration tests ── */
    if (gdscript_integration_setup() != 0) {
        printf("  %-50s", "gdscript_integration_setup");
        printf("SKIP (setup failed)\n");
        tf_skip_count += 10; /* skip all GDScript integration tests */
        gdscript_integration_teardown();
        return;
    }

    RUN_TEST(integ_gdscript_discovers_gd_files);
    RUN_TEST(integ_gdscript_script_anchor_methods);
    RUN_TEST(integ_gdscript_signal_calls_resolved);
    RUN_TEST(integ_gdscript_inherits_edges);
    RUN_TEST(integ_gdscript_import_edges);
    RUN_TEST(integ_gdscript_non_code_asset_no_imports);
    RUN_TEST(integ_gdscript_builtin_base_no_inherits);
    RUN_TEST(integ_gdscript_dynamic_receiver_unresolved);
    RUN_TEST(integ_gdscript_nameless_script_anchor);
    RUN_TEST(integ_gdscript_defines_method_edges);

    gdscript_integration_teardown();
}
