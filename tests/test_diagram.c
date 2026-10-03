/*
 * test_diagram.c — Tests for Native Architecture, Sequence, and Dataflow
 * Diagram Generation.
 */

#include "test_framework.h"
#include <diagram/diagram.h>
#include <store/store.h>
#include <mcp/mcp.h>
#include <yyjson/yyjson.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

/* ── Test 1: Sanitization ────────────────────────────────────────── */

TEST(diagram_sanitize_id_and_label) {
    char out[128];

    /* Basic identifier */
    cbm_diagram_sanitize_id("simple_func", out, sizeof(out));
    ASSERT_STR_EQ(out, "simple_func");

    /* Path and namespace separators */
    cbm_diagram_sanitize_id("src/diagram/emit_mermaid.c::emit_node", out, sizeof(out));
    ASSERT_STR_EQ(out, "src_diagram_emit_mermaid_c__emit_node");

    /* Hyphens, dots, brackets */
    cbm_diagram_sanitize_id("Order<T>::process-item.v1", out, sizeof(out));
    ASSERT_STR_EQ(out, "Order_T___process_item_v1");

    /* Leading digit gets prefixed */
    cbm_diagram_sanitize_id("123abc", out, sizeof(out));
    ASSERT_STR_EQ(out, "n_123abc");

    /* Label escaping quotes and backslashes */
    cbm_diagram_sanitize_label("A \"quoted\" text & backslash \\", out, sizeof(out));
    ASSERT_NOT_NULL(strstr(out, "\\\"quoted\\\""));

    /* NULL / empty safe */
    cbm_diagram_sanitize_id(NULL, out, sizeof(out));
    ASSERT_STR_EQ(out, "node");

    cbm_diagram_sanitize_id("", out, sizeof(out));
    ASSERT_STR_EQ(out, "node");

    PASS();
}

/* ── Test 2: Type and Format Parsers ─────────────────────────────── */

TEST(diagram_type_and_format_parsers) {
    cbm_diagram_type_t type;
    ASSERT_TRUE(cbm_diagram_type_from_string("architecture", &type));
    ASSERT_EQ(type, CBM_DIAGRAM_ARCHITECTURE);

    ASSERT_TRUE(cbm_diagram_type_from_string("arch", &type));
    ASSERT_EQ(type, CBM_DIAGRAM_ARCHITECTURE);

    ASSERT_TRUE(cbm_diagram_type_from_string("sequence", &type));
    ASSERT_EQ(type, CBM_DIAGRAM_SEQUENCE);

    ASSERT_TRUE(cbm_diagram_type_from_string("seq", &type));
    ASSERT_EQ(type, CBM_DIAGRAM_SEQUENCE);

    ASSERT_TRUE(cbm_diagram_type_from_string("dataflow", &type));
    ASSERT_EQ(type, CBM_DIAGRAM_DATAFLOW);

    ASSERT_TRUE(cbm_diagram_type_from_string("flow", &type));
    ASSERT_EQ(type, CBM_DIAGRAM_DATAFLOW);

    ASSERT_TRUE(cbm_diagram_type_from_string("dependencies", &type));
    ASSERT_EQ(type, CBM_DIAGRAM_DEPENDENCIES);

    ASSERT_TRUE(cbm_diagram_type_from_string("deps", &type));
    ASSERT_EQ(type, CBM_DIAGRAM_DEPENDENCIES);

    ASSERT_FALSE(cbm_diagram_type_from_string("invalid_type", &type));
    ASSERT_FALSE(cbm_diagram_type_from_string(NULL, &type));

    cbm_diagram_format_t fmt;
    ASSERT_TRUE(cbm_diagram_format_from_string("mermaid", &fmt));
    ASSERT_EQ(fmt, CBM_DIAGRAM_FORMAT_MERMAID);

    ASSERT_TRUE(cbm_diagram_format_from_string("mmd", &fmt));
    ASSERT_EQ(fmt, CBM_DIAGRAM_FORMAT_MERMAID);

    ASSERT_TRUE(cbm_diagram_format_from_string("dot", &fmt));
    ASSERT_EQ(fmt, CBM_DIAGRAM_FORMAT_DOT);

    ASSERT_TRUE(cbm_diagram_format_from_string("graphviz", &fmt));
    ASSERT_EQ(fmt, CBM_DIAGRAM_FORMAT_DOT);

    ASSERT_TRUE(cbm_diagram_format_from_string("svg", &fmt));
    ASSERT_EQ(fmt, CBM_DIAGRAM_FORMAT_SVG);

    ASSERT_FALSE(cbm_diagram_format_from_string("unknown_format", &fmt));
    ASSERT_FALSE(cbm_diagram_format_from_string(NULL, &fmt));

    PASS();
}

/* ── Test 3: Call Sequence Diagram Generation ────────────────────── */

TEST(diagram_sequence_query_and_emit) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "seq_proj", "/tmp/seq_proj");

    cbm_node_t fn1 = {
        .project = "seq_proj",
        .label = "Function",
        .name = "handle_request",
        .qualified_name = "server.handle_request",
        .file_path = "src/server.c",
        .start_line = 10,
    };
    int64_t id1 = cbm_store_upsert_node(s, &fn1);

    cbm_node_t fn2 = {
        .project = "seq_proj",
        .label = "Function",
        .name = "authenticate",
        .qualified_name = "auth.authenticate",
        .file_path = "src/auth.c",
        .start_line = 20,
    };
    int64_t id2 = cbm_store_upsert_node(s, &fn2);

    cbm_node_t fn3 = {
        .project = "seq_proj",
        .label = "Function",
        .name = "fetch_data",
        .qualified_name = "db.fetch_data",
        .file_path = "src/db.c",
        .start_line = 30,
    };
    int64_t id3 = cbm_store_upsert_node(s, &fn3);

    /* handle_request calls authenticate at line 15 */
    cbm_edge_t e1 = {
        .project = "seq_proj",
        .source_id = id1,
        .target_id = id2,
        .type = "CALLS",
        .properties_json = "{\"line\":15}",
    };
    cbm_store_insert_edge(s, &e1);

    /* handle_request calls fetch_data at line 25 */
    cbm_edge_t e2 = {
        .project = "seq_proj",
        .source_id = id1,
        .target_id = id3,
        .type = "CALLS",
        .properties_json = "{\"line\":25}",
    };
    cbm_store_insert_edge(s, &e2);

    /* Test Sequence options */
    cbm_diagram_options_t opts = {
        .type = CBM_DIAGRAM_SEQUENCE,
        .format = CBM_DIAGRAM_FORMAT_MERMAID,
        .entry_point = "handle_request",
        .max_depth = 3,
        .project = "seq_proj",
    };

    cbm_diagram_result_t res = {0};
    int rc = cbm_diagram_generate(s, &opts, &res);
    ASSERT_EQ(rc, 0);
    ASSERT_NOT_NULL(res.content);
    ASSERT_GT((int)strlen(res.content), 20);

    /* Verify Mermaid syntax elements */
    ASSERT_NOT_NULL(strstr(res.content, "sequenceDiagram"));
    ASSERT_NOT_NULL(strstr(res.content, "autonumber"));
    ASSERT_NOT_NULL(strstr(res.content, "participant"));
    ASSERT_NOT_NULL(strstr(res.content, "authenticate"));
    ASSERT_NOT_NULL(strstr(res.content, "fetch_data"));
    ASSERT_NOT_NULL(strstr(res.content, "->>"));

    cbm_diagram_result_free(&res);

    /* Test SVG output for sequence diagram */
    opts.format = CBM_DIAGRAM_FORMAT_SVG;
    rc = cbm_diagram_generate(s, &opts, &res);
    ASSERT_EQ(rc, 0);
    ASSERT_NOT_NULL(res.content);
    ASSERT_NOT_NULL(strstr(res.content, "<svg"));
    ASSERT_NOT_NULL(strstr(res.content, "</svg>"));
    ASSERT_NOT_NULL(strstr(res.content, "authenticate"));

    cbm_diagram_result_free(&res);
    cbm_store_close(s);
    PASS();
}

/* ── Test 4: Architecture Diagram Generation ─────────────────────── */

TEST(diagram_architecture_query_and_emit) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "arch_proj", "/tmp/arch_proj");

    cbm_node_t pkg1 = {
        .project = "arch_proj",
        .label = "Package",
        .name = "frontend",
        .qualified_name = "arch_proj.frontend",
    };
    int64_t id_p1 = cbm_store_upsert_node(s, &pkg1);

    cbm_node_t pkg2 = {
        .project = "arch_proj",
        .label = "Package",
        .name = "backend",
        .qualified_name = "arch_proj.backend",
    };
    int64_t id_p2 = cbm_store_upsert_node(s, &pkg2);

    cbm_node_t f1 = {
        .project = "arch_proj",
        .label = "File",
        .name = "ui.ts",
        .qualified_name = "arch_proj.frontend.ui",
        .file_path = "frontend/ui.ts",
    };
    cbm_store_upsert_node(s, &f1);

    cbm_node_t f2 = {
        .project = "arch_proj",
        .label = "File",
        .name = "api.go",
        .qualified_name = "arch_proj.backend.api",
        .file_path = "backend/api.go",
    };
    cbm_store_upsert_node(s, &f2);

    cbm_edge_t imp = {
        .project = "arch_proj",
        .source_id = id_p1,
        .target_id = id_p2,
        .type = "IMPORTS",
    };
    cbm_store_insert_edge(s, &imp);

    cbm_diagram_options_t opts = {
        .type = CBM_DIAGRAM_ARCHITECTURE,
        .format = CBM_DIAGRAM_FORMAT_MERMAID,
        .project = "arch_proj",
    };

    cbm_diagram_result_t res = {0};
    int rc = cbm_diagram_generate(s, &opts, &res);
    ASSERT_EQ(rc, 0);
    ASSERT_NOT_NULL(res.content);
    ASSERT_NOT_NULL(strstr(res.content, "graph TD"));
    ASSERT_NOT_NULL(strstr(res.content, "frontend"));
    ASSERT_NOT_NULL(strstr(res.content, "backend"));

    cbm_diagram_result_free(&res);

    /* Test DOT format */
    opts.format = CBM_DIAGRAM_FORMAT_DOT;
    rc = cbm_diagram_generate(s, &opts, &res);
    ASSERT_EQ(rc, 0);
    ASSERT_NOT_NULL(res.content);
    ASSERT_NOT_NULL(strstr(res.content, "digraph G {"));
    ASSERT_NOT_NULL(strstr(res.content, "->"));

    cbm_diagram_result_free(&res);

    /* Test SVG format */
    opts.format = CBM_DIAGRAM_FORMAT_SVG;
    rc = cbm_diagram_generate(s, &opts, &res);
    ASSERT_EQ(rc, 0);
    ASSERT_NOT_NULL(res.content);
    ASSERT_NOT_NULL(strstr(res.content, "<svg"));
    ASSERT_NOT_NULL(strstr(res.content, "</svg>"));

    cbm_diagram_result_free(&res);
    cbm_store_close(s);
    PASS();
}

/* ── Test 5: Dataflow Diagram Generation ─────────────────────────── */

TEST(diagram_dataflow_query_and_emit) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "flow_proj", "/tmp/flow_proj");

    cbm_node_t route = {
        .project = "flow_proj",
        .label = "Route",
        .name = "POST /api/orders",
        .qualified_name = "routes.post_orders",
        .file_path = "src/routes.go",
    };
    int64_t id_r = cbm_store_upsert_node(s, &route);

    cbm_node_t handler = {
        .project = "flow_proj",
        .label = "Function",
        .name = "OrderHandler",
        .qualified_name = "orders.OrderHandler",
        .file_path = "src/orders.go",
    };
    int64_t id_h = cbm_store_upsert_node(s, &handler);

    cbm_node_t table = {
        .project = "flow_proj",
        .label = "Table",
        .name = "orders",
        .qualified_name = "db.tables.orders",
        .file_path = "migrations/schema.sql",
    };
    int64_t id_t = cbm_store_upsert_node(s, &table);

    cbm_edge_t e_handles = {
        .project = "flow_proj",
        .source_id = id_r,
        .target_id = id_h,
        .type = "HANDLES",
    };
    cbm_store_insert_edge(s, &e_handles);

    cbm_edge_t e_writes = {
        .project = "flow_proj",
        .source_id = id_h,
        .target_id = id_t,
        .type = "WRITES",
    };
    cbm_store_insert_edge(s, &e_writes);

    cbm_diagram_options_t opts = {
        .type = CBM_DIAGRAM_DATAFLOW,
        .format = CBM_DIAGRAM_FORMAT_MERMAID,
        .entry_point = "POST /api/orders",
        .project = "flow_proj",
    };

    cbm_diagram_result_t res = {0};
    int rc = cbm_diagram_generate(s, &opts, &res);
    ASSERT_EQ(rc, 0);
    ASSERT_NOT_NULL(res.content);
    ASSERT_NOT_NULL(strstr(res.content, "flowchart LR"));
    ASSERT_NOT_NULL(strstr(res.content, "Ingress"));
    ASSERT_NOT_NULL(strstr(res.content, "OrderHandler"));
    ASSERT_NOT_NULL(strstr(res.content, "orders"));

    cbm_diagram_result_free(&res);

    /* Test DOT format */
    opts.format = CBM_DIAGRAM_FORMAT_DOT;
    rc = cbm_diagram_generate(s, &opts, &res);
    ASSERT_EQ(rc, 0);
    ASSERT_NOT_NULL(res.content);
    ASSERT_NOT_NULL(strstr(res.content, "rankdir=LR"));

    cbm_diagram_result_free(&res);
    cbm_store_close(s);
    PASS();
}

/* ── Test 6: Dependencies Diagram Generation ─────────────────────── */

TEST(diagram_dependencies_query_and_emit) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "deps_proj", "/tmp/deps_proj");

    cbm_node_t p1 = {.project = "deps_proj", .label = "Package", .name = "core", .qualified_name = "pkg.core"};
    int64_t id1 = cbm_store_upsert_node(s, &p1);

    cbm_node_t p2 = {.project = "deps_proj", .label = "Package", .name = "util", .qualified_name = "pkg.util"};
    int64_t id2 = cbm_store_upsert_node(s, &p2);

    cbm_edge_t imp = {.project = "deps_proj", .source_id = id1, .target_id = id2, .type = "IMPORTS"};
    cbm_store_insert_edge(s, &imp);

    cbm_diagram_options_t opts = {
        .type = CBM_DIAGRAM_DEPENDENCIES,
        .format = CBM_DIAGRAM_FORMAT_MERMAID,
        .project = "deps_proj",
    };

    cbm_diagram_result_t res = {0};
    int rc = cbm_diagram_generate(s, &opts, &res);
    ASSERT_EQ(rc, 0);
    ASSERT_NOT_NULL(res.content);
    ASSERT_NOT_NULL(strstr(res.content, "graph TD"));
    ASSERT_NOT_NULL(strstr(res.content, "core"));
    ASSERT_NOT_NULL(strstr(res.content, "util"));

    cbm_diagram_result_free(&res);
    cbm_store_close(s);
    PASS();
}

/* ── Test 7: MCP export_diagram Tool ─────────────────────────────── */

TEST(diagram_mcp_tool_export_diagram) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_store_t *s = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(s);
    cbm_mcp_server_set_project(srv, "mcp_diag");
    cbm_store_upsert_project(s, "mcp_diag", "/tmp/mcp_diag");

    cbm_node_t pkg = {.project = "mcp_diag", .label = "Package", .name = "main", .qualified_name = "mcp_diag.main"};
    cbm_store_upsert_node(s, &pkg);

    /* Call export_diagram tool with valid args */
    const char *args = "{\"type\":\"architecture\",\"format\":\"mermaid\",\"project\":\"mcp_diag\"}";
    char *result_json = cbm_mcp_handle_tool(srv, "export_diagram", args);
    ASSERT_NOT_NULL(result_json);

    yyjson_doc *doc = yyjson_read(result_json, strlen(result_json), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *root = yyjson_doc_get_root(doc);
    ASSERT_NOT_NULL(root);

    yyjson_val *content_arr = yyjson_obj_get(root, "content");
    ASSERT_NOT_NULL(content_arr);
    ASSERT_GT(yyjson_arr_size(content_arr), 0);

    yyjson_val *first_item = yyjson_arr_get(content_arr, 0);
    ASSERT_NOT_NULL(first_item);
    yyjson_val *text_val = yyjson_obj_get(first_item, "text");
    ASSERT_NOT_NULL(text_val);

    const char *text = yyjson_get_str(text_val);
    ASSERT_NOT_NULL(text);
    ASSERT_NOT_NULL(strstr(text, "diagram_type"));
    ASSERT_NOT_NULL(strstr(text, "architecture"));
    ASSERT_NOT_NULL(strstr(text, "graph TD"));

    yyjson_doc_free(doc);
    free(result_json);

    /* Call export_diagram with invalid type */
    char *err_res = cbm_mcp_handle_tool(srv, "export_diagram", "{\"type\":\"invalid\"}");
    ASSERT_NOT_NULL(err_res);
    ASSERT_NOT_NULL(strstr(err_res, "isError"));
    free(err_res);

    cbm_mcp_server_free(srv);
    PASS();
}

/* ── Suite Declaration ───────────────────────────────────────────── */

SUITE(diagram) {
    RUN_TEST(diagram_sanitize_id_and_label);
    RUN_TEST(diagram_type_and_format_parsers);
    RUN_TEST(diagram_sequence_query_and_emit);
    RUN_TEST(diagram_architecture_query_and_emit);
    RUN_TEST(diagram_dataflow_query_and_emit);
    RUN_TEST(diagram_dependencies_query_and_emit);
    RUN_TEST(diagram_mcp_tool_export_diagram);
}
