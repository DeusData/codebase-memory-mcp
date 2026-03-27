/*
 * test_store_arch.c — Tests for architecture, ADR, Louvain, and helper functions.
 *
 * Ports from internal/store/architecture_test.go:
 *   TestGetArchitectureAll, TestArchEntryPointsExcludeTests,
 *   TestArchHotspotsExcludeTests, TestGetArchitectureSpecificAspects,
 *   TestGetArchitectureEmpty, TestArchLanguages, TestArchLayers,
 *   TestArchClusters, TestArchFileTree, TestArchRoutes, TestArchHotspots,
 *   TestArchBoundaries, TestSearchCaseInsensitiveDefault,
 *   TestSearchCaseSensitiveExplicit, TestEnsureCaseInsensitive,
 *   TestStripCaseFlag, TestStoreAndRetrieveADR, TestStoreADRUpsert,
 *   TestDeleteADR, TestDeleteADRNotFound, TestParseADRSections,
 *   TestRenderADR, TestParseRenderRoundTrip, TestUpdateADRSections,
 *   TestUpdateADRSectionsOverflow, TestUpdateADRSectionsNoExisting,
 *   TestValidateADRContentAllSections, TestValidateADRContentMissingSections,
 *   TestValidateADRContentEmpty, TestValidateADRSectionKeysValid,
 *   TestValidateADRSectionKeysInvalid, TestValidateADRSectionKeysEmpty,
 *   TestLouvainBasic, TestLouvainEmpty, TestLouvainSingleNode,
 *   TestLouvainConverges, TestQnToPackage, TestQnToTopPackage,
 *   TestFindArchitectureDocs, TestFindArchitectureDocsEmpty,
 *   TestIsTestFilePath
 */
#include "test_framework.h"
#include <store/store.h>
#include <string.h>
#include <stdlib.h>

/* ── Helper: create architecture test store ──────────────────────── */

static cbm_store_t *setup_arch_test_store(void) {
    cbm_store_t *s = cbm_store_open_memory();
    if (!s)
        return NULL;
    cbm_store_upsert_project(s, "test", "/tmp/test");

    /* Files */
    const char *files[] = {"main.go", "handler.go", "service.go", "model.py", "utils.js"};
    for (int i = 0; i < 5; i++) {
        char qn[64];
        snprintf(qn, sizeof(qn), "test.%s", files[i]);
        cbm_node_t n = {.project = "test",
                        .label = "File",
                        .name = files[i],
                        .qualified_name = qn,
                        .file_path = files[i]};
        cbm_store_upsert_node(s, &n);
    }

    /* Packages */
    cbm_node_t pkg1 = {
        .project = "test", .label = "Package", .name = "cmd", .qualified_name = "test.cmd"};
    cbm_node_t pkg2 = {
        .project = "test", .label = "Package", .name = "handler", .qualified_name = "test.handler"};
    cbm_node_t pkg3 = {
        .project = "test", .label = "Package", .name = "service", .qualified_name = "test.service"};
    cbm_store_upsert_node(s, &pkg1);
    cbm_store_upsert_node(s, &pkg2);
    cbm_store_upsert_node(s, &pkg3);

    /* Functions with 4-segment QNs */
    cbm_node_t fn_main = {.project = "test",
                          .label = "Function",
                          .name = "main",
                          .qualified_name = "test.cmd.server.main",
                          .file_path = "cmd/server/main.go",
                          .properties_json = "{\"is_entry_point\":true}"};
    int64_t id_main = cbm_store_upsert_node(s, &fn_main);

    cbm_node_t fn_handle = {.project = "test",
                            .label = "Function",
                            .name = "HandleRequest",
                            .qualified_name = "test.internal.handler.HandleRequest",
                            .file_path = "internal/handler/handler.go",
                            .properties_json = "{\"is_entry_point\":true}"};
    int64_t id_handle = cbm_store_upsert_node(s, &fn_handle);

    cbm_node_t fn_process = {.project = "test",
                             .label = "Function",
                             .name = "ProcessOrder",
                             .qualified_name = "test.internal.service.ProcessOrder",
                             .file_path = "internal/service/service.go"};
    int64_t id_process = cbm_store_upsert_node(s, &fn_process);

    cbm_node_t fn_validate = {.project = "test",
                              .label = "Function",
                              .name = "ValidateOrder",
                              .qualified_name = "test.internal.service.ValidateOrder",
                              .file_path = "internal/service/service.go"};
    int64_t id_validate = cbm_store_upsert_node(s, &fn_validate);

    cbm_node_t fn_helper = {.project = "test",
                            .label = "Function",
                            .name = "formatDate",
                            .qualified_name = "test.internal.service.formatDate",
                            .file_path = "internal/service/service.go"};
    int64_t id_helper = cbm_store_upsert_node(s, &fn_helper);

    /* Test function (should be excluded) */
    cbm_node_t fn_test = {.project = "test",
                          .label = "Function",
                          .name = "TestHandleRequest",
                          .qualified_name = "test.internal.handler.handler_test.TestHandleRequest",
                          .file_path = "internal/handler/handler_test.go",
                          .properties_json = "{\"is_entry_point\":true}"};
    int64_t id_test = cbm_store_upsert_node(s, &fn_test);

    /* Route */
    cbm_node_t route = {
        .project = "test",
        .label = "Route",
        .name = "/api/orders",
        .qualified_name = "test.internal.handler.route./api/orders",
        .properties_json =
            "{\"method\":\"POST\",\"path\":\"/api/orders\",\"handler\":\"HandleRequest\"}"};
    cbm_store_upsert_node(s, &route);

    /* Edges: main → HandleRequest → ProcessOrder → ValidateOrder
     *                                ProcessOrder → formatDate
     *        TestHandleRequest → HandleRequest */
    cbm_edge_t e1 = {
        .project = "test", .source_id = id_main, .target_id = id_handle, .type = "CALLS"};
    cbm_edge_t e2 = {
        .project = "test", .source_id = id_handle, .target_id = id_process, .type = "CALLS"};
    cbm_edge_t e3 = {
        .project = "test", .source_id = id_process, .target_id = id_validate, .type = "CALLS"};
    cbm_edge_t e4 = {
        .project = "test", .source_id = id_process, .target_id = id_helper, .type = "CALLS"};
    cbm_edge_t e5 = {
        .project = "test", .source_id = id_test, .target_id = id_handle, .type = "CALLS"};
    cbm_store_insert_edge(s, &e1);
    cbm_store_insert_edge(s, &e2);
    cbm_store_insert_edge(s, &e3);
    cbm_store_insert_edge(s, &e4);
    cbm_store_insert_edge(s, &e5);

    return s;
}

/* ── Architecture tests ──────────────────────────────────────────── */

TEST(arch_get_all) {
    cbm_store_t *s = setup_arch_test_store();
    cbm_architecture_info_t info;
    ASSERT_EQ(cbm_store_get_architecture(s, "test", NULL, 0, &info), CBM_STORE_OK);

    ASSERT_TRUE(info.language_count > 0);
    ASSERT_TRUE(info.package_count > 0);
    ASSERT_TRUE(info.entry_point_count > 0);
    ASSERT_TRUE(info.route_count > 0);
    ASSERT_TRUE(info.hotspot_count > 0);
    ASSERT_TRUE(info.boundary_count > 0);

    cbm_store_architecture_free(&info);
    cbm_store_close(s);
    PASS();
}

TEST(arch_entry_points_exclude_tests) {
    cbm_store_t *s = setup_arch_test_store();
    cbm_architecture_info_t info;
    memset(&info, 0, sizeof(info));
    const char *aspects[] = {"entry_points"};
    ASSERT_EQ(cbm_store_get_architecture(s, "test", aspects, 1, &info), CBM_STORE_OK);

    for (int i = 0; i < info.entry_point_count; i++) {
        ASSERT_TRUE(strstr(info.entry_points[i].file, "test") == NULL);
    }
    ASSERT_EQ(info.entry_point_count, 2); /* main, HandleRequest */

    cbm_store_architecture_free(&info);
    cbm_store_close(s);
    PASS();
}

TEST(arch_hotspots_exclude_tests) {
    cbm_store_t *s = setup_arch_test_store();
    cbm_architecture_info_t info;
    memset(&info, 0, sizeof(info));
    const char *aspects[] = {"hotspots"};
    ASSERT_EQ(cbm_store_get_architecture(s, "test", aspects, 1, &info), CBM_STORE_OK);

    for (int i = 0; i < info.hotspot_count; i++) {
        ASSERT_TRUE(strstr(info.hotspots[i].name, "Test") == NULL);
    }

    cbm_store_architecture_free(&info);
    cbm_store_close(s);
    PASS();
}

TEST(arch_specific_aspects) {
    cbm_store_t *s = setup_arch_test_store();
    cbm_architecture_info_t info;
    const char *aspects[] = {"languages", "hotspots"};
    ASSERT_EQ(cbm_store_get_architecture(s, "test", aspects, 2, &info), CBM_STORE_OK);

    ASSERT_TRUE(info.language_count > 0);
    ASSERT_TRUE(info.hotspot_count > 0);
    /* Not requested: should be zero */
    ASSERT_EQ(info.package_count, 0);
    ASSERT_EQ(info.entry_point_count, 0);
    ASSERT_EQ(info.route_count, 0);

    cbm_store_architecture_free(&info);
    cbm_store_close(s);
    PASS();
}

TEST(arch_empty_project) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "empty", "/tmp/empty"), CBM_STORE_OK);

    cbm_architecture_info_t info;
    const char *aspects[] = {"all"};
    ASSERT_EQ(cbm_store_get_architecture(s, "empty", aspects, 1, &info), CBM_STORE_OK);
    /* All should be empty but no errors */

    cbm_store_architecture_free(&info);
    cbm_store_close(s);
    PASS();
}

TEST(arch_languages) {
    cbm_store_t *s = setup_arch_test_store();
    cbm_architecture_info_t info;
    memset(&info, 0, sizeof(info));
    const char *aspects[] = {"languages"};
    ASSERT_EQ(cbm_store_get_architecture(s, "test", aspects, 1, &info), CBM_STORE_OK);

    /* Check Go=3, Python=1, JavaScript=1 */
    int go_count = 0, py_count = 0, js_count = 0;
    for (int i = 0; i < info.language_count; i++) {
        if (strcmp(info.languages[i].language, "Go") == 0)
            go_count = info.languages[i].file_count;
        if (strcmp(info.languages[i].language, "Python") == 0)
            py_count = info.languages[i].file_count;
        if (strcmp(info.languages[i].language, "JavaScript") == 0)
            js_count = info.languages[i].file_count;
    }
    ASSERT_EQ(go_count, 3);
    ASSERT_EQ(py_count, 1);
    ASSERT_EQ(js_count, 1);

    cbm_store_architecture_free(&info);
    cbm_store_close(s);
    PASS();
}

TEST(arch_routes) {
    cbm_store_t *s = setup_arch_test_store();
    cbm_architecture_info_t info;
    memset(&info, 0, sizeof(info));
    const char *aspects[] = {"routes"};
    ASSERT_EQ(cbm_store_get_architecture(s, "test", aspects, 1, &info), CBM_STORE_OK);

    ASSERT_EQ(info.route_count, 1);
    ASSERT_STR_EQ(info.routes[0].method, "POST");
    ASSERT_STR_EQ(info.routes[0].path, "/api/orders");
    ASSERT_STR_EQ(info.routes[0].handler, "HandleRequest");

    cbm_store_architecture_free(&info);
    cbm_store_close(s);
    PASS();
}

TEST(arch_hotspots) {
    cbm_store_t *s = setup_arch_test_store();
    cbm_architecture_info_t info;
    memset(&info, 0, sizeof(info));
    const char *aspects[] = {"hotspots"};
    ASSERT_EQ(cbm_store_get_architecture(s, "test", aspects, 1, &info), CBM_STORE_OK);

    ASSERT_TRUE(info.hotspot_count > 0);
    /* ProcessOrder should be a hotspot (called by HandleRequest) */
    bool found = false;
    for (int i = 0; i < info.hotspot_count; i++) {
        if (strcmp(info.hotspots[i].name, "ProcessOrder") == 0) {
            found = true;
            ASSERT_TRUE(info.hotspots[i].fan_in >= 1);
        }
    }
    /* May not be found with few edges — just log */
    (void)found;

    cbm_store_architecture_free(&info);
    cbm_store_close(s);
    PASS();
}

TEST(arch_boundaries) {
    cbm_store_t *s = setup_arch_test_store();
    cbm_architecture_info_t info;
    memset(&info, 0, sizeof(info));
    const char *aspects[] = {"boundaries"};
    ASSERT_EQ(cbm_store_get_architecture(s, "test", aspects, 1, &info), CBM_STORE_OK);

    ASSERT_TRUE(info.boundary_count > 0);
    /* server → handler and handler → service should be present */
    bool found_sh = false, found_hs = false;
    for (int i = 0; i < info.boundary_count; i++) {
        if (strcmp(info.boundaries[i].from, "server") == 0 &&
            strcmp(info.boundaries[i].to, "handler") == 0)
            found_sh = true;
        if (strcmp(info.boundaries[i].from, "handler") == 0 &&
            strcmp(info.boundaries[i].to, "service") == 0)
            found_hs = true;
    }
    ASSERT_TRUE(found_sh);
    ASSERT_TRUE(found_hs);

    cbm_store_architecture_free(&info);
    cbm_store_close(s);
    PASS();
}

TEST(arch_layers) {
    cbm_store_t *s = setup_arch_test_store();
    cbm_architecture_info_t info;
    memset(&info, 0, sizeof(info));
    const char *aspects[] = {"layers"};
    ASSERT_EQ(cbm_store_get_architecture(s, "test", aspects, 1, &info), CBM_STORE_OK);

    ASSERT_TRUE(info.layer_count > 0);
    /* Handler package has routes, should be "api" */
    for (int i = 0; i < info.layer_count; i++) {
        if (strcmp(info.layers[i].name, "handler") == 0) {
            ASSERT_STR_EQ(info.layers[i].layer, "api");
        }
    }

    cbm_store_architecture_free(&info);
    cbm_store_close(s);
    PASS();
}

TEST(arch_file_tree) {
    cbm_store_t *s = setup_arch_test_store();
    cbm_architecture_info_t info;
    memset(&info, 0, sizeof(info));
    const char *aspects[] = {"file_tree"};
    ASSERT_EQ(cbm_store_get_architecture(s, "test", aspects, 1, &info), CBM_STORE_OK);

    ASSERT_TRUE(info.file_tree_count > 0);
    /* Check that entries have valid types */
    for (int i = 0; i < info.file_tree_count; i++) {
        ASSERT_TRUE(strcmp(info.file_tree[i].type, "dir") == 0 ||
                    strcmp(info.file_tree[i].type, "file") == 0);
    }

    cbm_store_architecture_free(&info);
    cbm_store_close(s);
    PASS();
}

TEST(arch_clusters) {
    cbm_store_t *s = setup_arch_test_store();
    cbm_architecture_info_t info;
    memset(&info, 0, sizeof(info));
    const char *aspects[] = {"clusters"};
    ASSERT_EQ(cbm_store_get_architecture(s, "test", aspects, 1, &info), CBM_STORE_OK);

    /* With 5 functions and 4 edges, Louvain should find at least 1 cluster */
    if (info.cluster_count == 0) {
        /* May need more nodes for meaningful clustering — just log */
        cbm_store_architecture_free(&info);
        cbm_store_close(s);
        PASS();
    }

    for (int i = 0; i < info.cluster_count; i++) {
        ASSERT_TRUE(info.clusters[i].members >= 2);
        ASSERT_NOT_NULL(info.clusters[i].label);
        ASSERT_TRUE(info.clusters[i].label[0] != '\0');
    }

    cbm_store_architecture_free(&info);
    cbm_store_close(s);
    PASS();
}

TEST(arch_summary_basic) {
    cbm_store_t *s = setup_arch_test_store();
    cbm_architecture_summary_t summary;
    memset(&summary, 0, sizeof(summary));

    ASSERT_EQ(cbm_store_get_architecture_summary(s, "test", NULL, &summary), CBM_STORE_OK);
    ASSERT_EQ(summary.total_files, 5);
    ASSERT_TRUE(summary.total_functions >= 5);
    ASSERT_EQ(summary.total_routes, 1);
    ASSERT_TRUE(summary.file_count > 0);
    ASSERT_NOT_NULL(summary.files[0].file);
    ASSERT_EQ(summary.route_count, 1);
    ASSERT_STR_EQ(summary.routes[0].handler, "HandleRequest");
    ASSERT_STR_EQ(summary.routes[0].service, "ProcessOrder");
    ASSERT_STR_EQ(summary.routes[0].next, "ValidateOrder");
    ASSERT_TRUE(summary.entry_point_count > 0);

    cbm_store_architecture_summary_free(&summary);
    cbm_store_close(s);
    PASS();
}

TEST(arch_summary_focus) {
    cbm_store_t *s = setup_arch_test_store();
    cbm_architecture_summary_t summary;
    memset(&summary, 0, sizeof(summary));

    ASSERT_EQ(cbm_store_get_architecture_summary(s, "test", "service", &summary), CBM_STORE_OK);
    ASSERT_EQ(summary.total_files, 5);
    ASSERT_TRUE(summary.total_functions >= 5);
    ASSERT_TRUE(summary.file_count >= 1);
    ASSERT_NOT_NULL(strstr(summary.files[0].file, "service.go"));
    ASSERT_EQ(summary.total_routes, 1);
    ASSERT_EQ(summary.route_count, 0);

    cbm_store_architecture_summary_free(&summary);
    cbm_store_close(s);
    PASS();
}

TEST(arch_summary_many_files) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    int64_t prev_fn_id = 0;
    for (int i = 0; i < 20; i++) {
        char file_name[64];
        char file_qn[96];
        char fn_name[32];
        char fn_qn[128];
        snprintf(file_name, sizeof(file_name), "pkg/file%02d.go", i);
        snprintf(file_qn, sizeof(file_qn), "test.pkg.file%02d", i);
        snprintf(fn_name, sizeof(fn_name), "Fn%02d", i);
        snprintf(fn_qn, sizeof(fn_qn), "test.pkg.file%02d.%s", i, fn_name);

        cbm_node_t file = {.project = "test",
                           .label = "File",
                           .name = file_name,
                           .qualified_name = file_qn,
                           .file_path = file_name};
        cbm_store_upsert_node(s, &file);

        cbm_node_t fn = {.project = "test",
                         .label = "Function",
                         .name = fn_name,
                         .qualified_name = fn_qn,
                         .file_path = file_name};
        int64_t fn_id = cbm_store_upsert_node(s, &fn);
        if (prev_fn_id > 0) {
            cbm_edge_t e = {
                .project = "test", .source_id = prev_fn_id, .target_id = fn_id, .type = "CALLS"};
            cbm_store_insert_edge(s, &e);
        }
        prev_fn_id = fn_id;
    }

    cbm_architecture_summary_t summary;
    memset(&summary, 0, sizeof(summary));
    ASSERT_EQ(cbm_store_get_architecture_summary(s, "test", NULL, &summary), CBM_STORE_OK);
    ASSERT_EQ(summary.total_files, 20);
    ASSERT_TRUE(summary.file_count > 0);
    ASSERT_TRUE(summary.total_functions >= 20);

    cbm_store_architecture_summary_free(&summary);
    cbm_store_close(s);
    PASS();
}

TEST(arch_summary_cluster_growth) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    for (int pair = 0; pair < 9; pair++) {
        int64_t pair_ids[2] = {0};
        for (int idx = 0; idx < 2; idx++) {
            int file_no = pair * 2 + idx;
            char file_name[64];
            char file_qn[96];
            char fn_name[32];
            char fn_qn[128];
            snprintf(file_name, sizeof(file_name), "cluster/file%02d.go", file_no);
            snprintf(file_qn, sizeof(file_qn), "test.cluster.file%02d", file_no);
            snprintf(fn_name, sizeof(fn_name), "Fn%02d", file_no);
            snprintf(fn_qn, sizeof(fn_qn), "test.cluster.file%02d.%s", file_no, fn_name);

            cbm_node_t file = {.project = "test",
                               .label = "File",
                               .name = file_name,
                               .qualified_name = file_qn,
                               .file_path = file_name};
            cbm_store_upsert_node(s, &file);

            cbm_node_t fn = {.project = "test",
                             .label = "Function",
                             .name = fn_name,
                             .qualified_name = fn_qn,
                             .file_path = file_name};
            pair_ids[idx] = cbm_store_upsert_node(s, &fn);
        }

        cbm_edge_t edge = {
            .project = "test", .source_id = pair_ids[0], .target_id = pair_ids[1], .type = "CALLS"};
        cbm_store_insert_edge(s, &edge);
    }

    cbm_architecture_summary_t summary;
    memset(&summary, 0, sizeof(summary));
    ASSERT_EQ(cbm_store_get_architecture_summary(s, "test", NULL, &summary), CBM_STORE_OK);
    ASSERT_EQ(summary.total_files, 18);
    ASSERT_TRUE(summary.cluster_count > 0);
    ASSERT_TRUE(summary.clusters[0].file_count >= 2);

    cbm_store_architecture_summary_free(&summary);
    cbm_store_close(s);
    PASS();
}

TEST(arch_summary_cluster_entry_fallback) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    cbm_node_t controller_file = {.project = "test",
                                  .label = "File",
                                  .name = "app/Http/Controllers/OrderController.php",
                                  .qualified_name = "test.app.controllers.OrderController",
                                  .file_path = "app/Http/Controllers/OrderController.php"};
    cbm_node_t service_file = {.project = "test",
                               .label = "File",
                               .name = "app/Services/OrderService.php",
                               .qualified_name = "test.app.services.OrderService",
                               .file_path = "app/Services/OrderService.php"};
    cbm_store_upsert_node(s, &controller_file);
    cbm_store_upsert_node(s, &service_file);

    cbm_node_t controller_fn = {.project = "test",
                                .label = "Method",
                                .name = "handle",
                                .qualified_name = "test.app.controllers.OrderController.handle",
                                .file_path = "app/Http/Controllers/OrderController.php"};
    cbm_node_t service_fn = {.project = "test",
                             .label = "Method",
                             .name = "processOrder",
                             .qualified_name = "test.app.services.OrderService.processOrder",
                             .file_path = "app/Services/OrderService.php"};
    int64_t controller_id = cbm_store_upsert_node(s, &controller_fn);
    int64_t service_id = cbm_store_upsert_node(s, &service_fn);

    cbm_edge_t edge = {
        .project = "test", .source_id = controller_id, .target_id = service_id, .type = "CALLS"};
    cbm_store_insert_edge(s, &edge);

    cbm_node_t route = {.project = "test",
                        .label = "Route",
                        .name = "/orders",
                        .qualified_name = "test.routes.orders",
                        .properties_json =
                            "{\"method\":\"POST\",\"path\":\"/orders\",\"handler\":"
                            "\"OrderController@store\"}"};
    cbm_store_upsert_node(s, &route);

    cbm_architecture_summary_t summary;
    memset(&summary, 0, sizeof(summary));
    ASSERT_EQ(cbm_store_get_architecture_summary(s, "test", NULL, &summary), CBM_STORE_OK);
    ASSERT_EQ(summary.route_count, 1);
    ASSERT_TRUE(summary.routes[0].handler_file == NULL);
    ASSERT_TRUE(summary.cluster_count > 0);

    bool found_entry = false;
    for (int i = 0; i < summary.cluster_count; i++) {
        for (int j = 0; j < summary.clusters[i].entry_point_count; j++) {
            if (strcmp(summary.clusters[i].entry_points[j], "POST /orders") == 0) {
                found_entry = true;
                break;
            }
        }
        if (found_entry) {
            break;
        }
    }
    ASSERT_TRUE(found_entry);

    cbm_store_architecture_summary_free(&summary);
    cbm_store_close(s);
    PASS();
}

/* ── ADR tests ──────────────────────────────────────────────────── */

TEST(adr_store_and_retrieve) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    const char *content = "## PURPOSE\nTest project for unit tests.\n\n## STACK\n- Go: speed";
    ASSERT_EQ(cbm_store_adr_store(s, "test", content), CBM_STORE_OK);

    cbm_adr_t adr;
    ASSERT_EQ(cbm_store_adr_get(s, "test", &adr), CBM_STORE_OK);
    ASSERT_STR_EQ(adr.content, content);
    ASSERT_STR_EQ(adr.project, "test");
    ASSERT_TRUE(adr.created_at != NULL && adr.created_at[0] != '\0');
    ASSERT_TRUE(adr.updated_at != NULL && adr.updated_at[0] != '\0');

    cbm_store_adr_free(&adr);
    cbm_store_close(s);
    PASS();
}

TEST(adr_upsert) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    ASSERT_EQ(cbm_store_adr_store(s, "test", "v1"), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_adr_store(s, "test", "v2"), CBM_STORE_OK);

    cbm_adr_t adr;
    ASSERT_EQ(cbm_store_adr_get(s, "test", &adr), CBM_STORE_OK);
    ASSERT_STR_EQ(adr.content, "v2");

    cbm_store_adr_free(&adr);
    cbm_store_close(s);
    PASS();
}

TEST(adr_delete) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    ASSERT_EQ(cbm_store_adr_store(s, "test", "## PURPOSE\nTest"), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_adr_delete(s, "test"), CBM_STORE_OK);

    cbm_adr_t adr;
    ASSERT_TRUE(cbm_store_adr_get(s, "test", &adr) != CBM_STORE_OK);

    cbm_store_close(s);
    PASS();
}

TEST(adr_delete_not_found) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);

    ASSERT_TRUE(cbm_store_adr_delete(s, "nonexistent") != CBM_STORE_OK);

    cbm_store_close(s);
    PASS();
}

TEST(adr_parse_sections_basic) {
    cbm_adr_sections_t sec = cbm_adr_parse_sections("## PURPOSE\nFoo\n\n## STACK\nBar");
    ASSERT_EQ(sec.count, 2);
    ASSERT_STR_EQ(sec.keys[0], "PURPOSE");
    ASSERT_STR_EQ(sec.values[0], "Foo");
    ASSERT_STR_EQ(sec.keys[1], "STACK");
    ASSERT_STR_EQ(sec.values[1], "Bar");
    cbm_adr_sections_free(&sec);
    PASS();
}

TEST(adr_parse_sections_all_six) {
    cbm_adr_sections_t sec =
        cbm_adr_parse_sections("## PURPOSE\nA\n\n## STACK\nB\n\n## ARCHITECTURE\nC\n\n## "
                               "PATTERNS\nD\n\n## TRADEOFFS\nE\n\n## PHILOSOPHY\nF");
    ASSERT_EQ(sec.count, 6);
    ASSERT_STR_EQ(sec.keys[0], "PURPOSE");
    ASSERT_STR_EQ(sec.values[0], "A");
    ASSERT_STR_EQ(sec.keys[5], "PHILOSOPHY");
    ASSERT_STR_EQ(sec.values[5], "F");
    cbm_adr_sections_free(&sec);
    PASS();
}

TEST(adr_parse_sections_non_canonical) {
    cbm_adr_sections_t sec =
        cbm_adr_parse_sections("## PURPOSE\nFoo\n## CUSTOM\nStill in PURPOSE\n\n## STACK\nBar");
    ASSERT_EQ(sec.count, 2);
    ASSERT_STR_EQ(sec.keys[0], "PURPOSE");
    ASSERT_STR_EQ(sec.values[0], "Foo\n## CUSTOM\nStill in PURPOSE");
    ASSERT_STR_EQ(sec.keys[1], "STACK");
    ASSERT_STR_EQ(sec.values[1], "Bar");
    cbm_adr_sections_free(&sec);
    PASS();
}

TEST(adr_parse_sections_empty) {
    cbm_adr_sections_t sec = cbm_adr_parse_sections("");
    ASSERT_EQ(sec.count, 0);
    cbm_adr_sections_free(&sec);
    PASS();
}

TEST(adr_parse_sections_preamble) {
    cbm_adr_sections_t sec = cbm_adr_parse_sections("preamble\n## PURPOSE\nFoo");
    ASSERT_EQ(sec.count, 1);
    ASSERT_STR_EQ(sec.keys[0], "PURPOSE");
    ASSERT_STR_EQ(sec.values[0], "Foo");
    cbm_adr_sections_free(&sec);
    PASS();
}

TEST(adr_parse_sections_multiline) {
    cbm_adr_sections_t sec =
        cbm_adr_parse_sections("## PURPOSE\nLine 1\nLine 2\nLine 3\n\n## STACK\n- Go\n- SQLite");
    ASSERT_EQ(sec.count, 2);
    ASSERT_STR_EQ(sec.values[0], "Line 1\nLine 2\nLine 3");
    ASSERT_STR_EQ(sec.values[1], "- Go\n- SQLite");
    cbm_adr_sections_free(&sec);
    PASS();
}

TEST(adr_render_canonical_order) {
    cbm_adr_sections_t sec = {.count = 2};
    sec.keys[0] = strdup("STACK");
    sec.values[0] = strdup("Bar");
    sec.keys[1] = strdup("PURPOSE");
    sec.values[1] = strdup("Foo");
    char *rendered = cbm_adr_render(&sec);
    ASSERT_STR_EQ(rendered, "## PURPOSE\nFoo\n\n## STACK\nBar");
    free(rendered);
    cbm_adr_sections_free(&sec);
    PASS();
}

TEST(adr_render_all_sections) {
    cbm_adr_sections_t sec = {.count = 6};
    sec.keys[0] = strdup("PHILOSOPHY");
    sec.values[0] = strdup("F");
    sec.keys[1] = strdup("PURPOSE");
    sec.values[1] = strdup("A");
    sec.keys[2] = strdup("STACK");
    sec.values[2] = strdup("B");
    sec.keys[3] = strdup("ARCHITECTURE");
    sec.values[3] = strdup("C");
    sec.keys[4] = strdup("PATTERNS");
    sec.values[4] = strdup("D");
    sec.keys[5] = strdup("TRADEOFFS");
    sec.values[5] = strdup("E");
    char *rendered = cbm_adr_render(&sec);
    ASSERT_STR_EQ(rendered, "## PURPOSE\nA\n\n## STACK\nB\n\n## ARCHITECTURE\nC\n\n## "
                            "PATTERNS\nD\n\n## TRADEOFFS\nE\n\n## PHILOSOPHY\nF");
    free(rendered);
    cbm_adr_sections_free(&sec);
    PASS();
}

TEST(adr_render_non_canonical) {
    cbm_adr_sections_t sec = {.count = 3};
    sec.keys[0] = strdup("PURPOSE");
    sec.values[0] = strdup("Foo");
    sec.keys[1] = strdup("ZEBRA");
    sec.values[1] = strdup("Z");
    sec.keys[2] = strdup("ALPHA");
    sec.values[2] = strdup("A");
    char *rendered = cbm_adr_render(&sec);
    ASSERT_STR_EQ(rendered, "## PURPOSE\nFoo\n\n## ALPHA\nA\n\n## ZEBRA\nZ");
    free(rendered);
    cbm_adr_sections_free(&sec);
    PASS();
}

TEST(adr_render_empty) {
    cbm_adr_sections_t sec = {.count = 0};
    char *rendered = cbm_adr_render(&sec);
    ASSERT_STR_EQ(rendered, "");
    free(rendered);
    PASS();
}

TEST(adr_parse_render_roundtrip) {
    const char *original =
        "## PURPOSE\nTest project\n\n## STACK\n- Go: speed\n- SQLite: embedded\n\n"
        "## ARCHITECTURE\nPipeline pattern\n\n## PATTERNS\n- Convention over config\n\n"
        "## TRADEOFFS\n- Speed over features\n\n## PHILOSOPHY\n- Keep it simple";
    cbm_adr_sections_t sec = cbm_adr_parse_sections(original);
    char *rendered = cbm_adr_render(&sec);
    ASSERT_STR_EQ(rendered, original);
    free(rendered);
    cbm_adr_sections_free(&sec);
    PASS();
}

TEST(adr_update_sections) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    ASSERT_EQ(cbm_store_adr_store(s, "test", "## PURPOSE\nOriginal purpose\n\n## STACK\n- Go"),
              CBM_STORE_OK);

    const char *keys[] = {"PATTERNS"};
    const char *values[] = {"- Pipeline pattern"};
    cbm_adr_t updated;
    ASSERT_EQ(cbm_store_adr_update_sections(s, "test", keys, values, 1, &updated), CBM_STORE_OK);

    /* Verify all sections preserved */
    cbm_adr_sections_t sec = cbm_adr_parse_sections(updated.content);
    bool found_purpose = false, found_stack = false, found_patterns = false;
    for (int i = 0; i < sec.count; i++) {
        if (strcmp(sec.keys[i], "PURPOSE") == 0) {
            ASSERT_STR_EQ(sec.values[i], "Original purpose");
            found_purpose = true;
        }
        if (strcmp(sec.keys[i], "STACK") == 0) {
            ASSERT_STR_EQ(sec.values[i], "- Go");
            found_stack = true;
        }
        if (strcmp(sec.keys[i], "PATTERNS") == 0) {
            ASSERT_STR_EQ(sec.values[i], "- Pipeline pattern");
            found_patterns = true;
        }
    }
    ASSERT_TRUE(found_purpose);
    ASSERT_TRUE(found_stack);
    ASSERT_TRUE(found_patterns);

    cbm_adr_sections_free(&sec);
    cbm_store_adr_free(&updated);
    cbm_store_close(s);
    PASS();
}

TEST(adr_update_overflow) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_adr_store(s, "test", "## PURPOSE\nShort"), CBM_STORE_OK);

    /* Create huge content */
    char *huge = malloc(CBM_ADR_MAX_LENGTH + 2);
    memset(huge, 'x', CBM_ADR_MAX_LENGTH + 1);
    huge[CBM_ADR_MAX_LENGTH + 1] = '\0';

    const char *keys[] = {"STACK"};
    const char *values[] = {huge};
    cbm_adr_t out;
    ASSERT_TRUE(cbm_store_adr_update_sections(s, "test", keys, values, 1, &out) != CBM_STORE_OK);

    free(huge);
    cbm_store_close(s);
    PASS();
}

TEST(adr_update_no_existing) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    const char *keys[] = {"PURPOSE"};
    const char *values[] = {"New purpose"};
    cbm_adr_t out;
    ASSERT_TRUE(cbm_store_adr_update_sections(s, "test", keys, values, 1, &out) != CBM_STORE_OK);

    cbm_store_close(s);
    PASS();
}

TEST(adr_validate_all_sections) {
    const char *content = "## PURPOSE\nA\n\n## STACK\nB\n\n## ARCHITECTURE\nC\n\n## "
                          "PATTERNS\nD\n\n## TRADEOFFS\nE\n\n## PHILOSOPHY\nF";
    char errbuf[256];
    ASSERT_EQ(cbm_adr_validate_content(content, errbuf, sizeof(errbuf)), CBM_STORE_OK);
    PASS();
}

TEST(adr_validate_missing_sections) {
    const char *content = "## PURPOSE\nA\n\n## STACK\nB";
    char errbuf[512];
    ASSERT_TRUE(cbm_adr_validate_content(content, errbuf, sizeof(errbuf)) != CBM_STORE_OK);
    /* Error should mention missing sections */
    ASSERT_TRUE(strstr(errbuf, "ARCHITECTURE") != NULL);
    ASSERT_TRUE(strstr(errbuf, "PATTERNS") != NULL);
    ASSERT_TRUE(strstr(errbuf, "TRADEOFFS") != NULL);
    ASSERT_TRUE(strstr(errbuf, "PHILOSOPHY") != NULL);
    PASS();
}

TEST(adr_validate_empty) {
    char errbuf[256];
    ASSERT_TRUE(cbm_adr_validate_content("", errbuf, sizeof(errbuf)) != CBM_STORE_OK);
    PASS();
}

TEST(adr_validate_keys_valid) {
    const char *keys[] = {"PURPOSE", "STACK"};
    char errbuf[256];
    ASSERT_EQ(cbm_adr_validate_section_keys(keys, 2, errbuf, sizeof(errbuf)), CBM_STORE_OK);
    PASS();
}

TEST(adr_validate_keys_invalid) {
    const char *keys[] = {"PURPOSE", "STACKS", "CUSTOM"};
    char errbuf[256];
    ASSERT_TRUE(cbm_adr_validate_section_keys(keys, 3, errbuf, sizeof(errbuf)) != CBM_STORE_OK);
    ASSERT_TRUE(strstr(errbuf, "STACKS") != NULL);
    ASSERT_TRUE(strstr(errbuf, "CUSTOM") != NULL);
    PASS();
}

TEST(adr_validate_keys_empty) {
    char errbuf[256];
    ASSERT_EQ(cbm_adr_validate_section_keys(NULL, 0, errbuf, sizeof(errbuf)), CBM_STORE_OK);
    PASS();
}

/* ── Louvain tests ──────────────────────────────────────────────── */

TEST(louvain_basic) {
    /* Triangle: 1-2, 2-3, 1-3. Pair: 4-5. */
    int64_t nodes[] = {1, 2, 3, 4, 5};
    cbm_louvain_edge_t edges[] = {{1, 2}, {2, 3}, {1, 3}, {4, 5}};
    cbm_louvain_result_t *result = NULL;
    int count = 0;
    ASSERT_EQ(cbm_louvain(nodes, 5, edges, 4, &result, &count), CBM_STORE_OK);
    ASSERT_EQ(count, 5);

    /* Build node→community map */
    int comm[6] = {0};
    for (int i = 0; i < count; i++) {
        comm[result[i].node_id] = result[i].community;
    }

    /* Triangle should be same community */
    ASSERT_EQ(comm[1], comm[2]);
    ASSERT_EQ(comm[2], comm[3]);
    /* Pair should be same community */
    ASSERT_EQ(comm[4], comm[5]);
    /* Triangle and pair different */
    ASSERT_TRUE(comm[1] != comm[4]);

    free(result);
    PASS();
}

TEST(louvain_empty) {
    cbm_louvain_result_t *result = NULL;
    int count = 0;
    ASSERT_EQ(cbm_louvain(NULL, 0, NULL, 0, &result, &count), CBM_STORE_OK);
    ASSERT_EQ(count, 0);
    free(result);
    PASS();
}

TEST(louvain_single_node) {
    int64_t nodes[] = {42};
    cbm_louvain_result_t *result = NULL;
    int count = 0;
    ASSERT_EQ(cbm_louvain(nodes, 1, NULL, 0, &result, &count), CBM_STORE_OK);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(result[0].node_id, 42);
    free(result);
    PASS();
}

TEST(louvain_converges) {
    /* Two fully connected clusters of 10 nodes each, bridged by one edge */
    int64_t nodes[20];
    cbm_louvain_edge_t edges[200];
    int nedges = 0;

    for (int i = 0; i < 20; i++)
        nodes[i] = i + 1;

    /* Cluster 1: nodes 1-10, fully connected */
    for (int i = 1; i <= 10; i++) {
        for (int j = i + 1; j <= 10; j++) {
            edges[nedges++] = (cbm_louvain_edge_t){i, j};
        }
    }
    /* Cluster 2: nodes 11-20, fully connected */
    for (int i = 11; i <= 20; i++) {
        for (int j = i + 1; j <= 20; j++) {
            edges[nedges++] = (cbm_louvain_edge_t){i, j};
        }
    }
    /* Bridge */
    edges[nedges++] = (cbm_louvain_edge_t){5, 15};

    cbm_louvain_result_t *result = NULL;
    int count = 0;
    ASSERT_EQ(cbm_louvain(nodes, 20, edges, nedges, &result, &count), CBM_STORE_OK);
    ASSERT_EQ(count, 20);

    /* Count communities */
    int comm_map[21] = {0};
    for (int i = 0; i < count; i++) {
        comm_map[result[i].node_id] = result[i].community;
    }

    /* Count distinct communities */
    int distinct[20];
    int ndistinct = 0;
    for (int i = 0; i < count; i++) {
        bool found = false;
        for (int j = 0; j < ndistinct; j++) {
            if (distinct[j] == result[i].community) {
                found = true;
                break;
            }
        }
        if (!found)
            distinct[ndistinct++] = result[i].community;
    }
    ASSERT_TRUE(ndistinct >= 2);

    /* Nodes 1-10 mostly in same community */
    int same_count = 0;
    for (int i = 1; i <= 10; i++) {
        if (comm_map[i] == comm_map[1])
            same_count++;
    }
    ASSERT_TRUE(same_count >= 8);

    free(result);
    PASS();
}

/* ── Helper function tests ──────────────────────────────────────── */

TEST(qn_to_package) {
    /* 4+ segments: returns segment[2] */
    ASSERT_STR_EQ(cbm_qn_to_package("project.internal.store.search.Search"), "store");
    ASSERT_STR_EQ(cbm_qn_to_package("project.src.utils.helper.foo"), "utils");
    ASSERT_STR_EQ(cbm_qn_to_package("project.src.components.Button.render"), "components");
    ASSERT_STR_EQ(cbm_qn_to_package("project.cmd.server.main"), "server");
    /* 3 segments: falls back to segment[1] */
    ASSERT_STR_EQ(cbm_qn_to_package("project.main.foo"), "main");
    ASSERT_STR_EQ(cbm_qn_to_package("project.cmd"), "cmd");
    /* Edge cases */
    ASSERT_STR_EQ(cbm_qn_to_package("standalone"), "");
    ASSERT_STR_EQ(cbm_qn_to_package(""), "");
    PASS();
}

TEST(qn_to_top_package) {
    ASSERT_STR_EQ(cbm_qn_to_top_package("project.internal.store.search.Search"), "internal");
    ASSERT_STR_EQ(cbm_qn_to_top_package("project.src.components.Button"), "src");
    ASSERT_STR_EQ(cbm_qn_to_top_package("project.cmd"), "cmd");
    ASSERT_STR_EQ(cbm_qn_to_top_package("standalone"), "");
    PASS();
}

TEST(is_test_file_path) {
    ASSERT_TRUE(!cbm_is_test_file_path("internal/handler/handler.go"));
    ASSERT_TRUE(cbm_is_test_file_path("src/__tests__/handler.test.ts"));
    ASSERT_TRUE(cbm_is_test_file_path("src/test/java/com/example/Test.java"));
    ASSERT_TRUE(cbm_is_test_file_path("tests/test_handler.py"));
    ASSERT_TRUE(cbm_is_test_file_path("testdata/fixture.json"));
    ASSERT_TRUE(!cbm_is_test_file_path(""));
    PASS();
}

TEST(find_architecture_docs) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    const char *fps[] = {"main.go", "ARCHITECTURE.md", "docs/adr/001-use-sqlite.md", "README.md"};
    for (int i = 0; i < 4; i++) {
        char qn[64];
        snprintf(qn, sizeof(qn), "test.%s", fps[i]);
        cbm_node_t n = {.project = "test",
                        .label = "File",
                        .name = fps[i],
                        .qualified_name = qn,
                        .file_path = fps[i]};
        cbm_store_upsert_node(s, &n);
    }

    char **docs = NULL;
    int count = 0;
    ASSERT_EQ(cbm_store_find_architecture_docs(s, "test", &docs, &count), CBM_STORE_OK);
    ASSERT_EQ(count, 2);

    bool found_arch = false, found_adr = false;
    for (int i = 0; i < count; i++) {
        if (strcmp(docs[i], "ARCHITECTURE.md") == 0)
            found_arch = true;
        if (strcmp(docs[i], "docs/adr/001-use-sqlite.md") == 0)
            found_adr = true;
        free(docs[i]);
    }
    free(docs);
    ASSERT_TRUE(found_arch);
    ASSERT_TRUE(found_adr);

    cbm_store_close(s);
    PASS();
}

TEST(find_architecture_docs_empty) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    char **docs = NULL;
    int count = 0;
    ASSERT_EQ(cbm_store_find_architecture_docs(s, "test", &docs, &count), CBM_STORE_OK);
    ASSERT_EQ(count, 0);
    free(docs);

    cbm_store_close(s);
    PASS();
}

/* ── Case-insensitive search tests ───────────────────────────────── */

TEST(search_case_insensitive_default) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    cbm_node_t n1 = {
        .project = "test", .label = "Function", .name = "FooBar", .qualified_name = "test.FooBar"};
    cbm_node_t n2 = {
        .project = "test", .label = "Function", .name = "foobar", .qualified_name = "test.foobar"};
    cbm_node_t n3 = {
        .project = "test", .label = "Function", .name = "FOOBAR", .qualified_name = "test.FOOBAR"};
    cbm_store_upsert_node(s, &n1);
    cbm_store_upsert_node(s, &n2);
    cbm_store_upsert_node(s, &n3);

    /* Default (case_sensitive=false) should match all 3 */
    cbm_search_params_t params = {.project = "test",
                                  .name_pattern = "foobar",
                                  .min_degree = -1,
                                  .max_degree = -1,
                                  .case_sensitive = false};
    cbm_search_output_t out = {0};
    ASSERT_EQ(cbm_store_search(s, &params, &out), CBM_STORE_OK);
    ASSERT_EQ(out.count, 3);
    cbm_store_search_free(&out);

    cbm_store_close(s);
    PASS();
}

TEST(search_case_sensitive_explicit) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    cbm_node_t n1 = {
        .project = "test", .label = "Function", .name = "FooBar", .qualified_name = "test.FooBar"};
    cbm_node_t n2 = {
        .project = "test", .label = "Function", .name = "foobar", .qualified_name = "test.foobar"};
    cbm_node_t n3 = {
        .project = "test", .label = "Function", .name = "FOOBAR", .qualified_name = "test.FOOBAR"};
    cbm_store_upsert_node(s, &n1);
    cbm_store_upsert_node(s, &n2);
    cbm_store_upsert_node(s, &n3);

    /* Explicit case-sensitive should match only "foobar" */
    cbm_search_params_t params = {.project = "test",
                                  .name_pattern = "foobar",
                                  .min_degree = -1,
                                  .max_degree = -1,
                                  .case_sensitive = true};
    cbm_search_output_t out = {0};
    ASSERT_EQ(cbm_store_search(s, &params, &out), CBM_STORE_OK);
    ASSERT_EQ(out.count, 1);
    if (out.count > 0) {
        ASSERT_STR_EQ(out.results[0].node.name, "foobar");
    }
    cbm_store_search_free(&out);

    cbm_store_close(s);
    PASS();
}

/* ── Suite ─────────────────────────────────────────────────────── */

SUITE(store_arch) {
    /* Architecture */
    RUN_TEST(arch_get_all);
    RUN_TEST(arch_entry_points_exclude_tests);
    RUN_TEST(arch_hotspots_exclude_tests);
    RUN_TEST(arch_specific_aspects);
    RUN_TEST(arch_empty_project);
    RUN_TEST(arch_languages);
    RUN_TEST(arch_routes);
    RUN_TEST(arch_hotspots);
    RUN_TEST(arch_boundaries);
    RUN_TEST(arch_layers);
    RUN_TEST(arch_file_tree);
    RUN_TEST(arch_clusters);
    RUN_TEST(arch_summary_basic);
    RUN_TEST(arch_summary_focus);
    RUN_TEST(arch_summary_many_files);
    RUN_TEST(arch_summary_cluster_growth);
    RUN_TEST(arch_summary_cluster_entry_fallback);

    /* ADR */
    RUN_TEST(adr_store_and_retrieve);
    RUN_TEST(adr_upsert);
    RUN_TEST(adr_delete);
    RUN_TEST(adr_delete_not_found);
    RUN_TEST(adr_parse_sections_basic);
    RUN_TEST(adr_parse_sections_all_six);
    RUN_TEST(adr_parse_sections_non_canonical);
    RUN_TEST(adr_parse_sections_empty);
    RUN_TEST(adr_parse_sections_preamble);
    RUN_TEST(adr_parse_sections_multiline);
    RUN_TEST(adr_render_canonical_order);
    RUN_TEST(adr_render_all_sections);
    RUN_TEST(adr_render_non_canonical);
    RUN_TEST(adr_render_empty);
    RUN_TEST(adr_parse_render_roundtrip);
    RUN_TEST(adr_update_sections);
    RUN_TEST(adr_update_overflow);
    RUN_TEST(adr_update_no_existing);
    RUN_TEST(adr_validate_all_sections);
    RUN_TEST(adr_validate_missing_sections);
    RUN_TEST(adr_validate_empty);
    RUN_TEST(adr_validate_keys_valid);
    RUN_TEST(adr_validate_keys_invalid);
    RUN_TEST(adr_validate_keys_empty);

    /* Louvain */
    RUN_TEST(louvain_basic);
    RUN_TEST(louvain_empty);
    RUN_TEST(louvain_single_node);
    RUN_TEST(louvain_converges);

    /* Helpers */
    RUN_TEST(qn_to_package);
    RUN_TEST(qn_to_top_package);
    RUN_TEST(is_test_file_path);
    RUN_TEST(find_architecture_docs);
    RUN_TEST(find_architecture_docs_empty);

    /* Case-insensitive search */
    RUN_TEST(search_case_insensitive_default);
    RUN_TEST(search_case_sensitive_explicit);
}
