#include "test_framework.h"
#include <store/architecture_projection.h>
#include <foundation/platform.h>
#include <yyjson/yyjson.h>
#include <sqlite3.h>

static int64_t projection_node(cbm_store_t *store, const char *label, const char *qn,
                               const char *file, bool entry) {
    cbm_node_t n = {.project = "projection",
                    .label = label,
                    .name = qn,
                    .qualified_name = qn,
                    .file_path = file,
                    .start_line = 10,
                    .end_line = 20,
                    .properties_json = entry ? "{\"is_entry_point\":true}" : "{}"};
    return cbm_store_upsert_node(store, &n);
}

static int64_t projection_edge(cbm_store_t *store, int64_t source, int64_t target,
                               const char *type) {
    cbm_edge_t e = {
        .project = "projection", .source_id = source, .target_id = target, .type = type};
    return cbm_store_insert_edge(store, &e);
}

static cbm_store_t *projection_store(void) {
    cbm_store_t *store = cbm_store_open_memory();
    if (store)
        cbm_store_upsert_project(store, "projection", "/tmp/projection");
    return store;
}

static yyjson_val *projection_field(yyjson_doc *doc, const char *field) {
    return yyjson_obj_get(yyjson_doc_get_root(doc), field);
}

TEST(projection_accounts_for_isolated_nodes_and_files) {
    cbm_store_t *store = projection_store();
    ASSERT_NOT_NULL(store);
    projection_node(store, "Project", "projection", "", false);
    projection_node(store, "Folder", "projection.folder", "folder", false);
    projection_node(store, "File", "projection.alone.c", "alone.c", false);
    projection_node(store, "Module", "projection.alone", "alone.c", false);
    projection_node(store, "Function", "projection.alone.work", "alone.c", false);
    projection_node(store, "File", "projection.notes.md", "notes.md", false);
    char *json = NULL;
    ASSERT_EQ(cbm_store_architecture_projection(store, "projection", NULL, &json), CBM_STORE_OK);
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *totals = projection_field(doc, "totals");
    ASSERT_EQ(yyjson_get_int(yyjson_obj_get(totals, "nodes")), 6);
    ASSERT_EQ(yyjson_get_int(yyjson_obj_get(totals, "accounted_nodes")), 4);
    ASSERT_EQ(yyjson_get_int(yyjson_obj_get(totals, "structural_nodes")), 2);
    ASSERT_EQ(yyjson_get_int(yyjson_obj_get(totals, "files")), 2);
    ASSERT_TRUE(yyjson_get_bool(projection_field(doc, "complete")));
    ASSERT_EQ(yyjson_arr_size(projection_field(doc, "components")), 2);
    ASSERT_EQ(yyjson_arr_size(projection_field(doc, "dependencies")), 0);
    yyjson_doc_free(doc);
    free(json);
    cbm_store_close(store);
    PASS();
}

TEST(projection_preserves_typed_edges_and_contiguous_paths) {
    cbm_store_t *store = projection_store();
    ASSERT_NOT_NULL(store);
    projection_node(store, "Package", "projection.a", "", false);
    projection_node(store, "Package", "projection.b", "", false);
    projection_node(store, "Package", "projection.c", "", false);
    int64_t a = projection_node(store, "Function", "projection.a.start", "a.c", true);
    int64_t b1 = projection_node(store, "Function", "projection.b.one", "b.c", false);
    int64_t b2 = projection_node(store, "Function", "projection.b.two", "b.c", false);
    int64_t c = projection_node(store, "Function", "projection.c.end", "c.c", false);
    int64_t first = projection_edge(store, a, b1, "CALLS");
    projection_edge(store, b2, c, "CALLS");
    projection_edge(store, b1, b2, "CALL_REFERENCE");
    char *json = NULL;
    cbm_architecture_projection_options_t opts = {.entry_node_id = a};
    ASSERT_EQ(cbm_store_architecture_projection(store, "projection", &opts, &json), CBM_STORE_OK);
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *paths = projection_field(doc, "paths");
    ASSERT_EQ(yyjson_arr_size(paths), 1);
    yyjson_val *path = yyjson_arr_get(paths, 0);
    yyjson_val *nodes = yyjson_obj_get(path, "nodes"), *edges = yyjson_obj_get(path, "edges");
    ASSERT_EQ(yyjson_arr_size(nodes), 2);
    ASSERT_EQ(yyjson_arr_size(edges), 1);
    ASSERT_EQ(yyjson_get_sint(yyjson_obj_get(yyjson_arr_get(nodes, 1), "id")), b1);
    ASSERT_EQ(yyjson_get_sint(yyjson_obj_get(yyjson_arr_get(edges, 0), "id")), first);
    ASSERT_EQ(yyjson_arr_size(projection_field(doc, "dependencies")), 2);
    yyjson_doc_free(doc);
    free(json);
    /* A real binding changes the answer; projected component edges alone did not. */
    projection_edge(store, b1, b2, "CALLS");
    ASSERT_EQ(cbm_store_architecture_projection(store, "projection", &opts, &json), CBM_STORE_OK);
    doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    nodes = yyjson_obj_get(yyjson_arr_get(projection_field(doc, "paths"), 0), "nodes");
    ASSERT_EQ(yyjson_arr_size(nodes), 4);
    ASSERT_EQ(yyjson_get_sint(yyjson_obj_get(yyjson_arr_get(nodes, 3), "id")), c);
    yyjson_doc_free(doc);
    free(json);
    cbm_store_close(store);
    PASS();
}

TEST(projection_reports_dependency_cycles_without_fabricated_execution) {
    cbm_store_t *store = projection_store();
    ASSERT_NOT_NULL(store);
    projection_node(store, "Package", "projection.a", "", false);
    projection_node(store, "Package", "projection.b", "", false);
    projection_node(store, "Package", "projection.c", "", false);
    int64_t a = projection_node(store, "Function", "projection.a.run", "a.c", true);
    int64_t b = projection_node(store, "Function", "projection.b.run", "b.c", false);
    int64_t c = projection_node(store, "Function", "projection.c.run", "c.c", false);
    projection_edge(store, a, b, "IMPORTS");
    projection_edge(store, b, c, "IMPLEMENTS");
    projection_edge(store, c, a, "USAGE");
    char *json = NULL;
    ASSERT_EQ(cbm_store_architecture_projection(store, "projection", NULL, &json), CBM_STORE_OK);
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *cycles = projection_field(doc, "cycles");
    ASSERT_EQ(yyjson_arr_size(cycles), 1);
    ASSERT_EQ(yyjson_arr_size(yyjson_obj_get(yyjson_arr_get(cycles, 0), "component_ids")), 3);
    ASSERT_EQ(yyjson_arr_size(projection_field(doc, "paths")), 0);
    yyjson_doc_free(doc);
    free(json);
    cbm_architecture_projection_options_t opts = {.max_components = 2};
    ASSERT_EQ(cbm_store_architecture_projection(store, "projection", &opts, &json), CBM_STORE_OK);
    doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    ASSERT_EQ(yyjson_arr_size(projection_field(doc, "cycles")), 0);
    ASSERT_EQ(yyjson_get_int(yyjson_obj_get(projection_field(doc, "totals"), "cycles")), 1);
    ASSERT_EQ(yyjson_get_int(yyjson_obj_get(projection_field(doc, "limits"), "omitted_cycles")), 1);
    yyjson_doc_free(doc);
    free(json);
    cbm_store_close(store);
    PASS();
}

TEST(projection_graph_limits_abstain_instead_of_sampling) {
    cbm_store_t *store = projection_store();
    ASSERT_NOT_NULL(store);
    int64_t a = projection_node(store, "Function", "projection.a", "a.c", true);
    int64_t b = projection_node(store, "Function", "projection.b", "b.c", false);
    projection_edge(store, a, b, "CALLS");
    projection_edge(store, b, a, "CALLS");
    cbm_architecture_projection_options_t opts = {.max_nodes = 1};
    char *json = NULL;
    ASSERT_EQ(cbm_store_architecture_projection(store, "projection", &opts, &json), CBM_STORE_OK);
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    ASSERT_STR_EQ(yyjson_get_str(projection_field(doc, "status")), "limited");
    ASSERT_FALSE(yyjson_get_bool(projection_field(doc, "complete")));
    ASSERT_EQ(yyjson_arr_size(projection_field(doc, "components")), 0);
    ASSERT_EQ(yyjson_get_int(yyjson_obj_get(projection_field(doc, "totals"), "nodes")), 2);
    yyjson_doc_free(doc);
    free(json);
    opts.max_nodes = 0;
    opts.max_edges = 1;
    ASSERT_EQ(cbm_store_architecture_projection(store, "projection", &opts, &json), CBM_STORE_OK);
    doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    ASSERT_STR_EQ(yyjson_get_str(projection_field(doc, "status")), "limited");
    ASSERT_EQ(yyjson_arr_size(projection_field(doc, "components")), 0);
    yyjson_doc_free(doc);
    free(json);
    cbm_store_close(store);
    PASS();
}

static bool projection_cancel(void *context) {
    (void)context;
    return true;
}

TEST(projection_cancellation_deadline_and_store_reuse) {
    cbm_store_t *store = projection_store();
    ASSERT_NOT_NULL(store);
    projection_node(store, "Function", "projection.a", "a.c", true);
    char *json = NULL;
    cbm_architecture_projection_options_t opts = {.cancel = projection_cancel};
    ASSERT_EQ(cbm_store_architecture_projection(store, "projection", &opts, &json),
              CBM_STORE_CANCELLED);
    ASSERT_NULL(json);
    opts.cancel = NULL;
    opts.deadline_ms = cbm_now_ms() - 1;
    ASSERT_EQ(cbm_store_architecture_projection(store, "projection", &opts, &json),
              CBM_STORE_SCAN_LIMIT);
    ASSERT_NULL(json);
    ASSERT_EQ(cbm_store_architecture_projection(store, "projection", NULL, &json), CBM_STORE_OK);
    ASSERT_NOT_NULL(json);
    free(json);
    ASSERT_EQ(sqlite3_get_autocommit(cbm_store_get_db(store)), 1);
    cbm_store_close(store);
    PASS();
}

TEST(projection_display_caps_preserve_total_accounting) {
    cbm_store_t *store = projection_store();
    ASSERT_NOT_NULL(store);
    projection_node(store, "File", "projection.a.c", "a.c", false);
    projection_node(store, "File", "projection.b.c", "b.c", false);
    projection_node(store, "File", "projection.c.c", "c.c", false);
    cbm_architecture_projection_options_t opts = {.max_components = 1};
    char *json = NULL;
    ASSERT_EQ(cbm_store_architecture_projection(store, "projection", &opts, &json), CBM_STORE_OK);
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    ASSERT_EQ(yyjson_arr_size(projection_field(doc, "components")), 1);
    ASSERT_EQ(yyjson_get_int(yyjson_obj_get(projection_field(doc, "totals"), "components")), 3);
    ASSERT_EQ(yyjson_get_int(yyjson_obj_get(projection_field(doc, "totals"), "accounted_nodes")),
              3);
    ASSERT_EQ(yyjson_get_int(yyjson_obj_get(projection_field(doc, "limits"), "omitted_components")),
              2);
    ASSERT_FALSE(yyjson_get_bool(projection_field(doc, "complete")));
    yyjson_doc_free(doc);
    free(json);
    cbm_store_close(store);
    PASS();
}

TEST(projection_unknown_project_and_empty_project) {
    cbm_store_t *store = projection_store();
    ASSERT_NOT_NULL(store);
    char *json = NULL;
    ASSERT_EQ(cbm_store_architecture_projection(store, "absent", NULL, &json), CBM_STORE_NOT_FOUND);
    ASSERT_NULL(json);
    ASSERT_EQ(cbm_store_architecture_projection(store, "projection", NULL, &json), CBM_STORE_OK);
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    ASSERT_TRUE(yyjson_get_bool(projection_field(doc, "complete")));
    ASSERT_EQ(yyjson_arr_size(projection_field(doc, "components")), 0);
    yyjson_doc_free(doc);
    free(json);
    projection_node(store, "Project", "projection", "", false);
    projection_node(store, "Folder", "projection.folder", "folder", false);
    ASSERT_EQ(cbm_store_architecture_projection(store, "projection", NULL, &json), CBM_STORE_OK);
    doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    ASSERT_TRUE(yyjson_get_bool(projection_field(doc, "complete")));
    ASSERT_EQ(yyjson_arr_size(projection_field(doc, "components")), 0);
    ASSERT_EQ(yyjson_get_int(yyjson_obj_get(projection_field(doc, "totals"), "structural_nodes")),
              2);
    yyjson_doc_free(doc);
    free(json);
    cbm_store_close(store);
    PASS();
}

TEST(projection_paths_depth_cap_is_explicit) {
    cbm_store_t *store = projection_store();
    ASSERT_NOT_NULL(store);
    int64_t nodes[5];
    for (int i = 0; i < 5; i++) {
        char qn[48];
        snprintf(qn, sizeof(qn), "projection.n%d", i);
        nodes[i] = projection_node(store, "Function", qn, "chain.c", i == 0);
        if (i)
            projection_edge(store, nodes[i - 1], nodes[i], "CALLS");
    }
    cbm_architecture_projection_options_t opts = {.max_depth = 2};
    char *json = NULL;
    ASSERT_EQ(cbm_store_architecture_projection(store, "projection", &opts, &json), CBM_STORE_OK);
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    ASSERT_TRUE(
        yyjson_get_bool(yyjson_obj_get(projection_field(doc, "limits"), "paths_truncated")));
    yyjson_val *path = yyjson_arr_get(projection_field(doc, "paths"), 0);
    ASSERT_EQ(yyjson_arr_size(yyjson_obj_get(path, "edges")), 2);
    yyjson_doc_free(doc);
    free(json);
    cbm_store_close(store);
    PASS();
}

TEST(projection_channel_relationships_are_typed_not_fake_calls) {
    cbm_store_t *store = projection_store();
    ASSERT_NOT_NULL(store);
    int64_t producer = projection_node(store, "Function", "projection.producer", "send.c", true);
    int64_t consumer =
        projection_node(store, "Function", "projection.consumer", "receive.c", false);
    int64_t channel = projection_node(store, "Channel", "projection.orders", "", false);
    projection_edge(store, producer, channel, "EMITS");
    projection_edge(store, consumer, channel, "LISTENS_ON");
    projection_edge(store, producer, consumer, "DATA_FLOWS");
    projection_edge(store, producer, consumer, "SIMILAR_TO");
    char *json = NULL;
    ASSERT_EQ(cbm_store_architecture_projection(store, "projection", NULL, &json), CBM_STORE_OK);
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    ASSERT_EQ(yyjson_arr_size(projection_field(doc, "dependencies")), 3);
    ASSERT_EQ(yyjson_arr_size(projection_field(doc, "paths")), 0);
    ASSERT_EQ(yyjson_get_int(yyjson_obj_get(projection_field(doc, "totals"), "unmodeled_edges")),
              1);
    yyjson_doc_free(doc);
    free(json);
    cbm_store_close(store);
    PASS();
}

TEST(projection_test_callers_cannot_absorb_source_components) {
    cbm_store_t *store = projection_store();
    ASSERT_NOT_NULL(store);
    projection_node(store, "Module", "projection.store", "src/store.c", false);
    int64_t production =
        projection_node(store, "Function", "projection.store.read", "src/store.c", false);
    for (int file = 0; file < 2; file++) {
        char module[64], path[64];
        snprintf(module, sizeof(module), "projection.test%d", file);
        snprintf(path, sizeof(path), "tests/check%d.c", file);
        projection_node(store, "Module", module, path, false);
        for (int i = 0; i < 3; i++) {
            char qn[80];
            snprintf(qn, sizeof(qn), "%s.check%d", module, i);
            int64_t test = projection_node(store, "Function", qn, path, false);
            projection_edge(store, test, production, "CALLS");
        }
    }
    char *json = NULL;
    ASSERT_EQ(cbm_store_architecture_projection(store, "projection", NULL, &json), CBM_STORE_OK);
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *parts = projection_field(doc, "components");
    ASSERT_EQ(yyjson_arr_size(parts), 3);
    int non_test = 0, tests = 0;
    for (size_t i = 0; i < yyjson_arr_size(parts); i++) {
        yyjson_val *part = yyjson_arr_get(parts, i);
        const char *role = yyjson_get_str(yyjson_obj_get(part, "role"));
        ASSERT_NOT_NULL(role);
        if (!strcmp(role, "non_test")) {
            non_test++;
            ASSERT_EQ(yyjson_get_int(yyjson_obj_get(part, "member_count")), 2);
        } else {
            tests++;
            ASSERT_EQ(yyjson_get_int(yyjson_obj_get(part, "member_count")), 4);
        }
    }
    ASSERT_EQ(non_test, 1);
    ASSERT_EQ(tests, 2);
    ASSERT_EQ(yyjson_get_int(yyjson_obj_get(projection_field(doc, "totals"), "accounted_nodes")),
              10);
    yyjson_doc_free(doc);
    free(json);
    cbm_store_close(store);
    PASS();
}

TEST(projection_indexed_embedded_test_has_separate_membership) {
    cbm_store_t *store = projection_store();
    ASSERT_NOT_NULL(store);
    int64_t type = projection_node(store, "Class", "projection.Store", "src/store.rs", false);
    int64_t method =
        projection_node(store, "Method", "projection.Store.read", "src/store.rs", false);
    cbm_node_t test = {.project = "projection",
                       .label = "Method",
                       .name = "embedded_check",
                       .qualified_name = "projection.Store.embedded_check",
                       .file_path = "src/store.rs",
                       .properties_json = "{\"is_test\":true}"};
    int64_t embedded = cbm_store_upsert_node(store, &test);
    projection_edge(store, type, method, "DEFINES_METHOD");
    projection_edge(store, type, embedded, "DEFINES_METHOD");
    projection_edge(store, embedded, method, "CALLS");
    char *json = NULL;
    ASSERT_EQ(cbm_store_architecture_projection(store, "projection", NULL, &json), CBM_STORE_OK);
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *parts = projection_field(doc, "components");
    ASSERT_EQ(yyjson_arr_size(parts), 2);
    bool found = false;
    for (size_t i = 0; i < yyjson_arr_size(parts); i++) {
        yyjson_val *part = yyjson_arr_get(parts, i);
        if (!strcmp(yyjson_get_str(yyjson_obj_get(part, "role")), "test")) {
            found = true;
            ASSERT_EQ(yyjson_get_int(yyjson_obj_get(part, "member_count")), 1);
            ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(part, "role_basis")), "indexed_is_test");
        }
    }
    ASSERT_TRUE(found);
    yyjson_doc_free(doc);
    free(json);
    cbm_store_close(store);
    PASS();
}

TEST(projection_shallow_leaves_do_not_hide_deeper_behavior) {
    cbm_store_t *store = projection_store();
    ASSERT_NOT_NULL(store);
    int64_t entry = projection_node(store, "Function", "projection.main", "main.c", true);
    /* These precede the meaningful branch in both node and edge order. */
    for (int i = 0; i < 13; i++) {
        char qn[64];
        snprintf(qn, sizeof(qn), "projection.utility%d", i);
        int64_t leaf = projection_node(store, "Function", qn, "utility.c", false);
        projection_edge(store, entry, leaf, "CALLS");
    }
    int64_t a = projection_node(store, "Function", "projection.dispatch", "dispatch.c", false);
    int64_t b = projection_node(store, "Function", "projection.query", "query.c", false);
    int64_t end = projection_node(store, "Function", "projection.persist", "store.c", false);
    projection_edge(store, entry, a, "CALLS");
    projection_edge(store, a, b, "CALLS");
    projection_edge(store, b, end, "CALLS");
    char *json = NULL;
    cbm_architecture_projection_options_t opts = {.entry_node_id = entry};
    ASSERT_EQ(cbm_store_architecture_projection(store, "projection", &opts, &json), CBM_STORE_OK);
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *paths = projection_field(doc, "paths");
    ASSERT_EQ(yyjson_arr_size(paths), 12);
    yyjson_val *first = yyjson_arr_get(paths, 0);
    ASSERT_EQ(yyjson_arr_size(yyjson_obj_get(first, "edges")), 3);
    yyjson_val *nodes = yyjson_obj_get(first, "nodes");
    ASSERT_EQ(yyjson_get_sint(yyjson_obj_get(yyjson_arr_get(nodes, 3), "id")), end);
    ASSERT_TRUE(
        yyjson_get_bool(yyjson_obj_get(projection_field(doc, "limits"), "paths_truncated")));
    yyjson_doc_free(doc);
    free(json);
    cbm_store_close(store);
    PASS();
}

TEST(projection_behavior_preserves_first_hop_diversity) {
    cbm_store_t *store = projection_store();
    ASSERT_NOT_NULL(store);
    int64_t entry = projection_node(store, "Function", "projection.main", "main.c", true);
    int64_t a = projection_node(store, "Function", "projection.a", "a.c", false);
    int64_t a1 = projection_node(store, "Function", "projection.a1", "a.c", false);
    int64_t a2 = projection_node(store, "Function", "projection.a2", "a.c", false);
    int64_t b = projection_node(store, "Function", "projection.b", "b.c", false);
    int64_t b1 = projection_node(store, "Function", "projection.b1", "b.c", false);
    projection_edge(store, entry, a, "CALLS");
    projection_edge(store, a, a1, "CALLS");
    projection_edge(store, a, a2, "CALLS");
    projection_edge(store, entry, b, "CALLS");
    projection_edge(store, b, b1, "CALLS");
    char *json = NULL;
    cbm_architecture_projection_options_t opts = {.entry_node_id = entry, .max_paths = 2};
    ASSERT_EQ(cbm_store_architecture_projection(store, "projection", &opts, &json), CBM_STORE_OK);
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *paths = projection_field(doc, "paths");
    ASSERT_EQ(yyjson_arr_size(paths), 2);
    yyjson_val *first = yyjson_obj_get(yyjson_arr_get(paths, 0), "nodes");
    yyjson_val *second = yyjson_obj_get(yyjson_arr_get(paths, 1), "nodes");
    int64_t hop1 = yyjson_get_sint(yyjson_obj_get(yyjson_arr_get(first, 1), "id"));
    int64_t hop2 = yyjson_get_sint(yyjson_obj_get(yyjson_arr_get(second, 1), "id"));
    ASSERT_TRUE((hop1 == a && hop2 == b) || (hop1 == b && hop2 == a));
    yyjson_doc_free(doc);
    free(json);
    cbm_store_close(store);
    PASS();
}

TEST(projection_corridor_preserves_joins_cycles_and_callsite_evidence) {
    cbm_store_t *store = projection_store();
    ASSERT_NOT_NULL(store);
    int64_t a = projection_node(store, "Function", "projection.a", "src/api/main.c", true);
    int64_t b = projection_node(store, "Function", "projection.b", "src/engine/b.c", false);
    int64_t c = projection_node(store, "Function", "projection.c", "src/engine/c.c", false);
    int64_t d = projection_node(store, "Function", "projection.d", "src/store/d.c", false);
    int64_t ignored =
        projection_node(store, "Function", "projection.callback", "src/events/e.c", false);
    cbm_edge_t evidence = {
        .project = "projection",
        .source_id = a,
        .target_id = b,
        .type = "CALLS",
        .properties_json =
            "{\"line\":42,\"strategy\":\"lsp_direct\",\"confidence\":0.95,\"candidates\":1}"};
    int64_t ab = cbm_store_insert_edge(store, &evidence);
    projection_edge(store, a, c, "CALLS");
    projection_edge(store, b, d, "CALLS");
    projection_edge(store, c, d, "CALLS");
    projection_edge(store, b, c, "CALLS");
    projection_edge(store, c, b, "CALLS");
    projection_edge(store, a, ignored, "CALL_REFERENCE");
    projection_edge(store, ignored, d, "EMITS");
    cbm_architecture_projection_options_t opts = {
        .entry_node_id = a, .target_node_id = d, .max_depth = 4};
    char *json = NULL;
    ASSERT_EQ(cbm_store_architecture_projection(store, "projection", &opts, &json), CBM_STORE_OK);
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *view = projection_field(doc, "behavior"), *nodes = yyjson_obj_get(view, "nodes"),
               *edges = yyjson_obj_get(view, "edges");
    ASSERT_TRUE(yyjson_get_bool(yyjson_obj_get(view, "complete")));
    ASSERT_TRUE(yyjson_get_bool(yyjson_obj_get(view, "reachable")));
    ASSERT_EQ(yyjson_arr_size(nodes), 4);
    ASSERT_EQ(yyjson_arr_size(edges), 6);
    int into_d = 0;
    bool found_evidence = false;
    for (size_t i = 0; i < yyjson_arr_size(edges); i++) {
        yyjson_val *edge = yyjson_arr_get(edges, i);
        if (yyjson_get_sint(yyjson_obj_get(edge, "target_id")) == d)
            into_d++;
        if (yyjson_get_sint(yyjson_obj_get(edge, "id")) == ab) {
            found_evidence = true;
            yyjson_val *site = yyjson_obj_get(edge, "callsite"),
                       *resolution = yyjson_obj_get(edge, "resolution");
            ASSERT_EQ(yyjson_get_sint(yyjson_obj_get(site, "line")), 42);
            ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(site, "file_path")), "src/api/main.c");
            ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(resolution, "strategy")), "lsp_direct");
            ASSERT_EQ(yyjson_get_sint(yyjson_obj_get(resolution, "candidates")), 1);
        }
    }
    ASSERT_EQ(into_d, 2);
    ASSERT_TRUE(found_evidence);
    yyjson_val *cycles = yyjson_obj_get(view, "cycles");
    ASSERT_EQ(yyjson_arr_size(cycles), 1);
    ASSERT_EQ(yyjson_arr_size(yyjson_obj_get(yyjson_arr_get(cycles, 0), "node_ids")), 2);
    yyjson_val *paths = projection_field(doc, "paths");
    ASSERT_TRUE(yyjson_arr_size(paths) >= 2);
    for (size_t i = 0; i < yyjson_arr_size(paths); i++) {
        yyjson_val *path = yyjson_arr_get(paths, i), *pn = yyjson_obj_get(path, "nodes"),
                   *pe = yyjson_obj_get(path, "edges");
        ASSERT_EQ(yyjson_arr_size(pn), yyjson_arr_size(pe) + 1);
        ASSERT_EQ(yyjson_get_sint(yyjson_obj_get(yyjson_arr_get(pn, 0), "id")), a);
        ASSERT_EQ(
            yyjson_get_sint(yyjson_obj_get(yyjson_arr_get(pn, yyjson_arr_size(pn) - 1), "id")), d);
        ASSERT_TRUE(yyjson_arr_size(pe) <= 4);
        for (size_t j = 0; j < yyjson_arr_size(pe); j++) {
            yyjson_val *edge = yyjson_arr_get(pe, j);
            ASSERT_EQ(yyjson_get_sint(yyjson_obj_get(edge, "source_id")),
                      yyjson_get_sint(yyjson_obj_get(yyjson_arr_get(pn, j), "id")));
            ASSERT_EQ(yyjson_get_sint(yyjson_obj_get(edge, "target_id")),
                      yyjson_get_sint(yyjson_obj_get(yyjson_arr_get(pn, j + 1), "id")));
        }
    }
    yyjson_doc_free(doc);
    free(json);
    opts.target_node_id = ignored;
    ASSERT_EQ(cbm_store_architecture_projection(store, "projection", &opts, &json), CBM_STORE_OK);
    doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    view = projection_field(doc, "behavior");
    ASSERT_FALSE(yyjson_get_bool(yyjson_obj_get(view, "reachable")));
    ASSERT_TRUE(yyjson_get_bool(yyjson_obj_get(view, "corridor_complete")));
    ASSERT_EQ(yyjson_arr_size(yyjson_obj_get(view, "nodes")), 0);
    ASSERT_EQ(yyjson_arr_size(projection_field(doc, "paths")), 0);
    yyjson_doc_free(doc);
    free(json);
    cbm_store_close(store);
    PASS();
}

TEST(projection_corridor_caps_abstain_without_hiding_joins) {
    cbm_store_t *store = projection_store();
    ASSERT_NOT_NULL(store);
    int64_t a = projection_node(store, "Function", "projection.a", "a.c", true);
    int64_t b = projection_node(store, "Function", "projection.b", "b.c", false);
    int64_t d = projection_node(store, "Function", "projection.d", "d.c", false);
    projection_edge(store, a, b, "CALLS");
    projection_edge(store, b, d, "CALLS");
    cbm_architecture_projection_options_t opts = {
        .entry_node_id = a, .target_node_id = d, .max_corridor_nodes = 2};
    char *json = NULL;
    ASSERT_EQ(cbm_store_architecture_projection(store, "projection", &opts, &json), CBM_STORE_OK);
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *view = projection_field(doc, "behavior");
    ASSERT_FALSE(yyjson_get_bool(yyjson_obj_get(view, "complete")));
    ASSERT_FALSE(yyjson_get_bool(yyjson_obj_get(view, "corridor_complete")));
    ASSERT_EQ(yyjson_get_int(yyjson_obj_get(yyjson_obj_get(view, "totals"), "corridor_nodes")), 3);
    ASSERT_EQ(yyjson_arr_size(yyjson_obj_get(view, "nodes")), 0);
    ASSERT_EQ(yyjson_arr_size(yyjson_obj_get(view, "edges")), 0);
    ASSERT_EQ(yyjson_arr_size(projection_field(doc, "paths")), 0);
    yyjson_doc_free(doc);
    free(json);
    opts.target_node_id = 0;
    opts.max_targets = 1;
    ASSERT_EQ(cbm_store_architecture_projection(store, "projection", &opts, &json), CBM_STORE_OK);
    doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    view = projection_field(doc, "behavior");
    ASSERT_EQ(yyjson_arr_size(yyjson_obj_get(view, "reachable_targets")), 1);
    ASSERT_EQ(yyjson_get_int(yyjson_obj_get(yyjson_obj_get(view, "totals"), "omitted_targets")), 1);
    ASSERT_FALSE(yyjson_get_bool(yyjson_obj_get(view, "complete")));
    yyjson_doc_free(doc);
    free(json);
    cbm_store_close(store);
    PASS();
}

TEST(projection_overview_accounts_for_components_beyond_legacy_cap) {
    cbm_store_t *store = projection_store();
    ASSERT_NOT_NULL(store);
    for (int i = 0; i < 300; i++) {
        char qn[80], file[80];
        snprintf(qn, sizeof(qn), "projection.part%d", i);
        snprintf(file, sizeof(file), "packages/part%d/file.c", i);
        projection_node(store, "File", qn, file, false);
    }
    int64_t entry = projection_node(store, "Function", "projection.entry", "src/main.c", true);
    int64_t target =
        projection_node(store, "Function", "projection.target", "src/service.c", false);
    projection_edge(store, entry, target, "CALLS");
    cbm_architecture_projection_options_t opts = {.max_components = 1, .entry_node_id = entry};
    char *json = NULL;
    ASSERT_EQ(cbm_store_architecture_projection(store, "projection", &opts, &json), CBM_STORE_OK);
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *overview = projection_field(doc, "overview"),
               *groups = yyjson_obj_get(overview, "groups"),
               *parts = yyjson_obj_get(overview, "components");
    ASSERT_TRUE(yyjson_get_bool(yyjson_obj_get(overview, "complete")));
    ASSERT_EQ(yyjson_arr_size(projection_field(doc, "components")), 1);
    ASSERT_EQ(yyjson_arr_size(parts), 302);
    ASSERT_TRUE(yyjson_arr_size(groups) <= 64);
    ASSERT_EQ(yyjson_get_int(yyjson_obj_get(overview, "grouping_depth")), 1);
    size_t membership = 0;
    int members = 0;
    for (size_t i = 0; i < yyjson_arr_size(groups); i++) {
        yyjson_val *group = yyjson_arr_get(groups, i);
        membership += yyjson_arr_size(yyjson_obj_get(group, "component_ids"));
        members += yyjson_get_int(yyjson_obj_get(group, "member_count"));
    }
    ASSERT_EQ(membership, 302);
    ASSERT_EQ(members, 302);
    yyjson_val *symbol =
        yyjson_arr_get(yyjson_obj_get(projection_field(doc, "behavior"), "reachable_targets"), 0);
    const char *group_id = yyjson_get_str(yyjson_obj_get(symbol, "group_id"));
    ASSERT_NOT_NULL(group_id);
    char stable[128];
    snprintf(stable, sizeof(stable), "%s", group_id);
    yyjson_doc_free(doc);
    free(json);
    opts.target_node_id = target;
    ASSERT_EQ(cbm_store_architecture_projection(store, "projection", &opts, &json), CBM_STORE_OK);
    doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    symbol =
        yyjson_arr_get(yyjson_obj_get(projection_field(doc, "behavior"), "reachable_targets"), 0);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(symbol, "group_id")), stable);
    ASSERT_EQ(yyjson_arr_size(yyjson_obj_get(projection_field(doc, "overview"), "components")),
              302);
    yyjson_doc_free(doc);
    free(json);
    cbm_store_close(store);
    PASS();
}

TEST(projection_aggregate_cycle_does_not_invent_symbol_cycle) {
    cbm_store_t *store = projection_store();
    ASSERT_NOT_NULL(store);
    int64_t a = projection_node(store, "Function", "projection.a", "src/api/a.c", true);
    int64_t b = projection_node(store, "Function", "projection.b", "src/store/b.c", false);
    int64_t c = projection_node(store, "Function", "projection.c", "src/api/c.c", false);
    projection_edge(store, a, b, "CALLS");
    projection_edge(store, b, c, "CALLS");
    cbm_architecture_projection_options_t opts = {.entry_node_id = a, .target_node_id = c};
    char *json = NULL;
    ASSERT_EQ(cbm_store_architecture_projection(store, "projection", &opts, &json), CBM_STORE_OK);
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    ASSERT_EQ(yyjson_arr_size(yyjson_obj_get(projection_field(doc, "overview"), "groups")), 2);
    ASSERT_EQ(yyjson_arr_size(yyjson_obj_get(projection_field(doc, "overview"), "connections")), 2);
    ASSERT_EQ(yyjson_arr_size(yyjson_obj_get(projection_field(doc, "behavior"), "cycles")), 0);
    yyjson_doc_free(doc);
    free(json);
    cbm_store_close(store);
    PASS();
}

TEST(projection_provenance_budget_counts_utf8_bytes) {
    cbm_store_t *store = projection_store();
    ASSERT_NOT_NULL(store);
    int64_t a = projection_node(store, "Function", "projection.a", "a.c", true);
    int64_t b = projection_node(store, "Function", "projection.b", "b.c", false);
    int64_t d = projection_node(store, "Function", "projection.d", "d.c", false);
    char properties[19000];
    size_t used = (size_t)snprintf(properties, sizeof(properties),
                                   "{\"line\":42,\"strategy\":\"unique_name\",\"ignored\":\"");
    for (int i = 0; i < 9000; i++) {
        properties[used++] = (char)0xc3;
        properties[used++] = (char)0xa9;
    }
    memcpy(properties + used, "\"}", 3);
    ASSERT_TRUE(strlen(properties) > 16384);
    cbm_edge_t edge = {.project = "projection",
                       .source_id = a,
                       .target_id = b,
                       .type = "CALLS",
                       .properties_json = properties};
    int64_t oversized = cbm_store_insert_edge(store, &edge);
    edge.source_id = b;
    edge.target_id = d;
    edge.properties_json = "{\"line\":24}";
    int64_t normal = cbm_store_insert_edge(store, &edge);
    cbm_architecture_projection_options_t opts = {.entry_node_id = a, .target_node_id = d};
    char *json = NULL;
    ASSERT_EQ(cbm_store_architecture_projection(store, "projection", &opts, &json), CBM_STORE_OK);
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *edges = yyjson_obj_get(projection_field(doc, "behavior"), "edges");
    ASSERT_EQ(yyjson_arr_size(edges), 2);
    for (size_t i = 0; i < yyjson_arr_size(edges); i++) {
        yyjson_val *value = yyjson_arr_get(edges, i);
        int64_t id = yyjson_get_sint(yyjson_obj_get(value, "id"));
        if (id == oversized) {
            ASSERT_NULL(yyjson_obj_get(value, "callsite"));
            ASSERT_NULL(yyjson_obj_get(value, "resolution"));
        } else {
            ASSERT_EQ(id, normal);
            ASSERT_EQ(yyjson_get_int(yyjson_obj_get(yyjson_obj_get(value, "callsite"), "line")),
                      24);
        }
    }
    yyjson_doc_free(doc);
    free(json);
    cbm_store_close(store);
    PASS();
}

TEST(projection_example_budgets_preserve_full_aggregate_counts) {
    cbm_store_t *store = projection_store();
    ASSERT_NOT_NULL(store);
    projection_node(store, "Package", "projection.a", "a.c", false);
    projection_node(store, "Package", "projection.b", "b.c", false);
    for (int i = 0; i < 5; i++) {
        char source[64], target[64];
        snprintf(source, sizeof(source), "projection.a.caller%d", i);
        snprintf(target, sizeof(target), "projection.b.callee%d", i);
        int64_t a = projection_node(store, "Function", source, "a.c", i == 0);
        int64_t b = projection_node(store, "Function", target, "b.c", false);
        projection_edge(store, a, b, "CALLS");
    }
    char *json = NULL;
    ASSERT_EQ(cbm_store_architecture_projection(store, "projection", NULL, &json), CBM_STORE_OK);
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *parts = projection_field(doc, "components"),
               *deps = projection_field(doc, "dependencies");
    ASSERT_EQ(yyjson_arr_size(parts), 2);
    ASSERT_EQ(yyjson_arr_size(deps), 1);
    yyjson_val *dependency = yyjson_arr_get(deps, 0);
    ASSERT_EQ(yyjson_get_int(yyjson_obj_get(dependency, "count")), 5);
    ASSERT_EQ(yyjson_arr_size(yyjson_obj_get(dependency, "witnesses")), 1);
    ASSERT_EQ(yyjson_get_int(yyjson_obj_get(dependency, "omitted_witnesses")), 4);
    yyjson_val *overview = projection_field(doc, "overview");
    ASSERT_TRUE(yyjson_get_bool(yyjson_obj_get(overview, "complete")));
    yyjson_val *all = yyjson_obj_get(overview, "components"),
               *groups = yyjson_obj_get(overview, "groups");
    ASSERT_EQ(yyjson_arr_size(all), 2);
    ASSERT_EQ(yyjson_arr_size(groups), 1);
    for (size_t i = 0; i < yyjson_arr_size(all); i++) {
        yyjson_val *part = yyjson_arr_get(all, i);
        ASSERT_EQ(yyjson_get_int(yyjson_obj_get(part, "member_count")), 6);
        ASSERT_EQ(yyjson_arr_size(yyjson_obj_get(part, "representatives")), 1);
        ASSERT_EQ(yyjson_get_int(yyjson_obj_get(part, "omitted_members")), 5);
    }
    yyjson_val *group = yyjson_arr_get(groups, 0);
    ASSERT_EQ(yyjson_arr_size(yyjson_obj_get(group, "component_ids")), 2);
    ASSERT_EQ(yyjson_get_int(yyjson_obj_get(group, "member_count")), 12);
    ASSERT_EQ(yyjson_arr_size(yyjson_obj_get(group, "representatives")), 1);
    ASSERT_EQ(yyjson_get_int(yyjson_obj_get(group, "omitted_representatives")), 1);
    yyjson_val *connection = yyjson_arr_get(yyjson_obj_get(overview, "connections"), 0);
    ASSERT_EQ(yyjson_get_int(yyjson_obj_get(connection, "count")), 5);
    ASSERT_EQ(yyjson_arr_size(yyjson_obj_get(connection, "witnesses")), 1);
    ASSERT_EQ(yyjson_get_int(yyjson_obj_get(connection, "omitted_witnesses")), 4);
    yyjson_doc_free(doc);
    free(json);
    cbm_store_close(store);
    PASS();
}

static int64_t projection_node_properties(cbm_store_t *store, const char *qn, const char *file,
                                          const char *properties) {
    cbm_node_t node = {.project = "projection",
                       .label = "Function",
                       .name = qn,
                       .qualified_name = qn,
                       .file_path = file,
                       .start_line = 10,
                       .end_line = 20,
                       .properties_json = properties};
    return cbm_store_upsert_node(store, &node);
}

static int projection_count_metadata_queries(unsigned kind, void *context, void *statement,
                                             void *detail) {
    (void)detail;
    if (kind == SQLITE_TRACE_STMT) {
        const char *sql = sqlite3_sql(statement);
        if (sql && strstr(sql, "FROM nodes WHERE id=?1"))
            (*(int *)context)++;
    }
    return 0;
}

/* Validate every duplicated occurrence, not just the first path or witness. */
static int projection_check_contracts(yyjson_val *value, int64_t symbol_id, int64_t edge_id,
                                      int *symbols, int *edges) {
    if (yyjson_is_obj(value)) {
        if (yyjson_obj_get(value, "qualified_name") &&
            yyjson_get_sint(yyjson_obj_get(value, "id")) == symbol_id) {
            (*symbols)++;
            ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(value, "signature")),
                          "save(value: Value) -> Result");
            ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(value, "return_type")), "Result");
            yyjson_val *parameters = yyjson_obj_get(value, "parameters");
            ASSERT_NOT_NULL(parameters);
            ASSERT_EQ(yyjson_get_sint(yyjson_obj_get(parameters, "count")), 1);
            ASSERT_STR_EQ(yyjson_get_str(yyjson_arr_get(yyjson_obj_get(parameters, "names"), 0)),
                          "value");
            ASSERT_STR_EQ(yyjson_get_str(yyjson_arr_get(yyjson_obj_get(parameters, "types"), 0)),
                          "Value");
        }
        if ((yyjson_obj_get(value, "source_id") &&
             yyjson_get_sint(yyjson_obj_get(value, "id")) == edge_id) ||
            yyjson_get_sint(yyjson_obj_get(value, "edge_id")) == edge_id) {
            (*edges)++;
            yyjson_val *arguments = yyjson_obj_get(value, "arguments");
            ASSERT_EQ(yyjson_arr_size(arguments), 1);
            yyjson_val *argument = yyjson_arr_get(arguments, 0);
            ASSERT_EQ(yyjson_get_sint(yyjson_obj_get(argument, "i")), 7);
            ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(argument, "e")), "input[\"key\"]");
            ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(argument, "v")), "line\nvalue");
            ASSERT_NULL(yyjson_obj_get(argument, "keyword"));
            ASSERT_EQ(yyjson_get_sint(yyjson_obj_get(value, "argument_limit")), 8);
            ASSERT_TRUE(yyjson_is_bool(yyjson_obj_get(value, "arguments_complete")));
            ASSERT_FALSE(yyjson_get_bool(yyjson_obj_get(value, "arguments_complete")));
            ASSERT_EQ(yyjson_get_sint(yyjson_obj_get(yyjson_obj_get(value, "callsite"), "line")),
                      42);
        }
        size_t index, maximum;
        yyjson_val *key, *child;
        yyjson_obj_foreach(value, index, maximum, key,
                           child) if (projection_check_contracts(child, symbol_id, edge_id, symbols,
                                                                 edges)) return 1;
    } else if (yyjson_is_arr(value)) {
        size_t index, maximum;
        yyjson_val *child;
        yyjson_arr_foreach(value, index, maximum,
                           child) if (projection_check_contracts(child, symbol_id, edge_id, symbols,
                                                                 edges)) return 1;
    }
    return 0;
}

TEST(projection_behavior_evidence_is_opt_in_and_shared_across_surfaces) {
    cbm_store_t *store = projection_store();
    ASSERT_NOT_NULL(store);
    projection_node(store, "Package", "projection.a", "", false);
    projection_node(store, "Package", "projection.b", "", false);
    int64_t a = projection_node(store, "Function", "projection.a.start", "src/api/start.c", true);
    int64_t b = projection_node_properties(
        store, "projection.b.save", "src/store/save.c",
        "{\"signature\":\"save(value: Value) -> Result\",\"return_type\":\"Result\","
        "\"param_names\":[\"value\"],\"param_types\":[\"Value\"],\"param_count\":1}");
    cbm_edge_t evidence = {.project = "projection",
                           .source_id = a,
                           .target_id = b,
                           .type = "CALLS",
                           .properties_json =
                               "{\"line\":42,\"args\":[{\"i\":7,\"e\":\"input[\\\"key\\\"]\",\"v\":"
                               "\"line\\nvalue\",\"keyword\":\"value\"}]}"};
    int64_t call = cbm_store_insert_edge(store, &evidence);
    ASSERT_TRUE(call > 0);
    int queries = 0;
    sqlite3_trace_v2(cbm_store_get_db(store), SQLITE_TRACE_STMT, projection_count_metadata_queries,
                     &queries);
    cbm_architecture_projection_options_t opts = {.entry_node_id = a, .target_node_id = b};
    char *json = NULL;
    ASSERT_EQ(cbm_store_architecture_projection(store, "projection", &opts, &json), CBM_STORE_OK);
    ASSERT_NULL(strstr(json, "\"signature\""));
    ASSERT_NULL(strstr(json, "\"return_type\""));
    ASSERT_NULL(strstr(json, "\"parameters\""));
    ASSERT_NULL(strstr(json, "\"arguments\""));
    ASSERT_NULL(strstr(json, "\"argument_limit\""));
    ASSERT_EQ(queries, 0);
    free(json);

    opts.include_behavior_evidence = true;
    ASSERT_EQ(cbm_store_architecture_projection(store, "projection", &opts, &json), CBM_STORE_OK);
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    int symbols = 0, edges = 0;
    ASSERT_EQ(projection_check_contracts(yyjson_doc_get_root(doc), b, call, &symbols, &edges), 0);
    ASSERT_TRUE(symbols >= 3);
    ASSERT_TRUE(edges >= 4);
    ASSERT_TRUE(queries > 0);
    ASSERT_TRUE(queries <= 4); /* cache once per displayed indexed node */
    yyjson_doc_free(doc);
    free(json);

    queries = 0;
    opts.include_behavior_evidence = false;
    ASSERT_EQ(cbm_store_architecture_projection(store, "projection", &opts, &json), CBM_STORE_OK);
    ASSERT_NULL(strstr(json, "\"return_type\""));
    ASSERT_NULL(strstr(json, "\"arguments\""));
    ASSERT_EQ(queries, 0);
    free(json);
    sqlite3_trace_v2(cbm_store_get_db(store), 0, NULL, NULL);
    cbm_store_close(store);
    PASS();
}

TEST(projection_argument_validation_keeps_exact_edge_and_callsite_identity) {
    cbm_store_t *store = projection_store();
    ASSERT_NOT_NULL(store);
    int64_t a = projection_node(store, "Function", "projection.a", "a.c", true);
    int64_t b = projection_node(store, "Function", "projection.b", "b.c", false);
    int64_t target = projection_node(store, "Function", "projection.target", "target.c", false);
    projection_edge(store, a, b, "CALLS");
    cbm_edge_t evidence = {
        .project = "projection",
        .source_id = a,
        .target_id = target,
        .type = "CALLS",
        .properties_json =
            "{\"line\":42,\"args\":[{\"i\":0,\"e\":\"first\",\"v\":\"exact\"},"
            "{\"i\":-1,\"e\":\"negative\"},{\"i\":2},{\"i\":3,\"e\":42},"
            "{\"i\":4,\"e\":\"keep\",\"v\":false},{\"i\":5,\"e\":\"actual\"},"
            "{\"i\":6.5,\"e\":\"float\"},{\"i\":7,\"e\":\"last\"},{\"i\":8,\"e\":\"ninth\"}]}"};
    int64_t first = cbm_store_insert_edge(store, &evidence);
    ASSERT_TRUE(first > 0);
    evidence.source_id = b;
    evidence.properties_json = "{\"line\":84,\"args\":[{\"i\":0,\"e\":\"different\"}]}";
    int64_t second = cbm_store_insert_edge(store, &evidence);
    ASSERT_TRUE(second > 0);
    cbm_architecture_projection_options_t opts = {
        .entry_node_id = a, .target_node_id = target, .include_behavior_evidence = true};
    char *json = NULL;
    ASSERT_EQ(cbm_store_architecture_projection(store, "projection", &opts, &json), CBM_STORE_OK);
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *edges = yyjson_obj_get(projection_field(doc, "behavior"), "edges");
    int seen = 0;
    for (size_t i = 0; i < yyjson_arr_size(edges); i++) {
        yyjson_val *value = yyjson_arr_get(edges, i), *args = yyjson_obj_get(value, "arguments");
        int64_t id = yyjson_get_sint(yyjson_obj_get(value, "id"));
        if (id == first) {
            seen++;
            ASSERT_EQ(yyjson_arr_size(args), 4);
            int indexes[] = {0, 4, 5, 7};
            for (size_t at = 0; at < 4; at++)
                ASSERT_EQ(yyjson_get_sint(yyjson_obj_get(yyjson_arr_get(args, at), "i")),
                          indexes[at]);
            ASSERT_NULL(yyjson_obj_get(yyjson_arr_get(args, 1), "v"));
            ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(yyjson_arr_get(args, 3), "e")), "last");
            ASSERT_EQ(yyjson_get_sint(yyjson_obj_get(yyjson_obj_get(value, "callsite"), "line")),
                      42);
        } else if (id == second) {
            seen++;
            ASSERT_EQ(yyjson_arr_size(args), 1);
            ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(yyjson_arr_get(args, 0), "e")),
                          "different");
            ASSERT_EQ(yyjson_get_sint(yyjson_obj_get(yyjson_obj_get(value, "callsite"), "line")),
                      84);
            ASSERT_STR_EQ(
                yyjson_get_str(yyjson_obj_get(yyjson_obj_get(value, "callsite"), "file_path")),
                "b.c");
        }
    }
    ASSERT_EQ(seen, 2);
    ASSERT_NULL(strstr(json, "ninth"));
    yyjson_doc_free(doc);
    free(json);
    cbm_store_close(store);
    PASS();
}

TEST(projection_missing_corrupt_and_noncall_arguments_remain_unknown) {
    const char *properties[] = {"{}", "{\"args\":\"corrupt\"}", "{\"args\":null}", "{\"args\":{}}",
                                "{\"args\":[]}"};
    for (size_t i = 0; i < sizeof(properties) / sizeof(properties[0]); i++) {
        cbm_store_t *store = projection_store();
        ASSERT_NOT_NULL(store);
        int64_t a = projection_node(store, "Function", "projection.a", "a.c", true);
        int64_t b = projection_node_properties(
            store, "projection.b", "b.c",
            "{\"signature\":42,\"return_type\":\"Known\",\"param_count\":1,\"param_names\":[\"x\"],"
            "\"param_types\":[false]}");
        cbm_edge_t evidence = {.project = "projection",
                               .source_id = a,
                               .target_id = b,
                               .type = "CALLS",
                               .properties_json = properties[i]};
        ASSERT_TRUE(cbm_store_insert_edge(store, &evidence) > 0);
        cbm_architecture_projection_options_t opts = {
            .entry_node_id = a, .target_node_id = b, .include_behavior_evidence = true};
        char *json = NULL;
        ASSERT_EQ(cbm_store_architecture_projection(store, "projection", &opts, &json),
                  CBM_STORE_OK);
        yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
        ASSERT_NOT_NULL(doc);
        yyjson_val *edge =
            yyjson_arr_get(yyjson_obj_get(projection_field(doc, "behavior"), "edges"), 0);
        if (i == 4) {
            ASSERT_TRUE(yyjson_is_arr(yyjson_obj_get(edge, "arguments")));
            ASSERT_EQ(yyjson_arr_size(yyjson_obj_get(edge, "arguments")), 0);
            ASSERT_FALSE(yyjson_get_bool(yyjson_obj_get(edge, "arguments_complete")));
        } else
            ASSERT_NULL(yyjson_obj_get(edge, "arguments"));
        ASSERT_NULL(strstr(json, "\"signature\""));
        ASSERT_NULL(strstr(json, "\"parameters\""));
        ASSERT_NOT_NULL(strstr(json, "\"return_type\":\"Known\""));
        yyjson_doc_free(doc);
        free(json);
        cbm_store_close(store);
    }
    cbm_store_t *store = projection_store();
    ASSERT_NOT_NULL(store);
    int64_t a = projection_node(store, "Function", "projection.a", "a.c", true);
    int64_t b = projection_node(store, "Function", "projection.b", "b.c", false);
    cbm_edge_t evidence = {.project = "projection",
                           .source_id = a,
                           .target_id = b,
                           .type = "HTTP_CALLS",
                           .properties_json =
                               "{\"args\":[{\"i\":0,\"e\":\"not_a_CALLS_property\"}]}"};
    ASSERT_TRUE(cbm_store_insert_edge(store, &evidence) > 0);
    cbm_architecture_projection_options_t opts = {
        .entry_node_id = a, .target_node_id = b, .include_behavior_evidence = true};
    char *json = NULL;
    ASSERT_EQ(cbm_store_architecture_projection(store, "projection", &opts, &json), CBM_STORE_OK);
    ASSERT_NULL(strstr(json, "\"arguments\""));
    free(json);
    cbm_store_close(store);
    PASS();
}

TEST(projection_oversized_evidence_is_omitted_without_truncating_values) {
    cbm_store_t *store = projection_store();
    ASSERT_NOT_NULL(store);
    int64_t a = projection_node(store, "Function", "projection.a", "a.c", true);
    char signature[4098], expression[1026], node_properties[4300], edge_properties[2300];
    memset(signature, 's', sizeof(signature) - 1);
    signature[sizeof(signature) - 1] = '\0';
    memset(expression, 'e', sizeof(expression) - 1);
    expression[sizeof(expression) - 1] = '\0';
    snprintf(node_properties, sizeof(node_properties),
             "{\"signature\":\"%s\",\"return_type\":\"Preserved\"}", signature);
    int64_t b = projection_node_properties(store, "projection.b", "b.c", node_properties);
    snprintf(edge_properties, sizeof(edge_properties),
             "{\"args\":[{\"i\":0,\"e\":\"%s\"},{\"i\":1,\"e\":\"kept\",\"v\":\"%s\"}]}",
             expression, expression);
    cbm_edge_t evidence = {.project = "projection",
                           .source_id = a,
                           .target_id = b,
                           .type = "CALLS",
                           .properties_json = edge_properties};
    ASSERT_TRUE(cbm_store_insert_edge(store, &evidence) > 0);
    cbm_architecture_projection_options_t opts = {
        .entry_node_id = a, .target_node_id = b, .include_behavior_evidence = true};
    char *json = NULL;
    ASSERT_EQ(cbm_store_architecture_projection(store, "projection", &opts, &json), CBM_STORE_OK);
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    ASSERT_NULL(strstr(json, "\"signature\""));
    ASSERT_NOT_NULL(strstr(json, "\"return_type\":\"Preserved\""));
    yyjson_val *edge =
        yyjson_arr_get(yyjson_obj_get(projection_field(doc, "behavior"), "edges"), 0);
    yyjson_val *args = yyjson_obj_get(edge, "arguments");
    ASSERT_EQ(yyjson_arr_size(args), 1);
    yyjson_val *arg = yyjson_arr_get(args, 0);
    ASSERT_EQ(yyjson_get_sint(yyjson_obj_get(arg, "i")), 1);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(arg, "e")), "kept");
    ASSERT_NULL(yyjson_obj_get(arg, "v"));
    ASSERT_FALSE(yyjson_get_bool(yyjson_obj_get(edge, "arguments_complete")));
    yyjson_doc_free(doc);
    free(json);
    cbm_store_close(store);
    PASS();
}

TEST(projection_repeated_behavior_evidence_preserves_base_graph_under_budget) {
    cbm_store_t *store = projection_store();
    ASSERT_NOT_NULL(store);
    enum { components = 80 };
    int64_t symbols[components];
    char signature[4097], expression[1025], properties[4400], arguments[1200];
    memset(signature, 's', sizeof(signature) - 1);
    signature[sizeof(signature) - 1] = '\0';
    memset(expression, 'e', sizeof(expression) - 1);
    expression[sizeof(expression) - 1] = '\0';
    snprintf(properties, sizeof(properties),
             "{\"signature\":\"%s\",\"return_type\":\"Result\",\"is_entry_point\":true}",
             signature);
    snprintf(arguments, sizeof(arguments), "{\"line\":42,\"args\":[{\"i\":0,\"e\":\"%s\"}]}",
             expression);
    for (int i = 0; i < components; i++) {
        char package[80], name[100], file[100];
        snprintf(package, sizeof(package), "projection.package%02d", i);
        snprintf(name, sizeof(name), "%s.function", package);
        snprintf(file, sizeof(file), "src/package%02d/function.c", i);
        ASSERT_TRUE(projection_node(store, "Package", package, "", false) > 0);
        symbols[i] = projection_node_properties(store, name, file, properties);
        ASSERT_TRUE(symbols[i] > 0);
    }
    /* Thousands of witnesses repeat a small indexed declaration set. Its
       uncapped mutable JSON plus serialized copy exceeded the shared budget. */
    for (int from = 0; from < components; from++)
        for (int to = 0; to < components; to++) {
            if (from == to)
                continue;
            cbm_edge_t edge = {.project = "projection",
                               .source_id = symbols[from],
                               .target_id = symbols[to],
                               .type = "CALLS",
                               .properties_json = arguments};
            ASSERT_TRUE(cbm_store_insert_edge(store, &edge) > 0);
        }
    cbm_architecture_projection_options_t opts = {
        .entry_node_id = symbols[0], .target_node_id = symbols[1], .max_depth = 1, .max_paths = 1};
    char *baseline_json = NULL, *enriched_json = NULL;
    ASSERT_EQ(cbm_store_architecture_projection(store, "projection", &opts, &baseline_json),
              CBM_STORE_OK);
    yyjson_doc *baseline = yyjson_read(baseline_json, strlen(baseline_json), 0);
    ASSERT_NOT_NULL(baseline);
    ASSERT_EQ(yyjson_arr_size(projection_field(baseline, "components")), components);
    opts.include_behavior_evidence = true;
    ASSERT_EQ(cbm_store_architecture_projection(store, "projection", &opts, &enriched_json),
              CBM_STORE_OK);
    yyjson_doc *enriched = yyjson_read(enriched_json, strlen(enriched_json), 0);
    ASSERT_NOT_NULL(enriched);
    const char *collections[] = {"components", "dependencies", "entrypoints", "paths"};
    for (size_t i = 0; i < sizeof(collections) / sizeof(collections[0]); i++)
        ASSERT_EQ(yyjson_arr_size(projection_field(enriched, collections[i])),
                  yyjson_arr_size(projection_field(baseline, collections[i])));
    ASSERT_EQ(
        yyjson_get_sint(yyjson_obj_get(projection_field(enriched, "totals"), "accounted_nodes")),
        yyjson_get_sint(yyjson_obj_get(projection_field(baseline, "totals"), "accounted_nodes")));
    ASSERT_NULL(strstr(enriched_json, "The architecture response exceeded its memory budget"));
    yyjson_val *path = yyjson_arr_get(projection_field(enriched, "paths"), 0);
    yyjson_val *source = yyjson_arr_get(yyjson_obj_get(path, "nodes"), 0);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(source, "signature")), signature);
    yyjson_val *edge = yyjson_arr_get(yyjson_obj_get(path, "edges"), 0);
    yyjson_val *argument = yyjson_arr_get(yyjson_obj_get(edge, "arguments"), 0);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(argument, "e")), expression);
    ASSERT_NOT_NULL(strstr(enriched_json, "Optional behavior evidence was omitted"));
    yyjson_doc_free(baseline);
    yyjson_doc_free(enriched);
    free(baseline_json);
    free(enriched_json);
    cbm_store_close(store);
    PASS();
}

SUITE(architecture_projection) {
    RUN_TEST(projection_accounts_for_isolated_nodes_and_files);
    RUN_TEST(projection_preserves_typed_edges_and_contiguous_paths);
    RUN_TEST(projection_reports_dependency_cycles_without_fabricated_execution);
    RUN_TEST(projection_graph_limits_abstain_instead_of_sampling);
    RUN_TEST(projection_cancellation_deadline_and_store_reuse);
    RUN_TEST(projection_display_caps_preserve_total_accounting);
    RUN_TEST(projection_unknown_project_and_empty_project);
    RUN_TEST(projection_paths_depth_cap_is_explicit);
    RUN_TEST(projection_channel_relationships_are_typed_not_fake_calls);
    RUN_TEST(projection_test_callers_cannot_absorb_source_components);
    RUN_TEST(projection_indexed_embedded_test_has_separate_membership);
    RUN_TEST(projection_shallow_leaves_do_not_hide_deeper_behavior);
    RUN_TEST(projection_behavior_preserves_first_hop_diversity);
    RUN_TEST(projection_corridor_preserves_joins_cycles_and_callsite_evidence);
    RUN_TEST(projection_corridor_caps_abstain_without_hiding_joins);
    RUN_TEST(projection_overview_accounts_for_components_beyond_legacy_cap);
    RUN_TEST(projection_aggregate_cycle_does_not_invent_symbol_cycle);
    RUN_TEST(projection_provenance_budget_counts_utf8_bytes);
    RUN_TEST(projection_example_budgets_preserve_full_aggregate_counts);
    RUN_TEST(projection_behavior_evidence_is_opt_in_and_shared_across_surfaces);
    RUN_TEST(projection_argument_validation_keeps_exact_edge_and_callsite_identity);
    RUN_TEST(projection_missing_corrupt_and_noncall_arguments_remain_unknown);
    RUN_TEST(projection_oversized_evidence_is_omitted_without_truncating_values);
    RUN_TEST(projection_repeated_behavior_evidence_preserves_base_graph_under_budget);
}
