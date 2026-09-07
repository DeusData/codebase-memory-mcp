/*
 * test_ui.c — Tests for the graph visualization UI module.
 *
 * Covers: config persistence, embedded asset lookup, layout engine.
 */
#include "../src/foundation/compat.h"
#include "../src/foundation/compat_fs.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "ui/config.h"
#include "ui/embedded_assets.h"
#include "ui/layout3d.h"
#include "ui/atlas.h"
#include "store/store.h"

#include <yyjson/yyjson.h>
#ifdef _WIN32
#include "foundation/win_utf8.h"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifndef _WIN32
#include <unistd.h>
#include <sys/wait.h>
#endif

/* ── Config tests ─────────────────────────────────────────────── */

TEST(config_load_defaults) {
    /* Loading with no config file should give defaults */
    cbm_ui_config_t cfg;
    cfg.ui_enabled = true; /* set non-default to verify load overwrites */
    cfg.ui_port = 1234;

    /* Use a temp HOME to avoid touching real config */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_test_config_XXXXXX");
    char *td = cbm_mkdtemp(tmpdir);
    ASSERT_NOT_NULL(td);

    char *old_home = getenv("HOME") ? strdup(getenv("HOME")) : NULL;
    cbm_setenv("HOME", td, 1);

    cbm_ui_config_load(&cfg);

    ASSERT_FALSE(cfg.ui_enabled);
    ASSERT_EQ(cfg.ui_port, 9749);

    /* Restore HOME */
    if (old_home) {
        cbm_setenv("HOME", old_home, 1);
        free(old_home);
    }

    PASS();
}

TEST(config_save_and_reload) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_test_config_XXXXXX");
    char *td = cbm_mkdtemp(tmpdir);
    ASSERT_NOT_NULL(td);

    char *old_home = getenv("HOME") ? strdup(getenv("HOME")) : NULL;
    cbm_setenv("HOME", td, 1);

    /* Save */
    cbm_ui_config_t cfg = {.ui_enabled = true, .ui_port = 8080};
    ASSERT_TRUE(cbm_ui_config_save(&cfg));

    /* Reload */
    cbm_ui_config_t loaded;
    cbm_ui_config_load(&loaded);

    ASSERT_TRUE(loaded.ui_enabled);
    ASSERT_EQ(loaded.ui_port, 8080);

    if (old_home) {
        cbm_setenv("HOME", old_home, 1);
        free(old_home);
    }

    PASS();
}

TEST(config_save_atomically_replaces_a_complete_generation) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_test_config_atomic_XXXXXX");
    char *td = cbm_mkdtemp(tmpdir);
    ASSERT_NOT_NULL(td);

    char *old_cache = getenv("CBM_CACHE_DIR") ? strdup(getenv("CBM_CACHE_DIR")) : NULL;
    ASSERT_EQ(cbm_setenv("CBM_CACHE_DIR", td, 1), 0);

    cbm_ui_config_t old_generation = {
        .ui_enabled = false,
        .ui_port = 11111,
    };
    ASSERT_TRUE(cbm_ui_config_save(&old_generation));

    char path[1024];
    cbm_ui_config_path(path, (int)sizeof(path));
#ifdef _WIN32
    wchar_t *wide_path = cbm_utf8_to_wide(path);
    ASSERT_NOT_NULL(wide_path);
    HANDLE old_handle =
        CreateFileW(wide_path, GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                    NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
    free(wide_path);
    ASSERT_TRUE(old_handle != INVALID_HANDLE_VALUE);
#else
    FILE *old_handle = cbm_fopen(path, "rb");
    ASSERT_NOT_NULL(old_handle);
#endif

    cbm_ui_config_t new_generation = {
        .ui_enabled = true,
        .ui_port = 22222,
    };
    /* Capture everything, clean up, and only then assert: an assert firing
     * before the env restore leaks CBM_CACHE_DIR into every later test in the
     * process, turning one regression into a cascade. */
    bool saved = cbm_ui_config_save(&new_generation);

    char old_bytes[512] = {0};
#ifdef _WIN32
    DWORD old_length = 0;
    bool old_read =
        ReadFile(old_handle, old_bytes, (DWORD)sizeof(old_bytes) - 1U, &old_length, NULL) != 0 &&
        old_length > 0;
    bool old_closed = CloseHandle(old_handle) != 0;
#else
    size_t old_length = fread(old_bytes, 1, sizeof(old_bytes) - 1, old_handle);
    bool old_read = old_length > 0;
    bool old_closed = fclose(old_handle) == 0;
#endif

    cbm_ui_config_t loaded = {0};
    cbm_ui_config_load(&loaded);

    if (old_cache) {
        (void)cbm_setenv("CBM_CACHE_DIR", old_cache, 1);
    } else {
        (void)cbm_unsetenv("CBM_CACHE_DIR");
    }
    free(old_cache);
    (void)th_rmtree(td);

    ASSERT_TRUE(saved);
    ASSERT_TRUE(old_read);
    ASSERT_TRUE(old_closed);
    /* An in-place truncate/rewrite mutates the already-open handle. Atomic
     * replacement leaves it attached to the complete prior generation. */
    ASSERT_NOT_NULL(strstr(old_bytes, "11111"));
    ASSERT_NULL(strstr(old_bytes, "22222"));
    ASSERT_TRUE(loaded.ui_enabled);
    ASSERT_EQ(loaded.ui_port, 22222);
    PASS();
}

TEST(config_overwrite) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_test_config_XXXXXX");
    char *td = cbm_mkdtemp(tmpdir);
    ASSERT_NOT_NULL(td);

    char *old_home = getenv("HOME") ? strdup(getenv("HOME")) : NULL;
    cbm_setenv("HOME", td, 1);

    /* Save with ui_enabled=true */
    cbm_ui_config_t cfg1 = {.ui_enabled = true, .ui_port = 9749};
    ASSERT_TRUE(cbm_ui_config_save(&cfg1));

    /* Overwrite with ui_enabled=false */
    cbm_ui_config_t cfg2 = {.ui_enabled = false, .ui_port = 9749};
    ASSERT_TRUE(cbm_ui_config_save(&cfg2));

    /* Reload should show false */
    cbm_ui_config_t loaded;
    cbm_ui_config_load(&loaded);
    ASSERT_FALSE(loaded.ui_enabled);

    if (old_home) {
        cbm_setenv("HOME", old_home, 1);
        free(old_home);
    }

    PASS();
}

TEST(config_corrupt_file) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_test_config_XXXXXX");
    char *td = cbm_mkdtemp(tmpdir);
    ASSERT_NOT_NULL(td);

    char *old_home = getenv("HOME") ? strdup(getenv("HOME")) : NULL;
    cbm_setenv("HOME", td, 1);

    /* Write garbage to config path */
    char path[1024];
    cbm_ui_config_path(path, (int)sizeof(path));

    /* Ensure directory exists (portable — no system("mkdir -p")) */
    char dir[1024];
    snprintf(dir, sizeof(dir), "%s/.cache/codebase-memory-mcp", td);
    cbm_mkdir_p(dir, 0755);

    FILE *f = fopen(path, "w");
    ASSERT_NOT_NULL(f);
    fprintf(f, "this is not json!!!");
    fclose(f);

    /* Should load defaults, not crash */
    cbm_ui_config_t cfg;
    cbm_ui_config_load(&cfg);
    ASSERT_FALSE(cfg.ui_enabled);
    ASSERT_EQ(cfg.ui_port, 9749);

    if (old_home) {
        cbm_setenv("HOME", old_home, 1);
        free(old_home);
    }

    PASS();
}

TEST(config_missing_fields) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_test_config_XXXXXX");
    char *td = cbm_mkdtemp(tmpdir);
    ASSERT_NOT_NULL(td);

    char *old_home = getenv("HOME") ? strdup(getenv("HOME")) : NULL;
    cbm_setenv("HOME", td, 1);

    /* Write JSON with only ui_port */
    char path[1024];
    cbm_ui_config_path(path, (int)sizeof(path));

    char dir[1024];
    snprintf(dir, sizeof(dir), "%s/.cache/codebase-memory-mcp", td);
    cbm_mkdir_p(dir, 0755);

    FILE *f = fopen(path, "w");
    ASSERT_NOT_NULL(f);
    fprintf(f, "{\"ui_port\": 5555}");
    fclose(f);

    cbm_ui_config_t cfg;
    cbm_ui_config_load(&cfg);
    ASSERT_FALSE(cfg.ui_enabled); /* defaults for missing field */
    ASSERT_EQ(cfg.ui_port, 5555); /* present field loaded */

    if (old_home) {
        cbm_setenv("HOME", old_home, 1);
        free(old_home);
    }

    PASS();
}

/* ── Embedded asset tests ─────────────────────────────────────── */

TEST(embedded_lookup_not_found) {
    /* With stub, everything should return NULL */
    const cbm_embedded_file_t *f = cbm_embedded_lookup("/nonexistent");
    ASSERT_NULL(f);
    PASS();
}

TEST(embedded_stub_count) {
    /* Stub should have 0 files */
    ASSERT_EQ(CBM_EMBEDDED_FILE_COUNT, 0);
    PASS();
}

/* ── Layout tests ─────────────────────────────────────────────── */

TEST(layout_empty_graph) {
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);

    /* No nodes in store → empty result */
    cbm_layout_result_t *r =
        cbm_layout_compute(store, "test-project", CBM_LAYOUT_OVERVIEW, NULL, 0, 100);
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(r->node_count, 0);
    ASSERT_EQ(r->edge_count, 0);

    cbm_layout_free(r);
    cbm_store_close(store);
    PASS();
}

TEST(layout_single_node) {
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);

    cbm_store_upsert_project(store, "test", "/tmp/test");
    cbm_node_t node = {
        .project = "test",
        .label = "Function",
        .name = "main",
        .qualified_name = "test::main",
        .file_path = "main.c",
        .start_line = 1,
        .end_line = 10,
    };
    int64_t id = cbm_store_upsert_node(store, &node);
    ASSERT_GT(id, 0);

    cbm_layout_result_t *r = cbm_layout_compute(store, "test", CBM_LAYOUT_OVERVIEW, NULL, 0, 100);
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(r->node_count, 1);
    ASSERT_STR_EQ(r->nodes[0].name, "main");
    ASSERT_EQ(r->total_nodes, 1);

    cbm_layout_free(r);
    cbm_store_close(store);
    PASS();
}

TEST(layout_two_connected) {
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);

    cbm_store_upsert_project(store, "test", "/tmp/test");

    cbm_node_t n1 = {.project = "test",
                     .label = "Function",
                     .name = "foo",
                     .qualified_name = "test::foo",
                     .file_path = "a.c",
                     .start_line = 1,
                     .end_line = 5};
    cbm_node_t n2 = {.project = "test",
                     .label = "Function",
                     .name = "bar",
                     .qualified_name = "test::bar",
                     .file_path = "b.c",
                     .start_line = 1,
                     .end_line = 5};
    int64_t id1 = cbm_store_upsert_node(store, &n1);
    int64_t id2 = cbm_store_upsert_node(store, &n2);

    cbm_edge_t edge = {.project = "test", .source_id = id1, .target_id = id2, .type = "CALLS"};
    cbm_store_insert_edge(store, &edge);

    cbm_layout_result_t *r = cbm_layout_compute(store, "test", CBM_LAYOUT_OVERVIEW, NULL, 0, 100);
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(r->node_count, 2);

    /* Nodes should be positioned apart (not at same point) */
    float dx = r->nodes[0].x - r->nodes[1].x;
    float dy = r->nodes[0].y - r->nodes[1].y;
    float dz = r->nodes[0].z - r->nodes[1].z;
    float dist = sqrtf(dx * dx + dy * dy + dz * dz);
    ASSERT_GT((long long)(dist * 100), 0);

    ASSERT_EQ(r->edge_count, 1);

    cbm_layout_free(r);
    cbm_store_close(store);
    PASS();
}

TEST(layout_respects_max_nodes) {
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);

    cbm_store_upsert_project(store, "test", "/tmp/test");

    /* Insert 20 nodes */
    for (int i = 0; i < 20; i++) {
        char name[32], qn[64];
        snprintf(name, sizeof(name), "fn%d", i);
        snprintf(qn, sizeof(qn), "test::fn%d", i);
        cbm_node_t n = {.project = "test",
                        .label = "Function",
                        .name = name,
                        .qualified_name = qn,
                        .file_path = "a.c",
                        .start_line = i,
                        .end_line = i + 1};
        cbm_store_upsert_node(store, &n);
    }

    /* max_nodes=5 should return at most 5 */
    cbm_layout_result_t *r = cbm_layout_compute(store, "test", CBM_LAYOUT_OVERVIEW, NULL, 0, 5);
    ASSERT_NOT_NULL(r);
    ASSERT_LTE(r->node_count, 5);
    ASSERT_EQ(r->total_nodes, 20);

    cbm_layout_free(r);
    cbm_store_close(store);
    PASS();
}

TEST(layout_clamps_render_cap_from_env) {
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);

    const char *old_raw = getenv("CBM_UI_MAX_RENDER_NODES");
    char *old_cap = old_raw ? strdup(old_raw) : NULL;
    cbm_setenv("CBM_UI_MAX_RENDER_NODES", "25", 1);

    cbm_store_upsert_project(store, "test", "/tmp/test");

    for (int i = 0; i < 40; i++) {
        char name[32], qn[64];
        snprintf(name, sizeof(name), "fn%d", i);
        snprintf(qn, sizeof(qn), "test::fn%d", i);
        cbm_node_t n = {.project = "test",
                        .label = "Function",
                        .name = name,
                        .qualified_name = qn,
                        .file_path = "a.c",
                        .start_line = i,
                        .end_line = i + 1};
        cbm_store_upsert_node(store, &n);
    }

    cbm_layout_result_t *r = cbm_layout_compute(store, "test", CBM_LAYOUT_OVERVIEW, NULL, 0, 50000);
    ASSERT_NOT_NULL(r);
    ASSERT_LTE(r->node_count, 25);
    ASSERT_EQ(r->total_nodes, 40);

    cbm_layout_free(r);
    cbm_store_close(store);
    if (old_cap) {
        cbm_setenv("CBM_UI_MAX_RENDER_NODES", old_cap, 1);
        free(old_cap);
    } else {
        cbm_unsetenv("CBM_UI_MAX_RENDER_NODES");
    }
    PASS();
}

/* A caller-requested budget above the default must be honored (up to the hard
 * ceiling) when no env cap is set — the default is a default, not a ceiling. */
TEST(layout_honors_budget_above_default) {
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);

    const char *old_raw = getenv("CBM_UI_MAX_RENDER_NODES");
    char *old_cap = old_raw ? strdup(old_raw) : NULL;
    cbm_unsetenv("CBM_UI_MAX_RENDER_NODES");

    cbm_store_upsert_project(store, "test", "/tmp/test");

    enum { BUDGET_NODES = 5100 };
    for (int i = 0; i < BUDGET_NODES; i++) {
        char name[32], qn[64];
        snprintf(name, sizeof(name), "fn%d", i);
        snprintf(qn, sizeof(qn), "test::fn%d", i);
        cbm_node_t n = {.project = "test",
                        .label = "Function",
                        .name = name,
                        .qualified_name = qn,
                        .file_path = "a.c",
                        .start_line = i,
                        .end_line = i + 1};
        cbm_store_upsert_node(store, &n);
    }

    cbm_layout_result_t *r =
        cbm_layout_compute(store, "test", CBM_LAYOUT_OVERVIEW, NULL, 0, BUDGET_NODES);
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(r->node_count, BUDGET_NODES);
    ASSERT_EQ(r->total_nodes, BUDGET_NODES);

    cbm_layout_free(r);
    cbm_store_close(store);
    if (old_cap) {
        cbm_setenv("CBM_UI_MAX_RENDER_NODES", old_cap, 1);
        free(old_cap);
    }
    PASS();
}

TEST(layout_deterministic) {
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);

    cbm_store_upsert_project(store, "test", "/tmp/test");

    cbm_node_t n1 = {.project = "test",
                     .label = "Function",
                     .name = "alpha",
                     .qualified_name = "test::alpha",
                     .file_path = "a.c",
                     .start_line = 1,
                     .end_line = 5};
    cbm_node_t n2 = {.project = "test",
                     .label = "Function",
                     .name = "beta",
                     .qualified_name = "test::beta",
                     .file_path = "b.c",
                     .start_line = 1,
                     .end_line = 5};
    cbm_store_upsert_node(store, &n1);
    cbm_store_upsert_node(store, &n2);

    /* Run twice, check positions match */
    cbm_layout_result_t *r1 = cbm_layout_compute(store, "test", CBM_LAYOUT_OVERVIEW, NULL, 0, 100);
    cbm_layout_result_t *r2 = cbm_layout_compute(store, "test", CBM_LAYOUT_OVERVIEW, NULL, 0, 100);
    ASSERT_NOT_NULL(r1);
    ASSERT_NOT_NULL(r2);
    ASSERT_EQ(r1->node_count, r2->node_count);

    for (int i = 0; i < r1->node_count; i++) {
        ASSERT_FLOAT_EQ(r1->nodes[i].x, r2->nodes[i].x, 0.001);
        ASSERT_FLOAT_EQ(r1->nodes[i].y, r2->nodes[i].y, 0.001);
        ASSERT_FLOAT_EQ(r1->nodes[i].z, r2->nodes[i].z, 0.001);
    }

    cbm_layout_free(r1);
    cbm_layout_free(r2);
    cbm_store_close(store);
    PASS();
}

TEST(layout_to_json) {
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);

    cbm_store_upsert_project(store, "test", "/tmp/test");

    cbm_node_t n = {.project = "test",
                    .label = "Function",
                    .name = "hello",
                    .qualified_name = "test::hello",
                    .file_path = "a.c",
                    .start_line = 1,
                    .end_line = 5};
    cbm_store_upsert_node(store, &n);

    cbm_layout_result_t *r = cbm_layout_compute(store, "test", CBM_LAYOUT_OVERVIEW, NULL, 0, 100);
    ASSERT_NOT_NULL(r);

    char *json = cbm_layout_to_json(r);
    ASSERT_NOT_NULL(json);

    /* Should contain key fields */
    ASSERT(strstr(json, "\"nodes\"") != NULL);
    ASSERT(strstr(json, "\"edges\"") != NULL);
    ASSERT(strstr(json, "\"total_nodes\"") != NULL);
    ASSERT(strstr(json, "\"hello\"") != NULL);
    ASSERT(strstr(json, "\"Function\"") != NULL);

    free(json);
    cbm_layout_free(r);
    cbm_store_close(store);
    PASS();
}

TEST(layout_null_inputs) {
    /* NULL store → NULL result */
    cbm_layout_result_t *r = cbm_layout_compute(NULL, "test", CBM_LAYOUT_OVERVIEW, NULL, 0, 100);
    ASSERT_NULL(r);

    /* NULL project → NULL result */
    cbm_store_t *store = cbm_store_open_memory();
    r = cbm_layout_compute(store, NULL, CBM_LAYOUT_OVERVIEW, NULL, 0, 100);
    ASSERT_NULL(r);

    /* cbm_layout_free(NULL) should not crash */
    cbm_layout_free(NULL);

    /* cbm_layout_to_json(NULL) should return NULL */
    char *json = cbm_layout_to_json(NULL);
    ASSERT_NULL(json);

    cbm_store_close(store);
    PASS();
}

/* ── Dead-code classification (distilled from PR #789) ────────── */

static const cbm_layout_node_t *find_layout_node(const cbm_layout_result_t *r, const char *name) {
    for (int i = 0; i < r->node_count; i++) {
        if (r->nodes[i].name && strcmp(r->nodes[i].name, name) == 0) {
            return &r->nodes[i];
        }
    }
    return NULL;
}

/* A function with zero callers/usages and no entry/test/exported flag is
 * "dead"; entry-point, test, and exported functions are NOT dead even at zero
 * callers; a called function reports its true full-graph incoming CALLS degree
 * ("single" at 1, "normal" at >=2). Non-Function labels are "structural". */
TEST(layout_dead_code_classification) {
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);
    ASSERT_EQ(cbm_store_upsert_project(store, "dc", "/tmp/dc"), CBM_STORE_OK);

    /* Candidates (Function, non-test path unless noted). */
    cbm_node_t dead = {.project = "dc",
                       .label = "Function",
                       .name = "deadfn",
                       .qualified_name = "dc::deadfn",
                       .file_path = "src/a.c",
                       .properties_json = "{\"is_entry_point\":false,\"is_test\":false,"
                                          "\"is_exported\":false}"};
    cbm_node_t entry = {.project = "dc",
                        .label = "Function",
                        .name = "entryfn",
                        .qualified_name = "dc::entryfn",
                        .file_path = "src/b.c",
                        .properties_json = "{\"is_entry_point\":true}"};
    cbm_node_t tst = {.project = "dc",
                      .label = "Function",
                      .name = "testfn",
                      .qualified_name = "dc::testfn",
                      .file_path = "src/c.c",
                      .properties_json = "{\"is_test\":true}"};
    cbm_node_t tstpath = {.project = "dc",
                          .label = "Function",
                          .name = "bypathfn",
                          .qualified_name = "dc::bypathfn",
                          .file_path = "tests/mod_helpers.c",
                          .properties_json = "{}"};
    cbm_node_t exp = {.project = "dc",
                      .label = "Function",
                      .name = "exportedfn",
                      .qualified_name = "dc::exportedfn",
                      .file_path = "src/d.c",
                      .properties_json = "{\"is_exported\":true}"};
    cbm_node_t single = {.project = "dc",
                         .label = "Function",
                         .name = "calledonce",
                         .qualified_name = "dc::calledonce",
                         .file_path = "src/e.c",
                         .properties_json = "{}"};
    cbm_node_t norm = {.project = "dc",
                       .label = "Function",
                       .name = "callednormal",
                       .qualified_name = "dc::callednormal",
                       .file_path = "src/f.c",
                       .properties_json = "{}"};
    cbm_node_t caller = {.project = "dc",
                         .label = "Function",
                         .name = "caller",
                         .qualified_name = "dc::caller",
                         .file_path = "src/g.c",
                         .properties_json = "{}"};
    /* A structural (non-Function) node is never a dead-code candidate. */
    cbm_node_t cls = {.project = "dc",
                      .label = "Class",
                      .name = "SomeClass",
                      .qualified_name = "dc::SomeClass",
                      .file_path = "src/h.c",
                      .properties_json = "{}"};

    int64_t id_dead = cbm_store_upsert_node(store, &dead);
    cbm_store_upsert_node(store, &entry);
    cbm_store_upsert_node(store, &tst);
    cbm_store_upsert_node(store, &tstpath);
    cbm_store_upsert_node(store, &exp);
    int64_t id_single = cbm_store_upsert_node(store, &single);
    int64_t id_norm = cbm_store_upsert_node(store, &norm);
    int64_t id_caller = cbm_store_upsert_node(store, &caller);
    cbm_store_upsert_node(store, &cls);
    ASSERT_GT(id_dead, 0);

    /* calledonce ← 1 CALLS; callednormal ← 2 CALLS (full-graph inbound). */
    cbm_edge_t e1 = {
        .project = "dc", .source_id = id_caller, .target_id = id_single, .type = "CALLS"};
    cbm_edge_t e2 = {
        .project = "dc", .source_id = id_caller, .target_id = id_norm, .type = "CALLS"};
    cbm_edge_t e3 = {.project = "dc", .source_id = id_dead, .target_id = id_norm, .type = "CALLS"};
    cbm_store_insert_edge(store, &e1);
    cbm_store_insert_edge(store, &e2);
    cbm_store_insert_edge(store, &e3);

    cbm_layout_result_t *r = cbm_layout_compute(store, "dc", CBM_LAYOUT_OVERVIEW, NULL, 0, 100);
    ASSERT_NOT_NULL(r);

    const cbm_layout_node_t *ln;

    ln = find_layout_node(r, "deadfn");
    ASSERT_NOT_NULL(ln);
    ASSERT_STR_EQ(ln->status, "dead");
    ASSERT_EQ(ln->in_calls, 0);

    ln = find_layout_node(r, "entryfn");
    ASSERT_NOT_NULL(ln);
    ASSERT_STR_EQ(ln->status, "entry");

    ln = find_layout_node(r, "testfn");
    ASSERT_NOT_NULL(ln);
    ASSERT_STR_EQ(ln->status, "test");

    ln = find_layout_node(r, "bypathfn"); /* test detected via file path */
    ASSERT_NOT_NULL(ln);
    ASSERT_STR_EQ(ln->status, "test");

    ln = find_layout_node(r, "exportedfn");
    ASSERT_NOT_NULL(ln);
    ASSERT_STR_EQ(ln->status, "exported");

    ln = find_layout_node(r, "calledonce");
    ASSERT_NOT_NULL(ln);
    ASSERT_STR_EQ(ln->status, "single");
    ASSERT_EQ(ln->in_calls, 1);

    ln = find_layout_node(r, "callednormal");
    ASSERT_NOT_NULL(ln);
    ASSERT_STR_EQ(ln->status, "normal");
    ASSERT_EQ(ln->in_calls, 2);

    ln = find_layout_node(r, "SomeClass");
    ASSERT_NOT_NULL(ln);
    ASSERT_STR_EQ(ln->status, "structural");

    /* The classification must survive JSON serialization. */
    char *json = cbm_layout_to_json(r);
    ASSERT_NOT_NULL(json);
    ASSERT(strstr(json, "\"status\":\"dead\"") != NULL);
    ASSERT(strstr(json, "\"in_calls\":2") != NULL);
    free(json);

    cbm_layout_free(r);
    cbm_store_close(store);
    PASS();
}

/* ── Octree recursion guard (distilled from PR #821; refs #498/#726/#402) ── */

/* Bodies that share a position made octree_insert subdivide forever — the
 * cell around them shrinks but never separates them, so one octree cell is
 * calloc'd per level until the process dies (stack overflow) or freezes the
 * machine allocating (the 34GB-swap reports). Fixed by the depth/half-size
 * floor in src/ui/layout3d.c (OCTREE_MAX_DEPTH / OCTREE_MIN_HALF).
 *
 * Coincident positions are reachable through the public layout API: layout3d
 * anchors each node by fnv1a(file cluster key) and jitters it with a PRNG
 * seeded by fnv1a(qualified_name). The three QNs below are distinct strings
 * with IDENTICAL 32-bit FNV-1a hashes (0x06bb012e, found by offline brute
 * force), so in the same file they get bit-identical positions on every
 * platform (integer hashing only — no libm in the coincidence path).
 *
 * A literal sub-ULP-separated pair cannot be constructed through the public
 * API: same-anchor positions are quantized to exact multiples of the jitter
 * quantum (5/4096 — exactly 20 ULP at anchor magnitude ~600), and
 * cross-anchor separations depend on the platform's cosf/sinf bits. Exact
 * coincidence is the API-reachable degenerate input, and it necessarily
 * drives the recursion through the sub-ULP regime: half_size falls below
 * ULP(center) with the bodies still unseparated, freezing child centers
 * while cells keep being allocated.
 */
#if !defined(_WIN32)
/* Child body: builds the store and runs the layout so a crash or hang cannot
 * take down the runner (alarm bounds a hang, fork isolates a SIGSEGV).
 * Deliberately NO memory rlimit: under a rlimit a failing calloc makes
 * octree_insert silently truncate and the UNFIXED code would complete —
 * turning this guard vacuously green. The alarm alone bounds the runaway.
 * Exit codes: 0 ok, 2 store setup, 3 layout NULL, 4 node count/lookup,
 * 5 fixture no longer coincident, 6 non-finite coordinate. Never returns. */
static void layout_octree_guard_child(void) {
    alarm(5); /* post-fix the whole child runs in milliseconds */
    cbm_store_t *store = cbm_store_open_memory();
    if (!store)
        _exit(2);
    if (cbm_store_upsert_project(store, "test", "/tmp/test") != CBM_STORE_OK)
        _exit(2);

    /* Distinct QNs, one fnv1a hash — coincident after anchor + jitter. */
    static const char *cqn[3] = {"test::octree_c5988474", "test::octree_c11394919",
                                 "test::octree_c33141700"};
    for (int i = 0; i < 3; i++) {
        char name[32];
        snprintf(name, sizeof(name), "co%d", i);
        cbm_node_t n = {.project = "test",
                        .label = "Function",
                        .name = name,
                        .qualified_name = cqn[i],
                        .file_path = "pkg/sub/mod/a.c",
                        .start_line = i + 1,
                        .end_line = i + 2};
        if (cbm_store_upsert_node(store, &n) <= 0)
            _exit(2);
    }
    /* A few normally-spread nodes so the octree root box has realistic
     * (non-degenerate) extent, as in the reported repositories. */
    for (int i = 0; i < 3; i++) {
        char name[32], qn[64], fp[32];
        snprintf(name, sizeof(name), "fn%d", i);
        snprintf(qn, sizeof(qn), "test::spread_fn%d", i);
        snprintf(fp, sizeof(fp), "dir%d/f%d.c", i, i);
        cbm_node_t n = {.project = "test",
                        .label = "Function",
                        .name = name,
                        .qualified_name = qn,
                        .file_path = fp,
                        .start_line = 1,
                        .end_line = 2};
        if (cbm_store_upsert_node(store, &n) <= 0)
            _exit(2);
    }

    cbm_layout_result_t *r = cbm_layout_compute(store, "test", CBM_LAYOUT_OVERVIEW, NULL, 0, 100);
    if (!r)
        _exit(3);
    if (r->node_count != 6)
        _exit(4);

    /* The colliding QNs must actually be coincident — identical output
     * coordinates (identical seeds → identical positions, and coincident
     * bodies receive identical forces every iteration, so they stay
     * together). If a seeding change ever breaks this, the fixture no longer
     * reproduces the bug: fail loudly instead of going vacuously green. */
    int ci[3], nc = 0;
    for (int i = 0; i < r->node_count && nc < 3; i++) {
        if (r->nodes[i].qualified_name &&
            strncmp(r->nodes[i].qualified_name, "test::octree_c", 14) == 0)
            ci[nc++] = i;
    }
    if (nc != 3)
        _exit(4);
    for (int k = 1; k < 3; k++) {
        if (r->nodes[ci[k]].x != r->nodes[ci[0]].x || r->nodes[ci[k]].y != r->nodes[ci[0]].y ||
            r->nodes[ci[k]].z != r->nodes[ci[0]].z)
            _exit(5);
    }
    for (int i = 0; i < r->node_count; i++) {
        if (!isfinite(r->nodes[i].x) || !isfinite(r->nodes[i].y) || !isfinite(r->nodes[i].z))
            _exit(6);
    }

    cbm_layout_free(r);
    cbm_store_close(store);
    _exit(0);
}
#endif

TEST(layout_coincident_nodes_bounded) {
#if defined(_WIN32)
    SKIP_PLATFORM("fork/alarm not available; POSIX-only bounded-hang reproduction");
#else
    fflush(NULL);
    pid_t pid = fork();
    if (pid < 0)
        FAIL("fork() failed");
    if (pid == 0)
        layout_octree_guard_child(); /* never returns */

    int status = 0;
    (void)waitpid(pid, &status, 0);

    /* Unfixed code dies here: SIGSEGV (unbounded recursion overflowing the
     * stack) or SIGALRM (tail-call-optimized allocation runaway cut off by
     * the child's alarm). Fixed code exits 0 well within the budget. */
    ASSERT_FALSE(WIFSIGNALED(status));
    ASSERT_TRUE(WIFEXITED(status));
    ASSERT_EQ(WEXITSTATUS(status), 0);
    PASS();
#endif
}

/* ── CBM Atlas region level ───────────────────────────────────── */

/* Two call communities in two folders, one cross call. The region level must
 * find both, name them by their dominant folder, aggregate the cross edge,
 * and lay them out deterministically. */
static cbm_store_t *regions_fixture(void) {
    cbm_store_t *store = cbm_store_open_memory();
    if (!store)
        return NULL;
    cbm_store_upsert_project(store, "regions-test", "/tmp/regions-test");
    static const struct {
        const char *name;
        const char *file;
    } defs[] = {
        {"alpha_one", "src/alpha/a1.c"},   {"alpha_two", "src/alpha/a1.c"},
        {"alpha_three", "src/alpha/a2.c"}, {"beta_one", "src/beta/b1.c"},
        {"beta_two", "src/beta/b1.c"},     {"beta_three", "src/beta/b2.c"},
    };
    int64_t ids[6];
    for (int i = 0; i < 6; i++) {
        cbm_node_t node;
        memset(&node, 0, sizeof(node));
        node.project = "regions-test";
        node.label = "Function";
        node.name = defs[i].name;
        char qn[128];
        snprintf(qn, sizeof(qn), "regions-test::%s", defs[i].name);
        node.qualified_name = qn;
        node.file_path = defs[i].file;
        node.start_line = 1;
        node.end_line = 5;
        ids[i] = cbm_store_upsert_node(store, &node);
    }
    /* Dense intra-community calls, one cross call alpha_one → beta_one. */
    static const int pairs[][2] = {{0, 1}, {1, 2}, {2, 0}, {0, 2}, {3, 4},
                                   {4, 5}, {5, 3}, {3, 5}, {0, 3}};
    for (size_t e = 0; e < sizeof(pairs) / sizeof(pairs[0]); e++) {
        cbm_edge_t edge;
        memset(&edge, 0, sizeof(edge));
        edge.project = "regions-test";
        edge.source_id = ids[pairs[e][0]];
        edge.target_id = ids[pairs[e][1]];
        edge.type = "CALLS";
        cbm_store_insert_edge(store, &edge);
    }
    return store;
}

TEST(layout_regions_two_communities) {
    cbm_layout_regions_cache_clear();
    cbm_store_t *store = regions_fixture();
    ASSERT_NOT_NULL(store);

    char *json = cbm_layout_regions_json(store, "regions-test");
    ASSERT_NOT_NULL(json);

    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *root = yyjson_doc_get_root(doc);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(root, "level")), "regions");
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(root, "method")), "leiden+folders");
    ASSERT_EQ((int)yyjson_get_int(yyjson_obj_get(root, "total_nodes")), 6);

    yyjson_val *regions = yyjson_obj_get(root, "regions");
    ASSERT_EQ((int)yyjson_arr_size(regions), 2);
    bool saw_alpha = false, saw_beta = false;
    size_t idx, max;
    yyjson_val *rv;
    yyjson_arr_foreach(regions, idx, max, rv) {
        const char *name = yyjson_get_str(yyjson_obj_get(rv, "name"));
        ASSERT_NOT_NULL(name);
        ASSERT_EQ((int)yyjson_get_int(yyjson_obj_get(rv, "files")), 2);
        ASSERT_EQ((int)yyjson_get_int(yyjson_obj_get(rv, "members")), 3);
        ASSERT_NOT_NULL(yyjson_obj_get(rv, "why"));
        ASSERT_NOT_NULL(yyjson_obj_get(rv, "x"));
        yyjson_val *tops = yyjson_obj_get(rv, "top_nodes");
        ASSERT_GT((int)yyjson_arr_size(tops), 0);
        if (strcmp(name, "src/alpha") == 0)
            saw_alpha = true;
        if (strcmp(name, "src/beta") == 0)
            saw_beta = true;
    }
    ASSERT_TRUE(saw_alpha);
    ASSERT_TRUE(saw_beta);

    /* Exactly one aggregated cross edge with weight 1. */
    yyjson_val *edges = yyjson_obj_get(root, "edges");
    ASSERT_EQ((int)yyjson_arr_size(edges), 1);
    yyjson_val *edge = yyjson_arr_get(edges, 0);
    ASSERT_EQ((int)yyjson_get_int(yyjson_obj_get(edge, "weight")), 1);

    /* Deterministic: a second computation (fresh cache) is byte-identical. */
    cbm_layout_regions_cache_clear();
    char *json2 = cbm_layout_regions_json(store, "regions-test");
    ASSERT_NOT_NULL(json2);
    ASSERT_STR_EQ(json, json2);

    free(json2);
    yyjson_doc_free(doc);
    free(json);
    cbm_store_close(store);
    cbm_layout_regions_cache_clear();
    PASS();
}

TEST(layout_regions_scope_restricts_nodes) {
    cbm_layout_regions_cache_clear();
    cbm_store_t *store = regions_fixture();
    ASSERT_NOT_NULL(store);

    /* Find the region id whose name is src/alpha. */
    char *json = cbm_layout_regions_json(store, "regions-test");
    ASSERT_NOT_NULL(json);
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    int alpha_id = -1;
    yyjson_val *regions = yyjson_obj_get(yyjson_doc_get_root(doc), "regions");
    size_t idx, max;
    yyjson_val *rv;
    yyjson_arr_foreach(regions, idx, max, rv) {
        const char *name = yyjson_get_str(yyjson_obj_get(rv, "name"));
        if (name && strcmp(name, "src/alpha") == 0)
            alpha_id = (int)yyjson_get_int(yyjson_obj_get(rv, "id"));
    }
    yyjson_doc_free(doc);
    free(json);
    ASSERT_GT(alpha_id, -1);

    cbm_layout_result_t *r = cbm_layout_compute_region(store, "regions-test", alpha_id, 100);
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(r->node_count, 3);
    ASSERT_EQ(r->total_nodes, 3);
    for (int i = 0; i < r->node_count; i++)
        ASSERT_NOT_NULL(strstr(r->nodes[i].file_path, "src/alpha/"));
    /* Only intra-region edges: the cross call to beta is not present. */
    for (int e = 0; e < r->edge_count; e++)
        for (int i = 0; i < r->node_count; i++)
            if (r->edges[e].target == r->nodes[i].id)
                ASSERT_NOT_NULL(strstr(r->nodes[i].file_path, "src/alpha/"));
    cbm_layout_free(r);

    /* Unknown region id → NULL, not a crash. */
    ASSERT_NULL(cbm_layout_compute_region(store, "regions-test", 99, 100));

    cbm_store_close(store);
    cbm_layout_regions_cache_clear();
    PASS();
}

/* ── CBM Atlas services: tree, symbol bundle, flows ───────────── */

/* The regions fixture plus: a docstring + entry flag on alpha_one, a test
 * node TESTS-ing alpha_one, SIMILAR_TO across the communities, File nodes
 * with a FILE_CHANGES_WITH edge, and one missed-coverage shadow file. */
static cbm_store_t *atlas_fixture(int64_t *alpha_one_id_out) {
    cbm_store_t *store = regions_fixture();
    if (!store)
        return NULL;
    /* Find alpha_one / alpha_two / beta_two ids by qualified name. */
    int64_t ids[3] = {-1, -1, -1};
    static const char *qns[3] = {"regions-test::alpha_one", "regions-test::alpha_two",
                                 "regions-test::beta_two"};
    for (int i = 0; i < 3; i++) {
        cbm_search_params_t params;
        memset(&params, 0, sizeof(params));
        params.project = "regions-test";
        params.qn_pattern = qns[i];
        params.limit = 1;
        params.min_degree = -1;
        params.max_degree = -1;
        cbm_search_output_t out;
        memset(&out, 0, sizeof(out));
        if (cbm_store_search(store, &params, &out) == CBM_STORE_OK && out.count == 1)
            ids[i] = out.results[0].node.id;
        cbm_store_search_free(&out);
    }
    if (ids[0] < 0 || ids[1] < 0 || ids[2] < 0) {
        cbm_store_close(store);
        return NULL;
    }
    /* Re-upsert alpha_one with docstring + entry flag. */
    cbm_node_t node;
    memset(&node, 0, sizeof(node));
    node.project = "regions-test";
    node.label = "Function";
    node.name = "alpha_one";
    node.qualified_name = "regions-test::alpha_one";
    node.file_path = "src/alpha/a1.c";
    node.start_line = 1;
    node.end_line = 5;
    node.properties_json = "{\"docstring\":\"Entry of alpha.\",\"is_entry_point\":true}";
    int64_t alpha_one_new = cbm_store_upsert_node(store, &node);
    if (alpha_one_new > 0)
        ids[0] = alpha_one_new;
    if (alpha_one_id_out)
        *alpha_one_id_out = ids[0];

    /* A test node exercising alpha_one. */
    memset(&node, 0, sizeof(node));
    node.project = "regions-test";
    node.label = "Function";
    node.name = "test_alpha_one";
    node.qualified_name = "regions-test::test_alpha_one";
    node.file_path = "tests/test_alpha.c";
    node.start_line = 1;
    node.end_line = 9;
    int64_t test_id = cbm_store_upsert_node(store, &node);

    /* File nodes for co-change. */
    int64_t file_ids[2];
    static const char *files[2] = {"src/alpha/a1.c", "src/beta/b1.c"};
    for (int i = 0; i < 2; i++) {
        memset(&node, 0, sizeof(node));
        node.project = "regions-test";
        node.label = "File";
        node.name = files[i];
        char qn[128];
        snprintf(qn, sizeof(qn), "regions-test::file::%d", i);
        node.qualified_name = qn;
        node.file_path = files[i];
        file_ids[i] = cbm_store_upsert_node(store, &node);
    }

    cbm_edge_t edge;
    memset(&edge, 0, sizeof(edge));
    edge.project = "regions-test";
    edge.source_id = test_id;
    edge.target_id = ids[0];
    edge.type = "TESTS";
    cbm_store_insert_edge(store, &edge);

    memset(&edge, 0, sizeof(edge));
    edge.project = "regions-test";
    edge.source_id = ids[1];
    edge.target_id = ids[2];
    edge.type = "SIMILAR_TO";
    edge.properties_json = "{\"score\":0.91}";
    cbm_store_insert_edge(store, &edge);

    memset(&edge, 0, sizeof(edge));
    edge.project = "regions-test";
    edge.source_id = ids[1]; /* alpha_two */
    edge.target_id = ids[2]; /* beta_two */
    edge.type = "DATA_FLOWS";
    edge.properties_json = "{\"args\":\"payload->config\"}";
    cbm_store_insert_edge(store, &edge);

    memset(&edge, 0, sizeof(edge));
    edge.project = "regions-test";
    edge.source_id = file_ids[0];
    edge.target_id = file_ids[1];
    edge.type = "FILE_CHANGES_WITH";
    edge.properties_json = "{\"coupling_score\":0.66}";
    cbm_store_insert_edge(store, &edge);

    /* One not-fully-covered file in the coverage shadow project. */
    char shadow[512];
    cbm_store_coverage_shadow_project(shadow, sizeof(shadow), "regions-test");
    cbm_store_upsert_project(store, shadow, "/tmp/regions-test");
    memset(&node, 0, sizeof(node));
    node.project = shadow;
    node.label = "File";
    node.name = "gen.sql";
    node.qualified_name = "regions-test::missed::gen.sql";
    node.file_path = "src/alpha/gen.sql";
    cbm_store_upsert_node(store, &node);

    return store;
}

TEST(atlas_tree_aggregates_children) {
    cbm_layout_regions_cache_clear();
    cbm_store_t *store = atlas_fixture(NULL);
    ASSERT_NOT_NULL(store);

    char *json = cbm_atlas_tree_json(store, "regions-test", "src");
    ASSERT_NOT_NULL(json);
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *children = yyjson_obj_get(root, "children");
    bool saw_alpha = false, saw_beta = false;
    size_t idx, max;
    yyjson_val *child;
    yyjson_arr_foreach(children, idx, max, child) {
        const char *name = yyjson_get_str(yyjson_obj_get(child, "name"));
        const char *kind = yyjson_get_str(yyjson_obj_get(child, "kind"));
        if (name && strcmp(name, "alpha") == 0) {
            saw_alpha = true;
            ASSERT_STR_EQ(kind, "dir");
            /* 2 code files + the File node's own row groups by file_path:
             * a1.c has 2 fns + File node = 3 symbols, a2.c has 1. */
            ASSERT_EQ((int)yyjson_get_int(yyjson_obj_get(child, "files")), 2);
            ASSERT_GT((int)yyjson_get_int(yyjson_obj_get(child, "symbols")), 3);
            /* The shadow file src/alpha/gen.sql marks alpha as missed. */
            ASSERT_EQ((int)yyjson_get_int(yyjson_obj_get(child, "missed")), 1);
        }
        if (name && strcmp(name, "beta") == 0)
            saw_beta = true;
    }
    ASSERT_TRUE(saw_alpha);
    ASSERT_TRUE(saw_beta);
    yyjson_doc_free(doc);
    free(json);
    cbm_store_close(store);
    cbm_layout_regions_cache_clear();
    PASS();
}

TEST(atlas_symbol_bundle_totals_and_sections) {
    cbm_layout_regions_cache_clear();
    int64_t alpha_one = -1;
    cbm_store_t *store = atlas_fixture(&alpha_one);
    ASSERT_NOT_NULL(store);
    ASSERT_GT(alpha_one, 0);

    char *json = cbm_atlas_symbol_json(store, "regions-test", alpha_one, NULL, 2, 0);
    ASSERT_NOT_NULL(json);
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *root = yyjson_doc_get_root(doc);

    yyjson_val *node = yyjson_obj_get(root, "node");
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(node, "name")), "alpha_one");
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(node, "docstring")), "Entry of alpha.");
    ASSERT_TRUE(yyjson_get_bool(yyjson_obj_get(node, "is_entry")));

    /* Region membership resolved from the cache. */
    yyjson_val *region = yyjson_obj_get(root, "region");
    ASSERT_NOT_NULL(region);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(region, "name")), "src/alpha");

    /* Callees: 3 CALLS total, page of 2 → items 2, total 3. */
    yyjson_val *callees = yyjson_obj_get(root, "callees");
    ASSERT_EQ((int)yyjson_get_int(yyjson_obj_get(callees, "total")), 3);
    ASSERT_EQ((int)yyjson_arr_size(yyjson_obj_get(callees, "items")), 2);
    ASSERT_EQ((int)yyjson_get_int(yyjson_obj_get(yyjson_obj_get(callees, "by_type"), "CALLS")), 3);

    /* Callers: alpha_three only. */
    yyjson_val *callers = yyjson_obj_get(root, "callers");
    ASSERT_EQ((int)yyjson_get_int(yyjson_obj_get(callers, "total")), 1);

    /* Tests / co-change / similar sections carry the fixture rows. */
    ASSERT_EQ((int)yyjson_arr_size(yyjson_obj_get(root, "tests")), 1);
    yyjson_val *cochange = yyjson_obj_get(root, "co_change");
    ASSERT_EQ((int)yyjson_arr_size(cochange), 1);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(yyjson_arr_get(cochange, 0), "file_path")),
                  "src/beta/b1.c");

    yyjson_doc_free(doc);
    free(json);

    /* qn lookup resolves the same node. */
    char *by_qn =
        cbm_atlas_symbol_json(store, "regions-test", -1, "regions-test::alpha_one", 10, 0);
    ASSERT_NOT_NULL(by_qn);
    free(by_qn);
    /* Unknown symbol → NULL. */
    ASSERT_NULL(cbm_atlas_symbol_json(store, "regions-test", 999999, NULL, 10, 0));

    cbm_store_close(store);
    cbm_layout_regions_cache_clear();
    PASS();
}

TEST(atlas_flows_walk_and_rank) {
    cbm_layout_regions_cache_clear();
    cbm_atlas_flows_cache_clear();
    cbm_store_t *store = atlas_fixture(NULL);
    ASSERT_NOT_NULL(store);

    char *json = cbm_atlas_flows_json(store, "regions-test");
    ASSERT_NOT_NULL(json);
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *flows = yyjson_obj_get(root, "flows");
    ASSERT_GT((int)yyjson_arr_size(flows), 0);

    /* The flagged entry (alpha_one) must lead the ranking's entries. */
    bool alpha_flow = false;
    int alpha_flow_id = -1;
    size_t idx, max;
    yyjson_val *flow;
    yyjson_arr_foreach(flows, idx, max, flow) {
        yyjson_val *entry = yyjson_obj_get(flow, "entry");
        const char *entry_name = yyjson_get_str(yyjson_obj_get(entry, "name"));
        if (entry_name && strcmp(entry_name, "alpha_one") == 0) {
            alpha_flow = true;
            alpha_flow_id = (int)yyjson_get_int(yyjson_obj_get(flow, "id"));
            ASSERT_GT((int)yyjson_get_int(yyjson_obj_get(flow, "steps")), 2);
            ASSERT_TRUE(yyjson_get_bool(yyjson_obj_get(flow, "cross_region")));
        }
    }
    ASSERT_TRUE(alpha_flow);
    yyjson_doc_free(doc);
    free(json);

    /* Flow detail: coherent DFS structure (parent before child, depths). */
    char *detail = cbm_atlas_flow_json(store, "regions-test", alpha_flow_id);
    ASSERT_NOT_NULL(detail);
    yyjson_doc *ddoc = yyjson_read(detail, strlen(detail), 0);
    ASSERT_NOT_NULL(ddoc);
    yyjson_val *steps = yyjson_obj_get(yyjson_doc_get_root(ddoc), "steps");
    int nsteps = (int)yyjson_arr_size(steps);
    ASSERT_GT(nsteps, 2);
    for (int i = 0; i < nsteps; i++) {
        yyjson_val *step = yyjson_arr_get(steps, (size_t)i);
        int parent = (int)yyjson_get_int(yyjson_obj_get(step, "parent"));
        int depth = (int)yyjson_get_int(yyjson_obj_get(step, "depth"));
        if (i == 0) {
            ASSERT_EQ(parent, -1);
            ASSERT_EQ(depth, 0);
        } else {
            ASSERT_GT(parent, -1);
            ASSERT_TRUE(parent < i);
            yyjson_val *pstep = yyjson_arr_get(steps, (size_t)parent);
            ASSERT_EQ(depth, (int)yyjson_get_int(yyjson_obj_get(pstep, "depth")) + 1);
        }
    }
    yyjson_doc_free(ddoc);
    free(detail);

    /* Determinism: fresh cache → byte-identical list. */
    char *again = NULL;
    cbm_atlas_flows_cache_clear();
    again = cbm_atlas_flows_json(store, "regions-test");
    ASSERT_NOT_NULL(again);
    char *first = cbm_atlas_flows_json(store, "regions-test");
    ASSERT_STR_EQ(again, first);
    free(again);
    free(first);

    /* Unknown flow id → NULL. */
    ASSERT_NULL(cbm_atlas_flow_json(store, "regions-test", 9999));

    cbm_store_close(store);
    cbm_atlas_flows_cache_clear();
    cbm_layout_regions_cache_clear();
    PASS();
}

TEST(atlas_metrics_totals_and_certainty) {
    cbm_layout_regions_cache_clear();
    cbm_atlas_metrics_cache_clear();
    cbm_store_t *store = atlas_fixture(NULL);
    ASSERT_NOT_NULL(store);

    char *json = cbm_atlas_metrics_json(store, "regions-test");
    ASSERT_NOT_NULL(json);
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *root = yyjson_doc_get_root(doc);

    yyjson_val *totals = yyjson_obj_get(root, "totals");
    /* 6 fixture functions + test_alpha_one. */
    ASSERT_EQ((int)yyjson_get_int(yyjson_obj_get(totals, "callables")), 7);
    /* test_alpha_one has no callers but is in a test path — flags absent, so
     * it counts as dead alongside beta callables the cycle keeps alive;
     * alpha_one is entry-flagged and beta_* all have inbound CALLS: only the
     * test function has zero inbound call-ish edges. */
    ASSERT_EQ((int)yyjson_get_int(yyjson_obj_get(totals, "dead")), 1);
    ASSERT_EQ((int)yyjson_get_int(yyjson_obj_get(totals, "tested_symbols")), 1);
    ASSERT_EQ((int)yyjson_get_int(yyjson_obj_get(totals, "similar_edges")), 1);
    ASSERT_EQ((int)yyjson_get_int(yyjson_obj_get(totals, "missed_files")), 1);

    yyjson_val *certainty = yyjson_obj_get(root, "certainty");
    ASSERT_EQ((int)yyjson_get_int(yyjson_obj_get(certainty, "calls")), 9);

    /* Histograms cover every callable exactly once. */
    yyjson_val *hist = yyjson_obj_get(root, "complexity_hist");
    long long sum = 0;
    size_t idx, max;
    yyjson_val *bucket;
    yyjson_arr_foreach(hist, idx, max, bucket) sum += yyjson_get_int(bucket);
    ASSERT_EQ((int)sum, 7);

    ASSERT_NOT_NULL(yyjson_obj_get(root, "history"));

    yyjson_doc_free(doc);
    free(json);
    cbm_store_close(store);
    cbm_atlas_metrics_cache_clear();
    cbm_layout_regions_cache_clear();
    PASS();
}

TEST(atlas_trace_reachability_calls_and_data) {
    cbm_layout_regions_cache_clear();
    cbm_atlas_flows_cache_clear();
    cbm_store_t *store = atlas_fixture(NULL);
    ASSERT_NOT_NULL(store);

    /* alpha_one reaches beta_three over CALLS (via beta_one). */
    char *json = cbm_atlas_trace_json(store, "regions-test", -1, "regions-test::alpha_one", -1,
                                      "regions-test::beta_three", "calls");
    ASSERT_NOT_NULL(json);
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *root = yyjson_doc_get_root(doc);
    ASSERT_TRUE(yyjson_get_bool(yyjson_obj_get(root, "reachable")));
    yyjson_val *path = yyjson_obj_get(root, "path");
    ASSERT_GT((int)yyjson_arr_size(path), 1);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(yyjson_arr_get(path, 0), "name")), "alpha_one");
    int hops = (int)yyjson_get_int(yyjson_obj_get(root, "hops"));
    ASSERT_EQ((int)yyjson_arr_size(path), hops + 1);
    yyjson_doc_free(doc);
    free(json);

    /* beta_three does NOT reach alpha_one (calls point the other way and
     * the beta cycle never calls into alpha). */
    json = cbm_atlas_trace_json(store, "regions-test", -1, "regions-test::beta_three", -1,
                                "regions-test::alpha_one", "calls");
    ASSERT_NOT_NULL(json);
    doc = yyjson_read(json, strlen(json), 0);
    ASSERT_FALSE(yyjson_get_bool(yyjson_obj_get(yyjson_doc_get_root(doc), "reachable")));
    yyjson_doc_free(doc);
    free(json);

    /* Data mode follows only DATA_FLOWS: alpha_two → beta_two works,
     * alpha_one → beta_two does not (no data edge from alpha_one). */
    json = cbm_atlas_trace_json(store, "regions-test", -1, "regions-test::alpha_two", -1,
                                "regions-test::beta_two", "data");
    ASSERT_NOT_NULL(json);
    doc = yyjson_read(json, strlen(json), 0);
    root = yyjson_doc_get_root(doc);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(root, "mode")), "data");
    ASSERT_TRUE(yyjson_get_bool(yyjson_obj_get(root, "reachable")));
    ASSERT_EQ((int)yyjson_get_int(yyjson_obj_get(root, "hops")), 1);
    yyjson_doc_free(doc);
    free(json);

    json = cbm_atlas_trace_json(store, "regions-test", -1, "regions-test::alpha_one", -1,
                                "regions-test::beta_two", "data");
    ASSERT_NOT_NULL(json);
    doc = yyjson_read(json, strlen(json), 0);
    ASSERT_FALSE(yyjson_get_bool(yyjson_obj_get(yyjson_doc_get_root(doc), "reachable")));
    yyjson_doc_free(doc);
    free(json);

    /* Unknown endpoint answers with an error, not a crash. */
    json = cbm_atlas_trace_json(store, "regions-test", -1, "regions-test::nope", -1,
                                "regions-test::beta_two", "calls");
    ASSERT_NOT_NULL(json);
    doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(yyjson_obj_get(yyjson_doc_get_root(doc), "error"));
    yyjson_doc_free(doc);
    free(json);

    cbm_store_close(store);
    cbm_atlas_flows_cache_clear();
    cbm_layout_regions_cache_clear();
    PASS();
}

/* Impact = reverse reachability with distances, region rollup, and the
 * test functions that reach the symbol. On the two-community fixture,
 * everything can reach beta_one except beta_one itself: alpha_one and
 * beta_three directly, alpha_three and beta_two at two hops, alpha_two
 * at three. */
TEST(atlas_impact_reverse_reachability_and_tests) {
    cbm_layout_regions_cache_clear();
    cbm_atlas_flows_cache_clear();
    cbm_store_t *store = regions_fixture();
    ASSERT_NOT_NULL(store);

    char *json = cbm_atlas_impact_json(store, "regions-test", -1, "regions-test::beta_one");
    ASSERT_NOT_NULL(json);
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *root = yyjson_doc_get_root(doc);
    ASSERT_EQ((int)yyjson_get_int(yyjson_obj_get(root, "reachable")), 5);
    ASSERT_EQ((int)yyjson_get_int(yyjson_obj_get(root, "max_distance")), 3);
    ASSERT_FALSE(yyjson_get_bool(yyjson_obj_get(root, "truncated")));
    yyjson_val *by_d = yyjson_obj_get(root, "by_distance");
    ASSERT_EQ((int)yyjson_arr_size(by_d), 3);
    ASSERT_EQ((int)yyjson_get_int(yyjson_arr_get(by_d, 0)), 2);
    ASSERT_EQ((int)yyjson_get_int(yyjson_arr_get(by_d, 1)), 2);
    ASSERT_EQ((int)yyjson_get_int(yyjson_arr_get(by_d, 2)), 1);
    /* The region split plus the unregioned tail must account for every
     * reacher; with both communities regioned, alpha's three outrank
     * beta's two. */
    yyjson_val *regions = yyjson_obj_get(root, "regions");
    int region_sum = 0;
    size_t ri, rmax;
    yyjson_val *rv;
    yyjson_arr_foreach(regions, ri, rmax, rv) {
        ASSERT_NOT_NULL(yyjson_get_str(yyjson_obj_get(rv, "name")));
        region_sum += (int)yyjson_get_int(yyjson_obj_get(rv, "count"));
    }
    ASSERT_EQ(region_sum + (int)yyjson_get_int(yyjson_obj_get(root, "unregioned")), 5);
    if (yyjson_arr_size(regions) == 2)
        ASSERT_EQ((int)yyjson_get_int(yyjson_obj_get(yyjson_arr_get(regions, 0), "count")), 3);
    yyjson_val *tests = yyjson_obj_get(root, "tests");
    ASSERT_EQ((int)yyjson_get_int(yyjson_obj_get(tests, "count")), 0);
    yyjson_doc_free(doc);
    free(json);

    /* A test function calling into the beta cycle is reported as a test
     * to run, with its call distance. */
    cbm_node_t tnode;
    memset(&tnode, 0, sizeof(tnode));
    tnode.project = "regions-test";
    tnode.label = "Function";
    tnode.name = "test_beta";
    tnode.qualified_name = "regions-test::test_beta";
    tnode.file_path = "tests/test_beta.c";
    tnode.start_line = 1;
    tnode.end_line = 5;
    int64_t test_id = cbm_store_upsert_node(store, &tnode);
    ASSERT_GT(test_id, 0);
    int64_t beta_two_id = -1;
    {
        cbm_search_params_t params;
        memset(&params, 0, sizeof(params));
        params.project = "regions-test";
        params.qn_pattern = "regions-test::beta_two";
        params.limit = 1;
        params.min_degree = -1;
        params.max_degree = -1;
        cbm_search_output_t out;
        memset(&out, 0, sizeof(out));
        if (cbm_store_search(store, &params, &out) == CBM_STORE_OK && out.count == 1)
            beta_two_id = out.results[0].node.id;
        cbm_store_search_free(&out);
    }
    ASSERT_GT(beta_two_id, 0);
    cbm_edge_t tedge;
    memset(&tedge, 0, sizeof(tedge));
    tedge.project = "regions-test";
    tedge.source_id = test_id;
    tedge.target_id = beta_two_id;
    tedge.type = "CALLS";
    ASSERT_GT(cbm_store_insert_edge(store, &tedge), 0);
    cbm_atlas_flows_cache_clear();

    json = cbm_atlas_impact_json(store, "regions-test", -1, "regions-test::beta_one");
    ASSERT_NOT_NULL(json);
    doc = yyjson_read(json, strlen(json), 0);
    root = yyjson_doc_get_root(doc);
    ASSERT_EQ((int)yyjson_get_int(yyjson_obj_get(root, "reachable")), 6);
    tests = yyjson_obj_get(root, "tests");
    ASSERT_EQ((int)yyjson_get_int(yyjson_obj_get(tests, "count")), 1);
    yyjson_val *tnear = yyjson_obj_get(tests, "nearest");
    ASSERT_EQ((int)yyjson_arr_size(tnear), 1);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(yyjson_arr_get(tnear, 0), "name")), "test_beta");
    ASSERT_EQ((int)yyjson_get_int(yyjson_obj_get(yyjson_arr_get(tnear, 0), "distance")), 3);
    yyjson_doc_free(doc);
    free(json);

    /* Unknown symbol answers with an error, not a crash. */
    json = cbm_atlas_impact_json(store, "regions-test", -1, "regions-test::nope");
    ASSERT_NOT_NULL(json);
    doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(yyjson_obj_get(yyjson_doc_get_root(doc), "error"));
    yyjson_doc_free(doc);
    free(json);

    cbm_store_close(store);
    cbm_atlas_flows_cache_clear();
    cbm_layout_regions_cache_clear();
    PASS();
}

TEST(atlas_symbol_data_flows_and_history_shape) {
    cbm_layout_regions_cache_clear();
    cbm_atlas_metrics_cache_clear();
    int64_t alpha_one = -1;
    cbm_store_t *store = atlas_fixture(&alpha_one);
    ASSERT_NOT_NULL(store);

    /* alpha_two has an outgoing DATA_FLOWS edge with detail. */
    char *json = cbm_atlas_symbol_json(store, "regions-test", -1, "regions-test::alpha_two", 10, 0);
    ASSERT_NOT_NULL(json);
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *data_out = yyjson_obj_get(root, "data_out");
    ASSERT_EQ((int)yyjson_arr_size(data_out), 1);
    yyjson_val *flow = yyjson_arr_get(data_out, 0);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(flow, "name")), "beta_two");
    yyjson_val *detail = yyjson_obj_get(flow, "detail");
    ASSERT_NOT_NULL(detail);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(detail, "args")), "payload->config");

    /* file_history is present and honest: no git at the fixture root, so
     * available:false (never fabricated history). */
    yyjson_val *history = yyjson_obj_get(root, "file_history");
    ASSERT_NOT_NULL(history);
    ASSERT_FALSE(yyjson_get_bool(yyjson_obj_get(history, "available")));

    yyjson_doc_free(doc);
    free(json);
    cbm_store_close(store);
    cbm_atlas_metrics_cache_clear();
    cbm_layout_regions_cache_clear();
    PASS();
}

TEST(atlas_scent_buckets_matches_by_region) {
    cbm_layout_regions_cache_clear();
    cbm_store_t *store = atlas_fixture(NULL);
    ASSERT_NOT_NULL(store);

    /* "alpha" matches the three alpha_* functions (one region) plus
     * test_alpha_one in the tests folder region — File nodes are excluded
     * so a file does not double-count its own symbols. */
    char *json = cbm_atlas_scent_json(store, "regions-test", "alpha");
    ASSERT_NOT_NULL(json);
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *root = yyjson_doc_get_root(doc);
    ASSERT_EQ((int)yyjson_get_int(yyjson_obj_get(root, "total")), 4);
    yyjson_val *regions = yyjson_obj_get(root, "regions");
    int max_count = 0;
    long long bucketed = 0;
    for (size_t i = 0; i < yyjson_arr_size(regions); i++) {
        int c = (int)yyjson_get_int(yyjson_obj_get(yyjson_arr_get(regions, i), "count"));
        bucketed += c;
        if (c > max_count)
            max_count = c;
    }
    ASSERT_EQ(max_count, 3);
    ASSERT_EQ((int)(bucketed + yyjson_get_int(yyjson_obj_get(root, "unmapped"))), 4);
    yyjson_doc_free(doc);
    free(json);

    json = cbm_atlas_scent_json(store, "regions-test", "_two");
    ASSERT_NOT_NULL(json);
    doc = yyjson_read(json, strlen(json), 0);
    root = yyjson_doc_get_root(doc);
    ASSERT_EQ((int)yyjson_get_int(yyjson_obj_get(root, "total")), 2);
    ASSERT_EQ((int)yyjson_arr_size(yyjson_obj_get(root, "regions")), 2);
    yyjson_doc_free(doc);
    free(json);

    /* Sub-2-char queries refuse rather than scan everything. */
    ASSERT_NULL(cbm_atlas_scent_json(store, "regions-test", "a"));

    cbm_store_close(store);
    cbm_layout_regions_cache_clear();
    PASS();
}

TEST(atlas_bridges_rank_by_region_reach) {
    cbm_layout_regions_cache_clear();
    cbm_store_t *store = atlas_fixture(NULL);
    ASSERT_NOT_NULL(store);

    /* The fixture's one cross-region CALLS edge is alpha_one → beta_one, so
     * both endpoints are boundary spanners touching exactly one foreign
     * region; nothing else qualifies (test_alpha_one has a TESTS edge, not
     * CALLS). */
    char *json = cbm_atlas_bridges_json(store, "regions-test");
    ASSERT_NOT_NULL(json);
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *bridges = yyjson_obj_get(root, "bridges");
    ASSERT_NOT_NULL(bridges);
    /* regions < 2 rows are dropped: with one cross pair each node touches
     * exactly 1 foreign region, so the honest answer is an empty list. */
    ASSERT_EQ((int)yyjson_arr_size(bridges), 0);
    yyjson_doc_free(doc);
    free(json);

    /* A third dense community (mirroring the beta pattern: a 3-cycle plus
     * exactly one inbound cross call) so alpha_one spans TWO foreign
     * regions. A single linked node would be absorbed into alpha's own
     * Leiden community and prove nothing. */
    int64_t gamma_ids[3] = {0, 0, 0};
    for (int i = 0; i < 3; i++) {
        cbm_node_t node;
        memset(&node, 0, sizeof(node));
        node.project = "regions-test";
        node.label = "Function";
        char name[32], qn[64], fp[32];
        snprintf(name, sizeof(name), "gamma_%d", i + 1);
        snprintf(qn, sizeof(qn), "regions-test::gamma_%d", i + 1);
        snprintf(fp, sizeof(fp), "src/gamma/g%d.c", i + 1);
        node.name = name;
        node.qualified_name = qn;
        node.file_path = fp;
        node.start_line = 1;
        node.end_line = 3;
        gamma_ids[i] = cbm_store_upsert_node(store, &node);
        ASSERT_GT(gamma_ids[i], 0);
    }
    for (int i = 0; i < 3; i++) {
        cbm_edge_t gedge;
        memset(&gedge, 0, sizeof(gedge));
        gedge.project = "regions-test";
        gedge.source_id = gamma_ids[i];
        gedge.target_id = gamma_ids[(i + 1) % 3];
        gedge.type = "CALLS";
        ASSERT_GT(cbm_store_insert_edge(store, &gedge), 0);
    }
    int64_t gamma_id = gamma_ids[0];

    int64_t alpha_id = -1;
    {
        cbm_search_params_t params;
        memset(&params, 0, sizeof(params));
        params.project = "regions-test";
        params.qn_pattern = "regions-test::alpha_one";
        params.limit = 1;
        params.min_degree = -1;
        params.max_degree = -1;
        cbm_search_output_t out;
        memset(&out, 0, sizeof(out));
        ASSERT_EQ(cbm_store_search(store, &params, &out), CBM_STORE_OK);
        ASSERT_EQ(out.count, 1);
        alpha_id = out.results[0].node.id;
        cbm_store_search_free(&out);
    }
    cbm_edge_t edge;
    memset(&edge, 0, sizeof(edge));
    edge.project = "regions-test";
    edge.source_id = alpha_id;
    edge.target_id = gamma_id;
    edge.type = "CALLS";
    ASSERT_GT(cbm_store_insert_edge(store, &edge), 0);
    cbm_layout_regions_cache_clear(); /* region map must include src/gamma */

    json = cbm_atlas_bridges_json(store, "regions-test");
    ASSERT_NOT_NULL(json);
    doc = yyjson_read(json, strlen(json), 0);
    root = yyjson_doc_get_root(doc);
    bridges = yyjson_obj_get(root, "bridges");
    ASSERT_GT((int)yyjson_arr_size(bridges), 0);
    yyjson_val *top = yyjson_arr_get(bridges, 0);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(top, "name")), "alpha_one");
    ASSERT_EQ((int)yyjson_get_int(yyjson_obj_get(top, "regions")), 2);
    yyjson_doc_free(doc);
    free(json);

    cbm_store_close(store);
    cbm_layout_regions_cache_clear();
    PASS();
}

TEST(atlas_symbol_overflow_tail_and_dataflow_presence) {
    cbm_layout_regions_cache_clear();
    cbm_atlas_metrics_cache_clear();
    int64_t alpha_one = -1;
    cbm_store_t *store = atlas_fixture(&alpha_one);
    ASSERT_NOT_NULL(store);

    /* limit=1 on beta_one's callers (alpha_one crosses in, beta_three
     * closes the cycle) forces an overflow tail grouped by file. */
    int64_t beta_one = -1;
    {
        cbm_search_params_t params;
        memset(&params, 0, sizeof(params));
        params.project = "regions-test";
        params.qn_pattern = "regions-test::beta_one";
        params.limit = 1;
        params.min_degree = -1;
        params.max_degree = -1;
        cbm_search_output_t out;
        memset(&out, 0, sizeof(out));
        ASSERT_EQ(cbm_store_search(store, &params, &out), CBM_STORE_OK);
        ASSERT_EQ(out.count, 1);
        beta_one = out.results[0].node.id;
        cbm_store_search_free(&out);
    }
    char *json = cbm_atlas_symbol_json(store, "regions-test", beta_one, NULL, 1, 0);
    ASSERT_NOT_NULL(json);
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *callers = yyjson_obj_get(root, "callers");
    long long total = yyjson_get_int(yyjson_obj_get(callers, "total"));
    ASSERT_GT((int)total, 1);
    ASSERT_EQ((int)yyjson_arr_size(yyjson_obj_get(callers, "items")), 1);
    yyjson_val *tail = yyjson_obj_get(callers, "overflow_by_file");
    ASSERT_NOT_NULL(tail);
    long long tail_total = 0;
    for (size_t i = 0; i < yyjson_arr_size(tail); i++) {
        yyjson_val *row = yyjson_arr_get(tail, i);
        ASSERT_NOT_NULL(yyjson_get_str(yyjson_obj_get(row, "file")));
        tail_total += yyjson_get_int(yyjson_obj_get(row, "count"));
    }
    /* The tail accounts for exactly what the page cut. */
    ASSERT_EQ((int)tail_total, (int)(total - 1));

    /* Connection rows carry the neighbor's region; the fixture has
     * DATA_FLOWS so the presence flag is true. */
    yyjson_val *first = yyjson_arr_get(yyjson_obj_get(callers, "items"), 0);
    ASSERT_NOT_NULL(yyjson_obj_get(first, "region"));
    ASSERT_TRUE(yyjson_get_bool(yyjson_obj_get(root, "project_has_data_flows")));

    yyjson_doc_free(doc);
    free(json);
    cbm_store_close(store);
    cbm_atlas_metrics_cache_clear();
    cbm_layout_regions_cache_clear();
    PASS();
}

TEST(atlas_blast_buckets_files_by_region) {
    cbm_layout_regions_cache_clear();
    cbm_store_t *store = atlas_fixture(NULL);
    ASSERT_NOT_NULL(store);

    char *json = cbm_atlas_blast_json(store, "regions-test",
                                      "src/alpha/a1.c,src/beta/b1.c,src/alpha/a2.c,unknown.c");
    ASSERT_NOT_NULL(json);
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *root = yyjson_doc_get_root(doc);
    ASSERT_EQ((int)yyjson_get_int(yyjson_obj_get(root, "files")), 4);
    yyjson_val *regions = yyjson_obj_get(root, "regions");
    long long bucketed = 0;
    int max_count = 0;
    for (size_t i = 0; i < yyjson_arr_size(regions); i++) {
        yyjson_val *row = yyjson_arr_get(regions, i);
        ASSERT_NOT_NULL(yyjson_get_str(yyjson_obj_get(row, "name")));
        int c = (int)yyjson_get_int(yyjson_obj_get(row, "count"));
        bucketed += c;
        if (c > max_count)
            max_count = c;
    }
    /* Every file is accounted for: bucketed + unmapped == files. */
    long long unmapped = yyjson_get_int(yyjson_obj_get(root, "unmapped"));
    ASSERT_EQ((int)(bucketed + unmapped), 4);
    ASSERT_EQ(max_count, 2); /* both alpha files land in one region */
    yyjson_doc_free(doc);
    free(json);

    ASSERT_NULL(cbm_atlas_blast_json(store, "regions-test", ""));

    cbm_store_close(store);
    cbm_layout_regions_cache_clear();
    PASS();
}

TEST(atlas_handout_is_selfcontained_and_escaped) {
    cbm_layout_regions_cache_clear();
    cbm_atlas_flows_cache_clear();
    cbm_atlas_metrics_cache_clear();
    cbm_store_t *store = atlas_fixture(NULL);
    ASSERT_NOT_NULL(store);

    char *html = cbm_atlas_handout_html(store, "regions-test");
    ASSERT_NOT_NULL(html);
    ASSERT_NOT_NULL(strstr(html, "<!doctype html>"));
    ASSERT_NOT_NULL(strstr(html, "regions-test"));
    /* The map renders the fixture regions. */
    ASSERT_NOT_NULL(strstr(html, "src/alpha"));
    /* No host paths leak into the shareable artifact. */
    ASSERT_NULL(strstr(html, "/tmp/"));
    ASSERT_NULL(strstr(html, "/Users/"));
    /* Honest churn: fixture root has no git history → the section says so. */
    ASSERT_NOT_NULL(strstr(html, "No readable git history"));
    free(html);

    cbm_store_close(store);
    cbm_atlas_metrics_cache_clear();
    cbm_atlas_flows_cache_clear();
    cbm_layout_regions_cache_clear();
    PASS();
}

/* A real on-disk C file so the on-demand guard extraction has something to
 * parse: alpha calls beta under two nested ifs, gamma inside a loop, and
 * delta in the else arm. */
static const char *WHY_SAMPLE_C = "void beta(void);\n"
                                  "void gamma_fn(void);\n"
                                  "void delta(void);\n"
                                  "void alpha(int mode, int strict) {\n"
                                  "    if (mode > 2) {\n"
                                  "        if (strict) {\n"
                                  "            beta();\n"
                                  "        } else {\n"
                                  "            delta();\n"
                                  "        }\n"
                                  "    }\n"
                                  "    for (int i = 0; i < mode; i++) {\n"
                                  "        gamma_fn();\n"
                                  "    }\n"
                                  "}\n";

/* Observed runtime calls attach to trace and flow JSON by qualified name:
 * an observed hop gains {count, label, last_seen}; an unobserved hop and
 * absent stores add nothing (absence is data, never "dead"). */
TEST(atlas_attach_observed_marks_hops) {
    cbm_layout_regions_cache_clear();
    cbm_atlas_flows_cache_clear();
    cbm_store_t *store = regions_fixture();
    ASSERT_NOT_NULL(store);

    /* alpha_one → beta_one observed twice in one run; the rest never ran. */
    ASSERT_EQ(cbm_store_observed_upsert_pair(store, "regions-test", "regions-test::alpha_one",
                                             "regions-test::beta_one", "pytest", 2),
              CBM_STORE_OK);

    char *json = cbm_atlas_trace_json(store, "regions-test", -1, "regions-test::alpha_one", -1,
                                      "regions-test::beta_three", "calls");
    ASSERT_NOT_NULL(json);
    json = cbm_atlas_attach_observed(store, "regions-test", json);
    ASSERT_NOT_NULL(json);
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *path = yyjson_obj_get(yyjson_doc_get_root(doc), "path");
    ASSERT_GT((int)yyjson_arr_size(path), 2);
    /* Hop 0→1 is alpha_one→beta_one: observed lands on the CALLEE hop. */
    yyjson_val *hop1 = yyjson_arr_get(path, 1);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(hop1, "name")), "beta_one");
    yyjson_val *obs = yyjson_obj_get(hop1, "observed");
    ASSERT_NOT_NULL(obs);
    ASSERT_EQ((int)yyjson_get_int(yyjson_obj_get(obs, "count")), 2);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(obs, "label")), "pytest");
    ASSERT_NOT_NULL(yyjson_get_str(yyjson_obj_get(obs, "last_seen")));
    /* The next hop (beta_one→beta_three) never ran: no marker. */
    yyjson_val *hop2 = yyjson_arr_get(path, 2);
    ASSERT_NULL(yyjson_obj_get(hop2, "observed"));
    yyjson_doc_free(doc);
    free(json);

    /* Flow shape: steps with parent indices join the same way. */
    int64_t alpha_one_id = -1, beta_one_id = -1;
    const struct {
        const char *qn;
        int64_t *out;
    } wanted[2] = {{"regions-test::alpha_one", &alpha_one_id},
                   {"regions-test::beta_one", &beta_one_id}};
    for (int i = 0; i < 2; i++) {
        cbm_search_params_t params;
        memset(&params, 0, sizeof(params));
        params.project = "regions-test";
        params.qn_pattern = wanted[i].qn;
        params.limit = 1;
        params.min_degree = -1;
        params.max_degree = -1;
        cbm_search_output_t out;
        memset(&out, 0, sizeof(out));
        if (cbm_store_search(store, &params, &out) == CBM_STORE_OK && out.count == 1)
            *wanted[i].out = out.results[0].node.id;
        cbm_store_search_free(&out);
    }
    ASSERT_GT(alpha_one_id, 0);
    ASSERT_GT(beta_one_id, 0);
    char flow_json[256];
    snprintf(flow_json, sizeof(flow_json),
             "{\"steps\":[{\"id\":%lld,\"parent\":-1},{\"id\":%lld,\"parent\":0}]}",
             (long long)alpha_one_id, (long long)beta_one_id);
    char *owned = malloc(strlen(flow_json) + 1);
    ASSERT_NOT_NULL(owned);
    strcpy(owned, flow_json);
    owned = cbm_atlas_attach_observed(store, "regions-test", owned);
    ASSERT_NOT_NULL(owned);
    doc = yyjson_read(owned, strlen(owned), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *steps = yyjson_obj_get(yyjson_doc_get_root(doc), "steps");
    yyjson_val *step1 = yyjson_arr_get(steps, 1);
    obs = yyjson_obj_get(step1, "observed");
    ASSERT_NOT_NULL(obs);
    ASSERT_EQ((int)yyjson_get_int(yyjson_obj_get(obs, "count")), 2);
    yyjson_doc_free(doc);
    free(owned);

    cbm_store_close(store);
    cbm_atlas_flows_cache_clear();
    cbm_layout_regions_cache_clear();
    PASS();
}

/* Who can help: authorship as evidence from the churn scan — commits in
 * this file, repo-wide breadth, last-touched where the retained newest
 * commits prove it. Real throwaway git repo; environments without git
 * pass through, like the feature itself. */
TEST(atlas_who_reports_authorship_evidence) {
#ifdef _WIN32
    if (system("git --version >NUL 2>&1") != 0)
#else
    if (system("git --version >/dev/null 2>&1") != 0)
#endif
        PASS();

    char tmpl[256] = "/tmp/cbm_who_XXXXXX";
    char *root = cbm_mkdtemp(tmpl);
    ASSERT_NOT_NULL(root);
    char dir[1024];
    snprintf(dir, sizeof(dir), "%s/src", root);
    cbm_mkdir_p(dir, 0755);
    static const char *const files[2] = {"src/a.c", "src/b.c"};
    for (int i = 0; i < 2; i++) {
        char path[1024];
        snprintf(path, sizeof(path), "%s/%s", root, files[i]);
        FILE *fp = cbm_fopen(path, "wb");
        ASSERT_NOT_NULL(fp);
        fputs("int x;\n", fp);
        fclose(fp);
    }
    char cmd[2048];
    snprintf(cmd, sizeof(cmd),
             "git -C \"%s\" init -q && git -C \"%s\" add . && "
             "git -C \"%s\" -c user.name=Alice -c user.email=a@t.io commit -qm seed",
             root, root, root);
    if (system(cmd) != 0) {
        (void)th_rmtree(root);
        PASS();
    }
    /* Bob touches a.c, then Alice touches a.c again — a.c ends with three
     * commits (Alice 2, Bob 1), b.c with one (Alice). */
    char touch[1024];
    snprintf(touch, sizeof(touch), "%s/src/a.c", root);
    FILE *fp = cbm_fopen(touch, "ab");
    ASSERT_NOT_NULL(fp);
    fputs("int y;\n", fp);
    fclose(fp);
    snprintf(cmd, sizeof(cmd),
             "git -C \"%s\" add . && "
             "git -C \"%s\" -c user.name=Bob -c user.email=b@t.io commit -qm bob",
             root, root);
    ASSERT_EQ(system(cmd), 0);
    fp = cbm_fopen(touch, "ab");
    ASSERT_NOT_NULL(fp);
    fputs("int z;\n", fp);
    fclose(fp);
    snprintf(cmd, sizeof(cmd),
             "git -C \"%s\" add . && "
             "git -C \"%s\" -c user.name=Alice -c user.email=a@t.io commit -qm alice2",
             root, root);
    ASSERT_EQ(system(cmd), 0);

    cbm_atlas_metrics_cache_clear();
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);
    cbm_store_upsert_project(store, "who-test", root);

    char *json = cbm_atlas_who_json(store, "who-test", "src/a.c");
    ASSERT_NOT_NULL(json);
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *jroot = yyjson_doc_get_root(doc);
    ASSERT_TRUE(yyjson_get_bool(yyjson_obj_get(jroot, "available")));
    ASSERT_EQ((int)yyjson_get_int(yyjson_obj_get(jroot, "commits_1y")), 3);
    ASSERT_EQ((int)yyjson_get_int(yyjson_obj_get(jroot, "people_total")), 2);
    yyjson_val *people = yyjson_obj_get(jroot, "people");
    ASSERT_EQ((int)yyjson_arr_size(people), 2);
    yyjson_val *first = yyjson_arr_get(people, 0);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(first, "name")), "Alice");
    ASSERT_EQ((int)yyjson_get_int(yyjson_obj_get(first, "commits_here")), 2);
    ASSERT_EQ((int)yyjson_get_int(yyjson_obj_get(first, "files_repo_wide")), 2);
    ASSERT_GT(yyjson_get_int(yyjson_obj_get(first, "last_seen")), 0);
    yyjson_val *second = yyjson_arr_get(people, 1);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(second, "name")), "Bob");
    ASSERT_EQ((int)yyjson_get_int(yyjson_obj_get(second, "commits_here")), 1);
    ASSERT_EQ((int)yyjson_get_int(yyjson_obj_get(second, "files_repo_wide")), 1);
    yyjson_doc_free(doc);
    free(json);

    /* A file with no recorded commits answers honestly, not with an error. */
    json = cbm_atlas_who_json(store, "who-test", "src/never.c");
    ASSERT_NOT_NULL(json);
    doc = yyjson_read(json, strlen(json), 0);
    jroot = yyjson_doc_get_root(doc);
    ASSERT_TRUE(yyjson_get_bool(yyjson_obj_get(jroot, "available")));
    ASSERT_EQ((int)yyjson_get_int(yyjson_obj_get(jroot, "commits_1y")), 0);
    ASSERT_EQ((int)yyjson_arr_size(yyjson_obj_get(jroot, "people")), 0);
    yyjson_doc_free(doc);
    free(json);

    cbm_store_close(store);
    cbm_atlas_metrics_cache_clear();
    (void)th_rmtree(root);
    PASS();
}

/* Per-symbol history: `git log -L` over the symbol's line range, with the
 * forge base URL normalized for the UI's commit/#ref links. Runs against a
 * real throwaway git repo; environments without git pass through (the
 * feature itself degrades to available:false the same way). */
TEST(atlas_symbol_history_log_l_and_remote) {
    /* Invalid input never spawns a subprocess. */
    cbm_store_t *probe = cbm_store_open_memory();
    ASSERT_NOT_NULL(probe);
    cbm_store_upsert_project(probe, "hist-test", "/tmp/nonexistent-hist");
    ASSERT_NULL(cbm_atlas_symbol_history_json(probe, "hist-test", "src/w1.c", 0, 3));
    ASSERT_NULL(cbm_atlas_symbol_history_json(probe, "hist-test", "src/w1.c", 5, 3));
    /* Traversal in the file path is refused, honestly. */
    char *json = cbm_atlas_symbol_history_json(probe, "hist-test", "../etc/passwd", 1, 3);
    ASSERT_NOT_NULL(json);
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_FALSE(yyjson_get_bool(yyjson_obj_get(yyjson_doc_get_root(doc), "available")));
    yyjson_doc_free(doc);
    free(json);
    cbm_store_close(probe);

#ifdef _WIN32
    if (system("git --version >NUL 2>&1") != 0)
#else
    if (system("git --version >/dev/null 2>&1") != 0)
#endif
        PASS(); /* no git in this environment; the guarded paths above ran */

    /* cbm_mkdtemp rewrites /tmp/… to %TEMP%\… on Windows and copies the
     * expanded path back — the template buffer must hold it (CBM_SZ_256
     * convention; a literal-sized array is a stack overflow there). */
    char tmpl[256] = "/tmp/cbm_hist_XXXXXX";
    char *root = cbm_mkdtemp(tmpl);
    ASSERT_NOT_NULL(root);
    char dir[1024];
    snprintf(dir, sizeof(dir), "%s/src", root);
    cbm_mkdir_p(dir, 0755);
    char file[1024];
    snprintf(file, sizeof(file), "%s/src/w1.c", root);
    FILE *fp = cbm_fopen(file, "wb");
    ASSERT_NOT_NULL(fp);
    fputs("int a(void) {\n    return 1;\n}\n\nint b(void) {\n    return 2;\n}\n", fp);
    fclose(fp);
    char cmd[2048];
    snprintf(cmd, sizeof(cmd),
             "git -C \"%s\" init -q && git -C \"%s\" add . && "
             "git -C \"%s\" -c user.name=Test -c user.email=t@t.io commit -qm \"seed a for #42\" "
             "&& git -C \"%s\" remote add origin git@github.com:foo/bar.git",
             root, root, root, root);
    if (system(cmd) != 0) {
        (void)th_rmtree(root);
        PASS(); /* git present but unusable here (sandboxed config, etc.) */
    }

    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);
    cbm_store_upsert_project(store, "hist-test", root);
    json = cbm_atlas_symbol_history_json(store, "hist-test", "src/w1.c", 1, 3);
    ASSERT_NOT_NULL(json);
    doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *hroot = yyjson_doc_get_root(doc);
    ASSERT_TRUE(yyjson_get_bool(yyjson_obj_get(hroot, "available")));
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(hroot, "remote_url")),
                  "https://github.com/foo/bar");
    yyjson_val *commits = yyjson_obj_get(hroot, "commits");
    ASSERT_EQ((int)yyjson_arr_size(commits), 1);
    yyjson_val *commit = yyjson_arr_get(commits, 0);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(commit, "subject")), "seed a for #42");
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(commit, "author")), "Test");
    ASSERT_FALSE(yyjson_get_bool(yyjson_obj_get(hroot, "truncated")));
    yyjson_doc_free(doc);
    free(json);
    cbm_store_close(store);
    (void)th_rmtree(root);
    PASS();
}

TEST(atlas_why_extracts_guard_chains) {
    /* Same CBM_SZ_256 convention: the Windows expansion must fit. */
    char tmpl[256] = "/tmp/cbm_why_XXXXXX";
    char *root = cbm_mkdtemp(tmpl);
    ASSERT_NOT_NULL(root);
    char dir[1024];
    snprintf(dir, sizeof(dir), "%s/src", root);
    cbm_mkdir_p(dir, 0755);
    char file[1024];
    snprintf(file, sizeof(file), "%s/src/w1.c", root);
    FILE *fp = cbm_fopen(file, "wb");
    ASSERT_NOT_NULL(fp);
    fputs(WHY_SAMPLE_C, fp);
    fclose(fp);

    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);
    cbm_store_upsert_project(store, "why-test", root);

    static const char *names[4] = {"beta", "gamma_fn", "delta", "alpha"};
    int64_t ids[4];
    for (int i = 0; i < 4; i++) {
        cbm_node_t node;
        memset(&node, 0, sizeof(node));
        node.project = "why-test";
        node.label = "Function";
        node.name = names[i];
        char qn[64];
        snprintf(qn, sizeof(qn), "why-test::%s", names[i]);
        node.qualified_name = qn;
        node.file_path = "src/w1.c";
        node.start_line = i == 3 ? 4 : 1;
        node.end_line = i == 3 ? 15 : 1;
        ids[i] = cbm_store_upsert_node(store, &node);
        ASSERT_GT(ids[i], 0);
    }
    /* alpha → beta at line 7 (if mode>2 → if strict), alpha → gamma_fn at
     * line 13 (loop), alpha → delta at line 9 (else arm of strict). */
    struct {
        int target;
        const char *props;
    } calls[3] = {{0, "{\"line\":7}"}, {1, "{\"line\":13}"}, {2, "{\"line\":9}"}};
    for (int i = 0; i < 3; i++) {
        cbm_edge_t edge;
        memset(&edge, 0, sizeof(edge));
        edge.project = "why-test";
        edge.source_id = ids[3];
        edge.target_id = ids[calls[i].target];
        edge.type = "CALLS";
        edge.properties_json = calls[i].props;
        ASSERT_GT(cbm_store_insert_edge(store, &edge), 0);
    }

    /* Upward: when does beta run? One caller (alpha), guards outermost-first
     * = [mode > 2, strict]. */
    char *json = cbm_atlas_why_json(store, "why-test", ids[0], NULL, true);
    ASSERT_NOT_NULL(json);
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *root_v = yyjson_doc_get_root(doc);
    yyjson_val *entries = yyjson_obj_get(root_v, "entries");
    ASSERT_EQ((int)yyjson_arr_size(entries), 1);
    yyjson_val *entry = yyjson_arr_get(entries, 0);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(entry, "name")), "alpha");
    yyjson_val *guards = yyjson_obj_get(entry, "guards");
    ASSERT_EQ((int)yyjson_arr_size(guards), 2);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(yyjson_arr_get(guards, 0), "cond")), "mode > 2");
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(yyjson_arr_get(guards, 1), "cond")), "strict");
    ASSERT_NULL(yyjson_obj_get(yyjson_arr_get(guards, 1), "negated"));
    yyjson_doc_free(doc);
    free(json);

    /* delta sits in the ELSE arm: same conditions, innermost negated. */
    json = cbm_atlas_why_json(store, "why-test", ids[2], NULL, true);
    ASSERT_NOT_NULL(json);
    doc = yyjson_read(json, strlen(json), 0);
    entry = yyjson_arr_get(yyjson_obj_get(yyjson_doc_get_root(doc), "entries"), 0);
    guards = yyjson_obj_get(entry, "guards");
    ASSERT_EQ((int)yyjson_arr_size(guards), 2);
    ASSERT_TRUE(yyjson_get_bool(yyjson_obj_get(yyjson_arr_get(guards, 1), "negated")));
    yyjson_doc_free(doc);
    free(json);

    /* gamma_fn runs in a loop; downward from alpha lists all three callees
     * with their guards computed from alpha's own file. */
    json = cbm_atlas_why_json(store, "why-test", ids[1], NULL, true);
    ASSERT_NOT_NULL(json);
    doc = yyjson_read(json, strlen(json), 0);
    entry = yyjson_arr_get(yyjson_obj_get(yyjson_doc_get_root(doc), "entries"), 0);
    ASSERT_TRUE(yyjson_get_bool(yyjson_obj_get(entry, "loop")));
    yyjson_doc_free(doc);
    free(json);

    json = cbm_atlas_why_json(store, "why-test", ids[3], NULL, false);
    ASSERT_NOT_NULL(json);
    doc = yyjson_read(json, strlen(json), 0);
    root_v = yyjson_doc_get_root(doc);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(root_v, "direction")), "down");
    ASSERT_EQ((int)yyjson_arr_size(yyjson_obj_get(root_v, "entries")), 3);
    yyjson_doc_free(doc);
    free(json);

    /* Callsite guards helper (the A→B trace hop path). */
    json = cbm_atlas_callsite_guards_json(store, "why-test", "src/w1.c", 7);
    ASSERT_NOT_NULL(json);
    doc = yyjson_read(json, strlen(json), 0);
    ASSERT_EQ((int)yyjson_arr_size(yyjson_obj_get(yyjson_doc_get_root(doc), "guards")), 2);
    yyjson_doc_free(doc);
    free(json);

    cbm_store_close(store);
    th_rmtree(root);
    PASS();
}

/* ── Suite ────────────────────────────────────────────────────── */

SUITE(ui) {
    /* Config */
    RUN_TEST(config_load_defaults);
    RUN_TEST(config_save_and_reload);
    RUN_TEST(config_save_atomically_replaces_a_complete_generation);
    RUN_TEST(config_overwrite);
    RUN_TEST(config_corrupt_file);
    RUN_TEST(config_missing_fields);

    /* Embedded assets (stub) */
    RUN_TEST(embedded_lookup_not_found);
    RUN_TEST(embedded_stub_count);

    /* Layout engine */
    RUN_TEST(layout_empty_graph);
    RUN_TEST(layout_single_node);
    RUN_TEST(layout_two_connected);
    RUN_TEST(layout_respects_max_nodes);
    RUN_TEST(layout_clamps_render_cap_from_env);
    RUN_TEST(layout_honors_budget_above_default);
    RUN_TEST(layout_deterministic);
    RUN_TEST(layout_to_json);
    RUN_TEST(layout_null_inputs);
    RUN_TEST(layout_dead_code_classification);
    RUN_TEST(layout_coincident_nodes_bounded);

    /* CBM Atlas region level */
    RUN_TEST(layout_regions_two_communities);
    RUN_TEST(layout_regions_scope_restricts_nodes);

    /* CBM Atlas services */
    RUN_TEST(atlas_tree_aggregates_children);
    RUN_TEST(atlas_symbol_bundle_totals_and_sections);
    RUN_TEST(atlas_flows_walk_and_rank);
    RUN_TEST(atlas_metrics_totals_and_certainty);
    RUN_TEST(atlas_trace_reachability_calls_and_data);
    RUN_TEST(atlas_impact_reverse_reachability_and_tests);
    RUN_TEST(atlas_symbol_data_flows_and_history_shape);
    RUN_TEST(atlas_scent_buckets_matches_by_region);
    RUN_TEST(atlas_bridges_rank_by_region_reach);
    RUN_TEST(atlas_symbol_overflow_tail_and_dataflow_presence);
    RUN_TEST(atlas_blast_buckets_files_by_region);
    RUN_TEST(atlas_handout_is_selfcontained_and_escaped);
    RUN_TEST(atlas_attach_observed_marks_hops);
    RUN_TEST(atlas_who_reports_authorship_evidence);
    RUN_TEST(atlas_symbol_history_log_l_and_remote);
    RUN_TEST(atlas_why_extracts_guard_chains);
}
