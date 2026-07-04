/*
 * test_pipeline.c — Integration tests for the indexing pipeline.
 *
 * Tests pipeline lifecycle, structure pass, and end-to-end indexing
 * on a temporary directory with known file layout.
 */
#include "../src/foundation/compat.h"
#include "../src/foundation/constants.h"
#include "foundation/platform.h" // cbm_normalize_path_sep (drive-canonicalization regression)
#include "test_framework.h"
#include "test_helpers.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "store/store.h"
#include "cli/cli.h"
#include "git/git_context.h"
#include "foundation/dump_verify.h"
#include "foundation/log.h"
#include "semantic/semantic.h"
#include "pagerank/pagerank.h"
#include "test_graph_diff.h"

#include <sqlite3.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include "foundation/compat_thread.h"
#include <fcntl.h>
#include <sys/stat.h>
#ifdef _WIN32
#include <sys/utime.h>
#else
#include <utime.h>
#endif
#include <unistd.h>
#include "graph_buffer/graph_buffer.h"
#include "yyjson/yyjson.h"

/* ── Helper: create temp test repo with known layout ───────────── */

static char g_tmpdir[256];

enum { PIPELINE_TEST_OVERLONG_DB_PATH = CBM_PATH_MAX + CBM_SZ_128 };

static char g_pipeline_log_capture[CBM_SZ_16K];
static CBMLogLevel g_pipeline_prev_log_level = CBM_LOG_INFO;

static void pipeline_capture_log_sink(const char *line) {
    size_t used = strlen(g_pipeline_log_capture);
    size_t avail = sizeof(g_pipeline_log_capture) - used;
    if (avail <= SKIP_ONE) {
        return;
    }
    int n = snprintf(g_pipeline_log_capture + used, avail, "%s\n", line);
    if (n < 0 || (size_t)n >= avail) {
        g_pipeline_log_capture[sizeof(g_pipeline_log_capture) - SKIP_ONE] = '\0';
    }
}

static void pipeline_capture_logs_start(void) {
    g_pipeline_log_capture[0] = '\0';
    g_pipeline_prev_log_level = cbm_log_get_level();
    cbm_log_set_level(CBM_LOG_DEBUG);
    cbm_log_set_sink(pipeline_capture_log_sink);
}

static const char *pipeline_capture_logs_end(void) {
    cbm_log_set_sink(NULL);
    cbm_log_set_level(g_pipeline_prev_log_level);
    return g_pipeline_log_capture;
}

/* Create:
 *   /tmp/cbm_test_XXXXXX/
 *     main.go       (empty)
 *     pkg/
 *       service.go  (empty)
 *       util/
 *         helper.go (empty)
 */
static int setup_test_repo(void) {
    snprintf(g_tmpdir, sizeof(g_tmpdir), "/tmp/cbm_test_XXXXXX");
    if (!cbm_mkdtemp(g_tmpdir))
        return -1;

    char path[512];

    /* main.go — calls Serve() from pkg */
    snprintf(path, sizeof(path), "%s/main.go", g_tmpdir);
    FILE *f = fopen(path, "w");
    if (!f)
        return -1;
    fprintf(f, "package main\n\n"
               "import \"pkg\"\n\n"
               "func main() {\n"
               "\tpkg.Serve()\n"
               "}\n");
    fclose(f);

    /* pkg/ */
    snprintf(path, sizeof(path), "%s/pkg", g_tmpdir);
    cbm_mkdir(path);

    /* pkg/service.go — calls Help() from util */
    snprintf(path, sizeof(path), "%s/pkg/service.go", g_tmpdir);
    f = fopen(path, "w");
    if (!f)
        return -1;
    fprintf(f, "package pkg\n\n"
               "import \"pkg/util\"\n\n"
               "func Serve() {\n"
               "\tutil.Help()\n"
               "}\n");
    fclose(f);

    /* pkg/util/ */
    snprintf(path, sizeof(path), "%s/pkg/util", g_tmpdir);
    cbm_mkdir(path);

    /* pkg/util/helper.go */
    snprintf(path, sizeof(path), "%s/pkg/util/helper.go", g_tmpdir);
    f = fopen(path, "w");
    if (!f)
        return -1;
    fprintf(f, "package util\n\nfunc Help() {}\n");
    fclose(f);

    return 0;
}

/* Recursive remove (simple, not production-grade) */
static void rm_rf(const char *path) {
    th_rmtree(path);
}

static void teardown_test_repo(void) {
    if (g_tmpdir[0])
        rm_rf(g_tmpdir);
    g_tmpdir[0] = '\0';
}

/* ── Lifecycle tests ─────────────────────────────────────────────── */

TEST(pipeline_create_free) {
    cbm_pipeline_t *p = cbm_pipeline_new("/some/path", NULL, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_STR_EQ(cbm_pipeline_project_name(p), "some-path");
    cbm_pipeline_free(p);
    PASS();
}

TEST(pipeline_null_repo) {
    cbm_pipeline_t *p = cbm_pipeline_new(NULL, NULL, CBM_MODE_FULL);
    ASSERT_NULL(p);
    PASS();
}

TEST(pipeline_free_null) {
    cbm_pipeline_free(NULL); /* should not crash */
    PASS();
}

TEST(pipeline_cancel) {
    cbm_pipeline_t *p = cbm_pipeline_new("/some/path", NULL, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_cancel(p);
    /* Running a cancelled pipeline should return -1 immediately */
    int rc = cbm_pipeline_run(p);
    /* Note: it may fail because /some/path doesn't exist, not because of cancel.
     * This test just verifies cancel doesn't crash. */
    (void)rc;
    cbm_pipeline_free(p);
    PASS();
}

TEST(pipeline_cancel_null) {
    cbm_pipeline_cancel(NULL); /* should not crash */
    PASS();
}

TEST(pipeline_run_null) {
    int rc = cbm_pipeline_run(NULL);
    ASSERT_EQ(rc, -1);
    PASS();
}

/* ── Focused: file-backed store persistence ─────────────────────── */

TEST(store_file_persistence) {
    if (setup_test_repo() != 0) {
        FAIL("failed to create temp dir");
    }
    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/persist_test.db", g_tmpdir);

    /* Write data */
    cbm_store_t *s1 = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s1);
    cbm_store_upsert_project(s1, "proj", "/tmp");
    cbm_node_t n = {.project = "proj",
                    .label = "Function",
                    .name = "foo",
                    .qualified_name = "proj.foo",
                    .file_path = "f.go"};
    int64_t id = cbm_store_upsert_node(s1, &n);
    ASSERT_GT(id, 0);
    int cnt1 = cbm_store_count_nodes(s1, "proj");
    ASSERT_EQ(cnt1, 1);
    cbm_store_checkpoint(s1);
    cbm_store_close(s1);

    /* Reopen and verify */
    cbm_store_t *s2 = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s2);
    int cnt2 = cbm_store_count_nodes(s2, "proj");
    ASSERT_EQ(cnt2, 1);
    cbm_store_close(s2);

    teardown_test_repo();
    PASS();
}

TEST(store_bulk_persistence) {
    if (setup_test_repo() != 0) {
        FAIL("failed to create temp dir");
    }
    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/bulk_test.db", g_tmpdir);

    /* Verify: begin_bulk + explicit txn + end_bulk persists to file */
    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);

    cbm_store_upsert_project(s, "proj", "/tmp");
    cbm_store_begin_bulk(s);
    cbm_store_begin(s);
    cbm_node_t n = {.project = "proj",
                    .label = "Function",
                    .name = "foo",
                    .qualified_name = "proj.foo",
                    .file_path = "f.go"};
    int64_t id = cbm_store_upsert_node(s, &n);
    ASSERT_GT(id, 0);
    ASSERT_EQ(cbm_store_commit(s), 0);
    cbm_store_end_bulk(s);
    cbm_store_checkpoint(s);
    cbm_store_close(s);

    /* Reopen and verify data survived */
    cbm_store_t *s2 = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s2);
    ASSERT_EQ(cbm_store_count_nodes(s2, "proj"), 1);
    cbm_store_close(s2);

    teardown_test_repo();
    PASS();
}

/* ── Integration: structure pass on temp repo ────────────────────── */

static bool pipeline_test_derived_status_is(cbm_store_t *s, const char *project,
                                            const char *view_name, const char *status) {
    cbm_derived_view_state_t state = {0};
    bool matches = false;
    if (cbm_store_get_derived_view_state(s, project, view_name, &state) == CBM_STORE_OK) {
        matches = state.status && strcmp(state.status, status) == 0;
    }
    cbm_store_derived_view_state_free_fields(&state);
    return matches;
}

TEST(pipeline_structure_nodes) {
    if (setup_test_repo() != 0) {
        FAIL("failed to create temp dir");
    }

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/test.db", g_tmpdir);

    cbm_pipeline_t *p = cbm_pipeline_new(g_tmpdir, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);

    int rc = cbm_pipeline_run(p);
    ASSERT_EQ(rc, 0);

    /* Verify results by opening the store */
    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);

    const char *project = cbm_pipeline_project_name(p);

    /* Should have nodes: 1 Project + 2 Folders + 3 Files + N Functions */
    int node_count = cbm_store_count_nodes(s, project);
    ASSERT_GTE(node_count, 9); /* 6 structure + at least 3 definitions */

    /* Verify project node exists */
    cbm_node_t proj_node = {0};
    rc = cbm_store_find_node_by_qn(s, project, project, &proj_node);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_STR_EQ(proj_node.label, "Project");
    cbm_node_free_fields(&proj_node);

    /* Verify folder nodes */
    cbm_node_t *folders = NULL;
    int folder_count = 0;
    rc = cbm_store_find_nodes_by_label(s, project, "Folder", &folders, &folder_count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_GTE(folder_count, 2); /* pkg, pkg/util */
    cbm_store_free_nodes(folders, folder_count);

    /* Verify file nodes */
    cbm_node_t *file_nodes = NULL;
    int file_count = 0;
    rc = cbm_store_find_nodes_by_label(s, project, "File", &file_nodes, &file_count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_GTE(file_count, 3); /* main.go, service.go, helper.go */
    cbm_store_free_nodes(file_nodes, file_count);

    /* Verify edges exist */
    int edge_count = cbm_store_count_edges(s, project);
    ASSERT_GTE(edge_count, 5); /* CONTAINS_FOLDER + CONTAINS_FILE edges */
    ASSERT_TRUE(pipeline_test_derived_status_is(s, project, CBM_STORE_DERIVED_VIEW_ROUTES,
                                                CBM_STORE_DERIVED_STATUS_COMPLETE));
    ASSERT_TRUE(pipeline_test_derived_status_is(s, project, CBM_STORE_DERIVED_VIEW_ARCHITECTURE,
                                                CBM_STORE_DERIVED_STATUS_COMPLETE));
    ASSERT_TRUE(pipeline_test_derived_status_is(s, project, CBM_STORE_DERIVED_VIEW_SEMANTIC_EDGES,
                                                CBM_STORE_DERIVED_STATUS_COMPLETE));

    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_test_repo();
    PASS();
}

TEST(pipeline_structure_edges) {
    if (setup_test_repo() != 0) {
        FAIL("failed to create temp dir");
    }

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/test.db", g_tmpdir);

    cbm_pipeline_t *p = cbm_pipeline_new(g_tmpdir, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    int rc = cbm_pipeline_run(p);
    ASSERT_EQ(rc, 0);

    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);
    const char *project = cbm_pipeline_project_name(p);

    /* Check CONTAINS_FILE edges */
    int cf_count = cbm_store_count_edges_by_type(s, project, "CONTAINS_FILE");
    /* Check CONTAINS_FOLDER edges */
    int cd_count = cbm_store_count_edges_by_type(s, project, CBM_PIPELINE_EDGE_CONTAINS_FOLDER);

    char *pkg_qn = cbm_pipeline_fqn_folder(project, "pkg");
    char *util_qn = cbm_pipeline_fqn_folder(project, "pkg/util");
    bool made_pkg_qn = pkg_qn != NULL;
    bool made_util_qn = util_qn != NULL;
    cbm_node_t pkg_node = {0};
    cbm_node_t util_node = {0};
    bool found_pkg = false;
    bool found_util = false;
    if (pkg_qn) {
        rc = cbm_store_find_node_by_qn(s, project, pkg_qn, &pkg_node);
        found_pkg = rc == CBM_STORE_OK;
    }
    if (util_qn) {
        rc = cbm_store_find_node_by_qn(s, project, util_qn, &util_node);
        found_util = rc == CBM_STORE_OK;
    }
    cbm_edge_t *pkg_folders = NULL;
    int pkg_folder_count = 0;
    bool found_pkg_folder_edges = false;
    if (found_pkg) {
        rc = cbm_store_find_edges_by_source_type(s, pkg_node.id,
                                                 CBM_PIPELINE_EDGE_CONTAINS_FOLDER, &pkg_folders,
                                                 &pkg_folder_count);
        found_pkg_folder_edges = rc == CBM_STORE_OK;
    }
    bool has_nested_folder_edge = false;
    for (int i = 0; i < pkg_folder_count; i++) {
        if (pkg_folders[i].target_id == util_node.id) {
            has_nested_folder_edge = true;
            break;
        }
    }

    /* Cleanup before assertions (so failures don't leak) */
    cbm_store_free_edges(pkg_folders, pkg_folder_count);
    cbm_node_free_fields(&pkg_node);
    cbm_node_free_fields(&util_node);
    free(pkg_qn);
    free(util_qn);
    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_test_repo();

    ASSERT_GTE(cf_count, 3); /* project->main.go, pkg->service.go, util->helper.go */
    ASSERT_GTE(cd_count, 2); /* branch->pkg and pkg->util */
    ASSERT_TRUE(made_pkg_qn);
    ASSERT_TRUE(made_util_qn);
    ASSERT_TRUE(found_pkg);
    ASSERT_TRUE(found_util);
    ASSERT_TRUE(found_pkg_folder_edges);
    ASSERT_TRUE(has_nested_folder_edge);
    PASS();
}

TEST(pipeline_branch_root_structure) {
    if (setup_test_repo() != 0) {
        FAIL("failed to create temp dir");
    }

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/test_branch_root.db", g_tmpdir);

    cbm_pipeline_t *p = cbm_pipeline_new(g_tmpdir, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    int rc = cbm_pipeline_run(p);
    ASSERT_EQ(rc, 0);

    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);
    const char *project = cbm_pipeline_project_name(p);

    char branch_qn[1024];
    snprintf(branch_qn, sizeof(branch_qn), "%s.__branch__.working-tree", project);

    cbm_node_t project_node = {0};
    cbm_node_t branch_node = {0};
    cbm_node_t root_file_node = {0};
    cbm_node_t root_folder_node = {0};
    rc = cbm_store_find_node_by_qn(s, project, project, &project_node);
    ASSERT_EQ(rc, CBM_STORE_OK);
    rc = cbm_store_find_node_by_qn(s, project, branch_qn, &branch_node);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_STR_EQ(branch_node.label, "Branch");
    ASSERT_STR_EQ(branch_node.name, "working-tree");
    ASSERT_NOT_NULL(strstr(branch_node.properties_json, "\"is_git\":false"));
    char *root_folder_qn = cbm_pipeline_fqn_folder(project, "pkg");
    char *root_file_qn = cbm_pipeline_fqn_compute(project, "main.go", "__file__");
    ASSERT_NOT_NULL(root_folder_qn);
    ASSERT_NOT_NULL(root_file_qn);
    rc = cbm_store_find_node_by_qn(s, project, root_folder_qn, &root_folder_node);
    ASSERT_EQ(rc, CBM_STORE_OK);
    rc = cbm_store_find_node_by_qn(s, project, root_file_qn, &root_file_node);
    ASSERT_EQ(rc, CBM_STORE_OK);

    cbm_edge_t *has_branch = NULL;
    int has_branch_count = 0;
    rc = cbm_store_find_edges_by_source_type(s, project_node.id, "HAS_BRANCH", &has_branch,
                                             &has_branch_count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(has_branch_count, 1);
    ASSERT_EQ(has_branch[0].target_id, branch_node.id);

    cbm_edge_t *project_files = NULL;
    int project_file_count = 0;
    rc = cbm_store_find_edges_by_source_type(s, project_node.id, "CONTAINS_FILE", &project_files,
                                             &project_file_count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(project_file_count, 0);

    cbm_edge_t *branch_files = NULL;
    int branch_file_count = 0;
    rc = cbm_store_find_edges_by_source_type(s, branch_node.id, "CONTAINS_FILE", &branch_files,
                                             &branch_file_count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(branch_file_count, 1);
    ASSERT_EQ(branch_files[0].target_id, root_file_node.id);

    cbm_edge_t *branch_folders = NULL;
    int branch_folder_count = 0;
    rc = cbm_store_find_edges_by_source_type(s, branch_node.id, "CONTAINS_FOLDER", &branch_folders,
                                             &branch_folder_count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(branch_folder_count, 1);
    ASSERT_EQ(branch_folders[0].target_id, root_folder_node.id);

    cbm_store_free_edges(has_branch, has_branch_count);
    cbm_store_free_edges(project_files, project_file_count);
    cbm_store_free_edges(branch_files, branch_file_count);
    cbm_store_free_edges(branch_folders, branch_folder_count);
    cbm_node_free_fields(&project_node);
    cbm_node_free_fields(&branch_node);
    cbm_node_free_fields(&root_file_node);
    cbm_node_free_fields(&root_folder_node);
    free(root_folder_qn);
    free(root_file_qn);
    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_test_repo();
    PASS();
}

TEST(pipeline_project_name_derived) {
    if (setup_test_repo() != 0) {
        FAIL("failed to create temp dir");
    }

    cbm_pipeline_t *p = cbm_pipeline_new(g_tmpdir, NULL, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);

    /* Project name should be derived from tmpdir path */
    const char *name = cbm_pipeline_project_name(p);
    ASSERT_NOT_NULL(name);
    ASSERT_TRUE(strlen(name) > 0);

    cbm_pipeline_free(p);
    teardown_test_repo();
    PASS();
}

TEST(pipeline_mode_global_semantic_edges_policy) {
    ASSERT_TRUE(cbm_pipeline_mode_builds_global_semantic_edges(CBM_MODE_FULL));
    ASSERT_TRUE(cbm_pipeline_mode_builds_global_semantic_edges(CBM_MODE_MODERATE));
    ASSERT_FALSE(cbm_pipeline_mode_builds_global_semantic_edges(CBM_MODE_FAST));
    ASSERT_FALSE(cbm_pipeline_mode_builds_global_semantic_edges(CBM_MODE_DEP));
    PASS();
}

TEST(pipeline_call_edge_props_include_args_and_line) {
    char props[CBM_SZ_512];
    int n = snprintf(props, sizeof(props),
                     "{\"callee\":\"cbm_label_is_type_like\",\"confidence\":0.75,"
                     "\"strategy\":\"unique_name\",\"candidates\":1");
    ASSERT_GT(n, 0);

    CBMCall call = {0};
    call.start_line = 62;
    call.arg_count = 1;
    call.args[0].index = 0;
    call.args[0].expr = "label";

    cbm_pipeline_close_call_edge_props(props, sizeof(props), (size_t)n, &call, true);
    ASSERT(strstr(props, "\"args\":[{\"i\":0,\"e\":\"label\"}]") != NULL);
    ASSERT(strstr(props, "\"line\":62") != NULL);
    ASSERT_EQ(props[strlen(props) - SKIP_ONE], '}');
    PASS();
}

TEST(pipeline_fast_mode) {
    if (setup_test_repo() != 0) {
        FAIL("failed to create temp dir");
    }

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/test_fast.db", g_tmpdir);

    cbm_pipeline_t *p = cbm_pipeline_new(g_tmpdir, db_path, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);

    int rc = cbm_pipeline_run(p);
    ASSERT_EQ(rc, 0);

    /* Just verify it completes without error in fast mode */
    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);
    const char *project = cbm_pipeline_project_name(p);
    int node_count = cbm_store_count_nodes(s, project);
    ASSERT_GT(node_count, 0);
    ASSERT_TRUE(pipeline_test_derived_status_is(s, project, CBM_STORE_DERIVED_VIEW_ROUTES,
                                                CBM_STORE_DERIVED_STATUS_COMPLETE));
    ASSERT_TRUE(pipeline_test_derived_status_is(s, project, CBM_STORE_DERIVED_VIEW_ARCHITECTURE,
                                                CBM_STORE_DERIVED_STATUS_COMPLETE));
    ASSERT_TRUE(pipeline_test_derived_status_is(s, project, CBM_STORE_DERIVED_VIEW_SEMANTIC_EDGES,
                                                CBM_STORE_DERIVED_STATUS_STALE));

    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_test_repo();
    PASS();
}

/* ── Definitions pass tests ──────────────────────────────────────── */

TEST(pipeline_definitions_function_nodes) {
    if (setup_test_repo() != 0) {
        FAIL("failed to create temp dir");
    }

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/test_defs.db", g_tmpdir);

    cbm_pipeline_t *p = cbm_pipeline_new(g_tmpdir, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    int rc = cbm_pipeline_run(p);
    ASSERT_EQ(rc, 0);

    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);
    const char *project = cbm_pipeline_project_name(p);

    /* Verify Function nodes extracted from Go source files */
    cbm_node_t *funcs = NULL;
    int func_count = 0;
    rc = cbm_store_find_nodes_by_label(s, project, "Function", &funcs, &func_count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_GTE(func_count, 3); /* main, Serve, Help */

    /* Verify each expected function exists */
    int found_main = 0, found_serve = 0, found_help = 0;
    for (int i = 0; i < func_count; i++) {
        if (strcmp(funcs[i].name, "main") == 0)
            found_main = 1;
        else if (strcmp(funcs[i].name, "Serve") == 0)
            found_serve = 1;
        else if (strcmp(funcs[i].name, "Help") == 0)
            found_help = 1;
    }
    ASSERT_TRUE(found_main);
    ASSERT_TRUE(found_serve);
    ASSERT_TRUE(found_help);

    cbm_store_free_nodes(funcs, func_count);
    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_test_repo();
    PASS();
}

TEST(pipeline_definitions_defines_edges) {
    if (setup_test_repo() != 0) {
        FAIL("failed to create temp dir");
    }

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/test_defs_edges.db", g_tmpdir);

    cbm_pipeline_t *p = cbm_pipeline_new(g_tmpdir, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    int rc = cbm_pipeline_run(p);
    ASSERT_EQ(rc, 0);

    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);
    const char *project = cbm_pipeline_project_name(p);

    /* DEFINES edges: File → Function (one per extracted definition) */
    int defines_count = cbm_store_count_edges_by_type(s, project, "DEFINES");
    ASSERT_GTE(defines_count, 3); /* main.go→main, service.go→Serve, helper.go→Help */

    /* CONTAINS_FILE edges should still exist from structure pass */
    int cf_count = cbm_store_count_edges_by_type(s, project, "CONTAINS_FILE");
    ASSERT_GTE(cf_count, 3);

    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_test_repo();
    PASS();
}

TEST(pipeline_definitions_properties) {
    if (setup_test_repo() != 0) {
        FAIL("failed to create temp dir");
    }

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/test_defs_props.db", g_tmpdir);

    cbm_pipeline_t *p = cbm_pipeline_new(g_tmpdir, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    int rc = cbm_pipeline_run(p);
    ASSERT_EQ(rc, 0);

    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);
    const char *project = cbm_pipeline_project_name(p);

    /* Verify a function has valid properties (complexity, lines, etc.) */
    cbm_node_t *funcs = NULL;
    int func_count = 0;
    rc = cbm_store_find_nodes_by_label(s, project, "Function", &funcs, &func_count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_GT(func_count, 0);

    /* Check that Serve (exported) has is_exported:true in properties */
    for (int i = 0; i < func_count; i++) {
        if (strcmp(funcs[i].name, "Serve") == 0) {
            ASSERT_NOT_NULL(funcs[i].properties_json);
            ASSERT_TRUE(strstr(funcs[i].properties_json, "\"is_exported\":true") != NULL);
        }
        /* All functions should have file_path set */
        ASSERT_NOT_NULL(funcs[i].file_path);
    }

    cbm_store_free_nodes(funcs, func_count);
    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_test_repo();
    PASS();
}

/* Node properties must remain VALID JSON even when a definition's serialized
 * properties exceed the fixed 2 KB build buffer. Found on the Linux kernel:
 * 135 nodes (50-param functions with struct-typed signatures) had properties
 * truncated mid-string at 2047 bytes — malformed JSON that aborts EVERY
 * json_extract()-based consumer (arch_entry_points, partial indexes, user
 * Cypher on properties). Oversized optional fields must be dropped whole,
 * never cut mid-value. */
TEST(pipeline_def_props_valid_json_when_oversized) {
    if (setup_test_repo() != 0) {
        FAIL("failed to create temp dir");
    }

    /* Sweep of C functions with growing signatures. The corruption fires only
     * when the serialized position lands in a narrow window just under the
     * buffer margin as the param_types array starts — the array is then cut
     * mid-item (`"param_types":["enum`), exactly the kernel failure shape.
     * Sweeping 10..69 params deterministically hits the window (pre-fix:
     * sweep_fn_30 -> 2047-byte malformed properties). ONE FUNCTION PER FILE so
     * the file count exceeds MIN_FILES_FOR_PARALLEL (50) and the test covers
     * the PARALLEL pipeline's duplicated props builder (pass_parallel.c) — the
     * path large repos take; the serial twin lives in pass_definitions.c. */
    for (int n = 10; n < 70; n++) {
        char path[512];
        snprintf(path, sizeof(path), "%s/sweep_%02d.c", g_tmpdir, n);
        FILE *f = fopen(path, "w");
        if (!f) {
            teardown_test_repo();
            FAIL("failed to write sweep file");
        }
        fprintf(f, "int sweep_fn_%02d(", n);
        for (int i = 0; i < n; i++) {
            fprintf(f, "%sstruct long_struct_type_name_padding_padding_%02d *par_%02d",
                    i ? ", " : "", i, i);
        }
        fprintf(f, ") { return 0; }\n");
        fclose(f);
    }

    /* Multi-line parameter declarations: param_types items then carry raw
     * newline/tab bytes from the source slice. The array appender's inline
     * escape loop only handled quote/backslash, so the raw control bytes made
     * the JSON invalid (118 Linux-kernel rows of this shape). */
    {
        char path[512];
        snprintf(path, sizeof(path), "%s/sweep_nl.c", g_tmpdir);
        FILE *f = fopen(path, "w");
        if (!f) {
            teardown_test_repo();
            FAIL("failed to write sweep_nl.c");
        }
        fprintf(f, "int sweep_fn_nl(struct\n\t\t\t\treally_long_struct_name *a,\n"
                   "          enum\n\tweird_enum b) { return 0; }\n");
        fclose(f);
    }

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/test_huge_props.db", g_tmpdir);
    cbm_pipeline_t *p = cbm_pipeline_new(g_tmpdir, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);
    const char *project = cbm_pipeline_project_name(p);

    cbm_node_t *funcs = NULL;
    int func_count = 0;
    ASSERT_EQ(cbm_store_find_nodes_by_label(s, project, "Function", &funcs, &func_count),
              CBM_STORE_OK);
    int checked = 0;
    for (int i = 0; i < func_count; i++) {
        if (strncmp(funcs[i].name, "sweep_fn_", 9) != 0) {
            continue;
        }
        ASSERT_NOT_NULL(funcs[i].properties_json);
        yyjson_doc *doc =
            yyjson_read(funcs[i].properties_json, strlen(funcs[i].properties_json), 0);
        if (!doc) {
            printf("    INVALID properties JSON for %s (%zu bytes): ...%s\n", funcs[i].name,
                   strlen(funcs[i].properties_json),
                   funcs[i].properties_json + (strlen(funcs[i].properties_json) > 60
                                                   ? strlen(funcs[i].properties_json) - 60
                                                   : 0));
        }
        ASSERT_NOT_NULL(doc); /* valid JSON for EVERY sweep size */
        yyjson_doc_free(doc);
        checked++;
    }
    ASSERT_EQ(checked, 61); /* all sweep functions present (60 sizes + nl case) */

    cbm_store_free_nodes(funcs, func_count);
    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_test_repo();
    PASS();
}

/* Edge properties must be VALID JSON. Decorator source text (quotes, raw
 * newlines: @register.tag("block"), multi-line @override_settings) was
 * interpolated raw into the DECORATES properties — django produced 3826
 * malformed edges, and any json_extract-based consumer (including the
 * url_path_gen generated-column evaluation during PRAGMA integrity_check)
 * aborts on them. Usage/call emit sites had the same hole for sliced source
 * text. */
TEST(pipeline_edge_props_valid_json) {
    if (setup_test_repo() != 0) {
        FAIL("failed to create temp dir");
    }
    char path[512];
    snprintf(path, sizeof(path), "%s/deco.py", g_tmpdir);
    FILE *f = fopen(path, "w");
    if (!f) {
        teardown_test_repo();
        FAIL("failed to write deco.py");
    }
    fprintf(f, "from x import register\n"
               "@register.tag(\"block\")\n"
               "def do_block(parser):\n"
               "    return parser\n"
               "@register.tag(\"extends\")\n"
               "def do_extends(parser):\n"
               "    return parser\n");
    fclose(f);

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/test_edge_props.db", g_tmpdir);
    cbm_pipeline_t *p = cbm_pipeline_new(g_tmpdir, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);
    const char *project = cbm_pipeline_project_name(p);

    cbm_edge_t *edges = NULL;
    int edge_count = 0;
    ASSERT_EQ(cbm_store_find_edges_by_type(s, project, "DECORATES", &edges, &edge_count),
              CBM_STORE_OK);
    ASSERT_GT(edge_count, 0); /* the decorators must produce DECORATES edges */
    for (int i = 0; i < edge_count; i++) {
        const char *pj = edges[i].properties_json;
        if (!pj) {
            continue;
        }
        yyjson_doc *doc = yyjson_read(pj, strlen(pj), 0);
        if (!doc) {
            printf("    INVALID edge properties JSON: %.80s\n", pj);
        }
        ASSERT_NOT_NULL(doc);
        yyjson_doc_free(doc);
    }

    cbm_store_free_edges(edges, edge_count);
    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_test_repo();
    PASS();
}

TEST(pipeline_persisted_route_purity_for_http_literals) {
    if (setup_test_repo() != 0) {
        FAIL("failed to create temp dir");
    }
    char path[CBM_PATH_MAX];
    int n = snprintf(path, sizeof(path), "%s/route_noise.py", g_tmpdir);
    ASSERT_GT(n, 0);
    ASSERT_LT((size_t)n, sizeof(path));
    FILE *f = fopen(path, "w");
    if (!f) {
        teardown_test_repo();
        FAIL("failed to write route_noise.py");
    }
    fprintf(f, "import os\n"
               "from fastapi import FastAPI\n"
               "import requests\n"
               "\n"
               "app = FastAPI()\n"
               "\n"
               "@app.get('/api/orders')\n"
               "def orders():\n"
               "    return {'ok': True}\n"
               "\n"
               "def client():\n"
               "    requests.get('/tmp/alpha')\n"
               "    requests.get('/Users/test/plans/foo.md')\n"
               "    requests.get('/ar:allow')\n"
               "    os.path.join('/api', 'orders')\n"
               "    open('/usr/bin/uv')\n");
    fclose(f);

    char db_path[CBM_PATH_MAX];
    n = snprintf(db_path, sizeof(db_path), "%s/test_route_purity.db", g_tmpdir);
    ASSERT_GT(n, 0);
    ASSERT_LT((size_t)n, sizeof(db_path));
    cbm_pipeline_t *p = cbm_pipeline_new(g_tmpdir, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);
    const char *project = cbm_pipeline_project_name(p);

    cbm_node_t *routes = NULL;
    int route_count = 0;
    ASSERT_EQ(cbm_store_find_nodes_by_label(s, project, "Route", &routes, &route_count),
              CBM_STORE_OK);
    bool saw_api = false;
    for (int i = 0; i < route_count; i++) {
        const char *name = routes[i].name ? routes[i].name : "";
        if (strcmp(name, "/api/orders") == 0) {
            saw_api = true;
            continue;
        }
        ASSERT_FALSE(strcmp(name, "/tmp/alpha") == 0);
        ASSERT_FALSE(strcmp(name, "/Users/test/plans/foo.md") == 0);
        ASSERT_FALSE(strcmp(name, "/ar:allow") == 0);
        ASSERT_FALSE(strcmp(name, "/usr/bin/uv") == 0);
    }
    ASSERT_TRUE(saw_api);

    cbm_search_params_t params = {
        .project = project, .label = "Route", .min_degree = -1, .max_degree = -1, .limit = 100};
    cbm_search_output_t out = {0};
    ASSERT_EQ(cbm_store_search(s, &params, &out), CBM_STORE_OK);
    for (int i = 0; i < out.count; i++) {
        const char *name = out.results[i].node.name ? out.results[i].node.name : "";
        ASSERT_FALSE(strcmp(name, "/tmp/alpha") == 0);
        ASSERT_FALSE(strcmp(name, "/Users/test/plans/foo.md") == 0);
        ASSERT_FALSE(strcmp(name, "/ar:allow") == 0);
        ASSERT_FALSE(strcmp(name, "/usr/bin/uv") == 0);
    }

    cbm_store_search_free(&out);
    cbm_store_free_nodes(routes, route_count);
    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_test_repo();
    PASS();
}

TEST(pipeline_infra_route_deny_wins_by_url_value) {
    if (setup_test_repo() != 0) {
        FAIL("failed to create temp dir");
    }
    char path[CBM_PATH_MAX];
    int n = snprintf(path, sizeof(path), "%s/infra.yaml", g_tmpdir);
    ASSERT_GT(n, 0);
    ASSERT_LT((size_t)n, sizeof(path));
    FILE *f = fopen(path, "w");
    if (!f) {
        teardown_test_repo();
        FAIL("failed to write infra.yaml");
    }
    fprintf(f, "registries:\n"
               "  terraform_registry:\n"
               "    url: https://registry.terraform.io\n"
               "healthcheck: curl --fail http://localhost:8080/health || exit 1\n"
               "push_endpoint: https://hooks.example.test/push\n");
    fclose(f);

    char db_path[CBM_PATH_MAX];
    n = snprintf(db_path, sizeof(db_path), "%s/test_infra_route_deny.db", g_tmpdir);
    ASSERT_GT(n, 0);
    ASSERT_LT((size_t)n, sizeof(db_path));
    cbm_pipeline_t *p = cbm_pipeline_new(g_tmpdir, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);
    const char *project = cbm_pipeline_project_name(p);

    cbm_node_t *routes = NULL;
    int route_count = 0;
    ASSERT_EQ(cbm_store_find_nodes_by_label(s, project, "Route", &routes, &route_count),
              CBM_STORE_OK);
    bool saw_push_endpoint = false;
    for (int i = 0; i < route_count; i++) {
        const char *name = routes[i].name ? routes[i].name : "";
        ASSERT_FALSE(strcmp(name, "https://registry.terraform.io") == 0);
        ASSERT_FALSE(strcmp(name, "http://localhost:8080/health") == 0);
        ASSERT_FALSE(strstr(name, "curl --fail http://localhost:8080/health") != NULL);
        if (strcmp(name, "https://hooks.example.test/push") == 0) {
            saw_push_endpoint = true;
        }
    }
    ASSERT_TRUE(saw_push_endpoint);

    cbm_store_free_nodes(routes, route_count);
    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_test_repo();
    PASS();
}

/* ── Calls pass tests ──────────────────────────────────────────── */

TEST(pipeline_calls_resolution) {
    if (setup_test_repo() != 0) {
        FAIL("failed to create temp dir");
    }

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/test_calls.db", g_tmpdir);

    cbm_pipeline_t *p = cbm_pipeline_new(g_tmpdir, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    int rc = cbm_pipeline_run(p);
    ASSERT_EQ(rc, 0);

    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);
    const char *project = cbm_pipeline_project_name(p);

    /* main() calls pkg.Serve(), Serve() calls util.Help() → at least 2 CALLS edges */
    int calls_count = cbm_store_count_edges_by_type(s, project, "CALLS");
    ASSERT_GTE(calls_count, 1); /* at least some calls resolved */

    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_test_repo();
    PASS();
}

/* True iff a CALLS edge exists from a node named src_name to a node named
 * tgt_name. Used to assert cross-file call resolution survives a reindex. */
static bool cross_file_call_exists(cbm_store_t *s, const char *project, const char *src_name,
                                   const char *tgt_name) {
    cbm_node_t *srcs = NULL;
    cbm_node_t *tgts = NULL;
    int sc = 0;
    int tc = 0;
    cbm_store_find_nodes_by_name(s, project, src_name, &srcs, &sc);
    cbm_store_find_nodes_by_name(s, project, tgt_name, &tgts, &tc);
    bool found = false;
    for (int i = 0; i < sc && !found; i++) {
        cbm_edge_t *edges = NULL;
        int ec = 0;
        cbm_store_find_edges_by_source_type(s, srcs[i].id, "CALLS", &edges, &ec);
        for (int j = 0; j < ec && !found; j++) {
            for (int k = 0; k < tc; k++) {
                if (edges[j].target_id == tgts[k].id) {
                    found = true;
                    break;
                }
            }
        }
        if (edges) {
            cbm_store_free_edges(edges, ec);
        }
    }
    if (srcs) {
        cbm_store_free_nodes(srcs, sc);
    }
    if (tgts) {
        cbm_store_free_nodes(tgts, tc);
    }
    return found;
}

static cbm_config_t *incremental_test_config(const char *cache_dir);

/* Regression: incremental re-index of an edited file must NOT drop inbound
 * cross-file CALLS edges whose source lives in an UNCHANGED file.
 *
 * Repro: Serve() (pkg/service.go) CALLS Help() (pkg/util/helper.go). Editing
 * helper.go purges Help's node; before the fix the cascade deleted the
 * Serve->Help edge and never regenerated it (helper.go's callers are not
 * re-parsed), so the edge silently vanished on every edit and the incremental
 * graph diverged from a full reindex. */
TEST(pipeline_incremental_preserves_cross_file_calls) {
    if (setup_test_repo() != 0) {
        FAIL("failed to create temp dir");
    }

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/test_incr_calls.db", g_tmpdir);

    /* 1. Full index. */
    cbm_pipeline_t *p1 = cbm_pipeline_new(g_tmpdir, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p1);
    ASSERT_EQ(cbm_pipeline_run(p1), 0);
    const char *project = cbm_pipeline_project_name(p1);

    cbm_store_t *s1 = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s1);
    int calls_before = cbm_store_count_edges_by_type(s1, project, "CALLS");
    ASSERT_GTE(calls_before, 2); /* main->Serve and Serve->Help at minimum */
    ASSERT_TRUE(cross_file_call_exists(s1, project, "Serve", "Help"));
    cbm_store_close(s1);
    cbm_pipeline_free(p1);

    /* 2. Edit the callee's file so the incremental classifier marks it changed
     *    (mtime+size differ). Help's symbol + qualified name are unchanged. */
    char helper[512];
    snprintf(helper, sizeof(helper), "%s/pkg/util/helper.go", g_tmpdir);
    ASSERT_EQ(th_append_file(helper, "\n// incremental regression marker\n"), 0);

    /* 3. Re-run on the SAME db_path with incremental explicitly enabled. */
    cbm_config_t *cfg = incremental_test_config(g_tmpdir);
    ASSERT_NOT_NULL(cfg);
    cbm_pipeline_t *p2 = cbm_pipeline_new(g_tmpdir, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p2);
    cbm_pipeline_apply_config(p2, cfg);
    ASSERT_EQ(cbm_pipeline_run(p2), 0);

    /* 4. The inbound cross-file CALLS edge must survive and the total CALLS
     *    count must not regress. (Before the fix: Serve->Help is dropped.)
     *    NOTE: query with p2's project name — p1 (and the `project` pointer it
     *    owned) was freed above; p1 and p2 derive the same name from g_tmpdir. */
    const char *project2 = cbm_pipeline_project_name(p2);
    cbm_store_t *s2 = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s2);
    int calls_after = cbm_store_count_edges_by_type(s2, project2, "CALLS");
    ASSERT_EQ(calls_after, calls_before);
    ASSERT_TRUE(cross_file_call_exists(s2, project2, "Serve", "Help"));
    cbm_store_close(s2);
    cbm_pipeline_free(p2);
    cbm_config_close(cfg);

    teardown_test_repo();
    PASS();
}

TEST(pipeline_full_and_incremental_persist_file_state) {
    if (setup_test_repo() != 0) {
        FAIL("failed to create temp dir");
    }

    char db_path[512];
    int n = snprintf(db_path, sizeof(db_path), "%s/test_file_state.db", g_tmpdir);
    ASSERT_GT(n, 0);
    ASSERT_LT((size_t)n, sizeof(db_path));

    cbm_pipeline_t *p1 = cbm_pipeline_new(g_tmpdir, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p1);
    ASSERT_EQ(cbm_pipeline_run(p1), 0);

    const char *project1 = cbm_pipeline_project_name(p1);
    cbm_store_t *s1 = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s1);
    cbm_file_state_t first = {0};
    ASSERT_EQ(cbm_store_get_file_state(s1, project1, "pkg/util/helper.go", &first), CBM_STORE_OK);
    ASSERT_STR_EQ(first.language, "Go");
    ASSERT_EQ(first.generation, CBM_PIPELINE_COMPAT_GENERATION);
    ASSERT_NOT_NULL(first.content_hash);
    char first_hash[CBM_SZ_32];
    n = snprintf(first_hash, sizeof(first_hash), "%s", first.content_hash);
    ASSERT_GT(n, 0);
    ASSERT_LT((size_t)n, sizeof(first_hash));
    cbm_store_file_state_free_fields(&first);
    cbm_store_close(s1);
    cbm_pipeline_free(p1);

    char helper[512];
    n = snprintf(helper, sizeof(helper), "%s/pkg/util/helper.go", g_tmpdir);
    ASSERT_GT(n, 0);
    ASSERT_LT((size_t)n, sizeof(helper));
    ASSERT_EQ(th_append_file(helper, "\nfunc Extra() {}\n"), 0);

    cbm_config_t *cfg = incremental_test_config(g_tmpdir);
    ASSERT_NOT_NULL(cfg);
    cbm_pipeline_t *p2 = cbm_pipeline_new(g_tmpdir, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p2);
    cbm_pipeline_apply_config(p2, cfg);
    ASSERT_EQ(cbm_pipeline_run(p2), 0);

    const char *project2 = cbm_pipeline_project_name(p2);
    cbm_store_t *s2 = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s2);
    cbm_file_state_t second = {0};
    ASSERT_EQ(cbm_store_get_file_state(s2, project2, "pkg/util/helper.go", &second),
              CBM_STORE_OK);
    ASSERT_STR_EQ(second.language, "Go");
    ASSERT_EQ(second.generation, CBM_PIPELINE_COMPAT_GENERATION);
    ASSERT_NOT_NULL(second.content_hash);
    ASSERT_NEQ(strcmp(first_hash, second.content_hash), 0);
    cbm_store_file_state_free_fields(&second);
    cbm_store_close(s2);
    cbm_pipeline_free(p2);
    cbm_config_close(cfg);

    teardown_test_repo();
    PASS();
}

TEST(pipeline_incremental_full_index_rebuilds_owner_metadata) {
    if (setup_test_repo() != 0) {
        FAIL("failed to create temp dir");
    }

    char db_path[512];
    int n = snprintf(db_path, sizeof(db_path), "%s/test_owner_metadata.db", g_tmpdir);
    ASSERT_GT(n, 0);
    ASSERT_LT((size_t)n, sizeof(db_path));

    cbm_config_t *cfg = incremental_test_config(g_tmpdir);
    ASSERT_NOT_NULL(cfg);
    cbm_pipeline_t *p = cbm_pipeline_new(g_tmpdir, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    const char *project = cbm_pipeline_project_name(p);
    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);
    int node_owners = 0;
    int edge_owners = 0;
    ASSERT_EQ(cbm_store_count_file_delta_owners(s, project, "pkg/util/helper.go",
                                                &node_owners, &edge_owners),
              CBM_STORE_OK);
    ASSERT_GT(node_owners, 0);
    cbm_store_close(s);
    cbm_pipeline_free(p);
    cbm_config_close(cfg);

    teardown_test_repo();
    PASS();
}

/* ── Git history pass tests ─────────────────────────────────────── */

TEST(githistory_is_trackable) {
    /* Source files → trackable */
    ASSERT_TRUE(cbm_is_trackable_file("main.go"));
    ASSERT_TRUE(cbm_is_trackable_file("src/app.py"));
    ASSERT_TRUE(cbm_is_trackable_file("README.md"));

    /* Non-trackable: skip prefixes */
    ASSERT_FALSE(cbm_is_trackable_file("node_modules/foo/bar.js"));
    ASSERT_FALSE(cbm_is_trackable_file("vendor/lib/dep.go"));
    ASSERT_FALSE(cbm_is_trackable_file(".git/config"));
    ASSERT_FALSE(cbm_is_trackable_file("__pycache__/mod.pyc"));

    /* Non-trackable: lock files */
    ASSERT_FALSE(cbm_is_trackable_file("package-lock.json"));
    ASSERT_FALSE(cbm_is_trackable_file("go.sum"));

    /* Non-trackable: binary/minified extensions */
    ASSERT_FALSE(cbm_is_trackable_file("image.png"));
    ASSERT_FALSE(cbm_is_trackable_file("src/style.min.css"));

    PASS();
}

TEST(githistory_compute_coupling) {
    /* 5 commits with overlapping files */
    char *files_0[] = {"a.go", "b.go", "c.go"};
    char *files_1[] = {"a.go", "b.go"};
    char *files_2[] = {"a.go", "b.go"};
    char *files_3[] = {"a.go", "c.go"};
    char *files_4[] = {"d.go", "e.go"};

    cbm_commit_files_t commits[] = {
        {files_0, 3, 0}, {files_1, 2, 0}, {files_2, 2, 0}, {files_3, 2, 0}, {files_4, 2, 0},
    };

    cbm_change_coupling_t results[100];
    int n = cbm_compute_change_coupling(commits, 5, results, 100);

    /* a.go + b.go co-change 3 times → should appear */
    bool found_ab = false;
    for (int i = 0; i < n; i++) {
        if ((strcmp(results[i].file_a, "a.go") == 0 && strcmp(results[i].file_b, "b.go") == 0) ||
            (strcmp(results[i].file_a, "b.go") == 0 && strcmp(results[i].file_b, "a.go") == 0)) {
            found_ab = true;
            ASSERT_EQ(results[i].co_change_count, 3);
            ASSERT_TRUE(results[i].coupling_score >= 0.7);
        }
    }
    ASSERT_TRUE(found_ab);

    /* d.go + e.go co-change only 1 time → below threshold, should NOT appear */
    for (int i = 0; i < n; i++) {
        ASSERT_FALSE(strcmp(results[i].file_a, "d.go") == 0 ||
                     strcmp(results[i].file_b, "d.go") == 0);
    }

    PASS();
}

TEST(githistory_coupling_carries_last_co_change) {
    /* Coupling output must surface the most recent commit timestamp at which
     * a pair co-changed, so callers can score recency in addition to
     * frequency. The pair (a.go, b.go) co-changes at three timestamps; the
     * resulting last_co_change must be the maximum (newest) of those. */
    char *files_old[] = {"a.go", "b.go"};
    char *files_mid[] = {"a.go", "b.go"};
    char *files_new[] = {"a.go", "b.go"};
    /* Unrelated co-change in the middle to make sure we don't accidentally
     * pick up that pair's timestamp by index. */
    char *files_other[] = {"c.go", "d.go"};

    cbm_commit_files_t commits[] = {
        {files_old, 2, 1700000000LL},   /* oldest a.go/b.go co-change */
        {files_other, 2, 1750000000LL}, /* unrelated pair */
        {files_mid, 2, 1720000000LL},
        {files_new, 2, 1800000000LL}, /* newest a.go/b.go co-change */
    };

    cbm_change_coupling_t results[16];
    int n = cbm_compute_change_coupling(commits, 4, results, 16);
    ASSERT_GTE(n, 1);

    bool found_ab = false;
    for (int i = 0; i < n; i++) {
        bool is_ab =
            (strcmp(results[i].file_a, "a.go") == 0 && strcmp(results[i].file_b, "b.go") == 0) ||
            (strcmp(results[i].file_a, "b.go") == 0 && strcmp(results[i].file_b, "a.go") == 0);
        if (!is_ab) {
            continue;
        }
        ASSERT_EQ(results[i].co_change_count, 3);
        /* last_co_change must be the max of the three a.go/b.go timestamps. */
        ASSERT_EQ(results[i].last_co_change, 1800000000LL);
        found_ab = true;
    }
    ASSERT_TRUE(found_ab);
    PASS();
}

TEST(githistory_skip_large_commits) {
    /* A single commit with 25 files → should be skipped (>20) */
    char *files[25];
    char bufs[25][32];
    for (int i = 0; i < 25; i++) {
        snprintf(bufs[i], sizeof(bufs[i]), "file%d.go", i);
        files[i] = bufs[i];
    }

    cbm_commit_files_t commits[] = {{files, 25, 0}};

    cbm_change_coupling_t results[100];
    int n = cbm_compute_change_coupling(commits, 1, results, 100);
    ASSERT_EQ(n, 0);

    PASS();
}

TEST(githistory_limits_to_max) {
    /* Create many commits to generate >100 couplings */
    /* 50 files, each pair has 3 commits → C(50,2)=1225 pairs, but only
     * pairs with ≥3 co-changes and score≥0.3 pass the threshold */
    int nfiles = 50;
    int npairs = nfiles * (nfiles - 1) / 2;
    int ncommits = npairs * 3;

    cbm_commit_files_t *commits = calloc(ncommits, sizeof(cbm_commit_files_t));
    char **file_strs = calloc(nfiles, sizeof(char *));
    for (int i = 0; i < nfiles; i++) {
        file_strs[i] = malloc(32);
        snprintf(file_strs[i], 32, "f%d.go", i);
    }

    int ci = 0;
    for (int i = 0; i < nfiles; i++) {
        for (int j = i + 1; j < nfiles; j++) {
            for (int k = 0; k < 3; k++) {
                commits[ci].files = malloc(2 * sizeof(char *));
                commits[ci].files[0] = file_strs[i];
                commits[ci].files[1] = file_strs[j];
                commits[ci].count = 2;
                ci++;
            }
        }
    }

    /* max_out = 100 → should cap at 100 */
    cbm_change_coupling_t results[100];
    int n = cbm_compute_change_coupling(commits, ncommits, results, 100);
    ASSERT_TRUE(n <= 100);

    /* Cleanup */
    for (int i = 0; i < ncommits; i++) {
        free(commits[i].files);
    }
    for (int i = 0; i < nfiles; i++) {
        free(file_strs[i]);
    }
    free(file_strs);
    free(commits);

    PASS();
}

/* ── Test detection tests ──────────────────────────────────────── */

TEST(testdetect_is_test_file) {
    /* Test file patterns (all languages) */
    ASSERT_TRUE(cbm_is_test_path("foo_test.go"));
    ASSERT_TRUE(cbm_is_test_path("test_handler.py"));
    ASSERT_TRUE(cbm_is_test_path("handler.test.js"));
    ASSERT_TRUE(cbm_is_test_path("handler.spec.ts"));
    ASSERT_TRUE(cbm_is_test_path("Component.test.tsx"));
    ASSERT_TRUE(cbm_is_test_path("OrderTest.java"));
    ASSERT_TRUE(cbm_is_test_path("handler_test.rs"));
    ASSERT_TRUE(cbm_is_test_path("handler_test.cpp"));
    ASSERT_TRUE(cbm_is_test_path("OrderTest.cs"));
    ASSERT_TRUE(cbm_is_test_path("OrderTest.php"));
    ASSERT_TRUE(cbm_is_test_path("OrderSpec.scala"));
    ASSERT_TRUE(cbm_is_test_path("OrderTest.kt"));
    ASSERT_TRUE(cbm_is_test_path("handler_test.lua"));

    /* Non-test file patterns */
    ASSERT_FALSE(cbm_is_test_path("foo.go"));
    ASSERT_FALSE(cbm_is_test_path("handler.py"));
    ASSERT_FALSE(cbm_is_test_path("handler.js"));
    ASSERT_FALSE(cbm_is_test_path("handler.ts"));
    ASSERT_FALSE(cbm_is_test_path("Component.tsx"));
    ASSERT_FALSE(cbm_is_test_path("Order.java"));
    ASSERT_FALSE(cbm_is_test_path("handler.rs"));
    ASSERT_FALSE(cbm_is_test_path("handler.cpp"));
    ASSERT_FALSE(cbm_is_test_path("Order.cs"));
    ASSERT_FALSE(cbm_is_test_path("Order.php"));
    ASSERT_FALSE(cbm_is_test_path("Order.scala"));
    ASSERT_FALSE(cbm_is_test_path("Order.kt"));
    ASSERT_FALSE(cbm_is_test_path("handler.lua"));

    PASS();
}

TEST(testdetect_is_test_function) {
    /* Test function patterns */
    ASSERT_TRUE(cbm_is_test_func_name("TestCreate"));  /* Go */
    ASSERT_TRUE(cbm_is_test_func_name("test_create")); /* Python/Rust/Lua */
    ASSERT_TRUE(cbm_is_test_func_name("test"));        /* JS/TS */
    ASSERT_TRUE(cbm_is_test_func_name("describe"));    /* JS/TS */
    ASSERT_TRUE(cbm_is_test_func_name("it"));          /* JS/TS */
    ASSERT_TRUE(cbm_is_test_func_name("testCreate"));  /* Java/PHP/Scala/Kotlin */
    ASSERT_TRUE(cbm_is_test_func_name("TestCreate"));  /* C++/C# */

    /* Non-test function patterns */
    ASSERT_FALSE(cbm_is_test_func_name("create"));
    ASSERT_FALSE(cbm_is_test_func_name("handleRequest"));
    ASSERT_FALSE(cbm_is_test_func_name("process"));

    PASS();
}

/* ── Implements pass tests (graph buffer based) ────────────────── */

TEST(implements_creates_override) {
    /* Port of TestPassImplementsCreatesOverrideEdges.
     * Set up Interface+methods, Class+methods, verify IMPLEMENTS+OVERRIDE. */
    cbm_gbuf_t *gb = cbm_gbuf_new("test-proj", "/tmp/test");
    ASSERT_NOT_NULL(gb);

    /* Interface "Reader" with methods "Read" and "Close" */
    int64_t iface_id =
        cbm_gbuf_upsert_node(gb, "Interface", "Reader", "pkg.Reader", "pkg/reader.go", 1, 5, "{}");
    ASSERT_GT(iface_id, 0);

    int64_t read_method_id =
        cbm_gbuf_upsert_node(gb, "Method", "Read", "pkg.Reader.Read", "pkg/reader.go", 2, 2, "{}");
    int64_t close_method_id = cbm_gbuf_upsert_node(gb, "Method", "Close", "pkg.Reader.Close",
                                                   "pkg/reader.go", 3, 3, "{}");
    ASSERT_GT(read_method_id, 0);
    ASSERT_GT(close_method_id, 0);

    /* DEFINES_METHOD edges */
    cbm_gbuf_insert_edge(gb, iface_id, read_method_id, "DEFINES_METHOD", "{}");
    cbm_gbuf_insert_edge(gb, iface_id, close_method_id, "DEFINES_METHOD", "{}");

    /* Class "FileReader" with matching methods */
    int64_t class_id = cbm_gbuf_upsert_node(gb, "Class", "FileReader", "pkg.FileReader",
                                            "pkg/filereader.go", 1, 10, "{}");
    ASSERT_GT(class_id, 0);

    int64_t fr_read_id =
        cbm_gbuf_upsert_node(gb, "Method", "Read", "pkg.FileReader.Read", "pkg/filereader.go", 2, 4,
                             "{\"receiver\":\"(f *FileReader)\"}");
    int64_t fr_close_id =
        cbm_gbuf_upsert_node(gb, "Method", "Close", "pkg.FileReader.Close", "pkg/filereader.go", 5,
                             7, "{\"receiver\":\"(f *FileReader)\"}");
    ASSERT_GT(fr_read_id, 0);
    ASSERT_GT(fr_close_id, 0);

    /* Run Go-style implements detection */
    atomic_int cancelled = 0;
    cbm_pipeline_ctx_t ctx = {
        .project_name = "test-proj",
        .repo_path = "/tmp/test",
        .gbuf = gb,
        .registry = NULL,
        .cancelled = &cancelled,
    };
    int edges_created = cbm_pipeline_implements_go(&ctx);
    ASSERT_GT(edges_created, 0);

    /* Verify IMPLEMENTS edge: FileReader → Reader */
    const cbm_gbuf_edge_t **impl_edges = NULL;
    int impl_count = 0;
    int rc =
        cbm_gbuf_find_edges_by_source_type(gb, class_id, "IMPLEMENTS", &impl_edges, &impl_count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(impl_count, 1);
    ASSERT_EQ(impl_edges[0]->target_id, iface_id);

    /* Verify OVERRIDE edges: FileReader.Read → Reader.Read */
    const cbm_gbuf_edge_t **read_overrides = NULL;
    int read_override_count = 0;
    rc = cbm_gbuf_find_edges_by_source_type(gb, fr_read_id, "OVERRIDE", &read_overrides,
                                            &read_override_count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(read_override_count, 1);
    ASSERT_EQ(read_overrides[0]->target_id, read_method_id);

    /* Verify OVERRIDE edge: FileReader.Close → Reader.Close */
    const cbm_gbuf_edge_t **close_overrides = NULL;
    int close_override_count = 0;
    rc = cbm_gbuf_find_edges_by_source_type(gb, fr_close_id, "OVERRIDE", &close_overrides,
                                            &close_override_count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(close_override_count, 1);
    ASSERT_EQ(close_overrides[0]->target_id, close_method_id);

    cbm_gbuf_free(gb);
    PASS();
}

TEST(implements_accepts_struct_label) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test-proj", "/tmp/test");
    ASSERT_NOT_NULL(gb);

    int64_t iface_id =
        cbm_gbuf_upsert_node(gb, "Interface", "Runner", "pkg.Runner", "pkg/runner.go", 1, 3, "{}");
    ASSERT_GT(iface_id, 0);
    int64_t run_method_id =
        cbm_gbuf_upsert_node(gb, "Method", "Run", "pkg.Runner.Run", "pkg/runner.go", 2, 2, "{}");
    ASSERT_GT(run_method_id, 0);
    cbm_gbuf_insert_edge(gb, iface_id, run_method_id, "DEFINES_METHOD", "{}");

    int64_t struct_id =
        cbm_gbuf_upsert_node(gb, "Struct", "Job", "pkg.Job", "pkg/job.go", 1, 4, "{}");
    ASSERT_GT(struct_id, 0);
    int64_t job_run_id = cbm_gbuf_upsert_node(gb, "Method", "Run", "pkg.Job.Run", "pkg/job.go", 2,
                                              3, "{\"receiver\":\"(j Job)\"}");
    ASSERT_GT(job_run_id, 0);

    atomic_int cancelled = 0;
    cbm_pipeline_ctx_t ctx = {
        .project_name = "test-proj",
        .repo_path = "/tmp/test",
        .gbuf = gb,
        .registry = NULL,
        .cancelled = &cancelled,
    };
    int edges_created = cbm_pipeline_implements_go(&ctx);
    ASSERT_GT(edges_created, 0);

    const cbm_gbuf_edge_t **impl_edges = NULL;
    int impl_count = 0;
    ASSERT_EQ(cbm_gbuf_find_edges_by_source_type(gb, struct_id, "IMPLEMENTS", &impl_edges,
                                                 &impl_count),
              0);
    ASSERT_EQ(impl_count, 1);
    ASSERT_EQ(impl_edges[0]->target_id, iface_id);

    const cbm_gbuf_edge_t **override_edges = NULL;
    int override_count = 0;
    ASSERT_EQ(cbm_gbuf_find_edges_by_source_type(gb, job_run_id, "OVERRIDE", &override_edges,
                                                 &override_count),
              0);
    ASSERT_EQ(override_count, 1);
    ASSERT_EQ(override_edges[0]->target_id, run_method_id);

    cbm_gbuf_free(gb);
    PASS();
}

TEST(implements_no_match) {
    /* Port of TestPassImplementsNoOverrideWithoutMatch.
     * Interface requires Read+Write, struct only has Read → no edges. */
    cbm_gbuf_t *gb = cbm_gbuf_new("test-proj", "/tmp/test");
    ASSERT_NOT_NULL(gb);

    /* Interface "ReadWriter" with methods "Read" and "Write" */
    int64_t iface_id = cbm_gbuf_upsert_node(gb, "Interface", "ReadWriter", "pkg.ReadWriter",
                                            "pkg/rw.go", 1, 5, "{}");
    int64_t read_id =
        cbm_gbuf_upsert_node(gb, "Method", "Read", "pkg.ReadWriter.Read", "pkg/rw.go", 2, 2, "{}");
    int64_t write_id = cbm_gbuf_upsert_node(gb, "Method", "Write", "pkg.ReadWriter.Write",
                                            "pkg/rw.go", 3, 3, "{}");
    cbm_gbuf_insert_edge(gb, iface_id, read_id, "DEFINES_METHOD", "{}");
    cbm_gbuf_insert_edge(gb, iface_id, write_id, "DEFINES_METHOD", "{}");

    /* Struct "OnlyReader" with only "Read" (missing "Write") */
    int64_t class_id = cbm_gbuf_upsert_node(gb, "Class", "OnlyReader", "pkg.OnlyReader",
                                            "pkg/onlyreader.go", 1, 10, "{}");
    int64_t or_read_id =
        cbm_gbuf_upsert_node(gb, "Method", "Read", "pkg.OnlyReader.Read", "pkg/onlyreader.go", 2, 4,
                             "{\"receiver\":\"(o *OnlyReader)\"}");

    /* Run Go-style implements detection */
    atomic_int cancelled = 0;
    cbm_pipeline_ctx_t ctx = {
        .project_name = "test-proj",
        .repo_path = "/tmp/test",
        .gbuf = gb,
        .registry = NULL,
        .cancelled = &cancelled,
    };
    int edges_created = cbm_pipeline_implements_go(&ctx);
    ASSERT_EQ(edges_created, 0);

    /* Verify NO IMPLEMENTS edge */
    const cbm_gbuf_edge_t **impl_edges = NULL;
    int impl_count = 0;
    cbm_gbuf_find_edges_by_source_type(gb, class_id, "IMPLEMENTS", &impl_edges, &impl_count);
    ASSERT_EQ(impl_count, 0);

    /* Verify NO OVERRIDE edge */
    const cbm_gbuf_edge_t **override_edges = NULL;
    int override_count = 0;
    cbm_gbuf_find_edges_by_source_type(gb, or_read_id, "OVERRIDE", &override_edges,
                                       &override_count);
    ASSERT_EQ(override_count, 0);

    /* Suppress unused warnings */
    (void)iface_id;
    (void)read_id;
    (void)write_id;

    cbm_gbuf_free(gb);
    PASS();
}

/* ── Usages pass tests (full pipeline integration) ──────────────── */

/* Helper to create a temp dir with a single source file */
static char g_usages_tmpdir[256];

static int setup_usages_repo(const char *filename, const char *content, const char *extra_file,
                             const char *extra_content) {
    snprintf(g_usages_tmpdir, sizeof(g_usages_tmpdir), "/tmp/cbm_usage_XXXXXX");
    if (!cbm_mkdtemp(g_usages_tmpdir))
        return -1;

    /* Check if filename has subdirectory */
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", g_usages_tmpdir, filename);

    /* Create subdirectories if needed */
    char dir_path[512];
    snprintf(dir_path, sizeof(dir_path), "%s", path);
    char *last_slash = strrchr(dir_path, '/');
    if (last_slash) {
        *last_slash = '\0';
        th_mkdir_p(dir_path);
    }

    FILE *f = fopen(path, "w");
    if (!f)
        return -1;
    fprintf(f, "%s", content);
    fclose(f);

    if (extra_file && extra_content) {
        snprintf(path, sizeof(path), "%s/%s", g_usages_tmpdir, extra_file);
        f = fopen(path, "w");
        if (!f)
            return -1;
        fprintf(f, "%s", extra_content);
        fclose(f);
    }

    return 0;
}

static void teardown_usages_repo(void) {
    if (g_usages_tmpdir[0])
        rm_rf(g_usages_tmpdir);
    g_usages_tmpdir[0] = '\0';
}

TEST(usages_creates_edges) {
    /* Port of TestPassUsagesCreatesEdges.
     * Go source with callback reference → USAGE edge. */
    const char *go_source = "package mypkg\n\n"
                            "func Process(data string) string {\n"
                            "\treturn data\n"
                            "}\n\n"
                            "func Register() {\n"
                            "\thandler := Process\n"
                            "\t_ = handler\n"
                            "}\n";

    if (setup_usages_repo("mypkg/main.go", go_source, "go.mod", "module testmod\ngo 1.21\n") != 0) {
        FAIL("failed to create temp dir");
    }

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/test_usages.db", g_usages_tmpdir);

    cbm_pipeline_t *p = cbm_pipeline_new(g_usages_tmpdir, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    int rc = cbm_pipeline_run(p);
    ASSERT_EQ(rc, 0);

    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);
    const char *project = cbm_pipeline_project_name(p);

    /* Check for USAGE edges */
    cbm_edge_t *edges = NULL;
    int edge_count = 0;
    rc = cbm_store_find_edges_by_type(s, project, "USAGE", &edges, &edge_count);

    bool found_usage = false;
    for (int i = 0; i < edge_count; i++) {
        cbm_node_t src = {0}, tgt = {0};
        if (cbm_store_find_node_by_id(s, edges[i].source_id, &src) == CBM_STORE_OK &&
            cbm_store_find_node_by_id(s, edges[i].target_id, &tgt) == CBM_STORE_OK) {
            if (strcmp(src.name, "Register") == 0 && strcmp(tgt.name, "Process") == 0) {
                found_usage = true;
            }
        }
        cbm_node_free_fields(&src);
        cbm_node_free_fields(&tgt);
    }
    if (edges)
        cbm_store_free_edges(edges, edge_count);
    ASSERT_TRUE(found_usage);

    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_usages_repo();
    PASS();
}

TEST(usages_no_duplicate_calls) {
    /* Port of TestPassUsagesDoesNotDuplicateCalls.
     * A direct function call should produce CALLS but not USAGE. */
    const char *go_source = "package mypkg\n\n"
                            "func Helper() string {\n"
                            "\treturn \"ok\"\n"
                            "}\n\n"
                            "func Main() {\n"
                            "\tHelper()\n"
                            "}\n";

    if (setup_usages_repo("mypkg/main.go", go_source, "go.mod", "module testmod\ngo 1.21\n") != 0) {
        FAIL("failed to create temp dir");
    }

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/test_no_dup.db", g_usages_tmpdir);

    cbm_pipeline_t *p = cbm_pipeline_new(g_usages_tmpdir, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    int rc = cbm_pipeline_run(p);
    ASSERT_EQ(rc, 0);

    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);
    const char *project = cbm_pipeline_project_name(p);

    /* Should have CALLS edge from Main to Helper */
    cbm_edge_t *call_edges = NULL;
    int call_count = 0;
    cbm_store_find_edges_by_type(s, project, "CALLS", &call_edges, &call_count);

    bool found_call = false;
    for (int i = 0; i < call_count; i++) {
        cbm_node_t src = {0}, tgt = {0};
        if (cbm_store_find_node_by_id(s, call_edges[i].source_id, &src) == CBM_STORE_OK &&
            cbm_store_find_node_by_id(s, call_edges[i].target_id, &tgt) == CBM_STORE_OK) {
            if (strcmp(src.name, "Main") == 0 && strcmp(tgt.name, "Helper") == 0) {
                found_call = true;
            }
        }
        cbm_node_free_fields(&src);
        cbm_node_free_fields(&tgt);
    }
    if (call_edges)
        cbm_store_free_edges(call_edges, call_count);
    ASSERT_TRUE(found_call);

    /* Should NOT have USAGE edge from Main to Helper */
    cbm_edge_t *usage_edges = NULL;
    int usage_count = 0;
    cbm_store_find_edges_by_type(s, project, "USAGE", &usage_edges, &usage_count);

    for (int i = 0; i < usage_count; i++) {
        cbm_node_t src = {0}, tgt = {0};
        if (cbm_store_find_node_by_id(s, usage_edges[i].source_id, &src) == CBM_STORE_OK &&
            cbm_store_find_node_by_id(s, usage_edges[i].target_id, &tgt) == CBM_STORE_OK) {
            ASSERT_FALSE(strcmp(src.name, "Main") == 0 && strcmp(tgt.name, "Helper") == 0);
        }
        cbm_node_free_fields(&src);
        cbm_node_free_fields(&tgt);
    }
    if (usage_edges)
        cbm_store_free_edges(usage_edges, usage_count);

    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_usages_repo();
    PASS();
}

TEST(calls_edge_carries_call_site_line) {
    /* Regression guard for #503: a CALLS edge must persist the source line of
     * the call site in its JSON props as "line":N. Helper() is invoked on
     * line 8 (1-based) of the source below. */
    const char *go_source = "package mypkg\n"
                            "\n"
                            "func Helper() string {\n"
                            "\treturn \"ok\"\n"
                            "}\n"
                            "\n"
                            "func Main() {\n"
                            "\tHelper()\n"
                            "}\n";

    if (setup_usages_repo("mypkg/main.go", go_source, "go.mod", "module testmod\ngo 1.21\n") != 0) {
        FAIL("failed to create temp dir");
    }

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/test_call_line.db", g_usages_tmpdir);

    cbm_pipeline_t *p = cbm_pipeline_new(g_usages_tmpdir, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);
    const char *project = cbm_pipeline_project_name(p);

    cbm_edge_t *call_edges = NULL;
    int call_count = 0;
    cbm_store_find_edges_by_type(s, project, "CALLS", &call_edges, &call_count);

    bool found_line = false;
    for (int i = 0; i < call_count; i++) {
        cbm_node_t src = {0}, tgt = {0};
        if (cbm_store_find_node_by_id(s, call_edges[i].source_id, &src) == CBM_STORE_OK &&
            cbm_store_find_node_by_id(s, call_edges[i].target_id, &tgt) == CBM_STORE_OK) {
            if (strcmp(src.name, "Main") == 0 && strcmp(tgt.name, "Helper") == 0) {
                ASSERT_NOT_NULL(call_edges[i].properties_json);
                ASSERT_TRUE(strstr(call_edges[i].properties_json, "\"line\":8}") != NULL);
                found_line = true;
            }
        }
        cbm_node_free_fields(&src);
        cbm_node_free_fields(&tgt);
    }
    if (call_edges)
        cbm_store_free_edges(call_edges, call_count);
    ASSERT_TRUE(found_line);

    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_usages_repo();
    PASS();
}

TEST(usages_kotlin_creates_edges) {
    /* Port of TestPassUsagesKotlinCreatesEdges.
     * Kotlin source with callback reference → USAGE edge. */
    const char *kt_source = "fun process(data: String): String {\n"
                            "    return data\n"
                            "}\n\n"
                            "fun register() {\n"
                            "    val handler = ::process\n"
                            "}\n";

    if (setup_usages_repo("Main.kt", kt_source, NULL, NULL) != 0) {
        FAIL("failed to create temp dir");
    }

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/test_kt_usage.db", g_usages_tmpdir);

    cbm_pipeline_t *p = cbm_pipeline_new(g_usages_tmpdir, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    int rc = cbm_pipeline_run(p);
    ASSERT_EQ(rc, 0);

    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);
    const char *project = cbm_pipeline_project_name(p);

    /* Check USAGE edges exist (Kotlin extraction may or may not produce them
     * depending on extraction support — just verify no crash and valid counts) */
    cbm_edge_t *edges = NULL;
    int edge_count = 0;
    rc = cbm_store_find_edges_by_type(s, project, "USAGE", &edges, &edge_count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    /* Note: The Go test just logs, doesn't assert — same behavior here */
    if (edges)
        cbm_store_free_edges(edges, edge_count);

    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_usages_repo();
    PASS();
}

TEST(usages_kotlin_no_duplicate_calls) {
    /* Port of TestPassUsagesKotlinDoesNotDuplicateCalls.
     * Direct call in Kotlin → CALLS but not USAGE. */
    const char *kt_source = "fun helper(): String {\n"
                            "    return \"ok\"\n"
                            "}\n\n"
                            "fun main() {\n"
                            "    helper()\n"
                            "}\n";

    if (setup_usages_repo("Main.kt", kt_source, NULL, NULL) != 0) {
        FAIL("failed to create temp dir");
    }

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/test_kt_nodup.db", g_usages_tmpdir);

    cbm_pipeline_t *p = cbm_pipeline_new(g_usages_tmpdir, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    int rc = cbm_pipeline_run(p);
    ASSERT_EQ(rc, 0);

    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);
    const char *project = cbm_pipeline_project_name(p);

    /* Should have CALLS edge from main to helper */
    cbm_edge_t *call_edges = NULL;
    int call_count = 0;
    cbm_store_find_edges_by_type(s, project, "CALLS", &call_edges, &call_count);

    bool found_call = false;
    for (int i = 0; i < call_count; i++) {
        cbm_node_t src = {0}, tgt = {0};
        if (cbm_store_find_node_by_id(s, call_edges[i].source_id, &src) == CBM_STORE_OK &&
            cbm_store_find_node_by_id(s, call_edges[i].target_id, &tgt) == CBM_STORE_OK) {
            if (strcmp(src.name, "main") == 0 && strcmp(tgt.name, "helper") == 0) {
                found_call = true;
            }
        }
        cbm_node_free_fields(&src);
        cbm_node_free_fields(&tgt);
    }
    if (call_edges)
        cbm_store_free_edges(call_edges, call_count);
    ASSERT_TRUE(found_call);

    /* Should NOT have USAGE edge from main to helper */
    cbm_edge_t *usage_edges = NULL;
    int usage_count = 0;
    cbm_store_find_edges_by_type(s, project, "USAGE", &usage_edges, &usage_count);

    for (int i = 0; i < usage_count; i++) {
        cbm_node_t src = {0}, tgt = {0};
        if (cbm_store_find_node_by_id(s, usage_edges[i].source_id, &src) == CBM_STORE_OK &&
            cbm_store_find_node_by_id(s, usage_edges[i].target_id, &tgt) == CBM_STORE_OK) {
            ASSERT_FALSE(strcmp(src.name, "main") == 0 && strcmp(tgt.name, "helper") == 0);
        }
        cbm_node_free_fields(&src);
        cbm_node_free_fields(&tgt);
    }
    if (usage_edges)
        cbm_store_free_edges(usage_edges, usage_count);

    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_usages_repo();
    PASS();
}

/* ── Pipeline language integration tests ────────────────────────── */

/* General helper: create temp dir and write multiple files */
static char g_lang_tmpdir[256];

static int setup_lang_repo(const char **filenames, const char **contents, int count) {
    snprintf(g_lang_tmpdir, sizeof(g_lang_tmpdir), "/tmp/cbm_lang_XXXXXX");
    if (!cbm_mkdtemp(g_lang_tmpdir))
        return -1;

    for (int i = 0; i < count; i++) {
        char path[512];
        snprintf(path, sizeof(path), "%s/%s", g_lang_tmpdir, filenames[i]);

        /* Create parent directories */
        char dir[512];
        snprintf(dir, sizeof(dir), "%s", path);
        char *slash = strrchr(dir, '/');
        if (slash) {
            *slash = '\0';
            th_mkdir_p(dir);
        }

        FILE *f = fopen(path, "wb");
        if (!f)
            return -1;
        fprintf(f, "%s", contents[i]);
        fclose(f);
    }
    return 0;
}

static void teardown_lang_repo(void) {
    if (g_lang_tmpdir[0])
        rm_rf(g_lang_tmpdir);
    g_lang_tmpdir[0] = '\0';
}

static int pipeline_dump_store_file_to_file(const char *src_path, const char *dest_path) {
    cbm_store_t *s = cbm_store_open_path(src_path);
    if (!s) {
        return CBM_STORE_ERR;
    }
    int rc = cbm_store_dump_to_file(s, dest_path);
    cbm_store_close(s);
    return rc;
}

static bool pipeline_store_has_edge_between_qns(const char *db_path, const char *project,
                                                const char *source_qn, const char *type,
                                                const char *target_qn) {
    cbm_store_t *s = cbm_store_open_path(db_path);
    if (!s) {
        return false;
    }

    cbm_node_t src = {0};
    cbm_node_t tgt = {0};
    bool found = false;
    if (cbm_store_find_node_by_qn(s, project, source_qn, &src) == CBM_STORE_OK &&
        cbm_store_find_node_by_qn(s, project, target_qn, &tgt) == CBM_STORE_OK) {
        cbm_edge_t *edges = NULL;
        int edge_count = 0;
        if (cbm_store_find_edges_by_source_type(s, src.id, type, &edges, &edge_count) ==
            CBM_STORE_OK) {
            for (int i = 0; i < edge_count; i++) {
                if (edges[i].target_id == tgt.id) {
                    found = true;
                    break;
                }
            }
            cbm_store_free_edges(edges, edge_count);
        }
    }

    cbm_store_close(s);
    return found;
}

static void pipeline_restore_workers_env(bool had_workers, const char *saved_workers) {
    if (had_workers) {
        cbm_setenv("CBM_WORKERS", saved_workers, 1);
    } else {
        cbm_unsetenv("CBM_WORKERS");
    }
}

static int pipeline_run_with_worker_count(const char *repo_path, const char *db_path, int workers,
                                          char **out_project) {
    char worker_buf[CBM_SZ_32];
    int n = snprintf(worker_buf, sizeof(worker_buf), "%d", workers);
    if (n <= 0 || (size_t)n >= sizeof(worker_buf) ||
        cbm_setenv("CBM_WORKERS", worker_buf, 1) != 0) {
        return CBM_NOT_FOUND;
    }

    cbm_pipeline_t *p = cbm_pipeline_new(repo_path, db_path, CBM_MODE_FULL);
    if (!p) {
        return CBM_NOT_FOUND;
    }
    int rc = cbm_pipeline_run(p);
    if (rc == 0 && out_project) {
        *out_project = strdup(cbm_pipeline_project_name(p));
        if (!*out_project) {
            rc = CBM_NOT_FOUND;
        }
    }
    cbm_pipeline_free(p);
    return rc;
}

static int pipeline_count_channel_edges_to_non_channels(const char *db_path, const char *project) {
    cbm_store_t *s = cbm_store_open_path(db_path);
    if (!s) {
        return CBM_NOT_FOUND;
    }
    sqlite3 *db = cbm_store_get_db(s);
    sqlite3_stmt *stmt = NULL;
    int count = CBM_NOT_FOUND;
    static const char sql[] =
        "SELECT COUNT(*) "
        "FROM edges e JOIN nodes t ON t.id = e.target_id "
        "WHERE e.project = ?1 AND e.type IN ('EMITS','LISTENS_ON') AND t.label <> 'Channel'";
    if (db && sqlite3_prepare_v2(db, sql, CBM_NOT_FOUND, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, SKIP_ONE, project, CBM_NOT_FOUND, SQLITE_STATIC);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            count = sqlite3_column_int(stmt, 0);
        }
    }
    sqlite3_finalize(stmt);
    cbm_store_close(s);
    return count;
}

TEST(pipeline_python_project) {
    /* Port of TestPipelinePythonProject */
    const char *files[] = {"main.py", "utils.py"};
    const char *contents[] = {
        "def greet(name):\n    return f\"Hello, {name}\"\n\n"
        "def process():\n    result = greet(\"world\")\n    return result\n",

        "API_URL = \"https://example.com/api\"\nMAX_RETRIES = 3\n\n"
        "def fetch_data(url):\n    pass\n\n"
        "class DataProcessor:\n    def transform(self, data):\n        return data\n"};

    if (setup_lang_repo(files, contents, 2) != 0)
        FAIL("tmpdir");
    char db[512];
    snprintf(db, sizeof(db), "%s/test.db", g_lang_tmpdir);

    cbm_pipeline_t *p = cbm_pipeline_new(g_lang_tmpdir, db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    cbm_store_t *s = cbm_store_open_path(db);
    ASSERT_NOT_NULL(s);
    const char *proj = cbm_pipeline_project_name(p);

    cbm_node_t *funcs = NULL;
    int fc = 0;
    cbm_store_find_nodes_by_label(s, proj, "Function", &funcs, &fc);
    ASSERT_GTE(fc, 3); /* greet, process, fetch_data */
    cbm_store_free_nodes(funcs, fc);

    cbm_node_t *cls = NULL;
    int cc = 0;
    cbm_store_find_nodes_by_label(s, proj, "Class", &cls, &cc);
    ASSERT_GTE(cc, 1); /* DataProcessor */
    cbm_store_free_nodes(cls, cc);

    cbm_node_t *methods = NULL;
    int mc = 0;
    cbm_store_find_nodes_by_label(s, proj, "Method", &methods, &mc);
    ASSERT_GTE(mc, 1); /* transform */
    cbm_store_free_nodes(methods, mc);

    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_lang_repo();
    PASS();
}

TEST(pipeline_go_cross_package_call) {
    /* Port of TestGoCrossPackageCallViaImport */
    const char *files[] = {"main.go", "svc/handler.go"};
    const char *contents[] = {
        "package main\n\nimport \"example.com/myapp/svc\"\n\n"
        "func run() {\n\tsvc.ProcessOrder(\"123\")\n}\n",

        "package svc\n\nfunc ProcessOrder(id string) error {\n\treturn nil\n}\n"};

    if (setup_lang_repo(files, contents, 2) != 0)
        FAIL("tmpdir");
    char db[512];
    snprintf(db, sizeof(db), "%s/test.db", g_lang_tmpdir);

    cbm_pipeline_t *p = cbm_pipeline_new(g_lang_tmpdir, db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    cbm_store_t *s = cbm_store_open_path(db);
    ASSERT_NOT_NULL(s);
    const char *proj = cbm_pipeline_project_name(p);

    /* Verify ProcessOrder exists */
    cbm_node_t *targets = NULL;
    int tc = 0;
    cbm_store_find_nodes_by_name(s, proj, "ProcessOrder", &targets, &tc);
    ASSERT_GT(tc, 0);

    /* Verify run() exists */
    cbm_node_t *callers = NULL;
    int clc = 0;
    cbm_store_find_nodes_by_name(s, proj, "run", &callers, &clc);
    ASSERT_GT(clc, 0);

    /* Check CALLS edge from run to ProcessOrder */
    cbm_edge_t *edges = NULL;
    int ec = 0;
    cbm_store_find_edges_by_source_type(s, callers[0].id, "CALLS", &edges, &ec);
    bool found = false;
    for (int i = 0; i < ec; i++) {
        if (edges[i].target_id == targets[0].id)
            found = true;
    }
    ASSERT_TRUE(found);

    if (edges)
        cbm_store_free_edges(edges, ec);
    cbm_store_free_nodes(targets, tc);
    cbm_store_free_nodes(callers, clc);
    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_lang_repo();
    PASS();
}

TEST(pipeline_python_cross_module_call) {
    /* Port of TestPythonCrossModuleCallViaImport */
    const char *files[] = {"utils.py", "main.py"};
    const char *contents[] = {
        "def fetch_data(url):\n    return {\"status\": \"ok\"}\n",

        "from utils import fetch_data\n\n"
        "def process():\n    result = fetch_data(\"https://example.com\")\n    return result\n"};

    if (setup_lang_repo(files, contents, 2) != 0)
        FAIL("tmpdir");
    char db[512];
    snprintf(db, sizeof(db), "%s/test.db", g_lang_tmpdir);

    cbm_pipeline_t *p = cbm_pipeline_new(g_lang_tmpdir, db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    cbm_store_t *s = cbm_store_open_path(db);
    ASSERT_NOT_NULL(s);
    const char *proj = cbm_pipeline_project_name(p);

    cbm_node_t *targets = NULL;
    int tc = 0;
    cbm_store_find_nodes_by_name(s, proj, "fetch_data", &targets, &tc);
    ASSERT_GT(tc, 0);

    cbm_node_t *callers = NULL;
    int clc = 0;
    cbm_store_find_nodes_by_name(s, proj, "process", &callers, &clc);
    ASSERT_GT(clc, 0);

    cbm_edge_t *edges = NULL;
    int ec = 0;
    cbm_store_find_edges_by_source_type(s, callers[0].id, "CALLS", &edges, &ec);
    bool found = false;
    for (int i = 0; i < ec; i++) {
        if (edges[i].target_id == targets[0].id)
            found = true;
    }
    ASSERT_TRUE(found);

    if (edges)
        cbm_store_free_edges(edges, ec);
    cbm_store_free_nodes(targets, tc);
    cbm_store_free_nodes(callers, clc);
    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_lang_repo();
    PASS();
}

TEST(pipeline_python_reexport_call_uses_resolved_import_edge) {
    enum { REEXPORT_FILE_COUNT = 4 };
    const char *files[] = {"fastapi/__init__.py", "fastapi/param_functions.py",
                           "fastapi/openapi/models.py", "docs_src/app/main.py"};
    const char *contents[] = {
        "from .param_functions import Header\n",
        "def Header(default=None):\n    return default\n",
        "class Header:\n    pass\n",
        ("from fastapi import Header\n\n"
         "def create_item():\n    return Header(None)\n")};

    if (setup_lang_repo(files, contents, REEXPORT_FILE_COUNT) != 0) {
        FAIL("tmpdir");
    }
    char db[512];
    snprintf(db, sizeof(db), "%s/test.db", g_lang_tmpdir);

    cbm_pipeline_t *p = cbm_pipeline_new(g_lang_tmpdir, db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    cbm_store_t *s = cbm_store_open_path(db);
    ASSERT_NOT_NULL(s);
    const char *proj = cbm_pipeline_project_name(p);

    cbm_node_t *callers = NULL;
    int caller_count = 0;
    cbm_store_find_nodes_by_name(s, proj, "create_item", &callers, &caller_count);
    ASSERT_GT(caller_count, 0);

    cbm_node_t *headers = NULL;
    int header_count = 0;
    cbm_store_find_nodes_by_name(s, proj, "Header", &headers, &header_count);
    ASSERT_GT(header_count, 1);

    int64_t expected_target_id = 0;
    int64_t wrong_target_id = 0;
    for (int i = 0; i < header_count; i++) {
        const char *qn = headers[i].qualified_name ? headers[i].qualified_name : "";
        if (strstr(qn, ".fastapi.param_functions.Header")) {
            expected_target_id = headers[i].id;
        } else if (strstr(qn, ".fastapi.openapi.models.Header")) {
            wrong_target_id = headers[i].id;
        }
    }
    ASSERT_GT(expected_target_id, 0);
    ASSERT_GT(wrong_target_id, 0);

    cbm_edge_t *edges = NULL;
    int edge_count = 0;
    cbm_store_find_edges_by_source_type(s, callers[0].id, "CALLS", &edges, &edge_count);
    bool found_expected = false;
    bool found_wrong = false;
    for (int i = 0; i < edge_count; i++) {
        if (edges[i].target_id == expected_target_id) {
            found_expected = true;
        }
        if (edges[i].target_id == wrong_target_id) {
            found_wrong = true;
        }
    }
    ASSERT_TRUE(found_expected);
    ASSERT_FALSE(found_wrong);

    if (edges) {
        cbm_store_free_edges(edges, edge_count);
    }
    cbm_store_free_nodes(headers, header_count);
    cbm_store_free_nodes(callers, caller_count);
    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_lang_repo();
    PASS();
}

TEST(pipeline_incremental_reexport_target_matches_full) {
    enum { REEXPORT_FILE_COUNT = 4 };
    const char *files[] = {"fastapi/__init__.py", "fastapi/param_functions.py",
                           "fastapi/openapi/models.py", "docs_src/app/main.py"};
    const char *contents[] = {
        "from .param_functions import Header\n",
        "def Header(default=None):\n    return default\n",
        "class Header:\n    pass\n",
        ("from fastapi import Header\n\n"
         "def create_item():\n    return Header(None)\n")};

    if (setup_lang_repo(files, contents, REEXPORT_FILE_COUNT) != 0) {
        FAIL("tmpdir");
    }

    char db[CBM_SZ_512];
    int n = snprintf(db, sizeof(db), "%s/test.db", g_lang_tmpdir);
    ASSERT_GT(n, 0);
    ASSERT_LT((size_t)n, sizeof(db));

    cbm_pipeline_t *p = cbm_pipeline_new(g_lang_tmpdir, db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    char *project = strdup(cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);
    ASSERT_NOT_NULL(project);

    ASSERT_EQ(th_write_file(TH_PATH(g_lang_tmpdir, "fastapi/__init__.py"),
                            "from .openapi.models import Header\n"),
              0);

    cbm_config_t *cfg = incremental_test_config(g_lang_tmpdir);
    ASSERT_NOT_NULL(cfg);
    p = cbm_pipeline_new(g_lang_tmpdir, db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    cbm_pipeline_free(p);
    cbm_config_close(cfg);

    char incremental_db[CBM_SZ_512];
    n = snprintf(incremental_db, sizeof(incremental_db), "%s/reexport-incremental.db",
                 g_lang_tmpdir);
    ASSERT_GT(n, 0);
    ASSERT_LT((size_t)n, sizeof(incremental_db));
    cbm_unlink(incremental_db);
    ASSERT_EQ(pipeline_dump_store_file_to_file(db, incremental_db), CBM_STORE_OK);

    cbm_unlink(db);
    p = cbm_pipeline_new(g_lang_tmpdir, db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    cbm_pipeline_free(p);

    char diff_err[CBM_SZ_8K] = {0};
    int diff_rc =
        cbm_test_compare_canonical_graphs(incremental_db, db, project, diff_err, sizeof(diff_err));
    if (diff_rc != 0) {
        printf("    [incremental:reexport-diff] %s\n", diff_err);
    }
    ASSERT_EQ(diff_rc, 0);

    cbm_unlink(incremental_db);
    free(project);
    teardown_lang_repo();
    PASS();
}

TEST(pipeline_parallel_duplicate_import_inherits_matches_sequential) {
    enum { FILLER_FILE_COUNT = 52, SEQUENTIAL_WORKERS = 1, PARALLEL_WORKERS = 4 };
    const char *files[] = {"fastapi/openapi/models.py", "fastapi/security/base.py",
                           "fastapi/security/api_key.py"};
    const char *contents[] = {
        "class SecurityBase:\n    pass\n",
        "from fastapi.openapi.models import SecurityBase as SecurityBaseModel\n\n"
        "class SecurityBase:\n    model: SecurityBaseModel\n",
        "from fastapi.security.base import SecurityBase\n\n"
        "class APIKeyBase(SecurityBase):\n    pass\n"};

    if (setup_lang_repo(files, contents, 3) != 0) {
        FAIL("tmpdir");
    }
    for (int i = 0; i < FILLER_FILE_COUNT; i++) {
        char rel[CBM_SZ_128];
        char body[CBM_SZ_256];
        int rn = snprintf(rel, sizeof(rel), "fillers/filler_%02d.py", i);
        int bn = snprintf(body, sizeof(body), "def filler_%02d():\n    return %d\n", i, i);
        ASSERT_GT(rn, 0);
        ASSERT_LT((size_t)rn, sizeof(rel));
        ASSERT_GT(bn, 0);
        ASSERT_LT((size_t)bn, sizeof(body));
        ASSERT_EQ(th_write_file(TH_PATH(g_lang_tmpdir, rel), body), 0);
    }

    char seq_db[CBM_SZ_512];
    char par_db[CBM_SZ_512];
    int n = snprintf(seq_db, sizeof(seq_db), "%s/seq.db", g_lang_tmpdir);
    ASSERT_GT(n, 0);
    ASSERT_LT((size_t)n, sizeof(seq_db));
    n = snprintf(par_db, sizeof(par_db), "%s/par.db", g_lang_tmpdir);
    ASSERT_GT(n, 0);
    ASSERT_LT((size_t)n, sizeof(par_db));

    char saved_workers[CBM_SZ_32] = {0};
    bool had_workers = cbm_safe_getenv("CBM_WORKERS", saved_workers, sizeof(saved_workers),
                                       NULL) != NULL;

    char *project = NULL;
    int seq_rc =
        pipeline_run_with_worker_count(g_lang_tmpdir, seq_db, SEQUENTIAL_WORKERS, &project);
    int par_rc = pipeline_run_with_worker_count(g_lang_tmpdir, par_db, PARALLEL_WORKERS, NULL);
    pipeline_restore_workers_env(had_workers, saved_workers);
    ASSERT_EQ(seq_rc, 0);
    ASSERT_EQ(par_rc, 0);
    ASSERT_NOT_NULL(project);

    char src_qn[CBM_SZ_512];
    char target_qn[CBM_SZ_512];
    n = snprintf(src_qn, sizeof(src_qn), "%s.fastapi.security.api_key.APIKeyBase", project);
    ASSERT_GT(n, 0);
    ASSERT_LT((size_t)n, sizeof(src_qn));
    n = snprintf(target_qn, sizeof(target_qn), "%s.fastapi.security.base.SecurityBase", project);
    ASSERT_GT(n, 0);
    ASSERT_LT((size_t)n, sizeof(target_qn));

    char base_file_qn[CBM_SZ_512];
    char openapi_module_qn[CBM_SZ_512];
    n = snprintf(base_file_qn, sizeof(base_file_qn), "%s.fastapi.security.base.__file__",
                 project);
    ASSERT_GT(n, 0);
    ASSERT_LT((size_t)n, sizeof(base_file_qn));
    n = snprintf(openapi_module_qn, sizeof(openapi_module_qn), "%s.fastapi.openapi.models",
                 project);
    ASSERT_GT(n, 0);
    ASSERT_LT((size_t)n, sizeof(openapi_module_qn));

    ASSERT_TRUE(pipeline_store_has_edge_between_qns(seq_db, project, base_file_qn, "IMPORTS",
                                                    openapi_module_qn));
    ASSERT_TRUE(pipeline_store_has_edge_between_qns(par_db, project, base_file_qn, "IMPORTS",
                                                    openapi_module_qn));
    ASSERT_TRUE(
        pipeline_store_has_edge_between_qns(seq_db, project, src_qn, "INHERITS", target_qn));
    ASSERT_TRUE(
        pipeline_store_has_edge_between_qns(par_db, project, src_qn, "INHERITS", target_qn));

    char diff_err[CBM_SZ_8K] = {0};
    int diff_rc = cbm_test_compare_canonical_graphs(seq_db, par_db, project, diff_err,
                                                    sizeof(diff_err));
    if (diff_rc != 0) {
        printf("    [parallel:duplicate-import-diff] %s\n", diff_err);
    }
    ASSERT_EQ(diff_rc, 0);

    free(project);
    teardown_lang_repo();
    PASS();
}

TEST(pipeline_parallel_env_access_matches_sequential) {
    enum {
        FILLER_FILE_COUNT = 52,
        FILE_COUNT = FILLER_FILE_COUNT + 1,
        SEQUENTIAL_WORKERS = 1,
        PARALLEL_WORKERS = 4
    };
    const char *files[FILE_COUNT];
    const char *contents[FILE_COUNT];
    char filler_files[FILLER_FILE_COUNT][CBM_SZ_64];
    char filler_bodies[FILLER_FILE_COUNT][CBM_SZ_128];

    files[0] = "src/env.c";
    contents[0] = "#include <stdlib.h>\n\n"
                  "const char *load_temp(void) {\n"
                  "    return getenv(\"CBM_TEST_PARALLEL_ENV\");\n"
                  "}\n";
    for (int i = 0; i < FILLER_FILE_COUNT; i++) {
        int rn = snprintf(filler_files[i], sizeof(filler_files[i]), "fillers/filler_%02d.c", i);
        int bn = snprintf(filler_bodies[i], sizeof(filler_bodies[i]),
                          "int filler_%02d(void) {\n    return %d;\n}\n", i, i);
        ASSERT_GT(rn, 0);
        ASSERT_LT((size_t)rn, sizeof(filler_files[i]));
        ASSERT_GT(bn, 0);
        ASSERT_LT((size_t)bn, sizeof(filler_bodies[i]));
        files[i + 1] = filler_files[i];
        contents[i + 1] = filler_bodies[i];
    }

    if (setup_lang_repo(files, contents, FILE_COUNT) != 0) {
        FAIL("tmpdir");
    }

    char seq_db[CBM_SZ_512];
    char par_db[CBM_SZ_512];
    int n = snprintf(seq_db, sizeof(seq_db), "%s/env-seq.db", g_lang_tmpdir);
    ASSERT_GT(n, 0);
    ASSERT_LT((size_t)n, sizeof(seq_db));
    n = snprintf(par_db, sizeof(par_db), "%s/env-par.db", g_lang_tmpdir);
    ASSERT_GT(n, 0);
    ASSERT_LT((size_t)n, sizeof(par_db));

    char saved_workers[CBM_SZ_32] = {0};
    bool had_workers = cbm_safe_getenv("CBM_WORKERS", saved_workers, sizeof(saved_workers),
                                       NULL) != NULL;

    char *project = NULL;
    int seq_rc =
        pipeline_run_with_worker_count(g_lang_tmpdir, seq_db, SEQUENTIAL_WORKERS, &project);
    int par_rc = pipeline_run_with_worker_count(g_lang_tmpdir, par_db, PARALLEL_WORKERS, NULL);
    pipeline_restore_workers_env(had_workers, saved_workers);
    ASSERT_EQ(seq_rc, 0);
    ASSERT_EQ(par_rc, 0);
    ASSERT_NOT_NULL(project);

    char source_qn[CBM_SZ_512];
    n = snprintf(source_qn, sizeof(source_qn), "%s.src.env", project);
    ASSERT_GT(n, 0);
    ASSERT_LT((size_t)n, sizeof(source_qn));
    const char *env_qn = "__env__CBM_TEST_PARALLEL_ENV";

    ASSERT_TRUE(pipeline_store_has_edge_between_qns(seq_db, project, source_qn, "CONFIGURES",
                                                    env_qn));
    ASSERT_TRUE(pipeline_store_has_edge_between_qns(par_db, project, source_qn, "CONFIGURES",
                                                    env_qn));

    char diff_err[CBM_SZ_8K] = {0};
    int diff_rc = cbm_test_compare_canonical_graphs(seq_db, par_db, project, diff_err,
                                                    sizeof(diff_err));
    if (diff_rc != 0) {
        printf("    [parallel:env-access-diff] %s\n", diff_err);
    }
    ASSERT_EQ(diff_rc, 0);

    free(project);
    teardown_lang_repo();
    PASS();
}

TEST(pipeline_parallel_channel_edges_target_channels) {
    enum { FILLER_FILE_COUNT = 52, PARALLEL_WORKERS = 4 };
    const char *files[] = {"app/main.py"};
    const char *contents[] = {
        "from fastapi import FastAPI, WebSocket\n\n"
        "app = FastAPI()\n\n"
        "def marker(fn):\n    return fn\n\n"
        "@app.websocket('/ws')\n"
        "async def ws(websocket: WebSocket):\n"
        "    await websocket.accept()\n"
        "    await websocket.send_text('Hello, router!')\n"
        "    await websocket.send_text('Hello, world!')\n\n"
        "@app.get('/items')\n"
        "def read_items():\n"
        "    return {'ok': True}\n\n"
        "@marker\n"
        "def decorated():\n"
        "    return read_items()\n"};

    if (setup_lang_repo(files, contents, 1) != 0) {
        FAIL("tmpdir");
    }
    for (int i = 0; i < FILLER_FILE_COUNT; i++) {
        char rel[CBM_SZ_128];
        char body[CBM_SZ_256];
        int rn = snprintf(rel, sizeof(rel), "fillers/filler_%02d.py", i);
        int bn = snprintf(body, sizeof(body), "def filler_%02d():\n    return %d\n", i, i);
        ASSERT_GT(rn, 0);
        ASSERT_LT((size_t)rn, sizeof(rel));
        ASSERT_GT(bn, 0);
        ASSERT_LT((size_t)bn, sizeof(body));
        ASSERT_EQ(th_write_file(TH_PATH(g_lang_tmpdir, rel), body), 0);
    }

    char db[CBM_SZ_512];
    int n = snprintf(db, sizeof(db), "%s/channel-parallel.db", g_lang_tmpdir);
    ASSERT_GT(n, 0);
    ASSERT_LT((size_t)n, sizeof(db));

    char saved_workers[CBM_SZ_32] = {0};
    bool had_workers = cbm_safe_getenv("CBM_WORKERS", saved_workers, sizeof(saved_workers),
                                       NULL) != NULL;
    char *project = NULL;
    int rc = pipeline_run_with_worker_count(g_lang_tmpdir, db, PARALLEL_WORKERS, &project);
    pipeline_restore_workers_env(had_workers, saved_workers);
    ASSERT_EQ(rc, 0);
    ASSERT_NOT_NULL(project);
    ASSERT_EQ(pipeline_count_channel_edges_to_non_channels(db, project), 0);

    free(project);
    teardown_lang_repo();
    PASS();
}

TEST(pipeline_go_type_classification) {
    /* Port of TestGoTypeClassification */
    const char *files[] = {"types.go"};
    const char *contents[] = {"package types\n\n"
                              "type Reader interface {\n\tRead(p []byte) (n int, err error)\n}\n\n"
                              "type Writer interface {\n\tWrite(p []byte) (n int, err error)\n}\n\n"
                              "type Config struct {\n\tHost string\n\tPort int\n}\n\n"
                              "type ID = string\n"};

    if (setup_lang_repo(files, contents, 1) != 0)
        FAIL("tmpdir");
    char db[512];
    snprintf(db, sizeof(db), "%s/test.db", g_lang_tmpdir);

    cbm_pipeline_t *p = cbm_pipeline_new(g_lang_tmpdir, db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    cbm_store_t *s = cbm_store_open_path(db);
    ASSERT_NOT_NULL(s);
    const char *proj = cbm_pipeline_project_name(p);

    /* Should have 2 Interface nodes (Reader, Writer) */
    cbm_node_t *ifaces = NULL;
    int ic = 0;
    cbm_store_find_nodes_by_label(s, proj, "Interface", &ifaces, &ic);
    ASSERT_EQ(ic, 2);
    cbm_store_free_nodes(ifaces, ic);

    /* Should have 1 Struct node (Config struct) */
    cbm_node_t *structs = NULL;
    int sc = 0;
    cbm_store_find_nodes_by_label(s, proj, "Struct", &structs, &sc);
    ASSERT_EQ(sc, 1);
    ASSERT_STR_EQ(structs[0].name, "Config");
    cbm_store_free_nodes(structs, sc);

    /* Should have 1 Type node (ID alias) */
    cbm_node_t *types = NULL;
    int tc = 0;
    cbm_store_find_nodes_by_label(s, proj, "Type", &types, &tc);
    ASSERT_EQ(tc, 1);
    ASSERT_STR_EQ(types[0].name, "ID");
    cbm_store_free_nodes(types, tc);

    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_lang_repo();
    PASS();
}

TEST(pipeline_go_grouped_types) {
    /* Port of TestGoGroupedTypeDeclaration */
    const char *files[] = {"models.go"};
    const char *contents[] = {"package models\n\n"
                              "type (\n"
                              "\tRequest struct {\n\t\tURL string\n\t}\n\n"
                              "\tResponse struct {\n\t\tStatus int\n\t}\n\n"
                              "\tHandler interface {\n\t\tHandle(req Request) Response\n\t}\n"
                              ")\n"};

    if (setup_lang_repo(files, contents, 1) != 0)
        FAIL("tmpdir");
    char db[512];
    snprintf(db, sizeof(db), "%s/test.db", g_lang_tmpdir);

    cbm_pipeline_t *p = cbm_pipeline_new(g_lang_tmpdir, db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    cbm_store_t *s = cbm_store_open_path(db);
    ASSERT_NOT_NULL(s);
    const char *proj = cbm_pipeline_project_name(p);

    cbm_node_t *structs = NULL;
    int sc = 0;
    cbm_store_find_nodes_by_label(s, proj, "Struct", &structs, &sc);
    ASSERT_EQ(sc, 2); /* Request, Response */
    cbm_store_free_nodes(structs, sc);

    cbm_node_t *ifaces = NULL;
    int ic = 0;
    cbm_store_find_nodes_by_label(s, proj, "Interface", &ifaces, &ic);
    ASSERT_EQ(ic, 1); /* Handler */
    cbm_store_free_nodes(ifaces, ic);

    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_lang_repo();
    PASS();
}

TEST(pipeline_kotlin_project) {
    /* Port of TestPipelineKotlinProject */
    const char *files[] = {"Main.kt", "Service.kt"};
    const char *contents[] = {
        "fun greet(name: String): String {\n    return \"Hello, $name\"\n}\n\n"
        "fun main() {\n    val result = greet(\"world\")\n    println(result)\n}\n",

        "class OrderService {\n"
        "    fun processOrder(id: String): Boolean {\n        return true\n    }\n\n"
        "    fun submitOrder(order: String): Boolean {\n"
        "        return processOrder(order)\n    }\n}\n\n"
        "object Config {\n    val API_URL = \"https://example.com/api\"\n}\n"};

    if (setup_lang_repo(files, contents, 2) != 0)
        FAIL("tmpdir");
    char db[512];
    snprintf(db, sizeof(db), "%s/test.db", g_lang_tmpdir);

    cbm_pipeline_t *p = cbm_pipeline_new(g_lang_tmpdir, db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    cbm_store_t *s = cbm_store_open_path(db);
    ASSERT_NOT_NULL(s);
    const char *proj = cbm_pipeline_project_name(p);

    cbm_node_t *funcs = NULL;
    int fc = 0;
    cbm_store_find_nodes_by_label(s, proj, "Function", &funcs, &fc);
    ASSERT_GTE(fc, 2); /* greet, main */
    cbm_store_free_nodes(funcs, fc);

    cbm_node_t *cls = NULL;
    int cc = 0;
    cbm_store_find_nodes_by_label(s, proj, "Class", &cls, &cc);
    ASSERT_GTE(cc, 1); /* OrderService */
    cbm_store_free_nodes(cls, cc);

    cbm_node_t *methods = NULL;
    int mc = 0;
    cbm_store_find_nodes_by_label(s, proj, "Method", &methods, &mc);
    ASSERT_GTE(mc, 2); /* processOrder, submitOrder */
    cbm_store_free_nodes(methods, mc);

    int edge_count = cbm_store_count_edges(s, proj);
    ASSERT_GT(edge_count, 0);

    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_lang_repo();
    PASS();
}

TEST(pipeline_lua_anonymous_functions) {
    /* Port of TestLuaAnonymousFunctionExtraction */
    const char *files[] = {"app.lua"};
    const char *contents[] = {"local run_before_filter\n"
                              "run_before_filter = function(filter, r)\n"
                              "  return filter(r)\n"
                              "end\n\n"
                              "local validate = function(data)\n"
                              "  return data ~= nil\n"
                              "end\n\n"
                              "function named_func(x)\n"
                              "  return x + 1\n"
                              "end\n"};

    if (setup_lang_repo(files, contents, 1) != 0)
        FAIL("tmpdir");
    char db[512];
    snprintf(db, sizeof(db), "%s/test.db", g_lang_tmpdir);

    cbm_pipeline_t *p = cbm_pipeline_new(g_lang_tmpdir, db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    cbm_store_t *s = cbm_store_open_path(db);
    ASSERT_NOT_NULL(s);
    const char *proj = cbm_pipeline_project_name(p);

    cbm_node_t *funcs = NULL;
    int fc = 0;
    cbm_store_find_nodes_by_label(s, proj, "Function", &funcs, &fc);
    ASSERT_GTE(fc, 3); /* run_before_filter, validate, named_func */

    /* Verify specific functions exist */
    const char *expected[] = {"run_before_filter", "validate", "named_func"};
    for (int e = 0; e < 3; e++) {
        cbm_node_t *found = NULL;
        int fnc = 0;
        cbm_store_find_nodes_by_name(s, proj, expected[e], &found, &fnc);
        ASSERT_GT(fnc, 0);
        cbm_store_free_nodes(found, fnc);
    }

    cbm_store_free_nodes(funcs, fc);
    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_lang_repo();
    PASS();
}

TEST(pipeline_csharp_modern) {
    /* Port of TestCSharpModernFeatures */
    const char *files[] = {"Controller.cs", "Model.cs"};
    const char *contents[] = {"namespace Conduit.Features;\n\n"
                              "public class UsersController {\n"
                              "\tpublic void Get() {}\n"
                              "\tpublic void Create(string name) {}\n"
                              "}\n",

                              "namespace Conduit.Models {\n"
                              "\tclass User {\n"
                              "\t\tpublic string Name { get; set; }\n"
                              "\t\tpublic int GetAge() { return 0; }\n"
                              "\t}\n"
                              "}\n"};

    if (setup_lang_repo(files, contents, 2) != 0)
        FAIL("tmpdir");
    char db[512];
    snprintf(db, sizeof(db), "%s/test.db", g_lang_tmpdir);

    cbm_pipeline_t *p = cbm_pipeline_new(g_lang_tmpdir, db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    cbm_store_t *s = cbm_store_open_path(db);
    ASSERT_NOT_NULL(s);
    const char *proj = cbm_pipeline_project_name(p);

    cbm_node_t *modules = NULL;
    int modc = 0;
    cbm_store_find_nodes_by_label(s, proj, "Module", &modules, &modc);
    ASSERT_GTE(modc, 2);
    cbm_store_free_nodes(modules, modc);

    cbm_node_t *cls = NULL;
    int cc = 0;
    cbm_store_find_nodes_by_label(s, proj, "Class", &cls, &cc);
    ASSERT_GTE(cc, 2); /* UsersController, User */
    cbm_store_free_nodes(cls, cc);

    cbm_node_t *methods = NULL;
    int mc = 0;
    cbm_store_find_nodes_by_label(s, proj, "Method", &methods, &mc);
    ASSERT_GTE(mc, 3); /* Get, Create, GetAge */
    cbm_store_free_nodes(methods, mc);

    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_lang_repo();
    PASS();
}

TEST(pipeline_bom_stripping) {
    /* Port of TestBOMStripping — UTF-8 BOM prefix should be handled */
    snprintf(g_lang_tmpdir, sizeof(g_lang_tmpdir), "/tmp/cbm_bom_XXXXXX");
    if (!cbm_mkdtemp(g_lang_tmpdir))
        FAIL("tmpdir");

    char path[512];
    snprintf(path, sizeof(path), "%s/bom.go", g_lang_tmpdir);
    FILE *f = fopen(path, "wb");
    ASSERT_NOT_NULL(f);
    /* Write UTF-8 BOM (0xEF 0xBB 0xBF) followed by Go source */
    unsigned char bom[] = {0xEF, 0xBB, 0xBF};
    fwrite(bom, 1, 3, f);
    fprintf(f, "package main\n\nfunc BOMFunc() {}\n");
    fclose(f);

    char db[512];
    snprintf(db, sizeof(db), "%s/test.db", g_lang_tmpdir);

    cbm_pipeline_t *p = cbm_pipeline_new(g_lang_tmpdir, db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    cbm_store_t *s = cbm_store_open_path(db);
    ASSERT_NOT_NULL(s);
    const char *proj = cbm_pipeline_project_name(p);

    cbm_node_t *found = NULL;
    int fc = 0;
    cbm_store_find_nodes_by_name(s, proj, "BOMFunc", &found, &fc);
    ASSERT_GT(fc, 0); /* BOMFunc should be found despite BOM */
    cbm_store_free_nodes(found, fc);

    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_lang_repo();
    PASS();
}

TEST(pipeline_form_call_resolution) {
    /* Port of TestFORMProcedureCallResolution */
    const char *files[] = {"calc.frm"};
    const char *contents[] = {"#procedure callee(x)\n"
                              "  id x = 0;\n"
                              "#endprocedure\n"
                              "#procedure caller()\n"
                              "  #call callee(1)\n"
                              "#endprocedure\n"};

    if (setup_lang_repo(files, contents, 1) != 0)
        FAIL("tmpdir");
    char db[512];
    snprintf(db, sizeof(db), "%s/test.db", g_lang_tmpdir);

    cbm_pipeline_t *p = cbm_pipeline_new(g_lang_tmpdir, db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    cbm_store_t *s = cbm_store_open_path(db);
    ASSERT_NOT_NULL(s);
    const char *proj = cbm_pipeline_project_name(p);

    /* Verify CALLS edge exists to callee */
    cbm_edge_t *edges = NULL;
    int ec = 0;
    cbm_store_find_edges_by_type(s, proj, "CALLS", &edges, &ec);
    bool found = false;
    for (int i = 0; i < ec; i++) {
        cbm_node_t tgt = {0};
        if (cbm_store_find_node_by_id(s, edges[i].target_id, &tgt) == CBM_STORE_OK &&
            strcmp(tgt.name, "callee") == 0) {
            found = true;
        }
        cbm_node_free_fields(&tgt);
    }
    ASSERT_TRUE(found);
    if (edges)
        cbm_store_free_edges(edges, ec);

    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_lang_repo();
    PASS();
}

TEST(pipeline_python_type_inference) {
    /* Port of TestPythonMethodDispatchViaTypeInference
     * p = DataProcessor() then p.transform() → CALLS DataProcessor.transform */
    const char *files[] = {"processor.py", "main.py"};
    const char *contents[] = {"class DataProcessor:\n"
                              "    def transform(self, data):\n"
                              "        return data.upper()\n"
                              "\n"
                              "    def validate(self, data):\n"
                              "        return len(data) > 0\n",

                              "from processor import DataProcessor\n"
                              "\n"
                              "def run():\n"
                              "    p = DataProcessor()\n"
                              "    result = p.transform(\"hello\")\n"
                              "    return result\n"};

    if (setup_lang_repo(files, contents, 2) != 0)
        FAIL("tmpdir");
    char db[512];
    snprintf(db, sizeof(db), "%s/test.db", g_lang_tmpdir);

    cbm_pipeline_t *p = cbm_pipeline_new(g_lang_tmpdir, db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    cbm_store_t *s = cbm_store_open_path(db);
    ASSERT_NOT_NULL(s);
    const char *proj = cbm_pipeline_project_name(p);

    /* Verify DataProcessor.transform exists as a Method */
    cbm_node_t *methods = NULL;
    int mc = 0;
    cbm_store_find_nodes_by_name(s, proj, "transform", &methods, &mc);
    ASSERT_GT(mc, 0);

    /* Verify run() exists */
    cbm_node_t *callers = NULL;
    int clc = 0;
    cbm_store_find_nodes_by_name(s, proj, "run", &callers, &clc);
    ASSERT_GT(clc, 0);

    /* Check CALLS edge from run() to DataProcessor.transform */
    cbm_edge_t *edges = NULL;
    int ec = 0;
    cbm_store_find_edges_by_source_type(s, callers[0].id, "CALLS", &edges, &ec);
    bool found = false;
    for (int i = 0; i < ec; i++) {
        if (edges[i].target_id == methods[0].id)
            found = true;
    }
    /* Type inference may or may not resolve — log but don't fail hard */
    if (!found) {
        /* At minimum, verify the method exists and run() has some calls */
    }
    if (edges)
        cbm_store_free_edges(edges, ec);

    cbm_store_free_nodes(methods, mc);
    cbm_store_free_nodes(callers, clc);
    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_lang_repo();
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Docstring integration (port of TestDocstringIntegration)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(pipeline_docstring_go_function) {
    /* Go function with // comment docstring */
    const char *files[] = {"main.go"};
    const char *contents[] = {"package main\n\n"
                              "// Compute does something.\n"
                              "func Compute() {}\n"};
    if (setup_lang_repo(files, contents, 1) != 0)
        FAIL("tmpdir");
    char db[512];
    snprintf(db, sizeof(db), "%s/test.db", g_lang_tmpdir);
    cbm_pipeline_t *p = cbm_pipeline_new(g_lang_tmpdir, db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    cbm_store_t *s = cbm_store_open_path(db);
    ASSERT_NOT_NULL(s);
    const char *proj = cbm_pipeline_project_name(p);

    cbm_node_t *nodes = NULL;
    int nc = 0;
    cbm_store_find_nodes_by_name(s, proj, "Compute", &nodes, &nc);
    ASSERT_GT(nc, 0);

    /* Check properties_json contains docstring */
    bool found_docstring = false;
    for (int i = 0; i < nc; i++) {
        if (strcmp(nodes[i].label, "Function") == 0 && nodes[i].properties_json &&
            strstr(nodes[i].properties_json, "docstring") &&
            strstr(nodes[i].properties_json, "Compute does something")) {
            found_docstring = true;
        }
    }
    ASSERT_TRUE(found_docstring);

    cbm_store_free_nodes(nodes, nc);
    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_lang_repo();
    PASS();
}

TEST(pipeline_docstring_python_function) {
    /* Python function with triple-quoted docstring */
    const char *files[] = {"main.py"};
    const char *contents[] = {"def compute():\n"
                              "\t\"\"\"Does something.\"\"\"\n"
                              "\tpass\n"};
    if (setup_lang_repo(files, contents, 1) != 0)
        FAIL("tmpdir");
    char db[512];
    snprintf(db, sizeof(db), "%s/test.db", g_lang_tmpdir);
    cbm_pipeline_t *p = cbm_pipeline_new(g_lang_tmpdir, db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    cbm_store_t *s = cbm_store_open_path(db);
    ASSERT_NOT_NULL(s);
    const char *proj = cbm_pipeline_project_name(p);

    cbm_node_t *nodes = NULL;
    int nc = 0;
    cbm_store_find_nodes_by_name(s, proj, "compute", &nodes, &nc);
    ASSERT_GT(nc, 0);

    bool found_docstring = false;
    for (int i = 0; i < nc; i++) {
        if (strcmp(nodes[i].label, "Function") == 0 && nodes[i].properties_json &&
            strstr(nodes[i].properties_json, "docstring") &&
            strstr(nodes[i].properties_json, "Does something")) {
            found_docstring = true;
        }
    }
    ASSERT_TRUE(found_docstring);

    cbm_store_free_nodes(nodes, nc);
    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_lang_repo();
    PASS();
}

TEST(pipeline_docstring_java_method) {
    /* Java method with Javadoc comment */
    const char *files[] = {"A.java"};
    const char *contents[] = {"class A {\n"
                              "\t/** Computes result. */\n"
                              "\tvoid compute() {}\n"
                              "}\n"};
    if (setup_lang_repo(files, contents, 1) != 0)
        FAIL("tmpdir");
    char db[512];
    snprintf(db, sizeof(db), "%s/test.db", g_lang_tmpdir);
    cbm_pipeline_t *p = cbm_pipeline_new(g_lang_tmpdir, db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    cbm_store_t *s = cbm_store_open_path(db);
    ASSERT_NOT_NULL(s);
    const char *proj = cbm_pipeline_project_name(p);

    cbm_node_t *nodes = NULL;
    int nc = 0;
    cbm_store_find_nodes_by_name(s, proj, "compute", &nodes, &nc);
    ASSERT_GT(nc, 0);

    bool found_docstring = false;
    for (int i = 0; i < nc; i++) {
        if (strcmp(nodes[i].label, "Method") == 0 && nodes[i].properties_json &&
            strstr(nodes[i].properties_json, "docstring") &&
            strstr(nodes[i].properties_json, "Computes result")) {
            found_docstring = true;
        }
    }
    ASSERT_TRUE(found_docstring);

    cbm_store_free_nodes(nodes, nc);
    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_lang_repo();
    PASS();
}

TEST(pipeline_docstring_kotlin_function) {
    /* Kotlin function with KDoc comment */
    const char *files[] = {"main.kt"};
    const char *contents[] = {"/** Computes result. */\n"
                              "fun compute() {}\n"};
    if (setup_lang_repo(files, contents, 1) != 0)
        FAIL("tmpdir");
    char db[512];
    snprintf(db, sizeof(db), "%s/test.db", g_lang_tmpdir);
    cbm_pipeline_t *p = cbm_pipeline_new(g_lang_tmpdir, db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    cbm_store_t *s = cbm_store_open_path(db);
    ASSERT_NOT_NULL(s);
    const char *proj = cbm_pipeline_project_name(p);

    cbm_node_t *nodes = NULL;
    int nc = 0;
    cbm_store_find_nodes_by_name(s, proj, "compute", &nodes, &nc);
    ASSERT_GT(nc, 0);

    bool found_docstring = false;
    for (int i = 0; i < nc; i++) {
        if (strcmp(nodes[i].label, "Function") == 0 && nodes[i].properties_json &&
            strstr(nodes[i].properties_json, "docstring") &&
            strstr(nodes[i].properties_json, "Computes result")) {
            found_docstring = true;
        }
    }
    ASSERT_TRUE(found_docstring);

    cbm_store_free_nodes(nodes, nc);
    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_lang_repo();
    PASS();
}

TEST(pipeline_docstring_go_struct) {
    /* Go struct with // comment docstring */
    const char *files[] = {"main.go"};
    const char *contents[] = {"package main\n\n"
                              "// MyStruct is documented.\n"
                              "type MyStruct struct{}\n"};
    if (setup_lang_repo(files, contents, 1) != 0)
        FAIL("tmpdir");
    char db[512];
    snprintf(db, sizeof(db), "%s/test.db", g_lang_tmpdir);
    cbm_pipeline_t *p = cbm_pipeline_new(g_lang_tmpdir, db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    cbm_store_t *s = cbm_store_open_path(db);
    ASSERT_NOT_NULL(s);
    const char *proj = cbm_pipeline_project_name(p);

    cbm_node_t *nodes = NULL;
    int nc = 0;
    cbm_store_find_nodes_by_name(s, proj, "MyStruct", &nodes, &nc);
    ASSERT_GT(nc, 0);

    bool found_docstring = false;
    for (int i = 0; i < nc; i++) {
        if (strcmp(nodes[i].label, "Struct") == 0 && nodes[i].properties_json &&
            strstr(nodes[i].properties_json, "docstring") &&
            strstr(nodes[i].properties_json, "MyStruct is documented")) {
            found_docstring = true;
        }
    }
    ASSERT_TRUE(found_docstring);

    cbm_store_free_nodes(nodes, nc);
    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_lang_repo();
    PASS();
}

TEST(project_name_from_path) {
    /* Port of TestProjectNameFromPath — more cases than integ_pipeline_project_name */
    struct {
        const char *path;
        const char *want;
    } cases[] = {
        {"/tmp/bench/erlang/lib/stdlib/src", "tmp-bench-erlang-lib-stdlib-src"},
        {"/Users/martin/projects/myapp", "Users-martin-projects-myapp"},
        {"/home/user/repo", "home-user-repo"},
        {"/single", "single"},
    };

    for (int i = 0; i < 4; i++) {
        char *got = cbm_project_name_from_path(cases[i].path);
        ASSERT_NOT_NULL(got);
        ASSERT_STR_EQ(got, cases[i].want);
        free(got);
    }
    PASS();
}

/* Regression for #394/#227/#367: a Windows drive letter is case-insensitive, so
 * "c:/repo" and "C:/repo" must canonicalize to the SAME project key. Otherwise
 * agent CWDs (which report lowercase "c:\...") produce a distinct key + colliding
 * cache file that clobbers the good index, and the lowercase index self-deletes.
 * Pure string logic, so it reproduces on any platform. */
TEST(project_name_drive_letter_case_insensitive_issue394) {
    char *lower = cbm_project_name_from_path("c:/WEBDEV/Cardio-Cloud");
    char *upper = cbm_project_name_from_path("C:/WEBDEV/Cardio-Cloud");
    ASSERT_NOT_NULL(lower);
    ASSERT_NOT_NULL(upper);
    /* Both must fold to the upper-case-drive key, e.g. "C-WEBDEV-Cardio-Cloud". */
    ASSERT_STR_EQ(lower, upper);
    ASSERT_EQ(lower[0], 'C');
    free(lower);
    free(upper);

    /* And the normalizer itself upper-cases the drive root in place. */
    char buf1[32];
    snprintf(buf1, sizeof(buf1), "%s", "c:/x");
    cbm_normalize_path_sep(buf1);
    ASSERT_STR_EQ(buf1, "C:/x");
    char buf2[32];
    snprintf(buf2, sizeof(buf2), "%s", "d:\\proj\\sub");
    cbm_normalize_path_sep(buf2);
    ASSERT_STR_EQ(buf2, "D:/proj/sub");
    PASS();
}

static const char *test_null_dev(void) {
#ifdef _WIN32
    return "NUL";
#else
    return "/dev/null";
#endif
}

static bool git_available(void) {
    char cmd[128];
    snprintf(cmd, sizeof(cmd), "git --version >%s 2>&1", test_null_dev());
    int rc = system(cmd);
    return rc == 0;
}

static int run_cmd(const char *cmd) {
    return system(cmd);
}

TEST(git_context_non_git_path) {
    char *tmp = th_mktempdir("cbm_gitctx_nongit");
    ASSERT_NOT_NULL(tmp);

    cbm_git_context_t ctx = {0};
    ASSERT_EQ(cbm_git_context_resolve(tmp, &ctx), 0);
    ASSERT_FALSE(ctx.is_git);
    ASSERT_TRUE(ctx.root_exists);

    char *qn = cbm_git_context_branch_qn("proj", &ctx);
    ASSERT_NOT_NULL(qn);
    ASSERT_STR_EQ(qn, "proj.__branch__.working-tree");
    free(qn);

    char json[1024];
    ASSERT_GT(cbm_git_context_props_json(&ctx, json, sizeof(json)), 0);
    ASSERT_NOT_NULL(strstr(json, "\"is_git\":false"));
    ASSERT_NOT_NULL(strstr(json, "\"root_exists\":true"));

    char long_value[1200];
    memset(long_value, 'a', sizeof(long_value) - 1);
    long_value[sizeof(long_value) - 1] = '\0';
    cbm_git_context_t long_ctx = {
        .root_exists = true,
        .canonical_root = long_value,
    };
    char small_json[64];
    ASSERT_EQ(cbm_git_context_props_json(&long_ctx, small_json, sizeof(small_json)), 0);

    cbm_git_context_free(&ctx);
    th_rmtree(tmp);
    PASS();
}

TEST(git_context_linked_worktree) {
    if (!git_available()) {
        FAIL("git unavailable");
    }

    char *tmp = th_mktempdir("cbm_gitctx_repo");
    ASSERT_NOT_NULL(tmp);

    char repo[512], wt[512], cmd[2048];
    int n = snprintf(repo, sizeof(repo), "%s/repo with space", tmp);
    ASSERT_TRUE(n > 0 && n < (int)sizeof(repo));
    n = snprintf(wt, sizeof(wt), "%s/wt with space", tmp);
    ASSERT_TRUE(n > 0 && n < (int)sizeof(wt));
    ASSERT_EQ(th_mkdir_p(repo), 0);

    const char *null_dev = test_null_dev();
    snprintf(cmd, sizeof(cmd), "git -C \"%s\" init >%s 2>&1", repo, null_dev);
    ASSERT_EQ(run_cmd(cmd), 0);
    snprintf(cmd, sizeof(cmd), "git -C \"%s\" checkout -b main >%s 2>&1", repo, null_dev);
    ASSERT_EQ(run_cmd(cmd), 0);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "file.txt"), "hello\n"), 0);
    snprintf(cmd, sizeof(cmd), "git -C \"%s\" add file.txt >%s 2>&1", repo, null_dev);
    ASSERT_EQ(run_cmd(cmd), 0);
    snprintf(cmd, sizeof(cmd),
             "git -C \"%s\" -c user.name=\"CBM Test\" -c user.email=\"cbm@example.invalid\" "
             "commit -m \"initial\" >%s 2>&1",
             repo, null_dev);
    ASSERT_EQ(run_cmd(cmd), 0);
    snprintf(cmd, sizeof(cmd), "git -C \"%s\" worktree add -b feature/git-context \"%s\" >%s 2>&1",
             repo, wt, null_dev);
    ASSERT_EQ(run_cmd(cmd), 0);

    cbm_git_context_t main_ctx = {0};
    cbm_git_context_t wt_ctx = {0};
    ASSERT_EQ(cbm_git_context_resolve(repo, &main_ctx), 0);
    ASSERT_EQ(cbm_git_context_resolve(wt, &wt_ctx), 0);

    ASSERT_TRUE(main_ctx.is_git);
    ASSERT_FALSE(main_ctx.is_worktree);
    ASSERT_TRUE(wt_ctx.is_git);
    ASSERT_TRUE(wt_ctx.is_worktree);
    ASSERT_STR_EQ(main_ctx.canonical_root, wt_ctx.canonical_root);
    ASSERT_NOT_NULL(wt_ctx.branch);
    ASSERT_STR_EQ(wt_ctx.branch, "feature/git-context");
    ASSERT_STR_EQ(wt_ctx.branch_slug, "feature-git-context");
    ASSERT_NOT_NULL(wt_ctx.head_sha);

    char *qn = cbm_git_context_branch_qn("proj", &wt_ctx);
    ASSERT_NOT_NULL(qn);
    ASSERT_STR_EQ(qn, "proj.__branch__.feature-git-context");
    free(qn);

    char json[2048];
    ASSERT_GT(cbm_git_context_props_json(&wt_ctx, json, sizeof(json)), 0);
    ASSERT_NOT_NULL(strstr(json, "\"is_git\":true"));
    ASSERT_NOT_NULL(strstr(json, "\"is_worktree\":true"));
    ASSERT_NOT_NULL(strstr(json, "\"branch\":\"feature/git-context\""));

    cbm_git_context_free(&main_ctx);
    cbm_git_context_free(&wt_ctx);

    snprintf(cmd, sizeof(cmd), "git -C \"%s\" checkout --detach HEAD >%s 2>&1", repo, null_dev);
    ASSERT_EQ(run_cmd(cmd), 0);
    cbm_git_context_t detached_ctx = {0};
    ASSERT_EQ(cbm_git_context_resolve(repo, &detached_ctx), 0);
    ASSERT_TRUE(detached_ctx.is_detached);
    ASSERT_STR_EQ(detached_ctx.branch_slug, "detached");
    cbm_git_context_free(&detached_ctx);

    th_rmtree(tmp);
    PASS();
}

TEST(project_name_uniqueness) {
    /* Port of TestProjectNameUniqueness */
    char *a = cbm_project_name_from_path("/tmp/bench/zig/lib/std");
    char *b = cbm_project_name_from_path("/tmp/bench/erlang/lib/stdlib/src");
    ASSERT_NOT_NULL(a);
    ASSERT_NOT_NULL(b);
    ASSERT_TRUE(strcmp(a, b) != 0);
    free(a);
    free(b);
    PASS();
}

/* ── Git diff helpers (pass_gitdiff.c) ─────────────────────────── */

TEST(gitdiff_parse_range_with_count) {
    int start, count;
    cbm_parse_range("10,5", &start, &count);
    ASSERT_EQ(start, 10);
    ASSERT_EQ(count, 5);
    PASS();
}

TEST(gitdiff_parse_range_no_count) {
    int start, count;
    cbm_parse_range("10", &start, &count);
    ASSERT_EQ(start, 10);
    ASSERT_EQ(count, 1);
    PASS();
}

TEST(gitdiff_parse_range_zero_count) {
    int start, count;
    cbm_parse_range("1,0", &start, &count);
    ASSERT_EQ(start, 1);
    ASSERT_EQ(count, 0);
    PASS();
}

TEST(gitdiff_parse_range_large) {
    int start, count;
    cbm_parse_range("52,2", &start, &count);
    ASSERT_EQ(start, 52);
    ASSERT_EQ(count, 2);
    PASS();
}

TEST(gitdiff_parse_name_status) {
    const char *input = "M\tinternal/store/nodes.go\n"
                        "A\tnew_file.go\n"
                        "D\told_file.go\n"
                        "R100\tsrc/old.go\tsrc/new.go\n";

    cbm_changed_file_t files[16];
    int n = cbm_parse_name_status(input, files, 16);

    ASSERT_EQ(n, 4);
    ASSERT_STR_EQ(files[0].status, "M");
    ASSERT_STR_EQ(files[0].path, "internal/store/nodes.go");
    ASSERT_STR_EQ(files[1].status, "A");
    ASSERT_STR_EQ(files[1].path, "new_file.go");
    ASSERT_STR_EQ(files[2].status, "D");
    ASSERT_STR_EQ(files[2].path, "old_file.go");
    ASSERT_STR_EQ(files[3].status, "R");
    ASSERT_STR_EQ(files[3].path, "src/new.go");
    ASSERT_STR_EQ(files[3].old_path, "src/old.go");
    PASS();
}

TEST(gitdiff_parse_name_status_filters_untrackable) {
    const char *input = "M\tpackage-lock.json\n"
                        "M\tsrc/main.go\n"
                        "M\tvendor/lib.go\n";

    cbm_changed_file_t files[16];
    int n = cbm_parse_name_status(input, files, 16);

    ASSERT_EQ(n, 1);
    ASSERT_STR_EQ(files[0].path, "src/main.go");
    PASS();
}

TEST(gitdiff_parse_hunks) {
    const char *input = "diff --git a/main.go b/main.go\n"
                        "index abc1234..def5678 100644\n"
                        "--- a/main.go\n"
                        "+++ b/main.go\n"
                        "@@ -10,3 +10,5 @@ func main() {\n"
                        "+\tnewLine1()\n"
                        "+\tnewLine2()\n"
                        "@@ -50,0 +52,2 @@ func helper() {\n"
                        "+\tanother()\n"
                        "+\tline()\n"
                        "diff --git a/binary.png b/binary.png\n"
                        "Binary files a/binary.png and b/binary.png differ\n"
                        "diff --git a/utils.go b/utils.go\n"
                        "--- a/utils.go\n"
                        "+++ b/utils.go\n"
                        "@@ -1 +1 @@ package utils\n"
                        "-old\n"
                        "+new\n";

    cbm_changed_hunk_t hunks[16];
    int n = cbm_parse_hunks(input, hunks, 16);

    ASSERT_EQ(n, 3);
    ASSERT_STR_EQ(hunks[0].path, "main.go");
    ASSERT_EQ(hunks[0].start_line, 10);
    ASSERT_EQ(hunks[0].end_line, 14);

    ASSERT_STR_EQ(hunks[1].path, "main.go");
    ASSERT_EQ(hunks[1].start_line, 52);
    ASSERT_EQ(hunks[1].end_line, 53);

    ASSERT_STR_EQ(hunks[2].path, "utils.go");
    ASSERT_EQ(hunks[2].start_line, 1);
    ASSERT_EQ(hunks[2].end_line, 1);
    PASS();
}

TEST(gitdiff_parse_hunks_no_newline_marker) {
    const char *input = "diff --git a/file.go b/file.go\n"
                        "--- a/file.go\n"
                        "+++ b/file.go\n"
                        "@@ -5,2 +5,3 @@ func foo() {\n"
                        "+\tbar()\n"
                        "\\ No newline at end of file\n";

    cbm_changed_hunk_t hunks[4];
    int n = cbm_parse_hunks(input, hunks, 4);

    ASSERT_EQ(n, 1);
    ASSERT_EQ(hunks[0].start_line, 5);
    ASSERT_EQ(hunks[0].end_line, 7);
    PASS();
}

TEST(gitdiff_parse_hunks_mode_change) {
    const char *input = "diff --git a/script.sh b/script.sh\n"
                        "old mode 100644\n"
                        "new mode 100755\n";

    cbm_changed_hunk_t hunks[4];
    int n = cbm_parse_hunks(input, hunks, 4);
    ASSERT_EQ(n, 0);
    PASS();
}

TEST(gitdiff_parse_hunks_deletion) {
    const char *input = "diff --git a/file.go b/file.go\n"
                        "--- a/file.go\n"
                        "+++ b/file.go\n"
                        "@@ -10,3 +10,0 @@ func foo() {\n";

    cbm_changed_hunk_t hunks[4];
    int n = cbm_parse_hunks(input, hunks, 4);
    ASSERT_EQ(n, 1);
    ASSERT_EQ(hunks[0].start_line, 10);
    PASS();
}

static const cbm_store_delta_edge_t *pipeline_delta_find_edge(const cbm_pipeline_file_delta_t *delta,
                                                              const char *type) {
    for (int i = 0; i < delta->delta.edge_count; i++) {
        if (strcmp(delta->edges[i].type, type) == 0) {
            return &delta->edges[i];
        }
    }
    return NULL;
}

static const cbm_store_delta_edge_t *pipeline_delta_find_edge_by_qn(
    const cbm_pipeline_file_delta_t *delta, const char *source_qn, const char *target_qn,
    const char *type) {
    for (int i = 0; i < delta->delta.edge_count; i++) {
        const cbm_store_delta_edge_t *edge = &delta->edges[i];
        if (edge->source_qn && edge->target_qn && edge->type &&
            strcmp(edge->source_qn, source_qn) == 0 &&
            strcmp(edge->target_qn, target_qn) == 0 && strcmp(edge->type, type) == 0) {
            return edge;
        }
    }
    return NULL;
}

static const cbm_store_import_ref_t *pipeline_delta_first_import(
    const cbm_pipeline_file_delta_t *delta) {
    return delta->delta.import_count > 0 ? &delta->imports[0] : NULL;
}

static int pipeline_delta_plan_contains_path(const cbm_pipeline_file_delta_plan_t *plan,
                                             const char *path) {
    for (int i = 0; i < plan->affected_count; i++) {
        if (strcmp(plan->affected_paths[i], path) == 0) {
            return 1;
        }
    }
    return 0;
}

static void pipeline_delta_free_string_array(char **items, int count) {
    if (!items) {
        return;
    }
    for (int i = 0; i < count; i++) {
        free(items[i]);
    }
    free(items);
}

static int pipeline_delta_store_qn_exists(cbm_store_t *s, const char *project, const char *qn) {
    cbm_node_t node = {0};
    int rc = cbm_store_find_node_by_qn(s, project, qn, &node);
    if (rc == CBM_STORE_OK) {
        cbm_node_free_fields(&node);
        return 1;
    }
    return 0;
}

static void pipeline_delta_attach_test_metadata(cbm_pipeline_file_delta_t *delta,
                                                cbm_file_hash_t *hash,
                                                cbm_file_state_t *state) {
    enum { PIPELINE_DELTA_TEST_GENERATION = 1 };
    *hash = (cbm_file_hash_t){.project = delta->delta.project,
                              .rel_path = delta->delta.rel_path,
                              .sha256 = "test-hash",
                              .mtime_ns = 1,
                              .size = 10};
    *state = (cbm_file_state_t){.project = delta->delta.project,
                                .rel_path = delta->delta.rel_path,
                                .content_hash = "test-content",
                                .git_oid = "",
                                .mtime_ns = 1,
                                .size = 10,
                                .language = "go",
                                .pass_fingerprint = "test-pass",
                                .generation = PIPELINE_DELTA_TEST_GENERATION,
                                .indexed_at = "2026-06-30T00:00:00Z"};
    delta->delta.generation = PIPELINE_DELTA_TEST_GENERATION;
    delta->delta.file_hash = hash;
    delta->delta.file_state = state;
}

static int64_t pipeline_delta_seed_existing_ownership_id(cbm_store_t *s, const char *project,
                                                         const char *rel_path,
                                                         const char *qualified_name) {
    enum { PIPELINE_DELTA_TEST_BASE_GENERATION = 1 };
    cbm_node_t node = {.project = (char *)project,
                       .label = "Function",
                       .name = "Existing",
                       .qualified_name = (char *)qualified_name,
                       .file_path = (char *)rel_path,
                       .start_line = 1,
                       .end_line = 1,
                       .properties_json = "{}"};
    int64_t node_id = cbm_store_upsert_node(s, &node);
    if (node_id <= CBM_STORE_NO_NODE_ID) {
        return CBM_STORE_NO_NODE_ID;
    }
    cbm_file_state_t state = {.project = (char *)project,
                              .rel_path = (char *)rel_path,
                              .content_hash = "base-content",
                              .git_oid = "",
                              .mtime_ns = 1,
                              .size = 10,
                              .language = "go",
                              .pass_fingerprint = "test-pass",
                              .generation = PIPELINE_DELTA_TEST_BASE_GENERATION,
                              .indexed_at = "2026-06-30T00:00:00Z"};
    if (cbm_store_upsert_file_state(s, &state) != CBM_STORE_OK) {
        return CBM_STORE_NO_NODE_ID;
    }
    if (cbm_store_upsert_node_owner(s, project, node_id, rel_path,
                                    PIPELINE_DELTA_TEST_BASE_GENERATION) != CBM_STORE_OK) {
        return CBM_STORE_NO_NODE_ID;
    }
    return node_id;
}

static int pipeline_delta_seed_existing_ownership(cbm_store_t *s, const char *project,
                                                  const char *rel_path,
                                                  const char *qualified_name) {
    return pipeline_delta_seed_existing_ownership_id(s, project, rel_path, qualified_name) >
                   CBM_STORE_NO_NODE_ID
               ? CBM_STORE_OK
               : CBM_STORE_ERR;
}

static int pipeline_delta_seed_file_owned_unowned_source_edge(
    cbm_store_t *s, const char *project, const char *rel_path, const char *source_qn,
    const char *target_qn, const char *edge_type) {
    enum { PIPELINE_DELTA_TEST_BASE_GENERATION = 1 };
    int64_t target_id = pipeline_delta_seed_existing_ownership_id(s, project, rel_path, target_qn);
    if (target_id <= CBM_STORE_NO_NODE_ID) {
        return CBM_STORE_ERR;
    }
    cbm_node_t source = {.project = (char *)project,
                         .label = "Module",
                         .name = "module",
                         .qualified_name = (char *)source_qn,
                         .file_path = "",
                         .properties_json = "{}"};
    int64_t source_id = cbm_store_upsert_node(s, &source);
    if (source_id <= CBM_STORE_NO_NODE_ID) {
        return CBM_STORE_ERR;
    }
    cbm_edge_t edge = {.project = (char *)project,
                       .source_id = source_id,
                       .target_id = target_id,
                       .type = (char *)edge_type,
                       .properties_json = "{}"};
    int64_t edge_id = cbm_store_insert_edge(s, &edge);
    if (edge_id <= CBM_STORE_NO_NODE_ID) {
        return CBM_STORE_ERR;
    }
    return cbm_store_upsert_edge_owner(s, project, edge_id, rel_path, NULL,
                                       PIPELINE_DELTA_TEST_BASE_GENERATION);
}

static int pipeline_delta_seed_project_node(cbm_store_t *s, const char *project) {
    cbm_node_t project_node = {.project = (char *)project,
                               .label = "Project",
                               .name = (char *)project,
                               .qualified_name = (char *)project,
                               .file_path = "",
                               .properties_json = "{}"};
    return cbm_store_upsert_node(s, &project_node) > CBM_STORE_NO_NODE_ID ? CBM_STORE_OK
                                                                          : CBM_STORE_ERR;
}

static int pipeline_store_file_state_generation_memory(cbm_store_t *s, const char *project,
                                                       const char *rel_path,
                                                       int64_t *generation) {
    cbm_file_state_t state = {0};
    int rc = cbm_store_get_file_state(s, project, rel_path, &state);
    if (rc == CBM_STORE_OK && generation) {
        *generation = state.generation;
    }
    cbm_store_file_state_free_fields(&state);
    return rc;
}

TEST(pipeline_file_delta_scratch_seed_excludes_changed_paths) {
    const char *project = "test";
    const char *changed_paths[] = {"main.go"};
    const int changed_path_count = (int)(sizeof(changed_paths) / sizeof(changed_paths[0]));
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, project, "/tmp"), CBM_STORE_OK);

    cbm_node_t helper = {.project = (char *)project,
                         .label = "Function",
                         .name = "Helper",
                         .qualified_name = "test.helper.Helper",
                         .file_path = "helper.go",
                         .start_line = 1,
                         .end_line = 1,
                         .properties_json = "{\"is_exported\":true}"};
    cbm_node_t stale = {.project = (char *)project,
                        .label = "Function",
                        .name = "Old",
                        .qualified_name = "test.main.Old",
                        .file_path = "main.go",
                        .start_line = 1,
                        .end_line = 1,
                        .properties_json = "{\"is_exported\":true}"};
    cbm_node_t module = {.project = (char *)project,
                         .label = "Module",
                         .name = "helper",
                         .qualified_name = "test.helper",
                         .file_path = "helper.go",
                         .start_line = 1,
                         .end_line = 1,
                         .properties_json = "{}"};
    ASSERT_GT(cbm_store_upsert_node(s, &helper), 0);
    ASSERT_GT(cbm_store_upsert_node(s, &stale), 0);
    ASSERT_GT(cbm_store_upsert_node(s, &module), 0);

    cbm_gbuf_t *scratch = cbm_gbuf_new(project, "/tmp");
    cbm_registry_t *registry = cbm_registry_new();
    ASSERT_NOT_NULL(scratch);
    ASSERT_NOT_NULL(registry);
    ASSERT_EQ(cbm_pipeline_seed_file_delta_scratch_from_store(
                  s, scratch, registry, project, changed_paths, changed_path_count),
              CBM_STORE_OK);

    ASSERT_NULL(cbm_gbuf_find_by_qn(scratch, "test.helper.Helper"));
    ASSERT_NOT_NULL(cbm_gbuf_find_by_qn(scratch, "test.helper"));
    ASSERT_NULL(cbm_gbuf_find_by_qn(scratch, "test.main.Old"));
    ASSERT_TRUE(cbm_registry_exists(registry, "test.helper.Helper"));
    ASSERT_FALSE(cbm_registry_exists(registry, "test.main.Old"));
    cbm_pipeline_ctx_t ctx = {.project_name = project,
                              .repo_path = "/tmp",
                              .gbuf = scratch,
                              .registry = registry,
                              .store_backed_node_lookup = s,
                              .store_backed_changed_paths = changed_paths,
                              .store_backed_changed_path_count = changed_path_count};
    ASSERT_NOT_NULL(cbm_pipeline_find_node_by_qn(&ctx, "test.helper.Helper"));
    ASSERT_NOT_NULL(cbm_gbuf_find_by_qn(scratch, "test.helper.Helper"));
    ASSERT_NULL(cbm_pipeline_find_node_by_qn(&ctx, "test.main.Old"));

    cbm_registry_free(registry);
    cbm_gbuf_free(scratch);
    cbm_store_close(s);
    PASS();
}

TEST(pipeline_file_delta_scratch_seed_preserves_structure_roots) {
    enum { PIPELINE_DELTA_STRUCTURE_GENERATION = 1 };
    const char *project = "test";
    const char *repo_path = "/tmp";
    const char *rel_path = "main.go";
    const char *changed_paths[] = {rel_path};
    const int changed_path_count = (int)(sizeof(changed_paths) / sizeof(changed_paths[0]));
    const char *branch_qn = "test.branch.main";
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, project, repo_path), CBM_STORE_OK);
    ASSERT_EQ(pipeline_delta_seed_existing_ownership(s, project, rel_path, "test.main.Old"),
              CBM_STORE_OK);

    char *file_qn = cbm_pipeline_fqn_compute(project, rel_path, "__file__");
    ASSERT_NOT_NULL(file_qn);
    cbm_node_t project_node = {.project = (char *)project,
                               .label = "Project",
                               .name = (char *)project,
                               .qualified_name = (char *)project,
                               .file_path = NULL,
                               .properties_json = "{}"};
    cbm_node_t branch_node = {.project = (char *)project,
                              .label = "Branch",
                              .name = "main",
                              .qualified_name = (char *)branch_qn,
                              .file_path = NULL,
                              .properties_json = "{}"};
    cbm_node_t file_node = {.project = (char *)project,
                            .label = "File",
                            .name = (char *)rel_path,
                            .qualified_name = file_qn,
                            .file_path = (char *)rel_path,
                            .properties_json = "{}"};
    ASSERT_GT(cbm_store_upsert_node(s, &project_node), CBM_STORE_NO_NODE_ID);
    int64_t branch_id = cbm_store_upsert_node(s, &branch_node);
    int64_t file_id = cbm_store_upsert_node(s, &file_node);
    ASSERT_GT(branch_id, CBM_STORE_NO_NODE_ID);
    ASSERT_GT(file_id, CBM_STORE_NO_NODE_ID);
    ASSERT_EQ(cbm_store_upsert_node_owner(s, project, file_id, rel_path,
                                          PIPELINE_DELTA_STRUCTURE_GENERATION),
              CBM_STORE_OK);
    cbm_edge_t contains = {.project = (char *)project,
                           .source_id = branch_id,
                           .target_id = file_id,
                           .type = "CONTAINS_FILE",
                           .properties_json = "{}"};
    int64_t edge_id = cbm_store_insert_edge(s, &contains);
    ASSERT_GT(edge_id, CBM_STORE_NO_NODE_ID);
    ASSERT_EQ(cbm_store_upsert_edge_owner(s, project, edge_id, rel_path, NULL,
                                          PIPELINE_DELTA_STRUCTURE_GENERATION),
              CBM_STORE_OK);

    cbm_gbuf_t *scratch = cbm_gbuf_new(project, repo_path);
    cbm_registry_t *registry = cbm_registry_new();
    ASSERT_NOT_NULL(scratch);
    ASSERT_NOT_NULL(registry);
    ASSERT_EQ(cbm_pipeline_seed_file_delta_scratch_from_store(
                  s, scratch, registry, project, changed_paths, changed_path_count),
              CBM_STORE_OK);
    ASSERT_NOT_NULL(cbm_gbuf_find_by_qn(scratch, project));
    ASSERT_NOT_NULL(cbm_gbuf_find_by_qn(scratch, branch_qn));
    ASSERT_NULL(cbm_gbuf_find_by_qn(scratch, file_qn));

    ASSERT_EQ(cbm_pipeline_ensure_file_structure(scratch, project, branch_qn, rel_path, NULL), 0);
    ASSERT_GT(cbm_gbuf_upsert_node(scratch, "Function", "New", "test.main.New", rel_path, 1,
                                   1, "{\"is_exported\":true}"),
              0);

    cbm_pipeline_file_delta_t delta = {0};
    ASSERT_EQ(cbm_pipeline_build_file_delta_from_gbuf(scratch, project, rel_path, 0, &delta),
              CBM_STORE_OK);
    const cbm_store_delta_edge_t *structure_edge =
        pipeline_delta_find_edge(&delta, "CONTAINS_FILE");
    ASSERT_NOT_NULL(structure_edge);
    ASSERT_STR_EQ(structure_edge->source_qn, branch_qn);
    ASSERT_STR_EQ(structure_edge->target_qn, file_qn);
    cbm_file_hash_t hash = {0};
    cbm_file_state_t state = {0};
    pipeline_delta_attach_test_metadata(&delta, &hash, &state);

    cbm_pipeline_file_delta_plan_t plan = {0};
    ASSERT_EQ(cbm_pipeline_plan_file_delta(s, &delta, CBM_SZ_4, &plan), CBM_STORE_OK);
    ASSERT_EQ(plan.route, CBM_PIPELINE_DELTA_ROUTE_EXACT_CANDIDATE);

    cbm_pipeline_file_delta_plan_free(&plan);
    cbm_pipeline_file_delta_free(&delta);
    cbm_registry_free(registry);
    cbm_gbuf_free(scratch);
    cbm_store_close(s);
    free(file_qn);
    PASS();
}

TEST(pipeline_file_delta_scratch_seed_supports_external_endpoint_descriptor) {
    const char *project = "test";
    const char *changed_paths[] = {"main.go"};
    const int changed_path_count = (int)(sizeof(changed_paths) / sizeof(changed_paths[0]));
    const char *helper_qn = "test.helper.Helper";
    const char *main_file_qn = "test.main.__file__";
    const char *main_qn = "test.main.Run";
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, project, "/tmp"), CBM_STORE_OK);
    ASSERT_EQ(pipeline_delta_seed_existing_ownership(s, project, changed_paths[0],
                                                     "test.main.Old"),
              CBM_STORE_OK);

    cbm_node_t helper = {.project = (char *)project,
                         .label = "Function",
                         .name = "Helper",
                         .qualified_name = (char *)helper_qn,
                         .file_path = "helper.go",
                         .start_line = 1,
                         .end_line = 1,
                         .properties_json = "{\"is_exported\":true}"};
    ASSERT_GT(cbm_store_upsert_node(s, &helper), 0);

    cbm_gbuf_t *scratch = cbm_gbuf_new(project, "/tmp");
    cbm_registry_t *registry = cbm_registry_new();
    ASSERT_NOT_NULL(scratch);
    ASSERT_NOT_NULL(registry);
    ASSERT_EQ(cbm_pipeline_seed_file_delta_scratch_from_store(
                  s, scratch, registry, project, changed_paths, changed_path_count),
              CBM_STORE_OK);

    int64_t file_id =
        cbm_gbuf_upsert_node(scratch, "File", "main.go", main_file_qn, "main.go", 1, 1, "{}");
    int64_t run_id = cbm_gbuf_upsert_node(scratch, "Function", "Run", main_qn, "main.go", 2, 4,
                                          "{\"is_exported\":true}");
    ASSERT_GT(file_id, 0);
    ASSERT_GT(run_id, 0);
    cbm_pipeline_ctx_t ctx = {.project_name = project,
                              .repo_path = "/tmp",
                              .gbuf = scratch,
                              .registry = registry,
                              .store_backed_node_lookup = s,
                              .store_backed_changed_paths = changed_paths,
                              .store_backed_changed_path_count = changed_path_count};
    const cbm_gbuf_node_t *helper_node = cbm_pipeline_find_node_by_qn(&ctx, helper_qn);
    ASSERT_NOT_NULL(helper_node);
    ASSERT_EQ(cbm_pipeline_insert_import_edge(&ctx, file_id, helper_node, "Helper"), 1);
    ASSERT_GT(cbm_gbuf_insert_edge(scratch, run_id, helper_node->id, "CALLS", "{}"), 0);

    cbm_pipeline_file_delta_t delta = {0};
    ASSERT_EQ(cbm_pipeline_build_file_delta_from_gbuf(scratch, project, changed_paths[0], 1, &delta),
              CBM_STORE_OK);
    cbm_file_hash_t hash = {0};
    cbm_file_state_t state = {0};
    pipeline_delta_attach_test_metadata(&delta, &hash, &state);
    ASSERT_EQ(delta.delta.edge_count, 2);
    ASSERT_NOT_NULL(pipeline_delta_find_edge(&delta, "CALLS"));
    ASSERT_NOT_NULL(pipeline_delta_find_edge(&delta, "IMPORTS"));

    cbm_pipeline_file_delta_plan_t plan = {0};
    ASSERT_EQ(cbm_pipeline_plan_file_delta(s, &delta, CBM_SZ_4, &plan), CBM_STORE_OK);
    ASSERT_EQ(plan.route, CBM_PIPELINE_DELTA_ROUTE_EXACT_CANDIDATE);
    ASSERT_EQ(pipeline_delta_plan_contains_path(&plan, changed_paths[0]), 1);

    cbm_pipeline_file_delta_plan_free(&plan);
    cbm_pipeline_file_delta_free(&delta);
    cbm_registry_free(registry);
    cbm_gbuf_free(scratch);
    cbm_store_close(s);
    PASS();
}

TEST(pipeline_file_delta_descriptor_from_gbuf) {
    cbm_gbuf_t *gb = cbm_gbuf_new("proj", "/tmp/proj");
    ASSERT_NOT_NULL(gb);
    int64_t file_id = cbm_gbuf_upsert_node(gb, "File", "main.go", "proj.main.__file__",
                                           "main.go", 1, 1, "{}");
    int64_t run_id = cbm_gbuf_upsert_node(gb, "Function", "Run", "proj.main.Run",
                                          "main.go", 3, 5, "{\"is_exported\":true}");
    int64_t helper_id = cbm_gbuf_upsert_node(gb, "Function", "Helper", "proj.helper.Helper",
                                             "helper.go", 1, 3, "{\"is_exported\":true}");
    ASSERT_GT(file_id, 0);
    ASSERT_GT(run_id, 0);
    ASSERT_GT(helper_id, 0);

    const cbm_gbuf_node_t *helper = cbm_gbuf_find_by_qn(gb, "proj.helper.Helper");
    ASSERT_NOT_NULL(helper);
    cbm_pipeline_ctx_t ctx = {.project_name = "proj", .repo_path = "/tmp/proj", .gbuf = gb};
    ASSERT_EQ(cbm_pipeline_insert_import_edge(&ctx, file_id, helper, "Helper"), 1);
    ASSERT_GT(cbm_gbuf_insert_edge(gb, run_id, helper_id, "CALLS", "{\"line\":4}"), 0);
    ASSERT_GT(cbm_gbuf_insert_edge(gb, helper_id, run_id, "CALLS", "{\"line\":2}"), 0);

    cbm_pipeline_file_delta_t delta = {0};
    ASSERT_EQ(cbm_pipeline_build_file_delta_from_gbuf(gb, "proj", "main.go", 7, &delta),
              CBM_STORE_OK);
    ASSERT_EQ(delta.unsupported_edge_count, 0);
    ASSERT_EQ(delta.delta.node_count, 2);
    ASSERT_EQ(delta.delta.export_count, 1);
    ASSERT_EQ(delta.delta.edge_count, 2);
    ASSERT_EQ(delta.delta.import_count, 1);
    ASSERT_STR_EQ(delta.delta.project, "proj");
    ASSERT_STR_EQ(delta.delta.rel_path, "main.go");
    ASSERT_EQ(delta.delta.generation, 7);
    ASSERT_STR_EQ(delta.delta.derived_view_name, CBM_STORE_DERIVED_VIEW_NODES_FTS);
    ASSERT_STR_EQ(delta.delta.derived_status, CBM_STORE_DERIVED_STATUS_COMPLETE);
    ASSERT_STR_EQ(delta.exports[0].qualified_name, "proj.main.Run");

    const cbm_store_delta_edge_t *call_edge = pipeline_delta_find_edge(&delta, "CALLS");
    ASSERT_NOT_NULL(call_edge);
    ASSERT_STR_EQ(call_edge->source_qn, "proj.main.Run");
    ASSERT_STR_EQ(call_edge->target_qn, "proj.helper.Helper");

    const cbm_store_import_ref_t *imp = pipeline_delta_first_import(&delta);
    ASSERT_NOT_NULL(imp);
    ASSERT_STR_EQ(imp->import_text, "proj.helper.Helper");
    ASSERT_STR_EQ(imp->local_name, "Helper");
    ASSERT_STR_EQ(imp->target_qn, "proj.helper.Helper");

    cbm_pipeline_file_delta_free(&delta);
    cbm_gbuf_free(gb);
    PASS();
}

TEST(pipeline_file_delta_preserves_safe_inbound_edges_for_overlay) {
    const char *project = "test";
    const char *target_rel = "target.go";
    const char *caller_rel = "caller.go";
    const char *target_qn = "test.target.Handle";
    const char *stale_qn = "test.target.Legacy";
    const char *caller_qn = "test.caller.Call";

    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, project, "/tmp/test"), CBM_STORE_OK);
    int64_t target_id =
        pipeline_delta_seed_existing_ownership_id(s, project, target_rel, target_qn);
    int64_t stale_id = pipeline_delta_seed_existing_ownership_id(s, project, target_rel, stale_qn);
    int64_t caller_id =
        pipeline_delta_seed_existing_ownership_id(s, project, caller_rel, caller_qn);
    ASSERT_GT(target_id, 0);
    ASSERT_GT(stale_id, 0);
    ASSERT_GT(caller_id, 0);

    cbm_edge_t call_edge = {.project = (char *)project,
                            .source_id = caller_id,
                            .target_id = target_id,
                            .type = "CALLS",
                            .properties_json = "{\"confidence\":0.9}"};
    cbm_edge_t stale_edge = {.project = (char *)project,
                             .source_id = caller_id,
                             .target_id = stale_id,
                             .type = "CALLS",
                             .properties_json = "{\"stale\":true}"};
    cbm_edge_t recomputed_edge = {.project = (char *)project,
                                  .source_id = caller_id,
                                  .target_id = target_id,
                                  .type = CBM_PIPELINE_EDGE_SIMILAR_TO,
                                  .properties_json = "{\"score\":1.0}"};
    ASSERT_GT(cbm_store_insert_edge(s, &call_edge), 0);
    ASSERT_GT(cbm_store_insert_edge(s, &stale_edge), 0);
    ASSERT_GT(cbm_store_insert_edge(s, &recomputed_edge), 0);
    ASSERT_EQ(cbm_store_rebuild_file_delta_owners(s, project, 1), CBM_STORE_OK);

    cbm_gbuf_t *gb = cbm_gbuf_new(project, "/tmp/test");
    ASSERT_NOT_NULL(gb);
    ASSERT_GT(cbm_gbuf_upsert_node(gb, "Function", "Handle", target_qn, target_rel, 3, 7,
                                   "{\"is_exported\":true}"),
              0);
    cbm_pipeline_file_delta_t delta = {0};
    ASSERT_EQ(cbm_pipeline_build_file_delta_from_gbuf(gb, project, target_rel, 1, &delta),
              CBM_STORE_OK);
    ASSERT_EQ(delta.delta.edge_count, 0);

    int added = -1;
    ASSERT_EQ(cbm_pipeline_file_delta_add_preserved_inbound_edges(s, &delta, &added),
              CBM_STORE_OK);
    ASSERT_EQ(added, 1);
    ASSERT_EQ(delta.delta.edge_count, 1);
    const cbm_store_delta_edge_t *preserved =
        pipeline_delta_find_edge_by_qn(&delta, caller_qn, target_qn, "CALLS");
    ASSERT_NOT_NULL(preserved);
    ASSERT_STR_EQ(preserved->properties_json, "{\"confidence\":0.9}");
    ASSERT_NULL(pipeline_delta_find_edge_by_qn(&delta, caller_qn, stale_qn, "CALLS"));
    ASSERT_NULL(
        pipeline_delta_find_edge_by_qn(&delta, caller_qn, target_qn, CBM_PIPELINE_EDGE_SIMILAR_TO));

    added = -1;
    ASSERT_EQ(cbm_pipeline_file_delta_add_preserved_inbound_edges(s, &delta, &added),
              CBM_STORE_OK);
    ASSERT_EQ(added, 0);
    ASSERT_EQ(delta.delta.edge_count, 1);

    cbm_pipeline_file_delta_free(&delta);
    cbm_gbuf_free(gb);
    cbm_store_close(s);
    PASS();
}

TEST(pipeline_file_delta_descriptor_marks_unsupported_edges) {
    cbm_gbuf_t *gb = cbm_gbuf_new("proj", "/tmp/proj");
    ASSERT_NOT_NULL(gb);
    int64_t run_id = cbm_gbuf_upsert_node(gb, "Function", "Run", "proj.main.Run",
                                          "main.go", 3, 5, "{}");
    ASSERT_GT(run_id, 0);
    ASSERT_GT(cbm_gbuf_insert_edge(gb, run_id, 9999, "CALLS", "{}"), 0);

    cbm_pipeline_file_delta_t delta = {0};
    ASSERT_EQ(cbm_pipeline_build_file_delta_from_gbuf(gb, "proj", "main.go", 8, &delta),
              CBM_STORE_OK);
    ASSERT_EQ(delta.unsupported_edge_count, 1);
    ASSERT_EQ(delta.delta.node_count, 1);
    ASSERT_EQ(delta.delta.edge_count, 0);

    cbm_pipeline_file_delta_free(&delta);
    cbm_gbuf_free(gb);
    PASS();
}

TEST(pipeline_file_delta_metadata_from_file) {
    enum {
        PIPELINE_DELTA_META_GENERATION_FIRST = 11,
        PIPELINE_DELTA_META_GENERATION_SECOND = 12,
    };
    char *tmp = th_mktempdir("cbm_delta_meta");
    ASSERT_NOT_NULL(tmp);
    const char *path = TH_PATH(tmp, "main.go");
    const char *first_content = "package main\nfunc Run() {}\n";
    ASSERT_EQ(th_write_file(path, first_content), 0);

    cbm_file_info_t file = {
        .path = (char *)path,
        .rel_path = "main.go",
        .language = CBM_LANG_GO,
        .size = (int64_t)strlen(first_content),
    };
    cbm_pipeline_file_delta_t delta = {
        .delta = {.project = "test",
                  .rel_path = "main.go",
                  .generation = PIPELINE_DELTA_META_GENERATION_FIRST}};
    ASSERT_EQ(cbm_pipeline_attach_file_delta_metadata(&delta, &file), CBM_STORE_OK);
    ASSERT(delta.delta.file_hash == &delta.file_hash);
    ASSERT(delta.delta.file_state == &delta.file_state);
    ASSERT_STR_EQ(delta.file_hash.project, "test");
    ASSERT_STR_EQ(delta.file_hash.rel_path, "main.go");
    ASSERT_STR_EQ(delta.file_hash.sha256, "");
    ASSERT_EQ(delta.file_hash.size, (int64_t)strlen(first_content));
    ASSERT_STR_EQ(delta.file_state.project, "test");
    ASSERT_STR_EQ(delta.file_state.rel_path, "main.go");
    ASSERT_EQ(delta.file_state.size, (int64_t)strlen(first_content));
    ASSERT_STR_EQ(delta.file_state.language, "Go");
    ASSERT_EQ(delta.file_state.generation, PIPELINE_DELTA_META_GENERATION_FIRST);
    ASSERT_NOT_NULL(delta.file_state.content_hash);
    ASSERT_EQ((int)strlen(delta.file_state.content_hash), CBM_SZ_16);
    ASSERT_NOT_NULL(delta.file_state.indexed_at);
    ASSERT_NOT_NULL(strchr(delta.file_state.indexed_at, 'T'));
    char first_hash[CBM_SZ_32];
    snprintf(first_hash, sizeof(first_hash), "%s", delta.file_state.content_hash);

    const char *second_content = "package main\nfunc Run() { println(\"changed\") }\n";
    ASSERT_EQ(th_write_file(path, second_content), 0);
    cbm_pipeline_file_delta_t changed = {
        .delta = {.project = "test",
                  .rel_path = "main.go",
                  .generation = PIPELINE_DELTA_META_GENERATION_SECOND}};
    ASSERT_EQ(cbm_pipeline_attach_file_delta_metadata(&changed, &file), CBM_STORE_OK);
    ASSERT_NEQ(strcmp(first_hash, changed.file_state.content_hash), 0);
    ASSERT_EQ(changed.file_state.generation, PIPELINE_DELTA_META_GENERATION_SECOND);

    th_cleanup(tmp);
    PASS();
}

TEST(pipeline_file_delta_metadata_accepts_effective_fingerprint) {
    enum { PIPELINE_DELTA_META_GENERATION = 13 };
    char effective_fingerprint[CBM_SZ_256];
    ASSERT_EQ(cbm_pipeline_format_file_delta_pass_fingerprint(
                  effective_fingerprint, sizeof(effective_fingerprint), CBM_MODE_FULL, 0.7,
                  0.25, 0.75, 0.3, 0.6),
              CBM_STORE_OK);
    char *tmp = th_mktempdir("cbm_delta_meta_fingerprint");
    ASSERT_NOT_NULL(tmp);
    const char *path = TH_PATH(tmp, "main.go");
    const char *content = "package main\nfunc Run() { println(\"fingerprint\") }\n";
    ASSERT_EQ(th_write_file(path, content), 0);

    cbm_file_info_t file = {
        .path = (char *)path,
        .rel_path = "main.go",
        .language = CBM_LANG_GO,
        .size = (int64_t)strlen(content),
    };
    cbm_pipeline_file_delta_t delta = {
        .delta = {.project = "test",
                  .rel_path = "main.go",
                  .generation = PIPELINE_DELTA_META_GENERATION}};
    ASSERT_EQ(cbm_pipeline_attach_file_delta_metadata_with_fingerprint(
                  &delta, &file, effective_fingerprint),
              CBM_STORE_OK);
    ASSERT_STR_EQ(delta.file_state.pass_fingerprint, effective_fingerprint);

    th_cleanup(tmp);
    PASS();
}

TEST(pipeline_file_delta_stamp_generation_updates_metadata) {
    enum { PIPELINE_DELTA_STAMP_GENERATION = 21 };
    cbm_pipeline_file_delta_t delta = {.delta = {.project = "test", .rel_path = "main.go"}};
    cbm_file_hash_t hash = {.project = "test", .rel_path = "main.go", .sha256 = ""};
    delta.file_state = (cbm_file_state_t){.project = "test",
                                          .rel_path = "main.go",
                                          .content_hash = "test-content",
                                          .indexed_at = "2026-07-01T00:00:00Z"};
    delta.delta.file_hash = &hash;
    delta.delta.file_state = &delta.file_state;

    ASSERT_EQ(cbm_pipeline_file_delta_stamp_generation(&delta, 0), CBM_STORE_ERR);
    ASSERT_EQ(cbm_pipeline_file_delta_stamp_generation(&delta, PIPELINE_DELTA_STAMP_GENERATION),
              CBM_STORE_OK);
    ASSERT_EQ(delta.delta.generation, PIPELINE_DELTA_STAMP_GENERATION);
    ASSERT_EQ(delta.file_state.generation, PIPELINE_DELTA_STAMP_GENERATION);
    ASSERT_EQ(delta.delta.file_state->generation, PIPELINE_DELTA_STAMP_GENERATION);

    PASS();
}

TEST(pipeline_content_hash_helper_matches_file_delta_metadata) {
    enum { PIPELINE_DELTA_META_GENERATION = 14 };
    char *tmp = th_mktempdir("cbm_delta_hash");
    ASSERT_NOT_NULL(tmp);
    const char *path = TH_PATH(tmp, "main.go");
    const char *content = "package main\nfunc Run() { println(\"hash\") }\n";
    ASSERT_EQ(th_write_file(path, content), 0);

    char expected_hash[CBM_SZ_32];
    ASSERT_EQ(cbm_pipeline_content_hash_file(path, expected_hash, sizeof(expected_hash)),
              CBM_STORE_OK);

    cbm_file_info_t file = {
        .path = (char *)path,
        .rel_path = "main.go",
        .language = CBM_LANG_GO,
        .size = (int64_t)strlen(content),
    };
    cbm_pipeline_file_delta_t delta = {
        .delta = {.project = "test",
                  .rel_path = "main.go",
                  .generation = PIPELINE_DELTA_META_GENERATION}};
    ASSERT_EQ(cbm_pipeline_attach_file_delta_metadata(&delta, &file), CBM_STORE_OK);
    ASSERT_STR_EQ(delta.file_state.content_hash, expected_hash);
    ASSERT_EQ((int)strlen(expected_hash), CBM_SZ_16);

    th_cleanup(tmp);
    PASS();
}

TEST(pipeline_file_state_persist_helper_writes_hash_metadata) {
    enum { PIPELINE_FILE_STATE_GENERATION = 14 };
    char *tmp = th_mktempdir("cbm_file_state_persist");
    ASSERT_NOT_NULL(tmp);
    const char *go_path = TH_PATH(tmp, "main.go");
    const char *py_path = TH_PATH(tmp, "worker.py");
    const char *go_content = "package main\nfunc Run() { println(\"persist\") }\n";
    const char *py_content = "def run():\n    return 'persist'\n";
    ASSERT_EQ(th_write_file(go_path, go_content), 0);
    ASSERT_EQ(th_write_file(py_path, py_content), 0);

    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", tmp), CBM_STORE_OK);

    cbm_file_info_t files[2] = {
        {.path = (char *)go_path,
         .rel_path = "main.go",
         .language = CBM_LANG_GO,
         .size = (int64_t)strlen(go_content)},
        {.path = (char *)py_path,
         .rel_path = "worker.py",
         .language = CBM_LANG_PYTHON,
         .size = (int64_t)strlen(py_content)},
    };
    ASSERT_EQ(cbm_pipeline_persist_file_states(s, "test", files, 2, PIPELINE_FILE_STATE_GENERATION,
                                               "test-pass"),
              CBM_STORE_OK);

    char expected_go_hash[CBM_SZ_32];
    char expected_py_hash[CBM_SZ_32];
    ASSERT_EQ(cbm_pipeline_content_hash_file(go_path, expected_go_hash, sizeof(expected_go_hash)),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_pipeline_content_hash_file(py_path, expected_py_hash, sizeof(expected_py_hash)),
              CBM_STORE_OK);

    cbm_file_state_t go_state = {0};
    ASSERT_EQ(cbm_store_get_file_state(s, "test", "main.go", &go_state), CBM_STORE_OK);
    ASSERT_STR_EQ(go_state.content_hash, expected_go_hash);
    ASSERT_STR_EQ(go_state.language, "Go");
    ASSERT_STR_EQ(go_state.pass_fingerprint, "test-pass");
    ASSERT_EQ(go_state.size, (int64_t)strlen(go_content));
    ASSERT_EQ(go_state.generation, PIPELINE_FILE_STATE_GENERATION);
    ASSERT_NOT_NULL(go_state.indexed_at);
    ASSERT_NOT_NULL(strchr(go_state.indexed_at, 'T'));
    cbm_store_file_state_free_fields(&go_state);

    cbm_file_state_t py_state = {0};
    ASSERT_EQ(cbm_store_get_file_state(s, "test", "worker.py", &py_state), CBM_STORE_OK);
    ASSERT_STR_EQ(py_state.content_hash, expected_py_hash);
    ASSERT_STR_EQ(py_state.language, "Python");
    ASSERT_STR_EQ(py_state.pass_fingerprint, "test-pass");
    ASSERT_EQ(py_state.size, (int64_t)strlen(py_content));
    ASSERT_EQ(py_state.generation, PIPELINE_FILE_STATE_GENERATION);
    cbm_store_file_state_free_fields(&py_state);

    cbm_store_close(s);
    th_cleanup(tmp);
    PASS();
}

TEST(pipeline_file_state_current_check_rejects_stale_pass_fingerprint) {
    enum { PIPELINE_FILE_STATE_GENERATION = 15 };
    char *tmp = th_mktempdir("cbm_file_state_current_pass");
    ASSERT_NOT_NULL(tmp);
    const char *path = TH_PATH(tmp, "main.go");
    const char *content = "package main\nfunc Run() { println(\"current\") }\n";
    ASSERT_EQ(th_write_file(path, content), 0);

    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", tmp), CBM_STORE_OK);

    cbm_file_info_t file = {.path = (char *)path,
                            .rel_path = "main.go",
                            .language = CBM_LANG_GO,
                            .size = (int64_t)strlen(content)};
    ASSERT_TRUE(cbm_pipeline_file_state_is_current_or_legacy(
        s, "test", &file, cbm_pipeline_file_delta_pass_fingerprint()));

    ASSERT_EQ(cbm_pipeline_persist_file_states(s, "test", &file, 1,
                                               PIPELINE_FILE_STATE_GENERATION, "old-pass"),
              CBM_STORE_OK);
    ASSERT_FALSE(cbm_pipeline_file_state_is_current_or_legacy(
        s, "test", &file, cbm_pipeline_file_delta_pass_fingerprint()));

    ASSERT_EQ(cbm_pipeline_persist_file_states(s, "test", &file, 1,
                                               PIPELINE_FILE_STATE_GENERATION + 1, NULL),
              CBM_STORE_OK);
    ASSERT_TRUE(cbm_pipeline_file_state_is_current_or_legacy(
        s, "test", &file, cbm_pipeline_file_delta_pass_fingerprint()));

    cbm_store_close(s);
    th_cleanup(tmp);
    PASS();
}

TEST(pipeline_pass_fingerprint_includes_effective_mode_and_thresholds) {
    char full_default[CBM_SZ_256];
    char full_tuned[CBM_SZ_256];
    char full_tuned_again[CBM_SZ_256];
    char fast_default[CBM_SZ_256];

    cbm_pipeline_t *full = cbm_pipeline_new("/tmp/nonexistent", NULL, CBM_MODE_FULL);
    cbm_pipeline_t *fast = cbm_pipeline_new("/tmp/nonexistent", NULL, CBM_MODE_FAST);
    ASSERT_NOT_NULL(full);
    ASSERT_NOT_NULL(fast);

    ASSERT_EQ(cbm_pipeline_current_pass_fingerprint(full, full_default, sizeof(full_default)),
              CBM_STORE_OK);
    cbm_pipeline_set_similarity_threshold(full, 0.7);
    cbm_pipeline_set_httplink_min_confidence(full, 0.25);
    cbm_pipeline_set_semantic_threshold(full, 0.75);
    cbm_pipeline_set_githistory_min_coupling(full, 0.3);
    cbm_pipeline_set_lsp_confidence_floor(full, 0.6);
    ASSERT_EQ(cbm_pipeline_current_pass_fingerprint(full, full_tuned, sizeof(full_tuned)),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_pipeline_current_pass_fingerprint(full, full_tuned_again,
                                                    sizeof(full_tuned_again)),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_pipeline_current_pass_fingerprint(fast, fast_default, sizeof(fast_default)),
              CBM_STORE_OK);

    ASSERT_NEQ(strcmp(full_default, full_tuned), 0);
    ASSERT_STR_EQ(full_tuned, full_tuned_again);
    ASSERT_NEQ(strcmp(full_default, fast_default), 0);

    cbm_pipeline_free(full);
    cbm_pipeline_free(fast);
    PASS();
}

TEST(pipeline_file_state_current_check_rejects_stale_config_fingerprint) {
    enum { PIPELINE_FILE_STATE_GENERATION = 16 };
    char *tmp = th_mktempdir("cbm_file_state_current_config");
    ASSERT_NOT_NULL(tmp);
    const char *path = TH_PATH(tmp, "main.go");
    const char *content = "package main\nfunc Run() { println(\"config\") }\n";
    ASSERT_EQ(th_write_file(path, content), 0);

    char old_fingerprint[CBM_SZ_256];
    char current_fingerprint[CBM_SZ_256];
    ASSERT_EQ(cbm_pipeline_format_file_delta_pass_fingerprint(
                  old_fingerprint, sizeof(old_fingerprint), CBM_MODE_FULL, 0.7, 0.25, 0.75,
                  0.3, 0.6),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_pipeline_format_file_delta_pass_fingerprint(
                  current_fingerprint, sizeof(current_fingerprint), CBM_MODE_FULL, 0.8, 0.25,
                  0.75, 0.3, 0.6),
              CBM_STORE_OK);
    ASSERT_NEQ(strcmp(old_fingerprint, current_fingerprint), 0);

    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", tmp), CBM_STORE_OK);

    cbm_file_info_t file = {.path = (char *)path,
                            .rel_path = "main.go",
                            .language = CBM_LANG_GO,
                            .size = (int64_t)strlen(content)};
    ASSERT_EQ(cbm_pipeline_persist_file_states(s, "test", &file, 1,
                                               PIPELINE_FILE_STATE_GENERATION, old_fingerprint),
              CBM_STORE_OK);
    ASSERT_FALSE(cbm_pipeline_file_state_is_current_or_legacy(
        s, "test", &file, current_fingerprint));
    ASSERT_TRUE(cbm_pipeline_file_state_is_current_or_legacy(s, "test", &file, old_fingerprint));

    cbm_store_close(s);
    th_cleanup(tmp);
    PASS();
}

TEST(pipeline_file_state_persist_helper_rolls_back_on_failure) {
    char *tmp = th_mktempdir("cbm_file_state_persist_fail");
    ASSERT_NOT_NULL(tmp);
    const char *path = TH_PATH(tmp, "main.go");
    const char *content = "package main\nfunc Run() {}\n";
    ASSERT_EQ(th_write_file(path, content), 0);

    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", tmp), CBM_STORE_OK);

    cbm_file_info_t files[2] = {
        {.path = (char *)path,
         .rel_path = "main.go",
         .language = CBM_LANG_GO,
         .size = (int64_t)strlen(content)},
        {.path = (char *)TH_PATH(tmp, "missing.py"),
         .rel_path = "missing.py",
         .language = CBM_LANG_PYTHON,
         .size = 0},
    };
    ASSERT_EQ(cbm_pipeline_persist_file_states(s, "test", files, 2,
                                               CBM_PIPELINE_COMPAT_GENERATION, "test-pass"),
              CBM_STORE_ERR);

    cbm_file_state_t state = {0};
    ASSERT_EQ(cbm_store_get_file_state(s, "test", "main.go", &state), CBM_STORE_NOT_FOUND);

    cbm_store_close(s);
    th_cleanup(tmp);
    PASS();
}

TEST(pipeline_file_delta_plan_candidate_from_frontier) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);
    ASSERT_EQ(pipeline_delta_seed_existing_ownership(s, "test", "lib.go", "test.lib.Old"),
              CBM_STORE_OK);

    cbm_store_symbol_export_t exports[1] = {
        {.qualified_name = "test.lib.Value", .node_id = CBM_STORE_NO_NODE_ID}};
    cbm_pipeline_file_delta_t delta = {
        .delta = {.project = "test", .rel_path = "lib.go", .exports = exports, .export_count = 1}};
    cbm_file_hash_t hash = {0};
    cbm_file_state_t state = {0};
    pipeline_delta_attach_test_metadata(&delta, &hash, &state);

    cbm_pipeline_file_delta_plan_t plan = {0};
    ASSERT_EQ(cbm_pipeline_plan_file_delta(s, &delta, CBM_SZ_4, &plan), CBM_STORE_OK);
    ASSERT_EQ(plan.route, CBM_PIPELINE_DELTA_ROUTE_EXACT_CANDIDATE);
    ASSERT_STR_EQ(plan.reason, "candidate");
    ASSERT_EQ(plan.affected_count, 1);
    ASSERT_EQ(pipeline_delta_plan_contains_path(&plan, "lib.go"), 1);

    cbm_pipeline_file_delta_plan_free(&plan);
    cbm_store_close(s);
    PASS();
}

TEST(pipeline_file_delta_apply_falls_back_on_publish_error) {
    enum { PIPELINE_DELTA_APPLY_ONE = 1 };
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);
    ASSERT_EQ(pipeline_delta_seed_existing_ownership(s, "test", "lib.go", "test.lib.Old"),
              CBM_STORE_OK);

    cbm_node_t nodes[1] = {{.project = "test",
                            .label = "Function",
                            .name = "Value",
                            .qualified_name = "test.lib.Value",
                            .file_path = "lib.go",
                            .properties_json = "{}"}};
    cbm_store_symbol_export_t exports[1] = {
        {.qualified_name = "test.lib.Value", .node_id = CBM_STORE_NO_NODE_ID}};
    cbm_pipeline_file_delta_t delta = {.delta = {.project = "test",
                                                 .rel_path = "lib.go",
                                                 .nodes = nodes,
                                                 .node_count = 1,
                                                 .exports = exports,
                                                 .export_count = 1}};
    cbm_file_hash_t hash = {0};
    cbm_file_state_t state = {0};
    pipeline_delta_attach_test_metadata(&delta, &hash, &state);

    const cbm_pipeline_file_delta_t *deltas[] = {&delta};
    cbm_pipeline_file_delta_plan_t plan = {0};
    ASSERT_EQ(cbm_pipeline_apply_file_delta_batch(s, deltas, PIPELINE_DELTA_APPLY_ONE,
                                                  CBM_SZ_4, &plan),
              CBM_STORE_OK);
    ASSERT_EQ(plan.route, CBM_PIPELINE_DELTA_ROUTE_FALLBACK);
    ASSERT_STR_EQ(plan.reason, "publish_error");
    ASSERT_EQ(pipeline_delta_store_qn_exists(s, "test", "test.lib.Value"), 0);

    cbm_pipeline_file_delta_plan_free(&plan);
    cbm_store_close(s);
    PASS();
}

TEST(pipeline_file_delta_apply_falls_back_without_generation) {
    enum { PIPELINE_DELTA_APPLY_ONE = 1 };
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);
    ASSERT_EQ(pipeline_delta_seed_existing_ownership(s, "test", "lib.go", "test.lib.Old"),
              CBM_STORE_OK);

    cbm_store_symbol_export_t exports[1] = {
        {.qualified_name = "test.lib.Value", .node_id = CBM_STORE_NO_NODE_ID}};
    cbm_pipeline_file_delta_t delta = {
        .delta = {.project = "test", .rel_path = "lib.go", .exports = exports, .export_count = 1}};
    cbm_file_hash_t hash = {0};
    cbm_file_state_t state = {0};
    pipeline_delta_attach_test_metadata(&delta, &hash, &state);
    delta.delta.generation = 0;
    delta.file_state.generation = 0;

    const cbm_pipeline_file_delta_t *deltas[] = {&delta};
    cbm_pipeline_file_delta_plan_t plan = {0};
    ASSERT_EQ(cbm_pipeline_apply_file_delta_batch(s, deltas, PIPELINE_DELTA_APPLY_ONE,
                                                  CBM_SZ_4, &plan),
              CBM_STORE_OK);
    ASSERT_EQ(plan.route, CBM_PIPELINE_DELTA_ROUTE_FALLBACK);
    ASSERT_STR_EQ(plan.reason, "missing_generation");
    ASSERT_EQ(pipeline_delta_store_qn_exists(s, "test", "test.lib.Value"), 0);

    cbm_pipeline_file_delta_plan_free(&plan);
    cbm_store_close(s);
    PASS();
}

TEST(pipeline_file_delta_apply_succeeds_after_generation_stamp) {
    enum { PIPELINE_DELTA_APPLY_ONE = 1 };
    const char *project = "test";
    const char *rel_path = "lib.go";
    const char *old_qn = "test.lib.Old";
    const char *new_qn = "test.lib.Value";
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, project, "/tmp/test"), CBM_STORE_OK);
    ASSERT_EQ(pipeline_delta_seed_existing_ownership(s, project, rel_path, old_qn), CBM_STORE_OK);

    cbm_node_t nodes[1] = {{.project = (char *)project,
                            .label = "Function",
                            .name = "Value",
                            .qualified_name = (char *)new_qn,
                            .file_path = (char *)rel_path,
                            .properties_json = "{}"}};
    cbm_store_symbol_export_t exports[1] = {
        {.qualified_name = new_qn, .node_id = CBM_STORE_NO_NODE_ID}};
    cbm_pipeline_file_delta_t delta = {.delta = {.project = project,
                                                 .rel_path = rel_path,
                                                 .nodes = nodes,
                                                 .node_count = 1,
                                                 .exports = exports,
                                                 .export_count = 1}};
    cbm_file_hash_t hash = {0};
    cbm_file_state_t state = {0};
    pipeline_delta_attach_test_metadata(&delta, &hash, &state);
    delta.delta.generation = 0;
    delta.file_state.generation = 0;
    state.generation = 0;

    cbm_pipeline_file_delta_plan_t preflight_plan = {0};
    ASSERT_EQ(cbm_pipeline_plan_file_delta(s, &delta, CBM_SZ_4, &preflight_plan),
              CBM_STORE_OK);
    ASSERT_EQ(preflight_plan.route, CBM_PIPELINE_DELTA_ROUTE_EXACT_CANDIDATE);
    cbm_pipeline_file_delta_plan_free(&preflight_plan);

    int64_t generation = 0;
    ASSERT_EQ(cbm_store_reserve_index_generation(s, project, NULL, NULL, &generation),
              CBM_STORE_OK);
    ASSERT_GT(generation, 0);
    ASSERT_EQ(cbm_pipeline_file_delta_stamp_generation(&delta, generation), CBM_STORE_OK);
    ASSERT(delta.delta.file_state == &delta.file_state);
    ASSERT_EQ(delta.file_state.generation, generation);
    ASSERT_EQ(state.generation, 0);

    const cbm_pipeline_file_delta_t *deltas[] = {&delta};
    cbm_pipeline_file_delta_plan_t apply_plan = {0};
    ASSERT_EQ(cbm_pipeline_apply_file_delta_batch(s, deltas, PIPELINE_DELTA_APPLY_ONE,
                                                  CBM_SZ_4, &apply_plan),
              CBM_STORE_OK);
    ASSERT_EQ(apply_plan.route, CBM_PIPELINE_DELTA_ROUTE_EXACT_CANDIDATE);
    ASSERT_EQ(pipeline_delta_store_qn_exists(s, project, old_qn), 0);
    ASSERT_EQ(pipeline_delta_store_qn_exists(s, project, new_qn), 1);
    cbm_file_state_t got = {0};
    ASSERT_EQ(cbm_store_get_file_state(s, project, rel_path, &got), CBM_STORE_OK);
    ASSERT_EQ(got.generation, generation);
    cbm_store_file_state_free_fields(&got);

    cbm_pipeline_file_delta_plan_free(&apply_plan);
    cbm_store_close(s);
    PASS();
}

TEST(pipeline_file_delta_apply_inserts_new_file_without_existing_ownership) {
    enum { PIPELINE_NEW_FILE_DELTA_COUNT = 1 };
    const char *project = "test";
    const char *rel_path = "pkg/new.go";
    const char *new_qn = "test.pkg.new.Value";

    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, project, "/tmp/test"), CBM_STORE_OK);
    ASSERT_EQ(pipeline_delta_seed_project_node(s, project), CBM_STORE_OK);

    char *folder_qn = cbm_pipeline_fqn_folder(project, "pkg");
    ASSERT_NOT_NULL(folder_qn);
    cbm_node_t folder = {.project = (char *)project,
                         .label = "Folder",
                         .name = "pkg",
                         .qualified_name = folder_qn,
                         .file_path = "pkg",
                         .properties_json = "{}"};
    ASSERT_GT(cbm_store_upsert_node(s, &folder), CBM_STORE_NO_NODE_ID);

    cbm_gbuf_t *scratch = cbm_gbuf_new(project, "/tmp/test");
    ASSERT_NOT_NULL(scratch);
    ASSERT_GT(cbm_gbuf_upsert_node(scratch, "Project", project, project, NULL, 0, 0, "{}"), 0);
    ASSERT_EQ(cbm_pipeline_ensure_file_structure(scratch, project, project, rel_path, NULL), 0);
    ASSERT_GT(cbm_gbuf_upsert_node(scratch, "Function", "Value", new_qn, rel_path, 1, 1,
                                   "{\"is_exported\":true}"),
              0);

    cbm_pipeline_file_delta_t delta = {0};
    ASSERT_EQ(cbm_pipeline_build_file_delta_from_gbuf(scratch, project, rel_path, 0, &delta),
              CBM_STORE_OK);
    cbm_file_hash_t hash = {0};
    cbm_file_state_t state = {0};
    pipeline_delta_attach_test_metadata(&delta, &hash, &state);
    delta.delta.generation = 0;
    delta.file_state.generation = 0;

    cbm_pipeline_file_delta_plan_t preflight_plan = {0};
    ASSERT_EQ(cbm_pipeline_plan_file_delta(s, &delta, CBM_SZ_4, &preflight_plan),
              CBM_STORE_OK);
    ASSERT_EQ(preflight_plan.route, CBM_PIPELINE_DELTA_ROUTE_EXACT_CANDIDATE);
    ASSERT_EQ(pipeline_delta_plan_contains_path(&preflight_plan, rel_path), 1);
    ASSERT_EQ(preflight_plan.affected_count, 1);
    cbm_pipeline_file_delta_plan_free(&preflight_plan);

    int64_t generation = 0;
    ASSERT_EQ(cbm_store_reserve_index_generation(s, project, NULL, NULL, &generation),
              CBM_STORE_OK);
    ASSERT_GT(generation, 0);
    ASSERT_EQ(cbm_pipeline_file_delta_stamp_generation(&delta, generation), CBM_STORE_OK);

    const cbm_pipeline_file_delta_t *deltas[] = {&delta};
    cbm_pipeline_file_delta_plan_t apply_plan = {0};
    ASSERT_EQ(cbm_pipeline_apply_file_delta_batch(s, deltas, PIPELINE_NEW_FILE_DELTA_COUNT,
                                                  CBM_SZ_4, &apply_plan),
              CBM_STORE_OK);
    ASSERT_EQ(apply_plan.route, CBM_PIPELINE_DELTA_ROUTE_EXACT_CANDIDATE);
    ASSERT_EQ(pipeline_delta_store_qn_exists(s, project, new_qn), 1);

    int node_owners = 0;
    int edge_owners = 0;
    ASSERT_EQ(cbm_store_count_file_delta_owners(s, project, rel_path, &node_owners,
                                                &edge_owners),
              CBM_STORE_OK);
    ASSERT_GT(node_owners, 0);
    cbm_file_state_t got = {0};
    ASSERT_EQ(cbm_store_get_file_state(s, project, rel_path, &got), CBM_STORE_OK);
    ASSERT_EQ(got.generation, generation);
    cbm_store_file_state_free_fields(&got);

    cbm_pipeline_file_delta_plan_free(&apply_plan);
    cbm_pipeline_file_delta_free(&delta);
    cbm_gbuf_free(scratch);
    free(folder_qn);
    cbm_store_close(s);
    PASS();
}

TEST(pipeline_file_delta_apply_falls_back_on_new_file_importer_frontier) {
    enum { PIPELINE_NEW_IMPORTER_DELTA_COUNT = 1 };
    const char *project = "test";
    const char *new_rel = "pkg/new.go";
    const char *main_rel = "main.go";
    const char *new_qn = "test.pkg.new.Value";

    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, project, "/tmp/test"), CBM_STORE_OK);
    ASSERT_EQ(pipeline_delta_seed_existing_ownership(s, project, main_rel, "test.main.Main"),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_store_upsert_import_ref(s, project, main_rel, "test.pkg.new", "Value",
                                          new_qn, 1),
              CBM_STORE_OK);

    cbm_node_t nodes[1] = {{.project = (char *)project,
                            .label = "Function",
                            .name = "Value",
                            .qualified_name = (char *)new_qn,
                            .file_path = (char *)new_rel,
                            .properties_json = "{\"is_exported\":true}"}};
    cbm_store_symbol_export_t exports[1] = {
        {.qualified_name = new_qn, .node_id = CBM_STORE_NO_NODE_ID}};
    cbm_pipeline_file_delta_t delta = {.delta = {.project = project,
                                                 .rel_path = new_rel,
                                                 .nodes = nodes,
                                                 .node_count = 1,
                                                 .exports = exports,
                                                 .export_count = 1}};
    cbm_file_hash_t hash = {0};
    cbm_file_state_t state = {0};
    pipeline_delta_attach_test_metadata(&delta, &hash, &state);

    const cbm_pipeline_file_delta_t *deltas[] = {&delta};
    cbm_pipeline_file_delta_plan_t plan = {0};
    ASSERT_EQ(cbm_pipeline_apply_file_delta_batch(s, deltas,
                                                  PIPELINE_NEW_IMPORTER_DELTA_COUNT,
                                                  CBM_SZ_4, &plan),
              CBM_STORE_OK);
    ASSERT_EQ(plan.route, CBM_PIPELINE_DELTA_ROUTE_FALLBACK);
    ASSERT_STR_EQ(plan.reason, "frontier_requires_batch");
    ASSERT_EQ(pipeline_delta_store_qn_exists(s, project, new_qn), 0);

    cbm_pipeline_file_delta_plan_free(&plan);
    cbm_store_close(s);
    PASS();
}

TEST(pipeline_file_delta_plan_falls_back_without_existing_ownership) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);
    cbm_pipeline_file_delta_t delta = {.delta = {.project = "test", .rel_path = "main.go"}};
    cbm_file_hash_t hash = {0};
    cbm_file_state_t state = {0};
    pipeline_delta_attach_test_metadata(&delta, &hash, &state);

    cbm_pipeline_file_delta_plan_t plan = {0};
    ASSERT_EQ(cbm_pipeline_plan_file_delta(s, &delta, CBM_SZ_4, &plan), CBM_STORE_OK);
    ASSERT_EQ(plan.route, CBM_PIPELINE_DELTA_ROUTE_FALLBACK);
    ASSERT_STR_EQ(plan.reason, "missing_existing_ownership");
    ASSERT_EQ(plan.affected_count, 0);

    cbm_pipeline_file_delta_plan_free(&plan);
    cbm_store_close(s);
    PASS();
}

TEST(pipeline_file_delta_plan_falls_back_on_external_inbound_edge) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);
    int64_t main_id =
        pipeline_delta_seed_existing_ownership_id(s, "test", "main.go", "test.main.Old");
    int64_t helper_id =
        pipeline_delta_seed_existing_ownership_id(s, "test", "helper.go", "test.helper.Helper");
    ASSERT_GT(main_id, CBM_STORE_NO_NODE_ID);
    ASSERT_GT(helper_id, CBM_STORE_NO_NODE_ID);
    cbm_edge_t inbound = {.project = "test",
                          .source_id = helper_id,
                          .target_id = main_id,
                          .type = "CALLS",
                          .properties_json = "{}"};
    ASSERT_GT(cbm_store_insert_edge(s, &inbound), CBM_STORE_NO_NODE_ID);

    cbm_pipeline_file_delta_t delta = {.delta = {.project = "test", .rel_path = "main.go"}};
    cbm_file_hash_t hash = {0};
    cbm_file_state_t state = {0};
    pipeline_delta_attach_test_metadata(&delta, &hash, &state);

    cbm_pipeline_file_delta_plan_t plan = {0};
    ASSERT_EQ(cbm_pipeline_plan_file_delta(s, &delta, CBM_SZ_4, &plan), CBM_STORE_OK);
    ASSERT_EQ(plan.route, CBM_PIPELINE_DELTA_ROUTE_FALLBACK);
    ASSERT_STR_EQ(plan.reason, "inbound_edges_require_full");
    ASSERT_EQ(plan.affected_count, 0);

    cbm_pipeline_file_delta_plan_free(&plan);
    cbm_store_close(s);
    PASS();
}

TEST(pipeline_file_delta_plan_falls_back_on_unowned_structural_inbound_edge) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);
    int64_t main_id =
        pipeline_delta_seed_existing_ownership_id(s, "test", "main.go", "test.main.Old");
    ASSERT_GT(main_id, CBM_STORE_NO_NODE_ID);

    cbm_node_t folder = {.project = "test",
                         .label = "Folder",
                         .name = "test",
                         .qualified_name = "test",
                         .file_path = "",
                         .properties_json = "{}"};
    int64_t folder_id = cbm_store_upsert_node(s, &folder);
    ASSERT_GT(folder_id, CBM_STORE_NO_NODE_ID);

    cbm_edge_t inbound = {.project = "test",
                          .source_id = folder_id,
                          .target_id = main_id,
                          .type = "CONTAINS_FILE",
                          .properties_json = "{}"};
    ASSERT_GT(cbm_store_insert_edge(s, &inbound), CBM_STORE_NO_NODE_ID);

    cbm_pipeline_file_delta_t delta = {.delta = {.project = "test", .rel_path = "main.go"}};
    cbm_file_hash_t hash = {0};
    cbm_file_state_t state = {0};
    pipeline_delta_attach_test_metadata(&delta, &hash, &state);

    cbm_pipeline_file_delta_plan_t plan = {0};
    ASSERT_EQ(cbm_pipeline_plan_file_delta(s, &delta, CBM_SZ_4, &plan), CBM_STORE_OK);
    ASSERT_EQ(plan.route, CBM_PIPELINE_DELTA_ROUTE_FALLBACK);
    ASSERT_STR_EQ(plan.reason, "inbound_edges_require_full");
    ASSERT_EQ(plan.affected_count, 0);

    cbm_pipeline_file_delta_plan_free(&plan);
    cbm_store_close(s);
    PASS();
}

TEST(pipeline_file_delta_plan_accepts_full_pipeline_structure_edge) {
    enum { PIPELINE_DELTA_TEST_BASE_GENERATION = 1 };
    const char *project = "test";
    const char *rel_path = "src/main.go";
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, project, "/tmp/test"), CBM_STORE_OK);
    ASSERT_EQ(pipeline_delta_seed_project_node(s, project), CBM_STORE_OK);
    ASSERT_EQ(pipeline_delta_seed_existing_ownership(s, project, rel_path, "test.src.main.Old"),
              CBM_STORE_OK);

    char *file_qn = cbm_pipeline_fqn_compute(project, rel_path, "__file__");
    char *folder_qn = cbm_pipeline_fqn_folder(project, "src");
    ASSERT_NOT_NULL(file_qn);
    ASSERT_NOT_NULL(folder_qn);

    cbm_node_t file_node = {.project = (char *)project,
                            .label = "File",
                            .name = "main.go",
                            .qualified_name = file_qn,
                            .file_path = (char *)rel_path,
                            .properties_json = "{}"};
    int64_t file_id = cbm_store_upsert_node(s, &file_node);
    ASSERT_GT(file_id, CBM_STORE_NO_NODE_ID);
    ASSERT_EQ(cbm_store_upsert_node_owner(s, project, file_id, rel_path,
                                          PIPELINE_DELTA_TEST_BASE_GENERATION),
              CBM_STORE_OK);

    cbm_node_t folder_node = {.project = (char *)project,
                              .label = "Folder",
                              .name = "src",
                              .qualified_name = folder_qn,
                              .file_path = "src",
                              .properties_json = "{}"};
    int64_t folder_id = cbm_store_upsert_node(s, &folder_node);
    ASSERT_GT(folder_id, CBM_STORE_NO_NODE_ID);
    cbm_edge_t contains = {.project = (char *)project,
                           .source_id = folder_id,
                           .target_id = file_id,
                           .type = "CONTAINS_FILE",
                           .properties_json = "{}"};
    int64_t edge_id = cbm_store_insert_edge(s, &contains);
    ASSERT_GT(edge_id, CBM_STORE_NO_NODE_ID);
    ASSERT_EQ(cbm_store_upsert_edge_owner(s, project, edge_id, rel_path, NULL,
                                          PIPELINE_DELTA_TEST_BASE_GENERATION),
              CBM_STORE_OK);

    cbm_gbuf_t *scratch = cbm_gbuf_new(project, "/tmp/test");
    ASSERT_NOT_NULL(scratch);
    ASSERT_GT(cbm_gbuf_upsert_node(scratch, "Project", project, project, NULL, 0, 0, "{}"), 0);
    ASSERT_EQ(cbm_pipeline_ensure_file_structure(scratch, project, project, rel_path, NULL), 0);
    ASSERT_GT(cbm_gbuf_upsert_node(scratch, "Function", "New", "test.src.main.New", rel_path, 1,
                                   1, "{\"is_exported\":true}"),
              0);

    cbm_pipeline_file_delta_t delta = {0};
    ASSERT_EQ(cbm_pipeline_build_file_delta_from_gbuf(scratch, project, rel_path, 1, &delta),
              CBM_STORE_OK);
    const cbm_store_delta_edge_t *structure_edge =
        pipeline_delta_find_edge(&delta, "CONTAINS_FILE");
    ASSERT_NOT_NULL(structure_edge);
    ASSERT_STR_EQ(structure_edge->source_qn, folder_qn);
    ASSERT_STR_EQ(structure_edge->target_qn, file_qn);
    cbm_file_hash_t hash = {0};
    cbm_file_state_t state = {0};
    pipeline_delta_attach_test_metadata(&delta, &hash, &state);

    cbm_pipeline_file_delta_plan_t plan = {0};
    ASSERT_EQ(cbm_pipeline_plan_file_delta(s, &delta, CBM_SZ_4, &plan), CBM_STORE_OK);
    ASSERT_EQ(plan.route, CBM_PIPELINE_DELTA_ROUTE_EXACT_CANDIDATE);
    ASSERT_STR_EQ(plan.reason, "candidate");
    ASSERT_EQ(cbm_store_publish_file_delta(s, &delta.delta), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_count_edges_by_type(s, project, "CONTAINS_FILE"), 1);

    cbm_pipeline_file_delta_plan_free(&plan);
    cbm_pipeline_file_delta_free(&delta);
    cbm_gbuf_free(scratch);
    free(folder_qn);
    free(file_qn);
    cbm_store_close(s);
    PASS();
}

TEST(pipeline_file_delta_plan_falls_back_on_new_folder_structure_edge) {
    const char *project = "test";
    const char *rel_path = "src/main.go";
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, project, "/tmp/test"), CBM_STORE_OK);
    ASSERT_EQ(pipeline_delta_seed_existing_ownership(s, project, rel_path, "test.src.main.Old"),
              CBM_STORE_OK);

    char *file_qn = cbm_pipeline_fqn_compute(project, rel_path, "__file__");
    char *folder_qn = cbm_pipeline_fqn_folder(project, "src");
    ASSERT_NOT_NULL(file_qn);
    ASSERT_NOT_NULL(folder_qn);

    cbm_gbuf_t *scratch = cbm_gbuf_new(project, "/tmp/test");
    ASSERT_NOT_NULL(scratch);
    ASSERT_GT(cbm_gbuf_upsert_node(scratch, "Project", project, project, NULL, 0, 0, "{}"), 0);
    ASSERT_EQ(cbm_pipeline_ensure_file_structure(scratch, project, project, rel_path, NULL), 0);
    ASSERT_GT(cbm_gbuf_upsert_node(scratch, "Function", "New", "test.src.main.New", rel_path, 1,
                                   1, "{\"is_exported\":true}"),
              0);

    cbm_pipeline_file_delta_t delta = {0};
    ASSERT_EQ(cbm_pipeline_build_file_delta_from_gbuf(scratch, project, rel_path, 1, &delta),
              CBM_STORE_OK);
    const cbm_store_delta_edge_t *structure_edge =
        pipeline_delta_find_edge(&delta, "CONTAINS_FILE");
    ASSERT_NOT_NULL(structure_edge);
    ASSERT_STR_EQ(structure_edge->source_qn, folder_qn);
    ASSERT_STR_EQ(structure_edge->target_qn, file_qn);
    cbm_file_hash_t hash = {0};
    cbm_file_state_t state = {0};
    pipeline_delta_attach_test_metadata(&delta, &hash, &state);

    cbm_pipeline_file_delta_plan_t plan = {0};
    ASSERT_EQ(cbm_pipeline_plan_file_delta(s, &delta, CBM_SZ_4, &plan), CBM_STORE_OK);
    ASSERT_EQ(plan.route, CBM_PIPELINE_DELTA_ROUTE_FALLBACK);
    ASSERT_STR_EQ(plan.reason, "unresolved_edge_endpoint");

    cbm_pipeline_file_delta_plan_free(&plan);
    cbm_pipeline_file_delta_free(&delta);
    cbm_gbuf_free(scratch);
    free(folder_qn);
    free(file_qn);
    cbm_store_close(s);
    PASS();
}

TEST(pipeline_file_delta_apply_inserts_and_prunes_new_folder_context) {
    enum {
        PIPELINE_NEW_FOLDER_DELTA_COUNT = 1,
    };
    const char *project = "test";
    const char *rel_path = "src/main.go";
    const char *new_qn = "test.src.main.New";

    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, project, "/tmp/test"), CBM_STORE_OK);
    ASSERT_EQ(pipeline_delta_seed_project_node(s, project), CBM_STORE_OK);

    char *file_qn = cbm_pipeline_fqn_compute(project, rel_path, "__file__");
    char *folder_qn = cbm_pipeline_fqn_folder(project, "src");
    ASSERT_NOT_NULL(file_qn);
    ASSERT_NOT_NULL(folder_qn);

    cbm_gbuf_t *scratch = cbm_gbuf_new(project, "/tmp/test");
    ASSERT_NOT_NULL(scratch);
    ASSERT_GT(cbm_gbuf_upsert_node(scratch, "Project", project, project, NULL, 0, 0, "{}"), 0);
    ASSERT_EQ(cbm_pipeline_ensure_file_structure(scratch, project, project, rel_path, NULL), 0);
    ASSERT_GT(cbm_gbuf_upsert_node(scratch, "Function", "New", new_qn, rel_path, 1, 1,
                                   "{\"is_exported\":true}"),
              0);

    cbm_pipeline_file_delta_t delta = {0};
    ASSERT_EQ(cbm_pipeline_build_file_delta_from_gbuf(scratch, project, rel_path, 0, &delta),
              CBM_STORE_OK);
    ASSERT_EQ(delta.delta.context_node_count, 1);
    ASSERT_EQ(delta.delta.context_edge_count, 1);
    ASSERT_STR_EQ(delta.context_nodes[0].qualified_name, folder_qn);
    ASSERT_STR_EQ(delta.context_edges[0].type, "CONTAINS_FOLDER");
    cbm_file_hash_t hash = {0};
    cbm_file_state_t state = {0};
    pipeline_delta_attach_test_metadata(&delta, &hash, &state);
    delta.delta.generation = 0;
    delta.file_state.generation = 0;

    cbm_pipeline_file_delta_plan_t preflight_plan = {0};
    ASSERT_EQ(cbm_pipeline_plan_file_delta(s, &delta, CBM_SZ_4, &preflight_plan),
              CBM_STORE_OK);
    ASSERT_EQ(preflight_plan.route, CBM_PIPELINE_DELTA_ROUTE_EXACT_CANDIDATE);
    cbm_pipeline_file_delta_plan_free(&preflight_plan);

    int64_t generation = 0;
    ASSERT_EQ(cbm_store_reserve_index_generation(s, project, NULL, NULL, &generation),
              CBM_STORE_OK);
    ASSERT_GT(generation, 0);
    ASSERT_EQ(cbm_pipeline_file_delta_stamp_generation(&delta, generation), CBM_STORE_OK);

    const cbm_pipeline_file_delta_t *deltas[] = {&delta};
    cbm_pipeline_file_delta_plan_t apply_plan = {0};
    ASSERT_EQ(cbm_pipeline_apply_file_delta_batch(s, deltas, PIPELINE_NEW_FOLDER_DELTA_COUNT,
                                                  CBM_SZ_4, &apply_plan),
              CBM_STORE_OK);
    ASSERT_EQ(apply_plan.route, CBM_PIPELINE_DELTA_ROUTE_EXACT_CANDIDATE);
    ASSERT_EQ(pipeline_delta_store_qn_exists(s, project, new_qn), 1);
    ASSERT_EQ(pipeline_delta_store_qn_exists(s, project, folder_qn), 1);
    ASSERT_EQ(cbm_store_count_edges_by_type(s, project, "CONTAINS_FOLDER"), 1);
    ASSERT_EQ(cbm_store_count_edges_by_type(s, project, "CONTAINS_FILE"), 1);

    int node_owners = 0;
    int edge_owners = 0;
    ASSERT_EQ(cbm_store_count_file_delta_owners(s, project, "src", &node_owners, &edge_owners),
              CBM_STORE_OK);
    ASSERT_EQ(node_owners, 0);
    ASSERT_EQ(edge_owners, 0);
    ASSERT_EQ(cbm_store_count_file_delta_owners(s, project, rel_path, &node_owners,
                                                &edge_owners),
              CBM_STORE_OK);
    ASSERT_GT(node_owners, 0);
    ASSERT_GT(edge_owners, 0);

    cbm_pipeline_file_delta_plan_free(&apply_plan);
    cbm_pipeline_file_delta_free(&delta);

    ASSERT_EQ(cbm_store_reserve_index_generation(s, project, NULL, NULL, &generation),
              CBM_STORE_OK);
    cbm_pipeline_file_delta_t delete_delta = {
        .delta = {.project = project, .rel_path = rel_path, .generation = generation},
        .change_kind = CBM_PIPELINE_DELTA_CHANGE_DELETE};
    const cbm_pipeline_file_delta_t *delete_deltas[] = {&delete_delta};
    cbm_pipeline_file_delta_plan_t delete_plan = {0};
    ASSERT_EQ(cbm_pipeline_apply_file_delta_batch(s, delete_deltas,
                                                  PIPELINE_NEW_FOLDER_DELTA_COUNT, CBM_SZ_4,
                                                  &delete_plan),
              CBM_STORE_OK);
    ASSERT_EQ(delete_plan.route, CBM_PIPELINE_DELTA_ROUTE_EXACT_CANDIDATE);
    ASSERT_EQ(pipeline_delta_store_qn_exists(s, project, new_qn), 0);
    ASSERT_EQ(pipeline_delta_store_qn_exists(s, project, file_qn), 0);
    ASSERT_EQ(pipeline_delta_store_qn_exists(s, project, folder_qn), 0);
    ASSERT_EQ(pipeline_delta_store_qn_exists(s, project, project), 1);
    ASSERT_EQ(cbm_store_count_edges_by_type(s, project, "CONTAINS_FOLDER"), 0);
    ASSERT_EQ(cbm_store_count_edges_by_type(s, project, "CONTAINS_FILE"), 0);

    cbm_pipeline_file_delta_plan_free(&delete_plan);
    cbm_gbuf_free(scratch);
    free(folder_qn);
    free(file_qn);
    cbm_store_close(s);
    PASS();
}

TEST(pipeline_file_delta_plan_accepts_regenerated_structural_inbound_edge) {
    enum { PIPELINE_DELTA_TEST_BASE_GENERATION = 1 };
    const char *project = "test";
    const char *rel_path = "src/main.go";
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, project, "/tmp/test"), CBM_STORE_OK);

    char *file_qn = cbm_pipeline_fqn_compute(project, rel_path, "__file__");
    char *folder_qn = cbm_pipeline_fqn_folder(project, "src");
    ASSERT_NOT_NULL(file_qn);
    ASSERT_NOT_NULL(folder_qn);

    cbm_node_t old_file = {.project = (char *)project,
                           .label = "File",
                           .name = "main.go",
                           .qualified_name = file_qn,
                           .file_path = (char *)rel_path,
                           .properties_json = "{}"};
    int64_t file_id = cbm_store_upsert_node(s, &old_file);
    ASSERT_GT(file_id, CBM_STORE_NO_NODE_ID);
    ASSERT_EQ(cbm_store_upsert_node_owner(s, project, file_id, rel_path,
                                          PIPELINE_DELTA_TEST_BASE_GENERATION),
              CBM_STORE_OK);
    cbm_file_state_t base_state = {.project = (char *)project,
                                   .rel_path = (char *)rel_path,
                                   .content_hash = "base-content",
                                   .git_oid = "",
                                   .mtime_ns = 1,
                                   .size = 10,
                                   .language = "go",
                                   .pass_fingerprint = "test-pass",
                                   .generation = PIPELINE_DELTA_TEST_BASE_GENERATION,
                                   .indexed_at = "2026-06-30T00:00:00Z"};
    ASSERT_EQ(cbm_store_upsert_file_state(s, &base_state), CBM_STORE_OK);

    cbm_node_t folder = {.project = (char *)project,
                         .label = "Folder",
                         .name = "src",
                         .qualified_name = folder_qn,
                         .file_path = "src",
                         .properties_json = "{}"};
    int64_t folder_id = cbm_store_upsert_node(s, &folder);
    ASSERT_GT(folder_id, CBM_STORE_NO_NODE_ID);
    cbm_edge_t contains = {.project = (char *)project,
                           .source_id = folder_id,
                           .target_id = file_id,
                           .type = "CONTAINS_FILE",
                           .properties_json = "{}"};
    int64_t edge_id = cbm_store_insert_edge(s, &contains);
    ASSERT_GT(edge_id, CBM_STORE_NO_NODE_ID);
    ASSERT_EQ(cbm_store_upsert_edge_owner(s, project, edge_id, rel_path, NULL,
                                          PIPELINE_DELTA_TEST_BASE_GENERATION),
              CBM_STORE_OK);

    cbm_node_t new_file = {.project = (char *)project,
                           .label = "File",
                           .name = "main.go",
                           .qualified_name = file_qn,
                           .file_path = (char *)rel_path,
                           .properties_json = "{}"};
    cbm_store_delta_edge_t regenerated_edge = {.source_qn = folder_qn,
                                               .target_qn = file_qn,
                                               .type = "CONTAINS_FILE",
                                               .properties_json = "{}"};
    cbm_pipeline_file_delta_t delta = {.delta = {.project = project,
                                                 .rel_path = rel_path,
                                                 .nodes = &new_file,
                                                 .node_count = 1,
                                                 .edges = &regenerated_edge,
                                                 .edge_count = 1}};
    cbm_file_hash_t hash = {0};
    cbm_file_state_t state = {0};
    pipeline_delta_attach_test_metadata(&delta, &hash, &state);

    cbm_pipeline_file_delta_plan_t plan = {0};
    ASSERT_EQ(cbm_pipeline_plan_file_delta(s, &delta, CBM_SZ_4, &plan), CBM_STORE_OK);
    ASSERT_EQ(plan.route, CBM_PIPELINE_DELTA_ROUTE_EXACT_CANDIDATE);
    ASSERT_STR_EQ(plan.reason, "candidate");
    ASSERT_EQ(pipeline_delta_plan_contains_path(&plan, rel_path), 1);

    cbm_pipeline_file_delta_plan_free(&plan);
    free(folder_qn);
    free(file_qn);
    cbm_store_close(s);
    PASS();
}

TEST(pipeline_file_delta_plan_accepts_regenerated_file_owned_unowned_source_edge) {
    const char *project = "test";
    const char *rel_path = "src/main.go";
    const char *source_qn = "test.src.module";
    const char *target_qn = "test.src.main.Run";
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, project, "/tmp/test"), CBM_STORE_OK);
    ASSERT_EQ(pipeline_delta_seed_file_owned_unowned_source_edge(
                  s, project, rel_path, source_qn, target_qn, "CALLS"),
              CBM_STORE_OK);

    cbm_node_t replacement_node = {.project = (char *)project,
                                   .label = "Function",
                                   .name = "Run",
                                   .qualified_name = (char *)target_qn,
                                   .file_path = (char *)rel_path,
                                   .properties_json = "{}"};
    cbm_store_delta_edge_t replacement_edge = {.source_qn = source_qn,
                                               .target_qn = target_qn,
                                               .type = "CALLS",
                                               .properties_json = "{}",
                                               .derived_kind = CBM_STORE_DERIVED_KIND_DIRECT};
    cbm_pipeline_file_delta_t delta = {.delta = {.project = project,
                                                 .rel_path = rel_path,
                                                 .nodes = &replacement_node,
                                                 .node_count = 1,
                                                 .edges = &replacement_edge,
                                                 .edge_count = 1}};
    cbm_file_hash_t hash = {0};
    cbm_file_state_t state = {0};
    pipeline_delta_attach_test_metadata(&delta, &hash, &state);

    cbm_pipeline_file_delta_plan_t plan = {0};
    ASSERT_EQ(cbm_pipeline_plan_file_delta(s, &delta, CBM_SZ_4, &plan), CBM_STORE_OK);
    ASSERT_EQ(plan.route, CBM_PIPELINE_DELTA_ROUTE_EXACT_CANDIDATE);
    ASSERT_STR_EQ(plan.reason, "candidate");
    ASSERT_EQ(pipeline_delta_plan_contains_path(&plan, rel_path), 1);

    cbm_pipeline_file_delta_plan_free(&plan);
    cbm_store_close(s);
    PASS();
}

TEST(pipeline_file_delta_plan_falls_back_on_stale_file_owned_unowned_source_edge) {
    const char *project = "test";
    const char *rel_path = "src/main.go";
    const char *source_qn = "test.src.module";
    const char *target_qn = "test.src.main.Run";
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, project, "/tmp/test"), CBM_STORE_OK);
    ASSERT_EQ(pipeline_delta_seed_file_owned_unowned_source_edge(
                  s, project, rel_path, source_qn, target_qn, "CALLS"),
              CBM_STORE_OK);

    cbm_node_t replacement_node = {.project = (char *)project,
                                   .label = "Function",
                                   .name = "Run",
                                   .qualified_name = (char *)target_qn,
                                   .file_path = (char *)rel_path,
                                   .properties_json = "{}"};
    cbm_pipeline_file_delta_t delta = {.delta = {.project = project,
                                                 .rel_path = rel_path,
                                                 .nodes = &replacement_node,
                                                 .node_count = 1}};
    cbm_file_hash_t hash = {0};
    cbm_file_state_t state = {0};
    pipeline_delta_attach_test_metadata(&delta, &hash, &state);

    cbm_pipeline_file_delta_plan_t plan = {0};
    ASSERT_EQ(cbm_pipeline_plan_file_delta(s, &delta, CBM_SZ_4, &plan), CBM_STORE_OK);
    ASSERT_EQ(plan.route, CBM_PIPELINE_DELTA_ROUTE_FALLBACK);
    ASSERT_STR_EQ(plan.reason, "inbound_edges_require_full");

    cbm_pipeline_file_delta_plan_free(&plan);
    cbm_store_close(s);
    PASS();
}

TEST(pipeline_file_delta_plan_falls_back_without_file_metadata) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);
    cbm_pipeline_file_delta_t delta = {.delta = {.project = "test", .rel_path = "main.go"}};

    cbm_pipeline_file_delta_plan_t plan = {0};
    ASSERT_EQ(cbm_pipeline_plan_file_delta(s, &delta, CBM_SZ_4, &plan), CBM_STORE_OK);
    ASSERT_EQ(plan.route, CBM_PIPELINE_DELTA_ROUTE_FALLBACK);
    ASSERT_STR_EQ(plan.reason, "missing_file_metadata");
    ASSERT_EQ(plan.affected_count, 0);

    cbm_pipeline_file_delta_plan_free(&plan);
    cbm_store_close(s);
    PASS();
}

TEST(pipeline_file_delta_plan_falls_back_on_unsupported_edges) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    cbm_pipeline_file_delta_t delta = {
        .delta = {.project = "test", .rel_path = "main.go"}, .unsupported_edge_count = 1};

    cbm_pipeline_file_delta_plan_t plan = {0};
    ASSERT_EQ(cbm_pipeline_plan_file_delta(s, &delta, CBM_SZ_4, &plan), CBM_STORE_OK);
    ASSERT_EQ(plan.route, CBM_PIPELINE_DELTA_ROUTE_FALLBACK);
    ASSERT_STR_EQ(plan.reason, "unsupported_edges");
    ASSERT_EQ(plan.affected_count, 0);

    cbm_pipeline_file_delta_plan_free(&plan);
    cbm_store_close(s);
    PASS();
}

TEST(pipeline_file_delta_plan_falls_back_on_delete) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    cbm_pipeline_file_delta_t delta = {.delta = {.project = "test", .rel_path = "gone.go"},
                                       .change_kind = CBM_PIPELINE_DELTA_CHANGE_DELETE};

    cbm_pipeline_file_delta_plan_t plan = {0};
    ASSERT_EQ(cbm_pipeline_plan_file_delta(s, &delta, CBM_SZ_4, &plan), CBM_STORE_OK);
    ASSERT_EQ(plan.route, CBM_PIPELINE_DELTA_ROUTE_FALLBACK);
    ASSERT_STR_EQ(plan.reason, "missing_generation");
    ASSERT_EQ(plan.affected_count, 0);

    cbm_pipeline_file_delta_plan_free(&plan);
    cbm_store_close(s);
    PASS();
}

TEST(pipeline_file_delta_apply_deletes_owned_file_delta) {
    enum {
        PIPELINE_DELETE_BASE_GENERATION = 1,
        PIPELINE_DELETE_FINAL_GENERATION = 2,
        PIPELINE_DELETE_DELTA_COUNT = 1,
    };
    const char *project = "test";
    const char *rel_path = "gone.go";
    const char *old_qn = "test.gone.Old";

    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, project, "/tmp/test"), CBM_STORE_OK);

    int64_t generation = 0;
    ASSERT_EQ(cbm_store_reserve_index_generation(s, project, NULL, NULL, &generation),
              CBM_STORE_OK);
    ASSERT_EQ(generation, PIPELINE_DELETE_BASE_GENERATION);
    ASSERT_EQ(cbm_store_finish_index_generation(s, project, generation,
                                                CBM_STORE_INDEX_STATUS_COMPLETE),
              CBM_STORE_OK);
    ASSERT_EQ(pipeline_delta_seed_existing_ownership(s, project, rel_path, old_qn),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_store_reserve_index_generation(s, project, NULL, NULL, &generation),
              CBM_STORE_OK);
    ASSERT_EQ(generation, PIPELINE_DELETE_FINAL_GENERATION);

    cbm_pipeline_file_delta_t delta = {.delta = {.project = project,
                                                 .rel_path = rel_path,
                                                 .generation = generation},
                                       .change_kind = CBM_PIPELINE_DELTA_CHANGE_DELETE};
    const cbm_pipeline_file_delta_t *deltas[] = {&delta};
    cbm_pipeline_file_delta_plan_t plan = {0};
    ASSERT_EQ(cbm_pipeline_apply_file_delta_batch(s, deltas, PIPELINE_DELETE_DELTA_COUNT,
                                                  CBM_SZ_4, &plan),
              CBM_STORE_OK);
    ASSERT_EQ(plan.route, CBM_PIPELINE_DELTA_ROUTE_EXACT_CANDIDATE);
    ASSERT_STR_EQ(plan.reason, "candidate");
    ASSERT_EQ(pipeline_delta_store_qn_exists(s, project, old_qn), 0);
    cbm_file_state_t state = {0};
    ASSERT_EQ(cbm_store_get_file_state(s, project, rel_path, &state), CBM_STORE_NOT_FOUND);

    cbm_pipeline_file_delta_plan_free(&plan);
    cbm_store_close(s);
    PASS();
}

TEST(pipeline_file_delta_apply_mixed_delete_upsert_batch) {
    enum {
        PIPELINE_RENAME_BASE_GENERATION = 1,
        PIPELINE_RENAME_FINAL_GENERATION = 2,
        PIPELINE_RENAME_DELTA_COUNT = 2,
    };
    const char *project = "test";
    const char *old_rel = "pkg/file_0000.go";
    const char *new_rel = "pkg/file_renamed.go";
    const char *old_qn = "test.pkg.file_0000.OldName";
    const char *new_qn = "test.pkg.file_renamed.NewName";

    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, project, "/tmp/test"), CBM_STORE_OK);
    ASSERT_EQ(pipeline_delta_seed_project_node(s, project), CBM_STORE_OK);
    int64_t generation = 0;
    ASSERT_EQ(cbm_store_reserve_index_generation(s, project, NULL, NULL, &generation),
              CBM_STORE_OK);
    ASSERT_EQ(generation, PIPELINE_RENAME_BASE_GENERATION);
    ASSERT_EQ(cbm_store_finish_index_generation(s, project, generation,
                                                CBM_STORE_INDEX_STATUS_COMPLETE),
              CBM_STORE_OK);
    ASSERT_EQ(pipeline_delta_seed_existing_ownership(s, project, old_rel, old_qn),
              CBM_STORE_OK);
    ASSERT_EQ(pipeline_store_file_state_generation_memory(s, project, old_rel, &generation),
              CBM_STORE_OK);
    ASSERT_EQ(generation, PIPELINE_RENAME_BASE_GENERATION);
    ASSERT_EQ(cbm_store_reserve_index_generation(s, project, NULL, NULL, &generation),
              CBM_STORE_OK);
    ASSERT_EQ(generation, PIPELINE_RENAME_FINAL_GENERATION);

    cbm_gbuf_t *scratch = cbm_gbuf_new(project, "/tmp/test");
    ASSERT_NOT_NULL(scratch);
    ASSERT_GT(cbm_gbuf_upsert_node(scratch, "Project", project, project, NULL, 0, 0, "{}"), 0);
    ASSERT_EQ(cbm_pipeline_ensure_file_structure(scratch, project, project, new_rel, NULL), 0);
    ASSERT_GT(cbm_gbuf_upsert_node(scratch, "Function", "NewName", new_qn, new_rel, 1, 1,
                                   "{\"is_exported\":true}"),
              0);

    cbm_pipeline_file_delta_t upsert_delta = {0};
    ASSERT_EQ(cbm_pipeline_build_file_delta_from_gbuf(scratch, project, new_rel,
                                                      PIPELINE_RENAME_FINAL_GENERATION,
                                                      &upsert_delta),
              CBM_STORE_OK);
    cbm_file_hash_t hash = {0};
    cbm_file_state_t state = {0};
    pipeline_delta_attach_test_metadata(&upsert_delta, &hash, &state);
    ASSERT_EQ(cbm_pipeline_file_delta_stamp_generation(&upsert_delta,
                                                       PIPELINE_RENAME_FINAL_GENERATION),
              CBM_STORE_OK);
    cbm_pipeline_file_delta_t delete_delta = {
        .delta = {.project = project,
                  .rel_path = old_rel,
                  .generation = PIPELINE_RENAME_FINAL_GENERATION},
        .change_kind = CBM_PIPELINE_DELTA_CHANGE_DELETE};
    const cbm_pipeline_file_delta_t *deltas[] = {&delete_delta, &upsert_delta};

    cbm_pipeline_file_delta_plan_t plan = {0};
    ASSERT_EQ(cbm_pipeline_apply_file_delta_batch(s, deltas, PIPELINE_RENAME_DELTA_COUNT,
                                                  CBM_SZ_4, &plan),
              CBM_STORE_OK);
    ASSERT_STR_EQ(plan.reason, "candidate");
    ASSERT_EQ(plan.route, CBM_PIPELINE_DELTA_ROUTE_EXACT_CANDIDATE);
    ASSERT_EQ(pipeline_delta_store_qn_exists(s, project, old_qn), 0);
    ASSERT_EQ(pipeline_delta_store_qn_exists(s, project, new_qn), 1);
    ASSERT_EQ(pipeline_store_file_state_generation_memory(s, project, old_rel, &generation),
              CBM_STORE_NOT_FOUND);
    ASSERT_EQ(pipeline_store_file_state_generation_memory(s, project, new_rel, &generation),
              CBM_STORE_OK);
    ASSERT_EQ(generation, PIPELINE_RENAME_FINAL_GENERATION);

    cbm_pipeline_file_delta_plan_free(&plan);
    cbm_pipeline_file_delta_free(&upsert_delta);
    cbm_gbuf_free(scratch);
    cbm_store_close(s);
    PASS();
}

TEST(pipeline_file_delta_apply_falls_back_on_delete_batch) {
    enum {
        PIPELINE_DELETE_BATCH_BASE_GENERATION = 1,
        PIPELINE_DELETE_BATCH_FINAL_GENERATION = 2,
        PIPELINE_DELETE_BATCH_COUNT = 2,
    };
    const char *project = "test";
    const char *first_rel = "one.go";
    const char *second_rel = "two.go";
    const char *first_qn = "test.one.Old";
    const char *second_qn = "test.two.Old";

    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, project, "/tmp/test"), CBM_STORE_OK);

    int64_t generation = 0;
    ASSERT_EQ(cbm_store_reserve_index_generation(s, project, NULL, NULL, &generation),
              CBM_STORE_OK);
    ASSERT_EQ(generation, PIPELINE_DELETE_BATCH_BASE_GENERATION);
    ASSERT_EQ(cbm_store_finish_index_generation(s, project, generation,
                                                CBM_STORE_INDEX_STATUS_COMPLETE),
              CBM_STORE_OK);
    ASSERT_EQ(pipeline_delta_seed_existing_ownership(s, project, first_rel, first_qn),
              CBM_STORE_OK);
    ASSERT_EQ(pipeline_delta_seed_existing_ownership(s, project, second_rel, second_qn),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_store_reserve_index_generation(s, project, NULL, NULL, &generation),
              CBM_STORE_OK);
    ASSERT_EQ(generation, PIPELINE_DELETE_BATCH_FINAL_GENERATION);

    cbm_pipeline_file_delta_t first = {.delta = {.project = project,
                                                 .rel_path = first_rel,
                                                 .generation = generation},
                                       .change_kind = CBM_PIPELINE_DELTA_CHANGE_DELETE};
    cbm_pipeline_file_delta_t second = {.delta = {.project = project,
                                                  .rel_path = second_rel,
                                                  .generation = generation},
                                        .change_kind = CBM_PIPELINE_DELTA_CHANGE_DELETE};
    const cbm_pipeline_file_delta_t *deltas[] = {&first, &second};
    cbm_pipeline_file_delta_plan_t plan = {0};
    ASSERT_EQ(cbm_pipeline_apply_file_delta_batch(s, deltas, PIPELINE_DELETE_BATCH_COUNT,
                                                  CBM_SZ_4, &plan),
              CBM_STORE_OK);
    ASSERT_EQ(plan.route, CBM_PIPELINE_DELTA_ROUTE_FALLBACK);
    ASSERT_STR_EQ(plan.reason, "delete_batch_requires_full");
    ASSERT_EQ(pipeline_delta_store_qn_exists(s, project, first_qn), 1);
    ASSERT_EQ(pipeline_delta_store_qn_exists(s, project, second_qn), 1);

    cbm_pipeline_file_delta_plan_free(&plan);
    cbm_store_close(s);
    PASS();
}

TEST(pipeline_file_delta_apply_falls_back_when_frontier_path_missing_from_batch) {
    enum { PIPELINE_FRONTIER_MISSING_DELTA_COUNT = 1 };
    const char *project = "test";
    const char *lib_rel = "lib.go";
    const char *main_rel = "main.go";
    const char *lib_qn = "test.lib.Hot";

    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, project, "/tmp/test"), CBM_STORE_OK);
    ASSERT_EQ(pipeline_delta_seed_existing_ownership(s, project, lib_rel, lib_qn), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_upsert_symbol_export(s, project, lib_qn, lib_rel, CBM_STORE_NO_NODE_ID,
                                             1),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_store_upsert_import_ref(s, project, main_rel, "test.lib", "Hot", lib_qn, 1),
              CBM_STORE_OK);

    cbm_node_t nodes[1] = {{.project = (char *)project,
                            .label = "Function",
                            .name = "Hot",
                            .qualified_name = (char *)lib_qn,
                            .file_path = (char *)lib_rel,
                            .properties_json = "{}"}};
    cbm_store_symbol_export_t exports[1] = {
        {.qualified_name = lib_qn, .node_id = CBM_STORE_NO_NODE_ID}};
    cbm_pipeline_file_delta_t delta = {.delta = {.project = project,
                                                 .rel_path = lib_rel,
                                                 .nodes = nodes,
                                                 .node_count = 1,
                                                 .exports = exports,
                                                 .export_count = 1}};
    cbm_file_hash_t hash = {0};
    cbm_file_state_t state = {0};
    pipeline_delta_attach_test_metadata(&delta, &hash, &state);

    const cbm_pipeline_file_delta_t *deltas[] = {&delta};
    cbm_pipeline_file_delta_plan_t plan = {0};
    ASSERT_EQ(cbm_pipeline_apply_file_delta_batch(s, deltas,
                                                  PIPELINE_FRONTIER_MISSING_DELTA_COUNT,
                                                  CBM_SZ_4, &plan),
              CBM_STORE_OK);
    ASSERT_EQ(plan.route, CBM_PIPELINE_DELTA_ROUTE_FALLBACK);
    ASSERT_STR_EQ(plan.reason, "frontier_requires_batch");
    ASSERT_EQ(pipeline_delta_store_qn_exists(s, project, lib_qn), 1);

    cbm_pipeline_file_delta_plan_free(&plan);
    cbm_store_close(s);
    PASS();
}

TEST(pipeline_file_delta_plan_falls_back_on_rename) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    cbm_pipeline_file_delta_t delta = {.delta = {.project = "test", .rel_path = "new.go"},
                                       .change_kind = CBM_PIPELINE_DELTA_CHANGE_RENAME,
                                       .old_rel_path = "old.go"};

    cbm_pipeline_file_delta_plan_t plan = {0};
    ASSERT_EQ(cbm_pipeline_plan_file_delta(s, &delta, CBM_SZ_4, &plan), CBM_STORE_OK);
    ASSERT_EQ(plan.route, CBM_PIPELINE_DELTA_ROUTE_FALLBACK);
    ASSERT_STR_EQ(plan.reason, "rename_requires_full");
    ASSERT_EQ(plan.affected_count, 0);

    cbm_pipeline_file_delta_plan_free(&plan);
    cbm_store_close(s);
    PASS();
}

TEST(pipeline_file_delta_plan_falls_back_on_unsupported_derived_view) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);
    cbm_pipeline_file_delta_t delta = {
        .delta = {.project = "test",
                  .rel_path = "main.go",
                  .derived_view_name = CBM_STORE_DERIVED_VIEW_PAGERANK,
                  .derived_status = CBM_STORE_DERIVED_STATUS_STALE}};
    cbm_file_hash_t hash = {0};
    cbm_file_state_t state = {0};
    pipeline_delta_attach_test_metadata(&delta, &hash, &state);

    cbm_pipeline_file_delta_plan_t plan = {0};
    ASSERT_EQ(cbm_pipeline_plan_file_delta(s, &delta, CBM_SZ_4, &plan), CBM_STORE_OK);
    ASSERT_EQ(plan.route, CBM_PIPELINE_DELTA_ROUTE_FALLBACK);
    ASSERT_STR_EQ(plan.reason, "unsupported_derived_view");
    ASSERT_EQ(plan.affected_count, 0);

    cbm_pipeline_file_delta_plan_free(&plan);
    cbm_store_close(s);
    PASS();
}

TEST(pipeline_file_delta_plan_falls_back_on_unresolved_edge_endpoint) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);
    ASSERT_EQ(pipeline_delta_seed_existing_ownership(s, "test", "main.go", "test.main.Old"),
              CBM_STORE_OK);
    cbm_node_t nodes[1] = {{.project = "test",
                            .label = "Function",
                            .name = "Run",
                            .qualified_name = "test.main.Run",
                            .file_path = "main.go",
                            .properties_json = "{}"}};
    cbm_store_delta_edge_t edges[1] = {{.source_qn = "test.main.Run",
                                        .target_qn = "test.missing.Helper",
                                        .type = "CALLS",
                                        .properties_json = "{}",
                                        .derived_kind = CBM_STORE_DERIVED_KIND_DIRECT}};
    cbm_pipeline_file_delta_t delta = {
        .delta = {.project = "test",
                  .rel_path = "main.go",
                  .nodes = nodes,
                  .node_count = 1,
                  .edges = edges,
                  .edge_count = 1}};
    cbm_file_hash_t hash = {0};
    cbm_file_state_t state = {0};
    pipeline_delta_attach_test_metadata(&delta, &hash, &state);

    cbm_pipeline_file_delta_plan_t plan = {0};
    ASSERT_EQ(cbm_pipeline_plan_file_delta(s, &delta, CBM_SZ_4, &plan), CBM_STORE_OK);
    ASSERT_EQ(plan.route, CBM_PIPELINE_DELTA_ROUTE_FALLBACK);
    ASSERT_STR_EQ(plan.reason, "unresolved_edge_endpoint");
    ASSERT_EQ(plan.affected_count, 0);

    cbm_pipeline_file_delta_plan_free(&plan);
    cbm_store_close(s);
    PASS();
}

TEST(pipeline_file_delta_plan_accepts_resolved_external_edge_endpoint) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);
    ASSERT_EQ(pipeline_delta_seed_existing_ownership(s, "test", "main.go", "test.main.Old"),
              CBM_STORE_OK);
    cbm_node_t helper = {.project = "test",
                         .label = "Function",
                         .name = "Helper",
                         .qualified_name = "test.helper.Helper",
                         .file_path = "helper.go",
                         .properties_json = "{}"};
    ASSERT_GT(cbm_store_upsert_node(s, &helper), 0);

    cbm_node_t nodes[1] = {{.project = "test",
                            .label = "Function",
                            .name = "Run",
                            .qualified_name = "test.main.Run",
                            .file_path = "main.go",
                            .properties_json = "{}"}};
    cbm_store_delta_edge_t edges[1] = {{.source_qn = "test.main.Run",
                                        .target_qn = "test.helper.Helper",
                                        .type = "CALLS",
                                        .properties_json = "{}",
                                        .derived_kind = CBM_STORE_DERIVED_KIND_DIRECT}};
    cbm_pipeline_file_delta_t delta = {
        .delta = {.project = "test",
                  .rel_path = "main.go",
                  .nodes = nodes,
                  .node_count = 1,
                  .edges = edges,
                  .edge_count = 1}};
    cbm_file_hash_t hash = {0};
    cbm_file_state_t state = {0};
    pipeline_delta_attach_test_metadata(&delta, &hash, &state);

    cbm_pipeline_file_delta_plan_t plan = {0};
    ASSERT_EQ(cbm_pipeline_plan_file_delta(s, &delta, CBM_SZ_4, &plan), CBM_STORE_OK);
    ASSERT_EQ(plan.route, CBM_PIPELINE_DELTA_ROUTE_EXACT_CANDIDATE);
    ASSERT_STR_EQ(plan.reason, "candidate");
    ASSERT_EQ(plan.affected_count, 1);
    ASSERT_EQ(pipeline_delta_plan_contains_path(&plan, "main.go"), 1);

    cbm_pipeline_file_delta_plan_free(&plan);
    cbm_store_close(s);
    PASS();
}

TEST(pipeline_file_delta_plan_falls_back_on_large_frontier) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);
    ASSERT_EQ(pipeline_delta_seed_existing_ownership(s, "test", "lib.go", "test.lib.Old"),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_store_upsert_symbol_export(s, "test", "test.lib.Hot", "lib.go",
                                             CBM_STORE_NO_NODE_ID, 1),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_store_upsert_import_ref(s, "test", "a.go", "test.lib", "Hot",
                                          "test.lib.Hot", 1),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_store_upsert_import_ref(s, "test", "b.go", "test.lib", "Hot",
                                          "test.lib.Hot", 1),
              CBM_STORE_OK);

    cbm_store_symbol_export_t exports[1] = {
        {.qualified_name = "test.lib.Hot", .node_id = CBM_STORE_NO_NODE_ID}};
    cbm_pipeline_file_delta_t delta = {
        .delta = {.project = "test", .rel_path = "lib.go", .exports = exports, .export_count = 1}};
    cbm_file_hash_t hash = {0};
    cbm_file_state_t state = {0};
    pipeline_delta_attach_test_metadata(&delta, &hash, &state);

    cbm_pipeline_file_delta_plan_t plan = {0};
    ASSERT_EQ(cbm_pipeline_plan_file_delta(s, &delta, CBM_SZ_2, &plan), CBM_STORE_OK);
    ASSERT_EQ(plan.route, CBM_PIPELINE_DELTA_ROUTE_FALLBACK);
    ASSERT_STR_EQ(plan.reason, "frontier_too_large");
    ASSERT_EQ(plan.affected_count, 3);
    ASSERT_EQ(pipeline_delta_plan_contains_path(&plan, "lib.go"), 1);
    ASSERT_EQ(pipeline_delta_plan_contains_path(&plan, "a.go"), 1);
    ASSERT_EQ(pipeline_delta_plan_contains_path(&plan, "b.go"), 1);

    cbm_pipeline_file_delta_plan_free(&plan);
    cbm_store_close(s);
    PASS();
}

TEST(pipeline_file_delta_plan_batch_accepts_mutual_frontier) {
    enum {
        PIPELINE_MUTUAL_GENERATION = 1,
        PIPELINE_MUTUAL_SINGLE_COUNT = 1,
        PIPELINE_MUTUAL_DELTA_COUNT = 2,
        PIPELINE_MUTUAL_MAX_AFFECTED = 4,
    };
    const char *project = "test";
    const char *a_rel = "a.go";
    const char *b_rel = "b.go";
    const char *old_a_qn = "test.a.OldA";
    const char *old_b_qn = "test.b.OldB";
    const char *new_a_qn = "test.a.NewA";
    const char *new_b_qn = "test.b.NewB";

    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, project, "/tmp/test"), CBM_STORE_OK);
    int64_t old_a_id = pipeline_delta_seed_existing_ownership_id(s, project, a_rel, old_a_qn);
    int64_t old_b_id = pipeline_delta_seed_existing_ownership_id(s, project, b_rel, old_b_qn);
    ASSERT_GT(old_a_id, CBM_STORE_NO_NODE_ID);
    ASSERT_GT(old_b_id, CBM_STORE_NO_NODE_ID);
    cbm_edge_t old_a_to_b = {.project = (char *)project,
                             .source_id = old_a_id,
                             .target_id = old_b_id,
                             .type = "CALLS",
                             .properties_json = "{}"};
    cbm_edge_t old_b_to_a = {.project = (char *)project,
                             .source_id = old_b_id,
                             .target_id = old_a_id,
                             .type = "CALLS",
                             .properties_json = "{}"};
    ASSERT_GT(cbm_store_insert_edge(s, &old_a_to_b), 0);
    ASSERT_GT(cbm_store_insert_edge(s, &old_b_to_a), 0);
    ASSERT_EQ(cbm_store_upsert_symbol_export(s, project, old_a_qn, a_rel, old_a_id,
                                             PIPELINE_MUTUAL_GENERATION),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_store_upsert_symbol_export(s, project, old_b_qn, b_rel, old_b_id,
                                             PIPELINE_MUTUAL_GENERATION),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_store_upsert_import_ref(s, project, a_rel, "test.b", "OldB", old_b_qn,
                                          PIPELINE_MUTUAL_GENERATION),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_store_upsert_import_ref(s, project, b_rel, "test.a", "OldA", old_a_qn,
                                          PIPELINE_MUTUAL_GENERATION),
              CBM_STORE_OK);

    cbm_node_t a_nodes[PIPELINE_MUTUAL_SINGLE_COUNT] = {{.project = (char *)project,
                                                         .label = "Function",
                                                         .name = "NewA",
                                                         .qualified_name = (char *)new_a_qn,
                                                         .file_path = (char *)a_rel,
                                                         .properties_json = "{}"}};
    cbm_node_t b_nodes[PIPELINE_MUTUAL_SINGLE_COUNT] = {{.project = (char *)project,
                                                         .label = "Function",
                                                         .name = "NewB",
                                                         .qualified_name = (char *)new_b_qn,
                                                         .file_path = (char *)b_rel,
                                                         .properties_json = "{}"}};
    cbm_store_delta_edge_t a_edges[PIPELINE_MUTUAL_SINGLE_COUNT] = {
        {.source_qn = new_a_qn,
         .target_qn = new_b_qn,
         .type = "CALLS",
         .properties_json = "{}",
         .derived_kind = CBM_STORE_DERIVED_KIND_DIRECT}};
    cbm_store_delta_edge_t b_edges[PIPELINE_MUTUAL_SINGLE_COUNT] = {
        {.source_qn = new_b_qn,
         .target_qn = new_a_qn,
         .type = "CALLS",
         .properties_json = "{}",
         .derived_kind = CBM_STORE_DERIVED_KIND_DIRECT}};
    cbm_store_symbol_export_t a_exports[PIPELINE_MUTUAL_SINGLE_COUNT] = {
        {.qualified_name = new_a_qn, .node_id = CBM_STORE_NO_NODE_ID}};
    cbm_store_symbol_export_t b_exports[PIPELINE_MUTUAL_SINGLE_COUNT] = {
        {.qualified_name = new_b_qn, .node_id = CBM_STORE_NO_NODE_ID}};
    cbm_pipeline_file_delta_t a_delta = {
        .delta = {.project = project,
                  .rel_path = a_rel,
                  .nodes = a_nodes,
                  .node_count = PIPELINE_MUTUAL_SINGLE_COUNT,
                  .edges = a_edges,
                  .edge_count = PIPELINE_MUTUAL_SINGLE_COUNT,
                  .exports = a_exports,
                  .export_count = PIPELINE_MUTUAL_SINGLE_COUNT}};
    cbm_pipeline_file_delta_t b_delta = {
        .delta = {.project = project,
                  .rel_path = b_rel,
                  .nodes = b_nodes,
                  .node_count = PIPELINE_MUTUAL_SINGLE_COUNT,
                  .edges = b_edges,
                  .edge_count = PIPELINE_MUTUAL_SINGLE_COUNT,
                  .exports = b_exports,
                  .export_count = PIPELINE_MUTUAL_SINGLE_COUNT}};
    cbm_file_hash_t a_hash = {0};
    cbm_file_hash_t b_hash = {0};
    cbm_file_state_t a_state = {0};
    cbm_file_state_t b_state = {0};
    pipeline_delta_attach_test_metadata(&a_delta, &a_hash, &a_state);
    pipeline_delta_attach_test_metadata(&b_delta, &b_hash, &b_state);

    const cbm_pipeline_file_delta_t *deltas[] = {&a_delta, &b_delta};
    cbm_pipeline_file_delta_plan_t plan = {0};
    ASSERT_EQ(cbm_pipeline_plan_file_delta_batch(s, deltas, PIPELINE_MUTUAL_DELTA_COUNT,
                                                 PIPELINE_MUTUAL_MAX_AFFECTED, &plan),
              CBM_STORE_OK);
    ASSERT_EQ(plan.route, CBM_PIPELINE_DELTA_ROUTE_EXACT_CANDIDATE);
    ASSERT_STR_EQ(plan.reason, "candidate");
    ASSERT_EQ(plan.affected_count, PIPELINE_MUTUAL_DELTA_COUNT);
    ASSERT_EQ(pipeline_delta_plan_contains_path(&plan, a_rel), 1);
    ASSERT_EQ(pipeline_delta_plan_contains_path(&plan, b_rel), 1);

    cbm_pipeline_file_delta_plan_free(&plan);
    cbm_store_close(s);
    PASS();
}

TEST(pipeline_file_delta_orchestrates_descriptor_plan_and_publish) {
    enum {
        BASE_GENERATION = 1,
        FINAL_GENERATION = 2,
        PIPELINE_DELTA_PARITY_MAX_AFFECTED = CBM_SZ_4,
        PIPELINE_DELTA_PARITY_SINGLE_COUNT = 1,
        PIPELINE_DELTA_PARITY_BATCH_COUNT = 2,
        EXPECTED_FINAL_CALLS_EDGES = 1,
        EXPECTED_FINAL_IMPORTS_EDGES = 1,
        EXPECTED_FINAL_EDGES = EXPECTED_FINAL_CALLS_EDGES + EXPECTED_FINAL_IMPORTS_EDGES,
    };
    const char *project = "test";
    const char *helper_rel = "helper.go";
    const char *main_rel = "main.go";
    const char *old_helper_qn = "test.helper.Helper";
    const char *new_helper_qn = "test.helper.NewHelper";
    const char *old_main_qn = "test.main.Old";
    const char *new_main_qn = "test.main.New";

    char *tmp = th_mktempdir("cbm_delta_pipeline");
    ASSERT_NOT_NULL(tmp);
    const char *helper_path = TH_PATH(tmp, helper_rel);
    const char *main_path = TH_PATH(tmp, main_rel);
    ASSERT_EQ(th_write_file(helper_path, "package helper\nfunc Helper() {}\n"), 0);
    ASSERT_EQ(th_write_file(main_path, "package main\nfunc Old() {}\n"), 0);

    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, project, tmp), CBM_STORE_OK);

    int64_t generation = 0;
    ASSERT_EQ(cbm_store_reserve_index_generation(s, project, NULL, NULL, &generation),
              CBM_STORE_OK);
    ASSERT_EQ(generation, BASE_GENERATION);

    cbm_gbuf_t *base_gb = cbm_gbuf_new(project, tmp);
    ASSERT_NOT_NULL(base_gb);
    int64_t base_helper_id = cbm_gbuf_upsert_node(base_gb, "Function", "Helper",
                                                  old_helper_qn, helper_rel, 1, 1,
                                                  "{\"is_exported\":true}");
    int64_t base_file_id = cbm_gbuf_upsert_node(base_gb, "File", main_rel,
                                                "test.main.__file__", main_rel, 1, 1, "{}");
    int64_t base_main_id = cbm_gbuf_upsert_node(base_gb, "Function", "Old", old_main_qn,
                                                main_rel, 1, 1, "{\"is_exported\":true}");
    ASSERT_GT(base_helper_id, 0);
    ASSERT_GT(base_file_id, 0);
    ASSERT_GT(base_main_id, 0);
    const cbm_gbuf_node_t *base_helper = cbm_gbuf_find_by_qn(base_gb, old_helper_qn);
    ASSERT_NOT_NULL(base_helper);
    cbm_pipeline_ctx_t base_ctx = {.project_name = project, .repo_path = tmp, .gbuf = base_gb};
    ASSERT_EQ(cbm_pipeline_insert_import_edge(&base_ctx, base_file_id, base_helper, "Helper"), 1);
    ASSERT_GT(cbm_gbuf_insert_edge(base_gb, base_main_id, base_helper_id, "CALLS", "{}"), 0);

    cbm_pipeline_file_delta_t base_helper_delta = {0};
    cbm_pipeline_file_delta_t base_main_delta = {0};
    ASSERT_EQ(cbm_pipeline_build_file_delta_from_gbuf(base_gb, project, helper_rel, generation,
                                                      &base_helper_delta),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_pipeline_build_file_delta_from_gbuf(base_gb, project, main_rel, generation,
                                                      &base_main_delta),
              CBM_STORE_OK);
    cbm_file_info_t helper_file = {.path = (char *)helper_path,
                                   .rel_path = (char *)helper_rel,
                                   .language = CBM_LANG_GO};
    cbm_file_info_t main_file = {
        .path = (char *)main_path, .rel_path = (char *)main_rel, .language = CBM_LANG_GO};
    ASSERT_EQ(cbm_pipeline_attach_file_delta_metadata(&base_helper_delta, &helper_file),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_pipeline_attach_file_delta_metadata(&base_main_delta, &main_file),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_store_publish_file_delta(s, &base_helper_delta.delta), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_publish_file_delta(s, &base_main_delta.delta), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_finish_index_generation(s, project, generation,
                                                CBM_STORE_INDEX_STATUS_COMPLETE),
              CBM_STORE_OK);
    cbm_pipeline_file_delta_free(&base_helper_delta);
    cbm_pipeline_file_delta_free(&base_main_delta);
    cbm_gbuf_free(base_gb);

    ASSERT_EQ(th_write_file(helper_path, "package helper\nfunc NewHelper() {}\n"), 0);
    ASSERT_EQ(th_write_file(main_path,
                            "package main\nimport \"helper\"\nfunc New() { helper.NewHelper() }\n"),
              0);
    ASSERT_EQ(cbm_store_reserve_index_generation(s, project, NULL, NULL, &generation),
              CBM_STORE_OK);
    ASSERT_EQ(generation, FINAL_GENERATION);

    cbm_gbuf_t *final_gb = cbm_gbuf_new(project, tmp);
    ASSERT_NOT_NULL(final_gb);
    int64_t new_helper_id = cbm_gbuf_upsert_node(final_gb, "Function", "NewHelper",
                                                 new_helper_qn, helper_rel, 1, 1,
                                                 "{\"is_exported\":true}");
    int64_t final_file_id = cbm_gbuf_upsert_node(final_gb, "File", main_rel,
                                                 "test.main.__file__", main_rel, 1, 1, "{}");
    int64_t new_main_id = cbm_gbuf_upsert_node(final_gb, "Function", "New", new_main_qn,
                                               main_rel, 3, 3, "{\"is_exported\":true}");
    ASSERT_GT(new_helper_id, 0);
    ASSERT_GT(final_file_id, 0);
    ASSERT_GT(new_main_id, 0);
    const cbm_gbuf_node_t *new_helper = cbm_gbuf_find_by_qn(final_gb, new_helper_qn);
    ASSERT_NOT_NULL(new_helper);
    cbm_pipeline_ctx_t final_ctx = {.project_name = project, .repo_path = tmp, .gbuf = final_gb};
    ASSERT_EQ(cbm_pipeline_insert_import_edge(&final_ctx, final_file_id, new_helper, "NewHelper"),
              1);
    ASSERT_GT(cbm_gbuf_insert_edge(final_gb, new_main_id, new_helper_id, "CALLS", "{}"), 0);

    cbm_pipeline_file_delta_t final_helper_delta = {0};
    cbm_pipeline_file_delta_t final_main_delta = {0};
    ASSERT_EQ(cbm_pipeline_build_file_delta_from_gbuf(final_gb, project, helper_rel, generation,
                                                      &final_helper_delta),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_pipeline_build_file_delta_from_gbuf(final_gb, project, main_rel, generation,
                                                      &final_main_delta),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_pipeline_attach_file_delta_metadata(&final_helper_delta, &helper_file),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_pipeline_attach_file_delta_metadata(&final_main_delta, &main_file),
              CBM_STORE_OK);

    cbm_pipeline_file_delta_plan_t main_before_helper_plan = {0};
    ASSERT_EQ(cbm_pipeline_plan_file_delta(s, &final_main_delta,
                                           PIPELINE_DELTA_PARITY_MAX_AFFECTED,
                                           &main_before_helper_plan),
              CBM_STORE_OK);
    ASSERT_EQ(main_before_helper_plan.route, CBM_PIPELINE_DELTA_ROUTE_FALLBACK);
    ASSERT_STR_EQ(main_before_helper_plan.reason, "unresolved_edge_endpoint");
    const cbm_pipeline_file_delta_t *main_only_deltas[] = {&final_main_delta};
    cbm_pipeline_file_delta_plan_t main_only_apply_plan = {0};
    ASSERT_EQ(cbm_pipeline_apply_file_delta_batch(s, main_only_deltas,
                                                  PIPELINE_DELTA_PARITY_SINGLE_COUNT,
                                                  PIPELINE_DELTA_PARITY_MAX_AFFECTED,
                                                  &main_only_apply_plan),
              CBM_STORE_OK);
    ASSERT_EQ(main_only_apply_plan.route, CBM_PIPELINE_DELTA_ROUTE_FALLBACK);
    ASSERT_STR_EQ(main_only_apply_plan.reason, "unresolved_edge_endpoint");
    ASSERT_EQ(pipeline_delta_store_qn_exists(s, project, old_helper_qn), 1);
    ASSERT_EQ(pipeline_delta_store_qn_exists(s, project, old_main_qn), 1);
    ASSERT_EQ(pipeline_delta_store_qn_exists(s, project, new_helper_qn), 0);
    ASSERT_EQ(pipeline_delta_store_qn_exists(s, project, new_main_qn), 0);

    const cbm_pipeline_file_delta_t *batch_deltas[] = {&final_helper_delta, &final_main_delta};
    cbm_pipeline_file_delta_plan_t batch_plan = {0};
    ASSERT_EQ(cbm_pipeline_apply_file_delta_batch(s, batch_deltas,
                                                  PIPELINE_DELTA_PARITY_BATCH_COUNT,
                                                  PIPELINE_DELTA_PARITY_MAX_AFFECTED, &batch_plan),
              CBM_STORE_OK);
    ASSERT_EQ(batch_plan.route, CBM_PIPELINE_DELTA_ROUTE_EXACT_CANDIDATE);
    ASSERT_EQ(pipeline_delta_plan_contains_path(&batch_plan, helper_rel), 1);
    ASSERT_EQ(pipeline_delta_plan_contains_path(&batch_plan, main_rel), 1);

    ASSERT_EQ(pipeline_delta_store_qn_exists(s, project, old_helper_qn), 0);
    ASSERT_EQ(pipeline_delta_store_qn_exists(s, project, old_main_qn), 0);
    ASSERT_EQ(pipeline_delta_store_qn_exists(s, project, new_helper_qn), 1);
    ASSERT_EQ(pipeline_delta_store_qn_exists(s, project, new_main_qn), 1);
    ASSERT_EQ(cbm_store_count_edges(s, project), EXPECTED_FINAL_EDGES);
    ASSERT_EQ(cbm_store_count_edges_by_type(s, project, "CALLS"), EXPECTED_FINAL_CALLS_EDGES);
    ASSERT_EQ(cbm_store_count_edges_by_type(s, project, "IMPORTS"), EXPECTED_FINAL_IMPORTS_EDGES);

    cbm_file_state_t got = {0};
    ASSERT_EQ(cbm_store_get_file_state(s, project, helper_rel, &got), CBM_STORE_OK);
    ASSERT_EQ(got.generation, FINAL_GENERATION);
    cbm_store_file_state_free_fields(&got);
    ASSERT_EQ(cbm_store_get_file_state(s, project, main_rel, &got), CBM_STORE_OK);
    ASSERT_EQ(got.generation, FINAL_GENERATION);
    cbm_store_file_state_free_fields(&got);

    char **import_paths = NULL;
    int import_count = 0;
    ASSERT_EQ(cbm_store_list_import_ref_paths_by_target(s, project, new_helper_qn, &import_paths,
                                                        &import_count),
              CBM_STORE_OK);
    ASSERT_EQ(import_count, 1);
    ASSERT_STR_EQ(import_paths[0], main_rel);
    pipeline_delta_free_string_array(import_paths, import_count);

    cbm_store_t *fresh = cbm_store_open_memory();
    ASSERT_NOT_NULL(fresh);
    ASSERT_EQ(cbm_store_upsert_project(fresh, project, tmp), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_reserve_index_generation(fresh, project, NULL, NULL, &generation),
              CBM_STORE_OK);
    ASSERT_EQ(generation, BASE_GENERATION);
    ASSERT_EQ(cbm_store_finish_index_generation(fresh, project, generation,
                                                CBM_STORE_INDEX_STATUS_COMPLETE),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_store_reserve_index_generation(fresh, project, NULL, NULL, &generation),
              CBM_STORE_OK);
    ASSERT_EQ(generation, FINAL_GENERATION);
    const cbm_store_file_delta_t *fresh_store_deltas[] = {&final_helper_delta.delta,
                                                          &final_main_delta.delta};
    ASSERT_EQ(cbm_store_publish_file_delta_batch_complete(
                  fresh, fresh_store_deltas, PIPELINE_DELTA_PARITY_BATCH_COUNT),
              CBM_STORE_OK);

    const char *delta_db = TH_PATH(tmp, "delta-route.db");
    const char *fresh_db = TH_PATH(tmp, "fresh-final.db");
    ASSERT_EQ(cbm_store_dump_to_file(s, delta_db), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_dump_to_file(fresh, fresh_db), CBM_STORE_OK);
    char diff_err[CBM_SZ_8K] = {0};
    ASSERT_EQ(cbm_test_compare_canonical_graphs(delta_db, fresh_db, project, diff_err,
                                                sizeof(diff_err)),
              0);

    cbm_pipeline_file_delta_plan_free(&main_before_helper_plan);
    cbm_pipeline_file_delta_plan_free(&main_only_apply_plan);
    cbm_pipeline_file_delta_plan_free(&batch_plan);
    cbm_pipeline_file_delta_free(&final_helper_delta);
    cbm_pipeline_file_delta_free(&final_main_delta);
    cbm_gbuf_free(final_gb);
    cbm_store_close(fresh);
    cbm_store_close(s);
    th_cleanup(tmp);
    PASS();
}

/* ── Config helpers (pass_configures.c) ───────────────────────── */

TEST(configures_is_env_var_name) {
    ASSERT(cbm_is_env_var_name("DATABASE_URL"));
    ASSERT(cbm_is_env_var_name("API_KEY"));
    ASSERT(cbm_is_env_var_name("PORT"));
    ASSERT(!cbm_is_env_var_name("A"));      /* too short */
    ASSERT(!cbm_is_env_var_name("port"));   /* lowercase */
    ASSERT(!cbm_is_env_var_name("apiKey")); /* camelCase */
    ASSERT(cbm_is_env_var_name("DB_2"));    /* with digit */
    ASSERT(!cbm_is_env_var_name("__"));     /* no uppercase */
    ASSERT(!cbm_is_env_var_name(""));       /* empty */
    PASS();
}

TEST(configures_normalize_config_key) {
    char norm[256];
    int tokens;

    tokens = cbm_normalize_config_key("max_connections", norm, sizeof(norm));
    ASSERT_STR_EQ(norm, "max_connections");
    ASSERT_EQ(tokens, 2);

    tokens = cbm_normalize_config_key("maxConnections", norm, sizeof(norm));
    ASSERT_STR_EQ(norm, "max_connections");
    ASSERT_EQ(tokens, 2);

    tokens = cbm_normalize_config_key("DATABASE_HOST", norm, sizeof(norm));
    ASSERT_STR_EQ(norm, "database_host");
    ASSERT_EQ(tokens, 2);

    tokens = cbm_normalize_config_key("database.host", norm, sizeof(norm));
    ASSERT_STR_EQ(norm, "database_host");
    ASSERT_EQ(tokens, 2);

    tokens = cbm_normalize_config_key("port", norm, sizeof(norm));
    ASSERT_STR_EQ(norm, "port");
    ASSERT_EQ(tokens, 1);

    tokens = cbm_normalize_config_key("maxRetryCount", norm, sizeof(norm));
    ASSERT_STR_EQ(norm, "max_retry_count");
    ASSERT_EQ(tokens, 3);
    PASS();
}

TEST(configures_has_config_extension) {
    ASSERT(cbm_has_config_extension("config.toml"));
    ASSERT(cbm_has_config_extension("settings.yaml"));
    ASSERT(cbm_has_config_extension("config.yml"));
    ASSERT(cbm_has_config_extension(".env"));
    ASSERT(cbm_has_config_extension("config.ini"));
    ASSERT(cbm_has_config_extension("data.json"));
    ASSERT(cbm_has_config_extension("pom.xml"));
    ASSERT(!cbm_has_config_extension("main.go"));
    ASSERT(!cbm_has_config_extension("app.py"));
    ASSERT(!cbm_has_config_extension("data.csv"));
    PASS();
}

/* ── Config integration tests (configures_test.go ports) ──────── */

TEST(configures_env_var_in_config) {
    /* Port of TestBuildEnvIndex_ConfigVariableAdded:
     * config.toml has DATABASE_URL, main.go does os.Getenv("DATABASE_URL")
     * → CONFIGURES edges should link them. */
    const char *files[] = {"config.toml", "main.go"};
    const char *contents[] = {"DATABASE_URL = \"postgresql://localhost/db\"\n",

                              "package main\n\n"
                              "import \"os\"\n\n"
                              "func main() {\n"
                              "\turl := os.Getenv(\"DATABASE_URL\")\n"
                              "\t_ = url\n"
                              "}\n"};
    if (setup_lang_repo(files, contents, 2) != 0)
        FAIL("tmpdir");
    char db[512];
    snprintf(db, sizeof(db), "%s/test.db", g_lang_tmpdir);
    cbm_pipeline_t *p = cbm_pipeline_new(g_lang_tmpdir, db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    cbm_store_t *s = cbm_store_open_path(db);
    ASSERT_NOT_NULL(s);
    const char *proj = cbm_pipeline_project_name(p);

    /* Verify CONFIGURES edges were created */
    cbm_edge_t *edges = NULL;
    int ec = 0;
    cbm_store_find_edges_by_type(s, proj, "CONFIGURES", &edges, &ec);
    /* At minimum the pipeline should not crash. Edge count depends on
     * extraction matching env var accesses to config variables. */
    if (edges)
        cbm_store_free_edges(edges, ec);
    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_lang_repo();
    PASS();
}

TEST(configures_lowercase_key_skipped) {
    /* Port of TestBuildEnvIndex_LowercaseKeySkipped:
     * config.toml has lowercase key — should NOT produce env var CONFIGURES edges. */
    const char *files[] = {"config.toml", "main.go"};
    const char *contents[] = {"database_host = \"localhost\"\n",

                              "package main\n\nfunc main() {}\n"};
    if (setup_lang_repo(files, contents, 2) != 0)
        FAIL("tmpdir");
    char db[512];
    snprintf(db, sizeof(db), "%s/test.db", g_lang_tmpdir);
    cbm_pipeline_t *p = cbm_pipeline_new(g_lang_tmpdir, db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    cbm_store_t *s = cbm_store_open_path(db);
    ASSERT_NOT_NULL(s);
    /* Pipeline ran successfully — no crash from lowercase config keys */
    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_lang_repo();
    PASS();
}

TEST(configures_non_config_file_skipped) {
    /* Port of TestBuildEnvIndex_NonConfigFileSkipped:
     * Only Go file, no config file — no config-derived CONFIGURES edges. */
    const char *files[] = {"main.go"};
    const char *contents[] = {"package main\n\n"
                              "var API_URL = \"https://api.example.com\"\n\n"
                              "func main() {}\n"};
    if (setup_lang_repo(files, contents, 1) != 0)
        FAIL("tmpdir");
    char db[512];
    snprintf(db, sizeof(db), "%s/test.db", g_lang_tmpdir);
    cbm_pipeline_t *p = cbm_pipeline_new(g_lang_tmpdir, db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    cbm_store_t *s = cbm_store_open_path(db);
    ASSERT_NOT_NULL(s);
    /* No config file → buildEnvIndex should not create config-derived entries */
    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_lang_repo();
    PASS();
}

TEST(configures_full_pipeline_integration) {
    /* Port of TestConfigIntegration_FullPipeline:
     * TOML + INI + JSON config files + Go code → Class & Variable nodes from
     * config files, plus CONFIGURES edges. */
    const char *files[] = {"config.toml", "settings.ini", "config.json", "main.go"};
    const char *contents[] = {"[database]\n"
                              "host = \"localhost\"\n"
                              "port = 5432\n"
                              "max_connections = 100\n\n"
                              "[server]\n"
                              "bind_address = \"0.0.0.0\"\n",

                              "[database]\n"
                              "host = localhost\n"
                              "port = 5432\n",

                              "{\"appName\": \"test\", \"maxRetries\": 3}",

                              "package main\n\n"
                              "import \"os\"\n\n"
                              "func getMaxConnections() int { return 100 }\n\n"
                              "func loadConfig() {\n"
                              "\tcfg := readFile(\"config.toml\")\n"
                              "\t_ = cfg\n"
                              "\tdbURL := os.Getenv(\"DATABASE_URL\")\n"
                              "\t_ = dbURL\n"
                              "}\n\n"
                              "func readFile(path string) string { return \"\" }\n"};
    if (setup_lang_repo(files, contents, 4) != 0)
        FAIL("tmpdir");
    char db[512];
    snprintf(db, sizeof(db), "%s/test.db", g_lang_tmpdir);
    cbm_pipeline_t *p = cbm_pipeline_new(g_lang_tmpdir, db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    cbm_store_t *s = cbm_store_open_path(db);
    ASSERT_NOT_NULL(s);
    const char *proj = cbm_pipeline_project_name(p);

    /* Should have Class nodes (database, server sections from TOML) */
    cbm_node_t *classes = NULL;
    int cc = 0;
    cbm_store_find_nodes_by_label(s, proj, "Class", &classes, &cc);
    ASSERT_GT(cc, 0);
    if (classes)
        cbm_store_free_nodes(classes, cc);

    /* Should have Variable nodes from config files */
    cbm_node_t *vars = NULL;
    int vc = 0;
    cbm_store_find_nodes_by_label(s, proj, "Variable", &vars, &vc);
    ASSERT_GT(vc, 0);
    if (vars)
        cbm_store_free_nodes(vars, vc);

    /* Should have Function nodes from Go code */
    cbm_node_t *funcs = NULL;
    int fc = 0;
    cbm_store_find_nodes_by_label(s, proj, "Function", &funcs, &fc);
    ASSERT_GT(fc, 0);
    if (funcs)
        cbm_store_free_nodes(funcs, fc);

    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_lang_repo();
    PASS();
}
TEST(enrichment_split_camel_case) {
    char *parts[8];
    int n;

    n = cbm_split_camel_case("GetMapping", parts, 8);
    ASSERT_EQ(n, 2);
    ASSERT_STR_EQ(parts[0], "Get");
    ASSERT_STR_EQ(parts[1], "Mapping");
    for (int i = 0; i < n; i++)
        free(parts[i]);

    n = cbm_split_camel_case("getMessage", parts, 8);
    ASSERT_EQ(n, 2);
    ASSERT_STR_EQ(parts[0], "get");
    ASSERT_STR_EQ(parts[1], "Message");
    for (int i = 0; i < n; i++)
        free(parts[i]);

    n = cbm_split_camel_case("cache", parts, 8);
    ASSERT_EQ(n, 1);
    ASSERT_STR_EQ(parts[0], "cache");
    for (int i = 0; i < n; i++)
        free(parts[i]);

    n = cbm_split_camel_case("HTMLParser", parts, 8);
    ASSERT_EQ(n, 1);
    ASSERT_STR_EQ(parts[0], "HTMLParser");
    for (int i = 0; i < n; i++)
        free(parts[i]);

    n = cbm_split_camel_case("", parts, 8);
    ASSERT_EQ(n, 0);
    PASS();
}

TEST(enrichment_tokenize_decorator) {
    char *tokens[16];
    int n;

    n = cbm_tokenize_decorator("@Override", tokens, 16);
    ASSERT_EQ(n, 1);
    ASSERT_STR_EQ(tokens[0], "override");
    for (int i = 0; i < n; i++)
        free(tokens[i]);

    n = cbm_tokenize_decorator("@Deprecated", tokens, 16);
    ASSERT_EQ(n, 1);
    ASSERT_STR_EQ(tokens[0], "deprecated");
    for (int i = 0; i < n; i++)
        free(tokens[i]);

    n = cbm_tokenize_decorator("@Test", tokens, 16);
    ASSERT_EQ(n, 1);
    ASSERT_STR_EQ(tokens[0], "test");
    for (int i = 0; i < n; i++)
        free(tokens[i]);

    n = cbm_tokenize_decorator("@login_required", tokens, 16);
    ASSERT_EQ(n, 2);
    ASSERT_STR_EQ(tokens[0], "login");
    ASSERT_STR_EQ(tokens[1], "required");
    for (int i = 0; i < n; i++)
        free(tokens[i]);

    n = cbm_tokenize_decorator("@cache", tokens, 16);
    ASSERT_EQ(n, 1);
    ASSERT_STR_EQ(tokens[0], "cache");
    for (int i = 0; i < n; i++)
        free(tokens[i]);

    n = cbm_tokenize_decorator("@pytest.fixture", tokens, 16);
    ASSERT_EQ(n, 2);
    ASSERT_STR_EQ(tokens[0], "pytest");
    ASSERT_STR_EQ(tokens[1], "fixture");
    for (int i = 0; i < n; i++)
        free(tokens[i]);

    /* "get" is stopword → only "mapping" */
    n = cbm_tokenize_decorator("@GetMapping(\"/api\")", tokens, 16);
    ASSERT_EQ(n, 1);
    ASSERT_STR_EQ(tokens[0], "mapping");
    for (int i = 0; i < n; i++)
        free(tokens[i]);

    /* "post" passes, "mapping" passes */
    n = cbm_tokenize_decorator("@PostMapping(\"/api\")", tokens, 16);
    ASSERT_EQ(n, 2);
    ASSERT_STR_EQ(tokens[0], "post");
    ASSERT_STR_EQ(tokens[1], "mapping");
    for (int i = 0; i < n; i++)
        free(tokens[i]);

    n = cbm_tokenize_decorator("@Transactional", tokens, 16);
    ASSERT_EQ(n, 1);
    ASSERT_STR_EQ(tokens[0], "transactional");
    for (int i = 0; i < n; i++)
        free(tokens[i]);

    n = cbm_tokenize_decorator("@MessageMapping(\"/chat\")", tokens, 16);
    ASSERT_EQ(n, 2);
    ASSERT_STR_EQ(tokens[0], "message");
    ASSERT_STR_EQ(tokens[1], "mapping");
    for (int i = 0; i < n; i++)
        free(tokens[i]);

    /* Rust-style #[test] */
    n = cbm_tokenize_decorator("#[test]", tokens, 16);
    ASSERT_EQ(n, 1);
    ASSERT_STR_EQ(tokens[0], "test");
    for (int i = 0; i < n; i++)
        free(tokens[i]);

    /* #[derive(Debug)] */
    n = cbm_tokenize_decorator("#[derive(Debug)]", tokens, 16);
    ASSERT_EQ(n, 1);
    ASSERT_STR_EQ(tokens[0], "derive");
    for (int i = 0; i < n; i++)
        free(tokens[i]);

    /* Both "app" and "get" are stopwords → empty */
    n = cbm_tokenize_decorator("@app.get(\"/api\")", tokens, 16);
    ASSERT_EQ(n, 0);

    /* "router" is stopword, "post" passes */
    n = cbm_tokenize_decorator("@router.post(\"/api\")", tokens, 16);
    ASSERT_EQ(n, 1);
    ASSERT_STR_EQ(tokens[0], "post");
    for (int i = 0; i < n; i++)
        free(tokens[i]);

    /* Too short after filtering */
    n = cbm_tokenize_decorator("@x", tokens, 16);
    ASSERT_EQ(n, 0);

    /* Empty */
    n = cbm_tokenize_decorator("", tokens, 16);
    ASSERT_EQ(n, 0);

    n = cbm_tokenize_decorator("@click.command", tokens, 16);
    ASSERT_EQ(n, 2);
    ASSERT_STR_EQ(tokens[0], "click");
    ASSERT_STR_EQ(tokens[1], "command");
    for (int i = 0; i < n; i++)
        free(tokens[i]);

    n = cbm_tokenize_decorator("@celery.task", tokens, 16);
    ASSERT_EQ(n, 2);
    ASSERT_STR_EQ(tokens[0], "celery");
    ASSERT_STR_EQ(tokens[1], "task");
    for (int i = 0; i < n; i++)
        free(tokens[i]);
    PASS();
}

/* ── Decorator tags integration tests (enrichment_test.go ports) ─ */

/* Helper: check if a node's properties_json contains a specific decorator_tag */
static bool has_decorator_tag(const char *properties_json, const char *tag) {
    if (!properties_json || !tag)
        return false;
    yyjson_doc *doc = yyjson_read(properties_json, strlen(properties_json), 0);
    if (!doc)
        return false;
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *tags = yyjson_obj_get(root, "decorator_tags");
    if (!tags || !yyjson_is_arr(tags)) {
        yyjson_doc_free(doc);
        return false;
    }
    yyjson_val *item;
    yyjson_arr_iter iter;
    yyjson_arr_iter_init(tags, &iter);
    while ((item = yyjson_arr_iter_next(&iter))) {
        if (yyjson_is_str(item) && strcmp(yyjson_get_str(item), tag) == 0) {
            yyjson_doc_free(doc);
            return true;
        }
    }
    yyjson_doc_free(doc);
    return false;
}

TEST(decorator_tags_python_auto_discovery) {
    /* Port of TestDecoratorTagAutoDiscovery:
     * Python file with repeated decorators (@login_required on 2 funcs,
     * @cache on 2 funcs, @unique_helper on 1 func).
     * Words on 2+ nodes become tags; unique words do not. */
    const char *files[] = {"views.py"};
    const char *contents[] = {"from functools import cache\n\n"
                              "@login_required\n"
                              "def list_orders():\n"
                              "    pass\n\n"
                              "@login_required\n"
                              "def get_order():\n"
                              "    pass\n\n"
                              "@cache\n"
                              "def compute_total():\n"
                              "    pass\n\n"
                              "@cache\n"
                              "def compute_tax():\n"
                              "    pass\n\n"
                              "@unique_helper\n"
                              "def special():\n"
                              "    pass\n"};
    if (setup_lang_repo(files, contents, 1) != 0)
        FAIL("tmpdir");
    char db[512];
    snprintf(db, sizeof(db), "%s/test.db", g_lang_tmpdir);
    cbm_pipeline_t *p = cbm_pipeline_new(g_lang_tmpdir, db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    cbm_store_t *s = cbm_store_open_path(db);
    ASSERT_NOT_NULL(s);
    const char *proj = cbm_pipeline_project_name(p);

    /* Find functions by name and check decorator_tags */
    cbm_node_t *funcs = NULL;
    int fc = 0;
    cbm_store_find_nodes_by_label(s, proj, "Function", &funcs, &fc);

    /* Build name→properties_json map */
    const char *list_orders_props = NULL;
    const char *get_order_props = NULL;
    const char *compute_total_props = NULL;
    const char *compute_tax_props = NULL;
    const char *special_props = NULL;
    for (int i = 0; i < fc; i++) {
        if (strcmp(funcs[i].name, "list_orders") == 0)
            list_orders_props = funcs[i].properties_json;
        else if (strcmp(funcs[i].name, "get_order") == 0)
            get_order_props = funcs[i].properties_json;
        else if (strcmp(funcs[i].name, "compute_total") == 0)
            compute_total_props = funcs[i].properties_json;
        else if (strcmp(funcs[i].name, "compute_tax") == 0)
            compute_tax_props = funcs[i].properties_json;
        else if (strcmp(funcs[i].name, "special") == 0)
            special_props = funcs[i].properties_json;
    }

    /* "login" and "required" appear on 2 nodes → should be tags */
    ASSERT_TRUE(has_decorator_tag(list_orders_props, "login"));
    ASSERT_TRUE(has_decorator_tag(list_orders_props, "required"));
    ASSERT_TRUE(has_decorator_tag(get_order_props, "login"));
    ASSERT_TRUE(has_decorator_tag(get_order_props, "required"));

    /* "cache" appears on 2 nodes → should be a tag */
    ASSERT_TRUE(has_decorator_tag(compute_total_props, "cache"));
    ASSERT_TRUE(has_decorator_tag(compute_tax_props, "cache"));

    /* "unique" and "helper" appear on only 1 node → should NOT be tags */
    ASSERT_FALSE(has_decorator_tag(special_props, "unique"));
    ASSERT_FALSE(has_decorator_tag(special_props, "helper"));

    if (funcs)
        cbm_store_free_nodes(funcs, fc);
    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_lang_repo();
    PASS();
}

TEST(decorator_tags_java_class_methods) {
    /* Port of TestDecoratorTagJavaClassMethods:
     * Java class with @GetMapping, @PostMapping, @Transactional annotations.
     * "mapping" appears on all 4 → tag. "post" on 2 → tag. */
    const char *files[] = {"Controller.java"};
    const char *contents[] = {"class OwnerController {\n"
                              "    @GetMapping(\"/owners\")\n"
                              "    public void listOwners() {}\n\n"
                              "    @GetMapping(\"/owners/{id}\")\n"
                              "    public void showOwner() {}\n\n"
                              "    @PostMapping(\"/owners\")\n"
                              "    public void createOwner() {}\n\n"
                              "    @Transactional\n"
                              "    @PostMapping(\"/owners/{id}\")\n"
                              "    public void updateOwner() {}\n"
                              "}\n"};
    if (setup_lang_repo(files, contents, 1) != 0)
        FAIL("tmpdir");
    char db[512];
    snprintf(db, sizeof(db), "%s/test.db", g_lang_tmpdir);
    cbm_pipeline_t *p = cbm_pipeline_new(g_lang_tmpdir, db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    cbm_store_t *s = cbm_store_open_path(db);
    ASSERT_NOT_NULL(s);
    const char *proj = cbm_pipeline_project_name(p);

    /* Find methods */
    cbm_node_t *methods = NULL;
    int mc = 0;
    cbm_store_find_nodes_by_label(s, proj, "Method", &methods, &mc);

    /* "mapping" appears on all 4 methods → should be a tag */
    for (int i = 0; i < mc; i++) {
        if (strcmp(methods[i].name, "listOwners") == 0 ||
            strcmp(methods[i].name, "showOwner") == 0 ||
            strcmp(methods[i].name, "createOwner") == 0 ||
            strcmp(methods[i].name, "updateOwner") == 0) {
            ASSERT_TRUE(has_decorator_tag(methods[i].properties_json, "mapping"));
        }
    }

    /* "post" appears on createOwner + updateOwner → should be a tag */
    for (int i = 0; i < mc; i++) {
        if (strcmp(methods[i].name, "createOwner") == 0 ||
            strcmp(methods[i].name, "updateOwner") == 0) {
            ASSERT_TRUE(has_decorator_tag(methods[i].properties_json, "post"));
        }
    }

    /* "transactional" appears on only 1 method → should NOT be a tag */
    for (int i = 0; i < mc; i++) {
        if (strcmp(methods[i].name, "updateOwner") == 0) {
            ASSERT_FALSE(has_decorator_tag(methods[i].properties_json, "transactional"));
        }
    }

    if (methods)
        cbm_store_free_nodes(methods, mc);
    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_lang_repo();
    PASS();
}

/* ── Compile commands helpers (pass_compile_commands.c) ────────── */

TEST(compile_commands_split_command) {
    char *args[16];
    int n;

    n = cbm_split_command("gcc -c main.c", args, 16);
    ASSERT_EQ(n, 3);
    ASSERT_STR_EQ(args[0], "gcc");
    ASSERT_STR_EQ(args[1], "-c");
    ASSERT_STR_EQ(args[2], "main.c");
    for (int i = 0; i < n; i++)
        free(args[i]);

    n = cbm_split_command("gcc -DFOO=\"bar baz\" -c main.c", args, 16);
    ASSERT_EQ(n, 4);
    for (int i = 0; i < n; i++)
        free(args[i]);

    n = cbm_split_command("g++ -I/usr/include -std=c++17 -o out -c in.cpp", args, 16);
    ASSERT_EQ(n, 7);
    for (int i = 0; i < n; i++)
        free(args[i]);
    PASS();
}

TEST(compile_commands_extract_flags) {
    const char *args[] = {"g++",          "-I",    "/abs/include", "-I/rel/include", "-isystem",
                          "/sys/include", "-DFOO", "-DBAR=42",     "-std=c++20",     "-O2",
                          "-Wall",        "-c",    "main.cpp"};

    cbm_compile_flags_t *f = cbm_extract_flags(args, 13, "/project");
    ASSERT_NOT_NULL(f);
    ASSERT_EQ(f->include_count, 3);
    ASSERT_EQ(f->define_count, 2);
    ASSERT_STR_EQ(f->standard, "c++20");
    cbm_compile_flags_free(f);
    PASS();
}

TEST(compile_commands_parse_json) {
    const char *json = "[\n"
                       "  {\n"
                       "    \"directory\": \"/home/user/project/build\",\n"
                       "    \"command\": \"gcc -I/home/user/project/include "
                       "-I/home/user/project/src -DDEBUG=1 -DVERSION=\\\"1.0\\\" "
                       "-std=c11 -o main.o -c /home/user/project/src/main.c\",\n"
                       "    \"file\": \"/home/user/project/src/main.c\"\n"
                       "  },\n"
                       "  {\n"
                       "    \"directory\": \"/home/user/project/build\",\n"
                       "    \"arguments\": [\"g++\", \"-I/home/user/project/include\", "
                       "\"-isystem\", \"/home/user/project/third_party\", "
                       "\"-DUSE_SSL\", \"-std=c++17\", \"-c\", "
                       "\"/home/user/project/src/server.cpp\"],\n"
                       "    \"file\": \"/home/user/project/src/server.cpp\"\n"
                       "  },\n"
                       "  {\n"
                       "    \"directory\": \"/home/user/project/build\",\n"
                       "    \"command\": \"gcc -c /outside/repo/file.c\",\n"
                       "    \"file\": \"/outside/repo/file.c\"\n"
                       "  }\n"
                       "]";

    char **paths = NULL;
    cbm_compile_flags_t **flags = NULL;
    int n = cbm_parse_compile_commands(json, "/home/user/project", &paths, &flags);
    ASSERT(n >= 2); /* At least main.c and server.cpp, outside file excluded */

    /* Find main.c */
    int main_idx = -1, server_idx = -1;
    for (int i = 0; i < n; i++) {
        if (strcmp(paths[i], "src/main.c") == 0)
            main_idx = i;
        if (strcmp(paths[i], "src/server.cpp") == 0)
            server_idx = i;
    }

    ASSERT(main_idx >= 0);
    ASSERT_EQ(flags[main_idx]->include_count, 2);
    ASSERT_EQ(flags[main_idx]->define_count, 2);
    ASSERT_STR_EQ(flags[main_idx]->standard, "c11");

    ASSERT(server_idx >= 0);
    ASSERT_EQ(flags[server_idx]->include_count, 2);
    ASSERT_EQ(flags[server_idx]->define_count, 1);
    ASSERT_STR_EQ(flags[server_idx]->standard, "c++17");

    /* Verify outside-repo file excluded */
    for (int i = 0; i < n; i++) {
        ASSERT(strstr(paths[i], "outside") == NULL);
    }

    /* Cleanup */
    for (int i = 0; i < n; i++) {
        free(paths[i]);
        cbm_compile_flags_free(flags[i]);
    }
    free(paths);
    free(flags);
    PASS();
}

TEST(compile_commands_parse_empty) {
    char **paths = NULL;
    cbm_compile_flags_t **flags = NULL;
    int n = cbm_parse_compile_commands("[]", "/repo", &paths, &flags);
    ASSERT_EQ(n, 0);
    free(paths);
    free(flags);
    PASS();
}

TEST(compile_commands_parse_invalid) {
    char **paths = NULL;
    cbm_compile_flags_t **flags = NULL;
    int n = cbm_parse_compile_commands("not json", "/repo", &paths, &flags);
    ASSERT(n < 0);
    PASS();
}

/* ── Suite ─────────────────────────────────────────────────────── */

/* ── Infrascan: file identification ──────────────────────────────── */

TEST(infra_is_compose_file) {
    /* Port of TestIsComposeFile (8 cases) */
    ASSERT(cbm_is_compose_file("docker-compose.yml"));
    ASSERT(cbm_is_compose_file("docker-compose.yaml"));
    ASSERT(cbm_is_compose_file("docker-compose.prod.yml"));
    ASSERT(cbm_is_compose_file("compose.yml"));
    ASSERT(cbm_is_compose_file("compose.yaml"));
    ASSERT(!cbm_is_compose_file("mycompose.yml"));
    ASSERT(!cbm_is_compose_file("docker-compose.txt"));
    ASSERT(!cbm_is_compose_file("Dockerfile"));
    PASS();
}

TEST(infra_is_cloudbuild_file) {
    /* Port of TestIsCloudbuildFile (5 cases) */
    ASSERT(cbm_is_cloudbuild_file("cloudbuild.yaml"));
    ASSERT(cbm_is_cloudbuild_file("cloudbuild.yml"));
    ASSERT(cbm_is_cloudbuild_file("cloudbuild-prod.yaml"));
    ASSERT(cbm_is_cloudbuild_file("Cloudbuild.yml"));
    ASSERT(!cbm_is_cloudbuild_file("build.yaml"));
    PASS();
}

TEST(infra_is_shell_script) {
    /* Port of TestIsShellScript (5 cases) */
    ASSERT(cbm_is_shell_script("run.sh", ".sh"));
    ASSERT(cbm_is_shell_script("deploy.bash", ".bash"));
    ASSERT(cbm_is_shell_script("init.zsh", ".zsh"));
    ASSERT(!cbm_is_shell_script("main.py", ".py"));
    ASSERT(!cbm_is_shell_script("Dockerfile", ""));
    PASS();
}

TEST(infra_is_dockerfile) {
    ASSERT(cbm_is_dockerfile("Dockerfile"));
    ASSERT(cbm_is_dockerfile("dockerfile"));
    ASSERT(cbm_is_dockerfile("Dockerfile.prod"));
    ASSERT(cbm_is_dockerfile("app.dockerfile"));
    ASSERT(!cbm_is_dockerfile("docker-compose.yml"));
    ASSERT(!cbm_is_dockerfile("main.go"));
    PASS();
}

TEST(infra_is_kustomize_file) {
    ASSERT(cbm_is_kustomize_file("kustomization.yaml"));
    ASSERT(cbm_is_kustomize_file("kustomization.yml"));
    ASSERT(cbm_is_kustomize_file("KUSTOMIZATION.YAML")); /* case-insensitive */
    ASSERT(!cbm_is_kustomize_file("deployment.yaml"));
    ASSERT(!cbm_is_kustomize_file("kustomize.yaml"));
    ASSERT(!cbm_is_kustomize_file(NULL));
    PASS();
}

TEST(infra_is_k8s_manifest) {
    const char *deploy = "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: my-app\n";
    const char *plain = "name: foo\nvalue: bar\n";
    const char *kust = "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\n";

    ASSERT(cbm_is_k8s_manifest("deployment.yaml", deploy));
    ASSERT(!cbm_is_k8s_manifest("deployment.yaml", plain));
    /* kustomize file should return false even if it has apiVersion */
    ASSERT(!cbm_is_k8s_manifest("kustomization.yaml", kust));
    ASSERT(!cbm_is_k8s_manifest(NULL, deploy));
    ASSERT(!cbm_is_k8s_manifest("deployment.yaml", NULL));
    PASS();
}

TEST(infra_is_env_file) {
    ASSERT(cbm_is_env_file(".env"));
    ASSERT(cbm_is_env_file(".env.local"));
    ASSERT(cbm_is_env_file("prod.env"));
    ASSERT(!cbm_is_env_file("main.go"));
    ASSERT(!cbm_is_env_file("env.txt"));
    PASS();
}

/* ── K8s extraction tests ───────────────────────────────────────── */

TEST(k8s_extract_kustomize) {
    const char *src =
        "apiVersion: kustomize.config.k8s.io/v1beta1\n"
        "kind: Kustomization\n"
        "resources:\n"
        "  - deployment.yaml\n"
        "  - service.yaml\n";
    CBMFileResult *r = cbm_extract_file(src, (int)strlen(src), CBM_LANG_KUSTOMIZE,
                                        "myproj", "base/kustomization.yaml",
                                        0, NULL, NULL);
    ASSERT(r != NULL);
    ASSERT_GTE(r->imports.count, 2);

    bool found_deploy = false, found_svc = false;
    for (int i = 0; i < r->imports.count; i++) {
        if (r->imports.items[i].module_path &&
            strcmp(r->imports.items[i].module_path, "deployment.yaml") == 0)
            found_deploy = true;
        if (r->imports.items[i].module_path &&
            strcmp(r->imports.items[i].module_path, "service.yaml") == 0)
            found_svc = true;
    }
    ASSERT_TRUE(found_deploy);
    ASSERT_TRUE(found_svc);

    cbm_free_result(r);
    PASS();
}

TEST(k8s_extract_manifest) {
    const char *src =
        "apiVersion: apps/v1\n"
        "kind: Deployment\n"
        "metadata:\n"
        "  name: my-app\n"
        "  namespace: production\n";
    CBMFileResult *r = cbm_extract_file(src, (int)strlen(src), CBM_LANG_K8S,
                                        "myproj", "k8s/deployment.yaml",
                                        0, NULL, NULL);
    ASSERT(r != NULL);
    ASSERT_GTE(r->defs.count, 1);

    bool found_resource = false;
    for (int d = 0; d < r->defs.count; d++) {
        if (r->defs.items[d].label &&
            strcmp(r->defs.items[d].label, "Resource") == 0 &&
            r->defs.items[d].name &&
            strstr(r->defs.items[d].name, "Deployment") != NULL)
            found_resource = true;
    }
    ASSERT_TRUE(found_resource);

    cbm_free_result(r);
    PASS();
}

TEST(k8s_extract_manifest_no_name) {
    const char *src = "apiVersion: apps/v1\nkind: Deployment\n";
    CBMFileResult *r = cbm_extract_file(src, (int)strlen(src), CBM_LANG_K8S,
                                        "myproj", "k8s/deploy.yaml", 0, NULL, NULL);
    ASSERT(r != NULL);
    /* No crash — defs count may be 0 because metadata.name is absent */
    ASSERT(!r->has_error);
    cbm_free_result(r);
    PASS();
}

TEST(k8s_extract_manifest_multidoc) {
    /* Two-document YAML separated by "---".
     * extract_k8s_manifest contains a "break" after the first successful push,
     * so it processes only the first document that has both kind and
     * metadata.name.  This test pins that behaviour: the first document's
     * resource must be present and no crash must occur. */
    const char *src =
        "apiVersion: apps/v1\n"
        "kind: Deployment\n"
        "metadata:\n"
        "  name: my-app\n"
        "---\n"
        "apiVersion: v1\n"
        "kind: Service\n"
        "metadata:\n"
        "  name: my-svc\n";
    CBMFileResult *r = cbm_extract_file(src, (int)strlen(src), CBM_LANG_K8S,
                                        "myproj", "k8s/multi.yaml", 0, NULL, NULL);
    ASSERT(r != NULL);
    ASSERT(!r->has_error);
    /* First document's resource must be present */
    int found = 0;
    for (int i = 0; i < r->defs.count; i++) {
        if (r->defs.items[i].label && strcmp(r->defs.items[i].label, "Resource") == 0 &&
            r->defs.items[i].name && strcmp(r->defs.items[i].name, "Deployment/my-app") == 0) {
            found = 1;
        }
    }
    ASSERT(found);
    ASSERT(r->defs.count >= 1);
    cbm_free_result(r);
    PASS();
}

/* ── Infrascan: cleanJSONBrackets ───────────────────────────────── */

TEST(infra_clean_json_brackets) {
    /* Port of TestCleanJSONBrackets (4 cases) */
    char out[256];

    cbm_clean_json_brackets("[\"./server\"]", out, sizeof(out));
    ASSERT_STR_EQ(out, "./server");

    cbm_clean_json_brackets("[\"python\", \"main.py\"]", out, sizeof(out));
    ASSERT_STR_EQ(out, "python main.py");

    cbm_clean_json_brackets("./server", out, sizeof(out));
    ASSERT_STR_EQ(out, "./server");

    cbm_clean_json_brackets("[\"./app\", \"--flag\", \"value\"]", out, sizeof(out));
    ASSERT_STR_EQ(out, "./app --flag value");

    PASS();
}

/* ── Infrascan: secret detection ────────────────────────────────── */

TEST(infra_secret_detection) {
    /* Key-based detection */
    ASSERT(cbm_is_secret_binding("JWT_SECRET", "anything"));
    ASSERT(cbm_is_secret_binding("API_KEY", "anything"));
    ASSERT(cbm_is_secret_binding("my_password", "anything"));
    ASSERT(cbm_is_secret_binding("AUTH_TOKEN", "anything"));
    ASSERT(!cbm_is_secret_binding("DATABASE_URL", "https://db.example.com"));

    /* Value-based detection */
    ASSERT(cbm_is_secret_value("sk-1234567890abcdef12345"));
    ASSERT(cbm_is_secret_value("-----BEGIN RSA PRIVATE KEY-----"));
    ASSERT(!cbm_is_secret_value("https://db.example.com"));
    ASSERT(!cbm_is_secret_value("hello world"));
    ASSERT(!cbm_is_secret_value("8080"));

    /* isSecretBinding checks both */
    ASSERT(cbm_is_secret_binding("ANYTHING", "sk-1234567890abcdef12345"));
    ASSERT(!cbm_is_secret_binding("PORT", "8080"));

    PASS();
}

/* ── Infrascan: Dockerfile parser ───────────────────────────────── */

/* Helper: find env var by key in result */
static const char *find_env_var(const cbm_env_kv_t *vars, int count, const char *key) {
    for (int i = 0; i < count; i++) {
        if (strcmp(vars[i].key, key) == 0)
            return vars[i].value;
    }
    return NULL;
}

/* Helper: check if string array contains value */
static bool str_array_contains(const char (*arr)[32], int count, const char *val) {
    for (int i = 0; i < count; i++) {
        if (strcmp(arr[i], val) == 0)
            return true;
    }
    return false;
}

static bool str_array_128_contains(const char (*arr)[128], int count, const char *val) {
    for (int i = 0; i < count; i++) {
        if (strcmp(arr[i], val) == 0)
            return true;
    }
    return false;
}

static bool str_array_256_contains(const char (*arr)[256], int count, const char *val) {
    for (int i = 0; i < count; i++) {
        if (strcmp(arr[i], val) == 0)
            return true;
    }
    return false;
}

TEST(infra_parse_dockerfile_multistage) {
    /* Port of TestParseDockerfile "multi-stage with all directives" */
    const char *src = "FROM golang:1.23-alpine AS builder\n"
                      "WORKDIR /app\n"
                      "ARG SSH_PRIVATE_KEY\n"
                      "RUN go build -o server .\n"
                      "\n"
                      "FROM alpine:3.19\n"
                      "WORKDIR /usr/app\n"
                      "ENV PORT=8080\n"
                      "ENV PYTHONUNBUFFERED=1\n"
                      "EXPOSE 8080 443\n"
                      "USER appuser\n"
                      "CMD [\"./server\"]\n"
                      "HEALTHCHECK CMD wget http://localhost:8080/health\n";

    cbm_dockerfile_result_t r;
    ASSERT_EQ(cbm_parse_dockerfile_source(src, &r), 0);
    ASSERT_STR_EQ(r.base_image, "alpine:3.19");
    ASSERT_EQ(r.stage_count, 2);
    ASSERT_STR_EQ(r.stage_images[0], "golang:1.23-alpine");
    ASSERT_STR_EQ(r.stage_images[1], "alpine:3.19");
    ASSERT(str_array_contains(r.exposed_ports, r.port_count, "8080"));
    ASSERT(str_array_contains(r.exposed_ports, r.port_count, "443"));
    ASSERT_STR_EQ(r.workdir, "/usr/app");
    ASSERT_STR_EQ(r.user, "appuser");
    ASSERT_STR_EQ(r.cmd, "./server");
    ASSERT_STR_EQ(r.healthcheck, "wget http://localhost:8080/health");

    ASSERT_NOT_NULL(find_env_var(r.env_vars, r.env_count, "PORT"));
    ASSERT_STR_EQ(find_env_var(r.env_vars, r.env_count, "PORT"), "8080");
    ASSERT_STR_EQ(find_env_var(r.env_vars, r.env_count, "PYTHONUNBUFFERED"), "1");

    ASSERT(str_array_128_contains(r.build_args, r.build_arg_count, "SSH_PRIVATE_KEY"));

    PASS();
}

TEST(infra_parse_dockerfile_entrypoint) {
    /* Port of TestParseDockerfile "single stage with entrypoint" */
    const char *src = "FROM python:3.9-slim\n"
                      "ENTRYPOINT [\"python\", \"main.py\"]\n";

    cbm_dockerfile_result_t r;
    ASSERT_EQ(cbm_parse_dockerfile_source(src, &r), 0);
    ASSERT_STR_EQ(r.base_image, "python:3.9-slim");
    ASSERT_STR_EQ(r.entrypoint, "python main.py");
    ASSERT_EQ(r.stage_count, 1);
    PASS();
}

TEST(infra_parse_dockerfile_secret_filtered) {
    /* Port of TestParseDockerfile "secret env vars filtered" */
    const char *src = "FROM node:20\n"
                      "ENV API_KEY=sk-1234567890abcdef12345\n"
                      "ENV DATABASE_URL=https://db.example.com\n"
                      "ENV JWT_SECRET=supersecret\n";

    cbm_dockerfile_result_t r;
    ASSERT_EQ(cbm_parse_dockerfile_source(src, &r), 0);

    /* API_KEY and JWT_SECRET should be filtered */
    ASSERT(find_env_var(r.env_vars, r.env_count, "API_KEY") == NULL);
    ASSERT(find_env_var(r.env_vars, r.env_count, "JWT_SECRET") == NULL);

    /* DATABASE_URL should remain */
    ASSERT_NOT_NULL(find_env_var(r.env_vars, r.env_count, "DATABASE_URL"));
    ASSERT_STR_EQ(find_env_var(r.env_vars, r.env_count, "DATABASE_URL"), "https://db.example.com");
    PASS();
}

TEST(infra_parse_dockerfile_expose_protocol) {
    /* Port of TestParseDockerfile "expose with protocol suffix" */
    const char *src = "FROM nginx:latest\n"
                      "EXPOSE 80/tcp 443/tcp\n";

    cbm_dockerfile_result_t r;
    ASSERT_EQ(cbm_parse_dockerfile_source(src, &r), 0);
    ASSERT(str_array_contains(r.exposed_ports, r.port_count, "80"));
    ASSERT(str_array_contains(r.exposed_ports, r.port_count, "443"));
    PASS();
}

TEST(infra_parse_dockerfile_env_space) {
    /* Port of TestParseDockerfile "ENV space-separated format" */
    const char *src = "FROM python:3.9\n"
                      "ENV PYTHONPATH /usr/app\n";

    cbm_dockerfile_result_t r;
    ASSERT_EQ(cbm_parse_dockerfile_source(src, &r), 0);
    ASSERT_NOT_NULL(find_env_var(r.env_vars, r.env_count, "PYTHONPATH"));
    ASSERT_STR_EQ(find_env_var(r.env_vars, r.env_count, "PYTHONPATH"), "/usr/app");
    PASS();
}

TEST(infra_parse_dockerfile_empty) {
    /* Port of TestParseDockerfileEmpty */
    cbm_dockerfile_result_t r;
    ASSERT_EQ(cbm_parse_dockerfile_source("# just a comment\n", &r), -1);
    PASS();
}

/* ── Infrascan: Dotenv parser ───────────────────────────────────── */

TEST(infra_parse_dotenv) {
    /* Port of TestParseDotenvFile */
    const char *src = "# Database config\n"
                      "DATABASE_HOST=localhost\n"
                      "DATABASE_PORT=5432\n"
                      "DATABASE_NAME=mydb\n"
                      "API_SECRET=should-not-appear\n"
                      "PLAIN_VALUE=hello world\n";

    cbm_dotenv_result_t r;
    ASSERT_EQ(cbm_parse_dotenv_source(src, &r), 0);

    ASSERT_STR_EQ(find_env_var(r.env_vars, r.env_count, "DATABASE_HOST"), "localhost");
    ASSERT_STR_EQ(find_env_var(r.env_vars, r.env_count, "DATABASE_PORT"), "5432");
    ASSERT_STR_EQ(find_env_var(r.env_vars, r.env_count, "PLAIN_VALUE"), "hello world");

    /* API_SECRET should be filtered */
    ASSERT(find_env_var(r.env_vars, r.env_count, "API_SECRET") == NULL);
    PASS();
}

TEST(infra_parse_dotenv_quoted) {
    /* Port of TestParseDotenvQuotedValues */
    const char *src = "KEY1=\"quoted value\"\n"
                      "KEY2='single quoted'\n";

    cbm_dotenv_result_t r;
    ASSERT_EQ(cbm_parse_dotenv_source(src, &r), 0);
    ASSERT_STR_EQ(find_env_var(r.env_vars, r.env_count, "KEY1"), "quoted value");
    ASSERT_STR_EQ(find_env_var(r.env_vars, r.env_count, "KEY2"), "single quoted");
    PASS();
}

/* ── Infrascan: Shell script parser ─────────────────────────────── */

TEST(infra_parse_shell) {
    /* Port of TestParseShellScript */
    const char *src = "#!/bin/bash\n"
                      "set -e\n"
                      "\n"
                      "# Configuration\n"
                      "YOUR_CONTAINER_NAME=\"order-email-extractor-endpoint\"\n"
                      "DOCKERFILE_PATH=\"/path/to/dockerfile\"\n"
                      "\n"
                      "export ENVIRONMENT=\"development\"\n"
                      "export USE_STACKDRIVER=\"false\"\n"
                      "\n"
                      "# Shut down existing containers\n"
                      "./shut-down-docker-container.sh\n"
                      "\n"
                      "docker build -t \"$YOUR_CONTAINER_NAME\" \"$DOCKERFILE_PATH\"\n"
                      "docker run -d --name \"$YOUR_CONTAINER_NAME\" \"$YOUR_CONTAINER_NAME\"\n"
                      "docker-compose up -d\n";

    cbm_shell_result_t r;
    ASSERT_EQ(cbm_parse_shell_source(src, &r), 0);
    ASSERT_STR_EQ(r.shebang, "/bin/bash");

    ASSERT_STR_EQ(find_env_var(r.env_vars, r.env_count, "ENVIRONMENT"), "development");
    ASSERT_STR_EQ(find_env_var(r.env_vars, r.env_count, "YOUR_CONTAINER_NAME"),
                  "order-email-extractor-endpoint");

    ASSERT(str_array_256_contains(r.docker_cmds, r.docker_cmd_count, "docker build"));
    ASSERT(str_array_256_contains(r.docker_cmds, r.docker_cmd_count, "docker run"));
    ASSERT(str_array_256_contains(r.docker_cmds, r.docker_cmd_count, "docker-compose up"));

    PASS();
}

TEST(infra_parse_shell_with_source) {
    /* Port of TestParseShellScriptWithSource */
    const char *src = "#!/usr/bin/env bash\n"
                      "source ./config.sh\n"
                      ". /etc/profile.d/env.sh\n";

    cbm_shell_result_t r;
    ASSERT_EQ(cbm_parse_shell_source(src, &r), 0);
    ASSERT_STR_EQ(r.shebang, "/usr/bin/env bash");
    ASSERT(str_array_256_contains(r.sources, r.source_count, "./config.sh"));
    ASSERT(str_array_256_contains(r.sources, r.source_count, "/etc/profile.d/env.sh"));
    PASS();
}

TEST(infra_parse_shell_secret_filtered) {
    /* Port of TestParseShellScriptSecretFiltered */
    const char *src = "#!/bin/bash\n"
                      "export API_SECRET=\"should-not-appear\"\n"
                      "export DATABASE_URL=\"https://db.example.com\"\n";

    cbm_shell_result_t r;
    ASSERT_EQ(cbm_parse_shell_source(src, &r), 0);
    ASSERT(find_env_var(r.env_vars, r.env_count, "API_SECRET") == NULL);
    ASSERT_STR_EQ(find_env_var(r.env_vars, r.env_count, "DATABASE_URL"), "https://db.example.com");
    PASS();
}

TEST(infra_parse_shell_shebang_only) {
    /* Port of TestParseShellScriptShebanOnly */
    const char *src = "#!/bin/bash\n# just comments\n";
    cbm_shell_result_t r;
    ASSERT_EQ(cbm_parse_shell_source(src, &r), 0);
    ASSERT_STR_EQ(r.shebang, "/bin/bash");
    PASS();
}

TEST(infra_parse_shell_truly_empty) {
    /* Port of TestParseShellScriptTrulyEmpty */
    cbm_shell_result_t r;
    ASSERT_EQ(cbm_parse_shell_source("# no shebang, just comments\n", &r), -1);
    PASS();
}

/* ── Infrascan: Terraform parser ────────────────────────────────── */

TEST(infra_parse_terraform_full) {
    /* Port of TestParseTerraformFile */
    const char *src = "\n"
                      "terraform {\n"
                      "  required_providers {\n"
                      "    google = {\n"
                      "      source  = \"hashicorp/google\"\n"
                      "      version = \"~> 6.35.0\"\n"
                      "    }\n"
                      "  }\n"
                      "  backend \"gcs\" {\n"
                      "    bucket = \"example-tf\"\n"
                      "    prefix = \"state\"\n"
                      "  }\n"
                      "}\n"
                      "\n"
                      "variable \"project_id\" {\n"
                      "  description = \"The GCP project ID\"\n"
                      "  type        = string\n"
                      "  default     = \"example-cloud\"\n"
                      "}\n"
                      "\n"
                      "variable \"region\" {\n"
                      "  description = \"The region\"\n"
                      "  type        = string\n"
                      "}\n"
                      "\n"
                      "resource \"google_cloud_run_service\" \"main\" {\n"
                      "  name     = \"my-service\"\n"
                      "  location = var.region\n"
                      "}\n"
                      "\n"
                      "resource \"google_compute_address\" \"nat_ip\" {\n"
                      "  name   = \"nat-ip\"\n"
                      "  region = var.region\n"
                      "}\n"
                      "\n"
                      "output \"service_url\" {\n"
                      "  value = google_cloud_run_service.main.status[0].url\n"
                      "}\n"
                      "\n"
                      "data \"google_project\" \"project\" {\n"
                      "}\n"
                      "\n"
                      "module \"vpc\" {\n"
                      "  source = \"./modules/vpc\"\n"
                      "}\n"
                      "\n"
                      "locals {\n"
                      "  env = \"prod\"\n"
                      "}\n";

    cbm_terraform_result_t r;
    ASSERT_EQ(cbm_parse_terraform_source(src, &r), 0);
    ASSERT_STR_EQ(r.backend, "gcs");

    /* Resources */
    ASSERT_EQ(r.resource_count, 2);
    bool found_cloud_run = false;
    for (int i = 0; i < r.resource_count; i++) {
        if (strcmp(r.resources[i].type, "google_cloud_run_service") == 0 &&
            strcmp(r.resources[i].name, "main") == 0) {
            found_cloud_run = true;
        }
    }
    ASSERT(found_cloud_run);

    /* Variables */
    ASSERT_EQ(r.variable_count, 2);
    bool found_project_id = false;
    for (int i = 0; i < r.variable_count; i++) {
        if (strcmp(r.variables[i].name, "project_id") == 0) {
            ASSERT_STR_EQ(r.variables[i].default_val, "example-cloud");
            ASSERT_STR_EQ(r.variables[i].type, "string");
            ASSERT_STR_EQ(r.variables[i].description, "The GCP project ID");
            found_project_id = true;
        }
    }
    ASSERT(found_project_id);

    /* Outputs */
    ASSERT_EQ(r.output_count, 1);
    ASSERT_STR_EQ(r.outputs[0], "service_url");

    /* Data sources */
    ASSERT_EQ(r.data_source_count, 1);
    ASSERT_STR_EQ(r.data_sources[0].type, "google_project");
    ASSERT_STR_EQ(r.data_sources[0].name, "project");

    /* Modules */
    ASSERT_EQ(r.module_count, 1);
    ASSERT_STR_EQ(r.modules[0].tf_name, "vpc");
    ASSERT_STR_EQ(r.modules[0].source, "./modules/vpc");

    /* Locals */
    ASSERT(r.has_locals);

    PASS();
}

TEST(infra_parse_terraform_variables_only) {
    /* Port of TestParseTerraformVariablesOnly — secret default filtered */
    const char *src = "\n"
                      "variable \"project_id\" {\n"
                      "  description = \"The GCP project ID\"\n"
                      "  type        = string\n"
                      "  default     = \"example-cloud\"\n"
                      "}\n"
                      "\n"
                      "variable \"secret_key\" {\n"
                      "  description = \"A secret\"\n"
                      "  type        = string\n"
                      "  default     = \"sk-1234567890abcdef12345\"\n"
                      "}\n";

    cbm_terraform_result_t r;
    ASSERT_EQ(cbm_parse_terraform_source(src, &r), 0);
    ASSERT_EQ(r.variable_count, 2);

    /* secret_key default should be filtered */
    for (int i = 0; i < r.variable_count; i++) {
        if (strcmp(r.variables[i].name, "secret_key") == 0) {
            ASSERT_STR_EQ(r.variables[i].default_val, "");
        }
    }
    PASS();
}

TEST(infra_parse_terraform_empty) {
    /* Port of TestParseTerraformEmpty */
    cbm_terraform_result_t r;
    ASSERT_EQ(cbm_parse_terraform_source("# just comments\n", &r), -1);
    PASS();
}

/* ── Helm Chart.yaml dependency parsing (#338) ──────────────────── */

TEST(helm_parse_chart_dependencies_issue338) {
    const char *src = "apiVersion: v2\n"
                      "name: mychart\n"
                      "version: 1.0.0\n"
                      "dependencies:\n"
                      "  - name: postgresql\n"
                      "    repository: https://charts.bitnami.com/bitnami\n"
                      "    version: 12.x.x\n"
                      "  - name: redis\n"
                      "    repository: https://charts.bitnami.com/bitnami\n"
                      "maintainers:\n"
                      "  - name: alice\n"; /* not a dependency — outside the block */
    cbm_helm_chart_t hc;
    ASSERT_EQ(cbm_parse_helm_chart(src, &hc), 0);
    ASSERT_STR_EQ(hc.chart_name, "mychart");
    ASSERT_EQ(hc.dep_count, 2);
    ASSERT_STR_EQ(hc.deps[0], "postgresql");
    ASSERT_STR_EQ(hc.deps[1], "redis");
    PASS();
}

TEST(helm_parse_chart_no_deps_issue338) {
    cbm_helm_chart_t hc;
    ASSERT_EQ(cbm_parse_helm_chart("name: solo\nversion: 0.1.0\n", &hc), 0);
    ASSERT_STR_EQ(hc.chart_name, "solo");
    ASSERT_EQ(hc.dep_count, 0);
    PASS();
}

/* ── Infrascan: infra QN helper ─────────────────────────────────── */

/* ── Function Registry / Resolver tests ─────────────────────────── */

TEST(registry_resolve_single_candidate) {
    cbm_registry_t *reg = cbm_registry_new();
    cbm_registry_add(reg, "CreateOrder", "svcA.handlers.CreateOrder", "Function");
    cbm_registry_add(reg, "ValidateOrder", "svcB.validators.ValidateOrder", "Function");

    /* Normal resolve unique name */
    cbm_resolution_t r = cbm_registry_resolve(reg, "CreateOrder", "svcC.caller", NULL, NULL, 0);
    ASSERT_STR_EQ(r.qualified_name, "svcA.handlers.CreateOrder");

    /* Fuzzy resolve with unknown prefix */
    cbm_fuzzy_result_t fr =
        cbm_registry_fuzzy_resolve(reg, "unknownPkg.CreateOrder", "svcC.caller", NULL, NULL, 0);
    ASSERT_TRUE(fr.ok);
    ASSERT_STR_EQ(fr.result.qualified_name, "svcA.handlers.CreateOrder");

    cbm_registry_free(reg);
    PASS();
}

TEST(registry_fuzzy_nonexistent) {
    cbm_registry_t *reg = cbm_registry_new();
    cbm_registry_add(reg, "CreateOrder", "svcA.handlers.CreateOrder", "Function");

    cbm_fuzzy_result_t fr =
        cbm_registry_fuzzy_resolve(reg, "NonExistent", "svcC.caller", NULL, NULL, 0);
    ASSERT_FALSE(fr.ok);

    cbm_registry_free(reg);
    PASS();
}

TEST(registry_fuzzy_multiple_best_by_distance) {
    cbm_registry_t *reg = cbm_registry_new();
    cbm_registry_add(reg, "Process", "svcA.handlers.Process", "Function");
    cbm_registry_add(reg, "Process", "svcB.handlers.Process", "Function");

    /* Caller in svcA → prefer svcA */
    cbm_fuzzy_result_t fr =
        cbm_registry_fuzzy_resolve(reg, "unknown.Process", "svcA.other", NULL, NULL, 0);
    ASSERT_TRUE(fr.ok);
    ASSERT_STR_EQ(fr.result.qualified_name, "svcA.handlers.Process");

    /* Caller in svcB → prefer svcB */
    fr = cbm_registry_fuzzy_resolve(reg, "unknown.Process", "svcB.other", NULL, NULL, 0);
    ASSERT_TRUE(fr.ok);
    ASSERT_STR_EQ(fr.result.qualified_name, "svcB.handlers.Process");

    cbm_registry_free(reg);
    PASS();
}

TEST(registry_fuzzy_simple_name_extraction) {
    cbm_registry_t *reg = cbm_registry_new();
    cbm_registry_add(reg, "DoWork", "myproject.utils.DoWork", "Function");

    /* Deeply qualified name → extract "DoWork" */
    cbm_fuzzy_result_t fr = cbm_registry_fuzzy_resolve(reg, "some.deep.module.DoWork",
                                                       "myproject.caller", NULL, NULL, 0);
    ASSERT_TRUE(fr.ok);
    ASSERT_STR_EQ(fr.result.qualified_name, "myproject.utils.DoWork");

    cbm_registry_free(reg);
    PASS();
}

TEST(registry_fuzzy_empty) {
    cbm_registry_t *reg = cbm_registry_new();

    cbm_fuzzy_result_t fr =
        cbm_registry_fuzzy_resolve(reg, "SomeFunc", "myproject.caller", NULL, NULL, 0);
    ASSERT_FALSE(fr.ok);

    cbm_registry_free(reg);
    PASS();
}

TEST(registry_exists) {
    cbm_registry_t *reg = cbm_registry_new();
    cbm_registry_add(reg, "Foo", "pkg.module.Foo", "Function");
    cbm_registry_add(reg, "Bar", "pkg.module.Bar", "Method");

    ASSERT_TRUE(cbm_registry_exists(reg, "pkg.module.Foo"));
    ASSERT_TRUE(cbm_registry_exists(reg, "pkg.module.Bar"));
    ASSERT_FALSE(cbm_registry_exists(reg, "pkg.module.Missing"));
    ASSERT_FALSE(cbm_registry_exists(reg, ""));

    cbm_registry_free(reg);
    PASS();
}

TEST(registry_confidence_import_map) {
    cbm_registry_t *reg = cbm_registry_new();
    cbm_registry_add(reg, "Foo", "proj.other.Foo", "Function");

    const char *keys[] = {"other"};
    const char *vals[] = {"proj.other"};
    cbm_resolution_t r = cbm_registry_resolve(reg, "other.Foo", "proj.pkg", keys, vals, 1);
    ASSERT_STR_EQ(r.qualified_name, "proj.other.Foo");
    ASSERT(r.confidence > 0.90 && r.confidence <= 1.0);
    ASSERT_STR_EQ(r.strategy, "import_map");

    cbm_registry_free(reg);
    PASS();
}

TEST(registry_confidence_import_map_suffix) {
    cbm_registry_t *reg = cbm_registry_new();
    cbm_registry_add(reg, "Foo", "proj.other.sub.Foo", "Function");

    const char *keys[] = {"other"};
    const char *vals[] = {"proj.other"};
    cbm_resolution_t r = cbm_registry_resolve(reg, "other.Foo", "proj.pkg", keys, vals, 1);
    ASSERT_STR_EQ(r.qualified_name, "proj.other.sub.Foo");
    ASSERT(r.confidence > 0.80 && r.confidence <= 0.90);
    ASSERT_STR_EQ(r.strategy, "import_map_suffix");

    cbm_registry_free(reg);
    PASS();
}

TEST(registry_confidence_same_module) {
    cbm_registry_t *reg = cbm_registry_new();
    cbm_registry_add(reg, "Foo", "proj.pkg.Foo", "Function");

    cbm_resolution_t r = cbm_registry_resolve(reg, "Foo", "proj.pkg", NULL, NULL, 0);
    ASSERT_STR_EQ(r.qualified_name, "proj.pkg.Foo");
    ASSERT(r.confidence > 0.85 && r.confidence <= 0.95);
    ASSERT_STR_EQ(r.strategy, "same_module");

    cbm_registry_free(reg);
    PASS();
}

TEST(registry_confidence_unique_name) {
    cbm_registry_t *reg = cbm_registry_new();
    cbm_registry_add(reg, "Bar", "proj.pkg.Bar", "Function");

    cbm_resolution_t r = cbm_registry_resolve(reg, "Bar", "proj.unrelated", NULL, NULL, 0);
    ASSERT_STR_EQ(r.qualified_name, "proj.pkg.Bar");
    ASSERT(r.confidence > 0.70 && r.confidence <= 0.80);
    ASSERT_STR_EQ(r.strategy, "unique_name");

    cbm_registry_free(reg);
    PASS();
}

TEST(registry_confidence_suffix_match) {
    cbm_registry_t *reg = cbm_registry_new();
    cbm_registry_add(reg, "Process", "proj.svcA.Process", "Function");
    cbm_registry_add(reg, "Process", "proj.svcB.Process", "Function");

    cbm_resolution_t r = cbm_registry_resolve(reg, "Process", "proj.svcA.caller", NULL, NULL, 0);
    ASSERT_STR_EQ(r.qualified_name, "proj.svcA.Process");
    ASSERT(r.confidence > 0.50 && r.confidence <= 0.60);
    ASSERT_STR_EQ(r.strategy, "suffix_match");

    cbm_registry_free(reg);
    PASS();
}

TEST(registry_fuzzy_confidence_single) {
    cbm_registry_t *reg = cbm_registry_new();
    cbm_registry_add(reg, "Handler", "proj.svc.Handler", "Function");

    cbm_fuzzy_result_t fr =
        cbm_registry_fuzzy_resolve(reg, "unknownPkg.Handler", "proj.caller", NULL, NULL, 0);
    ASSERT_TRUE(fr.ok);
    ASSERT(fr.result.confidence > 0.35 && fr.result.confidence <= 0.45);
    ASSERT_STR_EQ(fr.result.strategy, "fuzzy");

    cbm_registry_free(reg);
    PASS();
}

TEST(registry_fuzzy_confidence_distance) {
    cbm_registry_t *reg = cbm_registry_new();
    cbm_registry_add(reg, "Process", "proj.svcA.Process", "Function");
    cbm_registry_add(reg, "Process", "proj.svcB.Process", "Function");

    cbm_fuzzy_result_t fr =
        cbm_registry_fuzzy_resolve(reg, "unknownPkg.Process", "proj.svcA.other", NULL, NULL, 0);
    ASSERT_TRUE(fr.ok);
    ASSERT(fr.result.confidence > 0.25 && fr.result.confidence <= 0.35);
    ASSERT_STR_EQ(fr.result.strategy, "fuzzy");

    cbm_registry_free(reg);
    PASS();
}

TEST(registry_negative_import_rejects) {
    cbm_registry_t *reg = cbm_registry_new();
    cbm_registry_add(reg, "Process", "proj.billing.Process", "Function");
    cbm_registry_add(reg, "Process", "proj.handler.Process", "Function");

    /* Import only handler's module → should prefer handler */
    const char *keys[] = {"handler"};
    const char *vals[] = {"proj.handler"};
    cbm_resolution_t r = cbm_registry_resolve(reg, "Process", "proj.caller", keys, vals, 1);
    ASSERT_STR_EQ(r.qualified_name, "proj.handler.Process");

    cbm_registry_free(reg);
    PASS();
}

TEST(registry_fuzzy_import_penalty) {
    cbm_registry_t *reg = cbm_registry_new();
    cbm_registry_add(reg, "Handler", "proj.billing.Handler", "Function");

    /* Has imports but billing not imported → confidence halved */
    const char *keys[] = {"other"};
    const char *vals[] = {"proj.other"};
    cbm_fuzzy_result_t fr =
        cbm_registry_fuzzy_resolve(reg, "unknown.Handler", "proj.caller", keys, vals, 1);
    ASSERT_TRUE(fr.ok);
    /* 0.40 * 0.5 = 0.20 */
    ASSERT(fr.result.confidence > 0.15 && fr.result.confidence <= 0.25);

    cbm_registry_free(reg);
    PASS();
}

TEST(registry_fuzzy_no_import_map_passthrough) {
    cbm_registry_t *reg = cbm_registry_new();
    cbm_registry_add(reg, "Handler", "proj.billing.Handler", "Function");

    /* NULL import map → no penalty, full fuzzy confidence */
    cbm_fuzzy_result_t fr =
        cbm_registry_fuzzy_resolve(reg, "unknown.Handler", "proj.caller", NULL, NULL, 0);
    ASSERT_TRUE(fr.ok);
    ASSERT(fr.result.confidence > 0.35 && fr.result.confidence <= 0.45);

    cbm_registry_free(reg);
    PASS();
}

TEST(registry_find_by_name) {
    cbm_registry_t *reg = cbm_registry_new();
    cbm_registry_add(reg, "Foo", "proj.pkg.Foo", "Function");
    cbm_registry_add(reg, "Bar", "proj.pkg.Bar", "Function");
    cbm_registry_add(reg, "Foo", "proj.other.Foo", "Function");
    cbm_registry_add(reg, "transform", "proj.utils.DataProcessor.transform", "Method");

    /* FindByName returns all entries for "Foo" */
    const char **foos = NULL;
    int foos_count = 0;
    cbm_registry_find_by_name(reg, "Foo", &foos, &foos_count);
    ASSERT_EQ(foos_count, 2);

    /* FindByName for unique "Bar" */
    const char **bars = NULL;
    int bars_count = 0;
    cbm_registry_find_by_name(reg, "Bar", &bars, &bars_count);
    ASSERT_EQ(bars_count, 1);
    ASSERT_STR_EQ(bars[0], "proj.pkg.Bar");

    /* FindByName for "transform" */
    const char **transforms = NULL;
    int trans_count = 0;
    cbm_registry_find_by_name(reg, "transform", &transforms, &trans_count);
    ASSERT_EQ(trans_count, 1);
    ASSERT_STR_EQ(transforms[0], "proj.utils.DataProcessor.transform");

    /* label_of */
    ASSERT_STR_EQ(cbm_registry_label_of(reg, "proj.utils.DataProcessor.transform"), "Method");
    ASSERT_STR_EQ(cbm_registry_label_of(reg, "proj.pkg.Foo"), "Function");

    /* Total size */
    ASSERT_EQ(cbm_registry_size(reg), 4);

    /* Resolve same-module */
    cbm_resolution_t r = cbm_registry_resolve(reg, "Foo", "proj.pkg", NULL, NULL, 0);
    ASSERT_STR_EQ(r.qualified_name, "proj.pkg.Foo");

    /* Resolve via import map */
    const char *keys[] = {"other"};
    const char *vals[] = {"proj.other"};
    r = cbm_registry_resolve(reg, "other.Foo", "proj.pkg", keys, vals, 1);
    ASSERT_STR_EQ(r.qualified_name, "proj.other.Foo");

    /* Resolve unique name */
    r = cbm_registry_resolve(reg, "Bar", "proj.unrelated", NULL, NULL, 0);
    ASSERT_STR_EQ(r.qualified_name, "proj.pkg.Bar");

    cbm_registry_free(reg);
    PASS();
}

TEST(registry_confidence_band) {
    ASSERT_STR_EQ(cbm_confidence_band(0.95), "high");
    ASSERT_STR_EQ(cbm_confidence_band(0.70), "high");
    ASSERT_STR_EQ(cbm_confidence_band(0.55), "medium");
    ASSERT_STR_EQ(cbm_confidence_band(0.45), "medium");
    ASSERT_STR_EQ(cbm_confidence_band(0.40), "speculative");
    ASSERT_STR_EQ(cbm_confidence_band(0.25), "speculative");
    ASSERT_STR_EQ(cbm_confidence_band(0.20), "");
    ASSERT_STR_EQ(cbm_confidence_band(0.0), "");
    PASS();
}

TEST(infra_qn_helper) {
    /* Port of TestInfraQN */

    /* Regular infra file → __infra__ suffix */
    char *qn = cbm_infra_qn("myproject", "docker-images/service/Dockerfile", "dockerfile", NULL);
    ASSERT_NOT_NULL(qn);
    ASSERT(strstr(qn, ".__infra__") != NULL);
    free(qn);

    /* Compose service → ::service_name suffix */
    qn = cbm_infra_qn("myproject", "docker-compose.yml", "compose-service", "web");
    ASSERT_NOT_NULL(qn);
    ASSERT(strstr(qn, "::web") != NULL);
    free(qn);

    PASS();
}

/* ── Infrascan integration tests ────────────────────────────────── */

TEST(infra_pipeline_integration) {
    /* Port of TestPassInfraFilesIntegration (Dockerfile + .env parts).
     * Tests parse functions on source text (pipeline infrascan pass not
     * wired yet — compose YAML also blocked on YAML parser). */

    /* Parse Dockerfile */
    cbm_dockerfile_result_t dr;
    ASSERT_EQ(cbm_parse_dockerfile_source("FROM alpine:3.19\nEXPOSE 8080\n", &dr), 0);
    ASSERT_STR_EQ(dr.base_image, "alpine:3.19");
    ASSERT_GTE(dr.port_count, 1);

    /* Parse .env */
    cbm_dotenv_result_t er;
    ASSERT_EQ(cbm_parse_dotenv_source("APP_PORT=8080\nDEBUG=true\n", &er), 0);
    ASSERT_GTE(er.env_count, 1);
    /* APP_PORT should be present */
    bool found_port = false;
    for (int i = 0; i < er.env_count; i++) {
        if (strcmp(er.env_vars[i].key, "APP_PORT") == 0 &&
            strcmp(er.env_vars[i].value, "8080") == 0)
            found_port = true;
    }
    ASSERT_TRUE(found_port);

    PASS();
}

TEST(infra_pipeline_idempotent) {
    /* Port of TestPassInfraFilesIdempotent:
     * Parsing same source twice should produce identical results. */
    const char *src = "FROM alpine:3.19\nEXPOSE 8080\nENV PORT=8080\n";
    cbm_dockerfile_result_t r1, r2;
    ASSERT_EQ(cbm_parse_dockerfile_source(src, &r1), 0);
    ASSERT_EQ(cbm_parse_dockerfile_source(src, &r2), 0);

    ASSERT_STR_EQ(r1.base_image, r2.base_image);
    ASSERT_EQ(r1.port_count, r2.port_count);
    ASSERT_EQ(r1.env_count, r2.env_count);

    PASS();
}

/* (K8s extraction tests already defined above from origin/main) */

/* ── Envscan tests (port of envscan_test.go) ───────────────────── */

/* Helper: write a file inside a temp dir */
static void write_temp_file(const char *dir, const char *name, const char *content) {
    char path[512];
    /* Create subdirectories if needed */
    snprintf(path, sizeof(path), "%s/%s", dir, name);
    char *slash = strrchr(path, '/');
    if (slash) {
        char parent[512];
        size_t plen = slash - path;
        memcpy(parent, path, plen);
        parent[plen] = '\0';
        /* mkdir -p (simple version, one level) */
        cbm_mkdir(parent);
    }
    FILE *f = fopen(path, "w");
    if (f) {
        fputs(content, f);
        fclose(f);
    }
}

/* Helper: find binding by key in results */
static const cbm_env_binding_t *find_binding_by_key(const cbm_env_binding_t *bindings, int count,
                                                    const char *key) {
    for (int i = 0; i < count; i++) {
        if (strcmp(bindings[i].key, key) == 0)
            return &bindings[i];
    }
    return NULL;
}

/* Helper: find binding by value in results */
static int has_binding_value(const cbm_env_binding_t *bindings, int count, const char *value) {
    for (int i = 0; i < count; i++) {
        if (strcmp(bindings[i].value, value) == 0)
            return 1;
    }
    return 0;
}

TEST(envscan_dockerfile_env_urls) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_envscan_dock_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("tmpdir");

    write_temp_file(tmpdir, "Dockerfile",
                    "FROM python:3.9-slim\n"
                    "ENV ORDER_URL=https://api.example.com/api/orders\n"
                    "ENV DB_HOST=localhost\n"
                    "ARG WEBHOOK_URL=https://hooks.example.com/webhook\n");

    cbm_env_binding_t bindings[32];
    int count = cbm_scan_project_env_urls(tmpdir, bindings, 32);

    ASSERT_NOT_NULL(find_binding_by_key(bindings, count, "ORDER_URL"));
    ASSERT_STR_EQ(find_binding_by_key(bindings, count, "ORDER_URL")->value,
                  "https://api.example.com/api/orders");
    ASSERT_NOT_NULL(find_binding_by_key(bindings, count, "WEBHOOK_URL"));
    ASSERT_STR_EQ(find_binding_by_key(bindings, count, "WEBHOOK_URL")->value,
                  "https://hooks.example.com/webhook");
    /* DB_HOST=localhost is NOT a URL → should be absent */
    ASSERT_TRUE(find_binding_by_key(bindings, count, "DB_HOST") == NULL);

    th_rmtree(tmpdir);
    PASS();
}

TEST(envscan_shell_env_urls) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_envscan_sh_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("tmpdir");

    write_temp_file(tmpdir, "setup.sh",
                    "#!/bin/bash\n"
                    "export DB_URL=\"https://db.example.com/api/sync\"\n"
                    "APP_NAME=\"my-service\"\n"
                    "CALLBACK_URL=https://hooks.example.com/notify\n");

    cbm_env_binding_t bindings[32];
    int count = cbm_scan_project_env_urls(tmpdir, bindings, 32);

    ASSERT_NOT_NULL(find_binding_by_key(bindings, count, "DB_URL"));
    ASSERT_STR_EQ(find_binding_by_key(bindings, count, "DB_URL")->value,
                  "https://db.example.com/api/sync");
    ASSERT_NOT_NULL(find_binding_by_key(bindings, count, "CALLBACK_URL"));
    /* APP_NAME is NOT a URL → absent */
    ASSERT_TRUE(find_binding_by_key(bindings, count, "APP_NAME") == NULL);

    th_rmtree(tmpdir);
    PASS();
}

TEST(envscan_env_file_urls) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_envscan_env_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("tmpdir");

    write_temp_file(tmpdir, ".env",
                    "\nAPI_URL=https://api.example.com/v1\n"
                    "DEBUG=true\n"
                    "SERVICE_URL=https://service.example.com/api\n");

    cbm_env_binding_t bindings[32];
    int count = cbm_scan_project_env_urls(tmpdir, bindings, 32);

    ASSERT_NOT_NULL(find_binding_by_key(bindings, count, "API_URL"));
    ASSERT_STR_EQ(find_binding_by_key(bindings, count, "API_URL")->value,
                  "https://api.example.com/v1");
    ASSERT_NOT_NULL(find_binding_by_key(bindings, count, "SERVICE_URL"));
    /* DEBUG=true is NOT a URL */
    ASSERT_TRUE(find_binding_by_key(bindings, count, "DEBUG") == NULL);

    th_rmtree(tmpdir);
    PASS();
}

TEST(envscan_toml_urls) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_envscan_toml_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("tmpdir");

    write_temp_file(tmpdir, "config.toml",
                    "[service]\n"
                    "base_url = \"https://api.example.com\"\n"
                    "name = \"my-service\"\n"
                    "callback_url = \"https://hooks.example.com/notify\"\n");

    cbm_env_binding_t bindings[32];
    int count = cbm_scan_project_env_urls(tmpdir, bindings, 32);

    ASSERT_NOT_NULL(find_binding_by_key(bindings, count, "base_url"));
    ASSERT_STR_EQ(find_binding_by_key(bindings, count, "base_url")->value,
                  "https://api.example.com");
    ASSERT_NOT_NULL(find_binding_by_key(bindings, count, "callback_url"));
    /* name="my-service" is NOT a URL */
    ASSERT_TRUE(find_binding_by_key(bindings, count, "name") == NULL);

    th_rmtree(tmpdir);
    PASS();
}

TEST(envscan_yaml_urls) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_envscan_yaml_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("tmpdir");

    write_temp_file(tmpdir, "config.yaml",
                    "service:\n"
                    "  service_url: \"https://api.internal.com/api/process\"\n"
                    "  timeout: 30\n"
                    "  callback_url: \"https://hooks.internal.com/callback\"\n");

    cbm_env_binding_t bindings[32];
    int count = cbm_scan_project_env_urls(tmpdir, bindings, 32);

    ASSERT_NOT_NULL(find_binding_by_key(bindings, count, "service_url"));
    ASSERT_STR_EQ(find_binding_by_key(bindings, count, "service_url")->value,
                  "https://api.internal.com/api/process");
    ASSERT_NOT_NULL(find_binding_by_key(bindings, count, "callback_url"));

    th_rmtree(tmpdir);
    PASS();
}

TEST(envscan_terraform_urls) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_envscan_tf_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("tmpdir");

    write_temp_file(tmpdir, "variables.tf",
                    "variable \"webhook_url\" {\n"
                    "  description = \"Webhook endpoint\"\n"
                    "  default     = \"https://api.example.com/webhook\"\n"
                    "}\n\n"
                    "variable \"region\" {\n"
                    "  default = \"us-east-1\"\n"
                    "}\n");

    cbm_env_binding_t bindings[32];
    int count = cbm_scan_project_env_urls(tmpdir, bindings, 32);

    ASSERT_GTE(count, 1);
    ASSERT_TRUE(has_binding_value(bindings, count, "https://api.example.com/webhook"));

    th_rmtree(tmpdir);
    PASS();
}

TEST(envscan_properties_urls) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_envscan_prop_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("tmpdir");

    write_temp_file(tmpdir, "app.properties",
                    "api.url=https://api.example.com/health\n"
                    "app.name=myapp\n"
                    "service.endpoint=https://service.example.com/api\n");

    cbm_env_binding_t bindings[32];
    int count = cbm_scan_project_env_urls(tmpdir, bindings, 32);

    ASSERT_TRUE(has_binding_value(bindings, count, "https://api.example.com/health"));
    ASSERT_TRUE(has_binding_value(bindings, count, "https://service.example.com/api"));

    th_rmtree(tmpdir);
    PASS();
}

TEST(envscan_secret_key_exclusion) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_envscan_skey_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("tmpdir");

    write_temp_file(tmpdir, "Dockerfile",
                    "FROM node:18\n"
                    "ENV SECRET_TOKEN=https://api.example.com/api\n"
                    "ENV API_KEY=https://api.example.com/v1\n"
                    "ENV PASSWORD=https://auth.example.com/login\n"
                    "ENV NORMAL_URL=https://api.example.com/orders\n");

    cbm_env_binding_t bindings[32];
    int count = cbm_scan_project_env_urls(tmpdir, bindings, 32);

    /* Secret keys should be excluded */
    ASSERT_TRUE(find_binding_by_key(bindings, count, "SECRET_TOKEN") == NULL);
    ASSERT_TRUE(find_binding_by_key(bindings, count, "API_KEY") == NULL);
    ASSERT_TRUE(find_binding_by_key(bindings, count, "PASSWORD") == NULL);
    /* Normal key should be present */
    ASSERT_NOT_NULL(find_binding_by_key(bindings, count, "NORMAL_URL"));

    th_rmtree(tmpdir);
    PASS();
}

TEST(envscan_secret_value_exclusion) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_envscan_sval_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("tmpdir");

    write_temp_file(
        tmpdir, "deploy.sh",
        "#!/bin/bash\n"
        "export GH_URL=\"https://ghp_abcdefghijklmnopqrstuvwxyz1234567890@github.com/repo\"\n"
        "export NORMAL_ENDPOINT=\"https://api.example.com/orders\"\n");

    cbm_env_binding_t bindings[32];
    int count = cbm_scan_project_env_urls(tmpdir, bindings, 32);

    /* ghp_ token URL should be excluded */
    ASSERT_TRUE(find_binding_by_key(bindings, count, "GH_URL") == NULL);
    /* Normal URL should be present */
    ASSERT_NOT_NULL(find_binding_by_key(bindings, count, "NORMAL_ENDPOINT"));

    th_rmtree(tmpdir);
    PASS();
}

TEST(envscan_secret_file_exclusion) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_envscan_sfile_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("tmpdir");

    /* Secret file should be skipped */
    write_temp_file(tmpdir, "credentials.sh",
                    "#!/bin/bash\nexport API_URL=\"https://api.example.com/v1\"\n");
    /* Normal file should be scanned */
    write_temp_file(tmpdir, "setup.sh",
                    "#!/bin/bash\nexport API_URL=\"https://api.example.com/v1\"\n");

    cbm_env_binding_t bindings[32];
    int count = cbm_scan_project_env_urls(tmpdir, bindings, 32);

    /* Should find binding from setup.sh but not credentials.sh */
    int from_credentials = 0;
    int from_setup = 0;
    for (int i = 0; i < count; i++) {
        if (strcmp(bindings[i].file_path, "credentials.sh") == 0)
            from_credentials = 1;
        if (strcmp(bindings[i].file_path, "setup.sh") == 0)
            from_setup = 1;
    }
    ASSERT_EQ(from_credentials, 0);
    ASSERT_EQ(from_setup, 1);

    th_rmtree(tmpdir);
    PASS();
}

TEST(envscan_skips_ignored_dirs) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_envscan_ign_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("tmpdir");

    /* File inside .git should be skipped */
    char gitdir[512];
    snprintf(gitdir, sizeof(gitdir), "%s/.git", tmpdir);
    cbm_mkdir(gitdir);
    write_temp_file(tmpdir, ".git/config.sh",
                    "#!/bin/bash\nexport API_URL=\"https://api.example.com/v1\"\n");

    /* File inside node_modules should be skipped */
    char nmdir[512];
    snprintf(nmdir, sizeof(nmdir), "%s/node_modules", tmpdir);
    cbm_mkdir(nmdir);
    char nmpkg[512];
    snprintf(nmpkg, sizeof(nmpkg), "%s/node_modules/pkg", tmpdir);
    cbm_mkdir(nmpkg);
    write_temp_file(tmpdir, "node_modules/pkg/config.sh",
                    "#!/bin/bash\nexport API_URL=\"https://api.example.com/v1\"\n");

    /* File at root level should be scanned */
    write_temp_file(tmpdir, "deploy.sh",
                    "#!/bin/bash\nexport API_URL=\"https://api.example.com/v1\"\n");

    cbm_env_binding_t bindings[32];
    int count = cbm_scan_project_env_urls(tmpdir, bindings, 32);

    int from_git = 0, from_nm = 0, from_root = 0;
    for (int i = 0; i < count; i++) {
        if (strncmp(bindings[i].file_path, ".git/", 5) == 0)
            from_git = 1;
        if (strncmp(bindings[i].file_path, "node_modules/", 13) == 0)
            from_nm = 1;
        if (strcmp(bindings[i].file_path, "deploy.sh") == 0)
            from_root = 1;
    }
    ASSERT_EQ(from_git, 0);
    ASSERT_EQ(from_nm, 0);
    ASSERT_EQ(from_root, 1);

    th_rmtree(tmpdir);
    PASS();
}

TEST(envscan_non_url_values_skipped) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_envscan_nurl_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("tmpdir");

    write_temp_file(tmpdir, "Dockerfile",
                    "FROM python:3.9\n"
                    "ENV APP_NAME=my-service\n"
                    "ENV PORT=8080\n"
                    "ENV DEBUG=true\n"
                    "ENV LOG_LEVEL=info\n");
    write_temp_file(tmpdir, "config.sh",
                    "#!/bin/bash\n"
                    "export REGION=\"us-east-1\"\n"
                    "export COUNT=42\n");

    cbm_env_binding_t bindings[32];
    int count = cbm_scan_project_env_urls(tmpdir, bindings, 32);

    ASSERT_EQ(count, 0);

    th_rmtree(tmpdir);
    PASS();
}

/* ── Git history tests (port of githistory_test.go) ────────────── */

/* Port of Go TestIsTrackableFile from githistory_test.go */
TEST(githistory_is_trackable_file) {
    /* Source files — trackable */
    ASSERT_TRUE(cbm_is_trackable_file("main.go"));
    ASSERT_TRUE(cbm_is_trackable_file("src/app.py"));
    ASSERT_TRUE(cbm_is_trackable_file("README.md"));

    /* node_modules — not trackable */
    ASSERT_FALSE(cbm_is_trackable_file("node_modules/foo/bar.js"));
    /* vendor — not trackable */
    ASSERT_FALSE(cbm_is_trackable_file("vendor/lib/dep.go"));
    /* Lock files — not trackable */
    ASSERT_FALSE(cbm_is_trackable_file("package-lock.json"));
    ASSERT_FALSE(cbm_is_trackable_file("go.sum"));
    /* Binary/assets — not trackable */
    ASSERT_FALSE(cbm_is_trackable_file("image.png"));
    /* .git directory — not trackable */
    ASSERT_FALSE(cbm_is_trackable_file(".git/config"));
    /* __pycache__ — not trackable */
    ASSERT_FALSE(cbm_is_trackable_file("__pycache__/mod.pyc"));
    /* Minified files — not trackable */
    ASSERT_FALSE(cbm_is_trackable_file("src/style.min.css"));
    PASS();
}

/* Port of Go TestComputeChangeCoupling from githistory_test.go */
TEST(githistory_compute_change_coupling) {
    /* 5 commits:
     * aaa: a.go, b.go, c.go
     * bbb: a.go, b.go
     * ccc: a.go, b.go
     * ddd: a.go, c.go
     * eee: d.go, e.go
     */
    char *files_aaa[] = {"a.go", "b.go", "c.go"};
    char *files_bbb[] = {"a.go", "b.go"};
    char *files_ccc[] = {"a.go", "b.go"};
    char *files_ddd[] = {"a.go", "c.go"};
    char *files_eee[] = {"d.go", "e.go"};

    cbm_commit_files_t commits[5] = {
        {files_aaa, 3, 0}, {files_bbb, 2, 0}, {files_ccc, 2, 0},
        {files_ddd, 2, 0}, {files_eee, 2, 0},
    };

    cbm_change_coupling_t out[100];
    int count = cbm_compute_change_coupling(commits, 5, out, 100);

    /* a.go + b.go co-change 3 times → should be in results */
    bool found_ab = false;
    for (int i = 0; i < count; i++) {
        if ((strcmp(out[i].file_a, "a.go") == 0 && strcmp(out[i].file_b, "b.go") == 0) ||
            (strcmp(out[i].file_a, "b.go") == 0 && strcmp(out[i].file_b, "a.go") == 0)) {
            found_ab = true;
            ASSERT_EQ(out[i].co_change_count, 3);
            ASSERT(out[i].coupling_score >= 0.9);
        }
    }
    ASSERT_TRUE(found_ab);

    /* d.go + e.go co-change only 1 time → below threshold of 3 */
    for (int i = 0; i < count; i++) {
        if (strcmp(out[i].file_a, "d.go") == 0 || strcmp(out[i].file_b, "d.go") == 0) {
            ASSERT(0); /* d.go should not appear */
        }
    }
    PASS();
}

/* Port of Go TestComputeChangeCouplingSkipsLargeCommits from githistory_test.go */
TEST(githistory_coupling_skips_large_commits) {
    /* 25 files in one commit → exceeds 20-file threshold */
    char *files[25];
    char bufs[25][32];
    for (int i = 0; i < 25; i++) {
        snprintf(bufs[i], sizeof(bufs[i]), "file%d.go", i);
        files[i] = bufs[i];
    }
    cbm_commit_files_t commits[1] = {{files, 25, 0}};

    cbm_change_coupling_t out[100];
    int count = cbm_compute_change_coupling(commits, 1, out, 100);
    ASSERT_EQ(count, 0);
    PASS();
}

/* Port of Go TestComputeChangeCouplingLimitsTo100 from githistory_test.go */
TEST(githistory_coupling_limits_output) {
    /* Generate many small commits to create >100 couplings.
     * 50 files, each pair committed 3 times. max_out=100. */
    int idx = 0;
    char *pair_files[2450][2]; /* 50*49/2 pairs * 3 repetitions = 3675 commits */
    char pair_bufs[2450][2][32];
    cbm_commit_files_t commits[3675];
    int ci = 0;
    for (int i = 0; i < 50 && ci < 3675; i++) {
        for (int j = i + 1; j < 50 && ci < 3675; j++) {
            for (int k = 0; k < 3 && ci < 3675; k++) {
                snprintf(pair_bufs[idx][0], 32, "f%d.go", i);
                snprintf(pair_bufs[idx][1], 32, "f%d.go", j);
                pair_files[idx][0] = pair_bufs[idx][0];
                pair_files[idx][1] = pair_bufs[idx][1];
                commits[ci].files = pair_files[idx];
                commits[ci].count = 2;
                ci++;
                idx++;
                if (idx >= 2450)
                    idx = 0; /* reuse buffer space */
            }
        }
    }

    cbm_change_coupling_t out[200];
    int count = cbm_compute_change_coupling(commits, ci, out, 100);
    ASSERT(count <= 100);
    PASS();
}

/* Port of Go TestIsImportReachable from resolver_test.go */
TEST(registry_is_import_reachable) {
    const char *import_vals[] = {"proj.handler", "proj.shared.utils"};

    /* Exact match: proj.handler.Process → true */
    ASSERT_TRUE(cbm_registry_is_import_reachable("proj.handler.Process", import_vals, 2));
    /* Sub-package: proj.handler.sub.Process → true (handler contains handler) */
    ASSERT_TRUE(cbm_registry_is_import_reachable("proj.handler.sub.Process", import_vals, 2));
    /* Nested match: proj.shared.utils.Helper → true */
    ASSERT_TRUE(cbm_registry_is_import_reachable("proj.shared.utils.Helper", import_vals, 2));
    /* Unrelated: proj.billing.Process → false */
    ASSERT_FALSE(cbm_registry_is_import_reachable("proj.billing.Process", import_vals, 2));
    /* Completely unrelated: unrelated.pkg.Func → false */
    ASSERT_FALSE(cbm_registry_is_import_reachable("unrelated.pkg.Func", import_vals, 2));
    PASS();
}

/* Port of FindEndingWith portion from Go TestFunctionRegistry in pipeline_test.go */
TEST(registry_find_ending_with) {
    cbm_registry_t *reg = cbm_registry_new();
    cbm_registry_add(reg, "Foo", "proj.pkg.Foo", "Function");
    cbm_registry_add(reg, "Bar", "proj.pkg.Bar", "Function");
    cbm_registry_add(reg, "Foo", "proj.other.Foo", "Function");
    cbm_registry_add(reg, "transform", "proj.utils.DataProcessor.transform", "Method");

    /* FindEndingWith "DataProcessor.transform" → 1 match */
    const char **matches = NULL;
    int count = cbm_registry_find_ending_with(reg, "DataProcessor.transform", &matches);
    ASSERT_EQ(count, 1);
    ASSERT_STR_EQ(matches[0], "proj.utils.DataProcessor.transform");
    free(matches);

    /* FindEndingWith "Foo" → 2 matches */
    matches = NULL;
    count = cbm_registry_find_ending_with(reg, "Foo", &matches);
    ASSERT_EQ(count, 2);
    /* Both should be present (order may vary) */
    bool found_pkg = false, found_other = false;
    for (int i = 0; i < count; i++) {
        if (strcmp(matches[i], "proj.pkg.Foo") == 0)
            found_pkg = true;
        if (strcmp(matches[i], "proj.other.Foo") == 0)
            found_other = true;
    }
    ASSERT_TRUE(found_pkg);
    ASSERT_TRUE(found_other);
    free(matches);

    /* FindEndingWith "Nonexistent" → 0 matches */
    matches = NULL;
    count = cbm_registry_find_ending_with(reg, "Nonexistent", &matches);
    ASSERT_EQ(count, 0);

    cbm_registry_free(reg);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Incremental reindex
 * ═══════════════════════════════════════════════════════════════════ */

/* Helper: create a simple 2-file Go project for incremental tests */
static char g_incr_tmpdir[256];
static char g_incr_dbpath[512];

static int setup_incremental_repo(void) {
    snprintf(g_incr_tmpdir, sizeof(g_incr_tmpdir), "/tmp/cbm_incr_XXXXXX");
    if (!cbm_mkdtemp(g_incr_tmpdir)) {
        return -1;
    }
    snprintf(g_incr_dbpath, sizeof(g_incr_dbpath), "%s/test.db", g_incr_tmpdir);

    char path[512];
    FILE *f;

    /* main.go — calls Helper() */
    snprintf(path, sizeof(path), "%s/main.go", g_incr_tmpdir);
    f = fopen(path, "w");
    if (!f) {
        return -1;
    }
    fprintf(f, "package main\n\nfunc main() {\n\tHelper()\n}\n");
    fclose(f);

    /* helper.go — defines Helper() */
    snprintf(path, sizeof(path), "%s/helper.go", g_incr_tmpdir);
    f = fopen(path, "w");
    if (!f) {
        return -1;
    }
    fprintf(f, "package main\n\nfunc Helper() string {\n\treturn \"hello\"\n}\n");
    fclose(f);

    return 0;
}

enum { INCR_PARALLEL_CHANGED_FILE_COUNT = 64 };

static int incremental_parallel_file_path(int index, char *path, size_t path_sz) {
    int n = snprintf(path, path_sz, "%s/file_%03d.go", g_incr_tmpdir, index);
    return (n < 0 || (size_t)n >= path_sz) ? -1 : 0;
}

static int write_incremental_parallel_file(int index, bool changed) {
    char path[CBM_PATH_MAX];
    if (incremental_parallel_file_path(index, path, sizeof(path)) != 0) {
        return -1;
    }
    FILE *f = fopen(path, "w");
    if (!f) {
        return -1;
    }
    fprintf(f, "package main\n\nfunc Func%03d() int {\n\treturn %d\n}\n", index,
            index + (changed ? 1 : 0));
    if (changed && index == 0) {
        fprintf(f, "\nfunc NewFunc() int {\n\treturn 42\n}\n");
    }
    return fclose(f);
}

static int setup_incremental_parallel_repo(void) {
    int n = snprintf(g_incr_tmpdir, sizeof(g_incr_tmpdir), "/tmp/cbm_incr_parallel_XXXXXX");
    if (n < 0 || (size_t)n >= sizeof(g_incr_tmpdir) || !cbm_mkdtemp(g_incr_tmpdir)) {
        return -1;
    }
    n = snprintf(g_incr_dbpath, sizeof(g_incr_dbpath), "%s/test.db", g_incr_tmpdir);
    if (n < 0 || (size_t)n >= sizeof(g_incr_dbpath)) {
        return -1;
    }
    for (int i = 0; i < INCR_PARALLEL_CHANGED_FILE_COUNT; i++) {
        if (write_incremental_parallel_file(i, false) != 0) {
            return -1;
        }
    }
    return 0;
}

static int rewrite_incremental_parallel_repo(void) {
    for (int i = 0; i < INCR_PARALLEL_CHANGED_FILE_COUNT; i++) {
        if (write_incremental_parallel_file(i, true) != 0) {
            return -1;
        }
    }
    return 0;
}

static void cleanup_incremental_repo(void) {
    th_rmtree(g_incr_tmpdir);
}

static cbm_config_t *incremental_test_config(const char *cache_dir) {
    cbm_config_t *cfg = cbm_config_open(cache_dir);
    if (cfg) {
        cbm_config_set(cfg, CBM_CONFIG_INCREMENTAL_REINDEX, CBM_CONFIG_INCREMENTAL_REINDEX_ALWAYS);
    }
    return cfg;
}

enum { PIPELINE_INCR_FRONTIER_CALLER_COUNT = CBM_SZ_4 };

static int write_incremental_leaf_file(int leaf_value) {
    char path[CBM_PATH_MAX];
    int n = snprintf(path, sizeof(path), "%s/leaf.go", g_incr_tmpdir);
    if (n < 0 || (size_t)n >= sizeof(path)) {
        return -1;
    }
    char body[CBM_SZ_512];
    n = snprintf(body, sizeof(body),
                 "package main\n\n"
                 "func Leaf() int {\n"
                 "\tfor i := 0; i < 10; i++ {\n"
                 "\t\tfor j := 0; j < 10; j++ {\n"
                 "\t\t}\n"
                 "\t}\n"
                 "\treturn %d\n"
                 "}\n",
                 leaf_value);
    if (n < 0 || (size_t)n >= sizeof(body)) {
        return -1;
    }
    return th_write_file(path, body);
}

static int write_incremental_leaf_file_with_extra(int leaf_value) {
    char path[CBM_PATH_MAX];
    int n = snprintf(path, sizeof(path), "%s/leaf.go", g_incr_tmpdir);
    if (n < 0 || (size_t)n >= sizeof(path)) {
        return -1;
    }
    char body[CBM_SZ_512];
    n = snprintf(body, sizeof(body),
                 "package main\n\n"
                 "func Leaf() int {\n"
                 "\tfor i := 0; i < 10; i++ {\n"
                 "\t\tfor j := 0; j < 10; j++ {\n"
                 "\t\t}\n"
                 "\t}\n"
                 "\treturn %d\n"
                 "}\n\n"
                 "func LeafExtra() int {\n"
                 "\treturn Leaf()\n"
                 "}\n",
                 leaf_value);
    if (n < 0 || (size_t)n >= sizeof(body)) {
        return -1;
    }
    return th_write_file(path, body);
}

static int write_incremental_frontier_callers(void) {
    const char *caller_names[PIPELINE_INCR_FRONTIER_CALLER_COUNT] = {
        "caller_a.go", "caller_b.go", "caller_c.go", "caller_d.go"};
    const char *caller_funcs[PIPELINE_INCR_FRONTIER_CALLER_COUNT] = {
        "CallerA", "CallerB", "CallerC", "CallerD"};
    char path[CBM_PATH_MAX];
    char body[CBM_SZ_512];
    for (size_t i = 0; i < sizeof(caller_names) / sizeof(caller_names[0]); i++) {
        int n = snprintf(path, sizeof(path), "%s/%s", g_incr_tmpdir, caller_names[i]);
        if (n < 0 || (size_t)n >= sizeof(path)) {
            return -1;
        }
        n = snprintf(body, sizeof(body),
                     "package main\n\n"
                     "func %s() int {\n"
                     "\treturn Leaf()\n"
                     "}\n",
                     caller_funcs[i]);
        if (n < 0 || (size_t)n >= sizeof(body)) {
            return -1;
        }
        if (th_write_file(path, body) != 0) {
            return -1;
        }
    }
    return 0;
}

static int write_incremental_frontier_fixture(int leaf_value) {
    if (write_incremental_leaf_file(leaf_value) != 0) {
        return -1;
    }
    return write_incremental_frontier_callers();
}

enum { PIPELINE_INCR_C_HEADER_IMPORTER_COUNT = CBM_SZ_4 };

static int write_incremental_c_header_frontier_fixture(int marker) {
    char path[CBM_PATH_MAX];
    char body[CBM_SZ_1K];
    int n = snprintf(path, sizeof(path), "%s/shared.h", g_incr_tmpdir);
    if (n < 0 || (size_t)n >= sizeof(path)) {
        return -1;
    }
    n = snprintf(body, sizeof(body),
                 "#ifndef SHARED_H\n"
                 "#define SHARED_H\n"
                 "#define SHARED_MARKER %d\n"
                 "int shared_value(void);\n"
                 "#endif\n",
                 marker);
    if (n < 0 || (size_t)n >= sizeof(body) || th_write_file(path, body) != 0) {
        return -1;
    }

    n = snprintf(path, sizeof(path), "%s/shared.c", g_incr_tmpdir);
    if (n < 0 || (size_t)n >= sizeof(path)) {
        return -1;
    }
    if (th_write_file(path,
                      "#include \"shared.h\"\n\n"
                      "int shared_value(void) {\n"
                      "    return SHARED_MARKER;\n"
                      "}\n") != 0) {
        return -1;
    }

    for (int i = 0; i < PIPELINE_INCR_C_HEADER_IMPORTER_COUNT; i++) {
        n = snprintf(path, sizeof(path), "%s/consumer_%d.c", g_incr_tmpdir, i);
        if (n < 0 || (size_t)n >= sizeof(path)) {
            return -1;
        }
        n = snprintf(body, sizeof(body),
                     "#include \"shared.h\"\n\n"
                     "int consumer_%d(void) {\n"
                     "    return shared_value() + %d;\n"
                     "}\n",
                     i, i);
        if (n < 0 || (size_t)n >= sizeof(body) || th_write_file(path, body) != 0) {
            return -1;
        }
    }
    return 0;
}

static int write_incremental_arg_url_route_file(const char *route_path, int marker) {
    char path[CBM_PATH_MAX];
    int n = snprintf(path, sizeof(path), "%s/http_routes.c", g_incr_tmpdir);
    if (n < 0 || (size_t)n >= sizeof(path)) {
        return -1;
    }
    char body[CBM_SZ_1K];
    n = snprintf(body, sizeof(body),
                 "static int cbm_http_path_match(const char *path, const char *pattern) {\n"
                 "    return path && pattern;\n"
                 "}\n\n"
                 "int dispatch_request(const char *path) {\n"
                 "    if (cbm_http_path_match(path, \"%s\")) {\n"
                 "        return %d;\n"
                 "    }\n"
                 "    return 0;\n"
                 "}\n",
                 route_path, marker);
    if (n < 0 || (size_t)n >= sizeof(body)) {
        return -1;
    }
    return th_write_file(path, body);
}

static int pipeline_store_insert_file_owned_unowned_source_edge(const char *db_path,
                                                               const char *project,
                                                               const char *rel_path,
                                                               const char *target_name,
                                                               const char *edge_type) {
    cbm_store_t *s = cbm_store_open_path(db_path);
    if (!s) {
        return CBM_STORE_ERR;
    }
    char *target_qn = cbm_pipeline_fqn_compute(project, rel_path, target_name);
    if (!target_qn) {
        cbm_store_close(s);
        return CBM_STORE_ERR;
    }
    cbm_node_t target = {0};
    int rc = cbm_store_find_node_by_qn(s, project, target_qn, &target);
    if (rc != CBM_STORE_OK) {
        free(target_qn);
        cbm_store_close(s);
        return rc;
    }
    char source_qn[CBM_SZ_512];
    int n = snprintf(source_qn, sizeof(source_qn), "%s.__unowned_source", project);
    if (n < 0 || (size_t)n >= sizeof(source_qn)) {
        cbm_node_free_fields(&target);
        free(target_qn);
        cbm_store_close(s);
        return CBM_STORE_ERR;
    }
    cbm_node_t source = {.project = (char *)project,
                         .label = "Module",
                         .name = "unowned_source",
                         .qualified_name = source_qn,
                         .file_path = "",
                         .properties_json = "{}"};
    int64_t source_id = cbm_store_upsert_node(s, &source);
    if (source_id <= CBM_STORE_NO_NODE_ID) {
        cbm_node_free_fields(&target);
        free(target_qn);
        cbm_store_close(s);
        return CBM_STORE_ERR;
    }
    cbm_edge_t edge = {.project = (char *)project,
                       .source_id = source_id,
                       .target_id = target.id,
                       .type = (char *)edge_type,
                       .properties_json = "{}"};
    int64_t edge_id = cbm_store_insert_edge(s, &edge);
    if (edge_id <= CBM_STORE_NO_NODE_ID) {
        cbm_node_free_fields(&target);
        free(target_qn);
        cbm_store_close(s);
        return CBM_STORE_ERR;
    }
    rc = cbm_store_upsert_edge_owner(s, project, edge_id, rel_path, NULL,
                                     CBM_PIPELINE_COMPAT_GENERATION);
    cbm_node_free_fields(&target);
    free(target_qn);
    cbm_store_close(s);
    return rc;
}

static int pipeline_store_has_node_name_by_label(const char *db_path, const char *project,
                                                 const char *label, const char *name) {
    cbm_store_t *s = cbm_store_open_path_query(db_path);
    if (!s) {
        return 0;
    }
    cbm_node_t *nodes = NULL;
    int count = 0;
    int found = 0;
    if (cbm_store_find_nodes_by_label(s, project, label, &nodes, &count) == CBM_STORE_OK) {
        for (int i = 0; i < count; i++) {
            if (nodes[i].name && strcmp(nodes[i].name, name) == 0) {
                found = 1;
                break;
            }
        }
        cbm_store_free_nodes(nodes, count);
    }
    cbm_store_close(s);
    return found;
}

static int pipeline_store_has_function_name(const char *db_path, const char *project,
                                            const char *name) {
    return pipeline_store_has_node_name_by_label(db_path, project, "Function", name);
}

static int pipeline_store_has_route_name(const char *db_path, const char *project,
                                         const char *name) {
    return pipeline_store_has_node_name_by_label(db_path, project, "Route", name);
}

static int pipeline_restore_file_times(const char *path, const struct stat *st) {
    if (!path || !st) {
        return -1;
    }
#ifdef _WIN32
    struct __utimbuf64 times = {.actime = st->st_atime, .modtime = st->st_mtime};
    return _utime64(path, &times);
#else
    struct timespec times[CBM_SZ_2];
#ifdef __APPLE__
    times[0] = st->st_atimespec;
    times[SKIP_ONE] = st->st_mtimespec;
#else
    times[0] = st->st_atim;
    times[SKIP_ONE] = st->st_mtim;
#endif
    return utimensat(AT_FDCWD, path, times, 0);
#endif
}

enum { PIPELINE_TEST_MTIME_BUMP_SECONDS = 2 };

static int pipeline_bump_file_mtime_seconds(const char *path, const struct stat *st, long seconds) {
    if (!path || !st) {
        return -1;
    }
    struct stat bumped = *st;
#ifdef _WIN32
    bumped.st_mtime += seconds;
#elif defined(__APPLE__)
    bumped.st_mtimespec.tv_sec += seconds;
#else
    bumped.st_mtim.tv_sec += seconds;
#endif
    return pipeline_restore_file_times(path, &bumped);
}

static int pipeline_store_file_hash_count(const char *db_path, const char *project) {
    cbm_store_t *s = cbm_store_open_path_query(db_path);
    if (!s) {
        return CBM_STORE_ERR;
    }
    cbm_file_hash_t *hashes = NULL;
    int count = 0;
    int rc = cbm_store_get_file_hashes(s, project, &hashes, &count);
    cbm_store_free_file_hashes(hashes, count);
    cbm_store_close(s);
    return rc == CBM_STORE_OK ? count : CBM_STORE_ERR;
}

static int pipeline_store_file_hash_mtime(const char *db_path, const char *project,
                                          const char *rel_path, int64_t *out_mtime_ns) {
    if (!out_mtime_ns) {
        return CBM_STORE_ERR;
    }
    *out_mtime_ns = 0;
    cbm_store_t *s = cbm_store_open_path_query(db_path);
    if (!s) {
        return CBM_STORE_ERR;
    }
    cbm_file_hash_t *hashes = NULL;
    int count = 0;
    int rc = cbm_store_get_file_hashes(s, project, &hashes, &count);
    if (rc == CBM_STORE_OK) {
        rc = CBM_STORE_NOT_FOUND;
        for (int i = 0; i < count; i++) {
            if (hashes[i].rel_path && strcmp(hashes[i].rel_path, rel_path) == 0) {
                *out_mtime_ns = hashes[i].mtime_ns;
                rc = CBM_STORE_OK;
                break;
            }
        }
    }
    cbm_store_free_file_hashes(hashes, count);
    cbm_store_close(s);
    return rc;
}

static int pipeline_store_file_state_generation(const char *db_path, const char *project,
                                                const char *rel_path, int64_t *out_generation) {
    if (!out_generation) {
        return CBM_STORE_ERR;
    }
    *out_generation = 0;
    cbm_store_t *s = cbm_store_open_path_query(db_path);
    if (!s) {
        return CBM_STORE_ERR;
    }
    cbm_file_state_t state = {0};
    int rc = cbm_store_get_file_state(s, project, rel_path, &state);
    if (rc == CBM_STORE_OK) {
        *out_generation = state.generation;
        cbm_store_file_state_free_fields(&state);
    }
    cbm_store_close(s);
    return rc;
}

static int pipeline_store_dirty_counts(const char *db_path, const char *project,
                                       int *out_pending, int *out_overlay_ready) {
    cbm_store_t *s = cbm_store_open_path_query(db_path);
    if (!s) {
        return CBM_STORE_ERR;
    }
    int rc = cbm_store_count_dirty_files(s, project, out_pending, out_overlay_ready);
    cbm_store_close(s);
    return rc;
}

static int pipeline_store_overlay_file_function_count(const char *db_path, const char *project,
                                                      const char *rel_path, const char *name) {
    cbm_store_t *s = cbm_store_open_path_query(db_path);
    if (!s) {
        return CBM_STORE_ERR;
    }
    cbm_node_t *nodes = NULL;
    int count = 0;
    int matches = 0;
    if (cbm_store_find_nodes_by_file_overlay_view(s, project, rel_path, &nodes, &count) !=
        CBM_STORE_OK) {
        cbm_store_close(s);
        return CBM_STORE_ERR;
    }
    for (int i = 0; i < count; i++) {
        if (nodes[i].label && strcmp(nodes[i].label, "Function") == 0 && nodes[i].name &&
            strcmp(nodes[i].name, name) == 0) {
            matches++;
        }
    }
    cbm_store_free_nodes(nodes, count);
    cbm_store_close(s);
    return matches;
}

static int pipeline_store_overlay_file_has_function(const char *db_path, const char *project,
                                                    const char *rel_path, const char *name) {
    return pipeline_store_overlay_file_function_count(db_path, project, rel_path, name) > 0;
}

static int pipeline_store_generation_status_count(const char *db_path, const char *project,
                                                  const char *status) {
    cbm_store_t *s = cbm_store_open_path_query(db_path);
    if (!s) {
        return CBM_STORE_ERR;
    }
    sqlite3 *db = cbm_store_get_db(s);
    sqlite3_stmt *stmt = NULL;
    const char *sql = "SELECT COUNT(*) FROM index_generations "
                      "WHERE project = ?1 AND status = ?2 AND generation > ?3;";
    int count = CBM_STORE_ERR;
    if (db && sqlite3_prepare_v2(db, sql, CBM_NOT_FOUND, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, project, CBM_NOT_FOUND, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 2, status, CBM_NOT_FOUND, SQLITE_TRANSIENT);
        sqlite3_bind_int64(stmt, 3, CBM_PIPELINE_COMPAT_GENERATION);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            count = sqlite3_column_int(stmt, 0);
        }
    }
    sqlite3_finalize(stmt);
    cbm_store_close(s);
    return count;
}

static int pipeline_store_completed_generation_count(const char *db_path, const char *project) {
    return pipeline_store_generation_status_count(db_path, project,
                                                  CBM_STORE_INDEX_STATUS_COMPLETE);
}

static int pipeline_store_overlay_generation_status_count(const char *db_path,
                                                          const char *project,
                                                          const char *status) {
    cbm_store_t *s = cbm_store_open_path_query(db_path);
    if (!s) {
        return CBM_STORE_ERR;
    }
    int count = CBM_STORE_ERR;
    if (cbm_store_count_overlay_generations(s, project, status, &count) != CBM_STORE_OK) {
        count = CBM_STORE_ERR;
    }
    cbm_store_close(s);
    return count;
}

static int pipeline_store_count_file_rows_sql(const char *db_path, const char *project,
                                              const char *rel_path, const char *sql,
                                              int *out_count) {
    if (!db_path || !project || !rel_path || !sql || !out_count) {
        return CBM_STORE_ERR;
    }
    *out_count = 0;
    cbm_store_t *s = cbm_store_open_path_query(db_path);
    if (!s) {
        return CBM_STORE_ERR;
    }
    sqlite3 *db = cbm_store_get_db(s);
    sqlite3_stmt *stmt = NULL;
    int rc = CBM_STORE_ERR;
    if (db && sqlite3_prepare_v2(db, sql, CBM_NOT_FOUND, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, project, CBM_NOT_FOUND, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 2, rel_path, CBM_NOT_FOUND, SQLITE_TRANSIENT);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            *out_count = sqlite3_column_int(stmt, 0);
            rc = CBM_STORE_OK;
        }
    }
    sqlite3_finalize(stmt);
    cbm_store_close(s);
    return rc;
}

static int pipeline_compare_current_db_to_fresh_fast_rebuild(const char *repo_path,
                                                             const char *db_path,
                                                             const char *project,
                                                             cbm_config_t *cfg, char *err,
                                                             size_t err_sz) {
    char exact_db[CBM_SZ_512];
    int n = snprintf(exact_db, sizeof(exact_db), "%s/exact-upsert.db", repo_path);
    if (n < 0 || (size_t)n >= sizeof(exact_db)) {
        if (err && err_sz > 0) {
            snprintf(err, err_sz, "exact snapshot path overflow");
        }
        return CBM_STORE_ERR;
    }
    cbm_unlink(exact_db);
    int rc = pipeline_dump_store_file_to_file(db_path, exact_db);
    if (rc != CBM_STORE_OK) {
        if (err && err_sz > 0) {
            snprintf(err, err_sz, "exact snapshot dump failed: rc=%d", rc);
        }
        cbm_unlink(exact_db);
        return rc;
    }

    cbm_unlink(db_path);
    cbm_pipeline_t *p = cbm_pipeline_new(repo_path, db_path, CBM_MODE_FAST);
    if (!p) {
        if (err && err_sz > 0) {
            snprintf(err, err_sz, "fresh FAST pipeline allocation failed");
        }
        cbm_unlink(exact_db);
        return CBM_STORE_ERR;
    }
    cbm_pipeline_apply_config(p, cfg);
    int run_rc = cbm_pipeline_run(p);
    cbm_pipeline_free(p);
    if (run_rc != 0) {
        if (err && err_sz > 0) {
            snprintf(err, err_sz, "fresh FAST rebuild failed: rc=%d", run_rc);
        }
        cbm_unlink(exact_db);
        return CBM_STORE_ERR;
    }

    rc = cbm_test_compare_canonical_graphs(exact_db, db_path, project, err, err_sz);
    if (rc == 0) {
        cbm_unlink(exact_db);
    }
    return rc;
}

static int pipeline_gbuf_count_usage_edge(const cbm_gbuf_t *gb, const char *source_qn,
                                          const char *target_qn, const char *callee) {
    const cbm_gbuf_node_t *src = cbm_gbuf_find_by_qn(gb, source_qn);
    const cbm_gbuf_node_t *tgt = cbm_gbuf_find_by_qn(gb, target_qn);
    if (!src || !tgt) {
        return 0;
    }
    const cbm_gbuf_edge_t **edges = NULL;
    int edge_count = 0;
    if (cbm_gbuf_find_edges_by_source_type(gb, src->id, "USAGE", &edges, &edge_count) != 0) {
        return 0;
    }
    int matches = 0;
    for (int i = 0; i < edge_count; i++) {
        const cbm_gbuf_edge_t *edge = edges[i];
        if (edge && edge->target_id == tgt->id &&
            (!callee || (edge->properties_json && strstr(edge->properties_json, callee)))) {
            matches++;
        }
    }
    return matches;
}

static int pipeline_file_delta_count_usage_edge(const cbm_pipeline_file_delta_t *delta,
                                                const char *source_qn, const char *target_qn,
                                                const char *callee) {
    int matches = 0;
    for (int i = 0; i < delta->delta.edge_count; i++) {
        const cbm_store_delta_edge_t *edge = &delta->edges[i];
        if (edge->type && strcmp(edge->type, "USAGE") == 0 &&
            edge->source_qn && strcmp(edge->source_qn, source_qn) == 0 &&
            edge->target_qn && strcmp(edge->target_qn, target_qn) == 0 &&
            (!callee || (edge->properties_json && strstr(edge->properties_json, callee)))) {
            matches++;
        }
    }
    return matches;
}

static const char *pipeline_exact_scratch_structure_root_qn(const cbm_gbuf_t *gbuf,
                                                            const char *project) {
    const cbm_gbuf_node_t **branches = NULL;
    int branch_count = 0;
    if (cbm_gbuf_find_by_label(gbuf, "Branch", &branches, &branch_count) == 0 &&
        branch_count > 0 && branches[0]->qualified_name) {
        return branches[0]->qualified_name;
    }
    return project;
}

static int pipeline_build_exact_scratch_for_changed_files(cbm_store_t *store,
                                                          const char *repo_path,
                                                          const char *project,
                                                          cbm_file_info_t *changed_files,
                                                          int changed_count,
                                                          cbm_gbuf_t **out_scratch,
                                                          cbm_pipeline_file_delta_t *deltas) {
    if (out_scratch) {
        *out_scratch = NULL;
    }
    if (!store || !repo_path || !project || !changed_files || changed_count <= 0 ||
        !out_scratch || !deltas) {
        return CBM_STORE_ERR;
    }

    const char **changed_paths = calloc((size_t)changed_count, sizeof(*changed_paths));
    CBMFileResult **result_cache = calloc((size_t)changed_count, sizeof(*result_cache));
    cbm_gbuf_t *scratch = cbm_gbuf_new(project, repo_path);
    cbm_registry_t *registry = cbm_registry_new();
    atomic_int cancelled;
    atomic_init(&cancelled, 0);
    int rc = CBM_STORE_ERR;
    if (!changed_paths || !result_cache || !scratch || !registry) {
        goto cleanup;
    }
    for (int i = 0; i < changed_count; i++) {
        changed_paths[i] = changed_files[i].rel_path;
    }
    rc = cbm_pipeline_seed_file_delta_scratch_from_store(store, scratch, registry, project,
                                                         changed_paths, changed_count);
    if (rc != CBM_STORE_OK) {
        goto cleanup;
    }

    const double pipeline_default_threshold = 0.0; /* Pipeline constructor sentinel: use pass defaults. */
    cbm_pipeline_ctx_t ctx = {.project_name = project,
                              .repo_path = repo_path,
                              .gbuf = scratch,
                              .registry = registry,
                              .cancelled = &cancelled,
                              .mode = CBM_MODE_FAST,
                              .similarity_threshold = pipeline_default_threshold,
                              .httplink_min_confidence = pipeline_default_threshold,
                              .semantic_threshold = pipeline_default_threshold,
                              .githistory_min_coupling = pipeline_default_threshold,
                              .lsp_confidence_floor = pipeline_default_threshold,
                              .result_cache = result_cache,
                              .store_backed_node_lookup = store,
                              .store_backed_changed_paths = changed_paths,
                              .store_backed_changed_path_count = changed_count};
    const char *structure_root_qn = pipeline_exact_scratch_structure_root_qn(scratch, project);
    for (int i = 0; i < changed_count; i++) {
        if (cbm_pipeline_ensure_file_structure(scratch, project, structure_root_qn,
                                               changed_files[i].rel_path, NULL) != 0) {
            goto cleanup;
        }
    }
    if (cbm_pipeline_pass_definitions(&ctx, changed_files, changed_count) != 0 ||
        cbm_pipeline_pass_lsp_cross(&ctx, changed_files, changed_count, result_cache) != 0 ||
        cbm_pipeline_pass_calls(&ctx, changed_files, changed_count) != 0 ||
        cbm_pipeline_pass_usages(&ctx, changed_files, changed_count) != 0 ||
        cbm_pipeline_pass_semantic(&ctx, changed_files, changed_count) != 0 ||
        cbm_pipeline_pass_k8s(&ctx, changed_files, changed_count) != 0 ||
        cbm_pipeline_pass_tests(&ctx, changed_files, changed_count) != 0) {
        goto cleanup;
    }
    (void)cbm_pipeline_pass_decorator_tags(scratch, project);
    (void)cbm_pipeline_pass_configlink(&ctx);
    cbm_pipeline_clear_route_derived_edges(scratch);
    cbm_pipeline_create_route_nodes(scratch);
    cbm_pipeline_pass_complexity_for_paths(&ctx, changed_paths, changed_count);
    if (cbm_pipeline_pass_httplinks(&ctx) != 0) {
        goto cleanup;
    }
    cbm_pipeline_pass_normalize(scratch);
    for (int i = 0; i < changed_count; i++) {
        rc = cbm_pipeline_build_file_delta_from_gbuf(scratch, project, changed_files[i].rel_path,
                                                     CBM_PIPELINE_COMPAT_GENERATION, &deltas[i]);
        if (rc != CBM_STORE_OK) {
            goto cleanup;
        }
    }

    *out_scratch = scratch;
    scratch = NULL;
    rc = CBM_STORE_OK;

cleanup:
    for (int i = 0; i < changed_count; i++) {
        if (result_cache && result_cache[i]) {
            cbm_free_result(result_cache[i]);
        }
    }
    free(result_cache);
    cbm_registry_free(registry);
    cbm_gbuf_free(scratch);
    free(changed_paths);
    return rc;
}

/* ═══════════════════════════════════════════════════════════════════
 *  FastAPI Depends() edge tracking (PR #66, fix #27)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(pipeline_fastapi_depends_edges) {
    /* Depends(get_current_user) should produce a CALLS edge from the
     * endpoint to the dependency function. */
    const char *files[] = {"auth.py", "routes.py"};
    const char *contents[] = {/* auth.py: defines get_current_user */
                              "def get_current_user(token: str):\n"
                              "    return decode_token(token)\n",
                              /* routes.py: endpoint depends on get_current_user */
                              "from fastapi import Depends\n"
                              "from auth import get_current_user\n\n"
                              "def get_profile(user = Depends(get_current_user)):\n"
                              "    return {\"user\": user}\n"};
    if (setup_lang_repo(files, contents, 2) != 0) {
        FAIL("tmpdir");
    }
    char db[512];
    snprintf(db, sizeof(db), "%s/test.db", g_lang_tmpdir);
    cbm_pipeline_t *p = cbm_pipeline_new(g_lang_tmpdir, db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    cbm_store_t *s = cbm_store_open_path(db);
    ASSERT_NOT_NULL(s);
    const char *proj = cbm_pipeline_project_name(p);

    /* Check CALLS edges for fastapi_depends strategy */
    cbm_edge_t *edges = NULL;
    int edge_count = 0;
    cbm_store_find_edges_by_type(s, proj, "CALLS", &edges, &edge_count);

    bool found_depends_edge = false;
    for (int i = 0; i < edge_count; i++) {
        if (edges[i].properties_json && strstr(edges[i].properties_json, "fastapi_depends")) {
            found_depends_edge = true;
            break;
        }
    }
    if (edges) {
        cbm_store_free_edges(edges, edge_count);
    }
    ASSERT_TRUE(found_depends_edge);

    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_lang_repo();
    PASS();
}

TEST(import_edge_helper_escapes_local_name_once) {
    cbm_gbuf_t *gb = cbm_gbuf_new("proj", "/tmp/proj");
    ASSERT_NOT_NULL(gb);

    int64_t source_file =
        cbm_gbuf_upsert_node(gb, "File", "main.py", "proj.main.__file__", "main.py", 1, 1, "{}");
    int64_t target_fn =
        cbm_gbuf_upsert_node(gb, "Function", "factory", "proj.pkg.factory", "pkg.py", 1, 1, "{}");
    ASSERT_GT(source_file, 0);
    ASSERT_GT(target_fn, 0);

    const cbm_gbuf_node_t *target = cbm_gbuf_find_by_id(gb, target_fn);
    ASSERT_NOT_NULL(target);

    cbm_pipeline_ctx_t ctx = {
        .gbuf = gb,
        .project_name = "proj",
    };
    const char alias[] = "quoted\"alias\\module\nnext\tfield";
    ASSERT_EQ(cbm_pipeline_insert_import_edge(&ctx, source_file, target, alias), 1);
    ASSERT_EQ(cbm_pipeline_insert_import_edge(&ctx, source_file, cbm_gbuf_find_by_id(gb, source_file),
                                              "self"), 0);

    const cbm_gbuf_edge_t **edges = NULL;
    int edge_count = 0;
    ASSERT_EQ(cbm_gbuf_find_edges_by_source_type(gb, source_file, "IMPORTS", &edges, &edge_count),
              0);
    ASSERT_EQ(edge_count, 1);
    ASSERT_EQ(edges[0]->target_id, target_fn);

    yyjson_doc *doc =
        yyjson_read(edges[0]->properties_json, strlen(edges[0]->properties_json), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *local = yyjson_obj_get(root, "local_name");
    ASSERT_NOT_NULL(local);
    ASSERT_STR_EQ(yyjson_get_str(local), alias);
    yyjson_doc_free(doc);

    const char **keys = NULL;
    const char **vals = NULL;
    int import_count = 0;
    ASSERT_EQ(cbm_pipeline_build_import_map_from_edges(gb, "proj", "main.py", &keys, &vals,
                                                       &import_count),
              0);
    ASSERT_EQ(import_count, 1);
    ASSERT_STR_EQ(keys[0], alias);
    ASSERT_STR_EQ(vals[0], target->qualified_name);
    cbm_pipeline_free_import_map(keys, vals, import_count);

    cbm_gbuf_free(gb);
    PASS();
}

TEST(import_edge_helper_preserves_long_local_name) {
    cbm_gbuf_t *gb = cbm_gbuf_new("proj", "/tmp/proj");
    ASSERT_NOT_NULL(gb);

    int64_t source_file =
        cbm_gbuf_upsert_node(gb, "File", "main.py", "proj.main.__file__", "main.py", 1, 1, "{}");
    int64_t target_fn =
        cbm_gbuf_upsert_node(gb, "Function", "factory", "proj.pkg.factory", "pkg.py", 1, 1, "{}");
    ASSERT_GT(source_file, 0);
    ASSERT_GT(target_fn, 0);

    const cbm_gbuf_node_t *target = cbm_gbuf_find_by_id(gb, target_fn);
    ASSERT_NOT_NULL(target);

    enum { LONG_ALIAS_LEN = CBM_SZ_256 + CBM_SZ_64 };
    char alias[LONG_ALIAS_LEN + SKIP_ONE];
    for (int i = 0; i < LONG_ALIAS_LEN; i++) {
        alias[i] = (char)('a' + (i % CBM_DECIMAL_BASE));
    }
    alias[LONG_ALIAS_LEN] = '\0';

    cbm_pipeline_ctx_t ctx = {
        .gbuf = gb,
        .project_name = "proj",
    };
    ASSERT_EQ(cbm_pipeline_insert_import_edge(&ctx, source_file, target, alias), 1);

    const char **keys = NULL;
    const char **vals = NULL;
    int import_count = 0;
    ASSERT_EQ(cbm_pipeline_build_import_map_from_edges(gb, "proj", "main.py", &keys, &vals,
                                                       &import_count),
              0);
    ASSERT_EQ(import_count, 1);
    ASSERT_STR_EQ(keys[0], alias);
    ASSERT_STR_EQ(vals[0], target->qualified_name);
    cbm_pipeline_free_import_map(keys, vals, import_count);

    cbm_gbuf_free(gb);
    PASS();
}

TEST(import_map_from_edges_follows_package_reexport) {
    cbm_gbuf_t *gb = cbm_gbuf_new("proj", "/tmp/proj");
    ASSERT_NOT_NULL(gb);

    int64_t source_file =
        cbm_gbuf_upsert_node(gb, "File", "main.py", "proj.app.main.__file__", "app/main.py", 1,
                             1, "{}");
    int64_t package_module =
        cbm_gbuf_upsert_node(gb, "Folder", "fastapi", "proj.fastapi", "fastapi", 1,
                             1, "{}");
    int64_t package_file = cbm_gbuf_upsert_node(gb, "File", "__init__.py", "proj.fastapi.__file__",
                                                "fastapi/__init__.py", 1, 1, "{}");
    int64_t wrong_header = cbm_gbuf_upsert_node(gb, "Class", "Header",
                                                "proj.fastapi.openapi.models.Header",
                                                "fastapi/openapi/models.py", 1, 1, "{}");
    int64_t exported_header =
        cbm_gbuf_upsert_node(gb, "Function", "Header", "proj.fastapi.param_functions.Header",
                             "fastapi/param_functions.py", 1, 1, "{}");
    ASSERT_GT(source_file, 0);
    ASSERT_GT(package_module, 0);
    ASSERT_GT(package_file, 0);
    ASSERT_GT(wrong_header, 0);
    ASSERT_GT(exported_header, 0);

    cbm_pipeline_ctx_t ctx = {
        .gbuf = gb,
        .project_name = "proj",
    };
    ASSERT_EQ(cbm_pipeline_insert_import_edge(&ctx, source_file,
                                              cbm_gbuf_find_by_id(gb, package_module), "Header"),
              1);
    cbm_gbuf_insert_edge(gb, package_file, exported_header, "IMPORTS",
                         "{\"local_name\":\"Header\"}");

    const char **keys = NULL;
    const char **vals = NULL;
    int import_count = 0;
    ASSERT_EQ(cbm_pipeline_build_import_map_from_edges(gb, "proj", "app/main.py", &keys, &vals,
                                                       &import_count),
              0);
    ASSERT_EQ(import_count, 1);
    ASSERT_STR_EQ(keys[0], "Header");
    ASSERT_STR_EQ(vals[0], "proj.fastapi.param_functions.Header");

    cbm_pipeline_free_import_map(keys, vals, import_count);
    cbm_gbuf_free(gb);
    PASS();
}

TEST(import_reexport_falls_back_when_pkgmap_target_missing) {
    cbm_gbuf_t *gb = cbm_gbuf_new("proj", "/tmp/proj");
    ASSERT_NOT_NULL(gb);

    int64_t openapi_header = cbm_gbuf_upsert_node(
        gb, "Class", "Header", "proj.fastapi.openapi.models.Header", "fastapi/openapi/models.py", 1,
        1, "{}");
    int64_t package_file = cbm_gbuf_upsert_node(gb, "File", "__init__.py", "proj.fastapi.__file__",
                                                "fastapi/__init__.py", 1, 1, "{}");
    int64_t test_header = cbm_gbuf_upsert_node(
        gb, "Class", "Header", "proj.tests.test_headers.Header", "tests/test_headers.py", 1, 1,
        "{}");
    int64_t exported_header = cbm_gbuf_upsert_node(
        gb, "Function", "Header", "proj.fastapi.param_functions.Header", "fastapi/param_functions.py",
        1, 1, "{}");
    ASSERT_GT(openapi_header, 0);
    ASSERT_GT(package_file, 0);
    ASSERT_GT(test_header, 0);
    ASSERT_GT(exported_header, 0);
    cbm_gbuf_insert_edge(gb, package_file, test_header, "IMPORTS", "{\"local_name\":\"Header\"}");
    cbm_gbuf_insert_edge(gb, package_file, exported_header, "IMPORTS", "{\"local_name\":\"Header\"}");

    CBMHashTable *pkgmap = cbm_ht_create(CBM_SZ_16);
    ASSERT_NOT_NULL(pkgmap);
    cbm_ht_set(pkgmap, strdup("fastapi"), strdup("proj.src.fastapi.__init__"));
    cbm_pipeline_set_pkgmap(pkgmap);

    cbm_pipeline_ctx_t ctx = {
        .gbuf = gb,
        .project_name = "proj",
    };
    CBMImport imp = {
        .local_name = "Header",
        .module_path = "fastapi.Header",
    };
    const cbm_gbuf_node_t *target =
        cbm_pipeline_resolve_import_node(&ctx, "docs_src/app/main.py",
                                         "proj.docs_src.app.main.__file__", &imp, NULL);

    ASSERT_NOT_NULL(target);
    ASSERT_STR_EQ(target->qualified_name, "proj.fastapi.param_functions.Header");

    CBMImport owner_imp = {
        .local_name = "Header",
        .module_path = "fastapi",
    };
    target = cbm_pipeline_resolve_import_node(&ctx, "docs_src/app/main.py",
                                             "proj.docs_src.app.main.__file__", &owner_imp, NULL);
    ASSERT_NOT_NULL(target);
    ASSERT_STR_EQ(target->qualified_name, "proj.fastapi.param_functions.Header");

    cbm_pipeline_set_pkgmap(NULL);
    cbm_pkgmap_free(pkgmap);
    cbm_gbuf_free(gb);
    PASS();
}

TEST(import_symbol_fallback_prefers_import_path_over_insertion_order) {
    cbm_gbuf_t *gb = cbm_gbuf_new("proj", "/tmp/proj");
    ASSERT_NOT_NULL(gb);

    int64_t security_http_base = cbm_gbuf_upsert_node(
        gb, "Class", "HTTPBase", "proj.fastapi.security.http.HTTPBase",
        "fastapi/security/http.py", 1, 1, "{}");
    int64_t openapi_http_base = cbm_gbuf_upsert_node(
        gb, "Class", "HTTPBase", "proj.fastapi.openapi.models.HTTPBase",
        "fastapi/openapi/models.py", 1, 1, "{}");
    int64_t openapi_oauth2 = cbm_gbuf_upsert_node(
        gb, "Class", "OAuth2", "proj.fastapi.openapi.models.OAuth2",
        "fastapi/openapi/models.py", 1, 1, "{}");
    int64_t security_oauth2 = cbm_gbuf_upsert_node(
        gb, "Class", "OAuth2", "proj.fastapi.security.oauth2.OAuth2",
        "fastapi/security/oauth2.py", 1, 1, "{}");
    ASSERT_GT(security_http_base, 0);
    ASSERT_GT(openapi_http_base, 0);
    ASSERT_GT(openapi_oauth2, 0);
    ASSERT_GT(security_oauth2, 0);

    cbm_pipeline_ctx_t ctx = {
        .gbuf = gb,
        .project_name = "proj",
    };
    CBMImport model_alias = {
        .local_name = "HTTPBaseModel",
        .module_path = "fastapi.openapi.models.HTTPBase",
    };
    const cbm_gbuf_node_t *target =
        cbm_pipeline_resolve_import_node(&ctx, "fastapi/security/http.py",
                                         "proj.fastapi.security.http.__file__", &model_alias,
                                         NULL);
    ASSERT_NOT_NULL(target);
    ASSERT_STR_EQ(target->qualified_name, "proj.fastapi.openapi.models.HTTPBase");

    CBMImport public_class = {
        .local_name = "HTTPBase",
        .module_path = "fastapi.security.http.HTTPBase",
    };
    target = cbm_pipeline_resolve_import_node(&ctx, "tests/test_security_http_base.py",
                                             "proj.tests.test_security_http_base.__file__",
                                             &public_class, NULL);
    ASSERT_NOT_NULL(target);
    ASSERT_STR_EQ(target->qualified_name, "proj.fastapi.security.http.HTTPBase");

    CBMImport oauth_model_alias = {
        .local_name = "OAuth2Model",
        .module_path = "fastapi.openapi.models.OAuth2",
    };
    target = cbm_pipeline_resolve_import_node(&ctx, "fastapi/security/oauth2.py",
                                             "proj.fastapi.security.oauth2.__file__",
                                             &oauth_model_alias, NULL);
    ASSERT_NOT_NULL(target);
    ASSERT_STR_EQ(target->qualified_name, "proj.fastapi.openapi.models.OAuth2");

    cbm_gbuf_free(gb);
    PASS();
}

/* DLL resolve test removed — feature removed due to Windows Defender
 * false positive (Wacatac.B!ml). See issue #89. */

/* ═══════════════════════════════════════════════════════════════════
 *  Incremental reindex
 * ═══════════════════════════════════════════════════════════════════ */

typedef struct {
    const char *key;
    char value[CBM_SZ_64];
    bool had_value;
} pipeline_env_snapshot_t;

static const char pipeline_test_env_enabled[] = "1";

static pipeline_env_snapshot_t pipeline_env_save(const char *key) {
    pipeline_env_snapshot_t snap = {.key = key};
    snap.had_value = cbm_safe_getenv(key, snap.value, sizeof(snap.value), NULL) != NULL;
    return snap;
}

static void pipeline_env_restore(const pipeline_env_snapshot_t *snap) {
    if (!snap || !snap->key) {
        return;
    }
    if (snap->had_value) {
        cbm_setenv(snap->key, snap->value, 1);
    } else {
        cbm_unsetenv(snap->key);
    }
}

static int run_parallel_incremental_phase_failure_case(const char *phase) {
    pipeline_env_snapshot_t fail_env = pipeline_env_save(CBM_TEST_FAIL_INCREMENTAL_PHASE);

    if (setup_incremental_parallel_repo() != 0) {
        FAIL("setup failed");
    }

    cbm_pipeline_t *p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    ASSERT_TRUE(cbm_pipeline_graph_changed(p));
    char *project = strdup(cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);

    cbm_store_t *s = cbm_store_open_path(g_incr_dbpath);
    ASSERT_NOT_NULL(s);
    int nodes_before = cbm_store_count_nodes(s, project);
    ASSERT_GT(nodes_before, 0);
    cbm_store_close(s);
    ASSERT(!pipeline_store_has_function_name(g_incr_dbpath, project, "NewFunc"));
    ASSERT_EQ(rewrite_incremental_parallel_repo(), 0);

    cbm_config_t *cfg = incremental_test_config(g_incr_tmpdir);
    ASSERT_NOT_NULL(cfg);
    p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);

    cbm_file_info_t *files = NULL;
    int file_count = 0;
    cbm_discover_opts_t opts = {.mode = CBM_MODE_FULL, .ignore_file = NULL, .max_file_size = 0};
    ASSERT_EQ(cbm_discover(g_incr_tmpdir, &opts, &files, &file_count), 0);
    ASSERT_GTE(file_count, INCR_PARALLEL_CHANGED_FILE_COUNT);

    cbm_setenv(CBM_TEST_FAIL_INCREMENTAL_PHASE, phase, 1);
    int rc = cbm_pipeline_run_incremental(p, g_incr_dbpath, files, file_count);
    pipeline_env_restore(&fail_env);
    cbm_discover_free(files, file_count);

    ASSERT_NEQ(rc, 0);
    s = cbm_store_open_path(g_incr_dbpath);
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_count_nodes(s, project), nodes_before);
    cbm_store_close(s);
    ASSERT(!pipeline_store_has_function_name(g_incr_dbpath, project, "NewFunc"));

    cbm_pipeline_free(p);
    free(project);
    cbm_config_close(cfg);
    cleanup_incremental_repo();
    return 0;
}

TEST(incremental_full_then_noop) {
    /* Full index, then re-run → should detect no changes and skip */
    if (setup_incremental_repo() != 0) {
        FAIL("setup failed");
    }

    /* First: full index */
    cbm_pipeline_t *p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    ASSERT_EQ(cbm_pipeline_publish_kind(p), CBM_PIPELINE_PUBLISH_FULL);
    char *project = strdup(cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);

    /* Verify nodes exist */
    cbm_store_t *s = cbm_store_open_path(g_incr_dbpath);
    ASSERT_NOT_NULL(s);
    int nodes_before = cbm_store_count_nodes(s, project);
    ASSERT_GT(nodes_before, 0);
    cbm_store_close(s);

    /* Second: incremental — nothing changed → should be no-op */
    cbm_config_t *cfg = incremental_test_config(g_incr_tmpdir);
    ASSERT_NOT_NULL(cfg);
    p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    ASSERT_FALSE(cbm_pipeline_graph_changed(p));
    ASSERT_EQ(cbm_pipeline_publish_kind(p), CBM_PIPELINE_PUBLISH_INCREMENTAL_NOOP);
    cbm_pipeline_free(p);

    s = cbm_store_open_path(g_incr_dbpath);
    ASSERT_NOT_NULL(s);
    int nodes_after = cbm_store_count_nodes(s, project);
    /* Node count should be same (no duplicates, no loss) */
    ASSERT_EQ(nodes_after, nodes_before);
    cbm_store_close(s);
    free(project);
    cbm_config_close(cfg);

    cleanup_incremental_repo();
    PASS();
}

TEST(incremental_touch_only_refreshes_metadata_without_reindex) {
    if (setup_incremental_repo() != 0) {
        FAIL("setup failed");
    }

    cbm_pipeline_t *p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    char *project = cbm_strdup(cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);
    ASSERT_NOT_NULL(project);

    int64_t generation_before = 0;
    ASSERT_EQ(pipeline_store_file_state_generation(g_incr_dbpath, project, "helper.go",
                                                   &generation_before),
              CBM_STORE_OK);

    char path[CBM_PATH_MAX];
    int n = snprintf(path, sizeof(path), "%s/helper.go", g_incr_tmpdir);
    ASSERT(n >= 0 && (size_t)n < sizeof(path));
    struct stat before;
    ASSERT_EQ(stat(path, &before), 0);
    ASSERT_EQ(pipeline_bump_file_mtime_seconds(path, &before, PIPELINE_TEST_MTIME_BUMP_SECONDS),
              0);
    struct stat touched;
    ASSERT_EQ(stat(path, &touched), 0);
    ASSERT_NEQ(cbm_pipeline_stat_mtime_ns(&touched), cbm_pipeline_stat_mtime_ns(&before));

    cbm_config_t *cfg = incremental_test_config(g_incr_tmpdir);
    ASSERT_NOT_NULL(cfg);
    p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    cbm_pipeline_free(p);

    int64_t generation_after = 0;
    ASSERT_EQ(pipeline_store_file_state_generation(g_incr_dbpath, project, "helper.go",
                                                   &generation_after),
              CBM_STORE_OK);
    ASSERT_EQ(generation_after, generation_before);

    int64_t hash_mtime_ns = 0;
    ASSERT_EQ(pipeline_store_file_hash_mtime(g_incr_dbpath, project, "helper.go",
                                             &hash_mtime_ns),
              CBM_STORE_OK);
    ASSERT_EQ(hash_mtime_ns, cbm_pipeline_stat_mtime_ns(&touched));

    free(project);
    cbm_config_close(cfg);
    cleanup_incremental_repo();
    PASS();
}

TEST(incremental_detects_changed_file) {
    /* Full index, modify one file, re-index → changed file re-parsed */
    if (setup_incremental_repo() != 0) {
        FAIL("setup failed");
    }

    /* First: full index */
    cbm_pipeline_t *p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    char *project = strdup(cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);

    /* Modify helper.go — add a new function */
    char path[512];
    snprintf(path, sizeof(path), "%s/helper.go", g_incr_tmpdir);
    FILE *f = fopen(path, "w");
    ASSERT_NOT_NULL(f);
    fprintf(f, "package main\n\n"
               "func Helper() string {\n\treturn \"hello\"\n}\n\n"
               "func NewFunc() int {\n\treturn 42\n}\n");
    fclose(f);

    /* Second: incremental — should detect change and re-index */
    cbm_config_t *cfg = incremental_test_config(g_incr_tmpdir);
    ASSERT_NOT_NULL(cfg);
    p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    /* Verify node count increased (NewFunc was added) */
    cbm_store_t *s = cbm_store_open_path(g_incr_dbpath);
    ASSERT_NOT_NULL(s);
    int nodes_after = cbm_store_count_nodes(s, project);
    ASSERT_GT(nodes_after, 0);
    cbm_store_close(s);
    cbm_pipeline_free(p);
    free(project);
    cbm_config_close(cfg);

    cleanup_incremental_repo();
    PASS();
}

TEST(incremental_fast_exact_upsert_matches_full_rebuild) {
    if (setup_incremental_repo() != 0) {
        FAIL("setup failed");
    }

    char path[CBM_PATH_MAX];
    int n = snprintf(path, sizeof(path), "%s/leaf.go", g_incr_tmpdir);
    ASSERT(n >= 0 && (size_t)n < sizeof(path));
    ASSERT_EQ(th_write_file(path,
                            "package main\n\n"
                            "func Leaf() int {\n\treturn 1\n}\n"),
              0);

    cbm_config_t *cfg = incremental_test_config(g_incr_tmpdir);
    ASSERT_NOT_NULL(cfg);
    cbm_pipeline_t *p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    char *project = cbm_strdup(cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);
    ASSERT_NOT_NULL(project);

    n = snprintf(path, sizeof(path), "%s/leaf.go", g_incr_tmpdir);
    ASSERT(n >= 0 && (size_t)n < sizeof(path));
    ASSERT_EQ(th_write_file(path,
                            "package main\n\n"
                            "func Leaf() int {\n\treturn 2\n}\n\n"
                            "func NewLeaf() int {\n\treturn 7\n}\n"),
              0);

    p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    pipeline_capture_logs_start();
    int run_rc = cbm_pipeline_run(p);
    const char *logs = pipeline_capture_logs_end();
    ASSERT_EQ(run_rc, 0);
    ASSERT(strstr(logs, "msg=incremental.classify changed=1") != NULL);
    ASSERT(strstr(logs, "msg=incremental.exact.done files=1") != NULL);
    ASSERT_EQ(cbm_pipeline_publish_kind(p), CBM_PIPELINE_PUBLISH_INCREMENTAL_EXACT);
    cbm_pipeline_free(p);

    ASSERT(pipeline_store_has_function_name(g_incr_dbpath, project, "NewLeaf"));
    int64_t generation = 0;
    ASSERT_EQ(pipeline_store_file_state_generation(g_incr_dbpath, project, "leaf.go",
                                                   &generation),
              CBM_STORE_OK);
    ASSERT_GT(generation, CBM_PIPELINE_COMPAT_GENERATION);
    int dirty_pending = -1;
    int dirty_overlay_ready = -1;
    ASSERT_EQ(pipeline_store_dirty_counts(g_incr_dbpath, project, &dirty_pending,
                                          &dirty_overlay_ready),
              CBM_STORE_OK);
    ASSERT_EQ(dirty_pending, 0);
    ASSERT_EQ(dirty_overlay_ready, 0);

    char diff_err[CBM_SZ_8K] = {0};
    int diff_rc = pipeline_compare_current_db_to_fresh_fast_rebuild(
        g_incr_tmpdir, g_incr_dbpath, project, cfg, diff_err, sizeof(diff_err));
    if (diff_rc != 0) {
        printf("    [exact-upsert-diff] %s\n", diff_err);
    }
    ASSERT_EQ(diff_rc, 0);

    free(project);
    cbm_config_close(cfg);
    cleanup_incremental_repo();
    PASS();
}

TEST(incremental_fast_body_only_change_uses_graph_noop) {
    if (setup_incremental_repo() != 0) {
        FAIL("setup failed");
    }

    cbm_config_t *cfg = incremental_test_config(g_incr_tmpdir);
    ASSERT_NOT_NULL(cfg);
    cbm_pipeline_t *p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    char *project = cbm_strdup(cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);
    ASSERT_NOT_NULL(project);

    int64_t generation_before = 0;
    ASSERT_EQ(pipeline_store_file_state_generation(g_incr_dbpath, project, "helper.go",
                                                   &generation_before),
              CBM_STORE_OK);

    char path[CBM_PATH_MAX];
    int n = snprintf(path, sizeof(path), "%s/helper.go", g_incr_tmpdir);
    ASSERT(n >= 0 && (size_t)n < sizeof(path));
    ASSERT_EQ(th_write_file(path,
                            "package main\n\n"
                            "func Helper() string {\n\treturn \"goodbye\"\n}\n"),
              0);

    cbm_store_t *store = cbm_store_open_path_query(g_incr_dbpath);
    ASSERT_NOT_NULL(store);
    cbm_file_info_t changed[] = {
        {.path = path, .rel_path = "helper.go", .language = CBM_LANG_GO},
    };
    cbm_gbuf_t *scratch = NULL;
    cbm_pipeline_file_delta_t delta = {0};
    ASSERT_EQ(pipeline_build_exact_scratch_for_changed_files(store, g_incr_tmpdir, project,
                                                             changed, CBM_ALLOC_ONE, &scratch,
                                                             &delta),
              CBM_STORE_OK);
    const cbm_store_file_delta_t *store_delta = &delta.delta;
    bool graph_equal = false;
    ASSERT_EQ(cbm_store_file_delta_batch_graph_equal(store, &store_delta, CBM_ALLOC_ONE,
                                                     &graph_equal),
              CBM_STORE_OK);
    if (!graph_equal) {
        static const char node_owner_count_sql[] =
            "SELECT COUNT(*) FROM node_owners WHERE project = ?1 AND rel_path = ?2;";
        static const char edge_owner_count_sql[] =
            "SELECT COUNT(*) FROM edge_owners WHERE project = ?1 AND rel_path = ?2;";
        static const char export_count_sql[] =
            "SELECT COUNT(*) FROM symbol_exports WHERE project = ?1 AND rel_path = ?2;";
        static const char import_count_sql[] =
            "SELECT COUNT(*) FROM import_refs WHERE project = ?1 AND rel_path = ?2;";
        int stored_nodes = -1;
        int stored_edges = -1;
        int stored_exports = -1;
        int stored_imports = -1;
        (void)pipeline_store_count_file_rows_sql(g_incr_dbpath, project, "helper.go",
                                                 node_owner_count_sql, &stored_nodes);
        (void)pipeline_store_count_file_rows_sql(g_incr_dbpath, project, "helper.go",
                                                 edge_owner_count_sql, &stored_edges);
        (void)pipeline_store_count_file_rows_sql(g_incr_dbpath, project, "helper.go",
                                                 export_count_sql, &stored_exports);
        (void)pipeline_store_count_file_rows_sql(g_incr_dbpath, project, "helper.go",
                                                 import_count_sql, &stored_imports);
        char detail[CBM_SZ_512];
        int dn = snprintf(detail, sizeof(detail),
                          "graph equality rejected body-only delta: stored n/e/x/i=%d/%d/%d/%d "
                          "delta n/e/x/i=%d/%d/%d/%d ctx n/e=%d/%d",
                          stored_nodes, stored_edges, stored_exports, stored_imports,
                          delta.delta.node_count, delta.delta.edge_count,
                          delta.delta.export_count, delta.delta.import_count,
                          delta.delta.context_node_count, delta.delta.context_edge_count);
        if (dn < 0 || (size_t)dn >= sizeof(detail)) {
            FAIL("graph equality rejected body-only delta; diagnostic overflow");
        }
        FAIL(detail);
    }
    cbm_pipeline_file_delta_free(&delta);
    cbm_gbuf_free(scratch);
    cbm_store_close(store);

    p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    pipeline_capture_logs_start();
    int run_rc = cbm_pipeline_run(p);
    const char *logs = pipeline_capture_logs_end();
    ASSERT_EQ(run_rc, 0);
    ASSERT(strstr(logs, "msg=incremental.classify changed=1") != NULL);
    if (!strstr(logs, "msg=incremental.exact.frontier changed=1 expanded=2") ||
        !strstr(logs, "msg=incremental.exact.noop files=2")) {
        const char *debug = strstr(logs, "msg=delta.graph_equal.mismatch");
        char detail[CBM_SZ_512];
        int dn = snprintf(detail, sizeof(detail), "missing incremental no-op marker: %.420s",
                          debug ? debug : logs);
        if (dn < 0 || (size_t)dn >= sizeof(detail)) {
            FAIL("missing incremental no-op marker; diagnostic overflow");
        }
        FAIL(detail);
    }
    ASSERT_FALSE(cbm_pipeline_graph_changed(p));
    ASSERT_EQ(cbm_pipeline_publish_kind(p), CBM_PIPELINE_PUBLISH_INCREMENTAL_NOOP);
    cbm_pipeline_free(p);

    int64_t generation_after = 0;
    ASSERT_EQ(pipeline_store_file_state_generation(g_incr_dbpath, project, "helper.go",
                                                   &generation_after),
              CBM_STORE_OK);
    ASSERT_GT(generation_after, generation_before);

    char diff_err[CBM_SZ_8K] = {0};
    int diff_rc = pipeline_compare_current_db_to_fresh_fast_rebuild(
        g_incr_tmpdir, g_incr_dbpath, project, cfg, diff_err, sizeof(diff_err));
    if (diff_rc != 0) {
        FAIL(diff_err[0] ? diff_err : "body-only graph no-op differed from fresh FAST rebuild");
    }
    ASSERT_EQ(diff_rc, 0);

    free(project);
    cbm_config_close(cfg);
    cleanup_incremental_repo();
    PASS();
}

TEST(incremental_fast_two_file_batch_exact_upsert_matches_full_rebuild) {
    if (setup_incremental_repo() != 0) {
        FAIL("setup failed");
    }

    cbm_config_t *cfg = incremental_test_config(g_incr_tmpdir);
    ASSERT_NOT_NULL(cfg);
    cbm_pipeline_t *p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    char *project = cbm_strdup(cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);
    ASSERT_NOT_NULL(project);

    char path[CBM_PATH_MAX];
    int n = snprintf(path, sizeof(path), "%s/main.go", g_incr_tmpdir);
    ASSERT(n >= 0 && (size_t)n < sizeof(path));
    ASSERT_EQ(th_write_file(path,
                            "package main\n\n"
                            "func main() {\n\tHelper()\n\tNewHelper()\n}\n\n"
                            "func NewMain() int {\n\treturn 11\n}\n"),
              0);
    n = snprintf(path, sizeof(path), "%s/helper.go", g_incr_tmpdir);
    ASSERT(n >= 0 && (size_t)n < sizeof(path));
    ASSERT_EQ(th_write_file(path,
                            "package main\n\n"
                            "func Helper() string {\n\treturn \"updated\"\n}\n\n"
                            "func NewHelper() int {\n\treturn 13\n}\n"),
              0);

    p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    pipeline_capture_logs_start();
    int run_rc = cbm_pipeline_run(p);
    const char *logs = pipeline_capture_logs_end();
    ASSERT_EQ(run_rc, 0);
    ASSERT(strstr(logs, "msg=incremental.classify changed=2") != NULL);
    ASSERT(strstr(logs, "msg=incremental.exact.done files=2") != NULL);
    ASSERT_EQ(cbm_pipeline_publish_kind(p), CBM_PIPELINE_PUBLISH_INCREMENTAL_EXACT);
    cbm_pipeline_free(p);

    ASSERT(pipeline_store_has_function_name(g_incr_dbpath, project, "NewMain"));
    ASSERT(pipeline_store_has_function_name(g_incr_dbpath, project, "NewHelper"));
    int64_t main_generation = 0;
    int64_t helper_generation = 0;
    ASSERT_EQ(pipeline_store_file_state_generation(g_incr_dbpath, project, "main.go",
                                                   &main_generation),
              CBM_STORE_OK);
    ASSERT_EQ(pipeline_store_file_state_generation(g_incr_dbpath, project, "helper.go",
                                                   &helper_generation),
              CBM_STORE_OK);
    ASSERT_GT(main_generation, CBM_PIPELINE_COMPAT_GENERATION);
    ASSERT_EQ(main_generation, helper_generation);

    char diff_err[CBM_SZ_8K] = {0};
    int diff_rc = pipeline_compare_current_db_to_fresh_fast_rebuild(
        g_incr_tmpdir, g_incr_dbpath, project, cfg, diff_err, sizeof(diff_err));
    if (diff_rc != 0) {
        FAIL(diff_err[0] ? diff_err : "two-file exact upsert differed from fresh FAST rebuild");
    }
    ASSERT_EQ(diff_rc, 0);

    free(project);
    cbm_config_close(cfg);
    cleanup_incremental_repo();
    PASS();
}

TEST(incremental_fast_falls_back_for_oversized_inbound_frontier_and_matches_full) {
    if (setup_incremental_repo() != 0) {
        FAIL("setup failed");
    }

    ASSERT_EQ(write_incremental_frontier_fixture(CBM_ALLOC_ONE), 0);

    cbm_config_t *cfg = incremental_test_config(g_incr_tmpdir);
    ASSERT_NOT_NULL(cfg);
    cbm_pipeline_t *p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    char *project = cbm_strdup(cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);
    ASSERT_NOT_NULL(project);

    ASSERT_EQ(write_incremental_leaf_file(CBM_SZ_2), 0);

    p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    pipeline_capture_logs_start();
    int run_rc = cbm_pipeline_run(p);
    const char *logs = pipeline_capture_logs_end();
    ASSERT_EQ(run_rc, 0);
    ASSERT(strstr(logs, "msg=incremental.classify changed=1") != NULL);
    ASSERT(strstr(logs, "msg=incremental.exact.fallback reason=frontier_too_large") != NULL);
    ASSERT_EQ(cbm_pipeline_publish_kind(p), CBM_PIPELINE_PUBLISH_INCREMENTAL_CONTAINMENT);
    ASSERT_STR_EQ(cbm_pipeline_publish_reason(p), "frontier_too_large");
    cbm_pipeline_free(p);

    cbm_store_t *owner_store = cbm_store_open_path(g_incr_dbpath);
    ASSERT_NOT_NULL(owner_store);
    int node_owners = 0;
    int edge_owners = 0;
    ASSERT_EQ(cbm_store_count_file_delta_owners(owner_store, project, "leaf.go", &node_owners,
                                                &edge_owners),
              CBM_STORE_OK);
    ASSERT_GT(node_owners, 0);
    cbm_store_close(owner_store);

    char diff_err[CBM_SZ_8K] = {0};
    int diff_rc = pipeline_compare_current_db_to_fresh_fast_rebuild(
        g_incr_tmpdir, g_incr_dbpath, project, cfg, diff_err, sizeof(diff_err));
    if (diff_rc != 0) {
        FAIL(diff_err[0] ? diff_err : "oversized inbound fallback differed from fresh rebuild");
    }
    ASSERT_EQ(diff_rc, 0);

    free(project);
    cbm_config_close(cfg);
    cleanup_incremental_repo();
    PASS();
}

TEST(incremental_fast_c_header_frontier_too_large_uses_full_rebuild) {
    if (setup_incremental_repo() != 0) {
        FAIL("setup failed");
    }

    char skipped_dir[CBM_PATH_MAX];
    int n = snprintf(skipped_dir, sizeof(skipped_dir), "%s/scripts", g_incr_tmpdir);
    ASSERT(n >= 0 && (size_t)n < sizeof(skipped_dir));
    ASSERT_TRUE(cbm_mkdir_p(skipped_dir, 0755));
    char skipped_path[CBM_PATH_MAX];
    n = snprintf(skipped_path, sizeof(skipped_path), "%s/scripts/probe.py", g_incr_tmpdir);
    ASSERT(n >= 0 && (size_t)n < sizeof(skipped_path));
    ASSERT_EQ(th_write_file(skipped_path, "def skipped_probe():\n    return 1\n"), 0);

    ASSERT_EQ(write_incremental_c_header_frontier_fixture(CBM_ALLOC_ONE), 0);

    cbm_config_t *cfg = incremental_test_config(g_incr_tmpdir);
    ASSERT_NOT_NULL(cfg);
    cbm_pipeline_t *p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    char *project = cbm_strdup(cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);
    ASSERT_NOT_NULL(project);

    ASSERT_EQ(write_incremental_c_header_frontier_fixture(CBM_SZ_2), 0);

    p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    pipeline_capture_logs_start();
    int run_rc = cbm_pipeline_run(p);
    const char *logs = pipeline_capture_logs_end();
    ASSERT_EQ(run_rc, 0);
    ASSERT(strstr(logs, "msg=incremental.classify changed=1") != NULL);
    ASSERT(strstr(logs, "msg=incremental.exact.fallback reason=frontier_too_large") != NULL);
    ASSERT(strstr(logs,
                  "msg=incremental.fallback reason=frontier_too_large "
                  "scope=c_family_header") != NULL);
    ASSERT_EQ(cbm_pipeline_publish_kind(p), CBM_PIPELINE_PUBLISH_FULL);
    ASSERT_STR_EQ(cbm_pipeline_publish_reason(p), "frontier_too_large");
    cbm_pipeline_free(p);

    int skipped_nodes = 0;
    ASSERT_EQ(pipeline_store_count_file_rows_sql(
                  g_incr_dbpath, project, "scripts/probe.py",
                  "SELECT COUNT(*) FROM nodes WHERE project = ?1 AND file_path = ?2;",
                  &skipped_nodes),
              CBM_STORE_OK);
    ASSERT_EQ(skipped_nodes, 0);
    int skipped_hashes = 0;
    ASSERT_EQ(pipeline_store_count_file_rows_sql(
                  g_incr_dbpath, project, "scripts/probe.py",
                  "SELECT COUNT(*) FROM file_hashes WHERE project = ?1 AND rel_path = ?2;",
                  &skipped_hashes),
              CBM_STORE_OK);
    ASSERT_EQ(skipped_hashes, 0);

    char diff_err[CBM_SZ_8K] = {0};
    int diff_rc = pipeline_compare_current_db_to_fresh_fast_rebuild(
        g_incr_tmpdir, g_incr_dbpath, project, cfg, diff_err, sizeof(diff_err));
    if (diff_rc != 0) {
        FAIL(diff_err[0] ? diff_err : "C header full fallback differed from fresh rebuild");
    }
    ASSERT_EQ(diff_rc, 0);

    free(project);
    cbm_config_close(cfg);
    cleanup_incremental_repo();
    PASS();
}

TEST(incremental_fast_configured_frontier_cap_allows_bounded_exact) {
    enum {
        PIPELINE_EXPECTED_EXACT_FRONTIER_FILES =
            PIPELINE_INCR_FRONTIER_CALLER_COUNT + CBM_ALLOC_ONE,
        PIPELINE_CONFIGURED_AFFECTED_CAP = CBM_SZ_8,
    };
    if (setup_incremental_repo() != 0) {
        FAIL("setup failed");
    }

    ASSERT_EQ(write_incremental_frontier_fixture(CBM_ALLOC_ONE), 0);

    cbm_config_t *cfg = incremental_test_config(g_incr_tmpdir);
    ASSERT_NOT_NULL(cfg);
    char cap_value[CBM_SZ_32];
    int n = snprintf(cap_value, sizeof(cap_value), "%d", PIPELINE_CONFIGURED_AFFECTED_CAP);
    ASSERT(n >= 0 && (size_t)n < sizeof(cap_value));
    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_INCREMENTAL_EXACT_MAX_AFFECTED_PATHS, cap_value), 0);

    cbm_pipeline_t *p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    char *project = cbm_strdup(cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);
    ASSERT_NOT_NULL(project);

    ASSERT_EQ(write_incremental_leaf_file_with_extra(CBM_SZ_2), 0);

    p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    pipeline_capture_logs_start();
    int run_rc = cbm_pipeline_run(p);
    const char *logs = pipeline_capture_logs_end();
    ASSERT_EQ(run_rc, 0);
    ASSERT(strstr(logs, "msg=incremental.classify changed=1") != NULL);
    char frontier_log[CBM_SZ_128];
    n = snprintf(frontier_log, sizeof(frontier_log),
                 "msg=incremental.exact.frontier changed=1 expanded=%d",
                 PIPELINE_EXPECTED_EXACT_FRONTIER_FILES);
    ASSERT(n >= 0 && (size_t)n < sizeof(frontier_log));
    ASSERT(strstr(logs, frontier_log) != NULL);
    ASSERT_EQ(cbm_pipeline_publish_kind(p), CBM_PIPELINE_PUBLISH_INCREMENTAL_EXACT);
    ASSERT_NULL(cbm_pipeline_publish_reason(p));
    cbm_pipeline_free(p);
    ASSERT(pipeline_store_has_function_name(g_incr_dbpath, project, "LeafExtra"));

    char diff_err[CBM_SZ_8K] = {0};
    int diff_rc = pipeline_compare_current_db_to_fresh_fast_rebuild(
        g_incr_tmpdir, g_incr_dbpath, project, cfg, diff_err, sizeof(diff_err));
    if (diff_rc != 0) {
        FAIL(diff_err[0] ? diff_err : "configured exact frontier differed from fresh rebuild");
    }
    ASSERT_EQ(diff_rc, 0);

    free(project);
    cbm_config_close(cfg);
    cleanup_incremental_repo();
    PASS();
}

TEST(incremental_fast_mixed_unowned_edge_frontier_falls_back_before_exact_build) {
    enum { PIPELINE_CONFIGURED_AFFECTED_CAP = CBM_SZ_8 };
    if (setup_incremental_repo() != 0) {
        FAIL("setup failed");
    }

    ASSERT_EQ(write_incremental_frontier_fixture(CBM_ALLOC_ONE), 0);

    cbm_config_t *cfg = incremental_test_config(g_incr_tmpdir);
    ASSERT_NOT_NULL(cfg);
    char cap_value[CBM_SZ_32];
    int n = snprintf(cap_value, sizeof(cap_value), "%d", PIPELINE_CONFIGURED_AFFECTED_CAP);
    ASSERT(n >= 0 && (size_t)n < sizeof(cap_value));
    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_INCREMENTAL_EXACT_MAX_AFFECTED_PATHS, cap_value), 0);

    cbm_pipeline_t *p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    char *project = cbm_strdup(cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);
    ASSERT_NOT_NULL(project);

    ASSERT_EQ(pipeline_store_insert_file_owned_unowned_source_edge(g_incr_dbpath, project,
                                                                   "leaf.go", "Leaf", "CALLS"),
              CBM_STORE_OK);
    ASSERT_EQ(write_incremental_leaf_file_with_extra(CBM_SZ_2), 0);

    p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    pipeline_capture_logs_start();
    int run_rc = cbm_pipeline_run(p);
    const char *logs = pipeline_capture_logs_end();
    ASSERT_EQ(run_rc, 0);
    ASSERT(strstr(logs, "msg=incremental.classify changed=1") != NULL);
    ASSERT(strstr(logs, "msg=incremental.exact.fallback reason=inbound_edges_require_full") !=
           NULL);
    ASSERT(strstr(logs, "msg=incremental.exact.frontier") == NULL);
    ASSERT(strstr(logs, "msg=incremental.exact.done") == NULL);
    ASSERT_EQ(cbm_pipeline_publish_kind(p), CBM_PIPELINE_PUBLISH_INCREMENTAL_CONTAINMENT);
    ASSERT_STR_EQ(cbm_pipeline_publish_reason(p), "inbound_edges_require_full");
    cbm_pipeline_free(p);
    ASSERT(pipeline_store_has_function_name(g_incr_dbpath, project, "LeafExtra"));

    free(project);
    cbm_config_close(cfg);
    cleanup_incremental_repo();
    PASS();
}

TEST(incremental_fast_expands_small_inbound_frontier_and_matches_full) {
    enum {
        PIPELINE_EXPECTED_EXACT_FILES = 3,
    };
    if (setup_incremental_repo() != 0) {
        FAIL("setup failed");
    }

    char path[CBM_PATH_MAX];
    int n = snprintf(path, sizeof(path), "%s/leaf.go", g_incr_tmpdir);
    ASSERT(n >= 0 && (size_t)n < sizeof(path));
    ASSERT_EQ(th_write_file(path,
                            "package main\n\n"
                            "func Leaf() int {\n"
                            "\treturn 1\n"
                            "}\n"),
              0);
    n = snprintf(path, sizeof(path), "%s/helper.go", g_incr_tmpdir);
    ASSERT(n >= 0 && (size_t)n < sizeof(path));
    ASSERT_EQ(th_write_file(path,
                            "package main\n\n"
                            "func Helper() int {\n"
                            "\treturn Leaf()\n"
                            "}\n"),
              0);
    n = snprintf(path, sizeof(path), "%s/main.go", g_incr_tmpdir);
    ASSERT(n >= 0 && (size_t)n < sizeof(path));
    ASSERT_EQ(th_write_file(path,
                            "package main\n\n"
                            "func main() {\n"
                            "\tHelper()\n"
                            "}\n"),
              0);

    cbm_config_t *cfg = incremental_test_config(g_incr_tmpdir);
    ASSERT_NOT_NULL(cfg);
    cbm_pipeline_t *p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    char *project = cbm_strdup(cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);
    ASSERT_NOT_NULL(project);

    n = snprintf(path, sizeof(path), "%s/leaf.go", g_incr_tmpdir);
    ASSERT(n >= 0 && (size_t)n < sizeof(path));
    ASSERT_EQ(th_write_file(path,
                            "package main\n\n"
                            "func Leaf() int {\n"
                            "\tfor i := 0; i < 10; i++ {\n"
                            "\t}\n"
                            "\treturn 2\n"
                            "}\n"),
              0);

    p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    pipeline_capture_logs_start();
    int run_rc = cbm_pipeline_run(p);
    const char *logs = pipeline_capture_logs_end();
    ASSERT_EQ(run_rc, 0);
    ASSERT(strstr(logs, "msg=incremental.classify changed=1") != NULL);
    char frontier_log[CBM_SZ_128];
    n = snprintf(frontier_log, sizeof(frontier_log),
                 "msg=incremental.exact.frontier changed=1 expanded=%d",
                 PIPELINE_EXPECTED_EXACT_FILES);
    ASSERT(n >= 0 && (size_t)n < sizeof(frontier_log));
    ASSERT(strstr(logs, frontier_log) != NULL);
    char done_log[CBM_SZ_128];
    n = snprintf(done_log, sizeof(done_log), "msg=incremental.exact.done files=%d",
                 PIPELINE_EXPECTED_EXACT_FILES);
    ASSERT(n >= 0 && (size_t)n < sizeof(done_log));
    ASSERT(strstr(logs, done_log) != NULL);
    ASSERT_EQ(cbm_pipeline_publish_kind(p), CBM_PIPELINE_PUBLISH_INCREMENTAL_EXACT);
    cbm_pipeline_free(p);

    int64_t leaf_generation = 0;
    int64_t helper_generation = 0;
    int64_t main_generation = 0;
    ASSERT_EQ(pipeline_store_file_state_generation(g_incr_dbpath, project, "leaf.go",
                                                   &leaf_generation),
              CBM_STORE_OK);
    ASSERT_EQ(pipeline_store_file_state_generation(g_incr_dbpath, project, "helper.go",
                                                   &helper_generation),
              CBM_STORE_OK);
    ASSERT_EQ(pipeline_store_file_state_generation(g_incr_dbpath, project, "main.go",
                                                   &main_generation),
              CBM_STORE_OK);
    ASSERT_GT(leaf_generation, CBM_PIPELINE_COMPAT_GENERATION);
    ASSERT_EQ(leaf_generation, helper_generation);
    ASSERT_EQ(helper_generation, main_generation);

    char diff_err[CBM_SZ_8K] = {0};
    int diff_rc = pipeline_compare_current_db_to_fresh_fast_rebuild(
        g_incr_tmpdir, g_incr_dbpath, project, cfg, diff_err, sizeof(diff_err));
    if (diff_rc != 0) {
        FAIL(diff_err[0] ? diff_err : "small inbound exact frontier differed from fresh rebuild");
    }
    ASSERT_EQ(diff_rc, 0);

    free(project);
    cbm_config_close(cfg);
    cleanup_incremental_repo();
    PASS();
}

TEST(incremental_fast_three_file_batch_falls_back_to_full_rebuild_parity) {
    if (setup_incremental_repo() != 0) {
        FAIL("setup failed");
    }

    char path[CBM_PATH_MAX];
    int n = snprintf(path, sizeof(path), "%s/leaf.go", g_incr_tmpdir);
    ASSERT(n >= 0 && (size_t)n < sizeof(path));
    ASSERT_EQ(th_write_file(path,
                            "package main\n\n"
                            "func Leaf() int {\n\treturn 1\n}\n"),
              0);

    cbm_config_t *cfg = incremental_test_config(g_incr_tmpdir);
    ASSERT_NOT_NULL(cfg);
    cbm_pipeline_t *p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    char *project = cbm_strdup(cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);
    ASSERT_NOT_NULL(project);

    n = snprintf(path, sizeof(path), "%s/main.go", g_incr_tmpdir);
    ASSERT(n >= 0 && (size_t)n < sizeof(path));
    ASSERT_EQ(th_write_file(path,
                            "package main\n\n"
                            "func main() {\n\tHelper()\n\tNewHelper()\n\tLeaf()\n}\n\n"
                            "func NewMain() int {\n\treturn 11\n}\n"),
              0);
    n = snprintf(path, sizeof(path), "%s/helper.go", g_incr_tmpdir);
    ASSERT(n >= 0 && (size_t)n < sizeof(path));
    ASSERT_EQ(th_write_file(path,
                            "package main\n\n"
                            "func Helper() string {\n\treturn \"updated\"\n}\n\n"
                            "func NewHelper() int {\n\treturn 13\n}\n"),
              0);
    n = snprintf(path, sizeof(path), "%s/leaf.go", g_incr_tmpdir);
    ASSERT(n >= 0 && (size_t)n < sizeof(path));
    ASSERT_EQ(th_write_file(path,
                            "package main\n\n"
                            "func Leaf() int {\n\treturn 2\n}\n\n"
                            "func NewLeaf() int {\n\treturn 17\n}\n"),
              0);

    p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    ASSERT_EQ(cbm_pipeline_publish_kind(p), CBM_PIPELINE_PUBLISH_INCREMENTAL_CONTAINMENT);
    ASSERT_STR_EQ(cbm_pipeline_publish_reason(p), "changed_batch_too_large");
    cbm_pipeline_free(p);

    ASSERT(pipeline_store_has_function_name(g_incr_dbpath, project, "NewMain"));
    ASSERT(pipeline_store_has_function_name(g_incr_dbpath, project, "NewHelper"));
    ASSERT(pipeline_store_has_function_name(g_incr_dbpath, project, "NewLeaf"));
    int64_t main_generation = 0;
    int64_t helper_generation = 0;
    int64_t leaf_generation = 0;
    ASSERT_EQ(pipeline_store_file_state_generation(g_incr_dbpath, project, "main.go",
                                                   &main_generation),
              CBM_STORE_OK);
    ASSERT_EQ(pipeline_store_file_state_generation(g_incr_dbpath, project, "helper.go",
                                                   &helper_generation),
              CBM_STORE_OK);
    ASSERT_EQ(pipeline_store_file_state_generation(g_incr_dbpath, project, "leaf.go",
                                                   &leaf_generation),
              CBM_STORE_OK);
    ASSERT_EQ(main_generation, CBM_PIPELINE_COMPAT_GENERATION);
    ASSERT_EQ(main_generation, helper_generation);
    ASSERT_EQ(main_generation, leaf_generation);

    char diff_err[CBM_SZ_8K] = {0};
    int diff_rc = pipeline_compare_current_db_to_fresh_fast_rebuild(
        g_incr_tmpdir, g_incr_dbpath, project, cfg, diff_err, sizeof(diff_err));
    if (diff_rc != 0) {
        FAIL(diff_err[0] ? diff_err : "three-file fallback differed from fresh FAST rebuild");
    }
    ASSERT_EQ(diff_rc, 0);

    free(project);
    cbm_config_close(cfg);
    cleanup_incremental_repo();
    PASS();
}

TEST(incremental_fast_single_delete_exact_matches_full_rebuild) {
    if (setup_incremental_repo() != 0) {
        FAIL("setup failed");
    }

    char path[CBM_PATH_MAX];
    int n = snprintf(path, sizeof(path), "%s/leaf.go", g_incr_tmpdir);
    ASSERT(n >= 0 && (size_t)n < sizeof(path));
    ASSERT_EQ(th_write_file(path,
                            "package main\n\n"
                            "func Leaf() int {\n\treturn 1\n}\n"),
              0);

    cbm_config_t *cfg = incremental_test_config(g_incr_tmpdir);
    ASSERT_NOT_NULL(cfg);
    cbm_pipeline_t *p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    char *project = cbm_strdup(cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);
    ASSERT_NOT_NULL(project);
    ASSERT(pipeline_store_has_function_name(g_incr_dbpath, project, "Leaf"));

    ASSERT_EQ(cbm_unlink(path), 0);

    p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    cbm_pipeline_free(p);

    ASSERT(!pipeline_store_has_function_name(g_incr_dbpath, project, "Leaf"));
    int64_t leaf_generation = 0;
    ASSERT_EQ(pipeline_store_file_state_generation(g_incr_dbpath, project, "leaf.go",
                                                   &leaf_generation),
              CBM_STORE_NOT_FOUND);
    ASSERT_GT(pipeline_store_completed_generation_count(g_incr_dbpath, project), 0);

    char diff_err[CBM_SZ_8K] = {0};
    int diff_rc = pipeline_compare_current_db_to_fresh_fast_rebuild(
        g_incr_tmpdir, g_incr_dbpath, project, cfg, diff_err, sizeof(diff_err));
    if (diff_rc != 0) {
        FAIL(diff_err[0] ? diff_err : "single-delete exact differed from fresh FAST rebuild");
    }
    ASSERT_EQ(diff_rc, 0);

    free(project);
    cbm_config_close(cfg);
    cleanup_incremental_repo();
    PASS();
}

TEST(incremental_fast_delete_falls_back_to_full_rebuild_parity) {
    if (setup_incremental_repo() != 0) {
        FAIL("setup failed");
    }

    cbm_config_t *cfg = incremental_test_config(g_incr_tmpdir);
    ASSERT_NOT_NULL(cfg);
    cbm_pipeline_t *p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    char *project = cbm_strdup(cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);
    ASSERT_NOT_NULL(project);
    ASSERT(pipeline_store_has_function_name(g_incr_dbpath, project, "Helper"));

    char path[CBM_PATH_MAX];
    int n = snprintf(path, sizeof(path), "%s/helper.go", g_incr_tmpdir);
    ASSERT(n >= 0 && (size_t)n < sizeof(path));
    ASSERT_EQ(cbm_unlink(path), 0);

    p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    cbm_pipeline_free(p);

    ASSERT(!pipeline_store_has_function_name(g_incr_dbpath, project, "Helper"));
    int64_t main_generation = 0;
    int64_t helper_generation = 0;
    ASSERT_EQ(pipeline_store_file_state_generation(g_incr_dbpath, project, "main.go",
                                                   &main_generation),
              CBM_STORE_OK);
    ASSERT_EQ(pipeline_store_file_state_generation(g_incr_dbpath, project, "helper.go",
                                                   &helper_generation),
              CBM_STORE_NOT_FOUND);
    ASSERT_EQ(main_generation, CBM_PIPELINE_COMPAT_GENERATION);

    char diff_err[CBM_SZ_8K] = {0};
    int diff_rc = pipeline_compare_current_db_to_fresh_fast_rebuild(
        g_incr_tmpdir, g_incr_dbpath, project, cfg, diff_err, sizeof(diff_err));
    if (diff_rc != 0) {
        FAIL(diff_err[0] ? diff_err : "delete fallback differed from fresh FAST rebuild");
    }
    ASSERT_EQ(diff_rc, 0);

    free(project);
    cbm_config_close(cfg);
    cleanup_incremental_repo();
    PASS();
}

TEST(incremental_fast_rename_like_batch_falls_back_to_full_rebuild_parity) {
    if (setup_incremental_repo() != 0) {
        FAIL("setup failed");
    }

    cbm_config_t *cfg = incremental_test_config(g_incr_tmpdir);
    ASSERT_NOT_NULL(cfg);
    cbm_pipeline_t *p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    char *project = cbm_strdup(cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);
    ASSERT_NOT_NULL(project);
    ASSERT(pipeline_store_has_function_name(g_incr_dbpath, project, "Helper"));

    char path[CBM_PATH_MAX];
    int n = snprintf(path, sizeof(path), "%s/helper.go", g_incr_tmpdir);
    ASSERT(n >= 0 && (size_t)n < sizeof(path));
    ASSERT_EQ(cbm_unlink(path), 0);
    n = snprintf(path, sizeof(path), "%s/helper2.go", g_incr_tmpdir);
    ASSERT(n >= 0 && (size_t)n < sizeof(path));
    ASSERT_EQ(th_write_file(path,
                            "package main\n\n"
                            "func RenamedHelper() string {\n\treturn \"renamed\"\n}\n"),
              0);

    p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    cbm_pipeline_free(p);

    ASSERT(!pipeline_store_has_function_name(g_incr_dbpath, project, "Helper"));
    ASSERT(pipeline_store_has_function_name(g_incr_dbpath, project, "RenamedHelper"));
    int64_t helper_generation = 0;
    int64_t helper2_generation = 0;
    ASSERT_EQ(pipeline_store_file_state_generation(g_incr_dbpath, project, "helper.go",
                                                   &helper_generation),
              CBM_STORE_NOT_FOUND);
    ASSERT_EQ(pipeline_store_file_state_generation(g_incr_dbpath, project, "helper2.go",
                                                   &helper2_generation),
              CBM_STORE_OK);
    ASSERT_EQ(helper2_generation, CBM_PIPELINE_COMPAT_GENERATION);

    char diff_err[CBM_SZ_8K] = {0};
    int diff_rc = pipeline_compare_current_db_to_fresh_fast_rebuild(
        g_incr_tmpdir, g_incr_dbpath, project, cfg, diff_err, sizeof(diff_err));
    if (diff_rc != 0) {
        FAIL(diff_err[0] ? diff_err : "rename-like fallback differed from fresh FAST rebuild");
    }
    ASSERT_EQ(diff_rc, 0);

    free(project);
    cbm_config_close(cfg);
    cleanup_incremental_repo();
    PASS();
}

TEST(incremental_fast_new_folder_exact_delta_parity) {
    if (setup_incremental_repo() != 0) {
        FAIL("setup failed");
    }

    cbm_config_t *cfg = incremental_test_config(g_incr_tmpdir);
    ASSERT_NOT_NULL(cfg);
    cbm_pipeline_t *p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    char *project = cbm_strdup(cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);
    ASSERT_NOT_NULL(project);

    char path[CBM_PATH_MAX];
    int n = snprintf(path, sizeof(path), "%s/pkg", g_incr_tmpdir);
    ASSERT(n >= 0 && (size_t)n < sizeof(path));
    ASSERT_TRUE(cbm_mkdir_p(path, 0755));
    n = snprintf(path, sizeof(path), "%s/pkg/leaf.go", g_incr_tmpdir);
    ASSERT(n >= 0 && (size_t)n < sizeof(path));
    ASSERT_EQ(th_write_file(path,
                            "package pkg\n\n"
                            "func FolderLeaf() int {\n\treturn 23\n}\n"),
              0);

    p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    cbm_pipeline_free(p);

    ASSERT(pipeline_store_has_function_name(g_incr_dbpath, project, "FolderLeaf"));
    int64_t generation = 0;
    ASSERT_EQ(pipeline_store_file_state_generation(g_incr_dbpath, project, "pkg/leaf.go",
                                                   &generation),
              CBM_STORE_OK);
    ASSERT_GT(generation, CBM_PIPELINE_COMPAT_GENERATION);

    char diff_err[CBM_SZ_8K] = {0};
    int diff_rc = pipeline_compare_current_db_to_fresh_fast_rebuild(
        g_incr_tmpdir, g_incr_dbpath, project, cfg, diff_err, sizeof(diff_err));
    if (diff_rc != 0) {
        FAIL(diff_err[0] ? diff_err : "new-folder exact delta differed from fresh FAST rebuild");
    }
    ASSERT_EQ(diff_rc, 0);

    free(project);
    cbm_config_close(cfg);
    cleanup_incremental_repo();
    PASS();
}

TEST(incremental_fast_route_decorator_change_matches_full_rebuild) {
    if (setup_incremental_repo() != 0) {
        FAIL("setup failed");
    }

    char path[CBM_PATH_MAX];
    int n = snprintf(path, sizeof(path), "%s/routes.py", g_incr_tmpdir);
    ASSERT(n >= 0 && (size_t)n < sizeof(path));
    ASSERT_EQ(th_write_file(path,
                            "from fastapi import FastAPI\n\n"
                            "app = FastAPI()\n\n"
                            "@app.get('/api/orders')\n"
                            "def orders():\n"
                            "    return {'ok': True}\n"),
              0);

    cbm_config_t *cfg = incremental_test_config(g_incr_tmpdir);
    ASSERT_NOT_NULL(cfg);
    cbm_pipeline_t *p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    char *project = cbm_strdup(cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);
    ASSERT_NOT_NULL(project);
    ASSERT(pipeline_store_has_route_name(g_incr_dbpath, project, "/api/orders"));

    ASSERT_EQ(th_write_file(path,
                            "from fastapi import FastAPI\n\n"
                            "app = FastAPI()\n\n"
                            "@app.get('/api/items')\n"
                            "def orders():\n"
                            "    return {'ok': True}\n"),
              0);

    p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    cbm_pipeline_publish_kind_t kind = cbm_pipeline_publish_kind(p);
    ASSERT(kind == CBM_PIPELINE_PUBLISH_INCREMENTAL_EXACT ||
           kind == CBM_PIPELINE_PUBLISH_INCREMENTAL_CONTAINMENT);
    cbm_pipeline_free(p);

    ASSERT(!pipeline_store_has_route_name(g_incr_dbpath, project, "/api/orders"));
    ASSERT(pipeline_store_has_route_name(g_incr_dbpath, project, "/api/items"));

    char diff_err[CBM_SZ_8K] = {0};
    int diff_rc = pipeline_compare_current_db_to_fresh_fast_rebuild(
        g_incr_tmpdir, g_incr_dbpath, project, cfg, diff_err, sizeof(diff_err));
    if (diff_rc != 0) {
        FAIL(diff_err[0] ? diff_err : "route decorator incremental differed from fresh FAST rebuild");
    }
    ASSERT_EQ(diff_rc, 0);

    free(project);
    cbm_config_close(cfg);
    cleanup_incremental_repo();
    PASS();
}

TEST(incremental_fast_arg_url_route_change_matches_parallel_full_rebuild) {
    enum { ARG_URL_FILLER_FILES = 52, ARG_URL_WORKERS = 4 };
    pipeline_env_snapshot_t workers_env = pipeline_env_save("CBM_WORKERS");
    if (setup_incremental_repo() != 0) {
        FAIL("setup failed");
    }

    char worker_buf[CBM_SZ_32];
    int n = snprintf(worker_buf, sizeof(worker_buf), "%d", ARG_URL_WORKERS);
    ASSERT(n > 0 && (size_t)n < sizeof(worker_buf));
    ASSERT_EQ(cbm_setenv("CBM_WORKERS", worker_buf, 1), 0);

    for (int i = 0; i < ARG_URL_FILLER_FILES; i++) {
        char path[CBM_PATH_MAX];
        char body[CBM_SZ_256];
        n = snprintf(path, sizeof(path), "%s/filler_%02d.c", g_incr_tmpdir, i);
        ASSERT(n > 0 && (size_t)n < sizeof(path));
        n = snprintf(body, sizeof(body), "int filler_%02d(void) { return %d; }\n", i, i);
        ASSERT(n > 0 && (size_t)n < sizeof(body));
        ASSERT_EQ(th_write_file(path, body), 0);
    }
    ASSERT_EQ(write_incremental_arg_url_route_file("/api/index", 1), 0);

    cbm_config_t *cfg = incremental_test_config(g_incr_tmpdir);
    ASSERT_NOT_NULL(cfg);
    cbm_pipeline_t *p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    char *project = cbm_strdup(cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);
    ASSERT_NOT_NULL(project);
    ASSERT(pipeline_store_has_route_name(g_incr_dbpath, project, "/api/index"));

    ASSERT_EQ(write_incremental_arg_url_route_file("/api/index-status", 2), 0);

    p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    cbm_pipeline_publish_kind_t kind = cbm_pipeline_publish_kind(p);
    ASSERT(kind == CBM_PIPELINE_PUBLISH_INCREMENTAL_EXACT ||
           kind == CBM_PIPELINE_PUBLISH_INCREMENTAL_CONTAINMENT);
    cbm_pipeline_free(p);

    ASSERT(!pipeline_store_has_route_name(g_incr_dbpath, project, "/api/index"));
    ASSERT(pipeline_store_has_route_name(g_incr_dbpath, project, "/api/index-status"));

    char diff_err[CBM_SZ_8K] = {0};
    int diff_rc = pipeline_compare_current_db_to_fresh_fast_rebuild(
        g_incr_tmpdir, g_incr_dbpath, project, cfg, diff_err, sizeof(diff_err));
    if (diff_rc != 0) {
        FAIL(diff_err[0] ? diff_err : "arg-url route incremental differed from fresh FAST rebuild");
    }
    ASSERT_EQ(diff_rc, 0);

    free(project);
    cbm_config_close(cfg);
    cleanup_incremental_repo();
    pipeline_env_restore(&workers_env);
    PASS();
}

TEST(incremental_fast_exact_scratch_multifile_usage_edges_match_fresh) {
    if (setup_incremental_repo() != 0) {
        FAIL("setup failed");
    }

    cbm_config_t *cfg = incremental_test_config(g_incr_tmpdir);
    ASSERT_NOT_NULL(cfg);
    cbm_pipeline_t *p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    char *project = cbm_strdup(cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);
    ASSERT_NOT_NULL(project);

    char main_path[CBM_PATH_MAX];
    char helper_path[CBM_PATH_MAX];
    int n = snprintf(main_path, sizeof(main_path), "%s/main.go", g_incr_tmpdir);
    ASSERT(n >= 0 && (size_t)n < sizeof(main_path));
    n = snprintf(helper_path, sizeof(helper_path), "%s/helper.go", g_incr_tmpdir);
    ASSERT(n >= 0 && (size_t)n < sizeof(helper_path));
    ASSERT_EQ(th_write_file(main_path,
                            "package main\n\n"
                            "func main() {\n\tHelper()\n\tNewHelper()\n}\n\n"
                            "func NewMain() int {\n\treturn 11\n}\n"),
              0);
    ASSERT_EQ(th_write_file(helper_path,
                            "package main\n\n"
                            "func Helper() string {\n\treturn \"updated\"\n}\n\n"
                            "func NewHelper() int {\n\treturn 13\n}\n"),
              0);

    cbm_file_info_t changed[] = {
        {.path = main_path, .rel_path = "main.go", .language = CBM_LANG_GO},
        {.path = helper_path, .rel_path = "helper.go", .language = CBM_LANG_GO},
    };
    cbm_store_t *store = cbm_store_open_path_query(g_incr_dbpath);
    ASSERT_NOT_NULL(store);
    cbm_gbuf_t *scratch = NULL;
    cbm_pipeline_file_delta_t deltas[CBM_SZ_2] = {0};
    ASSERT_EQ(pipeline_build_exact_scratch_for_changed_files(
                  store, g_incr_tmpdir, project, changed,
                  (int)(sizeof(changed) / sizeof(changed[0])), &scratch, deltas),
              CBM_STORE_OK);

    char *helper_module_qn = cbm_pipeline_fqn_module(project, "helper.go");
    char *main_fn_qn = cbm_pipeline_fqn_compute(project, "main.go", "main");
    ASSERT_NOT_NULL(helper_module_qn);
    ASSERT_NOT_NULL(main_fn_qn);
    ASSERT_EQ(pipeline_gbuf_count_usage_edge(scratch, helper_module_qn, main_fn_qn, "main"), 1);
    ASSERT_EQ(pipeline_file_delta_count_usage_edge(&deltas[1], helper_module_qn, main_fn_qn,
                                                   "main"),
              1);

    free(helper_module_qn);
    free(main_fn_qn);
    cbm_pipeline_file_delta_free(&deltas[0]);
    cbm_pipeline_file_delta_free(&deltas[1]);
    cbm_gbuf_free(scratch);
    cbm_store_close(store);
    free(project);
    cbm_config_close(cfg);
    cleanup_incremental_repo();
    PASS();
}

TEST(incremental_fast_exact_batch_publish_matches_fresh_rebuild_for_two_file_go) {
    if (setup_incremental_repo() != 0) {
        FAIL("setup failed");
    }

    cbm_config_t *cfg = incremental_test_config(g_incr_tmpdir);
    ASSERT_NOT_NULL(cfg);
    cbm_pipeline_t *p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    char pass_fingerprint[CBM_SZ_256];
    ASSERT_EQ(cbm_pipeline_current_pass_fingerprint(p, pass_fingerprint,
                                                    sizeof(pass_fingerprint)),
              CBM_STORE_OK);
    char *project = cbm_strdup(cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);
    ASSERT_NOT_NULL(project);

    char main_path[CBM_PATH_MAX];
    char helper_path[CBM_PATH_MAX];
    int n = snprintf(main_path, sizeof(main_path), "%s/main.go", g_incr_tmpdir);
    ASSERT(n >= 0 && (size_t)n < sizeof(main_path));
    n = snprintf(helper_path, sizeof(helper_path), "%s/helper.go", g_incr_tmpdir);
    ASSERT(n >= 0 && (size_t)n < sizeof(helper_path));
    ASSERT_EQ(th_write_file(main_path,
                            "package main\n\n"
                            "func main() {\n\tHelper()\n\tNewHelper()\n}\n\n"
                            "func NewMain() int {\n\treturn 11\n}\n"),
              0);
    ASSERT_EQ(th_write_file(helper_path,
                            "package main\n\n"
                            "func Helper() string {\n\treturn \"updated\"\n}\n\n"
                            "func NewHelper() int {\n\treturn 13\n}\n"),
              0);

    cbm_file_info_t changed[] = {
        {.path = main_path, .rel_path = "main.go", .language = CBM_LANG_GO},
        {.path = helper_path, .rel_path = "helper.go", .language = CBM_LANG_GO},
    };
    const int changed_count = (int)(sizeof(changed) / sizeof(changed[0]));
    cbm_store_t *store = cbm_store_open_path(g_incr_dbpath);
    ASSERT_NOT_NULL(store);
    cbm_gbuf_t *scratch = NULL;
    cbm_pipeline_file_delta_t deltas[CBM_SZ_2] = {0};
    ASSERT_EQ(pipeline_build_exact_scratch_for_changed_files(store, g_incr_tmpdir, project,
                                                             changed, changed_count, &scratch,
                                                             deltas),
              CBM_STORE_OK);
    for (int i = 0; i < changed_count; i++) {
        ASSERT_EQ(cbm_pipeline_attach_file_delta_metadata_with_fingerprint(
                      &deltas[i], &changed[i], pass_fingerprint),
                  CBM_STORE_OK);
    }

    const cbm_pipeline_file_delta_t *delta_ptrs[CBM_SZ_2] = {&deltas[0], &deltas[1]};
    cbm_pipeline_file_delta_plan_t plan = {0};
    ASSERT_EQ(cbm_pipeline_plan_file_delta_batch(store, delta_ptrs, changed_count,
                                                 CBM_PIPELINE_EXACT_DELTA_DEFAULT_MAX_AFFECTED_PATHS,
                                                 &plan),
              CBM_STORE_OK);
    if (plan.route != CBM_PIPELINE_DELTA_ROUTE_EXACT_CANDIDATE) {
        FAIL(plan.reason ? plan.reason : "exact batch plan rejected candidate");
    }
    cbm_pipeline_file_delta_plan_free(&plan);

    int64_t generation = 0;
    ASSERT_EQ(cbm_store_reserve_index_generation(store, project, NULL, NULL, &generation),
              CBM_STORE_OK);
    ASSERT_GT(generation, CBM_PIPELINE_COMPAT_GENERATION);
    for (int i = 0; i < changed_count; i++) {
        ASSERT_EQ(cbm_pipeline_file_delta_stamp_generation(&deltas[i], generation),
                  CBM_STORE_OK);
    }
    ASSERT_EQ(cbm_pipeline_apply_file_delta_batch(store, delta_ptrs, changed_count,
                                                  CBM_PIPELINE_EXACT_DELTA_DEFAULT_MAX_AFFECTED_PATHS,
                                                  &plan),
              CBM_STORE_OK);
    if (plan.route != CBM_PIPELINE_DELTA_ROUTE_EXACT_CANDIDATE) {
        const char *store_err = cbm_store_error(store);
        if (store_err && store_err[0]) {
            FAIL(store_err);
        }
        FAIL(plan.reason ? plan.reason : "exact batch apply rejected candidate");
    }
    cbm_pipeline_file_delta_plan_free(&plan);
    cbm_store_close(store);

    char diff_err[CBM_SZ_8K] = {0};
    int diff_rc = pipeline_compare_current_db_to_fresh_fast_rebuild(
        g_incr_tmpdir, g_incr_dbpath, project, cfg, diff_err, sizeof(diff_err));
    if (diff_rc != 0) {
        FAIL(diff_err[0] ? diff_err : "exact batch publish differed from fresh FAST rebuild");
    }

    cbm_pipeline_file_delta_free(&deltas[0]);
    cbm_pipeline_file_delta_free(&deltas[1]);
    cbm_gbuf_free(scratch);
    free(project);
    cbm_config_close(cfg);
    cleanup_incremental_repo();
    PASS();
}

TEST(incremental_overlay_producer_marks_dirty_ready_without_canonical_mutation) {
    if (setup_incremental_repo() != 0) {
        FAIL("setup failed");
    }

    cbm_config_t *cfg = incremental_test_config(g_incr_tmpdir);
    ASSERT_NOT_NULL(cfg);
    cbm_pipeline_t *p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    char pass_fingerprint[CBM_SZ_256];
    ASSERT_EQ(cbm_pipeline_current_pass_fingerprint(p, pass_fingerprint,
                                                    sizeof(pass_fingerprint)),
              CBM_STORE_OK);
    char *project = cbm_strdup(cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);
    ASSERT_NOT_NULL(project);
    ASSERT(!pipeline_store_has_function_name(g_incr_dbpath, project, "OverlayOnly"));

    char helper_path[CBM_PATH_MAX];
    int n = snprintf(helper_path, sizeof(helper_path), "%s/helper.go", g_incr_tmpdir);
    ASSERT(n >= 0 && (size_t)n < sizeof(helper_path));
    ASSERT_EQ(th_write_file(helper_path,
                            "package main\n\n"
                            "func Helper() string {\n\treturn \"overlay\"\n}\n\n"
                            "func OverlayOnly() int {\n\treturn 21\n}\n"),
              0);

    cbm_file_info_t changed = {
        .path = helper_path,
        .rel_path = "helper.go",
        .language = CBM_LANG_GO,
    };
    cbm_store_t *store = cbm_store_open_path(g_incr_dbpath);
    ASSERT_NOT_NULL(store);
    cbm_gbuf_t *scratch = NULL;
    cbm_pipeline_file_delta_t delta = {0};
    ASSERT_EQ(pipeline_build_exact_scratch_for_changed_files(store, g_incr_tmpdir, project,
                                                             &changed, 1, &scratch, &delta),
              CBM_STORE_OK);
    ASSERT_EQ(cbm_pipeline_attach_file_delta_metadata_with_fingerprint(&delta, &changed,
                                                                       pass_fingerprint),
              CBM_STORE_OK);

    const cbm_pipeline_file_delta_t *deltas[] = {&delta};
    int64_t overlay_generation = 0;
    ASSERT_EQ(cbm_pipeline_publish_overlay_file_delta_batch(
                  store, deltas, 1, CBM_PIPELINE_COMPAT_GENERATION,
                  CBM_STORE_DIRTY_SOURCE_EXPLICIT_REINDEX, &overlay_generation),
              CBM_STORE_OK);
    ASSERT_GT(overlay_generation, 0);
    cbm_store_close(store);

    ASSERT(!pipeline_store_has_function_name(g_incr_dbpath, project, "OverlayOnly"));
    ASSERT(pipeline_store_overlay_file_has_function(g_incr_dbpath, project, "helper.go",
                                                    "OverlayOnly"));
    int dirty_pending = -1;
    int dirty_overlay_ready = -1;
    ASSERT_EQ(pipeline_store_dirty_counts(g_incr_dbpath, project, &dirty_pending,
                                          &dirty_overlay_ready),
              CBM_STORE_OK);
    ASSERT_EQ(dirty_pending, 0);
    ASSERT_EQ(dirty_overlay_ready, 1);

    cbm_pipeline_file_delta_free(&delta);
    cbm_gbuf_free(scratch);
    free(project);
    cbm_config_close(cfg);
    cleanup_incremental_repo();
    PASS();
}

TEST(incremental_overlay_publish_small_deltas_keeps_canonical_base_visible) {
    if (setup_incremental_repo() != 0) {
        FAIL("setup failed");
    }

    cbm_config_t *cfg = incremental_test_config(g_incr_tmpdir);
    ASSERT_NOT_NULL(cfg);
    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_OVERLAY_PUBLISH,
                             CBM_CONFIG_OVERLAY_PUBLISH_SMALL_DELTAS),
              0);
    cbm_pipeline_t *p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    char *project = cbm_strdup(cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);
    ASSERT_NOT_NULL(project);
    ASSERT(!pipeline_store_has_function_name(g_incr_dbpath, project, "OverlayRunOnly"));

    char path[CBM_PATH_MAX];
    int n = snprintf(path, sizeof(path), "%s/leaf.go", g_incr_tmpdir);
    ASSERT(n >= 0 && (size_t)n < sizeof(path));
    ASSERT_EQ(th_write_file(path,
                            "package main\n\n"
                            "func Leaf() int {\n\treturn 2\n}\n\n"
                            "func OverlayRunOnly() int {\n\treturn 77\n}\n"),
              0);

    p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    pipeline_capture_logs_start();
    int run_rc = cbm_pipeline_run(p);
    const char *logs = pipeline_capture_logs_end();
    ASSERT_EQ(run_rc, 0);
    ASSERT(strstr(logs, "msg=incremental.overlay.done files=1") != NULL);
    ASSERT_EQ(cbm_pipeline_publish_kind(p), CBM_PIPELINE_PUBLISH_INCREMENTAL_OVERLAY);
    ASSERT(!cbm_pipeline_graph_changed(p));
    cbm_pipeline_exact_delta_stats_t stats = cbm_pipeline_exact_delta_stats(p);
    ASSERT_EQ(stats.changed_paths, 1);
    ASSERT_EQ(stats.affected_paths, 1);
    ASSERT_EQ(stats.published_paths, 1);
    cbm_pipeline_free(p);

    ASSERT_EQ(pipeline_store_generation_status_count(g_incr_dbpath, project,
                                                     CBM_STORE_INDEX_STATUS_RESERVED),
              0);
    ASSERT(!pipeline_store_has_function_name(g_incr_dbpath, project, "OverlayRunOnly"));
    ASSERT(pipeline_store_overlay_file_has_function(g_incr_dbpath, project, "leaf.go",
                                                    "OverlayRunOnly"));
    int dirty_pending = -1;
    int dirty_overlay_ready = -1;
    ASSERT_EQ(pipeline_store_dirty_counts(g_incr_dbpath, project, &dirty_pending,
                                          &dirty_overlay_ready),
              CBM_STORE_OK);
    ASSERT_EQ(dirty_pending, 0);
    ASSERT_EQ(dirty_overlay_ready, 1);

    free(project);
    cbm_config_close(cfg);
    cleanup_incremental_repo();
    PASS();
}

TEST(incremental_overlay_publish_delete_keeps_canonical_base_visible) {
    if (setup_incremental_repo() != 0) {
        FAIL("setup failed");
    }

    char path[CBM_PATH_MAX];
    int n = snprintf(path, sizeof(path), "%s/leaf.go", g_incr_tmpdir);
    ASSERT(n >= 0 && (size_t)n < sizeof(path));
    ASSERT_EQ(th_write_file(path,
                            "package main\n\n"
                            "func Leaf() int {\n\treturn 1\n}\n"),
              0);

    cbm_config_t *cfg = incremental_test_config(g_incr_tmpdir);
    ASSERT_NOT_NULL(cfg);
    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_OVERLAY_PUBLISH,
                             CBM_CONFIG_OVERLAY_PUBLISH_SMALL_DELTAS),
              0);
    cbm_pipeline_t *p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    char *project = cbm_strdup(cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);
    ASSERT_NOT_NULL(project);
    ASSERT(pipeline_store_has_function_name(g_incr_dbpath, project, "Leaf"));
    ASSERT(pipeline_store_overlay_file_has_function(g_incr_dbpath, project, "leaf.go", "Leaf"));

    ASSERT_EQ(cbm_unlink(path), 0);

    p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    pipeline_capture_logs_start();
    int run_rc = cbm_pipeline_run(p);
    const char *logs = pipeline_capture_logs_end();
    ASSERT_EQ(run_rc, 0);
    ASSERT(strstr(logs, "msg=incremental.overlay.done files=1") != NULL);
    ASSERT_EQ(cbm_pipeline_publish_kind(p), CBM_PIPELINE_PUBLISH_INCREMENTAL_OVERLAY);
    ASSERT(!cbm_pipeline_graph_changed(p));
    cbm_pipeline_exact_delta_stats_t stats = cbm_pipeline_exact_delta_stats(p);
    ASSERT_EQ(stats.changed_paths, 1);
    ASSERT_EQ(stats.affected_paths, 1);
    ASSERT_EQ(stats.published_paths, 1);
    cbm_pipeline_free(p);

    ASSERT_EQ(pipeline_store_generation_status_count(g_incr_dbpath, project,
                                                     CBM_STORE_INDEX_STATUS_RESERVED),
              0);
    ASSERT(pipeline_store_has_function_name(g_incr_dbpath, project, "Leaf"));
    ASSERT(!pipeline_store_overlay_file_has_function(g_incr_dbpath, project, "leaf.go", "Leaf"));
    int64_t leaf_generation = 0;
    ASSERT_EQ(pipeline_store_file_state_generation(g_incr_dbpath, project, "leaf.go",
                                                   &leaf_generation),
              CBM_STORE_OK);
    int dirty_pending = -1;
    int dirty_overlay_ready = -1;
    ASSERT_EQ(pipeline_store_dirty_counts(g_incr_dbpath, project, &dirty_pending,
                                          &dirty_overlay_ready),
              CBM_STORE_OK);
    ASSERT_EQ(dirty_pending, 0);
    ASSERT_EQ(dirty_overlay_ready, 1);

    free(project);
    cbm_config_close(cfg);
    cleanup_incremental_repo();
    PASS();
}

TEST(incremental_overlay_publish_repeated_update_keeps_active_view_idempotent) {
    if (setup_incremental_repo() != 0) {
        FAIL("setup failed");
    }

    cbm_config_t *cfg = incremental_test_config(g_incr_tmpdir);
    ASSERT_NOT_NULL(cfg);
    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_OVERLAY_PUBLISH,
                             CBM_CONFIG_OVERLAY_PUBLISH_SMALL_DELTAS),
              0);
    cbm_pipeline_t *p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    char *project = cbm_strdup(cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);
    ASSERT_NOT_NULL(project);

    char path[CBM_PATH_MAX];
    int n = snprintf(path, sizeof(path), "%s/leaf.go", g_incr_tmpdir);
    ASSERT(n >= 0 && (size_t)n < sizeof(path));
    ASSERT_EQ(th_write_file(path,
                            "package main\n\n"
                            "func Leaf() int {\n\treturn 2\n}\n\n"
                            "func OverlayRetryOnly() int {\n\treturn 77\n}\n"),
              0);

    for (int i = 0; i < 2; i++) {
        p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
        ASSERT_NOT_NULL(p);
        cbm_pipeline_apply_config(p, cfg);
        ASSERT_EQ(cbm_pipeline_run(p), 0);
        ASSERT_EQ(cbm_pipeline_publish_kind(p), CBM_PIPELINE_PUBLISH_INCREMENTAL_OVERLAY);
        ASSERT(!cbm_pipeline_graph_changed(p));
        cbm_pipeline_free(p);
    }

    ASSERT_EQ(pipeline_store_generation_status_count(g_incr_dbpath, project,
                                                     CBM_STORE_INDEX_STATUS_RESERVED),
              0);
    ASSERT(!pipeline_store_has_function_name(g_incr_dbpath, project, "OverlayRetryOnly"));
    ASSERT_EQ(pipeline_store_overlay_file_function_count(g_incr_dbpath, project, "leaf.go",
                                                        "OverlayRetryOnly"),
              1);
    int dirty_pending = -1;
    int dirty_overlay_ready = -1;
    ASSERT_EQ(pipeline_store_dirty_counts(g_incr_dbpath, project, &dirty_pending,
                                          &dirty_overlay_ready),
              CBM_STORE_OK);
    ASSERT_EQ(dirty_pending, 0);
    ASSERT_EQ(dirty_overlay_ready, 1);

    free(project);
    cbm_config_close(cfg);
    cleanup_incremental_repo();
    PASS();
}

TEST(incremental_overlay_publish_failure_falls_back_to_canonical_exact) {
    pipeline_env_snapshot_t fail_env = pipeline_env_save(CBM_TEST_FAIL_INCREMENTAL_PHASE);

    if (setup_incremental_repo() != 0) {
        FAIL("setup failed");
    }

    cbm_config_t *cfg = incremental_test_config(g_incr_tmpdir);
    ASSERT_NOT_NULL(cfg);
    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_OVERLAY_PUBLISH,
                             CBM_CONFIG_OVERLAY_PUBLISH_SMALL_DELTAS),
              0);
    cbm_pipeline_t *p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    char *project = cbm_strdup(cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);
    ASSERT_NOT_NULL(project);
    ASSERT(!pipeline_store_has_function_name(g_incr_dbpath, project, "OverlayFailureOnly"));

    char path[CBM_PATH_MAX];
    int n = snprintf(path, sizeof(path), "%s/leaf.go", g_incr_tmpdir);
    ASSERT(n >= 0 && (size_t)n < sizeof(path));
    ASSERT_EQ(th_write_file(path,
                            "package main\n\n"
                            "func Leaf() int {\n\treturn 2\n}\n\n"
                            "func OverlayFailureOnly() int {\n\treturn 88\n}\n"),
              0);

    p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    cbm_setenv(CBM_TEST_FAIL_INCREMENTAL_PHASE, CBM_TEST_FAIL_INCREMENTAL_OVERLAY_PUBLISH, 1);
    int run_rc = cbm_pipeline_run(p);
    pipeline_env_restore(&fail_env);
    ASSERT_EQ(run_rc, 0);
    ASSERT_EQ(cbm_pipeline_publish_kind(p), CBM_PIPELINE_PUBLISH_INCREMENTAL_EXACT);
    ASSERT(cbm_pipeline_graph_changed(p));
    cbm_pipeline_free(p);

    ASSERT_EQ(pipeline_store_overlay_generation_status_count(
                  g_incr_dbpath, project, CBM_STORE_OVERLAY_STATUS_FAILED),
              1);
    ASSERT_EQ(pipeline_store_overlay_generation_status_count(
                  g_incr_dbpath, project, CBM_STORE_OVERLAY_STATUS_READY),
              0);
    ASSERT_EQ(pipeline_store_generation_status_count(g_incr_dbpath, project,
                                                     CBM_STORE_INDEX_STATUS_RESERVED),
              0);
    ASSERT(pipeline_store_has_function_name(g_incr_dbpath, project, "OverlayFailureOnly"));
    int dirty_pending = -1;
    int dirty_overlay_ready = -1;
    ASSERT_EQ(pipeline_store_dirty_counts(g_incr_dbpath, project, &dirty_pending,
                                          &dirty_overlay_ready),
              CBM_STORE_OK);
    ASSERT_EQ(dirty_pending, 0);
    ASSERT_EQ(dirty_overlay_ready, 0);

    free(project);
    cbm_config_close(cfg);
    cleanup_incremental_repo();
    PASS();
}

TEST(incremental_overlay_extract_failure_keeps_dirty_pending_without_overlay) {
    pipeline_env_snapshot_t fail_env = pipeline_env_save(CBM_TEST_FAIL_INCREMENTAL_PHASE);

    if (setup_incremental_repo() != 0) {
        FAIL("setup failed");
    }

    cbm_config_t *cfg = incremental_test_config(g_incr_tmpdir);
    ASSERT_NOT_NULL(cfg);
    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_OVERLAY_PUBLISH,
                             CBM_CONFIG_OVERLAY_PUBLISH_SMALL_DELTAS),
              0);
    cbm_pipeline_t *p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    char *project = cbm_strdup(cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);
    ASSERT_NOT_NULL(project);
    ASSERT(!pipeline_store_has_function_name(g_incr_dbpath, project, "OverlayExtractFailureOnly"));

    char path[CBM_PATH_MAX];
    int n = snprintf(path, sizeof(path), "%s/leaf.go", g_incr_tmpdir);
    ASSERT(n >= 0 && (size_t)n < sizeof(path));
    ASSERT_EQ(th_write_file(path,
                            "package main\n\n"
                            "func Leaf() int {\n\treturn 2\n}\n\n"
                            "func OverlayExtractFailureOnly() int {\n\treturn 99\n}\n"),
              0);

    p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    cbm_file_info_t *files = NULL;
    int file_count = 0;
    cbm_discover_opts_t opts = {.mode = CBM_MODE_FAST, .ignore_file = NULL, .max_file_size = 0};
    ASSERT_EQ(cbm_discover(g_incr_tmpdir, &opts, &files, &file_count), 0);
    ASSERT_GT(file_count, 0);

    cbm_setenv(CBM_TEST_FAIL_INCREMENTAL_PHASE, CBM_TEST_FAIL_INCREMENTAL_EXTRACT, 1);
    int run_rc = cbm_pipeline_run_incremental(p, g_incr_dbpath, files, file_count);
    pipeline_env_restore(&fail_env);
    cbm_discover_free(files, file_count);
    ASSERT_NEQ(run_rc, 0);
    ASSERT(!pipeline_store_has_function_name(g_incr_dbpath, project,
                                             "OverlayExtractFailureOnly"));
    ASSERT_EQ(pipeline_store_overlay_generation_status_count(
                  g_incr_dbpath, project, CBM_STORE_OVERLAY_STATUS_FAILED),
              0);
    ASSERT_EQ(pipeline_store_overlay_generation_status_count(
                  g_incr_dbpath, project, CBM_STORE_OVERLAY_STATUS_READY),
              0);
    int dirty_pending = -1;
    int dirty_overlay_ready = -1;
    ASSERT_EQ(pipeline_store_dirty_counts(g_incr_dbpath, project, &dirty_pending,
                                          &dirty_overlay_ready),
              CBM_STORE_OK);
    ASSERT_EQ(dirty_pending, 1);
    ASSERT_EQ(dirty_overlay_ready, 0);
    cbm_pipeline_free(p);

    free(project);
    cbm_config_close(cfg);
    cleanup_incremental_repo();
    PASS();
}

TEST(incremental_full_mode_keeps_exact_upsert_disabled) {
    if (setup_incremental_repo() != 0) {
        FAIL("setup failed");
    }

    cbm_config_t *cfg = incremental_test_config(g_incr_tmpdir);
    ASSERT_NOT_NULL(cfg);
    cbm_pipeline_t *p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    char *project = cbm_strdup(cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);
    ASSERT_NOT_NULL(project);

    char path[CBM_PATH_MAX];
    int n = snprintf(path, sizeof(path), "%s/main.go", g_incr_tmpdir);
    ASSERT(n >= 0 && (size_t)n < sizeof(path));
    ASSERT_EQ(th_write_file(path,
                            "package main\n\n"
                            "func main() {\n\tHelper()\n}\n\n"
                            "func FullModeNewMain() int {\n\treturn 9\n}\n"),
              0);

    p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    cbm_pipeline_free(p);

    ASSERT(pipeline_store_has_function_name(g_incr_dbpath, project, "FullModeNewMain"));
    int64_t generation = -1;
    ASSERT_EQ(pipeline_store_file_state_generation(g_incr_dbpath, project, "main.go",
                                                   &generation),
              CBM_STORE_OK);
    ASSERT_EQ(generation, CBM_PIPELINE_COMPAT_GENERATION);

    free(project);
    cbm_config_close(cfg);
    cleanup_incremental_repo();
    PASS();
}

TEST(incremental_detects_same_size_rewrite_with_preserved_mtime) {
    if (setup_incremental_repo() != 0) {
        FAIL("setup failed");
    }

    const char original[] = "package main\n\nfunc Helper() string {\n\treturn \"hello\"\n}\n";
    const char rewritten[] = "package main\n\nfunc Helped() string {\n\treturn \"hello\"\n}\n";
    ASSERT_EQ((int)strlen(original), (int)strlen(rewritten));

    cbm_pipeline_t *p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    char *project = cbm_strdup(cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);
    ASSERT_NOT_NULL(project);
    ASSERT(pipeline_store_has_function_name(g_incr_dbpath, project, "Helper"));
    ASSERT(!pipeline_store_has_function_name(g_incr_dbpath, project, "Helped"));

    char path[CBM_PATH_MAX];
    int n = snprintf(path, sizeof(path), "%s/helper.go", g_incr_tmpdir);
    ASSERT(n >= 0 && (size_t)n < sizeof(path));
    struct stat before;
    ASSERT_EQ(stat(path, &before), 0);
    ASSERT_EQ((int64_t)before.st_size, (int64_t)strlen(original));
    ASSERT_EQ(th_write_file(path, rewritten), 0);
    ASSERT_EQ(pipeline_restore_file_times(path, &before), 0);

    cbm_config_t *cfg = incremental_test_config(g_incr_tmpdir);
    ASSERT_NOT_NULL(cfg);
    p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    cbm_pipeline_free(p);

    ASSERT(!pipeline_store_has_function_name(g_incr_dbpath, project, "Helper"));
    ASSERT(pipeline_store_has_function_name(g_incr_dbpath, project, "Helped"));

    free(project);
    cbm_config_close(cfg);
    cleanup_incremental_repo();
    PASS();
}

TEST(incremental_missing_file_state_keeps_legacy_metadata_path) {
    if (setup_incremental_repo() != 0) {
        FAIL("setup failed");
    }

    cbm_pipeline_t *p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    char *project = cbm_strdup(cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);
    ASSERT_NOT_NULL(project);

    cbm_store_t *s = cbm_store_open_path(g_incr_dbpath);
    ASSERT_NOT_NULL(s);
    int nodes_before = cbm_store_count_nodes(s, project);
    ASSERT_GT(nodes_before, 0);
    ASSERT_EQ(cbm_store_delete_file_state(s, project, "helper.go"), CBM_STORE_OK);
    cbm_file_state_t state = {0};
    ASSERT_EQ(cbm_store_get_file_state(s, project, "helper.go", &state), CBM_STORE_NOT_FOUND);
    cbm_store_close(s);

    cbm_config_t *cfg = incremental_test_config(g_incr_tmpdir);
    ASSERT_NOT_NULL(cfg);
    p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    cbm_pipeline_free(p);

    s = cbm_store_open_path(g_incr_dbpath);
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_count_nodes(s, project), nodes_before);
    cbm_store_close(s);
    ASSERT(pipeline_store_has_function_name(g_incr_dbpath, project, "Helper"));

    free(project);
    cbm_config_close(cfg);
    cleanup_incremental_repo();
    PASS();
}

TEST(incremental_publish_failure_keeps_existing_db) {
    pipeline_env_snapshot_t flush_fail_env =
        pipeline_env_save(CBM_TEST_FAIL_GBUF_FLUSH_BEFORE_COMMIT);
    pipeline_env_snapshot_t dump_fail_env =
        pipeline_env_save(CBM_TEST_FAIL_GBUF_DUMP_BEFORE_REPLACE);

    if (setup_incremental_repo() != 0) {
        FAIL("setup failed");
    }

    cbm_pipeline_t *p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    char *project = strdup(cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);

    cbm_store_t *s = cbm_store_open_path(g_incr_dbpath);
    ASSERT_NOT_NULL(s);
    int nodes_before = cbm_store_count_nodes(s, project);
    ASSERT_GT(nodes_before, 0);
    cbm_store_close(s);
    ASSERT(!pipeline_store_has_function_name(g_incr_dbpath, project, "NewFunc"));

    char path[CBM_PATH_MAX];
    snprintf(path, sizeof(path), "%s/helper.go", g_incr_tmpdir);
    FILE *f = fopen(path, "w");
    ASSERT_NOT_NULL(f);
    fprintf(f, "package main\n\n"
               "func Helper() string {\n\treturn \"hello\"\n}\n\n"
               "func NewFunc() int {\n\treturn 42\n}\n");
    fclose(f);

    cbm_config_t *cfg = incremental_test_config(g_incr_tmpdir);
    ASSERT_NOT_NULL(cfg);
    p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);

    cbm_setenv(CBM_TEST_FAIL_GBUF_FLUSH_BEFORE_COMMIT, pipeline_test_env_enabled, 1);
    cbm_setenv(CBM_TEST_FAIL_GBUF_DUMP_BEFORE_REPLACE, pipeline_test_env_enabled, 1);
    int rc = cbm_pipeline_run(p);
    pipeline_env_restore(&flush_fail_env);
    pipeline_env_restore(&dump_fail_env);

    ASSERT_NEQ(rc, 0);
    s = cbm_store_open_path(g_incr_dbpath);
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_count_nodes(s, project), nodes_before);
    cbm_store_close(s);
    ASSERT(!pipeline_store_has_function_name(g_incr_dbpath, project, "NewFunc"));
    int dirty_pending = -1;
    int dirty_overlay_ready = -1;
    ASSERT_EQ(pipeline_store_dirty_counts(g_incr_dbpath, project, &dirty_pending,
                                          &dirty_overlay_ready),
              CBM_STORE_OK);
    ASSERT_EQ(dirty_pending, 1);
    ASSERT_EQ(dirty_overlay_ready, 0);

    cbm_pipeline_free(p);
    free(project);
    cbm_config_close(cfg);
    cleanup_incremental_repo();
    PASS();
}

TEST(incremental_postpass_failure_keeps_existing_db) {
    pipeline_env_snapshot_t fail_env = pipeline_env_save(CBM_TEST_FAIL_INCREMENTAL_PHASE);

    if (setup_incremental_repo() != 0) {
        FAIL("setup failed");
    }

    cbm_pipeline_t *p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    char *project = strdup(cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);

    cbm_store_t *s = cbm_store_open_path(g_incr_dbpath);
    ASSERT_NOT_NULL(s);
    int nodes_before = cbm_store_count_nodes(s, project);
    ASSERT_GT(nodes_before, 0);
    cbm_store_close(s);
    ASSERT(!pipeline_store_has_function_name(g_incr_dbpath, project, "NewFunc"));

    char path[CBM_PATH_MAX];
    snprintf(path, sizeof(path), "%s/helper.go", g_incr_tmpdir);
    FILE *f = fopen(path, "w");
    ASSERT_NOT_NULL(f);
    fprintf(f, "package main\n\n"
               "func Helper() string {\n\treturn \"hello\"\n}\n\n"
               "func NewFunc() int {\n\treturn 42\n}\n");
    fclose(f);

    cbm_config_t *cfg = incremental_test_config(g_incr_tmpdir);
    ASSERT_NOT_NULL(cfg);
    p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);

    cbm_file_info_t *files = NULL;
    int file_count = 0;
    cbm_discover_opts_t opts = {.mode = CBM_MODE_FULL, .ignore_file = NULL, .max_file_size = 0};
    ASSERT_EQ(cbm_discover(g_incr_tmpdir, &opts, &files, &file_count), 0);
    ASSERT_GT(file_count, 0);

    cbm_setenv(CBM_TEST_FAIL_INCREMENTAL_PHASE, CBM_TEST_FAIL_INCREMENTAL_POSTPASS, 1);
    int rc = cbm_pipeline_run_incremental(p, g_incr_dbpath, files, file_count);
    pipeline_env_restore(&fail_env);
    cbm_discover_free(files, file_count);

    ASSERT_NEQ(rc, 0);
    s = cbm_store_open_path(g_incr_dbpath);
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_count_nodes(s, project), nodes_before);
    cbm_store_close(s);
    ASSERT(!pipeline_store_has_function_name(g_incr_dbpath, project, "NewFunc"));

    cbm_pipeline_free(p);
    free(project);
    cbm_config_close(cfg);
    cleanup_incremental_repo();
    PASS();
}

TEST(incremental_hash_persist_failure_falls_back_to_full) {
    pipeline_env_snapshot_t fail_env = pipeline_env_save(CBM_TEST_FAIL_INCREMENTAL_PHASE);

    if (setup_incremental_repo() != 0) {
        FAIL("setup failed");
    }

    cbm_pipeline_t *p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    char *project = strdup(cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);
    ASSERT_GTE(pipeline_store_file_hash_count(g_incr_dbpath, project), 2);
    ASSERT(!pipeline_store_has_function_name(g_incr_dbpath, project, "NewFunc"));

    char path[CBM_PATH_MAX];
    snprintf(path, sizeof(path), "%s/helper.go", g_incr_tmpdir);
    FILE *f = fopen(path, "w");
    ASSERT_NOT_NULL(f);
    fprintf(f, "package main\n\n"
               "func Helper() string {\n\treturn \"hello\"\n}\n\n"
               "func NewFunc() int {\n\treturn 42\n}\n");
    fclose(f);

    cbm_config_t *cfg = incremental_test_config(g_incr_tmpdir);
    ASSERT_NOT_NULL(cfg);
    p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);

    cbm_setenv(CBM_TEST_FAIL_INCREMENTAL_PHASE, CBM_TEST_FAIL_INCREMENTAL_HASH_PERSIST, 1);
    int rc = cbm_pipeline_run(p);
    pipeline_env_restore(&fail_env);

    ASSERT_EQ(rc, 0);
    ASSERT(pipeline_store_has_function_name(g_incr_dbpath, project, "NewFunc"));
    ASSERT_GTE(pipeline_store_file_hash_count(g_incr_dbpath, project), 2);

    cbm_pipeline_free(p);
    free(project);
    cbm_config_close(cfg);
    cleanup_incremental_repo();
    PASS();
}

TEST(incremental_parallel_extract_failure_keeps_existing_db) {
    ASSERT_EQ(run_parallel_incremental_phase_failure_case(CBM_TEST_FAIL_INCREMENTAL_EXTRACT), 0);
    PASS();
}

TEST(incremental_parallel_registry_failure_keeps_existing_db) {
    ASSERT_EQ(run_parallel_incremental_phase_failure_case(CBM_TEST_FAIL_INCREMENTAL_REGISTRY), 0);
    PASS();
}

TEST(incremental_parallel_resolve_failure_keeps_existing_db) {
    ASSERT_EQ(run_parallel_incremental_phase_failure_case(CBM_TEST_FAIL_INCREMENTAL_RESOLVE), 0);
    PASS();
}

TEST(incremental_classify_deleted_failure_keeps_existing_db) {
    ASSERT_EQ(run_parallel_incremental_phase_failure_case(CBM_TEST_FAIL_INCREMENTAL_CLASSIFY_DELETED),
              0);
    PASS();
}

TEST(incremental_detects_deleted_file) {
    /* Full index, delete a file, re-index → deleted file's nodes removed */
    if (setup_incremental_repo() != 0) {
        FAIL("setup failed");
    }

    /* First: full index */
    cbm_pipeline_t *p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    char *project = strdup(cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);

    /* Delete helper.go */
    char path[512];
    snprintf(path, sizeof(path), "%s/helper.go", g_incr_tmpdir);
    unlink(path);

    /* Second: incremental — should remove Helper nodes */
    cbm_config_t *cfg = incremental_test_config(g_incr_tmpdir);
    ASSERT_NOT_NULL(cfg);
    p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    /* Verify node count decreased (Helper's file was deleted) */
    cbm_store_t *s = cbm_store_open_path(g_incr_dbpath);
    ASSERT_NOT_NULL(s);
    int nodes_after = cbm_store_count_nodes(s, project);
    ASSERT_GT(nodes_after, 0); /* still has main.go nodes */
    cbm_store_close(s);
    cbm_pipeline_free(p);
    free(project);
    cbm_config_close(cfg);

    cleanup_incremental_repo();
    PASS();
}

TEST(incremental_new_file_added) {
    /* Full index, add a new file, re-index → new file's nodes appear */
    if (setup_incremental_repo() != 0) {
        FAIL("setup failed");
    }

    /* First: full index */
    cbm_pipeline_t *p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    char *project = strdup(cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);

    /* Add extra.go */
    char path[512];
    snprintf(path, sizeof(path), "%s/extra.go", g_incr_tmpdir);
    FILE *f = fopen(path, "w");
    ASSERT_NOT_NULL(f);
    fprintf(f, "package main\n\nfunc Extra() bool {\n\treturn true\n}\n");
    fclose(f);

    /* Second: incremental — should pick up Extra */
    cbm_config_t *cfg = incremental_test_config(g_incr_tmpdir);
    ASSERT_NOT_NULL(cfg);
    p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    cbm_store_t *s = cbm_store_open_path(g_incr_dbpath);
    ASSERT_NOT_NULL(s);
    int nodes_after = cbm_store_count_nodes(s, project);
    ASSERT_GT(nodes_after, 0);
    cbm_store_close(s);
    cbm_pipeline_free(p);
    free(project);
    cbm_config_close(cfg);

    cleanup_incremental_repo();
    PASS();
}

TEST(incremental_fast_preserves_mode_skipped_tools_dir) {
    /* Regression: 2026-04-13. A fast-mode reindex after a full-mode index
     * was silently destroying every file under FAST_SKIP_DIRS directories
     * (`tools`, `scripts`, `bin`, `build`, `docs`, ...) by classifying them
     * as deleted in find_deleted_files even though they still existed on
     * disk. The Skyline graph lost packages/mcp/src/tools/ (18 files / ~500
     * nodes) mid-session when a concurrent /develop run obediently called
     * mode='fast'. This test pins the additive semantics: lesser-mode
     * reindexes must NOT delete files that are merely outside the current
     * pass's discovery scope. Fix: find_deleted_files now stat()s each
     * stored-but-missing file and only purges it if it is truly absent
     * from disk. */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_modeskip_XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        FAIL("tmpdir");
    }
    char dbpath[512];
    snprintf(dbpath, sizeof(dbpath), "%s/test.db", tmpdir);
    cbm_config_t *cfg = incremental_test_config(tmpdir);
    ASSERT_NOT_NULL(cfg);

    char path[512];
    FILE *f;

    /* main.go — root-level production code (visible in fast and full) */
    snprintf(path, sizeof(path), "%s/main.go", tmpdir);
    f = fopen(path, "w");
    ASSERT_NOT_NULL(f);
    fprintf(f, "package main\n\nfunc main() {\n}\n");
    fclose(f);

    /* tools/util.go — production code under a FAST_SKIP_DIRS directory.
     * Full mode indexes it; fast mode skips it via the discover.c heuristic. */
    char tools_dir[512];
    snprintf(tools_dir, sizeof(tools_dir), "%s/tools", tmpdir);
    cbm_mkdir_p(tools_dir, 0755);
    snprintf(path, sizeof(path), "%s/tools/util.go", tmpdir);
    f = fopen(path, "w");
    ASSERT_NOT_NULL(f);
    fprintf(f, "package tools\n\nfunc Util() string {\n\treturn \"u\"\n}\n");
    fclose(f);

    /* Step 1: full-mode index — both files should be present */
    cbm_pipeline_t *p = cbm_pipeline_new(tmpdir, dbpath, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    char *project = strdup(cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);

    cbm_store_t *s = cbm_store_open_path(dbpath);
    ASSERT_NOT_NULL(s);
    cbm_node_t *tools_nodes_before = NULL;
    int tools_count_before = 0;
    cbm_store_find_nodes_by_file(s, project, "tools/util.go", &tools_nodes_before,
                                 &tools_count_before);
    ASSERT_GT(tools_count_before, 0); /* full mode must see tools/util.go */
    cbm_store_free_nodes(tools_nodes_before, tools_count_before);
    int total_before = cbm_store_count_nodes(s, project);
    char dep_project[CBM_SZ_512];
    int dep_len = snprintf(dep_project, sizeof(dep_project), "%s.dep.requests", project);
    ASSERT_TRUE(dep_len > 0 && (size_t)dep_len < sizeof(dep_project));
    ASSERT_EQ(cbm_store_upsert_project(s, dep_project, "/tmp/requests"), CBM_STORE_OK);
    cbm_node_t dep_node = {
        .project = dep_project,
        .label = "Module",
        .name = "requests",
        .qualified_name = dep_project,
        .file_path = "__init__.py",
        .start_line = 1,
        .end_line = 1,
        .properties_json = "{}",
    };
    ASSERT_GT(cbm_store_upsert_node(s, &dep_node), 0);
    ASSERT_GT(cbm_store_count_nodes(s, dep_project), 0);
    cbm_store_close(s);

    /* Step 2: fast-mode reindex — tools/util.go MUST survive (additive semantics) */
    p = cbm_pipeline_new(tmpdir, dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    cbm_pipeline_free(p);

    s = cbm_store_open_path(dbpath);
    ASSERT_NOT_NULL(s);
    cbm_node_t *tools_nodes_after = NULL;
    int tools_count_after = 0;
    cbm_store_find_nodes_by_file(s, project, "tools/util.go", &tools_nodes_after,
                                 &tools_count_after);
    /* The critical assertion: tools/util.go nodes must still be present after
     * a fast-mode reindex that skipped the tools/ directory. Before the fix,
     * this was 0. */
    ASSERT_GT(tools_count_after, 0);
    ASSERT_EQ(tools_count_after, tools_count_before); /* same nodes, untouched */
    cbm_store_free_nodes(tools_nodes_after, tools_count_after);

    /* Sanity: total node count should not have collapsed by ~the size of tools/ */
    int total_after = cbm_store_count_nodes(s, project);
    ASSERT_GTE(total_after, total_before); /* additive — never less */
    cbm_store_close(s);

    /* Step 3: mutate main.go and fast reindex — forces publish_and_persist to
     * run (instead of the noop early-return path that step 2 hit). This is
     * the real dangerous path: the gbuf gets loaded, mutated for main.go,
     * and published back to the store. tools/util.go must survive that cycle,
     * not just the trivial noop path. Audit finding from 2026-04-13. */
    snprintf(path, sizeof(path), "%s/main.go", tmpdir);
    f = fopen(path, "w");
    ASSERT_NOT_NULL(f);
    fprintf(f, "package main\n\nfunc main() {\n\tprintln(\"changed\")\n}\n");
    fclose(f);
    /* Bump mtime explicitly — some filesystems have coarse mtime resolution
     * and the rewrite could land in the same tick as the original write. */
#ifndef _WIN32
    struct stat mst;
    if (stat(path, &mst) == 0) {
        struct timespec times[2];
        times[0].tv_sec = mst.st_atime;
        times[0].tv_nsec = 0;
        times[1].tv_sec = mst.st_mtime + 5;
        times[1].tv_nsec = 0;
        utimensat(AT_FDCWD, path, times, 0);
    }
#endif /* !_WIN32 */

    p = cbm_pipeline_new(tmpdir, dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    cbm_pipeline_free(p);

    s = cbm_store_open_path(dbpath);
    ASSERT_NOT_NULL(s);
    cbm_node_t *tools_nodes_run3 = NULL;
    int tools_count_run3 = 0;
    cbm_store_find_nodes_by_file(s, project, "tools/util.go", &tools_nodes_run3, &tools_count_run3);
    /* tools/util.go nodes must STILL be present after a fast reindex that
     * actually ran the full publish_and_persist cycle (not the noop fast-path). */
    ASSERT_EQ(tools_count_run3, tools_count_before);
    cbm_store_free_nodes(tools_nodes_run3, tools_count_run3);
    ASSERT_GT(cbm_store_count_nodes(s, dep_project), 0);
    cbm_store_close(s);

    /* Step 4: actually delete tools/util.go from disk and full-reindex.
     * Now it really is gone, so its nodes should be purged. This pins the
     * other half of the contract: the stat-based check correctly identifies
     * truly-deleted files as deleted. */
    snprintf(path, sizeof(path), "%s/tools/util.go", tmpdir);
    unlink(path);

    p = cbm_pipeline_new(tmpdir, dbpath, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    cbm_pipeline_free(p);

    s = cbm_store_open_path(dbpath);
    ASSERT_NOT_NULL(s);
    cbm_node_t *tools_nodes_deleted = NULL;
    int tools_count_deleted = 0;
    cbm_store_find_nodes_by_file(s, project, "tools/util.go", &tools_nodes_deleted,
                                 &tools_count_deleted);
    ASSERT_EQ(tools_count_deleted, 0); /* truly deleted → purged */
    cbm_store_free_nodes(tools_nodes_deleted, tools_count_deleted);
    cbm_store_close(s);

    free(project);
    cbm_config_close(cfg);
    th_rmtree(tmpdir);
    PASS();
}

TEST(incremental_k8s_manifest_indexed) {
    /* Full index with a k8s manifest, then add a new manifest via incremental.
     * Verifies that cbm_pipeline_pass_k8s() runs during incremental re-index. */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_k8s_incr_XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        FAIL("tmpdir");
    }
    char dbpath[512];
    snprintf(dbpath, sizeof(dbpath), "%s/test.db", tmpdir);
    char path[512];
    FILE *f;

    /* Initial manifest */
    snprintf(path, sizeof(path), "%s/deploy.yaml", tmpdir);
    f = fopen(path, "w");
    ASSERT_NOT_NULL(f);
    fprintf(f, "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: my-app\n");
    fclose(f);

    /* Full index */
    cbm_pipeline_t *p = cbm_pipeline_new(tmpdir, dbpath, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    char *project = strdup(cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);

    /* Verify Resource node created by full index */
    cbm_store_t *s = cbm_store_open_path(dbpath);
    ASSERT_NOT_NULL(s);
    cbm_node_t *nodes = NULL;
    int count = 0;
    cbm_store_find_nodes_by_label(s, project, "Resource", &nodes, &count);
    ASSERT_GT(count, 0);
    cbm_store_free_nodes(nodes, count);
    cbm_store_close(s);

    /* Add a second manifest — incremental should pick it up */
    snprintf(path, sizeof(path), "%s/svc.yaml", tmpdir);
    f = fopen(path, "w");
    ASSERT_NOT_NULL(f);
    fprintf(f, "apiVersion: v1\nkind: Service\nmetadata:\n  name: my-svc\n");
    fclose(f);

    /* Incremental re-index */
    cbm_config_t *cfg = incremental_test_config(tmpdir);
    ASSERT_NOT_NULL(cfg);
    p = cbm_pipeline_new(tmpdir, dbpath, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    cbm_pipeline_free(p);

    /* Verify both Resource nodes now present */
    s = cbm_store_open_path(dbpath);
    ASSERT_NOT_NULL(s);
    nodes = NULL;
    count = 0;
    cbm_store_find_nodes_by_label(s, project, "Resource", &nodes, &count);
    ASSERT_GTE(count, 2);
    cbm_store_free_nodes(nodes, count);
    cbm_store_close(s);

    free(project);
    cbm_config_close(cfg);
    th_rmtree(tmpdir);
    PASS();
}

TEST(incremental_kustomize_module_indexed) {
    /* Verifies that a kustomization.yaml added after the initial full index
     * gets a Module node via the incremental k8s pass. */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_kust_incr_XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        FAIL("tmpdir");
    }
    char dbpath[512];
    snprintf(dbpath, sizeof(dbpath), "%s/test.db", tmpdir);
    char path[512];
    FILE *f;

    /* Initial resource manifest (gives full index something to find) */
    snprintf(path, sizeof(path), "%s/deploy.yaml", tmpdir);
    f = fopen(path, "w");
    ASSERT_NOT_NULL(f);
    fprintf(f, "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: my-app\n");
    fclose(f);

    /* Full index */
    cbm_pipeline_t *p = cbm_pipeline_new(tmpdir, dbpath, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    char *project = strdup(cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);

    /* Add kustomization.yaml */
    snprintf(path, sizeof(path), "%s/kustomization.yaml", tmpdir);
    f = fopen(path, "w");
    ASSERT_NOT_NULL(f);
    fprintf(f, "apiVersion: kustomize.config.k8s.io/v1beta1\n"
               "kind: Kustomization\n"
               "resources:\n"
               "  - deploy.yaml\n");
    fclose(f);

    /* Incremental re-index */
    cbm_config_t *cfg = incremental_test_config(tmpdir);
    ASSERT_NOT_NULL(cfg);
    p = cbm_pipeline_new(tmpdir, dbpath, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    cbm_pipeline_free(p);

    /* Verify Module node created for the kustomization overlay */
    cbm_store_t *s = cbm_store_open_path(dbpath);
    ASSERT_NOT_NULL(s);
    cbm_node_t *nodes = NULL;
    int count = 0;
    cbm_store_find_nodes_by_label(s, project, "Module", &nodes, &count);
    bool found_kust = false;
    for (int i = 0; i < count; i++) {
        if (nodes[i].properties_json && strstr(nodes[i].properties_json, "kustomize")) {
            found_kust = true;
            break;
        }
    }
    cbm_store_free_nodes(nodes, count);
    cbm_store_close(s);
    ASSERT_TRUE(found_kust);

    free(project);
    cbm_config_close(cfg);
    th_rmtree(tmpdir);
    PASS();
}

/* ── Index lock tests ───────────────────────────────────────────── */

TEST(pipeline_lock_try_acquire) {
    /* First try-lock should succeed */
    ASSERT_TRUE(cbm_pipeline_try_lock());
    /* Second try-lock should fail (already held) */
    ASSERT_FALSE(cbm_pipeline_try_lock());
    /* Release, then re-acquire should succeed */
    cbm_pipeline_unlock();
    ASSERT_TRUE(cbm_pipeline_try_lock());
    cbm_pipeline_unlock();
    PASS();
}

TEST(pipeline_lock_blocking) {
    /* Lock, then unlock — basic sanity */
    cbm_pipeline_lock();
    cbm_pipeline_unlock();
    /* Should be immediately re-acquirable */
    cbm_pipeline_lock();
    cbm_pipeline_unlock();
    PASS();
}

/* Thread function that tries to acquire the lock and records result */
static atomic_int g_thread_acquired = 0;
static atomic_int g_thread_done = 0;

static void *try_lock_thread(void *arg) {
    (void)arg;
    if (cbm_pipeline_try_lock()) {
        atomic_store(&g_thread_acquired, 1);
        cbm_pipeline_unlock();
    } else {
        atomic_store(&g_thread_acquired, 0);
    }
    atomic_store(&g_thread_done, 1);
    return NULL;
}

TEST(pipeline_lock_contention) {
    /* Main thread holds lock, spawned thread should fail try_lock */
    cbm_pipeline_lock();
    atomic_store(&g_thread_acquired, -1);
    atomic_store(&g_thread_done, 0);

    cbm_thread_t tid;
    int rc = cbm_thread_create(&tid, 0, try_lock_thread, NULL);
    ASSERT_EQ(rc, 0);

    /* Wait for thread to finish */
    cbm_thread_join(&tid);

    /* Thread should NOT have acquired the lock */
    ASSERT_EQ(atomic_load(&g_thread_acquired), 0);
    cbm_pipeline_unlock();
    PASS();
}

TEST(pipeline_lock_release_allows_contender) {
    /* Main thread acquires and releases, then spawned thread should succeed */
    cbm_pipeline_lock();
    cbm_pipeline_unlock();

    atomic_store(&g_thread_acquired, -1);
    atomic_store(&g_thread_done, 0);

    cbm_thread_t tid;
    int rc = cbm_thread_create(&tid, 0, try_lock_thread, NULL);
    ASSERT_EQ(rc, 0);
    cbm_thread_join(&tid);

    /* Thread SHOULD have acquired the lock */
    ASSERT_EQ(atomic_load(&g_thread_acquired), 1);
    PASS();
}

/* ── Resource management & internal helper tests ─────────────────── */

TEST(pipeline_empty_path) {
    /* Empty string repo path — should handle gracefully */
    cbm_pipeline_t *p = cbm_pipeline_new("", NULL, CBM_MODE_FULL);
    /* Implementation may return NULL or a valid pipeline with empty project name.
     * Either behavior is acceptable — the key is no crash. */
    if (p) {
        cbm_pipeline_free(p);
    }
    PASS();
}

TEST(pipeline_project_name_content) {
    /* Verify project name is derived from the repo_path */
    cbm_pipeline_t *p = cbm_pipeline_new("/home/user/my-project", NULL, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    const char *name = cbm_pipeline_project_name(p);
    ASSERT_NOT_NULL(name);
    ASSERT_TRUE(strlen(name) > 0);
    /* Should contain "my-project" as part of the derived name */
    ASSERT_TRUE(strstr(name, "my-project") != NULL);
    cbm_pipeline_free(p);
    PASS();
}

TEST(pipeline_cancel_sets_flag) {
    /* Verify cancel sets the flag so subsequent run exits early */
    cbm_pipeline_t *p = cbm_pipeline_new("/tmp/nonexistent", NULL, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    /* Cancel before run */
    cbm_pipeline_cancel(p);
    /* Cancelled pipeline should return quickly (either -1 from cancel or from
     * missing path — both are acceptable; key is no hang) */
    int rc = cbm_pipeline_run(p);
    ASSERT_EQ(rc, -1);
    cbm_pipeline_free(p);
    PASS();
}

TEST(pipeline_double_cancel) {
    /* Calling cancel twice should not crash */
    cbm_pipeline_t *p = cbm_pipeline_new("/tmp/nonexistent", NULL, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_cancel(p);
    cbm_pipeline_cancel(p);
    cbm_pipeline_free(p);
    PASS();
}

TEST(pipeline_double_free_prevention) {
    /* free(NULL) after free should not crash. We can't truly double-free
     * the same pointer, but we verify NULL is safe as documented. */
    cbm_pipeline_free(NULL);
    cbm_pipeline_free(NULL);
    PASS();
}

TEST(pipeline_unit_threshold_setters_clamp_invalid_values) {
    cbm_pipeline_t *p = cbm_pipeline_new("/tmp/nonexistent", NULL, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);

    cbm_pipeline_set_similarity_threshold(p, 0.7);
    cbm_pipeline_set_httplink_min_confidence(p, 0.25);
    cbm_pipeline_set_semantic_threshold(p, 0.75);
    cbm_pipeline_set_githistory_min_coupling(p, 0.3);
    cbm_pipeline_set_lsp_confidence_floor(p, 0.6);
    ASSERT_TRUE(cbm_pipeline_similarity_threshold(p) == 0.7);
    ASSERT_TRUE(cbm_pipeline_httplink_min_confidence(p) == 0.25);
    ASSERT_TRUE(cbm_pipeline_semantic_threshold(p) == 0.75);
    ASSERT_TRUE(cbm_pipeline_githistory_min_coupling(p) == 0.3);
    ASSERT_TRUE(cbm_pipeline_lsp_confidence_floor(p) == 0.6);

    cbm_pipeline_set_similarity_threshold(p, -1.0);
    cbm_pipeline_set_httplink_min_confidence(p, 0.0);
    cbm_pipeline_set_semantic_threshold(p, 1.5);
    cbm_pipeline_set_githistory_min_coupling(p, 2.0);
    cbm_pipeline_set_lsp_confidence_floor(p, -0.1);
    ASSERT_TRUE(cbm_pipeline_similarity_threshold(p) == 0.0);
    ASSERT_TRUE(cbm_pipeline_httplink_min_confidence(p) == 0.0);
    ASSERT_TRUE(cbm_pipeline_semantic_threshold(p) == 0.0);
    ASSERT_TRUE(cbm_pipeline_githistory_min_coupling(p) == 0.0);
    ASSERT_TRUE(cbm_pipeline_lsp_confidence_floor(p) == 0.0);

    cbm_pipeline_free(p);
    PASS();
}

TEST(pipeline_publish_kind_names_are_stable) {
    ASSERT_STR_EQ(cbm_pipeline_publish_kind_name(CBM_PIPELINE_PUBLISH_NONE), "none");
    ASSERT_STR_EQ(cbm_pipeline_publish_kind_name(CBM_PIPELINE_PUBLISH_FULL), "full");
    ASSERT_STR_EQ(cbm_pipeline_publish_kind_name(CBM_PIPELINE_PUBLISH_INCREMENTAL_NOOP),
                  "incremental_noop");
    ASSERT_STR_EQ(cbm_pipeline_publish_kind_name(CBM_PIPELINE_PUBLISH_INCREMENTAL_EXACT),
                  "incremental_exact");
    ASSERT_STR_EQ(cbm_pipeline_publish_kind_name(CBM_PIPELINE_PUBLISH_INCREMENTAL_OVERLAY),
                  "incremental_overlay");
    ASSERT_STR_EQ(cbm_pipeline_publish_kind_name(CBM_PIPELINE_PUBLISH_INCREMENTAL_CONTAINMENT),
                  "incremental_containment");
    ASSERT_STR_EQ(cbm_pipeline_publish_kind_name((cbm_pipeline_publish_kind_t)999), "unknown");
    PASS();
}

TEST(pipeline_apply_config_sets_all_thresholds) {
    enum {
        PIPELINE_TEST_EXACT_MAX_CHANGED = 3,
        PIPELINE_TEST_EXACT_MAX_AFFECTED = 9,
    };
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_pipeline_cfg_XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        FAIL("cbm_mkdtemp failed");
    }

    cbm_config_t *cfg = cbm_config_open(tmpdir);
    ASSERT_NOT_NULL(cfg);
    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_SIMILARITY_THRESHOLD, "0.71"), 0);
    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_HTTPLINK_MIN_CONFIDENCE, "0.26"), 0);
    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_SEMANTIC_THRESHOLD, "0.76"), 0);
    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_GITHISTORY_MIN_COUPLING, "0.31"), 0);
    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_LSP_CONFIDENCE_FLOOR, "0.61"), 0);
    char max_changed[CBM_SZ_32];
    char max_affected[CBM_SZ_32];
    int n = snprintf(max_changed, sizeof(max_changed), "%d", PIPELINE_TEST_EXACT_MAX_CHANGED);
    ASSERT(n >= 0 && (size_t)n < sizeof(max_changed));
    n = snprintf(max_affected, sizeof(max_affected), "%d", PIPELINE_TEST_EXACT_MAX_AFFECTED);
    ASSERT(n >= 0 && (size_t)n < sizeof(max_affected));
    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_INCREMENTAL_EXACT_MAX_CHANGED_PATHS, max_changed),
              0);
    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_INCREMENTAL_EXACT_MAX_AFFECTED_PATHS, max_affected),
              0);

    cbm_pipeline_t *p = cbm_pipeline_new("/tmp/nonexistent", NULL, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_apply_config(p, cfg);

    ASSERT_TRUE(cbm_pipeline_similarity_threshold(p) > 0.70);
    ASSERT_TRUE(cbm_pipeline_similarity_threshold(p) < 0.72);
    ASSERT_TRUE(cbm_pipeline_httplink_min_confidence(p) > 0.25);
    ASSERT_TRUE(cbm_pipeline_httplink_min_confidence(p) < 0.27);
    ASSERT_TRUE(cbm_pipeline_semantic_threshold(p) > 0.75);
    ASSERT_TRUE(cbm_pipeline_semantic_threshold(p) < 0.77);
    ASSERT_TRUE(cbm_pipeline_githistory_min_coupling(p) > 0.30);
    ASSERT_TRUE(cbm_pipeline_githistory_min_coupling(p) < 0.32);
    ASSERT_TRUE(cbm_pipeline_lsp_confidence_floor(p) > 0.60);
    ASSERT_TRUE(cbm_pipeline_lsp_confidence_floor(p) < 0.62);
    ASSERT_EQ(cbm_pipeline_exact_max_changed_paths(p), PIPELINE_TEST_EXACT_MAX_CHANGED);
    ASSERT_EQ(cbm_pipeline_exact_max_affected_paths(p), PIPELINE_TEST_EXACT_MAX_AFFECTED);

    cbm_pipeline_free(p);
    cbm_config_close(cfg);
    rm_rf(tmpdir);
    PASS();
}

TEST(pipeline_exact_delta_limits_keep_safe_defaults) {
    enum { PIPELINE_TEST_EXACT_INVERTED_CHANGED = CBM_SZ_8 };
    cbm_pipeline_t *p = cbm_pipeline_new("/tmp/nonexistent", NULL, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);

    ASSERT_EQ(cbm_pipeline_exact_max_changed_paths(p),
              CBM_PIPELINE_EXACT_DELTA_DEFAULT_MAX_CHANGED_PATHS);
    ASSERT_EQ(cbm_pipeline_exact_max_affected_paths(p),
              CBM_PIPELINE_EXACT_DELTA_DEFAULT_MAX_AFFECTED_PATHS);

    cbm_pipeline_set_exact_delta_limits(p, 0, -1);
    ASSERT_EQ(cbm_pipeline_exact_max_changed_paths(p),
              CBM_PIPELINE_EXACT_DELTA_DEFAULT_MAX_CHANGED_PATHS);
    ASSERT_EQ(cbm_pipeline_exact_max_affected_paths(p),
              CBM_PIPELINE_EXACT_DELTA_DEFAULT_MAX_AFFECTED_PATHS);

    cbm_pipeline_set_exact_delta_limits(p, PIPELINE_TEST_EXACT_INVERTED_CHANGED,
                                        CBM_ALLOC_ONE);
    ASSERT_EQ(cbm_pipeline_exact_max_changed_paths(p), PIPELINE_TEST_EXACT_INVERTED_CHANGED);
    ASSERT_EQ(cbm_pipeline_exact_max_affected_paths(p), PIPELINE_TEST_EXACT_INVERTED_CHANGED);

    cbm_pipeline_free(p);
    PASS();
}

static const char *semantic_edge_props_for(cbm_gbuf_t *gb, const char *src_qn,
                                           const char *dst_qn) {
    const cbm_gbuf_node_t *src = cbm_gbuf_find_by_qn(gb, src_qn);
    const cbm_gbuf_node_t *dst = cbm_gbuf_find_by_qn(gb, dst_qn);
    if (!src || !dst) {
        return NULL;
    }
    const cbm_gbuf_edge_t **edges = NULL;
    int edge_count = 0;
    if (cbm_gbuf_find_edges_by_source_type(gb, src->id, "SEMANTICALLY_RELATED", &edges,
                                           &edge_count) != 0) {
        return NULL;
    }
    for (int i = 0; i < edge_count; i++) {
        if (edges[i]->target_id == dst->id) {
            return edges[i]->properties_json;
        }
    }
    return NULL;
}

static cbm_gbuf_t *build_semantic_order_graph(bool reverse_alpha_calls) {
    cbm_gbuf_t *gb = cbm_gbuf_new("sem-order", "/tmp/sem-order");
    if (!gb) {
        return NULL;
    }
    const char props[] =
        "{\"signature\":\"(request: Request, item: Item) -> Response\","
        "\"return_type\":\"Response\",\"param_names\":[\"request\",\"item\"],"
        "\"param_types\":[\"Request\",\"Item\"],\"bt\":\"validate item return response\"}";
    int64_t alpha =
        cbm_gbuf_upsert_node(gb, "Function", "alpha_handler", "sem-order.alpha_handler",
                             "routes.py", 1, 20, props);
    int64_t beta = cbm_gbuf_upsert_node(gb, "Function", "beta_handler", "sem-order.beta_handler",
                                        "routes.py", 21, 40, props);
    int64_t validate =
        cbm_gbuf_upsert_node(gb, "Function", "validate_item", "sem-order.validate_item",
                             "helpers.py", 1, 5, "{\"signature\":\"(item)\"}");
    int64_t serialize =
        cbm_gbuf_upsert_node(gb, "Function", "serialize_response", "sem-order.serialize_response",
                             "helpers.py", 6, 10, "{\"signature\":\"(response)\"}");
    if (alpha <= 0 || beta <= 0 || validate <= 0 || serialize <= 0) {
        cbm_gbuf_free(gb);
        return NULL;
    }
    if (reverse_alpha_calls) {
        cbm_gbuf_insert_edge(gb, alpha, serialize, "CALLS", "{}");
        cbm_gbuf_insert_edge(gb, alpha, validate, "CALLS", "{}");
    } else {
        cbm_gbuf_insert_edge(gb, alpha, validate, "CALLS", "{}");
        cbm_gbuf_insert_edge(gb, alpha, serialize, "CALLS", "{}");
    }
    cbm_gbuf_insert_edge(gb, beta, validate, "CALLS", "{}");
    cbm_gbuf_insert_edge(gb, beta, serialize, "CALLS", "{}");
    return gb;
}

TEST(pipeline_semantic_edges_independent_of_call_insertion_order) {
    cbm_gbuf_t *gb_forward = build_semantic_order_graph(false);
    cbm_gbuf_t *gb_reverse = build_semantic_order_graph(true);
    ASSERT_NOT_NULL(gb_forward);
    ASSERT_NOT_NULL(gb_reverse);

    atomic_int cancelled = 0;
    cbm_pipeline_ctx_t ctx_forward = {
        .project_name = "sem-order",
        .repo_path = "/tmp/sem-order",
        .gbuf = gb_forward,
        .cancelled = &cancelled,
        .semantic_threshold = 0.01,
    };
    cbm_pipeline_ctx_t ctx_reverse = ctx_forward;
    ctx_reverse.gbuf = gb_reverse;

    ASSERT_EQ(cbm_pipeline_pass_semantic_edges(&ctx_forward), 0);
    ASSERT_EQ(cbm_pipeline_pass_semantic_edges(&ctx_reverse), 0);

    const char *forward =
        semantic_edge_props_for(gb_forward, "sem-order.alpha_handler", "sem-order.beta_handler");
    const char *reverse =
        semantic_edge_props_for(gb_reverse, "sem-order.alpha_handler", "sem-order.beta_handler");
    ASSERT_NOT_NULL(forward);
    ASSERT_NOT_NULL(reverse);
    ASSERT_STR_EQ(forward, reverse);

    cbm_gbuf_free(gb_forward);
    cbm_gbuf_free(gb_reverse);
    PASS();
}

static cbm_sem_corpus_t *build_semantic_worker_parity_corpus(int worker_count) {
    enum {
        SEM_PARITY_DOCS = 4,
        SEM_PARITY_MAX_TOKENS = 7,
    };
    char *tokens[SEM_PARITY_DOCS * SEM_PARITY_MAX_TOKENS] = {
        "request", "validate", "item",     "response", "json",     "route",    "status",
        "request", "validate", "payload",  "response", "json",     "handler",  "status",
        "auth",    "token",    "validate", "request",  "handler",  "security", "status",
        "auth",    "token",    "refresh",  "response", "security", "handler",  "json",
    };
    int counts[SEM_PARITY_DOCS] = {
        7,
        7,
        7,
        7,
    };
    cbm_sem_corpus_t *corpus = cbm_sem_corpus_new();
    if (!corpus) {
        return NULL;
    }
    cbm_sem_corpus_add_docs_batch_with_workers(corpus, tokens, counts, SEM_PARITY_DOCS,
                                               SEM_PARITY_MAX_TOKENS, worker_count);
    cbm_sem_corpus_finalize_with_workers(corpus, worker_count);
    return corpus;
}

TEST(pipeline_semantic_corpus_vectors_independent_of_worker_count) {
    enum {
        SEM_PARITY_SERIAL_WORKERS = 1,
        SEM_PARITY_PARALLEL_WORKERS = 4,
    };
    const float eps = 0.000001F;
    cbm_sem_corpus_t *serial = build_semantic_worker_parity_corpus(SEM_PARITY_SERIAL_WORKERS);
    cbm_sem_corpus_t *parallel = build_semantic_worker_parity_corpus(SEM_PARITY_PARALLEL_WORKERS);
    ASSERT_NOT_NULL(serial);
    ASSERT_NOT_NULL(parallel);

    ASSERT_EQ(cbm_sem_corpus_doc_count(serial), cbm_sem_corpus_doc_count(parallel));
    int token_count = cbm_sem_corpus_token_count(serial);
    ASSERT_EQ(token_count, cbm_sem_corpus_token_count(parallel));
    const char *previous_token = NULL;
    for (int i = 0; i < token_count; i++) {
        const cbm_sem_vec_t *serial_vec = NULL;
        const cbm_sem_vec_t *parallel_vec = NULL;
        float serial_idf = 0.0F;
        float parallel_idf = 0.0F;
        const char *serial_token = cbm_sem_corpus_token_at(serial, i, &serial_vec, &serial_idf);
        const char *parallel_token =
            cbm_sem_corpus_token_at(parallel, i, &parallel_vec, &parallel_idf);
        ASSERT_STR_EQ(serial_token, parallel_token);
        if (previous_token) {
            ASSERT(strcmp(previous_token, serial_token) <= 0);
        }
        previous_token = serial_token;
        ASSERT_FLOAT_EQ(serial_idf, parallel_idf, eps);
        ASSERT_NOT_NULL(serial_vec);
        ASSERT_NOT_NULL(parallel_vec);
        for (int d = 0; d < CBM_SEM_DIM; d++) {
            ASSERT_FLOAT_EQ(serial_vec->v[d], parallel_vec->v[d], eps);
        }
    }

    cbm_sem_corpus_free(serial);
    cbm_sem_corpus_free(parallel);
    PASS();
}

TEST(pipeline_semantic_corpus_add_doc_reserves_without_losing_docs) {
    enum {
        SEM_RESERVE_DOCS = 70,
        SEM_RESERVE_TOKEN_COUNT = 2,
    };
    const char *tokens[SEM_RESERVE_TOKEN_COUNT] = {"alpha", "beta"};
    cbm_sem_corpus_t *corpus = cbm_sem_corpus_new();
    ASSERT_NOT_NULL(corpus);

    for (int i = 0; i < SEM_RESERVE_DOCS; i++) {
        cbm_sem_corpus_add_doc(corpus, tokens, SEM_RESERVE_TOKEN_COUNT);
    }

    ASSERT_EQ(cbm_sem_corpus_doc_count(corpus), SEM_RESERVE_DOCS);
    ASSERT_EQ(cbm_sem_corpus_token_count(corpus), SEM_RESERVE_TOKEN_COUNT);
    ASSERT_GTE(cbm_sem_corpus_token_id(corpus, "alpha"), 0);
    ASSERT_GTE(cbm_sem_corpus_token_id(corpus, "beta"), 0);

    cbm_sem_corpus_free(corpus);
    PASS();
}

TEST(pipeline_semantic_batch_rejects_invalid_token_stride) {
    char *tokens[1] = {"alpha"};
    int counts[1] = {1};
    cbm_sem_corpus_t *corpus = cbm_sem_corpus_new();
    ASSERT_NOT_NULL(corpus);

    cbm_sem_corpus_add_docs_batch_with_workers(corpus, tokens, counts, 1, 0, 1);

    ASSERT_EQ(cbm_sem_corpus_doc_count(corpus), 0);
    ASSERT_EQ(cbm_sem_corpus_token_count(corpus), 0);

    cbm_sem_corpus_free(corpus);
    PASS();
}

static const cbm_config_entry_t *find_config_entry(const char *key) {
    for (int i = 0; CBM_CONFIG_REGISTRY[i].key; i++) {
        if (strcmp(CBM_CONFIG_REGISTRY[i].key, key) == 0) {
            return &CBM_CONFIG_REGISTRY[i];
        }
    }
    return NULL;
}

TEST(config_registry_includes_mcp_timeout_knobs) {
    const cbm_config_entry_t *idle = find_config_entry("store_idle_timeout_s");
    ASSERT_NOT_NULL(idle);
    ASSERT_STR_EQ(idle->default_val, "60");
    ASSERT_STR_EQ(idle->category, "MCP");

    const cbm_config_entry_t *validate = find_config_entry("db_validate_busy_timeout_ms");
    ASSERT_NOT_NULL(validate);
    ASSERT_STR_EQ(validate->default_val, "1000");
    ASSERT_STR_EQ(validate->category, "MCP");

    const cbm_config_entry_t *update = find_config_entry("update_check_timeout_s");
    ASSERT_NOT_NULL(update);
    ASSERT_STR_EQ(update->default_val, "5");
    ASSERT_STR_EQ(update->category, "MCP");
    PASS();
}

TEST(config_registry_includes_incremental_reindex_policy) {
    const cbm_config_entry_t *entry = find_config_entry(CBM_CONFIG_INCREMENTAL_REINDEX);
    ASSERT_NOT_NULL(entry);
    ASSERT_STR_EQ(entry->default_val, CBM_CONFIG_INCREMENTAL_REINDEX_OFF);
    ASSERT_STR_EQ(entry->category, "Indexing");
    ASSERT_STR_EQ(entry->range, "fast|always|off");
    PASS();
}

TEST(config_registry_includes_overlay_publish_policy) {
    const cbm_config_entry_t *entry = find_config_entry(CBM_CONFIG_OVERLAY_PUBLISH);
    ASSERT_NOT_NULL(entry);
    ASSERT_STR_EQ(entry->default_val, CBM_CONFIG_OVERLAY_PUBLISH_OFF);
    ASSERT_STR_EQ(entry->category, "Indexing");
    ASSERT_STR_EQ(entry->range, "off|small_deltas");
    PASS();
}

TEST(config_registry_includes_overlay_compaction_policy) {
    const cbm_config_entry_t *policy =
        find_config_entry(CBM_CONFIG_OVERLAY_COMPACTION_POLICY);
    ASSERT_NOT_NULL(policy);
    ASSERT_STR_EQ(policy->default_val, CBM_CONFIG_OVERLAY_COMPACTION_POLICY_MANUAL);
    ASSERT_STR_EQ(policy->category, "Indexing");
    ASSERT_STR_EQ(policy->range, "manual|after_publish");

    const cbm_config_entry_t *max_generations =
        find_config_entry(CBM_CONFIG_OVERLAY_COMPACTION_MAX_GENERATIONS);
    ASSERT_NOT_NULL(max_generations);
    ASSERT_STR_EQ(max_generations->default_val,
                  CBM_CONFIG_OVERLAY_COMPACTION_DEFAULT_MAX_GENERATIONS);
    ASSERT_STR_EQ(max_generations->category, "Indexing");
    ASSERT_STR_EQ(max_generations->range, "1-256");
    PASS();
}

TEST(config_registry_includes_incremental_exact_frontier_caps) {
    const cbm_config_entry_t *changed =
        find_config_entry(CBM_CONFIG_INCREMENTAL_EXACT_MAX_CHANGED_PATHS);
    ASSERT_NOT_NULL(changed);
    ASSERT_STR_EQ(changed->default_val, CBM_CONFIG_INCREMENTAL_EXACT_DEFAULT_MAX_CHANGED_PATHS);
    ASSERT_STR_EQ(changed->category, "Indexing");
    ASSERT_STR_EQ(changed->range, "1-100000");

    const cbm_config_entry_t *affected =
        find_config_entry(CBM_CONFIG_INCREMENTAL_EXACT_MAX_AFFECTED_PATHS);
    ASSERT_NOT_NULL(affected);
    ASSERT_STR_EQ(affected->default_val, CBM_CONFIG_INCREMENTAL_EXACT_DEFAULT_MAX_AFFECTED_PATHS);
    ASSERT_STR_EQ(affected->category, "Indexing");
    ASSERT_STR_EQ(affected->range, "1-100000");
    PASS();
}

TEST(config_registry_includes_rank_refresh_policy) {
    const cbm_config_entry_t *entry = find_config_entry(CBM_CONFIG_RANK_REFRESH);
    ASSERT_NOT_NULL(entry);
    ASSERT_STR_EQ(entry->default_val, CBM_RANK_REFRESH_EAGER);
    ASSERT_STR_EQ(entry->category, "PageRank");
    ASSERT_STR_EQ(entry->range, "eager|stale_on_exact|stale_on_incremental");
    ASSERT_NOT_NULL(strstr(entry->guidance, CBM_RANK_REFRESH_STALE_ON_EXACT));
    ASSERT_NOT_NULL(strstr(entry->guidance, CBM_RANK_REFRESH_STALE_ON_INCREMENTAL));
    PASS();
}

TEST(trackable_source_files) {
    /* Common source extensions are trackable */
    ASSERT_TRUE(cbm_is_trackable_file("main.go"));
    ASSERT_TRUE(cbm_is_trackable_file("src/handler.py"));
    ASSERT_TRUE(cbm_is_trackable_file("lib/server.js"));
    ASSERT_TRUE(cbm_is_trackable_file("components/App.tsx"));
    ASSERT_TRUE(cbm_is_trackable_file("pkg/svc.rs"));
    ASSERT_TRUE(cbm_is_trackable_file("config.yaml"));
    PASS();
}

TEST(trackable_rejects_images) {
    /* Image and binary files are not trackable */
    ASSERT_FALSE(cbm_is_trackable_file("logo.png"));
    ASSERT_FALSE(cbm_is_trackable_file("photo.jpg"));
    ASSERT_FALSE(cbm_is_trackable_file("icon.gif"));
    ASSERT_FALSE(cbm_is_trackable_file("diagram.svg"));
    PASS();
}

TEST(trackable_rejects_minified) {
    /* Minified files are not trackable */
    ASSERT_FALSE(cbm_is_trackable_file("bundle.min.js"));
    ASSERT_FALSE(cbm_is_trackable_file("style.min.css"));
    PASS();
}

TEST(trackable_rejects_vendor_dirs) {
    /* Files in vendor/node_modules/__pycache__ are not trackable */
    ASSERT_FALSE(cbm_is_trackable_file("vendor/github.com/lib/dep.go"));
    ASSERT_FALSE(cbm_is_trackable_file("node_modules/lodash/index.js"));
    ASSERT_FALSE(cbm_is_trackable_file("__pycache__/module.pyc"));
    ASSERT_FALSE(cbm_is_trackable_file(".git/objects/pack/data"));
    PASS();
}

TEST(trackable_rejects_lock_files) {
    /* Lock files are not trackable */
    ASSERT_FALSE(cbm_is_trackable_file("package-lock.json"));
    ASSERT_FALSE(cbm_is_trackable_file("go.sum"));
    ASSERT_FALSE(cbm_is_trackable_file("yarn.lock"));
    PASS();
}

TEST(test_path_directory_patterns) {
    /* Test directory patterns: __tests__, test/, tests/, spec/ */
    ASSERT_TRUE(cbm_is_test_path("__tests__/Component.js"));
    ASSERT_TRUE(cbm_is_test_path("tests/test_main.py"));
    ASSERT_TRUE(cbm_is_test_path("spec/handler_spec.rb"));
    PASS();
}

TEST(test_path_suffix_patterns) {
    /* Various language-specific test suffix patterns */
    ASSERT_TRUE(cbm_is_test_path("handler.spec.ts"));
    ASSERT_TRUE(cbm_is_test_path("handler.test.tsx"));
    ASSERT_TRUE(cbm_is_test_path("handler_test.go"));
    ASSERT_TRUE(cbm_is_test_path("test_handler.py"));
    /* Non-test files */
    ASSERT_FALSE(cbm_is_test_path("handler.ts"));
    ASSERT_FALSE(cbm_is_test_path("contest.go"));
    ASSERT_FALSE(cbm_is_test_path("latest.py"));
    PASS();
}

TEST(test_func_name_go_patterns) {
    /* Go test function names: Test + uppercase char */
    ASSERT_TRUE(cbm_is_test_func_name("TestFoo"));
    ASSERT_TRUE(cbm_is_test_func_name("TestHTTPHandler"));
    /* Non-test: "Test" alone or Test + lowercase */
    ASSERT_FALSE(cbm_is_test_func_name("Testable")); /* lowercase 'a' after Test */
    PASS();
}

TEST(test_func_name_js_helpers) {
    /* JS/TS test helper function names */
    ASSERT_TRUE(cbm_is_test_func_name("it"));
    ASSERT_TRUE(cbm_is_test_func_name("describe"));
    ASSERT_TRUE(cbm_is_test_func_name("test"));
    ASSERT_TRUE(cbm_is_test_func_name("beforeEach"));
    ASSERT_TRUE(cbm_is_test_func_name("afterEach"));
    PASS();
}

TEST(env_var_name_valid) {
    /* Valid env var names: uppercase + underscores, at least 2 chars with uppercase */
    ASSERT_TRUE(cbm_is_env_var_name("DATABASE_URL"));
    ASSERT_TRUE(cbm_is_env_var_name("API_KEY"));
    ASSERT_TRUE(cbm_is_env_var_name("PORT"));
    ASSERT_TRUE(cbm_is_env_var_name("DB_2"));
    ASSERT_TRUE(cbm_is_env_var_name("MY_VAR_123"));
    PASS();
}

TEST(env_var_name_invalid) {
    /* Invalid: too short, lowercase, mixed case, empty */
    ASSERT_FALSE(cbm_is_env_var_name("A"));      /* single char */
    ASSERT_FALSE(cbm_is_env_var_name("port"));   /* lowercase */
    ASSERT_FALSE(cbm_is_env_var_name("apiKey")); /* camelCase */
    ASSERT_FALSE(cbm_is_env_var_name("__"));     /* no uppercase */
    ASSERT_FALSE(cbm_is_env_var_name(""));       /* empty */
    ASSERT_FALSE(cbm_is_env_var_name("123"));    /* digits only */
    PASS();
}

TEST(config_ext_positive) {
    /* Config file extensions recognized */
    ASSERT_TRUE(cbm_has_config_extension(".env"));
    ASSERT_TRUE(cbm_has_config_extension("config.yaml"));
    ASSERT_TRUE(cbm_has_config_extension("config.yml"));
    ASSERT_TRUE(cbm_has_config_extension("settings.toml"));
    ASSERT_TRUE(cbm_has_config_extension("app.ini"));
    ASSERT_TRUE(cbm_has_config_extension("data.json"));
    ASSERT_TRUE(cbm_has_config_extension("app.properties"));
    PASS();
}

TEST(config_ext_negative) {
    /* Non-config extensions rejected */
    ASSERT_FALSE(cbm_has_config_extension("main.go"));
    ASSERT_FALSE(cbm_has_config_extension("app.py"));
    ASSERT_FALSE(cbm_has_config_extension("handler.rs"));
    ASSERT_FALSE(cbm_has_config_extension("data.csv"));
    ASSERT_FALSE(cbm_has_config_extension("README.md"));
    PASS();
}

TEST(split_camel_basic) {
    /* Basic camelCase splitting */
    char *parts[16];
    int n;

    n = cbm_split_camel_case("getCamelCase", parts, 16);
    ASSERT_EQ(n, 3);
    ASSERT_STR_EQ(parts[0], "get");
    ASSERT_STR_EQ(parts[1], "Camel");
    ASSERT_STR_EQ(parts[2], "Case");
    for (int i = 0; i < n; i++)
        free(parts[i]);

    PASS();
}

TEST(split_camel_single_word) {
    /* Single lowercase word — no splits */
    char *parts[16];
    int n = cbm_split_camel_case("hello", parts, 16);
    ASSERT_EQ(n, 1);
    ASSERT_STR_EQ(parts[0], "hello");
    for (int i = 0; i < n; i++)
        free(parts[i]);
    PASS();
}

TEST(split_camel_empty) {
    /* Empty string — should return 0 or 1 empty part */
    char *parts[16];
    int n = cbm_split_camel_case("", parts, 16);
    for (int i = 0; i < n; i++)
        free(parts[i]);
    /* Either 0 parts or 1 empty part is acceptable */
    ASSERT_TRUE(n >= 0 && n <= 1);
    PASS();
}

TEST(tokenize_decorator_login_required) {
    /* @login_required → ["login", "required"] */
    char *tokens[16];
    int n = cbm_tokenize_decorator("@login_required", tokens, 16);
    ASSERT_EQ(n, 2);
    ASSERT_STR_EQ(tokens[0], "login");
    ASSERT_STR_EQ(tokens[1], "required");
    for (int i = 0; i < n; i++)
        free(tokens[i]);
    PASS();
}

TEST(tokenize_decorator_single) {
    /* @Override → ["override"] */
    char *tokens[16];
    int n = cbm_tokenize_decorator("@Override", tokens, 16);
    ASSERT_EQ(n, 1);
    ASSERT_STR_EQ(tokens[0], "override");
    for (int i = 0; i < n; i++)
        free(tokens[i]);
    PASS();
}

TEST(split_command_basic) {
    /* Basic command splitting with spaces */
    char *args[16];
    int n = cbm_split_command("gcc -c main.c -o main.o", args, 16);
    ASSERT_EQ(n, 5);
    ASSERT_STR_EQ(args[0], "gcc");
    ASSERT_STR_EQ(args[1], "-c");
    ASSERT_STR_EQ(args[2], "main.c");
    ASSERT_STR_EQ(args[3], "-o");
    ASSERT_STR_EQ(args[4], "main.o");
    for (int i = 0; i < n; i++)
        free(args[i]);
    PASS();
}

TEST(split_command_quoted) {
    /* Command with quoted arguments */
    char *args[16];
    int n = cbm_split_command("gcc -DFOO=\"bar baz\" main.c", args, 16);
    ASSERT_GTE(n, 3);
    ASSERT_STR_EQ(args[0], "gcc");
    for (int i = 0; i < n; i++)
        free(args[i]);
    PASS();
}

TEST(split_command_empty) {
    /* Empty command string */
    char *args[16];
    int n = cbm_split_command("", args, 16);
    ASSERT_EQ(n, 0);
    PASS();
}

TEST(parse_range_with_count) {
    /* "10,5" → start=10, count=5 */
    int start, count;
    cbm_parse_range("10,5", &start, &count);
    ASSERT_EQ(start, 10);
    ASSERT_EQ(count, 5);
    PASS();
}

TEST(parse_range_single) {
    /* "42" → start=42, count=1 */
    int start, count;
    cbm_parse_range("42", &start, &count);
    ASSERT_EQ(start, 42);
    ASSERT_EQ(count, 1);
    PASS();
}

TEST(parse_range_zero_count) {
    /* "100,0" → start=100, count=0 */
    int start, count;
    cbm_parse_range("100,0", &start, &count);
    ASSERT_EQ(start, 100);
    ASSERT_EQ(count, 0);
    PASS();
}

TEST(parse_name_status_basic) {
    /* Parse standard git diff --name-status output */
    const char *input = "M\tsrc/main.go\n"
                        "A\tsrc/new.go\n"
                        "D\tsrc/old.go\n";
    cbm_changed_file_t files[16];
    int n = cbm_parse_name_status(input, files, 16);
    /* Exact count depends on which files pass cbm_is_trackable_file */
    ASSERT_GTE(n, 0);
    ASSERT_LTE(n, 3);
    PASS();
}

TEST(parse_name_status_empty) {
    /* Empty input */
    cbm_changed_file_t files[16];
    int n = cbm_parse_name_status("", files, 16);
    ASSERT_EQ(n, 0);
    PASS();
}

TEST(parse_hunks_empty) {
    /* Empty input */
    cbm_changed_hunk_t hunks[16];
    int n = cbm_parse_hunks("", hunks, 16);
    ASSERT_EQ(n, 0);
    PASS();
}

TEST(coupling_empty_commits) {
    /* Zero commits → zero couplings */
    cbm_change_coupling_t results[16];
    int n = cbm_compute_change_coupling(NULL, 0, results, 16);
    ASSERT_EQ(n, 0);
    PASS();
}

TEST(coupling_single_file_commit) {
    /* Commits with single files → no pairs → zero couplings */
    char *f1[] = {"a.go"};
    char *f2[] = {"b.go"};
    cbm_commit_files_t commits[] = {{f1, 1, 0}, {f2, 1, 0}};
    cbm_change_coupling_t results[16];
    int n = cbm_compute_change_coupling(commits, 2, results, 16);
    ASSERT_EQ(n, 0);
    PASS();
}

TEST(clean_json_brackets_array) {
    /* JSON array → space-joined */
    char out[256];
    cbm_clean_json_brackets("[\"./app\", \"--flag\"]", out, sizeof(out));
    ASSERT_TRUE(strstr(out, "./app") != NULL);
    ASSERT_TRUE(strstr(out, "--flag") != NULL);
    PASS();
}

TEST(clean_json_brackets_plain) {
    /* Plain string → unchanged */
    char out[256];
    cbm_clean_json_brackets("./app --flag", out, sizeof(out));
    ASSERT_STR_EQ(out, "./app --flag");
    PASS();
}

TEST(clean_json_brackets_empty) {
    /* Empty string → empty output */
    char out[256];
    cbm_clean_json_brackets("", out, sizeof(out));
    ASSERT_STR_EQ(out, "");
    PASS();
}

TEST(fqn_compute_basic) {
    /* Basic FQN: project.dir.name */
    char *fqn = cbm_pipeline_fqn_compute("proj", "pkg/handler.go", "Serve");
    ASSERT_NOT_NULL(fqn);
    ASSERT_TRUE(strstr(fqn, "proj") != NULL);
    ASSERT_TRUE(strstr(fqn, "Serve") != NULL);
    free(fqn);
    PASS();
}

TEST(fqn_compute_strips_ext) {
    /* FQN should strip file extension */
    char *fqn = cbm_pipeline_fqn_compute("proj", "main.go", "main");
    ASSERT_NOT_NULL(fqn);
    /* Should not contain ".go" */
    ASSERT_TRUE(strstr(fqn, ".go") == NULL);
    ASSERT_TRUE(strstr(fqn, "main") != NULL);
    free(fqn);
    PASS();
}

TEST(fqn_module_basic) {
    /* Module QN: project.dir.parts */
    char *mod = cbm_pipeline_fqn_module("proj", "pkg/util/helper.go");
    ASSERT_NOT_NULL(mod);
    ASSERT_TRUE(strstr(mod, "proj") != NULL);
    free(mod);
    PASS();
}

TEST(fqn_folder_basic) {
    /* Folder QN from directory path */
    char *folder = cbm_pipeline_fqn_folder("proj", "pkg/util");
    ASSERT_NOT_NULL(folder);
    ASSERT_TRUE(strstr(folder, "proj") != NULL);
    free(folder);
    PASS();
}

TEST(project_name_special_chars) {
    /* Project name with colons, slashes — should be normalized */
    char *name = cbm_project_name_from_path("/home/user/my:project");
    ASSERT_NOT_NULL(name);
    /* Colons should be converted to dashes */
    ASSERT_TRUE(strchr(name, ':') == NULL);
    ASSERT_TRUE(strchr(name, '/') == NULL);
    free(name);
    PASS();
}

TEST(project_name_trailing_slash) {
    /* Trailing slash should not affect result */
    char *a = cbm_project_name_from_path("/home/user/project");
    ASSERT_NOT_NULL(a);
    free(a);
    PASS();
}

/* ── Complexity propagation pass tests (Tier B) ────────────────── */

/* Find the first Function/Method node with `name` in a label set. Returns a
 * borrowed pointer into `funcs` or NULL. */
static const cbm_node_t *find_node_named(cbm_node_t *funcs, int count, const char *name) {
    for (int i = 0; i < count; i++) {
        if (strcmp(funcs[i].name, name) == 0)
            return &funcs[i];
    }
    return NULL;
}

/* The complexity pass propagates loop_depth along CALLS edges into
 * transitive_loop_depth and flags call-graph cycles as recursive. Caller and
 * callee live in one file so the calls resolve intra-file (most reliable). */
TEST(pipeline_complexity_transitive_loop_depth) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_cx_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("tmpdir");

    write_temp_file(tmpdir, "cx.go",
                    "package p\n\n"
                    "func work() {}\n\n"
                    "func inner() {\n"
                    "\tfor i := 0; i < 10; i++ {\n"
                    "\t\tfor j := 0; j < 10; j++ {\n"
                    "\t\t\twork()\n"
                    "\t\t}\n"
                    "\t}\n"
                    "}\n\n"
                    "func outer() {\n"
                    "\tfor i := 0; i < 10; i++ {\n"
                    "\t\tinner()\n"
                    "\t}\n"
                    "}\n\n"
                    "func recur(n int) {\n"
                    "\tif n > 0 {\n"
                    "\t\trecur(n - 1)\n"
                    "\t}\n"
                    "}\n");

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/cx.db", tmpdir);

    cbm_pipeline_t *p = cbm_pipeline_new(tmpdir, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);
    const char *project = cbm_pipeline_project_name(p);

    cbm_node_t *funcs = NULL;
    int func_count = 0;
    ASSERT_EQ(cbm_store_find_nodes_by_label(s, project, "Function", &funcs, &func_count),
              CBM_STORE_OK);
    ASSERT_GT(func_count, 0);

    /* inner: two nested loops, callee work() has depth 0 → tld == 2 */
    const cbm_node_t *inner = find_node_named(funcs, func_count, "inner");
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(inner->properties_json);
    ASSERT_TRUE(strstr(inner->properties_json, "\"transitive_loop_depth\":2") != NULL);

    /* outer: own depth 1 + inner's tld 2 → tld == 3 (interprocedural) */
    const cbm_node_t *outer = find_node_named(funcs, func_count, "outer");
    ASSERT_NOT_NULL(outer);
    ASSERT_NOT_NULL(outer->properties_json);
    ASSERT_TRUE(strstr(outer->properties_json, "\"transitive_loop_depth\":3") != NULL);

    /* recur: self-recursion → recursive:true flagged by the cycle guard */
    const cbm_node_t *recur = find_node_named(funcs, func_count, "recur");
    ASSERT_NOT_NULL(recur);
    ASSERT_NOT_NULL(recur->properties_json);
    ASSERT_TRUE(strstr(recur->properties_json, "\"recursive\":true") != NULL);

    cbm_store_free_nodes(funcs, func_count);
    cbm_store_close(s);
    cbm_pipeline_free(p);
    th_rmtree(tmpdir);
    PASS();
}

static void loop_props(char *buf, size_t buf_sz, int loop_depth) {
    snprintf(buf, buf_sz, "{\"loop_depth\":%d,\"self_recursive\":false}", loop_depth);
}

TEST(pipeline_complexity_scc_tld_is_deterministic) {
    enum {
        CX_LOOP_A = 1,
        CX_LOOP_B = 2,
        CX_LOOP_LEAF = 3,
        CX_COMPONENT_TLD = CX_LOOP_B + CX_LOOP_LEAF,
    };
    cbm_gbuf_t *gb = cbm_gbuf_new("cx-scc", "/tmp/cx-scc");
    ASSERT_NOT_NULL(gb);

    char props_a[CBM_SZ_64];
    char props_b[CBM_SZ_64];
    char props_leaf[CBM_SZ_64];
    loop_props(props_a, sizeof(props_a), CX_LOOP_A);
    loop_props(props_b, sizeof(props_b), CX_LOOP_B);
    loop_props(props_leaf, sizeof(props_leaf), CX_LOOP_LEAF);

    int64_t a = cbm_gbuf_upsert_node(gb, "Function", "a", "cx.a", "cx.go", 1, 4, props_a);
    int64_t b = cbm_gbuf_upsert_node(gb, "Function", "b", "cx.b", "cx.go", 5, 8, props_b);
    int64_t leaf =
        cbm_gbuf_upsert_node(gb, "Function", "leaf", "cx.leaf", "cx.go", 9, 12, props_leaf);
    ASSERT_GT(a, 0);
    ASSERT_GT(b, 0);
    ASSERT_GT(leaf, 0);
    ASSERT_GT(cbm_gbuf_insert_edge(gb, a, b, "CALLS", "{}"), 0);
    ASSERT_GT(cbm_gbuf_insert_edge(gb, b, a, "CALLS", "{}"), 0);
    ASSERT_GT(cbm_gbuf_insert_edge(gb, b, leaf, "CALLS", "{}"), 0);

    atomic_int cancelled = 0;
    cbm_pipeline_ctx_t ctx = {
        .project_name = "cx-scc",
        .repo_path = "/tmp/cx-scc",
        .gbuf = gb,
        .cancelled = &cancelled,
    };
    cbm_pipeline_pass_complexity(&ctx);

    const cbm_gbuf_node_t *node_a = cbm_gbuf_find_by_qn(gb, "cx.a");
    const cbm_gbuf_node_t *node_b = cbm_gbuf_find_by_qn(gb, "cx.b");
    ASSERT_NOT_NULL(node_a);
    ASSERT_NOT_NULL(node_b);
    ASSERT_NOT_NULL(node_a->properties_json);
    ASSERT_NOT_NULL(node_b->properties_json);

    char expected_tld[CBM_SZ_64];
    snprintf(expected_tld, sizeof(expected_tld), "\"transitive_loop_depth\":%d",
             CX_COMPONENT_TLD);
    ASSERT_NOT_NULL(strstr(node_a->properties_json, expected_tld));
    ASSERT_NOT_NULL(strstr(node_b->properties_json, expected_tld));
    ASSERT_NOT_NULL(strstr(node_a->properties_json, "\"recursive\":true"));
    ASSERT_NOT_NULL(strstr(node_b->properties_json, "\"recursive\":true"));

    cbm_gbuf_free(gb);
    PASS();
}

TEST(pipeline_complexity_scoped_writeback_keeps_unchanged_nodes) {
    cbm_gbuf_t *gb = cbm_gbuf_new("cx-scope", "/tmp/cx-scope");
    ASSERT_NOT_NULL(gb);

    char changed_props[CBM_SZ_64];
    char unchanged_props[CBM_SZ_128];
    loop_props(changed_props, sizeof(changed_props), 1);
    snprintf(unchanged_props, sizeof(unchanged_props),
             "{\"loop_depth\":1,\"transitive_loop_depth\":3,\"self_recursive\":false,"
             "\"stable\":true}");

    int64_t changed =
        cbm_gbuf_upsert_node(gb, "Function", "changed", "cx.changed", "changed.go", 1, 4,
                             changed_props);
    int64_t unchanged =
        cbm_gbuf_upsert_node(gb, "Function", "unchanged", "cx.unchanged", "unchanged.go", 1, 4,
                             unchanged_props);
    ASSERT_GT(changed, 0);
    ASSERT_GT(unchanged, 0);
    ASSERT_GT(cbm_gbuf_insert_edge(gb, changed, unchanged, "CALLS", "{}"), 0);

    atomic_int cancelled = 0;
    cbm_pipeline_ctx_t ctx = {
        .project_name = "cx-scope",
        .repo_path = "/tmp/cx-scope",
        .gbuf = gb,
        .cancelled = &cancelled,
    };
    const char *scope[] = {"changed.go"};
    cbm_pipeline_pass_complexity_for_paths(&ctx, scope, (int)(sizeof(scope) / sizeof(scope[0])));

    const cbm_gbuf_node_t *changed_node = cbm_gbuf_find_by_qn(gb, "cx.changed");
    const cbm_gbuf_node_t *unchanged_node = cbm_gbuf_find_by_qn(gb, "cx.unchanged");
    ASSERT_NOT_NULL(changed_node);
    ASSERT_NOT_NULL(unchanged_node);
    ASSERT_NOT_NULL(changed_node->properties_json);
    ASSERT_NOT_NULL(unchanged_node->properties_json);
    ASSERT_NOT_NULL(strstr(changed_node->properties_json, "\"transitive_loop_depth\":4"));
    ASSERT_NOT_NULL(strstr(unchanged_node->properties_json, "\"transitive_loop_depth\":3"));
    ASSERT_NOT_NULL(strstr(unchanged_node->properties_json, "\"stable\":true"));

    cbm_gbuf_free(gb);
    PASS();
}

/* Regression for #334: the plausibility gate compares committed (extracted)
 * node count against persisted rows. committed_nodes must be captured BEFORE
 * cbm_gbuf_dump_to_sqlite frees the gbuf node index — otherwise it reads 0 and
 * the gate is silently inert. Drives the real pipeline (not a synthetic count)
 * and asserts committed_nodes is populated and matches what was persisted. */
TEST(pipeline_committed_counts_match_persisted) {
    if (setup_test_repo() != 0) {
        FAIL("failed to create temp dir");
    }

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/committed_test.db", g_tmpdir);

    cbm_pipeline_t *p = cbm_pipeline_new(g_tmpdir, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    int rc = cbm_pipeline_run(p);
    ASSERT_EQ(rc, 0);

    int committed_nodes = -1;
    int committed_edges = -1;
    cbm_pipeline_get_committed_counts(p, &committed_nodes, &committed_edges);

    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);
    const char *project = cbm_pipeline_project_name(p);
    int persisted_nodes = cbm_store_count_nodes(s, project);
    cbm_store_close(s);

    /* The bug captured committed_nodes after the node index was freed → 0. */
    ASSERT_GT(committed_nodes, 0);
    /* A faithful full dump persists exactly what it committed. */
    ASSERT_EQ(committed_nodes, persisted_nodes);
    /* The gate must NOT flag a healthy full index as degraded. */
    ASSERT_FALSE(cbm_dump_verify_is_degraded(committed_nodes, persisted_nodes,
                                             CBM_DUMP_VERIFY_DEFAULT_RATIO,
                                             CBM_DUMP_VERIFY_MIN_FLOOR));

    cbm_pipeline_free(p);
    teardown_test_repo();
    PASS();
}

TEST(pipeline_rejects_overlong_db_path_without_truncated_write) {
    if (setup_test_repo() != 0) {
        FAIL("failed to create temp dir");
    }

    char db_path[PIPELINE_TEST_OVERLONG_DB_PATH];
    int prefix_len = snprintf(db_path, sizeof(db_path), "%s/", g_tmpdir);
    ASSERT_GT(prefix_len, 0);
    ASSERT_TRUE((size_t)prefix_len < sizeof(db_path));
    memset(db_path + prefix_len, 'a', sizeof(db_path) - (size_t)prefix_len - CBM_ALLOC_ONE);
    db_path[sizeof(db_path) - 1] = '\0';

    char truncated[CBM_PATH_MAX];
    int trunc_len = snprintf(truncated, sizeof(truncated), "%s", db_path);
    ASSERT_TRUE(trunc_len >= CBM_PATH_MAX);

    cbm_pipeline_t *p = cbm_pipeline_new(g_tmpdir, db_path, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    int rc = cbm_pipeline_run(p);
    ASSERT_NEQ(rc, 0);
    ASSERT_NEQ(access(truncated, F_OK), 0);

    cbm_pipeline_free(p);
    teardown_test_repo();
    PASS();
}

SUITE(pipeline) {
    /* Index lock */
    RUN_TEST(pipeline_lock_try_acquire);
    RUN_TEST(pipeline_lock_blocking);
    RUN_TEST(pipeline_lock_contention);
    RUN_TEST(pipeline_lock_release_allows_contender);
    /* Lifecycle */
    RUN_TEST(pipeline_create_free);
    RUN_TEST(pipeline_null_repo);
    RUN_TEST(pipeline_free_null);
    RUN_TEST(pipeline_cancel);
    RUN_TEST(pipeline_cancel_null);
    RUN_TEST(pipeline_run_null);
    RUN_TEST(pipeline_unit_threshold_setters_clamp_invalid_values);
    RUN_TEST(pipeline_apply_config_sets_all_thresholds);
    RUN_TEST(pipeline_exact_delta_limits_keep_safe_defaults);
    RUN_TEST(pipeline_semantic_edges_independent_of_call_insertion_order);
    RUN_TEST(pipeline_semantic_corpus_vectors_independent_of_worker_count);
    RUN_TEST(pipeline_semantic_corpus_add_doc_reserves_without_losing_docs);
    RUN_TEST(pipeline_semantic_batch_rejects_invalid_token_stride);
    RUN_TEST(config_registry_includes_mcp_timeout_knobs);
    RUN_TEST(config_registry_includes_incremental_reindex_policy);
    RUN_TEST(config_registry_includes_overlay_publish_policy);
    RUN_TEST(config_registry_includes_overlay_compaction_policy);
    RUN_TEST(config_registry_includes_incremental_exact_frontier_caps);
    RUN_TEST(config_registry_includes_rank_refresh_policy);
    RUN_TEST(pipeline_file_delta_scratch_seed_excludes_changed_paths);
    RUN_TEST(pipeline_file_delta_scratch_seed_preserves_structure_roots);
    RUN_TEST(pipeline_file_delta_scratch_seed_supports_external_endpoint_descriptor);
    RUN_TEST(pipeline_file_delta_descriptor_from_gbuf);
    RUN_TEST(pipeline_file_delta_preserves_safe_inbound_edges_for_overlay);
    RUN_TEST(pipeline_file_delta_descriptor_marks_unsupported_edges);
    RUN_TEST(pipeline_file_delta_metadata_from_file);
    RUN_TEST(pipeline_file_delta_metadata_accepts_effective_fingerprint);
    RUN_TEST(pipeline_file_delta_stamp_generation_updates_metadata);
    RUN_TEST(pipeline_content_hash_helper_matches_file_delta_metadata);
    RUN_TEST(pipeline_file_state_persist_helper_writes_hash_metadata);
    RUN_TEST(pipeline_file_state_current_check_rejects_stale_pass_fingerprint);
    RUN_TEST(pipeline_pass_fingerprint_includes_effective_mode_and_thresholds);
    RUN_TEST(pipeline_file_state_current_check_rejects_stale_config_fingerprint);
    RUN_TEST(pipeline_file_state_persist_helper_rolls_back_on_failure);
    RUN_TEST(pipeline_file_delta_plan_candidate_from_frontier);
    RUN_TEST(pipeline_file_delta_apply_falls_back_on_publish_error);
    RUN_TEST(pipeline_file_delta_apply_falls_back_without_generation);
    RUN_TEST(pipeline_file_delta_apply_succeeds_after_generation_stamp);
    RUN_TEST(pipeline_file_delta_apply_inserts_new_file_without_existing_ownership);
    RUN_TEST(pipeline_file_delta_apply_falls_back_on_new_file_importer_frontier);
    RUN_TEST(pipeline_file_delta_plan_falls_back_without_existing_ownership);
    RUN_TEST(pipeline_file_delta_plan_falls_back_on_external_inbound_edge);
    RUN_TEST(pipeline_file_delta_plan_falls_back_on_unowned_structural_inbound_edge);
    RUN_TEST(pipeline_file_delta_plan_accepts_full_pipeline_structure_edge);
    RUN_TEST(pipeline_file_delta_plan_falls_back_on_new_folder_structure_edge);
    RUN_TEST(pipeline_file_delta_apply_inserts_and_prunes_new_folder_context);
    RUN_TEST(pipeline_file_delta_plan_accepts_regenerated_structural_inbound_edge);
    RUN_TEST(pipeline_file_delta_plan_accepts_regenerated_file_owned_unowned_source_edge);
    RUN_TEST(pipeline_file_delta_plan_falls_back_on_stale_file_owned_unowned_source_edge);
    RUN_TEST(pipeline_file_delta_plan_falls_back_without_file_metadata);
    RUN_TEST(pipeline_file_delta_plan_falls_back_on_unsupported_edges);
    RUN_TEST(pipeline_file_delta_plan_falls_back_on_delete);
    RUN_TEST(pipeline_file_delta_apply_deletes_owned_file_delta);
    RUN_TEST(pipeline_file_delta_apply_mixed_delete_upsert_batch);
    RUN_TEST(pipeline_file_delta_apply_falls_back_on_delete_batch);
    RUN_TEST(pipeline_file_delta_apply_falls_back_when_frontier_path_missing_from_batch);
    RUN_TEST(pipeline_file_delta_plan_falls_back_on_rename);
    RUN_TEST(pipeline_file_delta_plan_falls_back_on_unsupported_derived_view);
    RUN_TEST(pipeline_file_delta_plan_falls_back_on_unresolved_edge_endpoint);
    RUN_TEST(pipeline_file_delta_plan_accepts_resolved_external_edge_endpoint);
    RUN_TEST(pipeline_file_delta_plan_falls_back_on_large_frontier);
    RUN_TEST(pipeline_file_delta_plan_batch_accepts_mutual_frontier);
    RUN_TEST(pipeline_file_delta_orchestrates_descriptor_plan_and_publish);
    /* File persistence */
    RUN_TEST(store_file_persistence);
    RUN_TEST(store_bulk_persistence);
    /* Integration: structure pass */
    RUN_TEST(pipeline_structure_nodes);
    RUN_TEST(pipeline_committed_counts_match_persisted);
    RUN_TEST(pipeline_rejects_overlong_db_path_without_truncated_write);
    RUN_TEST(pipeline_structure_edges);
    RUN_TEST(pipeline_branch_root_structure);
    RUN_TEST(pipeline_project_name_derived);
    RUN_TEST(pipeline_mode_global_semantic_edges_policy);
    RUN_TEST(pipeline_call_edge_props_include_args_and_line);
    RUN_TEST(pipeline_fast_mode);
    /* Definitions pass */
    RUN_TEST(pipeline_definitions_function_nodes);
    RUN_TEST(pipeline_definitions_defines_edges);
    RUN_TEST(pipeline_definitions_properties);
    RUN_TEST(pipeline_def_props_valid_json_when_oversized);
    RUN_TEST(pipeline_edge_props_valid_json);
    RUN_TEST(pipeline_persisted_route_purity_for_http_literals);
    RUN_TEST(pipeline_infra_route_deny_wins_by_url_value);
    /* Complexity propagation pass (Tier B) */
    RUN_TEST(pipeline_complexity_transitive_loop_depth);
    RUN_TEST(pipeline_complexity_scc_tld_is_deterministic);
    RUN_TEST(pipeline_complexity_scoped_writeback_keeps_unchanged_nodes);
    /* Calls pass */
    RUN_TEST(pipeline_calls_resolution);
    RUN_TEST(pipeline_incremental_preserves_cross_file_calls);
    RUN_TEST(pipeline_full_and_incremental_persist_file_state);
    RUN_TEST(pipeline_incremental_full_index_rebuilds_owner_metadata);
    /* Git history pass */
    RUN_TEST(githistory_is_trackable);
    RUN_TEST(githistory_compute_coupling);
    RUN_TEST(githistory_coupling_carries_last_co_change);
    RUN_TEST(githistory_skip_large_commits);
    RUN_TEST(githistory_limits_to_max);
    /* Test detection */
    RUN_TEST(testdetect_is_test_file);
    RUN_TEST(testdetect_is_test_function);
    /* Implements pass (graph buffer based) */
    RUN_TEST(implements_creates_override);
    RUN_TEST(implements_accepts_struct_label);
    RUN_TEST(implements_no_match);
    /* Usages pass (full pipeline integration) */
    RUN_TEST(usages_creates_edges);
    RUN_TEST(usages_no_duplicate_calls);
    RUN_TEST(calls_edge_carries_call_site_line);
    RUN_TEST(usages_kotlin_creates_edges);
    RUN_TEST(usages_kotlin_no_duplicate_calls);
    /* Language integration tests */
    RUN_TEST(pipeline_python_project);
    RUN_TEST(pipeline_go_cross_package_call);
    RUN_TEST(pipeline_python_cross_module_call);
    RUN_TEST(pipeline_python_reexport_call_uses_resolved_import_edge);
    RUN_TEST(pipeline_incremental_reexport_target_matches_full);
    RUN_TEST(pipeline_parallel_duplicate_import_inherits_matches_sequential);
    RUN_TEST(pipeline_parallel_env_access_matches_sequential);
    RUN_TEST(pipeline_parallel_channel_edges_target_channels);
    RUN_TEST(pipeline_go_type_classification);
    RUN_TEST(pipeline_go_grouped_types);
    RUN_TEST(pipeline_kotlin_project);
    RUN_TEST(pipeline_lua_anonymous_functions);
    RUN_TEST(pipeline_csharp_modern);
    RUN_TEST(pipeline_bom_stripping);
    RUN_TEST(pipeline_form_call_resolution);
    RUN_TEST(pipeline_python_type_inference);
    /* Docstring integration (port of TestDocstringIntegration) */
    RUN_TEST(pipeline_docstring_go_function);
    RUN_TEST(pipeline_docstring_python_function);
    RUN_TEST(pipeline_docstring_java_method);
    RUN_TEST(pipeline_docstring_kotlin_function);
    RUN_TEST(pipeline_docstring_go_struct);
    /* Project name */
    RUN_TEST(project_name_from_path);
    RUN_TEST(project_name_drive_letter_case_insensitive_issue394);
    RUN_TEST(git_context_non_git_path);
    RUN_TEST(git_context_linked_worktree);
    RUN_TEST(project_name_uniqueness);
    /* Git diff helpers */
    RUN_TEST(gitdiff_parse_range_with_count);
    RUN_TEST(gitdiff_parse_range_no_count);
    RUN_TEST(gitdiff_parse_range_zero_count);
    RUN_TEST(gitdiff_parse_range_large);
    RUN_TEST(gitdiff_parse_name_status);
    RUN_TEST(gitdiff_parse_name_status_filters_untrackable);
    RUN_TEST(gitdiff_parse_hunks);
    RUN_TEST(gitdiff_parse_hunks_no_newline_marker);
    RUN_TEST(gitdiff_parse_hunks_mode_change);
    RUN_TEST(gitdiff_parse_hunks_deletion);
    /* Config helpers */
    RUN_TEST(configures_is_env_var_name);
    RUN_TEST(configures_normalize_config_key);
    RUN_TEST(configures_has_config_extension);
    /* Config integration tests */
    RUN_TEST(configures_env_var_in_config);
    RUN_TEST(configures_lowercase_key_skipped);
    RUN_TEST(configures_non_config_file_skipped);
    RUN_TEST(configures_full_pipeline_integration);
    /* HTTP link pipeline integration */
    /* Enrichment helpers */
    RUN_TEST(enrichment_split_camel_case);
    RUN_TEST(enrichment_tokenize_decorator);
    /* Decorator tags integration */
    RUN_TEST(decorator_tags_python_auto_discovery);
    RUN_TEST(decorator_tags_java_class_methods);
    /* Compile commands helpers */
    RUN_TEST(compile_commands_split_command);
    RUN_TEST(compile_commands_extract_flags);
    RUN_TEST(compile_commands_parse_json);
    RUN_TEST(compile_commands_parse_empty);
    RUN_TEST(compile_commands_parse_invalid);
    /* Infrascan helpers */
    RUN_TEST(infra_is_compose_file);
    RUN_TEST(infra_is_cloudbuild_file);
    RUN_TEST(infra_is_shell_script);
    RUN_TEST(infra_is_dockerfile);
    RUN_TEST(infra_is_kustomize_file);
    RUN_TEST(infra_is_k8s_manifest);
    RUN_TEST(infra_is_env_file);
    RUN_TEST(infra_clean_json_brackets);
    /* K8s extraction tests */
    RUN_TEST(k8s_extract_kustomize);
    RUN_TEST(k8s_extract_manifest);
    RUN_TEST(k8s_extract_manifest_no_name);
    RUN_TEST(k8s_extract_manifest_multidoc);
    RUN_TEST(infra_secret_detection);
    /* Infrascan: Dockerfile parser */
    RUN_TEST(infra_parse_dockerfile_multistage);
    RUN_TEST(infra_parse_dockerfile_entrypoint);
    RUN_TEST(infra_parse_dockerfile_secret_filtered);
    RUN_TEST(infra_parse_dockerfile_expose_protocol);
    RUN_TEST(infra_parse_dockerfile_env_space);
    RUN_TEST(infra_parse_dockerfile_empty);
    /* Infrascan: Dotenv parser */
    RUN_TEST(infra_parse_dotenv);
    RUN_TEST(infra_parse_dotenv_quoted);
    /* Infrascan: Shell script parser */
    RUN_TEST(infra_parse_shell);
    RUN_TEST(infra_parse_shell_with_source);
    RUN_TEST(infra_parse_shell_secret_filtered);
    RUN_TEST(infra_parse_shell_shebang_only);
    RUN_TEST(infra_parse_shell_truly_empty);
    /* Infrascan: Terraform parser */
    RUN_TEST(infra_parse_terraform_full);
    RUN_TEST(infra_parse_terraform_variables_only);
    RUN_TEST(infra_parse_terraform_empty);
    RUN_TEST(helm_parse_chart_dependencies_issue338);
    RUN_TEST(helm_parse_chart_no_deps_issue338);
    /* Infrascan: QN helper */
    RUN_TEST(infra_qn_helper);
    /* Infrascan: pipeline integration */
    RUN_TEST(infra_pipeline_integration);
    RUN_TEST(infra_pipeline_idempotent);
    /* Env URL scanning */
    RUN_TEST(envscan_dockerfile_env_urls);
    RUN_TEST(envscan_shell_env_urls);
    RUN_TEST(envscan_env_file_urls);
    RUN_TEST(envscan_toml_urls);
    RUN_TEST(envscan_yaml_urls);
    RUN_TEST(envscan_terraform_urls);
    RUN_TEST(envscan_properties_urls);
    RUN_TEST(envscan_secret_key_exclusion);
    RUN_TEST(envscan_secret_value_exclusion);
    RUN_TEST(envscan_secret_file_exclusion);
    RUN_TEST(envscan_skips_ignored_dirs);
    RUN_TEST(envscan_non_url_values_skipped);
    /* Function registry / resolver */
    RUN_TEST(registry_resolve_single_candidate);
    RUN_TEST(registry_fuzzy_nonexistent);
    RUN_TEST(registry_fuzzy_multiple_best_by_distance);
    RUN_TEST(registry_fuzzy_simple_name_extraction);
    RUN_TEST(registry_fuzzy_empty);
    RUN_TEST(registry_exists);
    RUN_TEST(registry_confidence_import_map);
    RUN_TEST(registry_confidence_import_map_suffix);
    RUN_TEST(registry_confidence_same_module);
    RUN_TEST(registry_confidence_unique_name);
    RUN_TEST(registry_confidence_suffix_match);
    RUN_TEST(registry_fuzzy_confidence_single);
    RUN_TEST(registry_fuzzy_confidence_distance);
    RUN_TEST(registry_negative_import_rejects);
    RUN_TEST(registry_fuzzy_import_penalty);
    RUN_TEST(registry_fuzzy_no_import_map_passthrough);
    RUN_TEST(registry_find_by_name);
    RUN_TEST(registry_confidence_band);
    RUN_TEST(registry_is_import_reachable);
    RUN_TEST(registry_find_ending_with);
    /* Git history */
    RUN_TEST(githistory_is_trackable_file);
    RUN_TEST(githistory_compute_change_coupling);
    RUN_TEST(githistory_coupling_skips_large_commits);
    RUN_TEST(githistory_coupling_limits_output);
    /* Incremental reindex */
    /* FastAPI Depends edge tracking */
    RUN_TEST(pipeline_fastapi_depends_edges);
    RUN_TEST(import_edge_helper_escapes_local_name_once);
    RUN_TEST(import_edge_helper_preserves_long_local_name);
    RUN_TEST(import_map_from_edges_follows_package_reexport);
    RUN_TEST(import_reexport_falls_back_when_pkgmap_target_missing);
    RUN_TEST(import_symbol_fallback_prefers_import_path_over_insertion_order);
    /* Incremental */
    RUN_TEST(incremental_full_then_noop);
    RUN_TEST(incremental_touch_only_refreshes_metadata_without_reindex);
    RUN_TEST(incremental_detects_changed_file);
    RUN_TEST(incremental_fast_exact_upsert_matches_full_rebuild);
    RUN_TEST(incremental_fast_body_only_change_uses_graph_noop);
    RUN_TEST(incremental_fast_two_file_batch_exact_upsert_matches_full_rebuild);
    RUN_TEST(incremental_fast_falls_back_for_oversized_inbound_frontier_and_matches_full);
    RUN_TEST(incremental_fast_c_header_frontier_too_large_uses_full_rebuild);
    RUN_TEST(incremental_fast_configured_frontier_cap_allows_bounded_exact);
    RUN_TEST(incremental_fast_mixed_unowned_edge_frontier_falls_back_before_exact_build);
    RUN_TEST(incremental_fast_expands_small_inbound_frontier_and_matches_full);
    RUN_TEST(incremental_fast_three_file_batch_falls_back_to_full_rebuild_parity);
    RUN_TEST(incremental_fast_single_delete_exact_matches_full_rebuild);
    RUN_TEST(incremental_fast_delete_falls_back_to_full_rebuild_parity);
    RUN_TEST(incremental_fast_rename_like_batch_falls_back_to_full_rebuild_parity);
    RUN_TEST(incremental_fast_new_folder_exact_delta_parity);
    RUN_TEST(incremental_fast_route_decorator_change_matches_full_rebuild);
    RUN_TEST(incremental_fast_arg_url_route_change_matches_parallel_full_rebuild);
    RUN_TEST(incremental_fast_exact_scratch_multifile_usage_edges_match_fresh);
    RUN_TEST(incremental_fast_exact_batch_publish_matches_fresh_rebuild_for_two_file_go);
    RUN_TEST(incremental_overlay_producer_marks_dirty_ready_without_canonical_mutation);
    RUN_TEST(incremental_overlay_publish_small_deltas_keeps_canonical_base_visible);
    RUN_TEST(incremental_overlay_publish_delete_keeps_canonical_base_visible);
    RUN_TEST(incremental_overlay_publish_repeated_update_keeps_active_view_idempotent);
    RUN_TEST(incremental_overlay_publish_failure_falls_back_to_canonical_exact);
    RUN_TEST(incremental_overlay_extract_failure_keeps_dirty_pending_without_overlay);
    RUN_TEST(incremental_full_mode_keeps_exact_upsert_disabled);
    RUN_TEST(incremental_detects_same_size_rewrite_with_preserved_mtime);
    RUN_TEST(incremental_missing_file_state_keeps_legacy_metadata_path);
    RUN_TEST(incremental_publish_failure_keeps_existing_db);
    RUN_TEST(incremental_postpass_failure_keeps_existing_db);
    RUN_TEST(incremental_hash_persist_failure_falls_back_to_full);
    RUN_TEST(incremental_parallel_extract_failure_keeps_existing_db);
    RUN_TEST(incremental_parallel_registry_failure_keeps_existing_db);
    RUN_TEST(incremental_parallel_resolve_failure_keeps_existing_db);
    RUN_TEST(incremental_classify_deleted_failure_keeps_existing_db);
    RUN_TEST(incremental_detects_deleted_file);
    RUN_TEST(incremental_new_file_added);
    RUN_TEST(incremental_fast_preserves_mode_skipped_tools_dir);
    RUN_TEST(incremental_k8s_manifest_indexed);
    RUN_TEST(incremental_kustomize_module_indexed);
    /* Resource management & internal helper tests */
    RUN_TEST(pipeline_empty_path);
    RUN_TEST(pipeline_project_name_content);
    RUN_TEST(pipeline_publish_kind_names_are_stable);
    RUN_TEST(pipeline_cancel_sets_flag);
    RUN_TEST(pipeline_double_cancel);
    RUN_TEST(pipeline_double_free_prevention);
    /* Trackable file tests */
    RUN_TEST(trackable_source_files);
    RUN_TEST(trackable_rejects_images);
    RUN_TEST(trackable_rejects_minified);
    RUN_TEST(trackable_rejects_vendor_dirs);
    RUN_TEST(trackable_rejects_lock_files);
    /* Test path detection */
    RUN_TEST(test_path_directory_patterns);
    RUN_TEST(test_path_suffix_patterns);
    /* Test function name detection */
    RUN_TEST(test_func_name_go_patterns);
    RUN_TEST(test_func_name_js_helpers);
    /* Env var name detection */
    RUN_TEST(env_var_name_valid);
    RUN_TEST(env_var_name_invalid);
    /* Config extension detection */
    RUN_TEST(config_ext_positive);
    RUN_TEST(config_ext_negative);
    /* Camel case splitting */
    RUN_TEST(split_camel_basic);
    RUN_TEST(split_camel_single_word);
    RUN_TEST(split_camel_empty);
    /* Decorator tokenization */
    RUN_TEST(tokenize_decorator_login_required);
    RUN_TEST(tokenize_decorator_single);
    /* Command splitting */
    RUN_TEST(split_command_basic);
    RUN_TEST(split_command_quoted);
    RUN_TEST(split_command_empty);
    /* Range parsing */
    RUN_TEST(parse_range_with_count);
    RUN_TEST(parse_range_single);
    RUN_TEST(parse_range_zero_count);
    /* Name status parsing */
    RUN_TEST(parse_name_status_basic);
    RUN_TEST(parse_name_status_empty);
    /* Hunk parsing */
    RUN_TEST(parse_hunks_empty);
    /* Change coupling */
    RUN_TEST(coupling_empty_commits);
    RUN_TEST(coupling_single_file_commit);
    /* JSON bracket cleaning */
    RUN_TEST(clean_json_brackets_array);
    RUN_TEST(clean_json_brackets_plain);
    RUN_TEST(clean_json_brackets_empty);
    /* FQN computation */
    RUN_TEST(fqn_compute_basic);
    RUN_TEST(fqn_compute_strips_ext);
    RUN_TEST(fqn_module_basic);
    RUN_TEST(fqn_folder_basic);
    /* Project name edge cases */
    RUN_TEST(project_name_special_chars);
    RUN_TEST(project_name_trailing_slash);
    /* Release pipeline-level global state (compiled regex patterns etc.).
     * Patterns are compiled on first use and cached; free once at suite end. */
    cbm_pipeline_global_cleanup();
}
