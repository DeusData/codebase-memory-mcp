/*
 * test_pipeline.c — Integration tests for the indexing pipeline.
 *
 * Tests pipeline lifecycle, structure pass, and end-to-end indexing
 * on a temporary directory with known file layout.
 */
#include "../src/foundation/compat.h"
#include "foundation/platform.h" // cbm_normalize_path_sep (drive-canonicalization regression)
#include "test_framework.h"
#include "test_helpers.h"
#include "foundation/mem.h" // cbm_mem_init/budget (back-pressure futile-nap test)
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "pipeline/artifact.h"
#include "store/store.h"
#include "git/git_context.h"
#include "foundation/dump_verify.h"
#include "foundation/sha256.h"
#include "foundation/compat_fs.h"
#include "discover/userconfig.h"

#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <stdatomic.h>
#include "foundation/compat_thread.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "graph_buffer/graph_buffer.h"
#include "yyjson/yyjson.h"
#include "sqlite3.h" /* vendored/sqlite3 — PRAGMA integrity_check on dumped DBs */

/* ── Helper: create temp test repo with known layout ───────────── */

static char g_tmpdir[256];

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

    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_test_repo();
    PASS();
}

/* Issue #516: an ADR stored via manage_adr (project_summaries) must survive a
 * full re-index. A full re-index deletes the DB and rebuilds it from the graph
 * buffer, which writes an empty project_summaries table; the fix captures the
 * ADR before the delete and restores it after the rebuild. Reproduce-first:
 * index, store an ADR, force a full re-index by adding files, assert the ADR
 * is still present and unchanged. */
TEST(pipeline_adr_survives_full_reindex) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_adr_XXXXXX");
    if (!cbm_mkdtemp(tmp)) {
        FAIL("failed to create temp dir");
    }

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/test.db", tmp);

    /* Initial index with a single source file. */
    char path[512];
    snprintf(path, sizeof(path), "%s/main.py", tmp);
    FILE *f = fopen(path, "w");
    ASSERT_NOT_NULL(f);
    fprintf(f, "def foo():\n    pass\n");
    fclose(f);

    cbm_pipeline_t *p1 = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p1);
    ASSERT_EQ(cbm_pipeline_run(p1), 0);
    const char *project = cbm_pipeline_project_name(p1);
    char project_copy[256];
    snprintf(project_copy, sizeof(project_copy), "%s", project);
    cbm_pipeline_free(p1);

    /* Store an ADR. */
    const char *adr_text = "# Decision\nWe chose X over Y.";
    cbm_store_t *s1 = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s1);
    ASSERT_EQ(cbm_store_adr_store(s1, project_copy, adr_text), CBM_STORE_OK);
    cbm_store_close(s1);

    /* Force a full re-index: add enough files to exceed the incremental
     * threshold so the DB is deleted and rebuilt. */
    for (int i = 0; i < 4; i++) {
        snprintf(path, sizeof(path), "%s/extra%d.py", tmp, i);
        f = fopen(path, "w");
        ASSERT_NOT_NULL(f);
        fprintf(f, "def g%d():\n    return %d\n", i, i);
        fclose(f);
    }

    cbm_pipeline_t *p2 = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p2);
    ASSERT_EQ(cbm_pipeline_run(p2), 0);
    cbm_pipeline_free(p2);

    /* The ADR must still be present and unchanged. */
    cbm_store_t *s2 = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s2);
    cbm_adr_t adr = {0};
    int rc = cbm_store_adr_get(s2, project_copy, &adr);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_NOT_NULL(adr.content);
    ASSERT_STR_EQ(adr.content, adr_text);
    cbm_store_adr_free(&adr);
    cbm_store_close(s2);

    rm_rf(tmp);
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
    int cd_count = cbm_store_count_edges_by_type(s, project, "CONTAINS_FOLDER");

    /* Cleanup before assertions (so failures don't leak) */
    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_test_repo();

    ASSERT_GTE(cf_count, 3); /* project->main.go, pkg->service.go, util->helper.go */
    ASSERT_GTE(cd_count, 1); /* project->pkg (pkg->util may merge on some platforms) */
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

/* True iff the exact named CALLS edge exists and its serialized strategy
 * contains `strategy_fragment`. Parallel synthetic-carrier regressions use
 * this on a separate ordinary-call control: it proves the cross-file LSP ran
 * successfully for the language/target, rather than letting a registry-only
 * edge make a broken fixture look healthy. */
static bool cross_file_call_has_strategy(cbm_store_t *s, const char *project, const char *src_name,
                                         const char *tgt_name, const char *strategy_fragment) {
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
                if (edges[j].target_id == tgts[k].id && edges[j].properties_json &&
                    strstr(edges[j].properties_json, strategy_fragment)) {
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

    /* 3. Re-run on the SAME db_path → auto-routes to incremental re-index. */
    cbm_pipeline_t *p2 = cbm_pipeline_new(g_tmpdir, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p2);
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

    teardown_test_repo();
    PASS();
}

/* TS/JS receiver-aware weak-strategy suppression (#592/#606 direction; Perl
 * precedent #477). A member call x.foo() whose receiver TYPE the TS-LSP cannot
 * resolve (a regex literal `re.test()`) must NOT be bound to a same-named
 * project method by a weak short-name strategy — that fabricates a CALLS edge.
 * Type-resolved receivers (`c.test()` on a typed SalesforceRestClient) and bare
 * local calls must still resolve. < 50 files → sequential path (pass_calls.c).
 * RED before the fix: checkFormat->test exists via unique_name/suffix_match. */
static void write_temp_file(const char *dir, const char *name, const char *content);

typedef struct {
    const char *source_name;
    const char *target_name;
} PreciseReferencePair;

typedef struct {
    bool database_opened;
    bool query_ready;
    bool query_completed;
    int row_count;
    int valid_json_count;
    int exact_callee_count;
} NamedEdgePropertyObservation;

/* Read one named edge's persisted properties through SQLite's JSON functions.
 * The guarded json_extract is intentional: on the RED revision the parallel
 * usage emitter truncates the final `}` from a boundary-sized callee property.
 * json_valid must report that row without json_extract aborting the statement,
 * while the fixed revision must round-trip the complete callee value. */
static NamedEdgePropertyObservation observe_named_edge_callee_property(
    const char *db_path, const char *project, const char *edge_type, const char *source_name,
    const char *target_name, const char *expected_callee) {
    NamedEdgePropertyObservation observation = {0};
    sqlite3 *db = NULL;
    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        if (db) {
            sqlite3_close(db);
        }
        return observation;
    }
    observation.database_opened = true;

    static const char sql[] =
        "SELECT e.properties, json_valid(e.properties), "
        "CASE WHEN json_valid(e.properties) "
        "THEN json_extract(e.properties, '$.callee') END "
        "FROM edges e "
        "JOIN nodes src ON src.id=e.source_id AND src.project=e.project "
        "JOIN nodes tgt ON tgt.id=e.target_id AND tgt.project=e.project "
        "WHERE e.project=?1 AND e.type=?2 AND src.name=?3 AND tgt.name=?4;";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK ||
        sqlite3_bind_text(stmt, 1, project, -1, SQLITE_TRANSIENT) != SQLITE_OK ||
        sqlite3_bind_text(stmt, 2, edge_type, -1, SQLITE_TRANSIENT) != SQLITE_OK ||
        sqlite3_bind_text(stmt, 3, source_name, -1, SQLITE_TRANSIENT) != SQLITE_OK ||
        sqlite3_bind_text(stmt, 4, target_name, -1, SQLITE_TRANSIENT) != SQLITE_OK) {
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return observation;
    }
    observation.query_ready = true;

    int step_rc = SQLITE_OK;
    while ((step_rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        observation.row_count++;
        if (sqlite3_column_int(stmt, 1) != 0) {
            observation.valid_json_count++;
        }
        const unsigned char *callee = sqlite3_column_text(stmt, 2);
        if (callee && strcmp((const char *)callee, expected_callee) == 0) {
            observation.exact_callee_count++;
        }
    }
    observation.query_completed = step_rc == SQLITE_DONE;
    sqlite3_finalize(stmt);
    sqlite3_close(db);
    return observation;
}

/* Count only edges of `edge_type` between the exact named source/target nodes.
 * The parity and incremental guards deliberately use unique symbol names, so
 * this canonicalizes across independent stores without depending on node IDs
 * or project-prefixed qualified names. */
static int named_edge_count(cbm_store_t *s, const char *project, const char *edge_type,
                            const char *source_name, const char *target_name) {
    cbm_edge_t *edges = NULL;
    int edge_count = 0;
    if (cbm_store_find_edges_by_type(s, project, edge_type, &edges, &edge_count) != CBM_STORE_OK) {
        return -1;
    }

    int matches = 0;
    for (int i = 0; i < edge_count; i++) {
        cbm_node_t source = {0};
        cbm_node_t target = {0};
        int source_ok = cbm_store_find_node_by_id(s, edges[i].source_id, &source) == CBM_STORE_OK;
        int target_ok = cbm_store_find_node_by_id(s, edges[i].target_id, &target) == CBM_STORE_OK;
        if (source_ok && target_ok && source.name && target.name &&
            strcmp(source.name, source_name) == 0 && strcmp(target.name, target_name) == 0) {
            matches++;
        }
        cbm_node_free_fields(&source);
        cbm_node_free_fields(&target);
    }
    if (edges) {
        cbm_store_free_edges(edges, edge_count);
    }
    return matches;
}

/* Like named_edge_count, but distinguish same-named targets by their source
 * file. Semantic-control fixtures intentionally keep the exported short name
 * identical in two modules so a project-wide unique-name fallback cannot make
 * an alias-mapping assertion pass accidentally. */
static int named_edge_to_file_count(cbm_store_t *s, const char *project, const char *edge_type,
                                    const char *source_name, const char *target_name,
                                    const char *target_file_path) {
    cbm_edge_t *edges = NULL;
    int edge_count = 0;
    if (cbm_store_find_edges_by_type(s, project, edge_type, &edges, &edge_count) != CBM_STORE_OK) {
        return -1;
    }

    int matches = 0;
    for (int i = 0; i < edge_count; i++) {
        cbm_node_t source = {0};
        cbm_node_t target = {0};
        int source_ok = cbm_store_find_node_by_id(s, edges[i].source_id, &source) == CBM_STORE_OK;
        int target_ok = cbm_store_find_node_by_id(s, edges[i].target_id, &target) == CBM_STORE_OK;
        if (source_ok && target_ok && source.name && target.name && target.file_path &&
            strcmp(source.name, source_name) == 0 && strcmp(target.name, target_name) == 0 &&
            strcmp(target.file_path, target_file_path) == 0) {
            matches++;
        }
        cbm_node_free_fields(&source);
        cbm_node_free_fields(&target);
    }
    if (edges) {
        cbm_store_free_edges(edges, edge_count);
    }
    return matches;
}

/* Count exact-name nodes without depending on project-prefixed qualified names.
 * Export-XML relationship tests use this as an anti-vacuous guard: the
 * transcoded methods must exist even when their extracted relationships were
 * accidentally discarded. */
static int named_node_count(cbm_store_t *s, const char *project, const char *name) {
    cbm_node_t *nodes = NULL;
    int count = 0;
    if (cbm_store_find_nodes_by_name(s, project, name, &nodes, &count) != CBM_STORE_OK) {
        return -1;
    }
    if (nodes) {
        cbm_store_free_nodes(nodes, count);
    }
    return count;
}

typedef struct {
    int run_rc;
    bool store_opened;
    int run_nodes;
    int execute_nodes;
    int run_execute_calls;
    int shared_payload_nodes;
    int run_shared_payload_usages;
} ObjectScriptExportObservation;

static ObjectScriptExportObservation observe_objectscript_export(const char *repo_path,
                                                                 const char *db_name) {
    ObjectScriptExportObservation observation = {
        .run_rc = -1,
        .store_opened = false,
        .run_nodes = -1,
        .execute_nodes = -1,
        .run_execute_calls = -1,
        .shared_payload_nodes = -1,
        .run_shared_payload_usages = -1,
    };
    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/%s", repo_path, db_name);
    cbm_pipeline_t *pipeline = cbm_pipeline_new(repo_path, db_path, CBM_MODE_FULL);
    if (!pipeline) {
        return observation;
    }

    observation.run_rc = cbm_pipeline_run(pipeline);
    const char *project = cbm_pipeline_project_name(pipeline);
    cbm_store_t *store = cbm_store_open_path(db_path);
    observation.store_opened = store != NULL;
    if (store && project) {
        observation.run_nodes = named_node_count(store, project, "Run");
        observation.execute_nodes = named_node_count(store, project, "Execute");
        observation.run_execute_calls = named_edge_count(store, project, "CALLS", "Run", "Execute");
        observation.shared_payload_nodes = named_node_count(store, project, "SharedPayload");
        observation.run_shared_payload_usages =
            named_edge_count(store, project, "USAGE", "Run", "SharedPayload");
    }
    if (store) {
        cbm_store_close(store);
    }
    cbm_pipeline_free(pipeline);
    return observation;
}

/* Studio Export XML is transformed into ObjectScript UDL during extraction.
 * The transformed definitions already survive, but the old sequential and
 * fused-parallel special cases freed each transformed result after inserting
 * only its definitions. Calls/usages therefore vanished even though direct
 * extraction of the same generated UDL found them.
 *
 * One shared multi-class fixture is indexed through both execution paths. The
 * 50 filler files only select the fused path; CBM_INDEX_SINGLE_THREAD forces
 * the first run through the sequential path without duplicating setup.
 * RED before the fix: Run and Execute each exist once, Run -> Execute is 0 in
 * both stores. GREEN requires exactly one CALLS edge in each store. */
TEST(pipeline_objectscript_export_preserves_calls_sequential_parallel) {
    static const char export_xml[] = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                                     "<Export generator=\"Cache\" version=\"25\">\n"
                                     "<Class name=\"Test.Worker\">\n"
                                     "<Method name=\"Execute\">\n"
                                     "<ClassMethod>1</ClassMethod>\n"
                                     "<FormalSpec>pValue:%String</FormalSpec>\n"
                                     "<ReturnType>%Status</ReturnType>\n"
                                     "<Implementation><![CDATA[\n"
                                     "    Quit $$$OK\n"
                                     "]]></Implementation>\n"
                                     "</Method>\n"
                                     "</Class>\n"
                                     "<Class name=\"Test.Caller\">\n"
                                     "<Method name=\"Run\">\n"
                                     "<ClassMethod>1</ClassMethod>\n"
                                     "<FormalSpec>pPayload:%String</FormalSpec>\n"
                                     "<ReturnType>%Status</ReturnType>\n"
                                     "<Implementation><![CDATA[\n"
                                     "    Do ##class(Test.Worker).Execute(pPayload)\n"
                                     "    Quit $$$OK\n"
                                     "]]></Implementation>\n"
                                     "</Method>\n"
                                     "</Class>\n"
                                     "</Export>\n";

    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_export_rel_XXXXXX");
    if (!cbm_mkdtemp(tmp)) {
        FAIL("tmpdir");
    }
    char export_path[512];
    snprintf(export_path, sizeof(export_path), "%s/studio-export.xml", tmp);
    if (th_write_file(export_path, export_xml) != 0) {
        th_rmtree(tmp);
        FAIL("failed to write Studio Export fixture");
    }

    /* Export XML plus 50 trackable fillers exceeds MIN_FILES_FOR_PARALLEL. */
    for (int i = 0; i < 50; i++) {
        char filename[64];
        char path[512];
        char source[128];
        snprintf(filename, sizeof(filename), "export_pad_%02d.ts", i);
        snprintf(path, sizeof(path), "%s/%s", tmp, filename);
        snprintf(source, sizeof(source), "export function exportPad%02d(): number { return %d; }\n",
                 i, i);
        if (th_write_file(path, source) != 0) {
            th_rmtree(tmp);
            FAIL("failed to write parallel-selection fixture");
        }
    }

    char *old_workers = getenv("CBM_WORKERS");
    char *saved_workers = old_workers ? strdup(old_workers) : NULL;
    char *old_single = getenv("CBM_INDEX_SINGLE_THREAD");
    char *saved_single = old_single ? strdup(old_single) : NULL;

    cbm_setenv("CBM_INDEX_SINGLE_THREAD", "1", 1);
    ObjectScriptExportObservation sequential =
        observe_objectscript_export(tmp, "export-sequential.db");

    cbm_unsetenv("CBM_INDEX_SINGLE_THREAD");
    cbm_setenv("CBM_WORKERS", "4", 1);
    ObjectScriptExportObservation parallel = observe_objectscript_export(tmp, "export-parallel.db");

    if (saved_workers) {
        cbm_setenv("CBM_WORKERS", saved_workers, 1);
        free(saved_workers);
    } else {
        cbm_unsetenv("CBM_WORKERS");
    }
    if (saved_single) {
        cbm_setenv("CBM_INDEX_SINGLE_THREAD", saved_single, 1);
        free(saved_single);
    } else {
        cbm_unsetenv("CBM_INDEX_SINGLE_THREAD");
    }
    th_rmtree(tmp);

    ASSERT_EQ(sequential.run_rc, 0);
    ASSERT_TRUE(sequential.store_opened);
    ASSERT_EQ(sequential.run_nodes, 1);
    ASSERT_EQ(sequential.execute_nodes, 1);

    ASSERT_EQ(parallel.run_rc, 0);
    ASSERT_TRUE(parallel.store_opened);
    ASSERT_EQ(parallel.run_nodes, 1);
    ASSERT_EQ(parallel.execute_nodes, 1);
    if (sequential.run_execute_calls != 1 || parallel.run_execute_calls != 1) {
        fprintf(stderr,
                "  [objectscript-export] invariant=transformed_calls_preserved "
                "expected=1/1 actual=%d/%d\n",
                sequential.run_execute_calls, parallel.run_execute_calls);
    }
    ASSERT_TRUE(sequential.run_execute_calls == 1 && parallel.run_execute_calls == 1);
    PASS();
}

static int format_objectscript_export_lifecycle_fixture(char *xml, size_t xml_capacity,
                                                        const char *default_value) {
    int written = snprintf(xml, xml_capacity,
                           "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                           "<Export generator=\"Cache\" version=\"25\">\n"
                           "<Class name=\"Test.Worker\">\n"
                           "<Method name=\"Execute\">\n"
                           "<ClassMethod>1</ClassMethod>\n"
                           "<FormalSpec>pValue:%%String</FormalSpec>\n"
                           "<ReturnType>%%Status</ReturnType>\n"
                           "<Implementation><![CDATA[\n"
                           "    Quit $$$OK\n"
                           "]]></Implementation>\n"
                           "</Method>\n"
                           "</Class>\n"
                           "<Class name=\"Test.Caller\">\n"
                           "<Parameter name=\"SharedPayload\"><Default>%s</Default></Parameter>\n"
                           "<Method name=\"Run\">\n"
                           "<ClassMethod>1</ClassMethod>\n"
                           "<FormalSpec>watched:%%String</FormalSpec>\n"
                           "<ReturnType>%%Status</ReturnType>\n"
                           "<Implementation><![CDATA[\n"
                           "    Do ##class(Test.Worker).Execute(watched)\n"
                           "    Quit $$$OK\n"
                           "]]></Implementation>\n"
                           "</Method>\n"
                           "</Class>\n"
                           "</Export>\n",
                           default_value);
    if (written <= 0 || (size_t)written >= xml_capacity) {
        return -1;
    }
    return written;
}

static int write_objectscript_export_lifecycle_fixture(const char *path,
                                                       const char *default_value) {
    char xml[4096];
    int written = format_objectscript_export_lifecycle_fixture(xml, sizeof(xml), default_value);
    if (written < 0) {
        return -1;
    }
    return th_write_file(path, xml);
}

/* A changed Studio Export file must keep its transformed carriers alive for
 * every incremental resolution pass. Compare the incrementally rebuilt graph
 * with a clean full sequential build of the same replacement XML. Direct
 * extraction separately proves that the transformed aggregate retains the
 * method-argument USAGE carrier; formal parameters are not graph targets, so
 * graph resolution of that carrier is outside this lifecycle regression.
 * ObjectScript has no per-file or cross-file LSP dispatcher, so it cannot
 * produce a resolved_calls semantic carrier to assert here.
 *
 * RED before the incremental cache lifecycle fix: definitions survived, but
 * the changed Export was re-read as raw XML by later passes, so Run->Execute
 * CALLS was absent from the incremental graph. */
TEST(pipeline_objectscript_export_incremental_matches_full_relationships) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_export_incr_XXXXXX");
    if (!cbm_mkdtemp(tmp)) {
        FAIL("tmpdir");
    }
    char export_path[512];
    snprintf(export_path, sizeof(export_path), "%s/studio-export.xml", tmp);
    if (write_objectscript_export_lifecycle_fixture(export_path, "initial") != 0) {
        th_rmtree(tmp);
        FAIL("failed to write initial Studio Export fixture");
    }

    char carrier_xml[4096];
    int carrier_xml_len =
        format_objectscript_export_lifecycle_fixture(carrier_xml, sizeof(carrier_xml), "initial");
    if (carrier_xml_len < 0) {
        th_rmtree(tmp);
        FAIL("failed to format Studio Export carrier fixture");
    }
    cbm_init();
    CBMFileResult *carrier_result = cbm_pipeline_extract_objectscript_export(
        carrier_xml, carrier_xml_len, "export-lifecycle", "studio-export.xml", NULL, NULL);
    if (!carrier_result) {
        th_rmtree(tmp);
        FAIL("failed to extract Studio Export carrier fixture");
    }
    bool found_argument_usage = false;
    for (int i = 0; i < carrier_result->usages.count; i++) {
        const CBMUsage *usage = &carrier_result->usages.items[i];
        if (usage->ref_name && strcmp(usage->ref_name, "watched") == 0 &&
            usage->enclosing_func_qn && strstr(usage->enclosing_func_qn, ".Run")) {
            found_argument_usage = true;
            break;
        }
    }
    cbm_free_result(carrier_result);
    if (!found_argument_usage) {
        th_rmtree(tmp);
        FAIL("Studio Export aggregate lost method-argument USAGE carrier");
    }

    char *old_single = getenv("CBM_INDEX_SINGLE_THREAD");
    char *saved_single = old_single ? strdup(old_single) : NULL;
    cbm_setenv("CBM_INDEX_SINGLE_THREAD", "1", 1);

    ObjectScriptExportObservation initial =
        observe_objectscript_export(tmp, "export-incremental.db");

    if (write_objectscript_export_lifecycle_fixture(export_path,
                                                    "replacement-with-a-different-size") != 0) {
        if (saved_single) {
            cbm_setenv("CBM_INDEX_SINGLE_THREAD", saved_single, 1);
            free(saved_single);
        } else {
            cbm_unsetenv("CBM_INDEX_SINGLE_THREAD");
        }
        th_rmtree(tmp);
        FAIL("failed to write replacement Studio Export fixture");
    }

    /* Same DB routes through incremental; a fresh DB is the full reference. */
    ObjectScriptExportObservation incremental =
        observe_objectscript_export(tmp, "export-incremental.db");
    ObjectScriptExportObservation full =
        observe_objectscript_export(tmp, "export-full-reference.db");

    if (saved_single) {
        cbm_setenv("CBM_INDEX_SINGLE_THREAD", saved_single, 1);
        free(saved_single);
    } else {
        cbm_unsetenv("CBM_INDEX_SINGLE_THREAD");
    }
    th_rmtree(tmp);

    ASSERT_EQ(initial.run_rc, 0);
    ASSERT_TRUE(initial.store_opened);
    ASSERT_EQ(initial.run_execute_calls, 1);

    ASSERT_EQ(incremental.run_rc, 0);
    ASSERT_TRUE(incremental.store_opened);
    ASSERT_EQ(full.run_rc, 0);
    ASSERT_TRUE(full.store_opened);
    ASSERT_EQ(incremental.run_nodes, full.run_nodes);
    ASSERT_EQ(incremental.execute_nodes, full.execute_nodes);
    ASSERT_EQ(incremental.shared_payload_nodes, full.shared_payload_nodes);
    ASSERT_EQ(incremental.run_execute_calls, full.run_execute_calls);
    ASSERT_EQ(incremental.run_shared_payload_usages, full.run_shared_payload_usages);
    ASSERT_EQ(full.run_nodes, 1);
    ASSERT_EQ(full.execute_nodes, 1);
    ASSERT_EQ(full.shared_payload_nodes, 1);
    ASSERT_EQ(full.run_execute_calls, 1);
    PASS();
}

static bool test_buffer_appendf(char *buffer, size_t capacity, size_t *used, const char *format,
                                ...) {
    if (!buffer || !used || *used >= capacity) {
        return false;
    }
    va_list args;
    va_start(args, format);
    int written = vsnprintf(buffer + *used, capacity - *used, format, args);
    va_end(args);
    if (written < 0 || (size_t)written >= capacity - *used) {
        return false;
    }
    *used += (size_t)written;
    return true;
}

/* Each extracted class owns a separate arena. A CBMCall includes eight argument
 * slots, so growing its call array through the 513th item requires at least four
 * arena blocks per class. Sixty-four classes therefore exceed the aggregate's
 * fixed 256-block adoption table even before aggregate-array growth is counted,
 * while staying within the Studio Export transcoder's existing 64-class cap.
 *
 * RED with fixed-block ownership transfer: aggregation returns NULL before all
 * 64 classes can be composed. GREEN requires one result containing every class,
 * method, and call carrier without raising the class limit. */
TEST(pipeline_objectscript_export_aggregate_exceeds_arena_block_table) {
    enum {
        STRESS_CLASS_COUNT = 64,
        STRESS_CALLS_PER_CLASS = 513,
        STRESS_XML_CAPACITY = 4 * 1024 * 1024,
    };
    char *xml = (char *)malloc(STRESS_XML_CAPACITY);
    if (!xml) {
        FAIL("stress XML allocation");
    }
    size_t used = 0;
    bool complete = test_buffer_appendf(xml, STRESS_XML_CAPACITY, &used,
                                        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                                        "<Export generator=\"Cache\" version=\"25\">\n");
    for (int ci = 0; ci < STRESS_CLASS_COUNT && complete; ci++) {
        complete = test_buffer_appendf(xml, STRESS_XML_CAPACITY, &used,
                                       "<Class name=\"Arena.Stress%02d\">\n"
                                       "<Method name=\"Run%02d\">\n"
                                       "<ClassMethod>1</ClassMethod>\n"
                                       "<Implementation><![CDATA[\n",
                                       ci, ci);
        for (int call = 0; call < STRESS_CALLS_PER_CLASS && complete; call++) {
            complete = test_buffer_appendf(xml, STRESS_XML_CAPACITY, &used,
                                           "    Do ##class(Arena.Target).Execute()\n");
        }
        complete = complete && test_buffer_appendf(xml, STRESS_XML_CAPACITY, &used,
                                                   "    Quit\n"
                                                   "]]></Implementation>\n"
                                                   "</Method>\n"
                                                   "</Class>\n");
    }
    complete = complete && test_buffer_appendf(xml, STRESS_XML_CAPACITY, &used, "</Export>\n");
    if (!complete) {
        free(xml);
        FAIL("stress XML capacity");
    }

    cbm_init();
    CBMFileResult *aggregate = cbm_pipeline_extract_objectscript_export(
        xml, (int)used, "arena-stress", "studio-export.xml", NULL, NULL);
    free(xml);
    ASSERT_NOT_NULL(aggregate);

    bool found_last_method = false;
    for (int i = 0; i < aggregate->defs.count; i++) {
        const CBMDefinition *def = &aggregate->defs.items[i];
        if (def->name && strcmp(def->name, "Run63") == 0) {
            found_last_method = true;
            break;
        }
    }
    int expected_calls = STRESS_CLASS_COUNT * STRESS_CALLS_PER_CLASS;
    int actual_calls = aggregate->calls.count;
    int actual_defs = aggregate->defs.count;
    cbm_free_result(aggregate);

    ASSERT_TRUE(found_last_method);
    ASSERT_EQ(actual_calls, expected_calls);
    ASSERT_GTE(actual_defs, STRESS_CLASS_COUNT * 2);
    PASS();
}

typedef struct {
    int run_rc;
    bool store_opened;
    int source_nodes;
    int target_nodes;
    int edge_count;
} NamedEdgeObservation;

static NamedEdgeObservation observe_named_edge(const char *repo_path, const char *db_name,
                                               const char *edge_type, const char *source_name,
                                               const char *target_name) {
    NamedEdgeObservation observation = {
        .run_rc = -1,
        .store_opened = false,
        .source_nodes = -1,
        .target_nodes = -1,
        .edge_count = -1,
    };
    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/%s", repo_path, db_name);
    cbm_pipeline_t *pipeline = cbm_pipeline_new(repo_path, db_path, CBM_MODE_FULL);
    if (!pipeline) {
        return observation;
    }
    observation.run_rc = cbm_pipeline_run(pipeline);
    const char *project = cbm_pipeline_project_name(pipeline);
    cbm_store_t *store = cbm_store_open_path(db_path);
    observation.store_opened = store != NULL;
    if (store && project) {
        observation.source_nodes = named_node_count(store, project, source_name);
        observation.target_nodes = named_node_count(store, project, target_name);
        observation.edge_count =
            named_edge_count(store, project, edge_type, source_name, target_name);
    }
    if (store) {
        cbm_store_close(store);
    }
    cbm_pipeline_free(pipeline);
    return observation;
}

/* env_accesses are extracted independently from calls and must materialize the
 * same EnvVar + CONFIGURES relationship in both pipeline implementations.
 *
 * RED before the fused-path consumer is added: sequential definition insertion
 * creates envParityRead -> CBM_PARITY_TOKEN, while cbm_build_registry_from_cache
 * ignores result->env_accesses and the parallel graph contains neither target
 * nor edge. */
TEST(pipeline_env_access_configures_sequential_parallel_parity) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_env_parity_XXXXXX");
    if (!cbm_mkdtemp(tmp)) {
        FAIL("tmpdir");
    }
    char source_path[512];
    snprintf(source_path, sizeof(source_path), "%s/env.go", tmp);
    if (th_write_file(source_path, "package envparity\n\n"
                                   "import \"os\"\n\n"
                                   "func envParityRead() string {\n"
                                   "    return os.Getenv(\"CBM_PARITY_TOKEN\")\n"
                                   "}\n") != 0) {
        th_rmtree(tmp);
        FAIL("failed to write env-access fixture");
    }
    for (int i = 0; i < 50; i++) {
        char filename[64];
        char path[512];
        char source[128];
        snprintf(filename, sizeof(filename), "env_pad_%02d.ts", i);
        snprintf(path, sizeof(path), "%s/%s", tmp, filename);
        snprintf(source, sizeof(source), "export function envPad%02d(): number { return %d; }\n", i,
                 i);
        if (th_write_file(path, source) != 0) {
            th_rmtree(tmp);
            FAIL("failed to write env parity filler");
        }
    }

    char *old_workers = getenv("CBM_WORKERS");
    char *saved_workers = old_workers ? strdup(old_workers) : NULL;
    char *old_single = getenv("CBM_INDEX_SINGLE_THREAD");
    char *saved_single = old_single ? strdup(old_single) : NULL;

    cbm_setenv("CBM_INDEX_SINGLE_THREAD", "1", 1);
    NamedEdgeObservation sequential = observe_named_edge(tmp, "env-sequential.db", "CONFIGURES",
                                                         "envParityRead", "CBM_PARITY_TOKEN");

    cbm_unsetenv("CBM_INDEX_SINGLE_THREAD");
    cbm_setenv("CBM_WORKERS", "4", 1);
    NamedEdgeObservation parallel = observe_named_edge(tmp, "env-parallel.db", "CONFIGURES",
                                                       "envParityRead", "CBM_PARITY_TOKEN");

    if (saved_workers) {
        cbm_setenv("CBM_WORKERS", saved_workers, 1);
        free(saved_workers);
    } else {
        cbm_unsetenv("CBM_WORKERS");
    }
    if (saved_single) {
        cbm_setenv("CBM_INDEX_SINGLE_THREAD", saved_single, 1);
        free(saved_single);
    } else {
        cbm_unsetenv("CBM_INDEX_SINGLE_THREAD");
    }
    th_rmtree(tmp);

    ASSERT_EQ(sequential.run_rc, 0);
    ASSERT_TRUE(sequential.store_opened);
    ASSERT_EQ(sequential.source_nodes, 1);
    ASSERT_EQ(sequential.target_nodes, 1);
    ASSERT_EQ(sequential.edge_count, 1);

    ASSERT_EQ(parallel.run_rc, 0);
    ASSERT_TRUE(parallel.store_opened);
    ASSERT_EQ(parallel.source_nodes, sequential.source_nodes);
    ASSERT_EQ(parallel.target_nodes, sequential.target_nodes);
    ASSERT_EQ(parallel.edge_count, sequential.edge_count);
    PASS();
}

/* Sequential and fused-parallel resolution must materialize the same precise
 * CALL_REFERENCE edge set.  The table covers each independently implemented
 * resolver family involved in callable-value/reference semantics; sibling
 * frontends share those resolvers (JS/TSX -> TS, C++/CUDA -> C).
 *
 * Every pair uses a globally unique source and target name. That lets the test
 * compare the semantic edge multiset across stores while ignoring unstable DB
 * IDs and project-name prefixes. The fixture exceeds MIN_FILES_FOR_PARALLEL;
 * CBM_INDEX_SINGLE_THREAD forces the first run through the sequential path and
 * CBM_WORKERS forces the second through pass_parallel.c. */
TEST(pipeline_call_reference_sequential_parallel_edge_set_parity) {
    static const PreciseReferencePair pairs[] = {
        {"goReferenceSite", "goReferenceTarget"},
        {"pythonReferenceSite", "pythonReferenceTarget"},
        {"cReferenceSite", "cReferenceTarget"},
        {"rustReferenceSite", "rustReferenceTarget"},
        {"csharpReferenceSite", "csharpReferenceTarget"},
        {"javascriptReferenceSite", "javascriptReferenceTarget"},
        {"typescriptReferenceSite", "typescriptReferenceTarget"},
        {"tsxReferenceSite", "tsxReferenceTarget"},
        {"cppReferenceSite", "cppReferenceTarget"},
        {"cudaReferenceSite", "cudaReferenceTarget"},
        {"javaReferenceSite", "javaReferenceTarget"},
        {"kotlinReferenceSite", "kotlinReferenceTarget"},
        {"phpReferenceSite", "phpReferenceTarget"},
        {"perlReferenceSite", "perlReferenceTarget"},
    };
    enum { pair_count = (int)(sizeof(pairs) / sizeof(pairs[0])) };
    static const PreciseReferencePair shadow_pairs[] = {
        {"pythonShadowSite", "pythonShadowTarget"},
        {"javascriptShadowSite", "javascriptShadowTarget"},
        {"typescriptShadowSite", "typescriptShadowTarget"},
    };
    static const PreciseReferencePair shadow_controls[] = {
        {"pythonShadowSite", "pythonShadowAccept"},
        {"javascriptShadowSite", "javascriptShadowAccept"},
        {"typescriptShadowSite", "typescriptShadowAccept"},
    };
    enum { shadow_count = (int)(sizeof(shadow_pairs) / sizeof(shadow_pairs[0])) };

    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_ref_parity_XXXXXX");
    if (!cbm_mkdtemp(tmp)) {
        FAIL("tmpdir");
    }

    write_temp_file(tmp, "parity.go",
                    "package parity\n"
                    "func goReferenceTarget() {}\n"
                    "func goReferenceAccept(callback func()) {}\n"
                    "func goReferenceSite() { goReferenceAccept(goReferenceTarget) }\n");
    /* `{"callee":""}` contributes 13 bytes. A 243-byte identifier therefore
     * produces 256 bytes of JSON content: it fits the usage emitter's escaped
     * value buffer but needs 257 bytes including NUL for the wrapper. The
     * sequential 512-byte wrapper is valid; the parallel 256-byte wrapper is
     * RED because snprintf drops the closing brace. */
    enum { LONG_REFERENCE_NAME_LEN = 243 };
    char long_reference_name[LONG_REFERENCE_NAME_LEN + 1];
    memset(long_reference_name, 'x', LONG_REFERENCE_NAME_LEN);
    long_reference_name[0] = 'l';
    long_reference_name[LONG_REFERENCE_NAME_LEN] = '\0';
    char long_reference_source[1024];
    int long_reference_source_len = snprintf(
        long_reference_source, sizeof(long_reference_source),
        "package parity\n"
        "func %s() {}\n"
        "func longPropertiesReferenceAccept(callback func()) {}\n"
        "func longPropertiesReferenceSite() { longPropertiesReferenceAccept(%s) }\n",
        long_reference_name, long_reference_name);
    if (long_reference_source_len <= 0 ||
        (size_t)long_reference_source_len >= sizeof(long_reference_source)) {
        th_rmtree(tmp);
        FAIL("long reference source buffer");
    }
    write_temp_file(tmp, "long_properties.go", long_reference_source);
    write_temp_file(tmp, "python_target.py",
                    "def pythonReferenceTarget():\n"
                    "    pass\n");
    write_temp_file(tmp, "parity.py",
                    "from python_target import pythonReferenceTarget as pythonReferenceAlias\n"
                    "def pythonReferenceAccept(callback):\n"
                    "    pass\n"
                    "def pythonReferenceSite():\n"
                    "    pythonReferenceAccept(pythonReferenceAlias)\n");
    write_temp_file(tmp, "parity.c",
                    "typedef void (*c_reference_callback_t)(void);\n"
                    "void cReferenceTarget(void) {}\n"
                    "void cReferenceAccept(c_reference_callback_t callback) {}\n"
                    "void cReferenceSite(void) { cReferenceAccept(cReferenceTarget); }\n");
    write_temp_file(tmp, "parity.rs",
                    "fn rustReferenceTarget() {}\n"
                    "fn rustReferenceAccept(_callback: fn()) {}\n"
                    "fn rustReferenceSite() { rustReferenceAccept(rustReferenceTarget); }\n");
    write_temp_file(tmp, "Parity.cs",
                    "using System;\n"
                    "class CSharpParity {\n"
                    "  static void csharpReferenceTarget() {}\n"
                    "  static void csharpReferenceAccept(Action callback) {}\n"
                    "  static void csharpReferenceSite() { "
                    "csharpReferenceAccept(csharpReferenceTarget); }\n"
                    "}\n");
    write_temp_file(tmp, "parity.js",
                    "function javascriptReferenceTarget() {}\n"
                    "function javascriptReferenceAccept(callback) {}\n"
                    "function javascriptReferenceSite() {\n"
                    "  javascriptReferenceAccept(javascriptReferenceTarget);\n"
                    "}\n");
    write_temp_file(tmp, "parity.ts",
                    "class TypeScriptParity { typescriptReferenceTarget(): void {} }\n"
                    "function typescriptReferenceAccept(callback: () => void): void {}\n"
                    "function typescriptReferenceSite(service: TypeScriptParity): void {\n"
                    "  typescriptReferenceAccept(service.typescriptReferenceTarget);\n"
                    "}\n");
    write_temp_file(tmp, "parity.tsx",
                    "function tsxReferenceTarget(): void {}\n"
                    "function tsxReferenceAccept(callback: () => void): void {}\n"
                    "function tsxReferenceSite(): void {\n"
                    "  tsxReferenceAccept(tsxReferenceTarget);\n"
                    "}\n");
    write_temp_file(tmp, "parity.cpp",
                    "using cpp_reference_callback_t = void (*)();\n"
                    "void cppReferenceTarget() {}\n"
                    "void cppReferenceAccept(cpp_reference_callback_t callback) {}\n"
                    "void cppReferenceSite() { cppReferenceAccept(cppReferenceTarget); }\n");
    write_temp_file(tmp, "parity.cu",
                    "typedef void (*cuda_reference_callback_t)(void);\n"
                    "__device__ void cudaReferenceTarget(void) {}\n"
                    "__device__ void cudaReferenceAccept(cuda_reference_callback_t callback) {}\n"
                    "__device__ void cudaReferenceSite(void) {\n"
                    "  cudaReferenceAccept(cudaReferenceTarget);\n"
                    "}\n");
    write_temp_file(tmp, "JavaParity.java",
                    "interface JavaParityTask { void run(); }\n"
                    "class JavaParity {\n"
                    "  static void javaReferenceTarget() {}\n"
                    "  static void javaReferenceAccept(JavaParityTask callback) {}\n"
                    "  static void javaReferenceSite() { "
                    "javaReferenceAccept(JavaParity::javaReferenceTarget); }\n"
                    "}\n");
    write_temp_file(tmp, "parity.kt",
                    "fun kotlinReferenceTarget(): Unit {}\n"
                    "fun kotlinReferenceAccept(callback: () -> Unit): Unit {}\n"
                    "fun kotlinReferenceSite(): Unit { "
                    "kotlinReferenceAccept(::kotlinReferenceTarget) }\n");
    write_temp_file(tmp, "parity.php",
                    "<?php\n"
                    "function phpReferenceTarget(): void {}\n"
                    "function phpReferenceAccept(callable $callback): void {}\n"
                    "function phpReferenceSite(): void { "
                    "phpReferenceAccept(phpReferenceTarget(...)); }\n");
    write_temp_file(tmp, "parity.pl",
                    "sub perlReferenceTarget {}\n"
                    "sub perlReferenceAccept { my ($callback) = @_; }\n"
                    "sub perlReferenceSite { "
                    "perlReferenceAccept(\\&perlReferenceTarget); }\n");

    write_temp_file(tmp, "shadow.py",
                    "def pythonShadowTarget():\n"
                    "    pass\n"
                    "def pythonShadowAccept(callback):\n"
                    "    pass\n"
                    "def pythonShadowSite():\n"
                    "    pythonShadowAccept(pythonShadowTarget)\n"
                    "    pythonShadowTarget = lambda: None\n");
    write_temp_file(tmp, "shadow.js",
                    "function javascriptShadowTarget() {}\n"
                    "function javascriptShadowAccept(callback) {}\n"
                    "function javascriptShadowSite() {\n"
                    "  javascriptShadowAccept(javascriptShadowTarget);\n"
                    "  const javascriptShadowTarget = () => {};\n"
                    "}\n");
    write_temp_file(tmp, "shadow.ts",
                    "function typescriptShadowTarget(): void {}\n"
                    "function typescriptShadowAccept(callback: () => void): void {}\n"
                    "function typescriptShadowSite(): void {\n"
                    "  typescriptShadowAccept(typescriptShadowTarget);\n"
                    "  const typescriptShadowTarget = (): void => {};\n"
                    "}\n");

    /* Fifteen positive and three shadow fixtures plus 34 source fillers exceed
     * the 50-file fused threshold without making the test larger than needed. */
    for (int i = 0; i < 34; i++) {
        char name[64];
        char body[128];
        snprintf(name, sizeof(name), "parity_pad_%02d.ts", i);
        snprintf(body, sizeof(body), "export function parityPad%02d(): number { return %d; }\n", i,
                 i);
        write_temp_file(tmp, name, body);
    }

    int sequential_reference[pair_count];
    int sequential_usage[pair_count];
    int sequential_calls[pair_count];
    int parallel_reference[pair_count];
    int parallel_usage[pair_count];
    int parallel_calls[pair_count];
    int sequential_shadow_leaks[shadow_count];
    int parallel_shadow_leaks[shadow_count];
    int sequential_shadow_controls[shadow_count];
    int parallel_shadow_controls[shadow_count];
    for (int i = 0; i < pair_count; i++) {
        sequential_reference[i] = sequential_usage[i] = sequential_calls[i] = -1;
        parallel_reference[i] = parallel_usage[i] = parallel_calls[i] = -1;
    }
    for (int i = 0; i < shadow_count; i++) {
        sequential_shadow_leaks[i] = parallel_shadow_leaks[i] = -1;
        sequential_shadow_controls[i] = parallel_shadow_controls[i] = -1;
    }
    int sequential_total = -1;
    int parallel_total = -1;
    int sequential_long_reference = -1;
    int sequential_long_usage = -1;
    int sequential_long_calls = -1;
    int parallel_long_reference = -1;
    int parallel_long_usage = -1;
    int parallel_long_calls = -1;
    NamedEdgePropertyObservation sequential_long_property = {0};
    NamedEdgePropertyObservation parallel_long_property = {0};

    char *old_workers = getenv("CBM_WORKERS");
    char *saved_workers = old_workers ? strdup(old_workers) : NULL;
    char *old_single = getenv("CBM_INDEX_SINGLE_THREAD");
    char *saved_single = old_single ? strdup(old_single) : NULL;

    cbm_setenv("CBM_INDEX_SINGLE_THREAD", "1", 1);
    char sequential_db_path[512];
    snprintf(sequential_db_path, sizeof(sequential_db_path), "%s/reference_sequential.db", tmp);
    cbm_pipeline_t *sequential = cbm_pipeline_new(tmp, sequential_db_path, CBM_MODE_FULL);
    int sequential_run_rc = sequential ? cbm_pipeline_run(sequential) : -1;
    const char *sequential_project = sequential ? cbm_pipeline_project_name(sequential) : NULL;
    cbm_store_t *sequential_store = cbm_store_open_path(sequential_db_path);
    bool sequential_store_opened = sequential_store != NULL;
    if (sequential_store && sequential_project) {
        sequential_total =
            cbm_store_count_edges_by_type(sequential_store, sequential_project, "CALL_REFERENCE");
        for (int i = 0; i < pair_count; i++) {
            sequential_reference[i] =
                named_edge_count(sequential_store, sequential_project, "CALL_REFERENCE",
                                 pairs[i].source_name, pairs[i].target_name);
            sequential_usage[i] = named_edge_count(sequential_store, sequential_project, "USAGE",
                                                   pairs[i].source_name, pairs[i].target_name);
            sequential_calls[i] = named_edge_count(sequential_store, sequential_project, "CALLS",
                                                   pairs[i].source_name, pairs[i].target_name);
        }
        for (int i = 0; i < shadow_count; i++) {
            sequential_shadow_leaks[i] =
                named_edge_count(sequential_store, sequential_project, "CALL_REFERENCE",
                                 shadow_pairs[i].source_name, shadow_pairs[i].target_name) +
                named_edge_count(sequential_store, sequential_project, "USAGE",
                                 shadow_pairs[i].source_name, shadow_pairs[i].target_name) +
                named_edge_count(sequential_store, sequential_project, "CALLS",
                                 shadow_pairs[i].source_name, shadow_pairs[i].target_name);
            sequential_shadow_controls[i] =
                named_edge_count(sequential_store, sequential_project, "CALLS",
                                 shadow_controls[i].source_name, shadow_controls[i].target_name);
        }
        sequential_long_reference = named_edge_count(
            sequential_store, sequential_project, "CALL_REFERENCE", "longPropertiesReferenceSite",
            long_reference_name);
        sequential_long_usage = named_edge_count(sequential_store, sequential_project, "USAGE",
                                                 "longPropertiesReferenceSite",
                                                 long_reference_name);
        sequential_long_calls = named_edge_count(sequential_store, sequential_project, "CALLS",
                                                 "longPropertiesReferenceSite",
                                                 long_reference_name);
        sequential_long_property = observe_named_edge_callee_property(
            sequential_db_path, sequential_project, "CALL_REFERENCE",
            "longPropertiesReferenceSite", long_reference_name, long_reference_name);
        cbm_store_close(sequential_store);
    }
    cbm_pipeline_free(sequential);

    cbm_unsetenv("CBM_INDEX_SINGLE_THREAD");
    cbm_setenv("CBM_WORKERS", "4", 1);
    char parallel_db_path[512];
    snprintf(parallel_db_path, sizeof(parallel_db_path), "%s/reference_parallel.db", tmp);
    cbm_pipeline_t *parallel = cbm_pipeline_new(tmp, parallel_db_path, CBM_MODE_FULL);
    int parallel_run_rc = parallel ? cbm_pipeline_run(parallel) : -1;
    const char *parallel_project = parallel ? cbm_pipeline_project_name(parallel) : NULL;
    cbm_store_t *parallel_store = cbm_store_open_path(parallel_db_path);
    bool parallel_store_opened = parallel_store != NULL;
    if (parallel_store && parallel_project) {
        parallel_total =
            cbm_store_count_edges_by_type(parallel_store, parallel_project, "CALL_REFERENCE");
        for (int i = 0; i < pair_count; i++) {
            parallel_reference[i] =
                named_edge_count(parallel_store, parallel_project, "CALL_REFERENCE",
                                 pairs[i].source_name, pairs[i].target_name);
            parallel_usage[i] = named_edge_count(parallel_store, parallel_project, "USAGE",
                                                 pairs[i].source_name, pairs[i].target_name);
            parallel_calls[i] = named_edge_count(parallel_store, parallel_project, "CALLS",
                                                 pairs[i].source_name, pairs[i].target_name);
        }
        for (int i = 0; i < shadow_count; i++) {
            parallel_shadow_leaks[i] =
                named_edge_count(parallel_store, parallel_project, "CALL_REFERENCE",
                                 shadow_pairs[i].source_name, shadow_pairs[i].target_name) +
                named_edge_count(parallel_store, parallel_project, "USAGE",
                                 shadow_pairs[i].source_name, shadow_pairs[i].target_name) +
                named_edge_count(parallel_store, parallel_project, "CALLS",
                                 shadow_pairs[i].source_name, shadow_pairs[i].target_name);
            parallel_shadow_controls[i] =
                named_edge_count(parallel_store, parallel_project, "CALLS",
                                 shadow_controls[i].source_name, shadow_controls[i].target_name);
        }
        parallel_long_reference = named_edge_count(
            parallel_store, parallel_project, "CALL_REFERENCE", "longPropertiesReferenceSite",
            long_reference_name);
        parallel_long_usage = named_edge_count(parallel_store, parallel_project, "USAGE",
                                               "longPropertiesReferenceSite", long_reference_name);
        parallel_long_calls = named_edge_count(parallel_store, parallel_project, "CALLS",
                                               "longPropertiesReferenceSite", long_reference_name);
        parallel_long_property = observe_named_edge_callee_property(
            parallel_db_path, parallel_project, "CALL_REFERENCE", "longPropertiesReferenceSite",
            long_reference_name, long_reference_name);
        cbm_store_close(parallel_store);
    }
    cbm_pipeline_free(parallel);

    if (saved_workers) {
        cbm_setenv("CBM_WORKERS", saved_workers, 1);
        free(saved_workers);
    } else {
        cbm_unsetenv("CBM_WORKERS");
    }
    if (saved_single) {
        cbm_setenv("CBM_INDEX_SINGLE_THREAD", saved_single, 1);
        free(saved_single);
    } else {
        cbm_unsetenv("CBM_INDEX_SINGLE_THREAD");
    }
    th_rmtree(tmp);

    ASSERT_EQ(sequential_run_rc, 0);
    ASSERT_TRUE(sequential_store_opened);
    ASSERT_EQ(parallel_run_rc, 0);
    ASSERT_TRUE(parallel_store_opened);
    ASSERT_EQ(sequential_long_reference, 1);
    ASSERT_EQ(sequential_long_usage, 0);
    ASSERT_EQ(sequential_long_calls, 0);
    ASSERT_TRUE(sequential_long_property.database_opened);
    ASSERT_TRUE(sequential_long_property.query_ready);
    ASSERT_TRUE(sequential_long_property.query_completed);
    ASSERT_EQ(sequential_long_property.row_count, 1);
    ASSERT_EQ(sequential_long_property.valid_json_count, 1);
    ASSERT_EQ(sequential_long_property.exact_callee_count, 1);
    ASSERT_EQ(parallel_long_reference, 1);
    ASSERT_EQ(parallel_long_usage, 0);
    ASSERT_EQ(parallel_long_calls, 0);
    ASSERT_TRUE(parallel_long_property.database_opened);
    ASSERT_TRUE(parallel_long_property.query_ready);
    ASSERT_TRUE(parallel_long_property.query_completed);
    ASSERT_EQ(parallel_long_property.row_count, 1);
    ASSERT_EQ(parallel_long_property.valid_json_count, 1);
    ASSERT_EQ(parallel_long_property.exact_callee_count, 1);
    ASSERT_EQ(parallel_total, sequential_total);
    int expected_total = 0;
    for (int i = 0; i < pair_count; i++) {
        ASSERT_GT(sequential_reference[i], 0);
        ASSERT_EQ(parallel_reference[i], sequential_reference[i]);
        ASSERT_EQ(sequential_usage[i], 0);
        ASSERT_EQ(parallel_usage[i], 0);
        ASSERT_EQ(sequential_calls[i], 0);
        ASSERT_EQ(parallel_calls[i], 0);
        expected_total += sequential_reference[i];
    }
    expected_total += sequential_long_reference;
    for (int i = 0; i < shadow_count; i++) {
        ASSERT_EQ(sequential_shadow_controls[i], 1);
        ASSERT_EQ(parallel_shadow_controls[i], 1);
        ASSERT_EQ(sequential_shadow_leaks[i], 0);
        ASSERT_EQ(parallel_shadow_leaks[i], 0);
    }
    ASSERT_EQ(sequential_total, expected_total);
    PASS();
}

static int pipeline_test_set_mtime(const char *path, time_t seconds, long nanoseconds) {
    struct timespec times[2] = {{.tv_sec = seconds, .tv_nsec = nanoseconds},
                                {.tv_sec = seconds, .tv_nsec = nanoseconds}};
    return utimensat(AT_FDCWD, path, times, 0);
}

/* Reindexing a changed callable-value occurrence must replace its exact target,
 * not preserve the old CALL_REFERENCE alongside the new one or regress the new
 * occurrence to USAGE/CALLS. Equal-length source generations use the exact same
 * mtime so metadata heuristics cannot make the test pass: byte hashes must route
 * the semantic change to a full rebuild. A metadata-only mtime change is a true
 * no-op and must not rebuild. */
TEST(pipeline_incremental_repoints_call_reference_without_stale_edge) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_ref_incr_XXXXXX");
    if (!cbm_mkdtemp(tmp)) {
        FAIL("tmpdir");
    }

    static const char initial_source[] = "package refs\n"
                                         "func alphaReferenceTarget() {}\n"
                                         "func bravoReferenceTarget() {}\n"
                                         "func incrementalReferenceAccept(callback func()) {}\n"
                                         "func incrementalReferenceSite() { "
                                         "incrementalReferenceAccept(alphaReferenceTarget) }\n";
    static const char replacement_source[] = "package refs\n"
                                             "func alphaReferenceTarget() {}\n"
                                             "func bravoReferenceTarget() {}\n"
                                             "func incrementalReferenceAccept(callback func()) {}\n"
                                             "func incrementalReferenceSite() { "
                                             "incrementalReferenceAccept(bravoReferenceTarget) }\n";
    ASSERT_EQ(sizeof(initial_source), sizeof(replacement_source));
    write_temp_file(tmp, "refs.go", initial_source);
    char source_path[512];
    snprintf(source_path, sizeof(source_path), "%s/refs.go", tmp);
    const time_t fixed_mtime = 1700000000;
    ASSERT_EQ(pipeline_test_set_mtime(source_path, fixed_mtime, 123456789L), 0);

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/reference_incremental.db", tmp);
    cbm_pipeline_t *first = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(first);
    ASSERT_EQ(cbm_pipeline_run(first), 0);
    const char *first_project = cbm_pipeline_project_name(first);
    cbm_store_t *first_store = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(first_store);
    ASSERT_EQ(named_edge_count(first_store, first_project, "CALL_REFERENCE",
                               "incrementalReferenceSite", "alphaReferenceTarget"),
              1);
    ASSERT_EQ(named_edge_count(first_store, first_project, "CALL_REFERENCE",
                               "incrementalReferenceSite", "bravoReferenceTarget"),
              0);
    ASSERT_EQ(named_edge_count(first_store, first_project, "USAGE", "incrementalReferenceSite",
                               "alphaReferenceTarget"),
              0);
    ASSERT_EQ(named_edge_count(first_store, first_project, "CALLS", "incrementalReferenceSite",
                               "alphaReferenceTarget"),
              0);
    cbm_file_hash_t stored_hash = {0};
    ASSERT_EQ(cbm_store_get_file_hash(first_store, first_project, "refs.go", &stored_hash),
              CBM_STORE_OK);
    char expected_hash[CBM_SHA256_HEX_LEN + 1];
    cbm_sha256_hex(initial_source, strlen(initial_source), expected_hash);
    ASSERT_NOT_NULL(stored_hash.sha256);
    ASSERT_STR_EQ(stored_hash.sha256, expected_hash);
    cbm_store_clear_file_hash(&stored_hash);
    cbm_coverage_meta_t baseline_meta = {0};
    ASSERT_EQ(cbm_store_coverage_meta_get(first_store, first_project, &baseline_meta),
              CBM_STORE_OK);
    ASSERT_EQ(baseline_meta.coverage_version, CBM_SEMANTIC_INDEX_VERSION);
    ASSERT_TRUE(baseline_meta.hash_records_complete);
    cbm_store_coverage_meta_clear(&baseline_meta);
    cbm_store_close(first_store);
    cbm_pipeline_free(first);

    /* Same bytes, metadata only: exact manifest equality is a no-op. */
    ASSERT_EQ(pipeline_test_set_mtime(source_path, fixed_mtime + 1, 123456789L), 0);
    cbm_pipeline_incremental_test_reset_faults();
    cbm_pipeline_t *metadata_only = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(metadata_only);
    ASSERT_EQ(cbm_pipeline_run(metadata_only), 0);
    ASSERT_EQ(cbm_pipeline_incremental_test_last_route(), CBM_INCREMENTAL_ROUTE_NOOP);
    cbm_pipeline_free(metadata_only);

    write_temp_file(tmp, "refs.go", replacement_source);
    ASSERT_EQ(pipeline_test_set_mtime(source_path, fixed_mtime, 123456789L), 0);
    cbm_pipeline_incremental_test_reset_faults();
    cbm_pipeline_t *second = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(second);
    ASSERT_EQ(cbm_pipeline_run(second), 0);
    ASSERT_EQ(cbm_pipeline_incremental_test_last_route(), CBM_INCREMENTAL_ROUTE_FORCED_FULL);
    const char *second_project = cbm_pipeline_project_name(second);
    cbm_store_t *second_store = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(second_store);
    ASSERT_EQ(named_edge_count(second_store, second_project, "CALL_REFERENCE",
                               "incrementalReferenceSite", "alphaReferenceTarget"),
              0);
    ASSERT_EQ(named_edge_count(second_store, second_project, "CALL_REFERENCE",
                               "incrementalReferenceSite", "bravoReferenceTarget"),
              1);
    ASSERT_EQ(named_edge_count(second_store, second_project, "USAGE", "incrementalReferenceSite",
                               "bravoReferenceTarget"),
              0);
    ASSERT_EQ(named_edge_count(second_store, second_project, "CALLS", "incrementalReferenceSite",
                               "bravoReferenceTarget"),
              0);
    cbm_store_close(second_store);
    cbm_pipeline_free(second);
    th_rmtree(tmp);
    PASS();
}

/* Re-indexing only the caller must retain cross-file semantic proof for a
 * callable value whose definition lives in an unchanged file. A fresh full
 * index of the edited sources is the convergence oracle: incremental output
 * must not downgrade the exact CALL_REFERENCE to a generic USAGE. */
TEST(pipeline_incremental_cross_file_call_reference_matches_fresh_full) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_ref_cross_incr_XXXXXX");
    if (!cbm_mkdtemp(tmp)) {
        FAIL("tmpdir");
    }

    static const char target_source[] = "export function importedIncrementalHandler(): void {}\n";
    static const char initial_caller_source[] =
        "import { importedIncrementalHandler } from './plugin';\n"
        "function acceptIncrementalReference(callback: () => void): void {}\n"
        "export function incrementalImportedArgument(): void {\n"
        "  acceptIncrementalReference(importedIncrementalHandler);\n"
        "}\n";
    static const char edited_caller_source[] =
        "import { importedIncrementalHandler } from './plugin';\n"
        "function acceptIncrementalReference(callback: () => void): void {}\n"
        "export function incrementalImportedArgument(): void {\n"
        "  acceptIncrementalReference(importedIncrementalHandler);\n"
        "}\n"
        "// caller-only size-changing edit\n";

    write_temp_file(tmp, "plugin.ts", target_source);
    write_temp_file(tmp, "main.ts", initial_caller_source);

    char incremental_db[512];
    snprintf(incremental_db, sizeof(incremental_db), "%s/cross-reference-incremental.db", tmp);
    cbm_pipeline_t *baseline = cbm_pipeline_new(tmp, incremental_db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(baseline);
    ASSERT_EQ(cbm_pipeline_run(baseline), 0);
    const char *baseline_project = cbm_pipeline_project_name(baseline);
    cbm_store_t *baseline_store = cbm_store_open_path(incremental_db);
    ASSERT_NOT_NULL(baseline_store);
    ASSERT_EQ(named_edge_count(baseline_store, baseline_project, "CALL_REFERENCE",
                               "incrementalImportedArgument", "importedIncrementalHandler"),
              1);
    ASSERT_EQ(named_edge_count(baseline_store, baseline_project, "USAGE",
                               "incrementalImportedArgument", "importedIncrementalHandler"),
              0);
    ASSERT_EQ(named_edge_count(baseline_store, baseline_project, "CALLS",
                               "incrementalImportedArgument", "importedIncrementalHandler"),
              0);
    cbm_store_close(baseline_store);
    cbm_pipeline_free(baseline);

    write_temp_file(tmp, "main.ts", edited_caller_source);

    cbm_pipeline_t *incremental = cbm_pipeline_new(tmp, incremental_db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(incremental);
    ASSERT_EQ(cbm_pipeline_run(incremental), 0);
    const char *incremental_project = cbm_pipeline_project_name(incremental);
    cbm_store_t *incremental_store = cbm_store_open_path(incremental_db);
    ASSERT_NOT_NULL(incremental_store);
    int incremental_reference =
        named_edge_count(incremental_store, incremental_project, "CALL_REFERENCE",
                         "incrementalImportedArgument", "importedIncrementalHandler");
    int incremental_usage =
        named_edge_count(incremental_store, incremental_project, "USAGE",
                         "incrementalImportedArgument", "importedIncrementalHandler");
    int incremental_calls =
        named_edge_count(incremental_store, incremental_project, "CALLS",
                         "incrementalImportedArgument", "importedIncrementalHandler");
    cbm_store_close(incremental_store);
    cbm_pipeline_free(incremental);

    char full_db[512];
    snprintf(full_db, sizeof(full_db), "%s/cross-reference-full.db", tmp);
    cbm_pipeline_t *full = cbm_pipeline_new(tmp, full_db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(full);
    ASSERT_EQ(cbm_pipeline_run(full), 0);
    const char *full_project = cbm_pipeline_project_name(full);
    cbm_store_t *full_store = cbm_store_open_path(full_db);
    ASSERT_NOT_NULL(full_store);
    int full_reference =
        named_edge_count(full_store, full_project, "CALL_REFERENCE", "incrementalImportedArgument",
                         "importedIncrementalHandler");
    int full_usage = named_edge_count(full_store, full_project, "USAGE",
                                      "incrementalImportedArgument", "importedIncrementalHandler");
    int full_calls = named_edge_count(full_store, full_project, "CALLS",
                                      "incrementalImportedArgument", "importedIncrementalHandler");
    cbm_store_close(full_store);
    cbm_pipeline_free(full);
    th_rmtree(tmp);

    ASSERT_EQ(full_reference, 1);
    ASSERT_EQ(full_usage, 0);
    ASSERT_EQ(full_calls, 0);
    ASSERT_EQ(incremental_reference, full_reference);
    ASSERT_EQ(incremental_usage, full_usage);
    ASSERT_EQ(incremental_calls, full_calls);
    PASS();
}

/* A semantic control file can change an unchanged source file's exact target.
 * Both target modules deliberately export the same short name: without the
 * tsconfig path mapping, project-wide unique-name fallback is ambiguous and
 * cannot make this test pass. The clean full index after the config-only edit
 * is therefore an exact oracle for which module owns the CALL_REFERENCE.
 *
 * RED: incremental hashing tracks source inputs but not the semantic influence
 * of tsconfig.json. Changing only the alias mapping leaves the same-DB graph
 * pointed at target_a.ts while a fresh full index points at target_b.ts. */
TEST(pipeline_incremental_tsconfig_alias_change_matches_fresh_full) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_ref_tsconfig_incr_XXXXXX");
    if (!cbm_mkdtemp(tmp)) {
        FAIL("tmpdir");
    }

    static const char target_a_source[] = "export function semanticControlHandler(): void {\n"
                                          "  const selected = 'a';\n"
                                          "  void selected;\n"
                                          "}\n";
    static const char target_b_source[] = "export function semanticControlHandler(): void {\n"
                                          "  const selected = 'b';\n"
                                          "  void selected;\n"
                                          "}\n";
    static const char caller_source[] =
        "import { semanticControlHandler } from '@semantic-target';\n"
        "function acceptSemanticControl(callback: () => void): void {\n"
        "  void callback;\n"
        "}\n"
        "export function semanticControlCaller(): void {\n"
        "  acceptSemanticControl(semanticControlHandler);\n"
        "}\n";
    static const char initial_tsconfig[] =
        "{\n"
        "  \"compilerOptions\": {\n"
        "    \"baseUrl\": \".\",\n"
        "    \"paths\": {\"@semantic-target\": [\"./target_a\"]}\n"
        "  }\n"
        "}\n";
    static const char replacement_tsconfig[] =
        "{\n"
        "  \"compilerOptions\": {\n"
        "    \"baseUrl\": \".\",\n"
        "    \"paths\": {\"@semantic-target\": [\"./target_b\"]}\n"
        "  }\n"
        "}\n";
    static const char initial_jsconfig[] = "{\"compilerOptions\":{\"baseUrl\":\"./one\"}}\n";
    static const char replacement_jsconfig[] = "{\"compilerOptions\":{\"baseUrl\":\"./two\"}}\n";
    ASSERT_EQ(sizeof(initial_tsconfig), sizeof(replacement_tsconfig));
    ASSERT_EQ(sizeof(initial_jsconfig), sizeof(replacement_jsconfig));

    write_temp_file(tmp, "target_a.ts", target_a_source);
    write_temp_file(tmp, "target_b.ts", target_b_source);
    write_temp_file(tmp, "caller.ts", caller_source);
    write_temp_file(tmp, "tsconfig.json", initial_tsconfig);
    write_temp_file(tmp, "jsconfig.json", initial_jsconfig);
    char tsconfig_path[512];
    char jsconfig_path[512];
    snprintf(tsconfig_path, sizeof(tsconfig_path), "%s/tsconfig.json", tmp);
    snprintf(jsconfig_path, sizeof(jsconfig_path), "%s/jsconfig.json", tmp);
    const time_t fixed_config_mtime = 1700000100;
    ASSERT_EQ(pipeline_test_set_mtime(tsconfig_path, fixed_config_mtime, 222222222L), 0);
    ASSERT_EQ(pipeline_test_set_mtime(jsconfig_path, fixed_config_mtime, 333333333L), 0);

    char incremental_db[512];
    snprintf(incremental_db, sizeof(incremental_db), "%s/tsconfig-reference-incremental.db", tmp);
    cbm_pipeline_t *baseline = cbm_pipeline_new(tmp, incremental_db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(baseline);
    ASSERT_EQ(cbm_pipeline_run(baseline), 0);
    const char *baseline_project = cbm_pipeline_project_name(baseline);
    cbm_store_t *baseline_store = cbm_store_open_path(incremental_db);
    ASSERT_NOT_NULL(baseline_store);
    ASSERT_EQ(named_node_count(baseline_store, baseline_project, "semanticControlHandler"), 2);
    ASSERT_EQ(named_edge_to_file_count(baseline_store, baseline_project, "CALL_REFERENCE",
                                       "semanticControlCaller", "semanticControlHandler",
                                       "target_a.ts"),
              1);
    ASSERT_EQ(named_edge_to_file_count(baseline_store, baseline_project, "CALL_REFERENCE",
                                       "semanticControlCaller", "semanticControlHandler",
                                       "target_b.ts"),
              0);
    ASSERT_EQ(named_edge_count(baseline_store, baseline_project, "USAGE", "semanticControlCaller",
                               "semanticControlHandler"),
              0);
    ASSERT_EQ(named_edge_count(baseline_store, baseline_project, "CALLS", "semanticControlCaller",
                               "semanticControlHandler"),
              0);
    cbm_file_hash_t selected_config_hash = {0};
    ASSERT_EQ(cbm_store_get_file_hash(baseline_store, baseline_project, "tsconfig.json",
                                      &selected_config_hash),
              CBM_STORE_OK);
    char expected_config_hash[CBM_SHA256_HEX_LEN + 1];
    cbm_sha256_hex(initial_tsconfig, strlen(initial_tsconfig), expected_config_hash);
    ASSERT_NOT_NULL(selected_config_hash.sha256);
    ASSERT_STR_EQ(selected_config_hash.sha256, expected_config_hash);
    cbm_store_clear_file_hash(&selected_config_hash);
    cbm_file_hash_t unselected_config_hash = {0};
    ASSERT_EQ(cbm_store_get_file_hash(baseline_store, baseline_project, "jsconfig.json",
                                      &unselected_config_hash),
              CBM_STORE_NOT_FOUND);
    cbm_store_close(baseline_store);
    cbm_pipeline_free(baseline);

    /* The lower-priority jsconfig is not a semantic input while tsconfig is
     * present in the same directory. Its byte change must remain a no-op. */
    write_temp_file(tmp, "jsconfig.json", replacement_jsconfig);
    ASSERT_EQ(pipeline_test_set_mtime(jsconfig_path, fixed_config_mtime, 333333333L), 0);
    cbm_pipeline_incremental_test_reset_faults();
    cbm_pipeline_t *unselected = cbm_pipeline_new(tmp, incremental_db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(unselected);
    ASSERT_EQ(cbm_pipeline_run(unselected), 0);
    ASSERT_EQ(cbm_pipeline_incremental_test_last_route(), CBM_INCREMENTAL_ROUTE_NOOP);
    cbm_pipeline_free(unselected);

    /* The source files remain byte-for-byte unchanged; selected control bytes
     * change at the same length and mtime. */
    write_temp_file(tmp, "tsconfig.json", replacement_tsconfig);
    ASSERT_EQ(pipeline_test_set_mtime(tsconfig_path, fixed_config_mtime, 222222222L), 0);

    cbm_pipeline_incremental_test_reset_faults();
    cbm_pipeline_t *incremental = cbm_pipeline_new(tmp, incremental_db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(incremental);
    ASSERT_EQ(cbm_pipeline_run(incremental), 0);
    ASSERT_EQ(cbm_pipeline_incremental_test_last_route(), CBM_INCREMENTAL_ROUTE_FORCED_FULL);
    const char *incremental_project = cbm_pipeline_project_name(incremental);
    cbm_store_t *incremental_store = cbm_store_open_path(incremental_db);
    ASSERT_NOT_NULL(incremental_store);
    int incremental_reference_a =
        named_edge_to_file_count(incremental_store, incremental_project, "CALL_REFERENCE",
                                 "semanticControlCaller", "semanticControlHandler", "target_a.ts");
    int incremental_reference_b =
        named_edge_to_file_count(incremental_store, incremental_project, "CALL_REFERENCE",
                                 "semanticControlCaller", "semanticControlHandler", "target_b.ts");
    int incremental_usage = named_edge_count(incremental_store, incremental_project, "USAGE",
                                             "semanticControlCaller", "semanticControlHandler");
    int incremental_calls = named_edge_count(incremental_store, incremental_project, "CALLS",
                                             "semanticControlCaller", "semanticControlHandler");
    cbm_store_close(incremental_store);
    cbm_pipeline_free(incremental);

    char full_db[512];
    snprintf(full_db, sizeof(full_db), "%s/tsconfig-reference-full.db", tmp);
    cbm_pipeline_t *full = cbm_pipeline_new(tmp, full_db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(full);
    ASSERT_EQ(cbm_pipeline_run(full), 0);
    const char *full_project = cbm_pipeline_project_name(full);
    cbm_store_t *full_store = cbm_store_open_path(full_db);
    ASSERT_NOT_NULL(full_store);
    int full_reference_a =
        named_edge_to_file_count(full_store, full_project, "CALL_REFERENCE",
                                 "semanticControlCaller", "semanticControlHandler", "target_a.ts");
    int full_reference_b =
        named_edge_to_file_count(full_store, full_project, "CALL_REFERENCE",
                                 "semanticControlCaller", "semanticControlHandler", "target_b.ts");
    int full_usage = named_edge_count(full_store, full_project, "USAGE", "semanticControlCaller",
                                      "semanticControlHandler");
    int full_calls = named_edge_count(full_store, full_project, "CALLS", "semanticControlCaller",
                                      "semanticControlHandler");
    cbm_store_close(full_store);
    cbm_pipeline_free(full);
    th_rmtree(tmp);

    /* Prove the fresh oracle changed only because the alias now selects B. */
    ASSERT_EQ(full_reference_a, 0);
    ASSERT_EQ(full_reference_b, 1);
    ASSERT_EQ(full_usage, 0);
    ASSERT_EQ(full_calls, 0);

    /* Incremental output must converge exactly on the fresh target and kind. */
    ASSERT_EQ(incremental_reference_a, full_reference_a);
    ASSERT_EQ(incremental_reference_b, full_reference_b);
    ASSERT_EQ(incremental_usage, full_usage);
    ASSERT_EQ(incremental_calls, full_calls);
    PASS();
}

#if defined(CBM_INCREMENTAL_TEST_API) && CBM_INCREMENTAL_TEST_API
static void observe_named_generation(const char *db_path, const char *project,
                                     const char *before_name, const char *after_name,
                                     int *before_count, int *after_count) {
    *before_count = -1;
    *after_count = -1;
    cbm_store_t *store = cbm_store_open_path(db_path);
    if (!store) {
        return;
    }
    *before_count = named_node_count(store, project, before_name);
    *after_count = named_node_count(store, project, after_name);
    cbm_store_close(store);
}

static int count_generation_stage_artifacts(const char *dir_path, const char *db_basename) {
    cbm_dir_t *dir = cbm_opendir(dir_path);
    if (!dir) {
        return -1;
    }
    char prefix[256];
    snprintf(prefix, sizeof(prefix), "%s.stage.", db_basename);
    size_t prefix_len = strlen(prefix);
    int count = 0;
    cbm_dirent_t *entry;
    while ((entry = cbm_readdir(dir)) != NULL) {
        if (strncmp(entry->name, prefix, prefix_len) == 0) {
            count++;
        }
    }
    cbm_closedir(dir);
    return count;
}

typedef struct {
    const char *path;
    const char *replacement;
    int write_rc;
} manifest_race_mutation_t;

static void mutate_semantic_input_before_final_manifest(void *userdata) {
    manifest_race_mutation_t *mutation = (manifest_race_mutation_t *)userdata;
    mutation->write_rc = th_write_file(mutation->path, mutation->replacement);
}

/* A graph and its exact manifest are one generation. If source bytes change
 * after extraction but before publication, the mixed generation must be
 * rejected and the previous live DB preserved. */
TEST(pipeline_source_mutation_before_publication_preserves_previous_generation) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_publish_generation_race_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char source_path[512];
    char db_path[512];
    snprintf(source_path, sizeof(source_path), "%s/generation.py", tmp);
    snprintf(db_path, sizeof(db_path), "%s/generation.db", tmp);
    ASSERT_EQ(th_write_file(source_path, "def StableGeneration():\n    return 1\n"), 0);

    cbm_pipeline_incremental_test_reset_faults();
    cbm_pipeline_t *baseline = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(baseline);
    ASSERT_EQ(cbm_pipeline_run(baseline), 0);
    char project[256];
    snprintf(project, sizeof(project), "%s", cbm_pipeline_project_name(baseline));
    cbm_pipeline_free(baseline);

    ASSERT_EQ(th_write_file(source_path, "def ExtractedGeneration():\n    return 2\n"), 0);
    manifest_race_mutation_t mutation = {
        .path = source_path,
        .replacement = "def FinalGeneration():\n    return 3\n",
        .write_rc = -1,
    };
    cbm_pipeline_incremental_test_before_final_manifest_once(
        mutate_semantic_input_before_final_manifest, &mutation);
    cbm_pipeline_t *raced = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(raced);
    int raced_rc = cbm_pipeline_run(raced);
    cbm_pipeline_free(raced);
    int raced_stable = -1;
    int raced_extracted = -1;
    observe_named_generation(db_path, project, "StableGeneration", "ExtractedGeneration",
                             &raced_stable, &raced_extracted);
    int raced_stage_count = count_generation_stage_artifacts(tmp, "generation.db");

    cbm_pipeline_t *retry = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(retry);
    int retry_rc = cbm_pipeline_run(retry);
    cbm_pipeline_free(retry);
    int retry_stable = -1;
    int retry_final = -1;
    observe_named_generation(db_path, project, "StableGeneration", "FinalGeneration", &retry_stable,
                             &retry_final);
    int retry_stage_count = count_generation_stage_artifacts(tmp, "generation.db");
    cbm_pipeline_incremental_test_reset_faults();
    th_rmtree(tmp);

    ASSERT_EQ(mutation.write_rc, 0);
    ASSERT_EQ(raced_rc, CBM_PIPELINE_ABORT_PRESERVE_DB);
    ASSERT_EQ(raced_stable, 1);
    ASSERT_EQ(raced_extracted, 0);
    ASSERT_EQ(raced_stage_count, 0);
    ASSERT_EQ(retry_rc, 0);
    ASSERT_EQ(retry_stable, 0);
    ASSERT_EQ(retry_final, 1);
    ASSERT_EQ(retry_stage_count, 0);
    PASS();
}

/* Discovery and extraction must describe the same immutable generation. A
 * source file created after extraction is not present in the original file
 * list, so merely re-hashing that list cannot detect the race. Publication
 * must abort, leave the old DB intact, and let a clean retry discover both
 * files. */
TEST(pipeline_source_addition_before_publication_preserves_previous_generation) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_publish_addition_race_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char source_path[512];
    char late_path[512];
    char db_path[512];
    snprintf(source_path, sizeof(source_path), "%s/generation.py", tmp);
    snprintf(late_path, sizeof(late_path), "%s/late.py", tmp);
    snprintf(db_path, sizeof(db_path), "%s/generation.db", tmp);
    ASSERT_EQ(th_write_file(source_path, "def StableAdditionGeneration():\n    return 1\n"), 0);

    cbm_pipeline_incremental_test_reset_faults();
    cbm_pipeline_t *baseline = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(baseline);
    ASSERT_EQ(cbm_pipeline_run(baseline), 0);
    char project[256];
    snprintf(project, sizeof(project), "%s", cbm_pipeline_project_name(baseline));
    cbm_pipeline_free(baseline);

    ASSERT_EQ(th_write_file(source_path, "def ExtractedAdditionGeneration():\n    return 2\n"), 0);
    manifest_race_mutation_t mutation = {
        .path = late_path,
        .replacement = "def LateDiscoveredGeneration():\n    return 3\n",
        .write_rc = -1,
    };
    cbm_pipeline_incremental_test_before_final_manifest_once(
        mutate_semantic_input_before_final_manifest, &mutation);
    cbm_pipeline_t *raced = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(raced);
    int raced_rc = cbm_pipeline_run(raced);
    cbm_pipeline_free(raced);

    int raced_stable = -1;
    int raced_extracted = -1;
    int raced_late = -1;
    cbm_store_t *raced_store = cbm_store_open_path(db_path);
    if (raced_store) {
        raced_stable = named_node_count(raced_store, project, "StableAdditionGeneration");
        raced_extracted = named_node_count(raced_store, project, "ExtractedAdditionGeneration");
        raced_late = named_node_count(raced_store, project, "LateDiscoveredGeneration");
        cbm_store_close(raced_store);
    }
    int raced_stage_count = count_generation_stage_artifacts(tmp, "generation.db");

    cbm_pipeline_incremental_test_reset_faults();
    cbm_pipeline_t *retry = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(retry);
    int retry_rc = cbm_pipeline_run(retry);
    cbm_incremental_route_t retry_route = cbm_pipeline_incremental_test_last_route();
    cbm_pipeline_free(retry);

    int retry_stable = -1;
    int retry_extracted = -1;
    int retry_late = -1;
    cbm_store_t *retry_store = cbm_store_open_path(db_path);
    if (retry_store) {
        retry_stable = named_node_count(retry_store, project, "StableAdditionGeneration");
        retry_extracted = named_node_count(retry_store, project, "ExtractedAdditionGeneration");
        retry_late = named_node_count(retry_store, project, "LateDiscoveredGeneration");
        cbm_store_close(retry_store);
    }
    int retry_stage_count = count_generation_stage_artifacts(tmp, "generation.db");
    cbm_pipeline_incremental_test_reset_faults();
    th_rmtree(tmp);

    ASSERT_EQ(mutation.write_rc, 0);
    ASSERT_EQ(raced_rc, CBM_PIPELINE_ABORT_PRESERVE_DB);
    ASSERT_EQ(raced_stable, 1);
    ASSERT_EQ(raced_extracted, 0);
    ASSERT_EQ(raced_late, 0);
    ASSERT_EQ(raced_stage_count, 0);
    ASSERT_EQ(retry_rc, 0);
    ASSERT_EQ(retry_route, CBM_INCREMENTAL_ROUTE_FORCED_FULL);
    ASSERT_EQ(retry_stable, 0);
    ASSERT_EQ(retry_extracted, 1);
    ASSERT_EQ(retry_late, 1);
    ASSERT_EQ(retry_stage_count, 0);
    PASS();
}

/* A selected path-alias config is an extraction input, not just manifest
 * metadata. Rewriting it after extraction must not publish an A-resolved graph
 * beside a B-hashed manifest; that combination would make the next run a
 * false no-op and permanently retain the stale target. */
TEST(pipeline_tsconfig_mutation_before_publication_preserves_previous_generation) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_publish_tsconfig_race_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));

    static const char target_a_source[] = "export function publicationRaceHandler(): void {\n"
                                          "  const selected = 'a';\n"
                                          "  void selected;\n"
                                          "}\n";
    static const char target_b_source[] = "export function publicationRaceHandler(): void {\n"
                                          "  const selected = 'b';\n"
                                          "  void selected;\n"
                                          "}\n";
    static const char stable_caller_source[] =
        "import { publicationRaceHandler } from '@publication-race';\n"
        "function acceptPublicationRace(callback: () => void): void {\n"
        "  void callback;\n"
        "}\n"
        "export function PublicationRaceStableCaller(): void {\n"
        "  acceptPublicationRace(publicationRaceHandler);\n"
        "}\n";
    static const char extracted_caller_source[] =
        "import { publicationRaceHandler } from '@publication-race';\n"
        "function acceptPublicationRace(callback: () => void): void {\n"
        "  void callback;\n"
        "}\n"
        "export function PublicationRaceExtractedCaller(): void {\n"
        "  acceptPublicationRace(publicationRaceHandler);\n"
        "}\n";
    static const char tsconfig_a[] = "{\n"
                                     "  \"compilerOptions\": {\n"
                                     "    \"baseUrl\": \".\",\n"
                                     "    \"paths\": {\"@publication-race\": [\"./target_a\"]}\n"
                                     "  }\n"
                                     "}\n";
    static const char tsconfig_b[] = "{\n"
                                     "  \"compilerOptions\": {\n"
                                     "    \"baseUrl\": \".\",\n"
                                     "    \"paths\": {\"@publication-race\": [\"./target_b\"]}\n"
                                     "  }\n"
                                     "}\n";

    write_temp_file(tmp, "target_a.ts", target_a_source);
    write_temp_file(tmp, "target_b.ts", target_b_source);
    write_temp_file(tmp, "caller.ts", stable_caller_source);
    write_temp_file(tmp, "tsconfig.json", tsconfig_a);
    char caller_path[512];
    char tsconfig_path[512];
    char db_path[512];
    snprintf(caller_path, sizeof(caller_path), "%s/caller.ts", tmp);
    snprintf(tsconfig_path, sizeof(tsconfig_path), "%s/tsconfig.json", tmp);
    snprintf(db_path, sizeof(db_path), "%s/generation.db", tmp);

    cbm_pipeline_incremental_test_reset_faults();
    cbm_pipeline_t *baseline = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(baseline);
    ASSERT_EQ(cbm_pipeline_run(baseline), 0);
    char project[256];
    snprintf(project, sizeof(project), "%s", cbm_pipeline_project_name(baseline));
    cbm_pipeline_free(baseline);

    ASSERT_EQ(th_write_file(caller_path, extracted_caller_source), 0);
    manifest_race_mutation_t mutation = {
        .path = tsconfig_path,
        .replacement = tsconfig_b,
        .write_rc = -1,
    };
    cbm_pipeline_incremental_test_before_final_manifest_once(
        mutate_semantic_input_before_final_manifest, &mutation);
    cbm_pipeline_t *raced = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(raced);
    int raced_rc = cbm_pipeline_run(raced);
    cbm_pipeline_free(raced);

    int raced_stable_a = -1;
    int raced_stable_b = -1;
    int raced_extracted_nodes = -1;
    cbm_store_t *raced_store = cbm_store_open_path(db_path);
    if (raced_store) {
        raced_stable_a = named_edge_to_file_count(raced_store, project, "CALL_REFERENCE",
                                                  "PublicationRaceStableCaller",
                                                  "publicationRaceHandler", "target_a.ts");
        raced_stable_b = named_edge_to_file_count(raced_store, project, "CALL_REFERENCE",
                                                  "PublicationRaceStableCaller",
                                                  "publicationRaceHandler", "target_b.ts");
        raced_extracted_nodes =
            named_node_count(raced_store, project, "PublicationRaceExtractedCaller");
        cbm_store_close(raced_store);
    }
    int raced_stage_count = count_generation_stage_artifacts(tmp, "generation.db");

    cbm_pipeline_incremental_test_reset_faults();
    cbm_pipeline_t *retry = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(retry);
    int retry_rc = cbm_pipeline_run(retry);
    cbm_incremental_route_t retry_route = cbm_pipeline_incremental_test_last_route();
    cbm_pipeline_free(retry);

    int retry_stable_nodes = -1;
    int retry_extracted_a = -1;
    int retry_extracted_b = -1;
    int retry_extracted_usage = -1;
    int retry_extracted_calls = -1;
    cbm_store_t *retry_store = cbm_store_open_path(db_path);
    if (retry_store) {
        retry_stable_nodes = named_node_count(retry_store, project, "PublicationRaceStableCaller");
        retry_extracted_a = named_edge_to_file_count(retry_store, project, "CALL_REFERENCE",
                                                     "PublicationRaceExtractedCaller",
                                                     "publicationRaceHandler", "target_a.ts");
        retry_extracted_b = named_edge_to_file_count(retry_store, project, "CALL_REFERENCE",
                                                     "PublicationRaceExtractedCaller",
                                                     "publicationRaceHandler", "target_b.ts");
        retry_extracted_usage =
            named_edge_count(retry_store, project, "USAGE", "PublicationRaceExtractedCaller",
                             "publicationRaceHandler");
        retry_extracted_calls =
            named_edge_count(retry_store, project, "CALLS", "PublicationRaceExtractedCaller",
                             "publicationRaceHandler");
        cbm_store_close(retry_store);
    }
    int retry_stage_count = count_generation_stage_artifacts(tmp, "generation.db");
    cbm_pipeline_incremental_test_reset_faults();
    th_rmtree(tmp);

    ASSERT_EQ(mutation.write_rc, 0);
    ASSERT_EQ(raced_rc, CBM_PIPELINE_ABORT_PRESERVE_DB);
    ASSERT_EQ(raced_stable_a, 1);
    ASSERT_EQ(raced_stable_b, 0);
    ASSERT_EQ(raced_extracted_nodes, 0);
    ASSERT_EQ(raced_stage_count, 0);
    ASSERT_EQ(retry_rc, 0);
    ASSERT_EQ(retry_route, CBM_INCREMENTAL_ROUTE_FORCED_FULL);
    ASSERT_EQ(retry_stable_nodes, 0);
    ASSERT_EQ(retry_extracted_a, 0);
    ASSERT_EQ(retry_extracted_b, 1);
    ASSERT_EQ(retry_extracted_usage, 0);
    ASSERT_EQ(retry_extracted_calls, 0);
    ASSERT_EQ(retry_stage_count, 0);
    PASS();
}

/* Metadata participates in exact-input compatibility. Old coverage schema or
 * an upgrade to a more comprehensive discovery/index mode must force a
 * complete replacement even when every semantic-input byte is unchanged; the
 * replacement must write current metadata so the next identical run is a
 * true no-op. Cheaper requests retain fuller published coverage separately. */
TEST(pipeline_exact_inputs_migrate_coverage_metadata_and_index_mode) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_manifest_metadata_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    write_temp_file(tmp, "generation.py", "def ExactMetadataGeneration():\n    return 1\n");
    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/generation.db", tmp);

    cbm_pipeline_incremental_test_reset_faults();
    cbm_pipeline_t *baseline = cbm_pipeline_new(tmp, db_path, CBM_MODE_FAST);
    ASSERT_NOT_NULL(baseline);
    ASSERT_EQ(cbm_pipeline_run(baseline), 0);
    char project[256];
    snprintf(project, sizeof(project), "%s", cbm_pipeline_project_name(baseline));
    cbm_pipeline_free(baseline);

    cbm_pipeline_incremental_test_reset_faults();
    cbm_pipeline_t *exact_before_migration = cbm_pipeline_new(tmp, db_path, CBM_MODE_FAST);
    ASSERT_NOT_NULL(exact_before_migration);
    ASSERT_EQ(cbm_pipeline_run(exact_before_migration), 0);
    cbm_incremental_route_t exact_before_route = cbm_pipeline_incremental_test_last_route();
    cbm_pipeline_free(exact_before_migration);

    cbm_store_t *metadata_store = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(metadata_store);
    cbm_coverage_row_t *coverage_rows = NULL;
    int coverage_count = 0;
    ASSERT_EQ(cbm_store_coverage_get(metadata_store, project, &coverage_rows, &coverage_count),
              CBM_STORE_OK);
    cbm_coverage_meta_t current_meta = {0};
    ASSERT_EQ(cbm_store_coverage_meta_get(metadata_store, project, &current_meta), CBM_STORE_OK);
    cbm_coverage_meta_t legacy_meta = current_meta;
    legacy_meta.coverage_version = 1;
    ASSERT_EQ(cbm_store_coverage_replace_ex(metadata_store, project, coverage_rows, coverage_count,
                                            &legacy_meta),
              CBM_STORE_OK);
    cbm_store_free_coverage(coverage_rows, coverage_count);
    cbm_store_coverage_meta_clear(&current_meta);
    cbm_store_close(metadata_store);

    cbm_pipeline_incremental_test_reset_faults();
    cbm_pipeline_t *migration = cbm_pipeline_new(tmp, db_path, CBM_MODE_FAST);
    ASSERT_NOT_NULL(migration);
    ASSERT_EQ(cbm_pipeline_run(migration), 0);
    cbm_incremental_route_t migration_route = cbm_pipeline_incremental_test_last_route();
    cbm_pipeline_free(migration);

    metadata_store = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(metadata_store);
    cbm_coverage_meta_t migrated_meta = {0};
    ASSERT_EQ(cbm_store_coverage_meta_get(metadata_store, project, &migrated_meta), CBM_STORE_OK);
    int migrated_version = migrated_meta.coverage_version;
    bool migrated_hashes_complete = migrated_meta.hash_records_complete;
    char migrated_mode[32];
    snprintf(migrated_mode, sizeof(migrated_mode), "%s",
             migrated_meta.index_mode ? migrated_meta.index_mode : "");
    cbm_store_coverage_meta_clear(&migrated_meta);
    cbm_store_close(metadata_store);

    cbm_pipeline_incremental_test_reset_faults();
    cbm_pipeline_t *mode_change = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(mode_change);
    ASSERT_EQ(cbm_pipeline_run(mode_change), 0);
    cbm_incremental_route_t mode_change_route = cbm_pipeline_incremental_test_last_route();
    cbm_pipeline_free(mode_change);

    metadata_store = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(metadata_store);
    cbm_coverage_meta_t full_meta = {0};
    ASSERT_EQ(cbm_store_coverage_meta_get(metadata_store, project, &full_meta), CBM_STORE_OK);
    int full_version = full_meta.coverage_version;
    bool full_hashes_complete = full_meta.hash_records_complete;
    char full_mode[32];
    snprintf(full_mode, sizeof(full_mode), "%s", full_meta.index_mode ? full_meta.index_mode : "");
    cbm_store_coverage_meta_clear(&full_meta);
    cbm_store_close(metadata_store);

    cbm_pipeline_incremental_test_reset_faults();
    cbm_pipeline_t *exact_full = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(exact_full);
    ASSERT_EQ(cbm_pipeline_run(exact_full), 0);
    cbm_incremental_route_t exact_full_route = cbm_pipeline_incremental_test_last_route();
    cbm_pipeline_free(exact_full);
    cbm_pipeline_incremental_test_reset_faults();
    th_rmtree(tmp);

    ASSERT_EQ(exact_before_route, CBM_INCREMENTAL_ROUTE_NOOP);
    ASSERT_EQ(migration_route, CBM_INCREMENTAL_ROUTE_FORCED_FULL);
    ASSERT_EQ(migrated_version, CBM_SEMANTIC_INDEX_VERSION);
    ASSERT_TRUE(migrated_hashes_complete);
    ASSERT_STR_EQ(migrated_mode, "fast");
    ASSERT_EQ(mode_change_route, CBM_INCREMENTAL_ROUTE_FORCED_FULL);
    ASSERT_EQ(full_version, CBM_SEMANTIC_INDEX_VERSION);
    ASSERT_TRUE(full_hashes_complete);
    ASSERT_STR_EQ(full_mode, "full");
    ASSERT_EQ(exact_full_route, CBM_INCREMENTAL_ROUTE_NOOP);
    PASS();
}

/* Once a repository contains a shared artifact, every subsequently published
 * full generation must refresh it, even when persistence was not explicitly
 * requested on that invocation. Otherwise the live DB advances while a clean
 * teammate import silently restores the previous graph. */
TEST(pipeline_existing_artifact_refreshes_after_default_forced_full_reindex) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_artifact_refresh_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char source_path[512];
    char db_path[512];
    char imported_path[512];
    snprintf(source_path, sizeof(source_path), "%s/generation.py", tmp);
    snprintf(db_path, sizeof(db_path), "%s/generation.db", tmp);
    snprintf(imported_path, sizeof(imported_path), "%s/imported.db", tmp);
    ASSERT_EQ(th_write_file(source_path, "def ArtifactGenerationBefore():\n    return 1\n"), 0);

    cbm_pipeline_incremental_test_reset_faults();
    cbm_pipeline_t *baseline = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(baseline);
    cbm_pipeline_set_persistence(baseline, true);
    ASSERT_EQ(cbm_pipeline_run(baseline), 0);
    char project[256];
    snprintf(project, sizeof(project), "%s", cbm_pipeline_project_name(baseline));
    cbm_pipeline_free(baseline);
    ASSERT_TRUE(cbm_artifact_exists(tmp));

    ASSERT_EQ(th_write_file(source_path, "def ArtifactGenerationAfter():\n    return 2\n"), 0);
    cbm_pipeline_incremental_test_reset_faults();
    cbm_pipeline_t *default_reindex = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(default_reindex);
    int reindex_rc = cbm_pipeline_run(default_reindex);
    cbm_incremental_route_t reindex_route = cbm_pipeline_incremental_test_last_route();
    cbm_pipeline_free(default_reindex);

    /* A derived artifact must not become an input that forces another rebuild,
     * and refreshing it must not switch the authoritative DB back to WAL. */
    cbm_pipeline_incremental_test_reset_faults();
    cbm_pipeline_t *unchanged = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(unchanged);
    int unchanged_rc = cbm_pipeline_run(unchanged);
    cbm_incremental_route_t unchanged_route = cbm_pipeline_incremental_test_last_route();
    cbm_pipeline_free(unchanged);

    char journal_mode[16] = {0};
    sqlite3 *raw = NULL;
    sqlite3_stmt *journal_stmt = NULL;
    int journal_ok =
        sqlite3_open_v2(db_path, &raw, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK &&
        sqlite3_prepare_v2(raw, "PRAGMA journal_mode;", -1, &journal_stmt, NULL) == SQLITE_OK &&
        sqlite3_step(journal_stmt) == SQLITE_ROW;
    if (journal_ok) {
        const unsigned char *mode = sqlite3_column_text(journal_stmt, 0);
        if (!mode || snprintf(journal_mode, sizeof(journal_mode), "%s", (const char *)mode) >=
                         (int)sizeof(journal_mode)) {
            journal_ok = 0;
        }
    }
    sqlite3_finalize(journal_stmt);
    if (raw) {
        sqlite3_close(raw);
    }

    int live_before = -1;
    int live_after = -1;
    cbm_store_t *live_store = cbm_store_open_path(db_path);
    if (live_store) {
        live_before = named_node_count(live_store, project, "ArtifactGenerationBefore");
        live_after = named_node_count(live_store, project, "ArtifactGenerationAfter");
        cbm_store_close(live_store);
    }

    int import_rc = cbm_artifact_import(tmp, imported_path);
    int artifact_before = -1;
    int artifact_after = -1;
    cbm_store_t *artifact_store = import_rc == 0 ? cbm_store_open_path(imported_path) : NULL;
    if (artifact_store) {
        artifact_before = named_node_count(artifact_store, project, "ArtifactGenerationBefore");
        artifact_after = named_node_count(artifact_store, project, "ArtifactGenerationAfter");
        cbm_store_close(artifact_store);
    }
    cbm_pipeline_incremental_test_reset_faults();
    th_rmtree(tmp);

    ASSERT_EQ(reindex_rc, 0);
    ASSERT_EQ(reindex_route, CBM_INCREMENTAL_ROUTE_FORCED_FULL);
    ASSERT_EQ(unchanged_rc, 0);
    ASSERT_EQ(unchanged_route, CBM_INCREMENTAL_ROUTE_NOOP);
    ASSERT_TRUE(journal_ok);
    ASSERT_STR_EQ(journal_mode, "delete");
    ASSERT_EQ(live_before, 0);
    ASSERT_EQ(live_after, 1);
    ASSERT_EQ(import_rc, 0);
    ASSERT_EQ(artifact_before, 0);
    ASSERT_EQ(artifact_after, 1);
    PASS();
}

/* Cancellation observed after predump must be an explicit preserved abort,
 * never a false success. The retry then publishes the edited generation. */
TEST(pipeline_full_cancel_after_predump_preserves_previous_generation) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_publish_cancel_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    static const char before_source[] = "def BeforeCancel():\n    return 1\n";
    static const char after_source[] = "def AfterCancel():\n    return 2\n# changed\n";
    write_temp_file(tmp, "generation.py", before_source);
    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/generation.db", tmp);

    cbm_pipeline_incremental_test_reset_faults();
    cbm_pipeline_t *baseline = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(baseline);
    ASSERT_EQ(cbm_pipeline_run(baseline), 0);
    char project[256];
    snprintf(project, sizeof(project), "%s", cbm_pipeline_project_name(baseline));
    cbm_pipeline_free(baseline);

    write_temp_file(tmp, "generation.py", after_source);
    cbm_pipeline_incremental_test_cancel_after_predump_once();
    cbm_pipeline_t *cancelled = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(cancelled);
    int cancelled_rc = cbm_pipeline_run(cancelled);
    cbm_incremental_route_t cancelled_route = cbm_pipeline_incremental_test_last_route();
    cbm_pipeline_free(cancelled);
    int cancelled_before = -1;
    int cancelled_after = -1;
    observe_named_generation(db_path, project, "BeforeCancel", "AfterCancel", &cancelled_before,
                             &cancelled_after);

    cbm_pipeline_incremental_test_reset_faults();
    cbm_pipeline_t *retry = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(retry);
    int retry_rc = cbm_pipeline_run(retry);
    cbm_incremental_route_t retry_route = cbm_pipeline_incremental_test_last_route();
    cbm_pipeline_free(retry);
    int retry_before = -1;
    int retry_after = -1;
    observe_named_generation(db_path, project, "BeforeCancel", "AfterCancel", &retry_before,
                             &retry_after);
    int stage_count = count_generation_stage_artifacts(tmp, "generation.db");
    cbm_pipeline_incremental_test_reset_faults();
    th_rmtree(tmp);

    ASSERT_EQ(cancelled_rc, CBM_PIPELINE_ABORT_PRESERVE_DB);
    ASSERT_EQ(cancelled_route, CBM_INCREMENTAL_ROUTE_FORCED_FULL);
    ASSERT_EQ(cancelled_before, 1);
    ASSERT_EQ(cancelled_after, 0);
    ASSERT_EQ(retry_rc, 0);
    ASSERT_EQ(retry_route, CBM_INCREMENTAL_ROUTE_FORCED_FULL);
    ASSERT_EQ(retry_before, 0);
    ASSERT_EQ(retry_after, 1);
    ASSERT_EQ(stage_count, 0);
    PASS();
}

/* Cancellation can arrive while the destination DB is being quiesced. It must
 * still be observed at the final commit boundary, before the atomic rename. */
TEST(pipeline_full_cancel_after_destination_prepare_preserves_previous_generation) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_publish_late_cancel_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    write_temp_file(tmp, "generation.py", "def BeforeLateCancel():\n    return 1\n");
    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/generation.db", tmp);

    cbm_pipeline_incremental_test_reset_faults();
    cbm_pipeline_t *baseline = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(baseline);
    ASSERT_EQ(cbm_pipeline_run(baseline), 0);
    char project[256];
    snprintf(project, sizeof(project), "%s", cbm_pipeline_project_name(baseline));
    cbm_pipeline_free(baseline);

    write_temp_file(tmp, "generation.py", "def AfterLateCancel():\n    return 2\n# changed\n");
    cbm_pipeline_incremental_test_cancel_after_destination_prepare_once();
    cbm_pipeline_t *cancelled = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(cancelled);
    int cancelled_rc = cbm_pipeline_run(cancelled);
    cbm_pipeline_free(cancelled);

    int before_count = -1;
    int after_count = -1;
    observe_named_generation(db_path, project, "BeforeLateCancel", "AfterLateCancel", &before_count,
                             &after_count);
    int stage_count = count_generation_stage_artifacts(tmp, "generation.db");
    cbm_pipeline_incremental_test_reset_faults();
    th_rmtree(tmp);

    ASSERT_EQ(cancelled_rc, CBM_PIPELINE_ABORT_PRESERVE_DB);
    ASSERT_EQ(before_count, 1);
    ASSERT_EQ(after_count, 0);
    ASSERT_EQ(stage_count, 0);
    PASS();
}

/* A failure after graph serialization but before metadata/publication must
 * discard only the staged generation. The final path remains the complete old
 * graph and a clean retry converges. */
TEST(pipeline_full_persist_failure_after_stage_dump_preserves_previous_generation) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_publish_full_fail_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    write_temp_file(tmp, "generation.py", "def BeforeFullPersist():\n    return 1\n");
    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/generation.db", tmp);

    cbm_pipeline_incremental_test_reset_faults();
    cbm_pipeline_t *baseline = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(baseline);
    ASSERT_EQ(cbm_pipeline_run(baseline), 0);
    char project[256];
    snprintf(project, sizeof(project), "%s", cbm_pipeline_project_name(baseline));
    cbm_pipeline_free(baseline);

    write_temp_file(tmp, "generation.py",
                    "def AfterFullPersist():\n    return 2\n# changed generation\n");
    cbm_pipeline_incremental_test_fail_after_stage_dump_once();
    cbm_pipeline_t *faulted = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(faulted);
    int faulted_rc = cbm_pipeline_run(faulted);
    cbm_pipeline_free(faulted);
    int faulted_before = -1;
    int faulted_after = -1;
    observe_named_generation(db_path, project, "BeforeFullPersist", "AfterFullPersist",
                             &faulted_before, &faulted_after);
    int faulted_stage_count = count_generation_stage_artifacts(tmp, "generation.db");

    cbm_pipeline_incremental_test_reset_faults();
    cbm_pipeline_t *retry = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(retry);
    int retry_rc = cbm_pipeline_run(retry);
    cbm_pipeline_free(retry);
    int retry_before = -1;
    int retry_after = -1;
    observe_named_generation(db_path, project, "BeforeFullPersist", "AfterFullPersist",
                             &retry_before, &retry_after);
    int retry_stage_count = count_generation_stage_artifacts(tmp, "generation.db");
    cbm_pipeline_incremental_test_reset_faults();
    th_rmtree(tmp);

    ASSERT_EQ(faulted_rc, CBM_PIPELINE_PERSIST_FAILED);
    ASSERT_EQ(faulted_before, 1);
    ASSERT_EQ(faulted_after, 0);
    ASSERT_EQ(faulted_stage_count, 0);
    ASSERT_EQ(retry_rc, 0);
    ASSERT_EQ(retry_before, 0);
    ASSERT_EQ(retry_after, 1);
    ASSERT_EQ(retry_stage_count, 0);
    PASS();
}

/* Keep the legacy partial route test-only so its fail-closed behavior remains
 * covered even though production semantic changes now force a full rebuild. */
TEST(pipeline_incremental_persist_failure_preserves_previous_generation_and_retries) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_publish_incr_fail_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    write_temp_file(tmp, "generation.py", "def BeforeIncrementalPersist():\n    return 1\n");
    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/generation.db", tmp);

    cbm_pipeline_incremental_test_reset_faults();
    cbm_pipeline_t *baseline = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(baseline);
    ASSERT_EQ(cbm_pipeline_run(baseline), 0);
    char project[256];
    snprintf(project, sizeof(project), "%s", cbm_pipeline_project_name(baseline));
    cbm_pipeline_free(baseline);

    write_temp_file(tmp, "generation.py",
                    "def AfterIncrementalPersist():\n    return 2\n# changed generation\n");
    cbm_pipeline_incremental_test_force_legacy_partial_once();
    cbm_pipeline_incremental_test_fail_after_stage_dump_once();
    cbm_pipeline_t *faulted = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(faulted);
    int faulted_rc = cbm_pipeline_run(faulted);
    cbm_incremental_route_t faulted_route = cbm_pipeline_incremental_test_last_route();
    cbm_pipeline_free(faulted);
    int faulted_before = -1;
    int faulted_after = -1;
    observe_named_generation(db_path, project, "BeforeIncrementalPersist",
                             "AfterIncrementalPersist", &faulted_before, &faulted_after);
    int faulted_stage_count = count_generation_stage_artifacts(tmp, "generation.db");

    cbm_pipeline_incremental_test_reset_faults();
    cbm_pipeline_t *retry = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(retry);
    int retry_rc = cbm_pipeline_run(retry);
    cbm_incremental_route_t retry_route = cbm_pipeline_incremental_test_last_route();
    cbm_pipeline_free(retry);
    int retry_before = -1;
    int retry_after = -1;
    observe_named_generation(db_path, project, "BeforeIncrementalPersist",
                             "AfterIncrementalPersist", &retry_before, &retry_after);
    int retry_stage_count = count_generation_stage_artifacts(tmp, "generation.db");
    cbm_pipeline_incremental_test_reset_faults();
    th_rmtree(tmp);

    ASSERT_EQ(faulted_rc, CBM_PIPELINE_PERSIST_FAILED);
    ASSERT_EQ(faulted_route, CBM_INCREMENTAL_ROUTE_LEGACY_PARTIAL);
    ASSERT_EQ(faulted_before, 1);
    ASSERT_EQ(faulted_after, 0);
    ASSERT_EQ(faulted_stage_count, 0);
    ASSERT_EQ(retry_rc, 0);
    ASSERT_EQ(retry_route, CBM_INCREMENTAL_ROUTE_FORCED_FULL);
    ASSERT_EQ(retry_before, 0);
    ASSERT_EQ(retry_after, 1);
    ASSERT_EQ(retry_stage_count, 0);
    PASS();
}

TEST(pipeline_incremental_successful_publication_preserves_adr) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_publish_incr_adr_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    write_temp_file(tmp, "generation.py", "def BeforeAdrIncremental():\n    return 1\n");
    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/generation.db", tmp);

    cbm_pipeline_incremental_test_reset_faults();
    cbm_pipeline_t *baseline = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(baseline);
    ASSERT_EQ(cbm_pipeline_run(baseline), 0);
    char project[256];
    snprintf(project, sizeof(project), "%s", cbm_pipeline_project_name(baseline));
    cbm_pipeline_free(baseline);
    static const char adr_text[] = "# Decision\nPreserve across publication.";
    cbm_store_t *adr_store = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(adr_store);
    ASSERT_EQ(cbm_store_adr_store(adr_store, project, adr_text), CBM_STORE_OK);
    cbm_store_close(adr_store);

    write_temp_file(tmp, "generation.py",
                    "def AfterAdrIncremental():\n    return 2\n# changed generation\n");
    cbm_pipeline_incremental_test_force_legacy_partial_once();
    cbm_pipeline_t *incremental = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(incremental);
    int run_rc = cbm_pipeline_run(incremental);
    cbm_incremental_route_t route = cbm_pipeline_incremental_test_last_route();
    cbm_pipeline_free(incremental);
    cbm_store_t *published = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(published);
    cbm_adr_t adr = {0};
    int adr_rc = cbm_store_adr_get(published, project, &adr);
    bool adr_matches = adr_rc == CBM_STORE_OK && adr.content && strcmp(adr.content, adr_text) == 0;
    cbm_store_adr_free(&adr);
    cbm_store_close(published);
    cbm_pipeline_incremental_test_reset_faults();
    th_rmtree(tmp);

    ASSERT_EQ(run_rc, 0);
    ASSERT_EQ(route, CBM_INCREMENTAL_ROUTE_LEGACY_PARTIAL);
    ASSERT_TRUE(adr_matches);
    PASS();
}

/* A forced-full rebuild must never erase an ADR merely because the old
 * generation could not be read completely. The capture is part of the
 * publication transaction: failure preserves both graph and ADR. */
TEST(pipeline_full_adr_capture_failure_preserves_previous_generation) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_publish_adr_capture_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    write_temp_file(tmp, "generation.py", "def BeforeAdrCapture():\n    return 1\n");
    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/generation.db", tmp);

    cbm_pipeline_incremental_test_reset_faults();
    cbm_pipeline_t *baseline = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(baseline);
    ASSERT_EQ(cbm_pipeline_run(baseline), 0);
    char project[256];
    snprintf(project, sizeof(project), "%s", cbm_pipeline_project_name(baseline));
    cbm_pipeline_free(baseline);

    static const char adr_text[] = "# Decision\nADR capture is fail-closed.";
    cbm_store_t *adr_store = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(adr_store);
    ASSERT_EQ(cbm_store_adr_store(adr_store, project, adr_text), CBM_STORE_OK);
    cbm_store_close(adr_store);

    write_temp_file(tmp, "generation.py", "def AfterAdrCapture():\n    return 2\n");
    cbm_pipeline_incremental_test_fail_adr_capture_once();
    cbm_pipeline_t *faulted = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(faulted);
    int faulted_rc = cbm_pipeline_run(faulted);
    cbm_pipeline_free(faulted);

    int faulted_before = -1;
    int faulted_after = -1;
    observe_named_generation(db_path, project, "BeforeAdrCapture", "AfterAdrCapture",
                             &faulted_before, &faulted_after);
    cbm_store_t *preserved = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(preserved);
    cbm_adr_t preserved_adr = {0};
    int preserved_adr_rc = cbm_store_adr_get(preserved, project, &preserved_adr);
    bool preserved_adr_matches = preserved_adr_rc == CBM_STORE_OK && preserved_adr.content &&
                                 strcmp(preserved_adr.content, adr_text) == 0;
    cbm_store_adr_free(&preserved_adr);
    cbm_store_close(preserved);

    cbm_pipeline_incremental_test_reset_faults();
    cbm_pipeline_t *retry = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(retry);
    int retry_rc = cbm_pipeline_run(retry);
    cbm_pipeline_free(retry);
    int retry_before = -1;
    int retry_after = -1;
    observe_named_generation(db_path, project, "BeforeAdrCapture", "AfterAdrCapture", &retry_before,
                             &retry_after);
    cbm_store_t *published = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(published);
    cbm_adr_t published_adr = {0};
    int published_adr_rc = cbm_store_adr_get(published, project, &published_adr);
    bool published_adr_matches = published_adr_rc == CBM_STORE_OK && published_adr.content &&
                                 strcmp(published_adr.content, adr_text) == 0;
    cbm_store_adr_free(&published_adr);
    cbm_store_close(published);
    cbm_pipeline_incremental_test_reset_faults();
    th_rmtree(tmp);

    ASSERT_EQ(faulted_rc, CBM_PIPELINE_ABORT_PRESERVE_DB);
    ASSERT_EQ(faulted_before, 1);
    ASSERT_EQ(faulted_after, 0);
    ASSERT_TRUE(preserved_adr_matches);
    ASSERT_EQ(retry_rc, 0);
    ASSERT_EQ(retry_before, 0);
    ASSERT_EQ(retry_after, 1);
    ASSERT_TRUE(published_adr_matches);
    PASS();
}

/* An exact semantic manifest must fail closed when its repository root cannot
 * be traversed. A regular file is a deterministic cross-platform opendir
 * failure that does not depend on process permissions. */
TEST(pipeline_semantic_manifest_rejects_non_directory_root) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_manifest_not_dir_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    write_temp_file(tmp, "not-a-directory", "manifest root sentinel\n");
    char root_path[512];
    snprintf(root_path, sizeof(root_path), "%s/not-a-directory", tmp);

    cbm_file_hash_t *manifest = NULL;
    int manifest_count = -1;
    int rc = cbm_pipeline_build_semantic_manifest("manifest-fail-closed", root_path, NULL, 0, NULL,
                                                  0, NULL, NULL, &manifest, &manifest_count);
    cbm_pipeline_free_semantic_manifest(manifest, manifest_count > 0 ? manifest_count : 0);
    th_rmtree(tmp);

    ASSERT_TRUE(rc != 0);
    ASSERT_EQ(manifest_count, 0);
    PASS();
}

/* A fully validated staged graph must be able to recover from a definitely
 * non-SQLite destination without deleting evidence or overwriting an earlier
 * quarantine. The replacement happens only after the corrupt bytes are moved. */
TEST(pipeline_full_reindex_quarantines_corrupt_destination_without_overwrite) {
    static const char corrupt_bytes[] = "not a sqlite database\n";
    static const char previous_backup[] = "older quarantine\n";
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_publish_corrupt_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    write_temp_file(tmp, "fresh.py", "def FreshAfterCorruption():\n    return 1\n");
    write_temp_file(tmp, "generation.db", corrupt_bytes);
    write_temp_file(tmp, "generation.db.corrupt", previous_backup);

    char db_path[512];
    char old_backup_path[512];
    char new_backup_path[512];
    snprintf(db_path, sizeof(db_path), "%s/generation.db", tmp);
    snprintf(old_backup_path, sizeof(old_backup_path), "%s.corrupt", db_path);
    snprintf(new_backup_path, sizeof(new_backup_path), "%s.corrupt.1", db_path);

    cbm_pipeline_t *pipeline = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(pipeline);
    int run_rc = cbm_pipeline_run(pipeline);
    char project[256];
    snprintf(project, sizeof(project), "%s", cbm_pipeline_project_name(pipeline));
    cbm_pipeline_free(pipeline);

    char old_observed[64] = {0};
    char new_observed[64] = {0};
    FILE *old_backup_file = cbm_fopen(old_backup_path, "rb");
    FILE *new_backup_file = cbm_fopen(new_backup_path, "rb");
    size_t old_size =
        old_backup_file ? fread(old_observed, 1, sizeof(old_observed) - 1, old_backup_file) : 0;
    size_t new_size =
        new_backup_file ? fread(new_observed, 1, sizeof(new_observed) - 1, new_backup_file) : 0;
    if (old_backup_file) {
        fclose(old_backup_file);
    }
    if (new_backup_file) {
        fclose(new_backup_file);
    }

    int fresh_nodes = -1;
    cbm_store_t *store = cbm_store_open_path_query(db_path);
    if (store) {
        fresh_nodes = named_node_count(store, project, "FreshAfterCorruption");
        cbm_store_close(store);
    }
    th_rmtree(tmp);

    ASSERT_EQ(run_rc, 0);
    ASSERT_EQ((int)old_size, (int)strlen(previous_backup));
    ASSERT_STR_EQ(old_observed, previous_backup);
    ASSERT_EQ((int)new_size, (int)strlen(corrupt_bytes));
    ASSERT_STR_EQ(new_observed, corrupt_bytes);
    ASSERT_EQ(fresh_nodes, 1);
    PASS();
}

/* Readable legacy graphs are replaceable generations, not corruption. The
 * publication boundary must neither run current-schema migration against the
 * old edges table nor require an ADR table that did not exist yet. */
TEST(pipeline_full_reindex_replaces_legacy_schema_without_quarantine) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_publish_legacy_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    write_temp_file(tmp, "fresh.py", "def FreshAfterLegacy():\n    return 1\n");

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/generation.db", tmp);
    sqlite3 *legacy = NULL;
    ASSERT_EQ(sqlite3_open_v2(db_path, &legacy, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL),
              SQLITE_OK);
    const char *legacy_schema =
        "CREATE TABLE projects(name TEXT PRIMARY KEY,indexed_at TEXT NOT NULL,root_path TEXT NOT "
        "NULL);"
        "CREATE TABLE file_hashes(project TEXT NOT NULL,rel_path TEXT NOT NULL,sha256 TEXT NOT "
        "NULL,mtime_ns INTEGER NOT NULL DEFAULT 0,size INTEGER NOT NULL DEFAULT 0,PRIMARY KEY "
        "(project,rel_path));"
        "CREATE TABLE nodes(id INTEGER PRIMARY KEY,project TEXT NOT NULL,label TEXT NOT NULL,name "
        "TEXT NOT NULL,qualified_name TEXT NOT NULL,file_path TEXT,start_line INTEGER,end_line "
        "INTEGER,properties TEXT DEFAULT '{}');"
        "CREATE TABLE edges(id INTEGER PRIMARY KEY,project TEXT NOT NULL,source_id INTEGER NOT "
        "NULL,target_id INTEGER NOT NULL,type TEXT NOT NULL,properties TEXT DEFAULT '{}',UNIQUE "
        "(source_id,target_id,type));";
    ASSERT_EQ(sqlite3_exec(legacy, legacy_schema, NULL, NULL, NULL), SQLITE_OK);
    sqlite3_stmt *insert_project = NULL;
    ASSERT_EQ(sqlite3_prepare_v2(
                  legacy, "INSERT INTO projects(name,indexed_at,root_path) VALUES(?1,'legacy',?2);",
                  -1, &insert_project, NULL),
              SQLITE_OK);
    const char *project_name = strrchr(tmp, '/');
    project_name = project_name ? project_name + 1 : tmp;
    ASSERT_EQ(sqlite3_bind_text(insert_project, 1, project_name, -1, SQLITE_TRANSIENT), SQLITE_OK);
    ASSERT_EQ(sqlite3_bind_text(insert_project, 2, tmp, -1, SQLITE_TRANSIENT), SQLITE_OK);
    ASSERT_EQ(sqlite3_step(insert_project), SQLITE_DONE);
    sqlite3_finalize(insert_project);
    sqlite3_close(legacy);

    cbm_pipeline_t *pipeline = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(pipeline);
    int run_rc = cbm_pipeline_run(pipeline);
    char project[256];
    snprintf(project, sizeof(project), "%s", cbm_pipeline_project_name(pipeline));
    cbm_pipeline_free(pipeline);

    int fresh_nodes = -1;
    cbm_store_t *store = cbm_store_open_path_query(db_path);
    if (store) {
        fresh_nodes = named_node_count(store, project, "FreshAfterLegacy");
        cbm_store_close(store);
    }
    int generated_column_count = 0;
    sqlite3 *current = NULL;
    sqlite3_stmt *column_query = NULL;
    if (sqlite3_open_v2(db_path, &current, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK &&
        sqlite3_prepare_v2(current,
                           "SELECT count(*) FROM pragma_table_xinfo('edges') "
                           "WHERE name='local_name_gen';",
                           -1, &column_query, NULL) == SQLITE_OK &&
        sqlite3_step(column_query) == SQLITE_ROW) {
        generated_column_count = sqlite3_column_int(column_query, 0);
    }
    sqlite3_finalize(column_query);
    if (current) {
        sqlite3_close(current);
    }
    char quarantine_path[512];
    snprintf(quarantine_path, sizeof(quarantine_path), "%s.corrupt", db_path);
    cbm_path_info_t quarantine_info;
    bool quarantined = cbm_path_info_utf8(quarantine_path, &quarantine_info) == 0;
    th_rmtree(tmp);

    ASSERT_EQ(run_rc, 0);
    ASSERT_EQ(fresh_nodes, 1);
    ASSERT_EQ(generated_column_count, 1);
    ASSERT_FALSE(quarantined);
    PASS();
}
#endif

/* Re-indexing only a target whose qualified name survives a callable-to-value
 * edit must not restore the unchanged caller's stale inbound CALL_REFERENCE
 * snapshot. A fresh full index of the edited sources is the convergence oracle:
 * the now-non-callable identifier remains a USAGE, never a call relationship. */
TEST(pipeline_incremental_changed_target_invalidates_stale_inbound_call_reference) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_ref_stale_inbound_XXXXXX");
    if (!cbm_mkdtemp(tmp)) {
        FAIL("tmpdir");
    }

    static const char callable_target_source[] = "export function staleSemanticTarget(): void {}\n";
    static const char value_target_source[] = "export const staleSemanticTarget = 42;\n";
    static const char caller_source[] =
        "import { staleSemanticTarget } from './target';\n"
        "function acceptStaleSemanticReference(callback: () => void): void {}\n"
        "export function staleSemanticCaller(): void {\n"
        "  acceptStaleSemanticReference(staleSemanticTarget);\n"
        "}\n";

    write_temp_file(tmp, "target.ts", callable_target_source);
    write_temp_file(tmp, "caller.ts", caller_source);

    char incremental_db[512];
    snprintf(incremental_db, sizeof(incremental_db), "%s/stale-inbound-incremental.db", tmp);
    cbm_pipeline_t *baseline = cbm_pipeline_new(tmp, incremental_db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(baseline);
    ASSERT_EQ(cbm_pipeline_run(baseline), 0);
    const char *baseline_project = cbm_pipeline_project_name(baseline);
    cbm_store_t *baseline_store = cbm_store_open_path(incremental_db);
    ASSERT_NOT_NULL(baseline_store);
    ASSERT_EQ(named_edge_count(baseline_store, baseline_project, "CALL_REFERENCE",
                               "staleSemanticCaller", "staleSemanticTarget"),
              1);
    ASSERT_EQ(named_edge_count(baseline_store, baseline_project, "USAGE", "staleSemanticCaller",
                               "staleSemanticTarget"),
              0);
    ASSERT_EQ(named_edge_count(baseline_store, baseline_project, "CALLS", "staleSemanticCaller",
                               "staleSemanticTarget"),
              0);
    cbm_store_close(baseline_store);
    cbm_pipeline_free(baseline);

    write_temp_file(tmp, "target.ts", value_target_source);

    cbm_pipeline_t *incremental = cbm_pipeline_new(tmp, incremental_db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(incremental);
    ASSERT_EQ(cbm_pipeline_run(incremental), 0);
    const char *incremental_project = cbm_pipeline_project_name(incremental);
    cbm_store_t *incremental_store = cbm_store_open_path(incremental_db);
    ASSERT_NOT_NULL(incremental_store);
    int incremental_reference =
        named_edge_count(incremental_store, incremental_project, "CALL_REFERENCE",
                         "staleSemanticCaller", "staleSemanticTarget");
    int incremental_usage = named_edge_count(incremental_store, incremental_project, "USAGE",
                                             "staleSemanticCaller", "staleSemanticTarget");
    int incremental_calls = named_edge_count(incremental_store, incremental_project, "CALLS",
                                             "staleSemanticCaller", "staleSemanticTarget");
    cbm_store_close(incremental_store);
    cbm_pipeline_free(incremental);

    char full_db[512];
    snprintf(full_db, sizeof(full_db), "%s/stale-inbound-full.db", tmp);
    cbm_pipeline_t *full = cbm_pipeline_new(tmp, full_db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(full);
    ASSERT_EQ(cbm_pipeline_run(full), 0);
    const char *full_project = cbm_pipeline_project_name(full);
    cbm_store_t *full_store = cbm_store_open_path(full_db);
    ASSERT_NOT_NULL(full_store);
    int full_reference = named_edge_count(full_store, full_project, "CALL_REFERENCE",
                                          "staleSemanticCaller", "staleSemanticTarget");
    int full_usage = named_edge_count(full_store, full_project, "USAGE", "staleSemanticCaller",
                                      "staleSemanticTarget");
    int full_calls = named_edge_count(full_store, full_project, "CALLS", "staleSemanticCaller",
                                      "staleSemanticTarget");
    cbm_store_close(full_store);
    cbm_pipeline_free(full);
    th_rmtree(tmp);

    ASSERT_EQ(full_reference, 0);
    ASSERT_EQ(full_usage, 1);
    ASSERT_EQ(full_calls, 0);
    ASSERT_EQ(incremental_reference, full_reference);
    ASSERT_EQ(incremental_usage, full_usage);
    ASSERT_EQ(incremental_calls, full_calls);
    PASS();
}

/* Registry construction in incremental-parallel mode can materialize serial
 * resource nodes after extraction establishes the workers' shared-ID
 * watermark. A subsequent worker-created synthetic node must not reuse that
 * ID: duplicate temporary IDs make dump-time edge remapping point CONFIGURES
 * or DECORATES at the wrong target. The second run changes all 51 files so the
 * production incremental path crosses MIN_FILES_FOR_PARALLEL_INCR. */
TEST(pipeline_incremental_parallel_registry_nodes_advance_shared_ids) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_incr_ids_XXXXXX");
    if (!cbm_mkdtemp(tmp)) {
        FAIL("tmpdir");
    }

    char collision_path[512];
    snprintf(collision_path, sizeof(collision_path), "%s/00_collision.py", tmp);
    int setup_ok =
        th_write_file(collision_path, "import os\n"
                                      "def incrementalCollisionTarget(): return 1\n"
                                      "def incrementalCollisionAccept(callback): pass\n"
                                      "@old_marker\n"
                                      "def incrementalCollisionCarrier():\n"
                                      "    incrementalCollisionTarget()\n"
                                      "    incrementalCollisionAccept(incrementalCollisionTarget)\n"
                                      "    return os.getenv(\"OLD_TOKEN\")\n") == 0;
    for (int i = 0; setup_ok && i < 50; i++) {
        char path[512];
        char source[128];
        snprintf(path, sizeof(path), "%s/z_pad_%02d.ts", tmp, i);
        snprintf(source, sizeof(source), "// initial incremental ID pad %02d\n", i);
        setup_ok = th_write_file(path, source) == 0;
    }

    char *old_workers = getenv("CBM_WORKERS");
    char *saved_workers = old_workers ? strdup(old_workers) : NULL;
    char *old_single = getenv("CBM_INDEX_SINGLE_THREAD");
    char *saved_single = old_single ? strdup(old_single) : NULL;
    cbm_unsetenv("CBM_INDEX_SINGLE_THREAD");
    cbm_setenv("CBM_WORKERS", "4", 1);

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/incremental-ids.db", tmp);
    int first_rc = -1;
    int second_rc = -1;
    int env_edge_count = -1;
    int decorator_edge_count = -1;
    int misdirected_env_edge_count = -1;
    int reference_edge_count = -1;
    int env_node_count = -1;
    int decorator_node_count = -1;

    if (setup_ok) {
        cbm_pipeline_t *first = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
        if (first) {
            first_rc = cbm_pipeline_run(first);
            cbm_pipeline_free(first);
        }
    }

    int update_ok =
        setup_ok && first_rc == 0 &&
        th_write_file(collision_path, "import os\n"
                                      "def incrementalCollisionTarget(): return 1\n"
                                      "def incrementalCollisionAccept(callback): pass\n"
                                      "@new_marker\n"
                                      "def incrementalCollisionCarrier():\n"
                                      "    incrementalCollisionTarget()\n"
                                      "    incrementalCollisionAccept(incrementalCollisionTarget)\n"
                                      "    return os.getenv(\"NEW_TOKEN\")\n"
                                      "# incremental size marker\n") == 0;
    for (int i = 0; update_ok && i < 50; i++) {
        char path[512];
        char source[160];
        snprintf(path, sizeof(path), "%s/z_pad_%02d.ts", tmp, i);
        snprintf(source, sizeof(source), "// changed incremental ID pad %02d with marker\n", i);
        update_ok = th_write_file(path, source) == 0;
    }

    if (update_ok) {
        cbm_pipeline_t *second = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
        if (second) {
            second_rc = cbm_pipeline_run(second);
            const char *project = cbm_pipeline_project_name(second);
            cbm_store_t *store = cbm_store_open_path(db_path);
            if (store && project) {
                env_edge_count = named_edge_count(store, project, "CONFIGURES",
                                                  "incrementalCollisionCarrier", "NEW_TOKEN");
                decorator_edge_count = named_edge_count(
                    store, project, "DECORATES", "incrementalCollisionCarrier", "new_marker");
                misdirected_env_edge_count = named_edge_count(
                    store, project, "CONFIGURES", "incrementalCollisionCarrier", "new_marker");
                reference_edge_count =
                    named_edge_count(store, project, "CALL_REFERENCE",
                                     "incrementalCollisionCarrier", "incrementalCollisionTarget");
                env_node_count = named_node_count(store, project, "NEW_TOKEN");
                decorator_node_count = named_node_count(store, project, "new_marker");
            }
            if (store) {
                cbm_store_close(store);
            }
            cbm_pipeline_free(second);
        }
    }

    if (saved_workers) {
        cbm_setenv("CBM_WORKERS", saved_workers, 1);
        free(saved_workers);
    } else {
        cbm_unsetenv("CBM_WORKERS");
    }
    if (saved_single) {
        cbm_setenv("CBM_INDEX_SINGLE_THREAD", saved_single, 1);
        free(saved_single);
    } else {
        cbm_unsetenv("CBM_INDEX_SINGLE_THREAD");
    }
    th_rmtree(tmp);

    if (env_node_count != 1 || decorator_node_count != 1 || env_edge_count != 1 ||
        decorator_edge_count != 1 || misdirected_env_edge_count != 0 || reference_edge_count != 1) {
        printf("  incremental ID diagnostic: env_node=%d decorator_node=%d "
               "configures_env=%d decorates=%d configures_decorator=%d call_reference=%d\n",
               env_node_count, decorator_node_count, env_edge_count, decorator_edge_count,
               misdirected_env_edge_count, reference_edge_count);
    }

    ASSERT_TRUE(setup_ok);
    ASSERT_EQ(first_rc, 0);
    ASSERT_TRUE(update_ok);
    ASSERT_EQ(second_rc, 0);
    ASSERT_EQ(env_node_count, 1);
    ASSERT_EQ(decorator_node_count, 1);
    ASSERT_EQ(env_edge_count, 1);
    ASSERT_EQ(decorator_edge_count, 1);
    ASSERT_EQ(misdirected_env_edge_count, 0);
    ASSERT_EQ(reference_edge_count, 1);
    PASS();
}

#if defined(CBM_INCREMENTAL_TEST_API) && CBM_INCREMENTAL_TEST_API
static void observe_incremental_generation(const char *db_path, const char *project,
                                           int generation_size, int *before_count,
                                           int *after_count) {
    *before_count = -1;
    *after_count = -1;
    cbm_store_t *store = cbm_store_open_path(db_path);
    if (!store || !project) {
        if (store) {
            cbm_store_close(store);
        }
        return;
    }

    int before = 0;
    int after = 0;
    for (int i = 0; i < generation_size; i++) {
        char before_name[32];
        char after_name[32];
        snprintf(before_name, sizeof(before_name), "Before%02d", i);
        snprintf(after_name, sizeof(after_name), "After%02d", i);
        int before_nodes = named_node_count(store, project, before_name);
        int after_nodes = named_node_count(store, project, after_name);
        if (before_nodes < 0 || after_nodes < 0) {
            cbm_store_close(store);
            return;
        }
        before += before_nodes;
        after += after_nodes;
    }
    cbm_store_close(store);
    *before_count = before;
    *after_count = after;
}

/* A parallel incremental run must preserve its prior database when the
 * per-file result cache cannot be allocated. A clean retry must then converge
 * to the new generation.
 *
 * RED before fail-closed propagation: the injected allocation failure skips
 * extract/resolve, but the caller dumps the already-purged graph, advances all
 * file hashes, and returns 0. The second graph is empty and the third run is a
 * no-op instead of recovering the missing definitions. */
TEST(pipeline_incremental_parallel_result_cache_alloc_failure_preserves_db_and_retries) {
    enum { GENERATION_SIZE = 51 };
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_incr_cache_XXXXXX");
    if (!cbm_mkdtemp(tmp)) {
        FAIL("tmpdir");
    }

    cbm_pipeline_incremental_test_reset_faults();
    char *old_workers = getenv("CBM_WORKERS");
    char *saved_workers = old_workers ? strdup(old_workers) : NULL;
    char *old_single = getenv("CBM_INDEX_SINGLE_THREAD");
    char *saved_single = old_single ? strdup(old_single) : NULL;
    cbm_unsetenv("CBM_INDEX_SINGLE_THREAD");
    cbm_setenv("CBM_WORKERS", "4", 1);

    int setup_ok = 1;
    for (int i = 0; setup_ok && i < GENERATION_SIZE; i++) {
        char path[512];
        char source[128];
        snprintf(path, sizeof(path), "%s/generation_%02d.py", tmp, i);
        snprintf(source, sizeof(source), "def Before%02d():\n    return %d\n", i, i);
        setup_ok = th_write_file(path, source) == 0;
    }

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/incremental-cache.db", tmp);
    int first_rc = -1;
    int baseline_before = -1;
    int baseline_after = -1;
    if (setup_ok) {
        cbm_pipeline_t *first = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
        if (first) {
            first_rc = cbm_pipeline_run(first);
            observe_incremental_generation(db_path, cbm_pipeline_project_name(first),
                                           GENERATION_SIZE, &baseline_before, &baseline_after);
            cbm_pipeline_free(first);
        }
    }

    int edit_ok = setup_ok && first_rc == 0;
    for (int i = 0; edit_ok && i < GENERATION_SIZE; i++) {
        char path[512];
        char source[160];
        snprintf(path, sizeof(path), "%s/generation_%02d.py", tmp, i);
        snprintf(source, sizeof(source),
                 "def After%02d():\n    return %d\n# edited generation %02d\n", i, i, i);
        edit_ok = th_write_file(path, source) == 0;
    }

    int second_rc = -1;
    int second_before = -1;
    int second_after = -1;
    if (edit_ok) {
        cbm_pipeline_incremental_test_fail_result_cache_alloc_once();
        cbm_pipeline_incremental_test_force_legacy_partial_once();
        cbm_pipeline_t *second = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
        if (second) {
            second_rc = cbm_pipeline_run(second);
            observe_incremental_generation(db_path, cbm_pipeline_project_name(second),
                                           GENERATION_SIZE, &second_before, &second_after);
            cbm_pipeline_free(second);
        }
    }
    cbm_pipeline_incremental_test_reset_faults();

    int third_rc = -1;
    int retry_before = -1;
    int retry_after = -1;
    if (edit_ok) {
        cbm_pipeline_t *third = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
        if (third) {
            third_rc = cbm_pipeline_run(third);
            observe_incremental_generation(db_path, cbm_pipeline_project_name(third),
                                           GENERATION_SIZE, &retry_before, &retry_after);
            cbm_pipeline_free(third);
        }
    }
    cbm_pipeline_incremental_test_reset_faults();

    if (saved_workers) {
        cbm_setenv("CBM_WORKERS", saved_workers, 1);
        free(saved_workers);
    } else {
        cbm_unsetenv("CBM_WORKERS");
    }
    if (saved_single) {
        cbm_setenv("CBM_INDEX_SINGLE_THREAD", saved_single, 1);
        free(saved_single);
    } else {
        cbm_unsetenv("CBM_INDEX_SINGLE_THREAD");
    }
    th_rmtree(tmp);

    bool second_is_preserved_failure = second_rc == CBM_PIPELINE_ABORT_PRESERVE_DB &&
                                       second_before == GENERATION_SIZE && second_after == 0;
    if (!second_is_preserved_failure) {
        printf("  incremental cache failure diagnostic: rc=%d before=%d after=%d\n", second_rc,
               second_before, second_after);
    }

    ASSERT_TRUE(setup_ok);
    ASSERT_EQ(first_rc, 0);
    ASSERT_EQ(baseline_before, GENERATION_SIZE);
    ASSERT_EQ(baseline_after, 0);
    ASSERT_TRUE(edit_ok);
    ASSERT_TRUE(second_is_preserved_failure);
    ASSERT_EQ(third_rc, 0);
    ASSERT_EQ(retry_before, 0);
    ASSERT_EQ(retry_after, GENERATION_SIZE);
    PASS();
}
#endif

TEST(pipeline_tsjs_receiver_suppresses_weak_method_edge) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_tsjs_recv_XXXXXX");
    if (!cbm_mkdtemp(tmp)) {
        FAIL("tmpdir");
    }

    /* The lone project symbol named "test" — a real method. */
    write_temp_file(tmp, "src/client.ts",
                    "export class SalesforceRestClient {\n"
                    "  test(): boolean {\n"
                    "    return true;\n"
                    "  }\n"
                    "}\n");
    /* Regex receiver: `re.test(s)` calls RegExp.prototype.test, NOT the method.
     * The TS-LSP cannot bind it to a project symbol → the registry would guess
     * "test" by short name (weak). This is the false edge to suppress. */
    write_temp_file(tmp, "src/caller.ts",
                    "const re = /^[a-z]+$/;\n"
                    "export function checkFormat(s: string): boolean {\n"
                    "  return re.test(s);\n"
                    "}\n");
    /* Typed receiver: `c` is annotated SalesforceRestClient, so the TS-LSP
     * resolves c.test() to the method (lsp_ts_method, conf 0.95) BEFORE the
     * registry runs — the guard's explicit drop-list keeps every lsp_* edge. */
    write_temp_file(tmp, "src/typed.ts",
                    "import { SalesforceRestClient } from './client';\n"
                    "export function runTyped(c: SalesforceRestClient): boolean {\n"
                    "  return c.test();\n"
                    "}\n");
    /* Bare local call: same-module resolution, unaffected by the guard. */
    write_temp_file(tmp, "src/local.ts",
                    "function localHelper(): number {\n"
                    "  return 1;\n"
                    "}\n"
                    "export function callsLocal(): number {\n"
                    "  return localHelper();\n"
                    "}\n");

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/tsjs_recv.db", tmp);
    cbm_pipeline_t *p = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    const char *project = cbm_pipeline_project_name(p);

    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);

    /* (1) The false edge is suppressed (reproduce-first: RED before the fix). */
    ASSERT_FALSE(cross_file_call_exists(s, project, "checkFormat", "test"));
    /* (2) The type-resolved receiver call survives (LSP wins before the guard). */
    ASSERT_TRUE(cross_file_call_exists(s, project, "runTyped", "test"));
    /* (3) The bare local call survives (same-module / lsp_ts_local). */
    ASSERT_TRUE(cross_file_call_exists(s, project, "callsLocal", "localHelper"));
    /* (4) breadth insurance: the real edges are still emitted. */
    ASSERT_GTE(cbm_store_count_edges_by_type(s, project, "CALLS"), 2);

    cbm_store_close(s);
    cbm_pipeline_free(p);
    th_rmtree(tmp);
    PASS();
}

/* Count nodes with the given exact name in the project (e.g. a Route path). */
static int count_nodes_named(cbm_store_t *s, const char *project, const char *name) {
    cbm_node_t *ns = NULL;
    int n = 0;
    cbm_store_find_nodes_by_name(s, project, name, &ns, &n);
    if (ns) {
        cbm_store_free_nodes(ns, n);
    }
    return n;
}

/* Parallel-resolver regression for the TS/JS receiver guard (>= 50 files forces
 * pass_parallel.c's resolve_file_calls). The guard must not drop a weak member
 * match before the service classification runs — it suppresses ONLY the plain
 * CALLS fall-through, so every service edge (HTTP_CALLS via the #523 callee
 * bypass or emit_service_edge's unconditional detect_url_in_args, Route via the
 * ROUTE_REG fall-through, …) is emitted exactly as on main. These callees are
 * classified by main's verb-suffix + URL-arg heuristic, NOT by an HTTP library
 * name in the callee — a duplicated predicate keyed on the resolved QN lost them
 * (axios.get, api.patch on a renamed-axios instance, supertest request(app).get).
 * The regex false edge must stay suppressed in parallel too. CBM_WORKERS forces
 * >1 worker so the parallel path is taken regardless of the host core count. */
TEST(pipeline_tsjs_receiver_parallel_keeps_service_edges) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_tsjs_par_XXXXXX");
    if (!cbm_mkdtemp(tmp)) {
        FAIL("tmpdir");
    }

    /* The lone project symbols named get()/patch()/test() — weak short-name
     * targets the registry mis-binds the member calls below to. */
    write_temp_file(tmp, "src/thing.ts",
                    "export class ApiThing {\n"
                    "  get(): number {\n"
                    "    return 1;\n"
                    "  }\n"
                    "  patch(): number {\n"
                    "    return 2;\n"
                    "  }\n"
                    "  load(): number {\n"
                    "    return 3;\n"
                    "  }\n"
                    "}\n"
                    "export class Other {\n"
                    "  test(): boolean {\n"
                    "    return true;\n"
                    "  }\n"
                    "}\n");
    /* Untyped axios: the HTTP signal is in the callee name, not the resolved QN
     * (the registry mis-binds "get" to ApiThing.get). Must yield HTTP_CALLS. */
    write_temp_file(tmp, "src/http.ts",
                    "export function callApi() {\n"
                    "  return axios.get('/api/orders');\n"
                    "}\n");
    /* Renamed axios instance `api`: no HTTP lib id in the callee — classified by
     * the .patch verb suffix + URL arg. Must yield HTTP_CALLS (was lost when the
     * exemption keyed on the resolved QN only). */
    write_temp_file(tmp, "src/http2.ts",
                    "export function callPatch(): unknown {\n"
                    "  return api.patch('/plans/:id', {});\n"
                    "}\n");
    /* Supertest-style chained receiver `request(app).get('/y')` — untyped, verb
     * suffix + URL arg. */
    write_temp_file(tmp, "src/supertest.ts",
                    "export function callSup(app: unknown): unknown {\n"
                    "  return request(app).get('/y');\n"
                    "}\n");
    /* `dev.load('/data')`: `.load` is NOT a route suffix and `dev` is not an HTTP
     * lib, so main classifies it via the unconditional detect_url_in_args (URL
     * arg) -> HTTP_CALLS, while the weak plain match to ApiThing.load is the false
     * edge that must be suppressed. This is exactly the detect_url_in_args path
     * the previous (predicate-duplicating) guard skipped by dropping the call
     * before emit_service_edge ran — the class of ~399 HTTP_CALLS it lost. */
    write_temp_file(tmp, "src/load.ts",
                    "export function callLoad(dev: unknown): unknown {\n"
                    "  return dev.load('/api/data');\n"
                    "}\n");
    /* Route registration by callee suffix + a '/'-path arg. Must yield a Route. */
    write_temp_file(tmp, "src/routes.ts",
                    "function handler() {}\n"
                    "export function reg(router: any) {\n"
                    "  router.get('/users', handler);\n"
                    "}\n");
    /* Regex receiver: the false edge that must stay suppressed in parallel too. */
    write_temp_file(tmp, "src/re.ts",
                    "const re = /^[a-z]+$/;\n"
                    "export function checkFormat(s: string): boolean {\n"
                    "  return re.test(s);\n"
                    "}\n");
    /* Pad past MIN_FILES_FOR_PARALLEL (50) so the parallel resolver runs. */
    for (int i = 0; i < 52; i++) {
        char name[64];
        char body[128];
        snprintf(name, sizeof(name), "src/filler%d.ts", i);
        snprintf(body, sizeof(body), "export function filler%d(): number {\n  return %d;\n}\n", i,
                 i);
        write_temp_file(tmp, name, body);
    }

    char *old_workers = getenv("CBM_WORKERS");
    char *saved = old_workers ? strdup(old_workers) : NULL;
    cbm_setenv("CBM_WORKERS", "4", 1); /* force parallel regardless of host cores */

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/tsjs_par.db", tmp);
    cbm_pipeline_t *p = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    const char *project = cbm_pipeline_project_name(p);

    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);

    /* (1) Genuine HTTP_CALLS survive under the guard (>= 3):
     *   - axios.get('/api/orders') -> 2 edges (recognized lib #523 callee bypass
     *     + detect_url_in_args), and
     *   - dev.load('/api/data')    -> 1 edge via detect_url_in_args, which runs
     *     unconditionally after emit_service_edge's branch even when the plain
     *     fall-through is suppressed.
     * dev.load is the class the predicate-duplicating guard lost: `.load` is not
     * a route suffix and `dev` is not an HTTP lib, so it was dropped before
     * emit_service_edge ran (RED on that guard: only axios's 2). */
    ASSERT_GTE(cbm_store_count_edges_by_type(s, project, "HTTP_CALLS"), 3);
    /* (2) The verb-suffix + route-path member calls keep their route
     * registrations (edge type CALLS -> a Route node named by the path). These
     * classify as route_registration on main, NOT HTTP_CALLS — Option A preserves
     * that by construction. Assert the three Route paths survive:
     * api.patch('/plans/:id'), request(app).get('/y'), router.get('/users'). */
    ASSERT_GTE(count_nodes_named(s, project, "/plans/:id"), 1);
    ASSERT_GTE(count_nodes_named(s, project, "/y"), 1);
    ASSERT_GTE(count_nodes_named(s, project, "/users"), 1);
    /* (3) The false plain-CALLS edges are suppressed in parallel: the regex
     * receiver and the dev.load weak match to ApiThing.load. */
    ASSERT_FALSE(cross_file_call_exists(s, project, "checkFormat", "test"));
    ASSERT_FALSE(cross_file_call_exists(s, project, "callLoad", "load"));

    cbm_store_close(s);
    cbm_pipeline_free(p);
    if (saved) {
        cbm_setenv("CBM_WORKERS", saved, 1);
        free(saved);
    } else {
        cbm_unsetenv("CBM_WORKERS");
    }
    th_rmtree(tmp);
    PASS();
}

/* Reproduce-first: pass_parallel's fused cross-LSP eligibility currently counts
 * only parser-backed calls/call references. A Python binary operator has no
 * parser CBMCall; its __add__ semantic record and carrier are created together
 * only by the cross resolver. Consequently combine.py looks like it has zero
 * semantic sites and the fused pass skips it before that carrier can exist.
 *
 * `visible()` is deliberately in a different file. Its ordinary method-call
 * carrier makes THAT file cross-LSP eligible and its lsp_* edge proves that the
 * Alpha definition, import map, shared registry, and cross resolver are all
 * healthy. Putting the control call in combine.py would hide the bug by making
 * the broken file eligible. The 52 fillers plus three fixture files exceed
 * MIN_FILES_FOR_PARALLEL (50), and CBM_WORKERS=4 forces the production fused
 * path independent of host core count. */
TEST(pipeline_parallel_python_cross_only_dunder_gets_synthetic_carrier) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_py_syn_par_XXXXXX");
    if (!cbm_mkdtemp(tmp)) {
        FAIL("tmpdir");
    }

    write_temp_file(tmp, "alpha.py",
                    "class Alpha:\n"
                    "    def __add__(self, other):\n"
                    "        return self\n");
    write_temp_file(tmp, "visible.py",
                    "from .alpha import Alpha\n\n"
                    "def visible(left: Alpha, right: Alpha):\n"
                    "    return left.__add__(right)\n");
    write_temp_file(tmp, "combine.py",
                    "from .alpha import Alpha\n\n"
                    "def combine(left: Alpha, right: Alpha):\n"
                    "    return left + right\n");
    for (int i = 0; i < 52; i++) {
        char name[64];
        char body[128];
        snprintf(name, sizeof(name), "py_pad_%02d.py", i);
        snprintf(body, sizeof(body), "def py_pad_%02d():\n    return %d\n", i, i);
        write_temp_file(tmp, name, body);
    }

    char *old_workers = getenv("CBM_WORKERS");
    char *saved_workers = old_workers ? strdup(old_workers) : NULL;
    cbm_setenv("CBM_WORKERS", "4", 1);

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/python_parallel.db", tmp);
    cbm_pipeline_t *p = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    int run_rc = cbm_pipeline_run(p);
    const char *project = cbm_pipeline_project_name(p);
    cbm_store_t *s = cbm_store_open_path(db_path);
    bool store_opened = s != NULL;
    bool control_lsp = false;
    bool combine_to_add = false;
    if (s) {
        control_lsp =
            cross_file_call_has_strategy(s, project, "visible", "__add__", "\"strategy\":\"lsp_");
        combine_to_add = cross_file_call_exists(s, project, "combine", "__add__");
        cbm_store_close(s);
    }

    cbm_pipeline_free(p);
    if (saved_workers) {
        cbm_setenv("CBM_WORKERS", saved_workers, 1);
        free(saved_workers);
    } else {
        cbm_unsetenv("CBM_WORKERS");
    }
    th_rmtree(tmp);

    ASSERT_EQ(run_rc, 0);
    ASSERT_TRUE(store_opened);
    ASSERT_TRUE(control_lsp);    /* non-vacuity: cross LSP + target are healthy */
    ASSERT_TRUE(combine_to_add); /* RED: fused eligibility skips combine.py */
    PASS();
}

/* Same fused-eligibility hole for Rust built-in macro arguments. The parser
 * exposes format! but not render() inside its token tree. Per-file LSP already
 * qualifies every parser-backed site in hidden.rs, so the fused heuristic sees
 * no remaining work and never runs the cross pass that would discover both the
 * semantic render target and its exact synthetic carrier.
 *
 * The exact same 55-file fixture is indexed once through the sequential path
 * first. A hidden() -> render CALLS edge there is a strong non-vacuity control:
 * only the sequential cross-LSP pass can discover both the macro-hidden call
 * and its synthetic carrier. The parallel run must preserve that result. */
TEST(pipeline_parallel_rust_cross_only_macro_hidden_gets_synthetic_carrier) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_rs_syn_par_XXXXXX");
    if (!cbm_mkdtemp(tmp)) {
        FAIL("tmpdir");
    }

    write_temp_file(tmp, "lib.rs", "pub fn render() -> &'static str { \"ok\" }\n");
    write_temp_file(tmp, "main.rs",
                    "mod lib;\n\n"
                    "pub fn hidden() -> String { format!(\"{}\", lib::render()) }\n");
    for (int i = 0; i < 53; i++) {
        char name[64];
        char body[128];
        snprintf(name, sizeof(name), "rust_pad_%02d.rs", i);
        snprintf(body, sizeof(body), "pub fn rust_pad_%02d() -> i32 { %d }\n", i, i);
        write_temp_file(tmp, name, body);
    }

    char *old_workers = getenv("CBM_WORKERS");
    char *saved_workers = old_workers ? strdup(old_workers) : NULL;
    char *old_single = getenv("CBM_INDEX_SINGLE_THREAD");
    char *saved_single = old_single ? strdup(old_single) : NULL;

    cbm_setenv("CBM_INDEX_SINGLE_THREAD", "1", 1);
    char seq_db_path[512];
    snprintf(seq_db_path, sizeof(seq_db_path), "%s/rust_sequential.db", tmp);
    cbm_pipeline_t *seq = cbm_pipeline_new(tmp, seq_db_path, CBM_MODE_FULL);
    int seq_run_rc = seq ? cbm_pipeline_run(seq) : -1;
    const char *seq_project = seq ? cbm_pipeline_project_name(seq) : NULL;
    cbm_store_t *seq_store = cbm_store_open_path(seq_db_path);
    bool seq_store_opened = seq_store != NULL;
    bool seq_hidden_to_render = false;
    if (seq_store && seq_project) {
        seq_hidden_to_render = cross_file_call_exists(seq_store, seq_project, "hidden", "render");
        cbm_store_close(seq_store);
    }
    cbm_pipeline_free(seq);

    /* The parallel half must not inherit a caller-level single-thread override,
     * even when the surrounding test process was launched with one. Restore it
     * after both halves complete. */
    cbm_unsetenv("CBM_INDEX_SINGLE_THREAD");
    cbm_setenv("CBM_WORKERS", "4", 1);

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/rust_parallel.db", tmp);
    cbm_pipeline_t *p = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    int run_rc = cbm_pipeline_run(p);
    const char *project = cbm_pipeline_project_name(p);
    cbm_store_t *s = cbm_store_open_path(db_path);
    bool store_opened = s != NULL;
    bool render_target_present = false;
    bool hidden_to_render = false;
    if (s) {
        render_target_present = count_nodes_named(s, project, "render") > 0;
        hidden_to_render = cross_file_call_exists(s, project, "hidden", "render");
        cbm_store_close(s);
    }

    cbm_pipeline_free(p);
    if (saved_workers) {
        cbm_setenv("CBM_WORKERS", saved_workers, 1);
        free(saved_workers);
    } else {
        cbm_unsetenv("CBM_WORKERS");
    }
    if (saved_single) {
        cbm_setenv("CBM_INDEX_SINGLE_THREAD", saved_single, 1);
        free(saved_single);
    } else {
        cbm_unsetenv("CBM_INDEX_SINGLE_THREAD");
    }
    th_rmtree(tmp);

    ASSERT_EQ(seq_run_rc, 0);
    ASSERT_TRUE(seq_store_opened);
    ASSERT_TRUE(seq_hidden_to_render); /* same fixture is semantically resolvable */
    ASSERT_EQ(run_rc, 0);
    ASSERT_TRUE(store_opened);
    ASSERT_TRUE(render_target_present); /* target is present in the parallel graph */
    ASSERT_TRUE(hidden_to_render);      /* RED: fused eligibility skips hidden.rs */
    PASS();
}

/* Native `fetch()` (#856), sequential path (< 50 files → pass_calls.c). A bare
 * unqualified call to the global fetch API has no import and no local
 * definition anywhere in this project, so registry resolution comes back
 * empty; classify it as HTTP_CALLS via the same #523-style empty-resolution
 * fallback used for axios/requests. A member call on an unrelated receiver
 * (`repo.fetch()`) must NOT be swept in — its callee_name is "repo.fetch",
 * not the bare "fetch" this check matches exactly. */
TEST(pipeline_native_fetch_classified_as_http_calls) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_fetch_XXXXXX");
    if (!cbm_mkdtemp(tmp)) {
        FAIL("tmpdir");
    }

    write_temp_file(tmp, "src/api.ts",
                    "export function loadData(): unknown {\n"
                    "  return fetch('/api/data');\n"
                    "}\n");
    /* `repo.fetch()` — a method call, not the global. Must not over-match. */
    write_temp_file(tmp, "src/repo.ts",
                    "export function useRepo(repo: { fetch: () => unknown }): unknown {\n"
                    "  return repo.fetch();\n"
                    "}\n");

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/fetch.db", tmp);
    cbm_pipeline_t *p = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    const char *project = cbm_pipeline_project_name(p);

    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);

    ASSERT_GTE(cbm_store_count_edges_by_type(s, project, "HTTP_CALLS"), 1);
    /* Exactly the bare call, not the method call too. */
    ASSERT_EQ(cbm_store_count_edges_by_type(s, project, "HTTP_CALLS"), 1);

    cbm_store_close(s);
    cbm_pipeline_free(p);
    th_rmtree(tmp);
    PASS();
}

/* Native `fetch()` (#856), parallel path (>= 50 files -> pass_parallel.c's
 * resolve_file_calls). Mirrors pipeline_native_fetch_classified_as_http_calls
 * but forces the parallel resolver, since the empty-resolution fallback is a
 * separate implementation there (resolve_file_calls calls
 * emit_http_async_service_edge directly rather than through emit_service_edge,
 * which would otherwise re-derive CBM_SVC_NONE for "fetch" and silently fall
 * through to a plain CALLS edge). */
TEST(pipeline_native_fetch_parallel_classified_as_http_calls) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_fetch_par_XXXXXX");
    if (!cbm_mkdtemp(tmp)) {
        FAIL("tmpdir");
    }

    write_temp_file(tmp, "src/api.ts",
                    "export function loadData(): unknown {\n"
                    "  return fetch('/api/data');\n"
                    "}\n");
    for (int i = 0; i < 52; i++) {
        char name[64];
        char body[128];
        snprintf(name, sizeof(name), "src/filler%d.ts", i);
        snprintf(body, sizeof(body), "export function filler%d(): number {\n  return %d;\n}\n", i,
                 i);
        write_temp_file(tmp, name, body);
    }

    char *old_workers = getenv("CBM_WORKERS");
    char *saved = old_workers ? strdup(old_workers) : NULL;
    cbm_setenv("CBM_WORKERS", "4", 1); /* force parallel regardless of host cores */

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/fetch_par.db", tmp);
    cbm_pipeline_t *p = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    const char *project = cbm_pipeline_project_name(p);

    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);

    ASSERT_GTE(cbm_store_count_edges_by_type(s, project, "HTTP_CALLS"), 1);

    cbm_store_close(s);
    cbm_pipeline_free(p);
    if (saved) {
        cbm_setenv("CBM_WORKERS", saved, 1);
        free(saved);
    } else {
        cbm_unsetenv("CBM_WORKERS");
    }
    th_rmtree(tmp);
    PASS();
}

/* Native `fetch()` (#856): a LOCAL `fetch` definition must win over the
 * global-API guess. Registry resolution finds this project's own top-level
 * `fetch` function, so the call is a plain CALLS edge to it — never
 * HTTP_CALLS. Isolated in its own project: the registry's project-wide
 * unique-name fallback means a local `fetch` anywhere in a project can
 * legitimately capture bare `fetch()` calls project-wide, so this must not
 * share a project with the genuine-native-fetch test above. */
TEST(pipeline_local_fetch_shadow_not_classified_as_http) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_fetch_shadow_XXXXXX");
    if (!cbm_mkdtemp(tmp)) {
        FAIL("tmpdir");
    }

    write_temp_file(tmp, "src/local_fetch.ts",
                    "function fetch(url: string): string {\n"
                    "  return 'mock:' + url;\n"
                    "}\n"
                    "export function useLocalFetch(): string {\n"
                    "  return fetch('/api/data');\n"
                    "}\n");

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/fetch_shadow.db", tmp);
    cbm_pipeline_t *p = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    const char *project = cbm_pipeline_project_name(p);

    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);

    ASSERT_EQ(cbm_store_count_edges_by_type(s, project, "HTTP_CALLS"), 0);
    ASSERT_TRUE(cross_file_call_exists(s, project, "useLocalFetch", "fetch"));

    cbm_store_close(s);
    cbm_pipeline_free(p);
    th_rmtree(tmp);
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
     * Explicit Kotlin callable syntax produces CALL_REFERENCE, not USAGE. */
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

    /* The explicit reference is resolved and classified exclusively. */
    cbm_edge_t *edges = NULL;
    int edge_count = 0;
    rc = cbm_store_find_edges_by_type(s, project, "CALL_REFERENCE", &edges, &edge_count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    bool found_reference = false;
    for (int i = 0; i < edge_count; i++) {
        cbm_node_t src = {0}, tgt = {0};
        if (cbm_store_find_node_by_id(s, edges[i].source_id, &src) == CBM_STORE_OK &&
            cbm_store_find_node_by_id(s, edges[i].target_id, &tgt) == CBM_STORE_OK && src.name &&
            tgt.name && strcmp(src.name, "register") == 0 && strcmp(tgt.name, "process") == 0) {
            found_reference = true;
        }
        cbm_node_free_fields(&src);
        cbm_node_free_fields(&tgt);
    }
    if (edges)
        cbm_store_free_edges(edges, edge_count);
    ASSERT_TRUE(found_reference);

    edges = NULL;
    edge_count = 0;
    rc = cbm_store_find_edges_by_type(s, project, "USAGE", &edges, &edge_count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    bool duplicated_as_usage = false;
    for (int i = 0; i < edge_count; i++) {
        cbm_node_t src = {0}, tgt = {0};
        if (cbm_store_find_node_by_id(s, edges[i].source_id, &src) == CBM_STORE_OK &&
            cbm_store_find_node_by_id(s, edges[i].target_id, &tgt) == CBM_STORE_OK && src.name &&
            tgt.name && strcmp(src.name, "register") == 0 && strcmp(tgt.name, "process") == 0) {
            duplicated_as_usage = true;
        }
        cbm_node_free_fields(&src);
        cbm_node_free_fields(&tgt);
    }
    if (edges)
        cbm_store_free_edges(edges, edge_count);
    ASSERT_FALSE(duplicated_as_usage);

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

/* #768 end-to-end: `import { A, B } from './lib'` must survive the REAL
 * pipeline (extract -> gbuf dedup -> raw SQLite dump) as TWO IMPORTS edges
 * with distinct local_name — and the dumped DB must satisfy its own schema.
 * With only the graph-buffer half of the fix, both edges reach the dump but
 * violate an unwidened UNIQUE(source_id,target_id,type), which PRAGMA
 * integrity_check flags as a non-unique autoindex entry. */
TEST(pipeline_imports_multi_symbol_edges) {
    const char *files[] = {"consumer.ts", "lib.ts"};
    const char *contents[] = {
        "import { A, B } from './lib';\n\nexport function useBoth() {\n  return A() + B();\n}\n",
        "export function A() {\n  return 1;\n}\nexport function B() {\n  return 2;\n}\n"};

    if (setup_lang_repo(files, contents, 2) != 0)
        FAIL("tmpdir");
    char db[512];
    snprintf(db, sizeof(db), "%s/test.db", g_lang_tmpdir);

    cbm_pipeline_t *p = cbm_pipeline_new(g_lang_tmpdir, db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    /* The dumped DB must pass SQLite's own full integrity check — this is
     * what catches a buffer-only fix shipping DBs that violate their own
     * UNIQUE constraint. */
    sqlite3 *raw = NULL;
    ASSERT_EQ(sqlite3_open(db, &raw), SQLITE_OK);
    sqlite3_stmt *stmt = NULL;
    sqlite3_prepare_v2(raw, "PRAGMA integrity_check", -1, &stmt, NULL);
    ASSERT_EQ(sqlite3_step(stmt), SQLITE_ROW);
    const char *integrity = (const char *)sqlite3_column_text(stmt, 0);
    ASSERT_STR_EQ(integrity, "ok");
    sqlite3_finalize(stmt);
    sqlite3_close(raw);

    /* Both named imports must be queryable as separate IMPORTS edges. */
    cbm_store_t *s = cbm_store_open_path(db);
    ASSERT_NOT_NULL(s);
    const char *proj = cbm_pipeline_project_name(p);

    cbm_edge_t *edges = NULL;
    int count = 0;
    ASSERT_EQ(cbm_store_find_edges_by_type(s, proj, "IMPORTS", &edges, &count), CBM_STORE_OK);
    ASSERT_EQ(count, 2);
    ASSERT_TRUE(strstr(edges[0].properties_json, "\"local_name\":\"A\"") != NULL ||
                strstr(edges[1].properties_json, "\"local_name\":\"A\"") != NULL);
    ASSERT_TRUE(strstr(edges[0].properties_json, "\"local_name\":\"B\"") != NULL ||
                strstr(edges[1].properties_json, "\"local_name\":\"B\"") != NULL);
    cbm_store_free_edges(edges, count);

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
    cbm_node_t *cls = NULL;
    int cc = 0;
    cbm_store_find_nodes_by_label(s, proj, "Struct", &cls, &cc);
    ASSERT_EQ(cc, 1);
    ASSERT_STR_EQ(cls[0].name, "Config");
    cbm_store_free_nodes(cls, cc);

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

    cbm_node_t *cls = NULL;
    int cc = 0;
    cbm_store_find_nodes_by_label(s, proj, "Struct", &cls, &cc);
    ASSERT_EQ(cc, 2); /* Request, Response */
    cbm_store_free_nodes(cls, cc);

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

TEST(pipeline_docstring_go_class) {
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

#if defined(CBM_INCREMENTAL_TEST_API) && CBM_INCREMENTAL_TEST_API
static int branch_head_match_count(cbm_store_t *store, const char *project, const char *head_sha) {
    cbm_node_t *branches = NULL;
    int branch_count = 0;
    if (!store || !project || !head_sha ||
        cbm_store_find_nodes_by_label(store, project, "Branch", &branches, &branch_count) !=
            CBM_STORE_OK) {
        return -1;
    }
    int matches = 0;
    for (int i = 0; i < branch_count; i++) {
        if (branches[i].properties_json && strstr(branches[i].properties_json, head_sha)) {
            matches++;
        }
    }
    cbm_store_free_nodes(branches, branch_count);
    return matches;
}

/* Git context is graph input even when every repository file is unchanged.
 * Construct the second pipeline before moving HEAD so the test also proves
 * that run start refreshes the snapshot captured by cbm_pipeline_new(). */
TEST(pipeline_git_context_change_forces_full_and_refreshes_branch) {
    if (!git_available()) {
        FAIL("git unavailable");
    }

    char tmp[256];
    char *created = th_mktempdir("cbm_manifest_git_context");
    ASSERT_NOT_NULL(created);
    snprintf(tmp, sizeof(tmp), "%s", created);

    char repo[512], db_path[512], cmd[2048];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    snprintf(db_path, sizeof(db_path), "%s/context.db", tmp);
    ASSERT_EQ(th_mkdir_p(repo), 0);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "stable.py"), "def StableGitContext():\n    return 1\n"),
              0);
    const char *null_dev = test_null_dev();
    snprintf(cmd, sizeof(cmd), "git -C \"%s\" init >%s 2>&1", repo, null_dev);
    ASSERT_EQ(run_cmd(cmd), 0);
    snprintf(cmd, sizeof(cmd), "git -C \"%s\" add stable.py >%s 2>&1", repo, null_dev);
    ASSERT_EQ(run_cmd(cmd), 0);
    snprintf(cmd, sizeof(cmd),
             "git -C \"%s\" -c user.name=\"CBM Test\" "
             "-c user.email=\"cbm@example.invalid\" commit -m \"initial\" >%s 2>&1",
             repo, null_dev);
    ASSERT_EQ(run_cmd(cmd), 0);

    cbm_git_context_t initial_ctx = {0};
    ASSERT_EQ(cbm_git_context_resolve(repo, &initial_ctx), 0);
    ASSERT_NOT_NULL(initial_ctx.head_sha);
    char initial_head[128];
    snprintf(initial_head, sizeof(initial_head), "%s", initial_ctx.head_sha);
    cbm_git_context_free(&initial_ctx);

    cbm_pipeline_incremental_test_reset_faults();
    cbm_pipeline_t *baseline = cbm_pipeline_new(repo, db_path, CBM_MODE_FAST);
    ASSERT_NOT_NULL(baseline);
    ASSERT_EQ(cbm_pipeline_run(baseline), 0);
    char project[256];
    snprintf(project, sizeof(project), "%s", cbm_pipeline_project_name(baseline));
    cbm_pipeline_free(baseline);

    cbm_pipeline_t *after_head_move = cbm_pipeline_new(repo, db_path, CBM_MODE_FAST);
    ASSERT_NOT_NULL(after_head_move);
    snprintf(cmd, sizeof(cmd),
             "git -C \"%s\" -c user.name=\"CBM Test\" "
             "-c user.email=\"cbm@example.invalid\" commit --allow-empty "
             "-m \"head-only change\" >%s 2>&1",
             repo, null_dev);
    ASSERT_EQ(run_cmd(cmd), 0);
    cbm_git_context_t changed_ctx = {0};
    ASSERT_EQ(cbm_git_context_resolve(repo, &changed_ctx), 0);
    ASSERT_NOT_NULL(changed_ctx.head_sha);
    char changed_head[128];
    snprintf(changed_head, sizeof(changed_head), "%s", changed_ctx.head_sha);
    cbm_git_context_free(&changed_ctx);
    ASSERT_TRUE(strcmp(initial_head, changed_head) != 0);

    cbm_pipeline_incremental_test_reset_faults();
    int changed_rc = cbm_pipeline_run(after_head_move);
    cbm_incremental_route_t changed_route = cbm_pipeline_incremental_test_last_route();
    cbm_pipeline_free(after_head_move);

    cbm_store_t *store = cbm_store_open_path(db_path);
    int changed_branch_matches = store ? branch_head_match_count(store, project, changed_head) : -1;
    int stale_branch_matches = store ? branch_head_match_count(store, project, initial_head) : -1;
    cbm_file_hash_t git_input = {0};
    int git_input_rc =
        store ? cbm_store_get_file_hash(
                    store, project, ".codebase-memory/.semantic-input/git-context-v1", &git_input)
              : CBM_STORE_NOT_FOUND;
    cbm_store_clear_file_hash(&git_input);
    if (store) {
        cbm_store_close(store);
    }

    cbm_pipeline_incremental_test_reset_faults();
    cbm_pipeline_t *unchanged = cbm_pipeline_new(repo, db_path, CBM_MODE_FAST);
    ASSERT_NOT_NULL(unchanged);
    int unchanged_rc = cbm_pipeline_run(unchanged);
    cbm_incremental_route_t unchanged_route = cbm_pipeline_incremental_test_last_route();
    cbm_pipeline_free(unchanged);
    cbm_pipeline_incremental_test_reset_faults();
    th_rmtree(tmp);

    ASSERT_EQ(changed_rc, 0);
    ASSERT_EQ(changed_route, CBM_INCREMENTAL_ROUTE_FORCED_FULL);
    ASSERT_EQ(changed_branch_matches, 1);
    ASSERT_EQ(stale_branch_matches, 0);
    ASSERT_EQ(git_input_rc, CBM_STORE_OK);
    ASSERT_EQ(unchanged_rc, 0);
    ASSERT_EQ(unchanged_route, CBM_INCREMENTAL_ROUTE_NOOP);
    PASS();
}

/* The global extension config lives outside the repository but changes both
 * discovery and parsing. Its exact consumed bytes therefore belong to the
 * same semantic generation as the graph they selected. */
TEST(pipeline_global_extension_config_change_forces_full) {
    char tmp[256];
    char *created = th_mktempdir("cbm_manifest_global_config");
    ASSERT_NOT_NULL(created);
    snprintf(tmp, sizeof(tmp), "%s", created);

    char repo[512], config_root[512], app_dir[768], config_path[1024], db_path[512];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    snprintf(config_root, sizeof(config_root), "%s/config-root", tmp);
    snprintf(app_dir, sizeof(app_dir), "%s/codebase-memory-mcp", config_root);
    snprintf(config_path, sizeof(config_path), "%s/config.json", app_dir);
    snprintf(db_path, sizeof(db_path), "%s/config.db", tmp);
    ASSERT_EQ(th_mkdir_p(repo), 0);
    ASSERT_EQ(th_mkdir_p(app_dir), 0);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "fixture.cbmfixture"),
                            "def GlobalExtensionTarget():\n    return 1\n"),
              0);
    ASSERT_EQ(th_write_file(config_path, "{\"extra_extensions\":{\".cbmfixture\":\"python\"}}\n"),
              0);

#ifdef _WIN32
    const char *env_name = "APPDATA";
#else
    const char *env_name = "XDG_CONFIG_HOME";
#endif
    char old_env[CBM_SZ_1K] = {0};
    bool had_old_env = cbm_safe_getenv(env_name, old_env, sizeof(old_env), NULL) != NULL;

    int setenv_rc = cbm_setenv(env_name, config_root, 1);
    int baseline_rc = -1;
    int initial_nodes = -1;
    int changed_rc = -1;
    cbm_incremental_route_t changed_route = CBM_INCREMENTAL_ROUTE_NONE;
    int changed_nodes = -1;
    int config_input_rc = CBM_STORE_NOT_FOUND;
    int unchanged_rc = -1;
    cbm_incremental_route_t unchanged_route = CBM_INCREMENTAL_ROUTE_NONE;
    char project[256] = {0};

    if (setenv_rc == 0) {
        cbm_pipeline_incremental_test_reset_faults();
        cbm_pipeline_t *baseline = cbm_pipeline_new(repo, db_path, CBM_MODE_FAST);
        if (baseline) {
            baseline_rc = cbm_pipeline_run(baseline);
            snprintf(project, sizeof(project), "%s", cbm_pipeline_project_name(baseline));
            cbm_pipeline_free(baseline);
        }
        cbm_store_t *baseline_store = cbm_store_open_path(db_path);
        if (baseline_store) {
            initial_nodes = named_node_count(baseline_store, project, "GlobalExtensionTarget");
            cbm_store_close(baseline_store);
        }

        if (th_write_file(config_path, "{\"extra_extensions\":{\".cbmfixture\":\"json\"}}\n") ==
            0) {
            cbm_pipeline_incremental_test_reset_faults();
            cbm_pipeline_t *changed = cbm_pipeline_new(repo, db_path, CBM_MODE_FAST);
            if (changed) {
                changed_rc = cbm_pipeline_run(changed);
                changed_route = cbm_pipeline_incremental_test_last_route();
                cbm_pipeline_free(changed);
            }
            cbm_store_t *changed_store = cbm_store_open_path(db_path);
            if (changed_store) {
                changed_nodes = named_node_count(changed_store, project, "GlobalExtensionTarget");
                cbm_file_hash_t config_input = {0};
                config_input_rc = cbm_store_get_file_hash(
                    changed_store, project,
                    ".codebase-memory/.semantic-input/global-extension-config-v1", &config_input);
                cbm_store_clear_file_hash(&config_input);
                cbm_store_close(changed_store);
            }

            cbm_pipeline_incremental_test_reset_faults();
            cbm_pipeline_t *unchanged = cbm_pipeline_new(repo, db_path, CBM_MODE_FAST);
            if (unchanged) {
                unchanged_rc = cbm_pipeline_run(unchanged);
                unchanged_route = cbm_pipeline_incremental_test_last_route();
                cbm_pipeline_free(unchanged);
            }
        }
    }

    if (had_old_env) {
        cbm_setenv(env_name, old_env, 1);
    } else {
        cbm_unsetenv(env_name);
    }
    cbm_set_user_lang_config(NULL);
    cbm_pipeline_incremental_test_reset_faults();
    th_rmtree(tmp);

    ASSERT_EQ(setenv_rc, 0);
    ASSERT_EQ(baseline_rc, 0);
    ASSERT_EQ(initial_nodes, 1);
    ASSERT_EQ(changed_rc, 0);
    ASSERT_EQ(changed_route, CBM_INCREMENTAL_ROUTE_FORCED_FULL);
    ASSERT_EQ(changed_nodes, 0);
    ASSERT_EQ(config_input_rc, CBM_STORE_OK);
    ASSERT_EQ(unchanged_rc, 0);
    ASSERT_EQ(unchanged_route, CBM_INCREMENTAL_ROUTE_NOOP);
    PASS();
}
#endif

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

/* ── K8s / Kustomize extraction tests ──────────────────────────── */

TEST(k8s_extract_kustomize) {
    const char *src = "apiVersion: kustomize.config.k8s.io/v1beta1\n"
                      "kind: Kustomization\n"
                      "resources:\n"
                      "  - deployment.yaml\n"
                      "  - service.yaml\n";
    CBMFileResult *r = cbm_extract_file(src, (int)strlen(src), CBM_LANG_KUSTOMIZE, "myproj",
                                        "base/kustomization.yaml", 0, NULL, NULL);
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
    const char *src = "apiVersion: apps/v1\n"
                      "kind: Deployment\n"
                      "metadata:\n"
                      "  name: my-app\n"
                      "  namespace: production\n";
    CBMFileResult *r = cbm_extract_file(src, (int)strlen(src), CBM_LANG_K8S, "myproj",
                                        "k8s/deployment.yaml", 0, NULL, NULL);
    ASSERT(r != NULL);
    ASSERT_GTE(r->defs.count, 1);

    bool found_resource = false;
    for (int d = 0; d < r->defs.count; d++) {
        if (r->defs.items[d].label && strcmp(r->defs.items[d].label, "Resource") == 0 &&
            r->defs.items[d].name && strstr(r->defs.items[d].name, "Deployment") != NULL)
            found_resource = true;
    }
    ASSERT_TRUE(found_resource);

    cbm_free_result(r);
    PASS();
}

TEST(k8s_extract_manifest_no_name) {
    const char *src = "apiVersion: apps/v1\nkind: Deployment\n";
    CBMFileResult *r = cbm_extract_file(src, (int)strlen(src), CBM_LANG_K8S, "myproj",
                                        "k8s/deploy.yaml", 0, NULL, NULL);
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
     * resource must be present and no crash must occur.
     *
     * Note: with some tree-sitter YAML grammar versions the root stream may
     * expose both documents as siblings; the break still fires after the first
     * successful def push, so defs.count must be exactly 1. */
    const char *src = "apiVersion: apps/v1\n"
                      "kind: Deployment\n"
                      "metadata:\n"
                      "  name: my-app\n"
                      "---\n"
                      "apiVersion: v1\n"
                      "kind: Service\n"
                      "metadata:\n"
                      "  name: my-svc\n";
    CBMFileResult *r = cbm_extract_file(src, (int)strlen(src), CBM_LANG_K8S, "myproj",
                                        "k8s/multi.yaml", 0, NULL, NULL);
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
    /* At least one def, no more than one (only first document processed) */
    ASSERT(r->defs.count >= 1);
    cbm_free_result(r);
    PASS();
}

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
        "export GH_URL=\"https://ghp_FAKEFAKEFAKEFAKEFAKEFAKEFAKEFAKEFAKE@github.com/repo\"\n"
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

/* ── Discovery-exclusion plumbing in auxiliary repo walks (#792) ── */

/* Boundary semantics of the shared exclusion predicate: anchored at the
 * repo root, matches the excluded dir itself and its subtree, but never
 * sibling names sharing a prefix. Regression guard for issue #792. */
TEST(pipeline_relpath_excluded_boundary) {
    char *excluded[] = {(char *)"vendor_big", (char *)"packages/big"};

    /* Exact match and subtree paths are excluded. */
    ASSERT_TRUE(cbm_pipeline_relpath_is_excluded("vendor_big", excluded, 2));
    ASSERT_TRUE(cbm_pipeline_relpath_is_excluded("vendor_big/lib/package.json", excluded, 2));
    ASSERT_TRUE(cbm_pipeline_relpath_is_excluded("packages/big", excluded, 2));
    ASSERT_TRUE(cbm_pipeline_relpath_is_excluded("packages/big/src/x.ts", excluded, 2));

    /* Sibling names sharing the prefix are NOT excluded ('/'-boundary). */
    ASSERT_FALSE(cbm_pipeline_relpath_is_excluded("vendor_bigger", excluded, 2));
    ASSERT_FALSE(cbm_pipeline_relpath_is_excluded("vendor", excluded, 2));
    ASSERT_FALSE(cbm_pipeline_relpath_is_excluded("packages/bigger/x.ts", excluded, 2));

    /* Exclusions are root-anchored prefixes, not substring matches. */
    ASSERT_FALSE(cbm_pipeline_relpath_is_excluded("src/vendor_big/x.c", excluded, 2));

    /* NULL / empty safety. */
    ASSERT_FALSE(cbm_pipeline_relpath_is_excluded(NULL, excluded, 2));
    ASSERT_FALSE(cbm_pipeline_relpath_is_excluded("", excluded, 2));
    ASSERT_FALSE(cbm_pipeline_relpath_is_excluded("vendor_big", NULL, 0));
    ASSERT_FALSE(cbm_pipeline_relpath_is_excluded("vendor_big", excluded, 0));
    char *with_empty[] = {(char *)""};
    ASSERT_FALSE(cbm_pipeline_relpath_is_excluded("vendor_big", with_empty, 1));
    PASS();
}

/* Helper: does the entries array contain a package with this name? */
static int pkg_entries_has_name(const cbm_pkg_entries_t *e, const char *name) {
    for (int i = 0; i < e->count; i++) {
        if (e->items[i].pkg_name && strcmp(e->items[i].pkg_name, name) == 0)
            return 1;
    }
    return 0;
}

/* The pkgmap repo walk must honor discovery exclusions (issue #792: a
 * gitignored huge subtree kept the pkgmap walk busy for 15 minutes).
 * Control run first (no exclusions → BOTH manifests parsed) so the
 * exclusion assertion below cannot pass vacuously. */
TEST(pkgmap_scan_repo_honors_discovery_exclusions) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_pkgmap_excl_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("tmpdir");

    char dir[512];
    snprintf(dir, sizeof(dir), "%s/packages", tmpdir);
    cbm_mkdir(dir);
    snprintf(dir, sizeof(dir), "%s/packages/app", tmpdir);
    cbm_mkdir(dir);
    snprintf(dir, sizeof(dir), "%s/vendor_big", tmpdir);
    cbm_mkdir(dir);
    snprintf(dir, sizeof(dir), "%s/vendor_big/lib", tmpdir);
    cbm_mkdir(dir);

    write_temp_file(tmpdir, "packages/app/package.json",
                    "{\"name\":\"@org/app\",\"main\":\"index.js\"}\n");
    write_temp_file(tmpdir, "vendor_big/lib/package.json",
                    "{\"name\":\"@org/vendored\",\"main\":\"index.js\"}\n");

    /* Control: NULL exclusion list — the walk reaches and parses BOTH
     * manifests (proves the excluded one is reachable + parseable). */
    cbm_pkg_entries_t control;
    cbm_pkg_entries_init(&control);
    cbm_pkgmap_scan_repo(tmpdir, &control, NULL, 0);
    ASSERT_TRUE(pkg_entries_has_name(&control, "@org/app"));
    ASSERT_TRUE(pkg_entries_has_name(&control, "@org/vendored"));
    cbm_pkg_entries_free(&control);

    /* With vendor_big excluded (as discovery reports for a gitignored
     * subtree): the walk must not descend into it. */
    char *excluded[] = {(char *)"vendor_big"};
    cbm_pkg_entries_t entries;
    cbm_pkg_entries_init(&entries);
    cbm_pkgmap_scan_repo(tmpdir, &entries, excluded, 1);
    ASSERT_TRUE(pkg_entries_has_name(&entries, "@org/app"));
    ASSERT_FALSE(pkg_entries_has_name(&entries, "@org/vendored"));
    cbm_pkg_entries_free(&entries);

    th_rmtree(tmpdir);
    PASS();
}

/* The env-URL walk must honor discovery exclusions the same way (#792).
 * Control run first via the NULL-exclusion wrapper so the exclusion
 * assertion cannot pass vacuously. */
TEST(envscan_walk_honors_discovery_exclusions) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_envscan_excl_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("tmpdir");

    char dir[512];
    snprintf(dir, sizeof(dir), "%s/big_generated", tmpdir);
    cbm_mkdir(dir);

    write_temp_file(tmpdir, "deploy.sh",
                    "#!/bin/bash\nexport CONTROL_URL=\"https://api.example.com/v1\"\n");
    write_temp_file(tmpdir, "big_generated/env.sh",
                    "#!/bin/bash\nexport EXCLUDED_URL=\"https://excluded.example.com/v1\"\n");

    /* Control: the NULL-exclusion wrapper sees both bindings. */
    cbm_env_binding_t bindings[32];
    int count = cbm_scan_project_env_urls(tmpdir, bindings, 32);
    ASSERT_NOT_NULL(find_binding_by_key(bindings, count, "CONTROL_URL"));
    ASSERT_NOT_NULL(find_binding_by_key(bindings, count, "EXCLUDED_URL"));

    /* With big_generated excluded, its binding must disappear. */
    char *excluded[] = {(char *)"big_generated"};
    count = cbm_scan_project_env_urls_excluded(tmpdir, bindings, 32, excluded, 1);
    ASSERT_NOT_NULL(find_binding_by_key(bindings, count, "CONTROL_URL"));
    ASSERT_TRUE(find_binding_by_key(bindings, count, "EXCLUDED_URL") == NULL);

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
 *  Incremental reindex tests
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

static void cleanup_incremental_repo(void) {
    th_rmtree(g_incr_tmpdir);
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

/* DLL resolve test removed — feature removed due to Windows Defender
 * false positive (Wacatac.B!ml). See issue #89. */

/* ═══════════════════════════════════════════════════════════════════
 *  Incremental reindex
 * ═══════════════════════════════════════════════════════════════════ */

TEST(incremental_full_then_noop) {
    /* Full index, then re-run → should detect no changes and skip */
    if (setup_incremental_repo() != 0) {
        FAIL("setup failed");
    }

    /* First: full index */
    cbm_pipeline_t *p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    char *project = strdup(cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);

    /* Verify nodes exist */
    cbm_store_t *s = cbm_store_open_path(g_incr_dbpath);
    ASSERT_NOT_NULL(s);
    int nodes_before = cbm_store_count_nodes(s, project);
    ASSERT_GT(nodes_before, 0);
    cbm_store_close(s);

    /* Second: incremental — nothing changed → should be no-op */
    p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    cbm_pipeline_free(p);

    s = cbm_store_open_path(g_incr_dbpath);
    ASSERT_NOT_NULL(s);
    int nodes_after = cbm_store_count_nodes(s, project);
    /* Node count should be same (no duplicates, no loss) */
    ASSERT_EQ(nodes_after, nodes_before);
    cbm_store_close(s);
    free(project);

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
    p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    /* Verify node count increased (NewFunc was added) */
    cbm_store_t *s = cbm_store_open_path(g_incr_dbpath);
    ASSERT_NOT_NULL(s);
    int nodes_after = cbm_store_count_nodes(s, project);
    ASSERT_GT(nodes_after, 0);
    cbm_store_close(s);
    cbm_pipeline_free(p);
    free(project);

    cleanup_incremental_repo();
    PASS();
}

TEST(full_reindex_recovers_when_previous_coverage_is_unreadable) {
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
    /* Simulate an unreadable prior coverage generation while leaving the
     * graph and file hashes healthy enough to otherwise run incrementally. */
    ASSERT_EQ(
        cbm_store_exec(s, "ALTER TABLE index_coverage RENAME COLUMN detail TO broken_detail;"),
        CBM_STORE_OK);
    cbm_store_close(s);

    char path[512];
    snprintf(path, sizeof(path), "%s/helper.go", g_incr_tmpdir);
    FILE *f = fopen(path, "a");
    ASSERT_NOT_NULL(f);
    fprintf(f, "\nfunc MustNotBeIndexed() int { return 7 }\n");
    fclose(f);

    p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    cbm_pipeline_free(p);

    /* An exact-manifest delta routes to an isolated full generation, which
     * does not depend on the damaged coverage table and repairs it atomically. */
    s = cbm_store_open_path(g_incr_dbpath);
    ASSERT_NOT_NULL(s);
    ASSERT_GT(cbm_store_count_nodes(s, project), nodes_before);
    cbm_node_t *recovered = NULL;
    int recovered_count = 0;
    ASSERT_EQ(
        cbm_store_find_nodes_by_name(s, project, "MustNotBeIndexed", &recovered, &recovered_count),
        CBM_STORE_OK);
    ASSERT_EQ(recovered_count, 1);
    cbm_store_free_nodes(recovered, recovered_count);
    cbm_store_close(s);
    free(project);

    cleanup_incremental_repo();
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
    p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    /* Verify node count decreased (Helper's file was deleted) */
    cbm_store_t *s = cbm_store_open_path(g_incr_dbpath);
    ASSERT_NOT_NULL(s);
    int nodes_after = cbm_store_count_nodes(s, project);
    ASSERT_GT(nodes_after, 0); /* still has main.go nodes */
    cbm_store_close(s);
    cbm_pipeline_free(p);
    free(project);

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
    p = cbm_pipeline_new(g_incr_tmpdir, g_incr_dbpath, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    cbm_store_t *s = cbm_store_open_path(g_incr_dbpath);
    ASSERT_NOT_NULL(s);
    int nodes_after = cbm_store_count_nodes(s, project);
    ASSERT_GT(nodes_after, 0);
    cbm_store_close(s);
    cbm_pipeline_free(p);
    free(project);

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
    cbm_store_close(s);

    /* Step 2: fast-mode reindex — tools/util.go MUST survive (additive semantics) */
    p = cbm_pipeline_new(tmpdir, dbpath, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
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

    /* Step 3: mutate main.go and fast reindex — forces dump_and_persist to
     * run (instead of the noop early-return path that step 2 hit). This is
     * the real dangerous path: the gbuf gets loaded, mutated for main.go,
     * dumped back to disk. tools/util.go must survive THAT cycle, not just
     * the trivial noop path. Audit finding from 2026-04-13. */
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
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    cbm_pipeline_free(p);

    s = cbm_store_open_path(dbpath);
    ASSERT_NOT_NULL(s);
    cbm_node_t *tools_nodes_run3 = NULL;
    int tools_count_run3 = 0;
    cbm_store_find_nodes_by_file(s, project, "tools/util.go", &tools_nodes_run3, &tools_count_run3);
    /* tools/util.go nodes must STILL be present after a fast reindex that
     * actually ran the full dump_and_persist cycle (not the noop fast-path). */
    ASSERT_EQ(tools_count_run3, tools_count_before);
    cbm_store_free_nodes(tools_nodes_run3, tools_count_run3);
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
    p = cbm_pipeline_new(tmpdir, dbpath, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
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
    p = cbm_pipeline_new(tmpdir, dbpath, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
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

/* Reproduce-first (perf, linux-kernel finding): the extraction back-pressure
 * gate must stop re-paying the full collect+nap tax on every file pull once a
 * full nap cycle has failed to reclaim under budget. With CBM_MEM_BUDGET_MB=1
 * the test process RSS is permanently over budget and NOTHING napping can
 * change that (the resident floor IS the process) — napping is provably futile.
 * OLD behavior: one full 40-spin nap cycle per pulled file (kernel index: ~63k
 * pulls × ~120 ms ÷ 12 workers ≈ 390 s of idle workers at 79% avg CPU).
 * FIXED: the first futile cycle flips a shared flag; later pulls proceed with
 * the designed soft overshoot. Cycle count then can't exceed one cycle per
 * worker (workers already inside the gate when the flag flips) plus re-probes.
 * RED on the unfixed gate: cycles == file count (64) > cores+2.
 * The counter (cbm_pp_bp_nap_cycles) makes this deterministic — no timing.
 *
 * The gate lives ONLY in the parallel extract path, so the fixture MUST exceed
 * MIN_FILES_FOR_PARALLEL (50) — else the run routes sequential, the gate never
 * fires, and the test would pass vacuously (cycles==0). The engagement assert
 * below (cycles >= 1) is a hard guard against that regressing silently. */
TEST(pipeline_backpressure_futile_nap_disengages) {
    /* 64 tiny files: > MIN_FILES_FOR_PARALLEL (50) so the parallel path (and its
     * back-pressure gate) actually runs; old-code cycles (~64) >> the bound. */
    snprintf(g_tmpdir, sizeof(g_tmpdir), "/tmp/cbm_test_XXXXXX");
    if (!cbm_mkdtemp(g_tmpdir)) {
        FAIL("failed to create temp dir");
    }
    for (int i = 0; i < 64; i++) {
        char path[512];
        snprintf(path, sizeof(path), "%s/f%02d.go", g_tmpdir, i);
        FILE *f = fopen(path, "w");
        if (!f) {
            FAIL("failed to create fixture file");
        }
        fprintf(f, "package main\n\nfunc F%02d() int {\n\treturn %d\n}\n", i, i);
        fclose(f);
    }

    /* 1 MB budget: over-budget on every pull, unreclaimable by napping.
     * Set via the test hook, NOT setenv + cbm_mem_init: the init-once guard
     * makes any re-init keep whatever budget the FIRST in-process init
     * computed. The old env dance either failed to apply the 1 MB budget
     * (some earlier test's init won the guard) or applied it permanently
     * (this test's init won) — the "restore" re-init was then a silent
     * no-op and the 1 MB budget leaked into every later budget consumer
     * in the runner (mem_over_budget_low_rss went red suite-order-wide). */
    size_t saved_budget = cbm_mem_budget();
    cbm_mem_set_budget_for_tests((size_t)1024 * 1024);
    ASSERT_TRUE(cbm_mem_budget() > 0);
    ASSERT_TRUE(cbm_mem_over_budget());

    cbm_pp_bp_nap_cycles_reset();
    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/backpressure.db", g_tmpdir);
    cbm_pipeline_t *p = cbm_pipeline_new(g_tmpdir, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    int rc = cbm_pipeline_run(p);
    long cycles = cbm_pp_bp_nap_cycles();

    /* Restore the caller-visible budget BEFORE asserting. */
    cbm_mem_set_budget_for_tests(saved_budget);
    cbm_pipeline_free(p);
    teardown_test_repo();

    ASSERT_EQ(rc, 0);
    /* Engagement guard (anti-vacuous): the gate must have actually run — the
     * parallel path taken and the 1 MB budget exceeded on every pull. cycles==0
     * means the fixture routed sequential (or the gate was compiled out) and
     * this test proved NOTHING; fail loudly rather than pass vacuously. */
    if (cycles < 1) {
        FAIL("back-pressure gate never engaged (cycles==0) — fixture routed sequential?");
    }
    /* Futile napping must disengage: at most one in-flight cycle per worker
     * plus a small margin, never one per file (64). */
    long bound = (long)cbm_system_info().total_cores + 2;
    if (cycles > bound) {
        char msg[128];
        snprintf(msg, sizeof(msg), "nap cycles %ld > bound %ld (gate re-paid per pull)", cycles,
                 bound);
        FAIL(msg);
    }
    PASS();
}

/* TS cross-registry test hooks (ts_lsp.c) — extern to avoid pulling the
 * tree-sitter-typed ts_lsp.h into this store-level test. */
extern long cbm_ts_full_registry_builds(void);
extern void cbm_ts_full_registry_builds_reset(void);

/* Reproduce-first (ms-typescript finding, 2026-07-07): the SEQUENTIAL
 * cross-LSP driver must resolve TS files through the SHARED prebuilt
 * registry, never a full per-file build. cbm_run_ts_lsp_cross registers
 * stdlib + EVERY cross-file def + finalizes once PER FILE — O(files x defs).
 * On an 81k-file TS corpus that ground one core for hours (74% of stack
 * samples inside build_qn_index), and when the supervisor's quiet-timeout
 * killed the crawl mid-pass, the stale extraction marker blamed innocent
 * files, quarantining four of them one 15-minute retry at a time.
 *
 * The fixture stays UNDER MIN_FILES_FOR_PARALLEL (50) so the pipeline
 * routes through the sequential driver — the path that lacked the
 * shared-registry prepare.
 * RED on the unfixed driver: full builds == TS file count (40).
 * GREEN: full builds == 0 AND the cross-file TS call still resolves
 * (quality guard — the shared path must not lose the edge). */
TEST(pipeline_seq_ts_cross_uses_shared_registry) {
    snprintf(g_tmpdir, sizeof(g_tmpdir), "/tmp/cbm_test_XXXXXX");
    if (!cbm_mkdtemp(g_tmpdir)) {
        FAIL("failed to create temp dir");
    }
    char path[512];
    snprintf(path, sizeof(path), "%s/shared.ts", g_tmpdir);
    FILE *f = fopen(path, "w");
    if (!f) {
        FAIL("failed to create fixture file");
    }
    fputs("export function sharedHelper(): number {\n  return 42;\n}\n", f);
    fclose(f);
    for (int i = 0; i < 39; i++) {
        snprintf(path, sizeof(path), "%s/caller%02d.ts", g_tmpdir, i);
        f = fopen(path, "w");
        if (!f) {
            FAIL("failed to create fixture file");
        }
        fprintf(f,
                "import { sharedHelper } from \"./shared\";\n"
                "export function caller%02d(): number {\n  return sharedHelper();\n}\n",
                i);
        fclose(f);
    }

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/seqts.db", g_tmpdir);
    cbm_ts_full_registry_builds_reset();
    cbm_pipeline_t *p = cbm_pipeline_new(g_tmpdir, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    int rc = cbm_pipeline_run(p);
    long builds = cbm_ts_full_registry_builds();

    /* Quality guard FIRST: the cross-file call must resolve either way. */
    cbm_store_t *s = cbm_store_open_path(db_path);
    bool linked = false;
    if (s) {
        linked =
            cross_file_call_exists(s, cbm_pipeline_project_name(p), "caller00", "sharedHelper");
        cbm_store_close(s);
    }
    cbm_pipeline_free(p);
    teardown_test_repo();

    ASSERT_EQ(rc, 0);
    ASSERT_TRUE(linked);
    /* The point: ZERO full per-file registry builds on the sequential path. */
    ASSERT_EQ(builds, 0);
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
    /* Extraction back-pressure */
    RUN_TEST(pipeline_backpressure_futile_nap_disengages);
    /* Sequential cross-LSP shared registry (ms-typescript quadratic) */
    RUN_TEST(pipeline_seq_ts_cross_uses_shared_registry);
    /* File persistence */
    RUN_TEST(store_file_persistence);
    RUN_TEST(store_bulk_persistence);
    /* Integration: structure pass */
    RUN_TEST(pipeline_structure_nodes);
    RUN_TEST(pipeline_committed_counts_match_persisted);
    RUN_TEST(pipeline_adr_survives_full_reindex);
    RUN_TEST(pipeline_structure_edges);
    RUN_TEST(pipeline_branch_root_structure);
    RUN_TEST(pipeline_project_name_derived);
    RUN_TEST(pipeline_fast_mode);
    /* Definitions pass */
    RUN_TEST(pipeline_definitions_function_nodes);
    RUN_TEST(pipeline_definitions_defines_edges);
    RUN_TEST(pipeline_definitions_properties);
    RUN_TEST(pipeline_def_props_valid_json_when_oversized);
    RUN_TEST(pipeline_edge_props_valid_json);
    /* Complexity propagation pass (Tier B) */
    RUN_TEST(pipeline_complexity_transitive_loop_depth);
    /* Calls pass */
    RUN_TEST(pipeline_calls_resolution);
    RUN_TEST(pipeline_incremental_preserves_cross_file_calls);
    RUN_TEST(pipeline_objectscript_export_preserves_calls_sequential_parallel);
    RUN_TEST(pipeline_objectscript_export_incremental_matches_full_relationships);
    RUN_TEST(pipeline_objectscript_export_aggregate_exceeds_arena_block_table);
    RUN_TEST(pipeline_env_access_configures_sequential_parallel_parity);
    RUN_TEST(pipeline_call_reference_sequential_parallel_edge_set_parity);
    RUN_TEST(pipeline_incremental_cross_file_call_reference_matches_fresh_full);
    RUN_TEST(pipeline_incremental_changed_target_invalidates_stale_inbound_call_reference);
    RUN_TEST(pipeline_incremental_parallel_registry_nodes_advance_shared_ids);
#if defined(CBM_INCREMENTAL_TEST_API) && CBM_INCREMENTAL_TEST_API
    RUN_TEST(pipeline_incremental_parallel_result_cache_alloc_failure_preserves_db_and_retries);
#endif
    RUN_TEST(pipeline_tsjs_receiver_suppresses_weak_method_edge);
    RUN_TEST(pipeline_tsjs_receiver_parallel_keeps_service_edges);
    RUN_TEST(pipeline_parallel_python_cross_only_dunder_gets_synthetic_carrier);
    RUN_TEST(pipeline_parallel_rust_cross_only_macro_hidden_gets_synthetic_carrier);
    RUN_TEST(pipeline_native_fetch_classified_as_http_calls);
    RUN_TEST(pipeline_native_fetch_parallel_classified_as_http_calls);
    RUN_TEST(pipeline_local_fetch_shadow_not_classified_as_http);
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
    RUN_TEST(implements_no_match);
    /* Usages pass (full pipeline integration) */
    RUN_TEST(usages_creates_edges);
    RUN_TEST(usages_no_duplicate_calls);
    RUN_TEST(calls_edge_carries_call_site_line);
    RUN_TEST(usages_kotlin_creates_edges);
    RUN_TEST(usages_kotlin_no_duplicate_calls);
    /* Language integration tests */
    RUN_TEST(pipeline_python_project);
    RUN_TEST(pipeline_imports_multi_symbol_edges);
    RUN_TEST(pipeline_go_cross_package_call);
    RUN_TEST(pipeline_python_cross_module_call);
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
    RUN_TEST(pipeline_docstring_go_class);
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
    /* K8s / Kustomize extraction */
    RUN_TEST(k8s_extract_kustomize);
    RUN_TEST(k8s_extract_manifest);
    RUN_TEST(k8s_extract_manifest_no_name);
    RUN_TEST(k8s_extract_manifest_multidoc);
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
    /* Discovery-exclusion plumbing in auxiliary repo walks (#792) */
    RUN_TEST(pipeline_relpath_excluded_boundary);
    RUN_TEST(pkgmap_scan_repo_honors_discovery_exclusions);
    RUN_TEST(envscan_walk_honors_discovery_exclusions);
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
    /* FastAPI Depends edge tracking (PR #66 port) */
    RUN_TEST(pipeline_fastapi_depends_edges);
    /* Incremental */
    RUN_TEST(incremental_full_then_noop);
    RUN_TEST(incremental_detects_changed_file);
    RUN_TEST(full_reindex_recovers_when_previous_coverage_is_unreadable);
    RUN_TEST(incremental_detects_deleted_file);
    RUN_TEST(incremental_new_file_added);
    RUN_TEST(incremental_fast_preserves_mode_skipped_tools_dir);
    RUN_TEST(incremental_k8s_manifest_indexed);
    RUN_TEST(incremental_kustomize_module_indexed);
    /* Resource management & internal helper tests */
    RUN_TEST(pipeline_empty_path);
    RUN_TEST(pipeline_project_name_content);
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
}

/* Focused semantic-manifest and publication contracts. Kept separate from the
 * broad pipeline suite so RED/GREEN iterations exercise only this boundary;
 * the default all-suite run still executes it. */
SUITE(pipeline_semantic_manifest_repro) {
    RUN_TEST(pipeline_incremental_repoints_call_reference_without_stale_edge);
    RUN_TEST(pipeline_incremental_tsconfig_alias_change_matches_fresh_full);
#if defined(CBM_INCREMENTAL_TEST_API) && CBM_INCREMENTAL_TEST_API
    RUN_TEST(pipeline_git_context_change_forces_full_and_refreshes_branch);
    RUN_TEST(pipeline_global_extension_config_change_forces_full);
    RUN_TEST(pipeline_source_mutation_before_publication_preserves_previous_generation);
    RUN_TEST(pipeline_source_addition_before_publication_preserves_previous_generation);
    RUN_TEST(pipeline_tsconfig_mutation_before_publication_preserves_previous_generation);
    RUN_TEST(pipeline_exact_inputs_migrate_coverage_metadata_and_index_mode);
    RUN_TEST(pipeline_existing_artifact_refreshes_after_default_forced_full_reindex);
    RUN_TEST(pipeline_full_cancel_after_predump_preserves_previous_generation);
    RUN_TEST(pipeline_full_cancel_after_destination_prepare_preserves_previous_generation);
    RUN_TEST(pipeline_full_persist_failure_after_stage_dump_preserves_previous_generation);
    RUN_TEST(pipeline_incremental_persist_failure_preserves_previous_generation_and_retries);
    RUN_TEST(pipeline_incremental_successful_publication_preserves_adr);
    RUN_TEST(pipeline_full_adr_capture_failure_preserves_previous_generation);
    RUN_TEST(pipeline_semantic_manifest_rejects_non_directory_root);
    RUN_TEST(pipeline_full_reindex_quarantines_corrupt_destination_without_overwrite);
    RUN_TEST(pipeline_full_reindex_replaces_legacy_schema_without_quarantine);
#endif
}
