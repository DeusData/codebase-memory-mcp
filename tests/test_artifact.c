/*
 * test_artifact.c — Tests for persistent artifact export/import.
 */
#include "test_framework.h"
#include "store/store.h"
#include "pipeline/artifact.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"

#include <sys/stat.h>
#include <stdio.h>

/* ── Helpers ─────────────────────────────────────────────────────── */

static char g_tmpdir[1024];
static char g_repo[1024];
static char g_db[1024];

static void setup_artifact_test(void) {
    snprintf(g_tmpdir, sizeof(g_tmpdir), "%s/cbm_test_artifact_XXXXXX", cbm_tmpdir());
    cbm_mkdtemp(g_tmpdir);

    snprintf(g_repo, sizeof(g_repo), "%s/repo", g_tmpdir);
    cbm_mkdir_p(g_repo, 0755);

    snprintf(g_db, sizeof(g_db), "%s/test.db", g_tmpdir);
}

/* Create a minimal but valid DB with some nodes and edges. */
static void create_test_db(const char *path) {
    cbm_store_t *s = cbm_store_open_path(path);
    if (!s) {
        return;
    }

    cbm_store_exec(s, "INSERT OR IGNORE INTO projects(name, indexed_at, root_path) "
                      "VALUES('test-proj', '2026-01-01', '/tmp/test');");

    cbm_store_exec(s, "INSERT INTO nodes(project, label, name, qualified_name, file_path) "
                      "VALUES('test-proj', 'Function', 'foo', 'test-proj.foo', 'main.c');");
    cbm_store_exec(s, "INSERT INTO nodes(project, label, name, qualified_name, file_path) "
                      "VALUES('test-proj', 'Function', 'bar', 'test-proj.bar', 'main.c');");

    cbm_store_exec(s, "INSERT INTO edges(project, source_id, target_id, type) "
                      "VALUES('test-proj', 1, 2, 'CALLS');");

    cbm_store_close(s);
}

static void cleanup_dir(const char *path) {
    char cmd[2048];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", path);
    (void)system(cmd);
}

/* ── Tests ───────────────────────────────────────────────────────── */

TEST(artifact_export_fast_roundtrip) {
    setup_artifact_test();
    create_test_db(g_db);

    /* Export with fast quality (zstd -3, no index stripping) */
    int rc = cbm_artifact_export(g_db, g_repo, "test-proj", CBM_ARTIFACT_FAST);
    ASSERT_EQ(rc, 0);

    /* Verify artifact files exist */
    char zst[1024];
    snprintf(zst, sizeof(zst), "%s/.codebase-memory/graph.db.zst", g_repo);
    struct stat st;
    ASSERT_EQ(stat(zst, &st), 0);
    ASSERT_GT((int)st.st_size, 0);

    char meta[1024];
    snprintf(meta, sizeof(meta), "%s/.codebase-memory/artifact.json", g_repo);
    ASSERT_EQ(stat(meta, &st), 0);

    /* Import to a new path */
    char import_db[1024];
    snprintf(import_db, sizeof(import_db), "%s/imported.db", g_tmpdir);
    rc = cbm_artifact_import(g_repo, import_db);
    ASSERT_EQ(rc, 0);

    /* Verify imported DB has correct data */
    cbm_store_t *s = cbm_store_open_path(import_db);
    ASSERT_NOT_NULL(s);
    int nodes = cbm_store_count_nodes(s, "test-proj");
    int edges = cbm_store_count_edges(s, "test-proj");
    ASSERT_EQ(nodes, 2);
    ASSERT_EQ(edges, 1);
    cbm_store_close(s);

    cleanup_dir(g_tmpdir);
    PASS();
}

TEST(artifact_export_best_roundtrip) {
    setup_artifact_test();
    create_test_db(g_db);

    /* Export with best quality (zstd -9, index stripping + VACUUM) */
    int rc = cbm_artifact_export(g_db, g_repo, "test-proj", CBM_ARTIFACT_BEST);
    ASSERT_EQ(rc, 0);

    /* Source DB should be untouched (VACUUM INTO doesn't modify source) */
    cbm_store_t *src = cbm_store_open_path(g_db);
    ASSERT_NOT_NULL(src);
    ASSERT_EQ(cbm_store_count_nodes(src, "test-proj"), 2);
    cbm_store_close(src);

    /* Import and verify */
    char import_db[1024];
    snprintf(import_db, sizeof(import_db), "%s/imported.db", g_tmpdir);
    rc = cbm_artifact_import(g_repo, import_db);
    ASSERT_EQ(rc, 0);

    cbm_store_t *s = cbm_store_open_path(import_db);
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_count_nodes(s, "test-proj"), 2);
    ASSERT_EQ(cbm_store_count_edges(s, "test-proj"), 1);
    cbm_store_close(s);

    cleanup_dir(g_tmpdir);
    PASS();
}

TEST(artifact_exists_check) {
    setup_artifact_test();
    create_test_db(g_db);

    /* No artifact yet */
    ASSERT_FALSE(cbm_artifact_exists(g_repo));

    /* Export creates the artifact */
    cbm_artifact_export(g_db, g_repo, "test-proj", CBM_ARTIFACT_FAST);
    ASSERT_TRUE(cbm_artifact_exists(g_repo));

    cleanup_dir(g_tmpdir);
    PASS();
}

TEST(artifact_commit_hash) {
    setup_artifact_test();
    create_test_db(g_db);

    cbm_artifact_export(g_db, g_repo, "test-proj", CBM_ARTIFACT_FAST);

    /* commit hash may be empty if repo is not a git repo, but should not crash */
    char *commit = cbm_artifact_commit(g_repo);
    /* For a non-git directory, commit will be NULL (git rev-parse HEAD fails) */
    free(commit);

    cleanup_dir(g_tmpdir);
    PASS();
}

TEST(artifact_schema_version_mismatch) {
    setup_artifact_test();
    create_test_db(g_db);
    cbm_artifact_export(g_db, g_repo, "test-proj", CBM_ARTIFACT_FAST);

    /* Overwrite artifact.json with incompatible schema version */
    char meta[1024];
    snprintf(meta, sizeof(meta), "%s/.codebase-memory/artifact.json", g_repo);
    FILE *fp = fopen(meta, "w");
    ASSERT_NOT_NULL(fp);
    fprintf(fp, "{\"schema_version\": 999, \"original_size\": 1000}");
    fclose(fp);

    /* exists should return false for incompatible version */
    ASSERT_FALSE(cbm_artifact_exists(g_repo));

    /* Import should fail */
    char import_db[1024];
    snprintf(import_db, sizeof(import_db), "%s/imported.db", g_tmpdir);
    int rc = cbm_artifact_import(g_repo, import_db);
    ASSERT_NEQ(rc, 0);

    cleanup_dir(g_tmpdir);
    PASS();
}

TEST(artifact_import_missing) {
    setup_artifact_test();

    /* Import from repo without artifact should fail gracefully */
    char import_db[1024];
    snprintf(import_db, sizeof(import_db), "%s/imported.db", g_tmpdir);
    int rc = cbm_artifact_import(g_repo, import_db);
    ASSERT_NEQ(rc, 0);

    cleanup_dir(g_tmpdir);
    PASS();
}

TEST(artifact_gitattributes_created) {
    setup_artifact_test();
    create_test_db(g_db);

    cbm_artifact_export(g_db, g_repo, "test-proj", CBM_ARTIFACT_FAST);

    char ga[1024];
    snprintf(ga, sizeof(ga), "%s/.codebase-memory/.gitattributes", g_repo);
    struct stat st;
    ASSERT_EQ(stat(ga, &st), 0);

    cleanup_dir(g_tmpdir);
    PASS();
}

TEST(artifact_null_safety) {
    ASSERT_NEQ(cbm_artifact_export(NULL, "/tmp", "p", 0), 0);
    ASSERT_NEQ(cbm_artifact_export("/tmp/x.db", NULL, "p", 0), 0);
    ASSERT_NEQ(cbm_artifact_import(NULL, "/tmp/x.db"), 0);
    ASSERT_NEQ(cbm_artifact_import("/tmp", NULL), 0);
    ASSERT_FALSE(cbm_artifact_exists(NULL));
    ASSERT_NULL(cbm_artifact_commit(NULL));
    PASS();
}

SUITE(artifact) {
    RUN_TEST(artifact_export_fast_roundtrip);
    RUN_TEST(artifact_export_best_roundtrip);
    RUN_TEST(artifact_exists_check);
    RUN_TEST(artifact_commit_hash);
    RUN_TEST(artifact_schema_version_mismatch);
    RUN_TEST(artifact_import_missing);
    RUN_TEST(artifact_gitattributes_created);
    RUN_TEST(artifact_null_safety);
}
