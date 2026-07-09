/*
 * test_artifact.c — Tests for persistent artifact export/import.
 */
#include "test_framework.h"
#include "store/store.h"
#include "pipeline/artifact.h"
#include "pipeline/pipeline.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/log.h"

#include <sys/stat.h>
#include <stdio.h>

/* ── Helpers ─────────────────────────────────────────────────────── */

static char g_tmpdir[1024];
static char g_repo[1024];
static char g_db[1024];
enum { ART_TEST_LOG_BUF = 32768 };
static char g_log_capture[ART_TEST_LOG_BUF];
static CBMLogLevel g_prev_log_level;

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

static void write_text_file(const char *path, const char *text) {
    FILE *fp = fopen(path, "w");
    if (!fp) {
        return;
    }
    fputs(text, fp);
    fclose(fp);
}

static void capture_log_sink(const char *line) {
    size_t used = strlen(g_log_capture);
    size_t avail = sizeof(g_log_capture) - used;
    if (avail <= 1) {
        return;
    }
    int n = snprintf(g_log_capture + used, avail, "%s\n", line);
    if (n < 0 || (size_t)n >= avail) {
        g_log_capture[sizeof(g_log_capture) - 1] = '\0';
    }
}

static void capture_logs_start(void) {
    g_log_capture[0] = '\0';
    g_prev_log_level = cbm_log_get_level();
    cbm_log_set_level(CBM_LOG_DEBUG);
    cbm_log_set_sink(capture_log_sink);
}

static const char *capture_logs_end(void) {
    cbm_log_set_sink(NULL);
    cbm_log_set_level(g_prev_log_level);
    return g_log_capture;
}

/* ── Tests ───────────────────────────────────────────────────────── */

/* Rewrite the "original_size" number in an artifact.json in place, adding
 * `delta` to it. Returns false if the field / a digit run isn't found. */
static bool bump_artifact_original_size(const char *meta_path, long delta) {
    FILE *fp = fopen(meta_path, "rb");
    if (!fp) {
        return false;
    }
    char buf[4096];
    size_t n = fread(buf, 1, sizeof(buf) - 1, fp);
    fclose(fp);
    buf[n] = '\0';
    char *key = strstr(buf, "\"original_size\"");
    if (!key) {
        return false;
    }
    char *colon = strchr(key, ':');
    if (!colon) {
        return false;
    }
    char *ds = colon + 1;
    while (*ds == ' ' || *ds == '\t') {
        ds++;
    }
    char *de = ds;
    while (*de >= '0' && *de <= '9') {
        de++;
    }
    if (de == ds) {
        return false;
    }
    long val = strtol(ds, NULL, 10) + delta;
    char out[4096];
    int pre = (int)(ds - buf);
    snprintf(out, sizeof(out), "%.*s%ld%s", pre, buf, val, de);
    fp = fopen(meta_path, "wb");
    if (!fp) {
        return false;
    }
    fwrite(out, 1, strlen(out), fp);
    fclose(fp);
    return true;
}

/* The decompressed size is driven by the zstd frame's own content-size header,
 * not the separately-stored original_size field (which travels in plaintext
 * artifact.json and is trivially editable). A mismatch between the two must be
 * rejected — this is the check that keeps the destination allocation and the
 * decoder capacity pinned to the same verified size, so a doctored size can
 * never make the decoder write past the buffer. */
TEST(artifact_import_rejects_size_mismatch) {
    setup_artifact_test();
    create_test_db(g_db);
    ASSERT_EQ(cbm_artifact_export(g_db, g_repo, "test-proj", CBM_ARTIFACT_FAST), 0);

    char meta[1024];
    snprintf(meta, sizeof(meta), "%s/.codebase-memory/artifact.json", g_repo);
    ASSERT_TRUE(
        bump_artifact_original_size(meta, 4096)); /* claim 4 KiB more than the frame holds */

    char import_db[1024];
    snprintf(import_db, sizeof(import_db), "%s/imported.db", g_tmpdir);
    int rc = cbm_artifact_import(g_repo, import_db);
    ASSERT_NEQ(rc, 0); /* must reject the mismatch, not import on the doctored size */

    cleanup_dir(g_tmpdir);
    PASS();
}

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

    /* Attribute ORDER is load-bearing: gitattributes apply left to right and
     * the `binary` macro expands to `-diff -merge -text`, so a trailing
     * `binary` unsets a preceding `merge=ours` (git check-attr merge reports
     * "unset" and concurrent artifact refreshes produce binary conflicts
     * instead of auto-resolving). The driver must come after the macro. */
    FILE *gaf = fopen(ga, "r");
    ASSERT_NOT_NULL(gaf);
    char content[512] = {0};
    size_t rd = fread(content, 1, sizeof(content) - 1, gaf);
    (void)fclose(gaf);
    ASSERT_TRUE(rd > 0);
    ASSERT_NOT_NULL(strstr(content, CBM_ARTIFACT_FILENAME " binary merge=ours"));
    ASSERT_TRUE(strstr(content, "merge=ours binary") == NULL);

    cleanup_dir(g_tmpdir);
    PASS();
}

TEST(artifact_export_rename_failure_logs_specific_error) {
    setup_artifact_test();
    create_test_db(g_db);

    char art_dir[1024];
    snprintf(art_dir, sizeof(art_dir), "%s/.codebase-memory", g_repo);
    cbm_mkdir_p(art_dir, 0755);

    char zst[1024];
    snprintf(zst, sizeof(zst), "%s/graph.db.zst", art_dir);
    cbm_mkdir_p(zst, 0755);

    capture_logs_start();
    int rc = cbm_artifact_export(g_db, g_repo, "test-proj", CBM_ARTIFACT_FAST);
    const char *logs = capture_logs_end();

    ASSERT_NEQ(rc, 0);
    ASSERT_FALSE(cbm_artifact_exists(g_repo));
    ASSERT_NOT_NULL(cbm_artifact_export_last_error());
    ASSERT(strstr(cbm_artifact_export_last_error(), "write_artifact") != NULL);
    ASSERT(strstr(cbm_artifact_export_last_error(), "rename_temp") != NULL);
    ASSERT(strstr(logs, "msg=artifact.export") != NULL);
    ASSERT(strstr(logs, "stage=write_artifact") != NULL);
    ASSERT(strstr(logs, "err=rename_temp") != NULL);

    cleanup_dir(g_tmpdir);
    PASS();
}

TEST(pipeline_persistence_export_failure_returns_error) {
    setup_artifact_test();

    char src[1024];
    snprintf(src, sizeof(src), "%s/main.c", g_repo);
    write_text_file(src, "int main(void) { return 0; }\n");

    char art_dir[1024];
    snprintf(art_dir, sizeof(art_dir), "%s/.codebase-memory", g_repo);
    cbm_mkdir_p(art_dir, 0755);

    char zst[1024];
    snprintf(zst, sizeof(zst), "%s/graph.db.zst", art_dir);
    cbm_mkdir_p(zst, 0755);

    cbm_pipeline_t *p = cbm_pipeline_new(g_repo, g_db, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_set_persistence(p, true);

    capture_logs_start();
    int rc = cbm_pipeline_run(p);
    const char *logs = capture_logs_end();
    cbm_pipeline_free(p);

    ASSERT_NEQ(rc, 0);
    ASSERT_FALSE(cbm_artifact_exists(g_repo));
    ASSERT(strstr(logs, "msg=pipeline.err") != NULL);
    ASSERT(strstr(logs, "phase=artifact_export") != NULL);

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

/* ── git shell-out path safety ────────────────────────────────────────────────
 *
 * artifact.c shells out to git via cbm_popen with the repo path interpolated into
 * the command. It previously used single quotes (`git -C '%s'`) with NO validation
 * — but cmd.exe does not honor single quotes, so on Windows a repo path with a space
 * broke argument grouping, and an embedded quote/metacharacter could break out of the
 * intended argument entirely. The hardening validates the path and switches to double
 * quotes; cbm_artifact_repo_path_is_shell_safe() is the guard. Rejecting quotes and
 * shell/cmd.exe metacharacters is the contract; spaces must stay allowed (double
 * quotes handle them) — that is the concrete regression the single-quote form caused. */
TEST(artifact_repo_path_shell_safe_accepts_plain_and_spaced) {
    ASSERT_TRUE(cbm_artifact_repo_path_is_shell_safe("/home/user/repo"));
    ASSERT_TRUE(cbm_artifact_repo_path_is_shell_safe("C:/Users/me/repo"));
    ASSERT_TRUE(cbm_artifact_repo_path_is_shell_safe("/home/user/my repo")); /* space OK */
    PASS();
}

TEST(artifact_repo_path_shell_safe_rejects_injection) {
    ASSERT_FALSE(cbm_artifact_repo_path_is_shell_safe(NULL));
    ASSERT_FALSE(cbm_artifact_repo_path_is_shell_safe("it's"));        /* single quote */
    ASSERT_FALSE(cbm_artifact_repo_path_is_shell_safe("a\"b"));        /* double quote */
    ASSERT_FALSE(cbm_artifact_repo_path_is_shell_safe("x; rm -rf /")); /* command sep */
    ASSERT_FALSE(cbm_artifact_repo_path_is_shell_safe("$(whoami)"));   /* substitution */
    ASSERT_FALSE(cbm_artifact_repo_path_is_shell_safe("a`id`b"));      /* backtick */
    ASSERT_FALSE(cbm_artifact_repo_path_is_shell_safe("a|b"));         /* pipe */
    PASS();
}

TEST(artifact_repo_path_shell_safe_rejects_cmd_metachars_on_windows) {
#ifdef _WIN32
    /* cmd.exe expands %VAR%, delayed !VAR!, and escapes with ^ even inside double
     * quotes — git_context.c rejects these on Windows and this must match. */
    ASSERT_FALSE(cbm_artifact_repo_path_is_shell_safe("C:/a%USERPROFILE%b"));
    ASSERT_FALSE(cbm_artifact_repo_path_is_shell_safe("C:/a!b"));
    ASSERT_FALSE(cbm_artifact_repo_path_is_shell_safe("C:/a^b"));
#else
    /* POSIX shells treat % ! ^ literally inside double quotes — allowed. */
    ASSERT_TRUE(cbm_artifact_repo_path_is_shell_safe("/a%b"));
    ASSERT_TRUE(cbm_artifact_repo_path_is_shell_safe("/a^b"));
#endif
    PASS();
}

SUITE(artifact) {
    RUN_TEST(artifact_repo_path_shell_safe_accepts_plain_and_spaced);
    RUN_TEST(artifact_repo_path_shell_safe_rejects_injection);
    RUN_TEST(artifact_repo_path_shell_safe_rejects_cmd_metachars_on_windows);
    RUN_TEST(artifact_export_fast_roundtrip);
    RUN_TEST(artifact_export_best_roundtrip);
    RUN_TEST(artifact_exists_check);
    RUN_TEST(artifact_commit_hash);
    RUN_TEST(artifact_schema_version_mismatch);
    RUN_TEST(artifact_import_missing);
    RUN_TEST(artifact_gitattributes_created);
    RUN_TEST(artifact_export_rename_failure_logs_specific_error);
    RUN_TEST(pipeline_persistence_export_failure_returns_error);
    RUN_TEST(artifact_import_rejects_size_mismatch);
    RUN_TEST(artifact_null_safety);
}
