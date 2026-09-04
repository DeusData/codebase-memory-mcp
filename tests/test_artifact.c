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
#include <stdarg.h>
#include <string.h>

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
    ASSERT_EQ(cbm_artifact_reconcile_hashes(NULL, "/tmp/x.db", "p"), -1);
    ASSERT_EQ(cbm_artifact_reconcile_hashes("/tmp", NULL, "p"), -1);
    ASSERT_EQ(cbm_artifact_reconcile_hashes("/tmp", "/tmp/x.db", NULL), -1);
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

/* #895: the FAST export path (watcher/incremental auto-update) read the
 * raw main-file bytes of a live WAL-mode store — committed rows still in
 * the -wal were missing and mid-checkpoint reads produced torn snapshots
 * that imported as page-corrupted caches. Export must snapshot
 * consistently (VACUUM INTO) on BOTH quality levels. */
TEST(artifact_fast_export_snapshots_live_wal_store) {
    setup_artifact_test();
    enum { WAL_NODES = 60 };

    /* Live store: rows committed but NOT checkpointed into the main file —
     * exactly the state the watcher export runs against. */
    cbm_store_t *s = cbm_store_open_path(g_db);
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "test-proj", "/tmp/test");
    for (int i = 0; i < WAL_NODES; i++) {
        char name[64];
        char qn[128];
        snprintf(name, sizeof(name), "walnode_%03d", i);
        snprintf(qn, sizeof(qn), "test-proj.mod.%s", name);
        cbm_node_t n = {.project = "test-proj",
                        .label = "Function",
                        .name = name,
                        .qualified_name = qn,
                        .file_path = "mod.py",
                        .start_line = i + 1,
                        .end_line = i + 2};
        ASSERT_TRUE(cbm_store_upsert_node(s, &n) > 0);
    }

    /* Export WHILE the writer connection is still open. */
    ASSERT_EQ(cbm_artifact_export(g_db, g_repo, "test-proj", CBM_ARTIFACT_FAST), 0);
    cbm_store_close(s);

    char import_db[1024];
    snprintf(import_db, sizeof(import_db), "%s/imported_wal.db", g_tmpdir);
    ASSERT_EQ(cbm_artifact_import(g_repo, import_db), 0);

    cbm_store_t *imp = cbm_store_open_path(import_db);
    ASSERT_NOT_NULL(imp);
    /* Torn snapshot = the WAL-resident rows are missing. */
    ASSERT_EQ(cbm_store_count_nodes(imp, "test-proj"), WAL_NODES);
    cbm_store_close(imp);
    PASS();
}

/* #895 (import half): page-level corruption must be refused at import.
 * The shallow integrity check only sanity-checks the projects table; the
 * deep variant runs PRAGMA quick_check and catches corrupt pages. */
TEST(store_deep_integrity_detects_page_corruption) {
    setup_artifact_test();
    enum { DEEP_NODES = 800, PAGE = 4096, ZERO_PAGES = 10 };
    char db2[1024];
    snprintf(db2, sizeof(db2), "%s/deep.db", g_tmpdir);
    cbm_store_t *s = cbm_store_open_path(db2);
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "deep", "/tmp/deep");
    for (int i = 0; i < DEEP_NODES; i++) {
        char name[64];
        char qn[192];
        snprintf(name, sizeof(name), "deep_probe_%04d", i);
        snprintf(qn, sizeof(qn), "deep.rather.long.module.path.for.page.fill.%s_pad_pad_pad",
                 name);
        cbm_node_t n = {.project = "deep",
                        .label = "Function",
                        .name = name,
                        .qualified_name = qn,
                        .file_path = "deep.py",
                        .start_line = i + 1,
                        .end_line = i + 2};
        ASSERT_TRUE(cbm_store_upsert_node(s, &n) > 0);
    }
    cbm_store_close(s);

    /* Healthy file passes the deep check. */
    cbm_store_t *ok = cbm_store_open_path(db2);
    ASSERT_NOT_NULL(ok);
    ASSERT_TRUE(cbm_store_check_integrity_deep(ok));
    cbm_store_close(ok);

    /* Zero a mid-file band and the deep check must refuse. */
    FILE *f = fopen(db2, "rb+");
    ASSERT_NOT_NULL(f);
    (void)fseek(f, 0, SEEK_END);
    long pages = ftell(f) / PAGE;
    ASSERT_TRUE(pages > ZERO_PAGES + 6);
    char zero[PAGE];
    memset(zero, 0, sizeof(zero));
    (void)fseek(f, (pages / 2) * (long)PAGE, SEEK_SET);
    for (int i = 0; i < ZERO_PAGES; i++) {
        ASSERT_EQ(fwrite(zero, 1, PAGE, f), (size_t)PAGE);
    }
    (void)fclose(f);

    cbm_store_t *bad = cbm_store_open_path(db2);
    ASSERT_NOT_NULL(bad);
    ASSERT_FALSE(cbm_store_check_integrity_deep(bad));
    cbm_store_close(bad);
    PASS();
}


/* ── Bootstrap reconciliation ─────────────────────────────────────────────────
 *
 * Ported from the contributor's stack (#868). Every git command below uses
 * DOUBLE quotes, matching the production rule stated in artifact.c: cmd.exe
 * does not honor single quotes, so `git -C '%s'` breaks on Windows. The same
 * rule that fixes production applies to this harness. */

/* printf-formatted system() wrapper; returns the exit status. */
static int runf(const char *fmt, ...) {
    char cmd[4096];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(cmd, sizeof(cmd), fmt, ap);
    va_end(ap);
    if (n < 0 || (size_t)n >= sizeof(cmd)) {
        return -1;
    }
    return system(cmd);
}

static void git_init(const char *repo) {
    runf("git -C \"%s\" init -q", repo);
    runf("git -C \"%s\" config user.email t@t.com", repo);
    runf("git -C \"%s\" config user.name t", repo);
    runf("git -C \"%s\" config commit.gpgsign false", repo);
}

/* Portable local mtime_ns for assertions (mirrors artifact.c's art_stat_mtime_ns
 * and pipeline_incremental.c's stat_mtime_ns — all three must agree or a
 * restamped row would never match the incremental classifier). */
static int64_t t_mtime_ns(const char *path) {
    struct stat st;
    if (stat(path, &st) != 0) {
        return -1;
    }
#ifdef __APPLE__
    return (int64_t)st.st_mtimespec.tv_sec * 1000000000LL + (int64_t)st.st_mtimespec.tv_nsec;
#elif defined(_WIN32)
    return (int64_t)st.st_mtime * 1000000000LL;
#else
    return (int64_t)st.st_mtim.tv_sec * 1000000000LL + (int64_t)st.st_mtim.tv_nsec;
#endif
}

/* True iff <repo>/.codebase-memory/artifact.json contains substr. */
static bool meta_contains(const char *repo, const char *substr) {
    char path[1024];
    snprintf(path, sizeof(path), "%s/.codebase-memory/artifact.json", repo);
    FILE *fp = fopen(path, "r");
    if (!fp) {
        return false;
    }
    char buf[4096];
    size_t n = fread(buf, 1, sizeof(buf) - 1, fp);
    fclose(fp);
    buf[n] = '\0';
    return strstr(buf, substr) != NULL;
}

static int64_t row_mtime(cbm_file_hash_t *rows, int n, const char *rel) {
    for (int i = 0; i < n; i++) {
        if (strcmp(rows[i].rel_path, rel) == 0) {
            return rows[i].mtime_ns;
        }
    }
    return -1;
}

/* Overwrite one file_hashes row's mtime with a caller-chosen sentinel.
 * Tests assert against this sentinel rather than against "a different
 * wall-clock time": on Windows mtime_ns has ONE-SECOND resolution, so two
 * writes in the same second are indistinguishable and an inequality
 * assertion would be a coin flip (O9 — a verdict must not depend on
 * filesystem timestamp granularity). */
static bool stamp_row_mtime(const char *db, const char *proj, const char *rel, int64_t sentinel,
                            const char *sha) {
    cbm_store_t *s = cbm_store_open_path(db);
    if (!s) {
        return false;
    }
    int rc = cbm_store_upsert_file_hash(s, proj, rel, sha ? sha : "", sentinel, 1);
    cbm_store_close(s);
    return rc == CBM_STORE_OK;
}

/* Why the last build_trusted_artifact_repo() call returned NULL. Read by the
 * BUILD_TRUSTED_REPO_OR_FAIL() call sites.
 *
 * A setup helper whose only failure signal is "NULL" is untestable on a
 * platform nobody can attach to: six distinct exits (git commit, pipeline
 * construction, pipeline run, marker absence, artifact commit, project-name
 * derivation) all reported the same thing, and CI could not tell a failed
 * `git commit` from a missing reconcile_basis marker. Every exit now names
 * itself and carries the number that decided it. */
static char g_build_repo_err[4096];

static void build_repo_errf(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(g_build_repo_err, sizeof(g_build_repo_err), fmt, ap);
    va_end(ap);
}

/* Report which of export's four clean-basis preconditions denied the marker.
 * Read straight from export's recorded blocker rather than recomputed: export's
 * own ensure_gitattributes leaves an untracked .gitattributes behind, so any
 * later evaluation answers tree_not_clean whatever the real cause was. Must run
 * on the thread that ran the pipeline (cbm_pipeline_run exports inline). */
static void describe_missing_marker(const char *repo) {
    const char *blocker = cbm_artifact_reconcile_basis_last_blocker();
    char meta[1152];
    snprintf(meta, sizeof(meta), "%s/.codebase-memory/artifact.json", repo);
    struct stat st;
    build_repo_errf(
        "step=reconcile_basis: marker absent from %s (artifact.json %s); export blocked by: %s",
        meta, stat(meta, &st) == 0 ? "present" : "MISSING",
        blocker ? blocker
                : "<nothing — export believed it wrote the marker, so the file read here is not "
                  "the file export wrote>");
}

/* Build a git repo at g_repo with 5 .rs files, full-index + export (clean tree
 * -> reconcile_basis marker set), and commit .codebase-memory so a clone receives
 * the artifact. Returns the derived project name (caller frees) or NULL, with
 * g_build_repo_err naming the failing step. */
static char *build_trusted_artifact_repo(void) {
    g_build_repo_err[0] = '\0';
    git_init(g_repo);
    const char *names[] = {"a.rs", "b.rs", "c.rs", "d.rs", "e.rs"};
    for (int i = 0; i < 5; i++) {
        char p[1024];
        snprintf(p, sizeof(p), "%s/%s", g_repo, names[i]);
        char body[128];
        snprintf(body, sizeof(body), "pub fn f%d() {}\n", i);
        write_text_file(p, body);
    }
    int git_rc = runf("git -C \"%s\" add -A && git -C \"%s\" commit -qm init", g_repo, g_repo);
    if (git_rc != 0) {
        build_repo_errf("step=commit_sources: `git add -A && git commit -qm init` in %s "
                        "exited status=%d",
                        g_repo, git_rc);
        return NULL;
    }
    cbm_pipeline_t *p = cbm_pipeline_new(g_repo, g_db, CBM_MODE_FAST);
    if (!p) {
        build_repo_errf("step=pipeline_new: cbm_pipeline_new(repo=%s, db=%s) returned NULL", g_repo,
                        g_db);
        return NULL;
    }
    cbm_pipeline_set_persistence(p, true);
    int rc = cbm_pipeline_run(p);
    cbm_pipeline_free(p);
    if (rc != 0) {
        build_repo_errf("step=pipeline_run: cbm_pipeline_run(repo=%s) rc=%d", g_repo, rc);
        return NULL;
    }
    char *project = cbm_project_name_from_path(g_repo);
    if (!project) {
        build_repo_errf("step=project_name: cbm_project_name_from_path(%s) returned NULL", g_repo);
        return NULL;
    }
    /* Clean source tree at export -> marker must be present. */
    if (!meta_contains(g_repo, "\"reconcile_basis\"")) {
        describe_missing_marker(g_repo);
        free(project);
        return NULL;
    }
    git_rc = runf("git -C \"%s\" add -A && git -C \"%s\" commit -qm artifact", g_repo, g_repo);
    if (git_rc != 0) {
        build_repo_errf("step=commit_artifact: `git add -A && git commit -qm artifact` in %s "
                        "exited status=%d",
                        g_repo, git_rc);
        free(project);
        return NULL;
    }
    return project;
}

/* Call-site form: keeps file:line on the failing TEST while printing the step
 * that actually failed inside the helper. */
#define BUILD_TRUSTED_REPO_OR_FAIL(var)        \
    do {                                       \
        (var) = build_trusted_artifact_repo(); \
        if (!(var)) {                          \
            FAIL(g_build_repo_err);            \
        }                                      \
    } while (0)

/* Clones g_repo (A) into <tmp>/work/repo (B) so both share the basename "repo"
 * and thus the derived project name — required for artifact bootstrap. */
static bool clone_to_b(char *repoB_out, size_t repoB_sz) {
    char work[1024];
    snprintf(work, sizeof(work), "%s/work", g_tmpdir);
    cbm_mkdir_p(work, 0755);
    snprintf(repoB_out, repoB_sz, "%s/repo", work);
    if (runf("git clone -q \"%s\" \"%s\"", g_repo, repoB_out) != 0) {
        return false;
    }
    /* A clone does NOT inherit the source repo's local user config, and CI
     * runners have no global identity -- `git commit` then exits 128. Set it
     * on the clone the same way git_init() does for the source repo. */
    return runf("git -C \"%s\" config user.email t@t.com", repoB_out) == 0 &&
           runf("git -C \"%s\" config user.name t", repoB_out) == 0;
}

TEST(artifact_export_marks_clean_basis) {
    setup_artifact_test();
    git_init(g_repo);
    char src[1024];
    snprintf(src, sizeof(src), "%s/a.rs", g_repo);
    write_text_file(src, "pub fn f0() {}\n");
    ASSERT_EQ(runf("git -C \"%s\" add -A && git -C \"%s\" commit -qm init", g_repo, g_repo), 0);

    char *proj = cbm_project_name_from_path(g_repo);
    ASSERT_NOT_NULL(proj);

    /* Full index + export on a clean source tree -> clean-basis marker set.
     * This assert is the one that failed on Windows CI for the original patch:
     * the single-quoted `:(exclude)` pathspec made tree_clean_for_reconcile
     * always report dirty under cmd.exe, so the marker was never written. It is
     * a PRODUCTION assert, not a harness artifact. */
    cbm_pipeline_t *p = cbm_pipeline_new(g_repo, g_db, CBM_MODE_FAST);
    ASSERT_NOT_NULL(p);
    cbm_pipeline_set_persistence(p, true);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    cbm_pipeline_free(p);
    if (!meta_contains(g_repo, "\"reconcile_basis\"")) {
        describe_missing_marker(g_repo);
        free(proj);
        FAIL(g_build_repo_err);
    }

    /* Dirty the source tree (uncommitted edit). Re-export must omit the marker:
     * the tree is non-clean outside .codebase-memory (and the a.rs hash row no
     * longer matches disk). */
    write_text_file(src, "pub fn dirty() {}\n");
    ASSERT_EQ(cbm_artifact_export(g_db, g_repo, proj, CBM_ARTIFACT_FAST), 0);
    ASSERT_FALSE(meta_contains(g_repo, "\"reconcile_basis\""));

    free(proj);
    cleanup_dir(g_tmpdir);
    PASS();
}

TEST(artifact_reconcile_restamps_unchanged) {
    setup_artifact_test();
    char *proj = NULL;
    BUILD_TRUSTED_REPO_OR_FAIL(proj);

    char repoB[1024];
    ASSERT(clone_to_b(repoB, sizeof(repoB)));

    /* B: modify 2 files + add 1, commit. (Clone gave B fresh mtimes.) */
    char m1[1152], m2[1152], add[1152];
    snprintf(m1, sizeof(m1), "%s/a.rs", repoB);
    snprintf(m2, sizeof(m2), "%s/b.rs", repoB);
    snprintf(add, sizeof(add), "%s/added.rs", repoB);
    write_text_file(m1, "pub fn f0() { /* changed */ }\n");
    write_text_file(m2, "pub fn f1() { /* changed */ }\n");
    write_text_file(add, "pub fn new_fn() {}\n");
    ASSERT_EQ(runf("git -C \"%s\" add -A && git -C \"%s\" commit -qm edits", repoB, repoB), 0);

    /* Import A's artifact into a fresh cache DB for B. */
    char dbB[1152];
    snprintf(dbB, sizeof(dbB), "%s/b.db", g_tmpdir);
    ASSERT_EQ(cbm_artifact_import(repoB, dbB), 0);

    /* Pin a.rs's row to a sentinel so "changed rows stay foreign" is a
     * deterministic assertion instead of an mtime-granularity race. */
    const int64_t foreign_mtime = 12345;
    ASSERT(stamp_row_mtime(dbB, proj, "a.rs", foreign_mtime, ""));

    /* Reconcile: 5 rows; a.rs+b.rs changed (left foreign), 3 unchanged restamped. */
    int restamped = cbm_artifact_reconcile_hashes(repoB, dbB, proj);
    ASSERT_EQ(restamped, 3);

    /* Unchanged row (c.rs) now carries B's local mtime; the changed row (a.rs)
     * still carries its foreign stamp — untouched. */
    cbm_store_t *s = cbm_store_open_path(dbB);
    ASSERT_NOT_NULL(s);
    cbm_file_hash_t *rows = NULL;
    int n = 0;
    ASSERT_EQ(cbm_store_get_file_hashes(s, proj, &rows, &n), 0);
    char c_path[1152];
    snprintf(c_path, sizeof(c_path), "%s/c.rs", repoB);
    ASSERT_EQ(row_mtime(rows, n, "c.rs"), t_mtime_ns(c_path));
    ASSERT_EQ(row_mtime(rows, n, "a.rs"), foreign_mtime);
    cbm_store_free_file_hashes(rows, n);
    cbm_store_close(s);

    free(proj);
    cleanup_dir(g_tmpdir);
    PASS();
}

TEST(artifact_reconcile_skips_untracked_rows) {
    setup_artifact_test();
    char *proj = NULL;
    BUILD_TRUSTED_REPO_OR_FAIL(proj);
    char repoB[1024];
    ASSERT(clone_to_b(repoB, sizeof(repoB)));

    /* B: a gitignored-yet-indexed file (the .cbmignore-negation shape, #500).
     * git diff/ls-files are both blind to it, so without the tracked-at-commit
     * gate it would be restamped as "unchanged" even though git cannot vouch
     * for its content. */
    char gen[1152], gi[1152];
    snprintf(gen, sizeof(gen), "%s/gen.rs", repoB);
    snprintf(gi, sizeof(gi), "%s/.gitignore", repoB);
    write_text_file(gen, "pub fn generated_local() {}\n");
    write_text_file(gi, "gen.rs\n");
    ASSERT_EQ(runf("git -C \"%s\" add .gitignore && git -C \"%s\" commit -qm ignore", repoB, repoB),
              0);

    char dbB[1152];
    snprintf(dbB, sizeof(dbB), "%s/b.db", g_tmpdir);
    ASSERT_EQ(cbm_artifact_import(repoB, dbB), 0);

    /* Simulate the exporter having indexed gen.rs: insert a foreign-mtime row. */
    const int64_t foreign_mtime = 12345;
    ASSERT(stamp_row_mtime(dbB, proj, "gen.rs", foreign_mtime, ""));

    /* Reconcile: the 5 tracked unchanged rows restamp; gen.rs must not. */
    ASSERT_EQ(cbm_artifact_reconcile_hashes(repoB, dbB, proj), 5);

    cbm_store_t *s = cbm_store_open_path(dbB);
    ASSERT_NOT_NULL(s);
    cbm_file_hash_t *rows = NULL;
    int n = 0;
    ASSERT_EQ(cbm_store_get_file_hashes(s, proj, &rows, &n), CBM_STORE_OK);
    ASSERT_EQ(row_mtime(rows, n, "gen.rs"), foreign_mtime);
    char c_path[1152];
    snprintf(c_path, sizeof(c_path), "%s/c.rs", repoB);
    ASSERT_EQ(row_mtime(rows, n, "c.rs"), t_mtime_ns(c_path));
    cbm_store_free_file_hashes(rows, n);
    cbm_store_close(s);

    free(proj);
    cleanup_dir(g_tmpdir);
    PASS();
}

TEST(artifact_reconcile_skips_untrusted_metadata) {
    setup_artifact_test();
    char *proj = NULL;
    BUILD_TRUSTED_REPO_OR_FAIL(proj);
    char repoB[1024];
    ASSERT(clone_to_b(repoB, sizeof(repoB)));

    char dbB[1152];
    snprintf(dbB, sizeof(dbB), "%s/b.db", g_tmpdir);
    ASSERT_EQ(cbm_artifact_import(repoB, dbB), 0);

    /* Snapshot one foreign mtime, then strip the trust marker. */
    cbm_store_t *s = cbm_store_open_path(dbB);
    ASSERT_NOT_NULL(s);
    cbm_file_hash_t *rows = NULL;
    int n = 0;
    ASSERT_EQ(cbm_store_get_file_hashes(s, proj, &rows, &n), CBM_STORE_OK);
    int64_t before = row_mtime(rows, n, "c.rs");
    cbm_store_free_file_hashes(rows, n);
    cbm_store_close(s);

    char meta[1152];
    snprintf(meta, sizeof(meta), "%s/.codebase-memory/artifact.json", repoB);
    /* Rewrite artifact.json without reconcile_basis (schema_version preserved). */
    FILE *fp = fopen(meta, "w");
    ASSERT_NOT_NULL(fp);
    fprintf(fp, "{\"schema_version\":2,\"commit\":\"deadbeef\","
                "\"original_size\":1000,\"indexed_at\":\"2026-01-01T00:00:00Z\"}");
    fclose(fp);

    ASSERT_EQ(cbm_artifact_reconcile_hashes(repoB, dbB, proj), -1);

    /* Rows untouched: still the foreign value captured before. */
    s = cbm_store_open_path(dbB);
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_get_file_hashes(s, proj, &rows, &n), CBM_STORE_OK);
    ASSERT_EQ(row_mtime(rows, n, "c.rs"), before);
    cbm_store_free_file_hashes(rows, n);
    cbm_store_close(s);

    free(proj);
    cleanup_dir(g_tmpdir);
    PASS();
}

TEST(artifact_reconcile_skips_unknown_commit) {
    setup_artifact_test();
    char *proj = NULL;
    BUILD_TRUSTED_REPO_OR_FAIL(proj);
    char repoB[1024];
    ASSERT(clone_to_b(repoB, sizeof(repoB)));

    char dbB[1152];
    snprintf(dbB, sizeof(dbB), "%s/b.db", g_tmpdir);
    ASSERT_EQ(cbm_artifact_import(repoB, dbB), 0);

    /* Rewrite artifact.json: keep the marker but point commit at a hex-valid
     * SHA that does not exist locally -> the object-existence gate fails. */
    char meta[1152];
    snprintf(meta, sizeof(meta), "%s/.codebase-memory/artifact.json", repoB);
    FILE *fp = fopen(meta, "w");
    ASSERT_NOT_NULL(fp);
    fprintf(fp,
            "{\"schema_version\":2,\"commit\":\"%s\","
            "\"original_size\":1000,\"reconcile_basis\":\"git-clean-head\"}",
            "1111111111111111111111111111111111111111");
    fclose(fp);

    ASSERT_EQ(cbm_artifact_reconcile_hashes(repoB, dbB, proj), -1);

    free(proj);
    cleanup_dir(g_tmpdir);
    PASS();
}

TEST(artifact_reconcile_skips_non_hex_commit) {
    setup_artifact_test();
    char *proj = NULL;
    BUILD_TRUSTED_REPO_OR_FAIL(proj);
    char repoB[1024];
    ASSERT(clone_to_b(repoB, sizeof(repoB)));

    char dbB[1152];
    snprintf(dbB, sizeof(dbB), "%s/b.db", g_tmpdir);
    ASSERT_EQ(cbm_artifact_import(repoB, dbB), 0);

    /* A commit field carrying shell metacharacters must never reach a command
     * string: is_hex_oid is the hard gate in front of every interpolation. */
    char meta[1152];
    snprintf(meta, sizeof(meta), "%s/.codebase-memory/artifact.json", repoB);
    FILE *fp = fopen(meta, "w");
    ASSERT_NOT_NULL(fp);
    fprintf(fp, "{\"schema_version\":2,\"commit\":\"a$(touch pwned)b\","
                "\"original_size\":1000,\"reconcile_basis\":\"git-clean-head\"}");
    fclose(fp);

    ASSERT_EQ(cbm_artifact_reconcile_hashes(repoB, dbB, proj), -1);

    char pwned[1152];
    snprintf(pwned, sizeof(pwned), "%s/pwned", repoB);
    struct stat st;
    ASSERT_NEQ(stat(pwned, &st), 0);

    free(proj);
    cleanup_dir(g_tmpdir);
    PASS();
}

TEST(artifact_reconcile_skips_without_git) {
    setup_artifact_test();
    char *proj = NULL;
    BUILD_TRUSTED_REPO_OR_FAIL(proj);
    char repoB[1024];
    ASSERT(clone_to_b(repoB, sizeof(repoB)));

    char dbB[1152];
    snprintf(dbB, sizeof(dbB), "%s/b.db", g_tmpdir);
    ASSERT_EQ(cbm_artifact_import(repoB, dbB), 0);

    /* Disable the repo by renaming .git (portable; `rm -rf` is not a cmd.exe
     * builtin). Marker + commit still present, but every git call now fails. */
    char gitdir[1152], disabled[1152];
    snprintf(gitdir, sizeof(gitdir), "%s/.git", repoB);
    snprintf(disabled, sizeof(disabled), "%s/.git-disabled", repoB);
    ASSERT_EQ(cbm_rename_replace(gitdir, disabled), 0);

    ASSERT_EQ(cbm_artifact_reconcile_hashes(repoB, dbB, proj), -1);

    free(proj);
    cleanup_dir(g_tmpdir);
    PASS();
}

SUITE(artifact) {
    RUN_TEST(artifact_fast_export_snapshots_live_wal_store);
    RUN_TEST(store_deep_integrity_detects_page_corruption);
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
    RUN_TEST(artifact_export_marks_clean_basis);
    RUN_TEST(artifact_reconcile_restamps_unchanged);
    RUN_TEST(artifact_reconcile_skips_untracked_rows);
    RUN_TEST(artifact_reconcile_skips_untrusted_metadata);
    RUN_TEST(artifact_reconcile_skips_unknown_commit);
    RUN_TEST(artifact_reconcile_skips_non_hex_commit);
    RUN_TEST(artifact_reconcile_skips_without_git);
}
