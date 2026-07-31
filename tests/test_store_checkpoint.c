/*
 * test_store_checkpoint.c — Tests for WAL checkpoint behavior.
 *
 * Verifies that cbm_store_checkpoint() does not truncate the on-disk
 * WAL file. SQLITE_CHECKPOINT_TRUNCATE shrinks the WAL via ftruncate(fd, 0)
 * on success; on macOS this can raise SIGBUS in a sibling process that
 * has the DB mmap'd through SQLite when it next faults a page in the
 * now-shorter region. SQLITE_CHECKPOINT_PASSIVE marks frames as
 * checkpointed in the WAL header without changing the file size — disk
 * space is reclaimed on the next write cycle, not on every checkpoint.
 */
#include "../src/foundation/compat.h"
#include "../src/foundation/log.h"
#include "test_framework.h"
#include "test_helpers.h"
#include <store/store.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static char g_publish_prepare_log[CBM_SZ_4K];

static void capture_publish_prepare_log(const char *line) {
    if (!line) {
        return;
    }
    size_t used = strlen(g_publish_prepare_log);
    size_t available = sizeof(g_publish_prepare_log) - used;
    if (available > 1) {
        snprintf(g_publish_prepare_log + used, available, "%s\n", line);
    }
}

TEST(checkpoint_does_not_truncate_wal) {
    enum { N_ROWS = 100, PATH_BUF = 256, PATH_BUF_EXT = 300 };
    char db_path[PATH_BUF];
    snprintf(db_path, sizeof(db_path), "%s/cbm_test_ckpt_%d.db", cbm_tmpdir(), (int)getpid());
    char wal_path[PATH_BUF_EXT];
    snprintf(wal_path, sizeof(wal_path), "%s-wal", db_path);
    char shm_path[PATH_BUF_EXT];
    snprintf(shm_path, sizeof(shm_path), "%s-shm", db_path);
    unlink(db_path);
    unlink(wal_path);
    unlink(shm_path);

    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT(s != NULL);

    /* Grow WAL beyond zero bytes via direct SQL. */
    int rc_sql = cbm_store_exec(s, "INSERT OR IGNORE INTO projects(name, indexed_at, root_path) "
                                   "VALUES('p', '2026-01-01', '/tmp/p');");
    ASSERT_EQ(rc_sql, 0);
    for (int i = 0; i < N_ROWS; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "INSERT INTO nodes(project, label, name, qualified_name, file_path) "
                 "VALUES('p', 'Function', 'fn', 'p.module.fn_%d', 'f.c');",
                 i);
        rc_sql = cbm_store_exec(s, sql);
        ASSERT_EQ(rc_sql, 0);
    }

    /* WAL must exist and be non-empty before the checkpoint call. */
    struct stat st_before;
    int rc_stat = stat(wal_path, &st_before);
    ASSERT_EQ(rc_stat, 0);
    ASSERT(st_before.st_size > 0);

    /* Under SQLITE_CHECKPOINT_TRUNCATE the WAL would be ftruncate()d to 0
     * bytes on success. Under SQLITE_CHECKPOINT_PASSIVE the file size is
     * preserved (frames marked, not removed). */
    int rc_ckpt = cbm_store_checkpoint(s);
    ASSERT_EQ(rc_ckpt, 0); /* CBM_STORE_OK */

    struct stat st_after;
    rc_stat = stat(wal_path, &st_after);
    ASSERT_EQ(rc_stat, 0);
    ASSERT(st_after.st_size > 0);

    cbm_store_close(s);
    unlink(db_path);
    unlink(wal_path);
    unlink(shm_path);
    PASS();
}

/* #897: any code path installing a fresh DB file must delete the
 * destination's -wal/-shm first. SQLite decides whether to replay a WAL
 * purely from the sidecar's own header/checksums — a leftover WAL from a
 * crashed previous session is recovered ON TOP of the freshly installed
 * file at the next open, splicing old-generation pages into it (short
 * indexes, btreeInitPage failures, or resurrected stale rows).
 *
 * Repro (per the issue): hot-copy a live WAL aside, close cleanly, restore
 * the copy as the crashed-session leftover, install a fresh generation via
 * cbm_store_dump_to_file, reopen — the stale generation's row must NOT be
 * visible and the fresh row must be. */
static int tsc_copy_file(const char *src, const char *dst) {
    FILE *in = fopen(src, "rb");
    if (!in) {
        return -1;
    }
    FILE *out = fopen(dst, "wb");
    if (!out) {
        (void)fclose(in);
        return -1;
    }
    char buf[4096];
    size_t n;
    while ((n = fread(buf, 1, sizeof(buf), in)) > 0) {
        if (fwrite(buf, 1, n, out) != n) {
            (void)fclose(in);
            (void)fclose(out);
            return -1;
        }
    }
    (void)fclose(in);
    (void)fclose(out);
    return 0;
}

TEST(dump_install_ignores_stale_wal_sidecar) {
    char *td = th_mktempdir("cbm_stalewal");
    char db_path[512];
    char wal_path[512];
    char stale_copy[512];
    snprintf(db_path, sizeof(db_path), "%s/gen.db", td);
    snprintf(wal_path, sizeof(wal_path), "%s-wal", db_path);
    snprintf(stale_copy, sizeof(stale_copy), "%s/stale.wal", td);

    /* Generation 1: file-backed store with a marker row living in the WAL. */
    cbm_store_t *s1 = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s1);
    cbm_store_upsert_project(s1, "walgen", "/tmp/walgen");
    cbm_node_t stale = {.project = "walgen",
                        .label = "Function",
                        .name = "stale_gen_node",
                        .qualified_name = "walgen.mod.stale_gen_node",
                        .file_path = "mod.py",
                        .start_line = 1,
                        .end_line = 2};
    ASSERT_TRUE(cbm_store_upsert_node(s1, &stale) > 0);

    /* Hot-copy the live WAL (must be non-empty or the repro is vacuous). */
    struct stat st_wal = {0};
    ASSERT_EQ(stat(wal_path, &st_wal), 0);
    ASSERT_TRUE(st_wal.st_size > 0);
    ASSERT_EQ(tsc_copy_file(wal_path, stale_copy), 0);
    cbm_store_close(s1); /* clean close checkpoints + removes the WAL */

    /* Simulate the crashed previous session's leftover sidecar. */
    ASSERT_EQ(tsc_copy_file(stale_copy, wal_path), 0);

    /* Generation 2: fresh store installed over db_path. */
    cbm_store_t *s2 = cbm_store_open_memory();
    ASSERT_NOT_NULL(s2);
    cbm_store_upsert_project(s2, "walgen", "/tmp/walgen");
    cbm_node_t fresh = {.project = "walgen",
                        .label = "Function",
                        .name = "fresh_gen_node",
                        .qualified_name = "walgen.mod.fresh_gen_node",
                        .file_path = "mod.py",
                        .start_line = 1,
                        .end_line = 2};
    ASSERT_TRUE(cbm_store_upsert_node(s2, &fresh) > 0);
    ASSERT_EQ(cbm_store_dump_to_file(s2, db_path), CBM_STORE_OK);
    cbm_store_close(s2);

    /* Reader: the stale WAL must not have been replayed onto gen 2. */
    cbm_store_t *s3 = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s3);
    cbm_node_t *hits = NULL;
    int hit_count = 0;
    ASSERT_EQ(cbm_store_find_nodes_by_name(s3, "walgen", "fresh_gen_node", &hits, &hit_count),
              CBM_STORE_OK);
    ASSERT_TRUE(hit_count >= 1);
    cbm_store_free_nodes(hits, hit_count);
    hits = NULL;
    hit_count = 0;
    ASSERT_EQ(cbm_store_find_nodes_by_name(s3, "walgen", "stale_gen_node", &hits, &hit_count),
              CBM_STORE_OK);
    ASSERT_EQ(hit_count, 0);
    cbm_store_free_nodes(hits, hit_count);
    cbm_store_close(s3);

    unlink(wal_path);
    char shm_path[512];
    snprintf(shm_path, sizeof(shm_path), "%s-shm", db_path);
    unlink(shm_path);
    unlink(db_path);
    unlink(stale_copy);
    PASS();
}

TEST(remove_db_sidecars_rejects_truncated_suffix_path) {
    /* The implementation's sidecar buffer is 4096 bytes. Before the fix,
     * snprintf truncation simply skipped every unlink and still returned
     * success, violating the helper's fail-closed contract. */
    char db_path[4096];
    memset(db_path, 'x', sizeof(db_path) - 1);
    db_path[sizeof(db_path) - 1] = '\0';

    ASSERT_TRUE(cbm_remove_db_sidecars(db_path) != 0);
    PASS();
}

/* An active WAL snapshot can prevent detaching the WAL journal after committed
 * frames have been checkpointed. Publication must fail closed instead of
 * unlinking sidecars that the reader still needs, and its diagnostic must
 * distinguish journal detachment from checkpoint or rename failures. Sealing
 * scans W WAL frames in O(W) time and uses O(1) caller memory; after the reader
 * releases its snapshot, the journal transition succeeds. */
TEST(prepare_for_replace_reports_journal_detach_blocked_by_active_reader) {
    char *td = th_mktempdir("cbm_publish_reader");
    ASSERT_NOT_NULL(td);
    char db_path[CBM_SZ_512];
    snprintf(db_path, sizeof(db_path), "%s/graph.db", td);

    cbm_store_t *writer = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(writer);
    ASSERT_EQ(cbm_store_exec(writer, "INSERT INTO projects(name,indexed_at,root_path) "
                                     "VALUES('p','2026-07-31','/tmp/p');"),
              CBM_STORE_OK);

    sqlite3 *reader = NULL;
    ASSERT_EQ(sqlite3_open_v2(db_path, &reader, SQLITE_OPEN_READONLY, NULL), SQLITE_OK);
    ASSERT_EQ(sqlite3_exec(reader, "BEGIN;", NULL, NULL, NULL), SQLITE_OK);
    sqlite3_stmt *snapshot = NULL;
    ASSERT_EQ(sqlite3_prepare_v2(reader, "SELECT count(*) FROM projects;", CBM_NOT_FOUND, &snapshot,
                                 NULL),
              SQLITE_OK);
    ASSERT_EQ(sqlite3_step(snapshot), SQLITE_ROW);
    sqlite3_finalize(snapshot);

    ASSERT_EQ(cbm_store_exec(writer, "INSERT INTO projects(name,indexed_at,root_path) "
                                     "VALUES('after','2026-07-31','/tmp/after');"),
              CBM_STORE_OK);
    cbm_store_close(writer);

    g_publish_prepare_log[0] = '\0';
    CBMLogLevel prior_level = cbm_log_get_level();
    cbm_log_set_level(CBM_LOG_DEBUG);
    cbm_log_set_sink(capture_publish_prepare_log);
    int blocked_rc = cbm_store_prepare_path_for_replace(db_path);
    cbm_log_set_sink(NULL);
    cbm_log_set_level(prior_level);

    ASSERT_EQ(blocked_rc, CBM_STORE_ERR);
    ASSERT_NOT_NULL(strstr(g_publish_prepare_log, "store.publish_prepare.err"));
    ASSERT_NOT_NULL(strstr(g_publish_prepare_log, "journal_delete_step"));

    ASSERT_EQ(sqlite3_exec(reader, "COMMIT;", NULL, NULL, NULL), SQLITE_OK);
    ASSERT_EQ(sqlite3_close(reader), SQLITE_OK);
    ASSERT_EQ(cbm_store_prepare_path_for_replace(db_path), CBM_STORE_OK);

    (void)cbm_remove_db_sidecars(db_path);
    (void)cbm_unlink(db_path);
    cbm_rmdir(td);
    PASS();
}

SUITE(store_checkpoint) {
    RUN_TEST(checkpoint_does_not_truncate_wal);
    RUN_TEST(dump_install_ignores_stale_wal_sidecar);
    RUN_TEST(remove_db_sidecars_rejects_truncated_suffix_path);
    RUN_TEST(prepare_for_replace_reports_journal_detach_blocked_by_active_reader);
}
