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
#include "test_framework.h"
#include "test_helpers.h"
#include <store/store.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

TEST(checkpoint_does_not_truncate_wal) {
    enum { N_ROWS = 100, PATH_BUF = 256, PATH_BUF_EXT = 300 };
    char db_path[PATH_BUF];
    snprintf(db_path, sizeof(db_path), "/tmp/cbm_test_ckpt_%d.db", (int)getpid());
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
    int rc_sql = cbm_store_exec(
        s,
        "INSERT OR IGNORE INTO projects(name, indexed_at, root_path) "
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

SUITE(store_checkpoint) {
    RUN_TEST(checkpoint_does_not_truncate_wal);
}
