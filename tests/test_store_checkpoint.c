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
#include <errno.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static void tsc_cleanup_db(const char *db_path) {
    char sidecar[512];
    unlink(db_path);
    snprintf(sidecar, sizeof(sidecar), "%s-wal", db_path);
    unlink(sidecar);
    snprintf(sidecar, sizeof(sidecar), "%s-shm", db_path);
    unlink(sidecar);
}

static int tsc_raw_journal_mode(const char *db_path, char *mode, size_t mode_size) {
    sqlite3 *db = NULL;
    sqlite3_stmt *stmt = NULL;
    int ok = 0;

    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        goto done;
    }
    if (sqlite3_prepare_v2(db, "PRAGMA journal_mode;", -1, &stmt, NULL) != SQLITE_OK) {
        goto done;
    }
    if (sqlite3_step(stmt) != SQLITE_ROW) {
        goto done;
    }
    const unsigned char *value = sqlite3_column_text(stmt, 0);
    if (!value || snprintf(mode, mode_size, "%s", (const char *)value) >= (int)mode_size) {
        goto done;
    }
    ok = 1;

done:
    sqlite3_finalize(stmt);
    if (db) {
        sqlite3_close(db);
    }
    return ok;
}

static int tsc_raw_project_count(const char *db_path, const char *project) {
    sqlite3 *db = NULL;
    sqlite3_stmt *stmt = NULL;
    int count = -1;

    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        goto done;
    }
    if (sqlite3_prepare_v2(db, "SELECT count(*) FROM projects WHERE name=?1;", -1, &stmt, NULL) !=
        SQLITE_OK) {
        goto done;
    }
    if (sqlite3_bind_text(stmt, 1, project, -1, SQLITE_STATIC) != SQLITE_OK) {
        goto done;
    }
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        count = sqlite3_column_int(stmt, 0);
    }

done:
    sqlite3_finalize(stmt);
    if (db) {
        sqlite3_close(db);
    }
    return count;
}

static int tsc_wal_absent_or_empty(const char *db_path) {
    char wal_path[512];
    struct stat st = {0};
    snprintf(wal_path, sizeof(wal_path), "%s-wal", db_path);
    if (stat(wal_path, &st) == 0) {
        return st.st_size == 0;
    }
    return errno == ENOENT;
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

TEST(seal_for_atomic_publish_makes_main_file_self_contained) {
    char db_path[256];
    char wal_path[300];
    snprintf(db_path, sizeof(db_path), "%s/cbm_test_seal_%d.db", cbm_tmpdir(), (int)getpid());
    snprintf(wal_path, sizeof(wal_path), "%s-wal", db_path);
    tsc_cleanup_db(db_path);

    cbm_store_t *store = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(store);
    ASSERT_EQ(cbm_store_upsert_project(store, "sealed", "/tmp/sealed"), CBM_STORE_OK);

    /* Prove this exercises a live WAL, rather than an already self-contained
     * database that would make the checkpoint assertion vacuous. */
    struct stat wal_before = {0};
    ASSERT_EQ(stat(wal_path, &wal_before), 0);
    ASSERT_TRUE(wal_before.st_size > 0);

    ASSERT_EQ(cbm_store_seal_for_atomic_publish(store), CBM_STORE_OK);

    /* Verify through an independent raw connection. The marker must be in the
     * main file and the mode returned by SQLite itself must be DELETE. */
    char mode[16] = {0};
    ASSERT_TRUE(tsc_raw_journal_mode(db_path, mode, sizeof(mode)));
    ASSERT_STR_EQ(mode, "delete");
    ASSERT_TRUE(tsc_wal_absent_or_empty(db_path));
    ASSERT_EQ(tsc_raw_project_count(db_path, "sealed"), 1);

    cbm_store_close(store);
    tsc_cleanup_db(db_path);
    PASS();
}

TEST(seal_for_atomic_publish_fails_closed_while_reader_pins_wal) {
    char db_path[256];
    char wal_path[300];
    snprintf(db_path, sizeof(db_path), "%s/cbm_test_seal_busy_%d.db", cbm_tmpdir(), (int)getpid());
    snprintf(wal_path, sizeof(wal_path), "%s-wal", db_path);
    tsc_cleanup_db(db_path);

    cbm_store_t *store = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(store);
    ASSERT_EQ(cbm_store_upsert_project(store, "reader_snapshot", "/tmp/reader"), CBM_STORE_OK);

    /* Hold an actual SELECT row so the raw connection pins its WAL snapshot. */
    sqlite3 *reader = NULL;
    sqlite3_stmt *pin = NULL;
    ASSERT_EQ(sqlite3_open_v2(db_path, &reader, SQLITE_OPEN_READONLY, NULL), SQLITE_OK);
    ASSERT_EQ(sqlite3_exec(reader, "BEGIN;", NULL, NULL, NULL), SQLITE_OK);
    ASSERT_EQ(sqlite3_prepare_v2(reader, "SELECT name FROM projects;", -1, &pin, NULL), SQLITE_OK);
    ASSERT_EQ(sqlite3_step(pin), SQLITE_ROW);

    /* This later frame cannot be folded into the main file while the older
     * reader snapshot remains pinned. Disable the test connection's timeout so
     * SQLITE_BUSY is deterministic and immediate. */
    ASSERT_EQ(cbm_store_upsert_project(store, "after_snapshot", "/tmp/after"), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_exec(store, "PRAGMA busy_timeout=0;"), CBM_STORE_OK);
    struct stat wal_before = {0};
    ASSERT_EQ(stat(wal_path, &wal_before), 0);
    ASSERT_TRUE(wal_before.st_size > 0);
    ASSERT_EQ(cbm_store_seal_for_atomic_publish(store), CBM_STORE_ERR);

    /* Releasing the reader must make the exact same store sealable. */
    ASSERT_EQ(sqlite3_finalize(pin), SQLITE_OK);
    pin = NULL;
    ASSERT_EQ(sqlite3_exec(reader, "COMMIT;", NULL, NULL, NULL), SQLITE_OK);
    ASSERT_EQ(sqlite3_close(reader), SQLITE_OK);
    reader = NULL;
    ASSERT_EQ(cbm_store_seal_for_atomic_publish(store), CBM_STORE_OK);

    char mode[16] = {0};
    ASSERT_TRUE(tsc_raw_journal_mode(db_path, mode, sizeof(mode)));
    ASSERT_STR_EQ(mode, "delete");
    ASSERT_TRUE(tsc_wal_absent_or_empty(db_path));
    ASSERT_EQ(tsc_raw_project_count(db_path, "reader_snapshot"), 1);
    ASSERT_EQ(tsc_raw_project_count(db_path, "after_snapshot"), 1);

    cbm_store_close(store);
    tsc_cleanup_db(db_path);
    PASS();
}

TEST(cached_count_queries_release_delete_mode_reader_lock) {
    char db_path[256];
    snprintf(db_path, sizeof(db_path), "%s/cbm_test_count_lock_%d.db", cbm_tmpdir(), (int)getpid());
    tsc_cleanup_db(db_path);

    cbm_store_t *setup = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(setup);
    ASSERT_EQ(cbm_store_upsert_project(setup, "count_lock", "/tmp/count_lock"), CBM_STORE_OK);
    cbm_node_t node = {.project = "count_lock",
                       .label = "Function",
                       .name = "target",
                       .qualified_name = "count_lock.target",
                       .file_path = "target.c",
                       .start_line = 1,
                       .end_line = 1};
    ASSERT_TRUE(cbm_store_upsert_node(setup, &node) > 0);
    ASSERT_EQ(cbm_store_seal_for_atomic_publish(setup), CBM_STORE_OK);
    cbm_store_close(setup);

    /* index_repository opens a query connection to verify the newly published
     * DELETE-mode DB. Cached COUNT statements must not remain parked on their
     * result row, otherwise a following writer cannot switch the DB back to WAL. */
    cbm_store_t *reader = cbm_store_open_path_query(db_path);
    ASSERT_NOT_NULL(reader);
    ASSERT_EQ(cbm_store_count_nodes(reader, "count_lock"), 1);
    ASSERT_EQ(cbm_store_count_edges(reader, "count_lock"), 0);

    cbm_store_t *writer = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(writer);

    cbm_store_close(writer);
    cbm_store_close(reader);
    tsc_cleanup_db(db_path);
    PASS();
}

TEST(cached_node_lookups_release_delete_mode_reader_lock) {
    char db_path[256];
    snprintf(db_path, sizeof(db_path), "%s/cbm_test_lookup_lock_%d.db", cbm_tmpdir(),
             (int)getpid());
    tsc_cleanup_db(db_path);

    cbm_store_t *setup = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(setup);
    ASSERT_EQ(cbm_store_upsert_project(setup, "lookup_lock", "/tmp/lookup_lock"), CBM_STORE_OK);
    cbm_node_t node = {.project = "lookup_lock",
                       .label = "Function",
                       .name = "target",
                       .qualified_name = "lookup_lock.target",
                       .file_path = "target.c",
                       .start_line = 1,
                       .end_line = 1};
    int64_t node_id = cbm_store_upsert_node(setup, &node);
    ASSERT_TRUE(node_id > 0);
    ASSERT_EQ(cbm_store_seal_for_atomic_publish(setup), CBM_STORE_OK);
    cbm_store_close(setup);

    cbm_store_t *reader = cbm_store_open_path_query(db_path);
    ASSERT_NOT_NULL(reader);
    cbm_node_t found = {0};
    ASSERT_EQ(cbm_store_find_node_by_id(reader, node_id, &found), CBM_STORE_OK);
    cbm_node_free_fields(&found);
    ASSERT_EQ(cbm_store_find_node_by_qn(reader, "lookup_lock", "lookup_lock.target", &found),
              CBM_STORE_OK);
    cbm_node_free_fields(&found);
    ASSERT_EQ(cbm_store_find_node_by_qn_any(reader, "lookup_lock.target", &found), CBM_STORE_OK);
    cbm_node_free_fields(&found);

    cbm_store_t *writer = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(writer);

    cbm_store_close(writer);
    cbm_store_close(reader);
    tsc_cleanup_db(db_path);
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

SUITE(store_checkpoint) {
    RUN_TEST(checkpoint_does_not_truncate_wal);
    RUN_TEST(seal_for_atomic_publish_makes_main_file_self_contained);
    RUN_TEST(seal_for_atomic_publish_fails_closed_while_reader_pins_wal);
    RUN_TEST(cached_count_queries_release_delete_mode_reader_lock);
    RUN_TEST(cached_node_lookups_release_delete_mode_reader_lock);
    RUN_TEST(dump_install_ignores_stale_wal_sidecar);
}
