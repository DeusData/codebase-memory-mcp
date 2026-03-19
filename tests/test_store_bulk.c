/*
 * test_store_bulk.c — Crash-safety tests for bulk write mode.
 *
 * Verifies that cbm_store_begin_bulk / cbm_store_end_bulk never switch away
 * from WAL journal mode.  Switching to MEMORY journal mode during bulk writes
 * makes the database unrecoverable on a crash because the in-memory rollback
 * journal is lost.  WAL mode is inherently crash-safe: uncommitted WAL entries
 * are discarded on the next open.
 *
 * Tests:
 *   bulk_pragma_wal_invariant     — journal_mode stays "wal" after begin_bulk
 *   bulk_pragma_end_wal_invariant — journal_mode stays "wal" after end_bulk
 *   bulk_crash_recovery           — DB is readable after simulated crash mid-bulk
 */
#include "test_framework.h"
#include <store/store.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifndef _WIN32
#include <unistd.h>
#include <sys/wait.h>
#endif

/* ── Helpers ──────────────────────────────────────────────────── */

/* Query journal_mode via a separate read-only connection so the result is
 * independent of any state held inside the cbm_store_t under test. */
static char *get_journal_mode(const char *db_path) {
    sqlite3 *db;
    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK)
        return NULL;
    sqlite3_stmt *stmt;
    char *mode = NULL;
    if (sqlite3_prepare_v2(db, "PRAGMA journal_mode;", -1, &stmt, NULL) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW)
            mode = strdup((const char *)sqlite3_column_text(stmt, 0));
        sqlite3_finalize(stmt);
    }
    sqlite3_close(db);
    return mode;
}

static void make_temp_path(char *buf, size_t n) {
    snprintf(buf, n, "/tmp/cmm_bulk_test_%d.db", (int)getpid());
}

static void cleanup_db(const char *path) {
    remove(path);
    char aux[512];
    snprintf(aux, sizeof(aux), "%s-wal", path);
    remove(aux);
    snprintf(aux, sizeof(aux), "%s-shm", path);
    remove(aux);
}

/* ── Tests ──────────────────────────────────────────────────────── */

/* begin_bulk must NOT switch journal_mode away from WAL. */
TEST(bulk_pragma_wal_invariant) {
    char db_path[256];
    make_temp_path(db_path, sizeof(db_path));
    cleanup_db(db_path);

    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);

    char *before = get_journal_mode(db_path);
    ASSERT_NOT_NULL(before);
    ASSERT_STR_EQ(before, "wal");
    free(before);

    int rc = cbm_store_begin_bulk(s);
    ASSERT_EQ(rc, CBM_STORE_OK);

    char *after = get_journal_mode(db_path);
    ASSERT_NOT_NULL(after);
    ASSERT_STR_EQ(after, "wal"); /* FAILS with bug, PASSES with fix */
    free(after);

    cbm_store_end_bulk(s);
    cbm_store_close(s);
    cleanup_db(db_path);
    PASS();
}

/* end_bulk must also leave journal_mode as WAL. */
TEST(bulk_pragma_end_wal_invariant) {
    char db_path[256];
    make_temp_path(db_path, sizeof(db_path));
    cleanup_db(db_path);

    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);

    cbm_store_begin_bulk(s);
    cbm_store_end_bulk(s);

    char *mode = get_journal_mode(db_path);
    ASSERT_NOT_NULL(mode);
    ASSERT_STR_EQ(mode, "wal");
    free(mode);

    cbm_store_close(s);
    cleanup_db(db_path);
    PASS();
}

/* Simulate a crash mid-bulk-write: fork a child that calls begin_bulk, opens
 * an explicit transaction, and then calls _exit() without committing or calling
 * end_bulk.  The parent verifies the database is still openable and that
 * committed baseline data is intact and uncommitted data is absent.
 *
 * This test uses fork()/waitpid() and is therefore POSIX-only. */
#ifndef _WIN32
TEST(bulk_crash_recovery) {
    char db_path[256];
    make_temp_path(db_path, sizeof(db_path));
    cleanup_db(db_path);

    /* Write committed baseline data. */
    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);
    int rc = cbm_store_upsert_project(s, "baseline", "/tmp/baseline");
    ASSERT_EQ(rc, CBM_STORE_OK);
    cbm_store_close(s);

    /* Child: enter bulk mode, start a transaction, write, then crash. */
    pid_t pid = fork();
    if (pid == 0) {
        cbm_store_t *cs = cbm_store_open_path(db_path);
        if (!cs)
            _exit(1);
        cbm_store_begin_bulk(cs);
        cbm_store_begin(cs); /* explicit open transaction */
        cbm_store_upsert_project(cs, "crashed", "/tmp/crashed");
        /* Crash: no COMMIT, no end_bulk, no close. */
        _exit(0);
    }
    ASSERT_GT(pid, 0);
    int status;
    waitpid(pid, &status, 0);
    /* Confirm child exited normally so the write actually occurred. */
    ASSERT(WIFEXITED(status) && WEXITSTATUS(status) == 0);

    /* Recovery: database must open cleanly. */
    cbm_store_t *recovered = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(recovered); /* NULL would indicate corruption */

    /* Baseline commit must survive. */
    cbm_project_t p = {0};
    rc = cbm_store_get_project(recovered, "baseline", &p);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_STR_EQ(p.name, "baseline");
    cbm_project_free_fields(&p);

    /* Uncommitted "crashed" write must NOT appear after recovery. */
    cbm_project_t p2 = {0};
    int rc2 = cbm_store_get_project(recovered, "crashed", &p2);
    ASSERT_NEQ(rc2, CBM_STORE_OK); /* row must be absent */

    cbm_store_close(recovered);
    cleanup_db(db_path);
    PASS();
}
#endif /* _WIN32 */

/* ── Suite ──────────────────────────────────────────────────────── */

SUITE(store_bulk) {
    RUN_TEST(bulk_pragma_wal_invariant);
    RUN_TEST(bulk_pragma_end_wal_invariant);
#ifndef _WIN32
    RUN_TEST(bulk_crash_recovery);
#endif
}
