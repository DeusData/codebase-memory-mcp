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
#include <foundation/compat.h>
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
    snprintf(buf, n, "%s/cmm_bulk_test_%d.db", cbm_tmpdir(), (int)getpid());
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

TEST(coverage_meta_has_independent_unresolved_completeness) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    sqlite3_stmt *stmt = NULL;
    ASSERT_EQ(sqlite3_prepare_v2(cbm_store_get_db(s),
                                 "SELECT unresolved_calls_complete FROM index_coverage_meta;", -1,
                                 &stmt, NULL),
              SQLITE_OK);
    sqlite3_finalize(stmt);
    cbm_store_close(s);
    PASS();
}

TEST(coverage_meta_legacy_reader_preserves_general_metadata) {
    char path[256];
    make_temp_path(path, sizeof(path));
    cleanup_db(path);
    cbm_store_t *writer = cbm_store_open_path(path);
    ASSERT_NOT_NULL(writer);
    ASSERT_EQ(cbm_store_upsert_project(writer, "legacy-coverage", "/tmp/legacy-coverage"),
              CBM_STORE_OK);
    ASSERT_EQ(
        sqlite3_exec(cbm_store_get_db(writer),
                     "DROP TABLE index_coverage_meta;"
                     "CREATE TABLE index_coverage_meta(project TEXT PRIMARY KEY,generation TEXT,"
                     "index_mode TEXT,recorded_at TEXT,recording_status TEXT,"
                     "ignored_files_stored INTEGER,ignored_files_total INTEGER,"
                     "coverage_version INTEGER,hash_records_complete INTEGER);"
                     "INSERT INTO index_coverage_meta VALUES('legacy-coverage','generation','full',"
                     "'recorded','truncated',1,2,5,1);",
                     NULL, NULL, NULL),
        SQLITE_OK);
    cbm_store_close(writer);
    cbm_store_t *reader = cbm_store_open_path_query(path);
    ASSERT_NOT_NULL(reader);
    cbm_coverage_meta_t meta = {0};
    ASSERT_EQ(cbm_store_coverage_meta_get(reader, "legacy-coverage", &meta), CBM_STORE_OK);
    ASSERT_STR_EQ(meta.recording_status, "truncated");
    ASSERT_EQ(meta.ignored_files_stored, 1);
    ASSERT_EQ(meta.ignored_files_total, 2);
    ASSERT_EQ(meta.coverage_version, 5);
    ASSERT_TRUE(meta.hash_records_complete);
    ASSERT_FALSE(meta.unresolved_calls_complete);
    cbm_store_coverage_meta_clear(&meta);
    cbm_store_close(reader);
    writer = cbm_store_open_path(path);
    ASSERT_NOT_NULL(writer);
    sqlite3_stmt *stmt = NULL;
    ASSERT_EQ(sqlite3_prepare_v2(cbm_store_get_db(writer),
                                 "SELECT unresolved_calls_complete FROM index_coverage_meta;", -1,
                                 &stmt, NULL),
              SQLITE_OK);
    ASSERT_EQ(sqlite3_step(stmt), SQLITE_ROW);
    ASSERT_EQ(sqlite3_column_int(stmt, 0), 0);
    sqlite3_finalize(stmt);
    ASSERT_EQ(cbm_store_coverage_meta_get(writer, "legacy-coverage", &meta), CBM_STORE_OK);
    ASSERT_STR_EQ(meta.recording_status, "truncated");
    ASSERT_FALSE(meta.unresolved_calls_complete);
    cbm_store_coverage_meta_clear(&meta);
    cbm_store_close(writer);
    cleanup_db(path);
    PASS();
}

TEST(coverage_meta_completeness_round_trip_and_failed_replace) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    const char *project = "capture-meta";
    ASSERT_EQ(cbm_store_upsert_project(s, project, "/tmp/capture-meta"), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_upsert_file_hash(s, project, "gap.js", "fixture", 0, 0), CBM_STORE_OK);
    cbm_coverage_row_t row = {.rel_path = "gap.js", .kind = "parse_partial", .detail = "2-3"};
    cbm_coverage_meta_t meta = {.generation = "baseline",
                                .index_mode = "full",
                                .recording_status = "complete",
                                .coverage_version = 5,
                                .hash_records_complete = true,
                                .unresolved_calls_complete = true};
    ASSERT_EQ(cbm_store_coverage_replace_ex(s, project, &row, 1, &meta), CBM_STORE_OK);
    cbm_coverage_meta_t fetched = {0};
    ASSERT_EQ(cbm_store_coverage_meta_get(s, project, &fetched), CBM_STORE_OK);
    ASSERT_TRUE(fetched.unresolved_calls_complete);
    cbm_store_coverage_meta_clear(&fetched);
    ASSERT_EQ(
        sqlite3_exec(cbm_store_get_db(s),
                     "CREATE TRIGGER reject_capture_meta BEFORE UPDATE ON index_coverage_meta "
                     "BEGIN SELECT RAISE(ABORT, 'metadata fault'); END;",
                     NULL, NULL, NULL),
        SQLITE_OK);
    meta.generation = "failed";
    meta.unresolved_calls_complete = false;
    ASSERT_EQ(cbm_store_coverage_replace_ex(s, project, NULL, 0, &meta), CBM_STORE_ERR);
    ASSERT_EQ(cbm_store_coverage_meta_get(s, project, &fetched), CBM_STORE_OK);
    ASSERT_STR_EQ(fetched.generation, "baseline");
    ASSERT_TRUE(fetched.unresolved_calls_complete);
    cbm_store_coverage_meta_clear(&fetched);
    cbm_coverage_row_t *rows = NULL;
    int count = 0;
    ASSERT_EQ(cbm_store_coverage_get(s, project, &rows, &count), CBM_STORE_OK);
    ASSERT_EQ(count, 1);
    ASSERT_STR_EQ(rows[0].kind, "parse_partial");
    cbm_store_free_coverage(rows, count);
    ASSERT_EQ(
        sqlite3_exec(cbm_store_get_db(s), "DROP TRIGGER reject_capture_meta;", NULL, NULL, NULL),
        SQLITE_OK);
    ASSERT_EQ(cbm_store_coverage_replace_ex(s, project, &row, 1, &meta), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_coverage_meta_get(s, project, &fetched), CBM_STORE_OK);
    ASSERT_FALSE(fetched.unresolved_calls_complete);
    ASSERT_STR_EQ(fetched.recording_status, "complete");
    cbm_store_coverage_meta_clear(&fetched);
    ASSERT_EQ(cbm_store_coverage_replace_ex(s, project, &row, 1, NULL), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_coverage_meta_get(s, project, &fetched), CBM_STORE_NOT_FOUND);
    cbm_store_close(s);
    PASS();
}

typedef struct {
    int statements;
    int fullscan_steps;
    int vm_steps;
} unresolved_query_cost_t;

static int record_unresolved_query_cost(unsigned type, void *userdata, void *statement,
                                        void *elapsed) {
    (void)elapsed;
    unresolved_query_cost_t *cost = userdata;
    sqlite3_stmt *stmt = statement;
    const char *sql = sqlite3_sql(stmt);
    if (type == SQLITE_TRACE_PROFILE && sql &&
        (strstr(sql, "FROM index_unresolved_candidates") ||
         strstr(sql, "AND kind = 'unresolved_calls'"))) {
        cost->statements++;
        cost->fullscan_steps += sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_FULLSCAN_STEP, 0);
        cost->vm_steps += sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_VM_STEP, 0);
    }
    return 0;
}

TEST(unresolved_coverage_queries_use_file_and_candidate_indexes) {
    enum { UNRELATED_FILES = 2000 };
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    const char *project = "bounded-lookups";
    ASSERT_EQ(cbm_store_upsert_project(s, project, "/tmp/bounded-lookups"), CBM_STORE_OK);
    sqlite3 *db = cbm_store_get_db(s);
    ASSERT_EQ(sqlite3_exec(db, "BEGIN;", NULL, NULL, NULL), SQLITE_OK);
    sqlite3_stmt *coverage = NULL;
    sqlite3_stmt *candidate = NULL;
    ASSERT_EQ(sqlite3_prepare_v2(db,
                                 "INSERT INTO index_coverage(project,rel_path,kind,detail) "
                                 "VALUES(?1,?2,'unresolved_calls',?3);",
                                 -1, &coverage, NULL),
              SQLITE_OK);
    ASSERT_EQ(
        sqlite3_prepare_v2(db,
                           "INSERT INTO index_unresolved_candidates(project,candidate,rel_path) "
                           "VALUES(?1,?2,?3);",
                           -1, &candidate, NULL),
        SQLITE_OK);
    for (int i = 0; i <= UNRELATED_FILES; i++) {
        char path[64];
        snprintf(path, sizeof(path), "file-%04d.js", i);
        ASSERT_EQ(sqlite3_bind_text(coverage, 1, project, -1, SQLITE_STATIC), SQLITE_OK);
        ASSERT_EQ(sqlite3_bind_text(coverage, 2, path, -1, SQLITE_TRANSIENT), SQLITE_OK);
        ASSERT_EQ(sqlite3_bind_text(coverage, 3,
                                    i == 0 ? "[{\"candidate\":\"target.fn\"}]"
                                           : "[{\"candidate\":\"other.fn\"}]",
                                    -1, SQLITE_STATIC),
                  SQLITE_OK);
        ASSERT_EQ(sqlite3_step(coverage), SQLITE_DONE);
        ASSERT_EQ(sqlite3_reset(coverage), SQLITE_OK);
        ASSERT_EQ(sqlite3_bind_text(candidate, 1, project, -1, SQLITE_STATIC), SQLITE_OK);
        ASSERT_EQ(
            sqlite3_bind_text(candidate, 2, i == 0 ? "target.fn" : "other.fn", -1, SQLITE_STATIC),
            SQLITE_OK);
        ASSERT_EQ(sqlite3_bind_text(candidate, 3, path, -1, SQLITE_TRANSIENT), SQLITE_OK);
        ASSERT_EQ(sqlite3_step(candidate), SQLITE_DONE);
        ASSERT_EQ(sqlite3_reset(candidate), SQLITE_OK);
    }
    sqlite3_finalize(coverage);
    sqlite3_finalize(candidate);
    ASSERT_EQ(sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL), SQLITE_OK);
    unresolved_query_cost_t cost = {0};
    ASSERT_EQ(sqlite3_trace_v2(db, SQLITE_TRACE_PROFILE, record_unresolved_query_cost, &cost),
              SQLITE_OK);
    cbm_coverage_row_t *rows = NULL;
    int count = 0;
    ASSERT_EQ(cbm_store_coverage_get_unresolved_path(s, project, "file-0000.js", &rows, &count),
              CBM_STORE_OK);
    ASSERT_EQ(count, 1);
    ASSERT_STR_EQ(rows[0].rel_path, "file-0000.js");
    cbm_store_free_coverage(rows, count);
    bool found = false;
    ASSERT_EQ(cbm_store_coverage_has_unresolved_candidate(s, project, "target.fn", &found),
              CBM_STORE_OK);
    ASSERT_TRUE(found);
    ASSERT_EQ(cbm_store_coverage_has_unresolved_candidate(s, project, "absent.fn", &found),
              CBM_STORE_OK);
    ASSERT_FALSE(found);
    ASSERT_EQ(sqlite3_trace_v2(db, 0, NULL, NULL), SQLITE_OK);
    ASSERT_EQ(cost.statements, 3);
    ASSERT_EQ(cost.fullscan_steps, 0);
    ASSERT_TRUE(cost.vm_steps < 300);
    cbm_store_close(s);
    PASS();
}

TEST(unresolved_candidate_index_tracks_replacement_pruning_and_markers) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    const char *project = "candidate-lifecycle";
    ASSERT_EQ(cbm_store_upsert_project(s, project, "/tmp/candidate-lifecycle"), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_upsert_file_hash(s, project, "calls.js", "fixture", 0, 0), CBM_STORE_OK);
    cbm_coverage_row_t row = {.rel_path = "calls.js",
                              .kind = "unresolved_calls",
                              .detail = "[{\"candidate\":\"old.fn\"}]"};
    ASSERT_EQ(cbm_store_coverage_replace(s, project, &row, 1), CBM_STORE_OK);
    bool found = false;
    ASSERT_EQ(cbm_store_coverage_has_unresolved_candidate(s, project, "old.fn", &found),
              CBM_STORE_OK);
    ASSERT_TRUE(found);
    row.detail = "[{\"candidate\":\"new.fn\"}]";
    ASSERT_EQ(cbm_store_coverage_replace(s, project, &row, 1), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_coverage_has_unresolved_candidate(s, project, "old.fn", &found),
              CBM_STORE_OK);
    ASSERT_FALSE(found);
    ASSERT_EQ(cbm_store_coverage_has_unresolved_candidate(s, project, "new.fn", &found),
              CBM_STORE_OK);
    ASSERT_TRUE(found);
    const char *markers[] = {"[{\"truncated\":true}]", "invalid-json", "{}", "[1]"};
    for (size_t i = 0; i < sizeof(markers) / sizeof(markers[0]); i++) {
        row.detail = markers[i];
        ASSERT_EQ(cbm_store_coverage_replace(s, project, &row, 1), CBM_STORE_OK);
        ASSERT_EQ(cbm_store_coverage_has_unresolved_candidate(s, project, "absent.fn", &found),
                  CBM_STORE_OK);
        ASSERT_TRUE(found);
    }
    ASSERT_EQ(cbm_store_delete_file_hash(s, project, "calls.js"), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_coverage_replace(s, project, &row, 1), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_coverage_has_unresolved_candidate(s, project, "absent.fn", &found),
              CBM_STORE_OK);
    ASSERT_FALSE(found);
    ASSERT_EQ(cbm_store_upsert_file_hash(s, project, "calls.js", "fixture", 0, 0), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_coverage_replace(s, project, &row, 1), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_delete_project(s, project), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_coverage_has_unresolved_candidate(s, project, "absent.fn", &found),
              CBM_STORE_OK);
    ASSERT_FALSE(found);
    cbm_store_close(s);
    PASS();
}

/* ── Suite ──────────────────────────────────────────────────────── */

SUITE(store_bulk) {
    RUN_TEST(coverage_meta_has_independent_unresolved_completeness);
    RUN_TEST(coverage_meta_legacy_reader_preserves_general_metadata);
    RUN_TEST(coverage_meta_completeness_round_trip_and_failed_replace);
    RUN_TEST(unresolved_coverage_queries_use_file_and_candidate_indexes);
    RUN_TEST(unresolved_candidate_index_tracks_replacement_pruning_and_markers);
    RUN_TEST(bulk_pragma_wal_invariant);
    RUN_TEST(bulk_pragma_end_wal_invariant);
#ifndef _WIN32
    RUN_TEST(bulk_crash_recovery);
#endif
}
