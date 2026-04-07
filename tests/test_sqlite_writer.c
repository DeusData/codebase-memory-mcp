/*
 * test_sqlite_writer.c — Tests for direct SQLite page writer.
 *
 * Ports from internal/cbm/sqlite_writer_test.go:
 *   TestWriteDB_MinimalData, TestWriteDB_ScaleAndIndexes, TestWriteDB_Empty
 *
 * The page writer (cbm_write_db) constructs B-tree pages directly,
 * bypassing the SQL parser entirely. These tests verify integrity.
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
/* sqlite_writer.h is at internal/cbm/ — Makefile adds -Iinternal/cbm */
#include "sqlite_writer.h" /* CBMDumpNode, CBMDumpEdge, cbm_write_db */
#include "sqlite3.h"       /* vendored/sqlite3/ via -Ivendored/sqlite3 */
#include <unistd.h>

/* ── Helper: create temp file path ─────────────────────────────── */

static int make_temp_db(char *path, size_t pathsz) {
    snprintf(path, pathsz, "/tmp/cbm_sw_test_XXXXXX");
    int fd = cbm_mkstemp(path);
    if (fd < 0)
        return -1;
    close(fd);
    return 0;
}

/* ── Tests ─────────────────────────────────────────────────────── */

TEST(sw_minimal_data) {
    char path[256];
    ASSERT_EQ(make_temp_db(path, sizeof(path)), 0);

    CBMDumpNode nodes[2] = {
        {.id = 1,
         .project = "test",
         .label = "Module",
         .name = "main",
         .qualified_name = "test.main",
         .file_path = "main.go",
         .start_line = 1,
         .end_line = 10,
         .properties = "{}"},
        {.id = 2,
         .project = "test",
         .label = "Function",
         .name = "hello",
         .qualified_name = "test.main.hello",
         .file_path = "main.go",
         .start_line = 3,
         .end_line = 5,
         .properties = "{}"},
    };
    CBMDumpEdge edges[1] = {
        {.id = 1,
         .project = "test",
         .source_id = 1,
         .target_id = 2,
         .type = "DEFINES",
         .properties = "{}",
         .url_path = ""},
    };

    int rc = cbm_write_db(path, "test", "/tmp/test", "2026-03-14T00:00:00Z", nodes, 2, edges, 1, NULL, 0, NULL, 0);
    ASSERT_EQ(rc, 0);

    /* Verify via SQLite */
    sqlite3 *db = NULL;
    rc = sqlite3_open(path, &db);
    ASSERT_EQ(rc, SQLITE_OK);

    /* Integrity check */
    sqlite3_stmt *stmt = NULL;
    sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1, &stmt, NULL);
    rc = sqlite3_step(stmt);
    ASSERT_EQ(rc, SQLITE_ROW);
    const char *integrity = (const char *)sqlite3_column_text(stmt, 0);
    ASSERT_STR_EQ(integrity, "ok");
    sqlite3_finalize(stmt);

    /* Node count */
    sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM nodes", -1, &stmt, NULL);
    sqlite3_step(stmt);
    ASSERT_EQ(sqlite3_column_int(stmt, 0), 2);
    sqlite3_finalize(stmt);

    /* Edge count */
    sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM edges", -1, &stmt, NULL);
    sqlite3_step(stmt);
    ASSERT_EQ(sqlite3_column_int(stmt, 0), 1);
    sqlite3_finalize(stmt);

    /* Project row */
    sqlite3_prepare_v2(db, "SELECT name, root_path FROM projects", -1, &stmt, NULL);
    sqlite3_step(stmt);
    ASSERT_STR_EQ((const char *)sqlite3_column_text(stmt, 0), "test");
    ASSERT_STR_EQ((const char *)sqlite3_column_text(stmt, 1), "/tmp/test");
    sqlite3_finalize(stmt);

    /* Node content: check node 2 */
    sqlite3_prepare_v2(db, "SELECT qualified_name, label FROM nodes WHERE id=2", -1, &stmt, NULL);
    rc = sqlite3_step(stmt);
    ASSERT_EQ(rc, SQLITE_ROW);
    ASSERT_STR_EQ((const char *)sqlite3_column_text(stmt, 0), "test.main.hello");
    ASSERT_STR_EQ((const char *)sqlite3_column_text(stmt, 1), "Function");
    sqlite3_finalize(stmt);

    /* Edge content: check edge 1 */
    sqlite3_prepare_v2(db, "SELECT source_id, target_id, type FROM edges WHERE id=1", -1, &stmt,
                       NULL);
    rc = sqlite3_step(stmt);
    ASSERT_EQ(rc, SQLITE_ROW);
    ASSERT_EQ(sqlite3_column_int64(stmt, 0), 1);
    ASSERT_EQ(sqlite3_column_int64(stmt, 1), 2);
    ASSERT_STR_EQ((const char *)sqlite3_column_text(stmt, 2), "DEFINES");
    sqlite3_finalize(stmt);

    sqlite3_close(db);
    unlink(path);
    PASS();
}

TEST(sw_scale_and_indexes) {
    char path[256];
    ASSERT_EQ(make_temp_db(path, sizeof(path)), 0);

    /* 100 nodes across multiple files/labels */
    CBMDumpNode nodes[100];
    const char *labels[] = {"Function", "Method", "Class", "Module", "Variable"};
    const char *files[] = {"alpha.go", "beta.go", "gamma.py", "delta.ts", "epsilon.rs"};
    char names[100][32];
    char qns[100][64];

    for (int i = 0; i < 100; i++) {
        snprintf(names[i], sizeof(names[i]), "sym_%03d", i);
        snprintf(qns[i], sizeof(qns[i]), "proj.pkg.sym_%03d", i);
        nodes[i] = (CBMDumpNode){
            .id = i + 1,
            .project = "proj",
            .label = labels[i % 5],
            .name = names[i],
            .qualified_name = qns[i],
            .file_path = files[i % 5],
            .start_line = i * 10 + 1,
            .end_line = i * 10 + 9,
            .properties = "{}",
        };
    }

    /* 200 edges with varied types — build unique (source, target, type) combos */
    const char *edge_types[] = {"CALLS", "DEFINES", "IMPORTS", "IMPLEMENTS", "USES"};
    CBMDumpEdge edges[200];
    char eprops[200][80];
    int edge_count = 0;
    int64_t edge_id = 1;

    /* Track seen keys via simple hash — good enough for 200 edges */
    typedef struct {
        int64_t s, t;
        int ty;
    } ekey_t;
    ekey_t seen[200];
    int nseen = 0;

    for (int i = 0; edge_count < 200 && i < 10000; i++) {
        int64_t src = (i % 100) + 1;
        int64_t tgt = ((i * 7 + 3) % 100) + 1;
        if (tgt == src)
            tgt = (tgt % 100) + 1;
        int ty_idx = i % 5;

        /* Check duplicate */
        int dup = 0;
        for (int j = 0; j < nseen; j++) {
            if (seen[j].s == src && seen[j].t == tgt && seen[j].ty == ty_idx) {
                dup = 1;
                break;
            }
        }
        if (dup)
            continue;
        seen[nseen++] = (ekey_t){src, tgt, ty_idx};

        snprintf(eprops[edge_count], sizeof(eprops[edge_count]), "{\"weight\":%d}", i);

        edges[edge_count] = (CBMDumpEdge){
            .id = edge_id++,
            .project = "proj",
            .source_id = src,
            .target_id = tgt,
            .type = edge_types[ty_idx],
            .properties = eprops[edge_count],
            .url_path = "",
        };
        edge_count++;
    }

    int rc =
        cbm_write_db(path, "proj", "/repo", "2026-03-14T12:00:00Z", nodes, 100, edges, edge_count, NULL, 0, NULL, 0);
    ASSERT_EQ(rc, 0);

    sqlite3 *db = NULL;
    rc = sqlite3_open(path, &db);
    ASSERT_EQ(rc, SQLITE_OK);

    /* Integrity */
    sqlite3_stmt *stmt = NULL;
    sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1, &stmt, NULL);
    sqlite3_step(stmt);
    ASSERT_STR_EQ((const char *)sqlite3_column_text(stmt, 0), "ok");
    sqlite3_finalize(stmt);

    /* Row counts */
    int nc = 0, ec = 0;
    sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM nodes", -1, &stmt, NULL);
    sqlite3_step(stmt);
    nc = sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt);
    ASSERT_EQ(nc, 100);

    sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM edges", -1, &stmt, NULL);
    sqlite3_step(stmt);
    ec = sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt);
    ASSERT_EQ(ec, edge_count);

    /* Index queries — exercise each index */
    int cnt = 0;

    /* label index */
    sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM nodes WHERE project='proj' AND label='Function'",
                       -1, &stmt, NULL);
    sqlite3_step(stmt);
    cnt = sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt);
    ASSERT_EQ(cnt, 20);

    /* name index */
    sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM nodes WHERE project='proj' AND name='sym_042'", -1,
                       &stmt, NULL);
    sqlite3_step(stmt);
    cnt = sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt);
    ASSERT_EQ(cnt, 1);

    /* file_path index */
    sqlite3_prepare_v2(db,
                       "SELECT COUNT(*) FROM nodes WHERE project='proj' AND file_path='alpha.go'",
                       -1, &stmt, NULL);
    sqlite3_step(stmt);
    cnt = sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt);
    ASSERT_EQ(cnt, 20);

    /* edge type index */
    sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM edges WHERE project='proj' AND type='DEFINES'", -1,
                       &stmt, NULL);
    sqlite3_step(stmt);
    cnt = sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt);
    ASSERT_GT(cnt, 0);

    /* QN unique lookup */
    sqlite3_prepare_v2(
        db, "SELECT id FROM nodes WHERE project='proj' AND qualified_name='proj.pkg.sym_050'", -1,
        &stmt, NULL);
    rc = sqlite3_step(stmt);
    ASSERT_EQ(rc, SQLITE_ROW);
    ASSERT_EQ(sqlite3_column_int64(stmt, 0), 51);
    sqlite3_finalize(stmt);

    sqlite3_close(db);
    unlink(path);
    PASS();
}

TEST(sw_empty) {
    char path[256];
    ASSERT_EQ(make_temp_db(path, sizeof(path)), 0);

    int rc = cbm_write_db(path, "test", "/tmp/test", "2026-03-14T00:00:00Z", NULL, 0, NULL, 0,
                          NULL, 0, NULL, 0);
    ASSERT_EQ(rc, 0);

    sqlite3 *db = NULL;
    rc = sqlite3_open(path, &db);
    ASSERT_EQ(rc, SQLITE_OK);

    sqlite3_stmt *stmt = NULL;
    sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1, &stmt, NULL);
    sqlite3_step(stmt);
    ASSERT_STR_EQ((const char *)sqlite3_column_text(stmt, 0), "ok");
    sqlite3_finalize(stmt);

    sqlite3_close(db);
    unlink(path);
    PASS();
}

/* --- Ported from scale_debug_test.go: TestWriteDB_MultiPage --- */
TEST(sw_multi_page) {
    char path[256];
    ASSERT_EQ(make_temp_db(path, sizeof(path)), 0);

    /* 192 nodes — enough to trigger multi-page B-tree */
    int N = 192;
    CBMDumpNode nodes[192];
    char node_names[192][16];
    char node_qns[192][32];
    char node_files[192][32];

    for (int i = 0; i < N; i++) {
        snprintf(node_names[i], sizeof(node_names[i]), "f%04d", i);
        snprintf(node_qns[i], sizeof(node_qns[i]), "p.f%04d", i);
        snprintf(node_files[i], sizeof(node_files[i]), "pkg%d/file.go", i % 10);
        nodes[i] = (CBMDumpNode){
            .id = (int64_t)(i + 1),
            .project = "p",
            .label = "Function",
            .name = node_names[i],
            .qualified_name = node_qns[i],
            .file_path = node_files[i],
            .start_line = i,
            .end_line = i + 1,
            .properties = "{}",
        };
    }

    int rc = cbm_write_db(path, "p", "/r", "2026-01-01T00:00:00Z", nodes, N, NULL, 0, NULL, 0,
                          NULL, 0);
    ASSERT_EQ(rc, 0);

    sqlite3 *db = NULL;
    rc = sqlite3_open(path, &db);
    ASSERT_EQ(rc, SQLITE_OK);

    /* Integrity check */
    sqlite3_stmt *stmt = NULL;
    sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1, &stmt, NULL);
    sqlite3_step(stmt);
    ASSERT_STR_EQ((const char *)sqlite3_column_text(stmt, 0), "ok");
    sqlite3_finalize(stmt);

    /* COUNT(*) must be exactly N */
    sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM nodes", -1, &stmt, NULL);
    sqlite3_step(stmt);
    ASSERT_EQ(sqlite3_column_int(stmt, 0), N);
    sqlite3_finalize(stmt);

    /* Verify no rowid gaps: min=1, max=N, count=N */
    sqlite3_prepare_v2(db, "SELECT MIN(rowid), MAX(rowid), COUNT(DISTINCT rowid) FROM nodes", -1,
                       &stmt, NULL);
    sqlite3_step(stmt);
    ASSERT_EQ(sqlite3_column_int(stmt, 0), 1);
    ASSERT_EQ(sqlite3_column_int(stmt, 1), N);
    ASSERT_EQ(sqlite3_column_int(stmt, 2), N);
    sqlite3_finalize(stmt);

    /* Check first and last node by rowid */
    sqlite3_prepare_v2(db, "SELECT name FROM nodes WHERE rowid=1", -1, &stmt, NULL);
    rc = sqlite3_step(stmt);
    ASSERT_EQ(rc, SQLITE_ROW);
    ASSERT_STR_EQ((const char *)sqlite3_column_text(stmt, 0), "f0000");
    sqlite3_finalize(stmt);

    sqlite3_prepare_v2(db, "SELECT name FROM nodes WHERE rowid=192", -1, &stmt, NULL);
    rc = sqlite3_step(stmt);
    ASSERT_EQ(rc, SQLITE_ROW);
    ASSERT_STR_EQ((const char *)sqlite3_column_text(stmt, 0), "f0191");
    sqlite3_finalize(stmt);

    sqlite3_close(db);
    unlink(path);
    PASS();
}

/* ── Suite ─────────────────────────────────────────────────────── */

SUITE(sqlite_writer) {
    RUN_TEST(sw_minimal_data);
    RUN_TEST(sw_scale_and_indexes);
    RUN_TEST(sw_empty);
    RUN_TEST(sw_multi_page);
}
