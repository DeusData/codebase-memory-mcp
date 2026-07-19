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
#include "foundation/compat_fs.h"
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

static int assert_node_name(sqlite3 *db, const char *expected) {
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(db, "SELECT name FROM nodes WHERE id=1", -1, &stmt, NULL);
    ASSERT_EQ(rc, SQLITE_OK);
    rc = sqlite3_step(stmt);
    ASSERT_EQ(rc, SQLITE_ROW);
    ASSERT_STR_EQ((const char *)sqlite3_column_text(stmt, 0), expected);
    sqlite3_finalize(stmt);
    return 0;
}

static int write_fixture_file(const char *path, const char *contents) {
    FILE *fp = cbm_fopen(path, "wb");
    if (!fp) {
        return -1;
    }
    size_t len = strlen(contents);
    size_t written = fwrite(contents, 1, len, fp);
    int close_rc = fclose(fp);
    return written == len && close_rc == 0 ? 0 : -1;
}

static int fixture_file_equals(const char *path, const char *expected) {
    FILE *fp = cbm_fopen(path, "rb");
    if (!fp) {
        return 0;
    }
    char buf[64] = {0};
    size_t n = fread(buf, 1, sizeof(buf), fp);
    int close_rc = fclose(fp);
    size_t expected_len = strlen(expected);
    return close_rc == 0 && n == expected_len && memcmp(buf, expected, expected_len) == 0;
}

static int count_temp_outputs_for(const char *path) {
    char dir[256];
    char base[256];
    const char *slash = strrchr(path, '/');
#ifdef _WIN32
    const char *backslash = strrchr(path, '\\');
    if (!slash || (backslash && backslash > slash)) {
        slash = backslash;
    }
#endif
    if (slash) {
        size_t dir_len = (size_t)(slash - path);
        if (dir_len == 0 || dir_len >= sizeof(dir)) {
            return -1;
        }
        memcpy(dir, path, dir_len);
        dir[dir_len] = '\0';
        snprintf(base, sizeof(base), "%s", slash + 1);
    } else {
        snprintf(dir, sizeof(dir), ".");
        snprintf(base, sizeof(base), "%s", path);
    }

    cbm_dir_t *d = cbm_opendir(dir);
    if (!d) {
        return -1;
    }
    size_t base_len = strlen(base);
    int count = 0;
    cbm_dirent_t *ent;
    while ((ent = cbm_readdir(d)) != NULL) {
        size_t name_len = strlen(ent->name);
        if (name_len > base_len + 5 && strncmp(ent->name, base, base_len) == 0 &&
            strncmp(ent->name + base_len, ".tmp.", 5) == 0) {
            count++;
        }
    }
    cbm_closedir(d);
    return count;
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

    int rc = cbm_write_db(path, "test", "/tmp/test", "2026-03-14T00:00:00Z", nodes, 2, edges, 1,
                          NULL, 0, NULL, 0);
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

/* #768 schema half: two IMPORTS edges between the same two files, keyed apart
 * by local_name, must be representable in the dumped DB. The dump's edges DDL
 * and the hand-built sqlite_autoindex_edges_1 must carry the local_name
 * discriminator — with the old UNIQUE(source_id,target_id,type) the two rows
 * violate the dump's own constraint and PRAGMA integrity_check flags a
 * non-unique autoindex entry. */
TEST(sw_imports_local_name_unique) {
    char path[256];
    ASSERT_EQ(make_temp_db(path, sizeof(path)), 0);

    CBMDumpNode nodes[2] = {
        {.id = 1,
         .project = "test",
         .label = "File",
         .name = "consumer.ts",
         .qualified_name = "test.consumer.ts.__file__",
         .file_path = "consumer.ts",
         .start_line = 1,
         .end_line = 3,
         .properties = "{}"},
        {.id = 2,
         .project = "test",
         .label = "File",
         .name = "lib.ts",
         .qualified_name = "test.lib.ts.__file__",
         .file_path = "lib.ts",
         .start_line = 1,
         .end_line = 2,
         .properties = "{}"},
    };
    CBMDumpEdge edges[2] = {
        {.id = 1,
         .project = "test",
         .source_id = 1,
         .target_id = 2,
         .type = "IMPORTS",
         .properties = "{\"local_name\":\"A\"}",
         .url_path = "",
         .local_name = "A"},
        {.id = 2,
         .project = "test",
         .source_id = 1,
         .target_id = 2,
         .type = "IMPORTS",
         .properties = "{\"local_name\":\"B\"}",
         .url_path = "",
         .local_name = "B"},
    };

    int rc = cbm_write_db(path, "test", "/tmp/test", "2026-03-14T00:00:00Z", nodes, 2, edges, 2,
                          NULL, 0, NULL, 0);
    ASSERT_EQ(rc, 0);

    sqlite3 *db = NULL;
    rc = sqlite3_open(path, &db);
    ASSERT_EQ(rc, SQLITE_OK);

    /* The dumped DB must satisfy its own UNIQUE constraint. */
    sqlite3_stmt *stmt = NULL;
    sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1, &stmt, NULL);
    rc = sqlite3_step(stmt);
    ASSERT_EQ(rc, SQLITE_ROW);
    const char *integrity = (const char *)sqlite3_column_text(stmt, 0);
    ASSERT_STR_EQ(integrity, "ok");
    sqlite3_finalize(stmt);

    /* Both sibling imports are queryable. */
    sqlite3_prepare_v2(db,
                       "SELECT COUNT(*) FROM edges "
                       "WHERE source_id=1 AND target_id=2 AND type='IMPORTS'",
                       -1, &stmt, NULL);
    sqlite3_step(stmt);
    ASSERT_EQ(sqlite3_column_int(stmt, 0), 2);
    sqlite3_finalize(stmt);

    /* The generated discriminator matches what the writer hand-indexed. */
    sqlite3_prepare_v2(db, "SELECT local_name_gen FROM edges ORDER BY id", -1, &stmt, NULL);
    rc = sqlite3_step(stmt);
    ASSERT_EQ(rc, SQLITE_ROW);
    ASSERT_STR_EQ((const char *)sqlite3_column_text(stmt, 0), "A");
    rc = sqlite3_step(stmt);
    ASSERT_EQ(rc, SQLITE_ROW);
    ASSERT_STR_EQ((const char *)sqlite3_column_text(stmt, 0), "B");
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

    int rc = cbm_write_db(path, "proj", "/repo", "2026-03-14T12:00:00Z", nodes, 100, edges,
                          edge_count, NULL, 0, NULL, 0);
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

/* Index keys longer than the index-page max-local payload (~16.4 KB at the
 * 64 KB page size) MUST spill to overflow pages; writing them fully inline
 * makes SQLite read key bytes as an overflow page number — PRAGMA
 * integrity_check reports "invalid page number 0x43654C6C ('CeLl')" and name
 * lookups silently return nothing (seen on elasticsearch: very long
 * Section/heading names). */
TEST(sw_long_index_keys_overflow) {
    char path[256];
    ASSERT_EQ(make_temp_db(path, sizeof(path)), 0);

    enum { LONGN = 20000 };
    char *longname = malloc(LONGN + 1);
    char *longqn = malloc(LONGN + 16);
    ASSERT_NOT_NULL(longname);
    ASSERT_NOT_NULL(longqn);
    memset(longname, 'N', LONGN);
    longname[LONGN] = '\0';
    snprintf(longqn, LONGN + 16, "test.%s", longname);

    CBMDumpNode nodes[3] = {
        {.id = 1,
         .project = "test",
         .label = "Module",
         .name = "main",
         .qualified_name = "test.main",
         .file_path = "main.md",
         .start_line = 1,
         .end_line = 2,
         .properties = "{}"},
        {.id = 2,
         .project = "test",
         .label = "Section",
         .name = longname,
         .qualified_name = longqn,
         .file_path = "main.md",
         .start_line = 3,
         .end_line = 9,
         .properties = "{}"},
        {.id = 3,
         .project = "test",
         .label = "Function",
         .name = "after",
         .qualified_name = "test.main.after",
         .file_path = "main.md",
         .start_line = 10,
         .end_line = 12,
         .properties = "{}"},
    };

    int rc = cbm_write_db(path, "test", "/tmp/test", "2026-03-14T00:00:00Z", nodes, 3, NULL, 0,
                          NULL, 0, NULL, 0);
    ASSERT_EQ(rc, 0);

    sqlite3 *db = NULL;
    ASSERT_EQ(sqlite3_open(path, &db), SQLITE_OK);
    sqlite3_stmt *stmt = NULL;
    sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1, &stmt, NULL);
    ASSERT_EQ(sqlite3_step(stmt), SQLITE_ROW);
    const char *integrity = (const char *)sqlite3_column_text(stmt, 0);
    ASSERT_STR_EQ(integrity, "ok");
    sqlite3_finalize(stmt);

    /* The long-named row must be findable via the name index. */
    sqlite3_prepare_v2(db, "SELECT id FROM nodes WHERE name = ?1", -1, &stmt, NULL);
    sqlite3_bind_text(stmt, 1, longname, -1, SQLITE_STATIC);
    ASSERT_EQ(sqlite3_step(stmt), SQLITE_ROW);
    ASSERT_EQ(sqlite3_column_int64(stmt, 0), 2);
    sqlite3_finalize(stmt);
    sqlite3_close(db);

    free(longname);
    free(longqn);
    unlink(path);
    PASS();
}

TEST(sw_empty) {
    char path[256];
    ASSERT_EQ(make_temp_db(path, sizeof(path)), 0);

    int rc = cbm_write_db(path, "test", "/tmp/test", "2026-03-14T00:00:00Z", NULL, 0, NULL, 0, NULL,
                          0, NULL, 0);
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

    int rc =
        cbm_write_db(path, "p", "/r", "2026-01-01T00:00:00Z", nodes, N, NULL, 0, NULL, 0, NULL, 0);
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

/* ── Oversized node: properties JSON > 65KB triggers overflow pages ─ */

TEST(sw_oversized_node) {
    char path[256];
    ASSERT_EQ(make_temp_db(path, sizeof(path)), 0);

    /* Build a properties JSON string that exceeds max_local (65501 bytes).
     * Use 70000 bytes of padding inside the JSON value so the full record,
     * which includes other text columns, is well above the threshold. */
    int prop_len = 70000;
    char *big_props = (char *)malloc(prop_len + 1);
    ASSERT_NOT_NULL(big_props);
    memset(big_props, 'x', prop_len);
    big_props[0] = '"';
    big_props[prop_len - 1] = '"';
    big_props[prop_len] = '\0';

    CBMDumpNode nodes[1] = {{
        .id = 1,
        .project = "test",
        .label = "Function",
        .name = "huge_fn",
        .qualified_name = "test.huge_fn",
        .file_path = "huge.go",
        .start_line = 1,
        .end_line = 9999,
        .properties = big_props,
    }};

    int rc = cbm_write_db(path, "test", "/tmp/test", "2026-03-28T00:00:00Z", nodes, 1, NULL, 0,
                          NULL, 0, NULL, 0);
    free(big_props);
    ASSERT_EQ(rc, 0);

    sqlite3 *db = NULL;
    rc = sqlite3_open(path, &db);
    ASSERT_EQ(rc, SQLITE_OK);

    /* Integrity check — SQLite will validate overflow page chain */
    sqlite3_stmt *stmt = NULL;
    sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1, &stmt, NULL);
    rc = sqlite3_step(stmt);
    ASSERT_EQ(rc, SQLITE_ROW);
    ASSERT_STR_EQ((const char *)sqlite3_column_text(stmt, 0), "ok");
    sqlite3_finalize(stmt);

    /* Verify we can read the node back */
    sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM nodes", -1, &stmt, NULL);
    sqlite3_step(stmt);
    ASSERT_EQ(sqlite3_column_int(stmt, 0), 1);
    sqlite3_finalize(stmt);

    /* Verify the name round-trips correctly */
    sqlite3_prepare_v2(db, "SELECT name FROM nodes WHERE id=1", -1, &stmt, NULL);
    rc = sqlite3_step(stmt);
    ASSERT_EQ(rc, SQLITE_ROW);
    ASSERT_STR_EQ((const char *)sqlite3_column_text(stmt, 0), "huge_fn");
    sqlite3_finalize(stmt);

    sqlite3_close(db);
    unlink(path);
    PASS();
}

TEST(sw_stream_open_does_not_truncate_destination) {
    char path[256];
    ASSERT_EQ(make_temp_db(path, sizeof(path)), 0);

    const char sentinel[] = "old-db-visible-until-finalize";
    ASSERT_EQ(write_fixture_file(path, sentinel), 0);

    cbm_db_writer_t *w = cbm_writer_open(path);
    ASSERT(w != NULL);

    char buf[64] = {0};
    FILE *fp = cbm_fopen(path, "rb");
    ASSERT(fp != NULL);
    size_t n = fread(buf, 1, sizeof(sentinel) - 1, fp);
    ASSERT_EQ(fclose(fp), 0);
    ASSERT_EQ(n, sizeof(sentinel) - 1);
    ASSERT_EQ(memcmp(buf, sentinel, sizeof(sentinel) - 1), 0);

    int rc = cbm_writer_finalize(w, "test", "/tmp/test", "2026-07-07T00:00:00Z", NULL, 0, NULL, 0,
                                 NULL, 0, NULL, 0);
    ASSERT_EQ(rc, 0);

    sqlite3 *db = NULL;
    rc = sqlite3_open(path, &db);
    ASSERT_EQ(rc, SQLITE_OK);

    sqlite3_stmt *stmt = NULL;
    sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1, &stmt, NULL);
    rc = sqlite3_step(stmt);
    ASSERT_EQ(rc, SQLITE_ROW);
    ASSERT_STR_EQ((const char *)sqlite3_column_text(stmt, 0), "ok");
    sqlite3_finalize(stmt);

    sqlite3_close(db);
    cbm_unlink(path);
    PASS();
}

TEST(sw_publish_removes_destination_sidecars) {
    char path[256];
    ASSERT_EQ(make_temp_db(path, sizeof(path)), 0);

    char wal[320];
    char shm[320];
    snprintf(wal, sizeof(wal), "%s-wal", path);
    snprintf(shm, sizeof(shm), "%s-shm", path);

    ASSERT_EQ(write_fixture_file(wal, "stale-wal"), 0);
    ASSERT_EQ(write_fixture_file(shm, "stale-shm"), 0);

    int rc = cbm_write_db(path, "test", "/tmp/test", "2026-07-07T00:00:00Z", NULL, 0, NULL, 0, NULL,
                          0, NULL, 0);
    ASSERT_EQ(rc, 0);
    ASSERT(!fixture_file_equals(wal, "stale-wal"));
    ASSERT(!fixture_file_equals(shm, "stale-shm"));

    sqlite3 *db = NULL;
    rc = sqlite3_open(path, &db);
    ASSERT_EQ(rc, SQLITE_OK);

    sqlite3_stmt *stmt = NULL;
    sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1, &stmt, NULL);
    rc = sqlite3_step(stmt);
    ASSERT_EQ(rc, SQLITE_ROW);
    ASSERT_STR_EQ((const char *)sqlite3_column_text(stmt, 0), "ok");
    sqlite3_finalize(stmt);

    sqlite3_close(db);
    cbm_unlink(path);
    PASS();
}

TEST(sw_publish_failure_preserves_destination_sidecars) {
    char path[256];
    snprintf(path, sizeof(path), "/tmp/cbm_sw_dir_XXXXXX");
    ASSERT(cbm_mkdtemp(path) != NULL);

    char wal[320];
    char shm[320];
    snprintf(wal, sizeof(wal), "%s-wal", path);
    snprintf(shm, sizeof(shm), "%s-shm", path);
    ASSERT_EQ(write_fixture_file(wal, "live-wal"), 0);
    ASSERT_EQ(write_fixture_file(shm, "live-shm"), 0);

    int rc = cbm_write_db(path, "test", "/tmp/test", "2026-07-07T00:00:00Z", NULL, 0, NULL, 0, NULL,
                          0, NULL, 0);
    ASSERT(rc != 0);

    ASSERT_EQ(count_temp_outputs_for(path), 0);
    ASSERT(fixture_file_equals(wal, "live-wal"));
    ASSERT(fixture_file_equals(shm, "live-shm"));

    cbm_unlink(wal);
    cbm_unlink(shm);
    cbm_rmdir(path);
    PASS();
}

TEST(sw_publish_supports_non_ascii_path) {
    char dir[256];
    snprintf(dir, sizeof(dir), "/tmp/cbm_sw_utf8_XXXXXX");
    ASSERT(cbm_mkdtemp(dir) != NULL);

    char path[320];
    char wal[384];
    char shm[384];
    snprintf(path, sizeof(path), "%s/db-\xC3\xBC.sqlite", dir);
    snprintf(wal, sizeof(wal), "%s-wal", path);
    snprintf(shm, sizeof(shm), "%s-shm", path);
    ASSERT_EQ(write_fixture_file(wal, "stale-wal"), 0);
    ASSERT_EQ(write_fixture_file(shm, "stale-shm"), 0);

    int rc = cbm_write_db(path, "test", "/tmp/test", "2026-07-07T00:00:00Z", NULL, 0, NULL, 0, NULL,
                          0, NULL, 0);
    ASSERT_EQ(rc, 0);
    ASSERT(!fixture_file_equals(wal, "stale-wal"));
    ASSERT(!fixture_file_equals(shm, "stale-shm"));

    sqlite3 *db = NULL;
    rc = sqlite3_open(path, &db);
    ASSERT_EQ(rc, SQLITE_OK);
    sqlite3_stmt *stmt = NULL;
    sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1, &stmt, NULL);
    ASSERT_EQ(sqlite3_step(stmt), SQLITE_ROW);
    ASSERT_STR_EQ((const char *)sqlite3_column_text(stmt, 0), "ok");
    sqlite3_finalize(stmt);
    sqlite3_close(db);

    cbm_unlink(path);
    cbm_rmdir(dir);
    PASS();
}

TEST(sw_publish_preserves_live_reader) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX open-reader rename semantics");
#endif
    char path[256];
    ASSERT_EQ(make_temp_db(path, sizeof(path)), 0);

    CBMDumpNode old_nodes[1] = {{
        .id = 1,
        .project = "test",
        .label = "Function",
        .name = "old_fn",
        .qualified_name = "test.old_fn",
        .file_path = "old.go",
        .start_line = 1,
        .end_line = 2,
        .properties = "{}",
    }};
    int rc = cbm_write_db(path, "test", "/tmp/test", "2026-07-07T00:00:00Z", old_nodes, 1, NULL, 0,
                          NULL, 0, NULL, 0);
    ASSERT_EQ(rc, 0);

    sqlite3 *reader = NULL;
    rc = sqlite3_open(path, &reader);
    ASSERT_EQ(rc, SQLITE_OK);
    rc = sqlite3_exec(reader, "PRAGMA mmap_size=268435456", NULL, NULL, NULL);
    ASSERT_EQ(rc, SQLITE_OK);
    rc = sqlite3_exec(reader, "BEGIN", NULL, NULL, NULL);
    ASSERT_EQ(rc, SQLITE_OK);
    ASSERT_EQ(assert_node_name(reader, "old_fn"), 0);

    CBMDumpNode new_nodes[1] = {{
        .id = 1,
        .project = "test",
        .label = "Function",
        .name = "new_fn",
        .qualified_name = "test.new_fn",
        .file_path = "new.go",
        .start_line = 1,
        .end_line = 2,
        .properties = "{}",
    }};
    rc = cbm_write_db(path, "test", "/tmp/test", "2026-07-07T00:00:01Z", new_nodes, 1, NULL, 0,
                      NULL, 0, NULL, 0);
    ASSERT_EQ(rc, 0);

    ASSERT_EQ(assert_node_name(reader, "old_fn"), 0);
    rc = sqlite3_exec(reader, "COMMIT", NULL, NULL, NULL);
    ASSERT_EQ(rc, SQLITE_OK);
    sqlite3_close(reader);

    sqlite3 *db = NULL;
    rc = sqlite3_open(path, &db);
    ASSERT_EQ(rc, SQLITE_OK);
    ASSERT_EQ(assert_node_name(db, "new_fn"), 0);
    sqlite3_close(db);

    cbm_unlink(path);
    PASS();
}

/* ── Suite ─────────────────────────────────────────────────────── */

SUITE(sqlite_writer) {
    RUN_TEST(sw_minimal_data);
    RUN_TEST(sw_imports_local_name_unique);
    RUN_TEST(sw_scale_and_indexes);
    RUN_TEST(sw_long_index_keys_overflow);
    RUN_TEST(sw_empty);
    RUN_TEST(sw_multi_page);
    RUN_TEST(sw_oversized_node);
    RUN_TEST(sw_stream_open_does_not_truncate_destination);
    RUN_TEST(sw_publish_removes_destination_sidecars);
    RUN_TEST(sw_publish_failure_preserves_destination_sidecars);
    RUN_TEST(sw_publish_supports_non_ascii_path);
    RUN_TEST(sw_publish_preserves_live_reader);
}
