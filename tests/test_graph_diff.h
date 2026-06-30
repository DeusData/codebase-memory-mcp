/*
 * test_graph_diff.h - Canonical graph comparison helpers for tests.
 *
 * These helpers compare graph facts by stable keys instead of transient row IDs.
 * They are intentionally test-only so production store APIs stay unchanged.
 */
#ifndef TEST_GRAPH_DIFF_H
#define TEST_GRAPH_DIFF_H

#include <sqlite3.h>

#include "../src/foundation/compat.h"
#include "../src/foundation/constants.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum { TG_ROW_SET_INIT_CAP = CBM_SZ_128 };

typedef struct {
    char **items;
    int count;
    int cap;
} tg_row_set_t;

static inline void tg_row_set_free(tg_row_set_t *rows) {
    if (!rows) {
        return;
    }
    for (int i = 0; i < rows->count; i++) {
        free(rows->items[i]);
    }
    free(rows->items);
    rows->items = NULL;
    rows->count = 0;
    rows->cap = 0;
}

static inline int tg_set_error(char *err, size_t err_sz, const char *msg) {
    if (err && err_sz > 0) {
        int n = snprintf(err, err_sz, "%s", msg ? msg : "graph diff failed");
        if (n < 0 || (size_t)n >= err_sz) {
            err[err_sz - 1] = '\0';
        }
    }
    return CBM_NOT_FOUND;
}

static inline int tg_row_set_push(tg_row_set_t *rows, const char *row, char *err, size_t err_sz) {
    if (rows->count == rows->cap) {
        int next_cap = rows->cap ? rows->cap * PAIR_LEN : TG_ROW_SET_INIT_CAP;
        char **next = (char **)realloc(rows->items, (size_t)next_cap * sizeof(*next));
        if (!next) {
            return tg_set_error(err, err_sz, "graph diff: out of memory growing row set");
        }
        rows->items = next;
        rows->cap = next_cap;
    }
    rows->items[rows->count] = cbm_strdup(row ? row : "");
    if (!rows->items[rows->count]) {
        return tg_set_error(err, err_sz, "graph diff: out of memory copying row");
    }
    rows->count++;
    return 0;
}

static inline int tg_collect_query(sqlite3 *db, const char *project, const char *sql,
                                   tg_row_set_t *rows, char *err, size_t err_sz) {
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        return tg_set_error(err, err_sz, sqlite3_errmsg(db));
    }
    rc = sqlite3_bind_text(stmt, 1, project, -1, SQLITE_TRANSIENT);
    if (rc != SQLITE_OK) {
        sqlite3_finalize(stmt);
        return tg_set_error(err, err_sz, sqlite3_errmsg(db));
    }
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        const unsigned char *txt = sqlite3_column_text(stmt, 0);
        if (tg_row_set_push(rows, (const char *)txt, err, err_sz) != 0) {
            sqlite3_finalize(stmt);
            return CBM_NOT_FOUND;
        }
    }
    sqlite3_finalize(stmt);
    if (rc != SQLITE_DONE) {
        return tg_set_error(err, err_sz, sqlite3_errmsg(db));
    }
    return 0;
}

static inline int tg_compare_rows(const char *kind, const tg_row_set_t *left,
                                  const tg_row_set_t *right, char *err, size_t err_sz) {
    if (left->count != right->count) {
        if (err && err_sz > 0) {
            int n = snprintf(err, err_sz, "%s count differs: left=%d right=%d", kind,
                             left->count, right->count);
            if (n < 0 || (size_t)n >= err_sz) {
                err[err_sz - 1] = '\0';
            }
        }
        return CBM_NOT_FOUND;
    }
    int left_idx = 0;
    int right_idx = 0;
    while (left_idx < left->count && right_idx < right->count) {
        int cmp = strcmp(left->items[left_idx], right->items[right_idx]);
        if (cmp == 0) {
            left_idx++;
            right_idx++;
            continue;
        }
        if (cmp < 0) {
            if (err && err_sz > 0) {
                int n = snprintf(err, err_sz,
                                 "%s left-only row %d:\n  left:  %s\n  right row %d: %s", kind,
                                 left_idx, left->items[left_idx], right_idx,
                                 right->items[right_idx]);
                if (n < 0 || (size_t)n >= err_sz) {
                    err[err_sz - 1] = '\0';
                }
            }
            return CBM_NOT_FOUND;
        }
        if (err && err_sz > 0) {
            int n = snprintf(err, err_sz,
                             "%s right-only row %d:\n  right: %s\n  left row %d: %s", kind,
                             right_idx, right->items[right_idx], left_idx, left->items[left_idx]);
            if (n < 0 || (size_t)n >= err_sz) {
                err[err_sz - 1] = '\0';
            }
        }
        return CBM_NOT_FOUND;
    }
    if (left_idx < left->count || right_idx < right->count) {
        if (err && err_sz > 0) {
            int n = snprintf(err, err_sz, "%s exhausted unevenly: left_row=%d right_row=%d", kind,
                             left_idx, right_idx);
            if (n < 0 || (size_t)n >= err_sz) {
                err[err_sz - 1] = '\0';
            }
        }
        return CBM_NOT_FOUND;
    }
    return 0;
}

static inline int tg_compare_query(sqlite3 *left_db, sqlite3 *right_db, const char *project,
                                   const char *kind, const char *sql, char *err,
                                   size_t err_sz) {
    tg_row_set_t left = {0};
    tg_row_set_t right = {0};
    int rc = tg_collect_query(left_db, project, sql, &left, err, err_sz);
    if (rc == 0) {
        rc = tg_collect_query(right_db, project, sql, &right, err, err_sz);
    }
    if (rc == 0) {
        rc = tg_compare_rows(kind, &left, &right, err, err_sz);
    }
    tg_row_set_free(&left);
    tg_row_set_free(&right);
    return rc;
}

static inline int cbm_test_compare_canonical_graphs(const char *left_db_path,
                                                    const char *right_db_path,
                                                    const char *project, char *err,
                                                    size_t err_sz) {
    static const char *nodes_sql =
        "SELECT quote(label) || char(9) || quote(name) || char(9) || "
        "quote(qualified_name) || char(9) || quote(coalesce(file_path,'')) || char(9) || "
        "start_line || char(9) || end_line || char(9) || " /* properties below */
        "COALESCE((SELECT group_concat(item, char(30)) FROM ("
        "SELECT quote(je.key) || '=' || je.type || '=' || "
        "COALESCE(quote(CAST(je.value AS TEXT)), 'NULL') AS item "
        "FROM json_each(n.properties) AS je "
        "ORDER BY je.key, je.type, CAST(je.value AS TEXT)"
        ")), '') "
        "FROM nodes n WHERE project = ?1 "
        "ORDER BY label, name, qualified_name, coalesce(file_path,''), start_line, end_line, "
        "COALESCE((SELECT group_concat(item, char(30)) FROM ("
        "SELECT quote(je.key) || '=' || je.type || '=' || "
        "COALESCE(quote(CAST(je.value AS TEXT)), 'NULL') AS item "
        "FROM json_each(n.properties) AS je "
        "ORDER BY je.key, je.type, CAST(je.value AS TEXT)"
        ")), '');";
    static const char *edges_sql =
        "SELECT quote(s.label) || char(9) || quote(s.qualified_name) || char(9) || "
        "quote(coalesce(s.file_path,'')) || char(9) || s.start_line || char(9) || "
        "s.end_line || char(9) || quote(t.label) || char(9) || quote(t.qualified_name) || "
        "char(9) || quote(coalesce(t.file_path,'')) || char(9) || t.start_line || char(9) || "
        "t.end_line || char(9) || quote(e.type) || char(9) || "
        "COALESCE((SELECT group_concat(item, char(30)) FROM ("
        "SELECT quote(je.key) || '=' || je.type || '=' || "
        "COALESCE(quote(CAST(je.value AS TEXT)), 'NULL') AS item "
        "FROM json_each(e.properties) AS je "
        "ORDER BY je.key, je.type, CAST(je.value AS TEXT)"
        ")), '') "
        "FROM edges e "
        "JOIN nodes s ON s.id = e.source_id "
        "JOIN nodes t ON t.id = e.target_id "
        "WHERE e.project = ?1 "
        "ORDER BY s.label, s.qualified_name, coalesce(s.file_path,''), s.start_line, s.end_line, "
        "t.label, t.qualified_name, coalesce(t.file_path,''), t.start_line, t.end_line, "
        "e.type, COALESCE((SELECT group_concat(item, char(30)) FROM ("
        "SELECT quote(je.key) || '=' || je.type || '=' || "
        "COALESCE(quote(CAST(je.value AS TEXT)), 'NULL') AS item "
        "FROM json_each(e.properties) AS je "
        "ORDER BY je.key, je.type, CAST(je.value AS TEXT)"
        ")), '');";
    static const char *hashes_sql =
        "SELECT quote(rel_path) || char(9) || quote(sha256) || char(9) || mtime_ns || char(9) || "
        "size FROM file_hashes WHERE project = ?1 ORDER BY rel_path;";

    sqlite3 *left_db = NULL;
    sqlite3 *right_db = NULL;
    int rc = sqlite3_open_v2(left_db_path, &left_db, SQLITE_OPEN_READONLY, NULL);
    if (rc != SQLITE_OK) {
        if (left_db) {
            tg_set_error(err, err_sz, sqlite3_errmsg(left_db));
            sqlite3_close(left_db);
        } else {
            tg_set_error(err, err_sz, "graph diff: cannot open left DB");
        }
        return CBM_NOT_FOUND;
    }
    rc = sqlite3_open_v2(right_db_path, &right_db, SQLITE_OPEN_READONLY, NULL);
    if (rc != SQLITE_OK) {
        if (right_db) {
            tg_set_error(err, err_sz, sqlite3_errmsg(right_db));
            sqlite3_close(right_db);
        } else {
            tg_set_error(err, err_sz, "graph diff: cannot open right DB");
        }
        sqlite3_close(left_db);
        return CBM_NOT_FOUND;
    }

    rc = tg_compare_query(left_db, right_db, project, "canonical nodes", nodes_sql, err, err_sz);
    if (rc == 0) {
        rc = tg_compare_query(left_db, right_db, project, "canonical edges", edges_sql, err, err_sz);
    }
    if (rc == 0) {
        rc = tg_compare_query(left_db, right_db, project, "file hashes", hashes_sql, err, err_sz);
    }

    sqlite3_close(right_db);
    sqlite3_close(left_db);
    return rc;
}

#endif /* TEST_GRAPH_DIFF_H */
