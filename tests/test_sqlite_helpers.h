#ifndef TEST_SQLITE_HELPERS_H
#define TEST_SQLITE_HELPERS_H

#include "sqlite3.h"

static inline int cbm_test_sqlite_object_exists(sqlite3 *db, const char *type, const char *name) {
    sqlite3_stmt *stmt = NULL;
    int exists = 0;
    if (!db || !type || !name ||
        sqlite3_prepare_v2(db,
                           "SELECT 1 FROM sqlite_master WHERE type = ?1 AND name = ?2 LIMIT 1",
                           -1, &stmt, NULL) != SQLITE_OK) {
        return 0;
    }
    sqlite3_bind_text(stmt, 1, type, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, name, -1, SQLITE_STATIC);
    exists = sqlite3_step(stmt) == SQLITE_ROW;
    sqlite3_finalize(stmt);
    return exists;
}

#endif
