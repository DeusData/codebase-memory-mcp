#include "store_resolver.h"
#include "resolve/policy.h"

#include <sqlite3.h>
#include <string.h>

static int resolver_bind_text(sqlite3_stmt *stmt, int column, const char *text) {
    return sqlite3_bind_text(stmt, column, text ? text : "", -1, SQLITE_TRANSIENT);
}

int cbm_store_resolver_requires_full(cbm_store_t *store, const char *project, bool *required) {
    if (!store || !project || !required)
        return CBM_STORE_ERR;
    *required = false;
    sqlite3 *db = cbm_store_get_db(store);
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(db,
                           "SELECT 1 FROM sqlite_master WHERE type='table' AND "
                           "name='resolver_generations'",
                           -1, &stmt, NULL) != SQLITE_OK)
        return CBM_STORE_ERR;
    int step = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if (step == SQLITE_DONE)
        return CBM_STORE_OK;
    if (step != SQLITE_ROW)
        return CBM_STORE_ERR;
    if (sqlite3_prepare_v2(db, "SELECT 1 FROM resolver_generations WHERE project=? LIMIT 1", -1,
                           &stmt, NULL) != SQLITE_OK)
        return CBM_STORE_ERR;
    bool ok = resolver_bind_text(stmt, 1, project) == SQLITE_OK;
    step = ok ? sqlite3_step(stmt) : SQLITE_ERROR;
    *required = step == SQLITE_ROW;
    sqlite3_finalize(stmt);
    return step == SQLITE_ROW || step == SQLITE_DONE ? CBM_STORE_OK : CBM_STORE_ERR;
}

static bool resolver_bind_row(sqlite3_stmt *stmt, const char *project,
                              const cbm_resolve_record_t *row, size_t ordinal) {
    const cbm_resolve_result_t *r = &row->result;
    return resolver_bind_text(stmt, 1, project) == SQLITE_OK &&
           sqlite3_bind_int64(stmt, 2, (sqlite3_int64)ordinal) == SQLITE_OK &&
           resolver_bind_text(stmt, 3, row->file_path) == SQLITE_OK &&
           resolver_bind_text(stmt, 4, row->scope) == SQLITE_OK &&
           resolver_bind_text(stmt, 5, row->relation) == SQLITE_OK &&
           resolver_bind_text(stmt, 6, row->spelling) == SQLITE_OK &&
           sqlite3_bind_int64(stmt, 7, row->start_byte) == SQLITE_OK &&
           sqlite3_bind_int64(stmt, 8, row->end_byte) == SQLITE_OK &&
           sqlite3_bind_int(stmt, 9, (int)row->source_origin) == SQLITE_OK &&
           sqlite3_bind_int(stmt, 10, (int)r->binding) == SQLITE_OK &&
           sqlite3_bind_int(stmt, 11, (int)r->domain) == SQLITE_OK &&
           sqlite3_bind_int(stmt, 12, (int)r->analysis) == SQLITE_OK &&
           sqlite3_bind_int(stmt, 13, (int)r->rule) == SQLITE_OK &&
           sqlite3_bind_int64(stmt, 14, r->facts) == SQLITE_OK &&
           resolver_bind_text(stmt, 15, r->target) == SQLITE_OK &&
           sqlite3_bind_int64(stmt, 16, r->candidate_count) == SQLITE_OK &&
           resolver_bind_text(stmt, 17, r->reason) == SQLITE_OK &&
           sqlite3_bind_int(stmt, 18, row->emitted ? 1 : 0) == SQLITE_OK &&
           sqlite3_bind_int(stmt, 19, CBM_RESOLVER_RULES_VERSION) == SQLITE_OK &&
           resolver_bind_text(stmt, 20, row->properties_json ? row->properties_json : "{}") ==
               SQLITE_OK;
}

int cbm_store_resolver_replace(cbm_store_t *store, const char *project,
                               const cbm_resolve_record_t *rows, size_t count) {
    if (!store || !project || (!rows && count))
        return CBM_STORE_ERR;
    sqlite3 *db = cbm_store_get_db(store);
    const char *schema =
        "CREATE TABLE IF NOT EXISTS resolver_occurrences ("
        "project TEXT NOT NULL, ordinal INTEGER NOT NULL, file_path TEXT NOT NULL,"
        "scope TEXT NOT NULL, relation TEXT NOT NULL, spelling TEXT NOT NULL,"
        "start_byte INTEGER NOT NULL, end_byte INTEGER NOT NULL, source_origin INTEGER NOT NULL,"
        "binding INTEGER NOT NULL, domain INTEGER NOT NULL, analysis INTEGER NOT NULL,"
        "rule INTEGER NOT NULL, facts INTEGER NOT NULL, target TEXT NOT NULL,"
        "candidate_count INTEGER NOT NULL, reason TEXT NOT NULL, emitted INTEGER NOT NULL,"
        "rules_version INTEGER NOT NULL, properties_json TEXT NOT NULL, PRIMARY "
        "KEY(project,ordinal));"
        "CREATE INDEX IF NOT EXISTS resolver_occurrences_file "
        "ON resolver_occurrences(project,file_path);"
        "CREATE TABLE IF NOT EXISTS resolver_generations (project TEXT PRIMARY KEY,"
        "rules_version INTEGER NOT NULL);";
    if (!db || sqlite3_exec(db, "SAVEPOINT resolver_replace", NULL, NULL, NULL) != SQLITE_OK)
        return CBM_STORE_ERR;
    bool ok = sqlite3_exec(db, schema, NULL, NULL, NULL) == SQLITE_OK;
    sqlite3_stmt *stmt = NULL;
    if (ok)
        ok = sqlite3_prepare_v2(db, "DELETE FROM resolver_occurrences WHERE project=?", -1, &stmt,
                                NULL) == SQLITE_OK;
    if (ok)
        ok = resolver_bind_text(stmt, 1, project) == SQLITE_OK && sqlite3_step(stmt) == SQLITE_DONE;
    sqlite3_finalize(stmt);
    stmt = NULL;
    if (ok)
        ok = sqlite3_prepare_v2(db,
                                "INSERT INTO resolver_occurrences VALUES"
                                "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                -1, &stmt, NULL) == SQLITE_OK;
    for (size_t i = 0; ok && i < count; i++) {
        /* A claimed emitted row must satisfy the same admission as its edge.
         * A failed metadata write aborts publication, never fabricates zero. */
        ok = (!rows[i].emitted || cbm_resolve_record_admit(&rows[i])) &&
             resolver_bind_row(stmt, project, &rows[i], i) && sqlite3_step(stmt) == SQLITE_DONE;
        sqlite3_reset(stmt);
        sqlite3_clear_bindings(stmt);
    }
    sqlite3_finalize(stmt);
    stmt = NULL;
    if (ok)
        ok = sqlite3_prepare_v2(db, "INSERT OR REPLACE INTO resolver_generations VALUES(?,?)", -1,
                                &stmt, NULL) == SQLITE_OK;
    if (ok)
        ok = resolver_bind_text(stmt, 1, project) == SQLITE_OK &&
             sqlite3_bind_int(stmt, 2, CBM_RESOLVER_RULES_VERSION) == SQLITE_OK &&
             sqlite3_step(stmt) == SQLITE_DONE;
    sqlite3_finalize(stmt);
    if (!ok) {
        sqlite3_exec(db, "ROLLBACK TO resolver_replace", NULL, NULL, NULL);
        sqlite3_exec(db, "RELEASE resolver_replace", NULL, NULL, NULL);
        return CBM_STORE_ERR;
    }
    return sqlite3_exec(db, "RELEASE resolver_replace", NULL, NULL, NULL) == SQLITE_OK
               ? CBM_STORE_OK
               : CBM_STORE_ERR;
}

int cbm_store_resolver_summary(cbm_store_t *store, const char *project,
                               cbm_resolve_counts_t *counts, bool *present) {
    if (!store || !project || !counts || !present)
        return CBM_STORE_ERR;
    memset(counts, 0, sizeof(*counts));
    *present = false;
    sqlite3 *db = cbm_store_get_db(store);
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(db,
                           "SELECT 1 FROM sqlite_master WHERE type='table' "
                           "AND name='resolver_generations'",
                           -1, &stmt, NULL) != SQLITE_OK)
        return CBM_STORE_ERR;
    int step = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if (step == SQLITE_DONE)
        return CBM_STORE_OK;
    if (step != SQLITE_ROW)
        return CBM_STORE_ERR;
    if (sqlite3_prepare_v2(db, "SELECT rules_version FROM resolver_generations WHERE project=?", -1,
                           &stmt, NULL) != SQLITE_OK)
        return CBM_STORE_ERR;
    resolver_bind_text(stmt, 1, project);
    step = sqlite3_step(stmt);
    *present = step == SQLITE_ROW && sqlite3_column_int(stmt, 0) == CBM_RESOLVER_RULES_VERSION;
    sqlite3_finalize(stmt);
    if (!*present)
        return step == SQLITE_DONE || step == SQLITE_ROW ? CBM_STORE_OK : CBM_STORE_ERR;
    if (sqlite3_prepare_v2(db,
                           "SELECT emitted,binding,analysis,COUNT(*) FROM resolver_occurrences "
                           "WHERE project=? GROUP BY emitted,binding,analysis",
                           -1, &stmt, NULL) != SQLITE_OK)
        return CBM_STORE_ERR;
    resolver_bind_text(stmt, 1, project);
    while ((step = sqlite3_step(stmt)) == SQLITE_ROW) {
        size_t n = (size_t)sqlite3_column_int64(stmt, 3);
        counts->occurrences += n;
        if (sqlite3_column_int(stmt, 0))
            counts->emitted += n;
        int binding = sqlite3_column_int(stmt, 1);
        int analysis = sqlite3_column_int(stmt, 2);
        if (binding == CBM_RESOLVE_BINDING_AMBIGUOUS)
            counts->ambiguous += n;
        if (binding == CBM_RESOLVE_BINDING_UNRESOLVED)
            counts->unresolved += n;
        if (analysis == CBM_RESOLVE_ANALYSIS_UNSUPPORTED)
            counts->unsupported += n;
        if (analysis == CBM_RESOLVE_ANALYSIS_LIMITED)
            counts->limited += n;
        if (analysis == CBM_RESOLVE_ANALYSIS_FAILED)
            counts->failed += n;
    }
    sqlite3_finalize(stmt);
    return step == SQLITE_DONE ? CBM_STORE_OK : CBM_STORE_ERR;
}

int cbm_store_resolver_visit_misses(cbm_store_t *store, const char *project, size_t offset,
                                    size_t limit, cbm_store_resolver_visit_fn visit, void *context,
                                    size_t *returned) {
    if (!store || !project || !visit || !returned || offset > INT64_MAX || limit > INT64_MAX)
        return CBM_STORE_ERR;
    *returned = 0;
    if (!limit)
        return CBM_STORE_OK;
    sqlite3_stmt *statement = NULL;
    const char *sql =
        "SELECT file_path,scope,relation,spelling,start_byte,end_byte,source_origin,"
        "binding,domain,analysis,rule,facts,target,candidate_count,reason FROM "
        "resolver_occurrences "
        "WHERE project=?1 AND emitted=0 ORDER BY file_path,start_byte,end_byte,source_origin,"
        "scope,relation,spelling,ordinal LIMIT ?2 OFFSET ?3";
    if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &statement, NULL) != SQLITE_OK)
        return CBM_STORE_ERR;
    bool ok = resolver_bind_text(statement, 1, project) == SQLITE_OK &&
              sqlite3_bind_int64(statement, 2, (sqlite3_int64)limit) == SQLITE_OK &&
              sqlite3_bind_int64(statement, 3, (sqlite3_int64)offset) == SQLITE_OK;
    int step = SQLITE_DONE;
    while (ok && (step = sqlite3_step(statement)) == SQLITE_ROW) {
        cbm_resolve_record_t row = {
            .file_path = (const char *)sqlite3_column_text(statement, 0),
            .scope = (const char *)sqlite3_column_text(statement, 1),
            .relation = (const char *)sqlite3_column_text(statement, 2),
            .spelling = (const char *)sqlite3_column_text(statement, 3),
            .start_byte = (uint32_t)sqlite3_column_int64(statement, 4),
            .end_byte = (uint32_t)sqlite3_column_int64(statement, 5),
            .source_origin = (uint32_t)sqlite3_column_int(statement, 6),
            .result = {.binding = (cbm_resolve_binding_t)sqlite3_column_int(statement, 7),
                       .domain = (cbm_resolve_domain_t)sqlite3_column_int(statement, 8),
                       .analysis = (cbm_resolve_analysis_t)sqlite3_column_int(statement, 9),
                       .rule = (cbm_resolve_rule_t)sqlite3_column_int(statement, 10),
                       .facts = (uint32_t)sqlite3_column_int64(statement, 11),
                       .target = (const char *)sqlite3_column_text(statement, 12),
                       .candidate_count = (uint32_t)sqlite3_column_int64(statement, 13),
                       .reason = (const char *)sqlite3_column_text(statement, 14)}};
        ok = visit(&row, context);
        if (ok)
            (*returned)++;
    }
    sqlite3_finalize(statement);
    return ok && step == SQLITE_DONE ? CBM_STORE_OK : CBM_STORE_ERR;
}
