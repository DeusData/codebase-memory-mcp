#include "store_resolver.h"
#include "store_resolver_evidence.h"
#include "resolve/policy.h"
#include "resolve/stats.h"

#include <sqlite3.h>
#include <stdio.h>
#include <string.h>

static int resolver_bind_text(sqlite3_stmt *stmt, int column, const char *text) {
    return sqlite3_bind_text(stmt, column, text ? text : "", -1, SQLITE_TRANSIENT);
}

/* The receipt certifies a complete atomic writer, not semantic completeness.
 * Unknown/unsupported outcomes remain valid metadata. Every later metadata row
 * mutation invalidates the receipt; schema/trigger loss fails closed. Checking
 * currency therefore visits a fixed set of schema objects, never the corpus. */
#define RESOLVER_RECEIPT_PREFIX "resolver_receipt:"
#define RESOLVER_RECEIPT_VERSION "1"
typedef struct {
    const char *name;
    const char *columns;
} resolver_table_t;
static const resolver_table_t resolver_tables[] = {
    {"resolver_generations", "project,rules_version"},
    {"resolver_projects", "pid,project,rules_version"},
    {"resolver_text", "pid,id,value"},
    {"resolver_occurrence_rows",
     "pid,ordinal,file,scope,relation,spelling,start_byte,span,source_origin,binding,domain,"
     "analysis,rule,facts,target,candidate_count,reason,emitted,props,line,content,configuration,"
     "proof,source_binding_proof"},
    {"resolver_fact_rows",
     "pid,id,kind,digest,source_project,file,module,scope,content,configuration,source_origin,"
     "lookup_domain,lookup_key,declaration,binding_identity,type_identity,detail,start_byte,"
     "end_byte,origin,flags,lookup_closed,binding,domain,analysis,rule,facts,target,"
     "candidate_count,reason,predecessors_closed"},
    {"resolver_support_rows", "pid,fact,ordinal,premise"},
    {"resolver_file_summaries",
     "project,file_path,module,language,raw_content_identity,preprocessed_content_identity,"
     "configuration_identity,raw_length,preprocessed_length,extracted_summary,derived_summary,"
     "complete"},
    {"resolver_derived_bindings",
     "project,file_path,key,kind,module,closed,binding,domain,analysis,target,proof,payload_json"}};
/* The logical surface. Rows live in the compact tables above; each of these
 * reads back the original columns and values. See store_resolver_evidence.c. */
static const resolver_table_t resolver_views[] = {
    {"resolver_occurrences",
     "project,ordinal,file_path,scope,relation,spelling,start_byte,end_byte,source_origin,"
     "binding,domain,analysis,rule,facts,target,candidate_count,reason,emitted,rules_version,"
     "properties_json"},
    {"resolver_facts", "project,id,kind,source_file,digest,payload"},
    {"resolver_supports", "project,fact,ordinal,premise"},
    {"resolver_lookups", "project,fact,domain,key,status"},
    {"resolver_occurrence_evidence",
     "project,ordinal,content_identity,configuration_identity,proof,source_binding_proof"}};

static bool resolver_object_current(sqlite3 *db, const resolver_table_t *table, const char *type) {
    sqlite3_stmt *statement = NULL;
    bool ok = sqlite3_prepare_v2(db, "SELECT 1 FROM main.sqlite_schema WHERE type=? AND name=?", -1,
                                 &statement, NULL) == SQLITE_OK &&
              resolver_bind_text(statement, 1, type) == SQLITE_OK &&
              resolver_bind_text(statement, 2, table->name) == SQLITE_OK &&
              sqlite3_step(statement) == SQLITE_ROW;
    sqlite3_finalize(statement);
    if (!ok)
        return false;
    char sql[1024];
    int length =
        snprintf(sql, sizeof(sql), "SELECT %s FROM main.%s LIMIT 0", table->columns, table->name);
    statement = NULL;
    ok = length > 0 && (size_t)length < sizeof(sql) &&
         sqlite3_prepare_v2(db, sql, -1, &statement, NULL) == SQLITE_OK &&
         sqlite3_step(statement) == SQLITE_DONE;
    return sqlite3_finalize(statement) == SQLITE_OK && ok;
}

static bool resolver_table_current(sqlite3 *db, const resolver_table_t *table) {
    return resolver_object_current(db, table, "table");
}

/* Compact rows name their project by pid, the rest by name. */
#define RESOLVER_RECEIPT_KEY(row)                                                    \
    "k='" RESOLVER_RECEIPT_PREFIX "'||(SELECT project FROM resolver_projects WHERE " \
    "pid=" row ".pid)"

static bool resolver_table_has_pid(const char *table) {
    return !strcmp(table, "resolver_text") || !strcmp(table, "resolver_occurrence_rows") ||
           !strcmp(table, "resolver_fact_rows") || !strcmp(table, "resolver_support_rows");
}

static bool resolver_guard(sqlite3 *db, const char *table, int operation, bool install) {
    static const char *const operations[] = {"INSERT", "UPDATE", "DELETE"};
    static const char *const by_name[] = {"k='" RESOLVER_RECEIPT_PREFIX "'||NEW.project",
                                          "k='" RESOLVER_RECEIPT_PREFIX
                                          "'||OLD.project OR k='" RESOLVER_RECEIPT_PREFIX
                                          "'||NEW.project",
                                          "k='" RESOLVER_RECEIPT_PREFIX "'||OLD.project"};
    static const char *const by_pid[] = {
        RESOLVER_RECEIPT_KEY("NEW"), RESOLVER_RECEIPT_KEY("OLD") " OR " RESOLVER_RECEIPT_KEY("NEW"),
        RESOLVER_RECEIPT_KEY("OLD")};
    const char *const *predicates = resolver_table_has_pid(table) ? by_pid : by_name;
    char name[128], sql[512];
    int n = snprintf(name, sizeof(name), "resolver_guard_%s_%s", table, operations[operation]);
    int length = snprintf(sql, sizeof(sql),
                          "CREATE TRIGGER %s AFTER %s ON %s BEGIN DELETE FROM store_meta WHERE "
                          "%s; END",
                          name, operations[operation], table, predicates[operation]);
    if (n < 1 || (size_t)n >= sizeof(name) || length < 1 || (size_t)length >= sizeof(sql))
        return false;
    sqlite3_stmt *statement = NULL;
    bool ok =
        sqlite3_prepare_v2(db, "SELECT sql FROM main.sqlite_schema WHERE type='trigger' AND name=?",
                           -1, &statement, NULL) == SQLITE_OK &&
        resolver_bind_text(statement, 1, name) == SQLITE_OK;
    int step = ok ? sqlite3_step(statement) : SQLITE_ERROR;
    const char *stored =
        step == SQLITE_ROW ? (const char *)sqlite3_column_text(statement, 0) : NULL;
    bool matches = stored && strcmp(stored, sql) == 0;
    ok = sqlite3_finalize(statement) == SQLITE_OK;
    if (!ok || step == SQLITE_ERROR || (step == SQLITE_ROW && !matches))
        return false;
    if (step == SQLITE_ROW)
        return true;
    if (step != SQLITE_DONE || !install)
        return false;
    /* A missing guard could have hidden edits to any project. Restoring it
     * must not revive old receipts belonging to untouched projects. */
    return sqlite3_exec(
               db, "DELETE FROM main.store_meta WHERE substr(k,1,17)='" RESOLVER_RECEIPT_PREFIX "'",
               NULL, NULL, NULL) == SQLITE_OK &&
           sqlite3_exec(db, sql, NULL, NULL, NULL) == SQLITE_OK;
}

static bool resolver_schema_current(sqlite3 *db, bool install) {
    const resolver_table_t meta = {"store_meta", "k,v"};
    if (!resolver_table_current(db, &meta))
        return false;
    for (size_t i = 0; i < sizeof(resolver_tables) / sizeof(resolver_tables[0]); i++) {
        if (!resolver_table_current(db, &resolver_tables[i]))
            return false;
        for (int operation = 0; operation < 3; operation++)
            if (!resolver_guard(db, resolver_tables[i].name, operation, install))
                return false;
    }
    for (size_t i = 0; i < sizeof(resolver_views) / sizeof(resolver_views[0]); i++)
        if (!resolver_object_current(db, &resolver_views[i], "view"))
            return false;
    return true;
}

static bool resolver_receipt_write(cbm_store_t *store, const char *project) {
    sqlite3 *db = cbm_store_get_db(store);
    const resolver_table_t meta = {"store_meta", "k,v"};
    /* Fresh stores have no metadata table until the generation seed writes
     * db_uid and mutation_gen. Use that owner before installing receipt guards;
     * creating an empty store_meta here would corrupt generation metadata. */
    if (!resolver_table_current(db, &meta) && cbm_store_generation_advance(store) != CBM_STORE_OK)
        return false;
    if (!resolver_schema_current(db, true))
        return false;
    sqlite3_stmt *statement = NULL;
    bool ok = sqlite3_prepare_v2(
                  db,
                  "INSERT OR REPLACE INTO main.store_meta(k,v) VALUES('" RESOLVER_RECEIPT_PREFIX
                  "'||?, '" RESOLVER_RECEIPT_VERSION "')",
                  -1, &statement, NULL) == SQLITE_OK &&
              resolver_bind_text(statement, 1, project) == SQLITE_OK &&
              sqlite3_step(statement) == SQLITE_DONE;
    if (ok)
        cbm_resolve_stats_add(CBM_RESOLVE_STAT_STORED_RECORDS, 1);
    return sqlite3_finalize(statement) == SQLITE_OK && ok;
}

static bool resolver_current(sqlite3 *db, const char *project) {
    if (!resolver_schema_current(db, false))
        return false;
    sqlite3_stmt *statement = NULL;
    bool current =
        sqlite3_prepare_v2(db,
                           "SELECT g.rules_version FROM main.resolver_generations g JOIN "
                           "main.store_meta m ON m.k='" RESOLVER_RECEIPT_PREFIX
                           "'||g.project WHERE g.project=? AND m.v='" RESOLVER_RECEIPT_VERSION "'",
                           -1, &statement, NULL) == SQLITE_OK &&
        resolver_bind_text(statement, 1, project) == SQLITE_OK &&
        sqlite3_step(statement) == SQLITE_ROW &&
        sqlite3_column_int(statement, 0) == CBM_RESOLVER_RULES_VERSION &&
        sqlite3_step(statement) == SQLITE_DONE;
    return sqlite3_finalize(statement) == SQLITE_OK && current;
}

int cbm_store_resolver_current(cbm_store_t *store, const char *project, bool *present) {
    if (!store || !project || !present)
        return CBM_STORE_ERR;
    *present = false;
    sqlite3 *db = cbm_store_get_db(store);
    if (sqlite3_exec(db, "SAVEPOINT resolver_current", NULL, NULL, NULL) != SQLITE_OK)
        return CBM_STORE_ERR;
    bool current = resolver_current(db, project);
    if (sqlite3_exec(db, "RELEASE resolver_current", NULL, NULL, NULL) != SQLITE_OK)
        return CBM_STORE_ERR;
    *present = current;
    return CBM_STORE_OK;
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

int cbm_store_resolver_replace(cbm_store_t *store, const char *project,
                               const cbm_resolve_record_t *rows, size_t count) {
    return cbm_store_resolver_replace_generation(store, project, rows, count, NULL, 0, NULL, 0);
}

static int resolver_write_generation(cbm_store_t *store, const char *project,
                                     const cbm_resolve_record_t *rows, size_t count,
                                     const cbm_resolve_file_t *files, size_t file_count,
                                     const cbm_resolve_summary_t *summaries, size_t summary_count,
                                     bool commit_batches) {
    if (!store || !project || (!rows && count))
        return CBM_STORE_ERR;
    sqlite3 *db = cbm_store_get_db(store);
    const char *schema =
        "CREATE TABLE IF NOT EXISTS resolver_generations (project TEXT PRIMARY KEY,"
        "rules_version INTEGER NOT NULL);";
    if (!db || sqlite3_exec(db, "SAVEPOINT resolver_replace", NULL, NULL, NULL) != SQLITE_OK)
        return CBM_STORE_ERR;
    bool ok = sqlite3_exec(db, schema, NULL, NULL, NULL) == SQLITE_OK &&
              cbm_store_resolver_evidence(db, project, rows, count, files, file_count, summaries,
                                          summary_count, commit_batches);
    sqlite3_stmt *stmt = NULL;
    if (ok)
        ok = sqlite3_prepare_v2(db, "INSERT OR REPLACE INTO resolver_generations VALUES(?,?)", -1,
                                &stmt, NULL) == SQLITE_OK;
    if (ok)
        ok = resolver_bind_text(stmt, 1, project) == SQLITE_OK &&
             sqlite3_bind_int(stmt, 2, CBM_RESOLVER_RULES_VERSION) == SQLITE_OK &&
             sqlite3_step(stmt) == SQLITE_DONE;
    sqlite3_finalize(stmt);
    if (ok) {
        cbm_resolve_stats_add(CBM_RESOLVE_STAT_STORED_RECORDS, 1);
        ok = resolver_receipt_write(store, project);
    }
    if (!ok) {
        sqlite3_exec(db, "ROLLBACK TO resolver_replace", NULL, NULL, NULL);
        sqlite3_exec(db, "RELEASE resolver_replace", NULL, NULL, NULL);
        return CBM_STORE_ERR;
    }
    return sqlite3_exec(db, "RELEASE resolver_replace", NULL, NULL, NULL) == SQLITE_OK
               ? CBM_STORE_OK
               : CBM_STORE_ERR;
}

int cbm_store_resolver_replace_generation(cbm_store_t *store, const char *project,
                                          const cbm_resolve_record_t *rows, size_t count,
                                          const cbm_resolve_file_t *files, size_t file_count,
                                          const cbm_resolve_summary_t *summaries,
                                          size_t summary_count) {
    return resolver_write_generation(store, project, rows, count, files, file_count, summaries,
                                     summary_count, false);
}

int cbm_store_resolver_publish_generation(cbm_store_t *store, const char *project,
                                          const cbm_resolve_record_t *rows, size_t count,
                                          const cbm_resolve_file_t *files, size_t file_count,
                                          const cbm_resolve_summary_t *summaries,
                                          size_t summary_count) {
    return resolver_write_generation(store, project, rows, count, files, file_count, summaries,
                                     summary_count, true);
}

static bool resolver_count_outcomes(sqlite3 *db, const char *project,
                                    cbm_resolve_counts_t *counts) {
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(
            db,
            "SELECT emitted,binding,analysis,COUNT(*) FROM main.resolver_occurrences "
            "WHERE project=? GROUP BY emitted,binding,analysis",
            -1, &stmt, NULL) != SQLITE_OK)
        return false;
    if (resolver_bind_text(stmt, 1, project) != SQLITE_OK) {
        sqlite3_finalize(stmt);
        return false;
    }
    int step;
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
    return sqlite3_finalize(stmt) == SQLITE_OK && step == SQLITE_DONE;
}

/* The currency check reads every table, so a refused read also fails it. That is an
 * unreadable generation, which must not be reported as one that was never written. */
static bool resolver_read_denied(sqlite3 *db) {
    sqlite3_stmt *statement = NULL;
    int rc = sqlite3_prepare_v2(db, "SELECT project FROM main.resolver_occurrences LIMIT 0", -1,
                                &statement, NULL);
    sqlite3_finalize(statement);
    return rc == SQLITE_AUTH;
}

int cbm_store_resolver_summary(cbm_store_t *store, const char *project,
                               cbm_resolve_counts_t *counts, bool *present) {
    if (!store || !project || !counts || !present)
        return CBM_STORE_ERR;
    memset(counts, 0, sizeof(*counts));
    *present = false;
    sqlite3 *db = cbm_store_get_db(store);
    if (sqlite3_exec(db, "SAVEPOINT resolver_summary", NULL, NULL, NULL) != SQLITE_OK)
        return CBM_STORE_ERR;
    bool current = resolver_current(db, project);
    bool ok = current ? resolver_count_outcomes(db, project, counts) : !resolver_read_denied(db);
    ok = sqlite3_exec(db, "RELEASE resolver_summary", NULL, NULL, NULL) == SQLITE_OK && ok;
    if (!ok) {
        memset(counts, 0, sizeof(*counts));
        return CBM_STORE_ERR;
    }
    *present = current;
    return CBM_STORE_OK;
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
