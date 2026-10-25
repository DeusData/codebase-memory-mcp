/* Source-labelled end-to-end goldens. Expected edges and occurrence offsets
 * below come from fixture source, independently of the resolver's own rows. */
#include "test_framework.h"
#include "test_helpers.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "store/store.h"
#include "store/store_resolver.h"
#include "sqlite3.h"

#include <stdlib.h>
#include <string.h>

#define RESOLVER_PROJECT "resolver-integration"

static const char IMPORT_SOURCE[] = "from lib import target as execute\n\n"
                                    "def caller():\n"
                                    "    return execute()\n\n"
                                    "def reference():\n"
                                    "    return execute\n";

static const char MISSING_SOURCE[] = "from missing import target as execute\n\n"
                                     "def caller():\n"
                                     "    execute()\n"
                                     "    execute()\n";

static const char TARGET_SOURCE[] = "def target():\n    return 1\n";
static const char DECOY_SOURCE[] = "def target():\n    return 99\n\n"
                                   "def execute():\n    return 98\n";

typedef struct {
    char root[512];
    char repo[768];
    char database[768];
    char fresh_database[768];
} resolver_fixture_t;

static bool fixture_init(resolver_fixture_t *fixture, const char *source, bool library,
                         int padding) {
    memset(fixture, 0, sizeof(*fixture));
    const char *tmp = th_mktempdir("cbm_resolver_integration");
    if (!tmp || strlen(tmp) >= sizeof(fixture->root))
        return false;
    snprintf(fixture->root, sizeof(fixture->root), "%s", tmp);
    snprintf(fixture->repo, sizeof(fixture->repo), "%s/repo", tmp);
    snprintf(fixture->database, sizeof(fixture->database), "%s/index.db", tmp);
    snprintf(fixture->fresh_database, sizeof(fixture->fresh_database), "%s/fresh.db", tmp);
    if (!cbm_mkdir_p(fixture->repo, 0755) ||
        th_write_file(TH_PATH(fixture->repo, "main.py"), source) != 0 ||
        th_write_file(TH_PATH(fixture->repo, "decoy.py"), DECOY_SOURCE) != 0)
        return false;
    if (library && th_write_file(TH_PATH(fixture->repo, "lib.py"), TARGET_SOURCE) != 0)
        return false;
    for (int i = 0; i < padding; i++) {
        char name[64], contents[96];
        snprintf(name, sizeof(name), "padding_%03d.py", i);
        snprintf(contents, sizeof(contents), "def padding_%03d():\n    return %d\n", i, i);
        if (th_write_file(TH_PATH(fixture->repo, name), contents) != 0)
            return false;
    }
    return true;
}

/* Restore worker settings before the caller's assertions can return early.
 * Both databases live outside the discovered source tree and use explicit
 * paths; these tests never open the user's default MCP cache. */
static int run_fixture(const resolver_fixture_t *fixture, const char *database, bool parallel) {
    const char *workers = getenv("CBM_WORKERS");
    const char *single = getenv("CBM_INDEX_SINGLE_THREAD");
    char *saved_workers = workers ? strdup(workers) : NULL;
    char *saved_single = single ? strdup(single) : NULL;
    if ((workers && !saved_workers) || (single && !saved_single)) {
        free(saved_workers);
        free(saved_single);
        return -1;
    }
    if (parallel) {
        cbm_unsetenv("CBM_INDEX_SINGLE_THREAD");
        cbm_setenv("CBM_WORKERS", "4", 1);
    } else {
        cbm_setenv("CBM_INDEX_SINGLE_THREAD", "1", 1);
        cbm_setenv("CBM_WORKERS", "1", 1);
    }
    cbm_pipeline_t *pipeline = cbm_pipeline_new(fixture->repo, database, CBM_MODE_FULL);
    int rc = pipeline && cbm_pipeline_set_project_name(pipeline, RESOLVER_PROJECT)
                 ? cbm_pipeline_run(pipeline)
                 : -1;
    cbm_pipeline_free(pipeline);
    if (saved_workers)
        cbm_setenv("CBM_WORKERS", saved_workers, 1);
    else
        cbm_unsetenv("CBM_WORKERS");
    if (saved_single)
        cbm_setenv("CBM_INDEX_SINGLE_THREAD", saved_single, 1);
    else
        cbm_unsetenv("CBM_INDEX_SINGLE_THREAD");
    free(saved_workers);
    free(saved_single);
    return rc;
}

static int count_edge(cbm_store_t *store, const char *source, const char *target,
                      const char *relation) {
    sqlite3_stmt *statement = NULL;
    const char *sql = "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id "
                      "JOIN nodes t ON t.id=e.target_id WHERE e.project=?1 AND "
                      "s.qualified_name=?2 AND t.qualified_name=?3 AND e.type=?4";
    if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &statement, NULL) != SQLITE_OK)
        return -1;
    bool bound =
        sqlite3_bind_text(statement, 1, RESOLVER_PROJECT, -1, SQLITE_STATIC) == SQLITE_OK &&
        sqlite3_bind_text(statement, 2, source, -1, SQLITE_STATIC) == SQLITE_OK &&
        sqlite3_bind_text(statement, 3, target, -1, SQLITE_STATIC) == SQLITE_OK &&
        sqlite3_bind_text(statement, 4, relation, -1, SQLITE_STATIC) == SQLITE_OK;
    int count =
        bound && sqlite3_step(statement) == SQLITE_ROW ? sqlite3_column_int(statement, 0) : -1;
    sqlite3_finalize(statement);
    return count;
}

static int count_sql(cbm_store_t *store, const char *sql) {
    sqlite3_stmt *statement = NULL;
    if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &statement, NULL) != SQLITE_OK)
        return -1;
    int count = sqlite3_step(statement) == SQLITE_ROW ? sqlite3_column_int(statement, 0) : -1;
    sqlite3_finalize(statement);
    return count;
}

/* Canonical occurrence content excludes the scheduler-dependent ordinal and
 * database row IDs, but retains evidence, spelling, spans and edge properties. */
static char *occurrence_listing(cbm_store_t *store) {
    const char *sql =
        "SELECT json_array(file_path,scope,relation,spelling,start_byte,end_byte,source_origin,"
        "binding,domain,analysis,rule,facts,target,candidate_count,reason,emitted,rules_version,"
        "properties_json) FROM resolver_occurrences WHERE project='resolver-integration' "
        "ORDER BY 1";
    sqlite3_stmt *statement = NULL;
    if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &statement, NULL) != SQLITE_OK)
        return NULL;
    char *listing = calloc(1, 1);
    size_t length = 0;
    int step = SQLITE_DONE;
    while (listing && (step = sqlite3_step(statement)) == SQLITE_ROW) {
        const char *row = (const char *)sqlite3_column_text(statement, 0);
        size_t row_length = row ? strlen(row) : 0;
        if (row_length > SIZE_MAX - length - 2) {
            free(listing);
            listing = NULL;
            break;
        }
        char *grown = realloc(listing, length + row_length + 2);
        if (!grown) {
            free(listing);
            listing = NULL;
            break;
        }
        listing = grown;
        memcpy(listing + length, row ? row : "", row_length);
        length += row_length;
        listing[length++] = '\n';
        listing[length] = '\0';
    }
    sqlite3_finalize(statement);
    if (listing && step != SQLITE_DONE) {
        free(listing);
        return NULL;
    }
    return listing;
}

/* The source has exactly two execute() invocations. Persisted distinct rows
 * must agree with those literal byte offsets, not merely with an edge count. */
static bool missing_call_occurrences(cbm_store_t *store, bool emitted, const char *target) {
    const char *first = strstr(MISSING_SOURCE, "execute()");
    const char *second = first ? strstr(first + 1, "execute()") : NULL;
    if (!first || !second)
        return false;
    size_t starts[] = {(size_t)(first - MISSING_SOURCE), (size_t)(second - MISSING_SOURCE)};
    sqlite3_stmt *statement = NULL;
    const char *sql =
        "SELECT start_byte,end_byte,source_origin,emitted,target FROM resolver_occurrences "
        "WHERE project='resolver-integration' AND file_path='main.py' AND relation='CALLS' "
        "AND scope='resolver-integration.main.caller' AND spelling='execute' ORDER BY start_byte";
    if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &statement, NULL) != SQLITE_OK)
        return false;
    bool ok = true;
    for (int occurrence = 0; occurrence < 2; occurrence++) {
        if (sqlite3_step(statement) != SQLITE_ROW) {
            ok = false;
            break;
        }
        const char *actual = (const char *)sqlite3_column_text(statement, 4);
        ok = ok && sqlite3_column_int64(statement, 0) == (sqlite3_int64)starts[occurrence] &&
             sqlite3_column_int64(statement, 1) ==
                 (sqlite3_int64)(starts[occurrence] + strlen("execute()")) &&
             sqlite3_column_int(statement, 2) == CBM_SOURCE_ORIGIN_RAW &&
             sqlite3_column_int(statement, 3) == (emitted ? 1 : 0) && actual &&
             strcmp(actual, target ? target : "") == 0;
    }
    ok = ok && sqlite3_step(statement) == SQLITE_DONE;
    sqlite3_finalize(statement);
    return ok;
}

static bool import_golden(cbm_store_t *store) {
    return count_edge(store, RESOLVER_PROJECT ".main.caller", RESOLVER_PROJECT ".lib.target",
                      "CALLS") == 1 &&
           count_edge(store, RESOLVER_PROJECT ".main.reference", RESOLVER_PROJECT ".lib.target",
                      "USAGE") == 1 &&
           count_edge(store, RESOLVER_PROJECT ".main.caller", RESOLVER_PROJECT ".decoy.target",
                      "CALLS") == 0 &&
           count_edge(store, RESOLVER_PROJECT ".main.caller", RESOLVER_PROJECT ".decoy.execute",
                      "CALLS") == 0 &&
           count_edge(store, RESOLVER_PROJECT ".main.reference", RESOLVER_PROJECT ".decoy.target",
                      "USAGE") == 0 &&
           count_sql(store, "SELECT count(*) FROM resolver_occurrences WHERE "
                            "project='resolver-integration' AND file_path='main.py' AND "
                            "relation='CALLS' AND spelling='execute' AND emitted=1 AND "
                            "target='resolver-integration.lib.target'") == 1;
}

TEST(resolver_integration_python_named_alias_sequential) {
    resolver_fixture_t fixture;
    ASSERT(fixture_init(&fixture, IMPORT_SOURCE, true, 0));
    ASSERT_EQ(run_fixture(&fixture, fixture.database, false), 0);
    cbm_store_t *store = cbm_store_open_path_existing(fixture.database);
    ASSERT_NOT_NULL(store);
    bool golden = import_golden(store);
    cbm_resolve_counts_t counts;
    bool present = false;
    int summary = cbm_store_resolver_summary(store, RESOLVER_PROJECT, &counts, &present);
    cbm_store_close(store);
    th_rmtree(fixture.root);
    ASSERT(golden);
    ASSERT_EQ(summary, CBM_STORE_OK);
    ASSERT(present);
    ASSERT_GTE(counts.emitted, 2);
    PASS();
}

TEST(resolver_integration_python_named_alias_parallel_matches_sequential) {
    resolver_fixture_t fixture;
    /* 53 Python files force the >50-file parallel path with four workers. */
    ASSERT(fixture_init(&fixture, IMPORT_SOURCE, true, 50));
    ASSERT_EQ(run_fixture(&fixture, fixture.database, false), 0);
    ASSERT_EQ(run_fixture(&fixture, fixture.fresh_database, true), 0);
    cbm_store_t *sequential = cbm_store_open_path_existing(fixture.database);
    cbm_store_t *parallel = cbm_store_open_path_existing(fixture.fresh_database);
    ASSERT_NOT_NULL(sequential);
    ASSERT_NOT_NULL(parallel);
    bool seq_golden = import_golden(sequential), par_golden = import_golden(parallel);
    char *seq_rows = occurrence_listing(sequential), *par_rows = occurrence_listing(parallel);
    bool equal = seq_rows && par_rows && strcmp(seq_rows, par_rows) == 0;
    free(seq_rows);
    free(par_rows);
    cbm_store_close(sequential);
    cbm_store_close(parallel);
    th_rmtree(fixture.root);
    ASSERT(seq_golden);
    ASSERT(par_golden);
    ASSERT(equal);
    PASS();
}

TEST(resolver_integration_missing_import_keeps_two_unresolved_occurrences) {
    resolver_fixture_t fixture;
    ASSERT(fixture_init(&fixture, MISSING_SOURCE, false, 0));
    ASSERT_EQ(run_fixture(&fixture, fixture.database, false), 0);
    cbm_store_t *store = cbm_store_open_path_existing(fixture.database);
    ASSERT_NOT_NULL(store);
    bool occurrences = missing_call_occurrences(store, false, "");
    int calls = cbm_store_count_edges_by_type(store, RESOLVER_PROJECT, "CALLS");
    cbm_resolve_counts_t counts;
    bool present = false;
    int summary = cbm_store_resolver_summary(store, RESOLVER_PROJECT, &counts, &present);
    cbm_store_close(store);
    th_rmtree(fixture.root);
    ASSERT(occurrences);
    ASSERT_EQ(calls, 0);
    ASSERT_EQ(summary, CBM_STORE_OK);
    ASSERT(present);
    ASSERT_GTE(counts.unresolved, 2);
    ASSERT_GTE(counts.occurrences, 2);
    PASS();
}

TEST(resolver_integration_added_module_rebuild_matches_fresh_and_noop_preserves_metadata) {
    resolver_fixture_t fixture;
    ASSERT(fixture_init(&fixture, MISSING_SOURCE, false, 0));
    ASSERT_EQ(run_fixture(&fixture, fixture.database, false), 0);
    cbm_store_t *initial = cbm_store_open_path_existing(fixture.database);
    ASSERT_NOT_NULL(initial);
    bool initial_misses = missing_call_occurrences(initial, false, "");
    cbm_store_close(initial);
    ASSERT(initial_misses);
    ASSERT_EQ(th_write_file(TH_PATH(fixture.repo, "missing.py"), TARGET_SOURCE), 0);
    ASSERT_EQ(run_fixture(&fixture, fixture.database, false), 0);
#if defined(CBM_INCREMENTAL_TEST_API) && CBM_INCREMENTAL_TEST_API
    ASSERT_EQ(cbm_pipeline_incremental_test_last_route(), CBM_INCREMENTAL_ROUTE_FORCED_FULL);
#endif
    ASSERT_EQ(run_fixture(&fixture, fixture.fresh_database, false), 0);
    cbm_store_t *updated = cbm_store_open_path_existing(fixture.database);
    cbm_store_t *fresh = cbm_store_open_path_existing(fixture.fresh_database);
    ASSERT_NOT_NULL(updated);
    ASSERT_NOT_NULL(fresh);
    bool updated_sites =
        missing_call_occurrences(updated, true, RESOLVER_PROJECT ".missing.target");
    bool fresh_sites = missing_call_occurrences(fresh, true, RESOLVER_PROJECT ".missing.target");
    int calls = count_edge(updated, RESOLVER_PROJECT ".main.caller",
                           RESOLVER_PROJECT ".missing.target", "CALLS");
    unsigned char updated_digest[32], fresh_digest[32];
    bool digest_ok =
        cbm_store_graph_content_digest(updated, RESOLVER_PROJECT, updated_digest) == CBM_STORE_OK &&
        cbm_store_graph_content_digest(fresh, RESOLVER_PROJECT, fresh_digest) == CBM_STORE_OK &&
        memcmp(updated_digest, fresh_digest, sizeof(updated_digest)) == 0;
    char *before_noop = occurrence_listing(updated), *fresh_rows = occurrence_listing(fresh);
    bool metadata_equal = before_noop && fresh_rows && strcmp(before_noop, fresh_rows) == 0;
    free(fresh_rows);
    cbm_store_close(updated);
    cbm_store_close(fresh);
    ASSERT(updated_sites);
    ASSERT(fresh_sites);
    ASSERT_EQ(calls, 1); /* two genuine occurrences aggregate to one relationship */
    ASSERT(digest_ok);
    ASSERT(metadata_equal);
    ASSERT_EQ(run_fixture(&fixture, fixture.database, false), 0);
#if defined(CBM_INCREMENTAL_TEST_API) && CBM_INCREMENTAL_TEST_API
    ASSERT_EQ(cbm_pipeline_incremental_test_last_route(), CBM_INCREMENTAL_ROUTE_NOOP);
#endif
    cbm_store_t *noop = cbm_store_open_path_existing(fixture.database);
    ASSERT_NOT_NULL(noop);
    char *after_noop = occurrence_listing(noop);
    bool metadata_retained = before_noop && after_noop && strcmp(before_noop, after_noop) == 0;
    cbm_resolve_counts_t counts;
    bool present = false;
    int summary = cbm_store_resolver_summary(noop, RESOLVER_PROJECT, &counts, &present);
    free(before_noop);
    free(after_noop);
    cbm_store_close(noop);
    th_rmtree(fixture.root);
    ASSERT(metadata_retained);
    ASSERT_EQ(summary, CBM_STORE_OK);
    ASSERT(present);
    ASSERT_GTE(counts.emitted, 2);
    PASS();
}

SUITE(resolver_integration) {
    RUN_TEST(resolver_integration_python_named_alias_sequential);
    RUN_TEST(resolver_integration_python_named_alias_parallel_matches_sequential);
    RUN_TEST(resolver_integration_missing_import_keeps_two_unresolved_occurrences);
    RUN_TEST(resolver_integration_added_module_rebuild_matches_fresh_and_noop_preserves_metadata);
}
