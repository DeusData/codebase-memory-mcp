/* Source-labelled end-to-end goldens. Expected edges and occurrence offsets
 * below come from fixture source, independently of the resolver's own rows. */
#include "test_framework.h"
#include "test_helpers.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "store/store.h"
#include "store/store_resolver.h"
#include "sqlite3.h"
#include "yyjson/yyjson.h"
#include "graph_buffer/graph_buffer.h"
#include "pipeline/shared_resolver.h"
#include "resolve/proof.h"
#include "resolve/policy.h"

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

TEST(resolver_integration_python_relative_imports_keep_exact_targets) {
    const char *sources[] = {
        ("from .lib import target as execute\ndef caller():\n    return execute()\n"),
        ("from ..lib import target as execute\ndef caller():\n    return execute()\n"),
        ("from .lib import target as execute\ndef caller(execute):\n    return execute()\n"),
        ("from .lib import target as execute\ndef caller():\n    execute = other\n    return "
         "execute()\n"),
        ("from ...lib import target as execute\ndef caller():\n    return execute()\n"),
    };
    size_t source_count = sizeof(sources) / sizeof(sources[0]);
    for (size_t run = 0; run <= source_count; run++) {
        /* Repeat the first real package through the >50-file worker path.
         * The tiny independent padding functions add no imports or calls. */
        bool parallel = run == source_count;
        size_t i = parallel ? 0 : run;
        resolver_fixture_t fixture;
        ASSERT(fixture_init(&fixture, "pass\n", false, parallel ? 50 : 0));
        ASSERT(cbm_mkdir_p(TH_PATH(fixture.repo, "pkg/sub"), 0755));
        ASSERT_EQ(th_write_file(TH_PATH(fixture.repo, "pkg/__init__.py"), ""), 0);
        ASSERT_EQ(th_write_file(TH_PATH(fixture.repo, "pkg/sub/__init__.py"), ""), 0);
        ASSERT_EQ(th_write_file(TH_PATH(fixture.repo, "pkg/lib.py"), TARGET_SOURCE), 0);
        const char *relative = i == 1 ? "pkg/sub/main.py" : "pkg/main.py";
        ASSERT_EQ(th_write_file(TH_PATH(fixture.repo, relative), sources[i]), 0);
        ASSERT_EQ(run_fixture(&fixture, fixture.database, parallel), 0);
        cbm_store_t *store = cbm_store_open_path_existing(fixture.database);
        ASSERT_NOT_NULL(store);
        const char *caller =
            i == 1 ? RESOLVER_PROJECT ".pkg.sub.main.caller" : RESOLVER_PROJECT ".pkg.main.caller";
        int genuine = count_edge(store, caller, RESOLVER_PROJECT ".pkg.lib.target", "CALLS");
        int decoy = count_edge(store, caller, RESOLVER_PROJECT ".decoy.target", "CALLS") +
                    count_edge(store, caller, RESOLVER_PROJECT ".decoy.execute", "CALLS");
        /* SHA-256 of the independently known empty source. A missing cache
         * result or placeholder summary must not pass as a captured file. */
        int empty_summaries =
            count_sql(store, "SELECT count(*) FROM resolver_file_summaries "
                             "WHERE project='resolver-integration' "
                             "AND file_path IN ('pkg/__init__.py','pkg/sub/__init__.py') "
                             "AND complete=1 AND raw_length=0 "
                             "AND raw_content_identity="
                             "'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' "
                             "AND length(module)>0 AND length(configuration_identity)=64 "
                             "AND length(extracted_summary)=64");
        if (genuine != (i < 2 ? 1 : 0) || decoy != 0 || empty_summaries != 2)
            fprintf(stderr,
                    "  [relative-import] case=%zu parallel=%d genuine=%d expected=%d "
                    "decoy=%d empty_summaries=%d\n",
                    i, parallel, genuine, i < 2 ? 1 : 0, decoy, empty_summaries);
        cbm_store_close(store);
        th_rmtree(fixture.root);
        ASSERT_EQ(genuine, i < 2 ? 1 : 0);
        ASSERT_EQ(decoy, 0);
        ASSERT_EQ(empty_summaries, 2);
    }
    PASS();
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

typedef struct {
    bool setup_ok;
    int run_rc;
    bool forced_full;
    bool damaged_metadata_unavailable;
    int summary_rc;
    bool present;
    bool golden;
    bool rows_equal;
    bool digest_equal;
} generation_refresh_result_t;

static generation_refresh_result_t generation_refresh_case(const char *invalidate,
                                                           bool erase_projection) {
    generation_refresh_result_t result = {.run_rc = -1, .summary_rc = CBM_STORE_ERR};
    resolver_fixture_t fixture;
    if (!fixture_init(&fixture, IMPORT_SOURCE, true, 0))
        return result;
    if (run_fixture(&fixture, fixture.database, false) != 0) {
        th_rmtree(fixture.root);
        return result;
    }
    cbm_store_t *store = cbm_store_open_path_existing(fixture.database);
    char *expected_rows = store ? occurrence_listing(store) : NULL;
    unsigned char expected_digest[32];
    cbm_resolve_counts_t counts;
    bool present = false;
    result.setup_ok =
        store && expected_rows && import_golden(store) &&
        cbm_store_graph_content_digest(store, RESOLVER_PROJECT, expected_digest) == CBM_STORE_OK;
    if (result.setup_ok) {
        /* Simulate stale/missing resolver metadata without changing any source.
         * Missing graph and outcome rows make a marker-only repair insufficient. */
        sqlite3 *db = cbm_store_get_db(store);
        result.setup_ok =
            sqlite3_exec(db, invalidate, NULL, NULL, NULL) == SQLITE_OK &&
            (!erase_projection ||
             sqlite3_exec(db,
                          "DELETE FROM resolver_occurrences WHERE project='" RESOLVER_PROJECT
                          "';DELETE FROM edges WHERE project='" RESOLVER_PROJECT
                          "' AND type IN ('CALLS','USAGE');",
                          NULL, NULL, NULL) == SQLITE_OK);
        if (result.setup_ok) {
            result.damaged_metadata_unavailable =
                cbm_store_resolver_summary(store, RESOLVER_PROJECT, &counts, &present) ==
                    CBM_STORE_OK &&
                !present;
            result.setup_ok = cbm_store_seal_for_atomic_publish(store) == CBM_STORE_OK;
        }
    }
    if (store)
        cbm_store_close(store);
    if (result.setup_ok) {
        result.run_rc = run_fixture(&fixture, fixture.database, false);
#if defined(CBM_INCREMENTAL_TEST_API) && CBM_INCREMENTAL_TEST_API
        result.forced_full =
            cbm_pipeline_incremental_test_last_route() == CBM_INCREMENTAL_ROUTE_FORCED_FULL;
#else
        result.forced_full = true; /* Production builds still check restored graph and outcomes. */
#endif
        store = cbm_store_open_path_existing(fixture.database);
        if (store) {
            unsigned char actual_digest[32];
            char *actual_rows = occurrence_listing(store);
            result.summary_rc =
                cbm_store_resolver_summary(store, RESOLVER_PROJECT, &counts, &result.present);
            result.golden = import_golden(store);
            result.rows_equal = actual_rows && strcmp(expected_rows, actual_rows) == 0;
            result.digest_equal =
                cbm_store_graph_content_digest(store, RESOLVER_PROJECT, actual_digest) ==
                    CBM_STORE_OK &&
                memcmp(expected_digest, actual_digest, sizeof(expected_digest)) == 0;
            free(actual_rows);
            cbm_store_close(store);
        }
    }
    free(expected_rows);
    th_rmtree(fixture.root);
    return result;
}

TEST(resolver_integration_old_rules_version_rebuilds_unchanged_source) {
    generation_refresh_result_t result =
        generation_refresh_case("UPDATE resolver_generations SET rules_version=rules_version-1 "
                                "WHERE project='" RESOLVER_PROJECT "';",
                                true);
    ASSERT(result.setup_ok);
    ASSERT_EQ(result.run_rc, 0);
    ASSERT(result.forced_full);
    ASSERT(result.damaged_metadata_unavailable);
    ASSERT_EQ(result.summary_rc, CBM_STORE_OK);
    ASSERT(result.present);
    ASSERT(result.golden);
    ASSERT(result.rows_equal);
    ASSERT(result.digest_equal);
    PASS();
}

TEST(resolver_integration_missing_generation_rebuilds_unchanged_source) {
    generation_refresh_result_t result =
        generation_refresh_case("DROP TABLE resolver_generations;", true);
    ASSERT(result.setup_ok);
    ASSERT_EQ(result.run_rc, 0);
    ASSERT(result.forced_full);
    ASSERT(result.damaged_metadata_unavailable);
    ASSERT_EQ(result.summary_rc, CBM_STORE_OK);
    ASSERT(result.present);
    ASSERT(result.golden);
    ASSERT(result.rows_equal);
    ASSERT(result.digest_equal);
    PASS();
}

TEST(resolver_integration_missing_facts_rebuilds_unchanged_source) {
    generation_refresh_result_t result =
        generation_refresh_case("DROP TABLE resolver_facts;", false);
    ASSERT(result.setup_ok);
    ASSERT_EQ(result.run_rc, 0);
    ASSERT(result.forced_full);
    ASSERT(result.damaged_metadata_unavailable);
    ASSERT_EQ(result.summary_rc, CBM_STORE_OK);
    ASSERT(result.present);
    ASSERT(result.golden);
    ASSERT(result.rows_equal);
    ASSERT(result.digest_equal);
    PASS();
}

TEST(resolver_integration_missing_evidence_rebuilds_unchanged_source) {
    generation_refresh_result_t result = generation_refresh_case(
        "DELETE FROM resolver_occurrence_evidence WHERE project='" RESOLVER_PROJECT "';", false);
    ASSERT(result.setup_ok);
    ASSERT_EQ(result.run_rc, 0);
    ASSERT(result.forced_full);
    ASSERT(result.damaged_metadata_unavailable);
    ASSERT_EQ(result.summary_rc, CBM_STORE_OK);
    ASSERT(result.present);
    ASSERT(result.golden);
    ASSERT(result.rows_equal);
    ASSERT(result.digest_equal);
    PASS();
}

static bool write_export_chain(const resolver_fixture_t *fixture, bool module_alias) {
    for (int i = 0; i < 48; i++) {
        char name[64], source[128];
        snprintf(name, sizeof(name), "link_%03d.py", i);
        if (i == 47)
            snprintf(source, sizeof(source), "%s",
                     module_alias ? "import lib as alias\n" : "from lib import target\n");
        else
            snprintf(source, sizeof(source), "from link_%03d import %s\n", i + 1,
                     module_alias ? "alias" : "target");
        if (th_write_file(TH_PATH(fixture->repo, name), source) != 0)
            return false;
    }
    return true;
}

TEST(resolver_integration_long_export_chain_stabilizes_before_parallel_emission) {
    static const char source[] = "from link_000 import target as execute\n\n"
                                 "def caller():\n    return execute()\n\n"
                                 "def reference():\n    return execute\n";
    resolver_fixture_t fixture;
    /* 55 files exercise parallel emission as well as a chain beyond the old 32-hop cap. */
    ASSERT(fixture_init(&fixture, source, true, 4));
    ASSERT(write_export_chain(&fixture, false));
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

TEST(resolver_integration_long_module_alias_chain_preserves_exact_suffix) {
    static const char source[] = "from link_000 import alias as bridge\n\n"
                                 "def caller():\n    bridge.target()\n    bridge.target.extra()\n";
    resolver_fixture_t fixture;
    ASSERT(fixture_init(&fixture, source, true, 0));
    ASSERT(write_export_chain(&fixture, true));
    ASSERT_EQ(run_fixture(&fixture, fixture.database, false), 0);
    cbm_store_t *store = cbm_store_open_path_existing(fixture.database);
    ASSERT_NOT_NULL(store);
    int exact =
        count_edge(store, RESOLVER_PROJECT ".main.caller", RESOLVER_PROJECT ".lib.target", "CALLS");
    int calls = cbm_store_count_edges_by_type(store, RESOLVER_PROJECT, "CALLS");
    int emitted =
        count_sql(store, "SELECT count(*) FROM resolver_occurrences WHERE file_path='main.py' "
                         "AND relation='CALLS' AND emitted=1");
    cbm_store_close(store);
    th_rmtree(fixture.root);
    ASSERT_EQ(exact, 1);
    ASSERT_EQ(calls, 1);
    ASSERT_EQ(emitted, 1);
    PASS();
}

typedef struct {
    const char *id;
    const char *source_file;
    const char *source;
    const char *target_file;
    const char *target;
    const char *spelling;
    bool required;
    bool genuine;
    const char *why_tried;
} literal_import_case_t;

static int literal_import_count(cbm_store_t *store, const literal_import_case_t *item,
                                bool occurrences, bool exact) {
    const char *sql =
        occurrences
            ? "SELECT count(*) FROM resolver_occurrences r JOIN nodes t "
              "ON t.project=r.project AND t.qualified_name=r.target "
              "WHERE r.project=?1 AND r.file_path=?2 AND r.relation='IMPORTS' AND r.emitted=1 "
              "AND (?3=0 OR (t.label='File' AND t.file_path=?4 AND r.spelling=?5 "
              "AND r.candidate_count=1))"
            : "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id "
              "JOIN nodes t ON t.id=e.target_id WHERE e.project=?1 AND s.file_path=?2 "
              "AND e.type='IMPORTS' AND (?3=0 OR (s.label='File' AND t.label='File' "
              "AND t.file_path=?4))";
    sqlite3_stmt *statement = NULL;
    if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &statement, NULL) != SQLITE_OK)
        return -1;
    bool bound =
        sqlite3_bind_text(statement, 1, RESOLVER_PROJECT, -1, SQLITE_STATIC) == SQLITE_OK &&
        sqlite3_bind_text(statement, 2, item->source_file, -1, SQLITE_STATIC) == SQLITE_OK &&
        sqlite3_bind_int(statement, 3, exact) == SQLITE_OK &&
        sqlite3_bind_text(statement, 4, item->target_file, -1, SQLITE_STATIC) == SQLITE_OK;
    if (occurrences)
        bound = bound &&
                sqlite3_bind_text(statement, 5, item->spelling, -1, SQLITE_STATIC) == SQLITE_OK;
    int count =
        bound && sqlite3_step(statement) == SQLITE_ROW ? sqlite3_column_int(statement, 0) : -1;
    sqlite3_finalize(statement);
    return count;
}

/* Source-authored import identities close the whole IMPORTS domain for each
 * source file, including wrong same-basename targets. Known missing syntax is
 * explicitly counted independently of the producer's extracted carriers. */
static int literal_path_import_cases(const char *prefix, int expected_genuine,
                                     int expected_missing) {
    static const literal_import_case_t cases[] = {
        {"just.literal", "scope/main.just", "import 'utils.just'\n", "scope/utils.just",
         "target:\n  @echo target\n", "utils.just", true, true, NULL},
        {"just.parent", "scope/nested/main.just", "import '../utils.just'\n", "scope/utils.just",
         "target:\n  @echo target\n", "../utils.just", true, true, NULL},
        {"just.optional", "scope/main.just", "import? './utils.just'\n", "scope/utils.just",
         "target:\n  @echo target\n", "./utils.just", true, true, NULL},
        {"just.home-is-not-literal-folder", "scope/main.just", "import '~/utils.just'\n",
         "scope/~/utils.just", "target:\n  @echo target\n", "~/utils.just", false, false, NULL},
        {"just.computed-prefix", "scope/main.just", "import './utils.just' + 'other'\n",
         "scope/utils.just", "target:\n  @echo target\n", "./utils.just", false, false, NULL},
        {"just.other-directory", "scope/main.just", "import 'utils.just'\n", "elsewhere/utils.just",
         "target:\n  @echo target\n", "utils.just", false, false, NULL},
        {"just.no-string", "scope/main.just", "import path\n", "scope/utils.just",
         "target:\n  @echo target\n", "utils.just", false, false, NULL},
        {"func.literal", "scope/main.fc", "#include \"utils.fc\";\n", "scope/utils.fc",
         "int target() { return 1; }\n", "utils.fc", true, true, NULL},
        {"func.parent", "scope/nested/main.fc", "#include \"../utils.fc\";\n", "scope/utils.fc",
         "int target() { return 1; }\n", "../utils.fc", true, true, NULL},
        {"func.computed-prefix", "scope/main.fc", "#include \"./utils.fc\" + \"other\";\n",
         "scope/utils.fc", "int target() { return 1; }\n", "./utils.fc", false, false, NULL},
        {"func.other-directory", "scope/main.fc", "#include \"utils.fc\";\n", "elsewhere/utils.fc",
         "int target() { return 1; }\n", "utils.fc", false, false, NULL},
        {"scss.use", "scope/main.scss", "@use \"./base.scss\";\n", "scope/base.scss",
         "$answer: 1;\n", "./base.scss", true, true, NULL},
        {"scss.forward", "scope/main.scss", "@forward \"base.scss\";\n", "scope/base.scss",
         "$answer: 1;\n", "base.scss", true, true, NULL},
        {"scss.partial", "scope/main.scss", "@use \"base\";\n", "scope/_base.scss", "$answer: 1;\n",
         "base", true, true, NULL},
        {"scss.interpolated", "scope/main.scss",
         "$suffix: \"other\";\n@import \"./base#{$suffix}.scss\";\n", "scope/base#{$suffix}.scss",
         "$answer: 1;\n", "./base#{$suffix}.scss", false, false, NULL},
        {"scss.mixin-string-not-import", "scope/main.scss", "@include wrapper(\"./base.scss\");\n",
         "scope/base.scss", "$answer: 1;\n", "./base.scss", false, false, NULL},
        {"scss.ambiguous", "scope/main.scss", "@use \"./base.scss\";\n", "scope/base.scss",
         "$answer: 1;\n", "./base.scss", false, false, NULL},
        {"scss.underscore-ambiguous", "scope/main.scss", "@use \"./_base.scss\";\n",
         "scope/_base.scss", "$base: 1;\n", "./_base.scss", false, false, NULL},
        {"scss.import-only-unsupported", "scope/main.scss", "@import \"base.scss\";\n",
         "scope/base.scss", "$answer: 1;\n", "base.scss", false, true,
         "Sass @import has separate import-only precedence; the bounded proof supports "
         "@use/@forward "
         "only. Literal module directives and competing partials are tested separately."},
        {"meson.literal", "scope/meson.build", "subdir('sub')\n", "scope/sub/meson.build",
         "answer = 1\n", "sub", true, true, NULL},
        {"meson.dot-relative", "scope/meson.build", "subdir('./sub')\n", "scope/sub/meson.build",
         "answer = 1\n", "./sub", true, true, NULL},
        {"meson.concat-prefix", "scope/meson.build", "subdir('sub' + '-other')\n",
         "scope/sub/meson.build", "answer = 1\n", "sub", false, false, NULL},
        {"meson.nested-expression", "scope/meson.build", "subdir(join_paths('sub', 'other'))\n",
         "scope/sub/meson.build", "answer = 1\n", "sub", false, false, NULL},
        {"meson.false-branch", "scope/meson.build", "if false\n  subdir('sub')\nendif\n",
         "scope/sub/meson.build", "answer = 1\n", "sub", false, false, NULL},
        {"meson.after-subdir-done", "scope/meson.build", "subdir_done()\nsubdir('sub')\n",
         "scope/sub/meson.build", "answer = 1\n", "sub", false, false, NULL},
        {"meson.parent-segment-forbidden", "scope/meson.build", "subdir('child/../sub')\n",
         "scope/sub/meson.build", "answer = 1\n", "child/../sub", false, false, NULL},
        {"meson.keyword-condition", "scope/meson.build",
         "subdir('sub', if_found: dependency('unknown', required: false))\n",
         "scope/sub/meson.build", "answer = 1\n", "sub", false, false, NULL},
        {"objc.literal", "scope/Main.m", "#import \"Utils.m\"\n@interface Main\n@end\n",
         "scope/Utils.m", "@interface Utils\n@end\n", "Utils.m", true, true, NULL},
        {"objc.parent", "scope/nested/Main.m", "#import \"../Utils.m\"\n@interface Main\n@end\n",
         "scope/Utils.m", "@interface Utils\n@end\n", "../Utils.m", true, true, NULL},
        {"objc.system-root-unknown", "scope/Main.m", "#import <Utils.m>\n@interface Main\n@end\n",
         "scope/Utils.m", "@interface Utils\n@end\n", "Utils.m", false, false, NULL},
        {"objc.other-directory", "scope/Main.m", "#import \"Utils.m\"\n@interface Main\n@end\n",
         "elsewhere/Utils.m", "@interface Utils\n@end\n", "Utils.m", false, false, NULL},
        {"objc.above-root", "scope/Main.m", "#import \"../../Utils.m\"\n@interface Main\n@end\n",
         "scope/Utils.m", "@interface Utils\n@end\n", "../../Utils.m", false, false, NULL},
        {"objc.matlab-comment", "scope/Main.m",
         "% #import \"Utils.m\"\nfunction value = Main()\nvalue = 1;\nend\n", "scope/Utils.m",
         "@interface Utils\n@end\n", "Utils.m", false, false, NULL},
        {"css.literal", "scope/main.css", "@import \"base.css\";\n", "scope/base.css",
         "body { color: red; }\n", "base.css", true, true, NULL},
        {"css.parent", "scope/nested/main.css", "@import \"../base.css\";\n", "scope/base.css",
         "body { color: red; }\n", "../base.css", true, true, NULL},
        {"css.url", "scope/main.css", "@import url(\"base.css\");\n", "scope/base.css",
         "body { color: red; }\n", "base.css", false, true,
         "URL function structure is not audited; quoted direct imports were verified separately."},
        {"css.escaped", "scope/main.css", "@import \"b\\61se.css\";\n", "scope/base.css",
         "body { color: red; }\n", "base.css", false, true,
         "CSS escape decoding is not implemented; accepting the raw token would name another "
         "file."},
        {"css.invalid-order", "scope/main.css", "body {}\n@import \"base.css\";\n",
         "scope/base.css", "body {}\n", "base.css", false, false, NULL},
        {"css.remote", "scope/main.css", "@import \"https://example.invalid/base.css\";\n",
         "scope/base.css", "body {}\n", "base.css", false, false, NULL},
        {"css.above-root", "scope/main.css", "@import \"../../base.css\";\n", "scope/base.css",
         "body {}\n", "base.css", false, false, NULL},
        {"css.other-directory", "scope/main.css", "@import \"base.css\";\n", "elsewhere/base.css",
         "body {}\n", "base.css", false, false, NULL},
        {"bicep.literal", "scope/main.bicep", "module target 'sub.bicep' = { name: 'test' }\n",
         "scope/sub.bicep", "output answer string = 'ok'\n", "sub.bicep", true, true, NULL},
        {"bicep.parent", "scope/nested/main.bicep",
         "module target '../sub.bicep' = { name: 'test' }\n", "scope/sub.bicep",
         "output answer string = 'ok'\n", "../sub.bicep", true, true, NULL},
        {"bicep.interpolation", "scope/main.bicep",
         "var leaf = 'sub'\nmodule target '${leaf}.bicep' = { name: 'test' }\n", "scope/sub.bicep",
         "output answer string = 'ok'\n", "sub.bicep", false, true,
         "Constant string interpolation lacks an evaluator; exact complete literals have separate "
         "positives."},
        {"bicep.registry", "scope/main.bicep",
         "module target 'br/example.invalid:sub' = { name: 'test' }\n", "scope/sub.bicep",
         "output answer string = 'ok'\n", "sub.bicep", false, false, NULL},
        {"capnp.literal", "scope/main.capnp",
         "@0xbabcabcabcabcabc;\nusing Common = import \"common.capnp\";\n", "scope/common.capnp",
         "@0xcabcabcabcabcabc;\nstruct Common { value @0 :Text; }\n", "common.capnp", true, true,
         NULL},
        {"capnp.parent", "scope/nested/main.capnp",
         "@0xbabcabcabcabcabc;\nusing Common = import \"../common.capnp\";\n", "scope/common.capnp",
         "@0xcabcabcabcabcabc;\nstruct Common { value @0 :Text; }\n", "../common.capnp", true, true,
         NULL},
        {"capnp.search-root", "scope/main.capnp",
         "@0xbabcabcabcabcabc;\nusing Common = import \"/common.capnp\";\n", "scope/common.capnp",
         "@0xcabcabcabcabcabc;\nstruct Common { value @0 :Text; }\n", "common.capnp", false, false,
         NULL},
    };
    for (int parallel = 0; parallel < 2; parallel++) {
        int genuine = 0, found = 0, missing = 0, wrong = 0;
        for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
            const literal_import_case_t *item = &cases[i];
            if (strncmp(item->id, prefix, strlen(prefix)))
                continue;
            resolver_fixture_t fixture;
            ASSERT(fixture_init(&fixture, "", false, 55));
            ASSERT(cbm_mkdir_p(TH_PATH(fixture.repo, "scope/nested"), 0755));
            ASSERT(cbm_mkdir_p(TH_PATH(fixture.repo, "elsewhere"), 0755));
            ASSERT(cbm_mkdir_p(TH_PATH(fixture.repo, "decoys"), 0755));
            char target_dir[512];
            snprintf(target_dir, sizeof(target_dir), "%s/%s", fixture.repo, item->target_file);
            char *target_slash = strrchr(target_dir, '/');
            ASSERT_NOT_NULL(target_slash);
            *target_slash = '\0';
            ASSERT(cbm_mkdir_p(target_dir, 0755));
            if (!strcmp(item->id, "scss.ambiguous")) {
                ASSERT_EQ(
                    th_write_file(TH_PATH(fixture.repo, "scope/_base.scss"), "$competing: 2;\n"),
                    0);
            } else if (!strcmp(item->id, "scss.underscore-ambiguous")) {
                ASSERT_EQ(
                    th_write_file(TH_PATH(fixture.repo, "scope/__base.scss"), "$competing: 2;\n"),
                    0);
            }
            ASSERT_EQ(th_write_file(TH_PATH(fixture.repo, item->source_file), item->source), 0);
            ASSERT_EQ(th_write_file(TH_PATH(fixture.repo, item->target_file), item->target), 0);
            const char *leaf = strrchr(item->target_file, '/');
            char decoy[128];
            snprintf(decoy, sizeof(decoy), "decoys/%s", leaf ? leaf + 1 : item->target_file);
            ASSERT_EQ(th_write_file(TH_PATH(fixture.repo, decoy), item->target), 0);
            ASSERT_EQ(run_fixture(&fixture, fixture.database, parallel), 0);
            cbm_store_t *store = cbm_store_open_path_existing(fixture.database);
            ASSERT_NOT_NULL(store);
            int edges = literal_import_count(store, item, false, false);
            int exact = literal_import_count(store, item, false, true);
            int rows = literal_import_count(store, item, true, false);
            int exact_rows = literal_import_count(store, item, true, true);
            cbm_store_close(store);
            th_rmtree(fixture.root);
            fprintf(stderr,
                    "literal import case=%s parallel=%d genuine=%d required=%d edges=%d "
                    "exact=%d rows=%d exact_rows=%d\n",
                    item->id, parallel, item->genuine, item->required, edges, exact, rows,
                    exact_rows);
            ASSERT_GTE(edges, 0);
            ASSERT_GTE(exact, 0);
            ASSERT_GTE(rows, 0);
            ASSERT_GTE(exact_rows, 0);
            genuine += item->genuine;
            found += item->genuine && exact == 1;
            missing += item->genuine && exact == 0;
            wrong += edges != exact || (!item->genuine && edges != 0);
            if (item->genuine && !item->required)
                ASSERT(item->why_tried && item->why_tried[0]);
            ASSERT_EQ(edges, item->required ? 1 : 0);
            ASSERT_EQ(exact, edges);
            ASSERT_EQ(rows, edges);
            ASSERT_EQ(exact_rows, rows);
        }
        fprintf(stderr,
                "literal import quality parallel=%d genuine=%d found=%d missing=%d wrong=%d\n",
                parallel, genuine, found, missing, wrong);
        ASSERT_EQ(genuine, expected_genuine);
        ASSERT_EQ(found, expected_genuine - expected_missing);
        ASSERT_EQ(missing, expected_missing);
        ASSERT_EQ(wrong, 0);
    }
    PASS();
}

TEST(resolver_integration_css_literal_paths_require_complete_source_tokens) {
    return literal_path_import_cases("css.", 4, 2);
}
TEST(resolver_integration_bicep_literal_paths_require_complete_source_tokens) {
    return literal_path_import_cases("bicep.", 3, 1);
}
TEST(resolver_integration_capnp_literal_paths_require_complete_source_tokens) {
    return literal_path_import_cases("capnp.", 2, 0);
}

TEST(resolver_integration_objc_imports_retain_extracted_language) {
    return literal_path_import_cases("objc.", 2, 0);
}

TEST(resolver_integration_objc_raw_imports_keep_delimiters) {
    static const char source[] = "#import \"Utils.m\"\n#import <System.h>\n@interface Main\n@end\n";
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_OBJC, "objc",
                                             "scope/Main.m", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    bool ok =
        !result->has_error && result->cached_lang == CBM_LANG_OBJC && result->imports.count == 2;
    if (ok) {
        const CBMImport *quoted = &result->imports.items[0], *system = &result->imports.items[1];
        ok = quoted->kind == CBM_IMPORT_KIND_INCLUDE_QUOTED && quoted->module_path &&
             !strcmp(quoted->module_path, "Utils.m") &&
             system->kind == CBM_IMPORT_KIND_INCLUDE_SYSTEM && system->module_path &&
             !strcmp(system->module_path, "System.h");
    }
    cbm_free_result(result);
    ASSERT_TRUE(ok);
    PASS();
}

TEST(resolver_integration_just_paths_require_complete_literal_proof) {
    return literal_path_import_cases("just.", 3, 0);
}
TEST(resolver_integration_func_paths_require_complete_literal_proof) {
    return literal_path_import_cases("func.", 2, 0);
}
TEST(resolver_integration_scss_paths_require_complete_literal_proof) {
    return literal_path_import_cases("scss.", 4, 1);
}
TEST(resolver_integration_meson_paths_require_complete_literal_proof) {
    return literal_path_import_cases("meson.", 2, 0);
}

/* A decoded ECMAScript StringValue is a dependency identity. Until decoding
 * is supported, the genuine escaped import/call remains measured recall debt;
 * the normalized raw escape's real file is always a forbidden target. */
static bool ecma_escape_call_site(cbm_store_t *store, const char *file, const char *source,
                                  const char *caller, bool required, int *wrong) {
    const char *site = strstr(source, "invoke()");
    if (!site)
        return false;
    char *sql = sqlite3_mprintf(
        "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id "
        "WHERE e.project='resolver-integration' AND s.qualified_name=%Q AND e.type='CALLS'",
        caller);
    int all = sql ? count_sql(store, sql) : -1;
    sqlite3_free(sql);
    int exact = count_edge(store, caller, RESOLVER_PROJECT ".scope.actual.target", "CALLS");
    sqlite3_stmt *statement = NULL;
    sql = sqlite3_mprintf("SELECT start_byte,end_byte,emitted,target FROM resolver_occurrences "
                          "WHERE project='resolver-integration' AND file_path=%Q AND scope=%Q "
                          "AND relation='CALLS' AND spelling='invoke'",
                          file, caller);
    bool ok =
        sql && sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &statement, NULL) == SQLITE_OK;
    sqlite3_free(sql);
    if (ok && sqlite3_step(statement) == SQLITE_ROW) {
        const char *target = (const char *)sqlite3_column_text(statement, 3);
        ok = sqlite3_column_int64(statement, 0) == (sqlite3_int64)(site - source) &&
             sqlite3_column_int64(statement, 1) ==
                 (sqlite3_int64)(site - source + strlen("invoke()")) &&
             sqlite3_column_int(statement, 2) == (required ? 1 : 0) &&
             (!required || (target && !strcmp(target, RESOLVER_PROJECT ".scope.actual.target"))) &&
             sqlite3_step(statement) == SQLITE_DONE;
    } else {
        ok = false;
    }
    sqlite3_finalize(statement);
    fprintf(stderr, "ECMA escape call file=%s required=%d all=%d exact=%d site_ok=%d\n", file,
            required, all, exact, ok);
    *wrong += all != exact;
    return ok && all == (required ? 1 : 0) && exact == all;
}

static int ecma_escaped_path_cases(const char *extension) {
    static const char *const names[] = {"plain", "escaped", "reexport", "shadowed_require"};
    static const char *const sources[] = {
        "import { target as invoke } from './actual.js';\n"
        "export function plainCall() { return invoke(); }\n",
        "import { target as invoke } from './\\u0061ctual.js';\n"
        "export function escapedCall() { return invoke(); }\n",
        "export { target } from './\\u0061ctual.js';\n",
        "function require(path) { return path; }\n"
        "const imported = require('./\\u0061ctual.js');\n",
    };
    static const char target[] = "export function target() { return 7; }\n";
    static const char decoy[] = "export function target() { return 99; }\n";
    for (int parallel = 0; parallel < 2; parallel++) {
        resolver_fixture_t fixture;
        ASSERT(fixture_init(&fixture, "", false, 55));
        ASSERT(cbm_mkdir_p(TH_PATH(fixture.repo, "scope"), 0755));
        ASSERT_EQ(th_write_file(TH_PATH(fixture.repo, "scope/actual.js"), target), 0);
        ASSERT_EQ(th_write_file(TH_PATH(fixture.repo, "scope/u0061ctual.js"), decoy), 0);
        char paths[4][128];
        for (size_t i = 0; i < 4; i++) {
            snprintf(paths[i], sizeof(paths[i]), "scope/%s.%s", names[i], extension);
            ASSERT_EQ(th_write_file(TH_PATH(fixture.repo, paths[i]), sources[i]), 0);
        }
        ASSERT_EQ(run_fixture(&fixture, fixture.database, parallel), 0);
        cbm_store_t *store = cbm_store_open_path_existing(fixture.database);
        ASSERT_NOT_NULL(store);
        int anchors = count_sql(
            store, "SELECT count(*) FROM nodes WHERE project='resolver-integration' "
                   "AND name='target' AND label='Function' AND start_line=1 AND end_line=1 "
                   "AND file_path IN ('scope/actual.js','scope/u0061ctual.js')");
        bool ok = anchors == 2;
        int found = 0, missing = 0, wrong = 0;
        for (size_t i = 0; i < 4; i++) {
            literal_import_case_t expected = {.source_file = paths[i],
                                              .target_file = "scope/actual.js",
                                              .spelling = i ? "./\\u0061ctual.js" : "./actual.js"};
            int all = literal_import_count(store, &expected, false, false);
            int exact = literal_import_count(store, &expected, false, true);
            int rows = literal_import_count(store, &expected, true, false);
            int exact_rows = literal_import_count(store, &expected, true, true);
            bool required = i == 0;
            bool genuine = i < 3;
            found += genuine && exact == 1;
            missing += genuine && exact == 0;
            wrong += all != exact || rows != exact_rows || (!genuine && all != 0);
            fprintf(
                stderr,
                "ECMA escape import file=%s required=%d all=%d exact=%d rows=%d exact_rows=%d\n",
                paths[i], required, all, exact, rows, exact_rows);
            ok = ok && all == (required ? 1 : 0) && exact == all && rows == all &&
                 exact_rows == rows;
        }
        bool plain_call = ecma_escape_call_site(
            store, paths[0], sources[0], RESOLVER_PROJECT ".scope.plain.plainCall", true, &wrong);
        bool escaped_call =
            ecma_escape_call_site(store, paths[1], sources[1],
                                  RESOLVER_PROJECT ".scope.escaped.escapedCall", false, &wrong);
        int plain_found = count_edge(store, RESOLVER_PROJECT ".scope.plain.plainCall",
                                     RESOLVER_PROJECT ".scope.actual.target", "CALLS");
        found += plain_found == 1;
        missing += plain_found == 0;
        int escaped_found = count_edge(store, RESOLVER_PROJECT ".scope.escaped.escapedCall",
                                       RESOLVER_PROJECT ".scope.actual.target", "CALLS");
        found += escaped_found == 1;
        missing += escaped_found == 0;
        cbm_store_close(store);
        th_rmtree(fixture.root);
        /* WHY: String escape decoding is unsupported. Tried: a real decoded
         * target, real raw-normalized decoy, named/reexport carriers,
         * a locally shadowed require negative, and a required plain import plus call.
         * The two escaped ESM dependencies and escaped invocation remain genuine
         * quality obligations; none may be satisfied by the decoy. */
        fprintf(
            stderr,
            "ECMA escape quality extension=%s parallel=%d genuine=5 found=%d missing=%d wrong=%d\n",
            extension, parallel, found, missing, wrong);
        ASSERT(ok);
        ASSERT(plain_call);
        ASSERT(escaped_call);
        ASSERT_EQ(found, 2);
        ASSERT_EQ(missing, 3);
        ASSERT_EQ(wrong, 0);
    }
    PASS();
}

TEST(resolver_integration_js_escaped_module_paths_never_bind_raw_decoys) {
    return ecma_escaped_path_cases("js");
}
TEST(resolver_integration_ts_escaped_module_paths_never_bind_raw_decoys) {
    return ecma_escaped_path_cases("ts");
}
TEST(resolver_integration_tsx_escaped_module_paths_never_bind_raw_decoys) {
    return ecma_escaped_path_cases("tsx");
}
TEST(resolver_integration_arkts_escaped_module_paths_never_bind_raw_decoys) {
    return ecma_escaped_path_cases("ets");
}

/* Constructed publication facts isolate the capture/projection contract from
 * language-engine recall. Spans and caller identities come from real extraction;
 * the two calls intentionally share a leaf but are distinct occurrences. */
static bool publication_capture_case(int variant, int *diagnostics, bool *complete,
                                     bool *private_keys_removed) {
    static const char source[] = "def caller():\n    receiver.run()\n    receiver.run()\n";
    CBMFileResult *parsed = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON,
                                             RESOLVER_PROJECT, "main.py", 0, NULL, NULL);
    if (!parsed || parsed->calls.count != 2) {
        cbm_free_result(parsed);
        return false;
    }
    CBMFileResult view = *parsed;
    CBMResolvedCall missing[2] = {0};
    for (int i = 0; i < 2; i++) {
        const CBMCall *call = &parsed->calls.items[i];
        missing[i] = (CBMResolvedCall){.kind = CBM_RESOLVED_INVOCATION,
                                       .caller_qn = call->enclosing_func_qn,
                                       .callee_qn = call->callee_name,
                                       .strategy = "lsp_unresolved",
                                       .reason = "unknown_receiver",
                                       .site_start_byte = call->site_start_byte,
                                       .site_end_byte = call->site_end_byte,
                                       .source_origin = call->source_origin};
    }
    /* Hybrid diagnostics are a subset: withdrawn resolved claims remain in
     * resolver occurrences, without fabricating an upstream missing carrier. */
    if (variant == 12)
        missing[0].strategy = "lsp_resolved";
    view.resolved_calls.items = missing;
    view.resolved_calls.count = variant == 11 ? 2 : 1;
    CBMCall duplicate[2] = {parsed->calls.items[0], parsed->calls.items[0]};
    if (variant == 9) {
        view.calls.items = duplicate;
        view.calls.count = 2;
    }
    CBMCall invalid_origin = parsed->calls.items[0];
    if (variant == 13) {
        invalid_origin.source_origin = (CBMSourceOrigin)-1;
        view.calls.items = &invalid_origin;
        view.calls.count = 1;
        missing[0].source_origin = invalid_origin.source_origin;
    }
    const CBMCall *call = &parsed->calls.items[0];
    cbm_resolve_result_t outcome = {
        .binding = CBM_RESOLVE_BINDING_UNIQUE,
        .domain = CBM_RESOLVE_DOMAIN_PROJECT,
        .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
        .rule = CBM_RESOLVE_RULE_SCOPE_BINDING,
        .facts = CBM_RESOLVE_FACT_EXACT_TARGET | CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                 CBM_RESOLVE_FACT_SCOPE_BOUND | CBM_RESOLVE_FACT_OCCURRENCE_EXACT,
        .target = RESOLVER_PROJECT ".lib.target",
        .candidate_count = 1,
        .reason = "constructed_publication_fact"};
    cbm_resolve_record_t record = {.file_path = "main.py",
                                   .scope = call->enclosing_func_qn,
                                   .spelling = call->callee_name,
                                   .relation = "CALLS",
                                   .start_byte = call->site_start_byte,
                                   .end_byte = call->site_end_byte,
                                   .source_origin = call->source_origin,
                                   .content_identity =
                                       parsed->source_content_id[call->source_origin],
                                   .configuration_identity = parsed->source_configuration_id,
                                   .result = outcome,
                                   .emitted = variant != 1 && variant != 12};
    if (variant == 3)
        record.file_path = "other.py";
    if (variant == 4)
        record.scope = RESOLVER_PROJECT ".other.caller";
    if (variant == 5)
        record.source_origin = CBM_SOURCE_ORIGIN_PREPROCESSED;
    if (variant == 6)
        record.spelling = "other.run";
    if (variant == 7)
        record.content_identity = "different-source";
    if (variant == 8)
        record.configuration_identity = "different-configuration";
    if (variant == 10)
        record.relation = "USAGE";
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_proof_pool_t pool;
    cbm_resolve_proof_pool_init(&pool, &arena, 0, 0, 0);
    cbm_resolve_proof_source_t origin = {.project = RESOLVER_PROJECT,
                                         .file = record.file_path,
                                         .scope = record.scope,
                                         .content_identity = record.content_identity,
                                         .configuration_identity = record.configuration_identity,
                                         .source_origin = record.source_origin};
    /* A producer can propose an edge yet fail final source-site sealing. */
    if (variant == 2)
        origin.scope = RESOLVER_PROJECT ".wrong.caller";
    cbm_resolve_proof_key_t key = {.lookup_domain = record.scope,
                                   .lookup_key = record.spelling,
                                   .binding_identity = outcome.target,
                                   .detail = record.relation,
                                   .start_byte = record.start_byte,
                                   .end_byte = record.end_byte,
                                   .origin = record.source_origin};
    record.result.proof =
        cbm_resolve_proof_leaf(&pool, CBM_PROOF_SOURCE_FACT, &origin, &key, &outcome);
    cbm_gbuf_t *graph = cbm_gbuf_new(RESOLVER_PROJECT, "/tmp");
    cbm_pipeline_t *pipeline = cbm_pipeline_new("/tmp", NULL, CBM_MODE_FULL);
    bool ok = record.result.proof && graph && pipeline;
    int64_t source_id = 0, target_id = 0;
    if (ok) {
        source_id = cbm_gbuf_upsert_node(graph, "Function", "caller", record.scope,
                                         record.file_path, 1, 3, "{}");
        target_id =
            cbm_gbuf_upsert_node(graph, "Function", "target", outcome.target, "lib.py", 1, 1, "{}");
        ok = source_id && target_id && cbm_gbuf_record_resolution(graph, &record);
    }
    if (ok && variant != 0)
        ok = cbm_gbuf_insert_edge(graph, source_id, target_id, "CALLS", "{}") != 0;
    if (ok) {
        CBMCallEvidence legacy[2] = {
            {.candidate_qn = outcome.target, .calls_emitted = variant != 0}, {0}};
        cbm_pipeline_record_unresolved_calls_for_projection(pipeline, "main.py", &view,
                                                            RESOLVER_PROJECT ".main", legacy);
        ok = cbm_gbuf_resolutions_own_shared(graph) && cbm_shared_resolver_project(graph) &&
             cbm_pipeline_reconcile_unresolved_calls(pipeline, graph);
    }
    if (ok && variant == 12) {
        size_t occurrence_count = 0;
        const cbm_resolve_record_t *occurrences = cbm_gbuf_resolutions(graph, &occurrence_count);
        ok = occurrence_count == 1 && !occurrences[0].emitted;
    }
    cbm_coverage_row_t *rows = NULL;
    int count = 0;
    cbm_pipeline_get_unresolved_calls(pipeline, &rows, &count, complete);
    *diagnostics = 0;
    *private_keys_removed = true;
    for (int i = 0; ok && i < count; i++) {
        yyjson_doc *doc = yyjson_read(rows[i].detail, strlen(rows[i].detail), 0);
        yyjson_val *sites = doc ? yyjson_doc_get_root(doc) : NULL;
        ok = yyjson_is_arr(sites);
        *diagnostics += (int)yyjson_arr_size(sites);
        *private_keys_removed &= strstr(rows[i].detail, "_resolver_") == NULL;
        yyjson_doc_free(doc);
    }
    cbm_pipeline_free(pipeline);
    cbm_gbuf_free(graph);
    cbm_arena_destroy(&arena);
    cbm_free_result(parsed);
    return ok;
}

TEST(resolver_integration_call_diagnostics_follow_final_publication) {
    for (int variant = 0; variant < 14; variant++) {
        int diagnostics = -1;
        bool complete = false, private_keys_removed = false;
        bool ok = publication_capture_case(variant, &diagnostics, &complete, &private_keys_removed);
        int expected = variant == 0 || variant == 12 ? 0 : 1;
        if (!ok || diagnostics != expected)
            fprintf(stderr, "  publication variant=%d ok=%d diagnostics=%d\n", variant, ok,
                    diagnostics);
        ASSERT(ok);
        ASSERT(complete);
        ASSERT(private_keys_removed);
        ASSERT_EQ(diagnostics, expected);
    }
    PASS();
}

/* Source files can be in any encoding. Each row is one route such bytes take to a
 * strict JSON writer on the way to publication: redis deps/tre/tests/retest.c and
 * neovim's legacy specs take the argument routes, and aborted their whole index. */
static const struct {
    const char *name;
    const char *contents;
} NON_UTF8_ROUTES[] = {
    {"route_argument.c", "int helper(const char *text, int n) { return n; }\n"
                         "int use(void) { return helper(\"caf\xe9 au lait\", 1); }\n"},
    {"route_argument.lua", "local function keep(t) return t end\nkeep(\"one \xe4 byte\")\n"},
    {"route_callee.py", "def go():\n    return \"caf\xe9\".upper()\n"},
    {"route_import.py", "import mod\xe9.sub\n\ndef call():\n    return 1\n"},
    {"route_import.rs", "use caf\xe9::thing;\nfn main() { thing(\"x\"); }\n"},
    {"route_import.js", "import a from \"./caf\xe9\";\na(\"x\");\n"},
};

/* Rows a strict reader rejects, whether for syntax or for encoding. */
static int count_rows_failing_strict_json(cbm_store_t *store, const char *sql) {
    sqlite3_stmt *statement = NULL;
    if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &statement, NULL) != SQLITE_OK)
        return -1;
    int failing = 0;
    while (sqlite3_step(statement) == SQLITE_ROW) {
        const char *json = (const char *)sqlite3_column_text(statement, 0);
        int length = sqlite3_column_bytes(statement, 0);
        yyjson_doc *doc = json ? yyjson_read(json, (size_t)length, 0) : NULL;
        if (!doc)
            failing++;
        yyjson_doc_free(doc);
    }
    sqlite3_finalize(statement);
    return failing;
}

/* The published generation is strict JSON throughout and the rest of the graph is
 * intact: the fixture's ordinary import still resolves beside the undecodable bytes. */
static bool non_utf8_generation_intact(const resolver_fixture_t *fixture) {
    cbm_store_t *store = cbm_store_open_path_existing(fixture->database);
    if (!store)
        return false;
    int edge_failures =
        count_rows_failing_strict_json(store, "SELECT properties FROM edges "
                                              "WHERE properties IS NOT NULL");
    int occurrence_failures = count_rows_failing_strict_json(
        store, "SELECT properties_json FROM resolver_occurrences");
    occurrence_failures +=
        count_rows_failing_strict_json(store, "SELECT payload FROM resolver_facts");
    int genuine = count_edge(store, RESOLVER_PROJECT ".main.caller", RESOLVER_PROJECT ".lib.target",
                             "CALLS");
    cbm_store_close(store);
    if (edge_failures || occurrence_failures || genuine != 1)
        fprintf(stderr, "  [non-utf8] edge_failures=%d occurrence_failures=%d genuine=%d\n",
                edge_failures, occurrence_failures, genuine);
    return !edge_failures && !occurrence_failures && genuine == 1;
}

TEST(resolver_integration_non_utf8_source_text_publishes_each_route) {
    int failed_routes = 0;
    for (size_t i = 0; i < sizeof(NON_UTF8_ROUTES) / sizeof(NON_UTF8_ROUTES[0]); i++) {
        resolver_fixture_t fixture;
        ASSERT(fixture_init(&fixture, IMPORT_SOURCE, true, 0));
        ASSERT_EQ(th_write_file(TH_PATH(fixture.repo, NON_UTF8_ROUTES[i].name),
                                NON_UTF8_ROUTES[i].contents),
                  0);
        int rc = run_fixture(&fixture, fixture.database, false);
        bool intact = rc == 0 && non_utf8_generation_intact(&fixture);
        if (!intact) {
            fprintf(stderr, "  [non-utf8] route=%s rc=%d\n", NON_UTF8_ROUTES[i].name, rc);
            failed_routes++;
        }
        th_rmtree(fixture.root);
    }
    ASSERT_EQ(failed_routes, 0);
    PASS();
}

/* The parallel path is what real repositories take, and it keeps the argument text:
 * the undecodable byte reaches the published CALLS edge as U+FFFD. Its surface rows
 * are all serialized together, so one such byte must not cost every file its row. */
TEST(resolver_integration_non_utf8_source_text_publishes_parallel) {
    resolver_fixture_t fixture;
    ASSERT(fixture_init(&fixture, IMPORT_SOURCE, true, 60));
    for (size_t i = 0; i < sizeof(NON_UTF8_ROUTES) / sizeof(NON_UTF8_ROUTES[0]); i++)
        ASSERT_EQ(th_write_file(TH_PATH(fixture.repo, NON_UTF8_ROUTES[i].name),
                                NON_UTF8_ROUTES[i].contents),
                  0);
    int rc = run_fixture(&fixture, fixture.database, true);
    if (rc != 0)
        fprintf(stderr, "  [non-utf8] parallel rc=%d\n", rc);
    bool intact = rc == 0 && non_utf8_generation_intact(&fixture);
    int replaced = -1, surfaces = -1;
    if (rc == 0) {
        cbm_store_t *store = cbm_store_open_path_existing(fixture.database);
        ASSERT_NOT_NULL(store);
        replaced = count_sql(store, "SELECT count(*) FROM edges WHERE type='CALLS' AND "
                                    "instr(properties, X'EFBFBD') > 0");
        surfaces = count_sql(store, "SELECT count(*) FROM lsp_surface");
        cbm_store_close(store);
    }
    th_rmtree(fixture.root);
    ASSERT_EQ(rc, 0);
    ASSERT(intact);
    ASSERT_GT(replaced, 0);
    ASSERT_GT(surfaces, 0);
    PASS();
}

SUITE(resolver_integration) {
    RUN_TEST(resolver_integration_non_utf8_source_text_publishes_each_route);
    RUN_TEST(resolver_integration_non_utf8_source_text_publishes_parallel);
    RUN_TEST(resolver_integration_call_diagnostics_follow_final_publication);
    RUN_TEST(resolver_integration_missing_facts_rebuilds_unchanged_source);
    RUN_TEST(resolver_integration_missing_evidence_rebuilds_unchanged_source);
    RUN_TEST(resolver_integration_js_escaped_module_paths_never_bind_raw_decoys);
    RUN_TEST(resolver_integration_ts_escaped_module_paths_never_bind_raw_decoys);
    RUN_TEST(resolver_integration_tsx_escaped_module_paths_never_bind_raw_decoys);
    RUN_TEST(resolver_integration_arkts_escaped_module_paths_never_bind_raw_decoys);

    RUN_TEST(resolver_integration_just_paths_require_complete_literal_proof);
    RUN_TEST(resolver_integration_func_paths_require_complete_literal_proof);
    RUN_TEST(resolver_integration_scss_paths_require_complete_literal_proof);
    RUN_TEST(resolver_integration_meson_paths_require_complete_literal_proof);
    RUN_TEST(resolver_integration_objc_raw_imports_keep_delimiters);
    RUN_TEST(resolver_integration_objc_imports_retain_extracted_language);
    RUN_TEST(resolver_integration_css_literal_paths_require_complete_source_tokens);
    RUN_TEST(resolver_integration_bicep_literal_paths_require_complete_source_tokens);
    RUN_TEST(resolver_integration_capnp_literal_paths_require_complete_source_tokens);
    RUN_TEST(resolver_integration_long_export_chain_stabilizes_before_parallel_emission);
    RUN_TEST(resolver_integration_long_module_alias_chain_preserves_exact_suffix);
    RUN_TEST(resolver_integration_old_rules_version_rebuilds_unchanged_source);
    RUN_TEST(resolver_integration_missing_generation_rebuilds_unchanged_source);
    RUN_TEST(resolver_integration_python_named_alias_sequential);
    RUN_TEST(resolver_integration_python_relative_imports_keep_exact_targets);
    RUN_TEST(resolver_integration_python_named_alias_parallel_matches_sequential);
    RUN_TEST(resolver_integration_missing_import_keeps_two_unresolved_occurrences);
    RUN_TEST(resolver_integration_added_module_rebuild_matches_fresh_and_noop_preserves_metadata);
}
