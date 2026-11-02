/* Independent source goldens: the denominator includes genuine references
 * even when extraction produces no occurrence. Known misses remain counted;
 * every edge to a different declaration is a correctness failure. */
#include "test_framework.h"
#include "test_helpers.h"
#include "pipeline/pipeline.h"
#include "store/store.h"
#include "sqlite3.h"
#include "resolver_quality.h"
#include "store/store_resolver.h"

typedef struct {
    const char *language, *main_path, *library_path, *decoy_path;
    const char *main_source, *library_source, *decoy_source;
    bool required;
    const char *why, *tried;
} quality_case_t;

static const quality_case_t QUALITY_CASES[] = {
    {"Python", "main.py", "actual.py", "decoy.py",
     "from actual import target\ndef caller():\n    target()\n", "def target():\n    pass\n",
     "def target():\n    pass\n", true, NULL, NULL},
    {"JavaScript", "main.js", "actual.js", "decoy.js",
     "import { target } from './actual.js';\nexport function caller() { target(); }\n",
     "export function target() {}\n", "export function target() {}\n", true, NULL, NULL},
    {"JavaScript alias", "main.js", "actual.js", "decoy.js",
     "import { target as invoke } from './actual.js';\nexport function caller() { invoke(); }\n",
     "export function target() {}\n", "export function target() {}\n", true, NULL, NULL},
    {"TypeScript", "main.ts", "actual.ts", "decoy.ts",
     "import { target } from './actual';\nexport function caller() { target(); }\n",
     "export function target() {}\n", "export function target() {}\n", true, NULL, NULL},
    {"Dart", "main.dart", "actual.dart", "decoy.dart",
     "import 'actual.dart';\nvoid caller() { target(); }\n", "void target() {}\n",
     "void target() {}\n", true, NULL, NULL},
    {"Rust", "main.rs", "actual.rs", "decoy.rs",
     "mod actual;\nuse crate::actual::target;\nfn caller() { target(); }\nfn main() {}\n",
     "pub fn target() {}\n", "pub fn target() {}\n", true, NULL, NULL},
    {"Lua", "main.lua", "actual.lua", "decoy.lua",
     "local actual = require('actual')\nlocal function caller() actual.target() end\n",
     "local function target() end\nreturn {target=target}\n",
     "local function target() end\nreturn {target=target}\n", true, NULL, NULL},
    /* WHY: Perl's legacy engine has no cross-file proof bridge.
     * TRIED: a literal require of a file declaring a main-package sub, with
     * repository-root cwd as the fixture's execution context. */
    {"Perl", "main.pl", "actual.pl", "decoy.pl",
     "require './actual.pl';\nsub caller { target(); }\n", "sub target { return 1; }\n1;\n",
     "package Decoy;\nsub target { return 2; }\n1;\n", false,
     "cross-file package-sub binding unimplemented", "literal require and main-package sub"},
    /* WHY: Ruby's loaded top-level methods have no proof adapter.
     * TRIED: require_relative and direct method invocation; project-wide
     * uniqueness was removed because it is not a visibility proof. */
    {"Ruby", "main.rb", "actual.rb", "decoy.rb",
     "require_relative './actual'\ndef caller\n  target()\nend\n", "def target\n  1\nend\n",
     "module Decoy\n  def target\n    2\n  end\nend\n", false,
     "loaded method binding unimplemented", "require_relative with direct target call"}};

typedef struct {
    int genuine, recovered, missing_extraction, wrong;
} quality_counts_t;
static int quality_count(sqlite3 *db, const char *query, const quality_case_t *fixture, int at) {
    sqlite3_stmt *statement = NULL;
    int count = -1;
    if (sqlite3_prepare_v2(db, query, -1, &statement, NULL) == SQLITE_OK &&
        sqlite3_bind_text(statement, 1, fixture->main_path, -1, SQLITE_STATIC) == SQLITE_OK &&
        sqlite3_bind_text(statement, 2, fixture->library_path, -1, SQLITE_STATIC) == SQLITE_OK &&
        sqlite3_bind_int(statement, 3, at) == SQLITE_OK && sqlite3_step(statement) == SQLITE_ROW)
        count = sqlite3_column_int(statement, 0);
    sqlite3_finalize(statement);
    return count;
}
static bool quality_case(const quality_case_t *fixture, quality_counts_t *counts) {
    const char *root = th_mktempdir("cbm_resolver_quality");
    if (!root)
        return false;
    char repo[768], database[768];
    snprintf(repo, sizeof(repo), "%s/repo", root);
    snprintf(database, sizeof(database), "%s/index.db", root);
    bool ok = cbm_mkdir_p(repo, 0755) &&
              th_write_file(TH_PATH(repo, fixture->main_path), fixture->main_source) == 0 &&
              th_write_file(TH_PATH(repo, fixture->library_path), fixture->library_source) == 0 &&
              th_write_file(TH_PATH(repo, fixture->decoy_path), fixture->decoy_source) == 0;
    const char *site = strstr(fixture->main_source, "target()");
    if (!site)
        site = strstr(fixture->main_source, "invoke()");
    ok = ok && site;
    cbm_pipeline_t *pipeline = ok ? cbm_pipeline_new(repo, database, CBM_MODE_FULL) : NULL;
    ok = pipeline && cbm_pipeline_set_project_name(pipeline, "quality") &&
         cbm_pipeline_run(pipeline) == 0;
    cbm_pipeline_free(pipeline);
    cbm_store_t *store = ok ? cbm_store_open_path_existing(database) : NULL;
    int good = -1, wrong = -1, occurrences = -1;
    if (store) {
        sqlite3 *db = cbm_store_get_db(store);
        int at = (int)(site - fixture->main_source);
        good = quality_count(db,
                             "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id JOIN "
                             "nodes t ON t.id=e.target_id "
                             "WHERE e.type='CALLS' AND s.file_path=?1 AND t.file_path=?2 AND "
                             "t.name='target' AND ?3>=0",
                             fixture, at);
        wrong = quality_count(db,
                              "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id JOIN "
                              "nodes t ON t.id=e.target_id "
                              "WHERE e.type='CALLS' AND s.file_path=?1 AND (t.file_path!=?2 OR "
                              "t.name!='target') AND ?3>=0",
                              fixture, at);
        occurrences = quality_count(
            db,
            "SELECT count(*) FROM resolver_occurrences WHERE file_path=?1 AND relation='CALLS' "
            "AND start_byte<=?3 AND end_byte>?3 AND length(?2)>0",
            fixture, at);
    }
    if (store)
        cbm_store_close(store);
    th_cleanup(root);
    ok = ok && good >= 0 && good <= 1 && wrong == 0 && occurrences >= 0;
    counts->genuine++;
    if (good == 1)
        counts->recovered++;
    if (occurrences == 0)
        counts->missing_extraction++;
    if (wrong > 0)
        counts->wrong += wrong;
    if (good != 1 || occurrences == 0 || wrong != 0)
        fprintf(stderr, "resolver quality detail: %s good=%d wrong=%d occurrences=%d\n",
                fixture->language, good, wrong, occurrences);
    if (good == 0)
        fprintf(stderr, "resolver quality miss: %s; %s; tried: %s; extraction=%s\n",
                fixture->language, fixture->why ? fixture->why : "unexpected regression",
                fixture->tried ? fixture->tried : "required source golden",
                occurrences ? "present" : "missing");
    return ok && (!fixture->required || (good == 1 && occurrences > 0)) &&
           (fixture->required || (fixture->why && fixture->tried));
}
TEST(resolver_quality_counts_known_misses_and_forbids_wrong_targets) {
    quality_counts_t counts = {0};
    bool ok = true;
    for (size_t i = 0; i < sizeof(QUALITY_CASES) / sizeof(QUALITY_CASES[0]); i++)
        ok = quality_case(&QUALITY_CASES[i], &counts) && ok;
    fprintf(stderr,
            "resolver quality oracle: genuine=%d recovered=%d missed=%d missing_extraction=%d "
            "false_positive=%d\n",
            counts.genuine, counts.recovered, counts.genuine - counts.recovered,
            counts.missing_extraction, counts.wrong);
    ASSERT(ok);
    ASSERT_EQ(counts.wrong, 0);
    PASS();
}
/* These are independent genuine-reference goldens, not expected-empty tests.
 * Each known materialization miss retains its WHY and what was tried. Future
 * correct edges improve recovered; any wrong declaration remains a hard failure. */
typedef struct {
    const char *path, *label;
    int start, end;
    const char *source, *why, *tried;
} value_quality_case_t;
static const value_quality_case_t VALUE_QUALITY_CASES[] = {
    {"field_shadow.py", "Field", 3, 3,
     "class Service:\n"
     "    def __init__(self):\n"
     "        self.handler = 42\n"
     "    def handler(self):\n"
     "        pass\n"
     "def accept(callback):\n"
     "    pass\n"
     "def argument(service: Service):\n"
     "    accept(service.handler)\n",
     "instance-field declaration not materialized separately from method namesake",
     "source-typed receiver plus exact declaration lookup"},
    {"function_rebinding.py", "Function", 6, 7,
     "def handler():\n"
     "    pass\n"
     "def replace_with_value(function):\n"
     "    return 42\n"
     "@replace_with_value\n"
     "def handler():\n"
     "    pass\n"
     "def accept(callback):\n"
     "    pass\n"
     "def argument():\n"
     "    accept(handler)\n",
     "later source binding shares graph QN with earlier declaration",
     "source-order binding rejected by exact graph declaration collision guard"},
    {"method_rebinding.py", "Method", 7, 8,
     "def replace_with_value(function):\n"
     "    return 42\n"
     "class Service:\n"
     "    def handler(self):\n"
     "        pass\n"
     "    @replace_with_value\n"
     "    def handler(self):\n"
     "        pass\n"
     "def accept(callback):\n"
     "    pass\n"
     "def argument(service: Service):\n"
     "    accept(service.handler)\n",
     "later method binding shares graph QN with earlier declaration",
     "decorated value proof cannot select the collapsed graph declaration"}};

static int value_quality_count(sqlite3 *db, const char *sql, const value_quality_case_t *item,
                               int at) {
    sqlite3_stmt *statement = NULL;
    int count = -1;
    if (sqlite3_prepare_v2(db, sql, -1, &statement, NULL) == SQLITE_OK &&
        sqlite3_bind_text(statement, 1, item->path, -1, SQLITE_STATIC) == SQLITE_OK &&
        sqlite3_bind_text(statement, 2, item->label, -1, SQLITE_STATIC) == SQLITE_OK &&
        sqlite3_bind_int(statement, 3, item->start) == SQLITE_OK &&
        sqlite3_bind_int(statement, 4, item->end) == SQLITE_OK &&
        sqlite3_bind_int(statement, 5, at) == SQLITE_OK && sqlite3_step(statement) == SQLITE_ROW)
        count = sqlite3_column_int(statement, 0);
    sqlite3_finalize(statement);
    return count;
}

static bool value_quality_case(const value_quality_case_t *item, quality_counts_t *counts) {
    const char *root = th_mktempdir("cbm_resolver_value_quality");
    if (!root)
        return false;
    char repo[768], database[768];
    snprintf(repo, sizeof(repo), "%s/repo", root);
    snprintf(database, sizeof(database), "%s/index.db", root);
    bool ok =
        cbm_mkdir_p(repo, 0755) && th_write_file(TH_PATH(repo, item->path), item->source) == 0;
    const char *argument = strstr(item->source, "    accept(");
    const char *site = argument ? strstr(argument, "handler") : NULL;
    ok = ok && site && item->why && item->tried;
    cbm_pipeline_t *pipeline = ok ? cbm_pipeline_new(repo, database, CBM_MODE_FULL) : NULL;
    ok = pipeline && cbm_pipeline_set_project_name(pipeline, "value_quality") &&
         cbm_pipeline_run(pipeline) == 0;
    cbm_pipeline_free(pipeline);
    cbm_store_t *store = ok ? cbm_store_open_path_existing(database) : NULL;
    int good = -1, wrong = -1, occurrences = -1;
    if (store) {
        sqlite3 *db = cbm_store_get_db(store);
        int at = (int)(site - item->source);
        good = value_quality_count(
            db,
            "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id "
            "JOIN nodes t ON t.id=e.target_id WHERE s.file_path=?1 AND s.name='argument' "
            "AND e.type='USAGE' AND t.file_path=?1 AND t.name='handler' AND t.label=?2 "
            "AND t.start_line=?3 AND t.end_line=?4 AND ?5>=0",
            item, at);
        wrong = value_quality_count(
            db,
            "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id "
            "JOIN nodes t ON t.id=e.target_id WHERE s.file_path=?1 AND s.name='argument' "
            "AND e.type IN ('USAGE','CALL_REFERENCE','CALLS') AND t.name='handler' AND "
            "(e.type!='USAGE' OR t.file_path!=?1 OR t.label!=?2 OR t.start_line!=?3 OR "
            "t.end_line!=?4) AND ?5>=0",
            item, at);
        occurrences = value_quality_count(
            db,
            "SELECT count(*) FROM resolver_occurrences WHERE file_path=?1 AND "
            "relation IN ('USAGE','CALL_REFERENCE') AND start_byte<=?5 AND end_byte>?5 "
            "AND length(?2)>0 AND ?3>0 AND ?4>0",
            item, at);
    }
    if (store)
        cbm_store_close(store);
    th_cleanup(root);
    counts->genuine++;
    counts->recovered += good == 1;
    counts->missing_extraction += occurrences == 0;
    if (wrong > 0)
        counts->wrong += wrong;
    if (good == 0)
        fprintf(stderr, "resolver quality miss: %s; %s; tried: %s; extraction=%s\n", item->path,
                item->why, item->tried, occurrences ? "present" : "missing");
    return ok && good >= 0 && good <= 1 && wrong == 0 && occurrences >= 0;
}

TEST(resolver_quality_python_materialization_gaps_are_counted) {
    quality_counts_t counts = {0};
    bool ok = true;
    for (size_t i = 0; i < sizeof(VALUE_QUALITY_CASES) / sizeof(VALUE_QUALITY_CASES[0]); i++)
        ok = value_quality_case(&VALUE_QUALITY_CASES[i], &counts) && ok;
    fprintf(stderr,
            "resolver value quality oracle: genuine=%d recovered=%d missed=%d "
            "missing_extraction=%d false_positive=%d\n",
            counts.genuine, counts.recovered, counts.genuine - counts.recovered,
            counts.missing_extraction, counts.wrong);
    ASSERT(ok);
    ASSERT_EQ(counts.genuine, 3);
    ASSERT_EQ(counts.wrong, 0);
    PASS();
}

/* Synthetic persisted generations exercise the oracle itself. Deliberate row
 * corruption bypasses production admission so a faulty writer cannot make the
 * test oracle certify its own mistake. Production proof tests remain separate. */
static const RQ_Source RQ_TEST_SOURCES[] = {
    {.path = "main.c",
     .text = "void caller(int value) {\n    target(value);\n}\n"
             "void other(int value) { target(value); }\n"},
    {.path = "lib.c", .text = "void target(int value) {}\n"},
    {.path = "decoy.c", .text = "void target(int value) {}\n"}};

typedef struct {
    cbm_store_t *store;
    int64_t caller, other, target, decoy;
    uint32_t start, end;
} rq_test_store_t;

static RQ_Expected rq_test_expected(RQ_Mode mode) {
    return (RQ_Expected){.case_id = "c.direct-call",
                         .site = {.file = "main.c", .caller = "caller", .needle = "target(value)"},
                         .relation = "CALLS",
                         .target = {.file = "lib.c",
                                    .suffix = "lib.target",
                                    .label = "Function",
                                    .declaration_line = 1,
                                    .declaration_text = "void target(int value) {}"},
                         .mode = mode,
                         .gap_id = "RQ-C-DIRECT-001",
                         .why = "synthetic absent binding/materialization for oracle validation",
                         .tried = "independent caller, source occurrence and declaration anchors"};
}
static bool rq_test_open(rq_test_store_t *fixture, bool materialized) {
    memset(fixture, 0, sizeof(*fixture));
    fixture->store = cbm_store_open_memory();
    if (!fixture->store || cbm_store_upsert_project(fixture->store, "rq", "/fixture") != 0)
        return false;
    cbm_node_t nodes[] = {{.project = "rq",
                           .label = "Function",
                           .name = "caller",
                           .qualified_name = "p.main.caller(int)",
                           .file_path = "main.c",
                           .start_line = 1,
                           .end_line = 3},
                          {.project = "rq",
                           .label = "Function",
                           .name = "other",
                           .qualified_name = "p.main.other(int)",
                           .file_path = "main.c",
                           .start_line = 4,
                           .end_line = 4},
                          {.project = "rq",
                           .label = "Function",
                           .name = "target",
                           .qualified_name = "p.lib.target(int)",
                           .file_path = "lib.c",
                           .start_line = 1,
                           .end_line = 1},
                          {.project = "rq",
                           .label = "Function",
                           .name = "target",
                           .qualified_name = "p.decoy.target(int)",
                           .file_path = "decoy.c",
                           .start_line = 1,
                           .end_line = 1}};
    fixture->caller = cbm_store_upsert_node(fixture->store, nodes);
    fixture->other = cbm_store_upsert_node(fixture->store, nodes + 1);
    fixture->target = materialized ? cbm_store_upsert_node(fixture->store, nodes + 2) : 0;
    fixture->decoy = cbm_store_upsert_node(fixture->store, nodes + 3);
    cbm_resolve_file_t files[3] = {0};
    char hashes[3][CBM_SHA256_HEX_LEN + 1];
    for (int i = 0; i < 3; i++) {
        cbm_sha256_hex(RQ_TEST_SOURCES[i].text, strlen(RQ_TEST_SOURCES[i].text), hashes[i]);
        files[i] = (cbm_resolve_file_t){.file_path = RQ_TEST_SOURCES[i].path,
                                        .module = "p",
                                        .raw_content_identity = hashes[i],
                                        .configuration_identity = "fixture",
                                        .raw_length = strlen(RQ_TEST_SOURCES[i].text),
                                        .language = CBM_LANG_C,
                                        .complete = true};
    }
    const char *site = strstr(RQ_TEST_SOURCES[0].text, "target(value)");
    fixture->start = (uint32_t)(site - RQ_TEST_SOURCES[0].text);
    fixture->end = fixture->start + (uint32_t)strlen("target(value)");
    return fixture->caller > 0 && fixture->other > 0 && fixture->decoy > 0 &&
           (!materialized || fixture->target > 0) &&
           cbm_store_resolver_replace_generation(fixture->store, "rq", NULL, 0, files, 3, NULL,
                                                 0) == 0;
}

typedef enum {
    RQ_TEST_EXACT,
    RQ_TEST_TARGET,
    RQ_TEST_SPAN,
    RQ_TEST_ROLE,
    RQ_TEST_CALLER,
    RQ_TEST_ORIGIN,
    RQ_TEST_NO_EDGE,
    RQ_TEST_NO_ROW,
    RQ_TEST_UNRELATED
} rq_test_mutation_t;

/* The logical occurrence tables are read-only views over compact rows, so a fixture
 * writes those rows, with its strings in the project's dictionary. */
static sqlite3_int64 rq_test_text(sqlite3 *db, const char *value) {
    sqlite3_stmt *stmt = NULL;
    sqlite3_int64 id = -1;
    if (sqlite3_prepare_v2(db,
                           "INSERT INTO resolver_text(pid,id,value) "
                           "SELECT pid,(SELECT coalesce(max(id),0)+1 FROM resolver_text),?1 "
                           "FROM resolver_projects WHERE project='rq' RETURNING id",
                           -1, &stmt, NULL) == SQLITE_OK &&
        sqlite3_bind_text(stmt, 1, value, -1, SQLITE_TRANSIENT) == SQLITE_OK &&
        sqlite3_step(stmt) == SQLITE_ROW)
        id = sqlite3_column_int64(stmt, 0);
    if (stmt && sqlite3_step(stmt) != SQLITE_DONE)
        id = -1;
    sqlite3_finalize(stmt);
    return id;
}

typedef struct {
    const char *file, *scope, *relation, *spelling, *target;
    uint32_t start, end;
    int origin, domain;
} rq_test_row_t;

static bool rq_test_row(sqlite3 *db, const rq_test_row_t *row) {
    const char *texts[] = {row->file,   row->scope, row->relation, row->spelling,
                           row->target, "fixture",  "{}"};
    const char *sql =
        "INSERT INTO resolver_occurrence_rows(pid,ordinal,file,scope,relation,spelling,start_byte,"
        "span,source_origin,binding,domain,analysis,rule,facts,target,candidate_count,reason,"
        "emitted,props,line,content,configuration,proof,source_binding_proof) "
        "SELECT pid,(SELECT coalesce(max(ordinal)+1,0) FROM resolver_occurrence_rows),?1,?2,?3,?4,"
        "?8,?9,?10,1,?11,0,0,0,?5,1,?6,1,?7,-1,NULL,NULL,0,0 "
        "FROM resolver_projects WHERE project='rq'";
    sqlite3_stmt *stmt = NULL;
    bool ok = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK;
    for (int i = 0; ok && i < 7; i++) {
        sqlite3_int64 id = rq_test_text(db, texts[i]);
        ok = id > 0 && sqlite3_bind_int64(stmt, i + 1, id) == SQLITE_OK;
    }
    ok = ok && sqlite3_bind_int64(stmt, 8, row->start) == SQLITE_OK &&
         sqlite3_bind_int64(stmt, 9, (sqlite3_int64)row->end - row->start) == SQLITE_OK &&
         sqlite3_bind_int(stmt, 10, row->origin) == SQLITE_OK &&
         sqlite3_bind_int(stmt, 11, row->domain) == SQLITE_OK && sqlite3_step(stmt) == SQLITE_DONE;
    sqlite3_finalize(stmt);
    return ok;
}

static bool rq_test_emit(rq_test_store_t *fixture, rq_test_mutation_t mutation) {
    bool other = mutation == RQ_TEST_CALLER || mutation == RQ_TEST_UNRELATED;
    const char *scope = other ? "p.main.other(int)" : "p.main.caller(int)";
    const char *target = mutation == RQ_TEST_TARGET ? "p.decoy.target(int)" : "p.lib.target(int)";
    const char *relation = mutation == RQ_TEST_ROLE ? "USAGE" : "CALLS";
    uint32_t start = fixture->start + (mutation == RQ_TEST_SPAN);
    uint32_t end = fixture->end;
    if (mutation == RQ_TEST_UNRELATED) {
        const char *source = RQ_TEST_SOURCES[0].text;
        start = (uint32_t)(strstr(source + fixture->end, "target(value)") - source);
        end = start + (uint32_t)strlen("target(value)");
    }
    bool ok = true;
    if (mutation != RQ_TEST_NO_ROW) {
        rq_test_row_t row = {.file = "main.c",
                             .scope = scope,
                             .relation = relation,
                             .spelling = "target",
                             .target = target,
                             .start = start,
                             .end = end,
                             .origin = mutation == RQ_TEST_ORIGIN ? CBM_SOURCE_ORIGIN_PREPROCESSED
                                                                  : CBM_SOURCE_ORIGIN_RAW,
                             .domain = 1};
        ok = rq_test_row(cbm_store_get_db(fixture->store), &row);
    }
    if (ok && mutation != RQ_TEST_NO_EDGE) {
        cbm_edge_t edge = {.project = "rq",
                           .source_id = other ? fixture->other : fixture->caller,
                           .target_id =
                               mutation == RQ_TEST_TARGET ? fixture->decoy : fixture->target,
                           .type = relation};
        ok = cbm_store_insert_edge(fixture->store, &edge) > 0;
    }
    return ok;
}
static bool rq_test_check(rq_test_store_t *fixture, RQ_Expected *item, RQ_Report *report) {
    return rq_check(fixture->store, "rq", "oracle-self-test", RQ_TEST_SOURCES, 3, item, 1, NULL, 0,
                    report, NULL);
}

/* Independently authored line-preserving transform of RQ_TEST_SOURCES[0].
 * The synthetic writer below is deliberately separate from the oracle. */
static const char RQ_TEST_PREPROCESSED[] = "void caller(int value) {\ntarget(value);\n}\n"
                                           "void other(int value) { target(value); }\n";

static bool rq_test_preprocessed(rq_test_store_t *fixture, RQ_Source sources[3],
                                 RQ_Expected *item) {
    memcpy(sources, RQ_TEST_SOURCES, sizeof(RQ_TEST_SOURCES));
    sources[0].preprocessed_text = RQ_TEST_PREPROCESSED;
    sources[0].preprocessed_configuration_identity = "rq-preprocessed-fixture-v1";
    *item = rq_test_expected(RQ_REQUIRED);
    const char *site = strstr(RQ_TEST_PREPROCESSED, "target(value)");
    item->site.preprocessed_start_byte = (uint32_t)(site - RQ_TEST_PREPROCESSED);
    item->site.preprocessed_end_byte =
        item->site.preprocessed_start_byte + (uint32_t)strlen("target(value)");
    if (!rq_test_emit(fixture, RQ_TEST_EXACT))
        return false;
    sqlite3 *db = cbm_store_get_db(fixture->store);
    char digest[CBM_SHA256_HEX_LEN + 1];
    cbm_sha256_hex(RQ_TEST_PREPROCESSED, strlen(RQ_TEST_PREPROCESSED), digest);
    sqlite3_stmt *stmt = NULL;
    bool ok =
        sqlite3_prepare_v2(db,
                           "UPDATE resolver_file_summaries SET preprocessed_content_identity=?1,"
                           "preprocessed_length=?2,configuration_identity=?3 "
                           "WHERE project='rq' AND file_path='main.c'",
                           -1, &stmt, NULL) == SQLITE_OK &&
        sqlite3_bind_text(stmt, 1, digest, -1, SQLITE_TRANSIENT) == SQLITE_OK &&
        sqlite3_bind_int64(stmt, 2, (sqlite3_int64)strlen(RQ_TEST_PREPROCESSED)) == SQLITE_OK &&
        sqlite3_bind_text(stmt, 3, sources[0].preprocessed_configuration_identity, -1,
                          SQLITE_STATIC) == SQLITE_OK &&
        sqlite3_step(stmt) == SQLITE_DONE;
    sqlite3_finalize(stmt);
    stmt = NULL;
    ok = ok &&
         sqlite3_prepare_v2(
             db,
             "UPDATE resolver_occurrence_rows SET start_byte=?1,span=?2-?1,source_origin=1 "
             "WHERE ordinal=0",
             -1, &stmt, NULL) == SQLITE_OK &&
         sqlite3_bind_int64(stmt, 1, item->site.preprocessed_start_byte) == SQLITE_OK &&
         sqlite3_bind_int64(stmt, 2, item->site.preprocessed_end_byte) == SQLITE_OK &&
         sqlite3_step(stmt) == SQLITE_DONE;
    sqlite3_finalize(stmt);
    stmt = NULL;
    sqlite3_int64 content = ok ? rq_test_text(db, digest) : -1;
    sqlite3_int64 configuration =
        ok ? rq_test_text(db, sources[0].preprocessed_configuration_identity) : -1;
    ok = content > 0 && configuration > 0 &&
         sqlite3_prepare_v2(db,
                            "UPDATE resolver_occurrence_rows SET content=?1,configuration=?2 "
                            "WHERE ordinal=0",
                            -1, &stmt, NULL) == SQLITE_OK &&
         sqlite3_bind_int64(stmt, 1, content) == SQLITE_OK &&
         sqlite3_bind_int64(stmt, 2, configuration) == SQLITE_OK &&
         sqlite3_step(stmt) == SQLITE_DONE;
    sqlite3_finalize(stmt);
    return ok;
}

TEST(resolver_quality_oracle_maps_one_preprocessed_occurrence_once) {
    rq_test_store_t fixture;
    ASSERT(rq_test_open(&fixture, true));
    RQ_Source sources[3];
    RQ_Expected item;
    ASSERT(rq_test_preprocessed(&fixture, sources, &item));
    RQ_Report report;
    ASSERT(rq_check(fixture.store, "rq", "mapped-origin", sources, 3, &item, 1, NULL, 0, &report,
                    NULL));
    ASSERT_EQ(report.genuine, 1);
    ASSERT_EQ(report.found, 1);
    ASSERT_EQ(report.missing, 0);
    /* The raw and transformed rows represent one golden, never two recoveries. */
    ASSERT(rq_test_emit(&fixture, RQ_TEST_EXACT));
    ASSERT(rq_check(fixture.store, "rq", "mapped-origin", sources, 3, &item, 1, NULL, 0, &report,
                    NULL));
    cbm_store_close(fixture.store);
    ASSERT_EQ(report.genuine, 1);
    ASSERT_EQ(report.found, 1);
    ASSERT_EQ(report.missing, 0);
    PASS();
}

TEST(resolver_quality_oracle_rejects_preprocessed_metadata_and_span_corruption) {
    static const char *const mutations[] = {
        "UPDATE resolver_occurrence_rows SET start_byte=start_byte+1,span=span-1",
        "UPDATE resolver_file_summaries SET preprocessed_content_identity='wrong' "
        "WHERE file_path='main.c'",
        "UPDATE resolver_file_summaries SET preprocessed_length=preprocessed_length+1 "
        "WHERE file_path='main.c'",
        "UPDATE resolver_file_summaries SET configuration_identity='wrong' "
        "WHERE file_path='main.c'",
        "UPDATE resolver_text SET value='wrong' WHERE id="
        "(SELECT content FROM resolver_occurrence_rows)",
        "UPDATE resolver_text SET value='wrong' WHERE id="
        "(SELECT configuration FROM resolver_occurrence_rows)",
        "UPDATE resolver_occurrence_rows SET source_origin=0",
        "UPDATE resolver_occurrence_rows SET source_origin=99",
        "UPDATE resolver_occurrence_rows SET start_byte=start_byte+4",
        "UPDATE resolver_occurrence_rows SET content=NULL,configuration=NULL"};
    for (size_t i = 0; i < sizeof(mutations) / sizeof(mutations[0]); i++) {
        rq_test_store_t fixture;
        ASSERT(rq_test_open(&fixture, true));
        RQ_Source sources[3];
        RQ_Expected item;
        ASSERT(rq_test_preprocessed(&fixture, sources, &item));
        ASSERT(sqlite3_exec(cbm_store_get_db(fixture.store), mutations[i], NULL, NULL, NULL) ==
               SQLITE_OK);
        RQ_Report report;
        bool ok = rq_check(fixture.store, "rq", "mapped-corruption", sources, 3, &item, 1, NULL, 0,
                           &report, NULL);
        cbm_store_close(fixture.store);
        ASSERT(!ok);
        ASSERT_EQ(report.genuine, 1);
        ASSERT_EQ(report.found, 0);
        ASSERT_EQ(report.missing, 1);
        ASSERT(report.metadata_errors + report.wrong > 0);
    }
    PASS();
}

TEST(resolver_quality_oracle_requires_explicit_preprocessed_mapping) {
    rq_test_store_t fixture;
    ASSERT(rq_test_open(&fixture, true));
    RQ_Source sources[3];
    RQ_Expected item;
    ASSERT(rq_test_preprocessed(&fixture, sources, &item));
    RQ_Report report;
    RQ_Expected unmapped = item;
    unmapped.site.preprocessed_start_byte = unmapped.site.preprocessed_end_byte = 0;
    ASSERT(!rq_check(fixture.store, "rq", "unmapped-origin", sources, 3, &unmapped, 1, NULL, 0,
                     &report, NULL));
    ASSERT(report.metadata_errors > 0);
    ASSERT_EQ(report.found, 0);
    RQ_Expected bad_span = item;
    bad_span.site.preprocessed_end_byte++;
    ASSERT(!rq_check(fixture.store, "rq", "bad-map", sources, 3, &bad_span, 1, NULL, 0, &report,
                     NULL));
    ASSERT(report.invalid_oracle > 0);
    const char *other =
        strstr(RQ_TEST_PREPROCESSED + item.site.preprocessed_end_byte, "target(value)");
    bad_span = item;
    bad_span.site.preprocessed_start_byte = (uint32_t)(other - RQ_TEST_PREPROCESSED);
    bad_span.site.preprocessed_end_byte = bad_span.site.preprocessed_start_byte + 13;
    ASSERT(!rq_check(fixture.store, "rq", "wrong-line-map", sources, 3, &bad_span, 1, NULL, 0,
                     &report, NULL));
    ASSERT(report.invalid_oracle > 0);
    sources[0].preprocessed_configuration_identity = NULL;
    ASSERT(!rq_check(fixture.store, "rq", "incomplete-map", sources, 3, &item, 1, NULL, 0, &report,
                     NULL));
    cbm_store_close(fixture.store);
    ASSERT(report.invalid_oracle > 0);
    PASS();
}

TEST(resolver_quality_oracle_rejects_many_raw_sites_mapped_to_one_occurrence) {
    RQ_Source sources[3];
    memcpy(sources, RQ_TEST_SOURCES, sizeof(sources));
    sources[0].text = "void caller(int value) {\n    target(value); target(value);\n}\n";
    sources[0].preprocessed_text = "void caller(int value) {\ntarget(value);\n}\n";
    sources[0].preprocessed_configuration_identity = "rq-preprocessed-fixture-v1";
    RQ_Expected items[2] = {rq_test_expected(RQ_REQUIRED), rq_test_expected(RQ_REQUIRED)};
    items[1].case_id = "c.second-direct-call";
    items[1].site.occurrence_index = 1;
    for (size_t i = 0; i < 2; i++) {
        items[i].site.preprocessed_start_byte = 25;
        items[i].site.preprocessed_end_byte = 38;
        RQ_State anchor = {.site = &items[i].site};
        ASSERT(rq_anchor(&anchor, sources, 3));
    }
    RQ_State states[2] = {0};
    RQ_Report report = {.genuine = 2};
    ASSERT(!rq_prepare(states, items, 2, NULL, 0, sources, 3, &report, NULL));
    ASSERT(report.invalid_oracle > 0);
    ASSERT_EQ(report.genuine, 2);
    ASSERT_EQ(report.found, 0);
    PASS();
}
TEST(resolver_quality_oracle_preserves_exact_occurrence_and_scope) {
    rq_test_store_t fixture;
    ASSERT(rq_test_open(&fixture, true));
    ASSERT(rq_test_emit(&fixture, RQ_TEST_EXACT));
    ASSERT(rq_test_emit(&fixture, RQ_TEST_UNRELATED));
    RQ_Expected item = rq_test_expected(RQ_REQUIRED);
    RQ_Report report;
    ASSERT(rq_test_check(&fixture, &item, &report));
    ASSERT_EQ(report.genuine, 1);
    ASSERT_EQ(report.found, 1);
    ASSERT_EQ(report.missing, 0);
    ASSERT_EQ(report.wrong, 0);
    ASSERT_EQ(report.projection_errors, 0);
    item.target.suffix = "lib.target(int)";
    ASSERT(rq_test_check(&fixture, &item, &report));
    item.target.suffix = "lib.target(float)";
    bool wrong_signature = rq_test_check(&fixture, &item, &report);
    cbm_store_close(fixture.store);
    ASSERT(!wrong_signature);
    ASSERT(report.wrong > 0);
    PASS();
}
TEST(resolver_quality_oracle_rejects_mutated_occurrences_and_projection) {
    const rq_test_mutation_t mutations[] = {RQ_TEST_TARGET, RQ_TEST_SPAN,    RQ_TEST_ROLE,
                                            RQ_TEST_CALLER, RQ_TEST_NO_EDGE, RQ_TEST_NO_ROW};
    for (size_t i = 0; i < sizeof(mutations) / sizeof(mutations[0]); i++) {
        rq_test_store_t fixture;
        ASSERT(rq_test_open(&fixture, true));
        ASSERT(rq_test_emit(&fixture, mutations[i]));
        RQ_Expected item = rq_test_expected(RQ_KNOWN_GAP);
        RQ_Report report;
        bool ok = rq_test_check(&fixture, &item, &report);
        cbm_store_close(fixture.store);
        ASSERT(!ok);
        ASSERT_EQ(report.genuine, 1);
        ASSERT_EQ(report.found, 0);
        ASSERT_EQ(report.missing, 1);
        ASSERT(report.wrong + report.projection_errors > 0);
    }
    PASS();
}
TEST(resolver_quality_oracle_counts_missing_materialization_and_required_miss) {
    rq_test_store_t fixture;
    ASSERT(rq_test_open(&fixture, false));
    RQ_Expected item = rq_test_expected(RQ_KNOWN_GAP);
    RQ_Report report;
    bool known = rq_test_check(&fixture, &item, &report);
    ASSERT(known);
    ASSERT_EQ(report.genuine, 1);
    ASSERT_EQ(report.missing, 1);
    ASSERT_EQ(report.found, 0);
    ASSERT_EQ(report.wrong, 0);
    item.target.suffix = NULL; /* Semantic constructor identity not yet representable. */
    ASSERT(rq_test_check(&fixture, &item, &report));
    ASSERT_EQ(report.genuine, 1);
    ASSERT_EQ(report.missing, 1);
    item = rq_test_expected(RQ_REQUIRED);
    bool required = rq_test_check(&fixture, &item, &report);
    cbm_store_close(fixture.store);
    ASSERT(!required);
    ASSERT_EQ(report.required_missing, 1);
    ASSERT_EQ(report.missing, 1);
    PASS();
}
TEST(resolver_quality_oracle_requires_promotion_of_recovered_gap) {
    rq_test_store_t fixture;
    ASSERT(rq_test_open(&fixture, true));
    ASSERT(rq_test_emit(&fixture, RQ_TEST_EXACT));
    RQ_Expected item = rq_test_expected(RQ_KNOWN_GAP);
    RQ_Report report;
    ASSERT(!rq_test_check(&fixture, &item, &report));
    ASSERT_EQ(report.found, 1);
    ASSERT_EQ(report.recovered_gaps, 1);
    item.expected_recovered = true;
    ASSERT(rq_test_check(&fixture, &item, &report));
    ASSERT_EQ(report.recovered_gaps, 0);
    ASSERT(sqlite3_exec(cbm_store_get_db(fixture.store),
                        "DELETE FROM edges; DELETE FROM resolver_occurrence_rows", NULL, NULL,
                        NULL) == SQLITE_OK);
    bool regressed = rq_test_check(&fixture, &item, &report);
    cbm_store_close(fixture.store);
    ASSERT(!regressed);
    ASSERT_EQ(report.required_missing, 1);
    PASS();
}
TEST(resolver_quality_oracle_keeps_forbidden_targets_out_of_denominator) {
    rq_test_store_t fixture;
    ASSERT(rq_test_open(&fixture, true));
    RQ_Expected item = rq_test_expected(RQ_REQUIRED);
    RQ_Forbidden forbidden = {.case_id = "c.direct-call.no-decoy",
                              .site = item.site,
                              .relation = "CALLS",
                              .target = {.file = "decoy.c",
                                         .suffix = "decoy.target",
                                         .label = "Function",
                                         .declaration_line = 1,
                                         .declaration_text = "void target(int value) {}"}};
    ASSERT(rq_test_emit(&fixture, RQ_TEST_EXACT));
    RQ_Report report;
    ASSERT(rq_check(fixture.store, "rq", "forbidden-control", RQ_TEST_SOURCES, 3, &item, 1,
                    &forbidden, 1, &report, NULL));
    ASSERT_EQ(report.genuine, 1);
    /* A value argument is not another call. The valid call may still project
     * the same aggregate edge; only this different exact site is forbidden. */
    RQ_Forbidden argument = {
        .case_id = "c.direct-call.argument-is-not-call",
        .site = {.file = "main.c", .caller = "caller", .needle = "value", .occurrence_index = 1},
        .relation = "CALLS",
        .target = item.target};
    ASSERT(rq_check(fixture.store, "rq", "aggregate-negative-control", RQ_TEST_SOURCES, 3, &item, 1,
                    &argument, 1, &report, NULL));
    ASSERT(rq_test_emit(&fixture, RQ_TEST_TARGET));
    bool ok = rq_check(fixture.store, "rq", "forbidden-control", RQ_TEST_SOURCES, 3, &item, 1,
                       &forbidden, 1, &report, NULL);
    cbm_store_close(fixture.store);
    ASSERT(!ok);
    ASSERT(report.wrong > 0);
    ASSERT_EQ(report.genuine, 1);
    PASS();
}
TEST(resolver_quality_oracle_wildcard_negative_is_relation_scoped) {
    rq_test_store_t fixture;
    ASSERT(rq_test_open(&fixture, true));
    /* The wildcard forbids CALLS at the parameter site. A USAGE at another
     * site in this caller is outside this deliberately narrow audit. */
    RQ_Forbidden forbidden = {.case_id = "c.parameter-is-not-call",
                              .site = {.file = "main.c", .caller = "caller", .needle = "value"},
                              .relation = "CALLS",
                              .any_target = true};
    ASSERT(rq_test_emit(&fixture, RQ_TEST_ROLE));
    RQ_Report report;
    ASSERT(rq_check(fixture.store, "rq", "wildcard-relation-control", RQ_TEST_SOURCES, 3, NULL, 0,
                    &forbidden, 1, &report, NULL));
    ASSERT_EQ(report.genuine, 0);
    ASSERT_EQ(report.wrong, 0);
    ASSERT_EQ(report.projection_errors, 0);
    /* All CALLS from this caller are inside the closed relation domain. */
    ASSERT(rq_test_emit(&fixture, RQ_TEST_EXACT));
    bool ok = rq_check(fixture.store, "rq", "wildcard-relation-control", RQ_TEST_SOURCES, 3, NULL,
                       0, &forbidden, 1, &report, NULL);
    cbm_store_close(fixture.store);
    ASSERT(!ok);
    ASSERT(report.wrong > 0);
    PASS();
}
/* Synthetic row mechanics for an explicitly declared multicast contract.
 * Ordinary single-target expectations still reject duplicate sites. */
TEST(resolver_quality_oracle_multicast_requires_distinct_declared_targets) {
    rq_test_store_t fixture;
    ASSERT(rq_test_open(&fixture, true));
    RQ_Source sources[3];
    memcpy(sources, RQ_TEST_SOURCES, sizeof(sources));
    sources[1].text = "void target(int value) {\n}\n";
    char digest[CBM_SHA256_HEX_LEN + 1];
    cbm_sha256_hex(sources[1].text, strlen(sources[1].text), digest);
    sqlite3_stmt *snapshot = NULL;
    ASSERT(sqlite3_prepare_v2(cbm_store_get_db(fixture.store),
                              "UPDATE resolver_file_summaries SET "
                              "raw_content_identity=?1,raw_length=?2 WHERE file_path='lib.c'",
                              -1, &snapshot, NULL) == SQLITE_OK);
    ASSERT(sqlite3_bind_text(snapshot, 1, digest, -1, SQLITE_TRANSIENT) == SQLITE_OK);
    ASSERT(sqlite3_bind_int64(snapshot, 2, (sqlite3_int64)strlen(sources[1].text)) == SQLITE_OK);
    ASSERT(sqlite3_step(snapshot) == SQLITE_DONE);
    sqlite3_finalize(snapshot);
    ASSERT(sqlite3_exec(cbm_store_get_db(fixture.store),
                        "UPDATE nodes SET end_line=2 WHERE file_path='lib.c'", NULL, NULL,
                        NULL) == SQLITE_OK);
    RQ_Expected items[2] = {rq_test_expected(RQ_KNOWN_GAP), rq_test_expected(RQ_KNOWN_GAP)};
    items[0].target.declaration_text = "void target(int value) {";
    items[1].case_id = "multicast.second";
    items[1].target = (RQ_Target){.file = "decoy.c",
                                  .suffix = "decoy.target",
                                  .label = "Function",
                                  .declaration_line = 1,
                                  .declaration_text = "void target(int value) {}"};
    RQ_Report report;
    ASSERT(
        !rq_check(fixture.store, "rq", "multicast", sources, 3, items, 2, NULL, 0, &report, NULL));
    ASSERT(report.invalid_oracle > 0);
    items[0].dispatch_group = items[1].dispatch_group = "explicit-notify-topic";
    ASSERT(
        rq_check(fixture.store, "rq", "multicast", sources, 3, items, 2, NULL, 0, &report, NULL));
    ASSERT_EQ(report.genuine, 2);
    ASSERT_EQ(report.missing, 2);
    ASSERT(rq_test_emit(&fixture, RQ_TEST_EXACT));
    ASSERT(
        !rq_check(fixture.store, "rq", "multicast", sources, 3, items, 2, NULL, 0, &report, NULL));
    ASSERT_EQ(report.recovered_gaps, 1);
    items[0].expected_recovered = true;
    ASSERT(
        rq_check(fixture.store, "rq", "multicast", sources, 3, items, 2, NULL, 0, &report, NULL));
    ASSERT_EQ(report.found, 1);
    ASSERT_EQ(report.missing, 1);
    ASSERT(rq_test_emit(&fixture, RQ_TEST_TARGET));
    items[1].expected_recovered = true;
    ASSERT(
        rq_check(fixture.store, "rq", "multicast", sources, 3, items, 2, NULL, 0, &report, NULL));
    ASSERT_EQ(report.found, 2);
    /* Different anchors inside one multi-line declaration are still one
     * semantic target. Suffix aliases cannot inflate recovery either. */
    items[1].target = items[0].target;
    items[1].target.declaration_line = 2;
    items[1].target.declaration_text = "}";
    ASSERT(
        !rq_check(fixture.store, "rq", "multicast", sources, 3, items, 2, NULL, 0, &report, NULL));
    ASSERT(report.invalid_oracle > 0);
    items[1].target = items[0].target;
    items[1].target.suffix = "target";
    ASSERT(
        !rq_check(fixture.store, "rq", "multicast", sources, 3, items, 2, NULL, 0, &report, NULL));
    ASSERT(report.invalid_oracle > 0);
    items[1].target = items[0].target;
    ASSERT(
        !rq_check(fixture.store, "rq", "multicast", sources, 3, items, 2, NULL, 0, &report, NULL));
    ASSERT(report.invalid_oracle > 0);
    items[1].dispatch_group = "different-topic";
    ASSERT(
        !rq_check(fixture.store, "rq", "multicast", sources, 3, items, 2, NULL, 0, &report, NULL));
    ASSERT(report.invalid_oracle > 0);
    cbm_store_close(fixture.store);
    PASS();
}
TEST(resolver_quality_oracle_nonresolver_relations_are_outside_scope) {
    rq_test_store_t fixture;
    ASSERT(rq_test_open(&fixture, true));
    ASSERT(rq_test_emit(&fixture, RQ_TEST_EXACT));
    cbm_edge_t edge = {.project = "rq",
                       .source_id = fixture.caller,
                       .target_id = fixture.target,
                       .type = "OVERRIDE"};
    ASSERT(cbm_store_insert_edge(fixture.store, &edge) > 0);
    RQ_Expected item = rq_test_expected(RQ_REQUIRED);
    RQ_Report report;
    bool ok = rq_test_check(&fixture, &item, &report);
    cbm_store_close(fixture.store);
    ASSERT(ok);
    ASSERT_EQ(report.found, 1);
    ASSERT_EQ(report.projection_errors, 0);
    PASS();
}
TEST(resolver_quality_oracle_rejects_unmapped_origin_and_stale_source) {
    rq_test_store_t fixture;
    ASSERT(rq_test_open(&fixture, true));
    ASSERT(rq_test_emit(&fixture, RQ_TEST_ORIGIN));
    RQ_Expected item = rq_test_expected(RQ_KNOWN_GAP);
    RQ_Report report;
    ASSERT(!rq_test_check(&fixture, &item, &report));
    ASSERT(report.metadata_errors > 0);
    RQ_Source stale[3];
    memcpy(stale, RQ_TEST_SOURCES, sizeof(stale));
    stale[0].text = "void caller(int value) { target(value); }\n";
    bool ok =
        rq_check(fixture.store, "rq", "stale-source", stale, 3, &item, 1, NULL, 0, &report, NULL);
    cbm_store_close(fixture.store);
    ASSERT(!ok);
    ASSERT(report.metadata_errors > 0);
    ASSERT_EQ(report.genuine, 1);
    PASS();
}
/* Structural nodes have no declaration interval. A zero range is accepted
 * only for an exact File or explicitly configured Folder, with an independent
 * source/configuration anchor; it never excuses an ordinary declaration. */
TEST(resolver_quality_oracle_configured_folder_requires_exact_identity_and_anchor) {
    const RQ_Source sources[] = {
        {.path = "Core/Package.swift", .text = ".target(name: \"Core\", dependencies: [])\n"},
        {.path = "Other/Package.swift", .text = ".target(name: \"Other\", dependencies: [])\n"},
    };
    RQ_Target target = {.file = "Core/Sources/Core",
                        .suffix = "Core.Sources.Core",
                        .label = "Folder",
                        .declaration_line = 1,
                        .declaration_text = ".target(name: \"Core\", dependencies: [])",
                        .domain = RQ_TARGET_CONFIGURED_FOLDER,
                        .configuration_file = "Core/Package.swift"};
    ASSERT(rq_target_anchor(&target, sources, 2));
    ASSERT(rq_target(&target, "Core/Sources/Core", "p.Core.Sources.Core", "Core", "Folder", 0, 0));
    ASSERT(
        !rq_target(&target, "Other/Sources/Core", "p.Core.Sources.Core", "Core", "Folder", 0, 0));
    ASSERT(!rq_target(&target, "Core/Sources/Core", "p.Other.Core", "Core", "Folder", 0, 0));
    ASSERT(!rq_target(&target, "Core/Sources/Core", "p.Core.Sources.Core", "Core", "File", 0, 0));
    ASSERT(!rq_target(&target, "Core/Sources/Core", "p.Core.Sources.Core", "Core", "Folder", 1, 1));
    target.configuration_file = "Other/Package.swift";
    ASSERT(!rq_target_anchor(&target, sources, 2));
    target.configuration_file = "Core/Package.swift";
    target.declaration_text = ".target(name: \"Other\", dependencies: [])";
    ASSERT(!rq_target_anchor(&target, sources, 2));
    target.domain = RQ_TARGET_PROJECT;
    ASSERT(!rq_target_anchor(&target, sources, 2));
    PASS();
}

TEST(resolver_quality_oracle_file_zero_ranges_keep_exact_source_and_label) {
    rq_test_store_t fixture;
    ASSERT(rq_test_open(&fixture, true));
    ASSERT(rq_test_emit(&fixture, RQ_TEST_EXACT));
    sqlite3 *db = cbm_store_get_db(fixture.store);
    ASSERT_EQ(sqlite3_exec(
                  db,
                  "UPDATE nodes SET label='File',start_line=0,end_line=0 WHERE qualified_name IN "
                  "('p.main.caller(int)','p.lib.target(int)')",
                  NULL, NULL, NULL),
              SQLITE_OK);
    RQ_Expected item = rq_test_expected(RQ_REQUIRED);
    item.target.label = "File";
    ASSERT(!rq_target(&item.target, "lib.c", "p.lib.target(int)", "target", "File", 2, 2));
    ASSERT(!rq_target(&item.target, "lib.c", "p.lib.target(int)", "target", "Function", 0, 0));
    RQ_Report report;
    ASSERT(rq_test_check(&fixture, &item, &report));
    ASSERT_EQ(report.found, 1);
    ASSERT_EQ(sqlite3_exec(
                  db, "UPDATE nodes SET label='Function' WHERE qualified_name='p.lib.target(int)'",
                  NULL, NULL, NULL),
              SQLITE_OK);
    ASSERT(!rq_test_check(&fixture, &item, &report));
    ASSERT(report.wrong > 0);
    ASSERT_EQ(sqlite3_exec(db,
                           "UPDATE nodes SET label='File',file_path='decoy.c' WHERE "
                           "qualified_name='p.lib.target(int)'",
                           NULL, NULL, NULL),
              SQLITE_OK);
    ASSERT(!rq_test_check(&fixture, &item, &report));
    ASSERT(report.wrong > 0);
    ASSERT_EQ(
        sqlite3_exec(db,
                     "UPDATE nodes SET file_path='lib.c' WHERE qualified_name='p.lib.target(int)';"
                     "UPDATE nodes SET label='Function' WHERE qualified_name='p.main.caller(int)'",
                     NULL, NULL, NULL),
        SQLITE_OK);
    ASSERT(!rq_test_check(&fixture, &item, &report));
    ASSERT(report.wrong > 0);
    cbm_store_close(fixture.store);
    PASS();
}

TEST(resolver_quality_oracle_rejects_unexplained_gap_and_invalid_anchor) {
    rq_test_store_t fixture;
    ASSERT(rq_test_open(&fixture, false));
    RQ_Expected item = rq_test_expected(RQ_KNOWN_GAP);
    RQ_Report report;
    item.gap_id = NULL;
    ASSERT(!rq_test_check(&fixture, &item, &report));
    ASSERT(report.invalid_oracle > 0);
    item = rq_test_expected(RQ_KNOWN_GAP);
    item.site.needle = "not_present(value)";
    bool ok = rq_test_check(&fixture, &item, &report);
    cbm_store_close(fixture.store);
    ASSERT(!ok);
    ASSERT(report.invalid_oracle > 0);
    PASS();
}

/* Builtin identities come from the materialization ontology, not project
 * fixture declarations. These synthetic rows test only the independent oracle;
 * production admission remains governed by the resolver's own proof policy. */
static const RQ_Source RQ_BUILTIN_SOURCES[] = {
    {.path = "tool.py", .text = "def lookup(xs):\n    return len(xs)\n"},
    {.path = "decoy.py", .text = "def len(xs):\n    return 0\n"}};

static RQ_Expected rq_builtin_expected(void) {
    return (RQ_Expected){
        .case_id = "python.external.len",
        .site = {.file = "tool.py", .caller = "tool.lookup", .needle = "len(xs)"},
        .relation = "CALLS",
        .target = {.file = "<python-builtins>",
                   .label = "Function",
                   .domain = RQ_TARGET_EXTERNAL,
                   .external_identity = "builtins.len"},
        .mode = RQ_KNOWN_GAP,
        .gap_id = "RQ-EXTERNAL-BUILTIN-001",
        .why = "synthetic missing external binding for independent oracle validation",
        .tried = "literal Python builtin site and independently declared external ontology"};
}
static bool rq_builtin_open(rq_test_store_t *fixture) {
    memset(fixture, 0, sizeof(*fixture));
    fixture->store = cbm_store_open_memory();
    if (!fixture->store || cbm_store_upsert_project(fixture->store, "rq", "/fixture") != 0)
        return false;
    cbm_node_t caller = {.project = "rq",
                         .label = "Function",
                         .name = "lookup",
                         .qualified_name = "rq.tool.lookup",
                         .file_path = "tool.py",
                         .start_line = 1,
                         .end_line = 2};
    fixture->caller = cbm_store_upsert_node(fixture->store, &caller);
    cbm_resolve_file_t files[2] = {0};
    char hashes[2][CBM_SHA256_HEX_LEN + 1];
    for (int i = 0; i < 2; i++) {
        cbm_sha256_hex(RQ_BUILTIN_SOURCES[i].text, strlen(RQ_BUILTIN_SOURCES[i].text), hashes[i]);
        files[i] = (cbm_resolve_file_t){.file_path = RQ_BUILTIN_SOURCES[i].path,
                                        .module = "rq",
                                        .raw_content_identity = hashes[i],
                                        .configuration_identity = "fixture",
                                        .raw_length = strlen(RQ_BUILTIN_SOURCES[i].text),
                                        .language = CBM_LANG_PYTHON,
                                        .complete = true};
    }
    fixture->start =
        (uint32_t)(strstr(RQ_BUILTIN_SOURCES[0].text, "len(xs)") - RQ_BUILTIN_SOURCES[0].text);
    fixture->end = fixture->start + (uint32_t)strlen("len(xs)");
    return fixture->caller > 0 && cbm_store_resolver_replace_generation(fixture->store, "rq", NULL,
                                                                        0, files, 2, NULL, 0) == 0;
}
static bool rq_builtin_emit(rq_test_store_t *fixture, const char *identity, const char *file,
                            const char *label, cbm_resolve_domain_t domain) {
    cbm_node_t target = {.project = "rq",
                         .label = label,
                         .name = "len",
                         .qualified_name = identity,
                         .file_path = file,
                         .start_line = 1,
                         .end_line = 1};
    fixture->target = cbm_store_upsert_node(fixture->store, &target);
    rq_test_row_t row = {.file = "tool.py",
                         .scope = "rq.tool.lookup",
                         .relation = "CALLS",
                         .spelling = "len",
                         .target = identity,
                         .start = fixture->start,
                         .end = fixture->end,
                         .origin = CBM_SOURCE_ORIGIN_RAW,
                         .domain = domain};
    bool ok = fixture->target > 0 && rq_test_row(cbm_store_get_db(fixture->store), &row);
    cbm_edge_t edge = {.project = "rq",
                       .source_id = fixture->caller,
                       .target_id = fixture->target,
                       .type = "CALLS"};
    return ok && cbm_store_insert_edge(fixture->store, &edge) > 0;
}
static bool rq_builtin_check(rq_test_store_t *fixture, RQ_Expected *item, RQ_Report *report) {
    return rq_check(fixture->store, "rq", "external-oracle-self-test", RQ_BUILTIN_SOURCES, 2, item,
                    1, NULL, 0, report, NULL);
}
TEST(resolver_quality_oracle_counts_external_misses_and_requires_promotion) {
    rq_test_store_t fixture;
    ASSERT(rq_builtin_open(&fixture));
    RQ_Expected item = rq_builtin_expected();
    RQ_Report report;
    ASSERT(rq_builtin_check(&fixture, &item, &report));
    ASSERT_EQ(report.genuine, 1);
    ASSERT_EQ(report.missing, 1);
    ASSERT_EQ(report.found, 0);
    ASSERT_EQ(report.invalid_oracle, 0);
    ASSERT(rq_builtin_emit(&fixture, "builtins.len", "<python-builtins>", "Function",
                           CBM_RESOLVE_DOMAIN_EXTERNAL));
    ASSERT(!rq_builtin_check(&fixture, &item, &report));
    ASSERT_EQ(report.found, 1);
    ASSERT_EQ(report.recovered_gaps, 1);
    item.expected_recovered = true;
    ASSERT(rq_builtin_check(&fixture, &item, &report));
    ASSERT_EQ(report.recovered_gaps, 0);
    item.mode = RQ_REQUIRED;
    ASSERT(rq_builtin_check(&fixture, &item, &report));
    cbm_store_close(fixture.store);
    PASS();
}
TEST(resolver_quality_oracle_rejects_external_namesakes_and_unknown_domains) {
    static const struct {
        const char *identity, *file, *label;
        cbm_resolve_domain_t domain;
    } wrong[] = {{"rq.decoy.len", "decoy.py", "Function", CBM_RESOLVE_DOMAIN_PROJECT},
                 {"rq.builtins.len", "<python-builtins>", "Function", CBM_RESOLVE_DOMAIN_EXTERNAL},
                 {"builtins.len", "decoy.py", "Function", CBM_RESOLVE_DOMAIN_EXTERNAL},
                 {"builtins.len", "<python-builtins>", "Method", CBM_RESOLVE_DOMAIN_EXTERNAL},
                 {"builtins.len", "<python-builtins>", "Function", CBM_RESOLVE_DOMAIN_UNKNOWN},
                 {"builtins.len", "<python-builtins>", "Function", CBM_RESOLVE_DOMAIN_PROJECT}};
    for (size_t i = 0; i < sizeof(wrong) / sizeof(wrong[0]); i++) {
        rq_test_store_t fixture;
        ASSERT(rq_builtin_open(&fixture));
        ASSERT(rq_builtin_emit(&fixture, wrong[i].identity, wrong[i].file, wrong[i].label,
                               wrong[i].domain));
        RQ_Expected item = rq_builtin_expected();
        RQ_Report report;
        bool ok = rq_builtin_check(&fixture, &item, &report);
        cbm_store_close(fixture.store);
        ASSERT(!ok);
        ASSERT_EQ(report.genuine, 1);
        ASSERT_EQ(report.found, 0);
        ASSERT(report.wrong > 0);
    }
    PASS();
}
TEST(resolver_quality_oracle_rejects_malformed_external_expectations) {
    rq_test_store_t fixture;
    ASSERT(rq_builtin_open(&fixture));
    for (int variant = 0; variant < 10; variant++) {
        RQ_Expected item = rq_builtin_expected();
        switch (variant) {
        case 0:
            item.target.domain = RQ_TARGET_PROJECT;
            break; /* omitted discriminator */
        case 1:
            item.target.domain = (RQ_TargetDomain)99;
            break;
        case 2:
            item.target.external_identity = NULL;
            break;
        case 3:
            item.target.external_identity = "builtins..len";
            break;
        case 4:
            item.target.file = "decoy.py";
            break;
        case 5:
            item.target.label = NULL;
            break;
        case 6:
            item.target.label = "Method";
            break;
        case 7:
            item.target.suffix = "builtins.len";
            break;
        case 8:
            item.target.declaration_line = 1;
            break;
        case 9:
            item.target.declaration_text = "def len(xs):";
            break;
        }
        RQ_Report report;
        ASSERT(!rq_builtin_check(&fixture, &item, &report));
        ASSERT(report.invalid_oracle > 0);
        ASSERT_EQ(report.genuine, 1);
    }
    cbm_store_close(fixture.store);
    PASS();
}

/* Project-domain mode excludes an unrelated compiler builtin, not a wrong
 * external target at the golden span or a project target at any other span. */
static bool rq_project_domain_extra(rq_test_store_t *fixture, int64_t target_id, const char *target,
                                    uint32_t start, uint32_t end, bool external) {
    rq_test_row_t row = {.file = "main.c",
                         .scope = "p.main.caller(int)",
                         .relation = "CALLS",
                         .spelling = "extra",
                         .target = target,
                         .start = start,
                         .end = end,
                         .origin = CBM_SOURCE_ORIGIN_RAW,
                         .domain =
                             external ? CBM_RESOLVE_DOMAIN_EXTERNAL : CBM_RESOLVE_DOMAIN_PROJECT};
    bool ok = rq_test_row(cbm_store_get_db(fixture->store), &row);
    cbm_edge_t edge = {
        .project = "rq", .source_id = fixture->caller, .target_id = target_id, .type = "CALLS"};
    return ok && cbm_store_insert_edge(fixture->store, &edge) > 0;
}

TEST(resolver_quality_project_domain_preserves_exact_sites_and_project_targets) {
    for (int mutation = 0; mutation < 3; mutation++) {
        rq_test_store_t fixture;
        ASSERT(rq_test_open(&fixture, true));
        RQ_Source sources[3];
        memcpy(sources, RQ_TEST_SOURCES, sizeof(sources));
        sources[0].text = "void caller(int value) {\n    target(value); __builtin_trap();\n}\n"
                          "void other(int value) { target(value); }\n";
        char hash[CBM_SHA256_HEX_LEN + 1];
        cbm_sha256_hex(sources[0].text, strlen(sources[0].text), hash);
        sqlite3 *db = cbm_store_get_db(fixture.store);
        sqlite3_stmt *stmt = NULL;
        bool setup =
            sqlite3_prepare_v2(db,
                               "UPDATE resolver_file_summaries SET raw_content_identity=?1,"
                               "raw_length=?2 WHERE project='rq' AND file_path='main.c'",
                               -1, &stmt, NULL) == SQLITE_OK &&
            sqlite3_bind_text(stmt, 1, hash, -1, SQLITE_TRANSIENT) == SQLITE_OK &&
            sqlite3_bind_int64(stmt, 2, (sqlite3_int64)strlen(sources[0].text)) == SQLITE_OK &&
            sqlite3_step(stmt) == SQLITE_DONE;
        sqlite3_finalize(stmt);
        cbm_node_t builtin = {.project = "rq",
                              .label = "Function",
                              .name = "__builtin_trap",
                              .qualified_name = "compiler.__builtin_trap",
                              .file_path = "<compiler-builtins>"};
        int64_t builtin_id = cbm_store_upsert_node(fixture.store, &builtin);
        setup = setup && builtin_id > 0 && rq_test_emit(&fixture, RQ_TEST_EXACT);
        const char *extra = strstr(sources[0].text, "__builtin_trap()");
        uint32_t start = mutation == 1 ? fixture.start : (uint32_t)(extra - sources[0].text);
        uint32_t end = mutation == 1 ? fixture.end : start + (uint32_t)strlen("__builtin_trap()");
        setup = setup && rq_project_domain_extra(
                             &fixture, mutation == 2 ? fixture.decoy : builtin_id,
                             mutation == 2 ? "p.decoy.target(int)" : "compiler.__builtin_trap",
                             start, end, mutation != 2);
        RQ_Expected item = rq_test_expected(RQ_REQUIRED);
        RQ_Report report = {0}, unrestricted = {0};
        bool accepted =
            setup && rq_check_project_declarations(fixture.store, "rq", "project-domain", sources,
                                                   3, &item, 1, NULL, 0, &report, NULL);
        bool default_accepted = setup && rq_check(fixture.store, "rq", "default-domain", sources, 3,
                                                  &item, 1, NULL, 0, &unrestricted, NULL);
        cbm_store_close(fixture.store);
        ASSERT(setup);
        ASSERT_EQ(accepted, mutation == 0);
        ASSERT(!default_accepted);
        ASSERT_EQ(report.genuine, 1);
        ASSERT_EQ(report.found, 1);
        ASSERT_EQ(report.missing, 0);
        ASSERT_EQ(report.metadata_errors + report.invalid_oracle, 0);
        if (mutation)
            ASSERT_GT(report.wrong, 0);
        else
            ASSERT_EQ(report.wrong + report.projection_errors, 0);
    }
    PASS();
}

TEST(resolver_quality_oracle_requires_exact_authored_caller_variant) {
    static const char *properties[] = {
        "{\"variants\":[{\"file_path\":\"main.c\",\"start_line\":1,\"end_line\":3}]}",
        "{}",
        "{\"variants\":[{\"file_path\":\"other.c\",\"start_line\":1,\"end_line\":3}]}",
        "{\"variants\":[{\"file_path\":\"main.c\",\"start_line\":2,\"end_line\":3}]}",
        "{\"variants\":[{\"file_path\":\"main.c\",\"start_line\":1,\"end_line\":4}]}",
        "{\"variants\":[{\"file_path\":\"main.c\",\"start_line\":1,\"end_line\":3},"
        "{\"file_path\":\"main.c\",\"start_line\":1,\"end_line\":3}]}",
        "{",
        "{\"variants\":{\"unexpected\":{\"file_path\":\"main.c\",\"start_line\":1,\"end_line\":3}}"
        "}"};
    for (size_t mutation = 0; mutation < 21; mutation++) {
        rq_test_store_t fixture;
        ASSERT(rq_test_open(&fixture, true));
        ASSERT(rq_test_emit(&fixture, mutation == 11 ? RQ_TEST_CALLER : RQ_TEST_EXACT));
        sqlite3 *db = cbm_store_get_db(fixture.store);
        sqlite3_stmt *statement = NULL;
        bool setup =
            sqlite3_prepare_v2(
                db,
                "UPDATE nodes SET file_path='canonical.c',start_line=4,end_line=4,properties=?1 "
                "WHERE id=?2",
                -1, &statement, NULL) == SQLITE_OK &&
            sqlite3_bind_text(statement, 1, properties[mutation < 8 ? mutation : 0], -1,
                              SQLITE_STATIC) == SQLITE_OK &&
            sqlite3_bind_int64(statement, 2, fixture.caller) == SQLITE_OK &&
            sqlite3_step(statement) == SQLITE_DONE;
        sqlite3_finalize(statement);
        if (mutation >= 12) {
            static const char *extra[] = {
                "{\"variants\":[{\"file_path\":\"main.c\",\"start_line\":\"1\",\"end_line\":3}]}",
                "{\"variants\":[{\"file_path\":\"main.c\",\"start_line\":1,\"end_line\":\"3\"}]}",
                "{\"variants\":[{\"file_path\":1,\"start_line\":1,\"end_line\":3}]}",
                "{\"variants\":[{\"file_path\":\"main.c\",\"start_line\":1,\"start_line\":2,\"end_"
                "line\":3}]}",
                "{\"variants\":[{\"file_path\":\"main.c\",\"start_line\":1,\"end_line\":3}],"
                "\"variants\":[]}"};
            if (mutation < 16 || mutation == 20) {
                statement = NULL;
                setup = setup &&
                        sqlite3_prepare_v2(db, "UPDATE nodes SET properties=?1 WHERE id=?2", -1,
                                           &statement, NULL) == SQLITE_OK &&
                        sqlite3_bind_text(statement, 1, extra[mutation == 20 ? 4 : mutation - 12],
                                          -1, SQLITE_STATIC) == SQLITE_OK &&
                        sqlite3_bind_int64(statement, 2, fixture.caller) == SQLITE_OK &&
                        sqlite3_step(statement) == SQLITE_DONE;
                sqlite3_finalize(statement);
            }
            if (mutation == 18) {
                /* Even an overlapping canonical range cannot bypass an explicit variant. */
                setup = setup &&
                        sqlite3_exec(db,
                                     "UPDATE nodes SET "
                                     "file_path='main.c',start_line=1,end_line=3,properties='{}' "
                                     "WHERE name='caller'",
                                     NULL, NULL, NULL) == SQLITE_OK;
            }
        }
        RQ_Expected item = rq_test_expected(RQ_REQUIRED);
        item.site.caller_variant_start_line = 1;
        item.site.caller_variant_end_line = 3;
        item.site.caller_variant_declaration = "void caller(int value) {";
        if (mutation == 8)
            item.site.caller_variant_declaration = "void invented(int value) {";
        if (mutation == 9)
            item.site.caller_variant_end_line = 0;
        if (mutation == 10)
            item.site.caller_variant_end_line = 1;
        if (mutation == 16)
            item.site.caller_variant_start_line = 0;
        if (mutation == 17)
            item.site.caller_variant_declaration = NULL;
        if (mutation == 19) {
            /* A stored variant alone cannot weaken an ordinary expectation. */
            item.site.caller_variant_start_line = item.site.caller_variant_end_line = 0;
            item.site.caller_variant_declaration = NULL;
        }
        RQ_Report report;
        bool accepted = setup && rq_test_check(&fixture, &item, &report);
        cbm_store_close(fixture.store);
        ASSERT(setup);
        ASSERT_EQ(accepted, mutation == 0);
        if (mutation == 0) {
            ASSERT_EQ(report.genuine, 1);
            ASSERT_EQ(report.found, 1);
            ASSERT_EQ(report.wrong + report.projection_errors + report.invalid_oracle, 0);
        } else {
            ASSERT(report.wrong + report.projection_errors + report.invalid_oracle > 0);
        }
    }
    PASS();
}

TEST(resolver_quality_oracle_variant_bounds_cover_whole_occurrence) {
    const RQ_Source sources[] = {
        {.path = "main.c", .text = "void caller(int value) {\n    target(\n        value);\n}\n"},
        {.path = "lib.c", .text = "void target(int value) {}\n"}};
    for (unsigned mutation = 0; mutation < 3; mutation++) {
        RQ_Expected item = rq_test_expected(RQ_REQUIRED);
        item.site.needle = "target(\n        value)";
        item.site.caller_variant_start_line = 1;
        item.site.caller_variant_end_line = mutation == 0 ? 4 : mutation == 1 ? 2 : 5;
        item.site.caller_variant_declaration = "void caller(int value) {";
        RQ_State state = {0};
        RQ_Report report = {0};
        bool accepted = rq_prepare(&state, &item, 1, NULL, 0, sources, 2, &report, NULL);
        ASSERT_EQ(accepted, mutation == 0);
        ASSERT_EQ(report.invalid_oracle, mutation == 0 ? 0 : 1);
    }
    PASS();
}

TEST(resolver_quality_oracle_accepts_crlf_without_weakening_declaration_identity) {
    const struct {
        const char *source;
        bool valid;
    } cases[] = {
        {"// library\r\nvoid target(int value) {}\r\n", true},
        {"// library\nvoid target(int value) {}\n", true},
        {"// library\r\nvoid target(int value) {}", true},
        {"// library\r\nvoid target(int value) {} \r\n", false},
        {"// library\r\nvoid target(int value) {}\r\r\n", false},
        {"// library\r\nvoid target(int value) {}\r", false},
        {"// library\r\nvoid target(\rint value) {}\r\n", false},
        {"// library\r\nvoid decoy(int value) {}\r\n", false},
    };
    const RQ_Target target = {.file = "lib.c",
                              .suffix = "lib.target",
                              .label = "Function",
                              .declaration_line = 2,
                              .declaration_text = "void target(int value) {}"};
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        RQ_Source input = {.path = "lib.c", .text = cases[i].source};
        ASSERT_EQ(rq_target_anchor(&target, &input, 1), cases[i].valid);
    }
    PASS();
}

SUITE(resolver_quality) {
    RUN_TEST(resolver_quality_oracle_accepts_crlf_without_weakening_declaration_identity);
    RUN_TEST(resolver_quality_oracle_variant_bounds_cover_whole_occurrence);
    RUN_TEST(resolver_quality_oracle_requires_exact_authored_caller_variant);
    RUN_TEST(resolver_quality_project_domain_preserves_exact_sites_and_project_targets);
    RUN_TEST(resolver_quality_oracle_maps_one_preprocessed_occurrence_once);
    RUN_TEST(resolver_quality_oracle_rejects_preprocessed_metadata_and_span_corruption);
    RUN_TEST(resolver_quality_oracle_requires_explicit_preprocessed_mapping);
    RUN_TEST(resolver_quality_oracle_rejects_many_raw_sites_mapped_to_one_occurrence);
    RUN_TEST(resolver_quality_oracle_multicast_requires_distinct_declared_targets);
    RUN_TEST(resolver_quality_oracle_nonresolver_relations_are_outside_scope);
    RUN_TEST(resolver_quality_oracle_counts_external_misses_and_requires_promotion);
    RUN_TEST(resolver_quality_oracle_rejects_external_namesakes_and_unknown_domains);
    RUN_TEST(resolver_quality_oracle_rejects_malformed_external_expectations);
    RUN_TEST(resolver_quality_oracle_preserves_exact_occurrence_and_scope);
    RUN_TEST(resolver_quality_oracle_rejects_mutated_occurrences_and_projection);
    RUN_TEST(resolver_quality_oracle_counts_missing_materialization_and_required_miss);
    RUN_TEST(resolver_quality_oracle_requires_promotion_of_recovered_gap);
    RUN_TEST(resolver_quality_oracle_keeps_forbidden_targets_out_of_denominator);
    RUN_TEST(resolver_quality_oracle_wildcard_negative_is_relation_scoped);
    RUN_TEST(resolver_quality_oracle_rejects_unmapped_origin_and_stale_source);
    RUN_TEST(resolver_quality_oracle_configured_folder_requires_exact_identity_and_anchor);
    RUN_TEST(resolver_quality_oracle_file_zero_ranges_keep_exact_source_and_label);
    RUN_TEST(resolver_quality_oracle_rejects_unexplained_gap_and_invalid_anchor);

    RUN_TEST(resolver_quality_python_materialization_gaps_are_counted);
    RUN_TEST(resolver_quality_counts_known_misses_and_forbids_wrong_targets);
}
