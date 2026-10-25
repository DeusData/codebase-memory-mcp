/* Independent source goldens: the denominator includes genuine references
 * even when extraction produces no occurrence. Known misses remain counted;
 * every edge to a different declaration is a correctness failure. */
#include "test_framework.h"
#include "test_helpers.h"
#include "pipeline/pipeline.h"
#include "store/store.h"
#include "sqlite3.h"

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
    /* WHY: Lua require result/export-table binding has no proof adapter.
     * TRIED: explicit local require plus a named function in a literal table;
     * scope-only binding cannot prove the cross-file returned member. */
    {"Lua", "main.lua", "actual.lua", "decoy.lua",
     "local actual = require('actual')\nlocal function caller() actual.target() end\n",
     "local function target() end\nreturn {target=target}\n",
     "local function target() end\nreturn {target=target}\n", false,
     "require return-table member binding unimplemented",
     "explicit local require and literal returned table"},
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

SUITE(resolver_quality) {
    RUN_TEST(resolver_quality_python_materialization_gaps_are_counted);
    RUN_TEST(resolver_quality_counts_known_misses_and_forbids_wrong_targets);
}
