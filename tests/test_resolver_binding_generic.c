#include "cbm.h"
#include "lang_specs.h"
#include "resolve/binding.h"
#include "resolve/policy.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "store/store.h"
#include "sqlite3.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "resolver_quality.h"

#include <stdlib.h>
#include <string.h>

typedef struct {
    bool occurrence, source_audit, mislabeled;
} dart_presence_t;

static bool dart_presence_observation(const cbm_resolve_proof_t *proof, void *context) {
    dart_presence_t *seen = context;
    const cbm_resolve_proof_view_t *view = cbm_resolve_proof_view(proof);
    if (!view->key.lookup_domain || !view->key.detail)
        return true;
    bool occurrence = !strcmp(view->key.lookup_domain, "source_occurrences") &&
                      !strcmp(view->key.detail, "exact_occurrence_observed");
    bool audit = !strcmp(view->key.lookup_domain, "source_binding_audit") &&
                 view->outcome.analysis == CBM_RESOLVE_ANALYSIS_COMPLETE;
    seen->occurrence |= occurrence;
    seen->source_audit |= audit;
    if (occurrence || audit)
        seen->mislabeled |=
            view->outcome.binding != CBM_RESOLVE_BINDING_UNIQUE || !view->outcome.target;
    return true;
}

typedef struct {
    bool absent, invalid;
} binding_absence_t;

static bool binding_absence_observation(const cbm_resolve_proof_t *proof, void *context) {
    binding_absence_t *seen = context;
    const cbm_resolve_proof_view_t *view = cbm_resolve_proof_view(proof);
    if (!view->key.lookup_domain || strcmp(view->key.lookup_domain, "source_occurrences"))
        return true;
    seen->absent = true;
    seen->invalid |= !view->key.detail || strcmp(view->key.detail, "exact_occurrence_absent") ||
                     !view->key.lookup_key || strcmp(view->key.lookup_key, "target") ||
                     !view->key.lookup_closed ||
                     view->outcome.analysis != CBM_RESOLVE_ANALYSIS_COMPLETE ||
                     view->outcome.binding != CBM_RESOLVE_BINDING_UNRESOLVED ||
                     view->outcome.target || view->outcome.candidate_count;
    return true;
}

/* Imports and declaration names create bindings, not source occurrences.
 * An empty occurrence index must still retain closed negative observations. */
TEST(resolver_binding_python_empty_occurrences_preserve_negative_evidence) {
    const char *source = "import requests\n\ndef target():\n    pass\n";
    CBMFileResult *file = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON, "test",
                                           "main.py", 0, NULL, NULL);
    ASSERT_NOT_NULL(file);
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_binding_index_t *index =
        cbm_resolve_binding_build(&arena, file, CBM_LANG_PYTHON, source, strlen(source));
    ASSERT_STR_EQ(cbm_resolve_binding_status(index), "binding_complete_supported_subset");
    ASSERT(cbm_resolve_binding_name_present(index, "target"));
    ASSERT(cbm_resolve_binding_import(index, "requests"));
    const char *declaration = strstr(source, "target");
    ASSERT_NOT_NULL(declaration);
    uint32_t start = (uint32_t)(declaration - source), end = start + 6;
    cbm_resolve_result_t candidate = {.binding = CBM_RESOLVE_BINDING_UNIQUE,
                                      .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                      .rule = CBM_RESOLVE_RULE_SCOPE_BINDING,
                                      .facts = CBM_RESOLVE_FACT_EXACT_TARGET,
                                      .target = "test.main.target",
                                      .candidate_count = 1};
    cbm_resolve_result_t results[] = {
        cbm_resolve_binding_local(index, "target", start, end, CBM_SOURCE_ORIGIN_RAW),
        cbm_resolve_binding_validate(index, "target", start, end, CBM_SOURCE_ORIGIN_RAW,
                                     candidate)};
    ASSERT_STR_EQ(results[1].reason, "binding_occurrence_missing");
    for (size_t i = 0; i < sizeof(results) / sizeof(results[0]); i++) {
        ASSERT(!cbm_resolve_admit(&results[i]));
        ASSERT_NOT_NULL(results[i].proof);
        binding_absence_t seen = {0};
        ASSERT(cbm_resolve_proof_visit(results[i].proof, 128, binding_absence_observation, &seen));
        ASSERT(seen.absent && !seen.invalid);
    }
    CBMCall forged = {.callee_name = "requests.get",
                      .enclosing_func_qn = "test.main.target",
                      .first_string_arg = "https://example.test",
                      .source_origin = CBM_SOURCE_ORIGIN_RAW,
                      .site_start_byte = start,
                      .site_end_byte = end};
    ASSERT(!cbm_resolve_binding_import_occurrence(index, &forged, "requests"));
    cbm_arena_destroy(&arena);
    cbm_free_result(file);
    PASS();
}

TEST(resolver_binding_dart_source_namespace_and_privacy) {
    const char *source = "int target() => 1;\nint _private() => 2;\n"
                         "void caller() { target(); }\n";
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_DART, "test",
                                             "main.dart", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_binding_index_t *index =
        cbm_resolve_binding_build(&arena, result, CBM_LANG_DART, source, strlen(source));
    ASSERT_STR_EQ(cbm_resolve_binding_status(index), "binding_complete_supported_subset");
    ASSERT(cbm_resolve_binding_namespace_complete(index));
    ASSERT_EQ(cbm_resolve_binding_export_kind(index, "test.main.target"), CBM_RESOLVE_EXPORT_VALUE);
    ASSERT_EQ(cbm_resolve_binding_export_kind(index, "test.main._private"), 0);
    const char *call = strstr(source, "target();");
    ASSERT_NOT_NULL(call);
    cbm_resolve_result_t candidate = {.binding = CBM_RESOLVE_BINDING_UNIQUE,
                                      .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                      .rule = CBM_RESOLVE_RULE_SCOPE_BINDING,
                                      .facts = CBM_RESOLVE_FACT_EXACT_TARGET,
                                      .target = "test.main.target",
                                      .candidate_count = 1};
    uint32_t start = (uint32_t)(call - source);
    cbm_resolve_result_t proof = cbm_resolve_binding_validate(index, "target", start, start + 8,
                                                              CBM_SOURCE_ORIGIN_RAW, candidate);
    ASSERT(cbm_resolve_admit(&proof));
    dart_presence_t seen = {0};
    ASSERT(cbm_resolve_proof_visit(proof.proof, 128, dart_presence_observation, &seen));
    ASSERT(seen.occurrence);
    ASSERT(seen.source_audit);
    ASSERT(!seen.mislabeled);
    cbm_arena_destroy(&arena);
    cbm_free_result(result);
    PASS();
}

TEST(resolver_binding_dart_import_source_combinators) {
    const char *source = "import 'lib.dart' show target hide other;\n"
                         "import 'tools.dart' as tools;\nvoid caller() { target(); }\n";
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_DART, "test",
                                             "main.dart", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_binding_index_t *index =
        cbm_resolve_binding_build(&arena, result, CBM_LANG_DART, source, strlen(source));
    ASSERT_STR_EQ(cbm_resolve_binding_status(index), "binding_complete_supported_subset");
    ASSERT_EQ(cbm_resolve_binding_import_count(index), 2);
    const cbm_resolve_source_import_t *first = cbm_resolve_binding_import_at(index, 0);
    const cbm_resolve_source_import_t *second = cbm_resolve_binding_import_at(index, 1);
    ASSERT_NOT_NULL(first);
    ASSERT_NOT_NULL(second);
    ASSERT_STR_EQ(first->uri, "lib.dart");
    ASSERT(first->has_show);
    ASSERT_EQ(first->show_count, 1);
    ASSERT_STR_EQ(first->show[0], "target");
    ASSERT_EQ(first->hide_count, 1);
    ASSERT_STR_EQ(first->hide[0], "other");
    ASSERT_STR_EQ(second->prefix, "tools");
    cbm_arena_destroy(&arena);
    cbm_free_result(result);
    PASS();
}

/* A complete URI in one declaration is not proof of another occurrence. */
TEST(resolver_binding_dart_duplicate_uri_preserves_occurrence_authority) {
    const char *suffixes[] = {"import 'lib.dart' if (dart.library.io) 'other.dart';\n",
                              "import 'lib.dart' deferred as delayed;\n",
                              "import '\\u006cib.dart';\n"};
    for (size_t i = 0; i < sizeof(suffixes) / sizeof(suffixes[0]); i++) {
        char source[256];
        snprintf(source, sizeof(source), "import 'lib.dart';\n%s", suffixes[i]);
        CBMFileResult *file = cbm_extract_file(source, (int)strlen(source), CBM_LANG_DART, "test",
                                               "main.dart", 0, NULL, NULL);
        ASSERT_NOT_NULL(file);
        ASSERT_EQ(file->imports.count, 2);
        ASSERT_EQ(file->imports.items[0].kind, CBM_IMPORT_KIND_LITERAL_RELATIVE_PATH);
        ASSERT_EQ(file->imports.items[1].kind, CBM_IMPORT_KIND_DEFAULT);
        ASSERT(file->imports.items[0].site_end_byte < file->imports.items[1].site_start_byte);
        CBMArena arena;
        cbm_arena_init(&arena);
        cbm_resolve_binding_index_t *index =
            cbm_resolve_binding_build(&arena, file, CBM_LANG_DART, source, strlen(source));
        ASSERT(!cbm_resolve_binding_namespace_complete(index));
        /* An incomplete library namespace exposes no binding import list,
         * including an individually literal declaration before the blocker. */
        ASSERT_EQ(cbm_resolve_binding_import_count(index), 0);
        ASSERT_NULL(cbm_resolve_binding_import_at(index, 0));
        cbm_arena_destroy(&arena);
        cbm_free_result(file);
    }
    /* Supported duplicate URI occurrences retain distinct source authority. */
    const char *source = "import 'lib.dart' as first;\nimport 'lib.dart' as second;\n";
    CBMFileResult *file = cbm_extract_file(source, (int)strlen(source), CBM_LANG_DART, "test",
                                           "main.dart", 0, NULL, NULL);
    ASSERT_NOT_NULL(file);
    ASSERT_EQ(file->imports.count, 2);
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_binding_index_t *index =
        cbm_resolve_binding_build(&arena, file, CBM_LANG_DART, source, strlen(source));
    ASSERT(cbm_resolve_binding_namespace_complete(index));
    ASSERT_EQ(cbm_resolve_binding_import_count(index), 2);
    for (size_t i = 0; i < 2; i++) {
        const cbm_resolve_source_import_t *import = cbm_resolve_binding_import_at(index, i);
        ASSERT_NOT_NULL(import);
        ASSERT_STR_EQ(import->uri, "lib.dart");
        ASSERT_STR_EQ(import->prefix, i ? "second" : "first");
        ASSERT_EQ(import->uri_start_byte, file->imports.items[i].site_start_byte);
        ASSERT_EQ(import->uri_end_byte, file->imports.items[i].site_end_byte);
        ASSERT_EQ(file->imports.items[i].kind, CBM_IMPORT_KIND_LITERAL_RELATIVE_PATH);
    }
    ASSERT(file->imports.items[0].site_end_byte < file->imports.items[1].site_start_byte);
    cbm_arena_destroy(&arena);
    cbm_free_result(file);
    PASS();
}

TEST(resolver_binding_generic_unmodeled_library_context_is_not_complete) {
    const char *dart = "part 'other.dart';\nvoid caller() {}\n";
    CBMFileResult *result = cbm_extract_file(dart, (int)strlen(dart), CBM_LANG_DART, "test",
                                             "main.dart", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_binding_index_t *index =
        cbm_resolve_binding_build(&arena, result, CBM_LANG_DART, dart, strlen(dart));
    ASSERT(!cbm_resolve_binding_namespace_complete(index));
    ASSERT_STR_EQ(cbm_resolve_binding_status(index), "dart_library_namespace_expansion_required");
    /* Swift's module membership is now read from SwiftPM targets by its adapter,
     * which decides per occurrence; this index only attests those sites. */
    cbm_resolve_binding_index_t *swift =
        cbm_resolve_binding_build(&arena, NULL, CBM_LANG_SWIFT, "", 0);
    ASSERT_STR_EQ(cbm_resolve_binding_status(swift), "source_adapter_binding");
    cbm_arena_destroy(&arena);
    cbm_free_result(result);
    PASS();
}

TEST(resolver_binding_rust_exact_names_and_blockers) {
    const char *sources[] = {"pub fn target() {} fn caller() { target(); }",
                             "fn caller() { target(); } pub fn target() {}",
                             "use crate::service::target; fn caller() { target(); }",
                             "fn target() {} fn caller(target: fn()) { target(); }",
                             "fn target() {} fn caller() { let target = unknown; target(); }",
                             "fn target() {} fn caller() { unknown!(); target(); }"};
    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++) {
        const char *source = sources[i];
        CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_RUST, "test",
                                                 "main.rs", 0, NULL, NULL);
        ASSERT_NOT_NULL(result);
        CBMArena arena;
        cbm_arena_init(&arena);
        cbm_resolve_binding_index_t *index =
            cbm_resolve_binding_build(&arena, result, CBM_LANG_RUST, source, strlen(source));
        const char *call = strstr(source, "target();");
        ASSERT_NOT_NULL(call);
        cbm_resolve_result_t candidate = {
            .binding = CBM_RESOLVE_BINDING_UNIQUE,
            .domain = CBM_RESOLVE_DOMAIN_PROJECT,
            .rule = i == 2 ? CBM_RESOLVE_RULE_IMPORT_BINDING : CBM_RESOLVE_RULE_SCOPE_BINDING,
            .facts = CBM_RESOLVE_FACT_EXACT_TARGET | CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                     CBM_RESOLVE_FACT_IMPORT_BOUND,
            .target = i == 2 ? "test.service.target" : "test.main.target",
            .candidate_count = 1};
        uint32_t start = (uint32_t)(call - source);
        cbm_resolve_result_t proof = cbm_resolve_binding_validate(index, "target", start, start + 8,
                                                                  CBM_SOURCE_ORIGIN_RAW, candidate);
        bool admitted = cbm_resolve_admit(&proof);
        unsigned exported = cbm_resolve_binding_export_kind(index, "test.main.target");
        cbm_arena_destroy(&arena);
        cbm_free_result(result);
        ASSERT_EQ(admitted, i < 3);
        ASSERT_EQ(exported != 0, i < 2);
    }
    PASS();
}

#define GENERIC_PROJECT "resolver-generic"

typedef struct {
    const char *source;
    const char *other;
    const char *target;
    bool admitted;
} dart_golden_t;

static int generic_count(cbm_store_t *store, const char *sql) {
    sqlite3_stmt *statement = NULL;
    if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &statement, NULL) != SQLITE_OK)
        return -1;
    int count = sqlite3_step(statement) == SQLITE_ROW ? sqlite3_column_int(statement, 0) : -1;
    sqlite3_finalize(statement);
    return count;
}

static int generic_edges(cbm_store_t *store, const char *target, const char *relation) {
    sqlite3_stmt *statement = NULL;
    const char *sql =
        "SELECT count(*) FROM edges e JOIN nodes s ON e.source_id=s.id "
        "JOIN nodes t ON e.target_id=t.id WHERE e.project='resolver-generic' "
        "AND s.qualified_name='resolver-generic.main.caller' AND t.qualified_name=?1 AND e.type=?2";
    if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &statement, NULL) != SQLITE_OK)
        return -1;
    bool bound = sqlite3_bind_text(statement, 1, target, -1, SQLITE_STATIC) == SQLITE_OK &&
                 sqlite3_bind_text(statement, 2, relation, -1, SQLITE_STATIC) == SQLITE_OK;
    int count =
        bound && sqlite3_step(statement) == SQLITE_ROW ? sqlite3_column_int(statement, 0) : -1;
    sqlite3_finalize(statement);
    return count;
}

/* Real extraction, import resolution and shared namespace indexing. Each
 * fixture has a same-name unrelated declaration and one labelled call site. */
static bool dart_pipeline_observations(const dart_golden_t *golden, int padding,
                                       const char *assertion_sql) {
    const char *tmp = th_mktempdir("cbm_dart_binding");
    if (!tmp)
        return false;
    char root[512], repo[768], database[768];
    snprintf(root, sizeof(root), "%s", tmp);
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    snprintf(database, sizeof(database), "%s/index.db", tmp);
    bool ok =
        cbm_mkdir_p(repo, 0755) && th_write_file(TH_PATH(repo, "main.dart"), golden->source) == 0 &&
        th_write_file(TH_PATH(repo, "lib.dart"), "int target() => 1;\nint _target() => 1;\n") ==
            0 &&
        th_write_file(TH_PATH(repo, "decoy.dart"), "int target() => 99;\nint _target() => 99;\n") ==
            0;
    if (ok && golden->other)
        ok = th_write_file(TH_PATH(repo, "other.dart"), golden->other) == 0;
    for (int i = 0; ok && i < padding; i++) {
        char name[64], contents[96];
        snprintf(name, sizeof(name), "padding_%03d.dart", i);
        snprintf(contents, sizeof(contents), "int padding_%03d() => %d;\n", i, i);
        ok = th_write_file(TH_PATH(repo, name), contents) == 0;
    }
    cbm_pipeline_t *pipeline = ok ? cbm_pipeline_new(repo, database, CBM_MODE_FULL) : NULL;
    ok = pipeline && cbm_pipeline_set_project_name(pipeline, GENERIC_PROJECT) &&
         cbm_pipeline_run(pipeline) == 0;
    cbm_pipeline_free(pipeline);
    cbm_store_t *store = ok ? cbm_store_open_path_existing(database) : NULL;
    if (!store)
        ok = false;
    if (store) {
        ok = generic_edges(store, golden->target, "CALLS") == (golden->admitted ? 1 : 0) &&
             generic_edges(store, GENERIC_PROJECT ".decoy.target", "CALLS") == 0 &&
             generic_edges(store, GENERIC_PROJECT ".other.target", "CALLS") == 0 &&
             generic_count(
                 store, "SELECT count(*) FROM resolver_occurrences WHERE "
                        "project='resolver-generic' AND file_path='main.dart' AND relation='CALLS' "
                        "AND scope='resolver-generic.main.caller'") == 1;
        if (!golden->admitted)
            ok = ok &&
                 generic_count(
                     store,
                     "SELECT count(*) FROM resolver_occurrences WHERE "
                     "project='resolver-generic' AND file_path='main.dart' AND relation='CALLS' "
                     "AND scope='resolver-generic.main.caller' AND emitted=0") == 1;
        if (assertion_sql)
            ok = ok && generic_count(store, assertion_sql) == 1;
        cbm_store_close(store);
    }
    th_rmtree(root);
    return ok;
}

static bool dart_pipeline_golden(const dart_golden_t *golden, int padding) {
    return dart_pipeline_observations(golden, padding, NULL);
}

/* Reachability makes these persisted facts actual occurrence dependencies,
 * rather than unrelated lookup rows elsewhere in the indexed project. */
#define DART_REACHABLE_LOOKUPS                                                           \
    "WITH RECURSIVE reachable(fact) AS ("                                                \
    "SELECT proof FROM resolver_occurrence_evidence WHERE project='resolver-generic' "   \
    "UNION SELECT s.premise FROM resolver_supports s JOIN reachable r ON s.fact=r.fact " \
    "WHERE s.project='resolver-generic') "                                               \
    "SELECT count(*)>0 FROM resolver_lookups l JOIN resolver_facts f "                   \
    "ON f.project=l.project AND f.id=l.fact JOIN reachable r ON r.fact=l.fact "          \
    "WHERE l.project='resolver-generic' "

TEST(resolver_binding_dart_closed_missing_member_has_imported_source_proof) {
    const dart_golden_t fixture = {
        "import 'lib.dart';\nimport 'other.dart';\nvoid caller() { target(); }\n",
        "int unrelated() => 2;\n", GENERIC_PROJECT ".lib.target", true};
    ASSERT(dart_pipeline_observations(&fixture, 0,
                                      DART_REACHABLE_LOOKUPS
                                      "AND f.source_file='other.dart' "
                                      "AND l.domain='resolver-generic.other' AND l.key='target' "
                                      "AND l.status='closed_negative'"));
    PASS();
}

TEST(resolver_binding_dart_visibility_excludes_unrelated_open_import) {
    const dart_golden_t fixture = {
        "import 'lib.dart';\nimport 'missing.dart' as excluded;\n"
        "import 'decoy.dart' hide target;\nvoid caller() { target(); }\n",
        NULL, GENERIC_PROJECT ".lib.target", true};
    ASSERT(dart_pipeline_observations(&fixture, 0,
                                      DART_REACHABLE_LOOKUPS
                                      "AND f.source_file='main.dart' "
                                      "AND l.domain='dart.import_visibility' "
                                      "AND l.key='1:missing.dart:target' AND l.status='positive'"));
    PASS();
}

TEST(resolver_binding_dart_unsupported_competitor_keeps_source_observation) {
    const dart_golden_t fixture = {
        "import 'lib.dart';\nimport 'other.dart';\nvoid caller() { target(); }\n",
        "var target = 2;\n", GENERIC_PROJECT ".lib.target", false};
    ASSERT(dart_pipeline_observations(&fixture, 0,
                                      DART_REACHABLE_LOOKUPS
                                      "AND f.source_file='other.dart' "
                                      "AND l.domain='resolver-generic.other' AND l.key='target' "
                                      "AND l.status='positive'"));
    PASS();
}

TEST(resolver_binding_dart_cross_file_exact_namespace_and_decoy) {
    const dart_golden_t fixtures[] = {
        {"import 'lib.dart';\nvoid caller() { target(); }\n", NULL, GENERIC_PROJECT ".lib.target",
         true},
        {("import 'lib.dart' show target;\nimport 'decoy.dart' hide target;\n"
          "void caller() { target(); }\n"),
         NULL, GENERIC_PROJECT ".lib.target", true},
        {"import 'lib.dart';\nimport 'lib.dart';\nvoid caller() { target(); }\n", NULL,
         GENERIC_PROJECT ".lib.target", true},
        {"import 'lib.dart';\nimport 'dart:core';\nvoid caller() { target(); }\n", NULL,
         GENERIC_PROJECT ".lib.target", true}};
    for (size_t i = 0; i < sizeof(fixtures) / sizeof(fixtures[0]); i++)
        ASSERT(dart_pipeline_golden(&fixtures[i], 0));
    PASS();
}

TEST(resolver_binding_dart_cross_file_parallel_namespace) {
    const dart_golden_t fixture = {"import 'lib.dart';\nvoid caller() { target(); }\n", NULL,
                                   GENERIC_PROJECT ".lib.target", true};
    ASSERT(dart_pipeline_golden(&fixture, 50));
    PASS();
}

TEST(resolver_binding_dart_cross_file_conflicts_and_missing_namespace) {
    const dart_golden_t fixtures[] = {
        {"import 'lib.dart';\nimport 'other.dart';\nvoid caller() { target(); }\n",
         "int target() => 2;\n", GENERIC_PROJECT ".lib.target", false},
        /* This pinned an abstention. Two imports providing one name are an error at its
         * use, so in code that compiles missing.dart, likely generated, cannot provide
         * target, and lib.dart's is the one selected. */
        {"import 'lib.dart';\nimport 'missing.dart';\nvoid caller() { target(); }\n", NULL,
         GENERIC_PROJECT ".lib.target", true},
        {"import 'lib.dart';\nimport 'other.dart';\nvoid caller() { target(); }\n",
         "var target = 2;\n", GENERIC_PROJECT ".lib.target", false},
        {"import 'lib.dart';\nvoid caller() { _target(); }\n", NULL, GENERIC_PROJECT ".lib._target",
         false},
        {"import 'lib.dart';\nimport 'lib.dart' if (dart.library.io) 'other.dart';\n"
         "void caller() { target(); }\n",
         "int target() => 2;\n", GENERIC_PROJECT ".lib.target", false}};
    for (size_t i = 0; i < sizeof(fixtures) / sizeof(fixtures[0]); i++)
        ASSERT(dart_pipeline_golden(&fixtures[i], 0));
    PASS();
}

TEST(resolver_binding_dart_cross_file_shadows_and_reassignment) {
    const dart_golden_t fixtures[] = {
        {"import 'lib.dart';\nvoid caller(Function target) { target(); }\n", NULL,
         GENERIC_PROJECT ".lib.target", false},
        {"import 'lib.dart';\nvoid caller() { var target = 2; target(); }\n", NULL,
         GENERIC_PROJECT ".lib.target", false},
        {"import 'lib.dart';\nvoid caller() { target = 2; target(); }\n", NULL,
         GENERIC_PROJECT ".lib.target", false}};
    for (size_t i = 0; i < sizeof(fixtures) / sizeof(fixtures[0]); i++)
        ASSERT(dart_pipeline_golden(&fixtures[i], 0));
    PASS();
}

TEST(resolver_binding_dart_declaration_ranges_include_exact_body) {
    const char *source = "void helper() {\n  print('helper');\n}\n\n"
                         "void run() {\n  helper();\n}\n"
                         "external void externalOnly();\n"
                         "int next() =>\n  1;\n"
                         "void commented()\n/* between signature and body */\n{\n  helper();\n}\n"
                         "class Holder {\n  void method() {\n    helper();\n  }\n}\n";
    const struct {
        const char *name, *label;
        uint32_t start, end;
    } expected[] = {{"helper", "Function", 1, 3},       {"run", "Function", 5, 7},
                    {"externalOnly", "Function", 8, 8}, {"next", "Function", 9, 10},
                    {"commented", "Function", 11, 15},  {"method", "Method", 17, 19}};
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_DART,
                                             "dart-ranges", "main.dart", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    bool parsed = !result->has_error && !result->parse_incomplete && !result->parse_unusable;
    unsigned missing = 0, wrong = 0;
    for (size_t i = 0; i < sizeof(expected) / sizeof(expected[0]); i++) {
        unsigned found = 0;
        for (int j = 0; j < result->defs.count; j++) {
            const CBMDefinition *def = result->defs.items + j;
            if (strcmp(def->name, expected[i].name) || strcmp(def->label, expected[i].label))
                continue;
            found++;
            if (def->start_line != expected[i].start || def->end_line != expected[i].end) {
                fprintf(stderr, "[DART-RANGE] %s actual=%u-%u expected=%u-%u\n", def->name,
                        def->start_line, def->end_line, expected[i].start, expected[i].end);
                wrong++;
            }
        }
        missing += found != 1;
    }
    cbm_free_result(result);
    ASSERT(parsed);
    ASSERT_EQ(missing, 0);
    ASSERT_EQ(wrong, 0);
    PASS();
}

TEST(resolver_binding_ecmascript_parameters_have_exact_lexical_owners) {
    static const struct {
        const char *body;
        bool admitted;
        bool script;
    } cases[] = {
        {"export function caller() { accept(handler); }\n"
         "function unrelated(handler) { return 0; }\n",
         true, false},
        {"export function caller(handler) { accept(handler); }\n", false, false},
        {"export function outer() { function inner() { accept(handler); } }\n"
         "function unrelated(handler) { return 0; }\n",
         true, false},
        {"export function outer(handler) { function inner() { accept(handler); } }\n", false,
         false},
        {"export function caller() { function inner(handler) { return 0; } accept(handler); }\n",
         true, false},
        {"export const caller = (handler) => accept(handler);\n", false, false},
        {"export const caller = handler => accept(handler);\n", false, false},
        {"export function caller() { accept(handler); }\n"
         "const unrelated = handler => 0;\n",
         true, false},
        {"export function caller() { accept(handler); }\n"
         "function unrelated(value = handler) { return 0; }\n",
         false, false},
        {"export function caller(handler = other) { accept(handler); }\n", false, false},
        {"export function caller() { accept(handler); }\n"
         "function unrelated(value = (handler = other)) { return 0; }\n",
         false, false},
        {"export function caller() { accept(handler); }\n"
         "function unrelated({handler}) { return 0; }\n",
         false, false},
        {"export function caller() { accept(handler); }\n"
         "function unrelated(...handler) { return 0; }\n",
         false, false},
        {"export function caller() { accept(handler); var handler = other; }\n", false, false},
        {"export function caller() { accept(handler); let handler = other; }\n", false, false},
        {"export function caller() { accept(handler); }\n"
         "function mutate() { handler = other; }\n",
         false, false},
        {"export function caller() { accept(handler); }\n"
         "function mutate() { handler++; }\n",
         false, false},
        {"import { handler } from './other';\n"
         "export function caller() { accept(handler); }\n",
         false, false},
        {"function caller() { accept(handler); }\n", false, true},
    };
    const CBMLanguage languages[] = {CBM_LANG_JAVASCRIPT, CBM_LANG_TYPESCRIPT, CBM_LANG_TSX,
                                     CBM_LANG_ARKTS};
    for (size_t language = 0; language < sizeof(languages) / sizeof(languages[0]); language++) {
        for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
            char source[768];
            snprintf(source, sizeof(source), "%sfunction accept(callback) {}\n%s",
                     cases[i].script ? "function handler() {}\n"
                                     : "import { handler } from './target';\n",
                     cases[i].body);
            CBMFileResult *file = cbm_extract_file(source, (int)strlen(source), languages[language],
                                                   "scoped", "main.ts", 0, NULL, NULL);
            ASSERT_NOT_NULL(file);
            ASSERT(!file->has_error && !file->parse_incomplete && !file->parse_unusable);
            CBMArena arena;
            cbm_arena_init(&arena);
            cbm_resolve_binding_index_t *index = cbm_resolve_binding_build(
                &arena, file, languages[language], source, strlen(source));
            const char *call = strstr(source, "accept(handler)");
            ASSERT_NOT_NULL(call);
            uint32_t start = (uint32_t)(call + strlen("accept(") - source);
            cbm_resolve_result_t candidate = {
                .binding = CBM_RESOLVE_BINDING_UNIQUE,
                .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                .rule = cases[i].script ? CBM_RESOLVE_RULE_SCOPE_BINDING
                                        : CBM_RESOLVE_RULE_IMPORT_BINDING,
                .facts = CBM_RESOLVE_FACT_EXACT_TARGET | CBM_RESOLVE_FACT_IMPORT_BOUND |
                         CBM_RESOLVE_FACT_LOOKUP_COMPLETE,
                .target = cases[i].script ? "scoped.main.handler" : "scoped.target.handler",
                .candidate_count = 1};
            cbm_resolve_result_t proof =
                cbm_resolve_binding_validate(index, "handler", start, start + strlen("handler"),
                                             CBM_SOURCE_ORIGIN_RAW, candidate);
            bool exact = cbm_resolve_admit(&proof) && proof.proof && proof.target &&
                         !strcmp(proof.target, candidate.target);
            cbm_resolve_result_t shifted =
                cbm_resolve_binding_validate(index, "handler", start + 1, start + strlen("handler"),
                                             CBM_SOURCE_ORIGIN_RAW, candidate);
            bool forged = cbm_resolve_admit(&shifted);
            if (exact != cases[i].admitted || forged)
                fprintf(stderr,
                        "ECMAScript parameter scope language=%zu case=%zu "
                        "status=%s reason=%s exact=%d forged=%d\n",
                        language, i, cbm_resolve_binding_status(index), proof.reason, exact,
                        forged);
            cbm_arena_destroy(&arena);
            cbm_free_result(file);
            ASSERT_EQ(exact, cases[i].admitted);
            ASSERT(!forged);
        }
    }
    PASS();
}

TEST(resolver_binding_typescript_parameter_annotations_preserve_import_identity) {
    const char *source = "import { handler } from './target';\n"
                         "function accept(callback: () => void): void {}\n"
                         "export function caller(): void { accept(handler); }\n"
                         "export function shadowed(handler: number): void { accept(handler); }\n";
    CBMFileResult *file = cbm_extract_file(source, (int)strlen(source), CBM_LANG_TYPESCRIPT,
                                           "scoped", "main.ts", 0, NULL, NULL);
    ASSERT_NOT_NULL(file);
    ASSERT(!file->has_error && !file->parse_incomplete && !file->parse_unusable);
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_binding_index_t *index =
        cbm_resolve_binding_build(&arena, file, CBM_LANG_TYPESCRIPT, source, strlen(source));
    cbm_resolve_result_t candidate = {.binding = CBM_RESOLVE_BINDING_UNIQUE,
                                      .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                      .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                      .rule = CBM_RESOLVE_RULE_IMPORT_BINDING,
                                      .facts = CBM_RESOLVE_FACT_EXACT_TARGET |
                                               CBM_RESOLVE_FACT_IMPORT_BOUND |
                                               CBM_RESOLVE_FACT_LOOKUP_COMPLETE,
                                      .target = "scoped.target.handler",
                                      .candidate_count = 1};
    const char *first = strstr(source, "accept(handler)");
    ASSERT_NOT_NULL(first);
    const char *second = strstr(first + 1, "accept(handler)");
    ASSERT_NOT_NULL(second);
    uint32_t first_start = (uint32_t)(first + strlen("accept(") - source);
    uint32_t second_start = (uint32_t)(second + strlen("accept(") - source);
    cbm_resolve_result_t positive = cbm_resolve_binding_validate(
        index, "handler", first_start, first_start + 7, CBM_SOURCE_ORIGIN_RAW, candidate);
    cbm_resolve_result_t negative = cbm_resolve_binding_validate(
        index, "handler", second_start, second_start + 7, CBM_SOURCE_ORIGIN_RAW, candidate);
    bool positive_ok = cbm_resolve_admit(&positive) && positive.proof && positive.target &&
                       !strcmp(positive.target, "scoped.target.handler");
    bool negative_ok = !cbm_resolve_admit(&negative);
    cbm_arena_destroy(&arena);
    cbm_free_result(file);
    ASSERT(positive_ok);
    ASSERT(negative_ok);
    PASS();
}

TEST(resolver_binding_http_import_occurrence_uses_lexical_owner) {
    const char *parameters[] = {"value", "client"};
    for (size_t i = 0; i < sizeof(parameters) / sizeof(parameters[0]); i++) {
        char source[512];
        snprintf(source, sizeof(source),
                 "import client from './client';\n"
                 "export function caller(%s) { return client.get('/items'); }\n"
                 "function unrelated(client) { return 0; }\n",
                 parameters[i]);
        CBMFileResult *file = cbm_extract_file(source, (int)strlen(source), CBM_LANG_TYPESCRIPT,
                                               "scoped", "main.ts", 0, NULL, NULL);
        ASSERT_NOT_NULL(file);
        ASSERT(!file->has_error && !file->parse_incomplete && !file->parse_unusable);
        CBMArena arena;
        cbm_arena_init(&arena);
        cbm_resolve_binding_index_t *index =
            cbm_resolve_binding_build(&arena, file, CBM_LANG_TYPESCRIPT, source, strlen(source));
        const CBMCall *site = NULL;
        for (int c = 0; c < file->calls.count; c++) {
            const CBMCall *call = &file->calls.items[c];
            if (call->callee_name && !strcmp(call->callee_name, "client.get")) {
                ASSERT(site == NULL);
                site = call;
            }
        }
        ASSERT_NOT_NULL(site);
        bool exact = cbm_resolve_binding_import_occurrence(index, site, "client");
        CBMCall shifted = *site;
        shifted.site_start_byte++;
        bool forged = cbm_resolve_binding_import_occurrence(index, &shifted, "client");
        cbm_arena_destroy(&arena);
        cbm_free_result(file);
        ASSERT_EQ(exact, i == 0);
        ASSERT(!forged);
    }
    PASS();
}

TEST(resolver_binding_python_parenthesized_arguments_keep_exact_identity) {
    static const struct {
        const char *expression;
        const char *parameters;
        const char *after;
        bool admitted;
    } cases[] = {
        {"(target)", "flag, box", "", true},
        {"((target))", "flag, box", "", true},
        {"( target )", "flag, box", "", true},
        {"(((target)))", "flag, box", "", true},
        {"(target,)", "flag, box", "", false},
        {"(target, other)", "flag, box", "", false},
        {"(target())", "flag, box", "", false},
        {"(target or other)", "flag, box", "", false},
        {"(target if flag else other)", "flag, box", "", false},
        {"(box.target)", "flag, box", "", false},
        {"(target)", "target", "", false},
        {"(target)", "flag, box", "    target = other\n", false},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char source[512];
        snprintf(source, sizeof(source),
                 "def target():\n    return 1\n"
                 "def other():\n    return 2\n"
                 "def accept(value):\n    return 0\n"
                 "def caller(%s):\n    accept(%s)\n%s",
                 cases[i].parameters, cases[i].expression, cases[i].after);
        CBMFileResult *file = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON,
                                               "wrapped", "main.py", 0, NULL, NULL);
        ASSERT_NOT_NULL(file);
        ASSERT(!file->has_error && !file->parse_incomplete && !file->parse_unusable);
        CBMArena arena;
        cbm_arena_init(&arena);
        cbm_resolve_binding_index_t *index =
            cbm_resolve_binding_build(&arena, file, CBM_LANG_PYTHON, source, strlen(source));
        const char *call = strstr(source, "    accept(");
        ASSERT_NOT_NULL(call);
        const char *site = call + strlen("    accept(");
        uint32_t start = (uint32_t)(site - source);
        uint32_t end = start + (uint32_t)strlen(cases[i].expression);
        cbm_resolve_result_t proof =
            cbm_resolve_binding_local(index, "target", start, end, CBM_SOURCE_ORIGIN_RAW);
        bool exact = cbm_resolve_admit(&proof) && proof.proof && proof.target &&
                     !strcmp(proof.target, "wrapped.main.target");
        dart_presence_t seen = {0};
        bool observations = !exact || (cbm_resolve_proof_visit(proof.proof, 128,
                                                               dart_presence_observation, &seen) &&
                                       seen.occurrence && seen.source_audit && !seen.mislabeled);
        /* A range containing the right spelling is insufficient: this range
         * drops only the opening parenthesis and is not an AST occurrence. */
        cbm_resolve_result_t shifted =
            cbm_resolve_binding_local(index, "target", start + 1, end, CBM_SOURCE_ORIGIN_RAW);
        cbm_resolve_result_t wrong_name =
            cbm_resolve_binding_local(index, "other", start, end, CBM_SOURCE_ORIGIN_RAW);
        cbm_resolve_result_t transformed =
            cbm_resolve_binding_local(index, "target", start, end, CBM_SOURCE_ORIGIN_PREPROCESSED);
        bool forged = cbm_resolve_admit(&shifted) || cbm_resolve_admit(&wrong_name) ||
                      cbm_resolve_admit(&transformed);
        bool leaf_preserved = true;
        if (cases[i].admitted) {
            const char *leaf = strstr(site, "target");
            uint32_t leaf_start = (uint32_t)(leaf - source);
            cbm_resolve_result_t leaf_proof = cbm_resolve_binding_local(
                index, "target", leaf_start, leaf_start + strlen("target"), CBM_SOURCE_ORIGIN_RAW);
            leaf_preserved = cbm_resolve_admit(&leaf_proof) && leaf_proof.proof &&
                             leaf_proof.target && !strcmp(leaf_proof.target, "wrapped.main.target");
        }
        if (exact != cases[i].admitted || !observations || forged || !leaf_preserved)
            fprintf(stderr,
                    "Python parenthesized argument case=%zu expression=%s status=%s "
                    "exact=%d observations=%d forged=%d leaf=%d\n",
                    i, cases[i].expression, cbm_resolve_binding_status(index), exact, observations,
                    forged, leaf_preserved);
        cbm_arena_destroy(&arena);
        cbm_free_result(file);
        ASSERT_EQ(exact, cases[i].admitted);
        ASSERT(observations);
        ASSERT(!forged);
        ASSERT(leaf_preserved);
    }
    PASS();
}

/* Ordinary Lua API spellings are not Python namespace operations. Exercise
 * declaration, parameter and attribute roles independently of the resolver's
 * own dynamic-name list and require the exact local target and export. */
TEST(resolver_binding_python_ordinary_lua_names_are_lexical) {
    const char *names[] = {"debug",      "load",    "rawset",  "loadfile", "dofile",
                           "loadstring", "setfenv", "getfenv", "_ENV",     "_G"};
    const char *roles[] = {"def %s():\n    return 2\n", "def unrelated(%s):\n    return 2\n",
                           "def unrelated(value):\n    return value.%s\n"};
    for (size_t name = 0; name < sizeof(names) / sizeof(names[0]); name++) {
        for (size_t role = 0; role < sizeof(roles) / sizeof(roles[0]); role++) {
            char extra[256], source[512];
            snprintf(extra, sizeof(extra), roles[role], names[name]);
            snprintf(source, sizeof(source),
                     "def target():\n    return 1\n%sdef caller():\n    return target()\n", extra);
            CBMFileResult *file = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON,
                                                   "isolation", "main.py", 0, NULL, NULL);
            ASSERT_NOT_NULL(file);
            ASSERT(!file->has_error && !file->parse_incomplete && !file->parse_unusable);
            CBMArena arena;
            cbm_arena_init(&arena);
            cbm_resolve_binding_index_t *index =
                cbm_resolve_binding_build(&arena, file, CBM_LANG_PYTHON, source, strlen(source));
            const char *site = strstr(source, "return target()");
            ASSERT_NOT_NULL(site);
            uint32_t start = (uint32_t)(site + strlen("return ") - source);
            cbm_resolve_result_t proof = cbm_resolve_binding_local(
                index, "target", start, start + strlen("target()"), CBM_SOURCE_ORIGIN_RAW);
            bool exact = cbm_resolve_admit(&proof) && proof.proof && proof.target &&
                         !strcmp(proof.target, "isolation.main.target");
            bool exported = cbm_resolve_binding_export(index, "isolation.main.target");
            if (!exact || !exported)
                fprintf(stderr,
                        "Python ordinary-name role=%zu name=%s status=%s exact=%d export=%d\n",
                        role, names[name], cbm_resolve_binding_status(index), exact, exported);
            cbm_arena_destroy(&arena);
            cbm_free_result(file);
            ASSERT(exact);
            ASSERT(exported);
        }
    }
    PASS();
}

TEST(resolver_binding_language_dynamic_controls_remain_blocked) {
    const char *python[] = {"exec('target = None')",          "eval('1')",
                            "globals()['target'] = None",     "locals()['target'] = None",
                            "vars()['target'] = None",        "setattr(value, 'target', None)",
                            "delattr(value, 'target')",       "__import__('unknown')",
                            "value.__dict__['target'] = None"};
    const char *lua[] = {"debug.getregistry()",
                         "load('return nil')",
                         "rawset(_G, 'target', nil)",
                         "loadfile('unknown.lua')",
                         "dofile('unknown.lua')",
                         "loadstring('return nil')",
                         "setfenv(1, {})",
                         "getfenv(1)",
                         "_ENV.target = nil",
                         "_G.target = nil"};
    for (int language = 0; language < 2; language++) {
        const char *const *effects = language ? lua : python;
        size_t count = language ? sizeof(lua) / sizeof(lua[0]) : sizeof(python) / sizeof(python[0]);
        for (size_t i = 0; i < count; i++) {
            char source[512];
            if (language)
                snprintf(source, sizeof(source),
                         "local function target() return 1 end\n%s\n"
                         "local function caller() return target() end\n",
                         effects[i]);
            else
                snprintf(source, sizeof(source),
                         "def target():\n    return 1\n%s\ndef caller():\n    return target()\n",
                         effects[i]);
            CBMLanguage kind = language ? CBM_LANG_LUA : CBM_LANG_PYTHON;
            CBMFileResult *file =
                cbm_extract_file(source, (int)strlen(source), kind, "isolation",
                                 language ? "main.lua" : "main.py", 0, NULL, NULL);
            ASSERT_NOT_NULL(file);
            ASSERT(!file->has_error && !file->parse_incomplete && !file->parse_unusable);
            CBMArena arena;
            cbm_arena_init(&arena);
            cbm_resolve_binding_index_t *index =
                cbm_resolve_binding_build(&arena, file, kind, source, strlen(source));
            bool blocked = !strcmp(cbm_resolve_binding_status(index), "dynamic_namespace_effect");
            const char *site = strstr(source, "return target()");
            ASSERT_NOT_NULL(site);
            uint32_t start = (uint32_t)(site + strlen("return ") - source);
            cbm_resolve_result_t candidate = {.binding = CBM_RESOLVE_BINDING_UNIQUE,
                                              .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                              .rule = CBM_RESOLVE_RULE_SCOPE_BINDING,
                                              .facts = CBM_RESOLVE_FACT_EXACT_TARGET,
                                              .target = "isolation.main.target",
                                              .candidate_count = 1};
            cbm_resolve_result_t proof =
                cbm_resolve_binding_validate(index, "target", start, start + strlen("target()"),
                                             CBM_SOURCE_ORIGIN_RAW, candidate);
            bool admitted = cbm_resolve_admit(&proof);
            if (!blocked || admitted)
                fprintf(stderr, "Dynamic control language=%d case=%zu status=%s admitted=%d\n",
                        language, i, cbm_resolve_binding_status(index), admitted);
            cbm_arena_destroy(&arena);
            cbm_free_result(file);
            ASSERT(blocked);
            ASSERT(!admitted);
        }
    }
    PASS();
}

static bool python_dynamic_name_golden(const char *library, bool admitted, int driver) {
    static const char main_source[] =
        "from library import target\n\ndef caller():\n    return target()\n";
    static const char decoy_source[] = "def target():\n    return 99\n";
    const RQ_Source sources[] = {{.path = "main.py", .text = main_source},
                                 {.path = "library.py", .text = library},
                                 {.path = "decoy.py", .text = decoy_source}};
    static const RQ_Expected expected[] = {
        {.case_id = "crossfile-exact-function",
         .site = {.file = "main.py", .caller = "caller", .needle = "target()"},
         .relation = "CALLS",
         .target = {.file = "library.py",
                    .suffix = "library.target",
                    .label = "Function",
                    .declaration_line = 1,
                    .declaration_text = "def target():"}},
        {.case_id = "local-exact-function",
         .site =
             {.file = "library.py", .caller = "local", .needle = "target()", .occurrence_index = 1},
         .relation = "CALLS",
         .target = {.file = "library.py",
                    .suffix = "library.target",
                    .label = "Function",
                    .declaration_line = 1,
                    .declaration_text = "def target():"}}};
    static const RQ_Forbidden forbidden[] = {
        {.case_id = "dynamic-or-rebound-import",
         .site = {.file = "main.py", .caller = "caller", .needle = "target()"},
         .relation = "CALLS",
         .any_target = true},
        {.case_id = "dynamic-or-rebound-local",
         .site =
             {.file = "library.py", .caller = "local", .needle = "target()", .occurrence_index = 1},
         .relation = "CALLS",
         .any_target = true}};
    const char *root = th_mktempdir("cbm_python_names");
    if (!root)
        return false;
    char repo[768], database[768];
    snprintf(repo, sizeof(repo), "%s/repo", root);
    snprintf(database, sizeof(database), "%s/index.db", root);
    bool ok = cbm_mkdir_p(repo, 0755);
    for (size_t i = 0; ok && i < sizeof(sources) / sizeof(sources[0]); i++)
        ok = th_write_file(TH_PATH(repo, sources[i].path), sources[i].text) == 0;
    for (int i = 0; ok && i < 55; i++) {
        char path[32];
        snprintf(path, sizeof(path), "filler%02d.py", i);
        ok = th_write_file(TH_PATH(repo, path), "# unrelated source\n") == 0;
    }
    const char *old_workers = getenv("CBM_WORKERS");
    char *saved_workers = old_workers ? strdup(old_workers) : NULL;
    ok = ok && (!old_workers || saved_workers);
    cbm_setenv("CBM_WORKERS", driver ? "4" : "1", 1);
    cbm_pipeline_t *pipeline = ok ? cbm_pipeline_new(repo, database, CBM_MODE_FULL) : NULL;
    ok = pipeline && cbm_pipeline_set_project_name(pipeline, "python-names") &&
         cbm_pipeline_run(pipeline) == 0;
    cbm_pipeline_free(pipeline);
    if (saved_workers) {
        cbm_setenv("CBM_WORKERS", saved_workers, 1);
        free(saved_workers);
    } else
        cbm_unsetenv("CBM_WORKERS");
    cbm_store_t *store = ok ? cbm_store_open_path_existing(database) : NULL;
    RQ_Report report = {0};
    ok =
        store &&
        rq_check(store, "python-names", "python-language-isolation", sources,
                 sizeof(sources) / sizeof(sources[0]), admitted ? expected : NULL,
                 admitted ? sizeof(expected) / sizeof(expected[0]) : 0, admitted ? NULL : forbidden,
                 admitted ? 0 : sizeof(forbidden) / sizeof(forbidden[0]), &report, stderr) &&
        report.genuine == (admitted ? 2u : 0u) && report.found == report.genuine &&
        report.missing == 0 && report.wrong == 0 && ok;
    if (store)
        cbm_store_close(store);
    th_cleanup(root);
    return ok;
}

TEST(resolver_binding_python_crossfile_language_names_and_real_effects) {
    const struct {
        const char *source;
        bool admitted;
    } cases[] = {{"def target():\n    return 1\ndef local():\n    return target()\n", true},
                 {"def target():\n    return 1\ndef unrelated(debug):\n    return debug\n"
                  "def local():\n    return target()\n",
                  true},
                 {"def target():\n    return 1\ndef load():\n    return 2\n"
                  "def local():\n    return target()\n",
                  true},
                 {"def target():\n    return 1\ndef unrelated(value):\n    return value.rawset\n"
                  "def local():\n    return target()\n",
                  true},
                 {"def target():\n    return 1\nexec('target = None')\n"
                  "def local():\n    return target()\n",
                  false},
                 {"def target():\n    return 1\neval('globals().update(target=None)')\n"
                  "def local():\n    return target()\n",
                  false},
                 {"def target():\n    return 1\nglobals()['target'] = None\n"
                  "def local():\n    return target()\n",
                  false},
                 {"def target():\n    return 1\ntarget = None\n"
                  "def local():\n    return target()\n",
                  false}};
    for (int driver = 0; driver < 2; driver++) {
        for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
            bool ok = python_dynamic_name_golden(cases[i].source, cases[i].admitted, driver);
            if (!ok)
                fprintf(stderr, "Python language-isolation driver=%d case=%zu\n", driver, i);
            ASSERT(ok);
        }
    }
    PASS();
}

/* Literal formatting introduces no namespace bindings or user conversion
 * hooks. Unknown values remain an effect boundary, including nested width
 * expressions; recursive source auditing must not skip these children. */
TEST(resolver_binding_python_literal_formatting_preserves_exact_bindings) {
    const struct {
        const char *expression;
        bool supported;
        bool effects_complete;
    } cases[] = {
        {"f'code={404}'", true, true},
        {"f'value={-2:+04}'", true, true},
        {"f\"name={'x'!r:>4}\"", true, true},
        {"f'{{escaped}}:{1}'", true, true},
        {"f'{12:{3}}'", true, true},
        {"f'{True!s}:{None!r}:{2.5!a}'", true, true},
        {"f\"nested={f'{1}'}\"", true, true},
        {"f'{(1)}'", true, true},
        {"f'{value}'", false, true},
        {"f'{value!r}'", false, true},
        {"f'{unknown()}'", false, true},
        {"f'{value.member}'", false, true},
        {"f'{value[0]}'", false, true},
        {"f'{1:{width}}'", false, true},
        {"f'{(target := None)}'", false, true},
        {"f\"{eval('1')}\"", false, false},
        {"f\"{exec('target = None')}\"", false, false},
        {"f\"{globals().__setitem__('target', None)}\"", false, false},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char source[768];
        snprintf(source, sizeof(source),
                 "def target():\n    return 1\nlabel = %s\n"
                 "def caller():\n    return target()\n",
                 cases[i].expression);
        CBMFileResult *file = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON,
                                               "formatting", "main.py", 0, NULL, NULL);
        ASSERT_NOT_NULL(file);
        ASSERT(!file->has_error && !file->parse_incomplete && !file->parse_unusable);
        CBMArena arena;
        cbm_arena_init(&arena);
        cbm_resolve_binding_index_t *index =
            cbm_resolve_binding_build(&arena, file, CBM_LANG_PYTHON, source, strlen(source));
        const char *site = strstr(source, "return target()");
        ASSERT_NOT_NULL(site);
        uint32_t start = (uint32_t)(site + strlen("return ") - source);
        cbm_resolve_result_t proof = cbm_resolve_binding_local(
            index, "target", start, start + strlen("target()"), CBM_SOURCE_ORIGIN_RAW);
        bool exact = cbm_resolve_admit(&proof) && proof.proof && proof.target &&
                     !strcmp(proof.target, "formatting.main.target");
        bool exported = cbm_resolve_binding_export(index, "formatting.main.target");
        bool complete =
            !strcmp(cbm_resolve_binding_status(index), "binding_complete_supported_subset");
        bool effects = cbm_resolve_binding_python_effects_complete(index);
        /* Supporting a positive binding does not make this module's executed
         * assignment an inert closed namespace for wildcard absence proofs. */
        bool closed = cbm_resolve_binding_python_namespace_closed(index);
        if (exact != cases[i].supported || exported != cases[i].supported ||
            complete != cases[i].supported || effects != cases[i].effects_complete || closed)
            fprintf(
                stderr,
                "Python formatting case=%zu status=%s exact=%d export=%d effects=%d closed=%d\n", i,
                cbm_resolve_binding_status(index), exact, exported, effects, closed);
        cbm_arena_destroy(&arena);
        cbm_free_result(file);
        ASSERT_EQ(exact, cases[i].supported);
        ASSERT_EQ(exported, cases[i].supported);
        ASSERT_EQ(complete, cases[i].supported);
        ASSERT_EQ(effects, cases[i].effects_complete);
        ASSERT(!closed);
    }
    PASS();
}

TEST(resolver_binding_python_formatting_keeps_module_escape_and_rebinding_guards) {
    const char *sources[] = {
        "import library\ndef target():\n    return 1\n"
        "label = f'{library!s}'\ndef caller():\n    return target()\n",
        "def target():\n    return 1\nlabel = f'{1}'\ntarget = None\n"
        "def caller():\n    return target()\n",
        "def target():\n    return 1\n"
        "def caller(target):\n    label = f'{1}'\n    return target()\n",
        "def target():\n    return 1\n"
        "def caller():\n    label = f'{1}'\n    target = None\n    return target()\n",
    };
    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++) {
        const char *source = sources[i];
        CBMFileResult *file = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON,
                                               "formatting", "main.py", 0, NULL, NULL);
        ASSERT_NOT_NULL(file);
        ASSERT(!file->has_error && !file->parse_incomplete && !file->parse_unusable);
        CBMArena arena;
        cbm_arena_init(&arena);
        cbm_resolve_binding_index_t *index =
            cbm_resolve_binding_build(&arena, file, CBM_LANG_PYTHON, source, strlen(source));
        const char *site = strstr(source, "return target()");
        ASSERT_NOT_NULL(site);
        uint32_t start = (uint32_t)(site + strlen("return ") - source);
        cbm_resolve_result_t proof = cbm_resolve_binding_local(
            index, "target", start, start + strlen("target()"), CBM_SOURCE_ORIGIN_RAW);
        bool admitted = cbm_resolve_admit(&proof);
        bool escaped = cbm_resolve_binding_python_import_escaped(index, "library");
        cbm_arena_destroy(&arena);
        cbm_free_result(file);
        ASSERT(!admitted);
        if (i == 0)
            ASSERT(escaped);
    }
    PASS();
}

/* Reuse the full pipeline helper's independently authored main.py caller,
 * library.py local caller, line-one target, decoy, and 55-file driver padding.
 * The bytes below change syntax/effects only, never oracle target selection. */
TEST(resolver_binding_python_literal_formatting_crossfile_and_effects) {
    const struct {
        const char *source;
        bool admitted;
    } cases[] = {
        {"def target():\n    return 1\n"
         "def unrelated():\n    return f'code={404}'\n"
         "def local():\n    return target()\n",
         true},
        {"def target():\n    return 1\n"
         "def unrelated():\n    return f'value={-2:+04}'\n"
         "def local():\n    return target()\n",
         true},
        {"def target():\n    return 1\n"
         "def unrelated():\n    return f\"name={'x'!r:>4}\"\n"
         "def local():\n    return target()\n",
         true},
        {"def target():\n    return 1\n"
         "def unrelated():\n    return f'{{escaped}}:{1}'\n"
         "def local():\n    return target()\n",
         true},
        {"def target():\n    return 1\n"
         "def unrelated():\n    return f'{12:{3}}'\n"
         "def local():\n    return target()\n",
         true},
        {"def target():\n    return 1\n"
         "def unrelated():\n    return f'{True!s}:{None!r}:{2.5!a}'\n"
         "def local():\n    return target()\n",
         true},
        {"def target():\n    return 1\n"
         "def unrelated():\n    return f\"nested={f'{1}'}\"\n"
         "def local():\n    return target()\n",
         true},
        {"def target():\n    return 1\n"
         "label = f'{(target := None)}'\n"
         "def local():\n    return target()\n",
         false},
        {"def target():\n    return 1\n"
         "label = f\"{eval('globals().update(target=None)')}\"\n"
         "def local():\n    return target()\n",
         false},
        {"def target():\n    return 1\n"
         "label = f\"{exec('target = None')}\"\n"
         "def local():\n    return target()\n",
         false},
        {"def target():\n    return 1\n"
         "label = f\"{globals().__setitem__('target', None)}\"\n"
         "def local():\n    return target()\n",
         false},
        {"def target():\n    return 1\nlabel = f'{1}'\ntarget = None\n"
         "def local():\n    return target()\n",
         false},
    };
    for (int driver = 0; driver < 2; driver++) {
        for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
            bool ok = python_dynamic_name_golden(cases[i].source, cases[i].admitted, driver);
            if (!ok)
                fprintf(stderr, "Python literal-formatting driver=%d case=%zu\n", driver, i);
            ASSERT(ok);
        }
    }
    PASS();
}

/* Exception handling does not invalidate unrelated lexical identities. Its
 * conditional declarations, handler aliases and writes still shadow bindings;
 * nested dynamic operations remain visible to the complete source walk. */
TEST(resolver_binding_python_try_preserves_scope_and_effect_barriers) {
    const struct {
        const char *body;
        bool admitted;
    } cases[] = {
        {"def caller():\n    try:\n        return target()\n"
         "    except ValueError:\n        return 0\n",
         true},
        {"def caller():\n    try:\n        pass\n"
         "    except ValueError:\n        return target()\n",
         true},
        {"def caller():\n    try:\n        pass\n"
         "    except ValueError:\n        pass\n    else:\n        return target()\n",
         true},
        {"def caller():\n    try:\n        pass\n    finally:\n        return target()\n", true},
        {"def caller():\n    try:\n        pass\n"
         "    except ValueError as target:\n        return target()\n",
         false},
        {"def caller():\n    try:\n        pass\n"
         "    except ValueError as target:\n        pass\n    return target()\n",
         false},
        {"def caller():\n    try:\n        target = None\n"
         "    finally:\n        return target()\n",
         false},
        {"def caller():\n    try:\n        exec('target = None')\n"
         "    finally:\n        return target()\n",
         false},
        {"try:\n    target = None\nexcept ValueError:\n    pass\n"
         "def caller():\n    return target()\n",
         false},
        {"def caller():\n    try:\n        globals()['target'] = None\n"
         "    finally:\n        return target()\n",
         false},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char source[768];
        snprintf(source, sizeof(source), "def target():\n    return 1\n%s", cases[i].body);
        CBMFileResult *file = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON,
                                               "exceptions", "main.py", 0, NULL, NULL);
        ASSERT_NOT_NULL(file);
        ASSERT(!file->has_error && !file->parse_incomplete && !file->parse_unusable);
        CBMArena arena;
        cbm_arena_init(&arena);
        cbm_resolve_binding_index_t *index =
            cbm_resolve_binding_build(&arena, file, CBM_LANG_PYTHON, source, strlen(source));
        const char *site = strstr(source, "return target()");
        ASSERT_NOT_NULL(site);
        uint32_t start = (uint32_t)(site + strlen("return ") - source);
        cbm_resolve_result_t proof = cbm_resolve_binding_local(
            index, "target", start, start + strlen("target()"), CBM_SOURCE_ORIGIN_RAW);
        bool admitted = cbm_resolve_admit(&proof);
        bool exact = admitted && proof.proof && proof.target &&
                     !strcmp(proof.target, "exceptions.main.target");
        if (admitted != cases[i].admitted || exact != cases[i].admitted)
            fprintf(stderr, "Python exception case=%zu status=%s admitted=%d exact=%d\n", i,
                    cbm_resolve_binding_status(index), admitted, exact);
        cbm_arena_destroy(&arena);
        cbm_free_result(file);
        ASSERT_EQ(admitted, cases[i].admitted);
        ASSERT_EQ(exact, cases[i].admitted);
    }
    PASS();
}

TEST(resolver_binding_python_try_crossfile_and_conditional_rebinding) {
    const struct {
        const char *source;
        bool admitted;
    } cases[] = {
        {"def target():\n    return 1\n"
         "def unrelated():\n    try:\n        return 2\n"
         "    except ValueError:\n        return 3\n"
         "def local():\n    return target()\n",
         true},
        {"def target():\n    return 1\n"
         "def unrelated():\n    try:\n        pass\n    finally:\n        pass\n"
         "def local():\n    return target()\n",
         true},
        {"def target():\n    return 1\n"
         "try:\n    target = None\nexcept ValueError:\n    pass\n"
         "def local():\n    return target()\n",
         false},
        {"def target():\n    return 1\n"
         "try:\n    pass\nexcept ValueError as target:\n    pass\n"
         "def local():\n    return target()\n",
         false},
        {"def target():\n    return 1\n"
         "try:\n    pass\nfinally:\n    exec('target = None')\n"
         "def local():\n    return target()\n",
         false},
    };
    for (int driver = 0; driver < 2; driver++) {
        for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
            bool ok = python_dynamic_name_golden(cases[i].source, cases[i].admitted, driver);
            if (!ok)
                fprintf(stderr, "Python exception driver=%d case=%zu\n", driver, i);
            ASSERT(ok);
        }
    }
    PASS();
}

/* No-value annotations are declarations, not runtime target reads or writes.
 * Check both semantic extraction entry points and retain real write controls. */
TEST(resolver_binding_python_annotation_only_raw_roles) {
    static const char source[] =
        "module_only: object\nassigned: object = 1\nplain = 2\n"
        "def local():\n    local_only: object\n    updated: object = 3\n    updated += 1\n"
        "class Box:\n    field_only: object\n    field_value: object = 4\n";
    static const char *const annotations[] = {"module_only", "local_only", "field_only"};
    static const char *const writes[] = {"assigned: object =", "plain =", "updated: object =",
                                         "updated +=", "field_value: object ="};
    CBMFileResult *file = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON,
                                           "annotation", "main.py", 0, NULL, NULL);
    ASSERT_NOT_NULL(file);
    ASSERT(!file->has_error && !file->parse_incomplete && !file->parse_unusable);
    TSParser *parser = ts_parser_new();
    ASSERT_NOT_NULL(parser);
    ASSERT(ts_parser_set_language(parser, cbm_ts_language(CBM_LANG_PYTHON)));
    TSTree *tree = cbm_parse_source(parser, source, (uint32_t)strlen(source), (TSParseOptions){0});
    ts_parser_delete(parser);
    ASSERT_NOT_NULL(tree);
    bool ok = true;
    /* Module annotation-only names have no runtime definition to export or
     * collide with an existing callable. Initialized module variables retain
     * their source identities; class annotations use type sidecars below. */
    static const struct {
        const char *name;
        unsigned line;
        const char *owner;
    } definitions[] = {{"assigned", 2, NULL}, {"plain", 3, NULL}};
    for (size_t i = 0; i < sizeof(definitions) / sizeof(definitions[0]); i++) {
        int found = 0;
        for (int d = 0; d < file->defs.count; d++) {
            const CBMDefinition *def = &file->defs.items[d];
            if (!def->name || strcmp(def->name, definitions[i].name))
                continue;
            found++;
            bool owner = definitions[i].owner
                             ? def->parent_class && !strcmp(def->parent_class, definitions[i].owner)
                             : !def->parent_class;
            ok = def->label && !strcmp(def->label, "Variable") &&
                 def->start_line == definitions[i].line && def->end_line == definitions[i].line &&
                 owner && ok;
        }
        ok = found == 1 && ok;
    }
    for (int d = 0; d < file->defs.count; d++) {
        const CBMDefinition *def = &file->defs.items[d];
        ok =
            (!def->name || (strcmp(def->name, "module_only") && strcmp(def->name, "local_only"))) &&
            ok;
    }
    static const char *const class_fields[] = {"field_only", "field_value"};
    for (size_t i = 0; i < sizeof(class_fields) / sizeof(class_fields[0]); i++) {
        int found = 0;
        for (int t = 0; t < file->field_types.count; t++) {
            const CBMFieldType *field = &file->field_types.items[t];
            found += field->class_qn && !strcmp(field->class_qn, "annotation.main.Box") &&
                     field->field_name && !strcmp(field->field_name, class_fields[i]) &&
                     field->type_text && !strcmp(field->type_text, "object");
        }
        ok = found == 1 && ok;
    }
    for (int driver = 0; driver < 2; driver++) {
        if (driver) {
            file->rw.count = 0;
            CBMExtractCtx context = {.arena = &file->arena,
                                     .result = file,
                                     .source = source,
                                     .source_len = (int)strlen(source),
                                     .language = CBM_LANG_PYTHON,
                                     .project = "annotation",
                                     .rel_path = "main.py",
                                     .module_qn = file->module_qn,
                                     .root = ts_tree_root_node(tree)};
            cbm_extract_semantic(&context);
        }
        ok = file->rw.count == 5 && ok;
        for (size_t i = 0; i < sizeof(annotations) / sizeof(annotations[0]); i++) {
            uint32_t start = (uint32_t)(strstr(source, annotations[i]) - source);
            uint32_t end = start + (uint32_t)strlen(annotations[i]);
            for (int r = 0; r < file->rw.count; r++)
                ok = !(file->rw.items[r].site_start_byte == start &&
                       file->rw.items[r].site_end_byte == end) &&
                     ok;
            for (int u = 0; u < file->usages.count; u++)
                ok = !(file->usages.items[u].site_start_byte == start &&
                       file->usages.items[u].site_end_byte == end) &&
                     ok;
        }
        for (size_t i = 0; i < sizeof(writes) / sizeof(writes[0]); i++) {
            uint32_t start = (uint32_t)(strstr(source, writes[i]) - source);
            size_t length = strcspn(writes[i], ": ");
            int found = 0;
            for (int r = 0; r < file->rw.count; r++) {
                const CBMReadWrite *row = &file->rw.items[r];
                found += row->is_write && row->site_start_byte == start &&
                         row->site_end_byte == start + length;
            }
            ok = found == 1 && ok;
        }
    }
    ts_tree_delete(tree);
    cbm_free_result(file);
    ASSERT(ok);
    PASS();
}

TEST(resolver_binding_python_annotation_scope_and_effects) {
    static const struct {
        const char *body;
        bool admitted, imported, effects;
    } cases[] = {
        {"callback: object\ndef caller():\n    callback()\n", true, true, true},
        {"def unrelated(__annotations__):\n    pass\ncallback: object\ndef caller():\n    "
         "callback()\n",
         true, true, true},
        {"def caller():\n    callback()\n    callback: object\n", false, true, true},
        {"def caller():\n    callback: object\n    callback()\n", false, true, true},
        {"class Box:\n    callback: object\ndef caller():\n    callback()\n", true, true, true},
        {"callback: object = other\ndef caller():\n    callback()\n", false, false, true},
        {"callback: factory()\ndef caller():\n    callback()\n", false, false, true},
        {"callback: eval('object')\ndef caller():\n    callback()\n", false, false, false},
        {"__annotations__ = box\ncallback: object\ndef caller():\n    callback()\n", false, false,
         true},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char source[512];
        snprintf(source, sizeof(source), "from target import handler as callback\n%s",
                 cases[i].body);
        CBMFileResult *file = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON,
                                               "annotation", "main.py", 0, NULL, NULL);
        ASSERT_NOT_NULL(file);
        ASSERT(!file->has_error && !file->parse_incomplete && !file->parse_unusable);
        CBMArena arena;
        cbm_arena_init(&arena);
        cbm_resolve_binding_index_t *index =
            cbm_resolve_binding_build(&arena, file, CBM_LANG_PYTHON, source, strlen(source));
        const char *call = strstr(source, "callback()");
        const char *annotation = strstr(source, "callback:");
        ASSERT_NOT_NULL(call);
        ASSERT_NOT_NULL(annotation);
        cbm_resolve_result_t candidate = {.binding = CBM_RESOLVE_BINDING_UNIQUE,
                                          .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                          .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                          .rule = CBM_RESOLVE_RULE_IMPORT_BINDING,
                                          .facts = CBM_RESOLVE_FACT_EXACT_TARGET |
                                                   CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                                                   CBM_RESOLVE_FACT_IMPORT_BOUND,
                                          .target = "annotation.target.handler",
                                          .candidate_count = 1};
        uint32_t start = (uint32_t)(call - source), lhs = (uint32_t)(annotation - source);
        cbm_resolve_result_t proof = cbm_resolve_binding_validate(
            index, "callback", start, start + 10, CBM_SOURCE_ORIGIN_RAW, candidate);
        bool admitted = cbm_resolve_admit(&proof);
        cbm_resolve_result_t target = cbm_resolve_binding_validate(
            index, "callback", lhs, lhs + 8, CBM_SOURCE_ORIGIN_RAW, candidate);
        bool imported = cbm_resolve_binding_import(index, "callback");
        bool effects = cbm_resolve_binding_python_effects_complete(index);
        bool target_admitted = cbm_resolve_admit(&target);
        if (admitted != cases[i].admitted || imported != cases[i].imported ||
            effects != cases[i].effects || target_admitted)
            fprintf(
                stderr,
                "Python annotation case=%zu status=%s admitted=%d import=%d effects=%d lhs=%d\n", i,
                cbm_resolve_binding_status(index), admitted, imported, effects, target_admitted);
        cbm_arena_destroy(&arena);
        cbm_free_result(file);
        ASSERT_EQ(admitted, cases[i].admitted);
        ASSERT_EQ(imported, cases[i].imported);
        ASSERT_EQ(effects, cases[i].effects);
        ASSERT(!target_admitted);
    }
    PASS();
}

static const RQ_Source PYTHON_ANNOTATION_SOURCES[] = {
    {.path = "target.py", .text = "def handler():\n    pass\n"},
    {.path = "use.py",
     .text = "from target import handler as callback\ncallback: object\ndef accept(fn):\n    "
             "pass\ndef crossArgument():\n    accept(callback)\n"},
    {.path = "relay.py", .text = "from target import handler as callback\ncallback: object\n"},
    {.path = "client.py",
     .text = "from relay import callback\ndef crossRelay():\n    callback()\n"},
    {.path = "local_control.py",
     .text = "def stable():\n    pass\nstable: object\ndef localControl():\n    stable()\n"},
    {.path = "local.py",
     .text = "from target import handler as callback\ndef before():\n    callback()\n    callback: "
             "object\ndef after():\n    callback: object\n    callback()\n"},
    {.path = "assigned.py",
     .text = "from target import handler as callback\ndef other():\n    pass\ncallback: object = "
             "other\ndef changed():\n    callback()\n"},
    {.path = "effects.py",
     .text = "from external import annotation\nfrom target import handler as callback\ncallback: "
             "annotation()\ndef effect():\n    callback()\n"},
    {.path = "storage.py",
     .text =
         "from external import mapping\nfrom target import handler as callback\n__annotations__ = "
         "mapping\ncallback: object\ndef changedStorage():\n    callback()\n"},
    {.path = "decoy.py", .text = "def handler():\n    pass\n"}};
static const RQ_Expected PYTHON_ANNOTATION_EXPECTED[] = {
    {.case_id = "original-module-callable-reference",
     .site = {.file = "use.py", .caller = "crossArgument", .start_byte = 113, .end_byte = 121},
     .relation = "CALL_REFERENCE",
     .target = {.file = "target.py",
                .suffix = "target.handler",
                .label = "Function",
                .declaration_line = 1,
                .declaration_text = "def handler():"}},
    {.case_id = "original-module-accept-call",
     .site = {.file = "use.py", .caller = "crossArgument", .start_byte = 106, .end_byte = 122},
     .relation = "CALLS",
     .target = {.file = "use.py",
                .suffix = "use.accept",
                .label = "Function",
                .declaration_line = 3,
                .declaration_text = "def accept(fn):"}},
    {.case_id = "annotation-reexport-preserves-target",
     .site = {.file = "client.py", .caller = "crossRelay", .start_byte = 49, .end_byte = 59},
     .relation = "CALLS",
     .target = {.file = "target.py",
                .suffix = "target.handler",
                .label = "Function",
                .declaration_line = 1,
                .declaration_text = "def handler():"}},
    {.case_id = "annotation-preserves-local-function",
     .site =
         {.file = "local_control.py", .caller = "localControl", .start_byte = 62, .end_byte = 70},
     .relation = "CALLS",
     .target = {.file = "local_control.py",
                .suffix = "local_control.stable",
                .label = "Function",
                .declaration_line = 1,
                .declaration_text = "def stable():"}},
    {.case_id = "initialized-annotation-selects-new-value",
     .site = {.file = "assigned.py", .caller = "changed", .start_byte = 105, .end_byte = 115},
     .relation = "CALLS",
     .target = {.file = "assigned.py",
                .suffix = "assigned.other",
                .label = "Function",
                .declaration_line = 2,
                .declaration_text = "def other():"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "python-ordered-annotated-rebinding",
     .why = "The initializer really replaces the import; exact ordered callable rebinding is "
            "outside the shared lexical proof subset.",
     .tried = "Original value assignment is retained and the imported handler is explicitly "
              "forbidden at the call; missing other remains counted."}};
static const RQ_Forbidden PYTHON_ANNOTATION_FORBIDDEN[] = {
    {.case_id = "use.py-use-annotation-lhs-CALLS",
     .site = {.file = "use.py", .caller = "use", .start_byte = 39, .end_byte = 47},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "use.py-use-annotation-lhs-CALL_REFERENCE",
     .site = {.file = "use.py", .caller = "use", .start_byte = 39, .end_byte = 47},
     .relation = "CALL_REFERENCE",
     .any_target = true},
    {.case_id = "use.py-use-annotation-lhs-USAGE",
     .site = {.file = "use.py", .caller = "use", .start_byte = 39, .end_byte = 47},
     .relation = "USAGE",
     .any_target = true},
    {.case_id = "use.py-use-annotation-lhs-READS",
     .site = {.file = "use.py", .caller = "use", .start_byte = 39, .end_byte = 47},
     .relation = "READS",
     .any_target = true},
    {.case_id = "use.py-use-annotation-lhs-WRITES",
     .site = {.file = "use.py", .caller = "use", .start_byte = 39, .end_byte = 47},
     .relation = "WRITES",
     .any_target = true},
    {.case_id = "relay.py-relay-annotation-lhs-CALLS",
     .site = {.file = "relay.py", .caller = "relay", .start_byte = 39, .end_byte = 47},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "relay.py-relay-annotation-lhs-CALL_REFERENCE",
     .site = {.file = "relay.py", .caller = "relay", .start_byte = 39, .end_byte = 47},
     .relation = "CALL_REFERENCE",
     .any_target = true},
    {.case_id = "relay.py-relay-annotation-lhs-USAGE",
     .site = {.file = "relay.py", .caller = "relay", .start_byte = 39, .end_byte = 47},
     .relation = "USAGE",
     .any_target = true},
    {.case_id = "relay.py-relay-annotation-lhs-READS",
     .site = {.file = "relay.py", .caller = "relay", .start_byte = 39, .end_byte = 47},
     .relation = "READS",
     .any_target = true},
    {.case_id = "relay.py-relay-annotation-lhs-WRITES",
     .site = {.file = "relay.py", .caller = "relay", .start_byte = 39, .end_byte = 47},
     .relation = "WRITES",
     .any_target = true},
    {.case_id = "local_control.py-local_control-annotation-lhs-CALLS",
     .site =
         {.file = "local_control.py", .caller = "local_control", .start_byte = 23, .end_byte = 29},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "local_control.py-local_control-annotation-lhs-CALL_REFERENCE",
     .site =
         {.file = "local_control.py", .caller = "local_control", .start_byte = 23, .end_byte = 29},
     .relation = "CALL_REFERENCE",
     .any_target = true},
    {.case_id = "local_control.py-local_control-annotation-lhs-USAGE",
     .site =
         {.file = "local_control.py", .caller = "local_control", .start_byte = 23, .end_byte = 29},
     .relation = "USAGE",
     .any_target = true},
    {.case_id = "local_control.py-local_control-annotation-lhs-READS",
     .site =
         {.file = "local_control.py", .caller = "local_control", .start_byte = 23, .end_byte = 29},
     .relation = "READS",
     .any_target = true},
    {.case_id = "local_control.py-local_control-annotation-lhs-WRITES",
     .site =
         {.file = "local_control.py", .caller = "local_control", .start_byte = 23, .end_byte = 29},
     .relation = "WRITES",
     .any_target = true},
    {.case_id = "local.py-before-annotation-lhs-CALLS",
     .site = {.file = "local.py", .caller = "before", .start_byte = 72, .end_byte = 80},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "local.py-before-annotation-lhs-CALL_REFERENCE",
     .site = {.file = "local.py", .caller = "before", .start_byte = 72, .end_byte = 80},
     .relation = "CALL_REFERENCE",
     .any_target = true},
    {.case_id = "local.py-before-annotation-lhs-USAGE",
     .site = {.file = "local.py", .caller = "before", .start_byte = 72, .end_byte = 80},
     .relation = "USAGE",
     .any_target = true},
    {.case_id = "local.py-before-annotation-lhs-READS",
     .site = {.file = "local.py", .caller = "before", .start_byte = 72, .end_byte = 80},
     .relation = "READS",
     .any_target = true},
    {.case_id = "local.py-before-annotation-lhs-WRITES",
     .site = {.file = "local.py", .caller = "before", .start_byte = 72, .end_byte = 80},
     .relation = "WRITES",
     .any_target = true},
    {.case_id = "local.py-after-annotation-lhs-CALLS",
     .site = {.file = "local.py", .caller = "after", .start_byte = 106, .end_byte = 114},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "local.py-after-annotation-lhs-CALL_REFERENCE",
     .site = {.file = "local.py", .caller = "after", .start_byte = 106, .end_byte = 114},
     .relation = "CALL_REFERENCE",
     .any_target = true},
    {.case_id = "local.py-after-annotation-lhs-USAGE",
     .site = {.file = "local.py", .caller = "after", .start_byte = 106, .end_byte = 114},
     .relation = "USAGE",
     .any_target = true},
    {.case_id = "local.py-after-annotation-lhs-READS",
     .site = {.file = "local.py", .caller = "after", .start_byte = 106, .end_byte = 114},
     .relation = "READS",
     .any_target = true},
    {.case_id = "local.py-after-annotation-lhs-WRITES",
     .site = {.file = "local.py", .caller = "after", .start_byte = 106, .end_byte = 114},
     .relation = "WRITES",
     .any_target = true},
    {.case_id = "effects.py-effects-annotation-lhs-CALLS",
     .site = {.file = "effects.py", .caller = "effects", .start_byte = 71, .end_byte = 79},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "effects.py-effects-annotation-lhs-CALL_REFERENCE",
     .site = {.file = "effects.py", .caller = "effects", .start_byte = 71, .end_byte = 79},
     .relation = "CALL_REFERENCE",
     .any_target = true},
    {.case_id = "effects.py-effects-annotation-lhs-USAGE",
     .site = {.file = "effects.py", .caller = "effects", .start_byte = 71, .end_byte = 79},
     .relation = "USAGE",
     .any_target = true},
    {.case_id = "effects.py-effects-annotation-lhs-READS",
     .site = {.file = "effects.py", .caller = "effects", .start_byte = 71, .end_byte = 79},
     .relation = "READS",
     .any_target = true},
    {.case_id = "effects.py-effects-annotation-lhs-WRITES",
     .site = {.file = "effects.py", .caller = "effects", .start_byte = 71, .end_byte = 79},
     .relation = "WRITES",
     .any_target = true},
    {.case_id = "storage.py-storage-annotation-lhs-CALLS",
     .site = {.file = "storage.py", .caller = "storage", .start_byte = 94, .end_byte = 102},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "storage.py-storage-annotation-lhs-CALL_REFERENCE",
     .site = {.file = "storage.py", .caller = "storage", .start_byte = 94, .end_byte = 102},
     .relation = "CALL_REFERENCE",
     .any_target = true},
    {.case_id = "storage.py-storage-annotation-lhs-USAGE",
     .site = {.file = "storage.py", .caller = "storage", .start_byte = 94, .end_byte = 102},
     .relation = "USAGE",
     .any_target = true},
    {.case_id = "storage.py-storage-annotation-lhs-READS",
     .site = {.file = "storage.py", .caller = "storage", .start_byte = 94, .end_byte = 102},
     .relation = "READS",
     .any_target = true},
    {.case_id = "storage.py-storage-annotation-lhs-WRITES",
     .site = {.file = "storage.py", .caller = "storage", .start_byte = 94, .end_byte = 102},
     .relation = "WRITES",
     .any_target = true},
    {.case_id = "before-no-proved-callable",
     .site = {.file = "local.py", .caller = "before", .start_byte = 57, .end_byte = 67},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "after-no-proved-callable",
     .site = {.file = "local.py", .caller = "after", .start_byte = 127, .end_byte = 137},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "effect-no-proved-callable",
     .site = {.file = "effects.py", .caller = "effect", .start_byte = 112, .end_byte = 122},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "changedStorage-no-proved-callable",
     .site = {.file = "storage.py", .caller = "changedStorage", .start_byte = 137, .end_byte = 147},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "initialized-annotation-replaces-import",
     .site = {.file = "assigned.py", .caller = "changed", .start_byte = 105, .end_byte = 115},
     .relation = "CALLS",
     .target = {.file = "target.py",
                .suffix = "target.handler",
                .label = "Function",
                .declaration_line = 1,
                .declaration_text = "def handler():"}}};

TEST(resolver_binding_python_annotation_pipeline_roles_and_exports) {
    for (int driver = 0; driver < 2; driver++) {
        const char *root = th_mktempdir("cbm_python_annotations");
        ASSERT_NOT_NULL(root);
        char repo[768], database[768];
        snprintf(repo, sizeof(repo), "%s/repo", root);
        snprintf(database, sizeof(database), "%s/index.db", root);
        bool ok = cbm_mkdir_p(repo, 0755);
        for (size_t i = 0;
             ok && i < sizeof(PYTHON_ANNOTATION_SOURCES) / sizeof(PYTHON_ANNOTATION_SOURCES[0]);
             i++)
            ok = th_write_file(TH_PATH(repo, PYTHON_ANNOTATION_SOURCES[i].path),
                               PYTHON_ANNOTATION_SOURCES[i].text) == 0;
        for (int i = 0; ok && i < 55; i++) {
            char path[32];
            snprintf(path, sizeof(path), "padding%02d.py", i);
            ok = th_write_file(TH_PATH(repo, path), "# unrelated source\n") == 0;
        }
        const char *old_workers = getenv("CBM_WORKERS");
        char *saved_workers = old_workers ? strdup(old_workers) : NULL;
        ok = ok && (!old_workers || saved_workers);
        cbm_setenv("CBM_WORKERS", driver ? "4" : "1", 1);
        cbm_pipeline_t *pipeline = ok ? cbm_pipeline_new(repo, database, CBM_MODE_FULL) : NULL;
        ok = pipeline && cbm_pipeline_set_project_name(pipeline, "python-annotations") &&
             cbm_pipeline_run(pipeline) == 0;
        cbm_pipeline_free(pipeline);
        if (saved_workers) {
            cbm_setenv("CBM_WORKERS", saved_workers, 1);
            free(saved_workers);
        } else
            cbm_unsetenv("CBM_WORKERS");
        cbm_store_t *store = ok ? cbm_store_open_path_existing(database) : NULL;
        RQ_Report report = {0};
        ok = store &&
             rq_check(store, "python-annotations", "python-annotation-roles",
                      PYTHON_ANNOTATION_SOURCES,
                      sizeof(PYTHON_ANNOTATION_SOURCES) / sizeof(PYTHON_ANNOTATION_SOURCES[0]),
                      PYTHON_ANNOTATION_EXPECTED,
                      sizeof(PYTHON_ANNOTATION_EXPECTED) / sizeof(PYTHON_ANNOTATION_EXPECTED[0]),
                      PYTHON_ANNOTATION_FORBIDDEN,
                      sizeof(PYTHON_ANNOTATION_FORBIDDEN) / sizeof(PYTHON_ANNOTATION_FORBIDDEN[0]),
                      &report, stderr) &&
             ok;
        if (store)
            cbm_store_close(store);
        th_cleanup(root);
        ASSERT(ok);
        ASSERT_EQ(report.genuine, 5);
        ASSERT_EQ(report.found, 4);
        ASSERT_EQ(report.missing, 1);
        ASSERT_EQ(report.required_missing + report.wrong, 0);
    }
    PASS();
}

static bool embedded_binding_venue(const char *path, const char *source, bool required,
                                   int driver) {
    static const char *control = "export function proven() { return 1; }\n"
                                 "export function check() { return proven(); }\n";
    const RQ_Source sources[] = {
        {.path = path, .text = source},
        {.path = "decoy.ts", .text = "export function callee() { return 0; }\n"},
        {.path = "control.mjs", .text = control}};
    const char *site = strstr(source, "return callee();");
    const char *declaration = strstr(source, "function callee()");
    if (!site || !declaration)
        return false;
    site += strlen("return ");
    uint32_t start = (uint32_t)(site - source), line = 1;
    for (const char *p = source; p < declaration; p++)
        line += *p == '\n';
    const char *end = strchr(declaration, '\n');
    char declaration_text[128];
    size_t n = end ? (size_t)(end - declaration) : strlen(declaration);
    if (n >= sizeof(declaration_text))
        return false;
    memcpy(declaration_text, declaration, n);
    declaration_text[n] = '\0';
    const char *control_site = strstr(control, "return proven();") + strlen("return ");
    const RQ_Expected expected[] = {
        {.case_id = "independent-esm-control",
         .site = {.file = "control.mjs",
                  .caller = "check",
                  .start_byte = (uint32_t)(control_site - control),
                  .end_byte = (uint32_t)(control_site - control) + 8},
         .relation = "CALLS",
         .target = {.file = "control.mjs",
                    .suffix = "proven",
                    .label = "Function",
                    .declaration_line = 1,
                    .declaration_text = "export function proven() { return 1; }"}},
        {.case_id = "component-script-exact-local-call",
         .site = {.file = path, .caller = "caller", .start_byte = start, .end_byte = start + 8},
         .relation = "CALLS",
         .target = {.file = path,
                    .suffix = "callee",
                    .label = "Function",
                    .declaration_line = line,
                    .declaration_text = declaration_text}}};
    const RQ_Forbidden forbidden[] = {
        {.case_id = "component-never-targets-same-name-other-file",
         .site = {.file = path, .caller = "caller", .start_byte = start, .end_byte = start + 8},
         .relation = "CALLS",
         .target = {.file = "decoy.ts",
                    .suffix = "callee",
                    .label = "Function",
                    .declaration_line = 1,
                    .declaration_text = "export function callee() { return 0; }"}},
        {.case_id = "unproved-component-scope-abstains",
         .site = {.file = path, .caller = "caller", .start_byte = start, .end_byte = start + 8},
         .relation = "CALLS",
         .any_target = true}};
    const char *root = th_mktempdir("cbm_embedded_binding");
    if (!root)
        return false;
    char repo[768], database[768];
    snprintf(repo, sizeof(repo), "%s/repo", root);
    snprintf(database, sizeof(database), "%s/index.db", root);
    bool ok = cbm_mkdir_p(repo, 0755);
    for (size_t i = 0; ok && i < sizeof(sources) / sizeof(sources[0]); i++)
        ok = th_write_file(TH_PATH(repo, sources[i].path), sources[i].text) == 0;
    for (int i = 0; ok && i < 55; i++) {
        char name[32];
        snprintf(name, sizeof(name), "padding%02d.ts", i);
        ok = th_write_file(TH_PATH(repo, name), "// unrelated source\n") == 0;
    }
    const char *old_workers = getenv("CBM_WORKERS");
    char *saved = old_workers ? strdup(old_workers) : NULL;
    ok = ok && (!old_workers || saved);
    cbm_setenv("CBM_WORKERS", driver ? "4" : "1", 1);
    cbm_pipeline_t *pipeline = ok ? cbm_pipeline_new(repo, database, CBM_MODE_FULL) : NULL;
    ok = pipeline && cbm_pipeline_set_project_name(pipeline, "embedded-bindings") &&
         cbm_pipeline_run(pipeline) == 0;
    cbm_pipeline_free(pipeline);
    if (saved) {
        cbm_setenv("CBM_WORKERS", saved, 1);
        free(saved);
    } else
        cbm_unsetenv("CBM_WORKERS");
    cbm_store_t *store = ok ? cbm_store_open_path_existing(database) : NULL;
    RQ_Report report = {0};
    ok = store &&
         rq_check(store, "embedded-bindings", path, sources, 3, expected, required ? 2 : 1,
                  forbidden, required ? 1 : 2, &report, stderr) &&
         ok;
    ok = ok && report.genuine == (required ? 2 : 1) && report.found == report.genuine &&
         !report.missing && !report.wrong;
    if (store)
        cbm_store_close(store);
    th_cleanup(root);
    return ok;
}

TEST(resolver_binding_component_scripts_exact_local_scope) {
    static const struct {
        const char *path, *source;
    } fixtures[] = {{"App.vue", "<template><p>Vue</p></template>\n<script setup lang=\"ts\">\n"
                                "function callee(): number { return 42; }\n"
                                "function caller(): number { return callee(); }\n</script>\n"},
                    {"App.vue", "<template><p>Vue</p></template>\n<script setup>\n"
                                "function callee() { return 42; }\n"
                                "function caller() { return callee(); }\n</script>\n"},
                    {"Toggle.svelte", "<script lang=\"ts\">\n"
                                      "function callee(): number { return 42; }\n"
                                      "function caller(): number { return callee(); }\n</script>\n"
                                      "<button on:click={caller}>Toggle</button>\n"},
                    {"Toggle.svelte", "<script>\nfunction callee() { return 42; }\n"
                                      "function caller() { return callee(); }\n</script>\n"
                                      "<button on:click={caller}>Toggle</button>\n"}};
    for (int driver = 0; driver < 2; driver++)
        for (size_t i = 0; i < sizeof(fixtures) / sizeof(fixtures[0]); i++)
            ASSERT(embedded_binding_venue(fixtures[i].path, fixtures[i].source, true, driver));
    PASS();
}

TEST(resolver_binding_component_scripts_scope_and_write_barriers) {
    static const struct {
        const char *path, *source;
    } fixtures[] = {
        {"App.vue", "<template><button @click=\"callee = caller\">Change</button></template>\n"
                    "<script setup>\nfunction callee() { return 42; }\n"
                    "function caller() { return callee(); }\n</script>\n"},
        {"Toggle.svelte", "<script>\nfunction callee() { return 42; }\n"
                          "function caller() { return callee(); }\n</script>\n"
                          "<button on:click={() => callee = caller}>Change</button>\n"},
        {"App.vue",
         "<script setup lang=\"ts\">\n"
         "function callee(): number { return 42; }\n"
         "function caller(callee: () => number): number { return callee(); }\n</script>\n"},
        {"Toggle.svelte", "<script>\nfunction callee() { return 42; }\n"
                          "function caller() { return callee(); }\ncallee = () => 0;\n</script>\n"},
        {"Toggle.svelte",
         "<script context=\"module\">\nfunction callee() { return 42; }\n"
         "</script>\n<script>\nfunction caller(callee) { return callee(); }\n</script>\n"}};
    for (int driver = 0; driver < 2; driver++)
        for (size_t i = 0; i < sizeof(fixtures) / sizeof(fixtures[0]); i++)
            ASSERT(embedded_binding_venue(fixtures[i].path, fixtures[i].source, false, driver));
    PASS();
}

/* Ordinary annotation/return containers are transparent to the binding audit;
 * their descendants retain declaration, evaluation and namespace-effect roles.
 */
TEST(resolver_binding_python_annotation_containers_and_parameter_separators) {
    const struct {
        const char *tail;
        bool exact;
    } cases[] = {
        {"def caller(value: Optional[str]) -> Tuple[str, str]:\n"
         "    return target(), value\n",
         true},
        {"def caller(value, /, *, option=None):\n    return target(), option\n", true},
        {"def caller(*, target=None):\n    return target()\n", false},
        {"def caller(target, /):\n    return target()\n", false},
        {"def caller(value: Optional[str]):\n    target, other = None, None\n"
         "    return target()\n",
         false},
        {"def caller(value: Optional[globals().__setitem__('target', None)]):\n  "
         "  return target()\n",
         false},
        {"def caller(value: Optional[str] = globals().__setitem__('target', "
         "None)):\n    return target()\n",
         false},
        /* QUALITY GAP python-pep695-binding-audit. WHY: these genuine target()
         * calls have unsupported declaration type scopes. TRIED: the source
         * audit still rejects the declaration's type_parameter, independently
         * of newly admitted generic-annotation argument containers. This is
         * a capability assertion, not a forbidden semantic target. */
        {"def caller[T](value: T):\n    return target()\n", false},
        {"class Container[T]:\n    pass\ndef caller():\n    return target()\n", false},
        /* QUALITY GAP python-annotation-subscription-effects. WHY: a no-value
         * annotation preserves this genuine runtime target binding, but the
         * subscription effect audit is intentionally unproved. TRIED: retain
         * python_inert_annotation's existing barrier; do not count this miss
         * as a false-positive target or weaken the separate annotation oracle. */
        {"target: Optional[str]\ndef caller():\n    return target()\n", false},
        {"def caller(value: Optional[str]):\n    pair = 1, "
         "globals().__setitem__('target', None)\n    return target()\n",
         false},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char source[1024];
        snprintf(source, sizeof(source),
                 "from typing import Optional, Tuple\ndef target():\n    return 1\n%s",
                 cases[i].tail);
        CBMFileResult *file = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON,
                                               "containers", "main.py", 0, NULL, NULL);
        ASSERT_NOT_NULL(file);
        ASSERT(!file->has_error && !file->parse_incomplete && !file->parse_unusable);
        CBMArena arena;
        cbm_arena_init(&arena);
        cbm_resolve_binding_index_t *index =
            cbm_resolve_binding_build(&arena, file, CBM_LANG_PYTHON, source, strlen(source));
        const char *site = strstr(source, "return target()");
        ASSERT_NOT_NULL(site);
        uint32_t start = (uint32_t)(site + strlen("return ") - source);
        cbm_resolve_result_t proof = cbm_resolve_binding_local(
            index, "target", start, start + strlen("target()"), CBM_SOURCE_ORIGIN_RAW);
        bool exact = cbm_resolve_admit(&proof) && proof.proof && proof.target &&
                     !strcmp(proof.target, "containers.main.target");
        if (exact != cases[i].exact)
            fprintf(stderr, "Python container case=%zu status=%s exact=%d\n", i,
                    cbm_resolve_binding_status(index), exact);
        cbm_arena_destroy(&arena);
        cbm_free_result(file);
        ASSERT_EQ(exact, cases[i].exact);
    }
    PASS();
}

static bool python_physical_package_golden(const char *manifest, bool source_layout,
                                           bool explicit_src, int driver) {
    static const char library[] = "from typing import Optional, Tuple\n"
                                  "def target(value: Optional[str] = None) -> Tuple[str, str]:\n"
                                  "    return '', ''\n";
    static const char decoy[] = "def target():\n    return 99\n";
    const char *library_path = source_layout ? "src/sample/utils.py" : "sample/utils.py";
    const char *decoy_path = source_layout ? "decoy/utils.py" : "src/sample/utils.py";
    const char *initializer = source_layout ? "src/sample/__init__.py" : "sample/__init__.py";
    bool supported = !source_layout || explicit_src;
    char main[256];
    snprintf(main, sizeof(main),
             "from %ssample.utils import target\ndef caller():\n    return target()\n"
             "def local():\n    return 1\ndef control():\n    return local()\n",
             explicit_src ? "src." : "");
    const RQ_Source sources[] = {{.path = "main.py", .text = main},
                                 {.path = library_path, .text = library},
                                 {.path = initializer, .text = ""},
                                 {.path = decoy_path, .text = decoy}};
    const RQ_Expected expected[] = {
        {.case_id = "physical-python-package-local-control",
         .site =
             {.file = "main.py", .caller = "control", .needle = "local()", .occurrence_index = 1},
         .relation = "CALLS",
         .target = {.file = "main.py",
                    .suffix = "main.local",
                    .label = "Function",
                    .declaration_line = 4,
                    .declaration_text = "def local():"}},
        {.case_id = "physical-python-package-exact-imported-function",
         .site = {.file = "main.py", .caller = "caller", .needle = "target()"},
         .relation = "CALLS",
         .target = {.file = library_path,
                    .suffix = source_layout ? "src.sample.utils.target" : "sample.utils.target",
                    .label = "Function",
                    .declaration_line = 2,
                    .declaration_text =
                        "def target(value: Optional[str] = None) -> Tuple[str, str]:"}}};
    const RQ_Forbidden forbidden = {
        .case_id = "distribution-name-does-not-authorize-src-layout",
        .site = {.file = "main.py", .caller = "caller", .needle = "target()"},
        .relation = "CALLS",
        .any_target = true};
    const char *root = th_mktempdir("cbm_python_physical_package");
    if (!root)
        return false;
    char repo[768], database[768];
    snprintf(repo, sizeof(repo), "%s/repo", root);
    snprintf(database, sizeof(database), "%s/index.db", root);
    bool ok = cbm_mkdir_p(repo, 0755) && cbm_mkdir_p(TH_PATH(repo, "sample"), 0755) &&
              cbm_mkdir_p(TH_PATH(repo, "src/sample"), 0755) &&
              cbm_mkdir_p(TH_PATH(repo, "decoy"), 0755);
    for (size_t i = 0; ok && i < sizeof(sources) / sizeof(sources[0]); i++)
        ok = th_write_file(TH_PATH(repo, sources[i].path), sources[i].text) == 0;
    if (ok && manifest)
        ok = th_write_file(TH_PATH(repo, "pyproject.toml"), manifest) == 0;
    for (int i = 0; ok && i < 55; i++) {
        char path[32];
        snprintf(path, sizeof(path), "filler%02d.py", i);
        ok = th_write_file(TH_PATH(repo, path), "# unrelated source\n") == 0;
    }
    const char *old_workers = getenv("CBM_WORKERS");
    char *saved_workers = old_workers ? strdup(old_workers) : NULL;
    ok = ok && (!old_workers || saved_workers);
    cbm_setenv("CBM_WORKERS", driver ? "4" : "1", 1);
    cbm_pipeline_t *pipeline = ok ? cbm_pipeline_new(repo, database, CBM_MODE_FULL) : NULL;
    ok = pipeline && cbm_pipeline_set_project_name(pipeline, "physical-python") &&
         cbm_pipeline_run(pipeline) == 0;
    cbm_pipeline_free(pipeline);
    if (saved_workers) {
        cbm_setenv("CBM_WORKERS", saved_workers, 1);
        free(saved_workers);
    } else
        cbm_unsetenv("CBM_WORKERS");
    cbm_store_t *store = ok ? cbm_store_open_path_existing(database) : NULL;
    RQ_Report report = {0};
    ok = store &&
         rq_check(store, "physical-python", "python-physical-package", sources,
                  sizeof(sources) / sizeof(sources[0]), expected, supported ? 2 : 1,
                  supported ? NULL : &forbidden, supported ? 0 : 1, &report, stderr) &&
         report.genuine == (supported ? 2u : 1u) && report.found == report.genuine &&
         !report.wrong && !report.missing && ok;
    if (store) {
        /* IMPORTS records the physical dependency, separately from the local
         * imported callable binding. Every emitted row must project to Files. */
        const char *sql = "SELECT count(*) FROM resolver_occurrences r JOIN nodes s ON "
                          "s.project=r.project AND s.qualified_name=r.scope JOIN nodes t ON "
                          "t.project=r.project AND t.qualified_name=r.target JOIN edges e ON "
                          "e.source_id=s.id AND e.target_id=t.id AND e.type=r.relation "
                          "WHERE r.project='physical-python' AND r.relation='IMPORTS' AND "
                          "r.emitted=1 "
                          "AND s.file_path='main.py' AND s.label='File' AND t.label='File'";
        ok = generic_count(store, sql) == (supported ? 1 : 0) && ok;
        ok = generic_count(store, "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id "
                                  "JOIN nodes t ON t.id=e.target_id WHERE e.type='IMPORTS' "
                                  "AND s.file_path='main.py' AND t.label!='File'") == 0 &&
             ok;
        cbm_store_close(store);
    }
    th_cleanup(root);
    return ok;
}

TEST(resolver_binding_python_distribution_name_is_not_import_root) {
    static const char manifest[] = "[project]\nname = 'sample'\nversion = '1.0'\n";
    static const char other_distribution[] = "[project]\nname = 'unrelated-dist'\n";
    for (int driver = 0; driver < 2; driver++) {
        ASSERT(python_physical_package_golden(NULL, false, false, driver));
        ASSERT(python_physical_package_golden(manifest, false, false, driver));
        ASSERT(python_physical_package_golden(other_distribution, false, false, driver));
        ASSERT(python_physical_package_golden(manifest, true, false, driver));
        ASSERT(python_physical_package_golden(manifest, true, true, driver));
    }
    PASS();
}

/* A captured DEFAULT path is syntax, not an audited dependency adapter.
 * These genuine literal imports stay in the recall denominator; the shell
 * expansion additionally demonstrates a wrong concrete target, independently
 * of the missing-adapter quality gaps. */
TEST(resolver_binding_unsupported_import_carriers_do_not_authorize_paths) {
    static const RQ_Source sources[] = {
        {.path = "main.jsonnet", .text = "local utils = import './utils.libsonnet';\nutils\n"},
        {.path = "utils.libsonnet", .text = "{ value: 1 }\n"},
        {.path = "main.ncl", .text = "let utils = import \"./utils.ncl\" in\nutils\n"},
        {.path = "utils.ncl", .text = "{ value = 1 }\n"},
        {.path = "main.sh", .text = "name=actual\nsource \"./${name}.sh\"\n"},
        {.path = "actual.sh", .text = "readonly actual=1\n"},
        {.path = "${name}.sh", .text = "readonly decoy=1\n"},
        {.path = "control.js",
         .text = "export function target() { return 1; }\n"
                 "export function caller() { return target(); }\n"}};
    static const struct {
        size_t source;
        CBMLanguage language;
        const char *path;
    } carriers[] = {{0, CBM_LANG_JSONNET, "./utils.libsonnet"},
                    {2, CBM_LANG_NICKEL, "./utils.ncl"},
                    {4, CBM_LANG_BASH, "./${name}.sh"}};
    for (size_t i = 0; i < sizeof(carriers) / sizeof(carriers[0]); i++) {
        const RQ_Source *source = &sources[carriers[i].source];
        CBMFileResult *file =
            cbm_extract_file(source->text, (int)strlen(source->text), carriers[i].language,
                             "unsupported-imports", source->path, 0, NULL, NULL);
        ASSERT_NOT_NULL(file);
        ASSERT(!file->has_error && !file->parse_incomplete && !file->parse_unusable);
        ASSERT_EQ(file->imports.count, 1);
        ASSERT_EQ(file->imports.items[0].kind, CBM_IMPORT_KIND_DEFAULT);
        ASSERT_STR_EQ(file->imports.items[0].module_path, carriers[i].path);
        cbm_free_result(file);
    }
    static const RQ_Expected expected[] = {
        {.case_id = "jsonnet-explicit-relative-import-missing-adapter",
         .site = {.file = "main.jsonnet",
                  .caller = "main.jsonnet.__file__",
                  .needle = "import './utils.libsonnet'"},
         .relation = "IMPORTS",
         .target = {.file = "utils.libsonnet",
                    .suffix = "utils.libsonnet.__file__",
                    .label = "File",
                    .declaration_line = 1,
                    .declaration_text = "{ value: 1 }"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "jsonnet-dependency-adapter-missing",
         .why = "The source-relative literal import is genuine, but no audited "
                "Jsonnet dependency "
                "adapter supplies occurrence and path-scope evidence.",
         .tried = "Required the actual DEFAULT extractor carrier with the exact "
                  "./ path and kept "
                  "the physical target source; generic path matching is "
                  "insufficient proof."},
        {.case_id = "nickel-explicit-relative-import-missing-adapter",
         .site = {.file = "main.ncl",
                  .caller = "main.ncl.__file__",
                  .needle = "import \"./utils.ncl\""},
         .relation = "IMPORTS",
         .target = {.file = "utils.ncl",
                    .suffix = "utils.ncl.__file__",
                    .label = "File",
                    .declaration_line = 1,
                    .declaration_text = "{ value = 1 }"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "nickel-dependency-adapter-missing",
         .why = "The source-relative literal import is genuine, but no audited "
                "Nickel dependency "
                "adapter supplies occurrence and path-scope evidence.",
         .tried = "Required the actual DEFAULT extractor carrier with the exact "
                  "./ path and kept "
                  "the physical target source; generic path matching is "
                  "insufficient proof."},
        {.case_id = "supported-esm-control-remains-required",
         .site = {.file = "control.js",
                  .caller = "control.caller",
                  .needle = "target()",
                  .occurrence_index = 1},
         .relation = "CALLS",
         .target = {.file = "control.js",
                    .suffix = "control.target",
                    .label = "Function",
                    .declaration_line = 1,
                    .declaration_text = "export function target() { return 1; }"}}};
    /* Expansion establishes the basename actual.sh, but the source captures
     * no runtime working directory. Do not infer that it imports the repository
     * actual.sh or count an invented source-relative obligation. The literal
     * ${name}.sh decoy is nevertheless wrong for every working directory. */
    static const RQ_Forbidden forbidden = {
        .case_id = "shell-expanded-variable-never-names-literal-dollar-file",
        .site = {.file = "main.sh",
                 .caller = "main.sh.__file__",
                 .needle = "source \"./${name}.sh\""},
        .relation = "IMPORTS",
        .target = {.file = "${name}.sh",
                   .suffix = "${name}.sh.__file__",
                   .label = "File",
                   .declaration_line = 1,
                   .declaration_text = "readonly decoy=1"}};
    for (int driver = 0; driver < 2; driver++) {
        const char *root = th_mktempdir("cbm_unsupported_imports");
        ASSERT_NOT_NULL(root);
        char repo[768], database[768];
        snprintf(repo, sizeof(repo), "%s/repo", root);
        snprintf(database, sizeof(database), "%s/index.db", root);
        bool ok = cbm_mkdir_p(repo, 0755);
        for (size_t i = 0; ok && i < sizeof(sources) / sizeof(sources[0]); i++)
            ok = th_write_file(TH_PATH(repo, sources[i].path), sources[i].text) == 0;
        for (int i = 0; ok && i < 55; i++) {
            char path[32];
            snprintf(path, sizeof(path), "filler%02d.py", i);
            ok = th_write_file(TH_PATH(repo, path), "# unrelated source\n") == 0;
        }
        const char *old_workers = getenv("CBM_WORKERS");
        char *saved_workers = old_workers ? strdup(old_workers) : NULL;
        ok = ok && (!old_workers || saved_workers);
        cbm_setenv("CBM_WORKERS", driver ? "4" : "1", 1);
        cbm_pipeline_t *pipeline = ok ? cbm_pipeline_new(repo, database, CBM_MODE_FULL) : NULL;
        ok = pipeline && cbm_pipeline_set_project_name(pipeline, "unsupported-imports") &&
             cbm_pipeline_run(pipeline) == 0;
        cbm_pipeline_free(pipeline);
        if (saved_workers) {
            cbm_setenv("CBM_WORKERS", saved_workers, 1);
            free(saved_workers);
        } else
            cbm_unsetenv("CBM_WORKERS");
        cbm_store_t *store = ok ? cbm_store_open_path_existing(database) : NULL;
        RQ_Report report = {0};
        ok = store &&
             rq_check(store, "unsupported-imports", "unsupported-import-path-gate", sources,
                      sizeof(sources) / sizeof(sources[0]), expected,
                      sizeof(expected) / sizeof(expected[0]), &forbidden, 1, &report, stderr) &&
             report.genuine == 3 && report.found == 1 && report.missing == 2 &&
             !report.required_missing && !report.wrong && ok;
        if (store) {
            /* Capture survives abstention, so unavailable adapters are visible
             * in quality data instead of silently losing the import carriers. */
            ok = generic_count(store, "SELECT count(*) FROM resolver_occurrences WHERE "
                                      "project='unsupported-imports' "
                                      "AND relation='IMPORTS' AND emitted=0 AND target='' "
                                      "AND reason='unsupported_import_language' AND file_path IN "
                                      "('main.jsonnet','main.ncl','main.sh')") == 3 &&
                 ok;
            ok = generic_count(store,
                               "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id "
                               "WHERE e.type='IMPORTS' AND s.file_path IN "
                               "('main.jsonnet','main.ncl','main.sh')") == 0 &&
                 ok;
            cbm_store_close(store);
        }
        th_cleanup(root);
        if (!ok)
            fprintf(stderr,
                    "Unsupported import gate driver=%d genuine=%zu found=%zu missing=%zu "
                    "wrong=%zu required_missing=%zu\n",
                    driver, report.genuine, report.found, report.missing, report.wrong,
                    report.required_missing);
        ASSERT(ok);
    }
    PASS();
}

TEST(resolver_binding_dart_import_requires_complete_direct_uri) {
    static const RQ_Source sources[] = {
        {.path = "direct.dart", .text = "import './stub.dart';\n"},
        {.path = "conditional.dart",
         .text = "import './stub.dart' if (dart.library.io) './real.dart';\n"},
        {.path = "exported.dart", .text = "export './stub.dart';\n"},
        {.path = "escaped.dart", .text = "import './\\u0073tub.dart';\n"},
        {.path = "stub.dart", .text = "int value = 1;\n"},
        {.path = "real.dart", .text = "int value = 2;\n"},
        {.path = "control.js",
         .text = "export function target() { return 1; }\n"
                 "export function caller() { return target(); }\n"}};
    static const char *const raw_paths[] = {"./stub.dart", "./stub.dart", "./stub.dart",
                                            "./\\u0073tub.dart"};
    for (size_t i = 0; i < 4; i++) {
        CBMFileResult *file =
            cbm_extract_file(sources[i].text, (int)strlen(sources[i].text), CBM_LANG_DART,
                             "dart-uri", sources[i].path, 0, NULL, NULL);
        ASSERT_NOT_NULL(file);
        ASSERT(!file->has_error && !file->parse_incomplete && !file->parse_unusable);
        ASSERT_EQ(file->imports.count, 1);
        ASSERT_STR_EQ(file->imports.items[0].module_path, raw_paths[i]);
        if (i)
            ASSERT_EQ(file->imports.items[0].kind, CBM_IMPORT_KIND_DEFAULT);
        cbm_free_result(file);
    }
    static const RQ_Expected expected[] = {
        {.case_id = "dart-export-dependency-remains-a-counted-gap",
         .site = {.file = "exported.dart",
                  .caller = "exported.dart.__file__",
                  .needle = "export './stub.dart';"},
         .relation = "IMPORTS",
         .target = {.file = "stub.dart",
                    .suffix = "stub.dart.__file__",
                    .label = "File",
                    .declaration_line = 1,
                    .declaration_text = "int value = 1;"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "dart-export-dependency-proof-missing",
         .why = "The exported library is a genuine dependency, but the audited "
                "adapter covers "
                "direct imports only; export namespace and occurrence proofs "
                "remain absent.",
         .tried = "Preserved the export carrier and exact physical file. A "
                  "descendant string "
                  "must not borrow the direct-import proof."},
        {.case_id = "dart-escaped-uri-dependency-remains-a-counted-gap",
         .site = {.file = "escaped.dart",
                  .caller = "escaped.dart.__file__",
                  .needle = "import './\\u0073tub.dart';"},
         .relation = "IMPORTS",
         .target = {.file = "stub.dart",
                    .suffix = "stub.dart.__file__",
                    .label = "File",
                    .declaration_line = 1,
                    .declaration_text = "int value = 1;"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "dart-uri-escape-decoding-proof-missing",
         .why = "Dart's Unicode escape spells stub.dart, but this adapter has no "
                "escape-decoding "
                "certificate and must retain the raw token as unproven.",
         .tried = "Kept the complete escaped URI and the decoded source target. "
                  "No substring "
                  "or raw-backslash filename is accepted as an equivalent URI."},
        {.case_id = "required-same-pipeline-esm-control",
         .site = {.file = "control.js",
                  .caller = "control.caller",
                  .needle = "target()",
                  .occurrence_index = 1},
         .relation = "CALLS",
         .target = {.file = "control.js",
                    .suffix = "control.target",
                    .label = "Function",
                    .declaration_line = 1,
                    .declaration_text = "export function target() { return 1; }"}}};
    static const RQ_Forbidden forbidden = {
        .case_id = "conditional-import-does-not-select-a-default-without-configuration",
        .site = {.file = "conditional.dart",
                 .caller = "conditional.dart.__file__",
                 .needle = "import './stub.dart' if (dart.library.io) './real.dart';"},
        .relation = "IMPORTS",
        .any_target = true};
    for (int driver = 0; driver < 2; driver++) {
        const char *root = th_mktempdir("cbm_dart_uri_proof");
        ASSERT_NOT_NULL(root);
        char repo[768], database[768];
        snprintf(repo, sizeof(repo), "%s/repo", root);
        snprintf(database, sizeof(database), "%s/index.db", root);
        bool ok = cbm_mkdir_p(repo, 0755);
        for (size_t i = 0; ok && i < sizeof(sources) / sizeof(sources[0]); i++)
            ok = th_write_file(TH_PATH(repo, sources[i].path), sources[i].text) == 0;
        for (int i = 0; ok && i < 55; i++) {
            char path[32];
            snprintf(path, sizeof(path), "filler%02d.py", i);
            ok = th_write_file(TH_PATH(repo, path), "# unrelated source\n") == 0;
        }
        const char *old_workers = getenv("CBM_WORKERS");
        char *saved_workers = old_workers ? strdup(old_workers) : NULL;
        ok = ok && (!old_workers || saved_workers);
        cbm_setenv("CBM_WORKERS", driver ? "4" : "1", 1);
        cbm_pipeline_t *pipeline = ok ? cbm_pipeline_new(repo, database, CBM_MODE_FULL) : NULL;
        ok = pipeline && cbm_pipeline_set_project_name(pipeline, "dart-uri") &&
             cbm_pipeline_run(pipeline) == 0;
        cbm_pipeline_free(pipeline);
        if (saved_workers) {
            cbm_setenv("CBM_WORKERS", saved_workers, 1);
            free(saved_workers);
        } else
            cbm_unsetenv("CBM_WORKERS");
        cbm_store_t *store = ok ? cbm_store_open_path_existing(database) : NULL;
        RQ_Report report = {0};
        ok = store &&
             rq_check(store, "dart-uri", "dart-import-uri-proof", sources,
                      sizeof(sources) / sizeof(sources[0]), expected,
                      sizeof(expected) / sizeof(expected[0]), &forbidden, 1, &report, stderr) &&
             report.genuine == 3 && report.found == 1 && report.missing == 2 &&
             !report.required_missing && !report.wrong && ok;
        if (store) {
            /* The required direct IMPORTS control now carries the URI literal's exact
             * bytes, which this pins along with the physical endpoints and agreement
             * between the row and the edge. It used to carry 0/0 legacy coordinates. */
            ok = generic_count(store,
                               "SELECT count(*) FROM resolver_occurrences r JOIN nodes s ON "
                               "s.project=r.project AND s.qualified_name=r.scope JOIN nodes t "
                               "ON "
                               "t.project=r.project AND t.qualified_name=r.target JOIN edges e "
                               "ON "
                               "e.source_id=s.id AND e.target_id=t.id AND e.type=r.relation "
                               "WHERE r.project='dart-uri' AND r.relation='IMPORTS' AND "
                               "r.emitted=1 "
                               "AND r.start_byte=7 AND r.end_byte=20 AND "
                               "s.file_path='direct.dart' "
                               "AND s.label='File' AND t.file_path='stub.dart' AND "
                               "t.label='File'") == 1 &&
                 ok;
            ok = generic_count(store, "SELECT count(*) FROM resolver_occurrences WHERE "
                                      "project='dart-uri' "
                                      "AND relation='IMPORTS' AND emitted=0 AND target='' "
                                      "AND file_path IN "
                                      "('conditional.dart','exported.dart','escaped.dart')") == 3 &&
                 ok;
            ok = generic_count(store,
                               "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id "
                               "WHERE e.type='IMPORTS' AND s.file_path IN "
                               "('conditional.dart','exported.dart','escaped.dart')") == 0 &&
                 ok;
            cbm_store_close(store);
        }
        th_cleanup(root);
        if (!ok)
            fprintf(stderr,
                    "Dart URI proof driver=%d genuine=%zu found=%zu missing=%zu "
                    "wrong=%zu\n",
                    driver, report.genuine, report.found, report.missing, report.wrong);
        ASSERT(ok);
    }
    PASS();
}

SUITE(resolver_binding_generic) {
    RUN_TEST(resolver_binding_python_empty_occurrences_preserve_negative_evidence);
    RUN_TEST(resolver_binding_dart_import_requires_complete_direct_uri);
    RUN_TEST(resolver_binding_unsupported_import_carriers_do_not_authorize_paths);
    RUN_TEST(resolver_binding_python_annotation_containers_and_parameter_separators);
    RUN_TEST(resolver_binding_python_distribution_name_is_not_import_root);
    RUN_TEST(resolver_binding_component_scripts_exact_local_scope);
    RUN_TEST(resolver_binding_component_scripts_scope_and_write_barriers);
    RUN_TEST(resolver_binding_python_annotation_only_raw_roles);
    RUN_TEST(resolver_binding_python_annotation_scope_and_effects);
    RUN_TEST(resolver_binding_python_annotation_pipeline_roles_and_exports);
    RUN_TEST(resolver_binding_ecmascript_parameters_have_exact_lexical_owners);
    RUN_TEST(resolver_binding_typescript_parameter_annotations_preserve_import_identity);
    RUN_TEST(resolver_binding_http_import_occurrence_uses_lexical_owner);
    RUN_TEST(resolver_binding_python_parenthesized_arguments_keep_exact_identity);
    RUN_TEST(resolver_binding_python_literal_formatting_preserves_exact_bindings);
    RUN_TEST(resolver_binding_python_formatting_keeps_module_escape_and_rebinding_guards);
    RUN_TEST(resolver_binding_python_literal_formatting_crossfile_and_effects);
    RUN_TEST(resolver_binding_python_try_preserves_scope_and_effect_barriers);
    RUN_TEST(resolver_binding_python_try_crossfile_and_conditional_rebinding);
    RUN_TEST(resolver_binding_python_ordinary_lua_names_are_lexical);
    RUN_TEST(resolver_binding_language_dynamic_controls_remain_blocked);
    RUN_TEST(resolver_binding_python_crossfile_language_names_and_real_effects);
    RUN_TEST(resolver_binding_dart_declaration_ranges_include_exact_body);
    RUN_TEST(resolver_binding_dart_closed_missing_member_has_imported_source_proof);
    RUN_TEST(resolver_binding_dart_visibility_excludes_unrelated_open_import);
    RUN_TEST(resolver_binding_dart_unsupported_competitor_keeps_source_observation);
    RUN_TEST(resolver_binding_rust_exact_names_and_blockers);
    RUN_TEST(resolver_binding_dart_source_namespace_and_privacy);
    RUN_TEST(resolver_binding_dart_import_source_combinators);
    RUN_TEST(resolver_binding_dart_duplicate_uri_preserves_occurrence_authority);
    RUN_TEST(resolver_binding_generic_unmodeled_library_context_is_not_complete);
    RUN_TEST(resolver_binding_dart_cross_file_exact_namespace_and_decoy);
    RUN_TEST(resolver_binding_dart_cross_file_parallel_namespace);
    RUN_TEST(resolver_binding_dart_cross_file_conflicts_and_missing_namespace);
    RUN_TEST(resolver_binding_dart_cross_file_shadows_and_reassignment);
}
