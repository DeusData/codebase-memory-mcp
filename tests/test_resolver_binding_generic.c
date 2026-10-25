#include "cbm.h"
#include "resolve/binding.h"
#include "resolve/policy.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "store/store.h"
#include "sqlite3.h"
#include "test_framework.h"
#include "test_helpers.h"

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
    cbm_resolve_binding_index_t *swift =
        cbm_resolve_binding_build(&arena, NULL, CBM_LANG_SWIFT, "", 0);
    ASSERT_STR_EQ(cbm_resolve_binding_status(swift), "swift_build_module_membership_required");
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
        {"import 'lib.dart';\nimport 'missing.dart';\nvoid caller() { target(); }\n", NULL,
         GENERIC_PROJECT ".lib.target", false},
        {"import 'lib.dart';\nimport 'other.dart';\nvoid caller() { target(); }\n",
         "var target = 2;\n", GENERIC_PROJECT ".lib.target", false},
        {"import 'lib.dart';\nvoid caller() { _target(); }\n", NULL, GENERIC_PROJECT ".lib._target",
         false}};
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

SUITE(resolver_binding_generic) {
    RUN_TEST(resolver_binding_dart_closed_missing_member_has_imported_source_proof);
    RUN_TEST(resolver_binding_dart_visibility_excludes_unrelated_open_import);
    RUN_TEST(resolver_binding_dart_unsupported_competitor_keeps_source_observation);
    RUN_TEST(resolver_binding_rust_exact_names_and_blockers);
    RUN_TEST(resolver_binding_dart_source_namespace_and_privacy);
    RUN_TEST(resolver_binding_dart_import_source_combinators);
    RUN_TEST(resolver_binding_generic_unmodeled_library_context_is_not_complete);
    RUN_TEST(resolver_binding_dart_cross_file_exact_namespace_and_decoy);
    RUN_TEST(resolver_binding_dart_cross_file_parallel_namespace);
    RUN_TEST(resolver_binding_dart_cross_file_conflicts_and_missing_namespace);
    RUN_TEST(resolver_binding_dart_cross_file_shadows_and_reassignment);
}
