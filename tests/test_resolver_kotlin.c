#include "test_framework.h"
#include "test_helpers.h"
#include "cbm.h"
#include "lsp/kotlin_proof.h"
#include "resolve/policy.h"
#include "pipeline/pipeline.h"
#include "store/store.h"
#include "sqlite3.h"

static const char KOTLIN_SERVICE[] =
    "package api\nclass Service {\n fun go() {}\n}\nfun work() {}\n";

static int kotlin_rows(const char *source, const char *library, int competitor, bool types) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMLSPDef defs[5] = {{.qualified_name = "api.Service",
                          .graph_qualified_name = "p.lib.Service",
                          .short_name = "Service",
                          .label = "Class",
                          .source_start_line = 2,
                          .declaration_identity = "lib:0"},
                         {.qualified_name = "api.Service.go",
                          .graph_qualified_name = "p.lib.Service.go",
                          .short_name = "go",
                          .receiver_type = "api.Service",
                          .label = "Method",
                          .source_start_line = 3,
                          .declaration_identity = "lib:1"},
                         {.qualified_name = "api.work",
                          .graph_qualified_name = "p.lib.work",
                          .short_name = "work",
                          .label = "Function",
                          .source_start_line = 5,
                          .declaration_identity = "lib:2"}};
    bool facts =
        cbm_kotlin_collect_source_facts(&arena, library, (int)strlen(library), "p.lib", defs, 3);
    int count = 3;
    if (competitor == 1) {
        defs[3] = defs[1];
        defs[3].declaration_identity = "other:1";
        count = 4;
    } else if (competitor == 2) {
        defs[3] =
            (CBMLSPDef){.qualified_name = "api.work", .short_name = "work", .label = "Variable"};
        count = 4;
    } else if (competitor == 3) {
        defs[3] = defs[0];
        defs[3].declaration_identity = "other:0";
        count = 4;
    }
    CBMTypeRegistry registry;
    cbm_registry_init(&registry, &arena);
    cbm_kotlin_register_lsp_defs(&arena, &registry, defs, count, NULL);
    cbm_registry_finalize(&registry);
    CBMFileResult *file = cbm_extract_file(source, (int)strlen(source), CBM_LANG_KOTLIN, "p",
                                           "Main.kt", 0, NULL, NULL);
    TSParser *parser = ts_parser_new();
    extern const TSLanguage *tree_sitter_kotlin(void);
    ts_parser_set_language(parser, tree_sitter_kotlin());
    TSTree *tree = ts_parser_parse_string(parser, NULL, source, (uint32_t)strlen(source));
    ts_parser_delete(parser);
    CBMResolvedCallArray out = {0};
    if (facts && file && tree)
        cbm_kotlin_proof_resolve(&arena, source, (int)strlen(source), ts_tree_root_node(tree),
                                 &registry, file, &out);
    int found = 0;
    for (int i = 0; i < out.count; i++) {
        CBMResolvedCall *row = &out.items[i];
        cbm_resolve_result_t proof = {.binding = CBM_RESOLVE_BINDING_UNIQUE,
                                      .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                      .analysis = row->evidence.analysis,
                                      .rule = row->evidence.rule,
                                      .facts = row->evidence.facts,
                                      .target = row->callee_qn,
                                      .candidate_count = 1};
        if (cbm_resolve_admit(&proof) && row->site_end_byte > row->site_start_byte &&
            (types || row->kind == CBM_RESOLVED_INVOCATION))
            found++;
    }
    if (tree)
        ts_tree_delete(tree);
    cbm_free_result(file);
    cbm_arena_destroy(&arena);
    return found;
}

TEST(resolver_kotlin_import_alias_receiver_and_free_function) {
    const char *source = "package client\nimport api.Service as Svc\nimport api.work as execute\n"
                         "fun run(value: Svc) { value.go(); execute() }\n";
    ASSERT_EQ(kotlin_rows(source, KOTLIN_SERVICE, 0, false), 2);
    ASSERT_EQ(kotlin_rows(source, KOTLIN_SERVICE, 0, true), 3);
    const char *same_package = "package api\nfun run(value: Service) { value.go(); work() }\n";
    ASSERT_EQ(kotlin_rows(same_package, KOTLIN_SERVICE, 0, false), 2);
    PASS();
}

TEST(resolver_kotlin_shadowed_and_unknown_receiver_abstain) {
    const char *sources[] = {
        ("package client\nimport api.Service\nfun run(value: Unknown) { value.go() }\n"),
        ("package client\nimport api.Service\nfun <Service> run(value: Service) { value.go() }\n"),
        ("package client\nimport api.Service\nfun run(value: Service?) { value.go() }\n"),
        ("package client\nimport api.Service\nfun run(value: Service) { val value=unknown(); "
         "value.go() }\n"),
        ("package client\nimport api.Service\nfun run(value: Service) { value.go(1) }\n"),
        ("package client\nimport api.Service\nfun Service.run(value: Service) { value.go() }\n"),
        ("package client\nimport api.work\nfun run(work: () -> Unit) { work() }\n"),
        ("package client\nimport api.work\nfun run() { val work=other; work() }\n"),
        ("package client\nimport api.work\nclass Main { fun work() {}\nfun run() { work() } }\n"),
        ("package client\nimport api.work\nfun run() { fun work() {}; work() }\n"),
        ("package client\nimport api.*\nfun run() { work() }\n")};
    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++) {
        int found = kotlin_rows(sources[i], KOTLIN_SERVICE, 0, false);
        if (found)
            fprintf(stderr, "Kotlin unexpected proof scenario %zu: %d\n", i, found);
        ASSERT_EQ(found, 0);
    }
    PASS();
}

TEST(resolver_kotlin_private_inherited_and_competing_declarations_abstain) {
    const char *source =
        "package client\nimport api.Service\nfun run(value: Service) { value.go() }\n";
    const char *private_source =
        "package api\nclass Service {\n private fun go() {}\n}\nfun work() {}\n";
    const char *inherited =
        "package api\nclass Service : Parent() {\n fun go() {}\n}\nfun work() {}\n";
    ASSERT_EQ(kotlin_rows(source, private_source, 0, false), 0);
    ASSERT_EQ(kotlin_rows(source, inherited, 0, false), 0);
    ASSERT_EQ(kotlin_rows(source, KOTLIN_SERVICE, 1, false), 0);
    ASSERT_EQ(kotlin_rows(source, KOTLIN_SERVICE, 3, false), 0);
    const char *free_call = "package client\nimport api.work\nfun run() { work() }\n";
    ASSERT_EQ(kotlin_rows(free_call, KOTLIN_SERVICE, 2, false), 0);
    PASS();
}

TEST(resolver_kotlin_pipeline_preserves_exact_crossfile_target) {
    const char *root = th_mktempdir("cbm_resolver_kotlin");
    ASSERT_NOT_NULL(root);
    char repo[768], database[768];
    snprintf(repo, sizeof(repo), "%s/repo", root);
    snprintf(database, sizeof(database), "%s/index.db", root);
    ASSERT(cbm_mkdir_p(repo, 0755));
    bool files =
        th_write_file(TH_PATH(repo, "Service.kt"), KOTLIN_SERVICE) == 0 &&
        th_write_file(
            TH_PATH(repo, "Main.kt"),
            "package client\nimport api.Service\nfun run(value: Service) { value.go() }\n") == 0 &&
        th_write_file(TH_PATH(repo, "Decoy.kt"),
                      "package other\nclass Service { fun go() {} }\n") == 0;
    cbm_pipeline_t *pipeline = files ? cbm_pipeline_new(repo, database, CBM_MODE_FULL) : NULL;
    int rc = pipeline && cbm_pipeline_set_project_name(pipeline, "kotlin-proof")
                 ? cbm_pipeline_run(pipeline)
                 : -1;
    cbm_pipeline_free(pipeline);
    cbm_store_t *store = rc == 0 ? cbm_store_open_path_existing(database) : NULL;
    sqlite3_stmt *statement = NULL;
    int edges = -1;
    if (store &&
        sqlite3_prepare_v2(
            cbm_store_get_db(store),
            "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON "
            "t.id=e.target_id "
            "WHERE e.type='CALLS' AND s.file_path='Main.kt' AND t.file_path='Service.kt'",
            -1, &statement, NULL) == SQLITE_OK &&
        sqlite3_step(statement) == SQLITE_ROW)
        edges = sqlite3_column_int(statement, 0);
    sqlite3_finalize(statement);
    if (store && edges != 1) {
        statement = NULL;
        if (sqlite3_prepare_v2(cbm_store_get_db(store),
                               "SELECT scope,spelling,target,reason,facts FROM "
                               "resolver_occurrences WHERE relation='CALLS'",
                               -1, &statement, NULL) == SQLITE_OK)
            while (sqlite3_step(statement) == SQLITE_ROW) {
                fprintf(stderr, "Kotlin pipeline diagnostic:");
                for (int c = 0; c < sqlite3_column_count(statement); c++)
                    fprintf(stderr, " %s",
                            sqlite3_column_text(statement, c)
                                ? (const char *)sqlite3_column_text(statement, c)
                                : "NULL");
                fprintf(stderr, "\n");
            }
        sqlite3_finalize(statement);
    }
    if (store)
        cbm_store_close(store);
    th_cleanup(root);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(edges, 1);
    PASS();
}

/* The oracle counts every outgoing controlled callable edge from Main.kt,
 * including accidental decoy targets. It does not infer success from strategy
 * names or from the resolver's own candidate counters. */
static bool kotlin_pipeline_counts_project(const char *project, const char *main_source,
                                           const char *library, const char *extra, int *calls,
                                           int *references) {
    const char *root = th_mktempdir("cbm_kotlin_binding");
    if (!root)
        return false;
    char repo[768], database[768];
    snprintf(repo, sizeof(repo), "%s/repo", root);
    snprintf(database, sizeof(database), "%s/index.db", root);
    bool files =
        cbm_mkdir_p(repo, 0755) && th_write_file(TH_PATH(repo, "Main.kt"), main_source) == 0 &&
        th_write_file(TH_PATH(repo, "Library.kt"), library) == 0 &&
        th_write_file(TH_PATH(repo, "Decoy.kt"), "package unrelated\nfun handler() {}\nfun "
                                                 "consume(value: Int) {}\n") == 0 &&
        (!extra || th_write_file(TH_PATH(repo, "Extra.kt"), extra) == 0);
    cbm_pipeline_t *pipeline = files ? cbm_pipeline_new(repo, database, CBM_MODE_FULL) : NULL;
    int rc = pipeline && cbm_pipeline_set_project_name(pipeline, project)
                 ? cbm_pipeline_run(pipeline)
                 : -1;
    cbm_pipeline_free(pipeline);
    cbm_store_t *store = rc == 0 ? cbm_store_open_path_existing(database) : NULL;
    sqlite3_stmt *statement = NULL;
    bool ok = store &&
              sqlite3_prepare_v2(cbm_store_get_db(store),
                                 "SELECT COALESCE(sum(e.type='CALLS'),0), "
                                 "COALESCE(sum(e.type='CALL_REFERENCE'),0) "
                                 "FROM edges e JOIN nodes s ON s.id=e.source_id "
                                 "WHERE s.file_path='Main.kt' "
                                 "AND e.type IN ('CALLS','CALL_REFERENCE')",
                                 -1, &statement, NULL) == SQLITE_OK &&
              sqlite3_step(statement) == SQLITE_ROW;
    if (ok) {
        *calls = sqlite3_column_int(statement, 0);
        *references = sqlite3_column_int(statement, 1);
    }
    sqlite3_finalize(statement);
    if (store)
        cbm_store_close(store);
    th_cleanup(root);
    return ok;
}

static bool kotlin_pipeline_counts(const char *main_source, const char *library, const char *extra,
                                   int *calls, int *references) {
    return kotlin_pipeline_counts_project("kotlin-binding", main_source, library, extra, calls,
                                          references);
}

TEST(resolver_kotlin_branch_reference_preserves_exact_candidate) {
    const char *sources[] = {
        "fun main() { if (true) { val callback = ::handler; accept(callback); callback() } }\n",
        "fun main() { if (true) { val callback = handler; accept(callback); callback() } }\n"};
    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++) {
        const char *source = sources[i];
        uint32_t start = (uint32_t)(strstr(source, "handler") - source);
        CBMFileResult *file = cbm_extract_file(source, (int)strlen(source), CBM_LANG_KOTLIN, "p",
                                               "Main.kt", 0, NULL, NULL);
        ASSERT_NOT_NULL(file);
        int matches = 0;
        bool exact = false;
        for (int u = 0; u < file->usages.count; u++) {
            const CBMUsage *usage = &file->usages.items[u];
            if (!usage->ref_name || strcmp(usage->ref_name, "handler"))
                continue;
            matches++;
            exact = usage->site_start_byte == start && usage->site_end_byte == start + 7 &&
                    usage->enclosing_func_qn && usage->kind == CBM_USAGE_VALUE &&
                    usage->may_be_call_reference == (i == 0);
        }
        cbm_free_result(file);
        ASSERT_EQ(matches, 1);
        ASSERT(exact);
    }
    PASS();
}

TEST(resolver_kotlin_default_package_callable_values_keep_exact_identity) {
    const char *library = "fun handler() {}\nfun accept(callback: () -> Unit) {}\n";
    const char *sources[] = {"val callback = ::handler\nfun main() { "
                             "accept(callback); callback(); handler() }\n",
                             "fun main() { val callback = ::handler; "
                             "accept(callback); callback(); handler() }\n",
                             "fun main() { accept(::handler); handler() }\n"};
    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++) {
        int calls = -1, references = -1;
        ASSERT(kotlin_pipeline_counts(sources[i], library, NULL, &calls, &references));
        ASSERT_EQ(calls, 2);
        ASSERT_EQ(references, 1);
    }
    PASS();
}

TEST(resolver_kotlin_callable_shadows_writes_and_competitors_abstain) {
    const char *library = "fun handler() {}\nfun accept(callback: () -> Unit) {}\n";
    int calls = -1, references = -1;
    ASSERT(kotlin_pipeline_counts("val callback = ::handler\nfun main(callback: "
                                  "() -> Unit) { accept(callback) }\n",
                                  library, NULL, &calls, &references));
    ASSERT_EQ(calls, 1);
    ASSERT_EQ(references, 0);
    ASSERT(kotlin_pipeline_counts("fun main() { var callback = ::handler; callback "
                                  "= unknown; callback(); accept(callback) }\n",
                                  library, NULL, &calls, &references));
    ASSERT_EQ(calls, 0);
    ASSERT_EQ(references, 1);
    ASSERT(kotlin_pipeline_counts("fun main() { if (true) { val callback = "
                                  "::handler; accept(callback); callback() } }\n",
                                  library, NULL, &calls, &references));
    ASSERT_EQ(calls, 0);
    ASSERT_EQ(references, 1);
    ASSERT(kotlin_pipeline_counts(
        "val callback = ::handler\nfun main() { callback(); accept(callback) }\n", library,
        "fun handler(value: Int) {}\n", &calls, &references));
    ASSERT_EQ(calls, 0);
    ASSERT_EQ(references, 0);
    ASSERT(kotlin_pipeline_counts("val callback = ::handler\nfun main() { callback() }\n", library,
                                  "val callback = ::handler\n", &calls, &references));
    ASSERT_EQ(calls, 0);
    ASSERT_EQ(references, 0);
    PASS();
}

TEST(resolver_kotlin_fixed_arguments_preserve_name_ambiguity_and_type_shadows) {
    const char *library = "fun consume(value: Int) {}\n";
    int calls = -1, references = -1;
    ASSERT(
        kotlin_pipeline_counts("fun main() { consume(1) }\n", library, NULL, &calls, &references));
    ASSERT_EQ(calls, 1);
    const char *negatives[] = {"fun main() { consume() }\n", "fun main() { consume(unknown) }\n",
                               "fun main() { consume(\"text\") }\n",
                               "fun main() { consume(2147483648) }\n"};
    for (size_t i = 0; i < sizeof(negatives) / sizeof(negatives[0]); i++) {
        ASSERT(kotlin_pipeline_counts(negatives[i], library, NULL, &calls, &references));
        ASSERT_EQ(calls, 0);
        ASSERT_EQ(references, 0);
    }
    const char *competitors[] = {"fun consume() {}\n",       "fun <T> consume(value: T) {}\n",
                                 "val consume = unknown\n",  "class Int {}\n",
                                 "typealias Int = String\n", "object Int {}\n"};
    for (size_t i = 0; i < sizeof(competitors) / sizeof(competitors[0]); i++) {
        ASSERT(kotlin_pipeline_counts("fun main() { consume(1) }\n", library, competitors[i],
                                      &calls, &references));
        ASSERT_EQ(calls, 0);
    }
    ASSERT(kotlin_pipeline_counts("fun main() { consume(1) }\n",
                                  "import other.Int\nfun consume(value: Int) {}\n", NULL, &calls,
                                  &references));
    ASSERT_EQ(calls, 0);
    PASS();
}

TEST(resolver_kotlin_import_domain_keeps_path_and_package_competitors) {
    const char *caller = "package client\nimport api.Library\nfun main() { Library() }\n";
    int calls = -1, references = -1;
    /* Project + Library.kt makes the file-module key api.Library, exactly
     * equal to the imported callable spelling. The other package declaration
     * must still participate before the zero-argument overload is chosen. */
    ASSERT(kotlin_pipeline_counts_project("api", caller, "package api\nfun Library() {}\n",
                                          "package api\nfun Library(value: Int) {}\n", &calls,
                                          &references));
    ASSERT_EQ(calls, 0);
    ASSERT_EQ(references, 0);
    /* A same-spelled file in another declared package cannot starve the
     * actual semantic import target in Extra.kt. */
    ASSERT(kotlin_pipeline_counts_project("api", caller, "package unrelated\nfun Library() {}\n",
                                          "package api\nfun Library() {}\n", &calls, &references));
    ASSERT_EQ(calls, 1);
    ASSERT_EQ(references, 0);
    PASS();
}

TEST(resolver_kotlin_imported_signature_keeps_type_shadow_competitors) {
    const char *caller = "package client\nimport api.Library\nfun main() { Library(1) }\n";
    int calls = -1, references = -1;
    ASSERT(kotlin_pipeline_counts_project("api", caller,
                                          "package api\nfun Library(value: Int) {}\n",
                                          "package api\nclass Int {}\n", &calls, &references));
    ASSERT_EQ(calls, 0);
    ASSERT_EQ(references, 0);
    ASSERT(kotlin_pipeline_counts_project("api", caller,
                                          "package api\nfun Library(value: kotlin.Int) {}\n",
                                          "package kotlin\nclass Int {}\n", &calls, &references));
    ASSERT_EQ(calls, 0);
    ASSERT_EQ(references, 0);
    PASS();
}

SUITE(resolver_kotlin) {
    RUN_TEST(resolver_kotlin_branch_reference_preserves_exact_candidate);
    RUN_TEST(resolver_kotlin_import_alias_receiver_and_free_function);
    RUN_TEST(resolver_kotlin_shadowed_and_unknown_receiver_abstain);
    RUN_TEST(resolver_kotlin_private_inherited_and_competing_declarations_abstain);
    RUN_TEST(resolver_kotlin_pipeline_preserves_exact_crossfile_target);
    RUN_TEST(resolver_kotlin_default_package_callable_values_keep_exact_identity);
    RUN_TEST(resolver_kotlin_callable_shadows_writes_and_competitors_abstain);
    RUN_TEST(resolver_kotlin_fixed_arguments_preserve_name_ambiguity_and_type_shadows);
    RUN_TEST(resolver_kotlin_import_domain_keeps_path_and_package_competitors);
    RUN_TEST(resolver_kotlin_imported_signature_keeps_type_shadow_competitors);
}
