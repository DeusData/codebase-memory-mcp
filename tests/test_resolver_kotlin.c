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
        /* A top-level initializer's ::handler is itself a reference, from the file. */
        ASSERT_EQ(references, i == 0 ? 2 : 1);
    }
    PASS();
}

TEST(resolver_kotlin_callable_shadows_writes_and_competitors_abstain) {
    const char *library = "fun handler() {}\nfun accept(callback: () -> Unit) {}\n";
    int calls = -1, references = -1;
    /* The parameter shadows the alias, so main references nothing. The initializer's
     * ::handler is still a reference, from the file. */
    ASSERT(kotlin_pipeline_counts("val callback = ::handler\nfun main(callback: "
                                  "() -> Unit) { accept(callback) }\n",
                                  library, NULL, &calls, &references));
    ASSERT_EQ(calls, 1);
    ASSERT_EQ(references, 1);
    /* accept is the only candidate of its arity, whatever its argument's type turns out
     * to be. The reassigned alias still selects nothing. */
    ASSERT(kotlin_pipeline_counts("fun main() { var callback = ::handler; callback "
                                  "= unknown; callback(); accept(callback) }\n",
                                  library, NULL, &calls, &references));
    ASSERT_EQ(calls, 1);
    ASSERT_EQ(references, 1);
    ASSERT(kotlin_pipeline_counts("fun main() { if (true) { val callback = "
                                  "::handler; accept(callback); callback() } }\n",
                                  library, NULL, &calls, &references));
    ASSERT_EQ(calls, 1);
    ASSERT_EQ(references, 1);
    ASSERT(kotlin_pipeline_counts(
        "val callback = ::handler\nfun main() { callback(); accept(callback) }\n", library,
        "fun handler(value: Int) {}\n", &calls, &references));
    ASSERT_EQ(calls, 1);
    ASSERT_EQ(references, 0);
    ASSERT(kotlin_pipeline_counts("val callback = ::handler\nfun main() { callback() }\n", library,
                                  "val callback = ::handler\n", &calls, &references));
    ASSERT_EQ(calls, 0);
    /* Two files declare the property, so the call has no target. Each initializer's
     * ::handler is still a reference, from its own file. */
    ASSERT_EQ(references, 1);
    PASS();
}

TEST(resolver_kotlin_fixed_arguments_preserve_name_ambiguity_and_type_shadows) {
    const char *library = "fun consume(value: Int) {}\n";
    int calls = -1, references = -1;
    ASSERT(
        kotlin_pipeline_counts("fun main() { consume(1) }\n", library, NULL, &calls, &references));
    ASSERT_EQ(calls, 1);
    /* An argument of unknown type keeps the only candidate of its arity: that is the
     * declaration a compiling program selected. */
    ASSERT(kotlin_pipeline_counts("fun main() { consume(unknown) }\n", library, NULL, &calls,
                                  &references));
    ASSERT_EQ(calls, 1);
    const char *negatives[] = {"fun main() { consume() }\n", "fun main() { consume(\"text\") }\n",
                               "fun main() { consume(2147483648) }\n"};
    for (size_t i = 0; i < sizeof(negatives) / sizeof(negatives[0]); i++) {
        ASSERT(kotlin_pipeline_counts(negatives[i], library, NULL, &calls, &references));
        ASSERT_EQ(calls, 0);
        ASSERT_EQ(references, 0);
    }
    /* Arity excludes the zero-argument overload, a function outranks a property of the
     * name at one scope level (Kotlin spec 11.3), and Int is more specific than T. */
    const char *selected[] = {"fun consume() {}\n", "val consume = unknown\n",
                              "fun <T> consume(value: T) {}\n"};
    for (size_t i = 0; i < sizeof(selected) / sizeof(selected[0]); i++) {
        ASSERT(kotlin_pipeline_counts("fun main() { consume(1) }\n", library, selected[i], &calls,
                                      &references));
        ASSERT_EQ(calls, 1);
    }
    const char *competitors[] = {"class Int {}\n", "typealias Int = String\n", "object Int {}\n"};
    for (size_t i = 0; i < sizeof(competitors) / sizeof(competitors[0]); i++) {
        ASSERT(kotlin_pipeline_counts("fun main() { consume(1) }\n", library, competitors[i],
                                      &calls, &references));
        ASSERT_EQ(calls, 0);
    }
    /* A parameter type outside the project cannot be compared, so the only candidate stands. */
    ASSERT(kotlin_pipeline_counts("fun main() { consume(1) }\n",
                                  "import other.Int\nfun consume(value: Int) {}\n", NULL, &calls,
                                  &references));
    ASSERT_EQ(calls, 1);
    PASS();
}

TEST(resolver_kotlin_import_domain_keeps_path_and_package_competitors) {
    const char *caller = "package client\nimport api.Library\nfun main() { Library() }\n";
    int calls = -1, references = -1;
    /* Project + Library.kt makes the file-module key api.Library, exactly
     * equal to the imported callable spelling. The other package declaration
     * participates, and arity selects the zero-argument overload. */
    ASSERT(kotlin_pipeline_counts_project("api", caller, "package api\nfun Library() {}\n",
                                          "package api\nfun Library(value: Int) {}\n", &calls,
                                          &references));
    ASSERT_EQ(calls, 1);
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

static bool kotlin_package_exact_row(const CBMResolvedCall *row, const char *caller,
                                     uint32_t start) {
    cbm_resolve_result_t proof = {.binding = CBM_RESOLVE_BINDING_UNIQUE,
                                  .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                  .analysis = row->evidence.analysis,
                                  .rule = row->evidence.rule,
                                  .facts = row->evidence.facts,
                                  .target = row->callee_qn,
                                  .candidate_count = 1,
                                  .proof = row->evidence.proof};
    return cbm_resolve_admit(&proof) && row->evidence.proof &&
           row->source_origin == CBM_SOURCE_ORIGIN_RAW && row->caller_qn &&
           !strcmp(row->caller_qn, caller) && row->callee_qn &&
           !strcmp(row->callee_qn, "p.util.Assert.pkgEq") && row->site_start_byte == start &&
           row->site_end_byte == start + strlen("pkgEq(id, id)");
}

static bool kotlin_package_call_rows(const char *source, bool shadow_string, int *exact,
                                     int *wrong) {
    static const char library[] =
        "package com.example.util\n\nfun pkgEq(a: String, b: String): Boolean {\n"
        "    return a == b\n}\n";
    static const char decoy[] =
        "package com.example.spec\nfun pkgSpec(label: String): Boolean { return true }\n";
    static const char homonym[] =
        "package unrelated\nfun pkgEq(a: String, b: String): Boolean { return false }\n";
    static const char shadow[] = "package com.example.app\nclass String {}\n";
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMLSPDef defs[] = {{.qualified_name = "com.example.util.pkgEq",
                         .graph_qualified_name = "p.util.Assert.pkgEq",
                         .short_name = "pkgEq",
                         .label = "Function",
                         .source_start_line = 3,
                         .signature_param_count = 2,
                         .declaration_identity = "util/Assert.kt:3"},
                        {.qualified_name = "com.example.spec.pkgSpec",
                         .graph_qualified_name = "p.spec.SpecHelpers.pkgSpec",
                         .short_name = "pkgSpec",
                         .label = "Function",
                         .source_start_line = 2,
                         .signature_param_count = 1,
                         .declaration_identity = "spec/SpecHelpers.kt:2"},
                        {.qualified_name = "unrelated.pkgEq",
                         .graph_qualified_name = "p.Other.pkgEq",
                         .short_name = "pkgEq",
                         .label = "Function",
                         .source_start_line = 2,
                         .signature_param_count = 2,
                         .declaration_identity = "Other.kt:2"},
                        {.qualified_name = "com.example.app.String",
                         .graph_qualified_name = "p.Shadow.String",
                         .short_name = "String",
                         .label = "Class",
                         .source_start_line = 2,
                         .declaration_identity = "Shadow.kt:2"}};
    bool facts = cbm_kotlin_collect_source_facts(&arena, library, (int)strlen(library),
                                                 "p.util.Assert", defs, 1) &&
                 cbm_kotlin_collect_source_facts(&arena, decoy, (int)strlen(decoy),
                                                 "p.spec.SpecHelpers", defs + 1, 1) &&
                 cbm_kotlin_collect_source_facts(&arena, homonym, (int)strlen(homonym), "p.Other",
                                                 defs + 2, 1) &&
                 cbm_kotlin_collect_source_facts(&arena, shadow, (int)strlen(shadow), "p.Shadow",
                                                 defs + 3, 1);
    CBMTypeRegistry registry;
    cbm_registry_init(&registry, &arena);
    cbm_kotlin_register_lsp_defs(&arena, &registry, defs, shadow_string ? 4 : 3, NULL);
    cbm_registry_finalize(&registry);
    CBMFileResult *file = cbm_extract_file(source, (int)strlen(source), CBM_LANG_KOTLIN, "p",
                                           "app/Query.kt", 0, NULL, NULL);
    const char *caller = NULL;
    for (int i = 0; file && i < file->defs.count; i++)
        if (file->defs.items[i].name && !strcmp(file->defs.items[i].name, "pkgBuild"))
            caller = file->defs.items[i].qualified_name;
    TSParser *parser = ts_parser_new();
    extern const TSLanguage *tree_sitter_kotlin(void);
    if (parser)
        ts_parser_set_language(parser, tree_sitter_kotlin());
    TSTree *tree =
        parser ? ts_parser_parse_string(parser, NULL, source, (uint32_t)strlen(source)) : NULL;
    ts_parser_delete(parser);
    bool ok = facts && file && tree && caller;
    CBMResolvedCallArray rows = {0};
    if (ok)
        cbm_kotlin_proof_resolve(&arena, source, (int)strlen(source), ts_tree_root_node(tree),
                                 &registry, file, &rows);
    const char *site = strstr(source, "pkgEq(id, id)");
    *exact = *wrong = 0;
    for (int i = 0; site && i < rows.count; i++) {
        const CBMResolvedCall *row = &rows.items[i];
        if (row->kind != CBM_RESOLVED_INVOCATION)
            continue;
        if (kotlin_package_exact_row(row, caller, (uint32_t)(site - source)))
            (*exact)++;
        else
            (*wrong)++;
    }
    if (tree)
        ts_tree_delete(tree);
    cbm_free_result(file);
    cbm_arena_destroy(&arena);
    return ok && site;
}

TEST(resolver_kotlin_imported_call_keeps_builtin_parameter_and_logical_body) {
    const struct {
        const char *extra_import, *parameter, *tail;
        bool shadow;
        int expected;
    } cases[] = {{"", "String", "", false, 1},
                 {"", "kotlin.String", "", false, 1},
                 {"", "String", " && pkgSpec(id)", false, 1},
                 {"", "kotlin.String", " && pkgSpec(id)", false, 1},
                 {"", "String", " || pkgSpec(id)", false, 1},
                 {"import kotlin.test.pkgEq\n", "String", " && pkgSpec(id)", false, 0},
                 {"import other.String\n", "String", " && pkgSpec(id)", false, 0},
                 {"import other.Value as String\n", "String", " && pkgSpec(id)", false, 0},
                 {"", "String", " && pkgSpec(id)", true, 0},
                 {"", "Int", " && pkgSpec(id)", false, 0},
                 {"", "String?", " && pkgSpec(id)", false, 0}};
    int exact[sizeof(cases) / sizeof(cases[0])] = {0};
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char source[1024];
        snprintf(source, sizeof(source),
                 "package com.example.app\n\nimport com.example.util.pkgEq\n%s"
                 "import kotlin.test.pkgSpec\n\nfun pkgBuild(id: %s): Boolean {\n"
                 "    return pkgEq(id, id)%s\n}\n",
                 cases[i].extra_import, cases[i].parameter, cases[i].tail);
        int wrong = 0;
        ASSERT(kotlin_package_call_rows(source, cases[i].shadow, exact + i, &wrong));
        ASSERT_EQ(wrong, 0);
    }
    /* Check ambiguity and type-shadow controls before positive quality gates. */
    for (size_t i = 5; i < sizeof(cases) / sizeof(cases[0]); i++)
        ASSERT_EQ(exact[i], cases[i].expected);
    for (size_t i = 0; i < 5; i++) {
        if (exact[i] != cases[i].expected)
            fprintf(stderr, "Kotlin imported-call scenario %zu: got=%d expected=%d\n", i, exact[i],
                    cases[i].expected);
        ASSERT_EQ(exact[i], cases[i].expected);
    }
    PASS();
}

#include "resolver_quality.h"
static const char KOTLIN_DIRECT_0[] = "class Calc {\n"
                                      "    fun helper(x: Int): Int = x + 1\n"
                                      "    fun run(y: Int): Int = helper(y)\n"
                                      "}\n";
static const char KOTLIN_DIRECT_1[] = "private class Local {\n"
                                      " private fun helper(x: Int): Int = x + 1\n"
                                      " internal fun wide(x: Long): Long = x\n"
                                      " fun implicit(y: Int): Int = helper(y)\n"
                                      " fun explicit(y: Int): Int = this.helper(y)\n"
                                      " fun typed(y: Long): Long = wide(y)\n"
                                      " fun shadow(helper: (Int) -> Int, x: Int): Int = helper(x)\n"
                                      " fun invalid(flag: Boolean): Int = this.helper(flag)\n"
                                      " fun labelled(y: Int): Int = this@Local.helper(y)\n"
                                      "}\n"
                                      "fun helper(value: String): String = value\n";
static const char KOTLIN_DIRECT_2[] = "package api\n"
                                      "class Service {\n"
                                      " fun keep(value: Int): Int = value\n"
                                      " fun wide(value: Long): Long = value\n"
                                      " fun flag(value: Boolean): Boolean = value\n"
                                      " fun text(value: String): String = value\n"
                                      " fun decimal(value: Float): Float = value\n"
                                      " fun over(value: Int): Int = value\n"
                                      " fun over(value: String): String = value\n"
                                      " fun optional(value: Int = 1): Int = value\n"
                                      " fun spread(vararg value: Int): Int = 0\n"
                                      " fun <T> identity(value: T): T = value\n"
                                      " private fun hidden(value: Int): Int = value\n"
                                      "}\n";
static const char KOTLIN_DIRECT_3[] =
    "package client\n"
    "import api.Service\n"
    "fun direct(value: Service, number: Int): Int = value.keep(number)\n"
    "fun wide(value: Service, number: Long): Long = value.wide(number)\n"
    "fun flag(value: Service, bit: Boolean): Boolean = value.flag(bit)\n"
    "fun text(value: Service, word: String): String = value.text(word)\n"
    "fun decimal(value: Service, fraction: Float): Float = value.decimal(fraction)\n"
    "fun intLiteral(value: Service): Int = value.keep(7)\n"
    "fun boolLiteral(value: Service): Boolean = value.flag(true)\n"
    "fun stringLiteral(value: Service): String = value.text(\"hello\")\n"
    "fun overloaded(value: Service, number: Int): Int = value.over(number)\n"
    "fun defaulted(value: Service): Int = value.optional()\n"
    "fun named(value: Service): Int = value.keep(value = 3)\n"
    "fun variableArity(value: Service): Int = value.spread(1, 2)\n"
    "fun generic(value: Service, number: Int): Int = value.identity(number)\n"
    "fun contextual(value: Service): Long = value.wide(1)\n"
    "fun localCopy(value: Service, number: Int): Int { val copied: Int = number; return "
    "value.keep(copied) }\n"
    "fun smart(value: Service?, number: Int): Int { if (value != null) return value.keep(number); "
    "return 0 }\n"
    "fun effect(value: Service, number: Int): Int { var changed = number; changed++; return "
    "value.keep(changed) }\n"
    "fun badBool(value: Service): Int = value.keep(false)\n"
    "fun badWide(value: Service, small: Int): Long = value.wide(small)\n"
    "fun badUnknown(value: Service): Int = value.keep(unknown)\n"
    "fun badArity(value: Service): Int = value.keep()\n"
    "fun badPrivate(value: Service): Int = value.hidden(1)\n"
    "fun badStatic(): Int = Service.keep(2)\n";
static const char KOTLIN_DIRECT_4[] = "package inherited\n"
                                      "open class Base {\n"
                                      " fun inherited(value: Int): Int = value\n"
                                      "}\n"
                                      "class Child : Base() {\n"
                                      " fun forward(number: Int): Int = inherited(number)\n"
                                      "}\n";
static const char KOTLIN_DIRECT_5[] =
    "package extension\n"
    "import api.Service\n"
    "fun Service.extension(value: Int): Int = value\n"
    "fun forward(service: Service, number: Int): Int = service.extension(number)\n";
static const char KOTLIN_DIRECT_6[] = "package shadow\n"
                                      "class Int\n"
                                      "class Shadow {\n"
                                      " fun pick(value: Int): Int = value\n"
                                      " fun invalid(): Int = pick(1)\n"
                                      "}\n";
static const char KOTLIN_DIRECT_7[] = "package alias\n"
                                      " typealias Int = String\n"
                                      "class Shadow {\n"
                                      " fun pick(value: Int): Int = value\n"
                                      " fun invalid(): Int = pick(2)\n"
                                      "}\n";
static const char KOTLIN_DIRECT_8[] =
    "package duplicate\n"
    "class Duplicate {\n"
    " fun same(value: Int): Int = value\n"
    "}\n"
    "fun invalid(value: Duplicate, number: Int): Int = value.same(number)\n";
static const char KOTLIN_DIRECT_9[] = "package duplicate\n"
                                      "class Duplicate {\n"
                                      " fun same(value: Int): Int = value\n"
                                      "}\n";
static const char KOTLIN_DIRECT_10[] = "package other\n"
                                       "class Service {\n"
                                       " fun keep(value: Int): Int = value\n"
                                       " fun wide(value: Long): Long = value\n"
                                       "}\n";
/* An argument of unknown type keeps the only candidate of its arity. Overloads hold one
 * graph node, so a wrong overload cannot be told from the right one at the node. */
static const RQ_Expected KOTLIN_DIRECT_EXPECTED[] = {
    {.case_id = "unknown-argument-keeps-the-candidate",
     .site = {.file = "Main.kt",
              .caller = "badUnknown",
              .needle = "value.keep(unknown)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "Service.kt",
                .suffix = "Service.keep",
                .label = "Method",
                .declaration_line = 3,
                .declaration_text = " fun keep(value: Int): Int = value"}},
    {.case_id = "original-contract",
     .site =
         {.file = "Calc.kt", .caller = "Calc.run", .needle = "helper(y)", .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "Calc.kt",
                .suffix = "Calc.helper",
                .label = "Method",
                .declaration_line = 2,
                .declaration_text = "    fun helper(x: Int): Int = x + 1"}},
    {.case_id = "lexical-private-implicit",
     .site = {.file = "Local.kt",
              .caller = "Local.implicit",
              .needle = "helper(y)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "Local.kt",
                .suffix = "Local.helper",
                .label = "Method",
                .declaration_line = 2,
                .declaration_text = " private fun helper(x: Int): Int = x + 1"}},
    {.case_id = "lexical-private-explicit",
     .site = {.file = "Local.kt",
              .caller = "Local.explicit",
              .needle = "this.helper(y)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "Local.kt",
                .suffix = "Local.helper",
                .label = "Method",
                .declaration_line = 2,
                .declaration_text = " private fun helper(x: Int): Int = x + 1"}},
    {.case_id = "lexical-internal-exact-long",
     .site =
         {.file = "Local.kt", .caller = "Local.typed", .needle = "wide(y)", .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "Local.kt",
                .suffix = "Local.wide",
                .label = "Method",
                .declaration_line = 3,
                .declaration_text = " internal fun wide(x: Long): Long = x"}},
    {.case_id = "crossfile-int-parameter",
     .site = {.file = "Main.kt",
              .caller = "direct",
              .needle = "value.keep(number)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "Service.kt",
                .suffix = "Service.keep",
                .label = "Method",
                .declaration_line = 3,
                .declaration_text = " fun keep(value: Int): Int = value"}},
    {.case_id = "crossfile-long-parameter",
     .site = {.file = "Main.kt",
              .caller = "wide",
              .needle = "value.wide(number)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "Service.kt",
                .suffix = "Service.wide",
                .label = "Method",
                .declaration_line = 4,
                .declaration_text = " fun wide(value: Long): Long = value"}},
    {.case_id = "crossfile-boolean-parameter",
     .site =
         {.file = "Main.kt", .caller = "flag", .needle = "value.flag(bit)", .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "Service.kt",
                .suffix = "Service.flag",
                .label = "Method",
                .declaration_line = 5,
                .declaration_text = " fun flag(value: Boolean): Boolean = value"}},
    {.case_id = "crossfile-string-parameter",
     .site =
         {.file = "Main.kt", .caller = "text", .needle = "value.text(word)", .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "Service.kt",
                .suffix = "Service.text",
                .label = "Method",
                .declaration_line = 6,
                .declaration_text = " fun text(value: String): String = value"}},
    {.case_id = "crossfile-float-parameter",
     .site = {.file = "Main.kt",
              .caller = "decimal",
              .needle = "value.decimal(fraction)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "Service.kt",
                .suffix = "Service.decimal",
                .label = "Method",
                .declaration_line = 7,
                .declaration_text = " fun decimal(value: Float): Float = value"}},
    {.case_id = "crossfile-int-literal",
     .site = {.file = "Main.kt",
              .caller = "intLiteral",
              .needle = "value.keep(7)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "Service.kt",
                .suffix = "Service.keep",
                .label = "Method",
                .declaration_line = 3,
                .declaration_text = " fun keep(value: Int): Int = value"}},
    {.case_id = "crossfile-boolean-literal",
     .site = {.file = "Main.kt",
              .caller = "boolLiteral",
              .needle = "value.flag(true)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "Service.kt",
                .suffix = "Service.flag",
                .label = "Method",
                .declaration_line = 5,
                .declaration_text = " fun flag(value: Boolean): Boolean = value"}},
    {.case_id = "crossfile-string-literal",
     .site = {.file = "Main.kt",
              .caller = "stringLiteral",
              .needle = "value.text(\"hello\")",
              .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "Service.kt",
                .suffix = "Service.text",
                .label = "Method",
                .declaration_line = 6,
                .declaration_text = " fun text(value: String): String = value"}},
    {.case_id = "overload-selection",
     .site = {.file = "Main.kt",
              .caller = "overloaded",
              .needle = "value.over(number)",
              .occurrence_index = 0},
     .relation = "CALLS",
     /* The Int overload is selected; both overloads are one graph node, which carries
      * the last declaration's line. */
     .target = {.file = "Service.kt",
                .suffix = "Service.over",
                .label = "Method",
                .declaration_line = 9,
                .declaration_text = " fun over(value: String): String = value"}},
    {.case_id = "default-argument",
     .site = {.file = "Main.kt",
              .caller = "defaulted",
              .needle = "value.optional()",
              .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "Service.kt",
                .suffix = "Service.optional",
                .label = "Method",
                .declaration_line = 10,
                .declaration_text = " fun optional(value: Int = 1): Int = value"}},
    {.case_id = "named-argument",
     .site = {.file = "Main.kt",
              .caller = "named",
              .needle = "value.keep(value = 3)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "Service.kt",
                .suffix = "Service.keep",
                .label = "Method",
                .declaration_line = 3,
                .declaration_text = " fun keep(value: Int): Int = value"}},
    {.case_id = "vararg-packing",
     .site = {.file = "Main.kt",
              .caller = "variableArity",
              .needle = "value.spread(1, 2)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "Service.kt",
                .suffix = "Service.spread",
                .label = "Method",
                .declaration_line = 11,
                .declaration_text = " fun spread(vararg value: Int): Int = 0"}},
    {.case_id = "generic-inference",
     .site = {.file = "Main.kt",
              .caller = "generic",
              .needle = "value.identity(number)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "Service.kt",
                .suffix = "Service.identity",
                .label = "Method",
                .declaration_line = 12,
                .declaration_text = " fun <T> identity(value: T): T = value"}},
    {.case_id = "contextual-literal",
     .site = {.file = "Main.kt",
              .caller = "contextual",
              .needle = "value.wide(1)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "Service.kt",
                .suffix = "Service.wide",
                .label = "Method",
                .declaration_line = 4,
                .declaration_text = " fun wide(value: Long): Long = value"}},
    {.case_id = "local-value-flow",
     .site = {.file = "Main.kt",
              .caller = "localCopy",
              .needle = "value.keep(copied)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "Service.kt",
                .suffix = "Service.keep",
                .label = "Method",
                .declaration_line = 3,
                .declaration_text = " fun keep(value: Int): Int = value"}},
    {.case_id = "smart-cast",
     .site = {.file = "Main.kt",
              .caller = "smart",
              .needle = "value.keep(number)",
              .occurrence_index = 1},
     .relation = "CALLS",
     .target = {.file = "Service.kt",
                .suffix = "Service.keep",
                .label = "Method",
                .declaration_line = 3,
                .declaration_text = " fun keep(value: Int): Int = value"}},
    {.case_id = "mutable-local-effect",
     .site = {.file = "Main.kt",
              .caller = "effect",
              .needle = "value.keep(changed)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "Service.kt",
                .suffix = "Service.keep",
                .label = "Method",
                .declaration_line = 3,
                .declaration_text = " fun keep(value: Int): Int = value"}},
    {.case_id = "inherited-member",
     .site = {.file = "Inherited.kt",
              .caller = "Child.forward",
              .needle = "inherited(number)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "Inherited.kt",
                .suffix = "Base.inherited",
                .label = "Method",
                .declaration_line = 3,
                .declaration_text = " fun inherited(value: Int): Int = value"}},
    {.case_id = "extension-member",
     .site = {.file = "Extension.kt",
              .caller = "forward",
              .needle = "service.extension(number)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "Extension.kt",
                .suffix = "extension",
                .declaration_line = 3,
                .declaration_text = "fun Service.extension(value: Int): Int = value"}},
    {.case_id = "labelled-this",
     .site = {.file = "Local.kt",
              .caller = "Local.labelled",
              .needle = "this@Local.helper(y)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "Local.kt",
                .suffix = "Local.helper",
                .label = "Method",
                .declaration_line = 2,
                .declaration_text = " private fun helper(x: Int): Int = x + 1"}},
};
static const RQ_Forbidden KOTLIN_DIRECT_FORBIDDEN[] = {
    {.case_id = "parameter-callable-shadow",
     .site = {.file = "Local.kt",
              .caller = "Local.shadow",
              .needle = "helper(x)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "lexical-wrong-primitive",
     .site = {.file = "Local.kt",
              .caller = "Local.invalid",
              .needle = "this.helper(flag)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "boolean-is-not-int",
     .site = {.file = "Main.kt",
              .caller = "badBool",
              .needle = "value.keep(false)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "no-numeric-widening",
     .site = {.file = "Main.kt",
              .caller = "badWide",
              .needle = "value.wide(small)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "wrong-arity",
     .site =
         {.file = "Main.kt", .caller = "badArity", .needle = "value.keep()", .occurrence_index = 0},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "private-other-class",
     .site = {.file = "Main.kt",
              .caller = "badPrivate",
              .needle = "value.hidden(1)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "instance-member-through-class",
     .site = {.file = "Main.kt",
              .caller = "badStatic",
              .needle = "Service.keep(2)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "package-builtin-shadow",
     .site = {.file = "Shadow.kt",
              .caller = "Shadow.invalid",
              .needle = "pick(1)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "alias-builtin-shadow",
     .site = {.file = "Alias.kt",
              .caller = "Shadow.invalid",
              .needle = "pick(2)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "duplicate-class-domain",
     .site = {.file = "Duplicate.kt",
              .caller = "invalid",
              .needle = "value.same(number)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .any_target = true},
};
TEST(resolver_kotlin_primitive_arguments_and_lexical_members_require_proofs) {
    for (int driver = 0; driver < 2; driver++) {
        const char *root = th_mktempdir("cbm_kotlin_direct");
        ASSERT_NOT_NULL(root);
        char repo[768], database[768];
        snprintf(repo, sizeof(repo), "%s/repo", root);
        snprintf(database, sizeof(database), "%s/index.db", root);
        const RQ_Source sources[] = {
            {.path = "Calc.kt", .text = KOTLIN_DIRECT_0},
            {.path = "Local.kt", .text = KOTLIN_DIRECT_1},
            {.path = "Service.kt", .text = KOTLIN_DIRECT_2},
            {.path = "Main.kt", .text = KOTLIN_DIRECT_3},
            {.path = "Inherited.kt", .text = KOTLIN_DIRECT_4},
            {.path = "Extension.kt", .text = KOTLIN_DIRECT_5},
            {.path = "Shadow.kt", .text = KOTLIN_DIRECT_6},
            {.path = "Alias.kt", .text = KOTLIN_DIRECT_7},
            {.path = "Duplicate.kt", .text = KOTLIN_DIRECT_8},
            {.path = "DuplicateOther.kt", .text = KOTLIN_DIRECT_9},
            {.path = "Decoy.kt", .text = KOTLIN_DIRECT_10},
        };
        bool ok = cbm_mkdir_p(repo, 0755);
        for (size_t i = 0; ok && i < sizeof(sources) / sizeof(sources[0]); i++)
            ok = th_write_file(TH_PATH(repo, sources[i].path), sources[i].text) == 0;
        for (int i = 0; ok && i < 55; i++) {
            char path[32];
            snprintf(path, sizeof(path), "Filler%02d.kt", i);
            ok = th_write_file(TH_PATH(repo, path), "// unrelated source file\n") == 0;
        }
        const char *old_workers = getenv("CBM_WORKERS");
        char *saved_workers = old_workers ? strdup(old_workers) : NULL;
        ok = ok && (!old_workers || saved_workers);
        cbm_setenv("CBM_WORKERS", driver ? "4" : "1", 1);
        cbm_pipeline_t *pipeline = ok ? cbm_pipeline_new(repo, database, CBM_MODE_FULL) : NULL;
        ok = pipeline && cbm_pipeline_set_project_name(pipeline, "kotlin-direct") &&
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
             rq_check(store, "kotlin-direct", "kotlin-direct-primitive-members", sources,
                      sizeof(sources) / sizeof(sources[0]), KOTLIN_DIRECT_EXPECTED,
                      sizeof(KOTLIN_DIRECT_EXPECTED) / sizeof(KOTLIN_DIRECT_EXPECTED[0]),
                      KOTLIN_DIRECT_FORBIDDEN,
                      sizeof(KOTLIN_DIRECT_FORBIDDEN) / sizeof(KOTLIN_DIRECT_FORBIDDEN[0]), &report,
                      stderr) &&
             ok;
        if (store)
            cbm_store_close(store);
        th_cleanup(root);
        ASSERT(ok);
        ASSERT_EQ(report.genuine, 25);
        ASSERT_EQ(report.found, 25);
        ASSERT_EQ(report.missing, 0);
        ASSERT_EQ(report.required_missing + report.wrong, 0);
    }
    PASS();
}

static const RQ_Source KOTLIN_ALIAS_SOURCES[] = {
    {.path = "Quality.kt",
     .text = "package quality\nfun handler() {}\nfun other() {}\nfun aliasControl() { val callback "
             "= ::handler; callback() }\nfun mutableAlias() { var callback = ::handler; callback() "
             "}\nfun assignedAlias() { var callback = ::handler; callback = ::other; callback() "
             "}\nfun branchAlias() { if (true) { val callback = ::handler; callback() } }\nfun "
             "parameterShadow(callback: () -> Unit) { callback() }\n"},
    {.path = "Default.kt",
     .text = "fun defaultHandler() {}\nfun defaultAlias() { val callback = ::defaultHandler; "
             "callback() }\n"},
    {.path = "Api.kt", .text = "package api\nfun importedHandler() {}\n"},
    {.path = "Imported.kt",
     .text = "package client\nimport api.importedHandler\nfun importedAlias() { val callback = "
             "::importedHandler; callback() }\n"},
    {.path = "Overloaded.kt",
     .text = "package overloaded\nfun handler() {}\nfun handler(value: Int) {}\nfun "
             "ambiguousAlias() { val callback = ::handler; callback() }\n"},
    {.path = "Decoy.kt",
     .text =
         "package decoy\nfun handler() {}\nfun defaultHandler() {}\nfun importedHandler() {}\n"},
};
static const RQ_Expected KOTLIN_ALIAS_EXPECTED[] = {
    {.case_id = "aliasControl-reference",
     .site = {.file = "Quality.kt", .caller = "aliasControl", .start_byte = 86, .end_byte = 93},
     .relation = "CALL_REFERENCE",
     .target = {.file = "Quality.kt",
                .suffix = "handler",
                .label = "Function",
                .declaration_line = 2,
                .declaration_text = "fun handler() {}"}},
    {.case_id = "aliasControl-invocation",
     .site = {.file = "Quality.kt", .caller = "aliasControl", .start_byte = 95, .end_byte = 105},
     .relation = "CALLS",
     .target = {.file = "Quality.kt",
                .suffix = "handler",
                .label = "Function",
                .declaration_line = 2,
                .declaration_text = "fun handler() {}"}},
    {.case_id = "defaultAlias-reference",
     .site = {.file = "Default.kt", .caller = "defaultAlias", .start_byte = 62, .end_byte = 76},
     .relation = "CALL_REFERENCE",
     .target = {.file = "Default.kt",
                .suffix = "defaultHandler",
                .label = "Function",
                .declaration_line = 1,
                .declaration_text = "fun defaultHandler() {}"}},
    {.case_id = "defaultAlias-invocation",
     .site = {.file = "Default.kt", .caller = "defaultAlias", .start_byte = 78, .end_byte = 88},
     .relation = "CALLS",
     .target = {.file = "Default.kt",
                .suffix = "defaultHandler",
                .label = "Function",
                .declaration_line = 1,
                .declaration_text = "fun defaultHandler() {}"}},
    {.case_id = "importedAlias-reference",
     .site = {.file = "Imported.kt", .caller = "importedAlias", .start_byte = 81, .end_byte = 96},
     .relation = "CALL_REFERENCE",
     .target = {.file = "Api.kt",
                .suffix = "importedHandler",
                .label = "Function",
                .declaration_line = 2,
                .declaration_text = "fun importedHandler() {}"}},
    {.case_id = "importedAlias-invocation",
     .site = {.file = "Imported.kt", .caller = "importedAlias", .start_byte = 98, .end_byte = 108},
     .relation = "CALLS",
     .target = {.file = "Api.kt",
                .suffix = "importedHandler",
                .label = "Function",
                .declaration_line = 2,
                .declaration_text = "fun importedHandler() {}"}},
    {.case_id = "mutableAlias-reference",
     .site = {.file = "Quality.kt", .caller = "mutableAlias", .start_byte = 146, .end_byte = 153},
     .relation = "CALL_REFERENCE",
     .target = {.file = "Quality.kt",
                .suffix = "handler",
                .label = "Function",
                .declaration_line = 2,
                .declaration_text = "fun handler() {}"}},
    {.case_id = "mutableAlias-invocation",
     .site = {.file = "Quality.kt", .caller = "mutableAlias", .start_byte = 155, .end_byte = 165},
     .relation = "CALLS",
     .target = {.file = "Quality.kt",
                .suffix = "handler",
                .label = "Function",
                .declaration_line = 2,
                .declaration_text = "fun handler() {}"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "kotlin-mutable-callable-flow",
     .why = "Mutable callable bindings require a flow proof before the call can select their "
            "current value.",
     .tried = "Exact source sites and target declarations are retained; the original conservative "
              "write and branch guards remain in force."},
    {.case_id = "assignedAlias-reference",
     .site = {.file = "Quality.kt", .caller = "assignedAlias", .start_byte = 207, .end_byte = 214},
     .relation = "CALL_REFERENCE",
     .target = {.file = "Quality.kt",
                .suffix = "handler",
                .label = "Function",
                .declaration_line = 2,
                .declaration_text = "fun handler() {}"}},
    {.case_id = "assignedAlias-replacement-reference",
     .site = {.file = "Quality.kt", .caller = "assignedAlias", .start_byte = 229, .end_byte = 234},
     .relation = "CALL_REFERENCE",
     .target = {.file = "Quality.kt",
                .suffix = "other",
                .label = "Function",
                .declaration_line = 3,
                .declaration_text = "fun other() {}"}},
    {.case_id = "assignedAlias-invocation",
     .site = {.file = "Quality.kt", .caller = "assignedAlias", .start_byte = 236, .end_byte = 246},
     .relation = "CALLS",
     .target = {.file = "Quality.kt",
                .suffix = "other",
                .label = "Function",
                .declaration_line = 3,
                .declaration_text = "fun other() {}"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "kotlin-reassigned-callable-flow",
     .why = "The final assignment selects other in Kotlin, but the audited resolver conservatively "
            "invalidates all writes.",
     .tried = "Exact source sites and target declarations are retained; the original conservative "
              "write and branch guards remain in force."},
    {.case_id = "branchAlias-reference",
     .site = {.file = "Quality.kt", .caller = "branchAlias", .start_byte = 298, .end_byte = 305},
     .relation = "CALL_REFERENCE",
     .target = {.file = "Quality.kt",
                .suffix = "handler",
                .label = "Function",
                .declaration_line = 2,
                .declaration_text = "fun handler() {}"}},
    {.case_id = "branchAlias-invocation",
     .site = {.file = "Quality.kt", .caller = "branchAlias", .start_byte = 307, .end_byte = 317},
     .relation = "CALLS",
     .target = {.file = "Quality.kt",
                .suffix = "handler",
                .label = "Function",
                .declaration_line = 2,
                .declaration_text = "fun handler() {}"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "kotlin-branch-local-callable-scope",
     .why = "The branch-local binding is genuine but lies outside the direct function-body alias "
            "scope.",
     .tried = "Exact source sites and target declarations are retained; the original conservative "
              "write and branch guards remain in force."}};
static const RQ_Forbidden KOTLIN_ALIAS_FORBIDDEN[] = {
    {.case_id = "parameter-has-no-project-target",
     .site =
         {.file = "Quality.kt", .caller = "parameterShadow", .start_byte = 366, .end_byte = 376},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "assignment-must-not-retain-old-target",
     .site = {.file = "Quality.kt", .caller = "assignedAlias", .start_byte = 236, .end_byte = 246},
     .relation = "CALLS",
     .target = {.file = "Quality.kt",
                .suffix = "handler",
                .label = "Function",
                .declaration_line = 2,
                .declaration_text = "fun handler() {}"}},
    {.case_id = "overload-call_reference-has-no-unique-target",
     .site =
         {.file = "Overloaded.kt", .caller = "ambiguousAlias", .start_byte = 103, .end_byte = 110},
     .relation = "CALL_REFERENCE",
     .any_target = true},
    {.case_id = "overload-calls-has-no-unique-target",
     .site =
         {.file = "Overloaded.kt", .caller = "ambiguousAlias", .start_byte = 112, .end_byte = 122},
     .relation = "CALLS",
     .any_target = true}};
TEST(resolver_kotlin_callable_alias_occurrences_require_exact_binding) {
    for (int driver = 0; driver < 2; driver++) {
        const char *root = th_mktempdir("cbm_kotlin_alias_occurrences");
        ASSERT_NOT_NULL(root);
        char repo[768], database[768];
        snprintf(repo, sizeof(repo), "%s/repo", root);
        snprintf(database, sizeof(database), "%s/index.db", root);
        bool ok = cbm_mkdir_p(repo, 0755);
        for (size_t i = 0; ok && i < sizeof(KOTLIN_ALIAS_SOURCES) / sizeof(KOTLIN_ALIAS_SOURCES[0]);
             i++)
            ok = th_write_file(TH_PATH(repo, KOTLIN_ALIAS_SOURCES[i].path),
                               KOTLIN_ALIAS_SOURCES[i].text) == 0;
        for (int i = 0; ok && i < 55; i++) {
            char path[32];
            snprintf(path, sizeof(path), "AliasFiller%02d.kt", i);
            ok = th_write_file(TH_PATH(repo, path), "// unrelated source file\n") == 0;
        }
        const char *old_workers = getenv("CBM_WORKERS");
        char *saved_workers = old_workers ? strdup(old_workers) : NULL;
        ok = ok && (!old_workers || saved_workers);
        cbm_setenv("CBM_WORKERS", driver ? "4" : "1", 1);
        cbm_pipeline_t *pipeline = ok ? cbm_pipeline_new(repo, database, CBM_MODE_FULL) : NULL;
        ok = pipeline && cbm_pipeline_set_project_name(pipeline, "kotlin-alias-occurrences") &&
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
             rq_check(store, "kotlin-alias-occurrences", "kotlin-alias-source-occurrences",
                      KOTLIN_ALIAS_SOURCES,
                      sizeof(KOTLIN_ALIAS_SOURCES) / sizeof(KOTLIN_ALIAS_SOURCES[0]),
                      KOTLIN_ALIAS_EXPECTED,
                      sizeof(KOTLIN_ALIAS_EXPECTED) / sizeof(KOTLIN_ALIAS_EXPECTED[0]),
                      KOTLIN_ALIAS_FORBIDDEN,
                      sizeof(KOTLIN_ALIAS_FORBIDDEN) / sizeof(KOTLIN_ALIAS_FORBIDDEN[0]), &report,
                      stderr) &&
             ok;
        if (store)
            cbm_store_close(store);
        th_cleanup(root);
        ASSERT(ok);
        ASSERT_EQ(report.genuine, 13);
        ASSERT_EQ(report.found, 10);
        ASSERT_EQ(report.missing, 3);
        ASSERT_EQ(report.required_missing + report.wrong, 0);
    }
    PASS();
}

/* An exact callable identity does not make a composite argument leaf a
 * direct callable argument. Keep these source-authored role obligations apart. */
static const RQ_Source KOTLIN_COMPOSITE_SOURCES[] = {
    {.path = "Main.kt",
     .text =
         "fun handler() {}\nfun alternate() {}\nfun accept(callback: () -> Unit) {}\nfun "
         "complex(flag: Boolean) { accept(if (flag) ::handler else ::alternate) }\nfun "
         "wrapped(flag: Boolean) { accept((if (flag) ::handler else ::alternate)) }\nfun direct() "
         "{ accept(::handler) }\nfun parenthesized() { accept((::handler)) }\nfun standalone() { "
         "::handler }\nfun immutable() { val callback = ::handler; callback() }\n"},
    {.path = "Mapped.kt",
     .text = "fun acceptMap(value: Any) {}\nfun mapped() { acceptMap(mapOf(\"first\" to ::handler, "
             "\"second\" to ::alternate)) }\n"},
    {.path = "Decoy.kt", .text = "package other\nfun handler() {}\nfun alternate() {}\n"}};
static const RQ_Expected KOTLIN_COMPOSITE_EXPECTED[] = {
    {.case_id = "complex-handler-USAGE",
     .site = {.file = "Main.kt", .caller = "complex", .start_byte = 120, .end_byte = 127},
     .relation = "USAGE",
     .target = {.file = "Main.kt",
                .suffix = "Main.handler",
                .label = "Function",
                .declaration_line = 1,
                .declaration_text = "fun handler() {}"}},
    {.case_id = "complex-alternate-USAGE",
     .site = {.file = "Main.kt", .caller = "complex", .start_byte = 135, .end_byte = 144},
     .relation = "USAGE",
     .target = {.file = "Main.kt",
                .suffix = "Main.alternate",
                .label = "Function",
                .declaration_line = 2,
                .declaration_text = "fun alternate() {}"}},
    {.case_id = "complex-accept-CALLS",
     .site = {.file = "Main.kt", .caller = "complex", .start_byte = 101, .end_byte = 145},
     .relation = "CALLS",
     .target = {.file = "Main.kt",
                .suffix = "Main.accept",
                .label = "Function",
                .declaration_line = 3,
                .declaration_text = "fun accept(callback: () -> Unit) {}"}},
    {.case_id = "wrapped-handler-USAGE",
     .site = {.file = "Main.kt", .caller = "wrapped", .start_byte = 197, .end_byte = 204},
     .relation = "USAGE",
     .target = {.file = "Main.kt",
                .suffix = "Main.handler",
                .label = "Function",
                .declaration_line = 1,
                .declaration_text = "fun handler() {}"}},
    {.case_id = "wrapped-alternate-USAGE",
     .site = {.file = "Main.kt", .caller = "wrapped", .start_byte = 212, .end_byte = 221},
     .relation = "USAGE",
     .target = {.file = "Main.kt",
                .suffix = "Main.alternate",
                .label = "Function",
                .declaration_line = 2,
                .declaration_text = "fun alternate() {}"}},
    {.case_id = "wrapped-accept-CALLS",
     .site = {.file = "Main.kt", .caller = "wrapped", .start_byte = 177, .end_byte = 223},
     .relation = "CALLS",
     .target = {.file = "Main.kt",
                .suffix = "Main.accept",
                .label = "Function",
                .declaration_line = 3,
                .declaration_text = "fun accept(callback: () -> Unit) {}"}},
    {.case_id = "direct-handler-CALL_REFERENCE",
     .site = {.file = "Main.kt", .caller = "direct", .start_byte = 250, .end_byte = 257},
     .relation = "CALL_REFERENCE",
     .target = {.file = "Main.kt",
                .suffix = "Main.handler",
                .label = "Function",
                .declaration_line = 1,
                .declaration_text = "fun handler() {}"}},
    {.case_id = "direct-accept-CALLS",
     .site = {.file = "Main.kt", .caller = "direct", .start_byte = 241, .end_byte = 258},
     .relation = "CALLS",
     .target = {.file = "Main.kt",
                .suffix = "Main.accept",
                .label = "Function",
                .declaration_line = 3,
                .declaration_text = "fun accept(callback: () -> Unit) {}"}},
    {.case_id = "parenthesized-handler-CALL_REFERENCE",
     .site = {.file = "Main.kt", .caller = "parenthesized", .start_byte = 293, .end_byte = 300},
     .relation = "CALL_REFERENCE",
     .target = {.file = "Main.kt",
                .suffix = "Main.handler",
                .label = "Function",
                .declaration_line = 1,
                .declaration_text = "fun handler() {}"}},
    {.case_id = "parenthesized-accept-CALLS",
     .site = {.file = "Main.kt", .caller = "parenthesized", .start_byte = 283, .end_byte = 302},
     .relation = "CALLS",
     .target = {.file = "Main.kt",
                .suffix = "Main.accept",
                .label = "Function",
                .declaration_line = 3,
                .declaration_text = "fun accept(callback: () -> Unit) {}"}},
    {.case_id = "standalone-handler-CALL_REFERENCE",
     .site = {.file = "Main.kt", .caller = "standalone", .start_byte = 326, .end_byte = 333},
     .relation = "CALL_REFERENCE",
     .target = {.file = "Main.kt",
                .suffix = "Main.handler",
                .label = "Function",
                .declaration_line = 1,
                .declaration_text = "fun handler() {}"}},
    {.case_id = "immutable-handler-CALL_REFERENCE",
     .site = {.file = "Main.kt", .caller = "immutable", .start_byte = 371, .end_byte = 378},
     .relation = "CALL_REFERENCE",
     .target = {.file = "Main.kt",
                .suffix = "Main.handler",
                .label = "Function",
                .declaration_line = 1,
                .declaration_text = "fun handler() {}"}},
    {.case_id = "immutable-handler-CALLS",
     .site = {.file = "Main.kt", .caller = "immutable", .start_byte = 380, .end_byte = 390},
     .relation = "CALLS",
     .target = {.file = "Main.kt",
                .suffix = "Main.handler",
                .label = "Function",
                .declaration_line = 1,
                .declaration_text = "fun handler() {}"}},
    {.case_id = "mapped-handler-USAGE",
     .site = {.file = "Mapped.kt", .caller = "mapped", .start_byte = 73, .end_byte = 80},
     .relation = "USAGE",
     .target = {.file = "Main.kt",
                .suffix = "Main.handler",
                .label = "Function",
                .declaration_line = 1,
                .declaration_text = "fun handler() {}"}},
    {.case_id = "mapped-alternate-USAGE",
     .site = {.file = "Mapped.kt", .caller = "mapped", .start_byte = 96, .end_byte = 105},
     .relation = "USAGE",
     .target = {.file = "Main.kt",
                .suffix = "Main.alternate",
                .label = "Function",
                .declaration_line = 2,
                .declaration_text = "fun alternate() {}"}},
    {.case_id = "mapped-acceptMap-CALLS",
     .site = {.file = "Mapped.kt", .caller = "mapped", .start_byte = 44, .end_byte = 107},
     .relation = "CALLS",
     .target = {.file = "Mapped.kt",
                .suffix = "Mapped.acceptMap",
                .label = "Function",
                .declaration_line = 1,
                .declaration_text = "fun acceptMap(value: Any) {}"}}};
static const RQ_Forbidden KOTLIN_COMPOSITE_FORBIDDEN[] = {
    {.case_id = "complex-handler-USAGE-forbid-CALLS",
     .site = {.file = "Main.kt", .caller = "complex", .start_byte = 120, .end_byte = 127},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "complex-handler-USAGE-forbid-CALL_REFERENCE",
     .site = {.file = "Main.kt", .caller = "complex", .start_byte = 120, .end_byte = 127},
     .relation = "CALL_REFERENCE",
     .any_target = true},
    {.case_id = "complex-alternate-USAGE-forbid-CALLS",
     .site = {.file = "Main.kt", .caller = "complex", .start_byte = 135, .end_byte = 144},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "complex-alternate-USAGE-forbid-CALL_REFERENCE",
     .site = {.file = "Main.kt", .caller = "complex", .start_byte = 135, .end_byte = 144},
     .relation = "CALL_REFERENCE",
     .any_target = true},
    {.case_id = "wrapped-handler-USAGE-forbid-CALLS",
     .site = {.file = "Main.kt", .caller = "wrapped", .start_byte = 197, .end_byte = 204},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "wrapped-handler-USAGE-forbid-CALL_REFERENCE",
     .site = {.file = "Main.kt", .caller = "wrapped", .start_byte = 197, .end_byte = 204},
     .relation = "CALL_REFERENCE",
     .any_target = true},
    {.case_id = "wrapped-alternate-USAGE-forbid-CALLS",
     .site = {.file = "Main.kt", .caller = "wrapped", .start_byte = 212, .end_byte = 221},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "wrapped-alternate-USAGE-forbid-CALL_REFERENCE",
     .site = {.file = "Main.kt", .caller = "wrapped", .start_byte = 212, .end_byte = 221},
     .relation = "CALL_REFERENCE",
     .any_target = true},
    {.case_id = "direct-handler-CALL_REFERENCE-forbid-CALLS",
     .site = {.file = "Main.kt", .caller = "direct", .start_byte = 250, .end_byte = 257},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "direct-handler-CALL_REFERENCE-forbid-USAGE",
     .site = {.file = "Main.kt", .caller = "direct", .start_byte = 250, .end_byte = 257},
     .relation = "USAGE",
     .any_target = true},
    {.case_id = "parenthesized-handler-CALL_REFERENCE-forbid-CALLS",
     .site = {.file = "Main.kt", .caller = "parenthesized", .start_byte = 293, .end_byte = 300},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "parenthesized-handler-CALL_REFERENCE-forbid-USAGE",
     .site = {.file = "Main.kt", .caller = "parenthesized", .start_byte = 293, .end_byte = 300},
     .relation = "USAGE",
     .any_target = true},
    {.case_id = "standalone-handler-CALL_REFERENCE-forbid-CALLS",
     .site = {.file = "Main.kt", .caller = "standalone", .start_byte = 326, .end_byte = 333},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "standalone-handler-CALL_REFERENCE-forbid-USAGE",
     .site = {.file = "Main.kt", .caller = "standalone", .start_byte = 326, .end_byte = 333},
     .relation = "USAGE",
     .any_target = true},
    {.case_id = "immutable-handler-CALL_REFERENCE-forbid-CALLS",
     .site = {.file = "Main.kt", .caller = "immutable", .start_byte = 371, .end_byte = 378},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "immutable-handler-CALL_REFERENCE-forbid-USAGE",
     .site = {.file = "Main.kt", .caller = "immutable", .start_byte = 371, .end_byte = 378},
     .relation = "USAGE",
     .any_target = true},
    {.case_id = "mapped-handler-USAGE-forbid-CALLS",
     .site = {.file = "Mapped.kt", .caller = "mapped", .start_byte = 73, .end_byte = 80},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "mapped-handler-USAGE-forbid-CALL_REFERENCE",
     .site = {.file = "Mapped.kt", .caller = "mapped", .start_byte = 73, .end_byte = 80},
     .relation = "CALL_REFERENCE",
     .any_target = true},
    {.case_id = "mapped-alternate-USAGE-forbid-CALLS",
     .site = {.file = "Mapped.kt", .caller = "mapped", .start_byte = 96, .end_byte = 105},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "mapped-alternate-USAGE-forbid-CALL_REFERENCE",
     .site = {.file = "Mapped.kt", .caller = "mapped", .start_byte = 96, .end_byte = 105},
     .relation = "CALL_REFERENCE",
     .any_target = true}};

TEST(resolver_kotlin_composite_reference_raw_occurrences_stay_values) {
    int checked = 0;
    for (size_t f = 0; f < 2; f++) {
        const RQ_Source *source = &KOTLIN_COMPOSITE_SOURCES[f];
        CBMFileResult *file =
            cbm_extract_file(source->text, (int)strlen(source->text), CBM_LANG_KOTLIN,
                             "kotlin-composite", source->path, 0, NULL, NULL);
        ASSERT_NOT_NULL(file);
        ASSERT(!file->has_error && !file->parse_incomplete && !file->parse_unusable);
        bool ok = true;
        for (size_t i = 0;
             i < sizeof(KOTLIN_COMPOSITE_EXPECTED) / sizeof(KOTLIN_COMPOSITE_EXPECTED[0]); i++) {
            const RQ_Expected *expected = &KOTLIN_COMPOSITE_EXPECTED[i];
            if (strcmp(expected->site.file, source->path) || !strcmp(expected->relation, "CALLS"))
                continue;
            char caller[128];
            snprintf(caller, sizeof(caller), "kotlin-composite.%s.%s", f ? "Mapped" : "Main",
                     expected->site.caller);
            int found = 0;
            for (int u = 0; u < file->usages.count; u++) {
                const CBMUsage *usage = &file->usages.items[u];
                if (usage->site_start_byte != expected->site.start_byte ||
                    usage->site_end_byte != expected->site.end_byte)
                    continue;
                found++;
                ok = usage->kind == CBM_USAGE_VALUE &&
                     usage->source_origin == CBM_SOURCE_ORIGIN_RAW && usage->enclosing_func_qn &&
                     !strcmp(usage->enclosing_func_qn, caller) && ok;
            }
            ok = found == 1 && ok;
            checked++;
        }
        cbm_free_result(file);
        ASSERT(ok);
    }
    ASSERT_EQ(checked, 10);
    PASS();
}
TEST(resolver_kotlin_composite_arguments_preserve_reference_roles) {
    for (int driver = 0; driver < 2; driver++) {
        const char *root = th_mktempdir("cbm_kotlin_composite_roles");
        ASSERT_NOT_NULL(root);
        char repo[768], database[768];
        snprintf(repo, sizeof(repo), "%s/repo", root);
        snprintf(database, sizeof(database), "%s/index.db", root);
        bool ok = cbm_mkdir_p(repo, 0755);
        for (size_t i = 0;
             ok && i < sizeof(KOTLIN_COMPOSITE_SOURCES) / sizeof(KOTLIN_COMPOSITE_SOURCES[0]); i++)
            ok = th_write_file(TH_PATH(repo, KOTLIN_COMPOSITE_SOURCES[i].path),
                               KOTLIN_COMPOSITE_SOURCES[i].text) == 0;
        for (int i = 0; ok && i < 55; i++) {
            char path[40];
            snprintf(path, sizeof(path), "CompositePadding%02d.kt", i);
            ok = th_write_file(TH_PATH(repo, path), "// unrelated source file\n") == 0;
        }
        const char *old_workers = getenv("CBM_WORKERS");
        char *saved_workers = old_workers ? strdup(old_workers) : NULL;
        ok = ok && (!old_workers || saved_workers);
        cbm_setenv("CBM_WORKERS", driver ? "4" : "1", 1);
        cbm_pipeline_t *pipeline = ok ? cbm_pipeline_new(repo, database, CBM_MODE_FULL) : NULL;
        ok = pipeline && cbm_pipeline_set_project_name(pipeline, "kotlin-composite") &&
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
             rq_check(store, "kotlin-composite", "kotlin-composite-source-roles",
                      KOTLIN_COMPOSITE_SOURCES,
                      sizeof(KOTLIN_COMPOSITE_SOURCES) / sizeof(KOTLIN_COMPOSITE_SOURCES[0]),
                      KOTLIN_COMPOSITE_EXPECTED,
                      sizeof(KOTLIN_COMPOSITE_EXPECTED) / sizeof(KOTLIN_COMPOSITE_EXPECTED[0]),
                      KOTLIN_COMPOSITE_FORBIDDEN,
                      sizeof(KOTLIN_COMPOSITE_FORBIDDEN) / sizeof(KOTLIN_COMPOSITE_FORBIDDEN[0]),
                      &report, stderr) &&
             ok;
        if (store)
            cbm_store_close(store);
        th_cleanup(root);
        ASSERT(ok);
        ASSERT_EQ(report.genuine, 16);
        ASSERT_EQ(report.found, 16);
        ASSERT_EQ(report.missing, 0);
        ASSERT_EQ(report.required_missing + report.wrong, 0);
    }
    PASS();
}

SUITE(resolver_kotlin) {
    RUN_TEST(resolver_kotlin_composite_reference_raw_occurrences_stay_values);
    RUN_TEST(resolver_kotlin_composite_arguments_preserve_reference_roles);
    RUN_TEST(resolver_kotlin_callable_alias_occurrences_require_exact_binding);
    RUN_TEST(resolver_kotlin_primitive_arguments_and_lexical_members_require_proofs);
    RUN_TEST(resolver_kotlin_imported_call_keeps_builtin_parameter_and_logical_body);
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
