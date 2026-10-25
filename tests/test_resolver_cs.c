#include "test_framework.h"
#include "test_helpers.h"
#include "cbm.h"
#include "lsp/cs_proof.h"
#include "resolve/policy.h"
#include "pipeline/pipeline.h"
#include "store/store.h"
#include "sqlite3.h"

static const char CS_SERVICE[] = "namespace Api {\npublic class Service {\n public void Go() {}\n "
                                 "public static void Build() {}\n}}\n";
static int cs_rows(const char *source, const char *library, int competitor, bool types) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMLSPDef defs[4] = {{.qualified_name = "p.Service.Service",
                          .graph_qualified_name = "p.Service.Service",
                          .short_name = "Service",
                          .label = "Class",
                          .source_start_line = 2,
                          .declaration_identity = "lib:0",
                          .def_module_qn = "p.Service",
                          .lang = CBM_LANG_CSHARP},
                         {.qualified_name = "p.Service.Service.Go",
                          .graph_qualified_name = "p.Service.Service.Go",
                          .short_name = "Go",
                          .receiver_type = "p.Service.Service",
                          .label = "Method",
                          .source_start_line = 3,
                          .declaration_identity = "lib:1",
                          .lang = CBM_LANG_CSHARP},
                         {.qualified_name = "p.Service.Service.Build",
                          .graph_qualified_name = "p.Service.Service.Build",
                          .short_name = "Build",
                          .receiver_type = "p.Service.Service",
                          .label = "Method",
                          .source_start_line = 4,
                          .declaration_identity = "lib:2",
                          .lang = CBM_LANG_CSHARP}};
    bool facts =
        cbm_cs_collect_source_facts(&arena, library, (int)strlen(library), "p.Service", defs, 3);
    int count = 3;
    if (competitor) {
        defs[3] = defs[competitor == 1 ? 1 : 0];
        defs[3].declaration_identity = "other:0";
        count = 4;
    }
    CBMTypeRegistry *registry = cbm_cs_build_cross_registry(&arena, defs, count);
    CBMFileResult *file = cbm_extract_file(source, (int)strlen(source), CBM_LANG_CSHARP, "p",
                                           "Main.cs", 0, NULL, NULL);
    TSParser *parser = ts_parser_new();
    extern const TSLanguage *tree_sitter_c_sharp(void);
    ts_parser_set_language(parser, tree_sitter_c_sharp());
    TSTree *tree = ts_parser_parse_string(parser, NULL, source, (uint32_t)strlen(source));
    ts_parser_delete(parser);
    CBMResolvedCallArray out = {0};
    if (facts && registry && file && tree)
        cbm_cs_proof_resolve(&arena, source, (int)strlen(source), ts_tree_root_node(tree), registry,
                             file, &out);
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
TEST(resolver_cs_alias_and_namespace_declared_receivers) {
    const char *alias = "using Svc = Api.Service;\nnamespace Client { public class Main { public "
                        "void Run(Svc value) { value.Go(); Svc.Build(); } } }\n";
    ASSERT_EQ(cs_rows(alias, CS_SERVICE, 0, false), 2);
    ASSERT_EQ(cs_rows(alias, CS_SERVICE, 0, true), 4);
    const char *using = "using Api;\nnamespace Client { public class Main { public void "
                        "Run(Service value) { value.Go(); } } }\n";
    ASSERT_EQ(cs_rows(using, CS_SERVICE, 0, false), 1);
    const char *same =
        "namespace Api { public class Main { public void Run(Service value) { value.Go(); } } }\n";
    ASSERT_EQ(cs_rows(same, CS_SERVICE, 0, false), 1);
    PASS();
}
TEST(resolver_cs_shadowed_mutated_and_unknown_types_abstain) {
    const char *sources[] = {
        ("using Api; public class Main { public void Run(Unknown value) { value.Go(); } }"),
        ("using Api; public class Main { public void Run<Service>(Service value) { value.Go(); } "
         "}"),
        ("using Api; public class Main<Service> { public void Run(Service value) { value.Go(); } "
         "}"),
        ("using Api; public class Main { public void Run(Service value) { value=other; value.Go(); "
         "} }"),
        ("using Api; public class Main { public void Run(Service value) { Change(ref value); "
         "value.Go(); } }"),
        ("using Api; public class Main { public void Run(Service value) { value.Go(1); } }"),
        ("using Svc=Api.Service; public class Main { public void Run(Unknown Svc) { Svc.Build(); } "
         "}"),
        ("using Svc=Api.Service; public class Main { public void Run() { var Svc=other; "
         "Svc.Build(); } }"),
        ("using Svc=Api.Service; public class Main { Unknown Svc; public void Run() { Svc.Build(); "
         "} }"),
        ("namespace A { using Svc=Api.Service; public class First {} } namespace B { public class "
         "Main { public void Run(Svc value) { value.Go(); } } }"),
        ("namespace Client; using Svc=Api.Service; public class Main { public void Run(Svc value) "
         "{ value.Go(); } }")};
    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++) {
        int found = cs_rows(sources[i], CS_SERVICE, 0, false);
        if (found)
            fprintf(stderr, "C# unexpected proof scenario %zu: %d\n", i, found);
        ASSERT_EQ(found, 0);
    }
    PASS();
}
TEST(resolver_cs_private_partial_inherited_and_duplicate_domains_abstain) {
    const char *source =
        "using Api; public class Main { public void Run(Service value) { value.Go(); } }";
    const char *private = "namespace Api {\npublic class Service {\n private void Go() {}\n public "
                          "static void Build() {}\n}}\n";
    const char *partial = "namespace Api {\npublic partial class Service {\n public void Go() {}\n "
                          "public static void Build() {}\n}}\n";
    const char *inherited = "namespace Api {\npublic class Service : Parent {\n public void Go() "
                            "{}\n public static void Build() {}\n}}\n";
    ASSERT_EQ(cs_rows(source, private, 0, false), 0);
    ASSERT_EQ(cs_rows(source, partial, 0, false), 0);
    ASSERT_EQ(cs_rows(source, inherited, 0, false), 0);
    ASSERT_EQ(cs_rows(source, CS_SERVICE, 1, false), 0);
    ASSERT_EQ(cs_rows(source, CS_SERVICE, 2, false), 0);
    PASS();
}
TEST(resolver_cs_pipeline_preserves_exact_crossfile_target) {
    const char *root = th_mktempdir("cbm_resolver_cs");
    ASSERT_NOT_NULL(root);
    char repo[768], database[768];
    snprintf(repo, sizeof(repo), "%s/repo", root);
    snprintf(database, sizeof(database), "%s/index.db", root);
    ASSERT(cbm_mkdir_p(repo, 0755));
    bool files =
        th_write_file(TH_PATH(repo, "Service.cs"), CS_SERVICE) == 0 &&
        th_write_file(TH_PATH(repo, "Main.cs"),
                      "using Api; namespace Client { public class Main { public void Run(Service "
                      "value) { value.Go(); } } }") == 0 &&
        th_write_file(TH_PATH(repo, "Decoy.cs"),
                      "namespace Other { public class Service { public void Go() {} } }") == 0;
    cbm_pipeline_t *pipeline = files ? cbm_pipeline_new(repo, database, CBM_MODE_FULL) : NULL;
    int rc = pipeline && cbm_pipeline_set_project_name(pipeline, "cs-proof")
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
            "WHERE e.type='CALLS' AND s.file_path='Main.cs' AND t.file_path='Service.cs'",
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
                fprintf(stderr, "C# pipeline diagnostic:");
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
SUITE(resolver_cs) {
    RUN_TEST(resolver_cs_alias_and_namespace_declared_receivers);
    RUN_TEST(resolver_cs_shadowed_mutated_and_unknown_types_abstain);
    RUN_TEST(resolver_cs_private_partial_inherited_and_duplicate_domains_abstain);
    RUN_TEST(resolver_cs_pipeline_preserves_exact_crossfile_target);
}
