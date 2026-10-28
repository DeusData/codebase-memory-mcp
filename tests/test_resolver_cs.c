#include "test_framework.h"
#include "test_helpers.h"
#include "cbm.h"
#include "lsp/cs_proof.h"
#include "resolve/policy.h"
#include "pipeline/pipeline.h"
#include "store/store.h"
#include "sqlite3.h"
#include "resolver_quality.h"

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

static const char CS_PRIMITIVE_MAIN[] =
    "using Api;\n"
    "namespace App {\n"
    "class Calc {\n"
    " private int Helper(int x) { return x + 1; }\n"
    " public void Run(int y) {\n"
    "  Helper(y);\n"
    "  this.Helper(7);\n"
    "  Util.Square(y);\n"
    "  Util.Wide(y);\n"
    "  Util.Flag(true);\n"
    " }\n"
    " public void Gaps(int y) { Util.Over(y); Util.Narrow(1); Internal.Square(y); Util.Generic(y); "
    "}\n"
    " public void Local() { int value = 1; Util.Square(value); }\n"
    " public void Bad(long wide, object unknown) {\n"
    "  Util.Square(wide); Util.Square(unknown); Helper(1L); this.Helper(true); this.Static(1);\n"
    " }\n"
    " public static void StaticBad() { this.Helper(1); Helper(2); }\n"
    " public void ByRef(int y) { Util.Square(ref y); }\n"
    " public void Shadow(int Helper) { Helper(3); }\n"
    " private static int Static(int x) { return x; }\n"
    " public static int Forward(int y) { return Static(y); }\n"
    "}\n"
    "}\n";

static const char CS_PRIMITIVE_UTIL[] = "namespace Api {\n"
                                        "public class Util {\n"
                                        " public static int Square(int x) { return x * x; }\n"
                                        " public static long Wide(long x) { return x; }\n"
                                        " public static bool Flag(bool value) { return value; }\n"
                                        " public static byte Narrow(byte value) { return value; }\n"
                                        " public static int Over(int x) { return x; }\n"
                                        " public static long Over(long x) { return x; }\n"
                                        " public static T Generic<T>(T value) { return value; }\n"
                                        "}\n"
                                        "}\n";

static const char CS_PRIMITIVE_INTERNAL[] = "namespace App {\n"
                                            "class Internal {\n"
                                            " public static int Square(int x) { return x * x; }\n"
                                            "}\n"
                                            "}\n";

static const char CS_PRIMITIVE_GLOBAL[] = "class Global {\n"
                                          " private int Keep(int x) { return x; }\n"
                                          " public int Run(int y) { return Keep(y); }\n"
                                          "}\n";

static const char CS_PRIMITIVE_DECOY[] = "namespace Other {\n"
                                         "public class Util {\n"
                                         " public static int Square(int x) { return x; }\n"
                                         "}\n"
                                         "}\n";

static const RQ_Expected CS_PRIMITIVE_EXPECTED[] = {
    {.case_id = "private-implicit",
     .site = {.file = "main.cs", .caller = "Calc.Run", .needle = "Helper(y)"},
     .relation = "CALLS",
     .target = {.file = "main.cs",
                .suffix = "Calc.Helper",
                .label = "Method",
                .declaration_line = 4,
                .declaration_text = " private int Helper(int x) { return x + 1; }"}},
    {.case_id = "private-this",
     .site = {.file = "main.cs", .caller = "Calc.Run", .needle = "this.Helper(7)"},
     .relation = "CALLS",
     .target = {.file = "main.cs",
                .suffix = "Calc.Helper",
                .label = "Method",
                .declaration_line = 4,
                .declaration_text = " private int Helper(int x) { return x + 1; }"}},
    {.case_id = "public-crossfile-parameter",
     .site = {.file = "main.cs", .caller = "Calc.Run", .needle = "Util.Square(y)"},
     .relation = "CALLS",
     .target = {.file = "Util.cs",
                .suffix = "Util.Square",
                .label = "Method",
                .declaration_line = 3,
                .declaration_text = " public static int Square(int x) { return x * x; }"}},
    {.case_id = "primitive-widening",
     .site = {.file = "main.cs", .caller = "Calc.Run", .needle = "Util.Wide(y)"},
     .relation = "CALLS",
     .target = {.file = "Util.cs",
                .suffix = "Util.Wide",
                .label = "Method",
                .declaration_line = 4,
                .declaration_text = " public static long Wide(long x) { return x; }"}},
    {.case_id = "boolean-literal",
     .site = {.file = "main.cs", .caller = "Calc.Run", .needle = "Util.Flag(true)"},
     .relation = "CALLS",
     .target = {.file = "Util.cs",
                .suffix = "Util.Flag",
                .label = "Method",
                .declaration_line = 5,
                .declaration_text = " public static bool Flag(bool value) { return value; }"}},
    {.case_id = "implicit-static-context",
     .site = {.file = "main.cs", .caller = "Calc.Forward", .needle = "Static(y)"},
     .relation = "CALLS",
     .target = {.file = "main.cs",
                .suffix = "Calc.Static",
                .label = "Method",
                .declaration_line = 20,
                .declaration_text = " private static int Static(int x) { return x; }"}},
    {.case_id = "global-namespace-lexical",
     .site = {.file = "Global.cs", .caller = "Global.Run", .needle = "Keep(y)"},
     .relation = "CALLS",
     .target = {.file = "Global.cs",
                .suffix = "Global.Keep",
                .label = "Method",
                .declaration_line = 2,
                .declaration_text = " private int Keep(int x) { return x; }"}},
    {.case_id = "overload-selection",
     .site = {.file = "main.cs", .caller = "Calc.Gaps", .needle = "Util.Over(y)"},
     .relation = "CALLS",
     .target = {.file = "Util.cs",
                .suffix = "Util.Over",
                .label = "Method",
                .declaration_line = 7,
                .declaration_text = " public static int Over(int x) { return x; }"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "csharp-overload-selection",
     .why = "the complete name domain retains both source overloads",
     .tried = "int-to-int selects the int declaration in C#; argument checks intentionally do not "
              "rank overloads"},
    {.case_id = "constant-narrowing",
     .site = {.file = "main.cs", .caller = "Calc.Gaps", .needle = "Util.Narrow(1)"},
     .relation = "CALLS",
     .target = {.file = "Util.cs",
                .suffix = "Util.Narrow",
                .label = "Method",
                .declaration_line = 6,
                .declaration_text = " public static byte Narrow(byte value) { return value; }"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "csharp-constant-narrowing",
     .why = "constant-expression narrowing has no proof rule yet",
     .tried = "the literal 1 fits byte, but the current primitive rules admit only identity and "
              "widening"},
    {.case_id = "internal-assembly-visibility",
     .site = {.file = "main.cs", .caller = "Calc.Gaps", .needle = "Internal.Square(y)"},
     .relation = "CALLS",
     .target = {.file = "Internal.cs",
                .suffix = "Internal.Square",
                .label = "Method",
                .declaration_line = 3,
                .declaration_text = " public static int Square(int x) { return x * x; }"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "csharp-internal-assembly-visibility",
     .why = "cross-file internal accessibility requires an explicit compilation/assembly domain",
     .tried = "the fixture is one C# compilation; equal namespace and repository membership are "
              "insufficient production evidence"},
    {.case_id = "generic-inference",
     .site = {.file = "main.cs", .caller = "Calc.Gaps", .needle = "Util.Generic(y)"},
     .relation = "CALLS",
     .target = {.file = "Util.cs",
                .suffix = "Util.Generic",
                .label = "Method",
                .declaration_line = 9,
                .declaration_text = " public static T Generic<T>(T value) { return value; }"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "csharp-generic-inference",
     .why = "generic inference and substitutions are not implemented by this proof bridge",
     .tried = "the compiler infers T=int; primitive fixed-signature evidence cannot prove that "
              "substitution"},
    {.case_id = "local-primitive-flow",
     .site = {.file = "main.cs", .caller = "Calc.Local", .needle = "Util.Square(value)"},
     .relation = "CALLS",
     .target = {.file = "Util.cs",
                .suffix = "Util.Square",
                .label = "Method",
                .declaration_line = 3,
                .declaration_text = " public static int Square(int x) { return x * x; }"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "csharp-local-primitive-flow",
     .why = "local declaration and assignment flow remain conservative in this adapter",
     .tried = "the explicit int local is genuine; only immutable parameter facts and source "
              "literals are currently admitted"},
};

static const RQ_Forbidden CS_PRIMITIVE_FORBIDDEN[] = {
    {.case_id = "long-to-int-narrowing",
     .site = {.file = "main.cs", .caller = "Calc.Bad", .needle = "Util.Square(wide)"},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "unknown-argument-type",
     .site = {.file = "main.cs", .caller = "Calc.Bad", .needle = "Util.Square(unknown)"},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "long-literal-narrowing",
     .site = {.file = "main.cs", .caller = "Calc.Bad", .needle = "Helper(1L)"},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "bool-is-not-int",
     .site = {.file = "main.cs", .caller = "Calc.Bad", .needle = "this.Helper(true)"},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "static-through-this",
     .site = {.file = "main.cs", .caller = "Calc.Bad", .needle = "this.Static(1)"},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "this-from-static",
     .site = {.file = "main.cs", .caller = "Calc.StaticBad", .needle = "this.Helper(1)"},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "implicit-instance-from-static",
     .site = {.file = "main.cs", .caller = "Calc.StaticBad", .needle = "Helper(2)"},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "ref-is-not-value",
     .site = {.file = "main.cs", .caller = "Calc.ByRef", .needle = "Util.Square(ref y)"},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "lexical-name-shadow",
     .site = {.file = "main.cs", .caller = "Calc.Shadow", .needle = "Helper(3)"},
     .relation = "CALLS",
     .any_target = true},
};

TEST(resolver_cs_primitive_arguments_and_lexical_members_require_proofs) {
    for (int driver = 0; driver < 2; driver++) {
        const char *root = th_mktempdir("cbm_cs_primitive");
        ASSERT_NOT_NULL(root);
        char repo[768], database[768];
        snprintf(repo, sizeof(repo), "%s/repo", root);
        snprintf(database, sizeof(database), "%s/index.db", root);
        const RQ_Source sources[] = {{.path = "main.cs", .text = CS_PRIMITIVE_MAIN},
                                     {.path = "Util.cs", .text = CS_PRIMITIVE_UTIL},
                                     {.path = "Internal.cs", .text = CS_PRIMITIVE_INTERNAL},
                                     {.path = "Global.cs", .text = CS_PRIMITIVE_GLOBAL},
                                     {.path = "Decoy.cs", .text = CS_PRIMITIVE_DECOY}};
        bool ok = cbm_mkdir_p(repo, 0755);
        for (size_t i = 0; ok && i < sizeof(sources) / sizeof(sources[0]); i++)
            ok = th_write_file(TH_PATH(repo, sources[i].path), sources[i].text) == 0;
        for (int i = 0; ok && i < 55; i++) {
            char path[32];
            snprintf(path, sizeof(path), "Filler%02d.cs", i);
            ok = th_write_file(TH_PATH(repo, path), "// unrelated source file\n") == 0;
        }
        const char *old_workers = getenv("CBM_WORKERS");
        char *saved_workers = old_workers ? strdup(old_workers) : NULL;
        ok = ok && (!old_workers || saved_workers);
        cbm_setenv("CBM_WORKERS", driver ? "4" : "1", 1);
        cbm_pipeline_t *pipeline = ok ? cbm_pipeline_new(repo, database, CBM_MODE_FULL) : NULL;
        ok = pipeline && cbm_pipeline_set_project_name(pipeline, "cs-direct") &&
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
             rq_check(store, "cs-direct", "primitive-csharp-members", sources,
                      sizeof(sources) / sizeof(sources[0]), CS_PRIMITIVE_EXPECTED,
                      sizeof(CS_PRIMITIVE_EXPECTED) / sizeof(CS_PRIMITIVE_EXPECTED[0]),
                      CS_PRIMITIVE_FORBIDDEN,
                      sizeof(CS_PRIMITIVE_FORBIDDEN) / sizeof(CS_PRIMITIVE_FORBIDDEN[0]), &report,
                      stderr) &&
             ok;
        if (store)
            cbm_store_close(store);
        th_cleanup(root);
        ASSERT(ok);
        ASSERT_EQ(report.genuine, 12);
        ASSERT_EQ(report.found, 7);
        ASSERT_EQ(report.missing, 5);
        ASSERT_EQ(report.required_missing + report.wrong, 0);
    }
    PASS();
}

SUITE(resolver_cs) {
    RUN_TEST(resolver_cs_primitive_arguments_and_lexical_members_require_proofs);
    RUN_TEST(resolver_cs_alias_and_namespace_declared_receivers);
    RUN_TEST(resolver_cs_shadowed_mutated_and_unknown_types_abstain);
    RUN_TEST(resolver_cs_private_partial_inherited_and_duplicate_domains_abstain);
    RUN_TEST(resolver_cs_pipeline_preserves_exact_crossfile_target);
}
