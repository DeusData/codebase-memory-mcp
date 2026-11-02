#include "test_framework.h"
#include "test_helpers.h"
#include "cbm.h"
#include "lsp/java_lsp.h"
#include "resolve/policy.h"
#include "pipeline/pipeline.h"
#include "store/store.h"
#include "sqlite3.h"
#include "resolver_quality.h"

static bool java_row_admitted(const CBMResolvedCall *row) {
    cbm_resolve_result_t result = {.binding = CBM_RESOLVE_BINDING_UNIQUE,
                                   .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                   .analysis = row->evidence.analysis,
                                   .rule = row->evidence.rule,
                                   .facts = row->evidence.facts,
                                   .target = row->callee_qn,
                                   .candidate_count = 1};
    return row->site_end_byte > row->site_start_byte && cbm_resolve_admit(&result);
}

static int java_proven_count(const CBMResolvedCallArray *rows, const char *target) {
    int count = 0;
    for (int i = 0; i < rows->count; i++)
        if (java_row_admitted(&rows->items[i]) &&
            (!target || !strcmp(target, rows->items[i].callee_qn)))
            count++;
    return count;
}

static const char SERVICE_SOURCE[] = "package api;\npublic class Service {\n"
                                     " public void call() {}\n"
                                     " public static void create() {}\n}\n";

static void java_service_defs(CBMLSPDef defs[3]) {
    defs[0] = (CBMLSPDef){.qualified_name = "api.Service",
                          .graph_qualified_name = "test.Service.Service",
                          .short_name = "Service",
                          .label = "Class",
                          .def_module_qn = "test.Service",
                          .namespace_name = "api",
                          .source_start_line = 2,
                          .declaration_identity = "Service:0"};
    defs[1] = (CBMLSPDef){.qualified_name = "api.Service.call",
                          .graph_qualified_name = "test.Service.Service.call",
                          .receiver_type = "api.Service",
                          .short_name = "call",
                          .label = "Method",
                          .source_start_line = 3,
                          .declaration_identity = "Service:1"};
    defs[2] = (CBMLSPDef){.qualified_name = "api.Service.create",
                          .graph_qualified_name = "test.Service.Service.create",
                          .receiver_type = "api.Service",
                          .short_name = "create",
                          .label = "Method",
                          .source_start_line = 4,
                          .declaration_identity = "Service:2"};
}

static void java_cross(CBMArena *arena, const char *source, CBMLSPDef *defs, int count,
                       CBMResolvedCallArray *out) {
    cbm_run_java_lsp_cross(arena, source, (int)strlen(source), "test.Main", defs, count, NULL, NULL,
                           0, NULL, out);
}

TEST(resolver_java_declared_receiver_static_and_reference_have_proof) {
    const char *source = "package client; import api.Service;\n"
                         "public class Main { public void run(Service service) {\n"
                         "service.call(); Service alias = service; alias.call();\n"
                         "consume(service::call); Service.create(); } }";
    CBMLSPDef defs[3];
    java_service_defs(defs);
    CBMArena arena;
    cbm_arena_init(&arena);
    bool facts = cbm_java_collect_source_facts(&arena, SERVICE_SOURCE, (int)strlen(SERVICE_SOURCE),
                                               "test.Service", defs, 3);
    CBMResolvedCallArray out = {0};
    java_cross(&arena, source, defs, 3, &out);
    int call = java_proven_count(&out, "test.Service.Service.call");
    int create = java_proven_count(&out, "test.Service.Service.create");
    int wrong = java_proven_count(&out, "api.Service.call");
    cbm_arena_destroy(&arena);
    ASSERT(facts);
    ASSERT_EQ(call, 3);
    ASSERT_EQ(create, 1);
    ASSERT_EQ(wrong, 0);
    PASS();
}

TEST(resolver_java_wrong_arity_varargs_and_null_source_abstain) {
    const char *source =
        "package client; import api.Service; public class Main {"
        "public void run(Service service) { service.call(1); Service.create(1); } }";
    const char *variadic_source = "package api;\npublic class Service {\n"
                                  " public void call(String... args) {}\n"
                                  " public static void create() {}\n}";
    CBMLSPDef defs[3];
    java_service_defs(defs);
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_java_collect_source_facts(&arena, SERVICE_SOURCE, (int)strlen(SERVICE_SOURCE),
                                  "test.Service", defs, 3);
    CBMResolvedCallArray wrong = {0}, variadic = {0}, missing = {0};
    java_cross(&arena, source, defs, 3, &wrong);
    const char *parameter_types[] = {"String[]", NULL};
    defs[1].signature_param_count = 1;
    defs[1].signature_param_types = parameter_types;
    cbm_java_collect_source_facts(&arena, variadic_source, (int)strlen(variadic_source),
                                  "test.Service", defs, 3);
    java_cross(&arena, source, defs, 3, &variadic);
    bool null_result = cbm_java_collect_source_facts(&arena, NULL, 0, "test.Service", defs, 3);
    java_cross(&arena, "import api.Service; class Main { void run(Service s) { s.call(); } }", defs,
               3, &missing);
    int count = java_proven_count(&wrong, NULL) + java_proven_count(&variadic, NULL) +
                java_proven_count(&missing, NULL);
    cbm_arena_destroy(&arena);
    ASSERT(!null_result);
    ASSERT_EQ(count, 0);
    PASS();
}

TEST(resolver_java_full_graph_identity_does_not_duplicate_signature) {
    CBMLSPDef defs[3];
    java_service_defs(defs);
    defs[1].graph_qualified_name = "test.Service.Service.call()";
    defs[1].sig = "()";
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_java_collect_source_facts(&arena, SERVICE_SOURCE, (int)strlen(SERVICE_SOURCE),
                                  "test.Service", defs, 3);
    CBMResolvedCallArray out = {0};
    java_cross(&arena, "import api.Service; class Main { void run(Service s) { s.call(); } }", defs,
               3, &out);
    int count = java_proven_count(&out, "test.Service.Service.call()");
    int duplicate = java_proven_count(&out, "test.Service.Service.call()()");
    cbm_arena_destroy(&arena);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(duplicate, 0);
    PASS();
}

TEST(resolver_java_unknown_shadow_alias_and_type_parameter_abstain) {
    const char *source = "package client; import api.Service;\n"
                         "public class Main {\n"
                         " public void run(Missing Service, Missing unknown) {\n"
                         "Service.create(); unknown.call(); var alias = unknown; alias.call(); }\n"
                         " public <Service> void generic(Service value) { value.call(); } }";
    CBMLSPDef defs[3];
    java_service_defs(defs);
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_java_collect_source_facts(&arena, SERVICE_SOURCE, (int)strlen(SERVICE_SOURCE),
                                  "test.Service", defs, 3);
    CBMResolvedCallArray out = {0};
    java_cross(&arena, source, defs, 3, &out);
    int count = java_proven_count(&out, NULL);
    cbm_arena_destroy(&arena);
    ASSERT_EQ(count, 0);
    PASS();
}

TEST(resolver_java_source_modifiers_and_missing_facts_abstain) {
    const char *source = "package client; import api.Service;\n"
                         "public class Main { public void run(Service value) { value.call(); } }";
    const char *private_source = "package api;\npublic class Service {\n"
                                 " private void call() {}\n public static void create() {}\n}";
    CBMLSPDef defs[3];
    java_service_defs(defs);
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray absent = {0}, private = {0};
    java_cross(&arena, source, defs, 3, &absent);
    cbm_java_collect_source_facts(&arena, private_source, (int)strlen(private_source),
                                  "test.Service", defs, 3);
    java_cross(&arena, source, defs, 3, &private);
    int count = java_proven_count(&absent, NULL) + java_proven_count(&private, NULL);
    cbm_arena_destroy(&arena);
    ASSERT_EQ(count, 0);
    PASS();
}

TEST(resolver_java_interface_target_and_inheritance_boundaries) {
    const char *interface_source = "package api;\npublic interface Reader {\n void read();\n}";
    CBMLSPDef defs[] = {{.qualified_name = "api.Reader",
                         .graph_qualified_name = "test.Reader.Reader",
                         .short_name = "Reader",
                         .label = "Interface",
                         .def_module_qn = "test.Reader",
                         .namespace_name = "api",
                         .source_start_line = 2,
                         .declaration_identity = "Reader:0"},
                        {.qualified_name = "api.Reader.read",
                         .graph_qualified_name = "test.Reader.Reader.read",
                         .short_name = "read",
                         .label = "Method",
                         .receiver_type = "api.Reader",
                         .source_start_line = 3,
                         .declaration_identity = "Reader:1"},
                        {.qualified_name = "api.Impl.read",
                         .graph_qualified_name = "test.Impl.Impl.read",
                         .short_name = "read",
                         .label = "Method",
                         .receiver_type = "api.Impl"}};
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_java_collect_source_facts(&arena, interface_source, (int)strlen(interface_source),
                                  "test.Reader", defs, 2);
    CBMResolvedCallArray out = {0}, inherited = {0};
    const char *source = "package client; import api.Reader; public class Main {\n"
                         "public void run(Reader reader) { reader.read(); } }";
    java_cross(&arena, source, defs, 3, &out);
    int reader = java_proven_count(&out, "test.Reader.Reader.read");
    int impl = java_proven_count(&out, "test.Impl.Impl.read");
    defs[0].source_flags &= ~CBM_SOURCE_CLASS_DOMAIN_COMPLETE;
    defs[0].embedded_types = "api.Parent";
    java_cross(&arena, source, defs, 3, &inherited);
    int inherited_count = java_proven_count(&inherited, NULL);
    cbm_arena_destroy(&arena);
    ASSERT_EQ(reader, 1);
    ASSERT_EQ(impl, 0);
    ASSERT_EQ(inherited_count, 0);
    PASS();
}

TEST(resolver_java_overloads_namespace_collisions_and_object_members_abstain) {
    const char *source =
        "package client; import api.Service; public class Main {\n"
        "public void run(Service service) { service.call(); service.equals(service); } }";
    CBMLSPDef defs[5];
    java_service_defs(defs);
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_java_collect_source_facts(&arena, SERVICE_SOURCE, (int)strlen(SERVICE_SOURCE),
                                  "test.Service", defs, 3);
    defs[3] = defs[1];
    defs[3].sig = "(int)";
    defs[3].declaration_identity = "Service:overload";
    defs[4] = defs[1];
    defs[4].qualified_name = "api.Service.equals";
    defs[4].graph_qualified_name = "test.Service.Service.equals";
    defs[4].short_name = "equals";
    defs[4].declaration_identity = "Service:equals-int";
    CBMResolvedCallArray overload = {0}, collision = {0};
    java_cross(&arena, source, defs, 5, &overload);
    defs[3] = defs[0];
    defs[3].graph_qualified_name = "other.Service.Service";
    defs[3].def_module_qn = "other.Service";
    defs[3].declaration_identity = "Other:0";
    java_cross(&arena, source, defs, 4, &collision);
    int count = java_proven_count(&overload, NULL) + java_proven_count(&collision, NULL);
    cbm_arena_destroy(&arena);
    ASSERT_EQ(count, 0);
    PASS();
}

TEST(resolver_namespace_index_is_exact_complete_and_bounded) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMTypeRegistry registry;
    cbm_registry_init(&registry, &arena);
    for (int i = 0; i < 512; i++) {
        CBMRegisteredType type = {.qualified_name = cbm_arena_sprintf(&arena, "graph.T%d", i),
                                  .short_name = cbm_arena_sprintf(&arena, "T%d", i),
                                  .namespace_qn = "source.pkg",
                                  .project_source = true};
        cbm_registry_add_type(&registry, type);
    }
    cbm_registry_finalize(&registry);
    CBMNamespaceTypeIter iterator;
    cbm_registry_types_in_namespace(&registry, "source.pkg", "T256", &iterator);
    const CBMRegisteredType *found = cbm_namespace_type_next(&iterator, NULL);
    bool exact = found && !strcmp(found->qualified_name, "graph.T256");
    bool only = cbm_namespace_type_next(&iterator, NULL) == NULL;
    bool complete = iterator.complete;
    uint64_t work = iterator.visits;
    CBMRegisteredType tail = {
        .qualified_name = "graph.new", .short_name = "T256", .namespace_qn = "source.pkg"};
    cbm_registry_add_type(&registry, tail);
    cbm_registry_types_in_namespace(&registry, "source.pkg", "T256", &iterator);
    bool tail_rejected = !cbm_namespace_type_next(&iterator, NULL) && !iterator.complete;
    cbm_arena_destroy(&arena);
    ASSERT(exact && only && complete && tail_rejected);
    ASSERT(work < 24);
    PASS();
}

TEST(resolver_java_pipeline_keeps_canonical_crossfile_target) {
    const char *root = th_mktempdir("cbm_resolver_java");
    ASSERT_NOT_NULL(root);
    char repo[768], database[768];
    snprintf(repo, sizeof(repo), "%s/repo", root);
    snprintf(database, sizeof(database), "%s/index.db", root);
    ASSERT(cbm_mkdir_p(repo, 0755));
    ASSERT(cbm_mkdir_p(TH_PATH(repo, "api"), 0755));
    ASSERT(cbm_mkdir_p(TH_PATH(repo, "other"), 0755));
    const char *main_source = "package client; import api.Service;\npublic class Main {\n"
                              " public void run(Service value) { value.call(); }\n}\n";
    bool files =
        th_write_file(TH_PATH(repo, "Main.java"), main_source) == 0 &&
        th_write_file(TH_PATH(repo, "api/Service.java"), SERVICE_SOURCE) == 0 &&
        th_write_file(TH_PATH(repo, "other/Service.java"),
                      "package other; public class Service { public void call() {} }") == 0;
    cbm_pipeline_t *pipeline = files ? cbm_pipeline_new(repo, database, CBM_MODE_FULL) : NULL;
    int rc = pipeline && cbm_pipeline_set_project_name(pipeline, "java-proof")
                 ? cbm_pipeline_run(pipeline)
                 : -1;
    cbm_pipeline_free(pipeline);
    cbm_store_t *store = rc == 0 ? cbm_store_open_path_existing(database) : NULL;
    sqlite3_stmt *statement = NULL;
    int edges = -1;
    if (store &&
        sqlite3_prepare_v2(cbm_store_get_db(store),
                           "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id "
                           "JOIN nodes t ON t.id=e.target_id WHERE e.type='CALLS' "
                           "AND s.qualified_name='java-proof.Main.run' "
                           "AND t.qualified_name='java-proof.api.Service.call'",
                           -1, &statement, NULL) == SQLITE_OK &&
        sqlite3_step(statement) == SQLITE_ROW)
        edges = sqlite3_column_int(statement, 0);
    sqlite3_finalize(statement);
    if (store && edges != 1) {
        const char *queries[] = {"SELECT qualified_name, label FROM nodes WHERE file_path IN "
                                 "('Main.java','Service.java')",
                                 "SELECT scope,spelling,target,reason,facts FROM "
                                 "resolver_occurrences WHERE relation='CALLS'"};
        for (size_t q = 0; q < sizeof(queries) / sizeof(queries[0]); q++) {
            statement = NULL;
            if (sqlite3_prepare_v2(cbm_store_get_db(store), queries[q], -1, &statement, NULL) ==
                SQLITE_OK)
                while (sqlite3_step(statement) == SQLITE_ROW) {
                    fprintf(stderr, "java proof diagnostic:");
                    for (int c = 0; c < sqlite3_column_count(statement); c++)
                        fprintf(stderr, " %s",
                                sqlite3_column_text(statement, c)
                                    ? (const char *)sqlite3_column_text(statement, c)
                                    : "NULL");
                    fprintf(stderr, "\n");
                }
            sqlite3_finalize(statement);
        }
    }
    if (store)
        cbm_store_close(store);
    th_cleanup(root);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(edges, 1);
    PASS();
}

static const char JAVA_DIRECT_SOURCE[] =
    "package app;\n"
    "class Calc {\n"
    " private int helper(int x) { return x + 1; }\n"
    " int twice(int x) { return x * 2; }\n"
    " long widen(long x) { return x; }\n"
    " boolean flag(boolean x) { return x; }\n"
    " int overloaded(int x) { return x; }\n"
    " int overloaded(long x) { return 0; }\n"
    " void run(int y, Alpha alpha, Beta beta) {\n"
    "  helper(y);\n"
    "  this.helper(7);\n"
    "  twice(helper(y));\n"
    "  widen(y);\n"
    "  flag(true);\n"
    "  alpha.handle();\n"
    "  beta.handle();\n"
    "  overloaded(y);\n"
    " }\n"
    " void forbidden(Other other, String unknown, int[] values) {\n"
    "  helper(unknown); helper(values); helper(1L); other.hidden(); Calc.helper(1);\n"
    " }\n"
    " static void staticForbidden() { helper(1); this.helper(1); }\n"
    "}\n";

static const char JAVA_PACKAGE_SOURCE[] = "package app;\n"
                                          "class Alpha { void handle() {} }\n"
                                          "class Beta { void handle() {} }\n"
                                          "class Other { private void hidden() {} }\n";

static const RQ_Expected JAVA_DIRECT_EXPECTED[] = {
    {.case_id = "implicit-private-primitive-argument",
     .site = {.file = "main.java", .caller = "Calc.run", .needle = "helper(y)"},
     .relation = "CALLS",
     .target = {.file = "main.java",
                .suffix = "Calc.helper",
                .label = "Method",
                .declaration_line = 3,
                .declaration_text = " private int helper(int x) { return x + 1; }"}},
    {.case_id = "explicit-this-primitive-literal",
     .site = {.file = "main.java", .caller = "Calc.run", .needle = "this.helper(7)"},
     .relation = "CALLS",
     .target = {.file = "main.java",
                .suffix = "Calc.helper",
                .label = "Method",
                .declaration_line = 3,
                .declaration_text = " private int helper(int x) { return x + 1; }"}},
    {.case_id = "source-proven-nested-return",
     .site = {.file = "main.java", .caller = "Calc.run", .needle = "twice(helper(y))"},
     .relation = "CALLS",
     .target = {.file = "main.java",
                .suffix = "Calc.twice",
                .label = "Method",
                .declaration_line = 4,
                .declaration_text = " int twice(int x) { return x * 2; }"}},
    {.case_id = "nested-invocation-identity",
     .site =
         {.file = "main.java", .caller = "Calc.run", .needle = "helper(y)", .occurrence_index = 1},
     .relation = "CALLS",
     .target = {.file = "main.java",
                .suffix = "Calc.helper",
                .label = "Method",
                .declaration_line = 3,
                .declaration_text = " private int helper(int x) { return x + 1; }"}},
    {.case_id = "primitive-widening",
     .site = {.file = "main.java", .caller = "Calc.run", .needle = "widen(y)"},
     .relation = "CALLS",
     .target = {.file = "main.java",
                .suffix = "Calc.widen",
                .label = "Method",
                .declaration_line = 5,
                .declaration_text = " long widen(long x) { return x; }"}},
    {.case_id = "boolean-identity",
     .site = {.file = "main.java", .caller = "Calc.run", .needle = "flag(true)"},
     .relation = "CALLS",
     .target = {.file = "main.java",
                .suffix = "Calc.flag",
                .label = "Method",
                .declaration_line = 6,
                .declaration_text = " boolean flag(boolean x) { return x; }"}},
    {.case_id = "same-package-alpha",
     .site = {.file = "main.java", .caller = "Calc.run", .needle = "alpha.handle()"},
     .relation = "CALLS",
     .target = {.file = "support.java",
                .suffix = "Alpha.handle",
                .label = "Method",
                .declaration_line = 2,
                .declaration_text = "class Alpha { void handle() {} }"}},
    {.case_id = "same-package-beta",
     .site = {.file = "main.java", .caller = "Calc.run", .needle = "beta.handle()"},
     .relation = "CALLS",
     .target = {.file = "support.java",
                .suffix = "Beta.handle",
                .label = "Method",
                .declaration_line = 3,
                .declaration_text = "class Beta { void handle() {} }"}},
    /* JLS picks the int overload; both are one graph node, carrying the last line. */
    {.case_id = "overload-selection",
     .site = {.file = "main.java", .caller = "Calc.run", .needle = "overloaded(y)"},
     .relation = "CALLS",
     .target = {.file = "main.java",
                .suffix = "Calc.overloaded",
                .label = "Method",
                .declaration_line = 8,
                .declaration_text = " int overloaded(long x) { return 0; }"}},
};

static const RQ_Forbidden JAVA_DIRECT_FORBIDDEN[] = {
    {.case_id = "unknown-argument",
     .site = {.file = "main.java", .caller = "Calc.forbidden", .needle = "helper(unknown)"},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "array-is-not-primitive",
     .site = {.file = "main.java", .caller = "Calc.forbidden", .needle = "helper(values)"},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "narrowing-is-not-invocation-conversion",
     .site = {.file = "main.java", .caller = "Calc.forbidden", .needle = "helper(1L)"},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "private-owner-mismatch",
     .site = {.file = "main.java", .caller = "Calc.forbidden", .needle = "other.hidden()"},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "instance-through-class",
     .site = {.file = "main.java", .caller = "Calc.forbidden", .needle = "Calc.helper(1)"},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "implicit-instance-from-static",
     .site = {.file = "main.java",
              .caller = "Calc.staticForbidden",
              .needle = "helper(1)",
              .occurrence_index = 1},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "explicit-this-from-static",
     .site = {.file = "main.java", .caller = "Calc.staticForbidden", .needle = "this.helper(1)"},
     .relation = "CALLS",
     .any_target = true},
};

TEST(resolver_java_direct_members_require_source_applicability) {
    for (int driver = 0; driver < 2; driver++) {
        const char *root = th_mktempdir("cbm_java_direct");
        ASSERT_NOT_NULL(root);
        char repo[768], database[768];
        snprintf(repo, sizeof(repo), "%s/repo", root);
        snprintf(database, sizeof(database), "%s/index.db", root);
        bool ok = cbm_mkdir_p(repo, 0755) &&
                  th_write_file(TH_PATH(repo, "main.java"), JAVA_DIRECT_SOURCE) == 0 &&
                  th_write_file(TH_PATH(repo, "support.java"), JAVA_PACKAGE_SOURCE) == 0;
        for (int i = 0; ok && i < 55; i++) {
            char file[32];
            snprintf(file, sizeof(file), "Filler%02d.java", i);
            ok = th_write_file(TH_PATH(repo, file), "// unrelated compilation unit\n") == 0;
        }
        const char *old_workers = getenv("CBM_WORKERS");
        char *saved_workers = old_workers ? strdup(old_workers) : NULL;
        ok = ok && (!old_workers || saved_workers);
        cbm_setenv("CBM_WORKERS", driver ? "4" : "1", 1);
        cbm_pipeline_t *pipeline = ok ? cbm_pipeline_new(repo, database, CBM_MODE_FULL) : NULL;
        ok = pipeline && cbm_pipeline_set_project_name(pipeline, "java-direct") &&
             cbm_pipeline_run(pipeline) == 0;
        cbm_pipeline_free(pipeline);
        if (saved_workers) {
            cbm_setenv("CBM_WORKERS", saved_workers, 1);
            free(saved_workers);
        } else
            cbm_unsetenv("CBM_WORKERS");
        cbm_store_t *store = ok ? cbm_store_open_path_existing(database) : NULL;
        const RQ_Source sources[] = {{.path = "main.java", .text = JAVA_DIRECT_SOURCE},
                                     {.path = "support.java", .text = JAVA_PACKAGE_SOURCE}};
        RQ_Report report = {0};
        ok = store &&
             rq_check(store, "java-direct", "direct-java-members", sources, 2, JAVA_DIRECT_EXPECTED,
                      sizeof(JAVA_DIRECT_EXPECTED) / sizeof(JAVA_DIRECT_EXPECTED[0]),
                      JAVA_DIRECT_FORBIDDEN,
                      sizeof(JAVA_DIRECT_FORBIDDEN) / sizeof(JAVA_DIRECT_FORBIDDEN[0]), &report,
                      stderr) &&
             ok;
        if (store)
            cbm_store_close(store);
        th_cleanup(root);
        ASSERT(ok);
        ASSERT_EQ(report.genuine, 9);
        ASSERT_EQ(report.found, 9);
        ASSERT_EQ(report.missing, 0);
        ASSERT_EQ(report.required_missing + report.wrong, 0);
    }
    PASS();
}

SUITE(resolver_java) {
    RUN_TEST(resolver_java_direct_members_require_source_applicability);
    RUN_TEST(resolver_java_declared_receiver_static_and_reference_have_proof);
    RUN_TEST(resolver_java_wrong_arity_varargs_and_null_source_abstain);
    RUN_TEST(resolver_java_full_graph_identity_does_not_duplicate_signature);
    RUN_TEST(resolver_java_unknown_shadow_alias_and_type_parameter_abstain);
    RUN_TEST(resolver_java_source_modifiers_and_missing_facts_abstain);
    RUN_TEST(resolver_java_interface_target_and_inheritance_boundaries);
    RUN_TEST(resolver_java_overloads_namespace_collisions_and_object_members_abstain);
    RUN_TEST(resolver_namespace_index_is_exact_complete_and_bounded);
    RUN_TEST(resolver_java_pipeline_keeps_canonical_crossfile_target);
}
