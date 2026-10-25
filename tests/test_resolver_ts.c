#include "test_framework.h"
#include "cbm.h"
#include "lsp/ts_lsp.h"
#include "resolve/policy.h"

static bool ts_proved_row(const CBMResolvedCall *row) {
    cbm_resolve_result_t result = {.binding = CBM_RESOLVE_BINDING_UNIQUE,
                                   .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                   .analysis = row->evidence.analysis,
                                   .rule = row->evidence.rule,
                                   .facts = row->evidence.facts,
                                   .target = row->callee_qn,
                                   .candidate_count = 1};
    return row->site_end_byte > row->site_start_byte && cbm_resolve_admit(&result);
}

static int ts_proved_count(const CBMResolvedCallArray *rows, const char *target) {
    int count = 0;
    for (int i = 0; i < rows->count; i++) {
        const CBMResolvedCall *row = &rows->items[i];
        if (ts_proved_row(row) && (!target || strcmp(target, row->callee_qn) == 0))
            count++;
    }
    return count;
}

TEST(resolver_ts_declared_receiver_selects_exact_occurrence) {
    const char *source = "class A { run() {} } class B { run() {} } "
                         "function use(a: A, b: B) { a.run(); b.run(); }";
    CBMFileResult *file = cbm_extract_file(source, (int)strlen(source), CBM_LANG_TYPESCRIPT, "test",
                                           "main.ts", 0, NULL, NULL);
    ASSERT_NOT_NULL(file);
    int a = ts_proved_count(&file->resolved_calls, "test.main.A.run");
    int b = ts_proved_count(&file->resolved_calls, "test.main.B.run");
    cbm_free_result(file);
    ASSERT_EQ(a, 1);
    ASSERT_EQ(b, 1);
    PASS();
}

TEST(resolver_ts_unknown_union_and_later_shadow_abstain) {
    const char *sources[] = {
        "class A { run() {} } function use(a) { a.run(); }",
        "class A { run() {} } class B { run() {} } function use(a: A | B) { a.run(); }",
        "class A { run() {} } function use(a: A) { { a.run(); let a; } }",
        "class A { run() {} } function use(a: A) { a.run(); a = unknownValue; }",
        ("class A { run() {} } function use(a: A) { mutate(); a.run(); "
         "function mutate() { a = unknownValue; } }")};
    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++) {
        CBMFileResult *file =
            cbm_extract_file(sources[i], (int)strlen(sources[i]), CBM_LANG_TYPESCRIPT, "test",
                             "main.ts", 0, NULL, NULL);
        ASSERT_NOT_NULL(file);
        int count = ts_proved_count(&file->resolved_calls, NULL);
        cbm_free_result(file);
        ASSERT_EQ(count, 0);
    }
    PASS();
}

TEST(resolver_ts_imported_type_requires_export_binding) {
    const char *source = "import { A } from './lib'; function use(a: A) { a.run(); }";
    CBMLSPDef defs[] = {{.qualified_name = "test.lib.A",
                         .short_name = "A",
                         .label = "Class",
                         .def_module_qn = "test.lib",
                         .lang = CBM_LANG_TYPESCRIPT,
                         .declaration_identity = "lib:class",
                         .export_binding_proven = true},
                        {.qualified_name = "test.lib.A.run",
                         .short_name = "run",
                         .label = "Method",
                         .receiver_type = "test.lib.A",
                         .def_module_qn = "test.lib",
                         .lang = CBM_LANG_TYPESCRIPT,
                         .declaration_identity = "lib:method"},
                        {.qualified_name = "test.decoy.A.run",
                         .short_name = "run",
                         .label = "Method",
                         .receiver_type = "test.decoy.A",
                         .def_module_qn = "test.decoy",
                         .lang = CBM_LANG_TYPESCRIPT,
                         .declaration_identity = "decoy:method"}};
    const char *names[] = {"A"}, *targets[] = {"test.lib"};
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray proven = {0}, private = {0};
    cbm_run_ts_lsp_cross(&arena, source, (int)strlen(source), "test.main", false, false, false,
                         defs, 3, names, targets, 1, NULL, &proven);
    defs[0].export_binding_proven = false;
    cbm_run_ts_lsp_cross(&arena, source, (int)strlen(source), "test.main", false, false, false,
                         defs, 3, names, targets, 1, NULL, &private);
    int exact = ts_proved_count(&proven, "test.lib.A.run");
    int decoy = ts_proved_count(&proven, "test.decoy.A.run");
    int rejected = ts_proved_count(&private, NULL);
    cbm_arena_destroy(&arena);
    ASSERT_EQ(exact, 1);
    ASSERT_EQ(decoy, 0);
    ASSERT_EQ(rejected, 0);
    PASS();
}

TEST(resolver_ts_overlay_copies_coalesce_only_same_declaration) {
    const char *source = "class A { run() {} } function use(a: A) { a.run(); }";
    CBMLSPDef defs[] = {{.qualified_name = "test.main.A",
                         .short_name = "A",
                         .label = "Class",
                         .def_module_qn = "test.main",
                         .lang = CBM_LANG_TYPESCRIPT,
                         .declaration_identity = "main:class"},
                        {.qualified_name = "test.main.A.run",
                         .short_name = "run",
                         .label = "Method",
                         .receiver_type = "test.main.A",
                         .def_module_qn = "test.main",
                         .lang = CBM_LANG_TYPESCRIPT,
                         .declaration_identity = "main:method"}};
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMTypeRegistry *registry = cbm_ts_build_cross_registry(&arena, defs, 2);
    ASSERT_NOT_NULL(registry);
    CBMResolvedCallArray rows = {0};
    cbm_run_ts_lsp_cross_with_registry(&arena, source, (int)strlen(source), "test.main", false,
                                       false, false, registry, defs, 2, NULL, NULL, 0, NULL, &rows);
    int count = ts_proved_count(&rows, "test.main.A.run");
    cbm_arena_destroy(&arena);
    ASSERT_EQ(count, 1);
    PASS();
}

TEST(resolver_ts_duplicate_method_declarations_abstain) {
    const char *source = "import { A } from './lib'; function use(a: A) { a.run(); }";
    CBMLSPDef defs[] = {{.qualified_name = "test.lib.A",
                         .short_name = "A",
                         .label = "Class",
                         .def_module_qn = "test.lib",
                         .lang = CBM_LANG_TYPESCRIPT,
                         .declaration_identity = "lib:class",
                         .export_binding_proven = true},
                        {.qualified_name = "test.lib.A.run",
                         .short_name = "run",
                         .label = "Method",
                         .receiver_type = "test.lib.A",
                         .def_module_qn = "test.lib",
                         .lang = CBM_LANG_TYPESCRIPT,
                         .declaration_identity = "lib:method1"},
                        {.qualified_name = "test.lib.A.run",
                         .short_name = "run",
                         .label = "Method",
                         .receiver_type = "test.lib.A",
                         .def_module_qn = "test.lib",
                         .lang = CBM_LANG_TYPESCRIPT,
                         .declaration_identity = "lib:method2"}};
    const char *names[] = {"A"}, *targets[] = {"test.lib"};
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray rows = {0};
    cbm_run_ts_lsp_cross(&arena, source, (int)strlen(source), "test.main", false, false, false,
                         defs, 3, names, targets, 1, NULL, &rows);
    int count = ts_proved_count(&rows, NULL);
    cbm_arena_destroy(&arena);
    ASSERT_EQ(count, 0);
    PASS();
}

SUITE(resolver_ts) {
    RUN_TEST(resolver_ts_declared_receiver_selects_exact_occurrence);
    RUN_TEST(resolver_ts_unknown_union_and_later_shadow_abstain);
    RUN_TEST(resolver_ts_imported_type_requires_export_binding);
    RUN_TEST(resolver_ts_overlay_copies_coalesce_only_same_declaration);
    RUN_TEST(resolver_ts_duplicate_method_declarations_abstain);
}
