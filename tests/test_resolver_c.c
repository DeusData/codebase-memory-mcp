#include "cbm.h"
#include "lsp/c_lsp.h"
#include "lsp/c_proof.h"
#include "resolve/policy.h"
#include "test_framework.h"
#include "discover/test_conventions.h"

extern const TSLanguage *tree_sitter_c(void);

static bool c_row_admitted(const CBMResolvedCall *row) {
    cbm_resolve_result_t proof = {.binding = CBM_RESOLVE_BINDING_UNIQUE,
                                  .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                  .analysis = row->evidence.analysis,
                                  .rule = row->evidence.rule,
                                  .facts = row->evidence.facts,
                                  .target = row->callee_qn,
                                  .candidate_count = 1};
    return row->site_end_byte > row->site_start_byte && cbm_resolve_admit(&proof);
}

static int c_proven_count(const CBMResolvedCallArray *rows, const char *target) {
    int count = 0;
    for (int i = 0; i < rows->count; i++) {
        const CBMResolvedCall *row = &rows->items[i];
        if (row->kind != CBM_RESOLVED_FIELD_REFERENCE && c_row_admitted(row) &&
            (!target || !strcmp(row->callee_qn, target)))
            count++;
    }
    return count;
}

static void c_cross(CBMArena *arena, const char *source, bool cpp, CBMLSPDef *defs, int count,
                    const char **paths, const char **modules, int imports,
                    CBMResolvedCallArray *out) {
    cbm_run_c_lsp_cross(arena, source, (int)strlen(source), "test.main", cpp, defs, count, paths,
                        modules, imports, NULL, out);
}

TEST(resolver_c_visible_functions_and_callable_values_have_proof) {
    const char *source = "#include \"lib.h\"\nvoid work(void); void sink(void (*fp)(void));\n"
                         "void run(void) { work(); imported(); sink(work); "
                         "void (*alias)(void) = &work; alias(); sink(alias); }\n";
    CBMLSPDef defs[] = {
        {.qualified_name = "test.main.work", .short_name = "work", .label = "Function"},
        {.qualified_name = "test.main.sink", .short_name = "sink", .label = "Function"},
        {.qualified_name = "test.lib.imported", .short_name = "imported", .label = "Function"},
        {.qualified_name = "other.work", .short_name = "work", .label = "Function"},
        {.qualified_name = "other.imported", .short_name = "imported", .label = "Function"}};
    const char *paths[] = {"lib.h"}, *modules[] = {"test.lib"};
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};
    c_cross(&arena, source, false, defs, 5, paths, modules, 1, &out);
    int work = c_proven_count(&out, "test.main.work");
    int imported = c_proven_count(&out, "test.lib.imported");
    int decoys = c_proven_count(&out, "other.work") + c_proven_count(&out, "other.imported");
    cbm_arena_destroy(&arena);
    ASSERT_EQ(work, 4);
    ASSERT_EQ(imported, 1);
    ASSERT_EQ(decoys, 0);
    PASS();
}

TEST(resolver_c_later_declarations_and_includes_are_not_visible) {
    const char *source = "void early(void) { later(); imported(); }\n"
                         "void later(void);\n#include \"lib.h\"\n"
                         "void late(void) { later(); imported(); }\n";
    CBMLSPDef defs[] = {
        {.qualified_name = "test.main.later", .short_name = "later", .label = "Function"},
        {.qualified_name = "test.lib.imported", .short_name = "imported", .label = "Function"}};
    const char *paths[] = {"lib.h"}, *modules[] = {"test.lib"};
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};
    c_cross(&arena, source, false, defs, 2, paths, modules, 1, &out);
    int later = c_proven_count(&out, "test.main.later");
    int imported = c_proven_count(&out, "test.lib.imported");
    int early = 0;
    for (int i = 0; i < out.count; i++)
        if (c_row_admitted(&out.items[i]) && strstr(out.items[i].caller_qn, ".early"))
            early++;
    cbm_arena_destroy(&arena);
    ASSERT_EQ(later, 1);
    ASSERT_EQ(imported, 1);
    ASSERT_EQ(early, 0);
    PASS();
}

TEST(resolver_c_unknown_shadow_and_include_conflicts_abstain) {
    const char *source = "#include \"one.h\"\n#include \"two.h\"\n"
                         "void target(void); void run(Missing target) { target(); clash(); }\n";
    CBMLSPDef defs[] = {
        {.qualified_name = "test.main.target", .short_name = "target", .label = "Function"},
        {.qualified_name = "test.one.clash", .short_name = "clash", .label = "Function"},
        {.qualified_name = "test.two.clash", .short_name = "clash", .label = "Function"}};
    const char *paths[] = {"one.h", "two.h"}, *modules[] = {"test.one", "test.two"};
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0}, collision = {0};
    c_cross(&arena, source, false, defs, 3, paths, modules, 2, &out);
    const char *same_paths[] = {"same.h", "same.h"};
    c_cross(&arena, "#include \"same.h\"\nvoid run(void) { clash(); }", false, defs, 3, same_paths,
            modules, 2, &collision);
    int count = c_proven_count(&out, NULL) + c_proven_count(&collision, NULL);
    cbm_arena_destroy(&arena);
    ASSERT_EQ(count, 0);
    PASS();
}

TEST(resolver_cpp_namespaces_and_using_require_unique_declarations) {
    CBMLSPDef defs[] = {
        {.qualified_name = "test.main.one.target", .short_name = "target", .label = "Function"},
        {.qualified_name = "test.main.two.target", .short_name = "target", .label = "Function"}};
    const char *source = "namespace one { void target(); void local() { target(); } }\n"
                         "namespace two { void target(); }\n"
                         "void qualified() { one::target(); }\n"
                         "using namespace one;\nvoid unique() { target(); }\n"
                         "using namespace two;\nvoid ambiguous() { target(); }\n";
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};
    c_cross(&arena, source, true, defs, 2, NULL, NULL, 0, &out);
    int one = c_proven_count(&out, "test.main.one.target");
    int two = c_proven_count(&out, "test.main.two.target");
    cbm_arena_destroy(&arena);
    ASSERT_EQ(one, 3);
    ASSERT_EQ(two, 0);
    PASS();
}

TEST(resolver_cpp_overloads_and_unmodelled_adl_abstain) {
    CBMLSPDef defs[] = {
        {.qualified_name = "test.main.target",
         .short_name = "target",
         .label = "Function",
         .sig = "(int)",
         .declaration_identity = "target-int"},
        {.qualified_name = "test.main.target",
         .short_name = "target",
         .label = "Function",
         .sig = "(double)",
         .declaration_identity = "target-double"},
        {.qualified_name = "test.main.single", .short_name = "single", .label = "Function"}};
    const char *source = "void target(int); void target(double); void single(Missing);\n"
                         "void run(Missing value) { target(1); consume(target); single(value); }";
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};
    c_cross(&arena, source, true, defs, 3, NULL, NULL, 0, &out);
    int count = c_proven_count(&out, NULL);
    cbm_arena_destroy(&arena);
    ASSERT_EQ(count, 0);
    PASS();
}

TEST(resolver_c_callable_mutations_and_escapes_clear_identity) {
    const char *source = "void work(void);\n"
                         "void run(void (*dynamic)(void), int cond) {\n"
                         "void (*first)(void) = work; { first = dynamic; } first();\n"
                         "void (*second)(void) = work; if (cond) second = dynamic; second();\n"
                         "void (*third)(void) = work; mutate(&third); third();\n"
                         "void (*loop)(void) = work; while (cond) { loop(); loop = dynamic; }\n"
                         "void (*fake)(void) = (void (*)(void))lookup(\"work\"); fake(); }";
    CBMLSPDef def = {.qualified_name = "test.main.work", .short_name = "work", .label = "Function"};
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};
    c_cross(&arena, source, false, &def, 1, NULL, NULL, 0, &out);
    int count = c_proven_count(&out, NULL);
    cbm_arena_destroy(&arena);
    ASSERT_EQ(count, 0);
    PASS();
}

TEST(resolver_c_proof_view_includes_late_registration_and_excludes_inference_tail) {
    const char *source = "int target(void); void first(void) { target(); }\n"
                         "int inference_only(void); void second(void) { target(); }\n";
    TSParser *parser = ts_parser_new();
    ASSERT_NOT_NULL(parser);
    ts_parser_set_language(parser, tree_sitter_c());
    TSTree *tree = ts_parser_parse_string(parser, NULL, source, (uint32_t)strlen(source));
    ts_parser_delete(parser);
    ASSERT_NOT_NULL(tree);
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMTypeRegistry registry;
    cbm_registry_init(&registry, &arena);
    CBMResolvedCallArray out = {0};
    CLSPContext ctx;
    c_lsp_init(&ctx, &arena, source, (int)strlen(source), &registry, "test.main", false, &out);
    CBMRegisteredFunc function = {.qualified_name = "test.main.target",
                                  .short_name = "target",
                                  .flags = CBM_FUNC_FLAG_PROJECT_SOURCE,
                                  .declaration_identity = "original-source-target"};
    cbm_registry_add_func(&registry, function);
    cbm_registry_finalize(&registry);
    int original_count = registry.func_count;
    c_lsp_process_file(&ctx, ts_tree_root_node(tree));
    int count = c_proven_count(&out, "test.main.target");
    bool tail_added = registry.func_count > original_count;
    cbm_arena_destroy(&arena);
    ts_tree_delete(tree);
    ASSERT(tail_added);
    ASSERT_EQ(count, 2);
    PASS();
}

TEST(resolver_c_field_identity_is_not_a_callable_target) {
    const char *source = "struct Ops { void (*callback)(void); };\n"
                         "void run(struct Ops *ops) { ops->callback(); }\n";
    CBMFileResult *result =
        cbm_extract_file(source, (int)strlen(source), CBM_LANG_C, "test", "main.c", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    uint32_t field = (uint32_t)(strstr(source, "ops->callback()") - source) + 5;
    int field_rows = 0, invocation_rows = 0, admitted_calls = 0;
    for (int i = 0; i < result->resolved_calls.count; i++) {
        const CBMResolvedCall *row = &result->resolved_calls.items[i];
        if (!c_row_admitted(row))
            continue;
        if (row->kind == CBM_RESOLVED_FIELD_REFERENCE) {
            if (row->site_start_byte == field && row->site_end_byte == field + 8)
                field_rows++;
            else
                invocation_rows++;
        } else if (row->kind == CBM_RESOLVED_INVOCATION) {
            admitted_calls++;
        }
    }
    cbm_free_result(result);
    ASSERT_EQ(field_rows, 1);
    ASSERT_EQ(invocation_rows, 0);
    ASSERT_EQ(admitted_calls, 0);
    PASS();
}

TEST(resolver_c_configured_owners_preserve_exact_body_calls) {
    const char *config = "{\"test_impact\":{\"version\":1,\"tests\":{\"conventions\":["
                         "{\"language\":\"c\",\"role\":\"case\",\"define_macro\":\"CHECK\","
                         "\"name_args\":[0]}]}}}";
    const char *sources[] = {
        "void target(void) {}\nCHECK(example, ignored) { target(); }\n",
        "void target(void) {}\nCHECK(example, ignored) { target(; }\n",
        "void target(void) {}\nint broken = ;\nCHECK(example, ignored) { target(); }\n"};
    cbm_test_declarations_t *declarations =
        cbm_test_declarations_parse(config, strlen(config), false);
    ASSERT_NOT_NULL(declarations);
    int proven[3] = {0};
    for (int i = 0; i < 3; i++) {
        const char *source = sources[i];
        CBMFileResult *result =
            cbm_extract_file_ex_with_tests(source, (int)strlen(source), CBM_LANG_C, "test",
                                           "main.c", 0, NULL, NULL, NULL, NULL, declarations);
        ASSERT_NOT_NULL(result);
        if (!i) {
            ASSERT_EQ(result->test_declarations_status, CBM_TEST_EXTRACT_OK);
            ASSERT(result->has_test_definition_owners);
        }
        proven[i] = c_proven_count(&result->resolved_calls, "test.main.target");
        cbm_free_result(result);
    }
    cbm_test_declarations_free(declarations);
    ASSERT_EQ(proven[0], 1);
    ASSERT_EQ(proven[1], 0);
    ASSERT_EQ(proven[2], 0);
    PASS();
}

TEST(resolver_cpp_direct_member_requires_source_class_and_visibility) {
    const char *libraries[] = {
        "struct Engine { void work() {} };\n", "class Engine { void work() {} };\n",
        "struct Engine : Other { void work() {} };\n", "struct Engine { void (*work)(); };\n"};
    int proven[4] = {0};
    for (int i = 0; i < 4; i++) {
        CBMArena arena;
        cbm_arena_init(&arena);
        CBMLSPDef defs[] = {{.qualified_name = "test.engine.Engine",
                             .graph_qualified_name = "test.engine.Engine",
                             .short_name = "Engine",
                             .label = "Class",
                             .lang = CBM_LANG_CPP,
                             .source_start_line = 1,
                             .declaration_identity = "engine:0"},
                            {.qualified_name = "test.engine.Engine.work",
                             .graph_qualified_name = "test.engine.Engine.work",
                             .short_name = "work",
                             .label = "Method",
                             .lang = CBM_LANG_CPP,
                             .receiver_type = "test.engine.Engine",
                             .source_start_line = 1,
                             .declaration_identity = "engine:1"}};
        ASSERT(cbm_c_collect_source_facts(&arena, libraries[i], (int)strlen(libraries[i]),
                                          "test.engine", defs, 2));
        const char *paths[] = {"engine.h"}, *modules[] = {"test.engine"};
        CBMResolvedCallArray rows = {0};
        c_cross(&arena, "#include \"engine.h\"\nvoid run(Engine value) { value.work(); }\n", true,
                defs, 2, paths, modules, 1, &rows);
        proven[i] = c_proven_count(&rows, "test.engine.Engine.work");
        cbm_arena_destroy(&arena);
    }
    ASSERT_EQ(proven[0], 1);
    ASSERT_EQ(proven[1], 0);
    ASSERT_EQ(proven[2], 0);
    ASSERT_EQ(proven[3], 0);
    PASS();
}

SUITE(resolver_c) {
    RUN_TEST(resolver_cpp_direct_member_requires_source_class_and_visibility);
    RUN_TEST(resolver_c_configured_owners_preserve_exact_body_calls);
    RUN_TEST(resolver_c_visible_functions_and_callable_values_have_proof);
    RUN_TEST(resolver_c_later_declarations_and_includes_are_not_visible);
    RUN_TEST(resolver_c_unknown_shadow_and_include_conflicts_abstain);
    RUN_TEST(resolver_cpp_namespaces_and_using_require_unique_declarations);
    RUN_TEST(resolver_cpp_overloads_and_unmodelled_adl_abstain);
    RUN_TEST(resolver_c_callable_mutations_and_escapes_clear_identity);
    RUN_TEST(resolver_c_proof_view_includes_late_registration_and_excludes_inference_tail);
    RUN_TEST(resolver_c_field_identity_is_not_a_callable_target);
}
