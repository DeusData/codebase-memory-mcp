#include "cbm.h"
#include "lsp/c_lsp.h"
#include "lsp/c_proof.h"
#include "resolve/policy.h"
#include "test_framework.h"
#include "discover/test_conventions.h"
#include <stdio.h>
#include <stdlib.h>

extern const TSLanguage *tree_sitter_c(void);
extern const TSLanguage *tree_sitter_cpp(void);

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
    CBMLSPDef defs[] = {{.qualified_name = "test.main.work",
                         .declaration_identity = "fixture:test.main.work",
                         .short_name = "work",
                         .label = "Function"},
                        {.qualified_name = "test.main.sink",
                         .declaration_identity = "fixture:test.main.sink",
                         .short_name = "sink",
                         .label = "Function"},
                        {.qualified_name = "test.lib.imported",
                         .declaration_identity = "fixture:test.lib.imported",
                         .short_name = "imported",
                         .label = "Function"},
                        {.qualified_name = "other.work",
                         .declaration_identity = "fixture:other.work",
                         .short_name = "work",
                         .label = "Function"},
                        {.qualified_name = "other.imported",
                         .declaration_identity = "fixture:other.imported",
                         .short_name = "imported",
                         .label = "Function"}};
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
    CBMLSPDef defs[] = {{.qualified_name = "test.main.later",
                         .declaration_identity = "fixture:test.main.later",
                         .short_name = "later",
                         .label = "Function"},
                        {.qualified_name = "test.lib.imported",
                         .declaration_identity = "fixture:test.lib.imported",
                         .short_name = "imported",
                         .label = "Function"}};
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
    CBMLSPDef defs[] = {{.qualified_name = "test.main.target",
                         .declaration_identity = "fixture:7:test.main.target",
                         .short_name = "target",
                         .label = "Function"},
                        {.qualified_name = "test.one.clash",
                         .declaration_identity = "fixture:8:test.one.clash",
                         .short_name = "clash",
                         .label = "Function"},
                        {.qualified_name = "test.two.clash",
                         .declaration_identity = "fixture:9:test.two.clash",
                         .short_name = "clash",
                         .label = "Function"}};
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
    CBMLSPDef defs[] = {{.qualified_name = "test.main.one.target",
                         .declaration_identity = "fixture:test.main.one.target",
                         .short_name = "target",
                         .label = "Function"},
                        {.qualified_name = "test.main.two.target",
                         .declaration_identity = "fixture:test.main.two.target",
                         .short_name = "target",
                         .label = "Function"}};
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
    CBMLSPDef defs[] = {{.qualified_name = "test.main.target",
                         .short_name = "target",
                         .label = "Function",
                         .sig = "(int)",
                         .declaration_identity = "target-int"},
                        {.qualified_name = "test.main.target",
                         .short_name = "target",
                         .label = "Function",
                         .sig = "(double)",
                         .declaration_identity = "target-double"},
                        {.qualified_name = "test.main.single",
                         .declaration_identity = "fixture:14:test.main.single",
                         .short_name = "single",
                         .label = "Function"}};
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
    CBMLSPDef def = {.qualified_name = "test.main.work",
                     .declaration_identity = "fixture:15:test.main.work",
                     .short_name = "work",
                     .label = "Function"};
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

/* Keep the guarded alpha.h declaration from the pipeline field-owner fixture.
 * Its exact source owner must survive the quoted include; a same-named field
 * or an unseen/ambiguous Alpha declaration must not supply receiver authority. */
TEST(resolver_c_quoted_header_fields_require_exact_source_owner) {
    static const char guarded[] =
        "#ifndef ALPHA_H\n#define ALPHA_H\n"
        "typedef void (*alpha_notify_fn)(int);\nstruct Alpha {\n"
        "    int count;\n    int alpha_only;\n    alpha_notify_fn notify;\n};\n#endif\n";
    static const char plain[] =
        "typedef void (*alpha_notify_fn)(int);\nstruct Alpha {\n"
        "    int count;\n    int alpha_only;\n    alpha_notify_fn notify;\n};\n";
    static const char feature[] =
        "#ifdef ENABLE_ALPHA\n"
        "typedef void (*alpha_notify_fn)(int);\nstruct Alpha {\n"
        "    int count;\n    int alpha_only;\n    alpha_notify_fn notify;\n};\n#endif\n";
    static const char other[] = "struct Alpha { int count; int alpha_only; };\n";
    static const char beta[] = "struct Beta { int count; int beta_only; };\n";
    const char *prefixes[] = {"#include \"alpha.h\"\n",
                              "#include \"alpha.h\"\n",
                              "#include \"alpha.h\"\n#include \"other.h\"\n",
                              "",
                              "#include \"beta.h\"\n",
                              "#define ALPHA_H\n#include \"alpha.h\"\n",
                              "#include \"alpha.h\"\n"};
    const char *paths[] = {"alpha.h", "other.h", "beta.h"};
    const char *modules[] = {"test.alpha", "test.other", "test.beta"};
    int exact[7] = {0}, wrong[7] = {0};
    uint32_t flags[7] = {0};
    for (int i = 0; i < 7; i++) {
        CBMArena arena;
        cbm_arena_init(&arena);
        const char *source =
            cbm_arena_sprintf(&arena,
                              "%s\nint use_alpha(struct Alpha *a) {\n"
                              "    a->count = 1;\n    return a->count + a->alpha_only;\n}\n",
                              prefixes[i]);
        ASSERT_NOT_NULL(source);
        const char *header = i == 0 || i == 5 ? guarded : i == 6 ? feature : plain;
        CBMLSPDef defs[] = {{.qualified_name = "test.alpha.Alpha",
                             .graph_qualified_name = "test.alpha.Alpha",
                             .short_name = "Alpha",
                             .label = "Class",
                             .def_module_qn = "test.alpha",
                             .source_start_line = i == 0 || i == 5 ? 4
                                                  : i == 6         ? 3
                                                                   : 2,
                             .declaration_identity = "alpha.h:Alpha",
                             .field_defs = "count:int|alpha_only:int|notify:alpha_notify_fn"},
                            {.qualified_name = "test.other.Alpha",
                             .graph_qualified_name = "test.other.Alpha",
                             .short_name = "Alpha",
                             .label = "Class",
                             .def_module_qn = "test.other",
                             .source_start_line = 1,
                             .declaration_identity = "other.h:Alpha",
                             .field_defs = "count:int|alpha_only:int"},
                            {.qualified_name = "test.beta.Beta",
                             .graph_qualified_name = "test.beta.Beta",
                             .short_name = "Beta",
                             .label = "Class",
                             .def_module_qn = "test.beta",
                             .source_start_line = 1,
                             .declaration_identity = "beta.h:Beta",
                             .field_defs = "count:int|beta_only:int"}};
        ASSERT(
            cbm_c_collect_source_facts(&arena, header, (int)strlen(header), "test.alpha", defs, 1));
        ASSERT(cbm_c_collect_source_facts(&arena, other, (int)strlen(other), "test.other", defs + 1,
                                          1));
        ASSERT(
            cbm_c_collect_source_facts(&arena, beta, (int)strlen(beta), "test.beta", defs + 2, 1));
        flags[i] = defs[0].source_flags;
        CBMResolvedCallArray rows = {0};
        int imports = i == 2 ? 2 : i == 3 ? 0 : 1;
        int first = i == 4 ? 2 : 0;
        c_cross(&arena, source, false, defs, 3, paths + first, modules + first, imports, &rows);
        const char *sites[] = {strstr(source, "a->count =") + 3, strstr(source, "a->count +") + 3,
                               strstr(source, "a->alpha_only") + 3};
        const char *targets[] = {"test.alpha.Alpha.count", "test.alpha.Alpha.count",
                                 "test.alpha.Alpha.alpha_only"};
        unsigned seen = 0;
        for (int j = 0; j < rows.count; j++) {
            const CBMResolvedCall *row = &rows.items[j];
            if (!c_row_admitted(row))
                continue;
            int matched = -1;
            for (int k = 0; k < 3; k++) {
                uint32_t start = (uint32_t)(sites[k] - source);
                uint32_t length = k == 2 ? 10 : 5;
                if (row->kind == CBM_RESOLVED_FIELD_REFERENCE && row->evidence.proof &&
                    row->source_origin == CBM_SOURCE_ORIGIN_RAW &&
                    !strcmp(row->caller_qn, "test.main.use_alpha") &&
                    !strcmp(row->callee_qn, targets[k]) && row->site_start_byte == start &&
                    row->site_end_byte == start + length)
                    matched = k;
            }
            if (matched < 0 || (seen & (1u << matched)))
                wrong[i]++;
            else {
                seen |= 1u << matched;
                exact[i]++;
            }
        }
        cbm_arena_destroy(&arena);
    }
    /* Check negative controls before the guarded positive so its initial RED
     * cannot conceal an incorrect namesake, missing include, disabled feature,
     * or a header suppressed by the caller defining its guard first. */
    for (int i = 0; i < 7; i++)
        ASSERT_EQ(wrong[i], 0);
    for (int i = 2; i < 7; i++)
        ASSERT_EQ(exact[i], 0);
    ASSERT_EQ(exact[1], 3);
    /* Quality gap C-GUARD-DECLARATION: all three guarded-header references
     * genuinely target Alpha's fields. WHY: raw header text alone does not prove
     * its declarations survived the caller's include-time macro environment.
     * TRIED: the focused proof test resolves all three plain-header sites, but guarded source facts
     * have flags=0. The five negative cases include a caller-suppressed guard and
     * an inactive feature; simply descending through #if would make them wrong.
     * Keep the independent three-site denominator and require promotion once
     * configured/preprocessed declaration-presence proof recovers any site. */
    fprintf(stderr,
            "[RQ-UNIT] C-GUARD-DECLARATION genuine=6 found=%d missing=%d wrong=0 "
            "guarded_flags=%u plain_flags=%u\n",
            exact[0] + exact[1], 6 - exact[0] - exact[1], flags[0], flags[1]);
    if (exact[0] != 0)
        FAIL("C-GUARD-DECLARATION recovered: promote the exact recovered sites to required");
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

TEST(resolver_cpp_written_header_type_requires_complete_visible_domain) {
    static const char library[] = "struct Engine { void work() {} };\n";
    const char *sources[] = {
        "#include \"engine.h\"\nvoid run(Engine value) { value.work(); }\n",
        "void run(Engine value) { value.work(); }\n#include \"engine.h\"\n",
        "#include \"engine.h\"\n#include \"other.h\"\n"
        "void run(Engine value) { value.work(); }\n",
        "#include \"engine.h\"\nvoid run(Missing::Engine value) { value.work(); }\n",
        "struct Engine {};\n#include \"engine.h\"\n"
        "void run(Engine value) { value.work(); }\n"};
    const char *paths[] = {"engine.h", "other.h"};
    const char *modules[] = {"test.engine", "test.other"};
    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++) {
        CBMArena arena;
        cbm_arena_init(&arena);
        CBMLSPDef defs[] = {{.qualified_name = "test.engine.Engine",
                             .graph_qualified_name = "test.engine.Engine",
                             .short_name = "Engine",
                             .label = "Class",
                             .source_start_line = 1,
                             .declaration_identity = "engine:0"},
                            {.qualified_name = "test.engine.Engine.work",
                             .graph_qualified_name = "test.engine.Engine.work",
                             .short_name = "work",
                             .label = "Method",
                             .source_start_line = 1,
                             .receiver_type = "test.engine.Engine",
                             .declaration_identity = "engine:1"},
                            /* A memberless, unaudited namesake still competes. */
                            {.qualified_name = "test.other.Engine",
                             .graph_qualified_name = "test.other.Engine",
                             .short_name = "Engine",
                             .label = "Class",
                             .source_start_line = 1,
                             .declaration_identity = "other:0"},
                            {.qualified_name = "test.main.Engine",
                             .graph_qualified_name = "test.main.Engine",
                             .short_name = "Engine",
                             .label = "Class",
                             .source_start_line = 1,
                             .declaration_identity = "local:0"}};
        ASSERT(cbm_c_collect_source_facts(&arena, library, (int)strlen(library), "test.engine",
                                          defs, 3));
        CBMResolvedCallArray rows = {0};
        c_cross(&arena, sources[i], true, defs, i == 4 ? 4 : 3, paths, modules, i == 2 ? 2 : 1,
                &rows);
        int exact = c_proven_count(&rows, "test.engine.Engine.work");
        int all = c_proven_count(&rows, NULL);
        cbm_arena_destroy(&arena);
        ASSERT_EQ(exact, i == 0 ? 1 : 0);
        ASSERT_EQ(all, exact);
    }
    PASS();
}

/* Tree-sitter's context-free parse can represent Widget w(n) as a function
 * declarator. Only an existing value binding may disambiguate that syntax;
 * an empty parameter list or a type name must never manufacture an object. */
TEST(resolver_cpp_direct_initializer_preserves_declared_receiver) {
    static const char library[] = "class Widget {\n    int id;\npublic:\n"
                                  "    Widget(int id) : id(id) {}\n"
                                  "    int get_id() const { return id; }\n};\n";
    const char *sources[] = {
        "#include \"widget.hpp\"\nint run(int n) {\n"
        "    Widget w(n);\n    return w.get_id();\n}\n",
        "#include \"widget.hpp\"\nstruct UnknownType {};\n"
        "int run() { Widget w(UnknownType); return w.get_id(); }\n",
        "#include \"widget.hpp\"\nint run() { Widget w(); return w.get_id(); }\n",
        "#include \"widget.hpp\"\nint run(int n) {\n"
        "    { using n = int; Widget w(n); return w.get_id(); }\n}\n"};
    int getters[4] = {0}, constructors[4] = {0};
    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++) {
        CBMArena arena;
        cbm_arena_init(&arena);
        CBMLSPDef defs[] = {{.qualified_name = "test.widget.Widget",
                             .graph_qualified_name = "test.widget.Widget",
                             .short_name = "Widget",
                             .label = "Class",
                             .lang = CBM_LANG_CPP,
                             .source_start_line = 1,
                             .declaration_identity = "widget:class"},
                            {.qualified_name = "test.widget.Widget.Widget",
                             .graph_qualified_name = "test.widget.Widget.Widget",
                             .short_name = "Widget",
                             .label = "Method",
                             .lang = CBM_LANG_CPP,
                             .receiver_type = "test.widget.Widget",
                             .source_start_line = 4,
                             .declaration_identity = "widget:constructor"},
                            {.qualified_name = "test.widget.Widget.get_id",
                             .graph_qualified_name = "test.widget.Widget.get_id",
                             .short_name = "get_id",
                             .label = "Method",
                             .lang = CBM_LANG_CPP,
                             .receiver_type = "test.widget.Widget",
                             .source_start_line = 5,
                             .declaration_identity = "widget:getter"}};
        ASSERT(cbm_c_collect_source_facts(&arena, library, (int)strlen(library), "test.widget",
                                          defs, 3));
        const char *paths[] = {"widget.hpp"}, *modules[] = {"test.widget"};
        CBMResolvedCallArray rows = {0};
        c_cross(&arena, sources[i], true, defs, 3, paths, modules, 1, &rows);
        getters[i] = c_proven_count(&rows, "test.widget.Widget.get_id");
        constructors[i] = c_proven_count(&rows, "test.widget.Widget.Widget");
        if (!i && getters[i] != 1) {
            TSParser *parser = ts_parser_new();
            ASSERT_NOT_NULL(parser);
            ts_parser_set_language(parser, tree_sitter_cpp());
            TSTree *tree =
                ts_parser_parse_string(parser, NULL, sources[i], (uint32_t)strlen(sources[i]));
            ASSERT_NOT_NULL(tree);
            const char *declaration = strstr(sources[i], "Widget w(n);");
            uint32_t start = (uint32_t)(declaration - sources[i]);
            TSNode node = ts_node_descendant_for_byte_range(ts_tree_root_node(tree), start,
                                                            start + strlen("Widget w(n);"));
            char *shape = ts_node_string(node);
            fprintf(stderr, "    [resolver_c] direct initialization AST: %s\n",
                    shape ? shape : "(unavailable)");
            free(shape);
            ts_tree_delete(tree);
            ts_parser_delete(parser);
        }
        cbm_arena_destroy(&arena);
    }
    ASSERT_EQ(getters[0], 1);
    ASSERT_EQ(getters[1], 0);
    ASSERT_EQ(getters[2], 0);
    ASSERT_EQ(getters[3], 0);
    for (size_t i = 0; i < sizeof(constructors) / sizeof(constructors[0]); i++)
        ASSERT_EQ(constructors[i], 0); /* Constructor proof remains a separate quality gap. */
    PASS();
}

SUITE(resolver_c) {
    RUN_TEST(resolver_c_quoted_header_fields_require_exact_source_owner);
    RUN_TEST(resolver_cpp_direct_initializer_preserves_declared_receiver);
    RUN_TEST(resolver_cpp_written_header_type_requires_complete_visible_domain);
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
