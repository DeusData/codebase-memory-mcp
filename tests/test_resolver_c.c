#include "cbm.h"
#include "lsp/c_lsp.h"
#include "lsp/c_proof.h"
#include "resolve/policy.h"
#include "test_framework.h"
#include "discover/test_conventions.h"
#include <stdio.h>
#include <stdlib.h>
#include "preprocessor.h"
#include "pipeline/pipeline.h"
#include "test_helpers.h"
#include "resolver_quality.h"

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

/* Each row is an independently specified caller/definition contract. Both
 * registration routes must find the required edge, and all rejected linkage
 * domains must stay empty rather than picking a plausible namesake. */
TEST(resolver_c_external_prototype_requires_compatible_unique_definition) {
    const struct {
        const char *caller, *library, *competitor;
        bool cpp;
        int expected;
    } cases[] = {
        {"int lib_value(void); int run(void) { return lib_value(); }",
         "int lib_value(void) { return 7; }", NULL, false, 1},
        {"int lib_value(); int run() { return lib_value(); }", "int lib_value() { return 7; }",
         NULL, true, 1},
        {"int lib_value(void); int run(void) { return lib_value(); }",
         "int lib_value(void) { return 7; }", "int lib_value(void) { return 9; }", false, 0},
        {"int lib_value(void); int run(void) { return lib_value(); }",
         "int lib_value(void) { return 7; }", "static int lib_value(void) { return 9; }", false, 1},
        {"int lib_value(void); int run(void) { return lib_value(); }",
         "static int lib_value(void) { return 7; }", NULL, false, 0},
        {"int lib_value(void); int run(void) { return lib_value(); }",
         "double lib_value(void) { return 7; }", NULL, false, 0},
        {"int lib_value(int); int run(void) { return lib_value(1); }",
         "int lib_value(double a) { return 7; }", NULL, false, 0},
        {"int run(void) { return lib_value(); }", "int lib_value(void) { return 7; }", NULL, false,
         0},
        {"int run(void) { return lib_value(); } int lib_value(void);",
         "int lib_value(void) { return 7; }", NULL, false, 0},
        {"static int lib_value(void); int run(void) { return lib_value(); }",
         "int lib_value(void) { return 7; }", NULL, false, 0},
        {"int lib_value(void); int run(void) { return lib_value(); }",
         "int lib_value(void) { return 7; }",
         "#ifdef FEATURE\nint lib_value(void) { return 9; }\n#endif", false, 0},
        {"int lib_value(void); int run(void) { return lib_value(); }",
         "#ifdef FEATURE\nint lib_value(void) { return 7; }\n#endif", NULL, false, 0},
        {"int lib_value(); int run() { return lib_value(); }",
         "extern \"C\" { int lib_value() { return 7; } }", NULL, true, 0},
        {"extern \"C\" { int lib_value(); } int run() { return lib_value(); }",
         "extern \"C\" { int lib_value() { return 7; } }", NULL, true, 1},
        {"int lib_value(); int run(void) { return lib_value(); }",
         "int lib_value(void) { return 7; }", NULL, false, 0},
        {"int lib_value(void); int lib_value(void); int run(void) { return lib_value(); }",
         "int lib_value(void) { return 7; }", NULL, false, 1},
        {"int lib_value(void); double lib_value(void); int run(void) { return lib_value(); }",
         "int lib_value(void) { return 7; }", NULL, false, 0},
        {"int lib_value(int, double); int run(void) { return lib_value(1, 2.0); }",
         "int lib_value(double a, int b) { return 7; }", NULL, false, 0},
        {"int lib_value(int); int run(void) { return lib_value(1); }",
         "int lib_value(int a, int b) { return 7; }", NULL, false, 0},
        {"#define lib_value other\nint lib_value(void); int run(void) { return lib_value(); }",
         "int lib_value(void) { return 7; }", NULL, false, 0},
        {"int lib_value(void); int run(void) { return lib_value(); }",
         "#define int double\nint lib_value(void) { return 7; }", NULL, false, 0},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        for (int shared = 0; shared < 2; shared++) {
            CBMArena arena;
            cbm_arena_init(&arena);
            CBMLSPDef defs[] = {
                {.qualified_name = "test.lib.lib_value",
                 .graph_qualified_name = "test.lib.lib_value",
                 .short_name = "lib_value",
                 .label = "Function",
                 .def_module_qn = "test.lib",
                 .source_start_line = cases[i].library[0] == '#' ? 2 : 1,
                 .declaration_identity = "lib.c:1:lib_value",
                 .lang = cases[i].cpp ? CBM_LANG_CPP : CBM_LANG_C},
                {.qualified_name = "test.other.lib_value",
                 .graph_qualified_name = "test.other.lib_value",
                 .short_name = "lib_value",
                 .label = "Function",
                 .def_module_qn = "test.other",
                 .source_start_line = cases[i].competitor && cases[i].competitor[0] == '#' ? 2 : 1,
                 .declaration_identity = "other.c:1:lib_value",
                 .lang = cases[i].cpp ? CBM_LANG_CPP : CBM_LANG_C}};
            ASSERT(cbm_c_collect_source_facts(&arena, cases[i].library,
                                              (int)strlen(cases[i].library), "test.lib", defs, 1));
            int count = cases[i].competitor ? 2 : 1;
            if (count == 2)
                ASSERT(cbm_c_collect_source_facts(&arena, cases[i].competitor,
                                                  (int)strlen(cases[i].competitor), "test.other",
                                                  defs + 1, 1));
            CBMResolvedCallArray rows = {0};
            if (shared) {
                CBMTypeRegistry *registry = cbm_c_build_cross_registry(&arena, defs, count);
                ASSERT_NOT_NULL(registry);
                ASSERT(cbm_run_c_lsp_cross_with_registry(
                    &arena, cases[i].caller, (int)strlen(cases[i].caller), "test.main",
                    cases[i].cpp, registry, NULL, NULL, 0, NULL, &rows));
            } else {
                c_cross(&arena, cases[i].caller, cases[i].cpp, defs, count, NULL, NULL, 0, &rows);
            }
            int exact = c_proven_count(&rows, "test.lib.lib_value");
            int total = c_proven_count(&rows, NULL);
            cbm_arena_destroy(&arena);
            ASSERT_EQ(exact, cases[i].expected);
            ASSERT_EQ(total, exact);
        }
    }
    PASS();
}

TEST(resolver_cpp_linkage_preserves_namespace_and_registered_prototype_identity) {
    const char *caller =
        "namespace api { int lib_value(); } int run() { return api::lib_value(); }";
    const char *libraries[] = {"namespace api { int lib_value() { return 7; } }",
                               "namespace other { int lib_value() { return 7; } }"};
    for (int different = 0; different < 2; different++) {
        CBMArena arena;
        cbm_arena_init(&arena);
        CBMLSPDef defs[] = {
            {.qualified_name = different ? "test.lib.other.lib_value" : "test.lib.api.lib_value",
             .graph_qualified_name =
                 different ? "test.lib.other.lib_value" : "test.lib.api.lib_value",
             .short_name = "lib_value",
             .label = "Function",
             .def_module_qn = "test.lib",
             .source_start_line = 1,
             .declaration_identity = "library.cpp:1:lib_value",
             .lang = CBM_LANG_CPP},
            {.qualified_name = "test.main.api.lib_value",
             .graph_qualified_name = "test.main.api.lib_value",
             .short_name = "lib_value",
             .label = "Function",
             .def_module_qn = "test.main",
             .source_start_line = 1,
             .declaration_identity = "caller.cpp:1:prototype",
             .lang = CBM_LANG_CPP}};
        ASSERT(cbm_c_collect_source_facts(&arena, libraries[different],
                                          (int)strlen(libraries[different]), "test.lib", defs, 1));
        ASSERT(cbm_c_collect_source_facts(&arena, caller, (int)strlen(caller), "test.main",
                                          defs + 1, 1));
        CBMResolvedCallArray rows = {0};
        c_cross(&arena, caller, true, defs, 2, NULL, NULL, 0, &rows);
        int exact = c_proven_count(&rows, "test.lib.api.lib_value");
        int total = c_proven_count(&rows, NULL);
        int prototypes = c_proven_count(&rows, "test.main.api.lib_value");
        cbm_arena_destroy(&arena);
        ASSERT_EQ(exact, different ? 0 : 1);
        ASSERT_EQ(prototypes, 0);
        ASSERT_EQ(total, exact);
    }
    PASS();
}

TEST(resolver_cpp_explicit_builtin_arguments_exclude_adl_only_with_lexical_proof) {
    const char *sources[] = {
        "int square(int); int run(int a) { return square(a); }",
        "int square(int); int run() { int a = 2; return square(a); }",
        "int square(int); int run() { auto a = 2; return square(a); }",
        "int square(int); int run(Missing a) { return square(a); }",
        "int square(int); int run(int a) { { Missing a; return square(a); } }",
        "int square(int); int run(int a) { { int b = 2; } return square(a); }",
    };
    const int expected[] = {1, 1, 0, 0, 0, 1};
    const char *library = "int square(int a) { return a * a; }";
    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++) {
        CBMArena arena;
        cbm_arena_init(&arena);
        CBMLSPDef def = {.qualified_name = "test.math.square",
                         .graph_qualified_name = "test.math.square",
                         .short_name = "square",
                         .label = "Function",
                         .def_module_qn = "test.math",
                         .source_start_line = 1,
                         .declaration_identity = "math.cpp:1:square",
                         .lang = CBM_LANG_CPP};
        ASSERT(cbm_c_collect_source_facts(&arena, library, (int)strlen(library), "test.math", &def,
                                          1));
        CBMResolvedCallArray rows = {0};
        c_cross(&arena, sources[i], true, &def, 1, NULL, NULL, 0, &rows);
        int exact = c_proven_count(&rows, "test.math.square");
        int total = c_proven_count(&rows, NULL);
        cbm_arena_destroy(&arena);
        ASSERT_EQ(exact, expected[i]);
        ASSERT_EQ(total, exact);
    }
    PASS();
}

/* Deliberately retain the permissive expansion as test data. Whether its bytes
 * exist and whether those bytes can authorize a relationship are different
 * contracts. Expected targets and expanded spans below are authored here. */
static const char C_PP_GUESSED[] = "void target(void) {}\n"
                                   "#ifdef LIB_API\n#define CALL() target()\n#else\n"
                                   "#define CALL() ((void)0)\n#endif\nvoid run(void) { CALL(); }\n";
static const char C_PP_MISSING[] = "#define CALL() target()\n#include \"missing.h\"\n"
                                   "void target(void) {}\nvoid run(void) { CALL(); }\n";
static const char C_PP_RAW[] = "void raw_target(void) {}\nvoid raw_run(void) { raw_target(); }\n";
/* Distinct external names prevent cross-file linkage ambiguity from hiding an
 * incorrect same-file expansion in the persisted regression. */
static const char C_PP_PERSIST_GUESSED[] =
    "void guessed_target(void) {}\n"
    "#ifdef LIB_API\n#define CALL() guessed_target()\n#else\n"
    "#define CALL() ((void)0)\n#endif\nvoid guessed_run(void) { CALL(); }\n";
static const char C_PP_PERSIST_MISSING[] =
    "#define CALL() missing_target()\n#include \"missing.h\"\n"
    "void missing_target(void) {}\nvoid missing_run(void) { CALL(); }\n";

TEST(resolver_c_preprocessed_proofs_require_complete_explicit_configuration) {
    const char *defined[] = {"LIB_API=1", NULL};
    const char *enabled[] = {"ENABLE_CALL=1", NULL};
    const char *root = th_mktempdir("cbm_c_pp_authority");
    ASSERT_NOT_NULL(root);
    ASSERT_EQ(th_write_file(TH_PATH(root, "empty.h"), "/* complete empty header */\n"), 0);
    const char *include_paths[] = {root, NULL};
    const struct {
        const char *id, *source, *expanded;
        const char **defines;
        int expected, genuine;
    } cases[] = {
        {"guessed-export-is-not-config", C_PP_GUESSED,
         "void target(void) {}\n\n\n\n\n\nvoid run(void) { target ( ) ; }", NULL, 0, 0},
        {"missing-header-is-not-complete", C_PP_MISSING,
         "\n\nvoid target(void) {}\nvoid run(void) { target ( ) ; }", NULL, 0, 0},
        {"explicit-export-config", C_PP_GUESSED,
         "void target(void) {}\n\n\n\n\n\nvoid run(void) { target ( ) ; }", defined, 1, 1},
        {"source-undef-overrides-config",
         "void target(void) {}\n#undef LIB_API\n#ifdef LIB_API\n#define CALL() target()\n"
         "#else\n#define CALL() ((void)0)\n#endif\nvoid run(void) { CALL(); }\n",
         "void target(void) {}\n\n\n\n\n\n\nvoid run(void) { ( ( void ) 0 ) ; }", defined, 0, 0},
        {"absent-ordinary-config",
         "void target(void) {}\n#ifdef ENABLE_CALL\n#define CALL() target()\n#else\n"
         "#define CALL() ((void)0)\n#endif\nvoid run(void) { CALL(); }\n",
         "void target(void) {}\n\n\n\n\n\nvoid run(void) { ( ( void ) 0 ) ; }", NULL, 0, 0},
        {"explicit-ordinary-config",
         "void target(void) {}\n#ifdef ENABLE_CALL\n#define CALL() target()\n#else\n"
         "#define CALL() ((void)0)\n#endif\nvoid run(void) { CALL(); }\n",
         "void target(void) {}\n\n\n\n\n\nvoid run(void) { target ( ) ; }", enabled, 1, 1},
        {"source-local-macro",
         "void target(void) {}\n#define CALL() target()\nvoid run(void) { CALL(); }\n",
         "void target(void) {}\n\nvoid run(void) { target ( ) ; }", NULL, 1, 1},
        /* C-PP-HEADER-CERTIFICATE: this authored empty header cannot change the
         * call. WHY: a loaded header has no retained dependency certificate.
         * TRIED: both missing and existing include paths are tested; allowing
         * either expanded buffer to self-attest loses the header identity. */
        {"existing-header-needs-certificate",
         "#define CALL() target()\n#include \"empty.h\"\n"
         "void target(void) {}\nvoid run(void) { CALL(); }\n",
         "\n\nvoid target(void) {}\nvoid run(void) { target ( ) ; }", NULL, 0, 1},
    };
    int wrong = 0, found = 0, genuine = 0, invalid = 0;
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        const char *source = cases[i].source;
        CBMPreprocessedSource *pp = cbm_preprocess_with_map(source, (int)strlen(source), "main.c",
                                                            cases[i].defines, include_paths, 0);
        bool bytes = pp && pp->source && !strcmp(pp->source, cases[i].expanded);
        /* Header comments may survive the permissive loader. Their spelling
         * is irrelevant to a negative authority check, so do not give them an
         * independently mapped positive span. */
        if (i == 7)
            bytes = pp && pp->source;
        if (!bytes) {
            fprintf(stderr, "[RQ-UNIT] invalid authored expansion case=%s\n", cases[i].id);
            invalid++;
        }
        CBMFileResult *result =
            cbm_extract_file_ex(source, (int)strlen(source), CBM_LANG_C, "test", "main.c", 0,
                                cases[i].defines, include_paths, NULL, NULL);
        int exact = 0, unexpected = 0;
        if (!result) {
            invalid++;
        } else {
            const char *site = strstr(cases[i].expanded, "target ( )");
            for (int j = 0; j < result->resolved_calls.count; j++) {
                const CBMResolvedCall *row = result->resolved_calls.items + j;
                if (!c_row_admitted(row))
                    continue;
                bool match = site && row->kind != CBM_RESOLVED_FIELD_REFERENCE &&
                             row->source_origin == CBM_SOURCE_ORIGIN_PREPROCESSED &&
                             !strcmp(row->caller_qn, "test.main.run") &&
                             !strcmp(row->callee_qn, "test.main.target") &&
                             row->site_start_byte == (uint32_t)(site - cases[i].expanded) &&
                             row->site_end_byte ==
                                 (uint32_t)(site - cases[i].expanded + strlen("target ( )"));
                if (match)
                    exact++;
                else
                    unexpected++;
            }
            cbm_free_result(result);
        }
        if (exact != cases[i].expected || unexpected) {
            fprintf(stderr, "[RQ-UNIT] case=%s expected=%d exact=%d unexpected=%d\n", cases[i].id,
                    cases[i].expected, exact, unexpected);
            wrong++;
        }
        found += exact && cases[i].genuine;
        genuine += cases[i].genuine;
        cbm_preprocessed_source_free(pp);
    }
    fprintf(stderr,
            "[RQ-UNIT] C-PREPROCESSOR-AUTHORITY genuine=%d found=%d missing=%d wrong=%d "
            "invalid_oracle=%d\n",
            genuine, found, genuine - found, wrong, invalid);
    th_cleanup(root);
    ASSERT_EQ(invalid, 0);
    ASSERT_EQ(wrong, 0);
    ASSERT_EQ(genuine, 4);
    ASSERT_EQ(found, 3);
    PASS();
}

TEST(resolver_c_unverified_preprocessing_cannot_persist_relationships) {
    const RQ_Source sources[] = {{.path = "guessed.c", .text = C_PP_PERSIST_GUESSED},
                                 {.path = "missing.c", .text = C_PP_PERSIST_MISSING},
                                 {.path = "raw.c", .text = C_PP_RAW}};
    const RQ_Expected expected[] = {
        {.case_id = "raw-direct-positive",
         .site = {.file = "raw.c", .caller = "raw.raw_run", .needle = "raw_target()"},
         .relation = "CALLS",
         .target = {.file = "raw.c",
                    .suffix = "raw.raw_target",
                    .label = "Function",
                    .declaration_line = 1,
                    .declaration_text = "void raw_target(void) {}"}}};
    const RQ_Forbidden forbidden[] = {
        {.case_id = "guessed-config-negative",
         .site = {.file = "guessed.c",
                  .caller = "guessed.guessed_run",
                  .needle = "CALL()",
                  .occurrence_index = 2},
         .relation = "CALLS",
         .any_target = true},
        {.case_id = "missing-header-negative",
         .site = {.file = "missing.c",
                  .caller = "missing.missing_run",
                  .needle = "CALL()",
                  .occurrence_index = 1},
         .relation = "CALLS",
         .any_target = true},
    };
    for (int driver = 0; driver < 2; driver++) {
        const char *root = th_mktempdir("cbm_c_pp_persist");
        ASSERT_NOT_NULL(root);
        char repo[768], database[768];
        snprintf(repo, sizeof(repo), "%s/repo", root);
        snprintf(database, sizeof(database), "%s/index.db", root);
        bool ok = cbm_mkdir_p(repo, 0755);
        for (size_t i = 0; ok && i < sizeof(sources) / sizeof(sources[0]); i++)
            ok = th_write_file(TH_PATH(repo, sources[i].path), sources[i].text) == 0;
        for (int i = 0; ok && i < 55; i++) {
            char path[32];
            snprintf(path, sizeof(path), "filler%02d.c", i);
            ok = th_write_file(TH_PATH(repo, path), "/* unrelated source */\n") == 0;
        }
        const char *old_workers = getenv("CBM_WORKERS");
        char *saved_workers = old_workers ? strdup(old_workers) : NULL;
        ok = ok && (!old_workers || saved_workers);
        cbm_setenv("CBM_WORKERS", driver ? "4" : "1", 1);
        cbm_pipeline_t *pipeline = ok ? cbm_pipeline_new(repo, database, CBM_MODE_FULL) : NULL;
        ok = pipeline && cbm_pipeline_set_project_name(pipeline, "c-pp-authority") &&
             cbm_pipeline_run(pipeline) == 0;
        cbm_pipeline_free(pipeline);
        if (saved_workers) {
            cbm_setenv("CBM_WORKERS", saved_workers, 1);
            free(saved_workers);
        } else
            cbm_unsetenv("CBM_WORKERS");
        /* Reopen the on-disk index: this checks admitted occurrence rows and
         * graph projection after persistence, for each execution driver. */
        cbm_store_t *store = ok ? cbm_store_open_path_existing(database) : NULL;
        RQ_Report report = {0};
        ok = store &&
             rq_check(store, "c-pp-authority", "c-preprocessor-authority", sources,
                      sizeof(sources) / sizeof(sources[0]), expected,
                      sizeof(expected) / sizeof(expected[0]), forbidden,
                      sizeof(forbidden) / sizeof(forbidden[0]), &report, stderr) &&
             ok;
        if (store)
            cbm_store_close(store);
        th_cleanup(root);
        ASSERT(ok);
        ASSERT_EQ(report.genuine, 1);
        ASSERT_EQ(report.found, 1);
        ASSERT_EQ(report.missing + report.wrong + report.metadata_errors, 0);
    }
    PASS();
}

/* Shape of redis src/defrag.c:1234. An #ifdef splitting a long else-if chain leaves
 * the first syntax error more than 16 levels under the root. The audit used to call
 * that a failure of the whole file, before the include table it reads later existed,
 * so the next declared binding dereferenced a NULL table and took the indexing worker
 * down. The boundary is the error's own position: earlier sites keep their proofs,
 * later ones do not, and declarations past it are simply not proven. */
TEST(resolver_c_nested_syntax_error_bounds_proofs_at_the_error) {
    const char *source = "#include \"lib.h\"\n"
                         "struct Holder { int value; };\n"
                         "void work(void);\n"
                         "void before(void) { work(); }\n"
                         "void chain(int n) {\n"
                         "    if (n == 0) { n = 1;\n"
                         "    } else if (n == 1) { n = 2;\n"
                         "    } else if (n == 2) { n = 3;\n"
                         "    } else if (n == 3) { n = 4;\n"
                         "    } else if (n == 4) { n = 5;\n"
                         "    } else if (n == 5) { n = 6;\n"
                         "    } else if (n == 6) { n = 7;\n"
                         "    } else if (n == 7) { n = 8;\n"
                         "    } else if (n == 8) { n = 9;\n"
                         "    } else if (n == 9) { n = 10;\n"
                         "#ifdef EXTRA\n"
                         "    } else if (n == 10) { n = 11;\n"
                         "#endif\n"
                         "    } else { n = 12;\n"
                         "    }\n"
                         "}\n"
                         "struct Holder shared_holder;\n"
                         "void after(void) { work(); }\n";
    CBMLSPDef defs[] = {{.qualified_name = "test.main.work",
                         .declaration_identity = "fixture:test.main.work",
                         .short_name = "work",
                         .label = "Function"},
                        {.qualified_name = "test.main.Holder",
                         .graph_qualified_name = "test.main.Holder",
                         .short_name = "Holder",
                         .label = "Class",
                         .def_module_qn = "test.main",
                         .declaration_identity = "fixture:test.main.Holder",
                         .field_defs = "value:int"}};
    const char *paths[] = {"lib.h"}, *modules[] = {"test.lib"};
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};
    c_cross(&arena, source, false, defs, 2, paths, modules, 1, &out);
    uint32_t error_region = (uint32_t)(strstr(source, "#ifdef EXTRA") - source);
    uint32_t before_call = (uint32_t)(strstr(source, "work(); }") - source);
    int before_proofs = 0, proofs_past_error = 0;
    for (int i = 0; i < out.count; i++) {
        const CBMResolvedCall *row = &out.items[i];
        if (row->kind == CBM_RESOLVED_FIELD_REFERENCE || !c_row_admitted(row) ||
            strcmp(row->callee_qn, "test.main.work"))
            continue;
        if (row->site_start_byte == before_call)
            before_proofs++;
        if (row->site_start_byte >= error_region)
            proofs_past_error++;
    }
    cbm_arena_destroy(&arena);
    ASSERT_EQ(before_proofs, 1);
    ASSERT_EQ(proofs_past_error, 0);
    PASS();
}

/* An include written through a macro names no file this resolver can follow. The
 * file with it must not gain an edge through a header it cannot prove it includes,
 * and another file's literal include of that header still resolves. */
TEST(resolver_c_macro_include_proves_nothing_for_its_file) {
    char *root = th_mktempdir("cbm_c_macro_include");
    ASSERT_NOT_NULL(root);
    ASSERT_EQ(th_write_file(TH_PATH(root, "lib.h"), "void helper(void);\n"), 0);
    ASSERT_EQ(th_write_file(TH_PATH(root, "lib.c"), "#include \"lib.h\"\nvoid helper(void) {}\n"),
              0);
    ASSERT_EQ(th_write_file(TH_PATH(root, "clean.c"),
                            "#include \"lib.h\"\nvoid clean(void) { helper(); }\n"),
              0);
    ASSERT_EQ(th_write_file(TH_PATH(root, "macro.c"),
                            "#define HEADER \"lib.h\"\n#include HEADER\n"
                            "void through_macro(void) { helper(); }\n"),
              0);
    char db[1024];
    snprintf(db, sizeof(db), "%s/index.db", root);
    cbm_pipeline_t *pipeline = cbm_pipeline_new(root, db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(pipeline);
    int rc = cbm_pipeline_run(pipeline);
    cbm_store_t *store = rc == 0 ? cbm_store_open_path(db) : NULL;
    int proven = -1, through_macro = -1;
    if (store) {
        char sql[512];
        sqlite3 *handle = cbm_store_get_db(store);
        const char *count_edge = "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id "
                                 "JOIN nodes t ON t.id=e.target_id WHERE e.type='CALLS' AND "
                                 "s.name='%s' AND t.name='helper'";
        sqlite3_stmt *statement = NULL;
        snprintf(sql, sizeof(sql), count_edge, "clean");
        if (sqlite3_prepare_v2(handle, sql, -1, &statement, NULL) == SQLITE_OK &&
            sqlite3_step(statement) == SQLITE_ROW)
            proven = sqlite3_column_int(statement, 0);
        sqlite3_finalize(statement);
        statement = NULL;
        snprintf(sql, sizeof(sql), count_edge, "through_macro");
        if (sqlite3_prepare_v2(handle, sql, -1, &statement, NULL) == SQLITE_OK &&
            sqlite3_step(statement) == SQLITE_ROW)
            through_macro = sqlite3_column_int(statement, 0);
        sqlite3_finalize(statement);
        cbm_store_close(store);
    }
    cbm_pipeline_free(pipeline);
    th_cleanup(root);
    ASSERT_EQ(rc, 0);
    /* Another file's call still resolves through its own literal include. */
    ASSERT_EQ(proven, 1);
    /* The macro's file has an unknown include, so it must not gain the same edge. */
    ASSERT_EQ(through_macro, 0);
    PASS();
}

/* clang's lexer tests include one header tens of thousands of times, past what one
 * file's proof pool analyses. That file's imports are limited, and visibly so. It
 * must not stop another file from publishing or resolving. */
TEST(resolver_c_file_past_the_proof_limit_does_not_stop_publication) {
    enum { LINES = 70000 };
    char *root = th_mktempdir("cbm_c_many_includes");
    ASSERT_NOT_NULL(root);
    static const char line[] = "#include \"lib.h\"\n";
    size_t bytes = (size_t)LINES * (sizeof(line) - 1) + 1;
    char *many = malloc(bytes);
    ASSERT_NOT_NULL(many);
    for (size_t i = 0; i < LINES; i++)
        memcpy(many + i * (sizeof(line) - 1), line, sizeof(line) - 1);
    many[bytes - 1] = '\0';
    ASSERT_EQ(th_write_file(TH_PATH(root, "many.h"), many), 0);
    free(many);
    ASSERT_EQ(th_write_file(TH_PATH(root, "lib.h"), "void helper(void);\n"), 0);
    ASSERT_EQ(th_write_file(TH_PATH(root, "lib.c"), "#include \"lib.h\"\nvoid helper(void) {}\n"),
              0);
    ASSERT_EQ(th_write_file(TH_PATH(root, "clean.c"),
                            "#include \"lib.h\"\nvoid clean(void) { helper(); }\n"),
              0);
    char db[1024];
    snprintf(db, sizeof(db), "%s/index.db", root);
    cbm_pipeline_t *pipeline = cbm_pipeline_new(root, db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(pipeline);
    int rc = cbm_pipeline_run(pipeline);
    cbm_store_t *store = rc == 0 ? cbm_store_open_path(db) : NULL;
    int proven = -1;
    if (store) {
        sqlite3_stmt *statement = NULL;
        if (sqlite3_prepare_v2(cbm_store_get_db(store),
                               "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id "
                               "JOIN nodes t ON t.id=e.target_id WHERE e.type='CALLS' AND "
                               "s.name='clean' AND t.name='helper'",
                               -1, &statement, NULL) == SQLITE_OK &&
            sqlite3_step(statement) == SQLITE_ROW)
            proven = sqlite3_column_int(statement, 0);
        sqlite3_finalize(statement);
        cbm_store_close(store);
    }
    cbm_pipeline_free(pipeline);
    th_cleanup(root);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(proven, 1);
    PASS();
}

SUITE(resolver_c) {
    RUN_TEST(resolver_c_file_past_the_proof_limit_does_not_stop_publication);
    RUN_TEST(resolver_c_macro_include_proves_nothing_for_its_file);
    RUN_TEST(resolver_c_nested_syntax_error_bounds_proofs_at_the_error);
    RUN_TEST(resolver_c_preprocessed_proofs_require_complete_explicit_configuration);
    RUN_TEST(resolver_c_unverified_preprocessing_cannot_persist_relationships);
    RUN_TEST(resolver_c_external_prototype_requires_compatible_unique_definition);
    RUN_TEST(resolver_cpp_linkage_preserves_namespace_and_registered_prototype_identity);
    RUN_TEST(resolver_cpp_explicit_builtin_arguments_exclude_adl_only_with_lexical_proof);
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
