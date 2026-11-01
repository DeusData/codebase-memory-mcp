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
        if ((row->kind == CBM_RESOLVED_INVOCATION || row->kind == CBM_RESOLVED_CALL_REFERENCE) &&
            c_row_admitted(row) && (!target || !strcmp(row->callee_qn, target)))
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
                             .field_defs = "count:int|beta_only:int"},
                            {.qualified_name = "test.alpha.@c-file-facts",
                             .short_name = "@c-file-facts",
                             .label = CBM_C_FILE_FACTS_LABEL,
                             .def_module_qn = "test.alpha"}};
        /* The header's facts carry its guard, which is how case 5 stays out. */
        CBMCFileFacts header_facts = {0};
        ASSERT(cbm_c_collect_file_facts(&arena, header, (int)strlen(header), "test.alpha",
                                        CBM_LANG_CPP, defs, 1, &header_facts));
        defs[3].c_file_facts = &header_facts;
        ASSERT(cbm_c_collect_source_facts(&arena, other, (int)strlen(other), "test.other", defs + 1,
                                          1));
        ASSERT(
            cbm_c_collect_source_facts(&arena, beta, (int)strlen(beta), "test.beta", defs + 2, 1));
        CBMResolvedCallArray rows = {0};
        int imports = i == 2 ? 2 : i == 3 ? 0 : 1;
        int first = i == 4 ? 2 : 0;
        c_cross(&arena, source, false, defs, 4, paths + first, modules + first, imports, &rows);
        const char *sites[] = {strstr(source, "a->count =") + 3, strstr(source, "a->count +") + 3,
                               strstr(source, "a->alpha_only") + 3};
        const char *targets[] = {"test.alpha.Alpha.count", "test.alpha.Alpha.count",
                                 "test.alpha.Alpha.alpha_only"};
        unsigned seen = 0;
        for (int j = 0; j < rows.count; j++) {
            const CBMResolvedCall *row = &rows.items[j];
            if (!c_row_admitted(row) || row->kind == CBM_RESOLVED_TYPE_REFERENCE)
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
    /* Every real C header is guarded, and every one of these three references
     * targets Alpha's fields. The guard's one way to hide them, the caller defining
     * it first, is case 5 above. An ordinary feature #ifdef around the struct, case
     * 6, stays unknown. */
    ASSERT_EQ(exact[0], 3);
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
    /* `struct Engine : Other { void work() {} };`: a class's own member hides whatever
     * its bases declare, so the unknown base changes nothing about this call. */
    ASSERT_EQ(proven[2], 1);
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
        /* The only definition of this external name, written in a branch. A caller
         * with a visible prototype either links to it or does not link to anything
         * in this project, so this is the declaration the call selects. */
        {"int lib_value(void); int run(void) { return lib_value(); }",
         "#ifdef FEATURE\nint lib_value(void) { return 7; }\n#endif", NULL, false, 1},
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
 * down. The boundary is the error's own position within its function: the sites after
 * it there are unread, and the next complete function has its own scopes. */
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
                         "    } else { n = 12; work();\n"
                         "    }\n"
                         "}\n"
                         "struct Holder shared_holder;\n"
                         "void declares(int n) {\n"
                         "    n = ( int work ;\n"
                         "    work();\n"
                         "}\n"
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
    uint32_t before_call = (uint32_t)(strstr(source, "work(); }") - source);
    uint32_t split_call = (uint32_t)(strstr(source, "n = 12; work();") - source + strlen("n = 12; "));
    uint32_t hidden_call =
        (uint32_t)(strstr(source, "( int work ;\n    work();") - source + strlen("( int work ;\n    "));
    uint32_t after_call = (uint32_t)(strstr(source, "after(void) { work(); }") - source +
                                     strlen("after(void) { "));
    int before_proofs = 0, split_proofs = 0, hidden_proofs = 0, after_proofs = 0, other = 0;
    for (int i = 0; i < out.count; i++) {
        const CBMResolvedCall *row = &out.items[i];
        if (row->kind == CBM_RESOLVED_FIELD_REFERENCE || !c_row_admitted(row) ||
            strcmp(row->callee_qn, "test.main.work"))
            continue;
        if (row->site_start_byte == before_call)
            before_proofs++;
        else if (row->site_start_byte == split_call)
            split_proofs++;
        else if (row->site_start_byte == hidden_call)
            hidden_proofs++;
        else if (row->site_start_byte == after_call)
            after_proofs++;
        else
            other++;
    }
    cbm_arena_destroy(&arena);
    ASSERT_EQ(before_proofs, 1);
    /* The #ifdef split error holds no type, so nothing it swallowed can hide `work`. */
    ASSERT_EQ(split_proofs, 1);
    /* This one swallowed `int work`, a local that would hide the function. */
    ASSERT_EQ(hidden_proofs, 0);
    ASSERT_EQ(after_proofs, 1);
    ASSERT_EQ(other, 0);
    PASS();
}

/* ── Translation-unit rules, on the shapes real C projects use ───────────────
 * Each fixture runs through both execution drivers. The sources are small copies
 * of forms seen in redis and neovim, so a rule that only holds on tidy input
 * fails here, and every positive sits beside the decoy that would falsify it. */
typedef struct {
    const char *suite;
    const RQ_Source *sources;
    size_t source_count;
    const RQ_Expected *expected;
    size_t expected_count;
    const RQ_Forbidden *forbidden;
    size_t forbidden_count;
    const char *compile_commands; /* %s is the repository root */
    size_t known_gaps;            /* rows in `expected` marked RQ_KNOWN_GAP */
} CTUCase;

static bool c_tu_run(const CTUCase *fixture, int driver) {
    const char *root = th_mktempdir("cbm_c_tu");
    if (!root)
        return false;
    char repo[768], database[768];
    snprintf(repo, sizeof(repo), "%s/repo", root);
    snprintf(database, sizeof(database), "%s/index.db", root);
    bool ok = cbm_mkdir_p(repo, 0755);
    for (size_t i = 0; ok && i < fixture->source_count; i++)
        ok = th_write_file(TH_PATH(repo, fixture->sources[i].path), fixture->sources[i].text) == 0;
    for (int i = 0; ok && i < 55; i++) {
        char path[32];
        snprintf(path, sizeof(path), "filler%02d.c", i);
        ok = th_write_file(TH_PATH(repo, path), "/* unrelated source */\n") == 0;
    }
    if (ok && fixture->compile_commands) {
        char config[2048];
        snprintf(config, sizeof(config), fixture->compile_commands, repo);
        ok = th_write_file(TH_PATH(repo, "compile_commands.json"), config) == 0;
    }
    const char *old_workers = getenv("CBM_WORKERS");
    char *saved_workers = old_workers ? strdup(old_workers) : NULL;
    ok = ok && (!old_workers || saved_workers);
    cbm_setenv("CBM_WORKERS", driver ? "4" : "1", 1);
    cbm_pipeline_t *pipeline = ok ? cbm_pipeline_new(repo, database, CBM_MODE_FULL) : NULL;
    ok = pipeline && cbm_pipeline_set_project_name(pipeline, "c-tu") &&
         cbm_pipeline_run(pipeline) == 0;
    cbm_pipeline_free(pipeline);
    if (saved_workers) {
        cbm_setenv("CBM_WORKERS", saved_workers, 1);
        free(saved_workers);
    } else {
        cbm_unsetenv("CBM_WORKERS");
    }
    cbm_store_t *store = ok ? cbm_store_open_path_existing(database) : NULL;
    RQ_Report report = {0};
    ok = store &&
         rq_check(store, "c-tu", fixture->suite, fixture->sources, fixture->source_count,
                  fixture->expected, fixture->expected_count, fixture->forbidden,
                  fixture->forbidden_count, &report, stderr) &&
         ok;
    if (store)
        cbm_store_close(store);
    th_cleanup(root);
    ok = ok && report.genuine == fixture->expected_count && report.required_missing == 0 &&
         report.recovered_gaps == 0 && report.found + fixture->known_gaps == report.genuine &&
         report.missing == fixture->known_gaps && report.wrong + report.metadata_errors == 0;
    if (!ok)
        fprintf(stderr, "[C-TU] %s driver=%d genuine=%zu found=%zu missing=%zu wrong=%zu meta=%zu\n",
                fixture->suite, driver, report.genuine, report.found, report.missing, report.wrong,
                report.metadata_errors);
    return ok;
}

#define C_TU_EXPECT(id, file_, caller_, needle_, relation_, target_file, target_suffix, label_,  \
                    line_, text_)                                                               \
    {.case_id = id,                                                                             \
     .site = {.file = file_, .caller = caller_, .needle = needle_},                             \
     .relation = relation_,                                                                     \
     .target = {.file = target_file,                                                            \
                .suffix = target_suffix,                                                        \
                .label = label_,                                                                \
                .declaration_line = line_,                                                      \
                .declaration_text = text_}}
#define C_TU_FORBID(id, file_, caller_, needle_, relation_)                                     \
    {.case_id = id,                                                                             \
     .site = {.file = file_, .caller = caller_, .needle = needle_},                             \
     .relation = relation_,                                                                     \
     .any_target = true}

/* The byte of `word` after the first `context` in text: an identifier's own span. */
static uint32_t c_tu_at(const char *text, const char *context, const char *word) {
    const char *from = strstr(text, context);
    const char *found = from ? strstr(from, word) : NULL;
    return found ? (uint32_t)(found - text) : 0;
}
#define C_TU_WORD(file_, text_, caller_, context, word)                                         \
    {.file = file_,                                                                             \
     .caller = caller_,                                                                         \
     .start_byte = c_tu_at(text_, context, word),                                               \
     .end_byte = c_tu_at(text_, context, word) + (uint32_t)strlen(word)}

/* redis: `server.h` includes `sds.h`, callers include only `server.h`, and each
 * function is defined in a .c file no caller includes. */
static const RQ_Source C_TU_REDIS[] = {
    {.path = "src/sds.h",
     .text = "#ifndef __SDS_H\n#define __SDS_H\n#include <stddef.h>\n"
             "typedef char *sds;\n"
             "sds sdsnew(const char *init);\n"
             "static inline int sdsavail(const sds s) { return s ? 1 : 0; }\n"
             "#endif\n"},
    {.path = "src/sds.c",
     .text = "#include \"sds.h\"\n"
             "static sds alloc_it(const char *init) { return (sds)init; }\n"
             "sds sdsnew(const char *init) { return alloc_it(init); }\n"},
    {.path = "src/server.h",
     .text = "#ifndef __REDIS_H\n#define __REDIS_H\n#include \"sds.h\"\n"
             "typedef struct client {\n    int flags;\n    sds querybuf;\n} client;\n"
             "void addReply(client *c, int obj);\n"
             "#endif\n"},
    {.path = "src/networking.c",
     .text = "#include \"server.h\"\n"
             "void addReply(client *c, int obj) { c->flags = obj; }\n"},
    {.path = "src/decoy.c",
     .text = "static char *sdsnew(const char *init) { return (char *)init; }\n"
             "char *decoy_use(void) { return sdsnew(\"x\"); }\n"},
    {.path = "src/t_string.c",
     .text = "#include \"server.h\"\n"
             "static int helper(client *c) { return c->flags; }\n"
             "void setCommand(client *c) {\n"
             "    sds s = sdsnew(\"x\");\n"
             "    addReply(c, helper(c));\n"
             "    c->flags = sdsavail(s);\n"
             "}\n"}};

TEST(resolver_c_header_prototype_binds_external_definition_through_includes) {
    const RQ_Expected expected[] = {
        C_TU_EXPECT("transitive-prototype", "src/t_string.c", "t_string.setCommand",
                    "sdsnew(\"x\")", "CALLS", "src/sds.c", "sds.sdsnew", "Function", 3,
                    "sds sdsnew(const char *init) { return alloc_it(init); }"),
        C_TU_EXPECT("direct-prototype", "src/t_string.c", "t_string.setCommand",
                    "addReply(c, helper(c))", "CALLS", "src/networking.c", "networking.addReply",
                    "Function", 2, "void addReply(client *c, int obj) { c->flags = obj; }"),
        C_TU_EXPECT("same-file-static", "src/t_string.c", "t_string.setCommand", "helper(c)",
                    "CALLS", "src/t_string.c", "t_string.helper", "Function", 2,
                    "static int helper(client *c) { return c->flags; }"),
        C_TU_EXPECT("header-static-inline", "src/t_string.c", "t_string.setCommand", "sdsavail(s)",
                    "CALLS", "src/sds.h", "sds.sdsavail", "Function", 6,
                    "static inline int sdsavail(const sds s) { return s ? 1 : 0; }"),
        C_TU_EXPECT("static-stays-in-its-file", "src/decoy.c", "decoy.decoy_use", "sdsnew(\"x\")",
                    "CALLS", "src/decoy.c", "decoy.sdsnew", "Function", 1,
                    "static char *sdsnew(const char *init) { return (char *)init; }"),
    };
    const CTUCase fixture = {.suite = "c-tu-redis",
                             .sources = C_TU_REDIS,
                             .source_count = sizeof(C_TU_REDIS) / sizeof(C_TU_REDIS[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* Everything here looks like the positive above, and none of it is proved. */
static const RQ_Source C_TU_ABSTAIN[] = {
    /* Two programs in one repository, and neither copy includes the header. */
    {.path = "dup.h", .text = "int dup_value(int v);\n"},
    {.path = "one/dup_value.c", .text = "int dup_value(int v) { return v; }\n"},
    {.path = "two/dup_value.c", .text = "int dup_value(int v) { return v + 1; }\n"},
    {.path = "uses_dup.c",
     .text = "#include \"dup.h\"\nint uses_dup(void) { return dup_value(1); }\n"},
    /* A macro of the name: which function runs depends on the expansion. */
    {.path = "wrapped.h",
     .text = "#define wrapped(v) wrapped_impl(v)\nint wrapped(int v);\nint wrapped_impl(int v);\n"},
    {.path = "wrapped.c", .text = "int wrapped(int v) { return v; }\n"},
    {.path = "uses_wrapped.c",
     .text = "#include \"wrapped.h\"\nint uses_wrapped(void) { return wrapped(1); }\n"},
    /* No include declares it: a name alone is not a binding. */
    {.path = "orphan.c", .text = "int orphan_value(int v) { return v; }\n"},
    {.path = "uses_orphan.c", .text = "int uses_orphan(void) { return orphan_value(1); }\n"},
    /* A parameter hides the function. */
    {.path = "shadow.h", .text = "int shadowed(int v);\n"},
    {.path = "shadow.c", .text = "int shadowed(int v) { return v; }\n"},
    {.path = "uses_shadow.c",
     .text = "#include \"shadow.h\"\nint uses_shadow(int (*shadowed)(int)) { return shadowed(1); }\n"},
    /* The only definition is another file's static. */
    {.path = "hidden.h", .text = "int hidden_value(int v);\n"},
    {.path = "hidden.c", .text = "static int hidden_value(int v) { return v; }\n"},
    {.path = "uses_hidden.c",
     .text = "#include \"hidden.h\"\nint uses_hidden(void) { return hidden_value(1); }\n"},
    /* A declaration whose parameters cannot match the definition's. */
    {.path = "arity.h", .text = "struct box;\nint arity_value(struct box *b);\n"},
    {.path = "arity.c",
     .text = "struct box;\nint arity_value(struct box *b, int n) { return n; }\n"},
    {.path = "uses_arity.c",
     .text = "#include \"arity.h\"\nint uses_arity(struct box *b) { return arity_value(b); }\n"},
    /* The include comes after the call. */
    {.path = "late.h", .text = "int late_value(int v);\n"},
    {.path = "late.c", .text = "int late_value(int v) { return v; }\n"},
    {.path = "uses_late.c",
     .text = "int uses_late(void) { return late_value(1); }\n#include \"late.h\"\n"},
    /* A caller that defines the guard first keeps the header's declarations out. */
    {.path = "guarded.h",
     .text = "#ifndef GUARDED_H\n#define GUARDED_H\nint guarded_value(int v);\n#endif\n"},
    {.path = "guarded.c", .text = "int guarded_value(int v) { return v; }\n"},
    {.path = "uses_guarded.c",
     .text = "#define GUARDED_H\n#include \"guarded.h\"\n"
             "int uses_guarded(void) { return guarded_value(1); }\n"}};

TEST(resolver_c_lookalike_calls_abstain) {
    const RQ_Forbidden forbidden[] = {
        C_TU_FORBID("two-definitions", "uses_dup.c", "uses_dup.uses_dup", "dup_value(1)", "CALLS"),
        C_TU_FORBID("macro-of-the-name", "uses_wrapped.c", "uses_wrapped.uses_wrapped",
                    "wrapped(1)", "CALLS"),
        C_TU_FORBID("not-declared", "uses_orphan.c", "uses_orphan.uses_orphan", "orphan_value(1)",
                    "CALLS"),
        C_TU_FORBID("parameter-shadow", "uses_shadow.c", "uses_shadow.uses_shadow", "shadowed(1)",
                    "CALLS"),
        C_TU_FORBID("other-files-static", "uses_hidden.c", "uses_hidden.uses_hidden",
                    "hidden_value(1)", "CALLS"),
        C_TU_FORBID("arity-mismatch", "uses_arity.c", "uses_arity.uses_arity", "arity_value(b)",
                    "CALLS"),
        C_TU_FORBID("include-after-call", "uses_late.c", "uses_late.uses_late", "late_value(1)",
                    "CALLS"),
        C_TU_FORBID("guard-defined-first", "uses_guarded.c", "uses_guarded.uses_guarded",
                    "guarded_value(1)", "CALLS"),
    };
    const CTUCase fixture = {.suite = "c-tu-abstain",
                             .sources = C_TU_ABSTAIN,
                             .source_count = sizeof(C_TU_ABSTAIN) / sizeof(C_TU_ABSTAIN[0]),
                             .forbidden = forbidden,
                             .forbidden_count = sizeof(forbidden) / sizeof(forbidden[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* Several definitions of one name in one repository: the one whose own file
 * includes this header was compiled against this declaration. */
static const RQ_Source C_TU_COPIES[] = {
    {.path = "app/sds.h", .text = "char *sdsdup(const char *s);\n"},
    {.path = "app/sds.c",
     .text = "#include \"sds.h\"\nchar *sdsdup(const char *s) { return (char *)s; }\n"},
    {.path = "copy/sds.h", .text = "char *sdsdup(const char *s);\n"},
    {.path = "copy/sds.c",
     .text = "#include \"sds.h\"\nchar *sdsdup(const char *s) { return (char *)s + 1; }\n"},
    {.path = "app/main.c",
     .text = "#include \"sds.h\"\nchar *app_copy(void) { return sdsdup(\"a\"); }\n"},
    {.path = "copy/use.c",
     .text = "#include \"sds.h\"\nchar *other_copy(void) { return sdsdup(\"v\"); }\n"}};

TEST(resolver_c_copied_library_binds_the_copy_compiled_against_its_header) {
    const RQ_Expected expected[] = {
        C_TU_EXPECT("app-copy", "app/main.c", "main.app_copy", "sdsdup(\"a\")", "CALLS",
                    "app/sds.c", "app.sds.sdsdup", "Function", 2,
                    "char *sdsdup(const char *s) { return (char *)s; }"),
        C_TU_EXPECT("other-copy", "copy/use.c", "use.other_copy", "sdsdup(\"v\")", "CALLS",
                    "copy/sds.c", "copy.sds.sdsdup", "Function", 2,
                    "char *sdsdup(const char *s) { return (char *)s + 1; }"),
    };
    const CTUCase fixture = {.suite = "c-tu-copies",
                             .sources = C_TU_COPIES,
                             .source_count = sizeof(C_TU_COPIES) / sizeof(C_TU_COPIES[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* The kernel shape behind the put_page audit: `-I include` selects one of two
 * headers with the same spelling. Without the flag this spelling is a collision. */
static const RQ_Source C_TU_KERNEL[] = {
    {.path = "include/linux/mm.h",
     .text = "struct page;\nstatic inline void put_page(struct page *page) { (void)page; }\n"},
    {.path = "tools/include/linux/mm.h",
     .text = "struct page;\nstatic inline void put_page(struct page *page) { (void)page; }\n"},
    {.path = "arch/arm64/include/asm/kvm_pgtable.h",
     .text = "struct kvm_pgtable_mm_ops {\n\tvoid (*put_page)(void *addr);\n};\n"},
    {.path = "drivers/net/rx.c",
     .text = "#include <linux/mm.h>\nvoid recv_free(struct page *page) { put_page(page); }\n"},
    {.path = "drivers/net/unconfigured.c",
     .text = "#include <linux/mm.h>\nvoid other_free(struct page *page) { put_page(page); }\n"}};

TEST(resolver_c_configured_include_root_reaches_calls) {
    const RQ_Expected expected[] = {
        C_TU_EXPECT("configured-root", "drivers/net/rx.c", "rx.recv_free", "put_page(page)",
                    "CALLS", "include/linux/mm.h", "include.linux.mm.put_page", "Function", 2,
                    "static inline void put_page(struct page *page) { (void)page; }"),
    };
    const RQ_Forbidden forbidden[] = {
        C_TU_FORBID("no-root-two-headers", "drivers/net/unconfigured.c", "unconfigured.other_free",
                    "put_page(page)", "CALLS"),
    };
    const CTUCase fixture = {
        .suite = "c-tu-kernel",
        .sources = C_TU_KERNEL,
        .source_count = sizeof(C_TU_KERNEL) / sizeof(C_TU_KERNEL[0]),
        .expected = expected,
        .expected_count = sizeof(expected) / sizeof(expected[0]),
        .forbidden = forbidden,
        .forbidden_count = sizeof(forbidden) / sizeof(forbidden[0]),
        .compile_commands = "[{\"directory\":\"%s\",\"file\":\"drivers/net/rx.c\","
                            "\"command\":\"cc -I include -c drivers/net/rx.c\"}]"};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* The same fixture with no build configuration: the angle spelling names one
 * project file, so it is the file any -I would select. */
TEST(resolver_c_unique_include_spelling_reaches_calls_without_configuration) {
    const RQ_Source sources[] = {
        {.path = "include/linux/mm.h",
         .text = "struct page;\nstatic inline void put_page(struct page *page) { (void)page; }\n"},
        {.path = "arch/arm64/include/asm/kvm_pgtable.h",
         .text = "struct kvm_pgtable_mm_ops {\n\tvoid (*put_page)(void *addr);\n};\n"},
        {.path = "drivers/net/rx.c",
         .text = "#include <linux/mm.h>\nvoid recv_free(struct page *page) { put_page(page); }\n"}};
    const RQ_Expected expected[] = {
        C_TU_EXPECT("unique-spelling", "drivers/net/rx.c", "rx.recv_free", "put_page(page)",
                    "CALLS", "include/linux/mm.h", "include.linux.mm.put_page", "Function", 2,
                    "static inline void put_page(struct page *page) { (void)page; }"),
    };
    const CTUCase fixture = {.suite = "c-tu-unique-spelling",
                             .sources = sources,
                             .source_count = sizeof(sources) / sizeof(sources[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* neovim: headers are spelled from a root under src/, and `foo.h` declares what
 * `foo.c` exports through `foo.h.generated.h`, which the build writes. */
static const RQ_Source C_TU_NEOVIM[] = {
    {.path = "src/nvim/api/buffer.h",
     .text = "#pragma once\n#include \"api/buffer.h.generated.h\"\n"},
    {.path = "src/nvim/api/buffer.c",
     .text = "#include \"nvim/api/buffer.h\"\n"
             "#include \"api/buffer.c.generated.h\"\n"
             "int nvim_buf_line_count(int buf) { return count_lines(buf); }\n"
             "static int count_lines(int buf) { return buf; }\n"
             "static int private_helper(int buf) { return buf; }\n"},
    {.path = "src/nvim/window.c",
     .text = "#include \"nvim/api/buffer.h\"\n"
             "int win_lines(int buf) { return nvim_buf_line_count(buf); }\n"
             "int win_private(int buf) { return private_helper(buf); }\n"}};

TEST(resolver_c_generated_declaration_header_exposes_its_source) {
    const RQ_Expected expected[] = {
        C_TU_EXPECT("generated-header", "src/nvim/window.c", "window.win_lines",
                    "nvim_buf_line_count(buf)", "CALLS", "src/nvim/api/buffer.c",
                    "api.buffer.nvim_buf_line_count", "Function", 3,
                    "int nvim_buf_line_count(int buf) { return count_lines(buf); }"),
        C_TU_EXPECT("own-definition-declared-by-generated-header", "src/nvim/api/buffer.c",
                    "buffer.nvim_buf_line_count", "count_lines(buf)", "CALLS",
                    "src/nvim/api/buffer.c", "api.buffer.count_lines", "Function", 4,
                    "static int count_lines(int buf) { return buf; }"),
    };
    const RQ_Forbidden forbidden[] = {
        C_TU_FORBID("generated-header-holds-no-statics", "src/nvim/window.c", "window.win_private",
                    "private_helper(buf)", "CALLS"),
    };
    const CTUCase fixture = {.suite = "c-tu-neovim",
                             .sources = C_TU_NEOVIM,
                             .source_count = sizeof(C_TU_NEOVIM) / sizeof(C_TU_NEOVIM[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0]),
                             .forbidden = forbidden,
                             .forbidden_count = sizeof(forbidden) / sizeof(forbidden[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* Member access through typedefs, chains and extern objects, from redis. */
#define C_TU_FIELDS_USE                                                                         \
    "#include \"server.h\"\n"                                                                   \
    "int read_flags(client *c) { return c->flags; }\n"                                          \
    "int read_chain(client *c) { return c->db->id; }\n"                                         \
    "int read_alias(client_ptr c) { return c->flags; }\n"                                       \
    "int read_global(void) { return server.port; }\n"                                           \
    "int read_deep(void) { return server.current->db->id; }\n"                                  \
    "client *get_client(void);\n"                                                               \
    "int read_returned(void) { return get_client()->flags; }\n"                                 \
    "void write_flags(client *c) { c->flags = 1; }\n"                                           \
    "void write_global(void) { server.port = 7; }\n"                                            \
    "void bump_flags(client *c) { c->flags++; }\n"                                              \
    "int local_shadow(void) { struct { int port; } server; server.port = 1; return server.port; }\n"
static const RQ_Source C_TU_FIELDS[] = {
    {.path = "src/server.h",
     .text = "#ifndef SERVER_H\n#define SERVER_H\n"
             "typedef struct redisDb {\n    int id;\n} redisDb;\n"
             "typedef struct client {\n    int flags;\n    redisDb *db;\n"
             "#ifdef USE_TLS\n    int tls;\n#endif\n} client;\n"
             "typedef struct client *client_ptr;\n"
             "struct redisServer {\n    int port;\n    client *current;\n};\n"
             "extern struct redisServer server;\n"
             "#endif\n"},
    {.path = "src/server.c",
     .text = "#include \"server.h\"\nstruct redisServer server;\n"
             "client *get_client(void) { return server.current; }\n"},
    {.path = "src/other.h", .text = "struct other {\n    int flags;\n};\n"},
    {.path = "src/use.c", .text = C_TU_FIELDS_USE}};

TEST(resolver_c_member_access_follows_written_declarations) {
    const char *use = C_TU_FIELDS_USE;
    const RQ_Target flags = {.file = "src/server.h",
                             .suffix = "client.flags",
                             .label = "Field",
                             .declaration_line = 7,
                             .declaration_text = "    int flags;"};
    const RQ_Target id = {.file = "src/server.h",
                          .suffix = "redisDb.id",
                          .label = "Field",
                          .declaration_line = 4,
                          .declaration_text = "    int id;"};
    const RQ_Target db = {.file = "src/server.h",
                          .suffix = "client.db",
                          .label = "Field",
                          .declaration_line = 8,
                          .declaration_text = "    redisDb *db;"};
    const RQ_Target port = {.file = "src/server.h",
                            .suffix = "redisServer.port",
                            .label = "Field",
                            .declaration_line = 15,
                            .declaration_text = "    int port;"};
    const RQ_Target current = {.file = "src/server.h",
                               .suffix = "redisServer.current",
                               .label = "Field",
                               .declaration_line = 16,
                               .declaration_text = "    client *current;"};
    const RQ_Target client_type = {.file = "src/server.h",
                                   .suffix = "server.client",
                                   .label = "Class",
                                   .declaration_line = 6,
                                   .declaration_text = "typedef struct client {"};
    const RQ_Target client_ptr = {.file = "src/server.h",
                                  .suffix = "server.client_ptr",
                                  .label = "Type",
                                  .declaration_line = 13,
                                  .declaration_text = "typedef struct client *client_ptr;"};
    const RQ_Target global = {.file = "src/server.c",
                              .suffix = "server.server",
                              .label = "Variable",
                              .declaration_line = 2,
                              .declaration_text = "struct redisServer server;"};
    const RQ_Expected expected[] = {
        {.case_id = "signature-type",
         .site = C_TU_WORD("src/use.c", use, "use.read_flags", "read_flags(", "client"),
         .relation = "USAGE",
         .target = client_type},
        {.case_id = "typedef-read",
         .site = C_TU_WORD("src/use.c", use, "use.read_flags", "return c->flags", "flags"),
         .relation = "USAGE",
         .target = flags},
        {.case_id = "chain-type",
         .site = C_TU_WORD("src/use.c", use, "use.read_chain", "read_chain(", "client"),
         .relation = "USAGE",
         .target = client_type},
        {.case_id = "chain-read-first",
         .site = C_TU_WORD("src/use.c", use, "use.read_chain", "c->db->id", "db"),
         .relation = "USAGE",
         .target = db},
        {.case_id = "chain-read-second",
         .site = C_TU_WORD("src/use.c", use, "use.read_chain", "c->db->id", "id"),
         .relation = "USAGE",
         .target = id},
        {.case_id = "pointer-typedef-type",
         .site = C_TU_WORD("src/use.c", use, "use.read_alias", "read_alias(", "client_ptr"),
         .relation = "USAGE",
         .target = client_ptr},
        {.case_id = "pointer-typedef-read",
         .site = C_TU_WORD("src/use.c", use, "use.read_alias", "client_ptr c) { return c->", "flags"),
         .relation = "USAGE",
         .target = flags},
        {.case_id = "extern-object",
         .site = C_TU_WORD("src/use.c", use, "use.read_global", "return server.port", "server"),
         .relation = "USAGE",
         .target = global},
        {.case_id = "extern-object-read",
         .site = C_TU_WORD("src/use.c", use, "use.read_global", "return server.port", "port"),
         .relation = "USAGE",
         .target = port},
        {.case_id = "extern-chain-object",
         .site = C_TU_WORD("src/use.c", use, "use.read_deep", "server.current->db->id", "server"),
         .relation = "USAGE",
         .target = global},
        {.case_id = "extern-chain-first",
         .site = C_TU_WORD("src/use.c", use, "use.read_deep", "server.current->db->id", "current"),
         .relation = "USAGE",
         .target = current},
        {.case_id = "extern-chain-second",
         .site = C_TU_WORD("src/use.c", use, "use.read_deep", "server.current->db->id", "db"),
         .relation = "USAGE",
         .target = db},
        {.case_id = "extern-chain-third",
         .site = C_TU_WORD("src/use.c", use, "use.read_deep", "server.current->db->id", "id"),
         .relation = "USAGE",
         .target = id},
        {.case_id = "returned-struct-read",
         .site = C_TU_WORD("src/use.c", use, "use.read_returned", "get_client()->flags", "flags"),
         .relation = "USAGE",
         .target = flags},
        {.case_id = "assignment-writes",
         .site = C_TU_WORD("src/use.c", use, "use.write_flags", "c->flags = 1", "flags"),
         .relation = "WRITES",
         .target = flags},
        {.case_id = "extern-assignment-writes",
         .site = C_TU_WORD("src/use.c", use, "use.write_global", "server.port = 7", "port"),
         .relation = "WRITES",
         .target = port},
        {.case_id = "increment-writes",
         .site = C_TU_WORD("src/use.c", use, "use.bump_flags", "c->flags++", "flags"),
         .relation = "WRITES",
         .target = flags},
        {.case_id = "increment-also-reads",
         .site = C_TU_WORD("src/use.c", use, "use.bump_flags", "c->flags++", "flags"),
         .relation = "USAGE",
         .target = flags},
    };
    const RQ_Forbidden forbidden[] = {
        {.case_id = "block-scope-struct-hides-the-global",
         .site = C_TU_WORD("src/use.c", use, "use.local_shadow", "server.port = 1", "port"),
         .relation = "WRITES",
         .any_target = true},
    };
    const CTUCase fixture = {.suite = "c-tu-fields",
                             .sources = C_TU_FIELDS,
                             .source_count = sizeof(C_TU_FIELDS) / sizeof(C_TU_FIELDS[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0]),
                             .forbidden = forbidden,
                             .forbidden_count = sizeof(forbidden) / sizeof(forbidden[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* Bare value and type names: a global through its extern declaration, an
 * enumerator from a header, a written type, each beside what would hide it. */
#define C_TU_VALUES_USE                                                                         \
    "#include \"server.h\"\n"                                                                   \
    "int read_global(void) { return hz; }\n"                                                    \
    "int read_enum(void) { return BLOCKED_LIST; }\n"                                            \
    "mstime_t read_type(mstime_t when) { mstime_t next = when; return next; }\n"                \
    "void write_global(void) { hz = 3; }\n"                                                     \
    "int param_shadow(int hz) { return hz; }\n"                                                 \
    "int local_shadow(void) { int hz = 1; return hz; }\n"                                       \
    "int undeclared(void) { return not_declared_anywhere; }\n"                                  \
    "int local_enum(void) { enum { BLOCKED_LIST = 9 }; return BLOCKED_LIST; }\n"                \
    "int local_tag(void) { struct mstime_t { int v; } m; struct mstime_t *p = &m; return p->v; }\n"
static const RQ_Source C_TU_VALUES[] = {
    {.path = "src/server.h",
     .text = "#ifndef SERVER_H\n#define SERVER_H\n"
             "enum blocked_type {\n    BLOCKED_NONE,\n    BLOCKED_LIST\n};\n"
             "typedef int mstime_t;\n"
             "extern int hz;\n"
             "#endif\n"},
    {.path = "src/server.c", .text = "#include \"server.h\"\nint hz;\n"},
    {.path = "src/elsewhere.c", .text = "static int hz;\nint read_static(void) { return hz; }\n"},
    {.path = "src/use.c", .text = C_TU_VALUES_USE}};

TEST(resolver_c_bare_values_and_types_bind_in_the_translation_unit) {
    const char *use = C_TU_VALUES_USE;
    const char *elsewhere = C_TU_VALUES[2].text;
    const RQ_Target mstime = {.file = "src/server.h",
                              .suffix = "server.mstime_t",
                              .label = "Type",
                              .declaration_line = 7,
                              .declaration_text = "typedef int mstime_t;"};
    const RQ_Target hz = {.file = "src/server.c",
                          .suffix = "server.hz",
                          .label = "Variable",
                          .declaration_line = 2,
                          .declaration_text = "int hz;"};
    const RQ_Target blocked = {.file = "src/server.h",
                               .suffix = "server.BLOCKED_LIST",
                               .label = "Variable",
                               .declaration_line = 5,
                               .declaration_text = "    BLOCKED_LIST"};
    const RQ_Target own_static = {.file = "src/elsewhere.c",
                                  .suffix = "elsewhere.hz",
                                  .label = "Variable",
                                  .declaration_line = 1,
                                  .declaration_text = "static int hz;"};
    const RQ_Expected expected[] = {
        {.case_id = "extern-object",
         .site = C_TU_WORD("src/use.c", use, "use.read_global", "return hz", "hz"),
         .relation = "USAGE",
         .target = hz},
        {.case_id = "header-enumerator",
         .site = C_TU_WORD("src/use.c", use, "use.read_enum", "return BLOCKED_LIST", "BLOCKED_LIST"),
         .relation = "USAGE",
         .target = blocked},
        {.case_id = "extern-object-write",
         .site = C_TU_WORD("src/use.c", use, "use.write_global", "hz = 3", "hz"),
         .relation = "WRITES",
         .target = hz},
        {.case_id = "own-file-static",
         .site = C_TU_WORD("src/elsewhere.c", elsewhere, "elsewhere.read_static", "return hz", "hz"),
         .relation = "USAGE",
         .target = own_static},
        {.case_id = "signature-return-type",
         .site = C_TU_WORD("src/use.c", use, "use.read_type", "mstime_t read_type", "mstime_t"),
         .relation = "USAGE",
         .target = mstime},
        {.case_id = "signature-parameter-type",
         .site = C_TU_WORD("src/use.c", use, "use.read_type", "mstime_t when", "mstime_t"),
         .relation = "USAGE",
         .target = mstime},
        {.case_id = "local-declaration-type",
         .site = C_TU_WORD("src/use.c", use, "use.read_type", "mstime_t next", "mstime_t"),
         .relation = "USAGE",
         .target = mstime},
    };
    const RQ_Forbidden forbidden[] = {
        {.case_id = "parameter-hides-global",
         .site = C_TU_WORD("src/use.c", use, "use.param_shadow", "return hz", "hz"),
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "block-scope-hides-global",
         .site = C_TU_WORD("src/use.c", use, "use.local_shadow", "int hz = 1; return hz", "return hz"),
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "no-declaration",
         .site = C_TU_WORD("src/use.c", use, "use.undeclared", "return not_", "not_declared_anywhere"),
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "block-enumerator-hides-header",
         .site = C_TU_WORD("src/use.c", use, "use.local_enum", "return BLOCKED_LIST", "BLOCKED_LIST"),
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "block-tag-hides-header-type",
         .site = C_TU_WORD("src/use.c", use, "use.local_tag", "struct mstime_t *p", "mstime_t"),
         .relation = "USAGE",
         .any_target = true},
    };
    const CTUCase fixture = {.suite = "c-tu-values",
                             .sources = C_TU_VALUES,
                             .source_count = sizeof(C_TU_VALUES) / sizeof(C_TU_VALUES[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0]),
                             .forbidden = forbidden,
                             .forbidden_count = sizeof(forbidden) / sizeof(forbidden[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* C++ as LLVM writes it: namespaces, a class in a header with its members defined in
 * a source file no caller includes, base classes, implicit members. */
#define C_TU_CXX_HEADER                                                                         \
    "#ifndef LLVM_ADT_STRINGREF_H\n"                                                            \
    "#define LLVM_ADT_STRINGREF_H\n"                                                            \
    "namespace llvm {\n"                                                                        \
    "class StringRef {\n"                                                                       \
    "public:\n"                                                                                 \
    "  int size() const { return Length; }\n"                                                   \
    "  bool empty() const;\n"                                                                   \
    "  int Count;\n"                                                                            \
    "private:\n"                                                                                \
    "  int secret() const { return 1; }\n"                                                      \
    "  int Length;\n"                                                                           \
    "};\n"                                                                                      \
    "class Derived : public StringRef {\n"                                                      \
    "public:\n"                                                                                 \
    "  int own() { return size(); }\n"                                                          \
    "};\n"                                                                                      \
    "int getAsInteger(StringRef S);\n"                                                          \
    "inline int headerHelper(StringRef S) { return S.empty() ? 0 : 1; }\n"                      \
    "struct Arrow {\n"                                                                          \
    "  StringRef *operator->();\n"                                                              \
    "  int Count;\n"                                                                            \
    "};\n"                                                                                      \
    "}\n"                                                                                       \
    "#endif\n"
#define C_TU_CXX_IMPL                                                                           \
    "#include \"llvm/ADT/StringRef.h\"\n"                                                       \
    "using namespace llvm;\n"                                                                   \
    "bool StringRef::empty() const { return Length == 0; }\n"                                   \
    "int llvm::getAsInteger(StringRef S) { return S.size(); }\n"
#define C_TU_CXX_USE                                                                            \
    "#include \"llvm/ADT/StringRef.h\"\n"                                                       \
    "namespace llvm {\n"                                                                        \
    "int useInline(StringRef S) { return S.size(); }\n"                                         \
    "int useOutOfLine(StringRef S) { return S.empty() ? 0 : 1; }\n"                             \
    "int useFree(StringRef S) { return getAsInteger(S); }\n"                                    \
    "int useHeader(StringRef S) { return headerHelper(S); }\n"                                  \
    "int usePointer(StringRef *S) { return S->Count; }\n"                                       \
    "int useBase(Derived D) { return D.size(); }\n"                                             \
    "int usePrivate(StringRef S) { return S.secret(); }\n"                                      \
    "int useArrow(Arrow A) { return A->Count; }\n"                                              \
    "int useDot(Arrow A) { return A.Count; }\n"                                                 \
    "}\n"                                                                                       \
    "int outer(llvm::StringRef S) { return llvm::getAsInteger(S); }\n"
#define C_TU_CXX_ADL                                                                            \
    "namespace a {\n"                                                                           \
    "struct W {};\n"                                                                            \
    "int pick(W w);\n"                                                                          \
    "}\n"                                                                                       \
    "namespace b {\n"                                                                           \
    "using namespace a;\n"                                                                      \
    "int pick(W w);\n"                                                                          \
    "int run(W w) { return pick(w); }\n"                                                        \
    "}\n"
static const RQ_Source C_TU_CXX[] = {{.path = "llvm/include/llvm/ADT/StringRef.h",
                                      .text = C_TU_CXX_HEADER},
                                     {.path = "llvm/lib/Support/StringRef.cpp",
                                      .text = C_TU_CXX_IMPL},
                                     {.path = "llvm/lib/Support/Use.cpp", .text = C_TU_CXX_USE},
                                     {.path = "adl.cpp", .text = C_TU_CXX_ADL}};

TEST(resolver_cpp_members_and_namespaces_bind_through_the_translation_unit) {
    const char *header = "llvm/include/llvm/ADT/StringRef.h";
    const char *impl = "llvm/lib/Support/StringRef.cpp";
    const char *use_file = "llvm/lib/Support/Use.cpp";
    const char *use = C_TU_CXX_USE;
    const char *impl_text = C_TU_CXX_IMPL;
    const RQ_Target size = {.file = header,
                            .suffix = "llvm.StringRef.size",
                            .label = "Method",
                            .declaration_line = 6,
                            .declaration_text = "  int size() const { return Length; }"};
    const RQ_Target empty = {.file = impl,
                             .suffix = "StringRef.StringRef.empty",
                             .label = "Method",
                             .declaration_line = 3,
                             .declaration_text =
                                 "bool StringRef::empty() const { return Length == 0; }"};
    const RQ_Target free_function = {.file = impl,
                                     .suffix = "llvm.getAsInteger",
                                     .label = "Method",
                                     .declaration_line = 4,
                                     .declaration_text =
                                         "int llvm::getAsInteger(StringRef S) { return S.size(); }"};
    const RQ_Target inline_function = {
        .file = header,
        .suffix = "llvm.headerHelper",
        .label = "Function",
        .declaration_line = 18,
        .declaration_text = "inline int headerHelper(StringRef S) { return S.empty() ? 0 : 1; }"};
    const RQ_Target count = {.file = header,
                             .suffix = "llvm.StringRef.Count",
                             .label = "Field",
                             .declaration_line = 8,
                             .declaration_text = "  int Count;"};
    const RQ_Target arrow_count = {.file = header,
                                   .suffix = "llvm.Arrow.Count",
                                   .label = "Field",
                                   .declaration_line = 21,
                                   .declaration_text = "  int Count;"};
    const RQ_Target length = {.file = header,
                              .suffix = "llvm.StringRef.Length",
                              .label = "Field",
                              .declaration_line = 11,
                              .declaration_text = "  int Length;"};
    const RQ_Target string_ref = {.file = header,
                                  .suffix = "llvm.StringRef",
                                  .label = "Class",
                                  .declaration_line = 4,
                                  .declaration_text = "class StringRef {"};
    const RQ_Target arrow_type = {.file = header,
                                  .suffix = "llvm.Arrow",
                                  .label = "Class",
                                  .declaration_line = 19,
                                  .declaration_text = "struct Arrow {"};
    const RQ_Expected expected[] = {
        {.case_id = "inline-member",
         .site = {.file = use_file, .caller = "llvm.useInline", .needle = "S.size()"},
         .relation = "CALLS",
         .target = size},
        {.case_id = "out-of-line-member",
         .site = {.file = use_file, .caller = "llvm.useOutOfLine", .needle = "S.empty()"},
         .relation = "CALLS",
         .target = empty},
        {.case_id = "namespace-function-qualified-definition",
         .site = {.file = use_file, .caller = "llvm.useFree", .needle = "getAsInteger(S)"},
         .relation = "CALLS",
         .target = free_function},
        {.case_id = "header-inline-function",
         .site = {.file = use_file, .caller = "llvm.useHeader", .needle = "headerHelper(S)"},
         .relation = "CALLS",
         .target = inline_function},
        {.case_id = "base-class-member",
         .site = {.file = use_file, .caller = "llvm.useBase", .needle = "D.size()"},
         .relation = "CALLS",
         .target = size},
        {.case_id = "qualified-call",
         .site = {.file = use_file, .caller = "Use.outer", .needle = "llvm::getAsInteger(S)"},
         .relation = "CALLS",
         .target = free_function},
        {.case_id = "member-call-in-out-of-line-definition",
         .site = {.file = impl, .caller = "llvm.getAsInteger", .needle = "S.size()"},
         .relation = "CALLS",
         .target = size},
        {.case_id = "overloaded-arrow-parameter-type",
         .site = C_TU_WORD(use_file, use, "llvm.useArrow", "(Arrow A) { return A->", "Arrow"),
         .relation = "USAGE",
         .target = arrow_type},
        {.case_id = "pointer-member-type",
         .site = C_TU_WORD(use_file, use, "llvm.usePointer", "usePointer(", "StringRef"),
         .relation = "USAGE",
         .target = string_ref},
        {.case_id = "pointer-member",
         .site = C_TU_WORD(use_file, use, "llvm.usePointer", "S->Count", "Count"),
         .relation = "USAGE",
         .target = count},
        {.case_id = "dot-on-a-class-with-arrow-type",
         .site = C_TU_WORD(use_file, use, "llvm.useDot", "useDot(", "Arrow"),
         .relation = "USAGE",
         .target = arrow_type},
        {.case_id = "dot-on-a-class-with-arrow",
         .site = C_TU_WORD(use_file, use, "llvm.useDot", "A.Count", "Count"),
         .relation = "USAGE",
         .target = arrow_count},
        {.case_id = "implicit-member-in-out-of-line-definition",
         .site = C_TU_WORD(impl, impl_text, "StringRef.StringRef.empty", "return Length", "Length"),
         .relation = "USAGE",
         .target = length},
    };
    const RQ_Forbidden forbidden[] = {
        {.case_id = "private-member-from-outside",
         .site = {.file = use_file, .caller = "llvm.usePrivate", .needle = "S.secret()"},
         .relation = "CALLS",
         .any_target = true},
        {.case_id = "overloaded-arrow-is-not-a-field-of-this-class",
         .site = C_TU_WORD(use_file, use, "llvm.useArrow", "A->Count", "Count"),
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "argument-dependent-lookup-adds-a-candidate",
         .site = {.file = "adl.cpp", .caller = "b.run", .needle = "pick(w)"},
         .relation = "CALLS",
         .any_target = true},
    };
    const CTUCase fixture = {.suite = "c-tu-cxx",
                             .sources = C_TU_CXX,
                             .source_count = sizeof(C_TU_CXX) / sizeof(C_TU_CXX[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0]),
                             .forbidden = forbidden,
                             .forbidden_count = sizeof(forbidden) / sizeof(forbidden[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* What the C rules leave unbound on real code. Each row is a genuine reference that the
 * resolver abstains on today; an edge here would be promoted to required, and an edge to
 * anything else is reported as wrong. */
#define C_TU_GAPS_HEADER                                                                        \
    "#ifndef GAPS_H\n"                                                                          \
    "#define GAPS_H\n"                                                                          \
    "typedef struct tabpage_S {\n"                                                              \
    "  struct tabpage_S *tp_next;\n"                                                            \
    "} tabpage_T;\n"                                                                            \
    "extern tabpage_T *first_tabpage;\n"                                                        \
    "#define FOR_ALL_TABS(tp) for (tabpage_T *(tp) = first_tabpage; (tp); (tp) = (tp)->tp_next)\n" \
    "#define LOG_LINE(s) log_line(s)\n"                                                         \
    "void log_line(const char *s);\n"                                                           \
    "struct item {\n"                                                                           \
    "  union {\n"                                                                               \
    "    int value;\n"                                                                          \
    "  };\n"                                                                                    \
    "};\n"                                                                                      \
    "#endif\n"
#define C_TU_GAPS_SOURCE                                                                        \
    "#include \"gaps.h\"\n"                                                                     \
    "tabpage_T *first_tabpage;\n"                                                               \
    "void log_line(const char *s) { (void)s; }\n"
#define C_TU_GAPS_USE                                                                           \
    "#include \"gaps.h\"\n"                                                                     \
    "int use_loop(void) {\n"                                                                    \
    "  int n = 0;\n"                                                                            \
    "  FOR_ALL_TABS(tp) {\n"                                                                    \
    "    n += tp->tp_next != 0;\n"                                                              \
    "  }\n"                                                                                     \
    "  return n;\n"                                                                             \
    "}\n"                                                                                       \
    "void use_macro(void) { LOG_LINE(\"x\"); }\n"                                               \
    "int use_union(struct item *o) { return o->value; }\n"
static const RQ_Source C_TU_GAPS[] = {{.path = "gaps.h", .text = C_TU_GAPS_HEADER},
                                      {.path = "gaps.c", .text = C_TU_GAPS_SOURCE},
                                      {.path = "use_gaps.c", .text = C_TU_GAPS_USE}};

TEST(resolver_c_known_gaps_stay_honest) {
    const char *use = C_TU_GAPS_USE;
    const RQ_Expected expected[] = {
        {.case_id = "macro-declared-loop-variable",
         .site = C_TU_WORD("use_gaps.c", use, "use_gaps.use_loop", "n += tp->", "tp_next"),
         .relation = "USAGE",
         .target = {.file = "gaps.h",
                    .suffix = "gaps.tabpage_S.tp_next",
                    .label = "Field",
                    .declaration_line = 4,
                    .declaration_text = "  struct tabpage_S *tp_next;"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "c-macro-declared-variable",
         .why = "`FOR_ALL_TABS(tp)` declares tp inside a macro body. The resolver does not "
                "expand macros, so tp has no declared type and its members stay unknown.",
         .tried = "Read neovim's FOR_ALL_* loops. Guessing the type from the macro text needs "
                  "expansion rules this resolver does not have."},
        {.case_id = "anonymous-aggregate-member",
         .site = C_TU_WORD("use_gaps.c", use, "use_gaps.use_union", "o->value", "value"),
         .relation = "USAGE",
         .target = {.file = "gaps.h",
                    .suffix = "gaps.item.value",
                    .label = "Field",
                    .declaration_line = 12,
                    .declaration_text = "    int value;"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "c-anonymous-aggregate-member",
         .why = "Members of an anonymous struct or union get no Field node, so there is nothing to "
                "bind. Neovim's `data` union in Object and redis's `u` unions have this form.",
         .tried = "A nested aggregate with a tag binds through the tag. Without one, extraction "
                  "must first attach the members to the enclosing struct."},
        {.case_id = "function-like-macro",
         .site = {.file = "use_gaps.c", .caller = "use_gaps.use_macro", .needle = "LOG_LINE(\"x\")"},
         .relation = "CALLS",
         .target = {.file = "gaps.h",
                    .suffix = "gaps.LOG_LINE#macro",
                    .label = "Macro",
                    .declaration_line = 8,
                    .declaration_text = "#define LOG_LINE(s) log_line(s)"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "c-function-like-macro",
         .why = "A function-like macro use reads as a call, and the function it reaches is only "
                "known after expansion. `serverLog`, `assert` and `kv_size` have this form.",
         .tried = "A macro of the name abstains wherever it is visible, so a same-named function "
                  "never takes the edge. Binding the Macro node as a call needs a decision on "
                  "what CALLS means for a macro."},
    };
    const CTUCase fixture = {.suite = "c-tu-gaps",
                             .sources = C_TU_GAPS,
                             .source_count = sizeof(C_TU_GAPS) / sizeof(C_TU_GAPS[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0]),
                             .known_gaps = sizeof(expected) / sizeof(expected[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* `class LLVM_ABI Name {` and `bool f(int c) ATTR_A ATTR_B {` are how real libraries export
 * things. Each form is only valid if a word is a macro, and the parse leaves that word out.
 * The fixture keeps the macro undefined, as an unconfigured index sees it. */
#define C_TU_ATTR_HEADER                                                                        \
    "#ifndef ATTR_H\n"                                                                          \
    "#define ATTR_H\n"                                                                          \
    "namespace llvm {\n"                                                                        \
    "class LLVM_GSL_POINTER StringRef {\n"                                                      \
    "public:\n"                                                                                 \
    "  LLVM_NODISCARD int size() const { return Length; }\n"                                    \
    "  int empty() const LLVM_READONLY { return 0; }\n"                                         \
    "  int Length;\n"                                                                           \
    "};\n"                                                                                      \
    "class LLVM_ABI Derived final : public StringRef {\n"                                       \
    "};\n"                                                                                      \
    "LLVM_NODISCARD StringRef makeRef();\n"                                                     \
    "}\n"                                                                                       \
    "#endif\n"
#define C_TU_ATTR_C_HEADER                                                                      \
    "#ifndef ASCII_H\n"                                                                         \
    "#define ASCII_H\n"                                                                         \
    "static inline int ascii_iswhite(int c) FUNC_ATTR_CONST { return c == 32; }\n"              \
    "static inline int ascii_isdigit(int c) FUNC_ATTR_CONST FUNC_ATTR_INLINE { return c > 47; }\n" \
    "#endif\n"
#define C_TU_ATTR_USE                                                                           \
    "#include \"attr.h\"\n"                                                                     \
    "namespace llvm {\n"                                                                        \
    "int useMember(StringRef S) { return S.size(); }\n"                                         \
    "int useTrailing(StringRef S) { return S.empty(); }\n"                                      \
    "int useField(StringRef *S) { return S->Length; }\n"                                        \
    "int useBase(Derived D) { return D.size(); }\n"                                             \
    "}\n"
#define C_TU_ATTR_C_USE                                                                         \
    "#include \"ascii.h\"\n"                                                                    \
    "int use_one(int c) { return ascii_iswhite(c); }\n"                                         \
    "int use_two(int c) { return ascii_isdigit(c); }\n"
/* `struct timeval tv { 0 };` has the words of the macro form and is a variable, so nothing
 * is left out of it. */
#define C_TU_ATTR_DECOY                                                                         \
    "struct timeval { int s; };\n"                                                              \
    "struct timeval tv { 0 };\n"                                                                \
    "int decoy_var(void) { return tv.s; }\n"
static const RQ_Source C_TU_ATTR[] = {{.path = "attr.h", .text = C_TU_ATTR_HEADER},
                                      {.path = "ascii.h", .text = C_TU_ATTR_C_HEADER},
                                      {.path = "use_attr.cpp", .text = C_TU_ATTR_USE},
                                      {.path = "use_ascii.c", .text = C_TU_ATTR_C_USE},
                                      {.path = "decoy.cpp", .text = C_TU_ATTR_DECOY}};

TEST(resolver_c_attribute_macros_do_not_hide_declarations) {
    const char *use = C_TU_ATTR_USE;
    const char *decoy = C_TU_ATTR_DECOY;
    const RQ_Target size = {.file = "attr.h",
                            .suffix = "llvm.StringRef.size",
                            .label = "Method",
                            .declaration_line = 6,
                            .declaration_text =
                                "  LLVM_NODISCARD int size() const { return Length; }"};
    const RQ_Target empty = {.file = "attr.h",
                             .suffix = "llvm.StringRef.empty",
                             .label = "Method",
                             .declaration_line = 7,
                             .declaration_text = "  int empty() const LLVM_READONLY { return 0; }"};
    const RQ_Target length = {.file = "attr.h",
                              .suffix = "llvm.StringRef.Length",
                              .label = "Field",
                              .declaration_line = 8,
                              .declaration_text = "  int Length;"};
    const RQ_Target string_ref = {.file = "attr.h",
                                  .suffix = "llvm.StringRef",
                                  .label = "Class",
                                  .declaration_line = 4,
                                  .declaration_text = "class LLVM_GSL_POINTER StringRef {"};
    const RQ_Target timeval_type = {.file = "decoy.cpp",
                                    .suffix = "decoy.timeval",
                                    .label = "Class",
                                    .declaration_line = 1,
                                    .declaration_text = "struct timeval { int s; };"};
    const RQ_Target timeval_field = {.file = "decoy.cpp",
                                     .suffix = "decoy.timeval.s",
                                     .label = "Field",
                                     .declaration_line = 1,
                                     .declaration_text = "struct timeval { int s; };"};
    const RQ_Target timeval_var = {.file = "decoy.cpp",
                                   .suffix = "decoy.tv",
                                   .label = "Variable",
                                   .declaration_line = 2,
                                   .declaration_text = "struct timeval tv { 0 };"};
    (void)timeval_type;
    const RQ_Expected expected[] = {
        {.case_id = "class-with-attribute-macro-has-its-members",
         .site = {.file = "use_attr.cpp", .caller = "llvm.useMember", .needle = "S.size()"},
         .relation = "CALLS",
         .target = size},
        {.case_id = "trailing-attribute-macro",
         .site = {.file = "use_attr.cpp", .caller = "llvm.useTrailing", .needle = "S.empty()"},
         .relation = "CALLS",
         .target = empty},
        {.case_id = "class-with-attribute-macro-is-a-class",
         .site = C_TU_WORD("use_attr.cpp", use, "llvm.useField", "useField(", "StringRef"),
         .relation = "USAGE",
         .target = string_ref},
        {.case_id = "field-of-class-with-attribute-macro",
         .site = C_TU_WORD("use_attr.cpp", use, "llvm.useField", "S->Length", "Length"),
         .relation = "USAGE",
         .target = length},
        {.case_id = "base-class-with-attribute-macros",
         .site = {.file = "use_attr.cpp", .caller = "llvm.useBase", .needle = "D.size()"},
         .relation = "CALLS",
         .target = size},
        {.case_id = "header-definition-one-attribute",
         .site = {.file = "use_ascii.c", .caller = "use_ascii.use_one", .needle = "ascii_iswhite(c)"},
         .relation = "CALLS",
         .target = {.file = "ascii.h",
                    .suffix = "ascii.ascii_iswhite",
                    .label = "Function",
                    .declaration_line = 3,
                    .declaration_text =
                        "static inline int ascii_iswhite(int c) FUNC_ATTR_CONST { return c == 32; }"}},
        {.case_id = "header-definition-two-attributes",
         .site = {.file = "use_ascii.c", .caller = "use_ascii.use_two", .needle = "ascii_isdigit(c)"},
         .relation = "CALLS",
         .target = {.file = "ascii.h",
                    .suffix = "ascii.ascii_isdigit",
                    .label = "Function",
                    .declaration_line = 4,
                    .declaration_text = "static inline int ascii_isdigit(int c) FUNC_ATTR_CONST "
                                        "FUNC_ATTR_INLINE { return c > 47; }"}},
        {.case_id = "brace-initialized-variable-stays-a-variable",
         .site = C_TU_WORD("decoy.cpp", decoy, "decoy.decoy_var", "return tv", "tv"),
         .relation = "USAGE",
         .target = timeval_var},
        {.case_id = "brace-initialized-variable-member",
         .site = C_TU_WORD("decoy.cpp", decoy, "decoy.decoy_var", "tv.s", "s"),
         .relation = "USAGE",
         .target = timeval_field},
    };
    const CTUCase fixture = {.suite = "c-tu-attr",
                             .sources = C_TU_ATTR,
                             .source_count = sizeof(C_TU_ATTR) / sizeof(C_TU_ATTR[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* The forms LLVM uses for its most-called names: a prototype returning a reference, a
 * static member through its class, and a member defined out of line inside a namespace
 * block rather than under `using namespace`. */
#define C_TU_SCOPED_HEADER                                                                      \
    "#ifndef OSTREAM_H\n"                                                                       \
    "#define OSTREAM_H\n"                                                                       \
    "namespace llvm {\n"                                                                        \
    "class raw_ostream {\n"                                                                     \
    "public:\n"                                                                                 \
    "  raw_ostream &write(int x);\n"                                                            \
    "};\n"                                                                                      \
    "raw_ostream &errs();\n"                                                                    \
    "class Error {\n"                                                                           \
    "public:\n"                                                                                 \
    "  static Error success();\n"                                                               \
    "private:\n"                                                                                \
    "  static Error hidden();\n"                                                                \
    "};\n"                                                                                      \
    "namespace detail {\n"                                                                      \
    "class Nested {\n"                                                                          \
    "public:\n"                                                                                 \
    "  int outer();\n"                                                                          \
    "private:\n"                                                                                \
    "  int inner() const { return 1; }\n"                                                       \
    "};\n"                                                                                      \
    "}\n"                                                                                       \
    "}\n"                                                                                       \
    "#endif\n"
#define C_TU_SCOPED_IMPL                                                                        \
    "#include \"ostream.h\"\n"                                                                  \
    "namespace llvm {\n"                                                                        \
    "raw_ostream &errs() { static raw_ostream S; return S; }\n"                                 \
    "raw_ostream &raw_ostream::write(int x) { return *this; }\n"                                \
    "Error Error::success() { return hidden(); }\n"                                             \
    "Error Error::hidden() { return Error(); }\n"                                               \
    "namespace detail {\n"                                                                      \
    "int Nested::outer() { return inner(); }\n"                                                 \
    "}\n"                                                                                       \
    "}\n"
#define C_TU_SCOPED_USE                                                                         \
    "#include \"ostream.h\"\n"                                                                  \
    "using namespace llvm;\n"                                                                   \
    "void useRef() { errs(); }\n"                                                               \
    "void useChain() { errs().write(1); }\n"                                                    \
    "Error useStatic() { return Error::success(); }\n"                                          \
    "Error useHidden() { return Error::hidden(); }\n"
static const RQ_Source C_TU_SCOPED[] = {{.path = "lib/ostream.h", .text = C_TU_SCOPED_HEADER},
                                        {.path = "lib/ostream.cpp", .text = C_TU_SCOPED_IMPL},
                                        {.path = "lib/use.cpp", .text = C_TU_SCOPED_USE}};

TEST(resolver_cpp_reference_prototypes_and_scoped_members) {
    const char *use = C_TU_SCOPED_USE;
    const RQ_Target errs = {.file = "lib/ostream.cpp",
                            .suffix = "ostream.llvm.errs",
                            .label = "Function",
                            .declaration_line = 3,
                            .declaration_text =
                                "raw_ostream &errs() { static raw_ostream S; return S; }"};
    const RQ_Target write = {.file = "lib/ostream.cpp",
                             .suffix = "ostream.raw_ostream.write",
                             .label = "Method",
                             .declaration_line = 4,
                             .declaration_text =
                                 "raw_ostream &raw_ostream::write(int x) { return *this; }"};
    const RQ_Expected expected[] = {
        {.case_id = "prototype-returning-a-reference",
         .site = C_TU_WORD("lib/use.cpp", use, "use.useRef", "useRef", "errs()"),
         .relation = "CALLS",
         .target = errs},
        {.case_id = "chain-starts-at-the-reference",
         .site = C_TU_WORD("lib/use.cpp", use, "use.useChain", "useChain", "errs()"),
         .relation = "CALLS",
         .target = errs},
        {.case_id = "member-of-the-returned-reference",
         .site = C_TU_WORD("lib/use.cpp", use, "use.useChain", "useChain", "errs().write(1)"),
         .relation = "CALLS",
         .target = write},
        {.case_id = "static-member-through-its-class",
         .site = {.file = "lib/use.cpp", .caller = "use.useStatic", .needle = "Error::success()"},
         .relation = "CALLS",
         .target = {.file = "lib/ostream.cpp",
                    .suffix = "ostream.Error.success",
                    .label = "Method",
                    .declaration_line = 5,
                    .declaration_text = "Error Error::success() { return hidden(); }"}},
        {.case_id = "private-member-from-inside-its-class",
         .site = {.file = "lib/ostream.cpp", .caller = "ostream.Error.success", .needle = "hidden()"},
         .relation = "CALLS",
         .target = {.file = "lib/ostream.cpp",
                    .suffix = "ostream.Error.hidden",
                    .label = "Method",
                    .declaration_line = 6,
                    .declaration_text = "Error Error::hidden() { return Error(); }"}},
        {.case_id = "out-of-line-member-inside-a-namespace-block",
         .site = {.file = "lib/ostream.cpp", .caller = "ostream.Nested.outer", .needle = "inner()"},
         .relation = "CALLS",
         .target = {.file = "lib/ostream.h",
                    .suffix = "ostream.llvm.detail.Nested.inner",
                    .label = "Method",
                    .declaration_line = 20,
                    .declaration_text = "  int inner() const { return 1; }"}},
    };
    const RQ_Forbidden forbidden[] = {
        {.case_id = "private-static-member-from-outside",
         .site = {.file = "lib/use.cpp", .caller = "use.useHidden", .needle = "Error::hidden()"},
         .relation = "CALLS",
         .any_target = true},
    };
    const CTUCase fixture = {.suite = "c-tu-scoped",
                             .sources = C_TU_SCOPED,
                             .source_count = sizeof(C_TU_SCOPED) / sizeof(C_TU_SCOPED[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0]),
                             .forbidden = forbidden,
                             .forbidden_count = sizeof(forbidden) / sizeof(forbidden[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* Inside a member function the class's own names come first, also the ones with no graph
 * node to bind: a pure virtual hides a free function of that name. */
#define C_TU_HIDING_HEADER                                                                      \
    "#ifndef PRINTER_H\n"                                                                       \
    "#define PRINTER_H\n"                                                                       \
    "int print(int x);\n"                                                                       \
    "int render(int x);\n"                                                                      \
    "int hidden(int x);\n"                                                                      \
    "class Base {\n"                                                                            \
    "public:\n"                                                                                 \
    "  int render(int x) { return x; }\n"                                                       \
    "};\n"                                                                                      \
    "class Printer {\n"                                                                         \
    "public:\n"                                                                                 \
    "  virtual int print(int x) = 0;\n"                                                         \
    "  int run() { return print(1); }\n"                                                        \
    "  int free_ok() { return render(1); }\n"                                                   \
    "};\n"                                                                                      \
    "class Derived : public Base {\n"                                                           \
    "public:\n"                                                                                 \
    "  int go() { return render(4); }\n"                                                        \
    "};\n"                                                                                      \
    "class FromUnknown : public Unknown {\n"                                                    \
    "public:\n"                                                                                 \
    "  int go2() { return render(2); }\n"                                                       \
    "};\n"                                                                                      \
    "class WithType {\n"                                                                        \
    "  using hidden = int;\n"                                                                   \
    "public:\n"                                                                                 \
    "  int go3() { return hidden(3); }\n"                                                       \
    "};\n"                                                                                      \
    "#endif\n"
#define C_TU_HIDING_IMPL                                                                        \
    "#include \"printer.h\"\n"                                                                  \
    "int print(int x) { return x; }\n"                                                          \
    "int render(int x) { return x; }\n"                                                         \
    "int hidden(int x) { return x; }\n"
static const RQ_Source C_TU_HIDING[] = {{.path = "printer.h", .text = C_TU_HIDING_HEADER},
                                        {.path = "printer_impl.cpp", .text = C_TU_HIDING_IMPL}};

TEST(resolver_cpp_class_names_hide_the_enclosing_scope) {
    const RQ_Expected expected[] = {
        {.case_id = "name-the-class-does-not-declare-is-the-free-function",
         .site = {.file = "printer.h", .caller = "printer.Printer.free_ok", .needle = "render(1)"},
         .relation = "CALLS",
         .target = {.file = "printer_impl.cpp",
                    .suffix = "printer_impl.render",
                    .label = "Function",
                    .declaration_line = 3,
                    .declaration_text = "int render(int x) { return x; }"}},
        {.case_id = "name-a-known-base-declares",
         .site = {.file = "printer.h", .caller = "printer.Derived.go", .needle = "render(4)"},
         .relation = "CALLS",
         .target = {.file = "printer.h",
                    .suffix = "printer.Base.render",
                    .label = "Method",
                    .declaration_line = 8,
                    .declaration_text = "  int render(int x) { return x; }"}},
    };
    const RQ_Forbidden forbidden[] = {
        {.case_id = "pure-virtual-hides-the-free-function",
         .site = {.file = "printer.h", .caller = "printer.Printer.run", .needle = "print(1)"},
         .relation = "CALLS",
         .any_target = true},
        {.case_id = "unknown-base-may-declare-the-name",
         .site = {.file = "printer.h", .caller = "printer.FromUnknown.go2", .needle = "render(2)"},
         .relation = "CALLS",
         .any_target = true},
        {.case_id = "nested-type-hides-the-free-function",
         .site = {.file = "printer.h", .caller = "printer.WithType.go3", .needle = "hidden(3)"},
         .relation = "CALLS",
         .any_target = true},
    };
    const CTUCase fixture = {.suite = "c-tu-hiding",
                             .sources = C_TU_HIDING,
                             .source_count = sizeof(C_TU_HIDING) / sizeof(C_TU_HIDING[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0]),
                             .forbidden = forbidden,
                             .forbidden_count = sizeof(forbidden) / sizeof(forbidden[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* neovim declares every global as `EXTERN win_T *curwin INIT( = NULL);`. */
#define C_TU_GLOBALS_HEADER                                                                     \
    "#ifndef GLOBALS_H\n"                                                                       \
    "#define GLOBALS_H\n"                                                                       \
    "typedef struct window_S win_T;\n"                                                          \
    "struct window_S {\n"                                                                       \
    "  int w_cursor;\n"                                                                         \
    "};\n"                                                                                      \
    "EXTERN win_T *curwin INIT( = NULL);\n"                                                     \
    "EXTERN int Rows INIT( = 0);\n"                                                             \
    "LUA_API int (lua_gettop) (int *L);\n"                                                      \
    "#endif\n"
#define C_TU_GLOBALS_USE                                                                        \
    "#include \"globals.h\"\n"                                                                  \
    "int read_rows(void) { return Rows; }\n"                                                    \
    "void set_cursor(int v) { curwin->w_cursor = v; }\n"                                        \
    "int shadowed(int Rows) { return Rows; }\n"                                                 \
    "int same(int a, int b) { return (a == b); }\n"                                             \
    "int top(int *L) { return lua_gettop(L); }\n"
static const RQ_Source C_TU_GLOBALS[] = {
    {.path = "globals.h", .text = C_TU_GLOBALS_HEADER},
    {.path = "lapi.c", .text = "#include \"globals.h\"\nLUA_API int lua_gettop (int *L) { return *L; }\n"},
    {.path = "use_globals.c", .text = C_TU_GLOBALS_USE}};

TEST(resolver_c_initializer_macro_declares_the_global) {
    const char *use = C_TU_GLOBALS_USE;
    const RQ_Expected expected[] = {
        {.case_id = "prototype-with-a-parenthesized-name",
         .site = {.file = "use_globals.c", .caller = "use_globals.top", .needle = "lua_gettop(L)"},
         .relation = "CALLS",
         .target = {.file = "lapi.c",
                    .suffix = "lapi.lua_gettop",
                    .label = "Function",
                    .declaration_line = 2,
                    .declaration_text = "LUA_API int lua_gettop (int *L) { return *L; }"}},
        {.case_id = "global-with-initializer-macro",
         .site = C_TU_WORD("use_globals.c", use, "use_globals.read_rows", "return Rows", "Rows"),
         .relation = "USAGE",
         .target = {.file = "globals.h",
                    .suffix = "globals.Rows",
                    .label = "Variable",
                    .declaration_line = 8,
                    .declaration_text = "EXTERN int Rows INIT( = 0);"}},
        {.case_id = "pointer-global-with-initializer-macro",
         .site = C_TU_WORD("use_globals.c", use, "use_globals.set_cursor", "curwin->", "curwin"),
         .relation = "USAGE",
         .target = {.file = "globals.h",
                    .suffix = "globals.curwin",
                    .label = "Variable",
                    .declaration_line = 7,
                    .declaration_text = "EXTERN win_T *curwin INIT( = NULL);"}},
        {.case_id = "member-through-the-global",
         .site = C_TU_WORD("use_globals.c", use, "use_globals.set_cursor", "curwin->w_cursor",
                           "w_cursor"),
         .relation = "WRITES",
         .target = {.file = "globals.h",
                    .suffix = "globals.window_S.w_cursor",
                    .label = "Field",
                    .declaration_line = 5,
                    .declaration_text = "  int w_cursor;"}},
    };
    const RQ_Forbidden forbidden[] = {
        {.case_id = "parameter-hides-the-global",
         .site = C_TU_WORD("use_globals.c", use, "use_globals.shadowed", "return Rows", "Rows"),
         .relation = "USAGE",
         .any_target = true},
    };
    const CTUCase fixture = {.suite = "c-tu-globals",
                             .sources = C_TU_GLOBALS,
                             .source_count = sizeof(C_TU_GLOBALS) / sizeof(C_TU_GLOBALS[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0]),
                             .forbidden = forbidden,
                             .forbidden_count = sizeof(forbidden) / sizeof(forbidden[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* A file's proofs all pass through one copy on their way to the graph. A cap there cost a
 * large file its last rows, so the call that matters here is the last one in the file. */
TEST(resolver_c_large_file_keeps_its_last_call) {
    enum { CALLS = 30000 };
    const char *head = "int helper(int x) { return x; }\n"
                       "int last_helper(int x) { return x; }\n"
                       "int many(void) {\n"
                       "  int n = 0;\n";
    const char *line = "  n += helper(0);\n";
    const char *tail = "  return n;\n"
                       "}\n"
                       "int last_one(void) { return last_helper(1); }\n";
    size_t size = strlen(head) + (size_t)CALLS * strlen(line) + strlen(tail) + 1;
    char *text = malloc(size);
    ASSERT(text != NULL);
    size_t used = 0;
    memcpy(text, head, strlen(head));
    used += strlen(head);
    for (int i = 0; i < CALLS; i++) {
        memcpy(text + used, line, strlen(line));
        used += strlen(line);
    }
    memcpy(text + used, tail, strlen(tail) + 1);
    const RQ_Source sources[] = {{.path = "big.c", .text = text}};
    const RQ_Expected expected[] = {
        {.case_id = "last-call-of-a-large-file",
         .site = {.file = "big.c", .caller = "big.last_one", .needle = "last_helper(1)"},
         .relation = "CALLS",
         .target = {.file = "big.c",
                    .suffix = "big.last_helper",
                    .label = "Function",
                    .declaration_line = 2,
                    .declaration_text = "int last_helper(int x) { return x; }"}},
    };
    const CTUCase fixture = {.suite = "c-tu-large",
                             .sources = sources,
                             .source_count = 1,
                             .expected = expected,
                             .expected_count = 1};
    bool ok = true;
    for (int driver = 0; driver < 2; driver++)
        ok = c_tu_run(&fixture, driver) && ok;
    free(text);
    ASSERT(ok);
    PASS();
}

/* What the C++ rules leave unbound on LLVM. Each is a genuine call the resolver abstains on. */
#define C_TU_CXX_GAPS_HEADER                                                                    \
    "#ifndef CXX_GAPS_H\n"                                                                      \
    "#define CXX_GAPS_H\n"                                                                      \
    "namespace llvm {\n"                                                                        \
    "template <typename T> class ArrayRef {\n"                                                  \
    "public:\n"                                                                                 \
    "  int size() const { return 1; }\n"                                                        \
    "};\n"                                                                                      \
    "class Value {\n"                                                                           \
    "public:\n"                                                                                 \
    "  int *parts();\n"                                                                         \
    "  const int *parts() const;\n"                                                             \
    "  int length() const { return 1; }\n"                                                      \
    "};\n"                                                                                      \
    "template <typename T> bool usesLayout(int x) { return x > 0; }\n"                          \
    "Value makeValue();\n"                                                                      \
    "}\n"                                                                                       \
    "#endif\n"
#define C_TU_CXX_GAPS_IMPL                                                                      \
    "#include \"cxx_gaps.h\"\n"                                                                 \
    "namespace llvm {\n"                                                                        \
    "int *Value::parts() { return 0; }\n"                                                       \
    "const int *Value::parts() const { return 0; }\n"                                           \
    "}\n"
#define C_TU_CXX_GAPS_USE                                                                       \
    "#include \"cxx_gaps.h\"\n"                                                                 \
    "namespace llvm {\n"                                                                        \
    "int useTemplate(ArrayRef<int> A) { return A.size(); }\n"                                   \
    "int useAuto() { auto V = makeValue(); return V.length(); }\n"                              \
    "int useConstOverload(Value V) { return *V.parts(); }\n"                                    \
    "bool useFunctionTemplate() { return usesLayout<int>(1); }\n"                               \
    "}\n"
static const RQ_Source C_TU_CXX_GAPS[] = {
    {.path = "cxx_gaps.h", .text = C_TU_CXX_GAPS_HEADER},
    {.path = "cxx_gaps.cpp", .text = C_TU_CXX_GAPS_IMPL},
    {.path = "cxx_gaps_use.cpp", .text = C_TU_CXX_GAPS_USE}};

TEST(resolver_cpp_known_gaps_stay_honest) {
    const RQ_Expected expected[] = {
        {.case_id = "member-of-a-class-template",
         .site = {.file = "cxx_gaps_use.cpp", .caller = "llvm.useTemplate", .needle = "A.size()"},
         .relation = "CALLS",
         .target = {.file = "cxx_gaps.h",
                    .suffix = "llvm.ArrayRef.size",
                    .label = "Method",
                    .declaration_line = 6,
                    .declaration_text = "  int size() const { return 1; }"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "cpp-class-template-member",
         .why = "A specialization can replace the member, and the graph has one node per template "
                "name, so a class template is not taken as a receiver's class.",
         .tried = "Read SmallVector, ArrayRef and Optional in LLVM. SmallVectorTemplateBase has two "
                  "specializations; a rule for templates needs specialization facts first."},
        {.case_id = "receiver-declared-auto",
         .site = {.file = "cxx_gaps_use.cpp", .caller = "llvm.useAuto", .needle = "V.length()"},
         .relation = "CALLS",
         .target = {.file = "cxx_gaps.h",
                    .suffix = "llvm.Value.length",
                    .label = "Method",
                    .declaration_line = 12,
                    .declaration_text = "  int length() const { return 1; }"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "cpp-auto-receiver",
         .why = "An `auto` variable's type is inferred from its initializer. Only written "
                "declarations give a receiver its type here.",
         .tried = "Written declaration types are followed through typedefs, fields and declared "
                  "return types. Initializer flow is left out on purpose: it can go stale."},
        /* Not a gap: the two definitions are one graph node, so which overload the call
         * selects does not change the edge. */
        {.case_id = "overloads-sharing-one-node",
         .site = {.file = "cxx_gaps_use.cpp", .caller = "llvm.useConstOverload", .needle = "V.parts()"},
         .relation = "CALLS",
         .target = {.file = "cxx_gaps.cpp",
                    .suffix = "cxx_gaps.Value.parts",
                    .label = "Method",
                    .declaration_line = 4,
                    .declaration_text = "const int *Value::parts() const { return 0; }"}},
        {.case_id = "explicit-template-arguments",
         .site = {.file = "cxx_gaps_use.cpp",
                  .caller = "llvm.useFunctionTemplate",
                  .needle = "usesLayout<int>(1)"},
         .relation = "CALLS",
         .target = {.file = "cxx_gaps.h",
                    .suffix = "llvm.usesLayout",
                    .label = "Function",
                    .declaration_line = 14,
                    .declaration_text =
                        "template <typename T> bool usesLayout(int x) { return x > 0; }"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "cpp-function-template",
         .why = "Function templates take part in overload resolution by deduction, which is not "
                "modelled.",
         .tried = "Plain functions and members bind. A template function is skipped as a candidate "
                  "so it never wins by being the only name visible."},
    };
    const CTUCase fixture = {.suite = "c-tu-cxx-gaps",
                             .sources = C_TU_CXX_GAPS,
                             .source_count = sizeof(C_TU_CXX_GAPS) / sizeof(C_TU_CXX_GAPS[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0]),
                             .known_gaps = sizeof(expected) / sizeof(expected[0]) - 1};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* A precision sample on neovim found `HAS_KEY(opts, win_text_height, start_row)` bound as a
 * reference to the function win_text_height. The macro pastes that word into another name. */
#define C_TU_MACRO_ARGS_HEADER                                                                  \
    "#ifndef KEYS_H\n"                                                                          \
    "#define KEYS_H\n"                                                                          \
    "#define HAS_KEY(d, typ, key) (KEYSET_OPTIDX_##typ##__##key & (d)->is_set__##typ##_)\n"     \
    "int win_text_height(int x);\n"                                                             \
    "extern int start_row;\n"                                                                   \
    "#endif\n"
#define C_TU_MACRO_ARGS_IMPL                                                                    \
    "#include \"keys.h\"\n"                                                                     \
    "int win_text_height(int x) { return x; }\n"                                                \
    "int start_row;\n"
#define C_TU_MACRO_ARGS_USE                                                                     \
    "#include \"keys.h\"\n"                                                                     \
    "int take_callback(int (*cb)(int));\n"                                                      \
    "int pasted(int *a) { return HAS_KEY(a, win_text_height, start_row); }\n"                   \
    "int real_reference(void) { return take_callback(win_text_height); }\n"                     \
    "int real_value(void) { return start_row; }\n"
static const RQ_Source C_TU_MACRO_ARGS[] = {
    {.path = "keys.h", .text = C_TU_MACRO_ARGS_HEADER},
    {.path = "keys.c", .text = C_TU_MACRO_ARGS_IMPL},
    {.path = "use_keys.c", .text = C_TU_MACRO_ARGS_USE}};

TEST(resolver_c_bare_macro_arguments_prove_nothing) {
    const char *use = C_TU_MACRO_ARGS_USE;
    const RQ_Expected expected[] = {
        {.case_id = "function-argument-of-a-function",
         .site = C_TU_WORD("use_keys.c", use, "use_keys.real_reference", "take_callback(win",
                           "win_text_height"),
         .relation = "CALL_REFERENCE",
         .target = {.file = "keys.c",
                    .suffix = "keys.win_text_height",
                    .label = "Function",
                    .declaration_line = 2,
                    .declaration_text = "int win_text_height(int x) { return x; }"}},
        {.case_id = "value-outside-the-macro",
         .site = C_TU_WORD("use_keys.c", use, "use_keys.real_value", "return start_row", "start_row"),
         .relation = "USAGE",
         .target = {.file = "keys.c",
                    .suffix = "keys.start_row",
                    .label = "Variable",
                    .declaration_line = 3,
                    .declaration_text = "int start_row;"}},
    };
    const RQ_Forbidden forbidden[] = {
        {.case_id = "pasted-function-name",
         .site = C_TU_WORD("use_keys.c", use, "use_keys.pasted", "HAS_KEY(a, ", "win_text_height"),
         .relation = "CALL_REFERENCE",
         .any_target = true},
        {.case_id = "pasted-value-name",
         .site = C_TU_WORD("use_keys.c", use, "use_keys.pasted", "HAS_KEY(a, ", "start_row"),
         .relation = "USAGE",
         .any_target = true},
    };
    const CTUCase fixture = {.suite = "c-tu-macro-args",
                             .sources = C_TU_MACRO_ARGS,
                             .source_count = sizeof(C_TU_MACRO_ARGS) / sizeof(C_TU_MACRO_ARGS[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0]),
                             .forbidden = forbidden,
                             .forbidden_count = sizeof(forbidden) / sizeof(forbidden[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* Two forms from LLVM's APFloat and attribute parsers. `typedef detail::IEEEFloat IEEEFloat;`
 * inside another class must not compete with the class of that name outside it, and a derived
 * class reaches its base's protected members but not its private ones. */
#define C_TU_ACCESS_HEADER                                                                      \
    "#ifndef ACCESS_H\n"                                                                        \
    "#define ACCESS_H\n"                                                                        \
    "namespace llvm {\n"                                                                        \
    "namespace detail {\n"                                                                      \
    "class Inner {\n"                                                                           \
    "public:\n"                                                                                 \
    "  int outer();\n"                                                                          \
    "private:\n"                                                                                \
    "  int inner() const { return 1; }\n"                                                       \
    "};\n"                                                                                      \
    "}\n"                                                                                       \
    "class Wrapper {\n"                                                                         \
    "  typedef detail::Inner Inner;\n"                                                          \
    "};\n"                                                                                      \
    "class Parser {\n"                                                                          \
    "protected:\n"                                                                              \
    "  int parse() { return 1; }\n"                                                             \
    "private:\n"                                                                                \
    "  int secret() { return 2; }\n"                                                            \
    "};\n"                                                                                      \
    "class ArmParser : public Parser {\n"                                                       \
    "public:\n"                                                                                 \
    "  int run() { return parse(); }\n"                                                         \
    "  int peek() { return secret(); }\n"                                                       \
    "};\n"                                                                                      \
    "int outside(ArmParser P);\n"                                                               \
    "}\n"                                                                                       \
    "#endif\n"
#define C_TU_ACCESS_IMPL                                                                        \
    "#include \"access.h\"\n"                                                                   \
    "namespace llvm {\n"                                                                        \
    "namespace detail {\n"                                                                      \
    "int Inner::outer() { return inner(); }\n"                                                  \
    "}\n"                                                                                       \
    "int outside(ArmParser P) { return P.parse(); }\n"                                          \
    "}\n"
static const RQ_Source C_TU_ACCESS[] = {{.path = "access.h", .text = C_TU_ACCESS_HEADER},
                                        {.path = "access.cpp", .text = C_TU_ACCESS_IMPL}};

TEST(resolver_cpp_nested_aliases_and_protected_members) {
    const RQ_Expected expected[] = {
        {.case_id = "alias-inside-another-class-does-not-compete",
         .site = {.file = "access.cpp", .caller = "access.Inner.outer", .needle = "inner()"},
         .relation = "CALLS",
         .target = {.file = "access.h",
                    .suffix = "access.llvm.detail.Inner.inner",
                    .label = "Method",
                    .declaration_line = 9,
                    .declaration_text = "  int inner() const { return 1; }"}},
        {.case_id = "protected-member-from-a-derived-class",
         .site = {.file = "access.h", .caller = "access.llvm.ArmParser.run", .needle = "parse()"},
         .relation = "CALLS",
         .target = {.file = "access.h",
                    .suffix = "access.llvm.Parser.parse",
                    .label = "Method",
                    .declaration_line = 17,
                    .declaration_text = "  int parse() { return 1; }"}},
    };
    const RQ_Forbidden forbidden[] = {
        {.case_id = "private-member-of-a-base",
         .site = {.file = "access.h", .caller = "access.llvm.ArmParser.peek", .needle = "secret()"},
         .relation = "CALLS",
         .any_target = true},
        {.case_id = "protected-member-from-outside",
         .site = {.file = "access.cpp", .caller = "llvm.outside", .needle = "P.parse()"},
         .relation = "CALLS",
         .any_target = true},
    };
    const CTUCase fixture = {.suite = "c-tu-access",
                             .sources = C_TU_ACCESS,
                             .source_count = sizeof(C_TU_ACCESS) / sizeof(C_TU_ACCESS[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0]),
                             .forbidden = forbidden,
                             .forbidden_count = sizeof(forbidden) / sizeof(forbidden[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* neovim's typval_T: `rettv->vval.v_number` goes through a union defined where its member
 * is declared. Two such unions have a member of one name, so only the tag can tell them apart. */
#define C_TU_NESTED_HEADER                                                                      \
    "#ifndef TYPVAL_H\n"                                                                        \
    "#define TYPVAL_H\n"                                                                        \
    "typedef struct {\n"                                                                        \
    "  int v_type;\n"                                                                           \
    "  union number_union {\n"                                                                  \
    "    long value;\n"                                                                         \
    "  } vval;\n"                                                                               \
    "  union string_union {\n"                                                                  \
    "    char *value;\n"                                                                        \
    "  } sval;\n"                                                                               \
    "} typval_T;\n"                                                                             \
    "#endif\n"
#define C_TU_NESTED_USE                                                                         \
    "#include \"typval.h\"\n"                                                                   \
    "void set_number(typval_T *rettv) { rettv->vval.value = 0; }\n"                             \
    "char *get_string(typval_T tv) { return tv.sval.value; }\n"
static const RQ_Source C_TU_NESTED[] = {{.path = "typval.h", .text = C_TU_NESTED_HEADER},
                                        {.path = "use_typval.c", .text = C_TU_NESTED_USE}};

TEST(resolver_c_member_through_a_union_defined_in_place) {
    const char *use = C_TU_NESTED_USE;
    const RQ_Expected expected[] = {
        {.case_id = "write-through-a-tagged-union",
         .site = C_TU_WORD("use_typval.c", use, "use_typval.set_number", "vval.value", "value"),
         .relation = "WRITES",
         .target = {.file = "typval.h",
                    .suffix = "typval.number_union.value",
                    .label = "Field",
                    .declaration_line = 6,
                    .declaration_text = "    long value;"}},
        {.case_id = "read-through-the-other-tagged-union",
         .site = C_TU_WORD("use_typval.c", use, "use_typval.get_string", "sval.value", "value"),
         .relation = "USAGE",
         .target = {.file = "typval.h",
                    .suffix = "typval.string_union.value",
                    .label = "Field",
                    .declaration_line = 9,
                    .declaration_text = "    char *value;"}},
        {.case_id = "the-member-holding-the-union",
         .site = C_TU_WORD("use_typval.c", use, "use_typval.get_string", "tv.sval", "sval"),
         .relation = "USAGE",
         .target = {.file = "typval.h",
                    .suffix = "typval.typval_T.sval",
                    .label = "Field",
                    .declaration_line = 8,
                    .declaration_text = "  union string_union {"}},
        {.case_id = "the-parameter-type",
         .site = C_TU_WORD("use_typval.c", use, "use_typval.get_string", "get_string(typval_T", "typval_T"),
         .relation = "USAGE",
         .target = {.file = "typval.h",
                    .suffix = "typval.typval_T",
                    .label = "Class",
                    .declaration_line = 3,
                    .declaration_text = "typedef struct {"}},
    };
    const CTUCase fixture = {.suite = "c-tu-nested",
                             .sources = C_TU_NESTED,
                             .source_count = sizeof(C_TU_NESTED) / sizeof(C_TU_NESTED[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

SUITE(resolver_c) {
    RUN_TEST(resolver_c_member_through_a_union_defined_in_place);
    RUN_TEST(resolver_cpp_nested_aliases_and_protected_members);
    RUN_TEST(resolver_c_bare_macro_arguments_prove_nothing);
    RUN_TEST(resolver_cpp_known_gaps_stay_honest);
    RUN_TEST(resolver_c_large_file_keeps_its_last_call);
    RUN_TEST(resolver_c_initializer_macro_declares_the_global);
    RUN_TEST(resolver_cpp_class_names_hide_the_enclosing_scope);
    RUN_TEST(resolver_cpp_reference_prototypes_and_scoped_members);
    RUN_TEST(resolver_c_attribute_macros_do_not_hide_declarations);
    RUN_TEST(resolver_c_known_gaps_stay_honest);
    RUN_TEST(resolver_cpp_members_and_namespaces_bind_through_the_translation_unit);
    RUN_TEST(resolver_c_header_prototype_binds_external_definition_through_includes);
    RUN_TEST(resolver_c_lookalike_calls_abstain);
    RUN_TEST(resolver_c_copied_library_binds_the_copy_compiled_against_its_header);
    RUN_TEST(resolver_c_configured_include_root_reaches_calls);
    RUN_TEST(resolver_c_unique_include_spelling_reaches_calls_without_configuration);
    RUN_TEST(resolver_c_generated_declaration_header_exposes_its_source);
    RUN_TEST(resolver_c_member_access_follows_written_declarations);
    RUN_TEST(resolver_c_bare_values_and_types_bind_in_the_translation_unit);
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
