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
#include "lsp/lsp_work.h"

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
    for (int i = 2; i < 6; i++)
        ASSERT_EQ(exact[i], 0);
    ASSERT_EQ(exact[1], 3);
    ASSERT_EQ(exact[6], 3);
    /* Every real C header is guarded, and every one of these three references
     * targets Alpha's fields. The guard's one way to hide them, the caller defining
     * it first, is case 5 above. An ordinary feature #ifdef around the struct, case
     * 6, is the only definition the code can be reading; the file that defines one
     * twice stays unknown, in resolver_c_struct_shapes_real_headers_use. */
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
/* The same header ahead of the definition can do nothing to it, unless the definition is
 * conditional: then the header may be what decides the branch. */
static const char C_PP_PERSIST_AFTER[] =
    "#include \"missing.h\"\n#define CALL() after_target()\n"
    "void after_target(void) {}\nvoid after_run(void) { CALL(); }\n";
static const char C_PP_PERSIST_GUARDED[] =
    "#include \"missing.h\"\n#ifndef CALL\n#define CALL() guarded_target()\n#endif\n"
    "void guarded_target(void) {}\nvoid guarded_run(void) { CALL(); }\n";

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
                                 {.path = "after.c", .text = C_PP_PERSIST_AFTER},
                                 {.path = "guarded.c", .text = C_PP_PERSIST_GUARDED},
                                 {.path = "raw.c", .text = C_PP_RAW}};
    const RQ_Expected expected[] = {
        {.case_id = "raw-direct-positive",
         .site = {.file = "raw.c", .caller = "raw.raw_run", .needle = "raw_target()"},
         .relation = "CALLS",
         .target = {.file = "raw.c",
                    .suffix = "raw.raw_target",
                    .label = "Function",
                    .declaration_line = 1,
                    .declaration_text = "void raw_target(void) {}"}},
        {.case_id = "raw-define-after-a-missing-header",
         .site = {.file = "after.c",
                  .caller = "after.after_run",
                  .needle = "CALL()",
                  .occurrence_index = 1},
         .relation = "CALLS",
         .target = {.file = "after.c",
                    .suffix = "after.after_target",
                    .label = "Function",
                    .declaration_line = 3,
                    .declaration_text = "void after_target(void) {}"}}};
    const RQ_Forbidden forbidden[] = {
        {.case_id = "guessed-config-negative",
         .site = {.file = "guessed.c",
                  .caller = "guessed.guessed_run",
                  .needle = "CALL()",
                  .occurrence_index = 2},
         .relation = "CALLS",
         .any_target = true},
        /* `missing.h` stands between the definition and the use, and may redefine CALL. */
        {.case_id = "missing-header-negative",
         .site = {.file = "missing.c",
                  .caller = "missing.missing_run",
                  .needle = "CALL()",
                  .occurrence_index = 1},
         .relation = "CALLS",
         .any_target = true},
        {.case_id = "a-missing-header-may-decide-the-branch",
         .site = {.file = "guarded.c",
                  .caller = "guarded.guarded_run",
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
        ASSERT_EQ(report.genuine, 2);
        ASSERT_EQ(report.found, 2);
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
        {.case_id = "increment-parameter-type",
         .site = C_TU_WORD("src/use.c", use, "use.bump_flags", "bump_flags(client", "client"),
         .relation = "USAGE",
         .target = client_type},
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
        /* Was a known gap until the macro's replacement list was read: it writes the type. */
        {.case_id = "macro-declared-loop-variable",
         .site = C_TU_WORD("use_gaps.c", use, "use_gaps.use_loop", "n += tp->", "tp_next"),
         .relation = "USAGE",
         .target = {.file = "gaps.h",
                    .suffix = "gaps.tabpage_S.tp_next",
                    .label = "Field",
                    .declaration_line = 4,
                    .declaration_text = "  struct tabpage_S *tp_next;"}},
        {.case_id = "the-tag-in-the-signature",
         .site = C_TU_WORD("use_gaps.c", use, "use_gaps.use_union", "struct item *o", "item"),
         .relation = "USAGE",
         .target = {.file = "gaps.h",
                    .suffix = "gaps.item",
                    .label = "Class",
                    .declaration_line = 10,
                    .declaration_text = "struct item {"}},
        {.case_id = "anonymous-aggregate-member",
         .site = C_TU_WORD("use_gaps.c", use, "use_gaps.use_union", "o->value", "value"),
         .relation = "USAGE",
         .target = {.file = "gaps.h",
                    .suffix = "gaps.item.value",
                    .label = "Field",
                    .declaration_line = 12,
                    .declaration_text = "    int value;"}},
        /* Was a known gap until the replacement list was read: the function it reaches is what
         * compiles here, so the edge goes there and not to the Macro node. */
        {.case_id = "function-like-macro",
         .site = {.file = "use_gaps.c", .caller = "use_gaps.use_macro", .needle = "LOG_LINE(\"x\")"},
         .relation = "CALLS",
         .target = {.file = "gaps.c",
                    .suffix = "gaps.log_line",
                    .label = "Function",
                    .declaration_line = 3,
                    .declaration_text = "void log_line(const char *s) { (void)s; }"}},
    };
    const CTUCase fixture = {.suite = "c-tu-gaps",
                             .sources = C_TU_GAPS,
                             .source_count = sizeof(C_TU_GAPS) / sizeof(C_TU_GAPS[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0])};
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
    "#pragma once\n"                                                                            \
    "// No macro here: the rows below all come from the file as written.\n"                     \
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
    "};\n"
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
    "#pragma once\n"                                                                            \
    "// The way neovim's globals.h is written.\n"                                               \
    "typedef struct window_S win_T;\n"                                                          \
    "struct window_S {\n"                                                                       \
    "  int w_cursor;\n"                                                                         \
    "};\n"                                                                                      \
    "EXTERN win_T *curwin INIT( = NULL);\n"                                                     \
    "EXTERN int Rows INIT( = 0);\n"                                                             \
    "LUA_API int (lua_gettop) (int *L);\n"
#define C_TU_GLOBALS_USE                                                                        \
    "#include \"globals.h\"\n"                                                                  \
    "int read_rows(void) { return Rows; }\n"                                                    \
    "void set_cursor(int v) { curwin->w_cursor = v; }\n"                                        \
    "int read_cursor(void) { return curwin->w_cursor; }\n"                                      \
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
         .site = C_TU_WORD("use_globals.c", use, "use_globals.read_cursor", "return curwin", "curwin"),
         .relation = "USAGE",
         .target = {.file = "globals.h",
                    .suffix = "globals.curwin",
                    .label = "Variable",
                    .declaration_line = 7,
                    .declaration_text = "EXTERN win_T *curwin INIT( = NULL);"}},
        {.case_id = "member-read-through-the-global",
         .site = C_TU_WORD("use_globals.c", use, "use_globals.read_cursor", "return curwin->w_cursor",
                           "w_cursor"),
         .relation = "USAGE",
         .target = {.file = "globals.h",
                    .suffix = "globals.window_S.w_cursor",
                    .label = "Field",
                    .declaration_line = 5,
                    .declaration_text = "  int w_cursor;"}},
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

/* Forms that used to abstain on LLVM. A class template's primary is the receiver's class when no
 * specialization exists, an `auto` has its initializer's written type, and every overload of one
 * name in one file is one node. */
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
    "Value makeValue() { return Value(); }\n"                                                   \
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
                    .declaration_text = "  int size() const { return 1; }"}},
        {.case_id = "initializer-of-the-auto-receiver",
         .site = {.file = "cxx_gaps_use.cpp", .caller = "llvm.useAuto", .needle = "makeValue()"},
         .relation = "CALLS",
         .target = {.file = "cxx_gaps.cpp",
                    .suffix = "llvm.makeValue",
                    .label = "Function",
                    .declaration_line = 5,
                    .declaration_text = "Value makeValue() { return Value(); }"}},
        {.case_id = "receiver-declared-auto",
         .site = {.file = "cxx_gaps_use.cpp", .caller = "llvm.useAuto", .needle = "V.length()"},
         .relation = "CALLS",
         .target = {.file = "cxx_gaps.h",
                    .suffix = "llvm.Value.length",
                    .label = "Method",
                    .declaration_line = 12,
                    .declaration_text = "  int length() const { return 1; }"}},
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
                        "template <typename T> bool usesLayout(int x) { return x > 0; }"}},
    };
    const CTUCase fixture = {.suite = "c-tu-cxx-gaps",
                             .sources = C_TU_CXX_GAPS,
                             .source_count = sizeof(C_TU_CXX_GAPS) / sizeof(C_TU_CXX_GAPS[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0])};
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

/* The replacement list says what happens to an argument. redis's `atomicIncr(counter, 1)` reads
 * the object; the same bare name pasted, stringified or read as a member names nothing. */
#define C_TU_MACRO_READ_HEADER                                                                  \
    "#ifndef ATOMICS_H\n"                                                                       \
    "#define ATOMICS_H\n"                                                                       \
    "#define atomicIncr(var,count) fetch_add(&var,(count),relaxed)\n"                           \
    "#define PASTED(name) global_##name\n"                                                      \
    "#define MEMBER(s, field) ((s)->field)\n"                                                   \
    "#define NAMED(x) #x\n"                                                                     \
    "#define THROUGH_PASTE(x) PASTED(x)\n"                                                      \
    "#define THROUGH_READ(x) atomicIncr(x, 1)\n"                                                \
    "#define CAPTURING(x) do { int tmp = (x); use_int(tmp); } while (0)\n"                      \
    "#define SWAPPED(x) ((void)(x))\n"                                                          \
    "void use_int(int v);\n"                                                                    \
    "#endif\n"
#define C_TU_MACRO_READ_ONE                                                                     \
    "#ifndef SWAP_A_H\n"                                                                        \
    "#define SWAP_A_H\n"                                                                        \
    "#ifdef LOUD\n"                                                                             \
    "#define SWAPPED_TWICE(x) log_it(#x)\n"                                                     \
    "#else\n"                                                                                   \
    "#define SWAPPED_TWICE(x) ((void)(x))\n"                                                    \
    "#endif\n"                                                                                  \
    "#endif\n"
#define C_TU_MACRO_READ_USE                                                                     \
    "#include \"atomics.h\"\n"                                                                  \
    "#include \"swap_a.h\"\n"                                                                   \
    "static int counter;\n"                                                                     \
    "static int tmp;\n"                                                                         \
    "static int global_counter;\n"                                                              \
    "static void reads(void) { atomicIncr(counter, 1); }\n"                                     \
    "static void nested(void) { THROUGH_READ(counter); }\n"                                     \
    "static void captured_other(void) { CAPTURING(counter); }\n"                                \
    "static void one_branch(void) { SWAPPED(counter); }\n"                                      \
    "static void pasted(void) { PASTED(counter); }\n"                                           \
    "static void member(int *p) { MEMBER(p, counter); }\n"                                      \
    "static void named(void) { NAMED(counter); }\n"                                             \
    "static void through(void) { THROUGH_PASTE(counter); }\n"                                   \
    "static void captured(void) { CAPTURING(tmp); }\n"                                          \
    "static void swapped_twice(void) { SWAPPED_TWICE(counter); }\n"
static const RQ_Source C_TU_MACRO_READ[] = {{.path = "atomics.h", .text = C_TU_MACRO_READ_HEADER},
                                            {.path = "swap_a.h", .text = C_TU_MACRO_READ_ONE},
                                            {.path = "use_read.c", .text = C_TU_MACRO_READ_USE}};

TEST(resolver_c_macro_argument_binds_when_the_replacement_substitutes_it) {
    const char *use = C_TU_MACRO_READ_USE;
    const RQ_Target counter = {.file = "use_read.c",
                               .suffix = "use_read.counter",
                               .label = "Variable",
                               .declaration_line = 3,
                               .declaration_text = "static int counter;"};
    const RQ_Expected expected[] = {
        {.case_id = "read-through-the-macro",
         .site = C_TU_WORD("use_read.c", use, "use_read.reads", "atomicIncr(", "counter"),
         .relation = "USAGE",
         .target = counter},
        {.case_id = "read-through-a-macro-handing-it-on",
         .site = C_TU_WORD("use_read.c", use, "use_read.nested", "THROUGH_READ(", "counter"),
         .relation = "USAGE",
         .target = counter},
        {.case_id = "other-name-than-what-the-macro-captures",
         .site = C_TU_WORD("use_read.c", use, "use_read.captured_other", "CAPTURING(", "counter"),
         .relation = "USAGE",
         .target = counter},
        {.case_id = "plain-in-the-only-definition",
         .site = C_TU_WORD("use_read.c", use, "use_read.one_branch", "SWAPPED(", "counter"),
         .relation = "USAGE",
         .target = counter},
        {.case_id = "the-name-the-paste-makes",
         .site = {.file = "use_read.c", .caller = "use_read.pasted", .needle = "PASTED(counter)"},
         .relation = "USAGE",
         .target = {.file = "use_read.c",
                    .suffix = "use_read.global_counter",
                    .label = "Variable",
                    .declaration_line = 5,
                    .declaration_text = "static int global_counter;"}},
    };
    const RQ_Forbidden forbidden[] = {
        {.case_id = "pasted-argument-is-no-read-of-its-own-name",
         .site = C_TU_WORD("use_read.c", use, "use_read.pasted", "PASTED(", "counter"),
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "member-name",
         .site = C_TU_WORD("use_read.c", use, "use_read.member", "MEMBER(p, ", "counter"),
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "stringified",
         .site = C_TU_WORD("use_read.c", use, "use_read.named", "NAMED(", "counter"),
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "pasted-by-a-macro-it-is-handed-to",
         .site = C_TU_WORD("use_read.c", use, "use_read.through", "THROUGH_PASTE(", "counter"),
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "captured-by-the-local-of-that-name",
         .site = C_TU_WORD("use_read.c", use, "use_read.captured", "CAPTURING(", "tmp"),
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "one-definition-of-two-stringifies",
         .site = C_TU_WORD("use_read.c", use, "use_read.swapped_twice", "SWAPPED_TWICE(", "counter"),
         .relation = "USAGE",
         .any_target = true},
    };
    const CTUCase fixture = {.suite = "c-tu-macro-read",
                             .sources = C_TU_MACRO_READ,
                             .source_count = sizeof(C_TU_MACRO_READ) / sizeof(C_TU_MACRO_READ[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0]),
                             .forbidden = forbidden,
                             .forbidden_count = sizeof(forbidden) / sizeof(forbidden[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* neovim's three loop macro forms: a name in parentheses, a second argument, and a macro that
 * hands its arguments to the macros that declare them. The name is a variable of the written
 * type only inside what the macro says. */
#define C_TU_LOOPS_HEADER                                                                       \
    "#ifndef LOOPS_H\n"                                                                         \
    "#define LOOPS_H\n"                                                                         \
    "typedef struct tab_S { struct tab_S *tp_next; int tp_flag; } tab_T;\n"                     \
    "typedef struct win_S { struct win_S *w_next; int w_buffer; } win_T;\n"                     \
    "extern tab_T *first_tab;\n"                                                                \
    "extern win_T *first_win;\n"                                                                \
    "extern int wp;\n"                                                                          \
    "#define FOR_TABS(tp) for (tab_T *(tp) = first_tab; (tp); (tp) = (tp)->tp_next)\n"          \
    "#define FOR_WINS(wp, tp) for (win_T *wp = (tp) ? first_win : 0; wp; wp = wp->w_next)\n"    \
    "#define FOR_TAB_WINS(tp, wp) FOR_TABS(tp) FOR_WINS(wp, tp)\n"                              \
    "#endif\n"
#define C_TU_LOOPS_USE                                                                          \
    "#include \"loops.h\"\n"                                                                    \
    "int use_tabs(void) {\n"                                                                    \
    "  int n = 0;\n"                                                                            \
    "  FOR_TABS(tp) {\n"                                                                        \
    "    n += tp->tp_flag;\n"                                                                   \
    "  }\n"                                                                                     \
    "  return n;\n"                                                                             \
    "}\n"                                                                                       \
    "int use_wins(tab_T *t) {\n"                                                                \
    "  int n = 0;\n"                                                                            \
    "  FOR_WINS(wp, t) {\n"                                                                     \
    "    n += wp->w_buffer;\n"                                                                  \
    "  }\n"                                                                                     \
    "  return n;\n"                                                                             \
    "}\n"                                                                                       \
    "int use_both(void) {\n"                                                                    \
    "  int n = 0;\n"                                                                            \
    "  FOR_TAB_WINS(tp, wp) {\n"                                                                \
    "    n += wp->w_buffer;\n"                                                                  \
    "  }\n"                                                                                     \
    "  return n;\n"                                                                             \
    "}\n"
static const RQ_Source C_TU_LOOPS[] = {{.path = "loops.h", .text = C_TU_LOOPS_HEADER},
                                       {.path = "use_loops.c", .text = C_TU_LOOPS_USE},
                                       {.path = "use_loops_cpp.cpp", .text = C_TU_LOOPS_USE}};

TEST(resolver_c_loop_macro_declares_its_variable) {
    const char *use = C_TU_LOOPS_USE;
    const RQ_Target tab_type = {.file = "loops.h",
                                .suffix = "loops.tab_T",
                                .label = "Type",
                                .declaration_line = 3,
                                .declaration_text =
                                    "typedef struct tab_S { struct tab_S *tp_next; int tp_flag; } "
                                    "tab_T;"};
    const RQ_Target win_type = {.file = "loops.h",
                                .suffix = "loops.win_T",
                                .label = "Type",
                                .declaration_line = 4,
                                .declaration_text =
                                    "typedef struct win_S { struct win_S *w_next; int w_buffer; } "
                                    "win_T;"};
    const RQ_Target w_buffer = {.file = "loops.h",
                                .suffix = "loops.win_S.w_buffer",
                                .label = "Field",
                                .declaration_line = 4,
                                .declaration_text =
                                    "typedef struct win_S { struct win_S *w_next; int w_buffer; } "
                                    "win_T;"};
    const RQ_Target tp_flag = {.file = "loops.h",
                               .suffix = "loops.tab_S.tp_flag",
                               .label = "Field",
                               .declaration_line = 3,
                               .declaration_text =
                                   "typedef struct tab_S { struct tab_S *tp_next; int tp_flag; } "
                                   "tab_T;"};
    const RQ_Expected expected[] = {
        {.case_id = "parenthesized-macro-variable",
         .site = C_TU_WORD("use_loops.c", use, "use_loops.use_tabs", "n += tp->", "tp_flag"),
         .relation = "USAGE",
         .target = tp_flag},
        {.case_id = "second-argument-loop",
         .site = C_TU_WORD("use_loops.c", use, "use_loops.use_wins", "n += wp->", "w_buffer"),
         .relation = "USAGE",
         .target = w_buffer},
        {.case_id = "macro-handing-on-to-declaring-macros",
         .site = C_TU_WORD("use_loops.c", use, "use_loops.use_both", "use_both(", "w_buffer"),
         .relation = "USAGE",
         .target = w_buffer},
        {.case_id = "parameter-type-of-the-second-argument-loop",
         .site = C_TU_WORD("use_loops.c", use, "use_loops.use_wins", "(tab_T *t", "tab_T"),
         .relation = "USAGE",
         .target = tab_type},
        {.case_id = "type-the-loop-macro-writes",
         .site = {.file = "use_loops.c", .caller = "use_loops.use_wins", .needle = "FOR_WINS(wp, t)"},
         .relation = "USAGE",
         .target = win_type,
         .dispatch_group = "wins"},
        {.case_id = "member-the-loop-macro-reads-through-its-own-variable",
         .site = {.file = "use_loops.c", .caller = "use_loops.use_wins", .needle = "FOR_WINS(wp, t)"},
         .relation = "USAGE",
         .target = {.file = "loops.h",
                    .suffix = "loops.win_S.w_next",
                    .label = "Field",
                    .declaration_line = 4,
                    .declaration_text =
                        "typedef struct win_S { struct win_S *w_next; int w_buffer; } win_T;"},
         .dispatch_group = "wins"},
        {.case_id = "type-of-the-macro-handed-on-first",
         .site = {.file = "use_loops.c",
                  .caller = "use_loops.use_both",
                  .needle = "FOR_TAB_WINS(tp, wp)"},
         .relation = "USAGE",
         .target = tab_type,
         .dispatch_group = "both"},
        {.case_id = "type-of-the-macro-handed-on-second",
         .site = {.file = "use_loops.c",
                  .caller = "use_loops.use_both",
                  .needle = "FOR_TAB_WINS(tp, wp)"},
         .relation = "USAGE",
         .target = win_type,
         .dispatch_group = "both"},
        {.case_id = "cpp-grammar-parenthesized-macro-variable",
         .site = C_TU_WORD("use_loops_cpp.cpp", use, "use_loops_cpp.use_tabs", "n += tp->",
                           "tp_flag"),
         .relation = "USAGE",
         .target = tp_flag},
    };
    const RQ_Forbidden forbidden[] = {
        /* `extern int wp;` is in the header, and the macro's own variable hides it. */
        {.case_id = "global-of-the-declared-name",
         .site = C_TU_WORD("use_loops.c", use, "use_loops.use_wins", "n += ", "wp"),
         .relation = "USAGE",
         .any_target = true},
    };
    const CTUCase fixture = {.suite = "c-tu-loops",
                             .sources = C_TU_LOOPS,
                             .source_count = sizeof(C_TU_LOOPS) / sizeof(C_TU_LOOPS[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0]),
                             .forbidden = forbidden,
                             .forbidden_count = sizeof(forbidden) / sizeof(forbidden[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* A function's name as a value, and the file-scope table it often sits in, past a syntax error
 * elsewhere in the file. A directive's names are not uses of anything. */
#define C_TU_FNVALUES_USE                                                                         \
    "#include \"values_dep.h\"\n"                                                               \
    "typedef struct ops { int (*cb)(int); } ops;\n"                                             \
    "enum colour { RED, GREEN };\n"                                                             \
    "static int handler(int x) { return x; }\n"                                                 \
    "static int (*hook)(int);\n"                                                                \
    "static int broken(void) { return ( ; }\n"                                                  \
    "static ops table = { .cb = handler };\n"                                                   \
    "static int colours[] = { GREEN };\n"                                                       \
    "void install(void) { hook = handler; }\n"                                                  \
    "void address(void) { hook = &handler; }\n"                                                 \
    "int shadowed(int handler) { return handler; }\n"                                           \
    "#define handler_alias handler\n"                                                           \
    "#if GREEN\n"                                                                               \
    "int conditioned;\n"                                                                        \
    "#endif\n"
static const RQ_Source C_TU_FNVALUES[] = {{.path = "values_dep.h", .text = "/* empty */\n"},
                                          {.path = "values.c", .text = C_TU_FNVALUES_USE}};

TEST(resolver_c_function_names_are_values_at_any_scope) {
    const char *use = C_TU_FNVALUES_USE;
    const RQ_Target handler = {.file = "values.c",
                               .suffix = "values.handler",
                               .label = "Function",
                               .declaration_line = 4,
                               .declaration_text = "static int handler(int x) { return x; }"};
    const RQ_Expected expected[] = {
        {.case_id = "assigned-to-a-pointer",
         .site = C_TU_WORD("values.c", use, "values.install", "hook = ", "handler"),
         .relation = "USAGE",
         .target = handler},
        {.case_id = "address-taken",
         .site = C_TU_WORD("values.c", use, "values.address", "hook = &", "handler"),
         .relation = "USAGE",
         .target = handler},
        {.case_id = "file-scope-table-past-an-error-elsewhere",
         .site = C_TU_WORD("values.c", use, "values", ".cb = ", "handler"),
         .relation = "USAGE",
         .target = handler},
        {.case_id = "designator-in-a-file-scope-table",
         .site = C_TU_WORD("values.c", use, "values", ".cb = ", "cb"),
         .relation = "USAGE",
         .target = {.file = "values.c",
                    .suffix = "values.ops.cb",
                    .label = "Field",
                    .declaration_line = 2,
                    .declaration_text = "typedef struct ops { int (*cb)(int); } ops;"}},
        {.case_id = "enumerator-in-a-file-scope-table",
         .site = C_TU_WORD("values.c", use, "values", "colours[] = { ", "GREEN"),
         .relation = "USAGE",
         .target = {.file = "values.c",
                    .suffix = "values.GREEN",
                    .label = "Variable",
                    .declaration_line = 3,
                    .declaration_text = "enum colour { RED, GREEN };"}},
        {.case_id = "type-of-a-file-scope-table",
         .site = C_TU_WORD("values.c", use, "values", "static ops table", "ops"),
         .relation = "USAGE",
         .target = {.file = "values.c",
                    .suffix = "values.ops",
                    .label = "Class",
                    .declaration_line = 2,
                    .declaration_text = "typedef struct ops { int (*cb)(int); } ops;"}},
    };
    const RQ_Forbidden forbidden[] = {
        {.case_id = "parameter-of-that-name",
         .site = C_TU_WORD("values.c", use, "values.shadowed", "return ", "handler"),
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "condition-of-a-directive",
         .site = C_TU_WORD("values.c", use, "values", "#if ", "GREEN"),
         .relation = "USAGE",
         .any_target = true},
    };
    const CTUCase fixture = {.suite = "c-tu-values",
                             .sources = C_TU_FNVALUES,
                             .source_count = sizeof(C_TU_FNVALUES) / sizeof(C_TU_FNVALUES[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0]),
                             .forbidden = forbidden,
                             .forbidden_count = sizeof(forbidden) / sizeof(forbidden[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* redis, hiredis and Lua write these. A header's `typedef struct X X;` beside `struct X {...}` in
 * the same-stem source is one type under one QN. A struct in an `#ifdef __cplusplus extern "C" {`
 * or below an include ahead of the guard is in a branch only by accident of how it parses.
 * Swapped layouts in two branches stay unknown. */
#define C_TU_SHAPES_LIST_H                                                                      \
    "#include <stdint.h>\n"                                                                     \
    "#ifndef LIST_H\n"                                                                          \
    "#define LIST_H\n"                                                                          \
    "typedef struct listNode {\n"                                                               \
    "    struct listNode *prev;\n"                                                              \
    "    int sz;\n"                                                                             \
    "} listNode;\n"                                                                             \
    "typedef struct opaque opaque;\n"                                                           \
    "#endif\n"
#define C_TU_SHAPES_LIST_C                                                                      \
    "#include \"list.h\"\n"                                                                     \
    "#define LIST_STATIC static\n"                                                              \
    "LIST_STATIC listNode *make_node(int sz);\n"                                                \
    "struct opaque { int secret; };\n"                                                          \
    "int read_opaque(opaque *o) { return o->secret; }\n"                                        \
    "int read_tag(struct opaque *o) { return o->secret; }\n"                                    \
    "int read_node(listNode *n) { return n->sz; }\n"
#define C_TU_SHAPES_REPLY_H                                                                     \
    "#ifndef REPLY_H\n"                                                                         \
    "#define REPLY_H\n"                                                                         \
    "#ifdef __cplusplus\n"                                                                      \
    "extern \"C\" {\n"                                                                          \
    "#endif\n"                                                                                  \
    "typedef struct reply { int type; } reply;\n"                                               \
    "#ifdef __cplusplus\n"                                                                      \
    "}\n"                                                                                       \
    "#endif\n"                                                                                  \
    "#endif\n"
#define C_TU_SHAPES_SWAP_H                                                                      \
    "#ifndef SWAP_H\n"                                                                          \
    "#define SWAP_H\n"                                                                          \
    "#ifdef WIDE\n"                                                                             \
    "struct swapped { long size; };\n"                                                          \
    "#else\n"                                                                                   \
    "struct swapped { int size; };\n"                                                           \
    "#endif\n"                                                                                  \
    "#endif\n"
#define C_TU_SHAPES_USE                                                                         \
    "#include \"list.h\"\n"                                                                     \
    "#include \"reply.h\"\n"                                                                    \
    "#include \"swap.h\"\n"                                                                     \
    "typedef struct sock { int fd; } sock;\n"                                                   \
    "static sock CT_Socket;\n"                                                                  \
    "int use_reply(reply *r) { return r->type; }\n"                                             \
    "int use_node(listNode *n) { return n->sz; }\n"                                             \
    "int use_swapped(struct swapped *s) { return s->size; }\n"                                  \
    "int use_opaque(opaque *o) { return o->secret; }\n"                                         \
    "int use_socket(sock *c) { return c->fd; }\n"                                               \
    "static sock CT_Socket = { 1 };\n"                                                          \
    "int use_after(sock *c) { return c->fd; }\n"
static const RQ_Source C_TU_SHAPES[] = {{.path = "list.h", .text = C_TU_SHAPES_LIST_H},
                                        {.path = "list.c", .text = C_TU_SHAPES_LIST_C},
                                        {.path = "reply.h", .text = C_TU_SHAPES_REPLY_H},
                                        {.path = "swap.h", .text = C_TU_SHAPES_SWAP_H},
                                        {.path = "use_shapes.c", .text = C_TU_SHAPES_USE}};

TEST(resolver_c_struct_shapes_real_headers_use) {
    const char *use = C_TU_SHAPES_USE;
    const char *impl = C_TU_SHAPES_LIST_C;
    const RQ_Target sz = {.file = "list.h",
                          .suffix = "list.listNode.sz",
                          .label = "Field",
                          .declaration_line = 6,
                          .declaration_text = "    int sz;"};
    const RQ_Target secret = {.file = "list.c",
                              .suffix = "list.opaque.secret",
                              .label = "Field",
                              .declaration_line = 4,
                              .declaration_text = "struct opaque { int secret; };"};
    const RQ_Target opaque_class = {.file = "list.c",
                                    .suffix = "list.opaque",
                                    .label = "Class",
                                    .declaration_line = 4,
                                    .declaration_text = "struct opaque { int secret; };"};
    const RQ_Target sock_class = {.file = "use_shapes.c",
                                  .suffix = "use_shapes.sock",
                                  .label = "Class",
                                  .declaration_line = 4,
                                  .declaration_text = "typedef struct sock { int fd; } sock;"};
    const RQ_Target fd = {.file = "use_shapes.c",
                          .suffix = "use_shapes.sock.fd",
                          .label = "Field",
                          .declaration_line = 4,
                          .declaration_text = "typedef struct sock { int fd; } sock;"};
    const RQ_Expected expected[] = {
        {.case_id = "struct-below-an-include-ahead-of-the-guard",
         .site = C_TU_WORD("use_shapes.c", use, "use_shapes.use_node", "n->", "sz"),
         .relation = "USAGE",
         .target = sz},
        {.case_id = "struct-inside-a-cplusplus-branch",
         .site = C_TU_WORD("use_shapes.c", use, "use_shapes.use_reply", "r->", "type"),
         .relation = "USAGE",
         .target = {.file = "reply.h",
                    .suffix = "reply.reply.type",
                    .label = "Field",
                    .declaration_line = 6,
                    .declaration_text = "typedef struct reply { int type; } reply;"}},
        {.case_id = "typedef-name-with-tag-in-the-same-stem-source",
         .site = C_TU_WORD("list.c", impl, "list.read_opaque", "o->", "secret"),
         .relation = "USAGE",
         .target = secret},
        {.case_id = "written-tag-beside-a-header-typedef",
         .site = C_TU_WORD("list.c", impl, "list.read_tag", "read_tag(", "secret"),
         .relation = "USAGE",
         .target = secret},
        {.case_id = "class-keeps-its-node-despite-a-macro-prefixed-prototype",
         .site = C_TU_WORD("list.c", impl, "list.read_node", "read_node(", "listNode"),
         .relation = "USAGE",
         .target = {.file = "list.h",
                    .suffix = "list.listNode",
                    .label = "Class",
                    .declaration_line = 4,
                    .declaration_text = "typedef struct listNode {"}},
        {.case_id = "typedef-name-of-a-tag-defined-in-the-source",
         .site = C_TU_WORD("list.c", impl, "list.read_opaque", "(opaque *o", "opaque"),
         .relation = "USAGE",
         .target = opaque_class},
        {.case_id = "a-tag-in-its-own-file",
         .site = C_TU_WORD("list.c", impl, "list.read_tag", "struct opaque *o", "opaque"),
         .relation = "USAGE",
         .target = opaque_class},
        {.case_id = "the-name-of-a-struct-with-two-layouts",
         .site = C_TU_WORD("use_shapes.c", use, "use_shapes.use_swapped", "struct swapped *s", "swapped"),
         .relation = "USAGE",
         .target = {.file = "swap.h",
                    .suffix = "swap.swapped",
                    .label = "Class",
                    .declaration_line = 6,
                    .declaration_text = "struct swapped { int size; };"}},
        {.case_id = "member-through-the-typedef-name-in-its-own-file",
         .site = C_TU_WORD("list.c", impl, "list.read_node", "n->", "sz"),
         .relation = "USAGE",
         .target = sz},
        {.case_id = "type-inside-a-cplusplus-branch",
         .site = C_TU_WORD("use_shapes.c", use, "use_shapes.use_reply", "(reply *r", "reply"),
         .relation = "USAGE",
         .target = {.file = "reply.h",
                    .suffix = "reply.reply",
                    .label = "Class",
                    .declaration_line = 6,
                    .declaration_text = "typedef struct reply { int type; } reply;"}},
        {.case_id = "type-below-an-include-ahead-of-the-guard",
         .site = C_TU_WORD("use_shapes.c", use, "use_shapes.use_node", "(listNode *n", "listNode"),
         .relation = "USAGE",
         .target = {.file = "list.h",
                    .suffix = "list.listNode",
                    .label = "Class",
                    .declaration_line = 4,
                    .declaration_text = "typedef struct listNode {"}},
        {.case_id = "typedef-name-from-an-includer-of-the-header",
         .site = C_TU_WORD("use_shapes.c", use, "use_shapes.use_opaque", "(opaque *o", "opaque"),
         .relation = "USAGE",
         .target = opaque_class},
        {.case_id = "type-before-a-second-file-scope-declaration",
         .site = C_TU_WORD("use_shapes.c", use, "use_shapes.use_socket", "(sock *c", "sock"),
         .relation = "USAGE",
         .target = sock_class},
        {.case_id = "type-after-a-second-file-scope-declaration",
         .site = C_TU_WORD("use_shapes.c", use, "use_shapes.use_after", "use_after(sock", "sock"),
         .relation = "USAGE",
         .target = sock_class},
        {.case_id = "member-before-a-second-file-scope-declaration",
         .site = C_TU_WORD("use_shapes.c", use, "use_shapes.use_socket", "c->", "fd"),
         .relation = "USAGE",
         .target = fd},
        {.case_id = "member-after-a-second-file-scope-declaration",
         .site = C_TU_WORD("use_shapes.c", use, "use_shapes.use_after", "use_after(", "fd"),
         .relation = "USAGE",
         .target = fd},
    };
    const RQ_Forbidden forbidden[] = {
        {.case_id = "swapped-layouts-stay-unknown",
         .site = C_TU_WORD("use_shapes.c", use, "use_shapes.use_swapped", "s->", "size"),
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "struct-body-the-unit-does-not-see",
         .site = C_TU_WORD("use_shapes.c", use, "use_shapes.use_opaque", "o->", "secret"),
         .relation = "USAGE",
         .any_target = true},
    };
    const CTUCase fixture = {.suite = "c-tu-shapes",
                             .sources = C_TU_SHAPES,
                             .source_count = sizeof(C_TU_SHAPES) / sizeof(C_TU_SHAPES[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0]),
                             .forbidden = forbidden,
                             .forbidden_count = sizeof(forbidden) / sizeof(forbidden[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* Macros no grammar can place: in a parameter list, in front of a parenthesized name, and between
 * the attribute and the return type. Each declaration still names what it names. Enum typedefs
 * are types too, and a macro's own #define line uses nothing. */
#define C_TU_PROTO_HEADER                                                                       \
    "#ifndef PROTO_H\n"                                                                         \
    "#define PROTO_H\n"                                                                         \
    "typedef struct dict { int n; } dict;\n"                                                    \
    "typedef enum { kOff, kOn } TriState;\n"                                                    \
    "int dictAdd(dict *d, void *key __stored_key, void *val);\n"                                \
    "LUA_API int   (lua_gettop) (dict *L);\n"                                                   \
    "ALWAYS_INLINE int\n"                                                                       \
    "inline_size(dict *d) {\n"                                                                  \
    "    return d->n;\n"                                                                        \
    "}\n"                                                                                       \
    "ALWAYS_INLINE dict *\n"                                                                    \
    "inline_self(dict *d) {\n"                                                                  \
    "    return d;\n"                                                                           \
    "}\n"                                                                                       \
    "ALWAYS_INLINE int\n"                                                                       \
    "inline_caller(dict *d) {\n"                                                                \
    "    return inline_size(inline_self(d));\n"                                                 \
    "}\n"                                                                                       \
    "#endif\n"
#define C_TU_PROTO_IMPL                                                                         \
    "#include \"proto.h\"\n"                                                                    \
    "int dictAdd(dict *d, void *key, void *val) { return d->n; }\n"                             \
    "int lua_gettop(dict *L) { return L->n; }\n"
#define C_TU_PROTO_USE                                                                          \
    "#include \"proto.h\"\n"                                                                    \
    "#define renamed dictAdd\n"                                                                 \
    "int call_add(dict *d) { return dictAdd(d, 0, 0); }\n"                                      \
    "int call_lua(dict *d) { return lua_gettop(d); }\n"                                         \
    "int call_inline(dict *d) { return inline_size(d); }\n"                                     \
    "TriState enum_type(TriState t) { return t; }\n"
static const RQ_Source C_TU_PROTO[] = {{.path = "proto.h", .text = C_TU_PROTO_HEADER},
                                       {.path = "proto.c", .text = C_TU_PROTO_IMPL},
                                       {.path = "use_proto.c", .text = C_TU_PROTO_USE}};

TEST(resolver_c_macros_in_declarations_do_not_hide_their_names) {
    const char *use = C_TU_PROTO_USE;
    const char *header = C_TU_PROTO_HEADER;
    const RQ_Target inline_size = {.file = "proto.h",
                                   .suffix = "proto.inline_size",
                                   .label = "Function",
                                   .declaration_line = 8,
                                   .declaration_text = "inline_size(dict *d) {"};
    const RQ_Expected expected[] = {
        {.case_id = "macro-among-the-parameters",
         .site = {.file = "use_proto.c", .caller = "use_proto.call_add", .needle = "dictAdd(d, 0, 0)"},
         .relation = "CALLS",
         .target = {.file = "proto.c",
                    .suffix = "proto.dictAdd",
                    .label = "Function",
                    .declaration_line = 2,
                    .declaration_text = "int dictAdd(dict *d, void *key, void *val) { return d->n; }"}},
        {.case_id = "macro-before-a-parenthesized-name",
         .site = {.file = "use_proto.c", .caller = "use_proto.call_lua", .needle = "lua_gettop(d)"},
         .relation = "CALLS",
         .target = {.file = "proto.c",
                    .suffix = "proto.lua_gettop",
                    .label = "Function",
                    .declaration_line = 3,
                    .declaration_text = "int lua_gettop(dict *L) { return L->n; }"}},
        {.case_id = "macro-prefixed-definition-from-its-includer",
         .site = {.file = "use_proto.c", .caller = "use_proto.call_inline", .needle = "inline_size(d)"},
         .relation = "CALLS",
         .target = inline_size},
        {.case_id = "macro-prefixed-definition-from-its-own-file",
         .site = {.file = "proto.h",
                  .caller = "proto.inline_caller",
                  .needle = "inline_size(inline_self(d))"},
         .relation = "CALLS",
         .target = inline_size},
        {.case_id = "macro-prefixed-pointer-definition-from-its-own-file",
         .site = {.file = "proto.h",
                  .caller = "proto.inline_caller",
                  .needle = "inline_self(d)"},
         .relation = "CALLS",
         .target = {.file = "proto.h",
                    .suffix = "proto.inline_self",
                    .label = "Function",
                    .declaration_line = 12,
                    .declaration_text = "inline_self(dict *d) {"}},
        {.case_id = "enum-typedef-is-a-type",
         .site = C_TU_WORD("use_proto.c", use, "use_proto.enum_type", "enum_type(", "TriState"),
         .relation = "USAGE",
         .target = {.file = "proto.h",
                    .suffix = "proto.TriState",
                    .label = "Enum",
                    .declaration_line = 4,
                    .declaration_text = "typedef enum { kOff, kOn } TriState;"}},
        {.case_id = "enum-typedef-as-the-return-type",
         .site = C_TU_WORD("use_proto.c", use, "use_proto.enum_type", "TriState enum_type(",
                           "TriState"),
         .relation = "USAGE",
         .target = {.file = "proto.h",
                    .suffix = "proto.TriState",
                    .label = "Enum",
                    .declaration_line = 4,
                    .declaration_text = "typedef enum { kOff, kOn } TriState;"}},
    };
    const RQ_Forbidden forbidden[] = {
        {.case_id = "macro-definition-line-uses-nothing",
         .site = C_TU_WORD("use_proto.c", use, "use_proto", "#define renamed ", "dictAdd"),
         .relation = "USAGE",
         .any_target = true},
    };
    (void)header;
    const CTUCase fixture = {.suite = "c-tu-proto",
                             .sources = C_TU_PROTO,
                             .source_count = sizeof(C_TU_PROTO) / sizeof(C_TU_PROTO[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0]),
                             .forbidden = forbidden,
                             .forbidden_count = sizeof(forbidden) / sizeof(forbidden[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* Two-phase lookup on the forms LLVM's headers use. What a template's own body names binds where
 * it is written, except its parameters and what an instantiation adds. */
#define C_TU_TMPL_HEADER                                                                        \
    "#ifndef TMPL_H\n"                                                                          \
    "#define TMPL_H\n"                                                                          \
    "namespace llvm {\n"                                                                        \
    "class T { public: int decoy() const { return 0; } };\n"                                    \
    "class Helper { public: int help() const { return 1; } };\n"                                \
    "int free_fn(int V);\n"                                                                     \
    "void swap(int &A, int &B);\n"                                                              \
    "template <typename U> int free_tpl(U V) { return free_fn(V); }\n"                          \
    "template <typename U> int fixed_tpl() { return free_fn(1); }\n"                            \
    "template <typename T> class Box {\n"                                                       \
    "public:\n"                                                                                 \
    "  enum Kind { Small, Large };\n"                                                           \
    "  int size() const { return count(); }\n"                                                  \
    "  int count() const { return Held; }\n"                                                    \
    "  Kind kind() const { return Small; }\n"                                                   \
    "  T *param(T *V) { return V; }\n"                                                          \
    "private:\n"                                                                                \
    "  int Held;\n"                                                                             \
    "};\n"                                                                                      \
    "template <typename T> class Derived : public Box<T> {\n"                                   \
    "public:\n"                                                                                 \
    "  int viaBase() { return size(); }\n"                                                      \
    "};\n"                                                                                      \
    "template <typename U> struct Traits { int width() const { return 1; } };\n"                \
    "template <> struct Traits<int> { int width() const { return 2; } };\n"                     \
    "class Triple {\n"                                                                          \
    "public:\n"                                                                                 \
    "  enum OSType { UnknownOS, Linux };\n"                                                     \
    "  enum class Scoped { Inner };\n"                                                          \
    "  OSType os() const { return Linux; }\n"                                                   \
    "};\n"                                                                                      \
    "}\n"                                                                                       \
    "#endif\n"
#define C_TU_TMPL_USE                                                                           \
    "#include <utility>\n"                                                                      \
    "#include \"tmpl.h\"\n"                                                                     \
    "using namespace llvm;\n"                                                                   \
    "int useBox(Box<int> B) { return B.size(); }\n"                                             \
    "int useTraits(Traits<int> S) { return S.width(); }\n"                                      \
    "int useCast(void *Q) { auto *P = static_cast<Helper *>(Q); return P->help(); }\n"          \
    "int useScoped() { return Triple::UnknownOS; }\n"                                           \
    "void useStdSwap(int A, int B) { using std::swap; swap(A, B); }\n"
static const RQ_Source C_TU_TMPL[] = {{.path = "tmpl.h", .text = C_TU_TMPL_HEADER},
                                      {.path = "tmpl_use.cpp", .text = C_TU_TMPL_USE}};

TEST(resolver_cpp_templates_bind_what_is_not_dependent) {
    const char *header = C_TU_TMPL_HEADER;
    const char *use = C_TU_TMPL_USE;
    const RQ_Target box_size = {.file = "tmpl.h",
                                .suffix = "llvm.Box.size",
                                .label = "Method",
                                .declaration_line = 13,
                                .declaration_text = "  int size() const { return count(); }"};
    const RQ_Target free_fn_target = {.file = "tmpl.h",
                                      .suffix = "llvm.free_fn",
                                      .label = NULL,
                                      .declaration_line = 6,
                                      .declaration_text = "int free_fn(int V);"};
    (void)free_fn_target;
    const RQ_Expected expected[] = {
        {.case_id = "own-member-of-the-current-instantiation",
         .site = {.file = "tmpl.h", .caller = "llvm.Box.size", .needle = "count()"},
         .relation = "CALLS",
         .target = {.file = "tmpl.h",
                    .suffix = "llvm.Box.count",
                    .label = "Method",
                    .declaration_line = 14,
                    .declaration_text = "  int count() const { return Held; }"}},
        {.case_id = "own-field-of-the-current-instantiation",
         .site = C_TU_WORD("tmpl.h", header, "llvm.Box.count", "return ", "Held"),
         .relation = "USAGE",
         .target = {.file = "tmpl.h",
                    .suffix = "llvm.Box.Held",
                    .label = "Field",
                    .declaration_line = 18,
                    .declaration_text = "  int Held;"}},
        {.case_id = "own-enum-as-a-return-type",
         .site = C_TU_WORD("tmpl.h", header, "llvm.Box.kind", "Kind kind()", "Kind"),
         .relation = "USAGE",
         .target = {.file = "tmpl.h",
                    .suffix = "llvm.Box.Kind",
                    .label = "Enum",
                    .declaration_line = 12,
                    .declaration_text = "  enum Kind { Small, Large };"}},
        {.case_id = "own-enumerator-of-the-current-instantiation",
         .site = C_TU_WORD("tmpl.h", header, "llvm.Box.kind", "return Small", "Small"),
         .relation = "USAGE",
         .target = {.file = "tmpl.h",
                    .suffix = "llvm.Box.Small",
                    .label = "Variable",
                    .declaration_line = 12,
                    .declaration_text = "  enum Kind { Small, Large };"}},
        {.case_id = "receiver-of-a-class-template-with-no-specialization",
         .site = {.file = "tmpl_use.cpp", .caller = "useBox", .needle = "B.size()"},
         .relation = "CALLS",
         .target = box_size},
        {.case_id = "receiver-of-a-written-cast-through-auto",
         .site = {.file = "tmpl_use.cpp", .caller = "useCast", .needle = "P->help()"},
         .relation = "CALLS",
         .target = {.file = "tmpl.h",
                    .suffix = "llvm.Helper.help",
                    .label = "Method",
                    .declaration_line = 5,
                    .declaration_text = "class Helper { public: int help() const { return 1; } };"}},
        {.case_id = "enumerator-through-its-class",
         .site = C_TU_WORD("tmpl_use.cpp", use, "useScoped", "useScoped(", "UnknownOS"),
         .relation = "USAGE",
         .target = {.file = "tmpl.h",
                    .suffix = "llvm.Triple.UnknownOS",
                    .label = "Variable",
                    .declaration_line = 28,
                    .declaration_text = "  enum OSType { UnknownOS, Linux };"}},
    };
    const RQ_Forbidden forbidden[] = {
        {.case_id = "template-parameter-hides-a-class-of-its-name",
         .site = C_TU_WORD("tmpl.h", header, "llvm.Box.param", "T *param(T", "T"),
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "unqualified-name-does-not-search-a-dependent-base",
         .site = {.file = "tmpl.h", .caller = "llvm.Derived.viaBase", .needle = "size()"},
         .relation = "CALLS",
         .any_target = true},
        {.case_id = "dependent-argument-may-add-candidates",
         .site = {.file = "tmpl.h", .caller = "llvm.free_tpl", .needle = "free_fn(V)"},
         .relation = "CALLS",
         .any_target = true},
        {.case_id = "a-specialization-may-replace-the-member",
         .site = {.file = "tmpl_use.cpp", .caller = "useTraits", .needle = "S.width()"},
         .relation = "CALLS",
         .any_target = true},
        {.case_id = "using-of-a-declaration-this-index-has-not",
         .site = {.file = "tmpl_use.cpp", .caller = "useStdSwap", .needle = "swap(A, B)"},
         .relation = "CALLS",
         .any_target = true},
    };
    const CTUCase fixture = {.suite = "c-tu-tmpl",
                             .sources = C_TU_TMPL,
                             .source_count = sizeof(C_TU_TMPL) / sizeof(C_TU_TMPL[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0]),
                             .forbidden = forbidden,
                             .forbidden_count = sizeof(forbidden) / sizeof(forbidden[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* redis's ae.c includes ae_epoll.c, which includes nothing it needs. A fragment with one includer
 * is compiled only there, seeing what stood before its `#include`. */
#define C_TU_FRAG_FRAGMENT                                                                      \
    "static int fragment_free(aeLoop *l) { return l->apidata != 0; }\n"                         \
    "static int fragment_late(lateLoop *l) { return l->later != 0; }\n"
#define C_TU_FRAG_SHARED                                                                        \
    "static int shared_free(thing *t) { return t->size; }\n"
static const RQ_Source C_TU_FRAG[] = {
    {.path = "ae.h", .text = "typedef struct aeLoop { void *apidata; } aeLoop;\n"},
    {.path = "late.h", .text = "typedef struct lateLoop { void *later; } lateLoop;\n"},
    {.path = "ae.c", .text = "#include \"ae.h\"\n#include \"ae_epoll.c\"\n#include \"late.h\"\n"},
    {.path = "ae_epoll.c", .text = C_TU_FRAG_FRAGMENT},
    {.path = "one.h", .text = "typedef struct thing { int size; } thing;\n"},
    {.path = "two.h", .text = "typedef struct thing { long size; } thing;\n"},
    {.path = "one.c", .text = "#include \"one.h\"\n#include \"shared.c\"\n"},
    {.path = "two.c", .text = "#include \"two.h\"\n#include \"shared.c\"\n"},
    {.path = "shared.c", .text = C_TU_FRAG_SHARED}};

TEST(resolver_c_fragment_sees_what_its_one_includer_saw) {
    const char *fragment = C_TU_FRAG_FRAGMENT;
    const char *shared = C_TU_FRAG_SHARED;
    const RQ_Expected expected[] = {
        {.case_id = "field-from-an-earlier-include-of-the-includer",
         .site = C_TU_WORD("ae_epoll.c", fragment, "ae_epoll.fragment_free", "l->", "apidata"),
         .relation = "USAGE",
         .target = {.file = "ae.h",
                    .suffix = "ae.aeLoop.apidata",
                    .label = "Field",
                    .declaration_line = 1,
                    .declaration_text = "typedef struct aeLoop { void *apidata; } aeLoop;"}},
        {.case_id = "type-from-an-earlier-include-of-the-includer",
         .site = C_TU_WORD("ae_epoll.c", fragment, "ae_epoll.fragment_free", "fragment_free(",
                           "aeLoop"),
         .relation = "USAGE",
         .target = {.file = "ae.h",
                    .suffix = "ae.aeLoop",
                    .label = "Class",
                    .declaration_line = 1,
                    .declaration_text = "typedef struct aeLoop { void *apidata; } aeLoop;"}},
    };
    const RQ_Forbidden forbidden[] = {
        {.case_id = "include-standing-after-the-fragment",
         .site = C_TU_WORD("ae_epoll.c", fragment, "ae_epoll.fragment_late", "l->", "later"),
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "fragment-of-two-includers",
         .site = C_TU_WORD("shared.c", shared, "shared.shared_free", "t->", "size"),
         .relation = "USAGE",
         .any_target = true},
    };
    const CTUCase fixture = {.suite = "c-tu-fragment",
                             .sources = C_TU_FRAG,
                             .source_count = sizeof(C_TU_FRAG) / sizeof(C_TU_FRAG[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0]),
                             .forbidden = forbidden,
                             .forbidden_count = sizeof(forbidden) / sizeof(forbidden[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* What a macro's replacement names compiles where the macro is used. Redis registers its module API
 * with REGISTER_API(name) and asserts through a macro around _serverAssert, neovim allocates through
 * kv_push and reads firstwin through FOR_ALL_WINDOWS. A local at the use takes the name, a local of
 * the macro's own has it, a member is no lookup, and a function nobody declared here stays unbound. */
#define C_TU_BODY_API_H                                                                         \
    "#ifndef BODY_API_H\n"                                                                      \
    "#define BODY_API_H\n"                                                                      \
    "void report_failure(const char *what);\n"                                                  \
    "extern int counter;\n"                                                                     \
    "int RM_Open(int v);\n"                                                                     \
    "int register_it(int (*f)(int));\n"                                                         \
    "int hi_new(int v);\n"                                                                      \
    "int helper(int v);\n"                                                                      \
    "typedef struct list_S { int len; struct list_S *next; } list_T;\n"                         \
    "#define LIST_LEN(l) ((l)->len)\n"                                                          \
    "#define NEXT_LEN(l) l->next->len\n"                                                        \
    "typedef struct other_S { int len; } other_T;\n"                                            \
    "#define THROUGH_CALL(o) (&to_list(o)->len)\n"                                              \
    "#define CHECK(x) do { if (!(x)) report_failure(\"x\"); } while (0)\n"                      \
    "#define READ_COUNTER() (counter)\n"                                                        \
    "#define REGISTER(name) register_it(RM_##name)\n"                                           \
    "#define CURRENT counter\n"                                                                 \
    "#define sd_new hi_new\n"                                                                   \
    "#define list_alias list_T\n"                                                               \
    "#define TWICE(x) CHECK(x); CHECK(x)\n"                                                     \
    "#define HELPER_LOCAL(a) do { int helper = (a); (void)helper; } while (0)\n"                \
    "#define MEMBER_OF(p) ((p)->report_failure)\n"                                              \
    "#define TAKE_MEMBER(p, m) ((p)->m)\n"                                                      \
    "#define PROTOTYPE_PARAMETER(a) { extern int proto_fn(void *counter); (void)(a); }\n"       \
    "struct object_like { int size; };\n"                                                       \
    "#define READ_OBJECT() (object_like.size)\n"                                                \
    "#define SELECT_BY_TYPE(x) _Generic((x), long: &counter, default: 0)\n"                     \
    "#define HAND_ON_MEMBER(p) TAKE_MEMBER(p, counter)\n"                                       \
    "#define HAND_TO_FUNCTION(v) register_value(v, counter)\n"                                  \
    "#define HAND_TO_UNKNOWN(v) not_declared_here(v, counter)\n"                                \
    "#define HAND_THROUGH_OFFSET(T) offsetof(T, counter)\n"                                     \
    "int register_value(int v, int w);\n"                                                       \
    "#ifdef FEATURE\n"                                                                          \
    "#define ONE_BRANCH(x) report_failure(\"x\")\n"                                             \
    "#define BOTH_BRANCHES(x) report_failure(\"a\")\n"                                           \
    "#else\n"                                                                                   \
    "#define ONE_BRANCH(x)\n"                                                                   \
    "#define BOTH_BRANCHES(x) report_failure(\"b\")\n"                                           \
    "#endif\n"                                                                                  \
    "#ifdef FEATURE\n"                                                                          \
    "#define LATER_BRANCH(x)\n"                                                                 \
    "#else\n"                                                                                   \
    "#define LATER_BRANCH(x) report_failure(\"x\")\n"                                           \
    "#endif\n"                                                                                  \
    "#endif\n"
#define C_TU_BODY_API_C                                                                         \
    "void report_failure(const char *what) { (void)what; }\n"                                   \
    "int counter;\n"                                                                            \
    "int RM_Open(int v) { return v; }\n"                                                        \
    "int register_it(int (*f)(int)) { return f(0); }\n"                                         \
    "int hi_new(int v) { return v; }\n"                                                         \
    "int helper(int v) { return v; }\n"                                                         \
    "int register_value(int v, int w) { return v + w; }\n"
#define C_TU_BODY_USE                                                                           \
    "#include \"body_api.h\"\n"                                                                 \
    "void checked(int v) { CHECK(v); }\n"                                                       \
    "int reads(void) { return READ_COUNTER(); }\n"                                              \
    "void registers(void) { REGISTER(Open); }\n"                                                \
    "int reads_object(void) { return CURRENT; }\n"                                              \
    "int renamed(int v) { return sd_new(v); }\n"                                                \
    "void nested(int v) { TWICE(v); }\n"                                                        \
    "void in_body(int v) {\n"                                                                   \
    "#define BODY_CHECK(x) report_failure(\"x\")\n"                                             \
    "  BODY_CHECK(v);\n"                                                                        \
    "}\n"                                                                                       \
    "int shadowed(void) { int counter = 1; return READ_COUNTER() + counter; }\n"                \
    "void own_local(int v) { HELPER_LOCAL(v); }\n"                                              \
    "struct holder { int report_failure; };\n"                                                  \
    "int member(struct holder *h) { return MEMBER_OF(h); }\n"                                   \
    "void one_branch(int v) { ONE_BRANCH(v); }\n"                                               \
    "void later_branch(int v) { LATER_BRANCH(v); }\n"                                           \
    "void both_branches(int v) { BOTH_BRANCHES(v); }\n"                                         \
    "int read_len(list_T *l) { return LIST_LEN(l); }\n"                                         \
    "int read_next(list_T *l) { return NEXT_LEN(l); }\n"                                        \
    "int no_receiver(void *p) { return LIST_LEN(p); }\n"                                        \
    "void through_call(other_T *o) { (void)THROUGH_CALL(o); }\n"                                \
    "int through_alias(list_alias *l) { return l != 0; }\n"                                     \
    "int handed_member(void *p) { return HAND_ON_MEMBER(p); }\n"                                \
    "int selected(int v) { return SELECT_BY_TYPE(v); }\n"                                       \
    "void prototype_parameter(int v) { PROTOTYPE_PARAMETER(v); }\n"                             \
    "int read_object(void) { return READ_OBJECT(); }\n"                                         \
    "int handed_function(int v) { return HAND_TO_FUNCTION(v); }\n"                              \
    "int handed_unknown(int v) { return HAND_TO_UNKNOWN(v); }\n"                                \
    "unsigned long handed_offset(void) { return HAND_THROUGH_OFFSET(list_T); }\n"               \
    "#undef CHECK\n"                                                                            \
    "void after_undef(int v) { CHECK(v); }\n"
#define C_TU_BODY_HIDDEN_C "int hidden_target(int v) { return v; }\n"
#define C_TU_BODY_HIDDEN_USE                                                                    \
    "#define CALL_HIDDEN(x) hidden_target(x)\n"                                                 \
    "int undeclared(int v) { return CALL_HIDDEN(v); }\n"
#define C_TU_BODY_QUALIFIED                                                                     \
    "#include \"body_api.h\"\n"                                                                 \
    "int qualified_helper(int v) { return v; }\n"                                               \
    "namespace other {\n"                                                                       \
    "int qualified_helper(int v) { return v + 1; }\n"                                           \
    "}\n"                                                                                       \
    "template <class T> struct Holder { static int qualified_helper(int v) { return v; } };\n"  \
    "#define QCALL(x) other::qualified_helper(x)\n"                                             \
    "#define TCALL(x) Holder<int>::qualified_helper(x)\n"                                       \
    "int qualified(int v) { return QCALL(v); }\n"                                               \
    "int through_template(int v) { return TCALL(v); }\n"                                        \
    "struct Own {\n"                                                                            \
    "  int qualified_helper(int v) { return v; }\n"                                             \
    "  int plain(int v);\n"                                                                     \
    "};\n"                                                                                      \
    "#define PLAIN(x) qualified_helper(x)\n"                                                    \
    "int Own::plain(int v) { return PLAIN(v); }\n"                                              \
    "#define GLOBAL(x) ::other::qualified_helper(x)\n"                                          \
    "struct Reaches {\n"                                                                        \
    "  int qualified_helper(int v) { return v; }\n"                                             \
    "  int global(int v) { return GLOBAL(v); }\n"                                               \
    "};\n"
static const RQ_Source C_TU_BODY[] = {{.path = "body_api.h", .text = C_TU_BODY_API_H},
                                      {.path = "body_api.c", .text = C_TU_BODY_API_C},
                                      {.path = "body_use.c", .text = C_TU_BODY_USE},
                                      {.path = "hidden_def.c", .text = C_TU_BODY_HIDDEN_C},
                                      {.path = "hidden_use.c", .text = C_TU_BODY_HIDDEN_USE},
                                      {.path = "qualified.cpp", .text = C_TU_BODY_QUALIFIED}};

TEST(resolver_c_macro_replacement_compiles_at_its_use) {
    const char *body_use = C_TU_BODY_USE;
    const RQ_Expected holder_row = {
        .case_id = "the-tag-in-the-signature",
        .site = C_TU_WORD("body_use.c", body_use, "body_use.member", "struct holder *h", "holder"),
        .relation = "USAGE",
        .target = {.file = "body_use.c",
                   .suffix = "body_use.holder",
                   .label = "Class",
                   .declaration_line = 14,
                   .declaration_text = "struct holder { int report_failure; };"}};
    const RQ_Target list_type = {.file = "body_api.h",
                                 .suffix = "body_api.list_T",
                                 .label = "Type",
                                 .declaration_line = 9,
                                 .declaration_text =
                                     "typedef struct list_S { int len; struct list_S *next; } list_T;"};
    const RQ_Target list_len = {.file = "body_api.h",
                                .suffix = "body_api.list_S.len",
                                .label = "Field",
                                .declaration_line = 9,
                                .declaration_text =
                                    "typedef struct list_S { int len; struct list_S *next; } list_T;"};
    const RQ_Target failure = {.file = "body_api.c",
                               .suffix = "body_api.report_failure",
                               .label = "Function",
                               .declaration_line = 1,
                               .declaration_text =
                                   "void report_failure(const char *what) { (void)what; }"};
    const RQ_Target counter = {.file = "body_api.c",
                               .suffix = "body_api.counter",
                               .label = "Variable",
                               .declaration_line = 2,
                               .declaration_text = "int counter;"};
    const RQ_Expected expected[] = {
        holder_row,
        {.case_id = "call-in-a-replacement",
         .site = {.file = "body_use.c", .caller = "body_use.checked", .needle = "CHECK(v)"},
         .relation = "CALLS",
         .target = failure},
        {.case_id = "read-in-a-replacement-without-parameters",
         .site = {.file = "body_use.c", .caller = "body_use.reads", .needle = "READ_COUNTER()"},
         .relation = "USAGE",
         .target = counter},
        {.case_id = "pasted-name",
         .site = {.file = "body_use.c", .caller = "body_use.registers", .needle = "REGISTER(Open)"},
         .relation = "USAGE",
         .target = {.file = "body_api.c",
                    .suffix = "body_api.RM_Open",
                    .label = "Function",
                    .declaration_line = 3,
                    .declaration_text = "int RM_Open(int v) { return v; }"}},
        {.case_id = "call-beside-the-pasted-name",
         .site = {.file = "body_use.c", .caller = "body_use.registers", .needle = "REGISTER(Open)"},
         .relation = "CALLS",
         .target = {.file = "body_api.c",
                    .suffix = "body_api.register_it",
                    .label = "Function",
                    .declaration_line = 4,
                    .declaration_text = "int register_it(int (*f)(int)) { return f(0); }"}},
        {.case_id = "object-like-macro-at-a-value",
         .site = {.file = "body_use.c", .caller = "body_use.reads_object", .needle = "CURRENT"},
         .relation = "USAGE",
         .target = counter},
        {.case_id = "object-like-rename-at-a-call",
         .site = {.file = "body_use.c", .caller = "body_use.renamed", .needle = "sd_new(v)"},
         .relation = "CALLS",
         .target = {.file = "body_api.c",
                    .suffix = "body_api.hi_new",
                    .label = "Function",
                    .declaration_line = 5,
                    .declaration_text = "int hi_new(int v) { return v; }"}},
        {.case_id = "member-is-the-field-and-not-the-function-of-that-name",
         .site = {.file = "body_use.c", .caller = "body_use.member", .needle = "MEMBER_OF(h)"},
         .relation = "USAGE",
         .target = {.file = "body_use.c",
                    .suffix = "body_use.holder.report_failure",
                    .label = "Field",
                    .declaration_line = 14,
                    .declaration_text = "struct holder { int report_failure; };"}},
        {.case_id = "type-a-template-scope-names",
         .site = {.file = "qualified.cpp", .caller = "qualified.through_template", .needle = "TCALL(v)"},
         .relation = "USAGE",
         .target = {.file = "qualified.cpp",
                    .suffix = "qualified.Holder",
                    .label = "Class",
                    .declaration_line = 6,
                    .declaration_text =
                        "template <class T> struct Holder { static int qualified_helper(int v) { return v; } };"}},
        {.case_id = "word-handed-to-a-function-this-unit-sees",
         .site = {.file = "body_use.c",
                  .caller = "body_use.handed_function",
                  .needle = "HAND_TO_FUNCTION(v)"},
         .relation = "USAGE",
         .target = counter},
        {.case_id = "the-function-it-is-handed-to",
         .site = {.file = "body_use.c",
                  .caller = "body_use.handed_function",
                  .needle = "HAND_TO_FUNCTION(v)"},
         .relation = "CALLS",
         .target = {.file = "body_api.c",
                    .suffix = "body_api.register_value",
                    .label = "Function",
                    .declaration_line = 7,
                    .declaration_text = "int register_value(int v, int w) { return v + w; }"}},
        {.case_id = "type-a-macro-renames",
         .site = C_TU_WORD("body_use.c", body_use, "body_use.through_alias", "(list_alias", "list_alias"),
         .relation = "USAGE",
         .target = list_type},
        {.case_id = "parameter-type-of-the-member-read",
         .site = C_TU_WORD("body_use.c", body_use, "body_use.read_len", "read_len(", "list_T"),
         .relation = "USAGE",
         .target = list_type},
        {.case_id = "member-read-through-a-parameter",
         .site = {.file = "body_use.c", .caller = "body_use.read_len", .needle = "LIST_LEN(l)"},
         .relation = "USAGE",
         .target = list_len},
        {.case_id = "parameter-type-of-the-member-chain",
         .site = C_TU_WORD("body_use.c", body_use, "body_use.read_next", "read_next(", "list_T"),
         .relation = "USAGE",
         .target = list_type},
        {.case_id = "first-member-of-a-chain",
         .site = {.file = "body_use.c", .caller = "body_use.read_next", .needle = "NEXT_LEN(l)"},
         .relation = "USAGE",
         .target = {.file = "body_api.h",
                    .suffix = "body_api.list_S.next",
                    .label = "Field",
                    .declaration_line = 9,
                    .declaration_text = "typedef struct list_S { int len; struct list_S *next; } list_T;"},
         .dispatch_group = "chain"},
        {.case_id = "second-member-of-a-chain",
         .site = {.file = "body_use.c", .caller = "body_use.read_next", .needle = "NEXT_LEN(l)"},
         .relation = "USAGE",
         .target = list_len,
         .dispatch_group = "chain"},
        {.case_id = "parameter-type-of-the-call-decoy",
         .site = C_TU_WORD("body_use.c", body_use, "body_use.through_call", "(other_T", "other_T"),
         .relation = "USAGE",
         .target = {.file = "body_api.h",
                    .suffix = "body_api.other_T",
                    .label = "Type",
                    .declaration_line = 12,
                    .declaration_text = "typedef struct other_S { int len; } other_T;"}},
        {.case_id = "every-definition-names-it",
         .site = {.file = "body_use.c", .caller = "body_use.both_branches", .needle = "BOTH_BRANCHES(v)"},
         .relation = "CALLS",
         .target = failure},
        {.case_id = "macro-defined-in-the-function-body",
         .site = {.file = "body_use.c", .caller = "body_use.in_body", .needle = "BODY_CHECK(v)"},
         .relation = "CALLS",
         .target = failure},
        {.case_id = "macro-handed-on-by-a-macro",
         .site = {.file = "body_use.c", .caller = "body_use.nested", .needle = "TWICE(v)"},
         .relation = "CALLS",
         .target = failure},
        {.case_id = "qualified-name-in-its-namespace",
         .site = {.file = "qualified.cpp", .caller = "qualified.qualified", .needle = "QCALL(v)"},
         .relation = "CALLS",
         .target = {.file = "qualified.cpp",
                    .suffix = "qualified.other.qualified_helper",
                    .label = "Function",
                    .declaration_line = 4,
                    .declaration_text = "int qualified_helper(int v) { return v + 1; }"}},
        {.case_id = "global-name-from-a-member-function",
         .site = {.file = "qualified.cpp", .caller = "qualified.Reaches.global", .needle = "GLOBAL(v)"},
         .relation = "CALLS",
         .target = {.file = "qualified.cpp",
                    .suffix = "qualified.other.qualified_helper",
                    .label = "Function",
                    .declaration_line = 4,
                    .declaration_text = "int qualified_helper(int v) { return v + 1; }"}},
    };
    const RQ_Forbidden forbidden[] = {
        {.case_id = "the-member-of-what-a-call-returns",
         .site = {.file = "body_use.c", .caller = "body_use.through_call", .needle = "THROUGH_CALL(o)"},
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "receiver-of-no-known-type",
         .site = {.file = "body_use.c", .caller = "body_use.no_receiver", .needle = "LIST_LEN(p)"},
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "parameter-of-a-prototype-in-a-replacement",
         .site = {.file = "body_use.c",
                  .caller = "body_use.prototype_parameter",
                  .needle = "PROTOTYPE_PARAMETER(v)"},
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "object-whose-tag-has-its-spelling",
         .site = {.file = "body_use.c", .caller = "body_use.read_object", .needle = "READ_OBJECT()"},
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "branch-of-a-generic-selection",
         .site = {.file = "body_use.c", .caller = "body_use.selected", .needle = "SELECT_BY_TYPE(v)"},
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "word-a-macro-reads-as-a-member",
         .site = {.file = "body_use.c", .caller = "body_use.handed_member", .needle = "HAND_ON_MEMBER(p)"},
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "word-handed-to-a-callee-nothing-declares",
         .site = {.file = "body_use.c", .caller = "body_use.handed_unknown", .needle = "HAND_TO_UNKNOWN(v)"},
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "word-offsetof-reads-as-a-member",
         .site = {.file = "body_use.c",
                  .caller = "body_use.handed_offset",
                  .needle = "HAND_THROUGH_OFFSET(list_T)"},
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "this-file-undefined-the-macro",
         .site = {.file = "body_use.c",
                  .caller = "body_use.after_undef",
                  .needle = "CHECK(v)",
                  .occurrence_index = 2},
         .relation = "CALLS",
         .any_target = true},
        {.case_id = "only-one-definition-names-it",
         .site = {.file = "body_use.c", .caller = "body_use.one_branch", .needle = "ONE_BRANCH(v)"},
         .relation = "CALLS",
         .any_target = true},
        {.case_id = "only-the-other-definition-names-it",
         .site = {.file = "body_use.c", .caller = "body_use.later_branch", .needle = "LATER_BRANCH(v)"},
         .relation = "CALLS",
         .any_target = true},
        {.case_id = "local-at-the-use-takes-the-name",
         .site = {.file = "body_use.c", .caller = "body_use.shadowed", .needle = "READ_COUNTER()"},
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "the-macros-own-local-takes-the-name",
         .site = {.file = "body_use.c", .caller = "body_use.own_local", .needle = "HELPER_LOCAL(v)"},
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "nothing-declares-the-function-here",
         .site = {.file = "hidden_use.c", .caller = "hidden_use.undeclared", .needle = "CALL_HIDDEN(v)"},
         .relation = "CALLS",
         .any_target = true},
        {.case_id = "a-template-scope-is-not-read",
         .site = {.file = "qualified.cpp",
                  .caller = "qualified.through_template",
                  .needle = "TCALL(v)"},
         .relation = "CALLS",
         .any_target = true},
        {.case_id = "the-class-comes-first-in-a-member-function",
         .site = {.file = "qualified.cpp", .caller = "qualified.Own.plain", .needle = "PLAIN(v)"},
         .relation = "CALLS",
         .any_target = true},
    };
    const CTUCase fixture = {.suite = "c-tu-macro-body",
                             .sources = C_TU_BODY,
                             .source_count = sizeof(C_TU_BODY) / sizeof(C_TU_BODY[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0]),
                             .forbidden = forbidden,
                             .forbidden_count = sizeof(forbidden) / sizeof(forbidden[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* Lua's lauxlib.h writes `#if COMPAT / #define luaL_getn(L,i) ... / #else / int (luaL_getn)(...); /
 * #endif`, and xxhash defines XXH_swap64 beside a builtin the same way: the function is one branch
 * of a conditional that is read in full. A macro with no such partner still decides the name. */
#define C_TU_ALT_H                                                                              \
    "#ifndef ALT_H\n"                                                                           \
    "#define ALT_H\n"                                                                           \
    "#if defined(USE_COMPAT)\n"                                                                 \
    "#define get_len(x) (x)\n"                                                                  \
    "#else\n"                                                                                   \
    "int (get_len)(int x);\n"                                                                   \
    "#endif\n"                                                                                  \
    "#if defined(USE_BUILTIN)\n"                                                                \
    "#define swap_bytes __builtin_bswap32\n"                                                    \
    "#else\n"                                                                                   \
    "static int swap_bytes(int v) { return v; }\n"                                              \
    "#endif\n"                                                                                  \
    "int always(int x);\n"                                                                      \
    "#define always(x) (x)\n"                                                                   \
    "#if defined(ONLY_MACRO)\n"                                                                 \
    "#define sometimes(x) (x)\n"                                                                \
    "#endif\n"                                                                                  \
    "int sometimes(int x);\n"                                                                   \
    "int plain_control(int x);\n"                                                               \
    "#endif\n"
#define C_TU_ALT_C                                                                              \
    "int (get_len)(int x) { return x; }\n"                                                      \
    "int (always)(int x) { return x; }\n"                                                       \
    "int (sometimes)(int x) { return x; }\n" \
    "int (plain_control)(int x) { return x; }\n"
#define C_TU_ALT_USE                                                                            \
    "#include \"alt.h\"\n"                                                                      \
    "int use_len(int v) { return get_len(v); }\n"                                               \
    "int use_swap(int v) { return swap_bytes(v); }\n"                                           \
    "int use_always(int v) { return always(v); }\n"                                             \
    "int use_sometimes(int v) { return sometimes(v); }\n"                                        \
    "int use_plain(int v) { return plain_control(v); }\n"
static const RQ_Source C_TU_ALT[] = {{.path = "alt.h", .text = C_TU_ALT_H},
                                     {.path = "alt.c", .text = C_TU_ALT_C},
                                     {.path = "alt_use.c", .text = C_TU_ALT_USE}};

/* A macro in one branch and the function in the other: in one configuration this is a call and in the
 * other it may be no call at all, so with no compile configuration the name is unknown. An edge here
 * would hold only by chance. See also pipeline_c_call_targets_definition_before_macro_c1. */
TEST(resolver_c_macro_in_one_branch_leaves_the_name_unknown) {
    const RQ_Expected expected[] = {
        {.case_id = "a-name-no-macro-touches",
         .site = {.file = "alt_use.c", .caller = "alt_use.use_plain", .needle = "plain_control(v)"},
         .relation = "CALLS",
         .target = {.file = "alt.c",
                    .suffix = "alt.plain_control",
                    .label = "Function",
                    .declaration_line = 4,
                    .declaration_text = "int (plain_control)(int x) { return x; }"}},
    };
    const RQ_Forbidden forbidden[] = {
        {.case_id = "prototype-in-the-other-branch",
         .site = {.file = "alt_use.c", .caller = "alt_use.use_len", .needle = "get_len(v)"},
         .relation = "CALLS",
         .any_target = true},
        {.case_id = "definition-in-the-other-branch",
         .site = {.file = "alt_use.c", .caller = "alt_use.use_swap", .needle = "swap_bytes(v)"},
         .relation = "CALLS",
         .any_target = true},
        {.case_id = "macro-outside-any-conditional",
         .site = {.file = "alt_use.c", .caller = "alt_use.use_always", .needle = "always(v)"},
         .relation = "CALLS",
         .any_target = true},
        {.case_id = "macro-in-a-conditional-with-no-other-branch",
         .site = {.file = "alt_use.c", .caller = "alt_use.use_sometimes", .needle = "sometimes(v)"},
         .relation = "CALLS",
         .any_target = true},
    };
    const CTUCase fixture = {.suite = "c-tu-macro-branch",
                             .sources = C_TU_ALT,
                             .source_count = sizeof(C_TU_ALT) / sizeof(C_TU_ALT[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0]),
                             .forbidden = forbidden,
                             .forbidden_count = sizeof(forbidden) / sizeof(forbidden[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* A macro after the parameters leaves the word as a statement of its own: redismodule.h lost its
 * RedisModule_Init that way, to 500 lines of `(*RedisModule_X)(...) REDISMODULE_ATTR;`. */
#define C_TU_TRAIL_H                                                                            \
    "#ifndef TRAIL_H\n"                                                                         \
    "#define TRAIL_H\n"                                                                         \
    "#define API\n"                                                                             \
    "#define API_ATTR __attribute__((common))\n"                                                \
    "#define API_UNUSED __attribute__((unused))\n"                                              \
    "API int (*Api_Open)(int v) API_ATTR;\n"                                                    \
    "API int (*Api_Close)(int v) API_ATTR;\n"                                                   \
    "API int (*Api_Read)(int v) API_ATTR;\n"                                                    \
    "API int Api_Twice(int v) API_ATTR API_UNUSED;\n"                                           \
    "API int Api_Other(int v) API_ATTR API_UNUSED;\n"                                           \
    "#define Api_IsReady(id) ((id) == 0)\n"                                                     \
    "static int Api_Init(int v) API_UNUSED;\n"                                                  \
    "static int Api_Init(int v) {\n"                                                            \
    "    return v;\n"                                                                           \
    "}\n"                                                                                       \
    "#endif\n"
#define C_TU_TRAIL_C "#include \"trail.h\"\nint Api_Twice(int v) { return v; }\n"
#define C_TU_TRAIL_USE                                                                          \
    "#include \"trail.h\"\n"                                                                    \
    "int on_load(int v) { return Api_Init(v); }\n"                                              \
    "int on_twice(int v) { return Api_Twice(v); }\n"
static const RQ_Source C_TU_TRAIL[] = {{.path = "trail.h", .text = C_TU_TRAIL_H},
                                       {.path = "trail.c", .text = C_TU_TRAIL_C},
                                       {.path = "trail_use.c", .text = C_TU_TRAIL_USE}};

TEST(resolver_c_macro_after_the_parameters_keeps_what_follows) {
    const RQ_Expected expected[] = {
        {.case_id = "definition-after-attribute-macro-prototypes",
         .site = {.file = "trail_use.c", .caller = "trail_use.on_load", .needle = "Api_Init(v)"},
         .relation = "CALLS",
         .target = {.file = "trail.h",
                    .suffix = "trail.Api_Init",
                    .label = "Function",
                    .declaration_line = 13,
                    .declaration_text = "static int Api_Init(int v) {"}},
        {.case_id = "prototype-between-two-attribute-macros",
         .site = {.file = "trail_use.c", .caller = "trail_use.on_twice", .needle = "Api_Twice(v)"},
         .relation = "CALLS",
         .target = {.file = "trail.c",
                    .suffix = "trail.Api_Twice",
                    .label = "Function",
                    .declaration_line = 2,
                    .declaration_text = "int Api_Twice(int v) { return v; }"}},
    };
    const CTUCase fixture = {.suite = "c-tu-trailing-macro",
                             .sources = C_TU_TRAIL,
                             .source_count = sizeof(C_TU_TRAIL) / sizeof(C_TU_TRAIL[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* redismodule.h: `#if defined(REDISMODULE_CORE) / #define RedisModuleString robj / #endif`, and only
 * server.h defines the switch before including it. A module's unit leaves the switch off, so the
 * name is the type. A switch nothing in the project defines may come from the build. */
#define C_TU_SWITCH_API                                                                         \
    "#ifndef API_H\n"                                                                           \
    "#define API_H\n"                                                                           \
    "#if defined(API_CORE)\n"                                                                   \
    "#define Thing core_thing\n"                                                                \
    "#endif\n"                                                                                  \
    "#ifndef API_CORE\n"                                                                        \
    "typedef struct Thing Thing;\n"                                                             \
    "#endif\n"                                                                                  \
    "typedef struct Other Other;\n"                                                             \
    "#ifdef FROM_THE_BUILD\n"                                                                   \
    "#define Other build_other\n"                                                               \
    "#endif\n"                                                                                  \
    "#endif\n"
#define C_TU_SWITCH_CORE "#define API_CORE 1\n#include \"api.h\"\ntypedef int core_thing;\n"
#define C_TU_SWITCH_MODULE                                                                      \
    "#include \"api.h\"\n"                                                                      \
    "int module_use(Thing *t) { return t != 0; }\n"                                             \
    "int module_other(Other *t) { return t != 0; }\n"
#define C_TU_SWITCH_INSIDE                                                                      \
    "#include \"core.h\"\n"                                                                     \
    "int inside_use(Thing *t) { return t != 0; }\n"
static const RQ_Source C_TU_SWITCH[] = {{.path = "api.h", .text = C_TU_SWITCH_API},
                                        {.path = "core.h", .text = C_TU_SWITCH_CORE},
                                        {.path = "module_use.c", .text = C_TU_SWITCH_MODULE},
                                        {.path = "inside_use.c", .text = C_TU_SWITCH_INSIDE}};

TEST(resolver_c_macro_behind_a_switch_this_unit_leaves_off) {
    const char *module = C_TU_SWITCH_MODULE;
    const char *inside = C_TU_SWITCH_INSIDE;
    const RQ_Expected expected[] = {
        {.case_id = "type-where-the-switch-is-off",
         .site = C_TU_WORD("module_use.c", module, "module_use.module_use", "(Thing", "Thing"),
         .relation = "USAGE",
         .target = {.file = "api.h",
                    .suffix = "api.Thing",
                    .label = "Type",
                    .declaration_line = 7,
                    .declaration_text = "typedef struct Thing Thing;"}},
        {.case_id = "what-the-macro-names-where-the-switch-is-on",
         .site = C_TU_WORD("inside_use.c", inside, "inside_use.inside_use", "(Thing", "Thing"),
         .relation = "USAGE",
         .target = {.file = "core.h",
                    .suffix = "core.core_thing",
                    .label = "Type",
                    .declaration_line = 3,
                    .declaration_text = "typedef int core_thing;"}},
    };
    const RQ_Forbidden forbidden[] = {
        {.case_id = "a-switch-only-the-build-can-set",
         .site = C_TU_WORD("module_use.c", module, "module_use.module_other", "(Other", "Other"),
         .relation = "USAGE",
         .any_target = true},
    };
    const CTUCase fixture = {.suite = "c-tu-switch",
                             .sources = C_TU_SWITCH,
                             .source_count = sizeof(C_TU_SWITCH) / sizeof(C_TU_SWITCH[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0]),
                             .forbidden = forbidden,
                             .forbidden_count = sizeof(forbidden) / sizeof(forbidden[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* `typval_T argv[3];` with no initializer bound no type, where `typval_T argv[] = {...}` did. neovim's
 * tv_dict_watcher_notify, parse_csi and suggest_trie_walk read members through such arrays. */
#define C_TU_ARR_H                                                                              \
    "#ifndef ARR_H\n"                                                                           \
    "#define ARR_H\n"                                                                           \
    "typedef struct item_S { int val; } item_T;\n"                                              \
    "typedef struct other_S { int more; } other_T;\n"                                           \
    "#endif\n"
#define C_TU_ARR_USE                                                                            \
    "#include \"arr.h\"\n"                                                                      \
    "int local_array(int i) { item_T argv[3]; return argv[i].val; }\n"                          \
    "int local_pointers(int i) { item_T *ptrs[3]; return ptrs[i]->val; }\n"                     \
    "int two_dimensions(int i) { item_T grid[2][2]; return grid[i][i].val; }\n"                 \
    "int no_such_member(int i) { other_T o[2]; return o[i].val; }\n"
static const RQ_Source C_TU_ARR[] = {{.path = "arr.h", .text = C_TU_ARR_H},
                                     {.path = "arr_use.c", .text = C_TU_ARR_USE}};

TEST(resolver_c_local_array_types_its_elements) {
    const char *use = C_TU_ARR_USE;
    const RQ_Target val = {.file = "arr.h",
                           .suffix = "arr.item_S.val",
                           .label = "Field",
                           .declaration_line = 3,
                           .declaration_text = "typedef struct item_S { int val; } item_T;"};
    const RQ_Target item = {.file = "arr.h",
                            .suffix = "arr.item_T",
                            .label = "Type",
                            .declaration_line = 3,
                            .declaration_text = "typedef struct item_S { int val; } item_T;"};
    const RQ_Expected expected[] = {
        {.case_id = "type-of-a-local-array",
         .site = C_TU_WORD("arr_use.c", use, "arr_use.local_array", "item_T argv", "item_T"),
         .relation = "USAGE",
         .target = item},
        {.case_id = "member-of-an-element",
         .site = C_TU_WORD("arr_use.c", use, "arr_use.local_array", "argv[i].val", "val"),
         .relation = "USAGE",
         .target = val},
        {.case_id = "type-of-an-array-of-pointers",
         .site = C_TU_WORD("arr_use.c", use, "arr_use.local_pointers", "item_T *ptrs", "item_T"),
         .relation = "USAGE",
         .target = item},
        {.case_id = "member-through-an-element-pointer",
         .site = C_TU_WORD("arr_use.c", use, "arr_use.local_pointers", "ptrs[i]->val", "val"),
         .relation = "USAGE",
         .target = val},
        {.case_id = "type-of-two-dimensions",
         .site = C_TU_WORD("arr_use.c", use, "arr_use.two_dimensions", "item_T grid", "item_T"),
         .relation = "USAGE",
         .target = item},
        {.case_id = "member-of-an-element-of-an-element",
         .site = C_TU_WORD("arr_use.c", use, "arr_use.two_dimensions", "grid[i][i].val", "val"),
         .relation = "USAGE",
         .target = val},
        {.case_id = "type-of-the-decoy-array",
         .site = C_TU_WORD("arr_use.c", use, "arr_use.no_such_member", "other_T o", "other_T"),
         .relation = "USAGE",
         .target = {.file = "arr.h",
                    .suffix = "arr.other_T",
                    .label = "Type",
                    .declaration_line = 4,
                    .declaration_text = "typedef struct other_S { int more; } other_T;"}},
    };
    const RQ_Forbidden forbidden[] = {
        {.case_id = "a-member-the-element-type-lacks",
         .site = C_TU_WORD("arr_use.c", use, "arr_use.no_such_member", "o[i].val", "val"),
         .relation = "USAGE",
         .any_target = true},
    };
    const CTUCase fixture = {.suite = "c-tu-local-arrays",
                             .sources = C_TU_ARR,
                             .source_count = sizeof(C_TU_ARR) / sizeof(C_TU_ARR[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0]),
                             .forbidden = forbidden,
                             .forbidden_count = sizeof(forbidden) / sizeof(forbidden[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* `union { ... } data;` has no tag, so its members had no node and the chain through it stopped:
 * neovim's Object.data, redis's RedisModuleKey.u, llvm's APInt.U. It is a class named for its owner
 * and the member. With no declarator its members are members of the class around it. */
#define C_TU_AGG_H                                                                              \
    "#ifndef AGG_H\n"                                                                           \
    "#define AGG_H\n"                                                                           \
    "typedef struct { int len; } Text;\n"                                                       \
    "typedef struct {\n"                                                                        \
    "  int kind;\n"                                                                             \
    "  union { int number; Text text; } data;\n"                                                \
    "  struct { int depth; struct { int leaf; } inner; } levels[2];\n"                          \
    "  union { int first; int second; };\n"                                                     \
    "} Value;\n"                                                                                \
    "typedef struct {\n"                                                                        \
    "  union { int number; } data;\n"                                                           \
    "} Other;\n"                                                                                \
    "#endif\n"
#define C_TU_AGG_USE                                                                            \
    "#include \"agg.h\"\n"                                                                      \
    "void write_number(Value *v) { v->data.number = 1; }\n"                                     \
    "int read_text(Value v) { return v.data.text.len; }\n"                                      \
    "int read_leaf(Value *v) { return v->levels[1].inner.leaf; }\n"                             \
    "int read_flat(Value *v) { return v->first; }\n"                                            \
    "int read_other(Other *o) { return o->data.number; }\n"                                     \
    "int read_absent(Value *v) { return v->data.absent; }\n"
#define C_TU_AGG_SAME                                                                           \
    "typedef struct { union { int here; } local; } Own;\n"                                      \
    "int same_file(Own *p) { return p->local.here; }\n"
static const RQ_Source C_TU_AGG[] = {{.path = "agg.h", .text = C_TU_AGG_H},
                                     {.path = "agg_use.c", .text = C_TU_AGG_USE},
                                     {.path = "agg_same.c", .text = C_TU_AGG_SAME}};

TEST(resolver_c_unnamed_aggregate_is_a_class_of_its_own) {
    const char *use = C_TU_AGG_USE;
    const char *same = C_TU_AGG_SAME;
    static const char data_line[] = "  union { int number; Text text; } data;";
    static const char levels_line[] = "  struct { int depth; struct { int leaf; } inner; } levels[2];";
    static const char flat_line[] = "  union { int first; int second; };";
    static const char other_data_line[] = "  union { int number; } data;";
#define AGG_FIELD(suffix_, line_, text_)                                                        \
    {.file = "agg.h", .suffix = suffix_, .label = "Field", .declaration_line = line_,            \
     .declaration_text = text_}
#define AGG_USAGE(id, caller, context, word, target_)                                           \
    {.case_id = id,                                                                              \
     .site = C_TU_WORD("agg_use.c", use, "agg_use." caller, context, word),                      \
     .relation = "USAGE",                                                                        \
     .target = target_}
    const RQ_Target value = {.file = "agg.h",
                             .suffix = "agg.Value",
                             .label = "Class",
                             .declaration_line = 4,
                             .declaration_text = "typedef struct {"};
    const RQ_Target data = AGG_FIELD("agg.Value.data", 6, data_line);
    const RQ_Expected expected[] = {
        AGG_USAGE("type-of-the-writer", "write_number", "Value *v) { v->data.number", "Value", value),
        {.case_id = "write-into-the-union",
         .site = C_TU_WORD("agg_use.c", use, "agg_use.write_number", "number = 1", "number"),
         .relation = "WRITES",
         .target = AGG_FIELD("agg.Value$data.number", 6, data_line)},
        AGG_USAGE("type-of-the-reader", "read_text", "Value v) {", "Value", value),
        AGG_USAGE("the-member-holding-the-union", "read_text", "data.text.len", "data", data),
        AGG_USAGE("a-member-of-the-union", "read_text", "text.len", "text",
                  AGG_FIELD("agg.Value$data.text", 6, data_line)),
        AGG_USAGE("the-chain-goes-on-through-its-type", "read_text", "len; }", "len",
                  AGG_FIELD("agg.Text.len", 3, "typedef struct { int len; } Text;")),
        AGG_USAGE("type-of-the-deep-reader", "read_leaf", "Value *v) { return v->levels", "Value",
                  value),
        AGG_USAGE("an-array-of-an-unnamed-struct", "read_leaf", "levels[1]", "levels",
                  AGG_FIELD("agg.Value.levels", 7, levels_line)),
        AGG_USAGE("an-unnamed-struct-inside-one", "read_leaf", "inner.leaf", "inner",
                  AGG_FIELD("agg.Value$levels.inner", 7, levels_line)),
        AGG_USAGE("a-member-two-levels-in", "read_leaf", "leaf; }", "leaf",
                  AGG_FIELD("agg.Value$levels$inner.leaf", 7, levels_line)),
        AGG_USAGE("type-of-the-flat-reader", "read_flat", "Value *v) { return v->first", "Value",
                  value),
        AGG_USAGE("no-declarator-belongs-to-the-class-around-it", "read_flat", "first; }", "first",
                  AGG_FIELD("agg.Value.first", 8, flat_line)),
        AGG_USAGE("type-of-the-other-struct", "read_other", "Other *o", "Other",
                  ((RQ_Target){.file = "agg.h",
                               .suffix = "agg.Other",
                               .label = "Class",
                               .declaration_line = 10,
                               .declaration_text = "typedef struct {"})),
        AGG_USAGE("its-own-member-of-that-name", "read_other", "data.number; }", "data",
                  AGG_FIELD("agg.Other.data", 11, other_data_line)),
        AGG_USAGE("its-own-union-not-the-namesake", "read_other", "number; }", "number",
                  AGG_FIELD("agg.Other$data.number", 11, other_data_line)),
        AGG_USAGE("type-of-the-absent-reader", "read_absent", "Value *v) { return v->data.absent",
                  "Value", value),
        AGG_USAGE("the-member-before-the-absent-one", "read_absent", "data.absent", "data", data),
        {.case_id = "type-in-the-same-file",
         .site = C_TU_WORD("agg_same.c", same, "agg_same.same_file", "Own *p", "Own"),
         .relation = "USAGE",
         .target = {.file = "agg_same.c",
                    .suffix = "agg_same.Own",
                    .label = "Class",
                    .declaration_line = 1,
                    .declaration_text = "typedef struct { union { int here; } local; } Own;"}},
        {.case_id = "member-in-the-same-file",
         .site = C_TU_WORD("agg_same.c", same, "agg_same.same_file", "local.here", "local"),
         .relation = "USAGE",
         .target = {.file = "agg_same.c",
                    .suffix = "agg_same.Own.local",
                    .label = "Field",
                    .declaration_line = 1,
                    .declaration_text = "typedef struct { union { int here; } local; } Own;"}},
        {.case_id = "through-a-union-in-the-same-file",
         .site = C_TU_WORD("agg_same.c", same, "agg_same.same_file", "here; }\n", "here"),
         .relation = "USAGE",
         .target = {.file = "agg_same.c",
                    .suffix = "agg_same.Own$local.here",
                    .label = "Field",
                    .declaration_line = 1,
                    .declaration_text = "typedef struct { union { int here; } local; } Own;"}},
    };
#undef AGG_USAGE
#undef AGG_FIELD
    const RQ_Forbidden forbidden[] = {
        {.case_id = "a-member-the-union-lacks",
         .site = C_TU_WORD("agg_use.c", use, "agg_use.read_absent", "absent; }", "absent"),
         .relation = "USAGE",
         .any_target = true},
    };
    const CTUCase fixture = {.suite = "c-tu-unnamed-aggregates",
                             .sources = C_TU_AGG,
                             .source_count = sizeof(C_TU_AGG) / sizeof(C_TU_AGG[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0]),
                             .forbidden = forbidden,
                             .forbidden_count = sizeof(forbidden) / sizeof(forbidden[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* `ns::Name` written outside ns stayed unbound, so nothing read through it bound either: every
 * `llvm::StringRef` in clang and `cl::opt` under `using namespace llvm`. The qualifier is matched
 * whole against the declaration's name, from a place the lookup of its first segment reaches. */
#define C_TU_Q_H                                                                                \
    "#ifndef Q_HPP\n"                                                                           \
    "#define Q_HPP\n"                                                                           \
    "namespace ns {\n"                                                                          \
    "struct Name { int in_ns; };\n"                                                             \
    "template <typename T> struct Box { T box_field; };\n"                                      \
    "}\n"                                                                                       \
    "namespace other {\n"                                                                       \
    "struct Name { int in_other; };\n"                                                          \
    "}\n"                                                                                       \
    "namespace outer {\n"                                                                       \
    "namespace in2 {\n"                                                                         \
    "struct Deep { int deep_field; };\n"                                                        \
    "}\n"                                                                                       \
    "}\n"                                                                                       \
    "namespace elsewhere {\n"                                                                   \
    "namespace hidden {\n"                                                                      \
    "struct Secret { int secret_field; };\n"                                                    \
    "}\n"                                                                                       \
    "}\n"                                                                                       \
    "namespace mid {\n"                                                                         \
    "namespace sub {\n"                                                                         \
    "struct Sub { int sub_field; };\n"                                                          \
    "}\n"                                                                                       \
    "namespace twice {\n"                                                                       \
    "struct Twice { int twice_field; };\n"                                                      \
    "}\n"                                                                                       \
    "}\n"                                                                                       \
    "namespace twice {\n"                                                                       \
    "struct Twice { int twice_field; };\n"                                                      \
    "}\n"                                                                                       \
    "template <typename T> struct Plain { T plain_field; };\n"                                  \
    "struct Holder { struct Inner { int inner_field; }; };\n"                                   \
    "#endif\n"
#define C_TU_Q_USE                                                                              \
    "#include \"q.hpp\"\n"                                                                      \
    "int two_namespaces(ns::Name *a, other::Name *b) { return a->in_ns + b->in_other; }\n"      \
    "using namespace outer;\n"                                                                  \
    "int through_using(in2::Deep *d) { return d->deep_field; }\n"                               \
    "int not_reachable(hidden::Secret *s) { return s->secret_field; }\n"                        \
    "namespace mid {\n"                                                                         \
    "int enclosing(sub::Sub *q) { return q->sub_field; }\n"                                     \
    "int two_candidates(twice::Twice *t) { return t->twice_field; }\n"                          \
    "}\n"                                                                                       \
    "int templates(ns::Box<int> *b, Plain<int> p) { return b->box_field + p.plain_field; }\n"   \
    "int rooted(::ns::Name *a) { return a->in_ns; }\n"                                          \
    "int nested(Holder::Inner *i) { return i->inner_field; }\n"                                 \
    "template <template <typename> class T> int parameter(T<int> *t) { return 0; }\n"
static const RQ_Source C_TU_Q[] = {{.path = "q.hpp", .text = C_TU_Q_H},
                                   {.path = "q_use.cpp", .text = C_TU_Q_USE}};

TEST(resolver_cpp_qualified_type_binds_from_where_its_lookup_starts) {
    const char *use = C_TU_Q_USE;
#define Q_TARGET(suffix_, label_, line_, text_)                                                 \
    ((RQ_Target){.file = "q.hpp",                                                                \
                 .suffix = suffix_,                                                              \
                 .label = label_,                                                                \
                 .declaration_line = line_,                                                      \
                 .declaration_text = text_})
#define Q_USAGE(id, caller, context, word, target_)                                             \
    {.case_id = id,                                                                              \
     .site = C_TU_WORD("q_use.cpp", use, "q_use." caller, context, word),                        \
     .relation = "USAGE",                                                                        \
     .target = target_}
    static const char name_line[] = "struct Name { int in_ns; };";
    static const char other_line[] = "struct Name { int in_other; };";
    static const char box_line[] = "template <typename T> struct Box { T box_field; };";
    static const char plain_line[] = "template <typename T> struct Plain { T plain_field; };";
    static const char holder_line[] = "struct Holder { struct Inner { int inner_field; }; };";
    static const char deep_line[] = "struct Deep { int deep_field; };";
    static const char sub_line[] = "struct Sub { int sub_field; };";
    const RQ_Expected expected[] = {
        Q_USAGE("the-namespace-it-names", "two_namespaces", "ns::Name *a", "Name",
                Q_TARGET("q.ns.Name", "Class", 4, name_line)),
        Q_USAGE("the-other-namesake", "two_namespaces", "other::Name *b", "Name",
                Q_TARGET("q.other.Name", "Class", 8, other_line)),
        Q_USAGE("a-member-through-the-first", "two_namespaces", "in_ns +", "in_ns",
                Q_TARGET("q.ns.Name.in_ns", "Field", 4, name_line)),
        Q_USAGE("a-member-through-the-second", "two_namespaces", "in_other; }", "in_other",
                Q_TARGET("q.other.Name.in_other", "Field", 8, other_line)),
        Q_USAGE("from-a-using-directive", "through_using", "in2::Deep", "Deep",
                Q_TARGET("q.outer.in2.Deep", "Class", 12, deep_line)),
        Q_USAGE("a-member-from-a-using-directive", "through_using", "deep_field; }", "deep_field",
                Q_TARGET("q.outer.in2.Deep.deep_field", "Field", 12, deep_line)),
        Q_USAGE("from-the-namespace-around-it", "mid.enclosing", "sub::Sub", "Sub",
                Q_TARGET("q.mid.sub.Sub", "Class", 22, sub_line)),
        Q_USAGE("a-member-from-the-namespace-around-it", "mid.enclosing", "sub_field; }", "sub_field",
                Q_TARGET("q.mid.sub.Sub.sub_field", "Field", 22, sub_line)),
        Q_USAGE("a-qualified-template", "templates", "ns::Box<int>", "Box",
                Q_TARGET("q.ns.Box", "Class", 5, box_line)),
        Q_USAGE("a-template-with-no-qualifier", "templates", "Plain<int>", "Plain",
                Q_TARGET("q.Plain", "Class", 31, plain_line)),
        Q_USAGE("a-member-through-a-qualified-template", "templates", "box_field +", "box_field",
                Q_TARGET("q.ns.Box.box_field", "Field", 5, box_line)),
        Q_USAGE("a-member-through-a-template", "templates", "plain_field; }", "plain_field",
                Q_TARGET("q.Plain.plain_field", "Field", 31, plain_line)),
        Q_USAGE("from-the-global-scope", "rooted", "::ns::Name", "Name",
                Q_TARGET("q.ns.Name", "Class", 4, name_line)),
        Q_USAGE("a-member-from-the-global-scope", "rooted", "in_ns; }", "in_ns",
                Q_TARGET("q.ns.Name.in_ns", "Field", 4, name_line)),
        Q_USAGE("a-class-as-the-qualifier", "nested", "Holder::Inner", "Inner",
                Q_TARGET("q.Holder.Inner", "Class", 32, holder_line)),
        Q_USAGE("a-member-through-a-nested-class", "nested", "inner_field; }", "inner_field",
                Q_TARGET("q.Holder.Inner.inner_field", "Field", 32, holder_line)),
    };
#undef Q_USAGE
#undef Q_TARGET
#define Q_NEVER(id, caller, context, word)                                                      \
    {.case_id = id,                                                                              \
     .site = C_TU_WORD("q_use.cpp", use, "q_use." caller, context, word),                        \
     .relation = "USAGE",                                                                        \
     .any_target = true}
    const RQ_Forbidden forbidden[] = {
        Q_NEVER("a-namespace-this-code-cannot-reach", "not_reachable", "hidden::Secret", "Secret"),
        Q_NEVER("nothing-through-what-it-cannot-reach", "not_reachable", "secret_field; }",
                "secret_field"),
        Q_NEVER("two-places-the-name-could-start", "mid.two_candidates", "twice::Twice", "Twice"),
        Q_NEVER("nothing-through-the-undecided-name", "mid.two_candidates", "twice_field; }",
                "twice_field"),
        Q_NEVER("a-template-template-parameter", "parameter", "T<int>", "T"),
    };
#undef Q_NEVER
    const CTUCase fixture = {.suite = "c-tu-qualified-types",
                             .sources = C_TU_Q,
                             .source_count = sizeof(C_TU_Q) / sizeof(C_TU_Q[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0]),
                             .forbidden = forbidden,
                             .forbidden_count = sizeof(forbidden) / sizeof(forbidden[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* `template <class T> void Box<T>::run() {}` was a free Function named run, and its body belonged to
 * no node at all: `Box<T>` went into the name of the scope, and `ns::Box<T>::run` got no node. Most
 * of llvm's Impl headers are written this way. It is a Method of Box, like `Plain::run`. */
#define C_TU_OOL_H                                                                              \
    "#ifndef TREE_HPP\n"                                                                        \
    "#define TREE_HPP\n"                                                                        \
    "namespace lib {\n"                                                                         \
    "template <class T> class Tree {\n"                                                         \
    "public:\n"                                                                                 \
    "  void walk();\n"                                                                          \
    "  void step();\n"                                                                          \
    "  int depth;\n"                                                                            \
    "};\n"                                                                                      \
    "class Solid {\n"                                                                           \
    "public:\n"                                                                                 \
    "  void go();\n"                                                                            \
    "  void back();\n"                                                                          \
    "  int count;\n"                                                                            \
    "};\n"                                                                                      \
    "}\n"                                                                                       \
    "#endif\n"
#define C_TU_OOL_USE                                                                            \
    "#include \"tree.hpp\"\n"                                                                   \
    "static void step() {}\n"                                                                   \
    "namespace lib {\n"                                                                         \
    "template <class T> void Tree<T>::step() {}\n"                                              \
    "template <class T> void Tree<T>::walk() { step(); depth = 1; }\n"                          \
    "void Solid::back() {}\n"                                                                   \
    "}\n"                                                                                       \
    "void lib::Solid::go() { back(); count = 1; }\n"
static const RQ_Source C_TU_OOL[] = {{.path = "tree.hpp", .text = C_TU_OOL_H},
                                     {.path = "tree_impl.cpp", .text = C_TU_OOL_USE}};

TEST(resolver_cpp_out_of_line_template_member_is_a_method_of_its_class) {
    const char *use = C_TU_OOL_USE;
    const RQ_Expected expected[] = {
        {.case_id = "the-class-before-the-free-namesake",
         .site = {.file = "tree_impl.cpp",
                  .caller = "tree_impl.Tree.walk",
                  .needle = "step()",
                  .occurrence_index = 2},
         .relation = "CALLS",
         .target = {.file = "tree_impl.cpp",
                    .suffix = "tree_impl.Tree.step",
                    .label = "Method",
                    .declaration_line = 4,
                    .declaration_text = "template <class T> void Tree<T>::step() {}"}},
        {.case_id = "a-field-of-the-template-from-its-body",
         .site = C_TU_WORD("tree_impl.cpp", use, "tree_impl.Tree.walk", "depth = 1", "depth"),
         .relation = "WRITES",
         .target = {.file = "tree.hpp",
                    .suffix = "tree.lib.Tree.depth",
                    .label = "Field",
                    .declaration_line = 8,
                    .declaration_text = "  int depth;"}},
        {.case_id = "a-sibling-through-the-qualified-name",
         .site = {.file = "tree_impl.cpp",
                  .caller = "tree_impl.Solid.go",
                  .needle = "back()",
                  .occurrence_index = 1},
         .relation = "CALLS",
         .target = {.file = "tree_impl.cpp",
                    .suffix = "tree_impl.Solid.back",
                    .label = "Method",
                    .declaration_line = 6,
                    .declaration_text = "void Solid::back() {}"}},
        {.case_id = "a-field-through-the-qualified-name",
         .site = C_TU_WORD("tree_impl.cpp", use, "tree_impl.Solid.go", "count = 1", "count"),
         .relation = "WRITES",
         .target = {.file = "tree.hpp",
                    .suffix = "tree.lib.Solid.count",
                    .label = "Field",
                    .declaration_line = 14,
                    .declaration_text = "  int count;"}},
    };
    const CTUCase fixture = {.suite = "c-tu-out-of-line-template-members",
                             .sources = C_TU_OOL,
                             .source_count = sizeof(C_TU_OOL) / sizeof(C_TU_OOL[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* `template <class U> int get(U u) { ... }` written in its class had no node: its body belonged to the
 * file and nothing could call it. llvm's DenseMap, PatternMatch and ItaniumDemangle are full of them. */
#define C_TU_MT_H                                                                               \
    "struct Host {\n"                                                                           \
    "  int count;\n"                                                                            \
    "  int helper() { return 1; }\n"                                                            \
    "  template <class U> int get(U u) { helper(); return count; }\n"                           \
    "};\n"                                                                                      \
    "template <class T> struct Tpl {\n"                                                         \
    "  template <class U> int inner(U u) { return 2; }\n"                                       \
    "};\n"
#define C_TU_MT_USE                                                                             \
    "#include \"mt.hpp\"\n"                                                                     \
    "int use(Host &h, Tpl<int> &t) { return h.get(1) + t.inner(2); }\n"
static const RQ_Source C_TU_MT[] = {{.path = "mt.hpp", .text = C_TU_MT_H},
                                    {.path = "mt_use.cpp", .text = C_TU_MT_USE}};

TEST(resolver_cpp_member_template_is_a_method_of_its_class) {
    const char *header = C_TU_MT_H;
    const char *use = C_TU_MT_USE;
    static const char get_line[] = "  template <class U> int get(U u) { helper(); return count; }";
    static const char inner_line[] = "  template <class U> int inner(U u) { return 2; }";
    const RQ_Expected expected[] = {
        {.case_id = "a-sibling-from-the-template-body",
         .site = {.file = "mt.hpp", .caller = "mt.Host.get", .needle = "helper()", .occurrence_index = 1},
         .relation = "CALLS",
         .target = {.file = "mt.hpp",
                    .suffix = "mt.Host.helper",
                    .label = "Method",
                    .declaration_line = 3,
                    .declaration_text = "  int helper() { return 1; }"}},
        {.case_id = "a-field-from-the-template-body",
         .site = C_TU_WORD("mt.hpp", header, "mt.Host.get", "count; }", "count"),
         .relation = "USAGE",
         .target = {.file = "mt.hpp",
                    .suffix = "mt.Host.count",
                    .label = "Field",
                    .declaration_line = 2,
                    .declaration_text = "  int count;"}},
        {.case_id = "a-call-of-the-member-template",
         .site = {.file = "mt_use.cpp", .caller = "mt_use.use", .needle = "h.get(1)"},
         .relation = "CALLS",
         .target = {.file = "mt.hpp",
                    .suffix = "mt.Host.get",
                    .label = "Method",
                    .declaration_line = 4,
                    .declaration_text = get_line}},
        {.case_id = "a-call-of-a-member-template-of-a-template",
         .site = {.file = "mt_use.cpp", .caller = "mt_use.use", .needle = "t.inner(2)"},
         .relation = "CALLS",
         .target = {.file = "mt.hpp",
                    .suffix = "mt.Tpl.inner",
                    .label = "Method",
                    .declaration_line = 7,
                    .declaration_text = inner_line}},
        {.case_id = "the-class-in-the-signature",
         .site = C_TU_WORD("mt_use.cpp", use, "mt_use.use", "Host &h", "Host"),
         .relation = "USAGE",
         .target = {.file = "mt.hpp",
                    .suffix = "mt.Host",
                    .label = "Class",
                    .declaration_line = 1,
                    .declaration_text = "struct Host {"}},
        {.case_id = "the-template-in-the-signature",
         .site = C_TU_WORD("mt_use.cpp", use, "mt_use.use", "Tpl<int>", "Tpl"),
         .relation = "USAGE",
         .target = {.file = "mt.hpp",
                    .suffix = "mt.Tpl",
                    .label = "Class",
                    .declaration_line = 6,
                    .declaration_text = "template <class T> struct Tpl {"}},
    };
    const CTUCase fixture = {.suite = "c-tu-member-templates",
                             .sources = C_TU_MT,
                             .source_count = sizeof(C_TU_MT) / sizeof(C_TU_MT[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* Two forms the grammar reads as a function that swallows what follows. `#define` inside an initializer
 * made a function OP of 1,400 lines of jemalloc's ctl.c, and a macro word before `namespace` made a
 * function `namespace` of all of llvm's ConvertUTF.cpp. The functions inside are the ones to keep. */
#define C_TU_SW_H                                                                               \
    "#ifndef SW_H\n"                                                                            \
    "#define SW_H\n"                                                                            \
    "#define LIST OP(1) OP(2)\n"                                                                \
    "#define Quiet_Warnings\n"                                                                  \
    "#endif\n"
#define C_TU_SW_TABLE                                                                           \
    "#include \"sw.h\"\n"                                                                       \
    "typedef struct { const char *a; int b; } node_t;\n"                                        \
    "static const char *NAME(const char *n) { return n; }\n"                                    \
    "static int CTL(int v) { return v; }\n"                                                     \
    "static const node_t table[] = {\n"                                                         \
    "#define OP(n) {NAME(#n), CHILD(named, stats_##n)},\n"                                      \
    "LIST\n"                                                                                    \
    "#undef OP\n"                                                                               \
    "  {NAME(\"reset\"), CTL(1)}\n"                                                             \
    "};\n"                                                                                      \
    "static int after_table(int v) { return v; }\n"                                             \
    "int call_after(int v) { return after_table(v); }\n"
#define C_TU_SW_NS                                                                              \
    "#include \"sw.h\"\n"                                                                       \
    "Quiet_Warnings\n"                                                                          \
    "namespace lib {\n"                                                                         \
    "static int inside(int v) { return v; }\n"                                                  \
    "int call_inside(int v) { return inside(v); }\n"                                            \
    "}\n"
#define C_TU_SW_HEADS                                                                           \
    "#include \"sw.h\"\n"                                                                       \
    "static int helper(int v) { return v; }\n"                                                  \
    "#ifndef CONFIG_ONE\n"                                                                      \
    "void prefork(void)\n"                                                                      \
    "#else\n"                                                                                   \
    "void other_prefork(void)\n"                                                                \
    "#endif\n"                                                                                  \
    "{\n"                                                                                       \
    "  helper(1);\n"                                                                            \
    "}\n"                                                                                       \
    "void caller(void) { prefork(); }\n"
#define C_TU_SW_LUA                                                                             \
    "#include \"sw.h\"\n"                                                                       \
    "#define QUOTED(x) \"'\" x \"'\"\n"                                                          \
    "static void report(const char *m) { (void)m; }\n"                                          \
    "static void before(int c) {\n"                                                             \
    "  switch (c) {\n"                                                                          \
    "    case 1: report(QUOTED(\"=\") \" or \" QUOTED(\"in\") \" expected\"); break;\n"           \
    "    default: report(\"no \" QUOTED(\"x\")); break;\n"                                        \
    "  }\n"                                                                                     \
    "}\n"                                                                                       \
    "static void after_it(void) { report(\"x\"); before(1); }\n"
static const RQ_Source C_TU_SW[] = {{.path = "sw.h", .text = C_TU_SW_H},
                                    {.path = "sw_heads.c", .text = C_TU_SW_HEADS},
                                    {.path = "sw_lua.c", .text = C_TU_SW_LUA},
                                    {.path = "sw_table.c", .text = C_TU_SW_TABLE},
                                    {.path = "sw_ns.cpp", .text = C_TU_SW_NS}};

TEST(resolver_c_a_broken_head_does_not_swallow_the_functions_after_it) {
    const RQ_Expected expected[] = {
        {.case_id = "a-function-after-a-define-in-a-table",
         .site = {.file = "sw_table.c", .caller = "sw_table.call_after", .needle = "after_table(v)"},
         .relation = "CALLS",
         .target = {.file = "sw_table.c",
                    .suffix = "sw_table.after_table",
                    .label = "Function",
                    .declaration_line = 11,
                    .declaration_text = "static int after_table(int v) { return v; }"}},
        {.case_id = "a-call-in-the-table-belongs-to-the-file",
         .site = {.file = "sw_table.c", .caller = "sw_table", .needle = "NAME(\"reset\")"},
         .relation = "CALLS",
         .target = {.file = "sw_table.c",
                    .suffix = "sw_table.NAME",
                    .label = "Function",
                    .declaration_line = 3,
                    .declaration_text = "static const char *NAME(const char *n) { return n; }"}},
        {.case_id = "and-so-does-the-next-one",
         .site = {.file = "sw_table.c", .caller = "sw_table", .needle = "CTL(1)"},
         .relation = "CALLS",
         .target = {.file = "sw_table.c",
                    .suffix = "sw_table.CTL",
                    .label = "Function",
                    .declaration_line = 4,
                    .declaration_text = "static int CTL(int v) { return v; }"}},
        {.case_id = "the-body-belongs-to-the-first-head",
         .site = {.file = "sw_heads.c", .caller = "sw_heads.prefork", .needle = "helper(1)"},
         .relation = "CALLS",
         .target = {.file = "sw_heads.c",
                    .suffix = "sw_heads.helper",
                    .label = "Function",
                    .declaration_line = 2,
                    .declaration_text = "static int helper(int v) { return v; }"}},
        {.case_id = "a-call-of-the-first-head",
         .site = {.file = "sw_heads.c", .caller = "sw_heads.caller", .needle = "prefork()"},
         .relation = "CALLS",
         .target = {.file = "sw_heads.c",
                    .suffix = "sw_heads.prefork",
                    .label = "Function",
                    .declaration_line = 4,
                    .declaration_text = "void prefork(void)"}},
        {.case_id = "a-call-with-a-macro-among-its-strings",
         .site = {.file = "sw_lua.c",
                  .caller = "sw_lua.before",
                  .needle = "report(QUOTED(\"=\") \" or \" QUOTED(\"in\") \" expected\")"},
         .relation = "CALLS",
         .target = {.file = "sw_lua.c",
                    .suffix = "sw_lua.report",
                    .label = "Function",
                    .declaration_line = 3,
                    .declaration_text = "static void report(const char *m) { (void)m; }"}},
        {.case_id = "a-macro-after-a-string",
         .site = {.file = "sw_lua.c",
                  .caller = "sw_lua.before",
                  .needle = "report(\"no \" QUOTED(\"x\"))"},
         .relation = "CALLS",
         .target = {.file = "sw_lua.c",
                    .suffix = "sw_lua.report",
                    .label = "Function",
                    .declaration_line = 3,
                    .declaration_text = "static void report(const char *m) { (void)m; }"}},
        {.case_id = "the-call-after-those-strings",
         .site = {.file = "sw_lua.c", .caller = "sw_lua.after_it", .needle = "report(\"x\")"},
         .relation = "CALLS",
         .target = {.file = "sw_lua.c",
                    .suffix = "sw_lua.report",
                    .label = "Function",
                    .declaration_line = 3,
                    .declaration_text = "static void report(const char *m) { (void)m; }"}},
        {.case_id = "the-function-before-them",
         .site = {.file = "sw_lua.c", .caller = "sw_lua.after_it", .needle = "before(1)"},
         .relation = "CALLS",
         .target = {.file = "sw_lua.c",
                    .suffix = "sw_lua.before",
                    .label = "Function",
                    .declaration_line = 4,
                    .declaration_text = "static void before(int c) {"}},
        {.case_id = "a-function-after-a-word-before-namespace",
         .site = {.file = "sw_ns.cpp", .caller = "sw_ns.lib.call_inside", .needle = "inside(v)"},
         .relation = "CALLS",
         .target = {.file = "sw_ns.cpp",
                    .suffix = "sw_ns.lib.inside",
                    .label = "Function",
                    .declaration_line = 4,
                    .declaration_text = "static int inside(int v) { return v; }"}},
    };
    const CTUCase fixture = {.suite = "c-tu-swallowing-heads",
                             .sources = C_TU_SW,
                             .source_count = sizeof(C_TU_SW) / sizeof(C_TU_SW[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* `wp->w_p_list` with `#define w_p_list w_onebuf_opt.wo_list`: no field of that name exists, and the
 * compiler reads the replacement after the arrow. neovim has a thousand of these, with the macro written
 * inside the struct beside the member, and the kernel's `sk->sk_family` has the same shape. */
#define C_TU_MP_H                                                                               \
    "#ifndef MP_H\n"                                                                            \
    "#define MP_H\n"                                                                            \
    "typedef struct {\n"                                                                        \
    "  int wo_list;\n"                                                                          \
    "#define w_p_list w_onebuf_opt.wo_list   // 'list'\n"                                       \
    "} winopt_T;\n"                                                                             \
    "typedef struct {\n"                                                                        \
    "  winopt_T w_onebuf_opt;\n"                                                                \
    "  int handle;\n"                                                                           \
    "  int both_a;\n"                                                                           \
    "  int both_b;\n"                                                                           \
    "} win_T;\n"                                                                                \
    "#define b_fnum handle\n"                                                                   \
    "#ifdef SOME_CONFIG\n"                                                                      \
    "#define alt_name both_a\n"                                                                 \
    "#else\n"                                                                                   \
    "#define alt_name both_b\n"                                                                 \
    "#endif\n"                                                                                  \
    "#define nowhere no_such_member.at_all\n"                                                   \
    "#endif\n"
#define C_TU_MP_USE                                                                             \
    "#include \"mp.h\"\n"                                                                       \
    "int through_a_path(win_T *wp) { return wp->w_p_list; }\n"                                  \
    "int one_word(win_T *wp) { return wp->b_fnum; }\n"                                          \
    "int two_definitions(win_T *wp) { return wp->alt_name; }\n"                                 \
    "int missing_step(win_T *wp) { return wp->nowhere; }\n"
static const RQ_Source C_TU_MP[] = {{.path = "mp.h", .text = C_TU_MP_H},
                                    {.path = "mp_use.c", .text = C_TU_MP_USE}};

TEST(resolver_c_macro_after_an_arrow_reads_the_members_it_names) {
    const char *use = C_TU_MP_USE;
    const RQ_Target win = {.file = "mp.h",
                           .suffix = "mp.win_T",
                           .label = "Class",
                           .declaration_line = 7,
                           .declaration_text = "typedef struct {"};
    const RQ_Expected expected[] = {
        {.case_id = "type-of-the-path-reader",
         .site = C_TU_WORD("mp_use.c", use, "mp_use.through_a_path",
                           "win_T *wp) { return wp->w_p_list", "win_T"),
         .relation = "USAGE",
         .target = win},
        {.case_id = "first-step-of-the-path",
         .site = C_TU_WORD("mp_use.c", use, "mp_use.through_a_path", "w_p_list; }", "w_p_list"),
         .relation = "USAGE",
         .dispatch_group = "the-whole-path",
         .target = {.file = "mp.h",
                    .suffix = "mp.win_T.w_onebuf_opt",
                    .label = "Field",
                    .declaration_line = 8,
                    .declaration_text = "  winopt_T w_onebuf_opt;"}},
        {.case_id = "last-step-of-the-path",
         .site = C_TU_WORD("mp_use.c", use, "mp_use.through_a_path", "w_p_list; }", "w_p_list"),
         .relation = "USAGE",
         .dispatch_group = "the-whole-path",
         .target = {.file = "mp.h",
                    .suffix = "mp.winopt_T.wo_list",
                    .label = "Field",
                    .declaration_line = 4,
                    .declaration_text = "  int wo_list;"}},
        {.case_id = "type-of-the-word-reader",
         .site = C_TU_WORD("mp_use.c", use, "mp_use.one_word", "win_T *wp) { return wp->b_fnum",
                           "win_T"),
         .relation = "USAGE",
         .target = win},
        {.case_id = "a-macro-for-one-member",
         .site = C_TU_WORD("mp_use.c", use, "mp_use.one_word", "b_fnum; }", "b_fnum"),
         .relation = "USAGE",
         .target = {.file = "mp.h",
                    .suffix = "mp.win_T.handle",
                    .label = "Field",
                    .declaration_line = 9,
                    .declaration_text = "  int handle;"}},
        {.case_id = "type-beside-two-definitions",
         .site = C_TU_WORD("mp_use.c", use, "mp_use.two_definitions",
                           "win_T *wp) { return wp->alt_name", "win_T"),
         .relation = "USAGE",
         .target = win},
        {.case_id = "type-beside-a-missing-step",
         .site = C_TU_WORD("mp_use.c", use, "mp_use.missing_step",
                           "win_T *wp) { return wp->nowhere", "win_T"),
         .relation = "USAGE",
         .target = win},
    };
    const RQ_Forbidden forbidden[] = {
        {.case_id = "definitions-that-disagree",
         .site = C_TU_WORD("mp_use.c", use, "mp_use.two_definitions", "alt_name; }", "alt_name"),
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "a-step-the-class-does-not-have",
         .site = C_TU_WORD("mp_use.c", use, "mp_use.missing_step", "nowhere; }", "nowhere"),
         .relation = "USAGE",
         .any_target = true},
    };
    const CTUCase fixture = {.suite = "c-tu-macro-member-paths",
                             .sources = C_TU_MP,
                             .source_count = sizeof(C_TU_MP) / sizeof(C_TU_MP[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0]),
                             .forbidden = forbidden,
                             .forbidden_count = sizeof(forbidden) / sizeof(forbidden[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* `kvec_t(uint64_t) update_channels;` made a Field named uint64_t and none for the member, since the
 * grammar ends the member at a `;` it invents and leaves the name as a member of its own. */
#define C_TU_KV_H                                                                               \
    "#ifndef KV_H\n"                                                                            \
    "#define KV_H\n"                                                                            \
    "#define kvec_t(type) struct { unsigned size; type *items; }\n"                             \
    "typedef struct { int cb; } Callback;\n"                                                    \
    "struct buffer {\n"                                                                         \
    "  int first;\n"                                                                            \
    "  // channels\n"                                                                           \
    "  kvec_t(uint64_t) update_channels;\n"                                                     \
    "  // callbacks\n"                                                                          \
    "  kvec_t(Callback) update_callbacks;\n"                                                    \
    "\n"                                                                                        \
    "  // windows\n"                                                                            \
    "  int between;\n"                                                                          \
    "  kvec_t(Callback *) pointer_vec;\n"                                                       \
    "  int last;\n"                                                                             \
    "};\n"                                                                                      \
    "#endif\n"
#define C_TU_KV_USE                                                                             \
    "#include \"kv.h\"\n"                                                                       \
    "void *read_channels(struct buffer *b) { return &b->update_channels; }\n"                   \
    "void *read_callbacks(struct buffer *b) { return &b->update_callbacks; }\n"                 \
    "void *read_pointers(struct buffer *b) { return &b->pointer_vec; }\n"
static const RQ_Source C_TU_KV[] = {{.path = "kv.h", .text = C_TU_KV_H},
                                    {.path = "kv_use.c", .text = C_TU_KV_USE}};

TEST(resolver_c_macro_typed_member_keeps_its_own_name) {
    const char *use = C_TU_KV_USE;
    const RQ_Target buffer = {.file = "kv.h",
                              .suffix = "kv.buffer",
                              .label = "Class",
                              .declaration_line = 5,
                              .declaration_text = "struct buffer {"};
    const RQ_Expected expected[] = {
        {.case_id = "the-tag-of-the-first-reader",
         .site = C_TU_WORD("kv_use.c", use, "kv_use.read_channels",
                           "buffer *b) { return &b->update_channels", "buffer"),
         .relation = "USAGE",
         .target = buffer},
        {.case_id = "the-tag-of-the-second-reader",
         .site = C_TU_WORD("kv_use.c", use, "kv_use.read_callbacks",
                           "buffer *b) { return &b->update_callbacks", "buffer"),
         .relation = "USAGE",
         .target = buffer},
        {.case_id = "the-tag-of-the-third-reader",
         .site = C_TU_WORD("kv_use.c", use, "kv_use.read_pointers", "buffer *b) { return &b->pointer_vec",
                           "buffer"),
         .relation = "USAGE",
         .target = buffer},
        {.case_id = "a-member-whose-name-the-grammar-split-off",
         .site = C_TU_WORD("kv_use.c", use, "kv_use.read_channels", "update_channels; }",
                           "update_channels"),
         .relation = "USAGE",
         .target = {.file = "kv.h",
                    .suffix = "kv.buffer.update_channels",
                    .label = "Field",
                    .declaration_line = 8,
                    .declaration_text = "  kvec_t(uint64_t) update_channels;"}},
        {.case_id = "a-member-the-grammar-reads-whole",
         .site = C_TU_WORD("kv_use.c", use, "kv_use.read_callbacks", "update_callbacks; }",
                           "update_callbacks"),
         .relation = "USAGE",
         .target = {.file = "kv.h",
                    .suffix = "kv.buffer.update_callbacks",
                    .label = "Field",
                    .declaration_line = 10,
                    .declaration_text = "  kvec_t(Callback) update_callbacks;"}},
        {.case_id = "a-member-the-grammar-reads-as-a-prototype",
         .site = C_TU_WORD("kv_use.c", use, "kv_use.read_pointers", "pointer_vec; }", "pointer_vec"),
         .relation = "USAGE",
         .target = {.file = "kv.h",
                    .suffix = "kv.buffer.pointer_vec",
                    .label = "Field",
                    .declaration_line = 14,
                    .declaration_text = "  kvec_t(Callback *) pointer_vec;"}},
    };
    const CTUCase fixture = {.suite = "c-tu-macro-typed-members",
                             .sources = C_TU_KV,
                             .source_count = sizeof(C_TU_KV) / sizeof(C_TU_KV[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* `Optional<int> Entries;` read in a member function lost its owner, since a template field had no type
 * name. And `template <class Value> class Pair { Value first; };` beside a project class Value read
 * `p.first.v_field` as that class's field: first has whatever type the instantiation supplies. */
#define C_TU_TF_H                                                                               \
    "#ifndef TF_HPP\n"                                                                          \
    "#define TF_HPP\n"                                                                          \
    "namespace lib {\n"                                                                         \
    "struct Value { int v_field; int getName() { return 1; } };\n"                              \
    "struct Key { int k_field; };\n"                                                            \
    "struct Real { int r_field; };\n"                                                           \
    "template <class Value, class Key> class Pair {\n"                                          \
    "public:\n"                                                                                 \
    "  bool ok() const { return true; }\n"                                                      \
    "  Value get();\n"                                                                          \
    "  Value first;\n"                                                                          \
    "  Key *second;\n"                                                                          \
    "  Real plain;\n"                                                                           \
    "};\n"                                                                                      \
    "template <class T> class Special { public: bool general() const { return true; } };\n"     \
    "template <> class Special<char> { public: bool only_char() const { return true; } };\n"    \
    "}\n"                                                                                       \
    "#endif\n"
#define C_TU_TF_USE                                                                             \
    "#include \"tf.hpp\"\n"                                                                     \
    "using namespace lib;\n"                                                                    \
    "struct Holder {\n"                                                                         \
    "  Pair<Real, Real> field;\n"                                                               \
    "  Special<int> special;\n"                                                                 \
    "  bool through_a_field() { return field.ok(); }\n"                                         \
    "  int a_real_member() { return field.plain.r_field; }\n"                                   \
    "  int a_parameter_type() { return field.first.v_field; }\n"                                \
    "  bool a_specialized_template() { return special.general(); }\n"                           \
    "};\n"                                                                                      \
    "int param_route(Pair<Real, Real> &p) {\n"                                                  \
    "  return p.first.v_field + p.first.getName() + p.second->k_field + p.get().v_field;\n"     \
    "}\n"
static const RQ_Source C_TU_TF[] = {{.path = "tf.hpp", .text = C_TU_TF_H},
                                    {.path = "tf_use.cpp", .text = C_TU_TF_USE}};

TEST(resolver_cpp_template_field_reads_the_template_and_not_its_parameter_spelling) {
    const char *use = C_TU_TF_USE;
#define TF_TARGET(file_, suffix_, label_, line_, text_)                                         \
    {.file = file_, .suffix = suffix_, .label = label_, .declaration_line = line_,               \
     .declaration_text = text_}
#define TF_USAGE(id, caller, context, word, target_)                                            \
    {.case_id = id,                                                                              \
     .site = C_TU_WORD("tf_use.cpp", use, "tf_use." caller, context, word),                      \
     .relation = "USAGE",                                                                        \
     .target = target_}
    const RQ_Target field = TF_TARGET("tf_use.cpp", "tf_use.Holder.field", "Field", 4,
                                      "  Pair<Real, Real> field;");
    const RQ_Target first = TF_TARGET("tf.hpp", "tf.lib.Pair.first", "Field", 11, "  Value first;");
    const RQ_Target real = TF_TARGET("tf.hpp", "tf.lib.Real", "Class", 6,
                                     "struct Real { int r_field; };");
    const RQ_Expected expected[] = {
        {.case_id = "a-method-through-a-template-field",
         .site = {.file = "tf_use.cpp", .caller = "tf_use.Holder.through_a_field", .needle = "field.ok()"},
         .relation = "CALLS",
         .target = TF_TARGET("tf.hpp", "tf.lib.Pair.ok", "Method", 9,
                             "  bool ok() const { return true; }")},
        TF_USAGE("the-field-before-a-method", "Holder.through_a_field", "field.ok()", "field", field),
        TF_USAGE("the-field-before-a-member", "Holder.a_real_member", "field.plain", "field", field),
        TF_USAGE("a-member-the-template-writes-out", "Holder.a_real_member", "plain.r_field", "plain",
                 TF_TARGET("tf.hpp", "tf.lib.Pair.plain", "Field", 13, "  Real plain;")),
        TF_USAGE("a-written-class-through-a-template-field", "Holder.a_real_member", "r_field; }",
                 "r_field",
                 TF_TARGET("tf.hpp", "tf.lib.Real.r_field", "Field", 6,
                           "struct Real { int r_field; };")),
        TF_USAGE("the-field-before-a-parameter-type", "Holder.a_parameter_type", "field.first", "field",
                 field),
        TF_USAGE("a-field-of-a-parameter-type-is-a-field", "Holder.a_parameter_type", "first.v_field; }",
                 "first", first),
        TF_USAGE("the-field-of-a-specialized-template", "Holder.a_specialized_template",
                 "special.general()", "special",
                 TF_TARGET("tf_use.cpp", "tf_use.Holder.special", "Field", 5,
                           "  Special<int> special;")),
        TF_USAGE("the-template-of-the-parameter", "param_route", "Pair<Real, Real> &p", "Pair",
                 TF_TARGET("tf.hpp", "tf.lib.Pair", "Class", 7,
                           "template <class Value, class Key> class Pair {")),
        TF_USAGE("the-first-argument", "param_route", "Real, Real> &p", "Real", real),
        TF_USAGE("the-second-argument", "param_route", "Real> &p", "Real", real),
        TF_USAGE("the-field-through-a-reference", "param_route", "first.v_field +", "first", first),
        TF_USAGE("the-field-before-a-method-call", "param_route", "first.getName()", "first", first),
        TF_USAGE("the-pointer-field", "param_route", "second->k_field", "second",
                 TF_TARGET("tf.hpp", "tf.lib.Pair.second", "Field", 12, "  Key *second;")),
    };
#undef TF_USAGE
#undef TF_TARGET
#define TF_NEVER(id, caller, context, word)                                                     \
    {.case_id = id,                                                                              \
     .site = C_TU_WORD("tf_use.cpp", use, "tf_use." caller, context, word),                      \
     .relation = "USAGE",                                                                        \
     .any_target = true}
    const RQ_Forbidden forbidden[] = {
        TF_NEVER("a-parameter-spelled-like-a-class-from-a-field", "Holder.a_parameter_type", "v_field; }",
                 "v_field"),
        TF_NEVER("a-parameter-spelled-like-a-class-from-a-reference", "param_route", "v_field + p.first",
                 "v_field"),
        TF_NEVER("a-pointer-to-a-parameter", "param_route", "k_field + p.get", "k_field"),
        TF_NEVER("what-a-method-returning-a-parameter-gives", "param_route", "v_field;\n}", "v_field"),
        {.case_id = "a-method-of-what-a-parameter-holds",
         .site = {.file = "tf_use.cpp", .caller = "tf_use.param_route", .needle = "p.first.getName()"},
         .relation = "CALLS",
         .any_target = true},
        {.case_id = "a-template-with-a-specialization",
         .site = {.file = "tf_use.cpp",
                  .caller = "tf_use.Holder.a_specialized_template",
                  .needle = "special.general()"},
         .relation = "CALLS",
         .any_target = true},
    };
#undef TF_NEVER
    const CTUCase fixture = {.suite = "c-tu-template-fields",
                             .sources = C_TU_TF,
                             .source_count = sizeof(C_TU_TF) / sizeof(C_TU_TF[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0]),
                             .forbidden = forbidden,
                             .forbidden_count = sizeof(forbidden) / sizeof(forbidden[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* `struct X *p` had no occurrence at all: the specifier's name counted as a declaration, so a tag in a
 * signature, a body, a cast or sizeof had no edge. Only a body or a lone `struct X;` declares. A tag is
 * not a typedef: `struct Thing *t` beside `typedef struct impl Thing;` is a tag Thing of its own. */
#define C_TU_TAG_H                                                                              \
    "#ifndef TAG_H\n"                                                                           \
    "#define TAG_H\n"                                                                           \
    "struct header_tag { int size; };\n"                                                        \
    "union shared_u { int a; };\n"                                                              \
    "enum shared_e { E_ONE };\n"                                                                \
    "struct real_impl { int r; };\n"                                                            \
    "typedef struct real_impl Thing;\n"                                                         \
    "typedef struct { int n; } NoTag;\n"                                                        \
    "typedef union { int n; } NoTagU;\n"                                                        \
    "typedef enum { NT_ONE } NoTagE;\n"                                                         \
    "struct second_tag { int size; };\n"                                                        \
    "struct fourth_tag { int size; };\n"                                                        \
    "struct fifth_tag { int size; };\n"                                                        \
    "#endif\n"
#define C_TU_TAG_USE                                                                            \
    "#include \"tag.h\"\n"                                                                      \
    "struct local_tag { int a; };\n"                                                            \
    "struct local_tag;\n"                                                                       \
    "int signature(struct header_tag *h) { return h != 0; }\n"                                  \
    "int in_body(void) { union shared_u u; enum shared_e e; (void)u; (void)e; return 0; }\n"    \
    "unsigned long sized(void) { return sizeof(struct header_tag); }\n"                         \
    "int cast_of(void *v) { return ((struct header_tag *)v) != 0; }\n"                          \
    "int own_file(struct local_tag *l) { return l != 0; }\n"                                    \
    "int opaque(struct only_here *o) { return o != 0; }\n"                                      \
    "int through_alias_tag(struct Thing *t) { return t->r; }\n"                                 \
    "int shadowed(void) { struct header_tag { int other; }; struct header_tag mine; return 0; }\n" \
    "void *in_macro_argument(void *p) { return container_of(p, struct header_tag, size); }\n"   \
    "void *alias_in_macro_argument(void *p) { return container_of(p, struct Thing, r); }\n"   \
    "int lone_in_body(void) { struct second_tag; struct second_tag *mine = 0; return mine != 0; }\n"\
    "int after_the_block(struct second_tag *s) { return s != 0; }\n"                            \
    "int no_tag(struct NoTag *t) { return t != 0; }\n"                                          \
    "int no_tag_member(struct NoTag *u) { return u->n; }\n"                                     \
    "int no_tag_union(union NoTagU *x) { return x != 0; }\n"                                    \
    "int no_tag_enum(enum NoTagE *y) { return y != 0; }\n"                                      \
    "int typedef_name(NoTag *v) { return v->n; }\n"                                             \
    "int hidden_member(void) { struct fourth_tag { int size; }; struct fourth_tag w; return w.size; }\n" \
    "int variable_of_that_spelling(void) { struct fifth_tag fifth_tag; return fifth_tag.size; }\n" \
    "typedef struct { int n; } OwnNoTag;\n"                                                     \
    "typedef union { int n; } OwnNoTagU;\n"                                                     \
    "int own_no_tag(struct OwnNoTag *o) { return o != 0; }\n"                                   \
    "int own_no_tag_member(struct OwnNoTag *p) { return p->n; }\n"                              \
    "int own_no_tag_union(union OwnNoTagU *q) { return q != 0; }\n"
static const RQ_Source C_TU_TAG[] = {{.path = "tag.h", .text = C_TU_TAG_H},
                                     {.path = "tag_use.c", .text = C_TU_TAG_USE}};

TEST(resolver_c_struct_tag_is_a_use_unless_it_declares) {
    const char *use = C_TU_TAG_USE;
    const RQ_Target header_tag = {.file = "tag.h",
                                  .suffix = "tag.header_tag",
                                  .label = "Class",
                                  .declaration_line = 3,
                                  .declaration_text = "struct header_tag { int size; };"};
#define TAG_USE(id, caller, context, word, target_)                                             \
    {.case_id = id,                                                                              \
     .site = C_TU_WORD("tag_use.c", use, "tag_use." caller, context, word),                      \
     .relation = "USAGE",                                                                        \
     .target = target_}
    const RQ_Expected expected[] = {
        TAG_USE("a-tag-in-a-signature", "signature", "header_tag *h", "header_tag", header_tag),
        TAG_USE("a-union-in-a-body", "in_body", "shared_u u", "shared_u",
                ((RQ_Target){.file = "tag.h",
                             .suffix = "tag.shared_u",
                             .label = "Class",
                             .declaration_line = 4,
                             .declaration_text = "union shared_u { int a; };"})),
        TAG_USE("an-enum-in-a-body", "in_body", "shared_e e", "shared_e",
                ((RQ_Target){.file = "tag.h",
                             .suffix = "tag.shared_e",
                             .label = "Enum",
                             .declaration_line = 5,
                             .declaration_text = "enum shared_e { E_ONE };"})),
        TAG_USE("a-tag-in-sizeof", "sized", "sizeof(struct header_tag", "header_tag", header_tag),
        TAG_USE("a-tag-in-a-cast", "cast_of", "(struct header_tag *)v", "header_tag", header_tag),
        TAG_USE("a-tag-among-call-arguments", "in_macro_argument", "struct header_tag, size",
                "header_tag", header_tag),
        TAG_USE("a-tag-of-this-file", "own_file", "local_tag *l", "local_tag",
                ((RQ_Target){.file = "tag_use.c",
                             .suffix = "tag_use.local_tag",
                             .label = "Class",
                             .declaration_line = 2,
                             .declaration_text = "struct local_tag { int a; };"})),
        TAG_USE("a-lone-tag-hides-only-in-its-block", "after_the_block", "second_tag *s", "second_tag",
                ((RQ_Target){.file = "tag.h",
                             .suffix = "tag.second_tag",
                             .label = "Class",
                             .declaration_line = 11,
                             .declaration_text = "struct second_tag { int size; };"})),
        TAG_USE("a-typedef-name-is-the-type", "typedef_name", "NoTag *v", "NoTag",
                ((RQ_Target){.file = "tag.h",
                             .suffix = "tag.NoTag",
                             .label = "Class",
                             .declaration_line = 8,
                             .declaration_text = "typedef struct { int n; } NoTag;"})),
        TAG_USE("a-member-through-the-typedef-name", "typedef_name", "v->n", "n",
                ((RQ_Target){.file = "tag.h",
                             .suffix = "tag.NoTag.n",
                             .label = "Field",
                             .declaration_line = 8,
                             .declaration_text = "typedef struct { int n; } NoTag;"})),
        TAG_USE("a-tag-beside-a-variable-of-its-spelling", "variable_of_that_spelling",
                "struct fifth_tag fifth_tag", "fifth_tag",
                ((RQ_Target){.file = "tag.h",
                             .suffix = "tag.fifth_tag",
                             .label = "Class",
                             .declaration_line = 13,
                             .declaration_text = "struct fifth_tag { int size; };"})),
        TAG_USE("that-variable-is-no-tag-of-its-own", "variable_of_that_spelling", "fifth_tag.size",
                "size",
                ((RQ_Target){.file = "tag.h",
                             .suffix = "tag.fifth_tag.size",
                             .label = "Field",
                             .declaration_line = 13,
                             .declaration_text = "struct fifth_tag { int size; };"})),
    };
#undef TAG_USE
#define TAG_NEVER(id, caller, context, word)                                                    \
    {.case_id = id,                                                                              \
     .site = C_TU_WORD("tag_use.c", use, caller, context, word),                                 \
     .relation = "USAGE",                                                                        \
     .any_target = true}
    const RQ_Forbidden forbidden[] = {
        TAG_NEVER("a-lone-tag-declares-it", "tag_use", "struct local_tag;", "local_tag"),
        TAG_NEVER("a-tag-this-unit-does-not-declare", "tag_use.opaque", "only_here *o", "only_here"),
        TAG_NEVER("a-tag-is-not-the-typedef-of-its-spelling", "tag_use.through_alias_tag", "Thing *t",
                  "Thing"),
        TAG_NEVER("nothing-through-that-tag", "tag_use.through_alias_tag", "t->r", "r"),
        TAG_NEVER("nor-among-call-arguments", "tag_use.alias_in_macro_argument", "struct Thing, r",
                  "Thing"),
        TAG_NEVER("a-tag-defined-in-the-function", "tag_use.shadowed", "struct header_tag mine",
                  "header_tag"),
        TAG_NEVER("a-lone-tag-in-a-block-is-a-new-one", "tag_use.lone_in_body", "second_tag *mine",
                  "second_tag"),
        TAG_NEVER("an-unnamed-struct-has-no-tag", "tag_use.no_tag", "NoTag *t", "NoTag"),
        TAG_NEVER("nor-through-it", "tag_use.no_tag_member", "NoTag *u", "NoTag"),
        TAG_NEVER("nor-a-member-through-it", "tag_use.no_tag_member", "u->n", "n"),
        TAG_NEVER("an-unnamed-union-has-no-tag", "tag_use.no_tag_union", "NoTagU *x", "NoTagU"),
        TAG_NEVER("an-unnamed-enum-has-no-tag", "tag_use.no_tag_enum", "NoTagE *y", "NoTagE"),
        TAG_NEVER("a-member-of-a-local-struct-is-not-the-outer-one", "tag_use.hidden_member", "w.size",
                  "size"),
        TAG_NEVER("nor-does-this-file-s-own-typedef-give-a-tag", "tag_use.own_no_tag", "OwnNoTag *o",
                  "OwnNoTag"),
        TAG_NEVER("nor-through-its-own", "tag_use.own_no_tag_member", "OwnNoTag *p", "OwnNoTag"),
        TAG_NEVER("nor-a-member-through-its-own", "tag_use.own_no_tag_member", "p->n", "n"),
        TAG_NEVER("nor-its-own-union", "tag_use.own_no_tag_union", "OwnNoTagU *q", "OwnNoTagU"),
    };
#undef TAG_NEVER
    const CTUCase fixture = {.suite = "c-tu-struct-tags",
                             .sources = C_TU_TAG,
                             .source_count = sizeof(C_TU_TAG) / sizeof(C_TU_TAG[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0]),
                             .forbidden = forbidden,
                             .forbidden_count = sizeof(forbidden) / sizeof(forbidden[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* `static int f(int x)` with an #ifdef splitting its braces is left loose in an ERROR, and the
 * signature there says how it is linked. It must still read as static, or a caller in another file
 * binds to it: through a prototype with no other definition, and in place of the real definition. */
#define C_TU_STRANDED_BODY                                                                      \
    "\n"                                                                                         \
    "{\n"                                                                                        \
    "#ifdef A\n"                                                                                 \
    "    if (x) {\n"                                                                             \
    "#else\n"                                                                                    \
    "    if (!x) {\n"                                                                            \
    "#endif\n"                                                                                   \
    "        return 1;\n"                                                                        \
    "    }\n"                                                                                    \
    "    return 0;\n"                                                                            \
    "}\n"
#define C_TU_STRANDED_CALLER                                                                    \
    "#include \"sapi.h\"\n"                                                                      \
    "int run_shared(void) { return shared(1); }\n"                                               \
    "int run_alone(void) { return alone(2); }\n"
static const RQ_Source C_TU_STRANDED[] = {
    {.path = "sapi.h",
     .text = "#ifndef SAPI_H\n#define SAPI_H\nint shared(int x);\nint alone(int x);\n#endif\n"},
    {.path = "split_shared.c", .text = "static int shared(int x)" C_TU_STRANDED_BODY},
    {.path = "split_alone.c", .text = "static int alone(int x)" C_TU_STRANDED_BODY},
    {.path = "real.c", .text = "int shared(int x) { return x; }\n"},
    {.path = "scaller.c", .text = C_TU_STRANDED_CALLER},
};

TEST(resolver_c_a_static_left_in_an_error_stays_static) {
    const char *caller = C_TU_STRANDED_CALLER;
    const RQ_Expected expected[] = {
        {.case_id = "the-real-definition-not-the-static-one",
         .site = C_TU_WORD("scaller.c", caller, "scaller.run_shared", "shared(1)", "shared(1)"),
         .relation = "CALLS",
         .target = {.file = "real.c",
                    .suffix = "real.shared",
                    .label = "Function",
                    .declaration_line = 1,
                    .declaration_text = "int shared(int x) { return x; }"}},
    };
    const RQ_Forbidden forbidden[] = {
        {.case_id = "a-static-in-another-file-is-not-the-definition",
         .site = C_TU_WORD("scaller.c", caller, "scaller.run_alone", "alone(2)", "alone(2)"),
         .relation = "CALLS",
         .any_target = true},
    };
    const CTUCase fixture = {.suite = "c-tu-stranded-static",
                             .sources = C_TU_STRANDED,
                             .source_count = sizeof(C_TU_STRANDED) / sizeof(C_TU_STRANDED[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0]),
                             .forbidden = forbidden,
                             .forbidden_count = sizeof(forbidden) / sizeof(forbidden[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* Whether a body holds a `#define`, to its last byte: a `#` with nothing after it, a word cut off by the
 * end of the text, a directive with space after the `#`, one after another directive. */
TEST(resolver_c_text_defines_macro_reads_to_the_end_of_the_text) {
    static const struct {
        const char *text;
        bool defines;
    } cases[] = {
        {"", false},          {"#", false},         {"#   ", false},          {"#defin", false},
        {"#define", true},    {"# \tdefine X", true}, {"x = 1;\n#define Y 2", true},
        {"#include <a.h>\n#define Z", true},        {"#include <a.h>\n", false},
        {"# # define", true}, {"a # b", false},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        bool found = cbm_c_text_defines_macro(cases[i].text, (uint32_t)strlen(cases[i].text));
        ASSERT_EQ(found, cases[i].defines);
    }
    PASS();
}

/* A header runs on the pass over one file alone, with no source file on its types and in C++ mode:
 * `struct other *p;` as a member and in a prototype name the header's own tag. One in a namespace is
 * out of reach of code outside it there as well. */
#define C_TU_TAG_HEADER                                                                         \
    "struct other { int a; };\n"                                                                 \
    "namespace within { struct nested { int b; }; }\n"                                           \
    "struct holder {\n"                                                                          \
    "  struct other *member;\n"                                                                  \
    "  struct nested *wrong_member;\n"                                                           \
    "};\n"                                                                                       \
    "int prototype(struct other *q);\n"                                                          \
    "namespace within { struct inner_holder { struct nested *right_member; }; }\n"
static const RQ_Source C_TU_TAG_HEADERS[] = {{.path = "taghdr.hpp", .text = C_TU_TAG_HEADER}};

TEST(resolver_cpp_tag_in_a_header_is_looked_up_from_its_own_module) {
    const char *source = C_TU_TAG_HEADER;
    const RQ_Target other = {.file = "taghdr.hpp",
                             .suffix = "taghdr.other",
                             .label = "Class",
                             .declaration_line = 1,
                             .declaration_text = "struct other { int a; };"};
    const RQ_Expected expected[] = {
        {.case_id = "a-member-names-the-header-s-own-tag",
         .site = C_TU_WORD("taghdr.hpp", source, "taghdr", "other *member", "other"),
         .relation = "USAGE",
         .target = other},
        {.case_id = "a-prototype-names-it-too",
         .site = C_TU_WORD("taghdr.hpp", source, "taghdr", "other *q", "other"),
         .relation = "USAGE",
         .target = other},
        {.case_id = "a-namespace-reaches-its-own-tag",
         .site = C_TU_WORD("taghdr.hpp", source, "taghdr", "nested *right_member", "nested"),
         .relation = "USAGE",
         .target = {.file = "taghdr.hpp",
                    .suffix = "taghdr.within.nested",
                    .label = "Class",
                    .declaration_line = 2,
                    .declaration_text = "namespace within { struct nested { int b; }; }"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "C-NAMESPACED-CLASS-MEMBER-TYPE",
         .why = "A member declared in a class inside a namespace has no type row. At the top level of "
                "a file it has one.",
         .tried = "Read on the binaries before and after the tag rule: the row is absent on both, so "
                  "it is not the lookup. Sized and left for a later rule."},
    };
    const RQ_Forbidden forbidden[] = {
        {.case_id = "outside-the-namespace-it-is-another-tag",
         .site = C_TU_WORD("taghdr.hpp", source, "taghdr", "nested *wrong_member", "nested"),
         .relation = "USAGE",
         .any_target = true},
    };
    const CTUCase fixture = {.suite = "c-tu-tag-in-header",
                             .sources = C_TU_TAG_HEADERS,
                             .source_count = 1,
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0]),
                             .known_gaps = 1,
                             .forbidden = forbidden,
                             .forbidden_count = sizeof(forbidden) / sizeof(forbidden[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* A lone tag declares it, wherever it stands. Nothing resolves there, so the decoys above stay
 * quiet with or without this: what it changes is an occurrence with no answer, counted as unresolved. */
static int c_usage_rows_named(const CBMFileResult *result, const char *name) {
    int count = 0;
    for (int i = 0; i < result->usages.count; i++)
        count += result->usages.items[i].ref_name && !strcmp(result->usages.items[i].ref_name, name);
    return count;
}

TEST(resolver_c_lone_tag_is_no_occurrence) {
    static const char source[] = "struct lone_file;\n"
                                 "struct used_file *a;\n"
                                 "int f(void) { struct lone_body; struct used_body *b; return b != 0; }\n"
                                 "struct holder { struct lone_member; struct used_member *c; };\n";
    CBMFileResult *c_file =
        cbm_extract_file(source, (int)strlen(source), CBM_LANG_C, "p", "tag.c", 0, NULL, NULL);
    ASSERT(c_file != NULL);
    ASSERT_EQ(c_usage_rows_named(c_file, "lone_file"), 0);
    ASSERT_EQ(c_usage_rows_named(c_file, "lone_body"), 0);
    ASSERT_EQ(c_usage_rows_named(c_file, "lone_member"), 0);
    ASSERT_EQ(c_usage_rows_named(c_file, "used_file"), 1);
    ASSERT_EQ(c_usage_rows_named(c_file, "used_body"), 1);
    ASSERT_EQ(c_usage_rows_named(c_file, "used_member"), 1);
    cbm_free_result(c_file);
    static const char cxx_source[] = "class lone_class;\n"
                                     "template <class T> struct lone_template;\n"
                                     "namespace n { struct lone_in_namespace; }\n"
                                     "class used_class *d;\n";
    CBMFileResult *cxx_file = cbm_extract_file(cxx_source, (int)strlen(cxx_source), CBM_LANG_CPP, "p",
                                               "tag.cpp", 0, NULL, NULL);
    ASSERT(cxx_file != NULL);
    ASSERT_EQ(c_usage_rows_named(cxx_file, "lone_class"), 0);
    ASSERT_EQ(c_usage_rows_named(cxx_file, "lone_template"), 0);
    ASSERT_EQ(c_usage_rows_named(cxx_file, "lone_in_namespace"), 0);
    ASSERT_EQ(c_usage_rows_named(cxx_file, "used_class"), 1);
    cbm_free_result(cxx_file);
    PASS();
}

/* C++ looks a tag up as it looks up anything: a class in a block hides the outer one from there on, and
 * one in a namespace is out of reach of code outside it. `struct Name` needs no earlier declaration, so
 * it compiles where a plain `Name` would not, and it means a new type there. */
#define C_TU_TL                                                                                 \
    "struct Outer { int o; };\n"                                                                \
    "class Klass { public: int k; };\n"                                                         \
    "namespace ns {\n"                                                                          \
    "struct InNs { int n; };\n"                                                                 \
    "int inside(struct InNs *a) { return a->n; }\n"                                             \
    "}\n"                                                                                       \
    "namespace used { struct Used { int u; }; }\n"                                              \
    "using namespace used;\n"                                                                   \
    "int via_using(struct Used *b) { return b->u; }\n"                                          \
    "int outside(struct InNs *c) { return c != nullptr; }\n"                                    \
    "int outside_member(struct InNs *d) { return d->n; }\n"                                     \
    "int local_class() { class Klass { public: int k; }; Klass e; return e.k; }\n"              \
    "int lone_class() { class Klass; Klass *f = nullptr; return f != nullptr; }\n"              \
    "int lone_struct() { struct Outer; Outer *g = nullptr; return g != nullptr; }\n"            \
    "int plain_class() { Klass h; return h.k; }\n"                                              \
    "int elaborated_class() { class Klass *i = nullptr; return i->k; }\n"
static const RQ_Source C_TU_TAG_LOOKUP[] = {{.path = "tl.cpp", .text = C_TU_TL}};

TEST(resolver_cpp_tag_lookup_stops_where_the_language_does) {
    const char *source = C_TU_TL;
    const RQ_Target klass = {.file = "tl.cpp",
                             .suffix = "tl.Klass",
                             .label = "Class",
                             .declaration_line = 2,
                             .declaration_text = "class Klass { public: int k; };"};
    const RQ_Target klass_k = {.file = "tl.cpp",
                               .suffix = "tl.Klass.k",
                               .label = "Field",
                               .declaration_line = 2,
                               .declaration_text = "class Klass { public: int k; };"};
#define TL_USE(id, caller, context, word, target_)                                              \
    {.case_id = id,                                                                              \
     .site = C_TU_WORD("tl.cpp", source, "tl." caller, context, word),                           \
     .relation = "USAGE",                                                                        \
     .target = target_}
#define TL_NEVER(id, caller, context, word)                                                     \
    {.case_id = id,                                                                              \
     .site = C_TU_WORD("tl.cpp", source, "tl." caller, context, word),                           \
     .relation = "USAGE",                                                                        \
     .any_target = true}
    const RQ_Expected expected[] = {
        TL_USE("a-tag-in-the-namespace-around", "ns.inside", "InNs *a", "InNs",
               ((RQ_Target){.file = "tl.cpp",
                            .suffix = "tl.ns.InNs",
                            .label = "Class",
                            .declaration_line = 4,
                            .declaration_text = "struct InNs { int n; };"})),
        TL_USE("a-member-through-it", "ns.inside", "a->n", "n",
               ((RQ_Target){.file = "tl.cpp",
                            .suffix = "tl.ns.InNs.n",
                            .label = "Field",
                            .declaration_line = 4,
                            .declaration_text = "struct InNs { int n; };"})),
        TL_USE("a-tag-by-a-using-directive", "via_using", "Used *b", "Used",
               ((RQ_Target){.file = "tl.cpp",
                            .suffix = "tl.used.Used",
                            .label = "Class",
                            .declaration_line = 7,
                            .declaration_text = "namespace used { struct Used { int u; }; }"})),
        TL_USE("a-member-by-a-using-directive", "via_using", "b->u", "u",
               ((RQ_Target){.file = "tl.cpp",
                            .suffix = "tl.used.Used.u",
                            .label = "Field",
                            .declaration_line = 7,
                            .declaration_text = "namespace used { struct Used { int u; }; }"})),
        TL_USE("the-outer-class-by-name", "plain_class", "Klass h", "Klass", klass),
        TL_USE("a-member-of-it", "plain_class", "h.k", "k", klass_k),
        TL_USE("the-outer-class-by-its-tag", "elaborated_class", "Klass *i", "Klass", klass),
        TL_USE("a-member-through-the-tag", "elaborated_class", "i->k", "k", klass_k),
    };
    const RQ_Forbidden forbidden[] = {
        TL_NEVER("a-tag-out-of-reach-is-a-new-one", "outside", "InNs *c", "InNs"),
        TL_NEVER("and-through-the-other-route", "outside_member", "InNs *d", "InNs"),
        TL_NEVER("with-no-member-through-it", "outside_member", "d->n", "n"),
        TL_NEVER("a-local-class-hides-the-outer-one", "local_class", "Klass e", "Klass"),
        TL_NEVER("and-its-members-with-it", "local_class", "e.k", "k"),
        TL_NEVER("a-lone-class-in-a-block-too", "lone_class", "Klass *f", "Klass"),
        TL_NEVER("a-lone-struct-in-a-block-too", "lone_struct", "Outer *g", "Outer"),
    };
#undef TL_USE
#undef TL_NEVER
    const CTUCase fixture = {.suite = "c-tu-tag-lookup",
                             .sources = C_TU_TAG_LOOKUP,
                             .source_count = sizeof(C_TU_TAG_LOOKUP) / sizeof(C_TU_TAG_LOOKUP[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0]),
                             .forbidden = forbidden,
                             .forbidden_count = sizeof(forbidden) / sizeof(forbidden[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* `hl_table[i].x` with `#define hl_table ((HlGroup *)(ga.ga_data))`: the replacement writes the type,
 * and with a paste `SDS_HDR(8, s)->len` writes it through the argument. It has to be one cast as a whole
 * and in parentheses: `(T *)p` used as `X->y` casts p->y. Every definition in force has to agree. */
#define C_TU_CAST_H                                                                             \
    "#ifndef C_H\n"                                                                             \
    "#define C_H\n"                                                                             \
    "typedef struct { int a; } Group;\n"                                                        \
    "struct sdshdr8 { int len; };\n"                                                            \
    "struct sdshdr16 { int len; };\n"                                                           \
    "typedef struct { void *data; } GA;\n"                                                      \
    "extern GA ga;\n"                                                                           \
    "typedef struct { int a; } T;\n"                                                            \
    "typedef struct { int a; } Other;\n"                                                        \
    "#define table ((Group *)(ga.data))\n"                                                      \
    "#define ITEMS(g) ((Group *)((g)->data))\n"                                                 \
    "#define HDR(T, s) ((struct sdshdr##T *)((s) - sizeof(struct sdshdr##T)))\n"                \
    "#define BARE (Group *)ga.data\n"                                                           \
    "#define PARAM_TYPE(T, p) ((T *)(p))\n"                                                     \
    "#define SUM(p) ((Group *)(p) + 1)\n"                                                       \
    "#ifdef FEATURE\n"                                                                          \
    "#define BRANCHED ((Group *)ga.data)\n"                                                     \
    "#else\n"                                                                                   \
    "#define BRANCHED ((Other *)ga.data)\n"                                                        \
    "#endif\n"                                                                                  \
    "#endif\n"
#define C_TU_CAST_USE                                                                           \
    "#include \"cast.h\"\n"                                                                     \
    "int from_table(int i) { return table[i].a; }\n"                                            \
    "int from_call(GA *g, int i) { return ITEMS(g)[i].a; }\n"                                   \
    "int pasted8(char *s) { return HDR(8, s)->len; }\n"                                         \
    "int pasted16(char *s) { return HDR(16, s)->len; }\n"                                       \
    "int bare(void) { return BARE->a; }\n"                                                      \
    "int param(GA *g) { return PARAM_TYPE(Group, g)->a; }\n"                                    \
    "int summed(void) { return SUM(ga.data)->a; }\n"                                            \
    "int branched(void) { return BRANCHED->a; }\n"
static const RQ_Source C_TU_CAST[] = {{.path = "cast.h", .text = C_TU_CAST_H},
                                      {.path = "cast_use.c", .text = C_TU_CAST_USE}};

TEST(resolver_c_macro_that_is_one_cast_writes_the_receiver_type) {
    const char *use = C_TU_CAST_USE;
    const RQ_Target group = {.file = "cast.h",
                             .suffix = "cast.Group",
                             .label = "Class",
                             .declaration_line = 3,
                             .declaration_text = "typedef struct { int a; } Group;"};
    const RQ_Target member_a = {.file = "cast.h",
                                .suffix = "cast.Group.a",
                                .label = "Field",
                                .declaration_line = 3,
                                .declaration_text = "typedef struct { int a; } Group;"};
    const RQ_Target ga_type = {.file = "cast.h",
                               .suffix = "cast.GA",
                               .label = "Class",
                               .declaration_line = 6,
                               .declaration_text = "typedef struct { void *data; } GA;"};
    const RQ_Target ga_data = {.file = "cast.h",
                               .suffix = "cast.GA.data",
                               .label = "Field",
                               .declaration_line = 6,
                               .declaration_text = "typedef struct { void *data; } GA;"};
#define CAST_USE(id, caller, context, word, target_)                                            \
    {.case_id = id,                                                                              \
     .site = C_TU_WORD("cast_use.c", use, "cast_use." caller, context, word),                    \
     .relation = "USAGE",                                                                        \
     .target = target_}
    const RQ_Expected expected[] = {
        CAST_USE("the-type-an-object-macro-casts-to", "from_table", "table[i]", "table", group),
        CAST_USE("a-member-through-an-object-macro", "from_table", "a; }\nint from_call", "a", member_a),
        CAST_USE("the-parameter-type", "from_call", "GA *g", "GA", ga_type),
        {.case_id = "the-type-a-function-macro-casts-to",
         .site = {.file = "cast_use.c", .caller = "cast_use.from_call", .needle = "ITEMS(g)"},
         .relation = "USAGE",
         .dispatch_group = "what-items-reads",
         .target = group},
        {.case_id = "the-member-a-function-macro-reads",
         .site = {.file = "cast_use.c", .caller = "cast_use.from_call", .needle = "ITEMS(g)"},
         .relation = "USAGE",
         .dispatch_group = "what-items-reads",
         .target = ga_data},
        CAST_USE("a-member-through-a-function-macro", "from_call", "a; }\nint pasted8", "a", member_a),
        CAST_USE("a-paste-of-one-argument", "pasted8", "HDR(8, s)->len", "len",
                 ((RQ_Target){.file = "cast.h",
                              .suffix = "cast.sdshdr8.len",
                              .label = "Field",
                              .declaration_line = 4,
                              .declaration_text = "struct sdshdr8 { int len; };"})),
        CAST_USE("a-paste-of-another", "pasted16", "HDR(16, s)->len", "len",
                 ((RQ_Target){.file = "cast.h",
                              .suffix = "cast.sdshdr16.len",
                              .label = "Field",
                              .declaration_line = 5,
                              .declaration_text = "struct sdshdr16 { int len; };"})),
        CAST_USE("the-type-in-a-bare-cast", "bare", "BARE->a", "BARE", group),
        CAST_USE("the-parameter-of-the-parameter-case", "param", "GA *g) { return PARAM", "GA", ga_type),
        {.case_id = "the-type-in-a-sum",
         .site = {.file = "cast_use.c", .caller = "cast_use.summed", .needle = "SUM(ga.data)"},
         .relation = "USAGE",
         .target = group},
        CAST_USE("the-argument-of-the-sum", "summed", "ga.data)->a", "data", ga_data),
    };
#undef CAST_USE
#define CAST_NEVER(id, caller, context)                                                         \
    {.case_id = id,                                                                              \
     .site = C_TU_WORD("cast_use.c", use, "cast_use." caller, context, "a; }"),                  \
     .relation = "USAGE",                                                                        \
     .any_target = true}
    const RQ_Forbidden forbidden[] = {
        {.case_id = "no-parentheses-means-the-member-is-cast",
         .site = C_TU_WORD("cast_use.c", use, "cast_use.bare", "BARE->a", "a"),
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "a-parameter-as-the-type",
         .site = C_TU_WORD("cast_use.c", use, "cast_use.param", "g)->a", "a"),
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "a-sum-is-not-read",
         .site = C_TU_WORD("cast_use.c", use, "cast_use.summed", ")->a", "a"),
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "definitions-that-cast-differently",
         .site = C_TU_WORD("cast_use.c", use, "cast_use.branched", "BRANCHED->a", "a"),
         .relation = "USAGE",
         .any_target = true},
    };
#undef CAST_NEVER
    const CTUCase fixture = {.suite = "c-tu-cast-macros",
                             .sources = C_TU_CAST,
                             .source_count = sizeof(C_TU_CAST) / sizeof(C_TU_CAST[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0]),
                             .forbidden = forbidden,
                             .forbidden_count = sizeof(forbidden) / sizeof(forbidden[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* `#define M0(x) M1(x) M1(x)` and so on, down `levels` macros to `leaf`. */
static size_t c_handoff_chain(char *out, size_t cap, const char *name, int levels, int fan,
                              const char *leaf) {
    size_t at = 0;
    for (int level = 0; level < levels; level++) {
        at += (size_t)snprintf(out + at, cap - at, "#define %s%d(x)", name, level);
        for (int i = 0; i < fan; i++)
            at += (size_t)snprintf(out + at, cap - at, " %s%d(x)", name, level + 1);
        at += (size_t)snprintf(out + at, cap - at, "\n");
    }
    at += (size_t)snprintf(out + at, cap - at, "#define %s%d(x) %s\n", name, levels, leaf);
    return at;
}

/* The work of one use at the top of a chain whose every macro hands its argument on twice. */
static uint64_t c_handoff_work(int levels, const char *use) {
    char source[8192];
    size_t at = c_handoff_chain(source, sizeof(source), "M", levels, 2, "sink(x)");
    snprintf(source + at, sizeof(source) - at,
             "void sink(int v);\nint value;\nvoid run(void) { %s }\n", use);
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMLSPDef defs[] = {{.qualified_name = "test.main.@c-file-facts",
                         .short_name = "@c-file-facts",
                         .label = CBM_C_FILE_FACTS_LABEL,
                         .def_module_qn = "test.main"}};
    CBMCFileFacts facts = {0};
    bool collected = cbm_c_collect_file_facts(&arena, source, (int)strlen(source), "test.main",
                                              CBM_LANG_C, defs, 1, &facts);
    defs[0].c_file_facts = &facts;
    CBMResolvedCallArray rows = {0};
    (void)cbm_lsp_work_take();
    if (collected)
        c_cross(&arena, source, false, defs, 1, NULL, NULL, 0, &rows);
    uint64_t work = cbm_lsp_work_take();
    cbm_arena_destroy(&arena);
    return collected ? work : 0;
}

/* Following every hand-off doubles with each hop: 2^16 reads of a declaration bucket for one use,
 * under the old limit. Each (macro, position) is read once now, so the work follows the chain's
 * length, on both walks. Counted, not timed. */
TEST(resolver_c_macro_handoffs_are_read_once_per_macro) {
    const char *const uses[] = {"M0(value);", "M0(item) { sink(1); }"};
    for (size_t i = 0; i < sizeof(uses) / sizeof(uses[0]); i++) {
        uint64_t shorter = c_handoff_work(6, uses[i]);
        uint64_t longer = c_handoff_work(12, uses[i]);
        printf("    %s work: 6 levels=%llu  12 levels=%llu\n", uses[i],
               (unsigned long long)shorter, (unsigned long long)longer);
        ASSERT_GT(shorter, 0);
        ASSERT(longer < shorter * 3);
    }
    PASS();
}

/* What a hand-off proves does not depend on how many there are: the old limit of 16 was a work bound
 * that decided the answer. A cycle ends where the preprocessor ends it, since a macro is not expanded
 * inside its own expansion, and what a definition in the cycle does to its argument still counts. */
TEST(resolver_c_macro_handoff_chain_has_no_length_limit) {
    static char source[16384];
    size_t at = (size_t)snprintf(source, sizeof(source),
                                 "struct Item { int a; };\n"
                                 "int value;\n"
                                 "int cycled;\n"
                                 "int broken;\n"
                                 "void sink(int v);\n");
    at += c_handoff_chain(source + at, sizeof(source) - at, "P", 40, 1, "sink(x)");
    at += c_handoff_chain(source + at, sizeof(source) - at, "L", 40, 1,
                          "for (struct Item *(x) = 0; (x); (x) = 0)");
    snprintf(source + at, sizeof(source) - at,
             "#define A0(x) A1(x)\n"
             "#define A1(x) A0(x)\n"
             "#define B0(x) B1(x)\n"
             "#define B1(x) B0(x) x##_y\n"
             "void read_value(void) { P0(value); }\n"
             "void read_member(void) { L0(it) { sink(it->a); } }\n"
             "void read_cycled(void) { A0(cycled); }\n"
             "void read_broken(void) { B0(broken); }\n");
    const RQ_Source sources[] = {{.path = "chain.c", .text = source}};
#define CHAIN_USE(id, caller, context, word, suffix_, label_, line, declaration)                \
    {.case_id = id,                                                                              \
     .site = C_TU_WORD("chain.c", source, "chain." caller, context, word),                       \
     .relation = "USAGE",                                                                        \
     .target = {.file = "chain.c",                                                               \
                .suffix = suffix_,                                                               \
                .label = label_,                                                                 \
                .declaration_line = line,                                                        \
                .declaration_text = declaration}}
    const RQ_Expected expected[] = {
        CHAIN_USE("forty-hops-reach-the-value", "read_value", "P0(value)", "value", "chain.value",
                  "Variable", 2, "int value;"),
        CHAIN_USE("forty-hops-reach-the-declaration", "read_member", "it->a", "a", "chain.Item.a",
                  "Field", 1, "struct Item { int a; };"),
        CHAIN_USE("a-cycle-ends-at-the-argument", "read_cycled", "A0(cycled)", "cycled",
                  "chain.cycled", "Variable", 3, "int cycled;"),
    };
#undef CHAIN_USE
    const RQ_Forbidden forbidden[] = {
        {.case_id = "a-paste-in-the-cycle-still-counts",
         .site = C_TU_WORD("chain.c", source, "chain.read_broken", "B0(broken)", "broken"),
         .relation = "USAGE",
         .any_target = true},
    };
    const CTUCase fixture = {.suite = "c-tu-macro-handoff-chains",
                             .sources = sources,
                             .source_count = 1,
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0]),
                             .forbidden = forbidden,
                             .forbidden_count = sizeof(forbidden) / sizeof(forbidden[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* The includes of a sole includer are marked while that step adds them, and only then. The step's
 * usual exit is the top of the chain, in the middle of its loop. */
TEST(resolver_c_includer_context_ends_with_its_step) {
    const char *source = "int f(void) { return 0; }\n";
    TSParser *parser = ts_parser_new();
    ASSERT_NOT_NULL(parser);
    ts_parser_set_language(parser, tree_sitter_c());
    TSTree *tree = ts_parser_parse_string(parser, NULL, source, (uint32_t)strlen(source));
    ts_parser_delete(parser);
    ASSERT_NOT_NULL(tree);
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMLSPDef defs[] = {{.qualified_name = "test.main.@c-file-facts",
                         .short_name = "@c-file-facts",
                         .label = CBM_C_FILE_FACTS_LABEL,
                         .def_module_qn = "test.main"}};
    CBMCFileFacts facts = {0};
    bool collected = cbm_c_collect_file_facts(&arena, source, (int)strlen(source), "test.main",
                                              CBM_LANG_C, defs, 1, &facts);
    defs[0].c_file_facts = &facts;
    CBMTypeRegistry registry;
    cbm_registry_init(&registry, &arena);
    registry.c_decls = cbm_c_decl_index_build(&arena, defs, 1);
    cbm_registry_finalize(&registry);
    CBMResolvedCallArray out = {0};
    CLSPContext ctx;
    c_lsp_init(&ctx, &arena, source, (int)strlen(source), &registry, "test.main", false, &out);
    c_lsp_process_file(&ctx, ts_tree_root_node(tree));
    bool facts_found = cbm_c_file_facts(ctx.c_decls, "test.main") != NULL;
    bool still_marking = ctx.tu_adding_includer_context;
    cbm_arena_destroy(&arena);
    ts_tree_delete(tree);
    ASSERT(collected);
    ASSERT(facts_found);
    ASSERT(!still_marking);
    PASS();
}

/* A C++ union with member functions is a class: its methods read its members, and its members are
 * public by default. The class path of the LSP listed only `class` and `struct`. */
#define C_TU_UN                                                                                 \
    "union Number {\n"                                                                          \
    "  int i;\n"                                                                                \
    "  int helper() { return i; }\n"                                                            \
    "  int get() { return helper(); }\n"                                                        \
    "};\n"                                                                                      \
    "int caller(Number &n) { return n.get(); }\n"
static const RQ_Source C_TU_UNION[] = {{.path = "un.cpp", .text = C_TU_UN}};

TEST(resolver_cpp_union_is_a_class_with_public_members) {
    const char *source = C_TU_UN;
    const RQ_Expected expected[] = {
        {.case_id = "a-member-from-a-method",
         .site = C_TU_WORD("un.cpp", source, "un.Number.helper", "return i;", "i"),
         .relation = "USAGE",
         .target = {.file = "un.cpp",
                    .suffix = "un.Number.i",
                    .label = "Field",
                    .declaration_line = 2,
                    .declaration_text = "  int i;"}},
        {.case_id = "a-sibling-from-a-method",
         .site = {.file = "un.cpp",
                  .caller = "un.Number.get",
                  .needle = "helper()",
                  .occurrence_index = 1},
         .relation = "CALLS",
         .target = {.file = "un.cpp",
                    .suffix = "un.Number.helper",
                    .label = "Method",
                    .declaration_line = 3,
                    .declaration_text = "  int helper() { return i; }"}},
        {.case_id = "a-method-from-outside",
         .site = {.file = "un.cpp", .caller = "un.caller", .needle = "n.get()"},
         .relation = "CALLS",
         .target = {.file = "un.cpp",
                    .suffix = "un.Number.get",
                    .label = "Method",
                    .declaration_line = 4,
                    .declaration_text = "  int get() { return helper(); }"}},
        {.case_id = "the-union-in-a-signature",
         .site = C_TU_WORD("un.cpp", source, "un.caller", "Number &n", "Number"),
         .relation = "USAGE",
         .target = {.file = "un.cpp",
                    .suffix = "un.Number",
                    .label = "Class",
                    .declaration_line = 1,
                    .declaration_text = "union Number {"}},
    };
    const CTUCase fixture = {.suite = "c-tu-cxx-unions",
                             .sources = C_TU_UNION,
                             .source_count = sizeof(C_TU_UNION) / sizeof(C_TU_UNION[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* A class in an anonymous namespace is declared in the file around it. Each .cpp has its own, and
 * another file's class of the same name stays out of it. */
#define C_TU_ANON_ONE                                                                           \
    "namespace {\n"                                                                             \
    "int freeHelper(int v) { return v; }\n"                                                     \
    "class Local {\n"                                                                           \
    "public:\n"                                                                                 \
    "  int helper() { return 1; }\n"                                                            \
    "  int run(int v) { return freeHelper(v) + helper(); }\n"                                   \
    "};\n"                                                                                      \
    "}\n"                                                                                       \
    "int useLocal() { Local l; return l.run(1); }\n"
#define C_TU_ANON_TWO                                                                           \
    "namespace {\n"                                                                             \
    "class Local {\n"                                                                           \
    "public:\n"                                                                                 \
    "  int run(int v) { return v; }\n"                                                          \
    "};\n"                                                                                      \
    "}\n"                                                                                       \
    "int useOther() { Local l; return l.run(2); }\n"
static const RQ_Source C_TU_ANON[] = {{.path = "anon_one.cpp", .text = C_TU_ANON_ONE},
                                      {.path = "anon_two.cpp", .text = C_TU_ANON_TWO}};

TEST(resolver_cpp_anonymous_namespace_declares_in_its_file) {
    const char *one = C_TU_ANON_ONE;
    const char *two = C_TU_ANON_TWO;
    const RQ_Expected expected[] = {
        {.case_id = "free-function-from-a-method",
         .site = {.file = "anon_one.cpp", .caller = "anon_one.Local.run", .needle = "freeHelper(v)"},
         .relation = "CALLS",
         .target = {.file = "anon_one.cpp",
                    .suffix = "anon_one.freeHelper",
                    .label = "Function",
                    .declaration_line = 2,
                    .declaration_text = "int freeHelper(int v) { return v; }"}},
        {.case_id = "sibling-method",
         .site = {.file = "anon_one.cpp",
                  .caller = "anon_one.Local.run",
                  .needle = "helper()",
                  .occurrence_index = 1},
         .relation = "CALLS",
         .target = {.file = "anon_one.cpp",
                    .suffix = "anon_one.Local.helper",
                    .label = "Method",
                    .declaration_line = 5,
                    .declaration_text = "  int helper() { return 1; }"}},
        {.case_id = "receiver-of-the-files-own-class",
         .site = {.file = "anon_one.cpp", .caller = "anon_one.useLocal", .needle = "l.run(1)"},
         .relation = "CALLS",
         .target = {.file = "anon_one.cpp",
                    .suffix = "anon_one.Local.run",
                    .label = "Method",
                    .declaration_line = 6,
                    .declaration_text = "  int run(int v) { return freeHelper(v) + helper(); }"}},
        {.case_id = "same-name-in-another-file",
         .site = {.file = "anon_two.cpp", .caller = "anon_two.useOther", .needle = "l.run(2)"},
         .relation = "CALLS",
         .target = {.file = "anon_two.cpp",
                    .suffix = "anon_two.Local.run",
                    .label = "Method",
                    .declaration_line = 4,
                    .declaration_text = "  int run(int v) { return v; }"}},
        {.case_id = "type-of-the-files-own-class",
         .site = C_TU_WORD("anon_one.cpp", one, "anon_one.useLocal", "{ Local l", "Local"),
         .relation = "USAGE",
         .target = {.file = "anon_one.cpp",
                    .suffix = "anon_one.Local",
                    .label = "Class",
                    .declaration_line = 3,
                    .declaration_text = "class Local {"}},
        {.case_id = "type-of-the-other-files-own-class",
         .site = C_TU_WORD("anon_two.cpp", two, "anon_two.useOther", "useOther() { ", "Local"),
         .relation = "USAGE",
         .target = {.file = "anon_two.cpp",
                    .suffix = "anon_two.Local",
                    .label = "Class",
                    .declaration_line = 2,
                    .declaration_text = "class Local {"}},
    };
    const CTUCase fixture = {.suite = "c-tu-anon",
                             .sources = C_TU_ANON,
                             .source_count = sizeof(C_TU_ANON) / sizeof(C_TU_ANON[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* `.free = my_free` names a member with no object in front of it, and the type being initialized
 * is its receiver: 1,228 such sites in redis and 1,586 in neovim. A type this file cannot see
 * proves nothing. */
#define C_TU_DESIG_H                                                                            \
    "#ifndef DESIG_H\n"                                                                         \
    "#define DESIG_H\n"                                                                         \
    "typedef struct inner_S { int depth; } inner_T;\n"                                          \
    "typedef struct outer_S {\n"                                                                \
    "  int size;\n"                                                                             \
    "  inner_T in;\n"                                                                           \
    "} outer_T;\n"                                                                              \
    "#endif\n"
#define C_TU_DESIG_USE                                                                          \
    "#include \"desig.h\"\n"                                                                    \
    "void declared(void) {\n"                                                                   \
    "  outer_T o = {.size = 1};\n"                                                              \
    "  (void)o;\n"                                                                              \
    "}\n"                                                                                       \
    "void literal(void) { (void)(inner_T){.depth = 2}; }\n"                                     \
    "void nested(void) {\n"                                                                     \
    "  outer_T o = {.in = {.depth = 3}};\n"                                                     \
    "  (void)o;\n"                                                                              \
    "}\n"                                                                                       \
    "unsigned long size_of(void) { return sizeof(inner_T); }\n"                                 \
    "unsigned long size_of_local(int inner_T) { unsigned long n = sizeof(inner_T); return n; }\n"
#define C_TU_DESIG_OTHER "typedef struct hidden_S { int count; } hidden_T;\n"
#define C_TU_DESIG_DECOY                                                                        \
    "#include \"desig.h\"\n"                                                                    \
    "void not_included(void) {\n"                                                               \
    "  hidden_T b = {.count = 1};\n"                                                            \
    "  (void)b;\n"                                                                              \
    "}\n"
static const RQ_Source C_TU_DESIG[] = {{.path = "desig.h", .text = C_TU_DESIG_H},
                                       {.path = "desig_use.c", .text = C_TU_DESIG_USE},
                                       {.path = "hidden.h", .text = C_TU_DESIG_OTHER},
                                       {.path = "desig_decoy.c", .text = C_TU_DESIG_DECOY}};

TEST(resolver_c_designated_member_binds_through_the_type_being_initialized) {
    const char *use = C_TU_DESIG_USE;
    const char *decoy = C_TU_DESIG_DECOY;
    const RQ_Target size = {.file = "desig.h",
                            .suffix = "desig.outer_S.size",
                            .label = "Field",
                            .declaration_line = 5,
                            .declaration_text = "  int size;"};
    const RQ_Target depth = {.file = "desig.h",
                             .suffix = "desig.inner_S.depth",
                             .label = "Field",
                             .declaration_line = 3,
                             .declaration_text = "typedef struct inner_S { int depth; } inner_T;"};
    const RQ_Target outer = {.file = "desig.h",
                             .suffix = "desig.outer_T",
                             .label = "Type",
                             .declaration_line = 7,
                             .declaration_text = "} outer_T;"};
    const RQ_Expected expected[] = {
        {.case_id = "type-of-a-declared-variable",
         .site = C_TU_WORD("desig_use.c", use, "desig_use.declared", "outer_T o = {.size", "outer_T"),
         .relation = "USAGE",
         .target = outer},
        {.case_id = "member-of-a-declared-variable",
         .site = C_TU_WORD("desig_use.c", use, "desig_use.declared", "{.size", "size"),
         .relation = "USAGE",
         .target = size},
        {.case_id = "type-of-a-compound-literal",
         .site = C_TU_WORD("desig_use.c", use, "desig_use.literal", "(inner_T){", "inner_T"),
         .relation = "USAGE",
         .target = {.file = "desig.h",
                    .suffix = "desig.inner_T",
                    .label = "Type",
                    .declaration_line = 3,
                    .declaration_text = "typedef struct inner_S { int depth; } inner_T;"}},
        {.case_id = "member-of-a-compound-literal",
         .site = C_TU_WORD("desig_use.c", use, "desig_use.literal", "{.depth", "depth"),
         .relation = "USAGE",
         .target = depth},
        {.case_id = "type-of-the-outer-variable",
         .site = C_TU_WORD("desig_use.c", use, "desig_use.nested", "outer_T o = {.in", "outer_T"),
         .relation = "USAGE",
         .target = outer},
        {.case_id = "member-holding-a-list",
         .site = C_TU_WORD("desig_use.c", use, "desig_use.nested", "{.in", "in"),
         .relation = "USAGE",
         .target = {.file = "desig.h",
                    .suffix = "desig.outer_S.in",
                    .label = "Field",
                    .declaration_line = 6,
                    .declaration_text = "  inner_T in;"}},
        {.case_id = "member-of-a-member",
         .site = C_TU_WORD("desig_use.c", use, "desig_use.nested", "{.in = {.depth", "depth"),
         .relation = "USAGE",
         .target = depth},
        {.case_id = "type-inside-sizeof",
         .site = C_TU_WORD("desig_use.c", use, "desig_use.size_of", "sizeof(inner_T", "inner_T"),
         .relation = "USAGE",
         .target = {.file = "desig.h",
                    .suffix = "desig.inner_T",
                    .label = "Type",
                    .declaration_line = 3,
                    .declaration_text = "typedef struct inner_S { int depth; } inner_T;"}},
    };
    const RQ_Forbidden forbidden[] = {
        {.case_id = "a-local-inside-sizeof",
         .site = C_TU_WORD("desig_use.c", use, "desig_use.size_of_local", "n = sizeof(", "inner_T"),
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "a-type-this-file-does-not-see",
         .site = C_TU_WORD("desig_decoy.c", decoy, "desig_decoy.not_included", "{.count", "count"),
         .relation = "USAGE",
         .any_target = true},
    };
    const CTUCase fixture = {.suite = "c-tu-designators",
                             .sources = C_TU_DESIG,
                             .source_count = sizeof(C_TU_DESIG) / sizeof(C_TU_DESIG[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0]),
                             .forbidden = forbidden,
                             .forbidden_count = sizeof(forbidden) / sizeof(forbidden[0])};
    for (int driver = 0; driver < 2; driver++)
        ASSERT(c_tu_run(&fixture, driver));
    PASS();
}

/* LLVM's StringRef lost its class to a macro word the attribute rules saw at only some of its uses:
 * a name is a macro throughout a file. A conditional inside parentheses has one reading per branch
 * and neovim's kvec_t(Object) names no type Object. */
#define C_TU_REPAIR_STR_H                                                                       \
    "#ifndef STR_H\n"                                                                           \
    "#define STR_H\n"                                                                           \
    "namespace llvm {\n"                                                                        \
    "int convert(int V);\n"                                                                     \
    "class MY_POINTER Str {\n"                                                                  \
    "public:\n"                                                                                 \
    "  MY_NODISCARD int size() const { return Len; }\n"                                         \
    "  MY_NODISCARD\n"                                                                          \
    "  bool empty() const { return Len == 0; }\n"                                               \
    "  MY_NODISCARD\n"                                                                          \
    "  char operator[](int I) const { return 'x'; }\n"                                          \
    "  MY_NODISCARD\n"                                                                          \
    "  int back() const { return Len; }\n"                                                      \
    "  int Len;\n"                                                                              \
    "};\n"                                                                                      \
    "class Sized {\n"                                                                           \
    "public:\n"                                                                                 \
    "  Sized(int S) : Size(convert(S)) {}\n"                                                    \
    "  int Size;\n"                                                                             \
    "};\n"                                                                                      \
    "}\n"                                                                                       \
    "#endif\n"
#define C_TU_REPAIR_STR_USE                                                                     \
    "#include \"str.h\"\n"                                                                      \
    "using namespace llvm;\n"                                                                   \
    "int useBack(Str S) { return S.back(); }\n"
#define C_TU_REPAIR_ARGS                                                                        \
    "#include \"defs.h\"\n"                                                                     \
    "static int left_side(int v) { return v; }\n"                                               \
    "static int right_side(int v) { return v; }\n"                                              \
    "static int pick(int a, int b) { return a + b; }\n"                                         \
    "int both(int v) {\n"                                                                       \
    "  return pick(v,\n"                                                                        \
    "#ifdef LEFT\n"                                                                             \
    "    left_side(v)\n"                                                                        \
    "#else\n"                                                                                   \
    "    right_side(v)\n"                                                                       \
    "#endif\n"                                                                                  \
    "  );\n"                                                                                    \
    "}\n"
#define C_TU_REPAIR_DEFS_H                                                                      \
    "#ifndef DEFS_H\n#define DEFS_H\ntypedef struct object Object;\n#endif\n"
#define C_TU_REPAIR_VEC_H                                                                       \
    "#ifndef VEC_H\n#define VEC_H\n#include \"defs.h\"\ntypedef kvec_t(Object) Array;\n#endif\n"
#define C_TU_REPAIR_VEC_USE                                                                     \
    "#include \"vec.h\"\n"                                                                      \
    "void useObject(Object *o) { (void)o; }\n"
/* lua_cmsgpack.c: `#if / lua_pushnumber(L, / #else / lua_pushinteger(L, / #endif`, each branch half
 * of a statement, cost every function after it. neovim's do_sub: a comment in a multi-line #define
 * ended the replacement for the grammar, and the rest of the macro closed the function. */
#define C_TU_REPAIR_SPLIT                                                                       \
    "#include \"defs.h\"\n"                                                                     \
    "static int pa(int a, int b) { return a + b; }\n"                                           \
    "int half(int v) {\n"                                                                       \
    "#ifdef OLD\n"                                                                              \
    "  int r = pa(v,\n"                                                                         \
    "#else\n"                                                                                   \
    "  int r = pa(v + 1,\n"                                                                     \
    "#endif\n"                                                                                  \
    "    v);\n"                                                                                 \
    "  return r;\n"                                                                             \
    "}\n"                                                                                       \
    "int after_half(int v) { return pa(v, 7); }\n"                                              \
    "int with_comment(int v) {\n"                                                               \
    "#define STEP() \\\n"                                                                       \
    "  do { \\\n"                                                                               \
    "    /* a comment in the replacement */ \\\n"                                               \
    "    if (v) { \\\n"                                                                         \
    "      v = 0; \\\n"                                                                         \
    "    } \\\n"                                                                                \
    "  } while (0)\n"                                                                           \
    "\n"                                                                                        \
    "  int i = pa(v, 9);\n"                                                                     \
    "  return i;\n"                                                                             \
    "}\n"
static const RQ_Source C_TU_REPAIR[] = {{.path = "str.h", .text = C_TU_REPAIR_STR_H},
                                        {.path = "split.c", .text = C_TU_REPAIR_SPLIT},
                                        {.path = "str_use.cpp", .text = C_TU_REPAIR_STR_USE},
                                        {.path = "args.c", .text = C_TU_REPAIR_ARGS},
                                        {.path = "defs.h", .text = C_TU_REPAIR_DEFS_H},
                                        {.path = "vec.h", .text = C_TU_REPAIR_VEC_H},
                                        {.path = "vec_use.c", .text = C_TU_REPAIR_VEC_USE}};

TEST(resolver_c_parse_repairs_keep_declarations) {
    const char *vec_use = C_TU_REPAIR_VEC_USE;
    const RQ_Target split_pa = {.file = "split.c",
                                .suffix = "split.pa",
                                .label = "Function",
                                .declaration_line = 2,
                                .declaration_text = "static int pa(int a, int b) { return a + b; }"};
    const RQ_Expected expected[] = {
        {.case_id = "class-past-a-macro-word-the-rules-saw-elsewhere",
         .site = {.file = "str_use.cpp", .caller = "useBack", .needle = "S.back()"},
         .relation = "CALLS",
         .target = {.file = "str.h",
                    .suffix = "llvm.Str.back",
                    .label = "Method",
                    .declaration_line = 13,
                    .declaration_text = "  int back() const { return Len; }"}},
        {.case_id = "constructor-initializer-runs-in-the-constructor",
         .site = {.file = "str.h", .caller = "llvm.Sized.Sized", .needle = "convert(S)"},
         .relation = "CALLS",
         .target = {.file = "str.h",
                    .suffix = "llvm.convert",
                    .label = NULL,
                    .declaration_line = 4,
                    .declaration_text = "int convert(int V);"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "c-declaration-without-a-definition",
         .why = "convert is declared and never defined in this fixture, so it has no node.",
         .tried = "A prototype alone has no graph node to point at."},
        {.case_id = "first-branch-of-a-conditional-inside-parentheses",
         .site = {.file = "args.c", .caller = "args.both", .needle = "left_side(v)"},
         .relation = "CALLS",
         .target = {.file = "args.c",
                    .suffix = "args.left_side",
                    .label = "Function",
                    .declaration_line = 2,
                    .declaration_text = "static int left_side(int v) { return v; }"}},
        {.case_id = "call-the-conditional-sits-in",
         .site = {.file = "args.c",
                  .caller = "args.both",
                  .needle = "pick(v,\n#ifdef LEFT\n    left_side(v)\n#else\n    right_side(v)\n#endif\n  )"},
         .relation = "CALLS",
         .target = {.file = "args.c",
                    .suffix = "args.pick",
                    .label = "Function",
                    .declaration_line = 4,
                    .declaration_text = "static int pick(int a, int b) { return a + b; }"}},
        {.case_id = "function-after-a-branch-that-is-half-a-statement",
         .site = {.file = "split.c", .caller = "split.after_half", .needle = "pa(v, 7)"},
         .relation = "CALLS",
         .target = split_pa},
        {.case_id = "call-below-a-comment-in-a-multi-line-define",
         .site = {.file = "split.c", .caller = "split.with_comment", .needle = "pa(v, 9)"},
         .relation = "CALLS",
         .target = split_pa},
        {.case_id = "macro-argument-in-a-typedef-names-no-type",
         .site = C_TU_WORD("vec_use.c", vec_use, "vec_use.useObject", "(Object *o", "Object"),
         .relation = "USAGE",
         .target = {.file = "defs.h",
                    .suffix = "defs.Object",
                    .label = "Type",
                    .declaration_line = 3,
                    .declaration_text = "typedef struct object Object;"}},
    };
    const CTUCase fixture = {.suite = "c-tu-repair",
                             .sources = C_TU_REPAIR,
                             .source_count = sizeof(C_TU_REPAIR) / sizeof(C_TU_REPAIR[0]),
                             .expected = expected,
                             .expected_count = sizeof(expected) / sizeof(expected[0]),
                             .known_gaps = 1};
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
    const char *header = C_TU_ACCESS_HEADER;
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
         .site = C_TU_WORD("access.h", header, "access.llvm.ArmParser.run", "return parse", "parse()"),
         .relation = "CALLS",
         .target = {.file = "access.h",
                    .suffix = "access.llvm.Parser.parse",
                    .label = "Method",
                    .declaration_line = 17,
                    .declaration_text = "  int parse() { return 1; }"}},
    };
    const RQ_Forbidden forbidden[] = {
        {.case_id = "private-member-of-a-base",
         .site = C_TU_WORD("access.h", header, "access.llvm.ArmParser.peek", "return secret", "secret()"),
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
    int largest_node = -1;
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
        /* A file's own node does not grow with how often it repeats an include. */
        statement = NULL;
        if (sqlite3_prepare_v2(cbm_store_get_db(store),
                               "SELECT max(length(properties)) FROM nodes "
                               "WHERE file_path='many.h'",
                               -1, &statement, NULL) == SQLITE_OK &&
            sqlite3_step(statement) == SQLITE_ROW)
            largest_node = sqlite3_column_int(statement, 0);
        sqlite3_finalize(statement);
        cbm_store_close(store);
    }
    cbm_pipeline_free(pipeline);
    th_cleanup(root);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(proven, 1);
    ASSERT_GT(largest_node, 0);
    ASSERT_LT(largest_node, 4096);
    PASS();
}

SUITE(resolver_c) {
    RUN_TEST(resolver_c_file_past_the_proof_limit_does_not_stop_publication);
    RUN_TEST(resolver_c_macro_include_proves_nothing_for_its_file);
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
    RUN_TEST(resolver_c_macro_argument_binds_when_the_replacement_substitutes_it);
    RUN_TEST(resolver_c_loop_macro_declares_its_variable);
    RUN_TEST(resolver_c_function_names_are_values_at_any_scope);
    RUN_TEST(resolver_c_struct_shapes_real_headers_use);
    RUN_TEST(resolver_c_parse_repairs_keep_declarations);
    RUN_TEST(resolver_c_macro_replacement_compiles_at_its_use);
    RUN_TEST(resolver_c_macro_in_one_branch_leaves_the_name_unknown);
    RUN_TEST(resolver_c_macro_after_the_parameters_keeps_what_follows);
    RUN_TEST(resolver_c_designated_member_binds_through_the_type_being_initialized);
    RUN_TEST(resolver_cpp_anonymous_namespace_declares_in_its_file);
    RUN_TEST(resolver_c_local_array_types_its_elements);
    RUN_TEST(resolver_c_unnamed_aggregate_is_a_class_of_its_own);
    RUN_TEST(resolver_cpp_qualified_type_binds_from_where_its_lookup_starts);
    RUN_TEST(resolver_cpp_out_of_line_template_member_is_a_method_of_its_class);
    RUN_TEST(resolver_cpp_member_template_is_a_method_of_its_class);
    RUN_TEST(resolver_c_a_broken_head_does_not_swallow_the_functions_after_it);
    RUN_TEST(resolver_c_macro_after_an_arrow_reads_the_members_it_names);
    RUN_TEST(resolver_c_macro_typed_member_keeps_its_own_name);
    RUN_TEST(resolver_cpp_template_field_reads_the_template_and_not_its_parameter_spelling);
    RUN_TEST(resolver_c_struct_tag_is_a_use_unless_it_declares);
    RUN_TEST(resolver_c_lone_tag_is_no_occurrence);
    RUN_TEST(resolver_cpp_tag_in_a_header_is_looked_up_from_its_own_module);
    RUN_TEST(resolver_c_a_static_left_in_an_error_stays_static);
    RUN_TEST(resolver_c_text_defines_macro_reads_to_the_end_of_the_text);
    RUN_TEST(resolver_c_macro_handoffs_are_read_once_per_macro);
    RUN_TEST(resolver_c_macro_handoff_chain_has_no_length_limit);
    RUN_TEST(resolver_c_includer_context_ends_with_its_step);
    RUN_TEST(resolver_cpp_tag_lookup_stops_where_the_language_does);
    RUN_TEST(resolver_c_macro_that_is_one_cast_writes_the_receiver_type);
    RUN_TEST(resolver_cpp_union_is_a_class_with_public_members);
    RUN_TEST(resolver_c_macro_behind_a_switch_this_unit_leaves_off);
    RUN_TEST(resolver_cpp_templates_bind_what_is_not_dependent);
    RUN_TEST(resolver_c_fragment_sees_what_its_one_includer_saw);
    RUN_TEST(resolver_c_macros_in_declarations_do_not_hide_their_names);
    RUN_TEST(resolver_c_callable_mutations_and_escapes_clear_identity);
    RUN_TEST(resolver_c_proof_view_includes_late_registration_and_excludes_inference_tail);
    RUN_TEST(resolver_c_field_identity_is_not_a_callable_target);
}
