#include "cbm.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "pipeline/pipeline.h"
#include "resolve/binding.h"
#include "resolve/policy.h"
#include "store/store.h"
#include "test_framework.h"
#include "test_helpers.h"

static cbm_resolve_result_t js_local(CBMArena *arena, const CBMFileResult *result,
                                     const char *name) {
    return (cbm_resolve_result_t){
        .binding = CBM_RESOLVE_BINDING_UNIQUE,
        .domain = CBM_RESOLVE_DOMAIN_PROJECT,
        .analysis = CBM_RESOLVE_ANALYSIS_UNSUPPORTED,
        .rule = CBM_RESOLVE_RULE_SCOPE_BINDING,
        .facts = CBM_RESOLVE_FACT_EXACT_TARGET | CBM_RESOLVE_FACT_SCOPE_BOUND,
        .target = cbm_arena_sprintf(arena, "%s.%s", result->module_qn, name),
        .candidate_count = 1};
}

static cbm_resolve_result_t js_import(const char *target) {
    return (cbm_resolve_result_t){.binding = CBM_RESOLVE_BINDING_UNIQUE,
                                  .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                  .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                  .rule = CBM_RESOLVE_RULE_IMPORT_BINDING,
                                  .facts = CBM_RESOLVE_FACT_EXACT_TARGET |
                                           CBM_RESOLVE_FACT_IMPORT_BOUND |
                                           CBM_RESOLVE_FACT_LOOKUP_COMPLETE,
                                  .target = target,
                                  .candidate_count = 1};
}

static cbm_resolve_result_t js_site(const cbm_resolve_binding_index_t *index, const char *source,
                                    const char *name, const char *spelling,
                                    cbm_resolve_result_t candidate) {
    const char *site = strstr(source, spelling);
    if (!site) {
        return (cbm_resolve_result_t){.reason = "fixture_site_missing"};
    }
    uint32_t start = (uint32_t)(site - source);
    return cbm_resolve_binding_validate(index, name, start, start + (uint32_t)strlen(spelling),
                                        CBM_SOURCE_ORIGIN_RAW, candidate);
}

TEST(resolver_binding_js_family_module_functions_and_named_exports) {
    const CBMLanguage languages[] = {CBM_LANG_JAVASCRIPT, CBM_LANG_TYPESCRIPT, CBM_LANG_TSX,
                                     CBM_LANG_ARKTS};
    const char *source = "export function target() { return 1; }\n"
                         "export function caller() { return target(); }\n";
    for (size_t l = 0; l < sizeof(languages) / sizeof(languages[0]); l++) {
        CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), languages[l], "test",
                                                 "binding.ts", 0, NULL, NULL);
        ASSERT_NOT_NULL(result);
        CBMArena arena;
        cbm_arena_init(&arena);
        cbm_resolve_binding_index_t *index =
            cbm_resolve_binding_build(&arena, result, languages[l], source, strlen(source));
        cbm_resolve_result_t candidate = js_local(&arena, result, "target");
        /* The full invocation is distinct from the declaration's identifier. */
        const char *call = strstr(source, "return target()") + strlen("return ");
        uint32_t start = (uint32_t)(call - source);
        cbm_resolve_result_t resolved = cbm_resolve_binding_validate(
            index, "target", start, start + 8, CBM_SOURCE_ORIGIN_RAW, candidate);
        ASSERT(cbm_resolve_admit(&resolved));
        ASSERT_EQ(cbm_resolve_binding_export_kind(index, candidate.target),
                  CBM_RESOLVE_EXPORT_VALUE);
        ASSERT(!cbm_resolve_binding_export(index, "test.decoy.target"));
        cbm_arena_destroy(&arena);
        cbm_free_result(result);
    }
    PASS();
}

/* These literals come from the source paths and declarations, independently
 * of the binding audit. Module QNs keep index; declaration QNs omit only an
 * actual index basename, never the index suffix of a dotted filename. */
TEST(resolver_binding_js_index_exports_use_exact_source_identity) {
    const CBMLanguage languages[] = {CBM_LANG_JAVASCRIPT, CBM_LANG_TYPESCRIPT, CBM_LANG_TSX,
                                     CBM_LANG_ARKTS};
    static const struct {
        const char *file, *module, *target, *wrong;
    } cases[] = {
        {"index.ts", "test.index", "test.target", "test.index.target"},
        {"lib/index.ts", "test.lib.index", "test.lib.target", "test.lib.index.target"},
        {"lib/entry.ts", "test.lib.entry", "test.lib.entry.target", "test.lib.target"},
        {"lib/public.index.ts", "test.lib.public.index", "test.lib.public.index.target",
         "test.lib.public.target"},
        {"index/index.ts", "test.index.index", "test.index.target", "test.target"},
    };
    const char *source = "export function target() { return 1; }\n"
                         "export function caller() { return target(); }\n";
    const char *call = strstr(source, "return target()") + strlen("return ");
    uint32_t start = (uint32_t)(call - source);
    for (size_t l = 0; l < sizeof(languages) / sizeof(languages[0]); l++) {
        for (size_t c = 0; c < sizeof(cases) / sizeof(cases[0]); c++) {
            CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), languages[l],
                                                     "test", cases[c].file, 0, NULL, NULL);
            ASSERT_NOT_NULL(result);
            ASSERT_STR_EQ(result->module_qn, cases[c].module);
            const CBMDefinition *declaration = NULL;
            for (int d = 0; d < result->defs.count; d++) {
                const CBMDefinition *definition = &result->defs.items[d];
                if (definition->name && !strcmp(definition->name, "target") && definition->label &&
                    !strcmp(definition->label, "Function")) {
                    ASSERT(declaration == NULL);
                    declaration = definition;
                }
            }
            ASSERT_NOT_NULL(declaration);
            ASSERT_STR_EQ(declaration->qualified_name, cases[c].target);
            CBMArena arena;
            cbm_arena_init(&arena);
            cbm_resolve_binding_index_t *index =
                cbm_resolve_binding_build(&arena, result, languages[l], source, strlen(source));
            ASSERT_EQ(cbm_resolve_binding_export_kind(index, cases[c].target),
                      CBM_RESOLVE_EXPORT_VALUE);
            ASSERT_EQ(cbm_resolve_binding_export_kind(index, cases[c].wrong), 0);
            cbm_resolve_result_t candidate = js_local(&arena, result, "target");
            candidate.target = cases[c].target;
            cbm_resolve_result_t resolved = cbm_resolve_binding_validate(
                index, "target", start, start + 8, CBM_SOURCE_ORIGIN_RAW, candidate);
            ASSERT(cbm_resolve_admit(&resolved));
            ASSERT_STR_EQ(resolved.target, cases[c].target);
            candidate.target = cases[c].wrong;
            resolved = cbm_resolve_binding_validate(index, "target", start, start + 8,
                                                    CBM_SOURCE_ORIGIN_RAW, candidate);
            ASSERT(!cbm_resolve_admit(&resolved));
            cbm_arena_destroy(&arena);
            cbm_free_result(result);
        }
    }
    PASS();
}

TEST(resolver_binding_js_index_scope_requires_captured_identity) {
    const char *source = "export function target() { return 1; }\n"
                         "export function caller() { return target(); }\n";
    const char *call = strstr(source, "return target()") + strlen("return ");
    uint32_t start = (uint32_t)(call - source);
    for (int mutation = 0; mutation < 5; mutation++) {
        CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_TYPESCRIPT,
                                                 "test", "lib/index.ts", 0, NULL, NULL);
        ASSERT_NOT_NULL(result);
        if (mutation == 0)
            result->source_project = NULL;
        else if (mutation == 1)
            result->source_file = NULL;
        else if (mutation == 2)
            result->source_content_id[CBM_SOURCE_ORIGIN_RAW][0] = '\0';
        else if (mutation == 3)
            result->source_configuration_id[0] = '\0';
        else
            result->source_file = "lib/public.index.ts";
        CBMArena arena;
        cbm_arena_init(&arena);
        cbm_resolve_binding_index_t *index =
            cbm_resolve_binding_build(&arena, result, CBM_LANG_TYPESCRIPT, source, strlen(source));
        ASSERT_EQ(cbm_resolve_binding_export_kind(index, "test.lib.target"), 0);
        ASSERT_EQ(cbm_resolve_binding_export_kind(index, "test.lib.index.target"), 0);
        cbm_resolve_result_t candidate = js_local(&arena, result, "target");
        candidate.target = "test.lib.target";
        cbm_resolve_result_t resolved = cbm_resolve_binding_validate(
            index, "target", start, start + 8, CBM_SOURCE_ORIGIN_RAW, candidate);
        ASSERT(!cbm_resolve_admit(&resolved));
        cbm_arena_destroy(&arena);
        cbm_free_result(result);
    }
    PASS();
}

TEST(resolver_binding_js_hoisted_function_and_private_export) {
    const char *source = "export function caller() { return helper(); }\n"
                         "function helper() { return 1; }\n";
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_TYPESCRIPT,
                                             "test", "binding.ts", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_binding_index_t *index =
        cbm_resolve_binding_build(&arena, result, CBM_LANG_TYPESCRIPT, source, strlen(source));
    cbm_resolve_result_t candidate = js_local(&arena, result, "helper");
    cbm_resolve_result_t resolved = js_site(index, source, "helper", "helper()", candidate);
    ASSERT(cbm_resolve_admit(&resolved));
    ASSERT(!cbm_resolve_binding_export(index, candidate.target));
    cbm_arena_destroy(&arena);
    cbm_free_result(result);
    PASS();
}

TEST(resolver_binding_js_import_alias_requires_target_export_proof) {
    const char *source = "import { target as execute } from './lib';\n"
                         "export function caller() { return execute(); }\n";
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_TYPESCRIPT,
                                             "test", "binding.ts", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_binding_index_t *index =
        cbm_resolve_binding_build(&arena, result, CBM_LANG_TYPESCRIPT, source, strlen(source));
    cbm_resolve_result_t candidate = js_import("test.lib.target");
    cbm_resolve_result_t resolved = js_site(index, source, "execute", "execute()", candidate);
    ASSERT(cbm_resolve_admit(&resolved));
    candidate.facts &= ~CBM_RESOLVE_FACT_LOOKUP_COMPLETE;
    resolved = js_site(index, source, "execute", "execute()", candidate);
    ASSERT(!cbm_resolve_admit(&resolved));
    ASSERT_STR_EQ(resolved.reason, "binding_target_or_export_unproven");
    cbm_arena_destroy(&arena);
    cbm_free_result(result);
    PASS();
}

TEST(resolver_binding_js_shadow_tdz_and_assignments_block_imports) {
    const char *sources[] = {
        "import { execute } from './lib'; export function caller(execute) { "
        "return execute(); }",
        "import { execute } from './lib'; export function caller() { execute(); "
        "const execute = other; }",
        "import { execute } from './lib'; export function caller() { execute(); "
        "var execute = other; }",
        "import { execute } from './lib'; export function caller() { execute = "
        "other; return execute(); }",
        "import { execute } from './lib'; export function caller() { const "
        "{execute} = value; return execute(); }",
        "import { execute } from './lib'; export const caller = (execute) => "
        "execute();",
        "import { execute } from './lib'; export function caller() { try {} "
        "catch (execute) { execute(); } }",
        "import type { execute } from './lib'; export function caller() { return "
        "execute(); }",
    };
    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++) {
        const char *source = sources[i];
        CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_TYPESCRIPT,
                                                 "test", "binding.ts", 0, NULL, NULL);
        ASSERT_NOT_NULL(result);
        CBMArena arena;
        cbm_arena_init(&arena);
        cbm_resolve_binding_index_t *index =
            cbm_resolve_binding_build(&arena, result, CBM_LANG_TYPESCRIPT, source, strlen(source));
        ASSERT_STR_EQ(cbm_resolve_binding_status(index), "binding_complete_supported_subset");
        cbm_resolve_result_t resolved =
            js_site(index, source, "execute", "execute()", js_import("test.lib.execute"));
        ASSERT(!cbm_resolve_admit(&resolved));
        cbm_arena_destroy(&arena);
        cbm_free_result(result);
    }
    PASS();
}

TEST(resolver_binding_js_exports_come_from_syntax) {
    const char *sources[] = {
        "function target() {} export {};",
        "export default function target() {}",
        "function target() {} export { target as alias };",
        "function target() {} export { target } from './other';",
        ("export function target() {} function other() {} export { other as "
         "target };"),
    };
    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++) {
        const char *source = sources[i];
        CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_TYPESCRIPT,
                                                 "test", "binding.ts", 0, NULL, NULL);
        ASSERT_NOT_NULL(result);
        CBMArena arena;
        cbm_arena_init(&arena);
        cbm_resolve_binding_index_t *index =
            cbm_resolve_binding_build(&arena, result, CBM_LANG_TYPESCRIPT, source, strlen(source));
        ASSERT(!cbm_resolve_binding_export(index, js_local(&arena, result, "target").target));
        cbm_arena_destroy(&arena);
        cbm_free_result(result);
    }
    PASS();
}

TEST(resolver_binding_js_local_export_clause_and_typed_declarations) {
    const char *source = "function target() {} export { target };\n"
                         "export class Service { run(): number { return 1; } }\n"
                         "export interface Contract { run(): number; }\n"
                         "interface Private { run(): number; }\n"
                         "class Hidden {}\n"
                         "class TypeOnly {} export type { TypeOnly };\n";
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_TYPESCRIPT,
                                             "test", "binding.ts", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_binding_index_t *index =
        cbm_resolve_binding_build(&arena, result, CBM_LANG_TYPESCRIPT, source, strlen(source));
    ASSERT_EQ(cbm_resolve_binding_export_kind(index, js_local(&arena, result, "target").target),
              CBM_RESOLVE_EXPORT_VALUE);
    ASSERT_EQ(cbm_resolve_binding_export_kind(index, js_local(&arena, result, "Service").target),
              CBM_RESOLVE_EXPORT_VALUE | CBM_RESOLVE_EXPORT_TYPE);
    ASSERT_EQ(cbm_resolve_binding_export_kind(index, js_local(&arena, result, "Contract").target),
              CBM_RESOLVE_EXPORT_TYPE);
    ASSERT(!cbm_resolve_binding_export(index, js_local(&arena, result, "Private").target));
    ASSERT(!cbm_resolve_binding_export(index, js_local(&arena, result, "Hidden").target));
    ASSERT_EQ(cbm_resolve_binding_export_kind(index, js_local(&arena, result, "TypeOnly").target),
              CBM_RESOLVE_EXPORT_TYPE);
    cbm_arena_destroy(&arena);
    cbm_free_result(result);
    PASS();
}

TEST(resolver_binding_js_script_globals_and_dynamic_effects_are_unproved) {
    const char *sources[] = {
        "function caller() { return helper(); } function helper() {}",
        "export function caller() { eval(code); return helper(); } function "
        "helper() {}",
        "export function caller() { const maker = Function(code); return "
        "helper(); } function helper() {}",
    };
    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++) {
        const char *source = sources[i];
        CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_JAVASCRIPT,
                                                 "test", "binding.js", 0, NULL, NULL);
        ASSERT_NOT_NULL(result);
        CBMArena arena;
        cbm_arena_init(&arena);
        cbm_resolve_binding_index_t *index =
            cbm_resolve_binding_build(&arena, result, CBM_LANG_JAVASCRIPT, source, strlen(source));
        cbm_resolve_result_t resolved =
            js_site(index, source, "helper", "helper()", js_local(&arena, result, "helper"));
        ASSERT(!cbm_resolve_admit(&resolved));
        ASSERT_EQ(resolved.analysis, CBM_RESOLVE_ANALYSIS_UNSUPPORTED);
        cbm_arena_destroy(&arena);
        cbm_free_result(result);
    }
    PASS();
}

TEST(resolver_binding_js_value_reference_initializer_preserves_import_binding) {
    const char *source = "import { target } from './lib'; export const callback = target;";
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_TYPESCRIPT,
                                             "test", "binding.ts", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_binding_index_t *index =
        cbm_resolve_binding_build(&arena, result, CBM_LANG_TYPESCRIPT, source, strlen(source));
    const char *site = strstr(source, "= target") + 2;
    uint32_t start = (uint32_t)(site - source);
    cbm_resolve_result_t resolved = cbm_resolve_binding_validate(
        index, "target", start, start + 6, CBM_SOURCE_ORIGIN_RAW, js_import("test.lib.target"));
    ASSERT(cbm_resolve_admit(&resolved));
    cbm_arena_destroy(&arena);
    cbm_free_result(result);
    PASS();
}

TEST(resolver_binding_tsx_jsx_keeps_exact_function_binding) {
    const char *source = "export function view() { return <div>{target()}</div>; } "
                         "function target() { return 1; }";
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_TSX, "test",
                                             "binding.tsx", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_binding_index_t *index =
        cbm_resolve_binding_build(&arena, result, CBM_LANG_TSX, source, strlen(source));
    cbm_resolve_result_t resolved =
        js_site(index, source, "target", "target()", js_local(&arena, result, "target"));
    ASSERT(cbm_resolve_admit(&resolved));
    cbm_arena_destroy(&arena);
    cbm_free_result(result);
    PASS();
}

static cbm_resolve_result_t js_value_reference(const cbm_resolve_binding_index_t *index,
                                               const char *source, const char *context,
                                               const char *name, cbm_resolve_result_t candidate) {
    const char *found = strstr(source, context);
    const char *site = found ? strstr(found, name) : NULL;
    if (!site)
        return (cbm_resolve_result_t){.reason = "fixture_site_missing"};
    uint32_t start = (uint32_t)(site - source);
    return cbm_resolve_binding_validate(index, name, start, start + (uint32_t)strlen(name),
                                        CBM_SOURCE_ORIGIN_RAW, candidate);
}

TEST(resolver_binding_js_family_const_values_keep_exact_declarations) {
    const CBMLanguage languages[] = {CBM_LANG_JAVASCRIPT, CBM_LANG_TYPESCRIPT, CBM_LANG_TSX,
                                     CBM_LANG_ARKTS};
    const char *source = "export const target = 42;\n"
                         "const privateValue = target, named = privateValue;\n"
                         "export { named };\n"
                         "export function caller() { return target; }\n";
    const char *names[] = {"target", "privateValue", "named"};
    for (size_t l = 0; l < sizeof(languages) / sizeof(languages[0]); l++) {
        CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), languages[l], "test",
                                                 "binding.ts", 0, NULL, NULL);
        ASSERT_NOT_NULL(result);
        CBMArena arena;
        cbm_arena_init(&arena);
        cbm_resolve_binding_index_t *index =
            cbm_resolve_binding_build(&arena, result, languages[l], source, strlen(source));
        ASSERT_STR_EQ(cbm_resolve_binding_status(index), "binding_complete_supported_subset");
        for (size_t n = 0; n < sizeof(names) / sizeof(names[0]); n++) {
            cbm_resolve_result_t candidate = js_local(&arena, result, names[n]);
            int definitions = 0;
            for (int d = 0; d < result->defs.count; d++) {
                const CBMDefinition *definition = &result->defs.items[d];
                if (strcmp(definition->qualified_name, candidate.target))
                    continue;
                ASSERT_STR_EQ(definition->label, "Variable");
                definitions++;
            }
            ASSERT_EQ(definitions, 1);
            ASSERT_EQ(cbm_resolve_binding_export_kind(index, candidate.target),
                      n == 1 ? 0 : CBM_RESOLVE_EXPORT_VALUE);
        }
        cbm_resolve_result_t candidate = js_local(&arena, result, "target");
        cbm_resolve_result_t resolved =
            js_value_reference(index, source, "= target", "target", candidate);
        ASSERT(cbm_resolve_admit(&resolved));
        ASSERT_STR_EQ(resolved.target, candidate.target);
        ASSERT(!(resolved.facts &
                 (CBM_RESOLVE_FACT_CLASS_CALLABLE | CBM_RESOLVE_FACT_TYPE_BOUND |
                  CBM_RESOLVE_FACT_RECEIVER_TYPED | CBM_RESOLVE_FACT_CALLABLE_TYPED)));
        resolved = js_value_reference(index, source, "return target", "target", candidate);
        ASSERT(cbm_resolve_admit(&resolved));
        const unsigned type_facts[] = {CBM_RESOLVE_FACT_CLASS_CALLABLE, CBM_RESOLVE_FACT_TYPE_BOUND,
                                       CBM_RESOLVE_FACT_RECEIVER_TYPED,
                                       CBM_RESOLVE_FACT_CALLABLE_TYPED};
        for (size_t f = 0; f < sizeof(type_facts) / sizeof(type_facts[0]); f++) {
            cbm_resolve_result_t typed = candidate;
            typed.facts |= type_facts[f];
            resolved = js_value_reference(index, source, "return target", "target", typed);
            ASSERT(!cbm_resolve_admit(&resolved));
        }
        candidate = js_local(&arena, result, "privateValue");
        resolved = js_value_reference(index, source, "= privateValue", "privateValue", candidate);
        ASSERT(cbm_resolve_admit(&resolved));
        candidate.target = "test.decoy.target";
        resolved = js_value_reference(index, source, "return target", "target", candidate);
        ASSERT(!cbm_resolve_admit(&resolved));
        cbm_arena_destroy(&arena);
        cbm_free_result(result);
    }
    PASS();
}

TEST(resolver_binding_js_const_values_do_not_prove_invocations_or_members) {
    const char *source = "export const target = 42;\n"
                         "target(); new target(); (target)();\n"
                         "export const object = { run: target }; object.run();\n";
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_TYPESCRIPT,
                                             "test", "binding.ts", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_binding_index_t *index =
        cbm_resolve_binding_build(&arena, result, CBM_LANG_TYPESCRIPT, source, strlen(source));
    ASSERT_STR_EQ(cbm_resolve_binding_status(index), "binding_complete_supported_subset");
    cbm_resolve_result_t candidate = js_local(&arena, result, "target");
    ASSERT_EQ(cbm_resolve_binding_export_kind(index, candidate.target), CBM_RESOLVE_EXPORT_VALUE);
    const char *calls[] = {"target()", "new target()"};
    for (size_t i = 0; i < sizeof(calls) / sizeof(calls[0]); i++) {
        cbm_resolve_result_t resolved = js_site(index, source, "target", calls[i], candidate);
        ASSERT(!cbm_resolve_admit(&resolved));
        resolved = js_value_reference(index, source, calls[i], "target", candidate);
        ASSERT(!cbm_resolve_admit(&resolved));
    }
    cbm_resolve_result_t resolved =
        js_value_reference(index, source, "(target)()", "target", candidate);
    ASSERT(!cbm_resolve_admit(&resolved));
    candidate = js_local(&arena, result, "object");
    ASSERT_EQ(cbm_resolve_binding_export_kind(index, candidate.target), CBM_RESOLVE_EXPORT_VALUE);
    resolved = js_site(index, source, "object.run", "object.run()", candidate);
    ASSERT(!cbm_resolve_admit(&resolved));
    candidate.rule = CBM_RESOLVE_RULE_TYPE_MEMBER;
    resolved = js_value_reference(index, source, "object.run()", "object", candidate);
    ASSERT(!cbm_resolve_admit(&resolved));
    cbm_arena_destroy(&arena);
    cbm_free_result(result);
    PASS();
}

TEST(resolver_binding_js_const_initialization_shadow_and_writes_abstain) {
    const struct {
        const char *source;
        const char *context;
    } fixtures[] = {
        {"export {}; const before = target; const target = 42;", "= target"},
        {"export const target = target;", "= target"},
        {"export const target = (() => target)();", "=> target"},
        {"export const target = 42; function caller(target) { return target; }", "return target"},
        {"export const target = 42; function caller() { return target; const "
         "target = 1; }",
         "return target"},
        {"export const target = 42; target = 1; function caller() { return "
         "target; }",
         "return target"},
        {"export const target = 42; target++; function caller() { return target; "
         "}",
         "return target"},
        {"export {}; function caller() { const target = 42; return target; }", "return target"},
        {"export const {target} = object; function caller() { return target; }", "return target"},
    };
    for (size_t i = 0; i < sizeof(fixtures) / sizeof(fixtures[0]); i++) {
        const char *source = fixtures[i].source;
        CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_TYPESCRIPT,
                                                 "test", "binding.ts", 0, NULL, NULL);
        ASSERT_NOT_NULL(result);
        CBMArena arena;
        cbm_arena_init(&arena);
        cbm_resolve_binding_index_t *index =
            cbm_resolve_binding_build(&arena, result, CBM_LANG_TYPESCRIPT, source, strlen(source));
        ASSERT_STR_EQ(cbm_resolve_binding_status(index), "binding_complete_supported_subset");
        cbm_resolve_result_t resolved = js_value_reference(
            index, source, fixtures[i].context, "target", js_local(&arena, result, "target"));
        ASSERT(!cbm_resolve_admit(&resolved));
        cbm_arena_destroy(&arena);
        cbm_free_result(result);
    }
    PASS();
}

/* These used to be pinned as abstentions. A `let` or `var` with a later write still
 * names its one declaration, and a USAGE edge is about which declaration a name
 * selects, not which value it holds. A function const is a declared function. */
TEST(resolver_binding_js_variable_and_function_const_references_bind) {
    static const char *const sources[] = {
        "export let target = 42; function caller() { return target; }",
        "export var target = 42; function caller() { return target; }",
        "export const target = () => 42; function caller() { return target; }",
    };
    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++) {
        CBMFileResult *result = cbm_extract_file(sources[i], (int)strlen(sources[i]),
                                                 CBM_LANG_TYPESCRIPT, "test", "binding.ts", 0, NULL,
                                                 NULL);
        ASSERT_NOT_NULL(result);
        CBMArena arena;
        cbm_arena_init(&arena);
        cbm_resolve_binding_index_t *index = cbm_resolve_binding_build(
            &arena, result, CBM_LANG_TYPESCRIPT, sources[i], strlen(sources[i]));
        cbm_resolve_result_t resolved = js_value_reference(index, sources[i], "return target",
                                                           "target",
                                                           js_local(&arena, result, "target"));
        ASSERT(cbm_resolve_admit(&resolved));
        cbm_arena_destroy(&arena);
        cbm_free_result(result);
    }
    PASS();
}

TEST(resolver_binding_js_const_export_requires_unique_materialized_value) {
    const char *source = "export const target = 42; export function caller() { return target; }";
    for (int mutation = 0; mutation < 4; mutation++) {
        CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_TYPESCRIPT,
                                                 "test", "binding.ts", 0, NULL, NULL);
        ASSERT_NOT_NULL(result);
        CBMDefinition *definition = NULL;
        for (int d = 0; d < result->defs.count; d++)
            if (!strcmp(result->defs.items[d].name, "target"))
                definition = &result->defs.items[d];
        ASSERT_NOT_NULL(definition);
        if (mutation == 0)
            definition->qualified_name = "test.decoy.target";
        else if (mutation == 1)
            definition->label = "Function";
        else if (mutation == 2)
            definition->start_line++;
        else
            cbm_defs_push(&result->defs, &result->arena, *definition);
        CBMArena arena;
        cbm_arena_init(&arena);
        cbm_resolve_binding_index_t *index =
            cbm_resolve_binding_build(&arena, result, CBM_LANG_TYPESCRIPT, source, strlen(source));
        cbm_resolve_result_t candidate = js_local(&arena, result, "target");
        cbm_resolve_result_t resolved =
            js_value_reference(index, source, "return target", "target", candidate);
        ASSERT(!cbm_resolve_admit(&resolved));
        ASSERT_EQ(cbm_resolve_binding_export_kind(index, candidate.target), 0);
        cbm_arena_destroy(&arena);
        cbm_free_result(result);
    }
    PASS();
}

/* ---------------------------------------------------------------------------
 * Real-code forms. Each fixture below is a shape read from expressjs/express or
 * Kilo-Org/kilocode, with a decoy beside it that must keep abstaining.
 * ------------------------------------------------------------------------- */

typedef struct {
    CBMFileResult *result;
    CBMArena arena;
    cbm_resolve_binding_index_t *index;
} js_fixture_t;

static js_fixture_t js_fixture(const char *source, CBMLanguage language, const char *file,
                               bool declared_module) {
    js_fixture_t fixture = {0};
    fixture.result =
        cbm_extract_file(source, (int)strlen(source), language, "test", file, 0, NULL, NULL);
    cbm_arena_init(&fixture.arena);
    if (fixture.result)
        fixture.index = cbm_resolve_binding_build_declared_module(
            &fixture.arena, fixture.result, language, source, strlen(source), declared_module);
    return fixture;
}

static void js_fixture_free(js_fixture_t *fixture) {
    cbm_arena_destroy(&fixture->arena);
    cbm_free_result(fixture->result);
}

/* A lexical binding at the first occurrence of `spelling` after `context`. */
static bool js_binds_at(js_fixture_t *fixture, const char *source, const char *context,
                        const char *name, const char *spelling) {
    const char *found = strstr(source, context);
    const char *site = found ? strstr(found, spelling) : NULL;
    if (!site)
        return false;
    uint32_t start = (uint32_t)(site - source);
    cbm_resolve_result_t resolved = cbm_resolve_binding_validate(
        fixture->index, name, start, start + (uint32_t)strlen(spelling), CBM_SOURCE_ORIGIN_RAW,
        js_local(&fixture->arena, fixture->result, name));
    return cbm_resolve_admit(&resolved);
}

/* Syntax that binds nothing must not void a whole file. Each of these used to
 * stop the walk: `?.` alone silenced every binding in a file. */
TEST(resolver_binding_js_ordinary_syntax_keeps_the_file_proved) {
    static const struct {
        const char *source;
        const char *file;
        CBMLanguage language;
    } cases[] = {
        {"export function caller(a) { return helper(a?.b); } function helper(x) {}\n", "a.ts",
         CBM_LANG_TYPESCRIPT},
        {"#!/usr/bin/env node\nexport function caller() { return helper(); } function helper() {}\n",
         "a.mjs", CBM_LANG_JAVASCRIPT},
        {"export function caller() { outer: for (;;) { helper(); break outer; } } "
         "function helper() {}\n",
         "a.ts", CBM_LANG_TYPESCRIPT},
        {"export function caller() { debugger; return import('./x').then(helper); } "
         "function helper() {}\n",
         "a.ts", CBM_LANG_TYPESCRIPT},
        {"@Component() export class View { @Input() a: string; helper() {} }\n"
         "export function caller() { return helper(); } function helper() {}\n",
         "a.ts", CBM_LANG_TYPESCRIPT},
        {"export abstract class Base { abstract run(a: string): this; static { init(); } }\n"
         "export function caller() { return helper(); } function helper() {} function init() {}\n",
         "a.ts", CBM_LANG_TYPESCRIPT},
        {"export function over(a: string): void;\nexport function over(a: number): void;\n"
         "export function over(a: any) {}\n"
         "export function caller() { return helper(); } function helper() {}\n",
         "a.ts", CBM_LANG_TYPESCRIPT},
        {"declare const VERSION: string; declare global { interface Window { x: number } }\n"
         "export function caller() { return helper(); } function helper() {}\n",
         "a.ts", CBM_LANG_TYPESCRIPT},
        {"export namespace NS { export function member() {} }\n"
         "export function caller() { return helper(); } function helper() {}\n",
         "a.ts", CBM_LANG_TYPESCRIPT},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        js_fixture_t fixture = js_fixture(cases[i].source, cases[i].language, cases[i].file, false);
        ASSERT_NOT_NULL(fixture.result);
        ASSERT_STR_EQ(cbm_resolve_binding_status(fixture.index),
                      "binding_complete_supported_subset");
        ASSERT(js_binds_at(&fixture, cases[i].source, "caller", "helper", "helper"));
        js_fixture_free(&fixture);
    }
    /* `with` really does change what a name means, so it stays unsupported. */
    const char *with = "exports.a = 1; function caller(o) { with (o) { helper(); } } "
                       "function helper() {}\n";
    js_fixture_t fixture = js_fixture(with, CBM_LANG_JAVASCRIPT, "a.js", false);
    ASSERT_STR_EQ(cbm_resolve_binding_status(fixture.index), "unsupported_binding_syntax");
    js_fixture_free(&fixture);
    PASS();
}

/* lib/utils.js of Express. A file using CommonJS has its own function scope, so
 * a same-file call cannot be replaced by another script. */
TEST(resolver_binding_js_commonjs_syntax_proves_a_module) {
    static const char *const modules[] = {
        "const fs = require('fs');\nfunction caller() { return helper(); } function helper() {}\n",
        "module.exports = caller;\nfunction caller() { return helper(); } function helper() {}\n",
        "exports.caller = caller;\nfunction caller() { return helper(); } function helper() {}\n",
        ("module.exports.caller = caller;\nfunction caller() { return helper(); } "
         "function helper() {}\n"),
    };
    for (size_t i = 0; i < sizeof(modules) / sizeof(modules[0]); i++) {
        js_fixture_t fixture = js_fixture(modules[i], CBM_LANG_JAVASCRIPT, "a.js", false);
        ASSERT_STR_EQ(cbm_resolve_binding_status(fixture.index),
                      "binding_complete_supported_subset");
        ASSERT(js_binds_at(&fixture, modules[i], "caller()", "helper", "helper()"));
        js_fixture_free(&fixture);
    }
    static const char *const scripts[] = {
        /* A classic script: another script can replace its global functions. */
        "function caller() { return helper(); } function helper() {}\n",
        /* Spellings that are not the CommonJS forms. */
        "function caller() { thing.require('x'); return helper(); } function helper() {}\n",
        "function caller() { thing.module.exports = 1; return helper(); } function helper() {}\n",
        "function caller() { return helper(exports); } function helper() {}\n",
    };
    for (size_t i = 0; i < sizeof(scripts) / sizeof(scripts[0]); i++) {
        js_fixture_t fixture = js_fixture(scripts[i], CBM_LANG_JAVASCRIPT, "a.js", false);
        ASSERT_STR_EQ(cbm_resolve_binding_status(fixture.index),
                      "javascript_script_scope_unproven");
        js_fixture_free(&fixture);
    }
    PASS();
}

TEST(resolver_binding_js_module_extensions_and_manifest_prove_a_module) {
    const char *source = "function caller() { return helper(); } function helper() {}\n";
    static const char *const files[] = {"a.mjs", "a.cjs", "a.jsx"};
    for (size_t i = 0; i < sizeof(files) / sizeof(files[0]); i++) {
        js_fixture_t fixture = js_fixture(source, CBM_LANG_JAVASCRIPT, files[i], false);
        ASSERT_STR_EQ(cbm_resolve_binding_status(fixture.index),
                      "binding_complete_supported_subset");
        js_fixture_free(&fixture);
    }
    js_fixture_t plain = js_fixture(source, CBM_LANG_JAVASCRIPT, "a.js", false);
    ASSERT_STR_EQ(cbm_resolve_binding_status(plain.index), "javascript_script_scope_unproven");
    js_fixture_free(&plain);
    js_fixture_t declared = js_fixture(source, CBM_LANG_JAVASCRIPT, "a.js", true);
    ASSERT_STR_EQ(cbm_resolve_binding_status(declared.index), "binding_complete_supported_subset");
    js_fixture_free(&declared);
    /* A TypeScript script cannot redeclare a global implementation: compile error. */
    js_fixture_t typescript = js_fixture(source, CBM_LANG_TYPESCRIPT, "a.ts", false);
    ASSERT_STR_EQ(cbm_resolve_binding_status(typescript.index),
                  "binding_complete_supported_subset");
    js_fixture_free(&typescript);
    PASS();
}

/* A local name blocks only where it is visible. `run` in one callback used to
 * veto every call to a module-level `run` in the file. */
TEST(resolver_binding_js_local_names_block_only_their_own_scope) {
    static const struct {
        const char *source;
        const char *context;
        bool binds;
    } cases[] = {
        {"export function helper() {} function a() { const helper = 1; return helper(); }\n"
         "function b() { return helper(); }",
         "function b", true},
        {"export function helper() {} function a() { const helper = 1; return helper(); }\n",
         "= 1; return", false},
        {"export function helper() {} function a() { { let helper = 1; } return helper(); }",
         "} return", true},
        {"export function helper() {} function a() { { let helper = 1; helper(); } }", "= 1;",
         false},
        {"export function helper() {} function a(x) { x.forEach((helper) => helper()); return "
         "helper(); }",
         "); return", true},
        {"export function helper() {} function a(x) { x.forEach((helper) => helper()); }", "=>",
         false},
        {"export function helper() {} function a({ helper }) { return helper(); }", "return",
         false},
        {"export function helper() {} function a([helper]) { return helper(); }", "return", false},
        {"export function helper() {} function a({ b: helper }) { return helper(); }", "return",
         false},
        {"export function helper() {} function a(...helper) { return helper(); }", "return", false},
        {"export function helper() {} function a(b = helper()) { return b; }", "b =", true},
        {"export function helper() {} function a() { const { b = helper() } = {}; return b; }",
         "b =", true},
        {"export function helper() {} function a() { helper(); var helper = 1; }", "a()", false},
        {"export function helper() {} function a() { if (b) { var helper = 1; } return helper(); }",
         "} return", false},
        {"export function helper() {} function a() { for (const helper of b) { helper(); } }",
         "of b", false},
        {"export function helper() {} function a() { for (const x of helper()) { x(); } }", "of",
         true},
        {"export function helper() {} function a() { try {} catch (helper) { helper(); } }",
         "catch", false},
        {"export function helper() {} function a() { try {} catch (e) { helper(); } }", "catch",
         true},
        /* Writing a member rebinds nothing; writing the name does. */
        {"export function helper() {} helper.cache = 1; function a() { return helper(); }",
         "function a", true},
        {"export function helper() {} let x = {}; x[helper] = 1; function a() { return helper(); }",
         "function a", true},
        {"export function helper() {} function b() { helper = other; } function a() { return "
         "helper(); }",
         "function a", false},
        {"export function helper() {} function a() { [helper] = other; return helper(); }",
         "]", false},
        /* A write to a local says nothing about the module's name. */
        {"export function helper() {} function b() { let helper = 1; helper = 2; } "
         "function a() { return helper(); }",
         "function a", true},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        js_fixture_t fixture = js_fixture(cases[i].source, CBM_LANG_TYPESCRIPT, "a.ts", false);
        ASSERT_STR_EQ(cbm_resolve_binding_status(fixture.index),
                      "binding_complete_supported_subset");
        ASSERT_EQ(js_binds_at(&fixture, cases[i].source, cases[i].context, "helper", "helper"),
                  cases[i].binds);
        js_fixture_free(&fixture);
    }
    PASS();
}

/* `const f = () => {}` is the common way to declare a function in TypeScript. */
TEST(resolver_binding_js_function_consts_and_nested_functions_bind_by_definition) {
    const char *source = "export const arrow = (x: number) => {\n  return x;\n};\n"
                         "const plain = function () { return 1; };\n"
                         "function outer() {\n"
                         "  function nestedDecl() { return 1; }\n"
                         "  const nestedArrow = () => 2;\n"
                         "  return nestedDecl() + nestedArrow();\n"
                         "}\n"
                         "export function caller() { return arrow(1) + plain(); }\n"
                         "export function late() { return later(); }\n"
                         "const later = () => 1;\n";
    js_fixture_t fixture = js_fixture(source, CBM_LANG_TYPESCRIPT, "a.ts", false);
    ASSERT_STR_EQ(cbm_resolve_binding_status(fixture.index), "binding_complete_supported_subset");
    ASSERT(js_binds_at(&fixture, source, "caller()", "arrow", "arrow(1)"));
    ASSERT(js_binds_at(&fixture, source, "caller()", "plain", "plain()"));
    ASSERT(js_binds_at(&fixture, source, "return nestedDecl", "nestedDecl", "nestedDecl()"));
    ASSERT(js_binds_at(&fixture, source, "return nestedDecl", "nestedArrow", "nestedArrow()"));
    /* A closure runs after the module body, so a later const is what it names. */
    ASSERT(js_binds_at(&fixture, source, "late()", "later", "later()"));
    ASSERT_EQ(cbm_resolve_binding_export_kind(fixture.index, "test.a.arrow"),
              CBM_RESOLVE_EXPORT_VALUE);
    js_fixture_free(&fixture);
    /* Decoys: a non-function value and a declaration the extractor never defined. */
    const char *values = "const data = make();\nexport function caller() { return data(); }\n"
                         "function outer() { const local = 1; return local; }\n";
    js_fixture_t decoy = js_fixture(values, CBM_LANG_TYPESCRIPT, "a.ts", false);
    ASSERT(!js_binds_at(&decoy, values, "caller()", "data", "data()"));
    ASSERT(!js_binds_at(&decoy, values, "return local", "local", "local"));
    js_fixture_free(&decoy);
    PASS();
}

/* Kilo's `export const Info = Schema...; export type Info = ...`. TypeScript keeps
 * the two spaces apart; `type Error = string` does not shadow the value Error. */
TEST(resolver_binding_js_value_and_type_names_do_not_collide) {
    const char *source = "export const Info = make();\n"
                         "export type Info = { a: string };\n"
                         "type Error = string;\n"
                         "export function caller(a: Info) { return Info; }\n"
                         "export function thrower() { throw new Error('x'); }\n";
    js_fixture_t fixture = js_fixture(source, CBM_LANG_TYPESCRIPT, "a.ts", false);
    ASSERT_STR_EQ(cbm_resolve_binding_status(fixture.index), "binding_complete_supported_subset");
    ASSERT(js_binds_at(&fixture, source, "return Info", "Info", "Info"));
    ASSERT(js_binds_at(&fixture, source, "a: Info", "Info", "Info"));
    ASSERT(!js_binds_at(&fixture, source, "new Error", "Error", "Error"));
    ASSERT_EQ(cbm_resolve_binding_export_kind(fixture.index, "test.a.Info"),
              CBM_RESOLVE_EXPORT_VALUE | CBM_RESOLVE_EXPORT_TYPE);
    js_fixture_free(&fixture);
    PASS();
}

static const cbm_resolve_js_export_t *js_export_named(const cbm_resolve_binding_index_t *index,
                                                      const char *name) {
    size_t count = cbm_resolve_binding_js_export_count(index);
    for (size_t i = 0; i < count; i++) {
        const cbm_resolve_js_export_t *item = cbm_resolve_binding_js_export_at(index, i);
        if (item->name && !strcmp(item->name, name))
            return item;
    }
    return NULL;
}

TEST(resolver_binding_js_export_surface_of_an_es_module) {
    const char *source = "function a() {} function b() {}\n"
                         "export { a as alias };\n"
                         "export default b;\n"
                         "export function own() {}\n"
                         "export { re as reAlias, other } from './other';\n"
                         "export * from './star';\n"
                         "export * as Self from './self';\n"
                         "export type { T } from './types';\n"
                         "import { inner } from './inner';\n"
                         "export { inner };\n"
                         "function hidden() {}\n";
    js_fixture_t fixture = js_fixture(source, CBM_LANG_TYPESCRIPT, "a.ts", false);
    ASSERT_STR_EQ(cbm_resolve_binding_status(fixture.index), "binding_complete_supported_subset");
    const cbm_resolve_js_export_t *item = js_export_named(fixture.index, "alias");
    ASSERT_NOT_NULL(item);
    ASSERT_STR_EQ(item->target, "test.a.a");
    item = js_export_named(fixture.index, "default");
    ASSERT_NOT_NULL(item);
    ASSERT_STR_EQ(item->target, "test.a.b");
    item = js_export_named(fixture.index, "own");
    ASSERT_NOT_NULL(item);
    ASSERT_STR_EQ(item->target, "test.a.own");
    item = js_export_named(fixture.index, "reAlias");
    ASSERT_NOT_NULL(item);
    ASSERT_STR_EQ(item->source, "./other");
    ASSERT_STR_EQ(item->member, "re");
    item = js_export_named(fixture.index, "Self");
    ASSERT_NOT_NULL(item);
    ASSERT_STR_EQ(item->source, "./self");
    ASSERT_NULL(item->member);
    item = js_export_named(fixture.index, "inner");
    ASSERT_NOT_NULL(item);
    ASSERT_STR_EQ(item->source, "./inner");
    ASSERT_STR_EQ(item->member, "inner");
    ASSERT_NULL(js_export_named(fixture.index, "a"));
    ASSERT_NULL(js_export_named(fixture.index, "b"));
    ASSERT_NULL(js_export_named(fixture.index, "hidden"));
    bool star = false;
    size_t count = cbm_resolve_binding_js_export_count(fixture.index);
    for (size_t i = 0; i < count; i++) {
        const cbm_resolve_js_export_t *entry = cbm_resolve_binding_js_export_at(fixture.index, i);
        star |= !entry->name && entry->source && !strcmp(entry->source, "./star");
    }
    ASSERT(star);
    js_fixture_free(&fixture);
    PASS();
}

/* lib/express.js, lib/application.js and lib/utils.js of Express. */
TEST(resolver_binding_js_export_surface_of_commonjs) {
    const char *whole = "exports = module.exports = createApplication;\n"
                        "function createApplication() {}\n"
                        "function etag() {}\n"
                        "exports.etag = etag;\n"
                        "createApplication.extra = function extra() {};\n"
                        "exports.lazy = require('./lazy');\n"
                        "exports.made = make();\n";
    js_fixture_t fixture = js_fixture(whole, CBM_LANG_JAVASCRIPT, "a.js", false);
    ASSERT_STR_EQ(cbm_resolve_binding_status(fixture.index), "binding_complete_supported_subset");
    ASSERT(cbm_resolve_binding_js_commonjs(fixture.index));
    const cbm_resolve_js_export_t *item = js_export_named(fixture.index, "module.exports");
    ASSERT_NOT_NULL(item);
    ASSERT(item->whole);
    ASSERT_STR_EQ(item->target, "test.a.createApplication");
    item = js_export_named(fixture.index, "etag");
    ASSERT_NOT_NULL(item);
    ASSERT_STR_EQ(item->target, "test.a.etag");
    item = js_export_named(fixture.index, "extra");
    ASSERT_NOT_NULL(item);
    ASSERT_STR_EQ(item->target, "test.a.extra");
    item = js_export_named(fixture.index, "lazy");
    ASSERT_NOT_NULL(item);
    ASSERT_STR_EQ(item->source, "./lazy");
    item = js_export_named(fixture.index, "made");
    ASSERT_NOT_NULL(item);
    ASSERT(!item->known);
    js_fixture_free(&fixture);

    const char *object = "function helper() {} function other() {}\n"
                         "module.exports = { helper, renamed: other, inline() {},\n"
                         "  arrow: () => 1, fn: function () {}, nested: {}, ...more };\n";
    fixture = js_fixture(object, CBM_LANG_JAVASCRIPT, "a.js", false);
    ASSERT_STR_EQ(cbm_resolve_binding_status(fixture.index), "binding_complete_supported_subset");
    item = js_export_named(fixture.index, "module.exports");
    ASSERT_NOT_NULL(item);
    ASSERT(item->is_object);
    item = js_export_named(fixture.index, "helper");
    ASSERT_NOT_NULL(item);
    ASSERT_STR_EQ(item->target, "test.a.helper");
    item = js_export_named(fixture.index, "renamed");
    ASSERT_NOT_NULL(item);
    ASSERT_STR_EQ(item->target, "test.a.other");
    item = js_export_named(fixture.index, "inline");
    ASSERT_NOT_NULL(item);
    ASSERT_STR_EQ(item->target, "test.a.inline");
    item = js_export_named(fixture.index, "arrow");
    ASSERT_NOT_NULL(item);
    ASSERT_STR_EQ(item->target, "test.a.arrow");
    item = js_export_named(fixture.index, "fn");
    ASSERT_NOT_NULL(item);
    ASSERT_STR_EQ(item->target, "test.a.fn");
    item = js_export_named(fixture.index, "nested");
    ASSERT_NOT_NULL(item);
    ASSERT(!item->known);
    /* A spread can overwrite or add names the file does not spell. */
    ASSERT(!cbm_resolve_binding_js_exports_complete(fixture.index));
    js_fixture_free(&fixture);

    /* `module.exports =` replaces the object a plain `exports.x` wrote to. */
    const char *replaced = "function a() {} function b() {}\n"
                           "exports.gone = a;\n"
                           "module.exports = b;\n";
    fixture = js_fixture(replaced, CBM_LANG_JAVASCRIPT, "a.js", false);
    item = js_export_named(fixture.index, "gone");
    ASSERT_NOT_NULL(item);
    ASSERT(!item->known);
    js_fixture_free(&fixture);
    PASS();
}

TEST(resolver_binding_js_require_bindings_are_scoped_imports) {
    const char *source = "const whole = require('./whole');\n"
                         "const { helper, b: renamed } = require('./pair');\n"
                         "var viaProp = require('./prop').member;\n"
                         "function caller() { return helper() + renamed() + viaProp() + "
                         "whole.run(); }\n"
                         "function lazy() { const late = require('./late'); return late(); }\n"
                         "function shadow(helper) { return helper(); }\n"
                         "function other() { return late(); }\n"
                         "function written() { return reassigned(); }\n"
                         "let reassigned = require('./gone'); reassigned = 1;\n"
                         "const computed = require(name);\n";
    js_fixture_t fixture = js_fixture(source, CBM_LANG_JAVASCRIPT, "a.js", false);
    ASSERT_STR_EQ(cbm_resolve_binding_status(fixture.index), "binding_complete_supported_subset");
    static const struct {
        const char *context, *name, *spelling, *import_source, *member;
    } bound[] = {
        {"caller()", "helper", "helper()", "./pair", "helper"},
        {"caller()", "renamed", "renamed()", "./pair", "b"},
        {"caller()", "viaProp", "viaProp()", "./prop", "member"},
        {"caller()", "whole.run", "whole.run()", "./whole", NULL},
        {"lazy()", "late", "late()", "./late", NULL},
    };
    for (size_t i = 0; i < sizeof(bound) / sizeof(bound[0]); i++) {
        const char *site = strstr(strstr(source, bound[i].context), bound[i].spelling);
        ASSERT_NOT_NULL(site);
        uint32_t start = (uint32_t)(site - source);
        const cbm_resolve_js_import_t *import = cbm_resolve_binding_js_import(
            fixture.index, bound[i].name, start, start + (uint32_t)strlen(bound[i].spelling),
            CBM_SOURCE_ORIGIN_RAW, NULL, NULL);
        ASSERT_NOT_NULL(import);
        ASSERT_STR_EQ(import->source, bound[i].import_source);
        if (bound[i].member)
            ASSERT_STR_EQ(import->member, bound[i].member);
        else
            ASSERT_NULL(import->member);
    }
    static const struct {
        const char *context, *name, *spelling;
    } unbound[] = {
        {"shadow(helper) {", "helper", "helper()"},
        {"other()", "late", "late()"},
        {"written()", "reassigned", "reassigned()"},
    };
    for (size_t i = 0; i < sizeof(unbound) / sizeof(unbound[0]); i++) {
        const char *site = strstr(strstr(source, unbound[i].context), unbound[i].spelling);
        ASSERT_NOT_NULL(site);
        uint32_t start = (uint32_t)(site - source);
        ASSERT_NULL(cbm_resolve_binding_js_import(fixture.index, unbound[i].name, start,
                                                  start + (uint32_t)strlen(unbound[i].spelling),
                                                  CBM_SOURCE_ORIGIN_RAW, NULL, NULL));
    }
    js_fixture_free(&fixture);
    PASS();
}

/* ---- Whole-project forms, through both resolvers. ---- */

typedef struct {
    const char *path;
    const char *text;
} js_project_file_t;

typedef struct {
    char root[256];
    char project[256];
    cbm_store_t *store;
} js_project_t;

static void js_project_write(const char *root, const char *relative, const char *text) {
    char path[768];
    snprintf(path, sizeof(path), "%s/%s", root, relative);
    for (char *slash = strchr(path + strlen(root) + 1, '/'); slash; slash = strchr(slash + 1, '/')) {
        *slash = '\0';
        (void)cbm_mkdir(path);
        *slash = '/';
    }
    FILE *file = fopen(path, "w");
    if (file) {
        fputs(text, file);
        fclose(file);
    }
}

/* padding > 50 files selects the parallel resolver; 0 keeps the sequential one. */
static bool js_project_index(js_project_t *project, const js_project_file_t *files, size_t count,
                             int padding) {
    snprintf(project->root, sizeof(project->root), "/tmp/cbm_js_project_XXXXXX");
    if (!cbm_mkdtemp(project->root))
        return false;
    for (size_t i = 0; i < count; i++)
        js_project_write(project->root, files[i].path, files[i].text);
    for (int i = 0; i < padding; i++) {
        char name[64], text[64];
        snprintf(name, sizeof(name), "padding/pad%d.js", i);
        snprintf(text, sizeof(text), "export function pad%d() { return %d; }\n", i, i);
        js_project_write(project->root, name, text);
    }
    char db[512];
    snprintf(db, sizeof(db), "%s/index.db", project->root);
    cbm_pipeline_t *pipeline = cbm_pipeline_new(project->root, db, CBM_MODE_FULL);
    if (!pipeline || cbm_pipeline_run(pipeline) != 0) {
        cbm_pipeline_free(pipeline);
        return false;
    }
    snprintf(project->project, sizeof(project->project), "%s", cbm_pipeline_project_name(pipeline));
    cbm_pipeline_free(pipeline);
    project->store = cbm_store_open_path(db);
    return project->store != NULL;
}

static void js_project_free(js_project_t *project) {
    if (project->store)
        cbm_store_close(project->store);
    th_rmtree(project->root);
}

/* Edges of this type from the node named `source` to the exact target QN suffix. */
static int js_project_edges(const js_project_t *project, const char *type, const char *source,
                            const char *target_suffix) {
    cbm_edge_t *edges = NULL;
    int count = 0;
    if (cbm_store_find_edges_by_type(project->store, project->project, type, &edges, &count) !=
        CBM_STORE_OK)
        return -1;
    int matches = 0;
    size_t suffix = strlen(target_suffix);
    for (int i = 0; i < count; i++) {
        cbm_node_t from = {0}, to = {0};
        if (cbm_store_find_node_by_id(project->store, edges[i].source_id, &from) == CBM_STORE_OK &&
            cbm_store_find_node_by_id(project->store, edges[i].target_id, &to) == CBM_STORE_OK &&
            from.name && !strcmp(from.name, source) && to.qualified_name) {
            size_t length = strlen(to.qualified_name);
            if (length >= suffix && !strcmp(to.qualified_name + length - suffix, target_suffix))
                matches++;
        }
        cbm_node_free_fields(&from);
        cbm_node_free_fields(&to);
    }
    cbm_store_free_edges(edges, count);
    return matches;
}

/* Express: `const express = require('../')`, `module.exports = require('./lib/express')`,
 * `exports = module.exports = createApplication`, `const { helper } = require(...)`. */
static int js_project_commonjs_case(int padding) {
    static const js_project_file_t files[] = {
        {"package.json", "{\"name\":\"demo\"}\n"},
        {"index.js", "module.exports = require('./lib/app');\n"},
        {"lib/app.js", "exports = module.exports = createApplication;\n"
                       "function createApplication() { return helper(); }\n"
                       "function helper() { return 1; }\n"
                       "exports.named = named;\n"
                       "function named() { return 1; }\n"
                       "createApplication.extra = function extra() { return 1; };\n"
                       "function unexported() { return 1; }\n"},
        {"lib/util.js", "function helper() { return 1; }\n"
                        "function other() { return 1; }\n"
                        "function secret() { return 1; }\n"
                        "module.exports = { helper, renamed: other,\n"
                        "  inline() { return 1; }, anon: function () { return 1; } };\n"},
        {"lib/each.js", "exports.first = function () { return 1; };\n"
                        "exports.second = () => 2;\n"},
        {"test/use.js", "const express = require('../');\n"
                        "const { helper, renamed: other } = require('../lib/util');\n"
                        "const util = require('../lib/util');\n"
                        "const each = require('../lib/each');\n"
                        "const { secret, missing } = require('../lib/util');\n"
                        "function run() {\n"
                        "  express();\n"
                        "  express.named();\n"
                        "  express.extra();\n"
                        "  helper();\n"
                        "  other();\n"
                        "  util.inline();\n"
                        "  util.anon();\n"
                        "  each.first();\n"
                        "  each.second();\n"
                        "}\n"
                        "function decoys() {\n"
                        "  secret();\n"
                        "  missing();\n"
                        "  express.unexported();\n"
                        "  util.secret();\n"
                        "  each.other();\n"
                        "}\n"
                        "function shadowed(helper) { return helper(); }\n"},
        /* Writes before the last `module.exports =` went to the replaced object, and a
         * plain `exports = x` detaches the name from module.exports altogether. */
        {"lib/order.js", "exports.early = function early() { return 1; };\n"
                         "module.exports.early2 = function early2() { return 1; };\n"
                         "exports = module.exports = whole;\n"
                         "function whole() { return 1; }\n"
                         "exports.late = function late() { return 1; };\n"
                         "module.exports.late2 = function late2() { return 1; };\n"},
        {"lib/rebind.js", "exports = { lost };\n"
                          "exports.lost2 = lost2;\n"
                          "function lost() { return 1; }\n"
                          "function lost2() { return 1; }\n"},
        {"test/order_use.js", "const order = require('../lib/order');\n"
                              "const { lost, lost2 } = require('../lib/rebind');\n"
                              "function orderRun() { order(); order.late(); order.late2(); }\n"
                              "function orderDecoys() {\n"
                              "  order.early();\n"
                              "  order.early2();\n"
                              "  lost();\n"
                              "  lost2();\n"
                              "}\n"},
    };
    js_project_t project = {0};
    ASSERT(js_project_index(&project, files, sizeof(files) / sizeof(files[0]), padding));
    ASSERT_EQ(js_project_edges(&project, "CALLS", "orderRun", "lib.order.whole"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "orderRun", "lib.order.late"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "orderRun", "lib.order.late2"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "orderDecoys", "lib.order.early"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "orderDecoys", "lib.order.early2"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "orderDecoys", "lib.rebind.lost"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "orderDecoys", "lib.rebind.lost2"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "run", "lib.app.createApplication"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "run", "lib.app.named"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "run", "lib.app.extra"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "run", "lib.util.helper"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "run", "lib.util.other"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "run", "lib.util.inline"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "run", "lib.util.anon"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "run", "lib.each.first"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "run", "lib.each.second"), 1);
    /* Same-file call in a CommonJS module. */
    ASSERT_EQ(js_project_edges(&project, "CALLS", "createApplication", "lib.app.helper"), 1);
    /* `secret` exists but is not exported; nothing names `missing`. */
    ASSERT_EQ(js_project_edges(&project, "CALLS", "decoys", "lib.util.secret"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "decoys", "lib.app.unexported"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "shadowed", "lib.util.helper"), 0);
    /* A directory import names the package's index file, not its Folder. */
    ASSERT_EQ(js_project_edges(&project, "IMPORTS", "use.js", "index.js.__file__"), 1);
    js_project_free(&project);
    PASS();
}

TEST(resolver_js_project_commonjs_sequential) {
    return js_project_commonjs_case(0);
}
TEST(resolver_js_project_commonjs_parallel) {
    return js_project_commonjs_case(55);
}

/* Kilo: `export * as Git from "./git"`, barrels, defaults, aliases and the
 * `export namespace` and `export const Info` + `export type Info` pairs. */
static int js_project_typescript_case(int padding) {
    static const js_project_file_t files[] = {
        {"src/thing.ts", "export class Thing { go() { return 1; } }\n"
                         "export function thingFn() { return 1; }\n"
                         "function hidden() { return 1; }\n"},
        {"src/util.ts", "export function helper() { return 1; }\n"
                        "export default function dflt() { return 1; }\n"
                        "export const arrow = () => 1;\n"
                        "function aliased() { return 1; }\n"
                        "export { aliased as publicName };\n"
                        "export * as Self from \"./util\";\n"
                        "export interface Self { a: string }\n"
                        "export function over(a: string): void;\n"
                        "export function over(a: number): void;\n"
                        "export function over(a: any) { return a; }\n"},
        {"src/barrel.ts", "export * from \"./thing\";\n"
                          "export { helper as barrelHelper } from \"./util\";\n"
                          "export { default as barrelDefault } from \"./util\";\n"},
        {"src/one.ts", "export function clash() { return 1; }\n"},
        {"src/two.ts", "export function clash() { return 2; }\n"},
        {"src/clash.ts", "export * from \"./one\";\nexport * from \"./two\";\n"},
        {"src/ns.ts", "export namespace Space { export function member() { return 1; } }\n"},
        {"src/dir/index.ts", "export function inDir() { return 1; }\n"},
        {"src/info.ts", "export const Info = { make: () => 1 };\n"
                        "export type Info = { a: string };\n"},
        {"tsconfig.json", "{\"compilerOptions\":{\"paths\":{\"@/*\":[\"./src/*\"]}}}\n"},
        {"src/aliased/index.ts", "export function viaAlias() { return 1; }\n"},
        {"src/base.ts", "export class Base { inherited() { return 1; } }\n"},
        {"src/child.ts", "import { Base } from \"./base\";\n"
                         "export class Child extends Base {\n"
                         "  run() { return this.inherited(); }\n"
                         "}\n"},
        {"src/main.ts", "import { barrelHelper, barrelDefault, thingFn, Thing } from \"./barrel\";\n"
                        "import dflt, { publicName, Self, arrow, over } from \"./util\";\n"
                        "import * as Util from \"./util\";\n"
                        "import { Space } from \"./ns\";\n"
                        "import { inDir } from \"./dir\";\n"
                        "import { clash } from \"./clash\";\n"
                        "import { hidden, aliased } from \"./barrel\";\n"
                        "import { Info } from \"./info\";\n"
                        "import { viaAlias } from \"@/aliased\";\n"
                        "export function run(t: Thing, i: Info, s: Self) {\n"
                        "  barrelHelper();\n"
                        "  barrelDefault();\n"
                        "  thingFn();\n"
                        "  dflt();\n"
                        "  publicName();\n"
                        "  Self.helper();\n"
                        "  arrow();\n"
                        "  over(1);\n"
                        "  Util.helper();\n"
                        "  Space.member();\n"
                        "  inDir();\n"
                        "  new Thing();\n"
                        "  t.go();\n"
                        "  viaAlias();\n"
                        "  return Info;\n"
                        "}\n"
                        "export function decoys() {\n"
                        "  clash();\n"
                        "  hidden();\n"
                        "  aliased();\n"
                        "}\n"},
    };
    js_project_t project = {0};
    ASSERT(js_project_index(&project, files, sizeof(files) / sizeof(files[0]), padding));
    static const char *const called[] = {
        "src.util.helper", "src.util.dflt", "src.thing.thingFn", "src.util.aliased",
        "src.util.arrow",  "src.util.over", "src.ns.Space.member", "src.dir.inDir",
        "src.thing.Thing", "src.thing.Thing.go", "src.aliased.viaAlias"};
    for (size_t i = 0; i < sizeof(called) / sizeof(called[0]); i++)
        ASSERT_EQ(js_project_edges(&project, "CALLS", "run", called[i]), 1);
    ASSERT_EQ(js_project_edges(&project, "USAGE", "run", "src.info.Info"), 1);
    /* Across files, through the import of the base class. */
    ASSERT_EQ(js_project_edges(&project, "CALLS", "run", "src.base.Base.inherited"), 1);
    ASSERT_EQ(js_project_edges(&project, "USES_TYPE", "run", "src.thing.Thing"), 1);
    ASSERT_EQ(js_project_edges(&project, "USES_TYPE", "run", "src.util.Self"), 1);
    ASSERT_EQ(js_project_edges(&project, "USES_TYPE", "run", "src.info.Info"), 1);
    /* Two stars answering differently exclude the name; not-exported stays out. */
    ASSERT_EQ(js_project_edges(&project, "CALLS", "decoys", "src.one.clash"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "decoys", "src.two.clash"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "decoys", "src.thing.hidden"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "decoys", "src.util.aliased"), 0);
    js_project_free(&project);
    PASS();
}

TEST(resolver_js_project_typescript_sequential) {
    return js_project_typescript_case(0);
}
TEST(resolver_js_project_typescript_parallel) {
    return js_project_typescript_case(55);
}

/* Known gaps, each pinned at the case with why it abstains and what was tried. A
 * pass here is honest abstention; turning one into an edge needs a real rule. */
TEST(resolver_js_project_known_gaps_abstain_with_a_reason) {
    static const js_project_file_t files[] = {
        /* Classic browser script: another <script> can replace these globals, and
         * nothing in the file or its manifest says otherwise. Tried: package.json
         * `type` and module extensions are honored; a plain .js has neither. */
        {"web/page.js", "function pageCaller() { return pageHelper(); }\n"
                        "function pageHelper() { return 1; }\n"},
        /* A prototype method through an instance from elsewhere: `new View().render()`
         * in another file. `this.render()` inside View's own members binds (see the
         * member test); this needs value flow from the constructor across files. */
        {"lib/view.js", "function View() {}\n"
                        "View.prototype.render = function render() { return 1; };\n"
                        "module.exports = View;\n"},
        {"lib/use.js", "const View = require('./view');\n"
                       "function protoCaller() { const v = new View(); return v.render(); }\n"
                       "module.exports = protoCaller;\n"},
        /* The value of a non-function const is invoked: `decode` holds whatever
         * makeDecoder returned. The existing callable rule withholds a Variable
         * target; whether CALLS may name a Variable is a contract question. */
        {"src/decode.ts", "const decode = makeDecoder();\n"
                          "export function decodeCaller() { return decode(1); }\n"
                          "function makeDecoder(): any { return null; }\n"},
        /* A require the file computes: no path, no module. */
        {"lib/dynamic.js", "const name = './view';\n"
                           "const View = require(name);\n"
                           "function dynamicCaller() { return new View(); }\n"
                           "module.exports = dynamicCaller;\n"},
    };
    js_project_t project = {0};
    ASSERT(js_project_index(&project, files, sizeof(files) / sizeof(files[0]), 0));
    ASSERT_EQ(js_project_edges(&project, "CALLS", "pageCaller", "web.page.pageHelper"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "protoCaller", "lib.view.render"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "decodeCaller", "src.decode.decode"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "dynamicCaller", "lib.view.View"), 0);
    js_project_free(&project);
    PASS();
}

/* A file with two nested `const handle` has one graph node showing the last. Edges
 * that the node's span settles are exact; the shadowed declaration keeps abstaining. */
TEST(resolver_js_project_same_named_nested_functions_settle_by_span) {
    static const js_project_file_t files[] = {
        {"src/a.ts", "export function helper() { return 1; }\n"
                     "export function first() {\n"
                     "  const handle = () => {\n"
                     "    helper();\n"
                     "  };\n"
                     "  handle();\n"
                     "}\n"
                     "export function second() {\n"
                     "  const handle = () => {\n"
                     "    return helper();\n"
                     "  };\n"
                     "  handle();\n"
                     "}\n"},
    };
    js_project_t project = {0};
    ASSERT(js_project_index(&project, files, sizeof(files) / sizeof(files[0]), 0));
    /* Only second's handle is what the node shows, for both of its edges. */
    ASSERT_EQ(js_project_edges(&project, "CALLS", "second", "src.a.handle"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "first", "src.a.handle"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "handle", "src.a.helper"), 1);
    js_project_free(&project);
    PASS();
}

/* lib/response.js and lib/view.js of Express: members the file itself assigns. */
static int js_project_members_case(int padding) {
    static const js_project_file_t files[] = {
        {"lib/res.js", "const res = {};\n"
                       "res.set = res.header = function header() { return 1; };\n"
                       "res.send = function send() {\n"
                       "  const later = () => this.set();\n"
                       "  this.header();\n"
                       "  this.replaced();\n"
                       "  this.missing();\n"
                       "  function plain() { return this.set(); }\n"
                       "  return later;\n"
                       "};\n"
                       "res.replaced = function replaced() { return 1; };\n"
                       "function swap() { res.replaced = other; }\n"
                       "function other() { return 1; }\n"
                       "function direct() { return res.send(); }\n"
                       "module.exports = res;\n"},
        {"lib/view.js", "function View(name) { this.lookup(name); }\n"
                        "View.prototype.lookup = function lookup(name) { return this.resolve(name); };\n"
                        "View.prototype.resolve = function resolve(name) { return name; };\n"
                        "View.prototype.anon = function () { return this.lookup(); };\n"
                        "function NotProto() { this.lookup(); }\n"
                        "module.exports = View;\n"},
        {"src/api.ts", "const api = {\n"
                       "  run() { return this.helper(); },\n"
                       "  helper: () => 1,\n"
                       "  named: function namedFn() { return 1; },\n"
                       "};\n"
                       "export function apiCaller() { return api.run(); }\n"
                       "let moved = { go() { return 1; } };\n"
                       "moved = other();\n"
                       "export function movedCaller() { return moved.go(); }\n"
                       "function other(): any { return null; }\n"},
        /* Hazards an adversarial read of the rule found. `Held` is the module's in every
         * positive; each decoy spells a member the module-level owner really has. */
        {"lib/hazard.js",
         "const Held = { keep() { return 1; } };\n"
         "function heldCaller() { return Held.keep(); }\n"
         "function shadowedOwner(Held) {\n"
         "  Held.local = function localThis() { return this.keep(); };\n"
         "}\n"
         "function nestedOwner() {\n"
         "  const Held = { inner() { return this.keep(); } };\n"
         "  return Held;\n"
         "}\n"
         "function getterValue() { return 1; }\n"
         "const Getter = {\n"
         "  get prop() { return getterValue; },\n"
         "  readProp() { return this.prop(); },\n"
         "};\n"
         "const Defined = { swapped() { return 1; }, useSwapped() { return this.swapped(); } };\n"
         "Object.defineProperty(Defined, 'swapped', { value: getterValue });\n"
         "const Mixed = { blended() { return 1; }, useBlended() { return this.blended(); } };\n"
         "Object.assign(Mixed, { blended: getterValue });\n"
         "const SelfMixed = { own() { return 1; }, init() { Object.assign(this, {}); return this.own(); } };\n"
         "const Gone = { removed() { return 1; }, useRemoved() { delete Gone.removed; return this.removed(); } };\n"
         "function Proto() {}\n"
         "Proto.prototype.onProto = function onProto() { return 1; };\n"
         "Proto.prototype.useProto = function useProto() { return this.onProto(); };\n"
         "mixin(Proto.prototype, {});\n"
         "const Map2 = { held() { return 1; }, useHeld() { return this.held(); } };\n"
         "const seen = new Map();\n"
         "seen.set(Map2, 1);\n"
         "const Outer = { outerTarget() { return 1; }, make() {\n"
         "  class Made { field = this.outerTarget(); }\n"
         "  return new Made();\n"
         "} };\n"
         "function mixin() {}\n"
         "module.exports = { heldCaller, shadowedOwner, nestedOwner, Getter, Defined, Mixed, "
         "SelfMixed, Gone, Proto, Map2, Outer };\n"},
    };
    js_project_t project = {0};
    ASSERT(js_project_index(&project, files, sizeof(files) / sizeof(files[0]), padding));
    ASSERT_EQ(js_project_edges(&project, "CALLS", "heldCaller", "lib.hazard.keep"), 1);
    /* The owner is matched by name, so a parameter or nested `Held` must not reach it. */
    ASSERT_EQ(js_project_edges(&project, "CALLS", "localThis", "lib.hazard.keep"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "inner", "lib.hazard.keep"), 0);
    /* A getter runs on the read; the call invokes what it returned. */
    ASSERT_EQ(js_project_edges(&project, "CALLS", "readProp", "lib.hazard.prop"), 0);
    /* Writes no assignment spells. */
    ASSERT_EQ(js_project_edges(&project, "CALLS", "useSwapped", "lib.hazard.swapped"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "useBlended", "lib.hazard.blended"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "init", "lib.hazard.own"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "useRemoved", "lib.hazard.removed"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "useProto", "lib.hazard.onProto"), 0);
    /* `seen.set(Map2, 1)` writes to `seen`; Map2's members are untouched. */
    ASSERT_EQ(js_project_edges(&project, "CALLS", "useHeld", "lib.hazard.held"), 1);
    /* A field initializer's `this` is the class instance, not the literal around it. */
    ASSERT_EQ(js_project_edges(&project, "CALLS", "make", "lib.hazard.outerTarget"), 0);
    /* Both names of `res.set = res.header = function header`. */
    ASSERT_EQ(js_project_edges(&project, "CALLS", "send", "lib.res.header"), 1);
    /* An assignment in a function can replace it; nothing assigns `missing`. */
    ASSERT_EQ(js_project_edges(&project, "CALLS", "send", "lib.res.replaced"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "direct", "lib.res.send"), 1);
    /* A plain nested function's `this` is not the owner's. */
    ASSERT_EQ(js_project_edges(&project, "CALLS", "plain", "lib.res.header"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "View", "lib.view.lookup"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "lookup", "lib.view.resolve"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "anon", "lib.view.lookup"), 1);
    /* `NotProto` has no prototype member of that name. */
    ASSERT_EQ(js_project_edges(&project, "CALLS", "NotProto", "lib.view.lookup"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "apiCaller", "src.api.run"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "run", "src.api.helper"), 1);
    /* A rewritten variable says nothing about its members any more. */
    ASSERT_EQ(js_project_edges(&project, "CALLS", "movedCaller", "src.api.go"), 0);
    js_project_free(&project);
    PASS();
}

TEST(resolver_js_project_members_sequential) {
    return js_project_members_case(0);
}
TEST(resolver_js_project_members_parallel) {
    return js_project_members_case(55);
}

/* package.json `type` decides a file with no module syntax of its own. */
TEST(resolver_js_project_manifest_type_decides_a_plain_file) {
    static const js_project_file_t files[] = {
        {"esm/package.json", "{\"name\":\"esm\",\"type\":\"module\"}\n"},
        {"esm/plain.js", "function caller() { return helper(); } function helper() {}\n"},
        {"cjs/package.json", "{\"name\":\"cjs\",\"type\":\"commonjs\"}\n"},
        {"cjs/plain.js", "function caller2() { return helper2(); } function helper2() {}\n"},
        {"web/package.json", "{\"name\":\"web\"}\n"},
        {"web/plain.js", "function caller3() { return helper3(); } function helper3() {}\n"},
    };
    js_project_t project = {0};
    ASSERT(js_project_index(&project, files, sizeof(files) / sizeof(files[0]), 0));
    ASSERT_EQ(js_project_edges(&project, "CALLS", "caller", "esm.plain.helper"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "caller2", "cjs.plain.helper2"), 1);
    /* No declaration: a browser may load it as a classic script. */
    ASSERT_EQ(js_project_edges(&project, "CALLS", "caller3", "web.plain.helper3"), 0);
    js_project_free(&project);
    PASS();
}

SUITE(resolver_binding_js) {
    RUN_TEST(resolver_binding_js_ordinary_syntax_keeps_the_file_proved);
    RUN_TEST(resolver_binding_js_commonjs_syntax_proves_a_module);
    RUN_TEST(resolver_binding_js_module_extensions_and_manifest_prove_a_module);
    RUN_TEST(resolver_binding_js_local_names_block_only_their_own_scope);
    RUN_TEST(resolver_binding_js_function_consts_and_nested_functions_bind_by_definition);
    RUN_TEST(resolver_binding_js_value_and_type_names_do_not_collide);
    RUN_TEST(resolver_binding_js_export_surface_of_an_es_module);
    RUN_TEST(resolver_binding_js_export_surface_of_commonjs);
    RUN_TEST(resolver_binding_js_require_bindings_are_scoped_imports);
    RUN_TEST(resolver_js_project_commonjs_sequential);
    RUN_TEST(resolver_js_project_commonjs_parallel);
    RUN_TEST(resolver_js_project_typescript_sequential);
    RUN_TEST(resolver_js_project_typescript_parallel);
    RUN_TEST(resolver_js_project_manifest_type_decides_a_plain_file);
    RUN_TEST(resolver_js_project_members_sequential);
    RUN_TEST(resolver_js_project_members_parallel);
    RUN_TEST(resolver_js_project_same_named_nested_functions_settle_by_span);
    RUN_TEST(resolver_js_project_known_gaps_abstain_with_a_reason);
    RUN_TEST(resolver_binding_js_index_exports_use_exact_source_identity);
    RUN_TEST(resolver_binding_js_index_scope_requires_captured_identity);
    RUN_TEST(resolver_binding_js_family_const_values_keep_exact_declarations);
    RUN_TEST(resolver_binding_js_const_values_do_not_prove_invocations_or_members);
    RUN_TEST(resolver_binding_js_const_initialization_shadow_and_writes_abstain);
    RUN_TEST(resolver_binding_js_const_export_requires_unique_materialized_value);
    RUN_TEST(resolver_binding_js_variable_and_function_const_references_bind);
    RUN_TEST(resolver_binding_js_family_module_functions_and_named_exports);
    RUN_TEST(resolver_binding_js_hoisted_function_and_private_export);
    RUN_TEST(resolver_binding_js_import_alias_requires_target_export_proof);
    RUN_TEST(resolver_binding_js_shadow_tdz_and_assignments_block_imports);
    RUN_TEST(resolver_binding_js_exports_come_from_syntax);
    RUN_TEST(resolver_binding_js_local_export_clause_and_typed_declarations);
    RUN_TEST(resolver_binding_js_script_globals_and_dynamic_effects_are_unproved);
    RUN_TEST(resolver_binding_js_value_reference_initializer_preserves_import_binding);
    RUN_TEST(resolver_binding_tsx_jsx_keeps_exact_function_binding);
}
