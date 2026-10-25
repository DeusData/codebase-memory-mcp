#include "cbm.h"
#include "resolve/binding.h"
#include "resolve/policy.h"
#include "test_framework.h"

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

SUITE(resolver_binding_js) {
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
