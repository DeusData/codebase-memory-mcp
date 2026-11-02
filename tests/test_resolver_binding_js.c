#include "cbm.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "pipeline/pipeline.h"
#include "resolve/binding.h"
#include "resolve/policy.h"
#include "resolve/stats.h"
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

/* Vite's `expect.any(Function)` and `typeof Function`, and Vue's `import { Function } from
 * '@babel/types'`, each used to void every proof in the file. The `Function` constructor
 * compiles in the global scope, which sees no binding here; only a direct eval runs code
 * in this scope. An indirect eval is global too, by the same rule. */
TEST(resolver_binding_js_only_direct_eval_is_a_dynamic_effect) {
    static const struct {
        CBMLanguage language;
        const char *path;
        const char *source;
    } cases[] = {
        {CBM_LANG_JAVASCRIPT, "binding.js",
         "export function caller() { const maker = Function(code); return helper(); } "
         "function helper() {}"},
        {CBM_LANG_JAVASCRIPT, "binding.js",
         "export function caller() { const maker = new Function(code); return helper(); } "
         "function helper() {}"},
        {CBM_LANG_JAVASCRIPT, "binding.js",
         "export function caller() { const run = eval; run(code); return helper(); } "
         "function helper() {}"},
        {CBM_LANG_TYPESCRIPT, "binding.ts",
         "export function caller(x: any) { expect.any(Function); return helper(); } "
         "function helper() {}"},
        {CBM_LANG_TYPESCRIPT, "binding.ts",
         "export const AsyncFunction = (async function () {}).constructor as typeof Function;\n"
         "export function caller() { return helper(); } function helper() {}"},
        {CBM_LANG_TYPESCRIPT, "binding.ts",
         "import type { Function } from '@babel/types';\n"
         "export function caller(node: Function) { return helper(); } function helper() {}"},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        const char *source = cases[i].source;
        CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), cases[i].language,
                                                 "test", cases[i].path, 0, NULL, NULL);
        ASSERT_NOT_NULL(result);
        CBMArena arena;
        cbm_arena_init(&arena);
        cbm_resolve_binding_index_t *index =
            cbm_resolve_binding_build(&arena, result, cases[i].language, source, strlen(source));
        cbm_resolve_result_t resolved =
            js_site(index, source, "helper", "helper()", js_local(&arena, result, "helper"));
        ASSERT(cbm_resolve_admit(&resolved));
        cbm_arena_destroy(&arena);
        cbm_free_result(result);
    }
    PASS();
}

/* Vite's node/index.ts, Vue's compiler-sfc/parse.ts and runtime-dom/jsx.ts: valid TypeScript
 * the grammar cannot place, each of which used to void every proof in its file. A real
 * error or a gap that could hide a binding keeps abstaining. */
TEST(resolver_binding_js_parse_gaps_in_types_keep_the_file_proved) {
    static const char *const inert[] = {
        "export type * as HttpProxy from 'http-proxy-3';\n",
        "export interface Block { scriptAst?: import('@babel/types').Statement[]; }\n",
        "export interface Attrs { x?: Numberish | undefined; in2?: Numberish | undefined; }\n",
    };
    for (size_t i = 0; i < sizeof(inert) / sizeof(inert[0]); i++) {
        char source[256];
        snprintf(source, sizeof(source),
                 "%sexport function caller() { return helper(); } function helper() {}\n",
                 inert[i]);
        CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_TYPESCRIPT,
                                                 "test", "binding.ts", 0, NULL, NULL);
        ASSERT_NOT_NULL(result);
        CBMArena arena;
        cbm_arena_init(&arena);
        cbm_resolve_binding_index_t *index =
            cbm_resolve_binding_build(&arena, result, CBM_LANG_TYPESCRIPT, source, strlen(source));
        cbm_resolve_result_t resolved =
            js_site(index, source, "helper", "helper()", js_local(&arena, result, "helper"));
        ASSERT(cbm_resolve_admit(&resolved));
        cbm_arena_destroy(&arena);
        cbm_free_result(result);
    }
    /* A gap in an expression, or one that swallows a brace, can hide a declaration. */
    static const char *const voiding[] = {
        "const a = ((;\nexport function caller() { return helper(); } function helper() {}\n",
        "export function caller() { return helper(); } function helper() {}\nconst = 1;\n",
        "@((x)) export function caller() { return helper(); } function helper() {}\n",
    };
    for (size_t i = 0; i < sizeof(voiding) / sizeof(voiding[0]); i++) {
        const char *source = voiding[i];
        CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_TYPESCRIPT,
                                                 "test", "binding.ts", 0, NULL, NULL);
        ASSERT_NOT_NULL(result);
        CBMArena arena;
        cbm_arena_init(&arena);
        cbm_resolve_binding_index_t *index =
            cbm_resolve_binding_build(&arena, result, CBM_LANG_TYPESCRIPT, source, strlen(source));
        cbm_resolve_result_t resolved =
            js_site(index, source, "helper", "helper()", js_local(&arena, result, "helper"));
        ASSERT(!cbm_resolve_admit(&resolved));
        cbm_arena_destroy(&arena);
        cbm_free_result(result);
    }
    PASS();
}

/* Vite's `;(resolved.build as Options) = v` and Vue's `;(next as string) += x`. The cast changes
 * no target: the name is rewritten, so it no longer holds the function it was given. */
static bool js_cast_write_case_admits(const char *source) {
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_TYPESCRIPT,
                                             "test", "binding.ts", 0, NULL, NULL);
    if (!result)
        return false;
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_binding_index_t *index =
        cbm_resolve_binding_build(&arena, result, CBM_LANG_TYPESCRIPT, source, strlen(source));
    cbm_resolve_result_t resolved =
        js_site(index, source, "handler", "handler()", js_local(&arena, result, "handler"));
    bool admitted = cbm_resolve_admit(&resolved);
    cbm_arena_destroy(&arena);
    cbm_free_result(result);
    return admitted;
}

TEST(resolver_binding_js_cast_write_rebinds_its_name) {
    /* The caller comes first: the site is found as the first `handler()` of the source. */
    ASSERT(js_cast_write_case_admits("export function caller() { return handler(); }\n"
                                     "function handler() {}\n"));
    ASSERT(!js_cast_write_case_admits("export function caller() { return handler(); }\n"
                                      "function handler() {}\n"
                                      ";(handler as any) = second;\n"
                                      "function second() {}\n"));
    ASSERT(!js_cast_write_case_admits("export function caller() { return handler(); }\n"
                                      "function handler() {}\n"
                                      ";(handler satisfies any) = second;\n"
                                      "function second() {}\n"));
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
 * selects, not which value it holds. A function const is a declared function, and a
 * name a module-level pattern declares is that variable, whatever `object` holds. */
TEST(resolver_binding_js_variable_and_function_const_references_bind) {
    static const char *const sources[] = {
        "export let target = 42; function caller() { return target; }",
        "export var target = 42; function caller() { return target; }",
        "export const target = () => 42; function caller() { return target; }",
        "export const {target} = object; function caller() { return target; }",
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

/* The fixture is returned by value and the index keeps its arena, so the arena must not move. */
typedef struct {
    CBMFileResult *result;
    CBMArena *arena;
    cbm_resolve_binding_index_t *index;
} js_fixture_t;

static js_fixture_t js_fixture(const char *source, CBMLanguage language, const char *file,
                               bool declared_module) {
    js_fixture_t fixture = {0};
    fixture.result =
        cbm_extract_file(source, (int)strlen(source), language, "test", file, 0, NULL, NULL);
    fixture.arena = malloc(sizeof(*fixture.arena));
    if (!fixture.arena)
        return fixture;
    cbm_arena_init(fixture.arena);
    if (fixture.result)
        fixture.index = cbm_resolve_binding_build_declared_module(
            fixture.arena, fixture.result, language, source, strlen(source), declared_module);
    return fixture;
}

static void js_fixture_free(js_fixture_t *fixture) {
    if (fixture->arena)
        cbm_arena_destroy(fixture->arena);
    free(fixture->arena);
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
        js_local(fixture->arena, fixture->result, name));
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
        {"src/base.ts", "export class Base { inherited() { return 1; } over() { return 1; } }\n"},
        {"src/deflt.ts", "export default class Tokenizer { getPos() { return 1; } }\n"
                         "class Hidden { getPos() { return 2; } }\n"
                         "export { Hidden as Shown };\n"},
        {"src/usedeflt.ts", "import Tokenizer, { Shown } from \"./deflt\";\n"
                            "const tokenizer = new Tokenizer();\n"
                            "export function defaultReceiver() { return tokenizer.getPos(); }\n"
                            "export function renamedReceiver(s: Shown) { return s.getPos(); }\n"},
        {"src/child.ts", "import { Base } from \"./base\";\n"
                         "export class Child extends Base {\n"
                         "  run() { return this.inherited(); }\n"
                         "  over() { return 2; }\n"
                         "  callSuper() { return super.over(); }\n"
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
    ASSERT_EQ(js_project_edges(&project, "CALLS", "callSuper", "src.base.Base.over"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "defaultReceiver", "src.deflt.Tokenizer.getPos"),
              1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "callSuper", "src.child.Child.over"), 0);
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

/* Kilo's workspace: object-form `workspaces`, `workspace:*` and catalog dependencies, and
 * exports patterns that map a subpath to a source file. The manifest says exactly which file
 * each import is. */
static int js_project_workspace_manifests_case(int padding) {
    static const js_project_file_t files[] = {
        {"package.json",
         "{\"name\":\"kilo\",\"private\":true,\"workspaces\":{\"packages\":"
         "[\"packages/*\",\"packages/sdk/js\"],\"catalog\":{\"zod\":\"1.0.0\"}}}\n"},
        {"packages/core/package.json",
         "{\"name\":\"@k/core\",\"version\":\"1.2.3\",\"exports\":{"
         "\"./effect/app\":\"./src/effect/app.ts\",\"./*\":\"./src/*.ts\","
         "\"./deep/*\":\"./src/nested/*.ts\",\"./secret/*\":null}}\n"},
        {"packages/core/src/effect/app.ts", "export function appFn() { return 1; }\n"},
        {"packages/core/src/util.ts", "export function utilFn() { return 1; }\n"},
        {"packages/core/src/deep/x.ts", "export function shadowedFn() { return 1; }\n"},
        {"packages/core/src/nested/x.ts", "export function nestedFn() { return 1; }\n"},
        {"packages/core/src/secret/s.ts", "export function secretFn() { return 1; }\n"},
        {"packages/sdk/js/package.json",
         "{\"name\":\"@k/sdk\",\"exports\":{\".\":\"./src/index.ts\"}}\n"},
        {"packages/sdk/js/src/index.ts", "export function sdkFn() { return 1; }\n"},
        {"packages/pinned/package.json",
         "{\"name\":\"@k/pinned\",\"version\":\"1.4.0\",\"exports\":\"./src/index.ts\"}\n"},
        {"packages/pinned/src/index.ts", "export function pinnedFn() { return 1; }\n"},
        {"packages/missed/package.json",
         "{\"name\":\"@k/missed\",\"version\":\"1.0.0\",\"exports\":\"./src/index.ts\"}\n"},
        {"packages/missed/src/index.ts", "export function missedFn() { return 1; }\n"},
        {"packages/aliased/package.json",
         "{\"name\":\"@k/aliased\",\"version\":\"1.0.0\",\"exports\":\"./src/index.ts\"}\n"},
        {"packages/aliased/src/index.ts", "export function aliasedFn() { return 1; }\n"},
        {"packages/catalogued/package.json",
         "{\"name\":\"@k/catalogued\",\"version\":\"1.0.0\",\"exports\":\"./src/index.ts\"}\n"},
        {"packages/catalogued/src/index.ts", "export function cataloguedFn() { return 1; }\n"},
        {"packages/app/package.json",
         "{\"name\":\"@k/app\",\"dependencies\":{\"@k/core\":\"workspace:*\","
         "\"@k/sdk\":\"workspace:^\",\"@k/pinned\":\"^1.0.0\",\"@k/missed\":\"^2.0.0\","
         "\"@k/aliased\":\"npm:other@1.0.0\",\"@k/catalogued\":\"catalog:\"},"
         "\"peerDependencies\":{\"@k/core\":\"^9.0.0\"}}\n"},
        {"packages/app/src/use.ts",
         "import { appFn } from '@k/core/effect/app'\n"
         "import { utilFn } from '@k/core/util'\n"
         "import { nestedFn } from '@k/core/deep/x'\n"
         "import { secretFn } from '@k/core/secret/s'\n"
         "import { sdkFn } from '@k/sdk'\n"
         "import { pinnedFn } from '@k/pinned'\n"
         "import { missedFn } from '@k/missed'\n"
         "import { aliasedFn } from '@k/aliased'\n"
         "import { cataloguedFn } from '@k/catalogued'\n"
         "export function viaExact() { return appFn(); }\n"
         "export function viaPattern() { return utilFn(); }\n"
         "export function viaLongerPattern() { return nestedFn(); }\n"
         "export function viaBlocked() { return secretFn(); }\n"
         "export function viaNested() { return sdkFn(); }\n"
         "export function viaRange() { return pinnedFn(); }\n"
         "export function viaMissedRange() { return missedFn(); }\n"
         "export function viaAlias() { return aliasedFn(); }\n"
         "export function viaCatalog() { return cataloguedFn(); }\n"},
    };
    js_project_t project = {0};
    ASSERT(js_project_index(&project, files, sizeof(files) / sizeof(files[0]), padding));
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaExact", "core.src.effect.app.appFn"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaPattern", "core.src.util.utilFn"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaLongerPattern", "core.src.nested.x.nestedFn"),
              1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaBlocked", "core.src.secret.s.secretFn"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaNested", "sdk.js.src.sdkFn"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaRange", "pinned.src.pinnedFn"), 1);
    /* These can install another copy ahead of the workspace package. */
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaMissedRange", "missed.src.missedFn"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaAlias", "aliased.src.aliasedFn"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaCatalog", "catalogued.src.cataloguedFn"), 0);
    js_project_free(&project);
    PASS();
}

TEST(resolver_js_project_workspace_manifests_sequential) {
    return js_project_workspace_manifests_case(0);
}
TEST(resolver_js_project_workspace_manifests_parallel) {
    return js_project_workspace_manifests_case(55);
}

/* A base config is read through `extends`, so changing it changes how files far from it resolve.
 * A later index over the same database must then match a fresh one. */
TEST(resolver_js_project_extended_config_change_reresolves) {
    static const js_project_file_t files[] = {
        {"tsconfig.base.json", "{\"compilerOptions\":{\"paths\":{\"@x/*\":[\"./one/*\"]}}}\n"},
        {"tsconfig.json", "{\"extends\":\"./tsconfig.base.json\"}\n"},
        {"one/thing.ts", "export function thing() { return 1; }\n"},
        {"two/thing.ts", "export function thing() { return 2; }\n"},
        {"src/use.ts", "import { thing } from '@x/thing'\n"
                       "export function useThing() { return thing(); }\n"},
    };
    js_project_t project = {0};
    ASSERT(js_project_index(&project, files, sizeof(files) / sizeof(files[0]), 0));
    ASSERT_EQ(js_project_edges(&project, "CALLS", "useThing", "one.thing.thing"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "useThing", "two.thing.thing"), 0);
    cbm_store_close(project.store);
    project.store = NULL;
    js_project_write(project.root, "tsconfig.base.json",
                     "{\"compilerOptions\":{\"paths\":{\"@x/*\":[\"./two/*\"]}}}\n");
    char db[512];
    snprintf(db, sizeof(db), "%s/index.db", project.root);
    cbm_pipeline_t *pipeline = cbm_pipeline_new(project.root, db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(pipeline);
    ASSERT_EQ(cbm_pipeline_run(pipeline), 0);
    cbm_pipeline_free(pipeline);
    project.store = cbm_store_open_path(db);
    ASSERT_NOT_NULL(project.store);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "useThing", "one.thing.thing"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "useThing", "two.thing.thing"), 1);
    js_project_free(&project);
    PASS();
}

/* The same, for a base with no telling name, in a directory the config does not govern, and for
 * a package's own manifest, which any importer in the project resolves through. */
TEST(resolver_js_project_far_config_change_reresolves) {
    static const js_project_file_t files[] = {
        {"configs/base.json", "{\"compilerOptions\":{\"paths\":{\"@x/*\":[\"../one/*\"]}}}\n"},
        {"app/tsconfig.json", "{\"extends\":\"../configs/base.json\"}\n"},
        {"one/thing.ts", "export function thing() { return 1; }\n"},
        {"two/thing.ts", "export function thing() { return 2; }\n"},
        {"package.json", "{\"private\":true,\"workspaces\":[\"pkg\"]}\n"},
        {"pkg/package.json", "{\"name\":\"pkg\",\"version\":\"1.0.0\",\"exports\":\"./a.ts\"}\n"},
        {"pkg/a.ts", "export function entry() { return 1; }\n"},
        {"pkg/b.ts", "export function entry() { return 2; }\n"},
        {"app/use.ts", "import { thing } from '@x/thing'\n"
                       "import { entry } from 'pkg'\n"
                       "export function useThing() { return thing(); }\n"
                       "export function useEntry() { return entry(); }\n"},
    };
    js_project_t project = {0};
    ASSERT(js_project_index(&project, files, sizeof(files) / sizeof(files[0]), 0));
    ASSERT_EQ(js_project_edges(&project, "CALLS", "useThing", "one.thing.thing"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "useEntry", "pkg.a.entry"), 1);
    cbm_store_close(project.store);
    project.store = NULL;
    js_project_write(project.root, "configs/base.json",
                     "{\"compilerOptions\":{\"paths\":{\"@x/*\":[\"../two/*\"]}}}\n");
    js_project_write(project.root, "pkg/package.json",
                     "{\"name\":\"pkg\",\"version\":\"1.0.0\",\"exports\":\"./b.ts\"}\n");
    char db[512];
    snprintf(db, sizeof(db), "%s/index.db", project.root);
    cbm_pipeline_t *pipeline = cbm_pipeline_new(project.root, db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(pipeline);
    ASSERT_EQ(cbm_pipeline_run(pipeline), 0);
    cbm_pipeline_free(pipeline);
    project.store = cbm_store_open_path(db);
    ASSERT_NOT_NULL(project.store);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "useThing", "one.thing.thing"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "useThing", "two.thing.thing"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "useEntry", "pkg.a.entry"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "useEntry", "pkg.b.entry"), 1);
    js_project_free(&project);
    PASS();
}

/* Express's `var self = this` in `res.render`: the closure's `self.send()` is `this.send()` of the
 * method it closes over. A write or a second declaration ends that. */
static int js_project_this_alias_case(int padding) {
    static const js_project_file_t files[] = {
        {"lib/res.js", "var res = Object.create(null);\n"
                       "module.exports = res;\n"
                       "res.send = function send() { return 1; };\n"
                       "res.render = function render() {\n"
                       "  var self = this;\n"
                       "  return function done() { return self.send(); };\n"
                       "};\n"
                       "res.rewritten = function rewritten(other) {\n"
                       "  var self = this;\n"
                       "  self = other;\n"
                       "  return self.send();\n"
                       "};\n"
                       "res.twice = function twice(other) {\n"
                       "  var self = this;\n"
                       "  if (other) { var self = other; }\n"
                       "  return self.send();\n"
                       "};\n"
                       "res.notThis = function notThis(other) {\n"
                       "  var self = other;\n"
                       "  return self.send();\n"
                       "};\n"
                       "function outer() {\n"
                       "  function inner() {\n"
                       "    var self = this;\n"
                       "    return self.send();\n"
                       "  }\n"
                       "  return inner;\n"
                       "}\n"
                       "outer.send = function send2() { return 2; };\n"},
    };
    js_project_t project = {0};
    ASSERT(js_project_index(&project, files, sizeof(files) / sizeof(files[0]), padding));
    ASSERT_EQ(js_project_edges(&project, "CALLS", "done", "lib.res.send"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "rewritten", "lib.res.send"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "twice", "lib.res.send"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "notThis", "lib.res.send"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "inner", "lib.res.send"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "inner", "lib.res.send2"), 0);
    js_project_free(&project);
    PASS();
}

TEST(resolver_js_project_this_alias_sequential) {
    return js_project_this_alias_case(0);
}

TEST(resolver_js_project_this_alias_parallel) {
    return js_project_this_alias_case(55);
}

/* A value narrowed below its declared class selects the narrower class's member, which a
 * declaration does not tell. `instanceof` and a declared guard both do that, in any spelling. */
static int js_project_narrowing_case(int padding) {
    static const js_project_file_t files[] = {
        {"tsconfig.json", "{}\n"},
        {"src/guards.ts", "import { Base, Sub } from './classes'\n"
                          "export function isSub(a: Base): a is Sub { return a instanceof Sub; }\n"
                          "export function notGuard(a: Base): boolean { return !!a; }\n"},
        {"src/classes.ts", "export class Base { m() { return 1; } }\n"
                           "export class Sub extends Base { m() { return 2; } }\n"},
        {"src/use.ts",
         "import { Base, Sub } from './classes'\n"
         "import { isSub, notGuard } from './guards'\n"
         "const localGuard = (a: Base): a is Sub => a instanceof Sub;\n"
         "export function viaConst(a: Base) { if (a instanceof Sub) { const c = a; c.m(); } }\n"
         "export function viaGuard(a: Base) { if (isSub(a)) { const c = a; c.m(); } }\n"
         "export function viaDirectGuard(a: Base) { if (isSub(a)) { a.m(); } }\n"
         "export function viaLocalGuard(a: Base) { if (localGuard(a)) { a.m(); } }\n"
         "export function viaEarly(a: Base) { if (!(a instanceof Sub)) return; const c = a; c.m(); }\n"
         "export function viaClosure(a: Base) { if (a instanceof Sub) { [1].map(() => a.m()); } }\n"
         "export function viaOtherGuard(a: Base) { if (notGuard(a)) { a.m(); } }\n"
         "export function viaPlainCall(a: Base) { notGuard(a); return a.m(); }\n"
         "export function viaOtherName(a: Base, b: Base) { if (b instanceof Sub) { a.m(); } }\n"
         "export function viaSibling(a: Base) { a.m(); }\n"
         "export function viaAssigned() { let a: Base = new Sub(); a.m(); }\n"
         "export function viaAnnotated() { const a = new Sub(); const b: Base = a; b.m(); }\n"
         "export class Holder {\n"
         "  held: Base = new Sub();\n"
         "  isIt(a: Base): a is Sub { return a instanceof Sub; }\n"
         "  viaMember() { if (this.held instanceof Sub) { this.held.m(); } }\n"
         "  viaMemberConst() { if (this.held instanceof Sub) { const c = this.held; c.m(); } }\n"
         "  viaThisGuard(a: Base) { if (this.isIt(a)) { a.m(); } }\n"
         "  viaUnrelated() { return this.held.m(); }\n"
         "}\n"
         "export function viaPath(h: Holder) { if (h.held instanceof Sub) { h.held!.m(); } }\n"},
    };
    js_project_t project = {0};
    ASSERT(js_project_index(&project, files, sizeof(files) / sizeof(files[0]), padding));
    const char *base = "src.classes.Base.m";
    static const char *const narrowed[] = {"viaConst",      "viaGuard",     "viaDirectGuard",
                                           "viaLocalGuard", "viaEarly",     "viaClosure",
                                           "viaMember",     "viaMemberConst", "viaThisGuard",
                                           "viaPath"};
    for (size_t i = 0; i < sizeof(narrowed) / sizeof(narrowed[0]); i++)
        ASSERT_EQ(js_project_edges(&project, "CALLS", narrowed[i], base), 0);
    static const char *const declared[] = {"viaOtherGuard", "viaPlainCall", "viaOtherName",
                                           "viaSibling",    "viaAssigned",  "viaAnnotated",
                                           "viaUnrelated"};
    for (size_t i = 0; i < sizeof(declared) / sizeof(declared[0]); i++)
        ASSERT_EQ(js_project_edges(&project, "CALLS", declared[i], base), 1);
    js_project_free(&project);
    PASS();
}

TEST(resolver_js_project_narrowing_has_no_declared_answer_sequential) {
    return js_project_narrowing_case(0);
}

TEST(resolver_js_project_narrowing_has_no_declared_answer_parallel) {
    return js_project_narrowing_case(55);
}

/* `**` matches any number of directories, and a list of them backtracks only to the last one: a
 * glob that cannot match a deep directory answers without trying every split of it. */
TEST(resolver_js_project_workspace_glob_with_many_double_stars) {
    char hostile[512] = "packages:\n  - '";
    for (int i = 0; i < 24; i++)
        strcat(hostile, "**/a/");
    strcat(hostile, "nothing'\n  - '**/a/**/lib'\n  - '!**/skipped/**'\n");
    js_project_file_t files[] = {
        {"pnpm-workspace.yaml", hostile},
        {"a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/lib/package.json",
         "{\"name\":\"deep\",\"version\":\"1.0.0\",\"main\":\"index.ts\"}\n"},
        {"a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/lib/index.ts",
         "export function deepFn() { return 1; }\n"},
        {"a/skipped/a/lib/package.json",
         "{\"name\":\"hidden\",\"version\":\"1.0.0\",\"main\":\"index.ts\"}\n"},
        {"a/skipped/a/lib/index.ts", "export function hiddenFn() { return 1; }\n"},
        {"package.json", "{\"name\":\"root\",\"private\":true}\n"},
        {"a/app/package.json", "{\"name\":\"app\",\"version\":\"1.0.0\","
                               "\"dependencies\":{\"deep\":\"workspace:*\","
                               "\"hidden\":\"workspace:*\"}}\n"},
        {"a/app/use.ts", "import { deepFn } from 'deep'\n"
                         "import { hiddenFn } from 'hidden'\n"
                         "export function useDeep() { return deepFn(); }\n"
                         "export function useHidden() { return hiddenFn(); }\n"},
    };
    js_project_t project = {0};
    ASSERT(js_project_index(&project, files, sizeof(files) / sizeof(files[0]), 0));
    ASSERT_EQ(js_project_edges(&project, "CALLS", "useDeep",
                               "a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.lib.deepFn"),
              1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "useHidden", "a.skipped.a.lib.hiddenFn"), 0);
    js_project_free(&project);
    PASS();
}

/* TypeScript tries baseUrl before any package, for a bare name too, and falls through when no
 * file is there. */
TEST(resolver_js_project_base_url_goes_before_a_package) {
    static const js_project_file_t files[] = {
        {"tsconfig.json", "{\"compilerOptions\":{\"baseUrl\":\"./src\"}}\n"},
        {"package.json", "{\"name\":\"root\",\"private\":true,\"workspaces\":[\"pkgs/*\"]}\n"},
        {"pkgs/foo/package.json", "{\"name\":\"foo\",\"version\":\"1.0.0\",\"main\":\"index.ts\"}\n"},
        {"pkgs/foo/index.ts", "export function fromPackage() { return 9; }\n"
                              "export function thing() { return 9; }\n"},
        {"pkgs/bar/package.json", "{\"name\":\"bar\",\"version\":\"1.0.0\",\"main\":\"index.ts\"}\n"},
        {"pkgs/bar/index.ts", "export function barThing() { return 9; }\n"},
        {"src/foo.ts", "export function thing() { return 1; }\n"},
        {"src/use.ts", "import { thing } from 'foo'\n"
                       "import { barThing } from 'bar'\n"
                       "export function useThing() { return thing(); }\n"
                       "export function useBar() { return barThing(); }\n"},
    };
    js_project_t project = {0};
    ASSERT(js_project_index(&project, files, sizeof(files) / sizeof(files[0]), 0));
    ASSERT_EQ(js_project_edges(&project, "CALLS", "useThing", "src.foo.thing"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "useThing", "pkgs.foo.thing"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "useBar", "pkgs.bar.barThing"), 1);
    js_project_free(&project);
    PASS();
}

/* A member call through a type alias selects what every constituent selects: vite's
 * `Environment` union of subclasses of one base, vue's `TypeResolveContext` of a class and a Pick
 * of it. Two constituents reaching two declarations select neither. */
static int js_project_alias_case(int padding) {
    static const js_project_file_t files[] = {
        {"tsconfig.json", "{}\n"},
        {"src/env.ts", "export class Base {\n"
                       "  shared() { return 1; }\n"
                       "  both() { return 1; }\n"
                       "}\n"
                       "export class Dev extends Base {\n"
                       "  both() { return 2; }\n"
                       "  onlyDev() { return 2; }\n"
                       "}\n"
                       "export class Build extends Base {\n"
                       "  both() { return 3; }\n"
                       "}\n"
                       "export class Unknown extends Base {}\n"
                       "export type Environment = Dev | Build | Unknown\n"
                       "export type Maybe = Dev | null\n"
                       "export type Plain = Dev\n"
                       "export class One extends Base {}\n"
                       "export class Two extends Base {}\n"
                       "export class Three extends Base {}\n"
                       "export class Four extends Base {}\n"
                       "export class Five extends Base {}\n"
                       "export class Six extends Base {}\n"
                       "export class Seven extends Base {}\n"
                       "export class Eight extends Base {}\n"
                       "export class Nine extends Base {}\n"
                       "export class Ten extends Base {}\n"
                       "export type Long = One | Two | Three | Four | Five | Six | Seven | Eight |\n"
                       "  Nine | Ten | Dev | Build | Unknown\n"},
        {"src/ctx.ts", "export class Holder {\n"
                       "  error() { return 1; }\n"
                       "  helper() { return 2; }\n"
                       "  hidden() { return 3; }\n"
                       "  warn() { return 4; }\n"
                       "  own() { return 5; }\n"
                       "}\n"
                       "export class Other {\n"
                       "  option() { return 1; }\n"
                       "}\n"},
        {"src/alias.ts", "import type { Holder, Other } from './ctx'\n"
                         "export type Simple = Pick<\n"
                         "  Holder,\n"
                         "  // utils\n"
                         "  | 'error'\n"
                         "  | 'helper'\n"
                         "> & Partial<Pick<Other, 'option'>> & {\n"
                         "    ast: string[]\n"
                         "    own(): void\n"
                         "  }\n"
                         "export type Context = (Holder | Simple) & { silent?: boolean }\n"
                         "export type Without = Omit<Holder, 'warn'>\n"
                         "export type Mixed = Holder | Other\n"
                         "type Local = Holder\n"
                         "export function viaLocal(l: Local) { return l.helper(); }\n"},
        {"src/use.ts", "import type { Environment, Long, Maybe, Plain } from './env'\n"
                       "import type { Context, Simple, Without, Mixed } from './alias'\n"
                       "export function sharedCall(e: Environment) { return e.shared(); }\n"
                       "export function bothCall(e: Environment) { return e.both(); }\n"
                       "export function maybeCall(e: Maybe) { return e!.onlyDev(); }\n"
                       "export function longCall(e: Long) { return e.shared(); }\n"
                       "export function plainCall(e: Plain) { return e.onlyDev(); }\n"
                       "export function errorCall(c: Context) { return c.error(); }\n"
                       "export function helperCall(c: Simple) { return c.helper(); }\n"
                       "export function hiddenCall(c: Simple) { return c.hidden(); }\n"
                       "export function ownCall(c: Simple) { return c.own(); }\n"
                       "export function optionCall(c: Simple) { return c.option(); }\n"
                       "export function kept(w: Without) { return w.helper(); }\n"
                       "export function dropped(w: Without) { return w.warn(); }\n"
                       "export function mixedCall(m: Mixed) { return m.error(); }\n"},
    };
    js_project_t project = {0};
    ASSERT(js_project_index(&project, files, sizeof(files) / sizeof(files[0]), padding));
    ASSERT_EQ(js_project_edges(&project, "CALLS", "sharedCall", "src.env.Base.shared"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "bothCall", "src.env.Base.both"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "bothCall", "src.env.Dev.both"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "bothCall", "src.env.Build.both"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "maybeCall", "src.env.Dev.onlyDev"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "longCall", "src.env.Base.shared"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "plainCall", "src.env.Dev.onlyDev"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "errorCall", "src.ctx.Holder.error"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "helperCall", "src.ctx.Holder.helper"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaLocal", "src.ctx.Holder.helper"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "optionCall", "src.ctx.Other.option"), 1);
    /* Pick left it out, so the checker rejects it; no edge claims otherwise. */
    ASSERT_EQ(js_project_edges(&project, "CALLS", "hiddenCall", "src.ctx.Holder.hidden"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "ownCall", "src.ctx.Holder.own"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "kept", "src.ctx.Holder.helper"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "dropped", "src.ctx.Holder.warn"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "mixedCall", "src.ctx.Holder.error"), 0);
    js_project_free(&project);
    PASS();
}

TEST(resolver_js_project_alias_selection_sequential) {
    return js_project_alias_case(0);
}

TEST(resolver_js_project_alias_selection_parallel) {
    return js_project_alias_case(55);
}

/* vite's `class PluginContext extends MinimalPluginContext implements Omit<RollupPluginContext,
 * 'cache'>`: what a class implements declares nothing for it, so an interface the index cannot see
 * does not hide its base's method. A base it cannot see still does. */
static int js_project_implements_case(int padding) {
    static const js_project_file_t files[] = {
        {"tsconfig.json", "{}\n"},
        {"src/ctx.ts", "import type { Rollup } from 'rollup'\n"
                       "import { External } from 'external'\n"
                       "export class Holder {\n"
                       "  go() { return 1; }\n"
                       "}\n"
                       "export class Base {\n"
                       "  debug() { return 1; }\n"
                       "  held: Holder = new Holder()\n"
                       "}\n"
                       "export class Middle extends Base implements Omit<Rollup, 'cache'> {}\n"
                       "export class Leaf extends Middle implements Rollup {\n"
                       "  debugTwice() { super.debug(); return this.debug(); }\n"
                       "}\n"
                       "export class Foreign extends External implements Rollup {}\n"
                       "export class Above extends Foreign {}\n"
                       "export class Lonely implements Rollup {}\n"},
        {"src/use.ts", "import { Leaf, Above, Lonely } from './ctx'\n"
                       "export function viaLeaf() { return new Leaf().debug(); }\n"
                       "export function memberLeaf(l: Leaf) { return l.held.go(); }\n"
                       "export function viaAbove() { return new Above().debug(); }\n"
                       "export function viaLonely(l: Lonely) { return l.debug(); }\n"},
    };
    js_project_t project = {0};
    ASSERT(js_project_index(&project, files, sizeof(files) / sizeof(files[0]), padding));
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaLeaf", "src.ctx.Base.debug"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "debugTwice", "src.ctx.Base.debug"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "memberLeaf", "src.ctx.Holder.go"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaAbove", "src.ctx.Base.debug"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaLonely", "src.ctx.Base.debug"), 0);
    js_project_free(&project);
    PASS();
}

TEST(resolver_js_project_implements_declares_nothing_sequential) {
    return js_project_implements_case(0);
}

TEST(resolver_js_project_implements_declares_nothing_parallel) {
    return js_project_implements_case(55);
}

/* vite's `environments: Record<'client' | 'ssr', DevEnvironment>`: any key holds the value type.
 * immutable's Record is not that, and a member redeclared with a type no class names hides the
 * base's. */
static int js_project_record_case(int padding) {
    static const js_project_file_t files[] = {
        {"tsconfig.json", "{}\n"},
        {"src/env.ts", "export class Env {\n"
                       "  go() { return 1; }\n"
                       "}\n"
                       "export class Graph {\n"
                       "  walk() { return 1; }\n"
                       "}\n"
                       "export interface Server {\n"
                       "  environments: Record<'client' | 'ssr', Env>\n"
                       "  list: Env[]\n"
                       "  named: Env\n"
                       "}\n"
                       "export class Base {\n"
                       "  thing: Env = new Env()\n"
                       "}\n"
                       "export class Sub extends Base {\n"
                       "  thing: string | Graph = ''\n"
                       "}\n"},
        {"src/foreign.ts", "import { Record } from 'immutable'\n"
                           "import { Env } from './env'\n"
                           "export interface Foreign {\n"
                           "  envs: Record<string, Env>\n"
                           "}\n"
                           "export function foreignLocal(r: Record<string, Env>) {\n"
                           "  return r.a.go();\n"
                           "}\n"},
        {"src/use.ts", "import type { Server } from './env'\n"
                       "import { Env, Sub } from './env'\n"
                       "import type { Foreign } from './foreign'\n"
                       "export function dotted(s: Server) { return s.environments.ssr.go(); }\n"
                       "export function indexed(s: Server, k: string) {\n"
                       "  return s.environments[k].go();\n"
                       "}\n"
                       "export function localRecord(r: Record<string, Env>) { return r.a.go(); }\n"
                       "export function localIndexed(r: Record<string, Env>) { return r['a'].go(); }\n"
                       "export function listMethod(s: Server) { return s.list.go(); }\n"
                       "export function namedChain(s: Server) { return s.named.go(); }\n"
                       "export function viaForeign(f: Foreign) { return f.envs.a.go(); }\n"
                       "export function hidden(s: Sub) { return s.thing.go(); }\n"},
    };
    js_project_t project = {0};
    ASSERT(js_project_index(&project, files, sizeof(files) / sizeof(files[0]), padding));
    ASSERT_EQ(js_project_edges(&project, "CALLS", "dotted", "src.env.Env.go"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "indexed", "src.env.Env.go"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "localRecord", "src.env.Env.go"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "localIndexed", "src.env.Env.go"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "namedChain", "src.env.Env.go"), 1);
    /* An array is no Record: `.go` on one is not on its element. */
    ASSERT_EQ(js_project_edges(&project, "CALLS", "listMethod", "src.env.Env.go"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaForeign", "src.env.Env.go"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "foreignLocal", "src.env.Env.go"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "hidden", "src.env.Env.go"), 0);
    js_project_free(&project);
    PASS();
}

TEST(resolver_js_project_record_values_sequential) {
    return js_project_record_case(0);
}

TEST(resolver_js_project_record_values_parallel) {
    return js_project_record_case(55);
}

/* TanStack's shape: `exports` names built files, and a first condition the tsconfig turns on
 * names the source. `!` and `**` come from the same pnpm-workspace.yaml. */
TEST(resolver_js_project_custom_condition_and_workspace_globs) {
    static const js_project_file_t files[] = {
        {"package.json", "{\"name\":\"root\",\"private\":true}\n"},
        {"pnpm-workspace.yaml", "packages:\n  - 'packages/*'\n  - 'tools/**'\n"
                                "  - '!packages/skipped*'\n"},
        {"tsconfig.json", "{\"compilerOptions\":{\"customConditions\":[\"@t/src\"]}}\n"},
        {"packages/core/package.json",
         "{\"name\":\"@t/core\",\"exports\":{\".\":{\"@t/src\":\"./src/index.ts\","
         "\"import\":\"./build/index.js\",\"require\":\"./build/index.cjs\"}}}\n"},
        {"packages/core/src/index.ts", "export function coreFn() { return 1; }\n"},
        /* Conditions run in object order, so a later one is not chosen by itself. */
        {"packages/late/package.json",
         "{\"name\":\"@t/late\",\"exports\":{\".\":{\"import\":\"./build/index.js\","
         "\"@t/src\":\"./src/index.ts\"}}}\n"},
        {"packages/late/src/index.ts", "export function lateFn() { return 1; }\n"},
        {"packages/skippedOne/package.json",
         "{\"name\":\"@t/skipped\",\"exports\":\"./src/index.ts\"}\n"},
        {"packages/skippedOne/src/index.ts", "export function skippedFn() { return 1; }\n"},
        {"tools/deep/nested/package.json",
         "{\"name\":\"@t/deep\",\"exports\":\"./src/index.ts\"}\n"},
        {"tools/deep/nested/src/index.ts", "export function deepFn() { return 1; }\n"},
        {"packages/app/package.json",
         "{\"name\":\"@t/app\",\"dependencies\":{\"@t/core\":\"workspace:*\","
         "\"@t/late\":\"workspace:*\",\"@t/deep\":\"workspace:*\","
         "\"@t/skipped\":\"workspace:*\"}}\n"},
        {"packages/app/tsconfig.json", "{\"extends\":\"../../tsconfig.json\"}\n"},
        {"packages/app/src/use.ts",
         "import { coreFn } from '@t/core'\n"
         "import { lateFn } from '@t/late'\n"
         "import { deepFn } from '@t/deep'\n"
         "import { skippedFn } from '@t/skipped'\n"
         "export function viaCondition() { return coreFn(); }\n"
         "export function viaLaterCondition() { return lateFn(); }\n"
         "export function viaDeepGlob() { return deepFn(); }\n"
         "export function viaNegated() { return skippedFn(); }\n"},
        /* This config neither sets nor inherits the condition. */
        {"packages/plain/package.json",
         "{\"name\":\"@t/plain\",\"dependencies\":{\"@t/core\":\"workspace:*\"}}\n"},
        {"packages/plain/tsconfig.json", "{\"compilerOptions\":{\"strict\":true}}\n"},
        {"packages/plain/src/use.ts",
         "import { coreFn } from '@t/core'\n"
         "export function viaNoCondition() { return coreFn(); }\n"},
    };
    js_project_t project = {0};
    ASSERT(js_project_index(&project, files, sizeof(files) / sizeof(files[0]), 0));
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaCondition", "core.src.coreFn"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaLaterCondition", "late.src.lateFn"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaDeepGlob", "nested.src.deepFn"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaNegated", "skippedOne.src.skippedFn"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaNoCondition", "core.src.coreFn"), 0);
    js_project_free(&project);
    PASS();
}

/* pnpm declares its workspace in pnpm-workspace.yaml, and links only `workspace:` by default. */
TEST(resolver_js_project_pnpm_workspace_yaml) {
    static const js_project_file_t files[] = {
        {"package.json", "{\"name\":\"root\",\"private\":true}\n"},
        {"pnpm-workspace.yaml", "# packages\npackages:\n  - 'packages/*'\n  - \"tools/one\"\n"
                                "catalog:\n  zod: 1.0.0\n"},
        {"packages/lib/package.json",
         "{\"name\":\"@p/lib\",\"version\":\"1.0.0\",\"exports\":\"./src/index.ts\"}\n"},
        {"packages/lib/src/index.ts", "export function libFn() { return 1; }\n"},
        {"tools/one/package.json",
         "{\"name\":\"@p/one\",\"version\":\"1.0.0\",\"exports\":\"./src/index.ts\"}\n"},
        {"tools/one/src/index.ts", "export function oneFn() { return 1; }\n"},
        {"packages/app/package.json",
         "{\"name\":\"@p/app\",\"dependencies\":{\"@p/lib\":\"workspace:*\","
         "\"@p/one\":\"^1.0.0\"}}\n"},
        {"packages/app/src/use.ts",
         "import { libFn } from '@p/lib'\n"
         "import { oneFn } from '@p/one'\n"
         "export function viaWorkspace() { return libFn(); }\n"
         "export function viaPlainRange() { return oneFn(); }\n"},
    };
    js_project_t project = {0};
    ASSERT(js_project_index(&project, files, sizeof(files) / sizeof(files[0]), 0));
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaWorkspace", "lib.src.libFn"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaPlainRange", "one.src.oneFn"), 0);
    js_project_free(&project);
    PASS();
}

/* Vue core: a pnpm workspace whose manifests export only built dist/ files. TypeScript tries
 * the governing tsconfig's `paths` before any package lookup, so `@vue/shared` is that source. */
static int js_project_workspace_paths_case(int padding) {
    static const js_project_file_t files[] = {
        {"tsconfig.json", "{\"compilerOptions\":{\"paths\":{\"@vue/*\":[\"./packages/*/src\"],"
                          "\"@ghost/*\":[\"./ghost/*\"],\"vue\":[\"./packages/vue/src\"]}}}\n"},
        {"package.json", "{\"name\":\"root\",\"private\":true,\"workspaces\":[\"packages/*\"]}\n"},
        {"packages/shared/package.json",
         "{\"name\":\"@vue/shared\",\"main\":\"index.js\",\"exports\":{\".\":"
         "{\"types\":\"./dist/shared.d.ts\",\"import\":\"./dist/shared.esm-bundler.js\","
         "\"require\":\"./index.js\"}}}\n"},
        {"packages/shared/src/index.ts", "export * from './makeMap'\n"},
        {"packages/shared/src/makeMap.ts", "export function makeMap() { return 1; }\n"},
        {"packages/vue/package.json", "{\"name\":\"vue\",\"main\":\"index.js\"}\n"},
        {"packages/vue/src/index.ts", "export { makeMap as vueMap } from '@vue/shared'\n"},
        {"packages/core/package.json",
         "{\"name\":\"@vue/core\",\"dependencies\":{\"@vue/shared\":\"workspace:*\"}}\n"},
        {"packages/core/src/use.ts",
         "import { makeMap } from '@vue/shared'\n"
         "import { vueMap } from 'vue'\n"
         "import { ghostFn } from '@ghost/lib'\n"
         "export function useShared() { return makeMap(); }\n"
         "export function useVue() { return vueMap(); }\n"
         "export function useGhost() { return ghostFn(); }\n"},
        /* The manifest wins here: Node reads no tsconfig, and dist/ is not in the tree. */
        /* Nest: `exports` names ./index.js and the tree holds only its source. A package a config
         * without `paths` imports through the workspace, with no build output on disk. */
        {"packages/lib/package.json",
         "{\"name\":\"@wk/lib\",\"type\":\"module\",\"main\":\"./index.js\","
         "\"exports\":{\".\":\"./index.js\",\"./sub\":\"./sub.js\"}}\n"},
        {"packages/lib/index.ts", "export function libFn() { return 1; }\n"},
        {"packages/lib/sub.ts", "export function subFn() { return 1; }\n"},
        {"packages/app/package.json", "{\"name\":\"@wk/app\"}\n"},
        {"packages/app/tsconfig.json", "{\"compilerOptions\":{\"strict\":true}}\n"},
        {"packages/app/use.ts",
         "import { libFn } from '@wk/lib'\n"
         "import { subFn } from '@wk/lib/sub'\n"
         "export function viaManifest() { return libFn(); }\n"
         "export function viaSubpath() { return subFn(); }\n"},
        /* Node reads no TypeScript: the same import from JavaScript has no file to bind. */
        {"packages/app/plain.js",
         "import { libFn } from '@wk/lib'\n"
         "export function plainManifest() { return libFn(); }\n"},
        /* A member of a sibling index.ts holds the QN of the module beside it. */
        {"packages/core/src/server/index.ts",
         "export function start() { const pluginContainer = () => 1; return pluginContainer(); }\n"},
        {"packages/core/src/server/pluginContainer.ts",
         "export function createContainer() { return 1; }\n"},
        {"packages/core/src/server/environment.ts",
         "import { createContainer } from './pluginContainer'\n"
         "export function init() { return createContainer(); }\n"},
        {"packages/core/src/plain.js",
         "import { makeMap } from '@vue/shared'\n"
         "export function usePlain() { return makeMap(); }\n"},
        /* A config governs its directory and extends nothing, so root's `paths` do not apply. */
        {"packages/solo/tsconfig.json", "{\"compilerOptions\":{\"strict\":true}}\n"},
        {"packages/solo/src/use.ts",
         "import { makeMap } from '@vue/shared'\n"
         "export function useSolo() { return makeMap(); }\n"},
        /* Nest's packages/core/tsconfig.json: `"files": []` and references compile nothing, so
         * the file belongs to the project above it. */
        {"packages/refs/tsconfig.json", "{\"files\":[],\"references\":[{\"path\":\"./b\"}]}\n"},
        {"packages/refs/src/use.ts",
         "import { makeMap } from '@vue/shared'\n"
         "export function useRefs() { return makeMap(); }\n"},
        /* Its base holds `paths`, whose targets are relative to the base's own directory. */
        {"configs/tsconfig.base.json",
         "{\"compilerOptions\":{\"paths\":{\"@base/*\":[\"../packages/shared/src/*\"]}}}\n"},
        {"apps/web/tsconfig.json", "{\"extends\":\"../../configs/tsconfig.base.json\"}\n"},
        {"apps/web/src/use.ts",
         "import { makeMap } from '@base/makeMap'\n"
         "export function useBase() { return makeMap(); }\n"},
        /* With baseUrl, `paths` targets are relative to it and not to the config. */
        {"apps/rooted/tsconfig.json",
         "{\"compilerOptions\":{\"baseUrl\":\"./src\",\"paths\":{\"~/*\":[\"lib/*\"]}}}\n"},
        {"apps/rooted/lib/decoy.ts", "export function rooted() { return 2; }\n"},
        {"apps/rooted/src/lib/real.ts", "export function rooted() { return 1; }\n"},
        {"apps/rooted/src/use.ts",
         "import { rooted } from '~/real'\n"
         "import { rooted as other } from '~/decoy'\n"
         "export function useRooted() { return rooted(); }\n"
         "export function useRootedDecoy() { return other(); }\n"},
    };
    js_project_t project = {0};
    ASSERT(js_project_index(&project, files, sizeof(files) / sizeof(files[0]), padding));
    const char *make_map = "packages.shared.src.makeMap.makeMap";
    ASSERT_EQ(js_project_edges(&project, "CALLS", "useShared", make_map), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "useVue", make_map), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "useBase", make_map), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaManifest", "packages.lib.libFn"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaSubpath", "packages.lib.sub.subFn"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "plainManifest", "packages.lib.libFn"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "init",
                               "packages.core.src.server.pluginContainer.createContainer"),
              1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "usePlain", make_map), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "useSolo", make_map), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "useRefs", make_map), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "useRooted", "apps.rooted.src.lib.real.rooted"),
              1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "useRootedDecoy", "apps.rooted.lib.decoy.rooted"),
              0);
    js_project_free(&project);
    PASS();
}

TEST(resolver_js_project_workspace_paths_sequential) {
    return js_project_workspace_paths_case(0);
}
TEST(resolver_js_project_workspace_paths_parallel) {
    return js_project_workspace_paths_case(55);
}

/* Vue's `runner.effect.stop()` and `instance.scope.on()`, Vite's `this.pluginContainer.close()`:
 * a member's type is what its class declares, wherever that class lives, in the file that
 * declares it. */
static int js_project_member_types_case(int padding) {
    static const js_project_file_t files[] = {
        {"src/effect.ts",
         "export class Effect { stop() { return 1; } run() { return 2; } }\n"
         "export class Other { stop() { return 3; } }\n"},
        {"src/types.ts",
         "import { Effect, Other } from './effect'\n"
         "export interface Runner<T = any> { (): T; effect: Effect; opt?: Effect; both: Effect | Other }\n"
         "export class Base { held: Effect = new Effect(); shadow: Other = new Other(); }\n"
         "export class Kid extends Base {\n"
         "  shadow: Effect = new Effect();\n"
         "  made = new Effect();\n"
         "  constructor(private readonly param: Effect, plain: Other) { super(); }\n"
         "  get viaGetter(): Effect { return this.made; }\n"
         "  static shared: Effect = new Effect();\n"
         "  method(): Effect { return this.made; }\n"
         "}\n"
         "export class Generic<Effect> { item: Effect; list: Other; }\n"
         "export class Unknown extends Missing { }\n"},
        /* Nest's RouterExplorer: the value argument is read before the arrow beside it is
         * walked, so one receiver is seen after a later site. That stale read ended every member
         * proof for the rest of a 500-line class. */
        {"src/order.ts",
         "class R { handler() { return 1; } }\n"
         "class S { run() { return 2; } }\n"
         "export function reg(route: R, other: S) {\n"
         "  const h = Object.assign((...args: unknown[]) => {\n"
         "    return route.handler();\n"
         "  }, route.handler);\n"
         "  return [h, other];\n"
         "}\n"
         "export function laterProof(other: S) { return other.run(); }\n"},
        {"src/use.ts",
         "import { Runner, Kid, Generic, Unknown } from './types'\n"
         "export function viaInterface(r: Runner) { r.effect.stop(); }\n"
         "export function viaOptional(r: Runner) { r.opt?.stop(); }\n"
         "export function viaUnion(r: Runner) { r.both.stop(); }\n"
         "export function viaInherited(k: Kid) { k.held.stop(); }\n"
         "export function viaShadow(k: Kid) { k.shadow.stop(); }\n"
         "export function viaInitializer(k: Kid) { k.made.stop(); }\n"
         "export function viaParameter(k: Kid) { k.param.stop(); }\n"
         "export function viaGetter(k: Kid) { k.viaGetter.stop(); }\n"
         "export function viaStatic(k: Kid) { k.shared.stop(); }\n"
         "export function viaMethod(k: Kid) { k.method.stop(); }\n"
         "export function viaTypeParam(g: Generic<any>) { g.item.stop(); }\n"
         "export function viaSibling(g: Generic<any>) { g.list.stop(); }\n"
         "export function viaUnknown(u: Unknown) { u.held.stop(); }\n"
         "export function viaPlain(k: Kid) { k.plain.stop(); }\n"
         "export class Local { kid: Kid; go() { this.kid.held.run(); } }\n"},
    };
    js_project_t project = {0};
    ASSERT(js_project_index(&project, files, sizeof(files) / sizeof(files[0]), padding));
    const char *stop = "src.effect.Effect.stop";
    const char *other = "src.effect.Other.stop";
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaInterface", stop), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaOptional", stop), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaUnion", stop), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaUnion", other), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaInherited", stop), 1);
    /* The subclass's own declaration of a member beats its base's. */
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaShadow", stop), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaShadow", other), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaInitializer", stop), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaParameter", stop), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaGetter", stop), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaStatic", stop), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaMethod", stop), 0);
    /* `Effect` is the class's own parameter here, not the imported class. */
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaTypeParam", stop), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaSibling", other), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaUnknown", stop), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaPlain", other), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "go", "src.effect.Effect.run"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "laterProof", "src.order.S.run"), 1);
    js_project_free(&project);
    PASS();
}

TEST(resolver_js_project_member_types_sequential) {
    return js_project_member_types_case(0);
}
TEST(resolver_js_project_member_types_parallel) {
    return js_project_member_types_case(55);
}

/* Nest's `const context = this.createContext(...); context.setType()`, Vite's
 * `const server = await createServer(); server.close()` and Vue's `const { scope } = instance`:
 * a const holds what its initializer is declared to yield. */
static int js_project_initializers_case(int padding) {
    static const js_project_file_t files[] = {
        {"src/core.ts",
         "export class Made { go() { return 1; } }\n"
         "export class Other { go() { return 2; } }\n"},
        {"src/made.ts",
         "import { Made, Other } from './core'\n"
         "export function make(): Made { return new Made(); }\n"
         "export async function makeLater(): Promise<Made> { return new Made(); }\n"
         "export function either(): Made | Other { return new Made(); }\n"
         "export function maybe(): Made | undefined { return new Made(); }\n"
         "export function generic<Made>(x: Made): Made { return x; }\n"
         "export function over(a: string): Made;\n"
         "export function over(a: number): Other;\n"
         "export function over(a: any): any { return a; }\n"
         "export function inferred() { return new Made(); }\n"
         "export class Factory {\n"
         "  held: Made = new Made();\n"
         "  create(): Made { return new Made(); }\n"
         "  get made(): Made { return new Made(); }\n"
         "  self(): Factory { return this; }\n"
         "}\n"
         "export class Box<Made> { open(): Made { return null as any; } }\n"},
        {"src/usemade.ts",
         "import { make, makeLater, either, maybe, generic, over, inferred, Factory, Box } from "
         "'./made'\n"
         "import { Made, Other } from './core'\n"
         "function pick(): any { return 1; }\n"
         "function local(): Other { return new Other(); }\n"
         "export function viaCall() { const m = make(); m.go(); }\n"
         "export function viaDirect() { make().go(); }\n"
         "export async function viaAwait() { const m = await makeLater(); m.go(); }\n"
         "export function viaPromise() { const m = makeLater(); m.go(); }\n"
         "export function viaEither() { const m = either(); m.go(); }\n"
         "export function viaMaybe() { const m = maybe(); m!.go(); }\n"
         "export function viaGeneric() { const m = generic(new Other()); m.go(); }\n"
         "export function viaOver() { const m = over('a'); m.go(); }\n"
         "export function viaInferred() { const m = inferred(); m.go(); }\n"
         "export function viaLocal() { const m = local(); m.go(); }\n"
         "export function viaMethod(f: Factory) { const m = f.create(); m.go(); }\n"
         "export function viaChain(f: Factory) { f.self().create().go(); }\n"
         "export function viaAlias(f: Factory) { const g = f; const m = g.made; m.go(); }\n"
         "export function viaShorthand(f: Factory) { const { held } = f; held.go(); }\n"
         "export function viaRenamed(f: Factory) { const { held: h } = f; h.go(); }\n"
         "export function viaRewritten() { let m = make(); m = pick(); m.go(); }\n"
         "export function viaTypeParam(b: Box<any>) { const m = b.open(); m.go(); }\n"
         "export function viaCycle() { const a = c.x; const c = a.y; a.go(); }\n"
         "export function viaIndex(list: Made[]) { list[0].go(); }\n"
         "export function viaOf(list: ReadonlyArray<Made>) { for (const m of list) { m.go(); } }\n"
         "export function viaSet(list: Set<Made>) { for (const m of list) { m.go(); } }\n"
         "export function viaIn(list: Made[]) { for (const k in list) { k.go(); } }\n"
         "export function viaMap(list: Map<string, Made>) { for (const m of list) { m.go(); } }\n"
         "export function viaSetIndex(list: Promise<Made>[]) { list[0].go(); }\n"
         "export class Holder {\n"
         "  items: Made[] = [];\n"
         "  others?: Other[];\n"
         "  viaField() { this.items[0].go(); }\n"
         "  viaFieldOf() { for (const o of this.others!) { o.go(); } }\n"
         "}\n"},
    };
    js_project_t project = {0};
    ASSERT(js_project_index(&project, files, sizeof(files) / sizeof(files[0]), padding));
    const char *made = "src.core.Made.go";
    const char *other = "src.core.Other.go";
    static const char *const proved[] = {"viaCall",   "viaDirect", "viaAwait",     "viaMaybe",
                                         "viaMethod", "viaChain",  "viaAlias",     "viaShorthand",
                                         "viaRenamed", "viaIndex", "viaOf", "viaSet", "viaField"};
    for (size_t i = 0; i < sizeof(proved) / sizeof(proved[0]); i++)
        ASSERT_EQ(js_project_edges(&project, "CALLS", proved[i], made), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaLocal", other), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaFieldOf", other), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaIn", made), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaMap", made), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaSetIndex", made), 0);
    static const char *const abstained[] = {"viaPromise", "viaEither",  "viaGeneric",  "viaOver",
                                            "viaInferred", "viaRewritten", "viaTypeParam", "viaCycle"};
    for (size_t i = 0; i < sizeof(abstained) / sizeof(abstained[0]); i++) {
        ASSERT_EQ(js_project_edges(&project, "CALLS", abstained[i], made), 0);
        ASSERT_EQ(js_project_edges(&project, "CALLS", abstained[i], other), 0);
    }
    js_project_free(&project);
    PASS();
}

TEST(resolver_js_project_initializers_sequential) {
    return js_project_initializers_case(0);
}
TEST(resolver_js_project_initializers_parallel) {
    return js_project_initializers_case(55);
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
        /* A JavaScript instance or inherited member. Nothing checks that `p.m` is still the
         * class's: any write can replace it per instance. Tried: the TS receiver rules, which
         * lean on the type checker; js_mode keeps them off and only own `this.m()` binds. */
        {"lib/jsclass.js", "class Plain { m() { return 1; } }\n"
                           "class Sub extends Plain { subRun() { return this.m(); } }\n"
                           "function viaLocal() { const p = new Plain(); return p.m(); }\n"
                           "module.exports = { Plain, Sub, viaLocal };\n"},
        /* A member assigned in a block may not run, so one assignment cannot stand for the
         * member. Tried: only a module-level statement counts as defining it. */
        {"lib/block.js", "const Owner = {};\n"
                         "if (process.env.X) { Owner.m = function blockM() { return 1; }; }\n"
                         "function blockCaller() { return Owner.m(); }\n"
                         "module.exports = blockCaller;\n"},
        /* A class type parameter blocks its name for the whole file: the walk records it at
         * the module scope, because the class has no scope of its own yet. This loses the
         * real `c: Cbaz` beside it and can never produce a wrong edge. */
        {"src/generic.ts", "export class Cbaz {}\n"
                           "export class Box<Cbaz> { item: Cbaz; }\n"
                           "export function genericCaller(c: Cbaz) { return c; }\n"},
    };
    js_project_t project = {0};
    ASSERT(js_project_index(&project, files, sizeof(files) / sizeof(files[0]), 0));
    ASSERT_EQ(js_project_edges(&project, "CALLS", "pageCaller", "web.page.pageHelper"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "protoCaller", "lib.view.render"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "decodeCaller", "src.decode.decode"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "dynamicCaller", "lib.view.View"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "viaLocal", "lib.jsclass.Plain.m"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "subRun", "lib.jsclass.Plain.m"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "blockCaller", "lib.block.blockM"), 0);
    ASSERT_EQ(js_project_edges(&project, "USES_TYPE", "genericCaller", "src.generic.Cbaz"), 0);
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
    /* The first `handle` has no node of its own, so its body belongs to `first`. */
    ASSERT_EQ(js_project_edges(&project, "CALLS", "first", "src.a.helper"), 1);
    /* `second` calls helper only through the `handle` the node shows. */
    ASSERT_EQ(js_project_edges(&project, "CALLS", "second", "src.a.helper"), 0);
    js_project_free(&project);
    PASS();
}

/* Vite's four nested `const handler` and Vue's `render: () => h(x)` test fixtures. Each
 * twin the node does not show gives its calls to the declaration around it, or the file. */
static int js_project_twins_case(int padding) {
    static const js_project_file_t files[] = {
        {"src/h.ts", "export function h() { return 1; }\n"
                     "export function inject() { return 1; }\n"},
        {"src/wrap.ts",
         "import { h, inject } from './h'\n"
         "export function wrapResolve(hook: any) {\n"
         "  const handler = function (id: string) {\n"
         "    return inject();\n"
         "  };\n"
         "  return handler;\n"
         "}\n"
         "export function wrapLoad(hook: any) {\n"
         "  const handler = function (id: string) {\n"
         "    return h();\n"
         "  };\n"
         "  return handler;\n"
         "}\n"},
        {"src/spec.ts",
         "import { h, inject } from './h'\n"
         "export const a = { render: () => h() };\n"
         "export const b = {\n"
         "  render: () => inject(),\n"
         "};\n"
         "export const c = {\n"
         "  render: () => {\n"
         "    return 1;\n"
         "  },\n"
         "};\n"},
        /* A one-line declaration cannot prove what shares its line. */
        {"src/oneline.ts",
         "import { h } from './h'\n"
         "export function near() { return 1; } export const d = { go: () => h() };\n"
         "export const e = { go: () => {\n"
         "  return 2;\n"
         "} };\n"},
        /* Accessors of one name: the node shows one, the other's calls go to the file. */
        {"src/accessor.ts",
         "import { h, inject } from './h'\n"
         "export class Dep { track() { return 1; } }\n"
         "export class Box {\n"
         "  dep = new Dep();\n"
         "  get value() {\n"
         "    this.dep.track();\n"
         "    return h();\n"
         "  }\n"
         "  set value(v: any) {\n"
         "    this.dep.track();\n"
         "    inject();\n"
         "  }\n"
         "}\n"},
    };
    js_project_t project = {0};
    ASSERT(js_project_index(&project, files, sizeof(files) / sizeof(files[0]), padding));
    ASSERT_EQ(js_project_edges(&project, "CALLS", "handler", "src.h.h"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "wrapResolve", "src.h.inject"), 1);
    /* Nothing crosses to the sibling wrapper or into the shown twin. */
    ASSERT_EQ(js_project_edges(&project, "CALLS", "wrapLoad", "src.h.inject"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "handler", "src.h.inject"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "wrapResolve", "src.h.h"), 0);
    /* The shown `render` calls nothing, so both twins go to the module. */
    ASSERT_EQ(js_project_edges(&project, "CALLS", "render", "src.h.h"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "render", "src.h.inject"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "src/spec.ts", "src.h.h"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "src/spec.ts", "src.h.inject"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "near", "src.h.h"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "src/oneline.ts", "src.h.h"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "src/accessor.ts", "src.h.h") +
                  js_project_edges(&project, "CALLS", "src/accessor.ts", "src.h.inject"),
              1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "value", "src.h.h") +
                  js_project_edges(&project, "CALLS", "value", "src.h.inject"),
              1);
    /* The proof of a typed member call moves with its occurrence. */
    ASSERT_EQ(js_project_edges(&project, "CALLS", "value", "src.accessor.Dep.track"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "src/accessor.ts", "src.accessor.Dep.track"), 1);
    js_project_free(&project);
    PASS();
}

TEST(resolver_js_project_twins_sequential) {
    return js_project_twins_case(0);
}
TEST(resolver_js_project_twins_parallel) {
    return js_project_twins_case(55);
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
        /* lib/application.js: `app[method] = fn` for every HTTP verb beside `app.set`. A computed
         * key declares nothing, and a literal key writes the member it spells. */
        {"lib/app.js", "var app = exports = module.exports = {};\n"
                       "app.init = function init() {\n"
                       "  Object.defineProperty(this, 'router', { get() { return 1; } });\n"
                       "  this.defaultConfiguration();\n"
                       "};\n"
                       "app.defaultConfiguration = function defaultConfiguration() {\n"
                       "  this.set('x');\n"
                       "};\n"
                       "app.set = function set(k) { return k; };\n"
                       "methods.forEach(function (method) {\n"
                       "  app[method] = function (path) { return this.set(path); };\n"
                       "});\n"
                       "app.replaced = function replaced() { return 1; };\n"
                       "app['replaced'] = other;\n"
                       "app.useReplaced = function useReplaced() { return this.replaced(); };\n"
                       "function other() { return 1; }\n"},
        /* lib/view.js: `var resolve = path.resolve` beside `View.prototype.resolve`, one QN. The
         * member names the function the node shows; the variable is not that function. */
        {"lib/mixed.js", "var resolve = require('path').resolve;\n"
                         "function View() {}\n"
                         "View.prototype.resolve = function resolve(d) {\n"
                         "  return d;\n"
                         "};\n"
                         "View.prototype.lookup = function lookup() {\n"
                         "  return this.resolve(1);\n"
                         "};\n"
                         "function plainCaller() { return resolve(1); }\n"
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
    ASSERT_EQ(js_project_edges(&project, "CALLS", "lookup", "lib.mixed.resolve"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "plainCaller", "lib.mixed.resolve"), 0);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "init", "lib.app.defaultConfiguration"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "defaultConfiguration", "lib.app.set"), 1);
    ASSERT_EQ(js_project_edges(&project, "CALLS", "useReplaced", "lib.app.replaced"), 0);
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

/* USAGE means a value. Kilo's `(input: Shape) => ...` and every other written type name
 * belongs to USES_TYPE alone, including the ones no signature carries. */
TEST(resolver_js_project_type_names_are_uses_type_never_usage) {
    static const js_project_file_t files[] = {
        {"types.ts", "export interface Shape { a: string }\n"
                     "export class Kind {}\n"
                     "export const value = 1;\n"
                     "export namespace Space { export type Inner = string; }\n"},
        {"use.ts", "import { Shape, Kind, value, Space } from \"./types\";\n"
                   "export class Holder { field: Kind; }\n"
                   "export function annotated(a: Shape, b: Space.Inner) { return a as Shape; }\n"
                   "export function viaTypeof(c: typeof value) { return c; }\n"
                   "export function viaValue() { return value; }\n"},
        /* These bind their own name, so a same-named module type is not what they use. */
        {"scoped.ts", "type Mapped = string;\n"
                      "type Inferred = number;\n"
                      "type Keys = 'a' | 'b';\n"
                      "export type M1 = { [Mapped in Keys]: Mapped };\n"
                      "export type C1<T> = T extends Array<infer Inferred> ? Inferred : never;\n"},
    };
    js_project_t project = {0};
    ASSERT(js_project_index(&project, files, sizeof(files) / sizeof(files[0]), 0));
    ASSERT_EQ(js_project_edges(&project, "USES_TYPE", "scoped.ts", "scoped.Keys"), 1);
    ASSERT_EQ(js_project_edges(&project, "USES_TYPE", "scoped.ts", "scoped.Mapped"), 0);
    ASSERT_EQ(js_project_edges(&project, "USES_TYPE", "scoped.ts", "scoped.Inferred"), 0);
    ASSERT_EQ(js_project_edges(&project, "USES_TYPE", "annotated", "types.Shape"), 1);
    ASSERT_EQ(js_project_edges(&project, "USES_TYPE", "annotated", "types.Space.Inner"), 1);
    /* A class field has no signature: the walk over type names is what finds it. */
    ASSERT_EQ(js_project_edges(&project, "USES_TYPE", "use.ts", "types.Kind"), 1);
    ASSERT_EQ(js_project_edges(&project, "USAGE", "annotated", "types.Shape"), 0);
    ASSERT_EQ(js_project_edges(&project, "USAGE", "annotated", "types.Space"), 0);
    ASSERT_EQ(js_project_edges(&project, "USAGE", "use.ts", "types.Kind"), 0);
    /* `typeof value` and a plain read both name the value, so both stay USAGE. */
    ASSERT_EQ(js_project_edges(&project, "USAGE", "viaTypeof", "types.value"), 1);
    ASSERT_EQ(js_project_edges(&project, "USAGE", "viaValue", "types.value"), 1);
    js_project_free(&project);
    PASS();
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

/* TypeScript's own baselines are files with tens of thousands of top-level statements, and
 * fetching a child by index walks from the first child: indexing that repository ran over
 * 28 minutes in this one audit before it was stopped. One pass reads each statement once. */
TEST(resolver_binding_js_http_default_audit_walks_the_top_level_once) {
    enum { STATEMENTS = 4000, LINE_ROOM = 32 };
    char *source = malloc((size_t)STATEMENTS * LINE_ROOM + LINE_ROOM);
    ASSERT_NOT_NULL(source);
    size_t length = 0;
    for (int i = 0; i < STATEMENTS; i++)
        length += (size_t)snprintf(source + length, LINE_ROOM, "const flat%d = %d;\n", i, i);
    length += (size_t)snprintf(source + length, LINE_ROOM, "export default flat0;\n");
    CBMFileResult *result =
        cbm_extract_file(source, (int)length, CBM_LANG_TYPESCRIPT, "test", "lib/flat.ts", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_stats_t stats;
    cbm_resolve_stats_reset(&stats);
    cbm_resolve_stats_t *previous = cbm_resolve_stats_bind(&stats);
    cbm_resolve_binding_index_t *index =
        cbm_resolve_binding_build(&arena, result, CBM_LANG_TYPESCRIPT, source, length);
    cbm_resolve_stats_bind(previous);
    ASSERT_NOT_NULL(index);
    cbm_resolve_stats_snapshot_t seen = cbm_resolve_stats_snapshot(&stats);
    ASSERT_EQ(seen.values[CBM_RESOLVE_STAT_SYNTAX_CHILD_STEPS], (uint64_t)STATEMENTS + 1);
    cbm_arena_destroy(&arena);
    cbm_free_result(result);
    free(source);
    PASS();
}

SUITE(resolver_binding_js) {
    RUN_TEST(resolver_binding_js_http_default_audit_walks_the_top_level_once);
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
    RUN_TEST(resolver_js_project_workspace_paths_sequential);
    RUN_TEST(resolver_js_project_workspace_manifests_sequential);
    RUN_TEST(resolver_js_project_workspace_manifests_parallel);
    RUN_TEST(resolver_js_project_pnpm_workspace_yaml);
    RUN_TEST(resolver_js_project_custom_condition_and_workspace_globs);
    RUN_TEST(resolver_js_project_this_alias_sequential);
    RUN_TEST(resolver_js_project_this_alias_parallel);
    RUN_TEST(resolver_js_project_narrowing_has_no_declared_answer_sequential);
    RUN_TEST(resolver_js_project_narrowing_has_no_declared_answer_parallel);
    RUN_TEST(resolver_js_project_workspace_glob_with_many_double_stars);
    RUN_TEST(resolver_js_project_base_url_goes_before_a_package);
    RUN_TEST(resolver_js_project_alias_selection_sequential);
    RUN_TEST(resolver_js_project_alias_selection_parallel);
    RUN_TEST(resolver_js_project_implements_declares_nothing_sequential);
    RUN_TEST(resolver_js_project_implements_declares_nothing_parallel);
    RUN_TEST(resolver_js_project_record_values_sequential);
    RUN_TEST(resolver_js_project_record_values_parallel);
    RUN_TEST(resolver_js_project_extended_config_change_reresolves);
    RUN_TEST(resolver_js_project_far_config_change_reresolves);
    RUN_TEST(resolver_js_project_member_types_sequential);
    RUN_TEST(resolver_js_project_member_types_parallel);
    RUN_TEST(resolver_js_project_initializers_sequential);
    RUN_TEST(resolver_js_project_initializers_parallel);
    RUN_TEST(resolver_js_project_workspace_paths_parallel);
    RUN_TEST(resolver_js_project_manifest_type_decides_a_plain_file);
    RUN_TEST(resolver_js_project_type_names_are_uses_type_never_usage);
    RUN_TEST(resolver_js_project_members_sequential);
    RUN_TEST(resolver_js_project_members_parallel);
    RUN_TEST(resolver_js_project_same_named_nested_functions_settle_by_span);
    RUN_TEST(resolver_js_project_twins_sequential);
    RUN_TEST(resolver_js_project_twins_parallel);
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
    RUN_TEST(resolver_binding_js_only_direct_eval_is_a_dynamic_effect);
    RUN_TEST(resolver_binding_js_parse_gaps_in_types_keep_the_file_proved);
    RUN_TEST(resolver_binding_js_cast_write_rebinds_its_name);
    RUN_TEST(resolver_binding_js_value_reference_initializer_preserves_import_binding);
    RUN_TEST(resolver_binding_tsx_jsx_keeps_exact_function_binding);
}
