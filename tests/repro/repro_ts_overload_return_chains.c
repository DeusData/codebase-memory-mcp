/*
 * repro_ts_overload_return_chains.c — REDs for argument-aware return typing
 * of TypeScript/TSX overloads outside method dispatch.
 *
 * Both fixtures register two overloads with the same QN and arity but reversed
 * positional types.  They invoke both orders and assert distinct downstream
 * method targets.  A declaration-order, last-definition, or arity-only fallback
 * can satisfy at most one chain; both become GREEN only when the call arguments
 * select the return signature used by expression and contextual callback typing.
 * Shadow and exact-import cases guard the selector's fail-closed boundaries.
 *
 * The shadowing fixtures use untyped parameters on purpose.  An UNKNOWN local
 * binding still owns the lexical name and must block both call-edge promotion
 * and return-type inheritance from a same-named registry symbol.
 */

#include "test_framework.h"

#include "arena.h"
#include "cbm.h"
#include "lsp/ts_lsp.h"

#include <stdbool.h>
#include <stdio.h>
#include <string.h>

enum { OVERLOAD_DEF_COUNT = 7 };

typedef struct {
    const char *symbol_module_qn;
    const char *caller_module_qn;
    const char *reversed_type_qn;
    const char *pair_type_qn;
    const char *reversed_method_qn;
    const char *pair_method_qn;
    const char *choose_qn;
    const char *run_qn;
} TSOverloadFixtureNames;

static const char *kNumberStringParams[] = {"number", "string"};
static const char *kStringNumberParams[] = {"string", "number"};
static const char *kNumberStringPairCallbackParams[] = {"number", "string",
                                                        "(value: Pair) => void"};
static const char *kStringNumberReversedCallbackParams[] = {"string", "number",
                                                            "(value: Reversed) => void"};

static bool ends_with(const char *text, const char *suffix) {
    if (!text || !suffix)
        return false;
    size_t text_len = strlen(text);
    size_t suffix_len = strlen(suffix);
    return suffix_len <= text_len && strcmp(text + text_len - suffix_len, suffix) == 0;
}

static int count_resolved_suffix(const CBMResolvedCallArray *calls, const char *callee_suffix,
                                 const char *strategy) {
    if (!calls)
        return 0;
    int count = 0;
    for (int i = 0; i < calls->count; i++) {
        const CBMResolvedCall *call = &calls->items[i];
        if (call->kind == CBM_RESOLVED_INVOCATION && call->confidence >= 0.5f && call->caller_qn &&
            strstr(call->caller_qn, "run") && ends_with(call->callee_qn, callee_suffix) &&
            call->strategy && strcmp(call->strategy, strategy) == 0) {
            count++;
        }
    }
    return count;
}

static int count_any_callee_suffix(const CBMResolvedCallArray *calls, const char *callee_suffix) {
    if (!calls)
        return 0;
    int count = 0;
    for (int i = 0; i < calls->count; i++) {
        const CBMResolvedCall *call = &calls->items[i];
        if (call->caller_qn && strstr(call->caller_qn, "run") &&
            ends_with(call->callee_qn, callee_suffix)) {
            count++;
        }
    }
    return count;
}

static void print_resolved_calls(const CBMResolvedCallArray *calls) {
    if (!calls)
        return;
    for (int i = 0; i < calls->count; i++) {
        const CBMResolvedCall *call = &calls->items[i];
        printf("    %s -> %s [%s %.2f kind=%d]\n", call->caller_qn ? call->caller_qn : "(null)",
               call->callee_qn ? call->callee_qn : "(null)",
               call->strategy ? call->strategy : "(null)", call->confidence, (int)call->kind);
    }
}

static void init_overload_defs(CBMLSPDef defs[OVERLOAD_DEF_COUNT], CBMLanguage language,
                               const TSOverloadFixtureNames *names) {
    memset(defs, 0, (size_t)OVERLOAD_DEF_COUNT * sizeof(*defs));

    defs[0].qualified_name = names->reversed_type_qn;
    defs[0].short_name = "Reversed";
    defs[0].label = "Interface";
    defs[0].def_module_qn = names->symbol_module_qn;
    defs[0].is_interface = true;
    defs[0].lang = language;

    defs[1].qualified_name = names->pair_type_qn;
    defs[1].short_name = "Pair";
    defs[1].label = "Interface";
    defs[1].def_module_qn = names->symbol_module_qn;
    defs[1].is_interface = true;
    defs[1].lang = language;

    defs[2].qualified_name = names->reversed_method_qn;
    defs[2].short_name = "reversedOnly";
    defs[2].label = "Method";
    defs[2].receiver_type = names->reversed_type_qn;
    defs[2].def_module_qn = names->symbol_module_qn;
    defs[2].return_types = "void";
    defs[2].lang = language;

    defs[3].qualified_name = names->pair_method_qn;
    defs[3].short_name = "pairOnly";
    defs[3].label = "Method";
    defs[3].receiver_type = names->pair_type_qn;
    defs[3].def_module_qn = names->symbol_module_qn;
    defs[3].return_types = "void";
    defs[3].lang = language;

    /* Same QN and arity; only ordered argument types distinguish these. */
    defs[4].qualified_name = names->choose_qn;
    defs[4].short_name = "choose";
    defs[4].label = "Function";
    defs[4].def_module_qn = names->symbol_module_qn;
    defs[4].return_types = "Pair";
    defs[4].signature_param_types = kNumberStringParams;
    defs[4].signature_param_count = 2;
    defs[4].lang = language;

    defs[5].qualified_name = names->choose_qn;
    defs[5].short_name = "choose";
    defs[5].label = "Function";
    defs[5].def_module_qn = names->symbol_module_qn;
    defs[5].return_types = "Reversed";
    defs[5].signature_param_types = kStringNumberParams;
    defs[5].signature_param_count = 2;
    defs[5].lang = language;

    defs[6].qualified_name = names->run_qn;
    defs[6].short_name = "run";
    defs[6].label = "Function";
    defs[6].def_module_qn = names->caller_module_qn;
    defs[6].return_types = "void";
    defs[6].lang = language;
}

static int assert_both_return_chains(const CBMResolvedCallArray *calls, const char *language_name,
                                     const char *fixture_name, const char *inner_strategy) {
    int inner_calls = count_resolved_suffix(calls, "choose", inner_strategy);
    bool pair = count_resolved_suffix(calls, "Pair.pairOnly", "lsp_ts_method") > 0;
    bool reversed = count_resolved_suffix(calls, "Reversed.reversedOnly", "lsp_ts_method") > 0;
    if (inner_calls < 2 || !pair || !reversed) {
        printf("  ts-overload-return: %s %s inner=%d pair=%d reversed=%d\n", language_name,
               fixture_name, inner_calls, pair, reversed);
        print_resolved_calls(calls);
    }
    return inner_calls >= 2 && pair && reversed ? 0 : 1;
}

static const char kFreeFunctionSource[] = "function run(): void {\n"
                                          "  choose(1, 'x').pairOnly();\n"
                                          "  choose('x', 1).reversedOnly();\n"
                                          "}\n";

static int assert_free_function_overload_returns(CBMLanguage language, bool jsx_mode,
                                                 const char *language_name) {
    static const TSOverloadFixtureNames names = {
        "repro.free",
        "repro.free",
        "repro.free.Reversed",
        "repro.free.Pair",
        "repro.free.Reversed.reversedOnly",
        "repro.free.Pair.pairOnly",
        "repro.free.choose",
        "repro.free.run",
    };
    CBMLSPDef defs[OVERLOAD_DEF_COUNT];
    init_overload_defs(defs, language, &names);

    CBMArena arena;
    cbm_arena_init(&arena);
    CBMTypeRegistry *registry = cbm_ts_build_cross_registry(&arena, defs, OVERLOAD_DEF_COUNT);
    CBMResolvedCallArray calls = {0};
    if (registry) {
        cbm_run_ts_lsp_cross_with_registry(&arena, kFreeFunctionSource,
                                           (int)strlen(kFreeFunctionSource), names.caller_module_qn,
                                           false, jsx_mode, false, registry, defs,
                                           OVERLOAD_DEF_COUNT, NULL, NULL, 0, NULL, &calls);
    }

    int rc = registry
                 ? assert_both_return_chains(&calls, language_name, "free-function", "lsp_ts_local")
                 : 1;
    if (!registry)
        printf("  ts-overload-return: %s free-function registry build failed\n", language_name);
    cbm_arena_destroy(&arena);
    return rc;
}

static const char kNamespaceSource[] = "import * as Lib from './provider';\n"
                                       "function run(): void {\n"
                                       "  Lib.choose(1, 'x').pairOnly();\n"
                                       "  Lib.choose('x', 1).reversedOnly();\n"
                                       "}\n";

static int assert_namespace_overload_returns(CBMLanguage language, bool jsx_mode,
                                             const char *language_name) {
    static const TSOverloadFixtureNames names = {
        "repro.provider",
        "repro.consumer",
        "repro.provider.Reversed",
        "repro.provider.Pair",
        "repro.provider.Reversed.reversedOnly",
        "repro.provider.Pair.pairOnly",
        "repro.provider.choose",
        "repro.consumer.run",
    };
    CBMLSPDef defs[OVERLOAD_DEF_COUNT];
    init_overload_defs(defs, language, &names);
    const char *import_names[] = {"Lib"};
    const char *import_qns[] = {names.symbol_module_qn};

    CBMArena arena;
    cbm_arena_init(&arena);
    CBMTypeRegistry *registry = cbm_ts_build_cross_registry(&arena, defs, OVERLOAD_DEF_COUNT);
    CBMResolvedCallArray calls = {0};
    if (registry) {
        cbm_run_ts_lsp_cross_with_registry(&arena, kNamespaceSource, (int)strlen(kNamespaceSource),
                                           names.caller_module_qn, false, jsx_mode, false, registry,
                                           defs, OVERLOAD_DEF_COUNT, import_names, import_qns, 1,
                                           NULL, &calls);
    }

    int rc = registry ? assert_both_return_chains(&calls, language_name, "namespace-import",
                                                  "lsp_ts_namespace")
                      : 1;
    if (!registry)
        printf("  ts-overload-return: %s namespace registry build failed\n", language_name);
    cbm_arena_destroy(&arena);
    return rc;
}

static const char kExactSymbolImportSource[] = "import { choose } from './provider';\n"
                                               "function run(): void {\n"
                                               "  choose(1, 'x').pairOnly();\n"
                                               "  choose('x', 1).reversedOnly();\n"
                                               "}\n";

static int assert_exact_symbol_import_overload_returns(CBMLanguage language, bool jsx_mode,
                                                       const char *language_name) {
    static const TSOverloadFixtureNames names = {
        "repro.exact.provider",
        "repro.exact.consumer",
        "repro.exact.provider.Reversed",
        "repro.exact.provider.Pair",
        "repro.exact.provider.Reversed.reversedOnly",
        "repro.exact.provider.Pair.pairOnly",
        "repro.exact.provider.choose",
        "repro.exact.consumer.run",
    };
    CBMLSPDef defs[OVERLOAD_DEF_COUNT];
    init_overload_defs(defs, language, &names);
    const char *import_names[] = {"choose"};
    /* Some production import maps already carry the exact exported-symbol QN.
     * The call lookup must not append the local name a second time. */
    const char *import_qns[] = {names.choose_qn};

    CBMArena arena;
    cbm_arena_init(&arena);
    CBMTypeRegistry *registry = cbm_ts_build_cross_registry(&arena, defs, OVERLOAD_DEF_COUNT);
    CBMResolvedCallArray calls = {0};
    if (registry) {
        cbm_run_ts_lsp_cross_with_registry(
            &arena, kExactSymbolImportSource, (int)strlen(kExactSymbolImportSource),
            names.caller_module_qn, false, jsx_mode, false, registry, defs, OVERLOAD_DEF_COUNT,
            import_names, import_qns, 1, NULL, &calls);
    }

    int chain_rc = registry ? assert_both_return_chains(&calls, language_name,
                                                        "exact-symbol-import", "lsp_ts_import")
                            : 1;
    int doubled = count_any_callee_suffix(&calls, "choose.choose");
    if (!registry || doubled != 0) {
        printf("  ts-overload-return: %s exact-symbol-import registry=%d doubled=%d\n",
               language_name, registry != NULL, doubled);
        print_resolved_calls(&calls);
    }
    cbm_arena_destroy(&arena);
    return registry && chain_rc == 0 && doubled == 0 ? 0 : 1;
}

static const char kContextualCallbackOverloadSource[] =
    "function run(): void {\n"
    "  choose(1, 'x', value => value.pairOnly());\n"
    "  choose('x', 1, value => value.reversedOnly());\n"
    "}\n";

static int assert_contextual_callback_uses_selected_overload(CBMLanguage language, bool jsx_mode,
                                                             const char *language_name) {
    static const TSOverloadFixtureNames names = {
        "repro.callback",
        "repro.callback",
        "repro.callback.Reversed",
        "repro.callback.Pair",
        "repro.callback.Reversed.reversedOnly",
        "repro.callback.Pair.pairOnly",
        "repro.callback.choose",
        "repro.callback.run",
    };
    CBMLSPDef defs[OVERLOAD_DEF_COUNT];
    init_overload_defs(defs, language, &names);
    defs[4].return_types = "void";
    defs[4].signature_param_types = kNumberStringPairCallbackParams;
    defs[4].signature_param_count = 3;
    defs[5].return_types = "void";
    defs[5].signature_param_types = kStringNumberReversedCallbackParams;
    defs[5].signature_param_count = 3;

    CBMArena arena;
    cbm_arena_init(&arena);
    CBMTypeRegistry *registry = cbm_ts_build_cross_registry(&arena, defs, OVERLOAD_DEF_COUNT);
    CBMResolvedCallArray calls = {0};
    if (registry) {
        cbm_run_ts_lsp_cross_with_registry(&arena, kContextualCallbackOverloadSource,
                                           (int)strlen(kContextualCallbackOverloadSource),
                                           names.caller_module_qn, false, jsx_mode, false, registry,
                                           defs, OVERLOAD_DEF_COUNT, NULL, NULL, 0, NULL, &calls);
    }

    int outer = count_resolved_suffix(&calls, "choose", "lsp_ts_local");
    int pair = count_resolved_suffix(&calls, "Pair.pairOnly", "lsp_ts_method");
    int reversed = count_resolved_suffix(&calls, "Reversed.reversedOnly", "lsp_ts_method");
    if (!registry || outer < 2 || pair < 1 || reversed < 1) {
        printf("  ts-overload-return: %s contextual-callback registry=%d outer=%d pair=%d "
               "reversed=%d\n",
               language_name, registry != NULL, outer, pair, reversed);
        print_resolved_calls(&calls);
    }
    cbm_arena_destroy(&arena);
    return registry && outer >= 2 && pair >= 1 && reversed >= 1 ? 0 : 1;
}

static int assert_no_shadowed_return_chains(const CBMResolvedCallArray *calls,
                                            const char *language_name, const char *fixture_name,
                                            const char *forbidden_strategy) {
    int promoted = count_resolved_suffix(calls, "choose", forbidden_strategy);
    int pair = count_resolved_suffix(calls, "Pair.pairOnly", "lsp_ts_method");
    int reversed = count_resolved_suffix(calls, "Reversed.reversedOnly", "lsp_ts_method");
    if (promoted != 0 || pair != 0 || reversed != 0) {
        printf("  ts-overload-return: %s %s promoted=%d pair=%d reversed=%d\n", language_name,
               fixture_name, promoted, pair, reversed);
        print_resolved_calls(calls);
    }
    return promoted == 0 && pair == 0 && reversed == 0 ? 0 : 1;
}

static const char kShadowedFreeFunctionSource[] = "function run(choose): void {\n"
                                                  "  choose(1, 'x').pairOnly();\n"
                                                  "  choose('x', 1).reversedOnly();\n"
                                                  "}\n";

static int assert_parameter_shadows_free_function(CBMLanguage language, bool jsx_mode,
                                                  const char *language_name) {
    static const TSOverloadFixtureNames names = {
        "repro.shadow.free",
        "repro.shadow.free",
        "repro.shadow.free.Reversed",
        "repro.shadow.free.Pair",
        "repro.shadow.free.Reversed.reversedOnly",
        "repro.shadow.free.Pair.pairOnly",
        "repro.shadow.free.choose",
        "repro.shadow.free.run",
    };
    CBMLSPDef defs[OVERLOAD_DEF_COUNT];
    init_overload_defs(defs, language, &names);

    CBMArena arena;
    cbm_arena_init(&arena);
    CBMTypeRegistry *registry = cbm_ts_build_cross_registry(&arena, defs, OVERLOAD_DEF_COUNT);
    CBMResolvedCallArray calls = {0};
    if (registry) {
        cbm_run_ts_lsp_cross_with_registry(&arena, kShadowedFreeFunctionSource,
                                           (int)strlen(kShadowedFreeFunctionSource),
                                           names.caller_module_qn, false, jsx_mode, false, registry,
                                           defs, OVERLOAD_DEF_COUNT, NULL, NULL, 0, NULL, &calls);
    }

    int rc = registry ? assert_no_shadowed_return_chains(&calls, language_name,
                                                         "parameter-shadows-free-function",
                                                         "lsp_ts_local")
                      : 1;
    if (!registry)
        printf("  ts-overload-return: %s free shadow registry build failed\n", language_name);
    cbm_arena_destroy(&arena);
    return rc;
}

static const char kShadowedNamespaceSource[] = "import * as Lib from './provider';\n"
                                               "function run(Lib): void {\n"
                                               "  Lib.choose(1, 'x').pairOnly();\n"
                                               "  Lib.choose('x', 1).reversedOnly();\n"
                                               "}\n";

static int assert_parameter_shadows_namespace(CBMLanguage language, bool jsx_mode,
                                              const char *language_name) {
    static const TSOverloadFixtureNames names = {
        "repro.shadow.provider",
        "repro.shadow.consumer",
        "repro.shadow.provider.Reversed",
        "repro.shadow.provider.Pair",
        "repro.shadow.provider.Reversed.reversedOnly",
        "repro.shadow.provider.Pair.pairOnly",
        "repro.shadow.provider.choose",
        "repro.shadow.consumer.run",
    };
    CBMLSPDef defs[OVERLOAD_DEF_COUNT];
    init_overload_defs(defs, language, &names);
    const char *import_names[] = {"Lib"};
    const char *import_qns[] = {names.symbol_module_qn};

    CBMArena arena;
    cbm_arena_init(&arena);
    CBMTypeRegistry *registry = cbm_ts_build_cross_registry(&arena, defs, OVERLOAD_DEF_COUNT);
    CBMResolvedCallArray calls = {0};
    if (registry) {
        cbm_run_ts_lsp_cross_with_registry(
            &arena, kShadowedNamespaceSource, (int)strlen(kShadowedNamespaceSource),
            names.caller_module_qn, false, jsx_mode, false, registry, defs, OVERLOAD_DEF_COUNT,
            import_names, import_qns, 1, NULL, &calls);
    }

    int rc = registry ? assert_no_shadowed_return_chains(&calls, language_name,
                                                         "parameter-shadows-namespace",
                                                         "lsp_ts_namespace")
                      : 1;
    if (!registry)
        printf("  ts-overload-return: %s namespace shadow registry build failed\n", language_name);
    cbm_arena_destroy(&arena);
    return rc;
}

TEST(repro_ts_free_function_overload_returns_typescript) {
    return assert_free_function_overload_returns(CBM_LANG_TYPESCRIPT, false, "TypeScript");
}

TEST(repro_ts_free_function_overload_returns_tsx) {
    return assert_free_function_overload_returns(CBM_LANG_TSX, true, "TSX");
}

TEST(repro_ts_namespace_overload_returns_typescript) {
    return assert_namespace_overload_returns(CBM_LANG_TYPESCRIPT, false, "TypeScript");
}

TEST(repro_ts_namespace_overload_returns_tsx) {
    return assert_namespace_overload_returns(CBM_LANG_TSX, true, "TSX");
}

TEST(repro_ts_exact_symbol_import_overload_returns_typescript) {
    return assert_exact_symbol_import_overload_returns(CBM_LANG_TYPESCRIPT, false, "TypeScript");
}

TEST(repro_ts_exact_symbol_import_overload_returns_tsx) {
    return assert_exact_symbol_import_overload_returns(CBM_LANG_TSX, true, "TSX");
}

TEST(repro_ts_contextual_callback_uses_selected_overload_typescript) {
    return assert_contextual_callback_uses_selected_overload(CBM_LANG_TYPESCRIPT, false,
                                                             "TypeScript");
}

TEST(repro_ts_contextual_callback_uses_selected_overload_tsx) {
    return assert_contextual_callback_uses_selected_overload(CBM_LANG_TSX, true, "TSX");
}

TEST(repro_ts_parameter_shadows_free_function_typescript) {
    return assert_parameter_shadows_free_function(CBM_LANG_TYPESCRIPT, false, "TypeScript");
}

TEST(repro_ts_parameter_shadows_free_function_tsx) {
    return assert_parameter_shadows_free_function(CBM_LANG_TSX, true, "TSX");
}

TEST(repro_ts_parameter_shadows_namespace_typescript) {
    return assert_parameter_shadows_namespace(CBM_LANG_TYPESCRIPT, false, "TypeScript");
}

TEST(repro_ts_parameter_shadows_namespace_tsx) {
    return assert_parameter_shadows_namespace(CBM_LANG_TSX, true, "TSX");
}

SUITE(repro_ts_overload_return_chains) {
    RUN_TEST(repro_ts_free_function_overload_returns_typescript);
    RUN_TEST(repro_ts_free_function_overload_returns_tsx);
    RUN_TEST(repro_ts_namespace_overload_returns_typescript);
    RUN_TEST(repro_ts_namespace_overload_returns_tsx);
    RUN_TEST(repro_ts_exact_symbol_import_overload_returns_typescript);
    RUN_TEST(repro_ts_exact_symbol_import_overload_returns_tsx);
    RUN_TEST(repro_ts_contextual_callback_uses_selected_overload_typescript);
    RUN_TEST(repro_ts_contextual_callback_uses_selected_overload_tsx);
    RUN_TEST(repro_ts_parameter_shadows_free_function_typescript);
    RUN_TEST(repro_ts_parameter_shadows_free_function_tsx);
    RUN_TEST(repro_ts_parameter_shadows_namespace_typescript);
    RUN_TEST(repro_ts_parameter_shadows_namespace_tsx);
}
