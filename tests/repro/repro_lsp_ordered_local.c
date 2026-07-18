/*
 * repro_lsp_ordered_local.c — Behaviour-level REDs for positional signatures
 * after a language's local/post-extraction registry refinement.
 *
 * These cases deliberately assert a downstream call target, not merely the
 * extracted metadata.  A GREEN transition therefore means the ordered slots
 * survived far enough to change resolver selection/type inference.  Strategy
 * and target checks prevent a raw unique-name fallback from satisfying the
 * oracle accidentally.
 */

#include "test_framework.h"

#include "arena.h"
#include "cbm.h"
#include "lsp/java_lsp.h"
#include "lsp/ts_lsp.h"

#include <stdbool.h>
#include <stdio.h>
#include <string.h>

static bool ends_with(const char *text, const char *suffix) {
    if (!text || !suffix)
        return false;
    size_t text_len = strlen(text);
    size_t suffix_len = strlen(suffix);
    return suffix_len <= text_len && strcmp(text + text_len - suffix_len, suffix) == 0;
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

static bool has_resolved_suffix(const CBMResolvedCallArray *calls, const char *caller_sub,
                                const char *callee_suffix, const char *strategy) {
    if (!calls)
        return false;
    for (int i = 0; i < calls->count; i++) {
        const CBMResolvedCall *call = &calls->items[i];
        if (call->kind != CBM_RESOLVED_INVOCATION || call->confidence < 0.5f || !call->caller_qn ||
            !strstr(call->caller_qn, caller_sub) || !ends_with(call->callee_qn, callee_suffix) ||
            !call->strategy || strcmp(call->strategy, strategy) != 0) {
            continue;
        }
        return true;
    }
    return false;
}

static bool has_resolved_exact(const CBMResolvedCallArray *calls, const char *caller_sub,
                               const char *callee_qn, const char *strategy) {
    if (!calls)
        return false;
    for (int i = 0; i < calls->count; i++) {
        const CBMResolvedCall *call = &calls->items[i];
        if (call->kind == CBM_RESOLVED_INVOCATION && call->confidence >= 0.5f && call->caller_qn &&
            strstr(call->caller_qn, caller_sub) && call->callee_qn &&
            strcmp(call->callee_qn, callee_qn) == 0 && call->strategy &&
            strcmp(call->strategy, strategy) == 0) {
            return true;
        }
    }
    return false;
}

static int assert_local_chained_target(CBMLanguage language, const char *rel_path,
                                       const char *source, const char *callee_suffix,
                                       const char *strategy) {
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), language,
                                             "repro_ordered_local", rel_path, 0, NULL, NULL);
    if (!result) {
        printf("  ordered-local: extraction returned NULL for %s\n", rel_path);
        return 1;
    }
    if (result->has_error) {
        printf("  ordered-local: fixture parse failed for %s\n", rel_path);
        cbm_free_result(result);
        return 1;
    }

    bool found = has_resolved_suffix(&result->resolved_calls, "run", callee_suffix, strategy);
    if (!found) {
        printf("  ordered-local: missing run -> *%s via %s for %s (have %d)\n", callee_suffix,
               strategy, rel_path, result->resolved_calls.count);
        print_resolved_calls(&result->resolved_calls);
    }
    cbm_free_result(result);
    return found ? 0 : 1;
}

/* Go's legacy type-usage projection stores [T,U] for `(first, second T,
 * last U)`.  Correct implicit inference requires the ordered [T,T,U] carrier,
 * including preservation through the AST generic-refinement pass. */
static const char kGoGroupedGeneric[] =
    "package main\n"
    "type Left struct{}\n"
    "type Right struct{}\n"
    "func (Right) RightOnly() {}\n"
    "func PickLast[T any, U any](first, second T, last U) U { return last }\n"
    "func run(left Left, right Right) {\n"
    "    got := PickLast(left, left, right)\n"
    "    got.RightOnly()\n"
    "}\n";

TEST(repro_lsp_ordered_local_go_grouped_generic) {
    /* Go method nodes intentionally use a flat `module.Method` graph QN;
     * receiver ownership is carried separately by receiver_type/parent_class.
     * Only Right defines RightOnly, and lsp_type_dispatch therefore proves the
     * positional generic result was inferred as Right. */
    return assert_local_chained_target(CBM_LANG_GO, "main.go", kGoGroupedGeneric, "RightOnly",
                                       "lsp_type_dispatch");
}

/* A C++ reference return wraps the function declarator in a
 * reference_declarator.  The local signature extractor must still reach the
 * nested parameter list; otherwise `operator[](int)` collapses to arity zero
 * and the exact-arity operator resolver correctly refuses it. */
static const char kCppReferenceReturnOperator[] =
    "struct Item {};\n"
    "struct Vec {\n"
    "    Item& operator[](int index) { static Item item; return item; }\n"
    "};\n"
    "void run(Vec& values) { values[0]; }\n";

static int assert_reference_return_operator_arity(CBMLanguage language, const char *rel_path,
                                                  const char *language_name) {
    CBMFileResult *result =
        cbm_extract_file(kCppReferenceReturnOperator, (int)strlen(kCppReferenceReturnOperator),
                         language, "repro_ordered_local", rel_path, 0, NULL, NULL);
    if (!result || result->has_error) {
        printf("  ordered-local: %s reference-return fixture extraction failed\n", language_name);
        if (result)
            cbm_free_result(result);
        return 1;
    }

    bool counted_signature = false;
    for (int i = 0; i < result->defs.count; i++) {
        const CBMDefinition *definition = &result->defs.items[i];
        if (definition->name && strcmp(definition->name, "operator[]") == 0 &&
            definition->signature_param_count == 1 && definition->signature_param_types &&
            definition->signature_param_types[0] &&
            strcmp(definition->signature_param_types[0], "int") == 0) {
            counted_signature = true;
            break;
        }
    }
    bool resolved =
        has_resolved_suffix(&result->resolved_calls, "run", "operator[]", "lsp_operator");
    if (!counted_signature || !resolved) {
        printf("  ordered-local: %s reference-return operator counted=%d resolved=%d\n",
               language_name, counted_signature, resolved);
        print_resolved_calls(&result->resolved_calls);
    }
    cbm_free_result(result);
    return counted_signature && resolved ? 0 : 1;
}

TEST(repro_lsp_ordered_local_cpp_reference_return_operator_arity) {
    return assert_reference_return_operator_arity(CBM_LANG_CPP, "main.cpp", "C++");
}

/* CUDA uses its own vendored grammar.  It aliases this syntax to the same
 * reference_declarator node shape today, but must retain an independent guard
 * so a future grammar update cannot silently reintroduce the arity collapse. */
TEST(repro_lsp_ordered_local_cuda_reference_return_operator_arity) {
    return assert_reference_return_operator_arity(CBM_LANG_CUDA, "main.cu", "CUDA");
}

/* The shared C-family name resolver permits eight declarator wrappers, but the
 * parameter walk historically stopped after five.  A five-level pointer return
 * therefore produced a named function whose signature silently had arity zero.
 * C, C++, and CUDA each get a real grammar guard; GLSL has no pointer syntax. */
static const char kDeepPointerReturn[] = "int *****deep(int value) { return 0; }\n";

static int assert_deep_pointer_return_arity(CBMLanguage language, const char *rel_path,
                                            const char *language_name) {
    CBMFileResult *result =
        cbm_extract_file(kDeepPointerReturn, (int)strlen(kDeepPointerReturn), language,
                         "repro_ordered_local", rel_path, 0, NULL, NULL);
    if (!result || result->has_error) {
        printf("  ordered-local: %s deep-pointer fixture extraction failed\n", language_name);
        if (result)
            cbm_free_result(result);
        return 1;
    }

    bool found = false;
    for (int i = 0; i < result->defs.count; i++) {
        const CBMDefinition *definition = &result->defs.items[i];
        if (definition->name && strcmp(definition->name, "deep") == 0 &&
            definition->signature_param_count == 1 && definition->signature_param_types &&
            definition->signature_param_types[0] &&
            strcmp(definition->signature_param_types[0], "int") == 0) {
            found = true;
            break;
        }
    }
    if (!found) {
        printf("  ordered-local: %s deep-pointer return lost parameter arity\n", language_name);
    }
    cbm_free_result(result);
    return found ? 0 : 1;
}

TEST(repro_lsp_ordered_local_c_deep_pointer_return_arity) {
    return assert_deep_pointer_return_arity(CBM_LANG_C, "main.c", "C");
}

TEST(repro_lsp_ordered_local_cpp_deep_pointer_return_arity) {
    return assert_deep_pointer_return_arity(CBM_LANG_CPP, "main.cpp", "C++");
}

TEST(repro_lsp_ordered_local_cuda_deep_pointer_return_arity) {
    return assert_deep_pointer_return_arity(CBM_LANG_CUDA, "main.cu", "CUDA");
}

/* GREEN controls for the local TypeScript AST-rebuild path.  The pseudo `this`
 * parameter is not an argument and is excluded from the ordered carrier.  The
 * AST rebuild also skips it because it is not an identifier pattern, then
 * reconstructs [T,T,U], so these pass even before ordered-carrier transport is
 * adopted.  They verify post-AST positional behavior, not transport itself. */
static const char kTsThisGeneric[] =
    "class Left {}\n"
    "class Right { rightOnly(): void {} }\n"
    "function pickLast<T, U>(this: void, first: T, second: T, last: U): U {\n"
    "    return last;\n"
    "}\n"
    "function run(left: Left, right: Right): void {\n"
    "    const got = pickLast(left, left, right);\n"
    "    got.rightOnly();\n"
    "}\n";

TEST(repro_lsp_ordered_local_control_typescript_this_receiver) {
    return assert_local_chained_target(CBM_LANG_TYPESCRIPT, "main.ts", kTsThisGeneric,
                                       "Right.rightOnly", "lsp_ts_method");
}

TEST(repro_lsp_ordered_local_control_tsx_this_receiver) {
    return assert_local_chained_target(CBM_LANG_TSX, "main.tsx", kTsThisGeneric, "Right.rightOnly",
                                       "lsp_ts_method");
}

/* Interface method signatures are synthesized by the TS AST shape sweep rather
 * than supplied as graph defs.  Put the same two parameter types in the wrong
 * order first, then require the reversed two-argument overload plus a non-first
 * zero-argument overload.  Arity alone cannot pass this: the selected edge and
 * return-chain type must agree on ordered parameter types, and the post-sweep
 * overloads must remain visible through a finalized registry. */
static const char kTsInterfaceOverloadReturnChains[] =
    "interface Reversed { reversedOnly(): void; }\n"
    "interface Empty { emptyOnly(): void; }\n"
    "interface Pair { pairOnly(): void; }\n"
    "interface Factory {\n"
    "  pick(s: string, n: number): Reversed;\n"
    "  pick(): Empty;\n"
    "  pick(n: number, s: string): Pair;\n"
    "}\n"
    "function run(f: Factory): void {\n"
    "  f.pick().emptyOnly();\n"
    "  f.pick(1, 'x').pairOnly();\n"
    "}\n";

static int assert_ts_interface_overload_return_chains(CBMLanguage language, bool jsx_mode,
                                                      const char *language_name) {
    static const char *const type_names[] = {"Reversed", "Empty", "Pair", "Factory"};
    CBMLSPDef defs[5];
    memset(defs, 0, sizeof(defs));
    for (int i = 0; i < 4; i++) {
        defs[i].qualified_name = type_names[i];
        defs[i].short_name = type_names[i];
        defs[i].label = "Interface";
        defs[i].def_module_qn = "repro.ts";
        defs[i].is_interface = true;
        defs[i].lang = language;
    }
    defs[0].qualified_name = "repro.ts.Reversed";
    defs[1].qualified_name = "repro.ts.Empty";
    defs[2].qualified_name = "repro.ts.Pair";
    defs[3].qualified_name = "repro.ts.Factory";
    defs[4].qualified_name = "repro.ts.run";
    defs[4].short_name = "run";
    defs[4].label = "Function";
    defs[4].def_module_qn = "repro.ts";
    defs[4].return_types = "void";
    defs[4].lang = language;

    CBMArena arena;
    cbm_arena_init(&arena);
    CBMTypeRegistry *registry = cbm_ts_build_cross_registry(&arena, defs, 5);
    CBMResolvedCallArray calls = {0};
    if (registry) {
        cbm_run_ts_lsp_cross_with_registry(
            &arena, kTsInterfaceOverloadReturnChains, (int)strlen(kTsInterfaceOverloadReturnChains),
            "repro.ts", false, jsx_mode, false, registry, defs, 5, NULL, NULL, 0, NULL, &calls);
    }

    bool empty = has_resolved_suffix(&calls, "run", "Empty.emptyOnly", "lsp_ts_method");
    bool pair = has_resolved_suffix(&calls, "run", "Pair.pairOnly", "lsp_ts_method");
    if (!registry || !empty || !pair) {
        printf("  ordered-local: %s interface overload chains empty=%d pair=%d registry=%d\n",
               language_name, empty, pair, registry != NULL);
        print_resolved_calls(&calls);
    }
    cbm_arena_destroy(&arena);
    return registry && empty && pair ? 0 : 1;
}

TEST(repro_lsp_ordered_local_typescript_interface_overload_return_chains) {
    return assert_ts_interface_overload_return_chains(CBM_LANG_TYPESCRIPT, false, "TypeScript");
}

TEST(repro_lsp_ordered_local_tsx_interface_overload_return_chains) {
    return assert_ts_interface_overload_return_chains(CBM_LANG_TSX, true, "TSX");
}

/* Java overloads normally share one graph QN, which hides which overload won.
 * The synthetic suffixes below make the registry choice observable while both
 * entries retain the source-level short name `pick`.  With both signatures
 * collapsed to zero slots, the three-argument call falls back to the first
 * registration ($zero); materializing its counted carrier must select $three. */
TEST(repro_lsp_ordered_local_java_overload_arity) {
    static const char source[] =
        "package demo;\n"
        "public class Caller {\n"
        "  public void callZero(demo.Target t) { t.pick(); }\n"
        "  public void callThree(demo.Target t) { t.pick(1, \"x\", true); }\n"
        "}\n";
    const char *three_params[] = {"int", "java.lang.String", "boolean", NULL};
    CBMLSPDef defs[3];
    memset(defs, 0, sizeof(defs));

    defs[0].qualified_name = "demo.Target";
    defs[0].short_name = "Target";
    defs[0].label = "Class";
    defs[0].def_module_qn = "demo";
    defs[0].lang = CBM_LANG_JAVA;

    defs[1].qualified_name = "demo.Target.pick$zero";
    defs[1].short_name = "pick";
    defs[1].label = "Method";
    defs[1].receiver_type = "demo.Target";
    defs[1].def_module_qn = "demo";
    defs[1].return_types = "void";
    defs[1].signature_param_types = NULL;
    defs[1].signature_param_count = 0;
    defs[1].lang = CBM_LANG_JAVA;

    defs[2].qualified_name = "demo.Target.pick$three";
    defs[2].short_name = "pick";
    defs[2].label = "Method";
    defs[2].receiver_type = "demo.Target";
    defs[2].def_module_qn = "demo";
    defs[2].return_types = "void";
    defs[2].signature_param_types = three_params;
    defs[2].signature_param_count = 3;
    defs[2].lang = CBM_LANG_JAVA;

    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray calls = {0};
    cbm_run_java_lsp_cross(&arena, source, (int)strlen(source), "demo", defs, 3, NULL, NULL, 0,
                           NULL, &calls);

    bool zero =
        has_resolved_exact(&calls, "callZero", "demo.Target.pick$zero", "lsp_type_dispatch");
    bool three =
        has_resolved_exact(&calls, "callThree", "demo.Target.pick$three", "lsp_type_dispatch");
    bool wrong =
        has_resolved_exact(&calls, "callThree", "demo.Target.pick$zero", "lsp_type_dispatch");
    if (!zero || !three || wrong) {
        printf("  ordered-local: Java overload selection zero=%d three=%d wrong-zero=%d\n", zero,
               three, wrong);
        print_resolved_calls(&calls);
    }
    cbm_arena_destroy(&arena);
    return zero && three && !wrong ? 0 : 1;
}

SUITE(repro_lsp_ordered_local) {
    RUN_TEST(repro_lsp_ordered_local_go_grouped_generic);
    RUN_TEST(repro_lsp_ordered_local_cpp_reference_return_operator_arity);
    RUN_TEST(repro_lsp_ordered_local_cuda_reference_return_operator_arity);
    RUN_TEST(repro_lsp_ordered_local_c_deep_pointer_return_arity);
    RUN_TEST(repro_lsp_ordered_local_cpp_deep_pointer_return_arity);
    RUN_TEST(repro_lsp_ordered_local_cuda_deep_pointer_return_arity);
    RUN_TEST(repro_lsp_ordered_local_control_typescript_this_receiver);
    RUN_TEST(repro_lsp_ordered_local_control_tsx_this_receiver);
    RUN_TEST(repro_lsp_ordered_local_typescript_interface_overload_return_chains);
    RUN_TEST(repro_lsp_ordered_local_tsx_interface_overload_return_chains);
    RUN_TEST(repro_lsp_ordered_local_java_overload_arity);
}
