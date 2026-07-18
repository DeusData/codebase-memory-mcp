/*
 * Exhaustive call-argument reproduction matrix, CBMLanguage 80..162.
 *
 * Every language in this numeric range whose live spec has call-node metadata
 * owns one TEST row below. Semantic applications are exercised twice: once as
 * a direct call argument and once as the same bare reference outside a call.
 * This makes the call-scope suppression defect independent from reference-node
 * vocabulary gaps. DSL and document grammars whose call metadata represents a
 * command, dependency, directive, or other non-callback construct instead use
 * an explicit scope-safety/non-applicability control. ObjectScript Studio
 * Export is intentionally absent: it is a transform-only input that is
 * transcoded to ObjectScript UDL before grammar extraction.
 */
#include "test_framework.h"
#include "cbm.h"
#include "lang_specs.h"

#include <ctype.h>
#include <stdio.h>
#include <string.h>

size_t repro_call_argument_matrix_b_copy_language_ids(CBMLanguage *language_ids, size_t capacity);

typedef struct {
    const char *tag;
    CBMLanguage language;
    const char *filename;
    const char *reason;
} CaseIdentity;

typedef struct {
    CaseIdentity identity;
    const char *inside_source;
    const char *bare_source;
    const char *call_node_kind;
    const char *caller;
    const char *callee;
    const char *argument;
    int callable_definition_count;
    int inside_total_calls;
    int bare_total_calls;
    int expect_lexical_local_shadow; /* routine parameters; never module/DSL rows */
} RoutineArgumentCase;

typedef struct {
    CaseIdentity identity;
    const char *inside_source;
    const char *bare_source;
    const char *call_node_kind;
    const char *callee;
    const char *argument;
    int inside_total_calls;
    int bare_total_calls;
} ModuleArgumentCase;

static int terminal_name_matches(const char *raw, const char *expected) {
    if (!raw || !expected)
        return 0;
    if (strcmp(raw, expected) == 0)
        return 1;

    size_t raw_len = strlen(raw);
    size_t expected_len = strlen(expected);
    if (raw_len < expected_len)
        return 0;
    const char *tail = raw + raw_len - expected_len;
    if (strcmp(tail, expected) != 0)
        return 0;
    if (tail == raw)
        return 1;

    unsigned char boundary = (unsigned char)tail[-1];
    return !isalnum(boundary) && boundary != '_';
}

typedef enum {
    SCOPE_ROUTINE,
    SCOPE_EXACT,
} ScopeMode;

static int scope_matches(const char *actual, const char *expected, ScopeMode mode) {
    if (!actual || !expected)
        return 0;
    if (mode == SCOPE_EXACT)
        return strcmp(actual, expected) == 0;
    return terminal_name_matches(actual, expected);
}

static int usage_count_scoped(const CBMFileResult *result, const char *target, const char *scope,
                              ScopeMode mode) {
    int count = 0;
    for (int i = 0; i < result->usages.count; i++) {
        const CBMUsage *usage = &result->usages.items[i];
        if (terminal_name_matches(usage->ref_name, target) &&
            scope_matches(usage->enclosing_func_qn, scope, mode))
            count++;
    }
    return count;
}

static int usage_kind_count_scoped(const CBMFileResult *result, const char *target,
                                   const char *scope, ScopeMode mode, CBMUsageKind kind) {
    int count = 0;
    for (int i = 0; i < result->usages.count; i++) {
        const CBMUsage *usage = &result->usages.items[i];
        if (usage->kind == kind && terminal_name_matches(usage->ref_name, target) &&
            scope_matches(usage->enclosing_func_qn, scope, mode)) {
            count++;
        }
    }
    return count;
}

static int local_shadow_usage_count_scoped(const CBMFileResult *result, const char *target,
                                           const char *scope, ScopeMode mode) {
    int count = 0;
    for (int i = 0; i < result->usages.count; i++) {
        const CBMUsage *usage = &result->usages.items[i];
        if (usage->kind == CBM_USAGE_VALUE && usage->semantic_reference_blocked &&
            usage->semantic_reference_local_shadow &&
            terminal_name_matches(usage->ref_name, target) &&
            scope_matches(usage->enclosing_func_qn, scope, mode)) {
            count++;
        }
    }
    return count;
}

static int call_count_scoped(const CBMFileResult *result, const char *target, const char *scope,
                             ScopeMode mode) {
    int count = 0;
    for (int i = 0; i < result->calls.count; i++) {
        const CBMCall *call = &result->calls.items[i];
        if (terminal_name_matches(call->callee_name, target) &&
            scope_matches(call->enclosing_func_qn, scope, mode))
            count++;
    }
    return count;
}

static int usage_within_call_count_scoped(const CBMFileResult *result, const char *target,
                                          const char *scope, ScopeMode mode, const char *callee) {
    int count = 0;
    for (int u = 0; u < result->usages.count; u++) {
        const CBMUsage *usage = &result->usages.items[u];
        if (!terminal_name_matches(usage->ref_name, target) ||
            !scope_matches(usage->enclosing_func_qn, scope, mode) ||
            usage->site_end_byte <= usage->site_start_byte) {
            continue;
        }
        for (int c = 0; c < result->calls.count; c++) {
            const CBMCall *call = &result->calls.items[c];
            if (!terminal_name_matches(call->callee_name, callee) ||
                !scope_matches(call->enclosing_func_qn, scope, mode) ||
                call->source_origin != usage->source_origin ||
                call->site_end_byte <= call->site_start_byte) {
                continue;
            }
            if (usage->site_start_byte >= call->site_start_byte &&
                usage->site_end_byte <= call->site_end_byte) {
                count++;
            }
        }
    }
    return count;
}

static int callable_definition_count(const CBMFileResult *result, const char *target) {
    int count = 0;
    for (int i = 0; i < result->defs.count; i++) {
        const CBMDefinition *definition = &result->defs.items[i];
        int callable_label = definition->label && (strcmp(definition->label, "Function") == 0 ||
                                                   strcmp(definition->label, "Method") == 0);
        if (callable_label && terminal_name_matches(definition->name, target) &&
            terminal_name_matches(definition->qualified_name, target))
            count++;
    }
    return count;
}

static const char *checked_callable_qn(const CaseIdentity *identity, const char *variant,
                                       const CBMFileResult *result, const char *target,
                                       int *failures) {
    const char *qualified_name = NULL;
    int count = 0;
    for (int i = 0; i < result->defs.count; i++) {
        const CBMDefinition *definition = &result->defs.items[i];
        int callable_label = definition->label && (strcmp(definition->label, "Function") == 0 ||
                                                   strcmp(definition->label, "Method") == 0);
        if (!callable_label || !terminal_name_matches(definition->name, target) ||
            !terminal_name_matches(definition->qualified_name, target))
            continue;
        qualified_name = definition->qualified_name;
        count++;
    }
    if (count == 1 && qualified_name && qualified_name[0])
        return qualified_name;
    fprintf(stderr,
            "  [call-argument-matrix-b] lang=%s variant=%s "
            "invariant=callable_qualified_name expected=1 actual=%d reason=%s\n",
            identity->tag, variant, count, identity->reason);
    (*failures)++;
    return "<missing-callable-qn>";
}

static void check_exact(const CaseIdentity *identity, const char *variant, const char *invariant,
                        int actual, int expected, int *failures) {
    if (actual == expected)
        return;
    fprintf(stderr,
            "  [call-argument-matrix-b] lang=%s variant=%s invariant=%s expected=%d actual=%d "
            "reason=%s\n",
            identity->tag, variant, invariant, expected, actual, identity->reason);
    (*failures)++;
}

static int ast_node_kind_count(TSNode node, const char *node_kind) {
    int count = strcmp(ts_node_type(node), node_kind) == 0 ? 1 : 0;
    uint32_t child_count = ts_node_child_count(node);
    for (uint32_t i = 0; i < child_count; i++)
        count += ast_node_kind_count(ts_node_child(node, i), node_kind);
    return count;
}

static void check_ast_kind_present(const CaseIdentity *identity, const char *variant,
                                   const char *source, const char *node_kind, int *failures) {
    const TSLanguage *language = cbm_ts_language(identity->language);
    TSParser *parser = ts_parser_new();
    if (!language || !parser || !ts_parser_set_language(parser, language)) {
        fprintf(stderr,
                "  [call-argument-matrix-b] lang=%s variant=%s invariant=ast_parser_ready "
                "expected=1 actual=0 reason=%s\n",
                identity->tag, variant, identity->reason);
        (*failures)++;
        if (parser)
            ts_parser_delete(parser);
        return;
    }

    TSTree *tree = ts_parser_parse_string(parser, NULL, source, (uint32_t)strlen(source));
    TSNode root = tree ? ts_tree_root_node(tree) : (TSNode){0};
    if (tree && ts_node_has_error(root)) {
        fprintf(stderr,
                "  [call-argument-matrix-b] lang=%s variant=%s invariant=ast_clean_parse "
                "expected=1 actual=0 reason=%s\n",
                identity->tag, variant, identity->reason);
        (*failures)++;
    }
    int count = tree ? ast_node_kind_count(root, node_kind) : 0;
    if (count == 0) {
        fprintf(stderr,
                "  [call-argument-matrix-b] lang=%s variant=%s invariant=ast_kind_present "
                "kind=%s expected=>0 actual=0 reason=%s\n",
                identity->tag, variant, node_kind, identity->reason);
        (*failures)++;
    }
    if (tree)
        ts_tree_delete(tree);
    ts_parser_delete(parser);
}

static CBMFileResult *extract_source(const CaseIdentity *identity, const char *variant,
                                     const char *source, int *failures) {
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), identity->language,
                                             "repro", identity->filename, 0, NULL, NULL);
    if (!result) {
        fprintf(stderr,
                "  [call-argument-matrix-b] lang=%s variant=%s invariant=extract_result "
                "expected=non-null reason=%s\n",
                identity->tag, variant, identity->reason);
        (*failures)++;
        return NULL;
    }
    if (result->has_error || result->parse_incomplete) {
        fprintf(stderr,
                "  [call-argument-matrix-b] lang=%s variant=%s invariant=valid_fixture "
                "expected=clean reason=%s\n",
                identity->tag, variant, identity->reason);
        (*failures)++;
    }
    return result;
}

static const char *checked_module_qn(const CaseIdentity *identity, const char *variant,
                                     const CBMFileResult *result, int *failures) {
    if (result->module_qn && result->module_qn[0])
        return result->module_qn;
    fprintf(stderr,
            "  [call-argument-matrix-b] lang=%s variant=%s invariant=module_qn "
            "expected=non-empty reason=%s\n",
            identity->tag, variant, identity->reason);
    (*failures)++;
    return "<missing-module-qn>";
}

static int run_routine_argument_case(const RoutineArgumentCase *test_case) {
    int failures = 0;
    check_ast_kind_present(&test_case->identity, "inside-call", test_case->inside_source,
                           test_case->call_node_kind, &failures);
    CBMFileResult *inside =
        extract_source(&test_case->identity, "inside-call", test_case->inside_source, &failures);
    if (inside) {
        check_exact(&test_case->identity, "inside-call", "caller_callable_definition",
                    callable_definition_count(inside, test_case->caller),
                    test_case->callable_definition_count, &failures);
        check_exact(&test_case->identity, "inside-call", "direct_callee_call_in_caller",
                    call_count_scoped(inside, test_case->callee, test_case->caller, SCOPE_ROUTINE),
                    1, &failures);
        check_exact(
            &test_case->identity, "inside-call", "argument_usage_in_caller",
            usage_count_scoped(inside, test_case->argument, test_case->caller, SCOPE_ROUTINE), 1,
            &failures);
        check_exact(&test_case->identity, "inside-call",
                    "argument_site_owned_by_direct_callee_call",
                    usage_within_call_count_scoped(inside, test_case->argument, test_case->caller,
                                                   SCOPE_ROUTINE, test_case->callee),
                    1, &failures);
        check_exact(&test_case->identity, "inside-call", "argument_is_ordinary_usage_in_caller",
                    usage_kind_count_scoped(inside, test_case->argument, test_case->caller,
                                            SCOPE_ROUTINE, CBM_USAGE_VALUE),
                    1, &failures);
        check_exact(&test_case->identity, "inside-call", "argument_not_call_reference_in_caller",
                    usage_kind_count_scoped(inside, test_case->argument, test_case->caller,
                                            SCOPE_ROUTINE, CBM_USAGE_CALL_REFERENCE),
                    0, &failures);
        if (test_case->expect_lexical_local_shadow) {
            check_exact(&test_case->identity, "inside-call",
                        "argument_blocks_callable_promotion_in_caller",
                        local_shadow_usage_count_scoped(inside, test_case->argument,
                                                        test_case->caller, SCOPE_ROUTINE),
                        1, &failures);
        }
        check_exact(&test_case->identity, "inside-call", "callee_not_usage_in_caller",
                    usage_count_scoped(inside, test_case->callee, test_case->caller, SCOPE_ROUTINE),
                    0, &failures);
        check_exact(
            &test_case->identity, "inside-call", "argument_not_call_in_caller",
            call_count_scoped(inside, test_case->argument, test_case->caller, SCOPE_ROUTINE), 0,
            &failures);
        check_exact(&test_case->identity, "inside-call", "total_calls", inside->calls.count,
                    test_case->inside_total_calls, &failures);
        cbm_free_result(inside);
    }

    CBMFileResult *bare =
        extract_source(&test_case->identity, "bare-reference", test_case->bare_source, &failures);
    if (bare) {
        check_exact(&test_case->identity, "bare-reference", "caller_callable_definition",
                    callable_definition_count(bare, test_case->caller),
                    test_case->callable_definition_count, &failures);
        check_exact(&test_case->identity, "bare-reference", "bare_usage_in_caller",
                    usage_count_scoped(bare, test_case->argument, test_case->caller, SCOPE_ROUTINE),
                    1, &failures);
        check_exact(&test_case->identity, "bare-reference", "bare_is_ordinary_usage_in_caller",
                    usage_kind_count_scoped(bare, test_case->argument, test_case->caller,
                                            SCOPE_ROUTINE, CBM_USAGE_VALUE),
                    1, &failures);
        check_exact(&test_case->identity, "bare-reference", "bare_not_call_reference_in_caller",
                    usage_kind_count_scoped(bare, test_case->argument, test_case->caller,
                                            SCOPE_ROUTINE, CBM_USAGE_CALL_REFERENCE),
                    0, &failures);
        check_exact(&test_case->identity, "bare-reference", "bare_reference_not_call_in_caller",
                    call_count_scoped(bare, test_case->argument, test_case->caller, SCOPE_ROUTINE),
                    0, &failures);
        check_exact(&test_case->identity, "bare-reference", "total_calls", bare->calls.count,
                    test_case->bare_total_calls, &failures);
        cbm_free_result(bare);
    }

    return failures == 0 ? 0 : 1;
}

static int run_module_argument_case(const ModuleArgumentCase *test_case) {
    int failures = 0;
    check_ast_kind_present(&test_case->identity, "inside-call", test_case->inside_source,
                           test_case->call_node_kind, &failures);
    CBMFileResult *inside =
        extract_source(&test_case->identity, "inside-call", test_case->inside_source, &failures);
    if (inside) {
        const char *module_qn =
            checked_module_qn(&test_case->identity, "inside-call", inside, &failures);
        check_exact(&test_case->identity, "inside-call", "direct_callee_call_in_module",
                    call_count_scoped(inside, test_case->callee, module_qn, SCOPE_EXACT), 1,
                    &failures);
        check_exact(&test_case->identity, "inside-call", "argument_usage_in_module",
                    usage_count_scoped(inside, test_case->argument, module_qn, SCOPE_EXACT), 1,
                    &failures);
        check_exact(&test_case->identity, "inside-call", "argument_is_ordinary_usage_in_module",
                    usage_kind_count_scoped(inside, test_case->argument, module_qn, SCOPE_EXACT,
                                            CBM_USAGE_VALUE),
                    1, &failures);
        check_exact(&test_case->identity, "inside-call", "argument_not_call_reference_in_module",
                    usage_kind_count_scoped(inside, test_case->argument, module_qn, SCOPE_EXACT,
                                            CBM_USAGE_CALL_REFERENCE),
                    0, &failures);
        check_exact(&test_case->identity, "inside-call", "callee_not_usage_in_module",
                    usage_count_scoped(inside, test_case->callee, module_qn, SCOPE_EXACT), 0,
                    &failures);
        check_exact(&test_case->identity, "inside-call", "argument_not_call_in_module",
                    call_count_scoped(inside, test_case->argument, module_qn, SCOPE_EXACT), 0,
                    &failures);
        check_exact(&test_case->identity, "inside-call", "total_calls", inside->calls.count,
                    test_case->inside_total_calls, &failures);
        cbm_free_result(inside);
    }

    CBMFileResult *bare =
        extract_source(&test_case->identity, "bare-reference", test_case->bare_source, &failures);
    if (bare) {
        const char *module_qn =
            checked_module_qn(&test_case->identity, "bare-reference", bare, &failures);
        check_exact(&test_case->identity, "bare-reference", "bare_usage_in_module",
                    usage_count_scoped(bare, test_case->argument, module_qn, SCOPE_EXACT), 1,
                    &failures);
        check_exact(&test_case->identity, "bare-reference", "bare_is_ordinary_usage_in_module",
                    usage_kind_count_scoped(bare, test_case->argument, module_qn, SCOPE_EXACT,
                                            CBM_USAGE_VALUE),
                    1, &failures);
        check_exact(&test_case->identity, "bare-reference", "bare_not_call_reference_in_module",
                    usage_kind_count_scoped(bare, test_case->argument, module_qn, SCOPE_EXACT,
                                            CBM_USAGE_CALL_REFERENCE),
                    0, &failures);
        check_exact(&test_case->identity, "bare-reference", "bare_reference_not_call_in_module",
                    call_count_scoped(bare, test_case->argument, module_qn, SCOPE_EXACT), 0,
                    &failures);
        check_exact(&test_case->identity, "bare-reference", "total_calls", bare->calls.count,
                    test_case->bare_total_calls, &failures);
        cbm_free_result(bare);
    }

    return failures == 0 ? 0 : 1;
}

/* Synthetic Lisp application; Racket's reference nodes are not yet in the
 * usage vocabulary, so both sources are independently RED. */
static const char RACKET_INSIDE[] = "#lang racket\n"
                                    "(define (accept value) value)\n"
                                    "(define (run watched) (accept watched))\n";
static const char RACKET_BARE[] = "#lang racket\n"
                                  "(define (run watched) watched)\n";

static const char ODIN_INSIDE[] = "package sample\n"
                                  "accept :: proc(value: int) -> int { return value }\n"
                                  "run :: proc(watched: int) -> int { return accept(watched) }\n";
static const char ODIN_BARE[] = "package sample\n"
                                "run :: proc(watched: int) -> int { return watched }\n";

static const char RESCRIPT_INSIDE[] = "let accept = value => value\n"
                                      "let run = watched => accept(watched)\n";
static const char RESCRIPT_BARE[] = "let run = watched => watched\n";

static const char PURESCRIPT_INSIDE[] = "module Main where\n"
                                        "accept value = value\n"
                                        "run watched = accept watched\n";
static const char PURESCRIPT_BARE[] = "module Main where\n"
                                      "run watched = watched\n";

static const char NICKEL_INSIDE[] = "let accept = fun value => value in\n"
                                    "let run = fun watched => accept watched in\n"
                                    "run\n";
static const char NICKEL_BARE[] = "let run = fun watched => watched in\n"
                                  "run\n";

static const char CRYSTAL_INSIDE[] = "def accept(value)\n"
                                     "  value\n"
                                     "end\n"
                                     "def run(watched)\n"
                                     "  accept(watched)\n"
                                     "end\n";
static const char CRYSTAL_BARE[] = "def run(watched)\n"
                                   "  watched\n"
                                   "end\n";

static const char LUA_TYPED_INSIDE[] = "local function accept(value: number): number\n"
                                       "    return value\n"
                                       "end\n"
                                       "local function run(watched: number): number\n"
                                       "    return accept(watched)\n"
                                       "end\n";
static const char LUA_TYPED_BARE[] = "local function run(watched: number): number\n"
                                     "    return watched\n"
                                     "end\n";

static const char HARE_INSIDE[] = "fn accept(value: int) int = {\n"
                                  "\treturn value;\n"
                                  "};\n"
                                  "fn run(watched: int) int = {\n"
                                  "\treturn accept(watched);\n"
                                  "};\n";
static const char HARE_BARE[] = "fn run(watched: int) int = {\n"
                                "\treturn watched;\n"
                                "};\n";

static const char PONY_INSIDE[] = "class Calculator\n"
                                  "  fun accept(value: I32): I32 => value\n"
                                  "  fun run(watched: I32): I32 => accept(watched)\n";
static const char PONY_BARE[] = "class Calculator\n"
                                "  fun run(watched: I32): I32 => watched\n";

static const char SWAY_INSIDE[] = "fn accept(value: u64) -> u64 {\n"
                                  "    value\n"
                                  "}\n"
                                  "fn run(watched: u64) -> u64 {\n"
                                  "    accept(watched)\n"
                                  "}\n";
static const char SWAY_BARE[] = "fn run(watched: u64) -> u64 {\n"
                                "    watched\n"
                                "}\n";

/* NASM actual_instruction is synthetic call metadata. A real call is valid;
 * mov must not be promoted to a call merely because it shares that boundary. */
static const char NASM_SCOPE[] = "section .text\n"
                                 "accept:\n"
                                 "    ret\n"
                                 "run:\n"
                                 "    mov rax, rbx\n"
                                 "    call accept\n"
                                 "    ret\n";
/* NASM function-like macros use the grammar's call_syntax_expression form. */
static const char NASM_CALL_SYNTAX[] = "%define accept(value) value\n"
                                       "dd accept(7)\n";

/* Just expressions support built-in function calls over identifier values. */
static const char JUST_INSIDE[] = "watched := \"value\"\n"
                                  "result := uppercase(watched)\n"
                                  "show:\n"
                                  "    @echo {{result}}\n";
static const char JUST_BARE[] = "watched := \"value\"\n"
                                "result := watched\n"
                                "show:\n"
                                "    @echo {{result}}\n";
static const char JUST_DEPENDENCY[] = "build:\n"
                                      "    @echo build\n"
                                      "test: build\n"
                                      "    @echo test\n";

/* Go-template function calls have pipeline values, not function-object
 * callback syntax. Keep the semantic printf call and reject a fabricated call
 * to the field argument. */
static const char GOTEMPLATE_INSIDE[] = "{{ printf \"%s\" .Watched }}\n";
static const char GOTEMPLATE_BARE[] = "{{ .Watched }}\n";

static const char TEMPL_INSIDE[] = "package sample\n"
                                   "func accept(value string) string {\n"
                                   "    return value\n"
                                   "}\n"
                                   "func run(watched string) string {\n"
                                   "    return accept(watched)\n"
                                   "}\n";
static const char TEMPL_BARE[] = "package sample\n"
                                 "func run(watched string) string {\n"
                                 "    return watched\n"
                                 "}\n";

/* Prisma call expressions are schema defaults, not higher-order application. */
static const char PRISMA_SCOPE[] = "model Event {\n"
                                   "  id Int @id\n"
                                   "  createdAt DateTime @default(now())\n"
                                   "}\n";

/* Diff command nodes are patch records and must never fabricate code calls. */
static const char DIFF_SCOPE[] = "diff --git a/main.c b/main.c\n"
                                 "index 1111111..2222222 100644\n"
                                 "--- a/main.c\n"
                                 "+++ b/main.c\n"
                                 "@@ -1 +1 @@\n"
                                 "-old\n"
                                 "+new\n";

static const char WGSL_INSIDE[] = "fn accept(value: f32) -> f32 {\n"
                                  "    return value;\n"
                                  "}\n"
                                  "fn run(watched: f32) -> f32 {\n"
                                  "    return accept(watched);\n"
                                  "}\n";
static const char WGSL_BARE[] = "fn run(watched: f32) -> f32 {\n"
                                "    return watched;\n"
                                "}\n";

static const char JSONNET_INSIDE[] = "local accept(value) = value;\n"
                                     "local run(watched) = accept(watched);\n"
                                     "run\n";
static const char JSONNET_BARE[] = "local run(watched) = watched;\n"
                                   "run\n";

/* BibTeX command nodes are document syntax and have no callable semantics. */
static const char BIBTEX_SCOPE[] = "@book{sample,\n"
                                   "  title = {Using \\LaTeX{} Today},\n"
                                   "  year = {2026}\n"
                                   "}\n";

static const char STARLARK_INSIDE[] = "def accept(value):\n"
                                      "    return value\n"
                                      "def run(watched):\n"
                                      "    return accept(watched)\n";
static const char STARLARK_BARE[] = "def run(watched):\n"
                                    "    return watched\n";

static const char BICEP_INSIDE[] = "func accept(value string) string => value\n"
                                   "func run(watched string) string => accept(watched)\n";
static const char BICEP_BARE[] = "func run(watched string) string => watched\n";

static const char HLSL_FAMILY_INSIDE[] = "float accept(float value) {\n"
                                         "    return value;\n"
                                         "}\n"
                                         "float run(float watched) {\n"
                                         "    return accept(watched);\n"
                                         "}\n";
static const char HLSL_FAMILY_BARE[] = "float run(float watched) {\n"
                                       "    return watched;\n"
                                       "}\n";

static const char VHDL_INSIDE[] = "package sample is\n"
                                  "  function accept(value : integer) return integer;\n"
                                  "  function run(watched : integer) return integer;\n"
                                  "end package;\n"
                                  "package body sample is\n"
                                  "  function accept(value : integer) return integer is\n"
                                  "  begin\n"
                                  "    return value;\n"
                                  "  end function;\n"
                                  "  function run(watched : integer) return integer is\n"
                                  "  begin\n"
                                  "    return accept(watched);\n"
                                  "  end function;\n"
                                  "end package body;\n";
static const char VHDL_BARE[] = "package sample is\n"
                                "  function run(watched : integer) return integer;\n"
                                "end package;\n"
                                "package body sample is\n"
                                "  function run(watched : integer) return integer is\n"
                                "  begin\n"
                                "    return watched;\n"
                                "  end function;\n"
                                "end package body;\n";

static const char SYSTEMVERILOG_INSIDE[] = "module sample;\n"
                                           "  function automatic int accept(int value);\n"
                                           "    return value;\n"
                                           "  endfunction\n"
                                           "  function automatic int run(int watched);\n"
                                           "    return accept(watched);\n"
                                           "  endfunction\n"
                                           "endmodule\n";
static const char SYSTEMVERILOG_BARE[] = "module sample;\n"
                                         "  function automatic int run(int watched);\n"
                                         "    return watched;\n"
                                         "  endfunction\n"
                                         "endmodule\n";

/* DeviceTree has call_expression metadata for macro-like constructs, but the
 * native property/reference form below has no function-object application. */
static const char DEVICETREE_SCOPE[] = "/dts-v1/;\n"
                                       "/ {\n"
                                       "  sample {\n"
                                       "    clock-frequency = <MHZ(24)>;\n"
                                       "  };\n"
                                       "};\n";

/* Linker-script built-ins do accept symbols. This is a vocabulary-gap test,
 * not a claim that linker scripts support callbacks. */
static const char LINKERSCRIPT_INSIDE[] = "SECTIONS {\n"
                                          "  . = ABSOLUTE(_start);\n"
                                          "}\n";
static const char LINKERSCRIPT_BARE[] = "SECTIONS {\n"
                                        "  . = _start;\n"
                                        "}\n";

/* GN is a build DSL, but its assert() call has a true value argument. */
static const char GN_INSIDE[] = "watched = true\n"
                                "assert(watched)\n";
static const char GN_BARE[] = "watched = true\n"
                              "copied = watched\n";

static const char BITBAKE_DOMAIN[] = "python do_run() {\n"
                                     "    bb.note('ready')\n"
                                     "}\n";

static const char CAIRO_INSIDE[] = "fn accept(value: felt252) -> felt252 {\n"
                                   "    value\n"
                                   "}\n"
                                   "fn run(watched: felt252) -> felt252 {\n"
                                   "    accept(watched)\n"
                                   "}\n";
static const char CAIRO_BARE[] = "fn run(watched: felt252) -> felt252 {\n"
                                 "    watched\n"
                                 "}\n";

static const char MOVE_INSIDE[] = "module 0x1::sample {\n"
                                  "    fun accept(value: u64) {\n"
                                  "    }\n"
                                  "    fun run(watched: u64) {\n"
                                  "        accept(watched);\n"
                                  "    }\n"
                                  "}\n";
static const char MOVE_BARE[] = "module 0x1::sample {\n"
                                "    fun run(watched: u64): u64 {\n"
                                "        return watched;\n"
                                "    }\n"
                                "}\n";

static const char SQUIRREL_INSIDE[] = "function accept(value) {\n"
                                      "    return value;\n"
                                      "}\n"
                                      "function run(watched) {\n"
                                      "    return accept(watched);\n"
                                      "}\n";
static const char SQUIRREL_BARE[] = "function run(watched) {\n"
                                    "    return watched;\n"
                                    "}\n";

static const char FUNC_INSIDE[] = "int accept(int value) {\n"
                                  "    return value;\n"
                                  "}\n"
                                  "int run(int watched) {\n"
                                  "    return accept(watched);\n"
                                  "}\n";
static const char FUNC_BARE[] = "int run(int watched) {\n"
                                "    return watched;\n"
                                "}\n";

static const char PUPPET_INSIDE[] = "function accept(String $value) >> String {\n"
                                    "  $value\n"
                                    "}\n"
                                    "function run(String $watched) >> String {\n"
                                    "  accept($watched)\n"
                                    "}\n";
static const char PUPPET_BARE[] = "function run(String $watched) >> String {\n"
                                  "  $watched\n"
                                  "}\n";

static const char LLVM_IR_INSIDE[] = "declare void @accept(ptr)\n"
                                     "define void @run(ptr %watched) {\n"
                                     "entry:\n"
                                     "  call void @accept(ptr %watched)\n"
                                     "  ret void\n"
                                     "}\n";
static const char LLVM_IR_BARE[] = "define ptr @run(ptr %watched) {\n"
                                   "entry:\n"
                                   "  ret ptr %watched\n"
                                   "}\n";

static const char TLAPLUS_INSIDE[] = "---- MODULE Sample ----\n"
                                     "Accept(value) == value\n"
                                     "Guard(values) == Accept(values)\n"
                                     "====\n";
static const char TLAPLUS_BARE[] = "---- MODULE Sample ----\n"
                                   "Guard(values) == values\n"
                                   "====\n";
static const char TLAPLUS_BOUNDED_QUANTIFICATION[] =
    "---- MODULE Bounded ----\n"
    "Guard(values) == \\A item \\in values : item = item\n"
    "====\n";

static const char APEX_INSIDE[] = "public class Sample {\n"
                                  "  private static Integer accept(Integer value) {\n"
                                  "    return value;\n"
                                  "  }\n"
                                  "  public static Integer run(Integer watched) {\n"
                                  "    return accept(watched);\n"
                                  "  }\n"
                                  "}\n";
static const char APEX_BARE[] = "public class Sample {\n"
                                "  public static Integer run(Integer watched) {\n"
                                "    return watched;\n"
                                "  }\n"
                                "}\n";

static const char PINE_INSIDE[] = "//@version=5\n"
                                  "indicator(\"Sample\")\n"
                                  "accept(value) => value\n"
                                  "run(watched) => accept(watched)\n";
static const char PINE_BARE[] = "//@version=5\n"
                                "indicator(\"Sample\")\n"
                                "run(watched) => watched\n";

static const char QML_INSIDE[] = "import QtQuick 2.15\n"
                                 "QtObject {\n"
                                 "  function accept(value) { return value; }\n"
                                 "  function run(watched) { return accept(watched); }\n"
                                 "}\n";
static const char QML_BARE[] = "import QtQuick 2.15\n"
                               "QtObject {\n"
                               "  function run(watched) { return watched; }\n"
                               "}\n";

static const char CFSCRIPT_INSIDE[] = "component {\n"
                                      "  function accept(value) { return value; }\n"
                                      "  function run(watched) { return accept(watched); }\n"
                                      "}\n";
static const char CFSCRIPT_BARE[] = "component {\n"
                                    "  function run(watched) { return watched; }\n"
                                    "}\n";

static const char CFML_INSIDE[] = "<cffunction name=\"accept\">\n"
                                  "  <cfargument name=\"value\">\n"
                                  "  <cfreturn arguments.value>\n"
                                  "</cffunction>\n"
                                  "<cffunction name=\"run\">\n"
                                  "  <cfargument name=\"watched\">\n"
                                  "  <cfreturn accept(arguments.watched)>\n"
                                  "</cffunction>\n";
static const char CFML_BARE[] = "<cffunction name=\"run\">\n"
                                "  <cfargument name=\"watched\">\n"
                                "  <cfreturn arguments.watched>\n"
                                "</cffunction>\n";

/* New focused Mojo fixture: native fn declarations and a value argument. */
static const char MOJO_INSIDE[] = "fn accept(value: Int) -> Int:\n"
                                  "    return value\n"
                                  "fn run(watched: Int) -> Int:\n"
                                  "    return accept(watched)\n";
static const char MOJO_BARE[] = "fn run(watched: Int) -> Int:\n"
                                "    return watched\n";

static const char OBJECTSCRIPT_UDL_INSIDE[] = "Class Sample.Callbacks Extends %RegisteredObject\n"
                                              "{\n"
                                              "ClassMethod Accept(value As %String) As %String\n"
                                              "{\n"
                                              "    Quit value\n"
                                              "}\n"
                                              "ClassMethod Run(watched As %String) As %String\n"
                                              "{\n"
                                              "    Quit ##class(Sample.Callbacks).Accept(watched)\n"
                                              "}\n"
                                              "}\n";
static const char OBJECTSCRIPT_UDL_BARE[] = "Class Sample.Callbacks Extends %RegisteredObject\n"
                                            "{\n"
                                            "ClassMethod Run(watched As %String) As %String\n"
                                            "{\n"
                                            "    Quit watched\n"
                                            "}\n"
                                            "}\n";

static const char OBJECTSCRIPT_ROUTINE_INSIDE[] = "SAMPLE\n"
                                                  "    Quit\n"
                                                  "Accept(value)\n"
                                                  "    Quit value\n"
                                                  "Run(watched)\n"
                                                  "    Set result = $$Accept(watched)\n"
                                                  "    Quit result\n";
static const char OBJECTSCRIPT_ROUTINE_BARE[] = "SAMPLE\n"
                                                "    Quit\n"
                                                "Run(watched)\n"
                                                "    Quit watched\n";

#define ROUTINE_ARGUMENT_CASE(tag_value, language_value, filename_value, inside_value, bare_value, \
                              kind_value, caller_value, callee_value, argument_value, defs_value,  \
                              inside_calls_value, bare_calls_value, reason_value)                  \
    {{tag_value, language_value, filename_value, reason_value},                                    \
     inside_value,                                                                                 \
     bare_value,                                                                                   \
     kind_value,                                                                                   \
     caller_value,                                                                                 \
     callee_value,                                                                                 \
     argument_value,                                                                               \
     defs_value,                                                                                   \
     inside_calls_value,                                                                           \
     bare_calls_value,                                                                             \
     1}

#define MODULE_ARGUMENT_CASE(tag_value, language_value, filename_value, inside_value, bare_value, \
                             kind_value, callee_value, argument_value, inside_calls_value,        \
                             bare_calls_value, reason_value)                                      \
    {{tag_value, language_value, filename_value, reason_value},                                   \
     inside_value,                                                                                \
     bare_value,                                                                                  \
     kind_value,                                                                                  \
     callee_value,                                                                                \
     argument_value,                                                                              \
     inside_calls_value,                                                                          \
     bare_calls_value}

static const RoutineArgumentCase RACKET_CASE = ROUTINE_ARGUMENT_CASE(
    "RACKET", CBM_LANG_RACKET, "sample.rkt", RACKET_INSIDE, RACKET_BARE, "list", "run", "accept",
    "watched", 1, 1, 0, "Racket list application and symbol-reference vocabulary");
static const RoutineArgumentCase ODIN_CASE = ROUTINE_ARGUMENT_CASE(
    "ODIN", CBM_LANG_ODIN, "sample.odin", ODIN_INSIDE, ODIN_BARE, "call_expression", "run",
    "accept", "watched", 1, 1, 0, "native routine application");
static const RoutineArgumentCase RESCRIPT_CASE = ROUTINE_ARGUMENT_CASE(
    "RESCRIPT", CBM_LANG_RESCRIPT, "Sample.res", RESCRIPT_INSIDE, RESCRIPT_BARE, "call_expression",
    "run", "accept", "watched", 1, 1, 0, "native routine application");
static const RoutineArgumentCase PURESCRIPT_CASE =
    ROUTINE_ARGUMENT_CASE("PURESCRIPT", CBM_LANG_PURESCRIPT, "Main.purs", PURESCRIPT_INSIDE,
                          PURESCRIPT_BARE, "exp_apply", "run", "accept", "watched", 1, 1, 0,
                          "PureScript application and variable-reference vocabulary");
static const RoutineArgumentCase NICKEL_CASE = ROUTINE_ARGUMENT_CASE(
    "NICKEL", CBM_LANG_NICKEL, "sample.ncl", NICKEL_INSIDE, NICKEL_BARE, "applicative", "run",
    "accept", "watched", 1, 1, 0, "Nickel application and identifier-reference vocabulary");
static const RoutineArgumentCase CRYSTAL_CASE = ROUTINE_ARGUMENT_CASE(
    "CRYSTAL", CBM_LANG_CRYSTAL, "sample.cr", CRYSTAL_INSIDE, CRYSTAL_BARE, "call", "run", "accept",
    "watched", 1, 1, 0, "native routine application");
static const RoutineArgumentCase TEAL_CASE = ROUTINE_ARGUMENT_CASE(
    "TEAL", CBM_LANG_TEAL, "sample.tl", LUA_TYPED_INSIDE, LUA_TYPED_BARE, "function_call", "run",
    "accept", "watched", 1, 1, 0, "native routine application");
static const RoutineArgumentCase HARE_CASE = ROUTINE_ARGUMENT_CASE(
    "HARE", CBM_LANG_HARE, "sample.ha", HARE_INSIDE, HARE_BARE, "call_expression", "run", "accept",
    "watched", 1, 1, 0, "native routine application");
static const RoutineArgumentCase PONY_CASE = ROUTINE_ARGUMENT_CASE(
    "PONY", CBM_LANG_PONY, "sample.pony", PONY_INSIDE, PONY_BARE, "call_expression", "run",
    "accept", "watched", 1, 1, 0, "native method application");
static const RoutineArgumentCase LUAU_CASE = ROUTINE_ARGUMENT_CASE(
    "LUAU", CBM_LANG_LUAU, "sample.luau", LUA_TYPED_INSIDE, LUA_TYPED_BARE, "function_call", "run",
    "accept", "watched", 1, 1, 0, "native routine application");
static const RoutineArgumentCase SWAY_CASE = ROUTINE_ARGUMENT_CASE(
    "SWAY", CBM_LANG_SWAY, "sample.sw", SWAY_INSIDE, SWAY_BARE, "call_expression", "run", "accept",
    "watched", 1, 1, 0, "native routine application");
static const RoutineArgumentCase TEMPL_CASE = ROUTINE_ARGUMENT_CASE(
    "TEMPL", CBM_LANG_TEMPL, "sample.templ", TEMPL_INSIDE, TEMPL_BARE, "call_expression", "run",
    "accept", "watched", 1, 1, 0, "embedded Go routine application");
static const RoutineArgumentCase WGSL_CASE =
    ROUTINE_ARGUMENT_CASE("WGSL", CBM_LANG_WGSL, "sample.wgsl", WGSL_INSIDE, WGSL_BARE,
                          "type_constructor_or_function_call_expression", "run", "accept",
                          "watched", 1, 1, 0, "native shader routine application");
static const RoutineArgumentCase JSONNET_CASE = ROUTINE_ARGUMENT_CASE(
    "JSONNET", CBM_LANG_JSONNET, "sample.jsonnet", JSONNET_INSIDE, JSONNET_BARE, "functioncall",
    "run", "accept", "watched", 1, 1, 0, "native configuration-function application");
static const RoutineArgumentCase STARLARK_CASE = ROUTINE_ARGUMENT_CASE(
    "STARLARK", CBM_LANG_STARLARK, "BUILD", STARLARK_INSIDE, STARLARK_BARE, "call", "run", "accept",
    "watched", 1, 1, 0, "native Starlark routine application");
static const RoutineArgumentCase BICEP_CASE = ROUTINE_ARGUMENT_CASE(
    "BICEP", CBM_LANG_BICEP, "sample.bicep", BICEP_INSIDE, BICEP_BARE, "call_expression", "run",
    "accept", "watched", 1, 1, 0, "native Bicep user-function application");
static const RoutineArgumentCase HLSL_CASE = ROUTINE_ARGUMENT_CASE(
    "HLSL", CBM_LANG_HLSL, "sample.hlsl", HLSL_FAMILY_INSIDE, HLSL_FAMILY_BARE, "call_expression",
    "run", "accept", "watched", 1, 1, 0, "native shader routine application");
static const RoutineArgumentCase VHDL_CASE = ROUTINE_ARGUMENT_CASE(
    "VHDL", CBM_LANG_VHDL, "sample.vhd", VHDL_INSIDE, VHDL_BARE, "parenthesis_group", "run",
    "accept", "watched", 2, 1, 0, "VHDL declaration and body application");
static const RoutineArgumentCase SYSTEMVERILOG_CASE = ROUTINE_ARGUMENT_CASE(
    "SYSTEMVERILOG", CBM_LANG_SYSTEMVERILOG, "sample.sv", SYSTEMVERILOG_INSIDE, SYSTEMVERILOG_BARE,
    "function_subroutine_call", "run", "accept", "watched", 1, 1, 0,
    "native hardware-description routine application");
static const RoutineArgumentCase ISPC_CASE = ROUTINE_ARGUMENT_CASE(
    "ISPC", CBM_LANG_ISPC, "sample.ispc", HLSL_FAMILY_INSIDE, HLSL_FAMILY_BARE, "call_expression",
    "run", "accept", "watched", 1, 1, 0, "native SPMD routine application");
static const RoutineArgumentCase CAIRO_CASE = ROUTINE_ARGUMENT_CASE(
    "CAIRO", CBM_LANG_CAIRO, "sample.cairo", CAIRO_INSIDE, CAIRO_BARE, "call_expression", "run",
    "accept", "watched", 1, 1, 0, "native smart-contract routine application");
static const RoutineArgumentCase MOVE_CASE = ROUTINE_ARGUMENT_CASE(
    "MOVE", CBM_LANG_MOVE, "sample.move", MOVE_INSIDE, MOVE_BARE, "call_expression", "run",
    "accept", "watched", 1, 1, 0, "native smart-contract routine application");
static const RoutineArgumentCase SQUIRREL_CASE = ROUTINE_ARGUMENT_CASE(
    "SQUIRREL", CBM_LANG_SQUIRREL, "sample.nut", SQUIRREL_INSIDE, SQUIRREL_BARE, "call_expression",
    "run", "accept", "watched", 1, 1, 0, "native routine application");
static const RoutineArgumentCase FUNC_CASE = ROUTINE_ARGUMENT_CASE(
    "FUNC", CBM_LANG_FUNC, "sample.fc", FUNC_INSIDE, FUNC_BARE, "function_application", "run",
    "accept", "watched", 1, 1, 0, "native FunC function application");
static const RoutineArgumentCase PUPPET_CASE = ROUTINE_ARGUMENT_CASE(
    "PUPPET", CBM_LANG_PUPPET, "sample.pp", PUPPET_INSIDE, PUPPET_BARE, "function_call", "run",
    "accept", "watched", 1, 1, 0, "Puppet function application and variable-reference vocabulary");
static const RoutineArgumentCase SLANG_CASE = ROUTINE_ARGUMENT_CASE(
    "SLANG", CBM_LANG_SLANG, "sample.slang", HLSL_FAMILY_INSIDE, HLSL_FAMILY_BARE,
    "call_expression", "run", "accept", "watched", 1, 1, 0, "native shader routine application");
static const RoutineArgumentCase LLVM_IR_CASE = ROUTINE_ARGUMENT_CASE(
    "LLVM_IR", CBM_LANG_LLVM_IR, "sample.ll", LLVM_IR_INSIDE, LLVM_IR_BARE, "call", "run", "accept",
    "watched", 1, 1, 0, "LLVM call instruction and local_var reference vocabulary");
static const RoutineArgumentCase TLAPLUS_CASE = ROUTINE_ARGUMENT_CASE(
    "TLAPLUS", CBM_LANG_TLAPLUS, "Sample.tla", TLAPLUS_INSIDE, TLAPLUS_BARE, "bound_op", "Guard",
    "Accept", "values", 1, 1, 0, "TLA+ operator application with a value argument");
static const RoutineArgumentCase APEX_CASE = ROUTINE_ARGUMENT_CASE(
    "APEX", CBM_LANG_APEX, "Sample.cls", APEX_INSIDE, APEX_BARE, "method_invocation", "run",
    "accept", "watched", 1, 1, 0, "native method application");
static const RoutineArgumentCase PINE_CASE = ROUTINE_ARGUMENT_CASE(
    "PINE", CBM_LANG_PINE, "sample.pine", PINE_INSIDE, PINE_BARE, "call", "run", "accept",
    "watched", 1, 2, 1, "Pine has one module-level indicator call in each fixture");
static const RoutineArgumentCase QML_CASE = ROUTINE_ARGUMENT_CASE(
    "QML", CBM_LANG_QML, "Sample.qml", QML_INSIDE, QML_BARE, "call_expression", "run", "accept",
    "watched", 1, 1, 0, "native QML JavaScript routine application");
static const RoutineArgumentCase CFSCRIPT_CASE = ROUTINE_ARGUMENT_CASE(
    "CFSCRIPT", CBM_LANG_CFSCRIPT, "Sample.cfc", CFSCRIPT_INSIDE, CFSCRIPT_BARE, "call_expression",
    "run", "accept", "watched", 1, 1, 0, "native CFScript routine application");
static const RoutineArgumentCase CFML_CASE = ROUTINE_ARGUMENT_CASE(
    "CFML", CBM_LANG_CFML, "sample.cfm", CFML_INSIDE, CFML_BARE, "call_expression", "run", "accept",
    "watched", 1, 1, 0, "native CFML tag-routine application");
static const RoutineArgumentCase MOJO_CASE =
    ROUTINE_ARGUMENT_CASE("MOJO", CBM_LANG_MOJO, "sample.mojo", MOJO_INSIDE, MOJO_BARE, "call",
                          "run", "accept", "watched", 1, 1, 0, "native Mojo routine application");
static const RoutineArgumentCase OBJECTSCRIPT_UDL_CASE = ROUTINE_ARGUMENT_CASE(
    "OBJECTSCRIPT_UDL", CBM_LANG_OBJECTSCRIPT_UDL, "Sample.cls", OBJECTSCRIPT_UDL_INSIDE,
    OBJECTSCRIPT_UDL_BARE, "class_method_call", "Run", "Accept", "watched", 1, 1, 0,
    "ObjectScript UDL class-method application with a value argument");
static const RoutineArgumentCase OBJECTSCRIPT_ROUTINE_CASE = ROUTINE_ARGUMENT_CASE(
    "OBJECTSCRIPT_ROUTINE", CBM_LANG_OBJECTSCRIPT_ROUTINE, "Sample.mac",
    OBJECTSCRIPT_ROUTINE_INSIDE, OBJECTSCRIPT_ROUTINE_BARE, "extrinsic_function", "Run", "Accept",
    "watched", 1, 1, 0, "ObjectScript routine extrinsic application with a value argument");

static const ModuleArgumentCase JUST_CASE = MODULE_ARGUMENT_CASE(
    "JUST", CBM_LANG_JUST, "justfile", JUST_INSIDE, JUST_BARE, "function_call", "uppercase",
    "watched", 1, 0, "Just expression call; recipe dependency is checked separately");
static const ModuleArgumentCase GOTEMPLATE_CASE =
    MODULE_ARGUMENT_CASE("GOTEMPLATE", CBM_LANG_GOTEMPLATE, "sample.tmpl", GOTEMPLATE_INSIDE,
                         GOTEMPLATE_BARE, "function_call", "printf", "Watched", 1, 0,
                         "Go-template pipeline value and paired bare field reference");
static const ModuleArgumentCase LINKERSCRIPT_CASE =
    MODULE_ARGUMENT_CASE("LINKERSCRIPT", CBM_LANG_LINKERSCRIPT, "sample.ld", LINKERSCRIPT_INSIDE,
                         LINKERSCRIPT_BARE, "call_expression", "ABSOLUTE", "_start", 1, 0,
                         "linker built-in argument and symbol-reference vocabulary");
static const ModuleArgumentCase GN_CASE =
    MODULE_ARGUMENT_CASE("GN", CBM_LANG_GN, "BUILD.gn", GN_INSIDE, GN_BARE, "call_expression",
                         "assert", "watched", 1, 0, "GN expression function with a value argument");

#undef MODULE_ARGUMENT_CASE
#undef ROUTINE_ARGUMENT_CASE

static int run_no_call_domain(const CaseIdentity *identity, const char *source,
                              const char *node_kind) {
    int failures = 0;
    check_ast_kind_present(identity, "domain-negative", source, node_kind, &failures);
    CBMFileResult *result = extract_source(identity, "domain-negative", source, &failures);
    if (result) {
        check_exact(identity, "domain-negative", "total_calls", result->calls.count, 0, &failures);
        cbm_free_result(result);
    }
    return failures == 0 ? 0 : 1;
}

static int run_module_domain_call(const CaseIdentity *identity, const char *source,
                                  const char *node_kind, const char *callee) {
    int failures = 0;
    check_ast_kind_present(identity, "domain-call", source, node_kind, &failures);
    CBMFileResult *result = extract_source(identity, "domain-call", source, &failures);
    if (result) {
        const char *module_qn = checked_module_qn(identity, "domain-call", result, &failures);
        check_exact(identity, "domain-call", "callee_call_in_module",
                    call_count_scoped(result, callee, module_qn, SCOPE_EXACT), 1, &failures);
        check_exact(identity, "domain-call", "callee_not_usage_in_module",
                    usage_count_scoped(result, callee, module_qn, SCOPE_EXACT), 0, &failures);
        check_exact(identity, "domain-call", "total_calls", result->calls.count, 1, &failures);
        cbm_free_result(result);
    }
    return failures == 0 ? 0 : 1;
}

static int run_just_dependency_control(void) {
    static const CaseIdentity identity = {
        "JUST", CBM_LANG_JUST, "justfile",
        "recipe dependency is build-graph metadata, separate from expression arguments"};
    int failures = 0;
    check_ast_kind_present(&identity, "dependency-control", JUST_DEPENDENCY, "dependency",
                           &failures);
    CBMFileResult *result =
        extract_source(&identity, "dependency-control", JUST_DEPENDENCY, &failures);
    if (result) {
        check_exact(&identity, "dependency-control", "build_callable_definition",
                    callable_definition_count(result, "build"), 1, &failures);
        check_exact(&identity, "dependency-control", "test_callable_definition",
                    callable_definition_count(result, "test"), 1, &failures);
        const char *test_qn =
            checked_callable_qn(&identity, "dependency-control", result, "test", &failures);
        check_exact(&identity, "dependency-control", "dependency_call_in_test",
                    call_count_scoped(result, "build", test_qn, SCOPE_EXACT), 1, &failures);
        check_exact(&identity, "dependency-control", "dependency_usage_in_test",
                    usage_count_scoped(result, "build", test_qn, SCOPE_EXACT), 1, &failures);
        check_exact(&identity, "dependency-control", "total_calls", result->calls.count, 1,
                    &failures);
        cbm_free_result(result);
    }
    return failures == 0 ? 0 : 1;
}

static int run_tlaplus_bounded_quantification_control(void) {
    static const CaseIdentity identity = {
        "TLAPLUS", CBM_LANG_TLAPLUS, "Bounded.tla",
        "bounded quantification is not an operator call and must retain its set reference"};
    int failures = 0;
    check_ast_kind_present(&identity, "bounded-quantification-control",
                           TLAPLUS_BOUNDED_QUANTIFICATION, "bounded_quantification", &failures);
    CBMFileResult *result = extract_source(&identity, "bounded-quantification-control",
                                           TLAPLUS_BOUNDED_QUANTIFICATION, &failures);
    if (result) {
        check_exact(&identity, "bounded-quantification-control", "guard_callable_definition",
                    callable_definition_count(result, "Guard"), 1, &failures);
        check_exact(&identity, "bounded-quantification-control", "set_usage_in_guard",
                    usage_count_scoped(result, "values", "Guard", SCOPE_ROUTINE), 1, &failures);
        check_exact(&identity, "bounded-quantification-control", "set_not_call_in_guard",
                    call_count_scoped(result, "values", "Guard", SCOPE_ROUTINE), 0, &failures);
        check_exact(&identity, "bounded-quantification-control", "bound_name_not_call_in_guard",
                    call_count_scoped(result, "item", "Guard", SCOPE_ROUTINE), 0, &failures);
        check_exact(&identity, "bounded-quantification-control", "total_calls", result->calls.count,
                    0, &failures);
        cbm_free_result(result);
    }
    return failures == 0 ? 0 : 1;
}

static int run_nasm_domain_control(void) {
    static const CaseIdentity identity = {
        "NASM", CBM_LANG_NASM, "sample.asm",
        "call instruction is semantic and must retain its enclosing label"};
    int failures = 0;
    check_ast_kind_present(&identity, "instruction-control", NASM_SCOPE, "actual_instruction",
                           &failures);
    CBMFileResult *result = extract_source(&identity, "instruction-control", NASM_SCOPE, &failures);
    if (result) {
        check_exact(&identity, "instruction-control", "run_callable_definition",
                    callable_definition_count(result, "run"), 1, &failures);
        check_exact(&identity, "instruction-control", "accept_callable_definition",
                    callable_definition_count(result, "accept"), 1, &failures);
        check_exact(&identity, "instruction-control", "real_call_in_run",
                    call_count_scoped(result, "accept", "run", SCOPE_ROUTINE), 1, &failures);
        check_exact(&identity, "instruction-control", "mov_not_call_in_run",
                    call_count_scoped(result, "mov", "run", SCOPE_ROUTINE), 0, &failures);
        check_exact(&identity, "instruction-control", "ret_not_call_in_run",
                    call_count_scoped(result, "ret", "run", SCOPE_ROUTINE), 0, &failures);
        check_exact(&identity, "instruction-control", "callee_not_usage_in_run",
                    usage_count_scoped(result, "accept", "run", SCOPE_ROUTINE), 0, &failures);
        check_exact(&identity, "instruction-control", "total_calls", result->calls.count, 1,
                    &failures);
        cbm_free_result(result);
    }
    return failures == 0 ? 0 : 1;
}

static int run_bitbake_domain_control(void) {
    static const CaseIdentity identity = {
        "BITBAKE", CBM_LANG_BITBAKE, "sample.bb",
        "BitBake call metadata covers embedded Python, not task-to-task callback semantics"};
    int failures = 0;
    check_ast_kind_present(&identity, "embedded-python", BITBAKE_DOMAIN,
                           "anonymous_python_function", &failures);
    check_ast_kind_present(&identity, "embedded-python", BITBAKE_DOMAIN, "call", &failures);
    CBMFileResult *result = extract_source(&identity, "embedded-python", BITBAKE_DOMAIN, &failures);
    if (result) {
        check_exact(&identity, "embedded-python", "task_callable_definition",
                    callable_definition_count(result, "do_run"), 1, &failures);
        check_exact(&identity, "embedded-python", "note_call_in_task",
                    call_count_scoped(result, "note", "do_run", SCOPE_ROUTINE), 1, &failures);
        check_exact(&identity, "embedded-python", "callee_not_usage_in_task",
                    usage_count_scoped(result, "note", "do_run", SCOPE_ROUTINE), 0, &failures);
        check_exact(&identity, "embedded-python", "total_calls", result->calls.count, 1, &failures);
        cbm_free_result(result);
    }
    return failures == 0 ? 0 : 1;
}

#define DEFINE_ROUTINE_ARGUMENT_TEST(name, case_name)   \
    TEST(repro_call_argument_matrix_b_routine_##name) { \
        return run_routine_argument_case(&(case_name)); \
    }

DEFINE_ROUTINE_ARGUMENT_TEST(racket, RACKET_CASE)
DEFINE_ROUTINE_ARGUMENT_TEST(odin, ODIN_CASE)
DEFINE_ROUTINE_ARGUMENT_TEST(rescript, RESCRIPT_CASE)
DEFINE_ROUTINE_ARGUMENT_TEST(purescript, PURESCRIPT_CASE)
DEFINE_ROUTINE_ARGUMENT_TEST(nickel, NICKEL_CASE)
DEFINE_ROUTINE_ARGUMENT_TEST(crystal, CRYSTAL_CASE)
DEFINE_ROUTINE_ARGUMENT_TEST(teal, TEAL_CASE)
DEFINE_ROUTINE_ARGUMENT_TEST(hare, HARE_CASE)
DEFINE_ROUTINE_ARGUMENT_TEST(pony, PONY_CASE)
DEFINE_ROUTINE_ARGUMENT_TEST(luau, LUAU_CASE)
DEFINE_ROUTINE_ARGUMENT_TEST(sway, SWAY_CASE)
DEFINE_ROUTINE_ARGUMENT_TEST(templ, TEMPL_CASE)
DEFINE_ROUTINE_ARGUMENT_TEST(wgsl, WGSL_CASE)
DEFINE_ROUTINE_ARGUMENT_TEST(jsonnet, JSONNET_CASE)
DEFINE_ROUTINE_ARGUMENT_TEST(starlark, STARLARK_CASE)
DEFINE_ROUTINE_ARGUMENT_TEST(bicep, BICEP_CASE)
DEFINE_ROUTINE_ARGUMENT_TEST(hlsl, HLSL_CASE)
DEFINE_ROUTINE_ARGUMENT_TEST(vhdl, VHDL_CASE)
DEFINE_ROUTINE_ARGUMENT_TEST(systemverilog, SYSTEMVERILOG_CASE)
DEFINE_ROUTINE_ARGUMENT_TEST(ispc, ISPC_CASE)
DEFINE_ROUTINE_ARGUMENT_TEST(cairo, CAIRO_CASE)
DEFINE_ROUTINE_ARGUMENT_TEST(move, MOVE_CASE)
DEFINE_ROUTINE_ARGUMENT_TEST(squirrel, SQUIRREL_CASE)
DEFINE_ROUTINE_ARGUMENT_TEST(func, FUNC_CASE)
DEFINE_ROUTINE_ARGUMENT_TEST(puppet, PUPPET_CASE)
DEFINE_ROUTINE_ARGUMENT_TEST(slang, SLANG_CASE)
DEFINE_ROUTINE_ARGUMENT_TEST(llvm_ir, LLVM_IR_CASE)
DEFINE_ROUTINE_ARGUMENT_TEST(apex, APEX_CASE)
DEFINE_ROUTINE_ARGUMENT_TEST(pine, PINE_CASE)
DEFINE_ROUTINE_ARGUMENT_TEST(qml, QML_CASE)
DEFINE_ROUTINE_ARGUMENT_TEST(cfscript, CFSCRIPT_CASE)
DEFINE_ROUTINE_ARGUMENT_TEST(cfml, CFML_CASE)
DEFINE_ROUTINE_ARGUMENT_TEST(mojo, MOJO_CASE)
DEFINE_ROUTINE_ARGUMENT_TEST(objectscript_udl, OBJECTSCRIPT_UDL_CASE)
DEFINE_ROUTINE_ARGUMENT_TEST(objectscript_routine, OBJECTSCRIPT_ROUTINE_CASE)

#undef DEFINE_ROUTINE_ARGUMENT_TEST

TEST(repro_call_argument_matrix_b_routine_tlaplus) {
    int failures = run_routine_argument_case(&TLAPLUS_CASE);
    failures += run_tlaplus_bounded_quantification_control();
    return failures == 0 ? 0 : 1;
}

TEST(repro_call_argument_matrix_b_module_just) {
    int failures = run_module_argument_case(&JUST_CASE);
    failures += run_just_dependency_control();
    return failures == 0 ? 0 : 1;
}

#define DEFINE_MODULE_ARGUMENT_TEST(name, case_name)   \
    TEST(repro_call_argument_matrix_b_module_##name) { \
        return run_module_argument_case(&(case_name)); \
    }

DEFINE_MODULE_ARGUMENT_TEST(gotemplate, GOTEMPLATE_CASE)
DEFINE_MODULE_ARGUMENT_TEST(linkerscript, LINKERSCRIPT_CASE)
DEFINE_MODULE_ARGUMENT_TEST(gn, GN_CASE)

#undef DEFINE_MODULE_ARGUMENT_TEST

TEST(repro_call_argument_matrix_b_domain_nasm) {
    static const CaseIdentity call_syntax_identity = {
        "NASM", CBM_LANG_NASM, "macro.asm",
        "function-like macro application is a semantic call_syntax_expression"};
    int failures = run_nasm_domain_control();
    failures += run_module_domain_call(&call_syntax_identity, NASM_CALL_SYNTAX,
                                       "call_syntax_expression", "accept");
    return failures == 0 ? 0 : 1;
}

TEST(repro_call_argument_matrix_b_domain_prisma) {
    static const CaseIdentity identity = {
        "PRISMA", CBM_LANG_PRISMA, "schema.prisma",
        "schema default function is a domain call, not callback-argument coverage"};
    return run_module_domain_call(&identity, PRISMA_SCOPE, "call_expression", "now");
}

TEST(repro_call_argument_matrix_b_domain_diff) {
    static const CaseIdentity identity = {
        "DIFF", CBM_LANG_DIFF, "sample.diff",
        "diff command is a document record and must not emit a code call"};
    return run_no_call_domain(&identity, DIFF_SCOPE, "command");
}

TEST(repro_call_argument_matrix_b_domain_bibtex) {
    static const CaseIdentity identity = {
        "BIBTEX", CBM_LANG_BIBTEX, "sample.bib",
        "BibTeX command is document syntax and must not emit a code call"};
    return run_no_call_domain(&identity, BIBTEX_SCOPE, "command");
}

TEST(repro_call_argument_matrix_b_domain_devicetree) {
    static const CaseIdentity identity = {
        "DEVICETREE", CBM_LANG_DEVICETREE, "sample.dts",
        "DeviceTree macro expression is a domain call, not callback-argument coverage"};
    return run_module_domain_call(&identity, DEVICETREE_SCOPE, "call_expression", "MHZ");
}

TEST(repro_call_argument_matrix_b_domain_bitbake) {
    return run_bitbake_domain_control();
}

enum {
    ROUTINE_ARGUMENT_LANGUAGE_COUNT = 36,
    MODULE_ARGUMENT_LANGUAGE_COUNT = 4,
    DOMAIN_CONTROL_LANGUAGE_COUNT = 6,
    MATRIX_LANGUAGE_COUNT = ROUTINE_ARGUMENT_LANGUAGE_COUNT + MODULE_ARGUMENT_LANGUAGE_COUNT +
                            DOMAIN_CONTROL_LANGUAGE_COUNT,
};

_Static_assert(MATRIX_LANGUAGE_COUNT == 46,
               "RACKET..OBJECTSCRIPT_ROUTINE call-capable matrix must contain exactly 46 "
               "language rows");

#define MATRIX_B_LANGUAGE_ROWS(X)                                                               \
    X(repro_call_argument_matrix_b_routine_racket, RACKET_CASE.identity.language)               \
    X(repro_call_argument_matrix_b_routine_odin, ODIN_CASE.identity.language)                   \
    X(repro_call_argument_matrix_b_routine_rescript, RESCRIPT_CASE.identity.language)           \
    X(repro_call_argument_matrix_b_routine_purescript, PURESCRIPT_CASE.identity.language)       \
    X(repro_call_argument_matrix_b_routine_nickel, NICKEL_CASE.identity.language)               \
    X(repro_call_argument_matrix_b_routine_crystal, CRYSTAL_CASE.identity.language)             \
    X(repro_call_argument_matrix_b_routine_teal, TEAL_CASE.identity.language)                   \
    X(repro_call_argument_matrix_b_routine_hare, HARE_CASE.identity.language)                   \
    X(repro_call_argument_matrix_b_routine_pony, PONY_CASE.identity.language)                   \
    X(repro_call_argument_matrix_b_routine_luau, LUAU_CASE.identity.language)                   \
    X(repro_call_argument_matrix_b_routine_sway, SWAY_CASE.identity.language)                   \
    X(repro_call_argument_matrix_b_routine_templ, TEMPL_CASE.identity.language)                 \
    X(repro_call_argument_matrix_b_routine_wgsl, WGSL_CASE.identity.language)                   \
    X(repro_call_argument_matrix_b_routine_jsonnet, JSONNET_CASE.identity.language)             \
    X(repro_call_argument_matrix_b_routine_starlark, STARLARK_CASE.identity.language)           \
    X(repro_call_argument_matrix_b_routine_bicep, BICEP_CASE.identity.language)                 \
    X(repro_call_argument_matrix_b_routine_hlsl, HLSL_CASE.identity.language)                   \
    X(repro_call_argument_matrix_b_routine_vhdl, VHDL_CASE.identity.language)                   \
    X(repro_call_argument_matrix_b_routine_systemverilog, SYSTEMVERILOG_CASE.identity.language) \
    X(repro_call_argument_matrix_b_routine_ispc, ISPC_CASE.identity.language)                   \
    X(repro_call_argument_matrix_b_routine_cairo, CAIRO_CASE.identity.language)                 \
    X(repro_call_argument_matrix_b_routine_move, MOVE_CASE.identity.language)                   \
    X(repro_call_argument_matrix_b_routine_squirrel, SQUIRREL_CASE.identity.language)           \
    X(repro_call_argument_matrix_b_routine_func, FUNC_CASE.identity.language)                   \
    X(repro_call_argument_matrix_b_routine_puppet, PUPPET_CASE.identity.language)               \
    X(repro_call_argument_matrix_b_routine_slang, SLANG_CASE.identity.language)                 \
    X(repro_call_argument_matrix_b_routine_llvm_ir, LLVM_IR_CASE.identity.language)             \
    X(repro_call_argument_matrix_b_routine_tlaplus, TLAPLUS_CASE.identity.language)             \
    X(repro_call_argument_matrix_b_routine_apex, APEX_CASE.identity.language)                   \
    X(repro_call_argument_matrix_b_routine_pine, PINE_CASE.identity.language)                   \
    X(repro_call_argument_matrix_b_routine_qml, QML_CASE.identity.language)                     \
    X(repro_call_argument_matrix_b_routine_cfscript, CFSCRIPT_CASE.identity.language)           \
    X(repro_call_argument_matrix_b_routine_cfml, CFML_CASE.identity.language)                   \
    X(repro_call_argument_matrix_b_routine_mojo, MOJO_CASE.identity.language)                   \
    X(repro_call_argument_matrix_b_routine_objectscript_udl,                                    \
      OBJECTSCRIPT_UDL_CASE.identity.language)                                                  \
    X(repro_call_argument_matrix_b_routine_objectscript_routine,                                \
      OBJECTSCRIPT_ROUTINE_CASE.identity.language)                                              \
    X(repro_call_argument_matrix_b_module_just, JUST_CASE.identity.language)                    \
    X(repro_call_argument_matrix_b_module_gotemplate, GOTEMPLATE_CASE.identity.language)        \
    X(repro_call_argument_matrix_b_module_linkerscript, LINKERSCRIPT_CASE.identity.language)    \
    X(repro_call_argument_matrix_b_module_gn, GN_CASE.identity.language)                        \
    X(repro_call_argument_matrix_b_domain_nasm, CBM_LANG_NASM)                                  \
    X(repro_call_argument_matrix_b_domain_prisma, CBM_LANG_PRISMA)                              \
    X(repro_call_argument_matrix_b_domain_diff, CBM_LANG_DIFF)                                  \
    X(repro_call_argument_matrix_b_domain_bibtex, CBM_LANG_BIBTEX)                              \
    X(repro_call_argument_matrix_b_domain_devicetree, CBM_LANG_DEVICETREE)                      \
    X(repro_call_argument_matrix_b_domain_bitbake, CBM_LANG_BITBAKE)

#define MATRIX_B_COUNT_ROW(test_name_, language_) +1
enum { MATRIX_B_ROW_COUNT = 0 MATRIX_B_LANGUAGE_ROWS(MATRIX_B_COUNT_ROW) };
#undef MATRIX_B_COUNT_ROW

_Static_assert(MATRIX_B_ROW_COUNT == MATRIX_LANGUAGE_COUNT,
               "matrix B suite and language-row count must stay synchronized");

size_t repro_call_argument_matrix_b_copy_language_ids(CBMLanguage *language_ids, size_t capacity) {
    size_t row_count = 0;
#define COPY_MATRIX_B_LANGUAGE(test_name_, language_) \
    do {                                              \
        if (language_ids && row_count < capacity)     \
            language_ids[row_count] = (language_);    \
        row_count++;                                  \
    } while (0);
    MATRIX_B_LANGUAGE_ROWS(COPY_MATRIX_B_LANGUAGE)
#undef COPY_MATRIX_B_LANGUAGE
    return row_count;
}

SUITE(repro_call_argument_matrix_b) {
#define RUN_MATRIX_B_LANGUAGE_TEST(test_name_, language_) RUN_TEST(test_name_);
    MATRIX_B_LANGUAGE_ROWS(RUN_MATRIX_B_LANGUAGE_TEST)
#undef RUN_MATRIX_B_LANGUAGE_TEST
}

#undef MATRIX_B_LANGUAGE_ROWS
