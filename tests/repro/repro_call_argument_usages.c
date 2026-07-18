/*
 * repro_call_argument_usages.c — references passed as call arguments.
 *
 * A direct callee is represented by CALLS and must not be duplicated as USAGE.
 * Every other resolvable identifier in the argument subtree remains a value
 * reference and must be retained as USAGE. The shared extractor currently drops
 * all such identifiers when WalkState.inside_call is true.
 */
#include "test_framework.h"
#include "repro_harness.h"
#include "foundation/compat.h"
#include "pipeline/lsp_resolve.h"

#include <store/store.h>

#include <stdio.h>
#include <string.h>

typedef struct {
    const char *tag;
    const char *filename;
    const char *source;
    CBMLanguage language;
    int check_bare_usage;
} ArgUsageCase;

static int qn_ends_with_name(const char *qn, const char *name) {
    if (!qn || !name)
        return 0;
    const char *last = strrchr(qn, '.');
    return strcmp(last ? last + 1 : qn, name) == 0;
}

static int raw_usage_count(const CBMFileResult *r, const char *caller, const char *target) {
    int count = 0;
    for (int i = 0; i < r->usages.count; i++) {
        const CBMUsage *u = &r->usages.items[i];
        if (u->ref_name && strcmp(u->ref_name, target) == 0 &&
            qn_ends_with_name(u->enclosing_func_qn, caller)) {
            count++;
        }
    }
    return count;
}

static int raw_usage_count_kind(const CBMFileResult *r, const char *caller, const char *target,
                                CBMUsageKind kind) {
    int count = 0;
    for (int i = 0; i < r->usages.count; i++) {
        const CBMUsage *u = &r->usages.items[i];
        if (u->kind == kind && u->ref_name && strcmp(u->ref_name, target) == 0 &&
            qn_ends_with_name(u->enclosing_func_qn, caller)) {
            count++;
        }
    }
    return count;
}

static int raw_usage_candidate_count(const CBMFileResult *r, const char *caller,
                                     const char *target) {
    int count = 0;
    for (int i = 0; i < r->usages.count; i++) {
        const CBMUsage *u = &r->usages.items[i];
        if (u->kind == CBM_USAGE_VALUE && u->may_be_call_reference && u->ref_name &&
            strcmp(u->ref_name, target) == 0 && qn_ends_with_name(u->enclosing_func_qn, caller)) {
            count++;
        }
    }
    return count;
}

static int raw_usage_exists(const CBMFileResult *r, const char *caller, const char *target) {
    return raw_usage_count(r, caller, target) > 0;
}

static int raw_call_count(const CBMFileResult *r, const char *caller, const char *target) {
    int count = 0;
    for (int i = 0; i < r->calls.count; i++) {
        const CBMCall *c = &r->calls.items[i];
        const char *callee = c->callee_name ? strrchr(c->callee_name, '.') : NULL;
        callee = callee ? callee + 1 : c->callee_name;
        if (callee && strcmp(callee, target) == 0 &&
            qn_ends_with_name(c->enclosing_func_qn, caller)) {
            count++;
        }
    }
    return count;
}

static int raw_call_exists(const CBMFileResult *r, const char *caller, const char *target) {
    return raw_call_count(r, caller, target) > 0;
}

static int resolved_call_count(const CBMFileResult *r, const char *caller, const char *target) {
    int count = 0;
    for (int i = 0; i < r->resolved_calls.count; i++) {
        const CBMResolvedCall *c = &r->resolved_calls.items[i];
        if (c->kind == CBM_RESOLVED_INVOCATION && qn_ends_with_name(c->caller_qn, caller) &&
            qn_ends_with_name(c->callee_qn, target)) {
            count++;
        }
    }
    return count;
}

static int edge_connects(cbm_store_t *store, const char *project, const char *type,
                         const char *caller, const char *target) {
    cbm_edge_t *edges = NULL;
    int count = 0;
    if (cbm_store_find_edges_by_type(store, project, type, &edges, &count) != CBM_STORE_OK)
        return 0;

    int found = 0;
    for (int i = 0; i < count && !found; i++) {
        cbm_node_t src = {0};
        cbm_node_t dst = {0};
        int src_ok = cbm_store_find_node_by_id(store, edges[i].source_id, &src) == CBM_STORE_OK;
        int dst_ok = cbm_store_find_node_by_id(store, edges[i].target_id, &dst) == CBM_STORE_OK;
        if (src_ok && dst_ok && src.name && dst.name && strcmp(src.name, caller) == 0 &&
            strcmp(dst.name, target) == 0) {
            found = 1;
        }
        cbm_node_free_fields(&src);
        cbm_node_free_fields(&dst);
    }
    cbm_store_free_edges(edges, count);
    return found;
}

static int edge_connects_callable(cbm_store_t *store, const char *project, const char *type,
                                  const char *caller, const char *target) {
    cbm_edge_t *edges = NULL;
    int count = 0;
    if (cbm_store_find_edges_by_type(store, project, type, &edges, &count) != CBM_STORE_OK)
        return 0;

    int found = 0;
    for (int i = 0; i < count && !found; i++) {
        cbm_node_t src = {0};
        cbm_node_t dst = {0};
        int src_ok = cbm_store_find_node_by_id(store, edges[i].source_id, &src) == CBM_STORE_OK;
        int dst_ok = cbm_store_find_node_by_id(store, edges[i].target_id, &dst) == CBM_STORE_OK;
        int callable_target =
            dst.label && (strcmp(dst.label, "Function") == 0 || strcmp(dst.label, "Method") == 0);
        if (src_ok && dst_ok && callable_target && src.name && dst.name &&
            strcmp(src.name, caller) == 0 && strcmp(dst.name, target) == 0) {
            found = 1;
        }
        cbm_node_free_fields(&src);
        cbm_node_free_fields(&dst);
    }
    cbm_store_free_edges(edges, count);
    return found;
}

static int edge_connects_target_qn(cbm_store_t *store, const char *project, const char *type,
                                   const char *caller, const char *target_qn_fragment) {
    cbm_edge_t *edges = NULL;
    int count = 0;
    if (cbm_store_find_edges_by_type(store, project, type, &edges, &count) != CBM_STORE_OK)
        return 0;

    int found = 0;
    for (int i = 0; i < count && !found; i++) {
        cbm_node_t src = {0};
        cbm_node_t dst = {0};
        int src_ok = cbm_store_find_node_by_id(store, edges[i].source_id, &src) == CBM_STORE_OK;
        int dst_ok = cbm_store_find_node_by_id(store, edges[i].target_id, &dst) == CBM_STORE_OK;
        if (src_ok && dst_ok && src.name && dst.qualified_name && strcmp(src.name, caller) == 0 &&
            strstr(dst.qualified_name, target_qn_fragment)) {
            found = 1;
        }
        cbm_node_free_fields(&src);
        cbm_node_free_fields(&dst);
    }
    cbm_store_free_edges(edges, count);
    return found;
}

static int check_raw_argument_case(const ArgUsageCase *c) {
    CBMFileResult *r = cbm_extract_file(c->source, (int)strlen(c->source), c->language, "repro",
                                        c->filename, 0, NULL, NULL);
    if (!r) {
        fprintf(stderr, "  [call-arg] lang=%s invariant=extract result=null\n", c->tag);
        return 1;
    }

    int failures = 0;
#define CHECK_RAW(condition, invariant)                                                \
    do {                                                                               \
        if (!(condition)) {                                                            \
            fprintf(stderr, "  [call-arg] lang=%s invariant=%s\n", c->tag, invariant); \
            failures++;                                                                \
        }                                                                              \
    } while (0)
    CHECK_RAW(!r->has_error && !r->parse_incomplete, "valid_fixture");
    CHECK_RAW(raw_call_exists(r, "argument", "accept"), "registrar_call_missing");
    CHECK_RAW(raw_call_exists(r, "direct", "handler"), "direct_call_missing");
    CBMUsageKind expected_kind = CBM_USAGE_VALUE;
    CBMUsageKind rejected_kind = CBM_USAGE_CALL_REFERENCE;
    if (c->check_bare_usage) {
        CHECK_RAW(raw_usage_count_kind(r, "bare", "handler", expected_kind) == 1,
                  "bare_usage_kind_missing");
        CHECK_RAW(raw_usage_count_kind(r, "bare", "handler", rejected_kind) == 0,
                  "bare_usage_kind_duplicated");
        if (c->language == CBM_LANG_KOTLIN) {
            CHECK_RAW(raw_usage_candidate_count(r, "bare", "handler") == 1,
                      "bare_semantic_candidate_missing");
        }
    }
    CHECK_RAW(raw_usage_count_kind(r, "argument", "handler", expected_kind) == 1,
              "argument_usage_kind_missing");
    CHECK_RAW(raw_usage_count_kind(r, "argument", "handler", rejected_kind) == 0,
              "argument_usage_kind_duplicated");
    CHECK_RAW(raw_usage_candidate_count(r, "argument", "handler") == 1,
              "argument_semantic_candidate_missing");
    CHECK_RAW(!raw_usage_exists(r, "direct", "handler"), "direct_call_duplicated_as_usage");
    CHECK_RAW(!raw_call_exists(r, "argument", "handler"), "argument_fabricated_as_call");
#undef CHECK_RAW

    cbm_free_result(r);
    return failures;
}

static const ArgUsageCase SHARED_CASES[] = {
    {"javascript", "main.js",
     "function handler() {}\n"
     "function accept(cb) {}\n"
     "function argument() { accept(handler); }\n"
     "function direct() { handler(); }\n"
     "function bare() { handler; }\n",
     CBM_LANG_JAVASCRIPT, 1},
    {"typescript", "main.ts",
     "function handler(): void {}\n"
     "function accept(cb: () => void): void {}\n"
     "function argument(): void { accept(handler); }\n"
     "function direct(): void { handler(); }\n"
     "function bare(): void { handler; }\n",
     CBM_LANG_TYPESCRIPT, 1},
    {"go", "main.go",
     "package sample\n"
     "func handler() {}\n"
     "func accept(cb func()) {}\n"
     "func argument() { accept(handler) }\n"
     "func direct() { handler() }\n"
     "func bare() { _ = handler }\n",
     CBM_LANG_GO, 1},
    {"python", "main.py",
     "def handler():\n    pass\n"
     "def accept(cb):\n    pass\n"
     "def argument():\n    accept(handler)\n"
     "def direct():\n    handler()\n"
     "def bare():\n    handler\n",
     CBM_LANG_PYTHON, 1},
    {"c", "main.c",
     "typedef void (*callback_t)(void);\n"
     "void handler(void) {}\n"
     "void accept(callback_t cb) {}\n"
     "void argument(void) { accept(handler); }\n"
     "void direct(void) { handler(); }\n"
     "void bare(void) { callback_t cb = handler; (void)cb; }\n",
     CBM_LANG_C, 1},
    {"cpp", "main.cpp",
     "using Callback = void (*)();\n"
     "void handler() {}\n"
     "void accept(Callback cb) {}\n"
     "void argument() { accept(handler); }\n"
     "void direct() { handler(); }\n"
     "void bare() { Callback cb = handler; (void)cb; }\n",
     CBM_LANG_CPP, 0},
    {"rust", "main.rs",
     "fn handler() {}\n"
     "fn accept(_cb: fn()) {}\n"
     "fn argument() { accept(handler); }\n"
     "fn direct() { handler(); }\n"
     "fn bare() { let _cb: fn() = handler; }\n",
     CBM_LANG_RUST, 1},
    {"csharp", "Main.cs",
     "using System;\n"
     "class Sample {\n"
     "  static void handler() {}\n"
     "  static void accept(Action cb) {}\n"
     "  static void argument() { accept(handler); }\n"
     "  static void direct() { handler(); }\n"
     "  static void bare() { Action cb = handler; }\n"
     "}\n",
     CBM_LANG_CSHARP, 1},
    {"kotlin", "Main.kt",
     "fun handler() {}\n"
     "fun accept(cb: () -> Unit) {}\n"
     "fun argument() { accept(::handler) }\n"
     "fun direct() { handler() }\n"
     "fun bare() { val cb = ::handler }\n",
     CBM_LANG_KOTLIN, 1},
    {"tsx", "main.tsx",
     "function handler(): void {}\n"
     "function accept(cb: () => void): void {}\n"
     "function argument(): void { accept(handler); }\n"
     "function direct(): void { handler(); }\n"
     "function bare(): void { handler; }\n",
     CBM_LANG_TSX, 1},
    {"cuda", "main.cu",
     "using Callback = void (*)();\n"
     "void handler() {}\n"
     "void accept(Callback cb) {}\n"
     "void argument() { accept(handler); }\n"
     "void direct() { handler(); }\n"
     "void bare() { Callback cb = handler; (void)cb; }\n",
     CBM_LANG_CUDA, 0},
};

TEST(repro_call_argument_javascript) {
    ASSERT_EQ(check_raw_argument_case(&SHARED_CASES[0]), 0);
    PASS();
}

TEST(repro_call_argument_typescript) {
    ASSERT_EQ(check_raw_argument_case(&SHARED_CASES[1]), 0);
    PASS();
}

TEST(repro_call_argument_go) {
    ASSERT_EQ(check_raw_argument_case(&SHARED_CASES[2]), 0);
    PASS();
}

TEST(repro_call_argument_python) {
    ASSERT_EQ(check_raw_argument_case(&SHARED_CASES[3]), 0);
    PASS();
}

TEST(repro_call_argument_c) {
    ASSERT_EQ(check_raw_argument_case(&SHARED_CASES[4]), 0);
    PASS();
}

TEST(repro_call_argument_cpp) {
    ASSERT_EQ(check_raw_argument_case(&SHARED_CASES[5]), 0);
    PASS();
}

TEST(repro_call_argument_rust) {
    ASSERT_EQ(check_raw_argument_case(&SHARED_CASES[6]), 0);
    PASS();
}

TEST(repro_call_argument_csharp) {
    ASSERT_EQ(check_raw_argument_case(&SHARED_CASES[7]), 0);
    PASS();
}

TEST(repro_call_argument_kotlin) {
    ASSERT_EQ(check_raw_argument_case(&SHARED_CASES[8]), 0);
    PASS();
}

TEST(repro_call_argument_tsx_raw) {
    ASSERT_EQ(check_raw_argument_case(&SHARED_CASES[9]), 0);
    PASS();
}

TEST(repro_call_argument_cuda_raw) {
    ASSERT_EQ(check_raw_argument_case(&SHARED_CASES[10]), 0);
    PASS();
}

TEST(repro_python_local_shadow_does_not_link_global_callable) {
    static const char *source = "def handler():\n    return 1\n"
                                "def accept(value):\n    return value\n"
                                "def argument(handler):\n    return accept(handler)\n"
                                "def direct():\n    return handler()\n";
    RProj project = {0};
    cbm_store_t *store = rh_index(&project, "main.py", source);
    if (!store) {
        FAIL("Python local-shadow fixture did not produce a graph store");
    }

    int failures = 0;
    if (!edge_connects_callable(store, project.project, "CALLS", "direct", "handler")) {
        fprintf(stderr, "  [call-shadow] lang=python invariant=direct_global_call_missing\n");
        failures++;
    }
    static const char *forbidden_edge_types[] = {"USAGE", "CALL_REFERENCE", "CALLS"};
    for (size_t i = 0; i < sizeof(forbidden_edge_types) / sizeof(forbidden_edge_types[0]); i++) {
        if (edge_connects_callable(store, project.project, forbidden_edge_types[i], "argument",
                                   "handler")) {
            fprintf(stderr,
                    "  [call-shadow] lang=python invariant=local_parameter_linked_global "
                    "edge=%s\n",
                    forbidden_edge_types[i]);
            failures++;
        }
    }

    rh_cleanup(&project, store);
    ASSERT_EQ(failures, 0);
    PASS();
}

/* Separate defect discovered by the control matrix: a C++ function value in a
 * variable initializer is lost even though it is not inside a call subtree. */
TEST(repro_cpp_bare_function_value_usage) {
    static const char *source = "using Callback = void (*)();\n"
                                "void handler() {}\n"
                                "void bare() { Callback cb = handler; (void)cb; }\n";
    CBMFileResult *r = cbm_extract_file(source, (int)strlen(source), CBM_LANG_CPP, "repro",
                                        "main.cpp", 0, NULL, NULL);
    ASSERT_NOT_NULL(r);
    int found = raw_usage_exists(r, "bare", "handler");
    cbm_free_result(r);
    if (!found)
        fprintf(stderr, "  [call-arg] lang=cpp invariant=bare_usage_missing\n");
    ASSERT_TRUE(found);
    PASS();
}

static int check_ts_lsp_mode(int disabled) {
    static const char *source = "function handler(): void {}\n"
                                "function accept(cb: () => void): void {}\n"
                                "function argument(): void { accept(handler); }\n";
    if (disabled)
        cbm_setenv("CBM_LSP_DISABLED", "1", 1);
    else
        cbm_unsetenv("CBM_LSP_DISABLED");

    CBMFileResult *r = cbm_extract_file(source, (int)strlen(source), CBM_LANG_TYPESCRIPT, "repro",
                                        "main.ts", 0, NULL, NULL);
    int failures = 0;
    if (!r) {
        failures++;
    } else {
        if (r->has_error) {
            fprintf(stderr, "  [call-arg] lang=typescript lsp=%s invariant=valid_fixture\n",
                    disabled ? "off" : "on");
            failures++;
        }
        if (raw_call_count(r, "argument", "accept") != 1) {
            fprintf(stderr, "  [call-arg] lang=typescript lsp=%s invariant=outer_raw_call_count\n",
                    disabled ? "off" : "on");
            failures++;
        }
        if (r->calls.count != 1) {
            fprintf(stderr,
                    "  [call-arg] lang=typescript lsp=%s invariant=raw_call_total "
                    "expected=1 actual=%d\n",
                    disabled ? "off" : "on", r->calls.count);
            failures++;
        }
        if (raw_usage_count(r, "argument", "handler") != 1) {
            fprintf(stderr, "  [call-arg] lang=typescript lsp=%s invariant=argument_usage_count\n",
                    disabled ? "off" : "on");
            failures++;
        }
        if (r->usages.count != 1) {
            fprintf(stderr,
                    "  [call-arg] lang=typescript lsp=%s invariant=raw_usage_total "
                    "expected=1 actual=%d\n",
                    disabled ? "off" : "on", r->usages.count);
            for (int i = 0; i < r->usages.count; i++) {
                fprintf(stderr, "  [call-arg] unexpected_usage[%d]=%s owner=%s\n", i,
                        r->usages.items[i].ref_name ? r->usages.items[i].ref_name : "<null>",
                        r->usages.items[i].enclosing_func_qn ? r->usages.items[i].enclosing_func_qn
                                                             : "<null>");
            }
            failures++;
        }
        int resolved = resolved_call_count(r, "argument", "accept");
        if ((!disabled && resolved != 1) || (disabled && resolved != 0)) {
            fprintf(stderr,
                    "  [call-arg] lang=typescript lsp=%s invariant=outer_lsp_call_count "
                    "expected=%d actual=%d\n",
                    disabled ? "off" : "on", disabled ? 0 : 1, resolved);
            failures++;
        }
        int resolved_handler = resolved_call_count(r, "argument", "handler");
        if (resolved_handler != 0) {
            fprintf(stderr,
                    "  [call-arg] lang=typescript lsp=%s invariant=argument_not_resolved_call "
                    "expected=0 actual=%d\n",
                    disabled ? "off" : "on", resolved_handler);
            failures++;
        }
        int expected_resolved_total = disabled ? 0 : 1;
        if (r->resolved_calls.count != expected_resolved_total) {
            fprintf(stderr,
                    "  [call-arg] lang=typescript lsp=%s invariant=resolved_call_total "
                    "expected=%d actual=%d\n",
                    disabled ? "off" : "on", expected_resolved_total, r->resolved_calls.count);
            failures++;
        }
        cbm_free_result(r);
    }
    return failures;
}

TEST(repro_call_argument_ts_lsp_cannot_rescue) {
    const char *prior = getenv("CBM_LSP_DISABLED");
    char *saved = prior ? cbm_strdup(prior) : NULL;
    int failures = check_ts_lsp_mode(1) + check_ts_lsp_mode(0);
    if (saved) {
        cbm_setenv("CBM_LSP_DISABLED", saved, 1);
        free(saved);
    } else {
        cbm_unsetenv("CBM_LSP_DISABLED");
    }
    ASSERT_EQ(failures, 0);
    PASS();
}

static int check_callable_reference_edges(const char *tag, const RFile *files, int nfiles,
                                          const char *caller, const char *registrar,
                                          const char *target) {
    RProj project;
    cbm_store_t *store = rh_index_files(&project, files, nfiles);
    if (!store) {
        fprintf(stderr, "  [call-ref] case=%s invariant=index_store_missing\n", tag);
        rh_cleanup(&project, store);
        return 1;
    }

    int failures = 0;
    if (!edge_connects(store, project.project, "CALLS", caller, registrar)) {
        fprintf(stderr, "  [call-ref] case=%s invariant=registrar_call_missing\n", tag);
        failures++;
    }
    if (!edge_connects(store, project.project, "CALL_REFERENCE", caller, target)) {
        fprintf(stderr, "  [call-ref] case=%s invariant=call_reference_missing\n", tag);
        failures++;
    }
    if (edge_connects(store, project.project, "CALLS", caller, target)) {
        fprintf(stderr, "  [call-ref] case=%s invariant=reference_fabricated_as_call\n", tag);
        failures++;
    }
    if (edge_connects(store, project.project, "USAGE", caller, target)) {
        fprintf(stderr, "  [call-ref] case=%s invariant=reference_duplicated_as_usage\n", tag);
        failures++;
    }

    rh_cleanup(&project, store);
    return failures;
}

TEST(repro_call_argument_ts_pipeline_shapes) {
    static const RFile same_file[] = {{
        "main.ts",
        "function handler(): void {}\n"
        "function accept(cb: () => void): void {}\n"
        "function argument(): void { accept(handler); }\n",
    }};
    static const RFile imported[] = {
        {"plugin.ts", "export function importedHandler(): void {}\n"},
        {"main.ts", "import { importedHandler } from './plugin';\n"
                    "function accept(cb: () => void): void {}\n"
                    "export function importedArgument(): void { accept(importedHandler); }\n"},
    };
    int failures = check_callable_reference_edges("ts_same_file", same_file, 1, "argument",
                                                  "accept", "handler");
    failures += check_callable_reference_edges("ts_imported", imported, 2, "importedArgument",
                                               "accept", "importedHandler");
    ASSERT_EQ(failures, 0);
    PASS();
}

TEST(repro_call_argument_javascript_same_file_value_reference) {
    static const RFile files[] = {{
        "main.js",
        "function handler() {}\n"
        "function accept(callback) {}\n"
        "function argument() { accept(handler); }\n",
    }};
    int failures = check_callable_reference_edges("javascript_same_file", files, 1, "argument",
                                                  "accept", "handler");
    ASSERT_EQ(failures, 0);
    PASS();
}

TEST(repro_call_argument_tsx_same_file_value_reference) {
    static const RFile files[] = {{
        "main.tsx",
        "function handler(): void {}\n"
        "function accept(callback: () => void): void {}\n"
        "function argument(): void { accept(handler); }\n",
    }};
    int failures =
        check_callable_reference_edges("tsx_same_file", files, 1, "argument", "accept", "handler");
    ASSERT_EQ(failures, 0);
    PASS();
}

TEST(repro_call_argument_ts_family_forward_declared_value_reference) {
    static const RFile js_files[] = {{
        "forward.js",
        "function accept(callback) {}\n"
        "function argument() { accept(handler); }\n"
        "function handler() {}\n",
    }};
    static const RFile ts_files[] = {{
        "forward.ts",
        "function accept(callback: () => void): void {}\n"
        "function argument(): void { accept(handler); }\n"
        "function handler(): void {}\n",
    }};
    static const RFile tsx_files[] = {{
        "forward.tsx",
        "function accept(callback: () => void): void {}\n"
        "function argument(): void { accept(handler); }\n"
        "function handler(): void {}\n",
    }};

    int failures = 0;
    failures += check_callable_reference_edges("javascript_forward", js_files, 1, "argument",
                                               "accept", "handler");
    failures += check_callable_reference_edges("typescript_forward", ts_files, 1, "argument",
                                               "accept", "handler");
    failures += check_callable_reference_edges("tsx_forward", tsx_files, 1, "argument", "accept",
                                               "handler");
    ASSERT_EQ(failures, 0);
    PASS();
}

TEST(repro_call_argument_ts_nested_and_value_refs) {
    static const RFile files[] = {{
        "main.ts",
        "type CallbackOptions = { handler: () => void };\n"
        "type RuntimeOptions = { enabled: boolean };\n"
        "const runtimeOptions: RuntimeOptions = { enabled: true };\n"
        "function handler(): void {}\n"
        "function register(options: CallbackOptions): void {}\n"
        "function consume(options: RuntimeOptions): void {}\n"
        "function nestedArgument(): void { register({ handler: handler }); }\n"
        "function valueArgument(): void { consume(runtimeOptions); }\n",
    }};
    RProj project;
    cbm_store_t *store = rh_index_files(&project, files, 1);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("TS value-reference fixture did not produce a graph store");
    }
    int nested = edge_connects(store, project.project, "USAGE", "nestedArgument", "handler");
    int value = edge_connects(store, project.project, "USAGE", "valueArgument", "runtimeOptions");
    if (!nested)
        fprintf(stderr, "  [call-arg] case=ts_nested invariant=argument_usage_missing\n");
    if (!value)
        fprintf(stderr, "  [call-arg] case=ts_value invariant=argument_usage_missing\n");
    rh_cleanup(&project, store);
    ASSERT_TRUE(nested);
    ASSERT_TRUE(value);
    PASS();
}

TEST(repro_call_argument_ts_member_ref) {
    static const RFile files[] = {{
        "main.ts",
        "class Service { handler(): void {} }\n"
        "function accept(cb: () => void): void {}\n"
        "function memberArgument(service: Service): void { accept(service.handler); }\n",
    }};
    int failures = check_callable_reference_edges("ts_member", files, 1, "memberArgument", "accept",
                                                  "handler");
    ASSERT_EQ(failures, 0);
    PASS();
}

TEST(repro_call_argument_kotlin_pipeline_join) {
    static const RFile files[] = {{
        "Main.kt",
        "fun handler() {}\n"
        "fun accept(cb: () -> Unit) {}\n"
        "fun kotlinArgument() { accept(::handler) }\n",
    }};
    int failures = check_callable_reference_edges("kotlin_callable_ref", files, 1, "kotlinArgument",
                                                  "accept", "handler");
    ASSERT_EQ(failures, 0);
    PASS();
}

TEST(repro_call_argument_java_method_reference_edge) {
    static const RFile files[] = {{
        "Main.java",
        "interface Task { void invoke(); }\n"
        "class Main {\n"
        "  static void handler() {}\n"
        "  static void accept(Task callback) {}\n"
        "  static void javaArgument() { accept(Main::handler); }\n"
        "}\n",
    }};
    int failures = check_callable_reference_edges("java_method_ref", files, 1, "javaArgument",
                                                  "accept", "handler");
    ASSERT_EQ(failures, 0);
    PASS();
}

TEST(repro_java_same_name_references_keep_occurrence_identity) {
    static const RFile files[] = {
        {"A.java", "class A { static void handler() {} }\n"},
        {"B.java", "class B { static void handler() {} }\n"},
        {"Use.java", "interface Task { void invoke(); }\n"
                     "class Use {\n"
                     "  static void accept(Task callback) {}\n"
                     "  static void references() { accept(A::handler); accept(B::handler); }\n"
                     "}\n"},
    };
    RProj project;
    cbm_store_t *store = rh_index_files(&project, files, 3);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("Java same-name reference fixture did not produce a graph store");
    }
    int a = edge_connects_target_qn(store, project.project, "CALL_REFERENCE", "references",
                                    ".A.handler");
    int b = edge_connects_target_qn(store, project.project, "CALL_REFERENCE", "references",
                                    ".B.handler");
    rh_cleanup(&project, store);
    ASSERT_TRUE(a);
    ASSERT_TRUE(b);
    PASS();
}

TEST(repro_kotlin_bound_reference_uses_instance_type) {
    static const RFile files[] = {{
        "Main.kt",
        "class Service { fun handler() {} }\n"
        "class Other { fun handler() {} }\n"
        "fun accept(callback: () -> Unit) {}\n"
        "fun boundReference(service: Service) { accept(service::handler) }\n",
    }};
    RProj project;
    cbm_store_t *store = rh_index_files(&project, files, 1);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("Kotlin bound-reference fixture did not produce a graph store");
    }
    int service = edge_connects_target_qn(store, project.project, "CALL_REFERENCE",
                                          "boundReference", ".Service.handler");
    int other = edge_connects_target_qn(store, project.project, "CALL_REFERENCE", "boundReference",
                                        ".Other.handler");
    rh_cleanup(&project, store);
    ASSERT_TRUE(service);
    ASSERT_FALSE(other);
    PASS();
}

static const RFile PHP_FIRST_CLASS_CALLABLE[] = {{
    "main.php",
    "<?php\n"
    "function handler(): void {}\n"
    "function accept(callable $cb): void {}\n"
    "function phpArgument(): void { accept(handler(...)); }\n",
}};

TEST(repro_php_first_class_callable_reference_edge) {
    int failures = check_callable_reference_edges("php_first_class", PHP_FIRST_CLASS_CALLABLE, 1,
                                                  "phpArgument", "accept", "handler");
    ASSERT_EQ(failures, 0);
    PASS();
}

TEST(repro_php_same_name_references_keep_occurrence_identity) {
    static const RFile files[] = {{
        "main.php",
        "<?php\n"
        "class A { public static function handler(): void {} }\n"
        "class B { public static function handler(): void {} }\n"
        "function accept(callable $callback): void {}\n"
        "function references(): void { accept(A::handler(...)); accept(B::handler(...)); }\n",
    }};
    RProj project;
    cbm_store_t *store = rh_index_files(&project, files, 1);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("PHP same-name reference fixture did not produce a graph store");
    }
    int a = edge_connects_target_qn(store, project.project, "CALL_REFERENCE", "references",
                                    ".A.handler");
    int b = edge_connects_target_qn(store, project.project, "CALL_REFERENCE", "references",
                                    ".B.handler");
    rh_cleanup(&project, store);
    ASSERT_TRUE(a);
    ASSERT_TRUE(b);
    PASS();
}

TEST(repro_php_magic_first_class_reference_uses_reason_match) {
    static const RFile files[] = {{
        "magic.php",
        "<?php\n"
        "class Magic {\n"
        "  public function __call(string $name, array $arguments): mixed { return null; }\n"
        "}\n"
        "function accept(callable $callback): void {}\n"
        "function references(Magic $object): void { accept($object->missing(...)); }\n",
    }};
    RProj project;
    cbm_store_t *store = rh_index_files(&project, files, 1);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("PHP magic callable-reference fixture did not produce a graph store");
    }
    int magic = edge_connects_target_qn(store, project.project, "CALL_REFERENCE", "references",
                                        ".Magic.__call");
    int fabricated =
        edge_connects(store, project.project, "CALL_REFERENCE", "references", "missing");
    rh_cleanup(&project, store);
    ASSERT_TRUE(magic);
    ASSERT_FALSE(fabricated);
    PASS();
}

TEST(repro_cpp_signature_types_preserve_positions_without_changing_legacy_types) {
    static const char source[] = "struct FreeVec {};\n"
                                 "FreeVec operator+(FreeVec lhs, FreeVec rhs) { return lhs; }\n"
                                 "void positional(int first, FreeVec middle, int last) {}\n";
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_CPP, "repro",
                                             "signature.cpp", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    ASSERT_FALSE(result->has_error || result->parse_incomplete);

    const CBMDefinition *operator_def = NULL;
    const CBMDefinition *positional_def = NULL;
    for (int i = 0; i < result->defs.count; i++) {
        const CBMDefinition *definition = &result->defs.items[i];
        if (definition->name && strcmp(definition->name, "operator+") == 0)
            operator_def = definition;
        if (definition->name && strcmp(definition->name, "positional") == 0)
            positional_def = definition;
    }

    ASSERT_NOT_NULL(operator_def);
    ASSERT_NOT_NULL(operator_def->param_types);
    ASSERT_STR_EQ(operator_def->param_types[0], "FreeVec");
    ASSERT_NULL(operator_def->param_types[1]);
    ASSERT_EQ(operator_def->signature_param_count, 2);
    ASSERT_NOT_NULL(operator_def->signature_param_types);
    ASSERT_STR_EQ(operator_def->signature_param_types[0], "FreeVec");
    ASSERT_STR_EQ(operator_def->signature_param_types[1], "FreeVec");

    ASSERT_NOT_NULL(positional_def);
    ASSERT_NOT_NULL(positional_def->param_types);
    ASSERT_STR_EQ(positional_def->param_types[0], "FreeVec");
    ASSERT_NULL(positional_def->param_types[1]);
    ASSERT_EQ(positional_def->signature_param_count, 3);
    ASSERT_NOT_NULL(positional_def->signature_param_types);
    ASSERT_STR_EQ(positional_def->signature_param_types[0], "int");
    ASSERT_STR_EQ(positional_def->signature_param_types[1], "FreeVec");
    ASSERT_STR_EQ(positional_def->signature_param_types[2], "int");

    cbm_free_result(result);
    PASS();
}

typedef struct {
    const char *tag;
    const char *filename;
    const char *source;
    CBMLanguage language;
    const char *definition_name;
    const char *expected[4];
    int expected_count;
} SignaturePositionCase;

static int check_signature_position_case(const SignaturePositionCase *c) {
    CBMFileResult *result = cbm_extract_file(c->source, (int)strlen(c->source), c->language,
                                             "signature", c->filename, 0, NULL, NULL);
    if (!result) {
        fprintf(stderr, "  [signature-position] lang=%s invariant=extract_result\n", c->tag);
        return 1;
    }

    int failures = 0;
    if (result->has_error || result->parse_incomplete) {
        fprintf(stderr, "  [signature-position] lang=%s invariant=valid_fixture\n", c->tag);
        failures++;
    }
    const CBMDefinition *found = NULL;
    for (int i = 0; i < result->defs.count; i++) {
        const CBMDefinition *definition = &result->defs.items[i];
        if (definition->name && strcmp(definition->name, c->definition_name) == 0) {
            found = definition;
            break;
        }
    }
    if (!found) {
        fprintf(stderr, "  [signature-position] lang=%s invariant=definition_missing\n", c->tag);
        failures++;
    } else {
        if (found->signature_param_count != c->expected_count) {
            fprintf(stderr,
                    "  [signature-position] lang=%s invariant=count expected=%d actual=%d\n",
                    c->tag, c->expected_count, found->signature_param_count);
            failures++;
        }
        for (int i = 0; i < c->expected_count; i++) {
            const char *actual = found->signature_param_types && i < found->signature_param_count
                                     ? found->signature_param_types[i]
                                     : NULL;
            if (!actual || strcmp(actual, c->expected[i]) != 0) {
                fprintf(stderr,
                        "  [signature-position] lang=%s invariant=slot index=%d "
                        "expected=%s actual=%s\n",
                        c->tag, i, c->expected[i], actual ? actual : "<null>");
                failures++;
            }
        }
    }
    cbm_free_result(result);
    return failures;
}

TEST(repro_lsp_language_signatures_preserve_argument_positions) {
    static const SignaturePositionCase cases[] = {
        {"c",
         "signature.c",
         "typedef struct Thing { int value; } Thing;\n"
         "void position(int first, Thing middle, int last) {}\n",
         CBM_LANG_C,
         "position",
         {"int", "Thing", "int", NULL},
         3},
        {"cuda",
         "signature.cu",
         "struct Thing { int value; };\n"
         "void position(int first, Thing middle, int last) {}\n",
         CBM_LANG_CUDA,
         "position",
         {"int", "Thing", "int", NULL},
         3},
        {"go",
         "signature.go",
         "package sample\n"
         "type Thing struct{}\n"
         "func position(first, middle Thing, last int) {}\n",
         CBM_LANG_GO,
         "position",
         {"Thing", "Thing", "int", NULL},
         3},
        {"javascript",
         "signature.js",
         "function position(first, middle, last) {}\n",
         CBM_LANG_JAVASCRIPT,
         "position",
         {"?", "?", "?", NULL},
         3},
        {"typescript",
         "signature.ts",
         "class Sample {}\n"
         "function position(this: Sample, first: number, middle, last: number): void {}\n",
         CBM_LANG_TYPESCRIPT,
         "position",
         {"number", "?", "number", NULL},
         3},
        {"tsx",
         "signature.tsx",
         "class Sample {}\n"
         "function position(this: Sample, first: number, middle, last: number): void {}\n",
         CBM_LANG_TSX,
         "position",
         {"number", "?", "number", NULL},
         3},
        {"python",
         "signature.py",
         "class Sample:\n"
         "    def position(self, first: int, middle, last: int):\n"
         "        pass\n",
         CBM_LANG_PYTHON,
         "position",
         {"int", "?", "int", NULL},
         3},
        {"java",
         "Signature.java",
         "class Thing {}\n"
         "class Signature { static void position(int first, Thing middle, int last) {} }\n",
         CBM_LANG_JAVA,
         "position",
         {"int", "Thing", "int", NULL},
         3},
        {"csharp",
         "Signature.cs",
         "class Thing {}\n"
         "class Signature { static void position(int first, Thing middle, int last) {} }\n",
         CBM_LANG_CSHARP,
         "position",
         {"int", "Thing", "int", NULL},
         3},
        {"kotlin",
         "Signature.kt",
         "class Thing\n"
         "fun position(first: Int, middle: Thing, last: Int) {}\n",
         CBM_LANG_KOTLIN,
         "position",
         {"Int", "Thing", "Int", NULL},
         3},
        {"php",
         "signature.php",
         "<?php\n"
         "class Thing {}\n"
         "function position(int $first, Thing $middle, int $last): void {}\n",
         CBM_LANG_PHP,
         "position",
         {"int", "Thing", "int", NULL},
         3},
        {"rust",
         "signature.rs",
         "struct Thing;\n"
         "struct Sample;\n"
         "impl Sample { fn position(&self, first: i32, middle: Thing, last: i32) {} }\n",
         CBM_LANG_RUST,
         "position",
         {"i32", "Thing", "i32", NULL},
         3},
    };

    int failures = 0;
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        failures += check_signature_position_case(&cases[i]);
    }
    ASSERT_EQ(failures, 0);
    PASS();
}

TEST(repro_cpp_operator_edges_require_semantic_resolution) {
    static const RFile files[] = {
        {"operators.cpp", "struct Vec {\n"
                          "  int value;\n"
                          "  Vec operator+(const Vec& rhs) const { return *this; }\n"
                          "};\n"
                          "Vec overloaded(Vec lhs, Vec rhs) { return lhs + rhs; }\n"},
        {"primitive.cpp", "int primitive(int lhs, int rhs) { return lhs + rhs; }\n"},
        {"free_operator.cpp",
         "struct FreeVec {};\n"
         "FreeVec operator+(FreeVec lhs, FreeVec rhs) { return lhs; }\n"
         "FreeVec freeOverloaded(FreeVec lhs, FreeVec rhs) { return lhs + rhs; }\n"},
    };

    RProj project;
    cbm_store_t *store = rh_index_files(&project, files, 3);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("C++ semantic-operator fixture did not produce a graph store");
    }

    int overloaded = edge_connects(store, project.project, "CALLS", "overloaded", "operator+");
    int free_overloaded =
        edge_connects(store, project.project, "CALLS", "freeOverloaded", "operator+");
    int primitive = edge_connects(store, project.project, "CALLS", "primitive", "operator+");
    if (!overloaded)
        fprintf(stderr, "  [cpp-operator] invariant=overloaded_lsp_call_missing\n");
    if (primitive)
        fprintf(stderr, "  [cpp-operator] invariant=primitive_bound_to_unrelated_overload\n");
    if (!free_overloaded)
        fprintf(stderr, "  [cpp-operator] invariant=free_adl_operator_lsp_call_missing\n");
    rh_cleanup(&project, store);

    ASSERT_TRUE(overloaded);
    ASSERT_TRUE(free_overloaded);
    ASSERT_FALSE(primitive);
    PASS();
}

TEST(repro_cpp_operator_lsp_join_is_occurrence_exact) {
    static const char source[] = "struct Vec {\n"
                                 "  Vec operator+(const Vec& rhs) const { return *this; }\n"
                                 "};\n"
                                 "void mixed(Vec lhs, Vec rhs, int a, int b) {\n"
                                 "  Vec custom = lhs + rhs;\n"
                                 "  int primitive = a + b;\n"
                                 "}\n";
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_CPP, "repro",
                                             "mixed.cpp", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    ASSERT_FALSE(result->has_error || result->parse_incomplete);
    const CBMCall *custom = NULL;
    const CBMCall *primitive = NULL;
    for (int i = 0; i < result->calls.count; i++) {
        const CBMCall *call = &result->calls.items[i];
        if (!call->callee_name || strcmp(call->callee_name, "operator+") != 0)
            continue;
        if (call->start_line == 5)
            custom = call;
        if (call->start_line == 6)
            primitive = call;
    }
    ASSERT_NOT_NULL(custom);
    ASSERT_NOT_NULL(primitive);
    ASSERT_NOT_NULL(cbm_pipeline_find_lsp_resolution(&result->resolved_calls, custom, false));
    ASSERT_NULL(cbm_pipeline_find_lsp_resolution(&result->resolved_calls, primitive, false));
    cbm_free_result(result);
    PASS();
}

typedef struct {
    const char *tag;
    const char *filename;
    CBMLanguage language;
} CompoundAssignmentCase;

static const char COMPOUND_ASSIGNMENT_SOURCE[] =
    "struct Accumulator {\n"
    "  Accumulator& operator+=(const Accumulator& rhs) { return *this; }\n"
    "};\n"
    "struct FreeAccumulator {};\n"
    "FreeAccumulator& operator+=(FreeAccumulator& lhs, const FreeAccumulator& rhs) { return lhs; "
    "}\n"
    "Accumulator memberCustom(Accumulator member_lhs, Accumulator member_rhs) { "
    "member_lhs += member_rhs; return member_lhs; }\n"
    "FreeAccumulator freeCustom(FreeAccumulator free_lhs, FreeAccumulator free_rhs) { "
    "free_lhs += free_rhs; return free_lhs; }\n"
    "int primitive(int primitive_lhs, int primitive_rhs) { "
    "primitive_lhs += primitive_rhs; return primitive_lhs; }\n";

static int check_compound_assignment_case(const CompoundAssignmentCase *test_case) {
    static const char member_site[] = "member_lhs += member_rhs";
    static const char free_site[] = "free_lhs += free_rhs";
    static const char primitive_site[] = "primitive_lhs += primitive_rhs";
    const char *member_text = strstr(COMPOUND_ASSIGNMENT_SOURCE, member_site);
    const char *free_text = strstr(COMPOUND_ASSIGNMENT_SOURCE, free_site);
    const char *primitive_text = strstr(COMPOUND_ASSIGNMENT_SOURCE, primitive_site);
    if (!member_text || !free_text || !primitive_text) {
        fprintf(stderr, "  [compound-assignment] lang=%s invariant=fixture_occurrences_missing\n",
                test_case->tag);
        return 1;
    }

    uint32_t member_start = (uint32_t)(member_text - COMPOUND_ASSIGNMENT_SOURCE);
    uint32_t member_end = member_start + (uint32_t)strlen(member_site);
    uint32_t free_start = (uint32_t)(free_text - COMPOUND_ASSIGNMENT_SOURCE);
    uint32_t free_end = free_start + (uint32_t)strlen(free_site);
    uint32_t primitive_start = (uint32_t)(primitive_text - COMPOUND_ASSIGNMENT_SOURCE);
    uint32_t primitive_end = primitive_start + (uint32_t)strlen(primitive_site);
    CBMFileResult *result = cbm_extract_file(
        COMPOUND_ASSIGNMENT_SOURCE, (int)strlen(COMPOUND_ASSIGNMENT_SOURCE), test_case->language,
        "compound_assignment", test_case->filename, 0, NULL, NULL);
    if (!result) {
        fprintf(stderr, "  [compound-assignment] lang=%s invariant=extract_result\n",
                test_case->tag);
        return 1;
    }

    int failures = 0;
#define CHECK_COMPOUND(condition, invariant)                                                  \
    do {                                                                                      \
        if (!(condition)) {                                                                   \
            fprintf(stderr, "  [compound-assignment] lang=%s invariant=%s\n", test_case->tag, \
                    invariant);                                                               \
            failures++;                                                                       \
        }                                                                                     \
    } while (0)
    CHECK_COMPOUND(!result->has_error && !result->parse_incomplete, "valid_fixture");

    const char *member_target_qn = NULL;
    const char *free_target_qn = NULL;
    int member_target_count = 0;
    int free_target_count = 0;
    for (int i = 0; i < result->defs.count; i++) {
        const CBMDefinition *definition = &result->defs.items[i];
        if (!definition->name || strcmp(definition->name, "operator+=") != 0 ||
            !definition->qualified_name) {
            continue;
        }
        if (definition->parent_class &&
            strstr(definition->qualified_name, ".Accumulator.operator+=")) {
            member_target_qn = definition->qualified_name;
            member_target_count++;
        } else if (!definition->parent_class) {
            free_target_qn = definition->qualified_name;
            free_target_count++;
        }
    }
    CHECK_COMPOUND(member_target_count == 1, "member_target_definition");
    CHECK_COMPOUND(free_target_count == 1, "free_target_definition");
    CHECK_COMPOUND(member_target_qn && free_target_qn &&
                       strcmp(member_target_qn, free_target_qn) != 0,
                   "member_free_targets_distinct");

    const CBMCall *member_carrier = NULL;
    const CBMCall *free_carrier = NULL;
    const CBMCall *primitive_carrier = NULL;
    int member_carrier_count = 0;
    int free_carrier_count = 0;
    int primitive_carrier_count = 0;
    for (int i = 0; i < result->calls.count; i++) {
        const CBMCall *call = &result->calls.items[i];
        if (!call->requires_lsp_resolution || call->source_origin != CBM_SOURCE_ORIGIN_RAW ||
            !call->callee_name || strcmp(call->callee_name, "operator+=") != 0) {
            continue;
        }
        if (qn_ends_with_name(call->enclosing_func_qn, "memberCustom") &&
            call->site_start_byte == member_start && call->site_end_byte == member_end) {
            member_carrier = call;
            member_carrier_count++;
        }
        if (qn_ends_with_name(call->enclosing_func_qn, "freeCustom") &&
            call->site_start_byte == free_start && call->site_end_byte == free_end) {
            free_carrier = call;
            free_carrier_count++;
        }
        if (qn_ends_with_name(call->enclosing_func_qn, "primitive") &&
            call->site_start_byte == primitive_start && call->site_end_byte == primitive_end) {
            primitive_carrier = call;
            primitive_carrier_count++;
        }
    }
    CHECK_COMPOUND(member_carrier_count == 1, "member_exact_gated_carrier");
    CHECK_COMPOUND(free_carrier_count == 1, "free_exact_gated_carrier");
    CHECK_COMPOUND(primitive_carrier_count == 1, "primitive_exact_gated_carrier");

    const CBMResolvedCall *member_semantic = NULL;
    const CBMResolvedCall *free_semantic = NULL;
    int member_semantic_count = 0;
    int free_semantic_count = 0;
    int member_wrong_target_count = 0;
    int free_wrong_target_count = 0;
    int primitive_semantic_count = 0;
    for (int i = 0; i < result->resolved_calls.count; i++) {
        const CBMResolvedCall *resolved = &result->resolved_calls.items[i];
        if (resolved->kind != CBM_RESOLVED_INVOCATION ||
            resolved->source_origin != CBM_SOURCE_ORIGIN_RAW || !resolved->callee_qn ||
            !qn_ends_with_name(resolved->callee_qn, "operator+=")) {
            continue;
        }
        if (qn_ends_with_name(resolved->caller_qn, "memberCustom") &&
            resolved->site_start_byte == member_start && resolved->site_end_byte == member_end) {
            if (member_target_qn && strcmp(resolved->callee_qn, member_target_qn) == 0) {
                member_semantic = resolved;
                member_semantic_count++;
            } else {
                member_wrong_target_count++;
            }
        }
        if (qn_ends_with_name(resolved->caller_qn, "freeCustom") &&
            resolved->site_start_byte == free_start && resolved->site_end_byte == free_end) {
            if (free_target_qn && strcmp(resolved->callee_qn, free_target_qn) == 0) {
                free_semantic = resolved;
                free_semantic_count++;
            } else {
                free_wrong_target_count++;
            }
        }
        if (qn_ends_with_name(resolved->caller_qn, "primitive") &&
            resolved->site_start_byte == primitive_start &&
            resolved->site_end_byte == primitive_end) {
            primitive_semantic_count++;
        }
    }
    CHECK_COMPOUND(member_semantic_count == 1, "member_exact_semantic_resolution");
    CHECK_COMPOUND(free_semantic_count == 1, "free_exact_semantic_resolution");
    CHECK_COMPOUND(member_wrong_target_count == 0, "member_semantic_wrong_target");
    CHECK_COMPOUND(free_wrong_target_count == 0, "free_semantic_wrong_target");
    CHECK_COMPOUND(primitive_semantic_count == 0, "primitive_semantic_false_positive");

    const CBMResolvedCall *member_join =
        member_carrier
            ? cbm_pipeline_find_lsp_resolution(&result->resolved_calls, member_carrier, false)
            : NULL;
    const CBMResolvedCall *free_join =
        free_carrier
            ? cbm_pipeline_find_lsp_resolution(&result->resolved_calls, free_carrier, false)
            : NULL;
    const CBMResolvedCall *primitive_join =
        primitive_carrier
            ? cbm_pipeline_find_lsp_resolution(&result->resolved_calls, primitive_carrier, false)
            : NULL;
    CHECK_COMPOUND(member_join && member_join == member_semantic, "member_exact_semantic_join");
    CHECK_COMPOUND(free_join && free_join == free_semantic, "free_exact_semantic_join");
    CHECK_COMPOUND(primitive_join == NULL, "primitive_join_false_positive");
    cbm_free_result(result);

    RProj project;
    cbm_store_t *store = rh_index(&project, test_case->filename, COMPOUND_ASSIGNMENT_SOURCE);
    if (!store) {
        fprintf(stderr, "  [compound-assignment] lang=%s invariant=index_store_missing\n",
                test_case->tag);
        failures++;
    } else {
        CHECK_COMPOUND(edge_connects_target_qn(store, project.project, "CALLS", "memberCustom",
                                               ".Accumulator.operator+="),
                       "member_graph_call_missing");
        CHECK_COMPOUND(!edge_connects_target_qn(store, project.project, "CALLS", "memberCustom",
                                                ".compound_assignment.operator+="),
                       "member_graph_wrong_target");
        CHECK_COMPOUND(edge_connects_target_qn(store, project.project, "CALLS", "freeCustom",
                                               ".compound_assignment.operator+="),
                       "free_graph_call_missing");
        CHECK_COMPOUND(!edge_connects_target_qn(store, project.project, "CALLS", "freeCustom",
                                                ".Accumulator.operator+="),
                       "free_graph_wrong_target");
        CHECK_COMPOUND(!edge_connects(store, project.project, "CALLS", "primitive", "operator+="),
                       "primitive_graph_call_false_positive");
    }
    rh_cleanup(&project, store);
#undef CHECK_COMPOUND
    return failures;
}

TEST(repro_cpp_cuda_compound_assignment_calls_are_semantic_and_occurrence_exact) {
    static const CompoundAssignmentCase cases[] = {
        {"cpp", "compound_assignment.cpp", CBM_LANG_CPP},
        {"cuda", "compound_assignment.cu", CBM_LANG_CUDA},
    };
    int failures = 0;
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++)
        failures += check_compound_assignment_case(&cases[i]);
    ASSERT_EQ(failures, 0);
    PASS();
}

TEST(repro_kotlin_synthetic_calls_require_semantic_resolution) {
    static const char source[] = "class Box { operator fun plus(other: Box): Box = this }\n"
                                 "fun mixed(lhs: Box, rhs: Box, a: Int, b: Int) {\n"
                                 "  val custom = lhs + rhs\n"
                                 "  val primitive = a + b\n"
                                 "}\n";
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_KOTLIN, "repro",
                                             "Mixed.kt", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    ASSERT_FALSE(result->has_error || result->parse_incomplete);
    const CBMCall *custom = NULL;
    const CBMCall *primitive = NULL;
    for (int i = 0; i < result->calls.count; i++) {
        const CBMCall *call = &result->calls.items[i];
        if (!call->callee_name || strcmp(call->callee_name, "plus") != 0)
            continue;
        if (call->start_line == 3)
            custom = call;
        if (call->start_line == 4)
            primitive = call;
    }
    ASSERT_NOT_NULL(custom);
    ASSERT_NOT_NULL(primitive);
    const CBMResolvedCall *custom_resolution =
        cbm_pipeline_find_lsp_resolution(&result->resolved_calls, custom, true);
    const CBMResolvedCall *primitive_resolution =
        cbm_pipeline_find_lsp_resolution(&result->resolved_calls, primitive, true);
    ASSERT_NOT_NULL(custom_resolution);
    ASSERT_TRUE(strstr(custom_resolution->callee_qn, ".Box.plus") != NULL);
    /* `Int.plus` is a valid semantic LSP resolution, but it is an external
     * stdlib target and must not borrow the in-project Box.plus occurrence.
     * The graph-level assertions below prove that no primitive CALLS edge is
     * emitted; this direct check proves the two occurrence joins stay apart. */
    if (primitive_resolution) {
        ASSERT_TRUE(strstr(primitive_resolution->callee_qn, ".Box.plus") == NULL);
        ASSERT_EQ(primitive_resolution->site_start_byte, primitive->site_start_byte);
        ASSERT_EQ(primitive_resolution->site_end_byte, primitive->site_end_byte);
    }
    cbm_free_result(result);

    static const RFile files[] = {{
        "Builtins.kt",
        "class Trap {\n"
        "  fun iterator(): Trap = this\n"
        "  fun component1(): Int = 0\n"
        "}\n"
        "fun loopBuiltIn(values: IntArray) { for (value in values) { println(value) } }\n"
        "fun destructureBuiltIn(value: Pair<Int, Int>) { val (left, right) = value }\n",
    }};
    RProj project;
    cbm_store_t *store = rh_index_files(&project, files, 1);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("Kotlin synthetic-call negative fixture did not produce a graph store");
    }
    int loop = edge_connects(store, project.project, "CALLS", "loopBuiltIn", "iterator");
    int component =
        edge_connects(store, project.project, "CALLS", "destructureBuiltIn", "component1");
    rh_cleanup(&project, store);
    ASSERT_FALSE(loop);
    ASSERT_FALSE(component);
    PASS();
}

TEST(repro_callable_reference_parallel_pipeline) {
    enum { PRIMARY_FILES = 6, FILLER_FILES = 45, TOTAL_FILES = PRIMARY_FILES + FILLER_FILES };
    static const RFile primary[PRIMARY_FILES] = {
        {"JavaTarget.java", "class JavaTarget { static void javaHandler() {} }\n"},
        {"JavaUse.java", "interface JavaTask { void invoke(); }\n"
                         "class JavaUse {\n"
                         "  static void acceptJava(JavaTask callback) {}\n"
                         "  static void javaReference() { acceptJava(JavaTarget::javaHandler); }\n"
                         "}\n"},
        {"KotlinTarget.kt", "fun kotlinHandler() {}\n"},
        {"KotlinUse.kt", "fun acceptKotlin(callback: () -> Unit) {}\n"
                         "fun kotlinReference() { acceptKotlin(::kotlinHandler) }\n"},
        {"php_target.php", "<?php\nfunction phpHandler(): void {}\n"},
        {"php_use.php", "<?php\n"
                        "require_once 'php_target.php';\n"
                        "function acceptPhp(callable $callback): void {}\n"
                        "function phpReference(): void { acceptPhp(phpHandler(...)); }\n"},
    };

    RFile files[TOTAL_FILES];
    char *filler_names[FILLER_FILES];
    char *filler_sources[FILLER_FILES];
    memset(filler_names, 0, sizeof(filler_names));
    memset(filler_sources, 0, sizeof(filler_sources));
    for (int i = 0; i < PRIMARY_FILES; i++)
        files[i] = primary[i];

    int built = PRIMARY_FILES;
    for (int i = 0; i < FILLER_FILES; i++) {
        filler_names[i] = malloc(64);
        filler_sources[i] = malloc(128);
        if (!filler_names[i] || !filler_sources[i])
            break;
        snprintf(filler_names[i], 64, "Filler%02d.java", i);
        snprintf(filler_sources[i], 128, "class Filler%02d { int value%02d; }\n", i, i);
        files[built].name = filler_names[i];
        files[built].content = filler_sources[i];
        built++;
    }

    const char *prior_workers = getenv("CBM_WORKERS");
    char *saved_workers = prior_workers ? cbm_strdup(prior_workers) : NULL;
    cbm_setenv("CBM_WORKERS", "2", 1);

    RProj project;
    memset(&project, 0, sizeof(project));
    cbm_store_t *store = built == TOTAL_FILES ? rh_index_files(&project, files, built) : NULL;
    int failures = 0;
    if (!store) {
        fprintf(stderr, "  [call-ref] case=parallel invariant=index_store_missing\n");
        failures++;
    } else {
        static const struct {
            const char *caller;
            const char *target;
        } expected[] = {{"javaReference", "javaHandler"},
                        {"kotlinReference", "kotlinHandler"},
                        {"phpReference", "phpHandler"}};
        for (size_t i = 0; i < sizeof(expected) / sizeof(expected[0]); i++) {
            if (!edge_connects(store, project.project, "CALL_REFERENCE", expected[i].caller,
                               expected[i].target)) {
                fprintf(stderr,
                        "  [call-ref] case=parallel caller=%s target=%s "
                        "invariant=call_reference_missing\n",
                        expected[i].caller, expected[i].target);
                failures++;
            }
            if (edge_connects(store, project.project, "CALLS", expected[i].caller,
                              expected[i].target) ||
                edge_connects(store, project.project, "USAGE", expected[i].caller,
                              expected[i].target)) {
                fprintf(stderr,
                        "  [call-ref] case=parallel caller=%s target=%s "
                        "invariant=reference_relationship_not_exclusive\n",
                        expected[i].caller, expected[i].target);
                failures++;
            }
        }
    }

    rh_cleanup(&project, store);
    if (saved_workers) {
        cbm_setenv("CBM_WORKERS", saved_workers, 1);
        free(saved_workers);
    } else {
        cbm_unsetenv("CBM_WORKERS");
    }
    for (int i = 0; i < FILLER_FILES; i++) {
        free(filler_names[i]);
        free(filler_sources[i]);
    }

    ASSERT_EQ(failures, 0);
    PASS();
}

SUITE(repro_call_argument_usages) {
    RUN_TEST(repro_call_argument_javascript);
    RUN_TEST(repro_call_argument_typescript);
    RUN_TEST(repro_call_argument_go);
    RUN_TEST(repro_call_argument_python);
    RUN_TEST(repro_call_argument_c);
    RUN_TEST(repro_call_argument_cpp);
    RUN_TEST(repro_call_argument_rust);
    RUN_TEST(repro_call_argument_csharp);
    RUN_TEST(repro_call_argument_kotlin);
    RUN_TEST(repro_call_argument_tsx_raw);
    RUN_TEST(repro_call_argument_cuda_raw);
    RUN_TEST(repro_python_local_shadow_does_not_link_global_callable);
    RUN_TEST(repro_cpp_bare_function_value_usage);
    RUN_TEST(repro_call_argument_ts_lsp_cannot_rescue);
    RUN_TEST(repro_call_argument_ts_pipeline_shapes);
    RUN_TEST(repro_call_argument_javascript_same_file_value_reference);
    RUN_TEST(repro_call_argument_tsx_same_file_value_reference);
    RUN_TEST(repro_call_argument_ts_family_forward_declared_value_reference);
    RUN_TEST(repro_call_argument_ts_nested_and_value_refs);
    RUN_TEST(repro_call_argument_ts_member_ref);
    RUN_TEST(repro_call_argument_kotlin_pipeline_join);
    RUN_TEST(repro_call_argument_java_method_reference_edge);
    RUN_TEST(repro_java_same_name_references_keep_occurrence_identity);
    RUN_TEST(repro_kotlin_bound_reference_uses_instance_type);
    RUN_TEST(repro_php_first_class_callable_reference_edge);
    RUN_TEST(repro_php_same_name_references_keep_occurrence_identity);
    RUN_TEST(repro_php_magic_first_class_reference_uses_reason_match);
    RUN_TEST(repro_cpp_signature_types_preserve_positions_without_changing_legacy_types);
    RUN_TEST(repro_lsp_language_signatures_preserve_argument_positions);
    RUN_TEST(repro_cpp_operator_edges_require_semantic_resolution);
    RUN_TEST(repro_cpp_operator_lsp_join_is_occurrence_exact);
    RUN_TEST(repro_cpp_cuda_compound_assignment_calls_are_semantic_and_occurrence_exact);
    RUN_TEST(repro_kotlin_synthetic_calls_require_semantic_resolution);
    RUN_TEST(repro_callable_reference_parallel_pipeline);
}
