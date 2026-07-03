/*
 * test_stack_overflow.c — Regression tests for GitHub issue #199.
 *
 * Verifies that extraction functions do NOT silently drop AST nodes
 * when files exceed the fixed traversal stack capacity (512 for calls,
 * 256 for variables, 64 for Elixir, etc.).
 *
 * These tests generate source code with more call sites / definitions /
 * imports than the stack cap, then assert the extraction count matches
 * the expected total. Before the fix, counts plateau at the cap.
 */
#include "test_framework.h"
#include "cbm.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* tree-sitter runtime allocator hooks (ts_runtime/src/alloc.h, TS_PUBLIC) and
 * mimalloc (vendored) — for the #424 allocator-binding regression test. */
extern void *(*ts_current_malloc)(size_t);
extern void *(*ts_current_calloc)(size_t, size_t);
extern void *(*ts_current_realloc)(void *, size_t);
extern void (*ts_current_free)(void *);
extern void *mi_malloc(size_t);
extern void *mi_calloc(size_t, size_t);
extern void *mi_realloc(void *, size_t);
extern void mi_free(void *);

/* ── Helpers ───────────────────────────────────────────────────── */

static CBMFileResult *extract(const char *src, CBMLanguage lang, const char *proj,
                              const char *path) {
    CBMFileResult *r = cbm_extract_file(src, (int)strlen(src), lang, proj, path, 0, NULL, NULL);
    return r;
}

/* ═══════════════════════════════════════════════════════════════════
 * Test: JavaScript calls exceeding 512 stack cap
 *
 * Generates a JS function with 600 unique function calls.
 * Before fix: walk_calls() stops at ~512 due to CALLS_STACK_CAP.
 * After fix: all 600 calls are extracted.
 * ═══════════════════════════════════════════════════════════════════ */

TEST(js_calls_exceed_512) {
    const int CALL_COUNT = 600;
    /* Generate calls spread across many small functions to create wide AST.
     * Each function has ~20 calls, 30 functions = 600 calls total.
     * The DFS stack must hold sibling function nodes simultaneously. */
    const int FUNCS = 30;
    const int CALLS_PER = CALL_COUNT / FUNCS;
    size_t buf_sz = 256 + (size_t)CALL_COUNT * 48;
    char *src = malloc(buf_sz);
    ASSERT_NOT_NULL(src);

    char *p = src;
    for (int f = 0; f < FUNCS; f++) {
        p += snprintf(p, (size_t)(buf_sz - (size_t)(p - src)), "function handler_%d() {\n", f);
        for (int c = 0; c < CALLS_PER; c++) {
            int idx = f * CALLS_PER + c;
            p += snprintf(p, (size_t)(buf_sz - (size_t)(p - src)), "  func_%d();\n", idx);
        }
        p += snprintf(p, (size_t)(buf_sz - (size_t)(p - src)), "}\n");
    }

    CBMFileResult *r = extract(src, CBM_LANG_JAVASCRIPT, "test", "many_calls.js");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    /* Count calls that match our generated pattern */
    int matched = 0;
    for (int i = 0; i < r->calls.count; i++) {
        if (strncmp(r->calls.items[i].callee_name, "func_", 5) == 0) {
            matched++;
        }
    }

    printf("    calls extracted: %d / %d expected\n", matched, CALL_COUNT);
    ASSERT_EQ(matched, CALL_COUNT);

    cbm_free_result(r);
    free(src);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 * Test: Python calls exceeding 512 stack cap
 *
 * Same test in Python syntax to verify language-independence.
 * ═══════════════════════════════════════════════════════════════════ */

TEST(python_calls_exceed_512) {
    const int CALL_COUNT = 600;
    size_t buf_sz = 256 + (size_t)CALL_COUNT * 32;
    char *src = malloc(buf_sz);
    ASSERT_NOT_NULL(src);

    char *p = src;
    p += snprintf(p, (size_t)(buf_sz - (size_t)(p - src)), "def main():\n");
    for (int i = 0; i < CALL_COUNT; i++) {
        p += snprintf(p, (size_t)(buf_sz - (size_t)(p - src)), "    func_%d()\n", i);
    }

    CBMFileResult *r = extract(src, CBM_LANG_PYTHON, "test", "many_calls.py");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    int matched = 0;
    for (int i = 0; i < r->calls.count; i++) {
        if (strncmp(r->calls.items[i].callee_name, "func_", 5) == 0) {
            matched++;
        }
    }

    printf("    calls extracted: %d / %d expected\n", matched, CALL_COUNT);
    ASSERT_EQ(matched, CALL_COUNT);

    cbm_free_result(r);
    free(src);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 * Test: Go calls at exactly 1024 (well past 512 cap)
 *
 * Larger count to ensure the fix handles 2x overflow gracefully.
 * ═══════════════════════════════════════════════════════════════════ */

TEST(go_calls_exceed_1024) {
    const int CALL_COUNT = 1024;
    size_t buf_sz = 256 + (size_t)CALL_COUNT * 40;
    char *src = malloc(buf_sz);
    ASSERT_NOT_NULL(src);

    char *p = src;
    p += snprintf(p, (size_t)(buf_sz - (size_t)(p - src)), "package main\n\nfunc main() {\n");
    for (int i = 0; i < CALL_COUNT; i++) {
        p += snprintf(p, (size_t)(buf_sz - (size_t)(p - src)), "\tfunc_%d()\n", i);
    }
    p += snprintf(p, (size_t)(buf_sz - (size_t)(p - src)), "}\n");

    CBMFileResult *r = extract(src, CBM_LANG_GO, "test", "many_calls.go");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    int matched = 0;
    for (int i = 0; i < r->calls.count; i++) {
        if (strncmp(r->calls.items[i].callee_name, "func_", 5) == 0) {
            matched++;
        }
    }

    printf("    calls extracted: %d / %d expected\n", matched, CALL_COUNT);
    ASSERT_EQ(matched, CALL_COUNT);

    cbm_free_result(r);
    free(src);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 * Test: Express-style route file (original reporter's scenario)
 *
 * ~150 route definitions — the actual use case from issue #199.
 * Each route has a handler call, so both defs and calls matter.
 * ═══════════════════════════════════════════════════════════════════ */

TEST(express_routes_exceed_512) {
    const int ROUTE_COUNT = 150;
    /* Each route: app.get('/route_NNN', handler_NNN); */
    size_t buf_sz = 512 + (size_t)ROUTE_COUNT * 80;
    char *src = malloc(buf_sz);
    ASSERT_NOT_NULL(src);

    char *p = src;
    p += snprintf(p, (size_t)(buf_sz - (size_t)(p - src)), "const express = require('express');\n");
    p += snprintf(p, (size_t)(buf_sz - (size_t)(p - src)), "const app = express();\n\n");
    for (int i = 0; i < ROUTE_COUNT; i++) {
        p += snprintf(p, (size_t)(buf_sz - (size_t)(p - src)),
                      "app.get('/route_%d', handler_%d);\n", i, i);
    }
    p += snprintf(p, (size_t)(buf_sz - (size_t)(p - src)), "\napp.listen(3000);\n");

    CBMFileResult *r = extract(src, CBM_LANG_JAVASCRIPT, "test", "routes.js");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    /* Each app.get() is a call — count calls containing "get" */
    int get_calls = 0;
    for (int i = 0; i < r->calls.count; i++) {
        if (strstr(r->calls.items[i].callee_name, "get") != NULL) {
            get_calls++;
        }
    }

    printf("    route calls extracted: %d / %d expected\n", get_calls, ROUTE_COUNT);
    ASSERT_EQ(get_calls, ROUTE_COUNT);

    cbm_free_result(r);
    free(src);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 * Test: ES imports exceeding 512 (walk_es_imports uses same cap)
 *
 * Generate a TS file with 600 import statements.
 * ═══════════════════════════════════════════════════════════════════ */

TEST(ts_imports_exceed_512) {
    const int IMPORT_COUNT = 600;
    size_t buf_sz = 256 + (size_t)IMPORT_COUNT * 64;
    char *src = malloc(buf_sz);
    ASSERT_NOT_NULL(src);

    char *p = src;
    for (int i = 0; i < IMPORT_COUNT; i++) {
        p += snprintf(p, (size_t)(buf_sz - (size_t)(p - src)),
                      "import { mod_%d } from './module_%d';\n", i, i);
    }
    p += snprintf(p, (size_t)(buf_sz - (size_t)(p - src)), "console.log('done');\n");

    CBMFileResult *r = extract(src, CBM_LANG_TYPESCRIPT, "test", "many_imports.ts");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    printf("    imports extracted: %d / %d expected\n", r->imports.count, IMPORT_COUNT);
    ASSERT_GTE(r->imports.count, IMPORT_COUNT);

    cbm_free_result(r);
    free(src);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 * Test: Deeply nested calls (tests stack depth, not just breadth)
 *
 * Generates nested function calls: a(b(c(d(e(...)))))
 * A deep call chain can overflow the stack even with fewer total nodes.
 * ═══════════════════════════════════════════════════════════════════ */

TEST(js_deeply_nested_calls) {
    const int DEPTH = 200;
    /* Build: outermost( level_0( level_1( ... level_199() ... ))) */
    size_t buf_sz = 256 + (size_t)DEPTH * 32;
    char *src = malloc(buf_sz);
    ASSERT_NOT_NULL(src);

    char *p = src;
    p += snprintf(p, (size_t)(buf_sz - (size_t)(p - src)), "function main() {\n  ");
    for (int i = 0; i < DEPTH; i++) {
        p += snprintf(p, (size_t)(buf_sz - (size_t)(p - src)), "level_%d(", i);
    }
    /* Close all parens */
    for (int i = 0; i < DEPTH; i++) {
        p += snprintf(p, (size_t)(buf_sz - (size_t)(p - src)), ")");
    }
    p += snprintf(p, (size_t)(buf_sz - (size_t)(p - src)), ";\n}\n");

    CBMFileResult *r = extract(src, CBM_LANG_JAVASCRIPT, "test", "nested_calls.js");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    int matched = 0;
    for (int i = 0; i < r->calls.count; i++) {
        if (strncmp(r->calls.items[i].callee_name, "level_", 6) == 0) {
            matched++;
        }
    }

    printf("    nested calls extracted: %d / %d expected\n", matched, DEPTH);
    ASSERT_EQ(matched, DEPTH);

    cbm_free_result(r);
    free(src);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 * Test: YAML variables exceeding 256 (walk_variables_iter cap)
 *
 * Generate a YAML file with 300 top-level keys.
 * ═══════════════════════════════════════════════════════════════════ */

TEST(yaml_vars_exceed_256) {
    const int VAR_COUNT = 300;
    size_t buf_sz = (size_t)VAR_COUNT * 32;
    char *src = malloc(buf_sz);
    ASSERT_NOT_NULL(src);

    char *p = src;
    for (int i = 0; i < VAR_COUNT; i++) {
        p += snprintf(p, (size_t)(buf_sz - (size_t)(p - src)), "key_%d: value_%d\n", i, i);
    }

    CBMFileResult *r = extract(src, CBM_LANG_YAML, "test", "many_keys.yaml");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    int var_count = 0;
    for (int i = 0; i < r->defs.count; i++) {
        if (strcmp(r->defs.items[i].label, "Variable") == 0) {
            var_count++;
        }
    }

    printf("    YAML vars extracted: %d / %d expected\n", var_count, VAR_COUNT);
    ASSERT_EQ(var_count, VAR_COUNT);

    cbm_free_result(r);
    free(src);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 * Test: tree-sitter runtime allocator is bound to cbm's allocator (#424)
 *
 * cbm_init() must bind the vendored tree-sitter runtime to mimalloc via
 * ts_set_allocator(). Otherwise the runtime allocates through its overridable
 * ts_current_malloc/free defaults (plain malloc/free); under the production
 * MI_OVERRIDE=1 build (esp. Windows static-MinGW + --allow-multiple-definition)
 * ts_malloc and ts_free can resolve to different allocators, corrupting the
 * heap and crashing mid-parse on large templated C++ headers. Asserting the
 * binding reproduces the mismatch CONDITION deterministically on every
 * platform: RED before the fix (ts_current_free == plain free), GREEN after.
 * ═══════════════════════════════════════════════════════════════════ */

TEST(ts_allocator_bound_to_mimalloc_issue424) {
    cbm_init();
#if defined(CBM_BIND_TS_ALLOCATOR) && CBM_BIND_TS_ALLOCATOR
    /* Production build: cbm_init must have bound the ts runtime to mimalloc so
     * ts_malloc and ts_free can never resolve to different allocators (#424). */
    ASSERT_TRUE(ts_current_malloc == mi_malloc);
    ASSERT_TRUE(ts_current_free == mi_free);
#else
    /* Test build (MI_OVERRIDE=0, CRT + ASan): binding is intentionally OFF (it
     * would mismatch ASan/CRT frees). The crash only affects the prod binary;
     * its reproduction runs in the prod smoke job. Independently verify the fix
     * MECHANISM — ts_set_allocator wires the runtime hooks — then restore the
     * CRT defaults so the rest of this build stays allocator-consistent. */
    void *(*save_m)(size_t) = ts_current_malloc;
    void *(*save_c)(size_t, size_t) = ts_current_calloc;
    void *(*save_r)(void *, size_t) = ts_current_realloc;
    void (*save_f)(void *) = ts_current_free;
    ts_set_allocator(mi_malloc, mi_calloc, mi_realloc, mi_free);
    int wired = (ts_current_malloc == mi_malloc) && (ts_current_free == mi_free);
    ts_set_allocator(save_m, save_c, save_r, save_f); /* restore BEFORE asserting */
    ASSERT_TRUE(wired);
    ASSERT_TRUE(ts_current_free == save_f);
#endif
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 * Test: large templated C++ header extraction does not crash (#424)
 *
 * Generates a json.hpp-style header (hundreds of templated structs with
 * operator overloads) — the parse path that SEGV'd in #424. Asserts
 * extraction completes. (In the MI_OVERRIDE=0 test build the allocator is
 * consistent so this cannot fault here; it guards extraction on large
 * templated input and addresses the C++ gap in this file. The production-
 * binary crash reproduction runs in the smoke job.)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(cpp_large_templated_header_no_crash_issue424) {
    const int N = 400; /* ~400 templated structs → ~2400 lines */
    size_t buf_sz = (size_t)N * 600;
    char *src = malloc(buf_sz);
    ASSERT_NOT_NULL(src);
    char *p = src;
    p += snprintf(p, buf_sz, "#include <cstddef>\nnamespace ns {\n");
    for (int i = 0; i < N; i++) {
        p += snprintf(p, (size_t)(buf_sz - (size_t)(p - src)),
                      "template <typename T> struct Box%d {\n"
                      "  T value;\n"
                      "  bool operator<(const Box%d &o) const { return value < o.value; }\n"
                      "  bool operator==(const Box%d &o) const { return value == o.value; }\n"
                      "  T get() const { return value; }\n"
                      "};\n",
                      i, i, i);
    }
    snprintf(p, (size_t)(buf_sz - (size_t)(p - src)), "}\n");

    CBMFileResult *r = extract(src, CBM_LANG_CPP, "test", "templated.hpp");
    ASSERT_NOT_NULL(r); /* no crash is the real assertion */
    cbm_free_result(r);
    free(src);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 * LSP resolve recursion guards (perf-sweep crashes, 2026-06-10)
 *
 * Indexing real OSS crashed in the LSP RESOLVE walks (distinct from the
 * extraction-walk caps above): elasticsearch → SIGSEGV under deep recursive
 * java_resolve_calls_in_node frames (bind_lambda_args), bitcoin → SIGSEGV
 * under deep c_resolve_calls_in_node frames (cbm_type_substitute via
 * c_adl_resolve), microsoft/TypeScript → SIGBUS under an unbounded
 * lookup_member_type cycle. The walks now carry depth guards; these
 * reproductions fork a child so a regression cannot kill the test runner
 * (the TS cyclic-type shape is only reachable with a real cross-file
 * registry, so that one is verified at the real-repo tier; the synthetic
 * cyclic fixture here guards the in-file path).
 * ═══════════════════════════════════════════════════════════════════ */

#if !defined(_WIN32)
#include <sys/wait.h>
#include <unistd.h>
#endif

/* Run cbm_extract_file in a forked child; true if the child died by signal.
 * Mirrors tests/test_lang_contract.c. On Windows run in-process (a genuine
 * crash there aborts the runner — hard, visible failure). */
static bool so_extract_crashes(const char *content, CBMLanguage lang, const char *relpath) {
#if defined(_WIN32)
    CBMFileResult *r =
        cbm_extract_file(content, (int)strlen(content), lang, "so", relpath, 0, NULL, NULL);
    if (r) {
        cbm_free_result(r);
    }
    return false;
#else
    fflush(NULL);
    pid_t pid = fork();
    if (pid < 0) {
        return false;
    }
    if (pid == 0) {
        CBMFileResult *r =
            cbm_extract_file(content, (int)strlen(content), lang, "so", relpath, 0, NULL, NULL);
        if (r) {
            cbm_free_result(r);
        }
        _exit(0);
    }
    int status = 0;
    (void)waitpid(pid, &status, 0);
    return WIFSIGNALED(status);
#endif
}

TEST(lsp_java_deep_nesting_no_crash) {
    /* Deeply nested call expressions — the same shape as the elasticsearch
     * crash (fast SIGSEGV under recursive java_resolve_calls_in_node frames;
     * pre-guard prod probe: rc=139 in under a second at this depth). Nested
     * BLOCKS are deliberately not used: java block processing is minutes-slow
     * at depth >=3000 (separate adversarial-input pathology, documented in
     * the perf-sweep report). */
    const int DEPTH = 30000;
    size_t sz = (size_t)DEPTH * 3 + 256;
    char *src = malloc(sz);
    ASSERT_NOT_NULL(src);
    char *p = src;
    p += snprintf(p, sz, "class X { static int f(int a) { return a; } static int g() { return ");
    for (int i = 0; i < DEPTH; i++) {
        *p++ = 'f';
        *p++ = '(';
    }
    *p++ = '1';
    memset(p, ')', DEPTH);
    p += DEPTH;
    snprintf(p, sz - (size_t)(p - src), "; } }\n");
    ASSERT_FALSE(so_extract_crashes(src, CBM_LANG_JAVA, "X.java"));
    free(src);
    PASS();
}

TEST(lsp_cpp_deep_expression_no_crash) {
    /* See lsp_java_deep_nesting_no_crash on the depth choice. */
    const int DEPTH = 30000;
    size_t sz = (size_t)DEPTH * 3 + 256;
    char *src = malloc(sz);
    ASSERT_NOT_NULL(src);
    char *p = src;
    p += snprintf(p, sz, "int f(int x) { return x; }\nint g() { return ");
    for (int i = 0; i < DEPTH; i++) {
        *p++ = 'f';
        *p++ = '(';
    }
    *p++ = '1';
    memset(p, ')', DEPTH);
    p += DEPTH;
    snprintf(p, sz - (size_t)(p - src), "; }\n");
    ASSERT_FALSE(so_extract_crashes(src, CBM_LANG_CPP, "deep.cpp"));
    free(src);
    PASS();
}

TEST(lsp_java_lambda_args_exceed_params_no_crash) {
    /* A call with MORE arguments than the resolved method's declared params:
     * bind_lambda_args indexed the NULL-terminated signature param_types array
     * by the call-site argument index, reading past the terminator — a garbage
     * CBMType* then got dereferenced (elasticsearch SIGSEGV, java_lsp.c:2364
     * via :2722; same OOB family as #427). */
    const char *src = "class A {\n"
                      "    void run(Runnable r) {}\n"
                      "    void go() {\n"
                      "        run(() -> {}, () -> {}, () -> {}, () -> {}, () -> {}, () -> {});\n"
                      "    }\n"
                      "}\n";
    ASSERT_FALSE(so_extract_crashes(src, CBM_LANG_JAVA, "A.java"));
    PASS();
}

TEST(lsp_ts_cyclic_types_no_crash) {
    const char *src = "type A = B | null;\n"
                      "type B = A | number;\n"
                      "interface C extends D { c: number; }\n"
                      "interface D extends C { d: number; }\n"
                      "declare const a: A;\n"
                      "declare const c: C;\n"
                      "function useIt(p: C) { return p.missing_member; }\n"
                      "const y = c.also_missing;\n";
    ASSERT_FALSE(so_extract_crashes(src, CBM_LANG_TYPESCRIPT, "cycle.ts"));
    PASS();
}

/* ─── Deeply-nested calls drive the per-language LSP resolve walkers into
 * per-nesting-level native recursion. Unguarded, these SIGSEGV and take down
 * the whole index. Each walker now has a walk_depth cap (CBM_LSP_MAX_WALK_DEPTH,
 * env-overridable) that skips the too-deep subtree — graceful degradation.
 * These mirror lsp_java_deep_nesting_no_crash: same fixture shape, one per
 * previously-unguarded walker (py_resolve_calls_in, resolve_calls_in_node[go],
 * php_resolve_calls_in_node, kt_resolve_calls_in_node). RED proof: run the
 * suite with CBM_LSP_MAX_WALK_DEPTH set huge (disabling only these caps) and
 * each of these four SIGSEGVs — proving the guard, not the fixture, is what
 * keeps them green. ─── */

TEST(lsp_python_deep_nesting_no_crash) {
    /* py_resolve_calls_in recurses per nesting level; see the Java analog. */
    const int DEPTH = 30000;
    size_t sz = (size_t)DEPTH * 3 + 256;
    char *src = malloc(sz);
    ASSERT_NOT_NULL(src);
    char *p = src;
    p += snprintf(p, sz, "def f(a):\n    return a\ndef g():\n    return ");
    for (int i = 0; i < DEPTH; i++) {
        *p++ = 'f';
        *p++ = '(';
    }
    *p++ = '1';
    memset(p, ')', DEPTH);
    p += DEPTH;
    snprintf(p, sz - (size_t)(p - src), "\n");
    ASSERT_FALSE(so_extract_crashes(src, CBM_LANG_PYTHON, "deep.py"));
    free(src);
    PASS();
}

TEST(lsp_go_deep_nesting_no_crash) {
    /* resolve_calls_in_node recurses per nesting level; see the Java analog. */
    const int DEPTH = 30000;
    size_t sz = (size_t)DEPTH * 3 + 256;
    char *src = malloc(sz);
    ASSERT_NOT_NULL(src);
    char *p = src;
    p += snprintf(p, sz, "package p\nfunc f(a int) int { return a }\nfunc g() int { return ");
    for (int i = 0; i < DEPTH; i++) {
        *p++ = 'f';
        *p++ = '(';
    }
    *p++ = '1';
    memset(p, ')', DEPTH);
    p += DEPTH;
    snprintf(p, sz - (size_t)(p - src), " }\n");
    ASSERT_FALSE(so_extract_crashes(src, CBM_LANG_GO, "deep.go"));
    free(src);
    PASS();
}

TEST(lsp_php_deep_nesting_no_crash) {
    /* php_resolve_calls_in_node recurses per nesting level; Java analog. */
    const int DEPTH = 30000;
    size_t sz = (size_t)DEPTH * 3 + 256;
    char *src = malloc(sz);
    ASSERT_NOT_NULL(src);
    char *p = src;
    p += snprintf(p, sz, "<?php\nfunction f($a) { return $a; }\nfunction g() { return ");
    for (int i = 0; i < DEPTH; i++) {
        *p++ = 'f';
        *p++ = '(';
    }
    *p++ = '1';
    memset(p, ')', DEPTH);
    p += DEPTH;
    snprintf(p, sz - (size_t)(p - src), "; }\n");
    ASSERT_FALSE(so_extract_crashes(src, CBM_LANG_PHP, "deep.php"));
    free(src);
    PASS();
}

TEST(lsp_kotlin_deep_nesting_no_crash) {
    /* kt_resolve_calls_in_node recurses per nesting level; Java analog. */
    const int DEPTH = 30000;
    size_t sz = (size_t)DEPTH * 3 + 256;
    char *src = malloc(sz);
    ASSERT_NOT_NULL(src);
    char *p = src;
    p += snprintf(p, sz, "fun f(a: Int): Int { return a }\nfun g(): Int { return ");
    for (int i = 0; i < DEPTH; i++) {
        *p++ = 'f';
        *p++ = '(';
    }
    *p++ = '1';
    memset(p, ')', DEPTH);
    p += DEPTH;
    snprintf(p, sz - (size_t)(p - src), " }\n");
    ASSERT_FALSE(so_extract_crashes(src, CBM_LANG_KOTLIN, "deep.kt"));
    free(src);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 * Suite registration
 * ═══════════════════════════════════════════════════════════════════ */

SUITE(stack_overflow) {
    cbm_init();

    RUN_TEST(ts_allocator_bound_to_mimalloc_issue424);
    RUN_TEST(cpp_large_templated_header_no_crash_issue424);
    RUN_TEST(lsp_java_deep_nesting_no_crash);
    RUN_TEST(lsp_java_lambda_args_exceed_params_no_crash);
    RUN_TEST(lsp_cpp_deep_expression_no_crash);
    RUN_TEST(lsp_ts_cyclic_types_no_crash);
    RUN_TEST(lsp_python_deep_nesting_no_crash);
    RUN_TEST(lsp_go_deep_nesting_no_crash);
    RUN_TEST(lsp_php_deep_nesting_no_crash);
    RUN_TEST(lsp_kotlin_deep_nesting_no_crash);

    RUN_TEST(js_calls_exceed_512);
    RUN_TEST(python_calls_exceed_512);
    RUN_TEST(go_calls_exceed_1024);
    RUN_TEST(express_routes_exceed_512);
    RUN_TEST(ts_imports_exceed_512);
    RUN_TEST(js_deeply_nested_calls);
    RUN_TEST(yaml_vars_exceed_256);

    cbm_shutdown();
}
