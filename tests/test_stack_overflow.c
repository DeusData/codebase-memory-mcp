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
        p += sprintf(p, "function handler_%d() {\n", f);
        for (int c = 0; c < CALLS_PER; c++) {
            int idx = f * CALLS_PER + c;
            p += sprintf(p, "  func_%d();\n", idx);
        }
        p += sprintf(p, "}\n");
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
    p += sprintf(p, "def main():\n");
    for (int i = 0; i < CALL_COUNT; i++) {
        p += sprintf(p, "    func_%d()\n", i);
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
    p += sprintf(p, "package main\n\nfunc main() {\n");
    for (int i = 0; i < CALL_COUNT; i++) {
        p += sprintf(p, "\tfunc_%d()\n", i);
    }
    p += sprintf(p, "}\n");

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
    p += sprintf(p, "const express = require('express');\n");
    p += sprintf(p, "const app = express();\n\n");
    for (int i = 0; i < ROUTE_COUNT; i++) {
        p += sprintf(p, "app.get('/route_%d', handler_%d);\n", i, i);
    }
    p += sprintf(p, "\napp.listen(3000);\n");

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
        p += sprintf(p, "import { mod_%d } from './module_%d';\n", i, i);
    }
    p += sprintf(p, "console.log('done');\n");

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
    p += sprintf(p, "function main() {\n  ");
    for (int i = 0; i < DEPTH; i++) {
        p += sprintf(p, "level_%d(", i);
    }
    /* Close all parens */
    for (int i = 0; i < DEPTH; i++) {
        p += sprintf(p, ")");
    }
    p += sprintf(p, ";\n}\n");

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
        p += sprintf(p, "key_%d: value_%d\n", i, i);
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
 * Suite registration
 * ═══════════════════════════════════════════════════════════════════ */

SUITE(stack_overflow) {
    cbm_init();

    RUN_TEST(js_calls_exceed_512);
    RUN_TEST(python_calls_exceed_512);
    RUN_TEST(go_calls_exceed_1024);
    RUN_TEST(express_routes_exceed_512);
    RUN_TEST(ts_imports_exceed_512);
    RUN_TEST(js_deeply_nested_calls);
    RUN_TEST(yaml_vars_exceed_256);

    cbm_shutdown();
}
