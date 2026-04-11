/*
 * pass_tests.c — Create TESTS edges from test functions to production functions.
 *
 * Scans CALLS edges in the graph buffer: if the source function has
 * is_test=true and the target does not, creates a TESTS edge.
 *
 * Also creates TESTS_FILE edges from test File nodes to the production
 * File nodes they correspond to (naming convention: _test.go → .go, etc.)
 *
 * Depends on: pass_calls having populated CALLS edges
 */
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "graph_buffer/graph_buffer.h"
#include "foundation/log.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char *itoa_log(int val) {
    static char bufs[4][32];
    static int idx = 0;
    int i = idx;
    idx = (idx + 1) & 3;
    snprintf(bufs[i], sizeof(bufs[i]), "%d", val);
    return bufs[i];
}

/* Check if a node has is_test:true in its properties JSON. */
static bool node_is_test(const cbm_gbuf_node_t *n) {
    if (!n || !n->properties_json) {
        return false;
    }
    return strstr(n->properties_json, "\"is_test\":true") != NULL;
}

/* Helper to check suffix. */
static bool str_ends_with(const char *s, size_t slen, const char *suffix) {
    size_t sflen = strlen(suffix);
    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
    return slen >= sflen && strcmp(s + slen - sflen, suffix) == 0;
}

/* Check if a file path looks like a test file (language-agnostic). */
bool cbm_is_test_path(const char *path) {
    if (!path) {
        return false;
    }
    const char *base = strrchr(path, '/');
    base = base ? base + 1 : path;
    size_t len = strlen(path);

    /* Prefix-based: Python test_*.py */
    if (strncmp(base, "test_", 5) == 0) {
        return true;
    }

    /* Suffix-based: _test.<ext> pattern (Go, Python, Rust, C++, Lua) */
    if (str_ends_with(path, len, "_test.go") || str_ends_with(path, len, "_test.py") ||
        str_ends_with(path, len, "_test.rs") || str_ends_with(path, len, "_test.cpp") ||
        str_ends_with(path, len, "_test.lua")) {
        return true;
    }

    /* .test.<ext> / .spec.<ext> pattern (JS/TS/TSX) */
    if (strstr(path, ".test.ts") || strstr(path, ".spec.ts") || strstr(path, ".test.js") ||
        strstr(path, ".spec.js") || strstr(path, ".test.tsx") || strstr(path, ".spec.tsx")) {
        return true;
    }

    /* Name ends with "Test" or "Spec" before extension (Java, Kotlin, C#, PHP, Scala) */
    if (str_ends_with(path, len, "Test.java") || str_ends_with(path, len, "Test.kt") ||
        str_ends_with(path, len, "Test.cs") || str_ends_with(path, len, "Test.php") ||
        str_ends_with(path, len, "Spec.scala")) {
        return true;
    }

    /* Directory-based: __tests__/, tests/, test/, spec/ */
    if (strstr(path, "__tests__/") || strstr(path, "/tests/") || strstr(path, "/test/") ||
        strstr(path, "/spec/")) {
        return true;
    }
    /* Also match if path STARTS with these directories */
    if (strncmp(path, "tests/", 6) == 0 || strncmp(path, "test/", 5) == 0 ||
        strncmp(path, "spec/", 5) == 0 || strncmp(path, "__tests__/", 10) == 0) {
        return true;
    }

    /* Ruby: _spec.rb suffix */
    if (str_ends_with(path, len, "_spec.rb")) {
        return true;
    }

    return false;
}

/* Check if a function name looks like a test function (language-agnostic). */
bool cbm_is_test_func_name(const char *name) {
    if (!name) {
        return false;
    }
    /* Go: Test/Benchmark/Example + uppercase letter or end-of-string.
     * "TestFoo" = test, "Testable" = not test (lowercase after prefix). */
    if (strncmp(name, "Test", 4) == 0 && (name[4] == '\0' || (name[4] >= 'A' && name[4] <= 'Z'))) {
        return true;
    }
    if (strncmp(name, "Benchmark", 9) == 0 &&
        (name[9] == '\0' || (name[9] >= 'A' && name[9] <= 'Z'))) {
        return true;
    }
    if (strncmp(name, "Example", 7) == 0 &&
        (name[7] == '\0' || (name[7] >= 'A' && name[7] <= 'Z'))) {
        return true;
    }
    /* Python/Rust/C++/Lua/Java: test_ or test prefix (lowercase) */
    if (strncmp(name, "test_", 5) == 0) {
        return true;
    }
    if (strncmp(name, "test", 4) == 0 && name[4] >= 'A' && name[4] <= 'Z') {
        return true;
    }
    /* JS/TS: test/it/describe + lifecycle hooks */
    if (strcmp(name, "test") == 0 || strcmp(name, "it") == 0 || strcmp(name, "describe") == 0 ||
        strcmp(name, "beforeAll") == 0 || strcmp(name, "afterAll") == 0 ||
        strcmp(name, "beforeEach") == 0 || strcmp(name, "afterEach") == 0) {
        return true;
    }
    /* Julia: @testset, @test */
    if (strcmp(name, "@testset") == 0 || strcmp(name, "@test") == 0) {
        return true;
    }
    return false;
}

/* Try to derive the production file path from a test file path.
 * Returns heap-allocated string or NULL. Caller must free(). */
static char *test_to_prod_path(const char *test_path) {
    if (!test_path) {
        return NULL;
    }

    const char *base = strrchr(test_path, '/');
    const char *dir_end = base ? base : test_path;
    size_t dir_len = base ? (size_t)(base - test_path) : 0;

    base = base ? base + 1 : test_path;

    /* Go: foo_test.go → foo.go */
    const char *suffix = strstr(base, "_test.go");
    if (suffix && suffix[8] == '\0') {
        size_t name_len = suffix - base;
        char *result = malloc(dir_len + 1 + name_len + 3 + 1);
        if (dir_len > 0) {
            memcpy(result, test_path, dir_len);
            result[dir_len] = '/';
            memcpy(result + dir_len + 1, base, name_len);
            memcpy(result + dir_len + 1 + name_len, ".go", 4);
        } else {
            memcpy(result, base, name_len);
            memcpy(result + name_len, ".go", 4);
        }
        return result;
    }

    /* Python: test_foo.py → foo.py */
    if (strncmp(base, "test_", 5) == 0) {
        const char *ext = strstr(base, ".py");
        if (ext && ext[3] == '\0') {
            const char *name_start = base + 5;
            size_t name_len = ext - name_start;
            char *result = malloc(dir_len + 1 + name_len + 3 + 1);
            if (dir_len > 0) {
                memcpy(result, test_path, dir_len);
                result[dir_len] = '/';
                memcpy(result + dir_len + 1, name_start, name_len);
                memcpy(result + dir_len + 1 + name_len, ".py", 4);
            } else {
                memcpy(result, name_start, name_len);
                memcpy(result + name_len, ".py", 4);
            }
            return result;
        }
    }

    /* JS/TS: foo.test.ts → foo.ts, foo.spec.ts → foo.ts */
    for (int s = 0; s < 2; s++) {
        const char *pat = s == 0 ? ".test." : ".spec.";
        const char *found = strstr(base, pat);
        if (found) {
            size_t prefix_len = found - base;
            const char *ext = found + 5; /* skip ".test" or ".spec" (both 5 chars) */
            size_t ext_len = strlen(ext);
            char *result = malloc(dir_len + 1 + prefix_len + ext_len + 1);
            if (dir_len > 0) {
                memcpy(result, test_path, dir_len);
                result[dir_len] = '/';
                memcpy(result + dir_len + 1, base, prefix_len);
                memcpy(result + dir_len + 1 + prefix_len, ext, ext_len + 1);
            } else {
                memcpy(result, base, prefix_len);
                memcpy(result + prefix_len, ext, ext_len + 1);
            }
            return result;
        }
    }

    (void)dir_end;
    return NULL;
}

// NOLINTNEXTLINE(misc-include-cleaner) — cbm_file_info_t provided by standard header
int cbm_pipeline_pass_tests(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files, int file_count) {
    cbm_log_info("pass.start", "pass", "tests");
    (void)files;
    (void)file_count;

    int tests_count = 0;
    int tests_file_count = 0;

    /* ── TESTS edges: test function → production function ────────── */
    /* Scan CALLS edges: if src is test + target is not test → TESTS edge */
    const cbm_gbuf_edge_t **call_edges = NULL;
    int call_count = 0;
    int rc = cbm_gbuf_find_edges_by_type(ctx->gbuf, "CALLS", &call_edges, &call_count);
    if (rc == 0 && call_count > 0) {
        for (int i = 0; i < call_count; i++) {
            const cbm_gbuf_edge_t *e = call_edges[i];
            const cbm_gbuf_node_t *src = cbm_gbuf_find_by_id(ctx->gbuf, e->source_id);
            const cbm_gbuf_node_t *tgt = cbm_gbuf_find_by_id(ctx->gbuf, e->target_id);
            if (!src || !tgt) {
                continue;
            }

            /* Source must be a test, target must not */
            bool src_is_test =
                // NOLINTNEXTLINE(readability-implicit-bool-conversion)
                node_is_test(src) || (src->file_path && cbm_is_test_path(src->file_path));
            if (!src_is_test) {
                continue;
            }

            bool tgt_is_test =
                // NOLINTNEXTLINE(readability-implicit-bool-conversion)
                node_is_test(tgt) || (tgt->file_path && cbm_is_test_path(tgt->file_path));
            if (tgt_is_test) {
                continue;
            }

            /* Gate: source must have a test function name */
            if (!cbm_is_test_func_name(src->name)) {
                continue;
            }

            cbm_gbuf_insert_edge(ctx->gbuf, src->id, tgt->id, "TESTS", "{}");
            tests_count++;
        }
    }

    /* ── TESTS_FILE edges: test file → production file ───────────── */
    /* For each File node that is a test, find the corresponding prod file */
    const cbm_gbuf_node_t **file_nodes = NULL;
    int file_node_count = 0;
    rc = cbm_gbuf_find_by_label(ctx->gbuf, "File", &file_nodes, &file_node_count);
    if (rc == 0) {
        for (int i = 0; i < file_node_count; i++) {
            const cbm_gbuf_node_t *fnode = file_nodes[i];
            if (!fnode->file_path || !cbm_is_test_path(fnode->file_path)) {
                continue;
            }

            char *prod_path = test_to_prod_path(fnode->file_path);
            if (!prod_path) {
                continue;
            }

            /* Find the production file node */
            char *prod_qn = cbm_pipeline_fqn_compute(ctx->project_name, prod_path, "__file__");
            const cbm_gbuf_node_t *prod_node = cbm_gbuf_find_by_qn(ctx->gbuf, prod_qn);
            free(prod_qn);
            free(prod_path);

            if (prod_node && fnode->id != prod_node->id) {
                cbm_gbuf_insert_edge(ctx->gbuf, fnode->id, prod_node->id, "TESTS_FILE", "{}");
                tests_file_count++;
            }
        }
    }

    cbm_log_info("pass.done", "pass", "tests", "tests", itoa_log(tests_count), "tests_file",
                 itoa_log(tests_file_count));
    return 0;
}
