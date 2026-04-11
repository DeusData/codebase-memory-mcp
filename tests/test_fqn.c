/*
 * test_fqn.c -- Tests for FQN (Fully Qualified Name) computation.
 *
 * Covers: cbm_pipeline_fqn_compute, cbm_pipeline_fqn_module,
 *         cbm_pipeline_fqn_folder, cbm_project_name_from_path.
 */
#include "test_framework.h"
#include "../src/pipeline/pipeline.h"

#include <stdlib.h>
#include <string.h>

/* ── Helper: assert FQN result and free ────────────────────────── */

#define ASSERT_FQN(expr, expected)    \
    do {                              \
        char *_r = (expr);            \
        ASSERT_NOT_NULL(_r);          \
        ASSERT_STR_EQ(_r, expected);  \
        free(_r);                     \
    } while (0)

/* ================================================================
 * cbm_pipeline_fqn_compute
 * ================================================================ */

/* ── Basic: project + path + name ─────────────────────────────── */

TEST(fqn_compute_basic_go) {
    ASSERT_FQN(cbm_pipeline_fqn_compute("myproj", "main.go", "main"), "myproj.main.main");
    PASS();
}

TEST(fqn_compute_basic_py) {
    ASSERT_FQN(cbm_pipeline_fqn_compute("proj", "app.py", "run"), "proj.app.run");
    PASS();
}

TEST(fqn_compute_basic_ts) {
    ASSERT_FQN(cbm_pipeline_fqn_compute("proj", "server.ts", "handler"), "proj.server.handler");
    PASS();
}

TEST(fqn_compute_basic_js) {
    ASSERT_FQN(cbm_pipeline_fqn_compute("proj", "util.js", "parse"), "proj.util.parse");
    PASS();
}

TEST(fqn_compute_basic_c) {
    ASSERT_FQN(cbm_pipeline_fqn_compute("proj", "core.c", "init"), "proj.core.init");
    PASS();
}

TEST(fqn_compute_basic_rs) {
    ASSERT_FQN(cbm_pipeline_fqn_compute("proj", "lib.rs", "new"), "proj.lib.new");
    PASS();
}

/* ── Nested paths ─────────────────────────────────────────────── */

TEST(fqn_compute_nested_two_levels) {
    ASSERT_FQN(cbm_pipeline_fqn_compute("proj", "src/pkg/module.go", "FuncName"),
               "proj.src.pkg.module.FuncName");
    PASS();
}

TEST(fqn_compute_nested_three_levels) {
    ASSERT_FQN(cbm_pipeline_fqn_compute("proj", "a/b/c/file.py", "Class"),
               "proj.a.b.c.file.Class");
    PASS();
}

TEST(fqn_compute_nested_deep) {
    ASSERT_FQN(cbm_pipeline_fqn_compute("proj", "a/b/c/d/e/f/g.ts", "fn"),
               "proj.a.b.c.d.e.f.g.fn");
    PASS();
}

/* ── Python __init__.py ───────────────────────────────────────── */

TEST(fqn_compute_init_py_with_name) {
    /* __init__ stripped when name is provided */
    ASSERT_FQN(cbm_pipeline_fqn_compute("proj", "pkg/__init__.py", "MyClass"),
               "proj.pkg.MyClass");
    PASS();
}

TEST(fqn_compute_init_py_without_name) {
    /* __init__ kept when no name (module QN for the file itself) */
    ASSERT_FQN(cbm_pipeline_fqn_compute("proj", "pkg/__init__.py", NULL),
               "proj.pkg.__init__");
    PASS();
}

TEST(fqn_compute_init_py_empty_name) {
    /* Empty string name also keeps __init__ */
    ASSERT_FQN(cbm_pipeline_fqn_compute("proj", "pkg/__init__.py", ""),
               "proj.pkg.__init__");
    PASS();
}

TEST(fqn_compute_init_py_nested) {
    ASSERT_FQN(cbm_pipeline_fqn_compute("proj", "a/b/__init__.py", "Foo"),
               "proj.a.b.Foo");
    PASS();
}

TEST(fqn_compute_init_py_root) {
    /* __init__.py at root with name */
    ASSERT_FQN(cbm_pipeline_fqn_compute("proj", "__init__.py", "X"),
               "proj.X");
    PASS();
}

TEST(fqn_compute_init_py_root_no_name) {
    /* __init__.py at root without name -- only project + __init__ (seg_count=2 > 1) */
    ASSERT_FQN(cbm_pipeline_fqn_compute("proj", "__init__.py", NULL),
               "proj.__init__");
    PASS();
}

/* ── JS/TS index files ────────────────────────────────────────── */

TEST(fqn_compute_index_js_with_name) {
    ASSERT_FQN(cbm_pipeline_fqn_compute("proj", "pkg/index.js", "render"),
               "proj.pkg.render");
    PASS();
}

TEST(fqn_compute_index_js_without_name) {
    ASSERT_FQN(cbm_pipeline_fqn_compute("proj", "pkg/index.js", NULL),
               "proj.pkg.index");
    PASS();
}

TEST(fqn_compute_index_ts_with_name) {
    ASSERT_FQN(cbm_pipeline_fqn_compute("proj", "src/index.ts", "App"),
               "proj.src.App");
    PASS();
}

TEST(fqn_compute_index_ts_without_name) {
    ASSERT_FQN(cbm_pipeline_fqn_compute("proj", "src/index.ts", NULL),
               "proj.src.index");
    PASS();
}

TEST(fqn_compute_index_ts_empty_name) {
    ASSERT_FQN(cbm_pipeline_fqn_compute("proj", "lib/index.ts", ""),
               "proj.lib.index");
    PASS();
}

TEST(fqn_compute_index_root_with_name) {
    ASSERT_FQN(cbm_pipeline_fqn_compute("proj", "index.js", "main"),
               "proj.main");
    PASS();
}

TEST(fqn_compute_index_root_no_name) {
    ASSERT_FQN(cbm_pipeline_fqn_compute("proj", "index.js", NULL),
               "proj.index");
    PASS();
}

/* ── Empty / NULL parameters ──────────────────────────────────── */

TEST(fqn_compute_empty_rel_path) {
    ASSERT_FQN(cbm_pipeline_fqn_compute("proj", "", "func"), "proj.func");
    PASS();
}

TEST(fqn_compute_empty_name) {
    ASSERT_FQN(cbm_pipeline_fqn_compute("proj", "mod.go", ""), "proj.mod");
    PASS();
}

TEST(fqn_compute_both_empty) {
    ASSERT_FQN(cbm_pipeline_fqn_compute("proj", "", ""), "proj");
    PASS();
}

TEST(fqn_compute_null_project) {
    ASSERT_FQN(cbm_pipeline_fqn_compute(NULL, "foo.go", "bar"), "");
    PASS();
}

TEST(fqn_compute_null_rel_path) {
    ASSERT_FQN(cbm_pipeline_fqn_compute("proj", NULL, "fn"), "proj.fn");
    PASS();
}

TEST(fqn_compute_null_name) {
    ASSERT_FQN(cbm_pipeline_fqn_compute("proj", "mod.go", NULL), "proj.mod");
    PASS();
}

TEST(fqn_compute_all_null) {
    ASSERT_FQN(cbm_pipeline_fqn_compute(NULL, NULL, NULL), "");
    PASS();
}

TEST(fqn_compute_null_project_null_path) {
    ASSERT_FQN(cbm_pipeline_fqn_compute(NULL, NULL, "fn"), "");
    PASS();
}

/* ── Backslash paths (Windows) ────────────────────────────────── */

TEST(fqn_compute_backslash_simple) {
    ASSERT_FQN(cbm_pipeline_fqn_compute("proj", "src\\main.go", "run"),
               "proj.src.main.run");
    PASS();
}

TEST(fqn_compute_backslash_nested) {
    ASSERT_FQN(cbm_pipeline_fqn_compute("proj", "a\\b\\c\\file.py", "X"),
               "proj.a.b.c.file.X");
    PASS();
}

TEST(fqn_compute_backslash_mixed) {
    ASSERT_FQN(cbm_pipeline_fqn_compute("proj", "a/b\\c/d.ts", "fn"),
               "proj.a.b.c.d.fn");
    PASS();
}

/* ── Multiple extensions ──────────────────────────────────────── */

TEST(fqn_compute_double_ext) {
    /* Only last extension stripped: foo.test.ts -> foo.test */
    ASSERT_FQN(cbm_pipeline_fqn_compute("proj", "foo.test.ts", "bar"),
               "proj.foo.test.bar");
    PASS();
}

TEST(fqn_compute_spec_ext) {
    ASSERT_FQN(cbm_pipeline_fqn_compute("proj", "util.spec.js", "it"),
               "proj.util.spec.it");
    PASS();
}

/* ── Leading / trailing slashes ───────────────────────────────── */

TEST(fqn_compute_leading_slash) {
    /* Leading slash produces empty segment which is skipped */
    ASSERT_FQN(cbm_pipeline_fqn_compute("proj", "/src/main.go", "fn"),
               "proj.src.main.fn");
    PASS();
}

TEST(fqn_compute_trailing_slash) {
    /* Trailing slash: path becomes empty after last /, extension strip is no-op */
    ASSERT_FQN(cbm_pipeline_fqn_compute("proj", "src/", "fn"),
               "proj.src.fn");
    PASS();
}

TEST(fqn_compute_double_slash) {
    ASSERT_FQN(cbm_pipeline_fqn_compute("proj", "a//b.go", "fn"),
               "proj.a.b.fn");
    PASS();
}

/* ── No extension ─────────────────────────────────────────────── */

TEST(fqn_compute_no_ext) {
    ASSERT_FQN(cbm_pipeline_fqn_compute("proj", "Makefile", "target"),
               "proj.Makefile.target");
    PASS();
}

/* ── Project-only (no path, no name) ──────────────────────────── */

TEST(fqn_compute_project_only) {
    ASSERT_FQN(cbm_pipeline_fqn_compute("proj", NULL, NULL), "proj");
    PASS();
}

/* ── Non-init/index filenames that start similarly ────────────── */

TEST(fqn_compute_init_not_stripped) {
    /* __init_data__ is NOT __init__, should not be stripped */
    ASSERT_FQN(cbm_pipeline_fqn_compute("proj", "pkg/__init_data__.py", "F"),
               "proj.pkg.__init_data__.F");
    PASS();
}

TEST(fqn_compute_index2_not_stripped) {
    /* "indexer" is NOT "index", should not be stripped */
    ASSERT_FQN(cbm_pipeline_fqn_compute("proj", "pkg/indexer.ts", "F"),
               "proj.pkg.indexer.F");
    PASS();
}

/* ================================================================
 * cbm_pipeline_fqn_module
 * ================================================================ */

TEST(fqn_module_basic) {
    ASSERT_FQN(cbm_pipeline_fqn_module("proj", "src/app.py"), "proj.src.app");
    PASS();
}

TEST(fqn_module_go) {
    ASSERT_FQN(cbm_pipeline_fqn_module("proj", "cmd/server.go"), "proj.cmd.server");
    PASS();
}

TEST(fqn_module_init_py) {
    /* fqn_module passes NULL name -> __init__ kept */
    ASSERT_FQN(cbm_pipeline_fqn_module("proj", "pkg/__init__.py"), "proj.pkg.__init__");
    PASS();
}

TEST(fqn_module_index_js) {
    ASSERT_FQN(cbm_pipeline_fqn_module("proj", "components/index.js"), "proj.components.index");
    PASS();
}

TEST(fqn_module_empty_path) {
    ASSERT_FQN(cbm_pipeline_fqn_module("proj", ""), "proj");
    PASS();
}

TEST(fqn_module_null_path) {
    ASSERT_FQN(cbm_pipeline_fqn_module("proj", NULL), "proj");
    PASS();
}

TEST(fqn_module_null_project) {
    ASSERT_FQN(cbm_pipeline_fqn_module(NULL, "foo.go"), "");
    PASS();
}

TEST(fqn_module_deep) {
    ASSERT_FQN(cbm_pipeline_fqn_module("proj", "a/b/c/d/e.rs"), "proj.a.b.c.d.e");
    PASS();
}

/* ================================================================
 * cbm_pipeline_fqn_folder
 * ================================================================ */

TEST(fqn_folder_basic) {
    ASSERT_FQN(cbm_pipeline_fqn_folder("proj", "src"), "proj.src");
    PASS();
}

TEST(fqn_folder_nested) {
    ASSERT_FQN(cbm_pipeline_fqn_folder("proj", "src/pkg/util"), "proj.src.pkg.util");
    PASS();
}

TEST(fqn_folder_empty_dir) {
    ASSERT_FQN(cbm_pipeline_fqn_folder("proj", ""), "proj");
    PASS();
}

TEST(fqn_folder_null_dir) {
    ASSERT_FQN(cbm_pipeline_fqn_folder("proj", NULL), "proj");
    PASS();
}

TEST(fqn_folder_null_project) {
    ASSERT_FQN(cbm_pipeline_fqn_folder(NULL, "src"), "");
    PASS();
}

TEST(fqn_folder_backslash) {
    ASSERT_FQN(cbm_pipeline_fqn_folder("proj", "src\\pkg\\util"), "proj.src.pkg.util");
    PASS();
}

TEST(fqn_folder_backslash_mixed) {
    ASSERT_FQN(cbm_pipeline_fqn_folder("proj", "src/pkg\\util"), "proj.src.pkg.util");
    PASS();
}

TEST(fqn_folder_trailing_slash) {
    ASSERT_FQN(cbm_pipeline_fqn_folder("proj", "src/pkg/"), "proj.src.pkg");
    PASS();
}

TEST(fqn_folder_leading_slash) {
    ASSERT_FQN(cbm_pipeline_fqn_folder("proj", "/src/pkg"), "proj.src.pkg");
    PASS();
}

TEST(fqn_folder_double_slash) {
    ASSERT_FQN(cbm_pipeline_fqn_folder("proj", "a//b"), "proj.a.b");
    PASS();
}

/* ================================================================
 * cbm_project_name_from_path
 * ================================================================ */

TEST(project_name_unix_path) {
    ASSERT_FQN(cbm_project_name_from_path("/Users/dev/my-project"),
               "Users-dev-my-project");
    PASS();
}

TEST(project_name_windows_path) {
    ASSERT_FQN(cbm_project_name_from_path("C:\\Users\\dev\\project"),
               "C-Users-dev-project");
    PASS();
}

TEST(project_name_with_colons) {
    /* Colons replaced with dashes (e.g., C: drive) */
    ASSERT_FQN(cbm_project_name_from_path("C:/dev/proj"),
               "C-dev-proj");
    PASS();
}

TEST(project_name_multiple_slashes) {
    /* Consecutive slashes become one dash */
    ASSERT_FQN(cbm_project_name_from_path("/home///user//code"),
               "home-user-code");
    PASS();
}

TEST(project_name_leading_trailing_slashes) {
    /* Leading/trailing dashes trimmed */
    ASSERT_FQN(cbm_project_name_from_path("/foo/bar/"),
               "foo-bar");
    PASS();
}

TEST(project_name_empty) {
    ASSERT_FQN(cbm_project_name_from_path(""), "root");
    PASS();
}

TEST(project_name_null) {
    ASSERT_FQN(cbm_project_name_from_path(NULL), "root");
    PASS();
}

TEST(project_name_all_slashes) {
    /* All separators become dashes -> all trimmed -> "root" */
    ASSERT_FQN(cbm_project_name_from_path("///"), "root");
    PASS();
}

TEST(project_name_single_segment) {
    ASSERT_FQN(cbm_project_name_from_path("myproject"), "myproject");
    PASS();
}

TEST(project_name_mixed_separators) {
    /* Mix of forward slash, backslash, colon */
    ASSERT_FQN(cbm_project_name_from_path("C:\\Users/dev:proj"),
               "C-Users-dev-proj");
    PASS();
}

TEST(project_name_already_dashed) {
    /* Dashes are preserved, not collapsed unless from separator conversion */
    ASSERT_FQN(cbm_project_name_from_path("/my-great-project"),
               "my-great-project");
    PASS();
}

TEST(project_name_deep_path) {
    ASSERT_FQN(cbm_project_name_from_path("/a/b/c/d/e/f/g"),
               "a-b-c-d-e-f-g");
    PASS();
}

TEST(project_name_colon_only) {
    /* Single colon -> single dash -> trimmed -> root */
    ASSERT_FQN(cbm_project_name_from_path(":"), "root");
    PASS();
}

TEST(project_name_backslash_only) {
    ASSERT_FQN(cbm_project_name_from_path("\\"), "root");
    PASS();
}

TEST(project_name_consecutive_colons) {
    ASSERT_FQN(cbm_project_name_from_path("a::b"), "a-b");
    PASS();
}

/* ================================================================
 * Suite
 * ================================================================ */

SUITE(fqn) {
    /* fqn_compute: basic extensions */
    RUN_TEST(fqn_compute_basic_go);
    RUN_TEST(fqn_compute_basic_py);
    RUN_TEST(fqn_compute_basic_ts);
    RUN_TEST(fqn_compute_basic_js);
    RUN_TEST(fqn_compute_basic_c);
    RUN_TEST(fqn_compute_basic_rs);

    /* fqn_compute: nested paths */
    RUN_TEST(fqn_compute_nested_two_levels);
    RUN_TEST(fqn_compute_nested_three_levels);
    RUN_TEST(fqn_compute_nested_deep);

    /* fqn_compute: Python __init__.py */
    RUN_TEST(fqn_compute_init_py_with_name);
    RUN_TEST(fqn_compute_init_py_without_name);
    RUN_TEST(fqn_compute_init_py_empty_name);
    RUN_TEST(fqn_compute_init_py_nested);
    RUN_TEST(fqn_compute_init_py_root);
    RUN_TEST(fqn_compute_init_py_root_no_name);

    /* fqn_compute: JS/TS index files */
    RUN_TEST(fqn_compute_index_js_with_name);
    RUN_TEST(fqn_compute_index_js_without_name);
    RUN_TEST(fqn_compute_index_ts_with_name);
    RUN_TEST(fqn_compute_index_ts_without_name);
    RUN_TEST(fqn_compute_index_ts_empty_name);
    RUN_TEST(fqn_compute_index_root_with_name);
    RUN_TEST(fqn_compute_index_root_no_name);

    /* fqn_compute: empty / NULL parameters */
    RUN_TEST(fqn_compute_empty_rel_path);
    RUN_TEST(fqn_compute_empty_name);
    RUN_TEST(fqn_compute_both_empty);
    RUN_TEST(fqn_compute_null_project);
    RUN_TEST(fqn_compute_null_rel_path);
    RUN_TEST(fqn_compute_null_name);
    RUN_TEST(fqn_compute_all_null);
    RUN_TEST(fqn_compute_null_project_null_path);

    /* fqn_compute: backslash (Windows) */
    RUN_TEST(fqn_compute_backslash_simple);
    RUN_TEST(fqn_compute_backslash_nested);
    RUN_TEST(fqn_compute_backslash_mixed);

    /* fqn_compute: multiple extensions */
    RUN_TEST(fqn_compute_double_ext);
    RUN_TEST(fqn_compute_spec_ext);

    /* fqn_compute: leading / trailing slashes */
    RUN_TEST(fqn_compute_leading_slash);
    RUN_TEST(fqn_compute_trailing_slash);
    RUN_TEST(fqn_compute_double_slash);

    /* fqn_compute: edge cases */
    RUN_TEST(fqn_compute_no_ext);
    RUN_TEST(fqn_compute_project_only);
    RUN_TEST(fqn_compute_init_not_stripped);
    RUN_TEST(fqn_compute_index2_not_stripped);

    /* fqn_module */
    RUN_TEST(fqn_module_basic);
    RUN_TEST(fqn_module_go);
    RUN_TEST(fqn_module_init_py);
    RUN_TEST(fqn_module_index_js);
    RUN_TEST(fqn_module_empty_path);
    RUN_TEST(fqn_module_null_path);
    RUN_TEST(fqn_module_null_project);
    RUN_TEST(fqn_module_deep);

    /* fqn_folder */
    RUN_TEST(fqn_folder_basic);
    RUN_TEST(fqn_folder_nested);
    RUN_TEST(fqn_folder_empty_dir);
    RUN_TEST(fqn_folder_null_dir);
    RUN_TEST(fqn_folder_null_project);
    RUN_TEST(fqn_folder_backslash);
    RUN_TEST(fqn_folder_backslash_mixed);
    RUN_TEST(fqn_folder_trailing_slash);
    RUN_TEST(fqn_folder_leading_slash);
    RUN_TEST(fqn_folder_double_slash);

    /* project_name_from_path */
    RUN_TEST(project_name_unix_path);
    RUN_TEST(project_name_windows_path);
    RUN_TEST(project_name_with_colons);
    RUN_TEST(project_name_multiple_slashes);
    RUN_TEST(project_name_leading_trailing_slashes);
    RUN_TEST(project_name_empty);
    RUN_TEST(project_name_null);
    RUN_TEST(project_name_all_slashes);
    RUN_TEST(project_name_single_segment);
    RUN_TEST(project_name_mixed_separators);
    RUN_TEST(project_name_already_dashed);
    RUN_TEST(project_name_deep_path);
    RUN_TEST(project_name_colon_only);
    RUN_TEST(project_name_backslash_only);
    RUN_TEST(project_name_consecutive_colons);
}
