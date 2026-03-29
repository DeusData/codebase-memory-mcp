/*
 * test_discover.c — Tests for directory skip logic, suffix filters, and file walk.
 *
 * RED phase: Tests define expected filtering behavior for the discover module.
 */
#include "test_framework.h"
#include "../src/foundation/compat.h"
#include "discover/discover.h"
#ifndef _WIN32
#include <sys/stat.h>
#endif

/* Discover integration tests use system("rm -rf && mkdir -p && echo >")
 * which are Unix shell commands not available on Windows.
 * TODO: Port to C file operations for full Windows support. */
#ifdef _WIN32
#define SKIP_UNIX_SHELL SKIP("uses Unix shell — Windows port pending")
#else
#define SKIP_UNIX_SHELL ((void)0)
#endif

/* ── Directory skip (always skipped) ───────────────────────────── */

TEST(skip_git) {
    ASSERT_TRUE(cbm_should_skip_dir(".git", CBM_MODE_FULL));
    PASS();
}
TEST(skip_node_modules) {
    ASSERT_TRUE(cbm_should_skip_dir("node_modules", CBM_MODE_FULL));
    PASS();
}
TEST(skip_pycache) {
    ASSERT_TRUE(cbm_should_skip_dir("__pycache__", CBM_MODE_FULL));
    PASS();
}
TEST(skip_venv) {
    ASSERT_TRUE(cbm_should_skip_dir("venv", CBM_MODE_FULL));
    PASS();
}
TEST(skip_dist) {
    ASSERT_TRUE(cbm_should_skip_dir("dist", CBM_MODE_FULL));
    PASS();
}
TEST(skip_target) {
    ASSERT_TRUE(cbm_should_skip_dir("target", CBM_MODE_FULL));
    PASS();
}
TEST(skip_vendor) {
    ASSERT_TRUE(cbm_should_skip_dir("vendor", CBM_MODE_FULL));
    PASS();
}
TEST(skip_terraform) {
    ASSERT_TRUE(cbm_should_skip_dir(".terraform", CBM_MODE_FULL));
    PASS();
}
TEST(skip_coverage) {
    ASSERT_TRUE(cbm_should_skip_dir("coverage", CBM_MODE_FULL));
    PASS();
}
TEST(skip_idea) {
    ASSERT_TRUE(cbm_should_skip_dir(".idea", CBM_MODE_FULL));
    PASS();
}
TEST(skip_claude) {
    ASSERT_TRUE(cbm_should_skip_dir(".claude", CBM_MODE_FULL));
    PASS();
}

/* Not skipped in full mode */
TEST(no_skip_src) {
    ASSERT_FALSE(cbm_should_skip_dir("src", CBM_MODE_FULL));
    PASS();
}
TEST(no_skip_lib) {
    ASSERT_FALSE(cbm_should_skip_dir("lib", CBM_MODE_FULL));
    PASS();
}
TEST(no_skip_docs_full) {
    ASSERT_FALSE(cbm_should_skip_dir("docs", CBM_MODE_FULL));
    PASS();
}
TEST(no_skip_test_full) {
    ASSERT_FALSE(cbm_should_skip_dir("__tests__", CBM_MODE_FULL));
    PASS();
}

/* Fast mode additional skips */
TEST(skip_fast_docs) {
    ASSERT_TRUE(cbm_should_skip_dir("docs", CBM_MODE_FAST));
    PASS();
}
TEST(skip_fast_examples) {
    ASSERT_TRUE(cbm_should_skip_dir("examples", CBM_MODE_FAST));
    PASS();
}
TEST(skip_fast_tests) {
    ASSERT_TRUE(cbm_should_skip_dir("__tests__", CBM_MODE_FAST));
    PASS();
}
TEST(skip_fast_fixtures) {
    ASSERT_TRUE(cbm_should_skip_dir("fixtures", CBM_MODE_FAST));
    PASS();
}
TEST(skip_fast_testdata) {
    ASSERT_TRUE(cbm_should_skip_dir("testdata", CBM_MODE_FAST));
    PASS();
}
TEST(skip_fast_generated) {
    ASSERT_TRUE(cbm_should_skip_dir("generated", CBM_MODE_FAST));
    PASS();
}
TEST(skip_fast_assets) {
    ASSERT_TRUE(cbm_should_skip_dir("assets", CBM_MODE_FAST));
    PASS();
}
TEST(skip_fast_3rdparty) {
    ASSERT_TRUE(cbm_should_skip_dir("third_party", CBM_MODE_FAST));
    PASS();
}
TEST(skip_fast_e2e) {
    ASSERT_TRUE(cbm_should_skip_dir("e2e", CBM_MODE_FAST));
    PASS();
}

/* ── Suffix filters ────────────────────────────────────────────── */

TEST(suffix_pyc) {
    ASSERT_TRUE(cbm_has_ignored_suffix("module.pyc", CBM_MODE_FULL));
    PASS();
}
TEST(suffix_o) {
    ASSERT_TRUE(cbm_has_ignored_suffix("main.o", CBM_MODE_FULL));
    PASS();
}
TEST(suffix_so) {
    ASSERT_TRUE(cbm_has_ignored_suffix("lib.so", CBM_MODE_FULL));
    PASS();
}
TEST(suffix_png) {
    ASSERT_TRUE(cbm_has_ignored_suffix("icon.png", CBM_MODE_FULL));
    PASS();
}
TEST(suffix_jpg) {
    ASSERT_TRUE(cbm_has_ignored_suffix("photo.jpg", CBM_MODE_FULL));
    PASS();
}
TEST(suffix_wasm) {
    ASSERT_TRUE(cbm_has_ignored_suffix("app.wasm", CBM_MODE_FULL));
    PASS();
}
TEST(suffix_db) {
    ASSERT_TRUE(cbm_has_ignored_suffix("data.db", CBM_MODE_FULL));
    PASS();
}
TEST(suffix_sqlite) {
    ASSERT_TRUE(cbm_has_ignored_suffix("store.sqlite3", CBM_MODE_FULL));
    PASS();
}
TEST(suffix_tmp) {
    ASSERT_TRUE(cbm_has_ignored_suffix("file.tmp", CBM_MODE_FULL));
    PASS();
}
TEST(suffix_tilde) {
    ASSERT_TRUE(cbm_has_ignored_suffix("file~", CBM_MODE_FULL));
    PASS();
}

/* Not ignored */
TEST(suffix_go) {
    ASSERT_FALSE(cbm_has_ignored_suffix("main.go", CBM_MODE_FULL));
    PASS();
}
TEST(suffix_py) {
    ASSERT_FALSE(cbm_has_ignored_suffix("app.py", CBM_MODE_FULL));
    PASS();
}
TEST(suffix_c) {
    ASSERT_FALSE(cbm_has_ignored_suffix("lib.c", CBM_MODE_FULL));
    PASS();
}

/* Fast mode additional suffixes */
TEST(suffix_fast_zip) {
    ASSERT_TRUE(cbm_has_ignored_suffix("archive.zip", CBM_MODE_FAST));
    PASS();
}
TEST(suffix_fast_pdf) {
    ASSERT_TRUE(cbm_has_ignored_suffix("manual.pdf", CBM_MODE_FAST));
    PASS();
}
TEST(suffix_fast_mp3) {
    ASSERT_TRUE(cbm_has_ignored_suffix("sound.mp3", CBM_MODE_FAST));
    PASS();
}
TEST(suffix_fast_pem) {
    ASSERT_TRUE(cbm_has_ignored_suffix("cert.pem", CBM_MODE_FAST));
    PASS();
}

/* ── Filename skip (fast mode) ─────────────────────────────────── */

TEST(fn_skip_license) {
    ASSERT_TRUE(cbm_should_skip_filename("LICENSE", CBM_MODE_FAST));
    PASS();
}
TEST(fn_skip_changelog) {
    ASSERT_TRUE(cbm_should_skip_filename("CHANGELOG.md", CBM_MODE_FAST));
    PASS();
}
TEST(fn_skip_gosum) {
    ASSERT_TRUE(cbm_should_skip_filename("go.sum", CBM_MODE_FAST));
    PASS();
}
TEST(fn_skip_yarnlock) {
    ASSERT_TRUE(cbm_should_skip_filename("yarn.lock", CBM_MODE_FAST));
    PASS();
}
TEST(fn_skip_pkglock) {
    ASSERT_TRUE(cbm_should_skip_filename("package-lock.json", CBM_MODE_FAST));
    PASS();
}

/* Not skipped in full mode */
TEST(fn_no_skip_license_full) {
    ASSERT_FALSE(cbm_should_skip_filename("LICENSE", CBM_MODE_FULL));
    PASS();
}

/* ── Fast mode patterns ────────────────────────────────────────── */

TEST(pattern_dts) {
    ASSERT_TRUE(cbm_matches_fast_pattern("types.d.ts", CBM_MODE_FAST));
    PASS();
}
TEST(pattern_pbgo) {
    ASSERT_TRUE(cbm_matches_fast_pattern("service.pb.go", CBM_MODE_FAST));
    PASS();
}
TEST(pattern_pb2py) {
    ASSERT_TRUE(cbm_matches_fast_pattern("api_pb2.py", CBM_MODE_FAST));
    PASS();
}
TEST(pattern_mock) {
    ASSERT_TRUE(cbm_matches_fast_pattern("mock_service.go", CBM_MODE_FAST));
    PASS();
}
TEST(pattern_test_dot) {
    ASSERT_TRUE(cbm_matches_fast_pattern("App.test.js", CBM_MODE_FAST));
    PASS();
}
TEST(pattern_spec) {
    ASSERT_TRUE(cbm_matches_fast_pattern("App.spec.ts", CBM_MODE_FAST));
    PASS();
}
TEST(pattern_stories) {
    ASSERT_TRUE(cbm_matches_fast_pattern("Button.stories.tsx", CBM_MODE_FAST));
    PASS();
}

/* Not matched in full mode */
TEST(pattern_dts_full) {
    ASSERT_FALSE(cbm_matches_fast_pattern("types.d.ts", CBM_MODE_FULL));
    PASS();
}

/* ── File discovery (integration) ──────────────────────────────── */

TEST(discover_simple) {
    SKIP_UNIX_SHELL;
    /* Create a temp directory with a few files */
    const char *base = "/tmp/test_discover_simple";
    char cmd[512];
    snprintf(cmd, sizeof(cmd),
             "rm -rf %s && mkdir -p %s/src && "
             "echo 'package main' > %s/src/main.go && "
             "echo 'print(1)' > %s/src/app.py && "
             "echo 'binary' > %s/src/icon.png", /* should be filtered */
             base, base, base, base, base);
    system(cmd);

    cbm_discover_opts_t opts = {0};
    cbm_file_info_t *files = NULL;
    int count = 0;

    int rc = cbm_discover(base, &opts, &files, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 2); /* main.go + app.py, not icon.png */

    /* Verify languages detected */
    bool found_go = false, found_py = false;
    for (int i = 0; i < count; i++) {
        if (files[i].language == CBM_LANG_GO)
            found_go = true;
        if (files[i].language == CBM_LANG_PYTHON)
            found_py = true;
    }
    ASSERT_TRUE(found_go);
    ASSERT_TRUE(found_py);

    cbm_discover_free(files, count);
    snprintf(cmd, sizeof(cmd), "rm -rf %s", base);
    system(cmd);
    PASS();
}

TEST(discover_skips_git_dir) {
    SKIP_UNIX_SHELL;
    const char *base = "/tmp/test_discover_gitdir";
    char cmd[512];
    snprintf(cmd, sizeof(cmd),
             "rm -rf %s && mkdir -p %s/.git && mkdir -p %s/src && "
             "echo 'x' > %s/.git/config && "
             "echo 'package main' > %s/src/main.go",
             base, base, base, base, base);
    system(cmd);

    cbm_discover_opts_t opts = {0};
    cbm_file_info_t *files = NULL;
    int count = 0;

    int rc = cbm_discover(base, &opts, &files, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 1); /* only main.go, .git/config excluded */

    cbm_discover_free(files, count);
    snprintf(cmd, sizeof(cmd), "rm -rf %s", base);
    system(cmd);
    PASS();
}

TEST(discover_with_gitignore) {
    SKIP_UNIX_SHELL;
    const char *base = "/tmp/test_discover_gitignore";
    char cmd[512];
    snprintf(cmd, sizeof(cmd),
             "rm -rf %s && mkdir -p %s/src && mkdir -p %s/.git && "
             "echo '*.log' > %s/.gitignore && "
             "echo 'package main' > %s/src/main.go && "
             "echo 'error' > %s/src/debug.log",
             base, base, base, base, base, base);
    system(cmd);

    cbm_discover_opts_t opts = {0};
    cbm_file_info_t *files = NULL;
    int count = 0;

    int rc = cbm_discover(base, &opts, &files, &count);
    ASSERT_EQ(rc, 0);
    /* Should find main.go only, debug.log ignored by .gitignore */
    ASSERT_EQ(count, 1);
    ASSERT_EQ(files[0].language, CBM_LANG_GO);

    cbm_discover_free(files, count);
    snprintf(cmd, sizeof(cmd), "rm -rf %s", base);
    system(cmd);
    PASS();
}

TEST(discover_max_file_size) {
    SKIP_UNIX_SHELL;
    const char *base = "/tmp/test_discover_maxsize";
    char cmd[512];
    snprintf(cmd, sizeof(cmd),
             "rm -rf %s && mkdir -p %s && "
             "echo 'small' > %s/small.go && "
             "dd if=/dev/zero of=%s/big.go bs=1024 count=100 2>/dev/null",
             base, base, base, base);
    system(cmd);

    cbm_discover_opts_t opts = {0};
    opts.max_file_size = 1024; /* 1KB limit */
    cbm_file_info_t *files = NULL;
    int count = 0;

    int rc = cbm_discover(base, &opts, &files, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 1); /* only small.go */

    cbm_discover_free(files, count);
    snprintf(cmd, sizeof(cmd), "rm -rf %s", base);
    system(cmd);
    PASS();
}

TEST(discover_null_path) {
    cbm_file_info_t *files = NULL;
    int count = 0;
    int rc = cbm_discover(NULL, NULL, &files, &count);
    ASSERT_EQ(rc, -1);
    PASS();
}

TEST(discover_nonexistent_path) {
    cbm_file_info_t *files = NULL;
    int count = 0;
    int rc = cbm_discover("/tmp/nonexistent_repo_12345", NULL, &files, &count);
    ASSERT_EQ(rc, -1);
    PASS();
}

TEST(discover_free_null) {
    cbm_discover_free(NULL, 0); /* should not crash */
    PASS();
}

/* --- Ported from discover_test.go: TestDiscoverSkipsWorktrees --- */
TEST(discover_skips_worktrees) {
    SKIP_UNIX_SHELL;
    const char *base = "/tmp/test_discover_worktrees";
    char cmd[512];
    snprintf(cmd, sizeof(cmd),
             "rm -rf %s && mkdir -p %s/src && mkdir -p %s/.worktrees/feature/src && "
             "echo 'package main' > %s/src/main.go && "
             "echo 'package app' > %s/.worktrees/feature/src/app.go",
             base, base, base, base, base);
    system(cmd);

    cbm_discover_opts_t opts = {0};
    cbm_file_info_t *files = NULL;
    int count = 0;

    int rc = cbm_discover(base, &opts, &files, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 1); /* only main.go, .worktrees skipped */

    bool found_main = false;
    for (int i = 0; i < count; i++) {
        if (strstr(files[i].rel_path, "main.go"))
            found_main = true;
        /* Must NOT find worktree file */
        ASSERT_NULL(strstr(files[i].rel_path, ".worktrees"));
    }
    ASSERT_TRUE(found_main);

    cbm_discover_free(files, count);
    snprintf(cmd, sizeof(cmd), "rm -rf %s", base);
    system(cmd);
    PASS();
}

/* --- Ported from discover_test.go: TestCBMIgnoreBasic --- */
TEST(discover_cbmignore) {
    SKIP_UNIX_SHELL;
    const char *base = "/tmp/test_discover_cbmignore";
    char cmd[512];
    snprintf(cmd, sizeof(cmd),
             "rm -rf %s && mkdir -p %s/generated && mkdir -p %s/.git && "
             "echo 'generated/\n*.pb.go' > %s/.cbmignore && "
             "echo 'package main' > %s/main.go && "
             "echo 'package gen' > %s/generated/types.go && "
             "echo 'package api' > %s/api.pb.go",
             base, base, base, base, base, base, base);
    system(cmd);

    cbm_discover_opts_t opts = {0};
    cbm_file_info_t *files = NULL;
    int count = 0;

    int rc = cbm_discover(base, &opts, &files, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 1); /* only main.go */
    ASSERT_TRUE(strstr(files[0].rel_path, "main.go") != NULL);

    cbm_discover_free(files, count);
    snprintf(cmd, sizeof(cmd), "rm -rf %s", base);
    system(cmd);
    PASS();
}

/* --- Ported from discover_test.go: TestCBMIgnoreStacksOnGitignore --- */
TEST(discover_cbmignore_stacks) {
    SKIP_UNIX_SHELL;
    const char *base = "/tmp/test_discover_cbmignore_stack";
    char cmd[512];
    snprintf(cmd, sizeof(cmd),
             "rm -rf %s && mkdir -p %s/docs && mkdir -p %s/.git && "
             "echo '*.log' > %s/.gitignore && "
             "echo 'docs/' > %s/.cbmignore && "
             "echo 'package main' > %s/main.go && "
             "echo 'package docs' > %s/docs/api.go",
             base, base, base, base, base, base, base);
    system(cmd);

    cbm_discover_opts_t opts = {0};
    cbm_file_info_t *files = NULL;
    int count = 0;

    int rc = cbm_discover(base, &opts, &files, &count);
    ASSERT_EQ(rc, 0);
    /* main.go only — docs/api.go excluded by .cbmignore */
    ASSERT_EQ(count, 1);

    bool found_docs = false;
    for (int i = 0; i < count; i++) {
        if (strstr(files[i].rel_path, "docs/"))
            found_docs = true;
    }
    ASSERT_FALSE(found_docs);

    cbm_discover_free(files, count);
    snprintf(cmd, sizeof(cmd), "rm -rf %s", base);
    system(cmd);
    PASS();
}

/* --- Ported from discover_test.go: TestSymlinkedFilesSkipped --- */
TEST(discover_symlink_skipped) {
    SKIP_UNIX_SHELL;
    const char *base = "/tmp/test_discover_symlink";
    char cmd[512];
    snprintf(cmd, sizeof(cmd),
             "rm -rf %s && mkdir -p %s && "
             "echo 'package main' > %s/real.go && "
             "ln -sf %s/real.go %s/link.go",
             base, base, base, base, base);
    system(cmd);

    cbm_discover_opts_t opts = {0};
    cbm_file_info_t *files = NULL;
    int count = 0;

    int rc = cbm_discover(base, &opts, &files, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 1); /* only real.go, not link.go (symlink) */

    bool found_real = false, found_link = false;
    for (int i = 0; i < count; i++) {
        if (strstr(files[i].rel_path, "real.go"))
            found_real = true;
        if (strstr(files[i].rel_path, "link.go"))
            found_link = true;
    }
    ASSERT_TRUE(found_real);
    ASSERT_FALSE(found_link);

    cbm_discover_free(files, count);
    snprintf(cmd, sizeof(cmd), "rm -rf %s", base);
    system(cmd);
    PASS();
}

/* --- Ported from discover_test.go: TestNewIgnorePatterns --- */
TEST(discover_new_ignore_patterns) {
    SKIP_UNIX_SHELL;
    const char *base = "/tmp/test_discover_newignore";
    /* Create dirs that should be in IGNORE_PATTERNS */
    const char *dirs[] = {".next", ".terraform", "zig-cache", ".cargo", "elm-stuff", "bazel-out"};
    int ndirs = 6;
    char cmd[1024];
    snprintf(cmd, sizeof(cmd), "rm -rf %s && mkdir -p %s", base, base);
    system(cmd);

    for (int i = 0; i < ndirs; i++) {
        snprintf(cmd, sizeof(cmd), "mkdir -p %s/%s && echo 'package x' > %s/%s/file.go", base,
                 dirs[i], base, dirs[i]);
        system(cmd);
    }
    snprintf(cmd, sizeof(cmd), "echo 'package main' > %s/main.go", base);
    system(cmd);

    cbm_discover_opts_t opts = {0};
    cbm_file_info_t *files = NULL;
    int count = 0;

    int rc = cbm_discover(base, &opts, &files, &count);
    ASSERT_EQ(rc, 0);
    /* Only main.go — all dirs should be skipped */
    ASSERT_EQ(count, 1);
    ASSERT_TRUE(strstr(files[0].rel_path, "main.go") != NULL);

    cbm_discover_free(files, count);
    snprintf(cmd, sizeof(cmd), "rm -rf %s", base);
    system(cmd);
    PASS();
}

/* --- Ported from discover_test.go: TestGenericDirsNotIgnoredInFullMode --- */
TEST(discover_generic_dirs_full_mode) {
    SKIP_UNIX_SHELL;
    const char *base = "/tmp/test_discover_generic_full";
    char cmd[512];
    snprintf(cmd, sizeof(cmd),
             "rm -rf %s && mkdir -p %s/bin && mkdir -p %s/build && mkdir -p %s/out && "
             "echo 'package bin' > %s/bin/main.go && "
             "echo 'package build' > %s/build/main.go && "
             "echo 'package out' > %s/out/main.go",
             base, base, base, base, base, base, base);
    system(cmd);

    cbm_discover_opts_t opts = {.mode = CBM_MODE_FULL};
    cbm_file_info_t *files = NULL;
    int count = 0;

    int rc = cbm_discover(base, &opts, &files, &count);
    ASSERT_EQ(rc, 0);
    /* bin, build, out should NOT be skipped in full mode */
    ASSERT_EQ(count, 3);

    cbm_discover_free(files, count);
    snprintf(cmd, sizeof(cmd), "rm -rf %s", base);
    system(cmd);
    PASS();
}

/* --- Ported from discover_test.go: TestGenericDirsIgnoredInFastMode --- */
TEST(discover_generic_dirs_fast_mode) {
    SKIP_UNIX_SHELL;
    const char *base = "/tmp/test_discover_generic_fast";
    char cmd[512];
    snprintf(cmd, sizeof(cmd),
             "rm -rf %s && mkdir -p %s/bin && mkdir -p %s/build && mkdir -p %s/out && "
             "echo 'package bin' > %s/bin/main.go && "
             "echo 'package build' > %s/build/main.go && "
             "echo 'package out' > %s/out/main.go",
             base, base, base, base, base, base, base);
    system(cmd);

    cbm_discover_opts_t opts = {.mode = CBM_MODE_FAST};
    cbm_file_info_t *files = NULL;
    int count = 0;

    int rc = cbm_discover(base, &opts, &files, &count);
    ASSERT_EQ(rc, 0);
    /* bin, build, out should be skipped in fast mode */
    ASSERT_EQ(count, 0);

    cbm_discover_free(files, count);
    snprintf(cmd, sizeof(cmd), "rm -rf %s", base);
    system(cmd);
    PASS();
}

/* --- Ported from discover_test.go: TestCBMIgnoreWithoutGitRepo --- */
TEST(discover_cbmignore_no_git) {
    SKIP_UNIX_SHELL;
    const char *base = "/tmp/test_discover_cbmignore_nogit";
    char cmd[512];
    /* No .git directory — .cbmignore should still work */
    snprintf(cmd, sizeof(cmd),
             "rm -rf %s && mkdir -p %s/scratch && "
             "echo 'scratch/' > %s/.cbmignore && "
             "echo 'package main' > %s/main.go && "
             "echo 'package scratch' > %s/scratch/tmp.go",
             base, base, base, base, base);
    system(cmd);

    cbm_discover_opts_t opts = {0};
    cbm_file_info_t *files = NULL;
    int count = 0;

    int rc = cbm_discover(base, &opts, &files, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 1); /* only main.go */
    ASSERT_TRUE(strstr(files[0].rel_path, "main.go") != NULL);

    cbm_discover_free(files, count);
    snprintf(cmd, sizeof(cmd), "rm -rf %s", base);
    system(cmd);
    PASS();
}

/* --- Issue #178: nested .gitignore files should be respected --- */

/* Basic nested .gitignore: webapp/.gitignore with ".output" should exclude
 * webapp/.output/ and its contents from indexing. */
TEST(discover_nested_gitignore) {
    SKIP_UNIX_SHELL;
    const char *base = "/tmp/test_discover_nested_gi";
    char cmd[1024];
    snprintf(cmd, sizeof(cmd),
             "rm -rf %s && mkdir -p %s/.git && "
             "mkdir -p %s/webapp/.output/chunks && "
             "mkdir -p %s/webapp/src && "
             "printf '.output\\n' > %s/webapp/.gitignore && "
             "echo 'package main' > %s/main.go && "
             "echo 'const x = 1' > %s/webapp/src/app.js && "
             "echo 'const chunk = 1' > %s/webapp/.output/chunks/app.js",
             base, base, base, base, base, base, base, base);
    system(cmd);

    cbm_discover_opts_t opts = {0};
    cbm_file_info_t *files = NULL;
    int count = 0;

    int rc = cbm_discover(base, &opts, &files, &count);
    ASSERT_EQ(rc, 0);

    bool found_output = false;
    for (int i = 0; i < count; i++) {
        if (strstr(files[i].rel_path, ".output"))
            found_output = true;
    }
    ASSERT_FALSE(found_output); /* .output excluded by webapp/.gitignore */

    /* webapp/src/app.js and main.go should be present */
    bool found_app = false, found_main = false;
    for (int i = 0; i < count; i++) {
        if (strstr(files[i].rel_path, "webapp/src/app.js"))
            found_app = true;
        if (strstr(files[i].rel_path, "main.go"))
            found_main = true;
    }
    ASSERT_TRUE(found_app);
    ASSERT_TRUE(found_main);

    cbm_discover_free(files, count);
    snprintf(cmd, sizeof(cmd), "rm -rf %s", base);
    system(cmd);
    PASS();
}

/* Nested .gitignore patterns apply to deeper descendants, not just immediate
 * children: webapp/.gitignore with "generated/" should exclude
 * webapp/src/generated/foo.go even though it is two levels deep. */
TEST(discover_nested_gitignore_deep) {
    SKIP_UNIX_SHELL;
    const char *base = "/tmp/test_discover_nested_gi_deep";
    char cmd[1024];
    snprintf(cmd, sizeof(cmd),
             "rm -rf %s && mkdir -p %s/.git && "
             "mkdir -p %s/webapp/src/generated && "
             "mkdir -p %s/webapp/src/api && "
             "printf 'generated/\\n' > %s/webapp/.gitignore && "
             "echo 'const x = 1' > %s/webapp/src/api/routes.js && "
             "echo 'const g = 1' > %s/webapp/src/generated/schema.js",
             base, base, base, base, base, base, base);
    system(cmd);

    cbm_discover_opts_t opts = {0};
    cbm_file_info_t *files = NULL;
    int count = 0;

    int rc = cbm_discover(base, &opts, &files, &count);
    ASSERT_EQ(rc, 0);

    bool found_generated = false, found_routes = false;
    for (int i = 0; i < count; i++) {
        if (strstr(files[i].rel_path, "generated"))
            found_generated = true;
        if (strstr(files[i].rel_path, "routes.js"))
            found_routes = true;
    }
    ASSERT_FALSE(found_generated); /* excluded by nested gitignore */
    ASSERT_TRUE(found_routes);     /* not excluded */

    cbm_discover_free(files, count);
    snprintf(cmd, sizeof(cmd), "rm -rf %s", base);
    system(cmd);
    PASS();
}

/* Root .gitignore and nested .gitignore both apply independently. */
TEST(discover_nested_gitignore_stacks_with_root) {
    SKIP_UNIX_SHELL;
    const char *base = "/tmp/test_discover_nested_gi_stack";
    char cmd[1024];
    snprintf(cmd, sizeof(cmd),
             "rm -rf %s && mkdir -p %s/.git && "
             "mkdir -p %s/webapp/.output && "
             "mkdir -p %s/webapp/src && "
             "printf '*.log\\n' > %s/.gitignore && "
             "printf '.output\\n' > %s/webapp/.gitignore && "
             "echo 'package main' > %s/main.go && "
             "echo 'error log' > %s/error.log && "
             "echo 'const x = 1' > %s/webapp/src/app.js && "
             "echo 'output data' > %s/webapp/.output/data.js",
             base, base, base, base, base, base, base, base, base, base);
    system(cmd);

    cbm_discover_opts_t opts = {0};
    cbm_file_info_t *files = NULL;
    int count = 0;

    int rc = cbm_discover(base, &opts, &files, &count);
    ASSERT_EQ(rc, 0);

    bool found_log = false, found_output = false;
    bool found_main = false, found_app = false;
    for (int i = 0; i < count; i++) {
        if (strstr(files[i].rel_path, ".log"))
            found_log = true;
        if (strstr(files[i].rel_path, ".output"))
            found_output = true;
        if (strstr(files[i].rel_path, "main.go"))
            found_main = true;
        if (strstr(files[i].rel_path, "app.js"))
            found_app = true;
    }
    ASSERT_FALSE(found_log);    /* excluded by root .gitignore */
    ASSERT_FALSE(found_output); /* excluded by nested .gitignore */
    ASSERT_TRUE(found_main);
    ASSERT_TRUE(found_app);

    cbm_discover_free(files, count);
    snprintf(cmd, sizeof(cmd), "rm -rf %s", base);
    system(cmd);
    PASS();
}

/* ── Suite ─────────────────────────────────────────────────────── */

SUITE(discover) {
    /* Directory skip — always */
    RUN_TEST(skip_git);
    RUN_TEST(skip_node_modules);
    RUN_TEST(skip_pycache);
    RUN_TEST(skip_venv);
    RUN_TEST(skip_dist);
    RUN_TEST(skip_target);
    RUN_TEST(skip_vendor);
    RUN_TEST(skip_terraform);
    RUN_TEST(skip_coverage);
    RUN_TEST(skip_idea);
    RUN_TEST(skip_claude);

    /* Not skipped */
    RUN_TEST(no_skip_src);
    RUN_TEST(no_skip_lib);
    RUN_TEST(no_skip_docs_full);
    RUN_TEST(no_skip_test_full);

    /* Fast mode directory skips */
    RUN_TEST(skip_fast_docs);
    RUN_TEST(skip_fast_examples);
    RUN_TEST(skip_fast_tests);
    RUN_TEST(skip_fast_fixtures);
    RUN_TEST(skip_fast_testdata);
    RUN_TEST(skip_fast_generated);
    RUN_TEST(skip_fast_assets);
    RUN_TEST(skip_fast_3rdparty);
    RUN_TEST(skip_fast_e2e);

    /* Suffix filters */
    RUN_TEST(suffix_pyc);
    RUN_TEST(suffix_o);
    RUN_TEST(suffix_so);
    RUN_TEST(suffix_png);
    RUN_TEST(suffix_jpg);
    RUN_TEST(suffix_wasm);
    RUN_TEST(suffix_db);
    RUN_TEST(suffix_sqlite);
    RUN_TEST(suffix_tmp);
    RUN_TEST(suffix_tilde);
    RUN_TEST(suffix_go);
    RUN_TEST(suffix_py);
    RUN_TEST(suffix_c);
    RUN_TEST(suffix_fast_zip);
    RUN_TEST(suffix_fast_pdf);
    RUN_TEST(suffix_fast_mp3);
    RUN_TEST(suffix_fast_pem);

    /* Filename skip */
    RUN_TEST(fn_skip_license);
    RUN_TEST(fn_skip_changelog);
    RUN_TEST(fn_skip_gosum);
    RUN_TEST(fn_skip_yarnlock);
    RUN_TEST(fn_skip_pkglock);
    RUN_TEST(fn_no_skip_license_full);

    /* Fast mode patterns */
    RUN_TEST(pattern_dts);
    RUN_TEST(pattern_pbgo);
    RUN_TEST(pattern_pb2py);
    RUN_TEST(pattern_mock);
    RUN_TEST(pattern_test_dot);
    RUN_TEST(pattern_spec);
    RUN_TEST(pattern_stories);
    RUN_TEST(pattern_dts_full);

    /* Integration tests */
    RUN_TEST(discover_simple);
    RUN_TEST(discover_skips_git_dir);
    RUN_TEST(discover_with_gitignore);
    RUN_TEST(discover_max_file_size);
    RUN_TEST(discover_null_path);
    RUN_TEST(discover_nonexistent_path);
    RUN_TEST(discover_free_null);

    /* Go test ports */
    RUN_TEST(discover_skips_worktrees);
    RUN_TEST(discover_cbmignore);
    RUN_TEST(discover_cbmignore_stacks);
    RUN_TEST(discover_symlink_skipped);
    RUN_TEST(discover_new_ignore_patterns);
    RUN_TEST(discover_generic_dirs_full_mode);
    RUN_TEST(discover_generic_dirs_fast_mode);
    RUN_TEST(discover_cbmignore_no_git);

    /* Nested .gitignore tests (issue #178) */
    RUN_TEST(discover_nested_gitignore);
    RUN_TEST(discover_nested_gitignore_deep);
    RUN_TEST(discover_nested_gitignore_stacks_with_root);
}
