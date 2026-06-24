/*
 * test_discover.c — Tests for directory skip logic, suffix filters, and file walk.
 *
 * RED phase: Tests define expected filtering behavior for the discover module.
 */
#include "test_framework.h"
#include "test_helpers.h"
#include "discover/discover.h"

typedef struct {
    char *home;
    char *xdg_config_home;
} git_env_snapshot_t;

static git_env_snapshot_t save_git_env(void) {
    git_env_snapshot_t snapshot = {0};
    const char *home = getenv("HOME");
    const char *xdg = getenv("XDG_CONFIG_HOME");
    snapshot.home = home ? cbm_strdup(home) : NULL;
    snapshot.xdg_config_home = xdg ? cbm_strdup(xdg) : NULL;
    return snapshot;
}

static void restore_git_env(git_env_snapshot_t *snapshot) {
    if (snapshot->home) {
        cbm_setenv("HOME", snapshot->home, 1);
        free(snapshot->home);
    } else {
        cbm_unsetenv("HOME");
    }

    if (snapshot->xdg_config_home) {
        cbm_setenv("XDG_CONFIG_HOME", snapshot->xdg_config_home, 1);
        free(snapshot->xdg_config_home);
    } else {
        cbm_unsetenv("XDG_CONFIG_HOME");
    }
}

static bool discover_has_rel_path(const cbm_file_info_t *files, int count, const char *rel_path) {
    for (int i = 0; i < count; i++) {
        if (strcmp(files[i].rel_path, rel_path) == 0) {
            return true;
        }
    }
    return false;
}

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
TEST(skip_vendored) {
    ASSERT_TRUE(cbm_should_skip_dir("vendored", CBM_MODE_FULL));
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

/* ── File discovery (integration) — cross-platform via test_helpers.h ── */

TEST(discover_simple) {
    char *base = th_mktempdir("cbm_disc_simple");
    ASSERT(base != NULL);

    th_write_file(TH_PATH(base, "src/main.go"), "package main\n");
    th_write_file(TH_PATH(base, "src/app.py"), "print(1)\n");
    th_write_file(TH_PATH(base, "src/icon.png"), "binary\n");

    cbm_discover_opts_t opts = {0};
    cbm_file_info_t *files = NULL;
    int count = 0;

    int rc = cbm_discover(base, &opts, &files, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 2); /* main.go + app.py, not icon.png */

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
    th_cleanup(base);
    PASS();
}

TEST(discover_skips_git_dir) {
    char *base = th_mktempdir("cbm_disc_git");
    ASSERT(base != NULL);

    th_mkdir_p(TH_PATH(base, ".git"));
    th_write_file(TH_PATH(base, ".git/config"), "x\n");
    th_write_file(TH_PATH(base, "src/main.go"), "package main\n");

    cbm_discover_opts_t opts = {0};
    cbm_file_info_t *files = NULL;
    int count = 0;

    int rc = cbm_discover(base, &opts, &files, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 1);

    cbm_discover_free(files, count);
    th_cleanup(base);
    PASS();
}

TEST(discover_with_gitignore) {
    char *base = th_mktempdir("cbm_disc_gi");
    ASSERT(base != NULL);

    th_mkdir_p(TH_PATH(base, ".git"));
    th_write_file(TH_PATH(base, ".gitignore"), "*.log\n");
    th_write_file(TH_PATH(base, "src/main.go"), "package main\n");
    th_write_file(TH_PATH(base, "src/debug.log"), "error\n");

    cbm_discover_opts_t opts = {0};
    cbm_file_info_t *files = NULL;
    int count = 0;

    int rc = cbm_discover(base, &opts, &files, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(files[0].language, CBM_LANG_GO);

    cbm_discover_free(files, count);
    th_cleanup(base);
    PASS();
}

TEST(discover_with_global_xdg_ignore) {
    git_env_snapshot_t env = save_git_env();
    char *tmp = th_mktempdir("cbm_disc_global_xdg");
    ASSERT(tmp != NULL);

    char base[512], repo[512], home[512], xdg[512], xdg_env[512];
    snprintf(base, sizeof(base), "%s", tmp);
    snprintf(repo, sizeof(repo), "%s/repo", base);
    snprintf(home, sizeof(home), "%s/home", base);
    snprintf(xdg, sizeof(xdg), "%s/xdg", base);
    snprintf(xdg_env, sizeof(xdg_env), "%s/", xdg);

    cbm_setenv("HOME", home, 1);
    cbm_setenv("XDG_CONFIG_HOME", xdg_env, 1);

    th_mkdir_p(TH_PATH(repo, ".git"));
    th_write_file(TH_PATH(repo, ".git/config"), "[core]\n");
    th_write_file(TH_PATH(xdg, "git/ignore"), "secret.go\nignored_dir/\n");
    th_write_file(TH_PATH(repo, "main.go"), "package main\n");
    th_write_file(TH_PATH(repo, "secret.go"), "package secret\n");
    th_write_file(TH_PATH(repo, "ignored_dir/thing.go"), "package ignored\n");

    cbm_discover_opts_t opts = {0};
    cbm_file_info_t *files = NULL;
    int count = 0;

    int rc = cbm_discover(repo, &opts, &files, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 1);
    ASSERT_TRUE(discover_has_rel_path(files, count, "main.go"));
    ASSERT_FALSE(discover_has_rel_path(files, count, "secret.go"));
    ASSERT_FALSE(discover_has_rel_path(files, count, "ignored_dir/thing.go"));

    cbm_discover_free(files, count);
    restore_git_env(&env);
    th_cleanup(base);
    PASS();
}

TEST(discover_global_excludesfile_from_gitconfig_tilde) {
    git_env_snapshot_t env = save_git_env();
    char *tmp = th_mktempdir("cbm_disc_global_cfg");
    ASSERT(tmp != NULL);

    char base[512], repo[512], home[512];
    snprintf(base, sizeof(base), "%s", tmp);
    snprintf(repo, sizeof(repo), "%s/repo", base);
    snprintf(home, sizeof(home), "%s/home", base);

    cbm_setenv("HOME", home, 1);
    cbm_unsetenv("XDG_CONFIG_HOME");

    th_mkdir_p(TH_PATH(repo, ".git"));
    th_write_file(TH_PATH(repo, ".git/config"), "[core]\n");
    th_write_file(TH_PATH(home, ".gitconfig"), "[core]\n    excludesFile = ~/custom-ignore\n");
    th_write_file(TH_PATH(home, "custom-ignore"), "skip-me.go\n");
    th_write_file(TH_PATH(repo, "keep.go"), "package keep\n");
    th_write_file(TH_PATH(repo, "skip-me.go"), "package skip\n");

    cbm_discover_opts_t opts = {0};
    cbm_file_info_t *files = NULL;
    int count = 0;

    int rc = cbm_discover(repo, &opts, &files, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 1);
    ASSERT_TRUE(discover_has_rel_path(files, count, "keep.go"));
    ASSERT_FALSE(discover_has_rel_path(files, count, "skip-me.go"));

    cbm_discover_free(files, count);
    restore_git_env(&env);
    th_cleanup(base);
    PASS();
}

TEST(discover_repo_local_excludesfile_is_ignored) {
    git_env_snapshot_t env = save_git_env();
    char *tmp = th_mktempdir("cbm_disc_repo_cfg_ignored");
    ASSERT(tmp != NULL);

    char base[512], repo[512], home[512], secret[512], config[1024];
    snprintf(base, sizeof(base), "%s", tmp);
    snprintf(repo, sizeof(repo), "%s/repo", base);
    snprintf(home, sizeof(home), "%s/home", base);
    snprintf(secret, sizeof(secret), "%s/secret-ignore", home);
    snprintf(config, sizeof(config), "[core]\n    excludesFile = %s\n", secret);

    cbm_setenv("HOME", home, 1);
    cbm_unsetenv("XDG_CONFIG_HOME");

    th_mkdir_p(TH_PATH(repo, ".git"));
    th_write_file(TH_PATH(repo, ".git/config"), config);
    th_write_file(secret, "skip-me.go\n");
    th_write_file(TH_PATH(repo, "keep.go"), "package keep\n");
    th_write_file(TH_PATH(repo, "skip-me.go"), "package skip\n");

    cbm_discover_opts_t opts = {0};
    cbm_file_info_t *files = NULL;
    int count = 0;

    int rc = cbm_discover(repo, &opts, &files, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 2);
    ASSERT_TRUE(discover_has_rel_path(files, count, "keep.go"));
    ASSERT_TRUE(discover_has_rel_path(files, count, "skip-me.go"));

    cbm_discover_free(files, count);
    restore_git_env(&env);
    th_cleanup(base);
    PASS();
}

TEST(discover_missing_global_excludes_is_noop) {
    git_env_snapshot_t env = save_git_env();
    char *tmp = th_mktempdir("cbm_disc_global_missing");
    ASSERT(tmp != NULL);

    char base[512], repo[512], home[512];
    snprintf(base, sizeof(base), "%s", tmp);
    snprintf(repo, sizeof(repo), "%s/repo", base);
    snprintf(home, sizeof(home), "%s/home", base);

    cbm_setenv("HOME", home, 1);
    cbm_unsetenv("XDG_CONFIG_HOME");

    th_mkdir_p(TH_PATH(repo, ".git"));
    th_write_file(TH_PATH(repo, ".git/config"), "[core]\n");
    th_write_file(TH_PATH(repo, "main.go"), "package main\n");
    th_write_file(TH_PATH(repo, "would-be-global.go"), "package global\n");

    cbm_discover_opts_t opts = {0};
    cbm_file_info_t *files = NULL;
    int count = 0;

    int rc = cbm_discover(repo, &opts, &files, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 2);
    ASSERT_TRUE(discover_has_rel_path(files, count, "main.go"));
    ASSERT_TRUE(discover_has_rel_path(files, count, "would-be-global.go"));

    cbm_discover_free(files, count);
    restore_git_env(&env);
    th_cleanup(base);
    PASS();
}

TEST(discover_cbmignore_negates_global_ignore) {
    git_env_snapshot_t env = save_git_env();
    char *tmp = th_mktempdir("cbm_disc_global_neg");
    ASSERT(tmp != NULL);

    char base[512], repo[512], home[512], xdg[512];
    snprintf(base, sizeof(base), "%s", tmp);
    snprintf(repo, sizeof(repo), "%s/repo", base);
    snprintf(home, sizeof(home), "%s/home", base);
    snprintf(xdg, sizeof(xdg), "%s/xdg", base);

    cbm_setenv("HOME", home, 1);
    cbm_setenv("XDG_CONFIG_HOME", xdg, 1);

    th_mkdir_p(TH_PATH(repo, ".git"));
    th_write_file(TH_PATH(repo, ".git/config"), "[core]\n");
    th_write_file(TH_PATH(xdg, "git/ignore"), "rescued.go\nblocked.go\n");
    th_write_file(TH_PATH(repo, ".cbmignore"), "!rescued.go\n");
    th_write_file(TH_PATH(repo, "main.go"), "package main\n");
    th_write_file(TH_PATH(repo, "rescued.go"), "package rescued\n");
    th_write_file(TH_PATH(repo, "blocked.go"), "package blocked\n");

    cbm_discover_opts_t opts = {0};
    cbm_file_info_t *files = NULL;
    int count = 0;

    int rc = cbm_discover(repo, &opts, &files, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 2);
    ASSERT_TRUE(discover_has_rel_path(files, count, "main.go"));
    ASSERT_TRUE(discover_has_rel_path(files, count, "rescued.go"));
    ASSERT_FALSE(discover_has_rel_path(files, count, "blocked.go"));

    cbm_discover_free(files, count);
    restore_git_env(&env);
    th_cleanup(base);
    PASS();
}

/* issue #234: a directory listed in the root .gitignore (e.g. "vendor/") must
 * be excluded from discovery even when untracked — Composer/PHP projects rely
 * on this. */
TEST(discover_gitignore_dir_excluded_issue234) {
    char *base = th_mktempdir("cbm_disc_gi234");
    ASSERT(base != NULL);

    th_mkdir_p(TH_PATH(base, ".git"));
    th_write_file(TH_PATH(base, ".gitignore"), "vendor/\n");
    th_write_file(TH_PATH(base, "src/main.php"), "<?php\nfunction appmain() {}\n");
    th_write_file(TH_PATH(base, "vendor/autoload.php"), "<?php\nfunction autoload() {}\n");
    th_write_file(TH_PATH(base, "vendor/pkg/lib.php"), "<?php\nfunction lib() {}\n");

    cbm_discover_opts_t opts = {0};
    cbm_file_info_t *files = NULL;
    int count = 0;

    int rc = cbm_discover(base, &opts, &files, &count);
    ASSERT_EQ(rc, 0);
    /* Nothing under vendor/ should be discovered. */
    for (int i = 0; i < count; i++) {
        ASSERT_TRUE(strstr(files[i].rel_path, "vendor") == NULL);
    }
    ASSERT_EQ(count, 1); /* only src/main.php */

    cbm_discover_free(files, count);
    th_cleanup(base);
    PASS();
}

TEST(discover_max_file_size) {
    char *base = th_mktempdir("cbm_disc_size");
    ASSERT(base != NULL);

    th_write_file(TH_PATH(base, "small.go"), "small\n");
    /* Create a large file (> 1KB) */
    char bigpath[512];
    snprintf(bigpath, sizeof(bigpath), "%s/big.go", base);
    FILE *f = fopen(bigpath, "w");
    ASSERT(f != NULL);
    for (int i = 0; i < 200; i++) {
        fprintf(f, "// padding line %d to exceed 1KB\n", i);
    }
    fclose(f);

    cbm_discover_opts_t opts = {0};
    opts.max_file_size = 1024;
    cbm_file_info_t *files = NULL;
    int count = 0;

    int rc = cbm_discover(base, &opts, &files, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 1);

    cbm_discover_free(files, count);
    th_cleanup(base);
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
    char *base = th_mktempdir("cbm_disc_noexist");
    char fake[512];
    snprintf(fake, sizeof(fake), "%s/nonexistent_12345", base ? base : "/tmp");
    cbm_file_info_t *files = NULL;
    int count = 0;
    int rc = cbm_discover(fake, NULL, &files, &count);
    ASSERT_EQ(rc, -1);
    th_cleanup(base);
    PASS();
}

TEST(discover_free_null) {
    cbm_discover_free(NULL, 0);
    PASS();
}

TEST(discover_skips_worktrees) {
    char *base = th_mktempdir("cbm_disc_wt");
    ASSERT(base != NULL);

    th_write_file(TH_PATH(base, "src/main.go"), "package main\n");
    th_write_file(TH_PATH(base, ".worktrees/feature/src/app.go"), "package app\n");

    cbm_discover_opts_t opts = {0};
    cbm_file_info_t *files = NULL;
    int count = 0;

    int rc = cbm_discover(base, &opts, &files, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 1);

    bool found_main = false;
    for (int i = 0; i < count; i++) {
        if (strstr(files[i].rel_path, "main.go"))
            found_main = true;
        ASSERT_NULL(strstr(files[i].rel_path, ".worktrees"));
    }
    ASSERT_TRUE(found_main);

    cbm_discover_free(files, count);
    th_cleanup(base);
    PASS();
}

TEST(discover_cbmignore) {
    char *base = th_mktempdir("cbm_disc_cbmi");
    ASSERT(base != NULL);

    th_mkdir_p(TH_PATH(base, ".git"));
    th_write_file(TH_PATH(base, ".cbmignore"), "generated/\n*.pb.go\n");
    th_write_file(TH_PATH(base, "main.go"), "package main\n");
    th_write_file(TH_PATH(base, "generated/types.go"), "package gen\n");
    th_write_file(TH_PATH(base, "api.pb.go"), "package api\n");

    cbm_discover_opts_t opts = {0};
    cbm_file_info_t *files = NULL;
    int count = 0;

    int rc = cbm_discover(base, &opts, &files, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 1);
    ASSERT_TRUE(strstr(files[0].rel_path, "main.go") != NULL);

    cbm_discover_free(files, count);
    th_cleanup(base);
    PASS();
}

TEST(discover_cbmignore_stacks) {
    char *base = th_mktempdir("cbm_disc_stack");
    ASSERT(base != NULL);

    th_mkdir_p(TH_PATH(base, ".git"));
    th_write_file(TH_PATH(base, ".gitignore"), "*.log\n");
    th_write_file(TH_PATH(base, ".cbmignore"), "docs/\n");
    th_write_file(TH_PATH(base, "main.go"), "package main\n");
    th_write_file(TH_PATH(base, "docs/api.go"), "package docs\n");

    cbm_discover_opts_t opts = {0};
    cbm_file_info_t *files = NULL;
    int count = 0;

    int rc = cbm_discover(base, &opts, &files, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 1);

    bool found_docs = false;
    for (int i = 0; i < count; i++) {
        if (strstr(files[i].rel_path, "docs/"))
            found_docs = true;
    }
    ASSERT_FALSE(found_docs);

    cbm_discover_free(files, count);
    th_cleanup(base);
    PASS();
}

TEST(discover_symlink_skipped) {
#ifdef _WIN32
    /* Symlinks require elevated privileges on Windows — skip.
     * Guard the entire body: symlink() doesn't exist on Windows. */
    SKIP_PLATFORM("Windows: symlinks need admin / symlink() unavailable");
#else
    char *base = th_mktempdir("cbm_disc_sym");
    ASSERT(base != NULL);

    th_write_file(TH_PATH(base, "real.go"), "package main\n");
    char real_path[512], link_path[512];
    snprintf(real_path, sizeof(real_path), "%s/real.go", base);
    snprintf(link_path, sizeof(link_path), "%s/link.go", base);
    symlink(real_path, link_path);

    cbm_discover_opts_t opts = {0};
    cbm_file_info_t *files = NULL;
    int count = 0;

    int rc = cbm_discover(base, &opts, &files, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 1);

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
    th_cleanup(base);
    PASS();
#endif
}

TEST(discover_new_ignore_patterns) {
    char *base = th_mktempdir("cbm_disc_newign");
    ASSERT(base != NULL);

    const char *dirs[] = {".next", ".terraform", "zig-cache", ".cargo", "elm-stuff", "bazel-out"};
    for (int i = 0; i < 6; i++) {
        char path[512];
        snprintf(path, sizeof(path), "%s/%s/file.go", base, dirs[i]);
        th_write_file(path, "package x\n");
    }
    th_write_file(TH_PATH(base, "main.go"), "package main\n");

    cbm_discover_opts_t opts = {0};
    cbm_file_info_t *files = NULL;
    int count = 0;

    int rc = cbm_discover(base, &opts, &files, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 1);
    ASSERT_TRUE(strstr(files[0].rel_path, "main.go") != NULL);

    cbm_discover_free(files, count);
    th_cleanup(base);
    PASS();
}

TEST(discover_generic_dirs_full_mode) {
    char *base = th_mktempdir("cbm_disc_genfull");
    ASSERT(base != NULL);

    th_write_file(TH_PATH(base, "bin/main.go"), "package bin\n");
    th_write_file(TH_PATH(base, "build/main.go"), "package build\n");
    th_write_file(TH_PATH(base, "out/main.go"), "package out\n");

    cbm_discover_opts_t opts = {.mode = CBM_MODE_FULL};
    cbm_file_info_t *files = NULL;
    int count = 0;

    int rc = cbm_discover(base, &opts, &files, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 3);

    cbm_discover_free(files, count);
    th_cleanup(base);
    PASS();
}

TEST(discover_generic_dirs_fast_mode) {
    char *base = th_mktempdir("cbm_disc_genfast");
    ASSERT(base != NULL);

    th_write_file(TH_PATH(base, "bin/main.go"), "package bin\n");
    th_write_file(TH_PATH(base, "build/main.go"), "package build\n");
    th_write_file(TH_PATH(base, "out/main.go"), "package out\n");

    cbm_discover_opts_t opts = {.mode = CBM_MODE_FAST};
    cbm_file_info_t *files = NULL;
    int count = 0;

    int rc = cbm_discover(base, &opts, &files, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 0);

    cbm_discover_free(files, count);
    th_cleanup(base);
    PASS();
}

TEST(discover_deploy_excluded_full_mode) {
    char *base = th_mktempdir("cbm_disc_deploy");
    ASSERT(base != NULL);

    th_write_file(TH_PATH(base, "src/main.go"), "package main\n");
    th_write_file(TH_PATH(base, "deploy/main.go"), "package deploy\n");
    th_write_file(TH_PATH(base, "deployed/main.go"), "package deployed\n");

    cbm_discover_opts_t opts = {.mode = CBM_MODE_FULL};
    cbm_file_info_t *files = NULL;
    int count = 0;

    int rc = cbm_discover(base, &opts, &files, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 1);

    cbm_discover_free(files, count);
    th_cleanup(base);
    PASS();
}

TEST(discover_cbmignore_no_git) {
    char *base = th_mktempdir("cbm_disc_nogit");
    ASSERT(base != NULL);

    th_write_file(TH_PATH(base, ".cbmignore"), "scratch/\n");
    th_write_file(TH_PATH(base, "main.go"), "package main\n");
    th_write_file(TH_PATH(base, "scratch/tmp.go"), "package scratch\n");

    cbm_discover_opts_t opts = {0};
    cbm_file_info_t *files = NULL;
    int count = 0;

    int rc = cbm_discover(base, &opts, &files, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 1);
    ASSERT_TRUE(strstr(files[0].rel_path, "main.go") != NULL);

    cbm_discover_free(files, count);
    th_cleanup(base);
    PASS();
}

/* ── .git/info/exclude tests (issue #489) ─────────────────────── */

/* Per-clone excludes written to .git/info/exclude (not committed) must be
 * honored the same as .gitignore.  Without this, repos that keep worktrees
 * under a path excluded only via info/exclude hit OOM during indexing. */
TEST(discover_git_info_exclude) {
    char *base = th_mktempdir("cbm_disc_exc");
    ASSERT(base != NULL);

    th_mkdir_p(TH_PATH(base, ".git/info"));
    th_write_file(TH_PATH(base, ".git/info/exclude"), "worktrees/\n");
    th_write_file(TH_PATH(base, "src/main.go"), "package main\n");
    th_write_file(TH_PATH(base, "worktrees/feature/app.go"), "package app\n");

    cbm_discover_opts_t opts = {0};
    cbm_file_info_t *files = NULL;
    int count = 0;
    int rc = cbm_discover(base, &opts, &files, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 1);
    ASSERT_TRUE(strstr(files[0].rel_path, "main.go") != NULL);

    cbm_discover_free(files, count);
    th_cleanup(base);
    PASS();
}

TEST(discover_git_info_exclude_stacks_with_gitignore) {
    char *base = th_mktempdir("cbm_disc_exc_stack");
    ASSERT(base != NULL);

    th_mkdir_p(TH_PATH(base, ".git/info"));
    th_write_file(TH_PATH(base, ".gitignore"), "*.log\n");
    th_write_file(TH_PATH(base, ".git/info/exclude"), "scratch/\n");
    th_write_file(TH_PATH(base, "main.go"), "package main\n");
    th_write_file(TH_PATH(base, "debug.log"), "log\n");
    th_write_file(TH_PATH(base, "scratch/tmp.go"), "package scratch\n");

    cbm_discover_opts_t opts = {0};
    cbm_file_info_t *files = NULL;
    int count = 0;
    int rc = cbm_discover(base, &opts, &files, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 1);

    bool found_log = false;
    bool found_scratch = false;
    for (int i = 0; i < count; i++) {
        if (strstr(files[i].rel_path, ".log"))    found_log     = true;
        if (strstr(files[i].rel_path, "scratch")) found_scratch = true;
    }
    ASSERT_FALSE(found_log);
    ASSERT_FALSE(found_scratch);
    ASSERT_TRUE(strstr(files[0].rel_path, "main.go") != NULL);

    cbm_discover_free(files, count);
    th_cleanup(base);
    PASS();
}

/* ── Nested .gitignore tests (issue #178) ──────────────────────── */

TEST(discover_nested_gitignore) {
    char *base = th_mktempdir("cbm_disc_ngi");
    ASSERT(base != NULL);

    th_mkdir_p(TH_PATH(base, ".git"));
    th_write_file(TH_PATH(base, "main.go"), "package main\n");
    th_write_file(TH_PATH(base, "webapp/.gitignore"), "generated/\n");
    th_write_file(TH_PATH(base, "webapp/src/routes.js"), "export default []\n");
    th_write_file(TH_PATH(base, "webapp/generated/types.js"), "export {}\n");

    cbm_discover_opts_t opts = {0};
    cbm_file_info_t *files = NULL;
    int count = 0;
    int rc = cbm_discover(base, &opts, &files, &count);
    ASSERT_EQ(rc, 0);

    bool found_generated = false;
    bool found_routes = false;
    for (int i = 0; i < count; i++) {
        if (strstr(files[i].rel_path, "generated"))
            found_generated = true;
        if (strstr(files[i].rel_path, "routes.js"))
            found_routes = true;
    }
    ASSERT_FALSE(found_generated);
    ASSERT_TRUE(found_routes);

    cbm_discover_free(files, count);
    th_cleanup(base);
    PASS();
}

TEST(discover_nested_gitignore_stacks_with_root) {
    char *base = th_mktempdir("cbm_disc_ngi_stack");
    ASSERT(base != NULL);

    th_mkdir_p(TH_PATH(base, ".git"));
    th_write_file(TH_PATH(base, ".gitignore"), "*.log\n");
    th_write_file(TH_PATH(base, "webapp/.gitignore"), ".output/\n");
    th_write_file(TH_PATH(base, "main.go"), "package main\n");
    th_write_file(TH_PATH(base, "error.log"), "error log\n");
    th_write_file(TH_PATH(base, "webapp/src/app.js"), "const x = 1\n");
    th_write_file(TH_PATH(base, "webapp/.output/data.js"), "output data\n");

    cbm_discover_opts_t opts = {0};
    cbm_file_info_t *files = NULL;
    int count = 0;
    int rc = cbm_discover(base, &opts, &files, &count);
    ASSERT_EQ(rc, 0);

    bool found_log = false;
    bool found_output = false;
    bool found_main = false;
    bool found_app = false;
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
    ASSERT_FALSE(found_log);
    ASSERT_FALSE(found_output);
    ASSERT_TRUE(found_main);
    ASSERT_TRUE(found_app);

    cbm_discover_free(files, count);
    th_cleanup(base);
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
    RUN_TEST(skip_vendored);
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

    /* Integration tests (cross-platform) */
    RUN_TEST(discover_simple);
    RUN_TEST(discover_skips_git_dir);
    RUN_TEST(discover_with_gitignore);
    RUN_TEST(discover_with_global_xdg_ignore);
    RUN_TEST(discover_global_excludesfile_from_gitconfig_tilde);
    RUN_TEST(discover_repo_local_excludesfile_is_ignored);
    RUN_TEST(discover_missing_global_excludes_is_noop);
    RUN_TEST(discover_cbmignore_negates_global_ignore);
    RUN_TEST(discover_gitignore_dir_excluded_issue234);
    RUN_TEST(discover_max_file_size);
    RUN_TEST(discover_null_path);
    RUN_TEST(discover_nonexistent_path);
    RUN_TEST(discover_free_null);

    /* Go test ports (cross-platform) */
    RUN_TEST(discover_skips_worktrees);
    RUN_TEST(discover_cbmignore);
    RUN_TEST(discover_cbmignore_stacks);
    RUN_TEST(discover_symlink_skipped);
    RUN_TEST(discover_new_ignore_patterns);
    RUN_TEST(discover_generic_dirs_full_mode);
    RUN_TEST(discover_generic_dirs_fast_mode);
    RUN_TEST(discover_deploy_excluded_full_mode);
    RUN_TEST(discover_cbmignore_no_git);

    /* .git/info/exclude support (issue #489) */
    RUN_TEST(discover_git_info_exclude);
    RUN_TEST(discover_git_info_exclude_stacks_with_gitignore);

    /* Nested .gitignore tests (issue #178) */
    RUN_TEST(discover_nested_gitignore);
    RUN_TEST(discover_nested_gitignore_stacks_with_root);
}
