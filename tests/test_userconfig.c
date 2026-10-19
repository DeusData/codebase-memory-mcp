/*
 * test_userconfig.c — Tests for user-defined extension→language mappings.
 *
 * Tests cbm_userconfig_load(), cbm_userconfig_lookup(), and the
 * cbm_set_user_lang_config() / cbm_language_for_extension() integration.
 */
#include "../src/foundation/compat.h"
#include "../src/foundation/compat_fs.h"
#include "../src/foundation/platform.h"
#include "test_framework.h"
#include "discover/discover.h"
#include "discover/userconfig.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Helpers ─────────────────────────────────────────────────────── */

/* Write a JSON file to path. Returns 0 on success. */
static int write_json(const char *path, const char *json) {
    FILE *f = fopen(path, "w");
    if (!f) {
        return -1;
    }
    fputs(json, f);
    fclose(f);
    return 0;
}

/* ── Tests: project config ───────────────────────────────────────── */

TEST(userconfig_project_basic) {
    /* Write a .codebase-memory.json in a temp dir */
    char dir[256];
    snprintf(dir, sizeof(dir), "%s/uctest_proj_basic", cbm_tmpdir());
    cbm_mkdir_p(dir, 0755); /* from compat_fs.h via compat.h */

    char proj[512];
    snprintf(proj, sizeof(proj), "%s/.codebase-memory.json", dir);
    ASSERT_EQ(
        write_json(proj, "{\"extra_extensions\":{\".blade.php\":\"php\",\".mjs\":\"javascript\"}}"),
        0);

    cbm_userconfig_t *cfg = cbm_userconfig_load(dir);
    ASSERT_NOT_NULL(cfg);

    ASSERT_EQ(cbm_userconfig_lookup(cfg, ".blade.php"), CBM_LANG_PHP);
    ASSERT_EQ(cbm_userconfig_lookup(cfg, ".mjs"), CBM_LANG_JAVASCRIPT);
    ASSERT_EQ(cbm_userconfig_lookup(cfg, ".go"), CBM_LANG_COUNT); /* not in user config */

    cbm_userconfig_free(cfg);
    remove(proj);
    PASS();
}

/* ── Tests: global config ────────────────────────────────────────── */

TEST(userconfig_global_via_env) {
    /* Point config dir to a temp dir via the platform-appropriate env var:
     * XDG_CONFIG_HOME on Linux/macOS, APPDATA on Windows. */
    char cfg_dir[256];
    snprintf(cfg_dir, sizeof(cfg_dir), "%s/uctest_global_xdg", cbm_tmpdir());

    char app_dir[512];
    snprintf(app_dir, sizeof(app_dir), "%s/codebase-memory-mcp", cfg_dir);
    cbm_mkdir_p(app_dir, 0755);

    char global_path[768];
    snprintf(global_path, sizeof(global_path), "%s/config.json", app_dir);
    ASSERT_EQ(
        write_json(global_path, "{\"extra_extensions\":{\".twig\":\"html\"}}"),
        0);

#ifdef _WIN32
    char old_appdata[512] = "";
    cbm_safe_getenv("APPDATA", old_appdata, sizeof(old_appdata), NULL);
    cbm_setenv("APPDATA", cfg_dir, 1);
#else
    cbm_setenv("XDG_CONFIG_HOME", cfg_dir, 1);
#endif
    cbm_userconfig_t *cfg = cbm_userconfig_load(NULL); /* no project dir */
#ifdef _WIN32
    if (old_appdata[0]) {
        cbm_setenv("APPDATA", old_appdata, 1);
    } else {
        cbm_unsetenv("APPDATA");
    }
#else
    cbm_unsetenv("XDG_CONFIG_HOME");
#endif

    ASSERT_NOT_NULL(cfg);
    ASSERT_EQ(cbm_userconfig_lookup(cfg, ".twig"), CBM_LANG_HTML);

    cbm_userconfig_free(cfg);
    remove(global_path);
    PASS();
}

/* ── Tests: project wins over global ────────────────────────────── */

TEST(userconfig_project_wins_over_global) {
    /* Global says .xyz → python; project says .xyz → rust */
    char xdg_dir[256];
    snprintf(xdg_dir, sizeof(xdg_dir), "%s/uctest_priority_xdg", cbm_tmpdir());

    char app_dir[512];
    snprintf(app_dir, sizeof(app_dir), "%s/codebase-memory-mcp", xdg_dir);
    cbm_mkdir_p(app_dir, 0755);

    char global_path[768];
    snprintf(global_path, sizeof(global_path), "%s/config.json", app_dir);
    ASSERT_EQ(
        write_json(global_path, "{\"extra_extensions\":{\".xyz\":\"python\"}}"),
        0);

    char proj_dir[256];
    snprintf(proj_dir, sizeof(proj_dir), "%s/uctest_priority_proj", cbm_tmpdir());
    cbm_mkdir_p(proj_dir, 0755);

    char proj_path[512];
    snprintf(proj_path, sizeof(proj_path), "%s/.codebase-memory.json", proj_dir);
    ASSERT_EQ(
        write_json(proj_path, "{\"extra_extensions\":{\".xyz\":\"rust\"}}"),
        0);

    cbm_setenv("XDG_CONFIG_HOME", xdg_dir, 1);
    cbm_userconfig_t *cfg = cbm_userconfig_load(proj_dir);
    cbm_unsetenv("XDG_CONFIG_HOME");

    ASSERT_NOT_NULL(cfg);
    /* Project definition (rust) must win */
    ASSERT_EQ(cbm_userconfig_lookup(cfg, ".xyz"), CBM_LANG_RUST);

    cbm_userconfig_free(cfg);
    remove(global_path);
    remove(proj_path);
    PASS();
}

/* ── Tests: unknown language values are skipped ──────────────────── */

TEST(userconfig_unknown_lang_skipped) {
    char dir[256];
    snprintf(dir, sizeof(dir), "%s/uctest_unknown_lang", cbm_tmpdir());
    cbm_mkdir_p(dir, 0755);

    char proj[512];
    snprintf(proj, sizeof(proj), "%s/.codebase-memory.json", dir);
    /* "klingon" is not a valid language; ".wasm" should be silently skipped */
    ASSERT_EQ(
        write_json(proj,
                   "{\"extra_extensions\":{\".wasm\":\"klingon\",\".mjs\":\"javascript\"}}"),
        0);

    cbm_userconfig_t *cfg = cbm_userconfig_load(dir);
    ASSERT_NOT_NULL(cfg);

    /* .wasm with unknown lang → not in config */
    ASSERT_EQ(cbm_userconfig_lookup(cfg, ".wasm"), CBM_LANG_COUNT);
    /* .mjs with valid lang → present */
    ASSERT_EQ(cbm_userconfig_lookup(cfg, ".mjs"), CBM_LANG_JAVASCRIPT);

    cbm_userconfig_free(cfg);
    remove(proj);
    PASS();
}

/* ── Tests: missing files are silently ignored ───────────────────── */

TEST(userconfig_missing_files_ok) {
    /* Point to a non-existent repo dir */
    cbm_userconfig_t *cfg = cbm_userconfig_load("/tmp/__nonexistent_repo_12345__");
    ASSERT_NOT_NULL(cfg); /* must not return NULL — just empty */
    ASSERT_EQ(cfg->count, 0);
    cbm_userconfig_free(cfg);
    PASS();
}

/* ── Tests: integration with cbm_language_for_extension ─────────── */

TEST(userconfig_integration_override) {
    /* Verify that setting the global config makes cbm_language_for_extension
     * respect the override. We map ".blade.php" → PHP, which is not in the
     * built-in table. */
    char dir[256];
    snprintf(dir, sizeof(dir), "%s/uctest_integ", cbm_tmpdir());
    cbm_mkdir_p(dir, 0755);

    char proj[512];
    snprintf(proj, sizeof(proj), "%s/.codebase-memory.json", dir);
    ASSERT_EQ(
        write_json(proj, "{\"extra_extensions\":{\".blade.php\":\"php\"}}"),
        0);

    cbm_userconfig_t *cfg = cbm_userconfig_load(dir);
    ASSERT_NOT_NULL(cfg);

    /* Before setting, .blade.php is unknown */
    ASSERT_EQ(cbm_language_for_extension(".blade.php"), CBM_LANG_COUNT);

    cbm_set_user_lang_config(cfg);
    /* After setting, .blade.php → PHP */
    ASSERT_EQ(cbm_language_for_extension(".blade.php"), CBM_LANG_PHP);
    /* Built-in extensions still work */
    ASSERT_EQ(cbm_language_for_extension(".go"), CBM_LANG_GO);

    /* Clean up global state */
    cbm_set_user_lang_config(NULL);
    cbm_userconfig_free(cfg);
    remove(proj);
    PASS();
}

/* ── Tests: free is NULL-safe ────────────────────────────────────── */

TEST(userconfig_free_null) {
    cbm_userconfig_free(NULL); /* must not crash */
    PASS();
}

/* One-read project config evidence for declaration/index consumers. */
typedef struct {
    char dir[1024];
    char path[1280];
} source_fixture_t;

static bool source_fixture(source_fixture_t *f) {
    int n = snprintf(f->dir, sizeof(f->dir), "%s/uc_source_XXXXXX", cbm_tmpdir());
    if (n < 0 || (size_t)n >= sizeof(f->dir) || !cbm_mkdtemp(f->dir))
        return false;
    n = snprintf(f->path, sizeof(f->path), "%s/.codebase-memory.json", f->dir);
    return n > 0 && (size_t)n < sizeof(f->path);
}

static int source_write(const char *path, const char *bytes) {
    FILE *file = cbm_fopen(path, "wb");
    if (!file) return -1;
    size_t len = strlen(bytes);
    bool ok = fwrite(bytes, 1, len, file) == len;
    if (fclose(file) != 0) ok = false;
    return ok ? 0 : -1;
}

static void source_fixture_close(source_fixture_t *f) {
    (void)remove(f->path);
    (void)cbm_rmdir(f->path);
    (void)cbm_rmdir(f->dir);
}

TEST(userconfig_source_owned_and_shared) {
    source_fixture_t f;
    ASSERT(source_fixture(&f));
    const char before[] = " {\"extra_extensions\":{\".snap\":\"c\"}} \n";
    const char after[] = " {\"extra_extensions\":{\".snap\":\"r\"}} \n";
    ASSERT_EQ(source_write(f.path, before), 0);
    cbm_userconfig_t *legacy = cbm_userconfig_load(f.dir);
    cbm_userconfig_t *first = cbm_userconfig_load_with_source(f.dir);
    ASSERT_EQ(source_write(f.path, after), 0);
    cbm_userconfig_t *second = cbm_userconfig_load_with_source(f.dir);
    const char *bytes = NULL;
    size_t len = 0;
    bool captured = cbm_userconfig_project_source(first, &bytes, &len) ==
                        CBM_USERCONFIG_SOURCE_PRESENT &&
                    bytes && len == sizeof(before) - 1 && memcmp(bytes, before, len) == 0;
    bool shared = legacy && first &&
                  cbm_userconfig_lookup(legacy, ".snap") == CBM_LANG_C &&
                  cbm_userconfig_lookup(first, ".snap") == CBM_LANG_C &&
                  strcmp(legacy->project_source_sha256, first->project_source_sha256) == 0;
    const char *new_bytes = NULL;
    size_t new_len = 0;
    bool changed = cbm_userconfig_project_source(second, &new_bytes, &new_len) ==
                       CBM_USERCONFIG_SOURCE_PRESENT &&
                   new_bytes && new_len == sizeof(after) - 1 &&
                   memcmp(new_bytes, after, new_len) == 0 && first && second &&
                   strcmp(first->project_source_sha256, second->project_source_sha256) != 0;
    cbm_userconfig_free(legacy);
    cbm_userconfig_free(second);
    source_fixture_close(&f);
    /* Closing another owner and deleting the file cannot invalidate this view. */
    captured = captured && bytes && memcmp(bytes, before, sizeof(before) - 1) == 0;
    cbm_userconfig_free(first);
    ASSERT(captured);
    ASSERT(shared);
    ASSERT(changed);
    PASS();
}

TEST(userconfig_source_absence_is_explicit) {
    source_fixture_t f;
    ASSERT(source_fixture(&f));
    cbm_userconfig_t *legacy = cbm_userconfig_load(f.dir);
    cbm_userconfig_t *cfg = cbm_userconfig_load_with_source(f.dir);
    const char *bytes = "sentinel";
    size_t len = 99;
    bool absent = cbm_userconfig_project_source(cfg, &bytes, &len) ==
                      CBM_USERCONFIG_SOURCE_ABSENT &&
                  !bytes && !len;
    bool same_digest = legacy && cfg &&
                       strcmp(legacy->project_source_sha256, cfg->project_source_sha256) == 0;
    cbm_userconfig_free(legacy);
    cbm_userconfig_free(cfg);
    source_fixture_close(&f);
    ASSERT(absent);
    ASSERT(same_digest);
    PASS();
}

TEST(userconfig_source_empty_and_malformed_are_present) {
    source_fixture_t f;
    ASSERT(source_fixture(&f));
    ASSERT_EQ(source_write(f.path, ""), 0);
    cbm_userconfig_t *empty = cbm_userconfig_load_with_source(f.dir);
    const char *bytes = NULL;
    size_t len = 17;
    bool empty_present = cbm_userconfig_project_source(empty, &bytes, &len) ==
                             CBM_USERCONFIG_SOURCE_PRESENT &&
                         bytes && !len && bytes[0] == '\0';
    const char malformed[] = "{ incomplete";
    ASSERT_EQ(source_write(f.path, malformed), 0);
    cbm_userconfig_t *bad = cbm_userconfig_load_with_source(f.dir);
    bool bad_present = cbm_userconfig_project_source(bad, &bytes, &len) ==
                           CBM_USERCONFIG_SOURCE_PRESENT &&
                       bytes && len == sizeof(malformed) - 1 &&
                       memcmp(bytes, malformed, len) == 0;
    bool distinct = empty && bad &&
                    strcmp(empty->project_source_sha256, bad->project_source_sha256) != 0;
    cbm_userconfig_free(empty);
    cbm_userconfig_free(bad);
    source_fixture_close(&f);
    ASSERT(empty_present);
    ASSERT(bad_present);
    ASSERT(distinct);
    PASS();
}

TEST(userconfig_source_limit_and_errors) {
    source_fixture_t f;
    ASSERT(source_fixture(&f));
    char bytes[65538];
    memset(bytes, ' ', sizeof(bytes));
    bytes[0] = '{';
    bytes[1] = '}';
    bytes[65536] = '\0';
    ASSERT_EQ(source_write(f.path, bytes), 0);
    cbm_userconfig_t *maximum = cbm_userconfig_load_with_source(f.dir);
    const char *view = NULL;
    size_t len = 0;
    bool max_ok = cbm_userconfig_project_source(maximum, &view, &len) ==
                      CBM_USERCONFIG_SOURCE_PRESENT &&
                  view && len == 65536 && memcmp(view, bytes, len) == 0;
    bytes[65536] = ' ';
    bytes[65537] = '\0';
    ASSERT_EQ(source_write(f.path, bytes), 0);
    cbm_userconfig_t *oversized = cbm_userconfig_load_with_source(f.dir);
    view = "sentinel";
    len = 99;
    bool oversize_error = oversized &&
                          cbm_userconfig_project_source(oversized, &view, &len) ==
                              CBM_USERCONFIG_SOURCE_ERROR && !view && !len;
    ASSERT_EQ(remove(f.path), 0);
    ASSERT(cbm_mkdir_p(f.path, 0755));
    cbm_userconfig_t *directory = cbm_userconfig_load_with_source(f.dir);
    view = "sentinel";
    len = 99;
    bool directory_error = directory &&
                           cbm_userconfig_project_source(directory, &view, &len) ==
                               CBM_USERCONFIG_SOURCE_ERROR && !view && !len;
    cbm_userconfig_free(maximum);
    cbm_userconfig_free(oversized);
    cbm_userconfig_free(directory);
    source_fixture_close(&f);
    ASSERT(max_ok);
    ASSERT(oversize_error);
    ASSERT(directory_error);
    PASS();
}

TEST(userconfig_source_unavailable_clears_views) {
    const char *bytes = "sentinel";
    size_t len = 13;
    ASSERT_EQ(cbm_userconfig_project_source(NULL, &bytes, &len), CBM_USERCONFIG_SOURCE_ERROR);
    ASSERT_NULL(bytes);
    ASSERT_EQ(len, 0);
    cbm_userconfig_t *legacy = cbm_userconfig_load(NULL);
    ASSERT_NOT_NULL(legacy);
    bytes = "sentinel";
    len = 13;
    bool unavailable = cbm_userconfig_project_source(legacy, &bytes, &len) ==
                           CBM_USERCONFIG_SOURCE_ERROR && !bytes && !len;
    cbm_userconfig_free(legacy);
    ASSERT(unavailable);
    PASS();
}


/* ── Suite ──────────────────────────────────────────────────────── */

SUITE(userconfig) {
    RUN_TEST(userconfig_source_owned_and_shared);
    RUN_TEST(userconfig_source_absence_is_explicit);
    RUN_TEST(userconfig_source_empty_and_malformed_are_present);
    RUN_TEST(userconfig_source_limit_and_errors);
    RUN_TEST(userconfig_source_unavailable_clears_views);

    RUN_TEST(userconfig_project_basic);
    RUN_TEST(userconfig_global_via_env);
    RUN_TEST(userconfig_project_wins_over_global);
    RUN_TEST(userconfig_unknown_lang_skipped);
    RUN_TEST(userconfig_missing_files_ok);
    RUN_TEST(userconfig_integration_override);
    RUN_TEST(userconfig_free_null);
}
