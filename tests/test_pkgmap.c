/*
 * test_pkgmap.c — Tests for package specifier resolution (pass_pkgmap.c).
 *
 * Covers: cbm_pipeline_build_pkg_map, cbm_pipeline_free_pkg_map,
 *         cbm_pipeline_resolve_module.
 */
#include "test_framework.h"
#include "../src/pipeline/pipeline_internal.h"
#include "../src/foundation/compat.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

/* ── Helpers ────────────────────────────────────────────────────── */

/* Write content to a file (creating parent dirs must be done first). */
static int write_file(const char *path, const char *content) {
    FILE *f = fopen(path, "w");
    if (!f) {
        return -1;
    }
    fputs(content, f);
    fclose(f);
    return 0;
}

/* Create directory and all parents. Returns 0 on success. */
static int make_dirs(const char *path) {
    char tmp[1024];
    snprintf(tmp, sizeof(tmp), "%s", path);
    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            (void)mkdir(tmp, 0755);
            *p = '/';
        }
    }
    return mkdir(tmp, 0755) == 0 || errno == EEXIST ? 0 : -1;
}

/* Recursively remove a directory tree (simple: up to 2 levels deep). */
static void remove_tree(const char *root) {
    char cmd[1024];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", root);
    (void)system(cmd);
}

/* ── Fixture: minimal monorepo temp directory ─────────────────── */

static char g_tmpdir[512];

/* Build a monorepo fixture:
 *
 * tmpdir/
 *   packages/utils/package.json     {"name":"@test/utils","main":"src/index.ts"}
 *   packages/utils/src/index.ts     (empty)
 *   packages/utils/src/helpers.ts   (empty, for subpath test)
 *   packages/core/package.json      {"name":"test-core","main":"index.js"}
 *   packages/core/index.js          (empty)
 *   packages/exports/package.json   {"name":"@test/exports","exports":{".":\
 *                                    {"import":"./src/main.ts"}}}
 *   packages/exports/src/main.ts    (empty)
 *   packages/noname/package.json    {} (no "name" — should be skipped)
 *   packages/noentry/package.json   {"name":"test-noentry"} (no entry point — skipped)
 */
static int setup_monorepo(void) {
    char path[1024];

    /* @test/utils with "main" entry point */
    snprintf(path, sizeof(path), "%s/packages/utils/src", g_tmpdir);
    if (make_dirs(path) != 0) {
        return -1;
    }
    snprintf(path, sizeof(path), "%s/packages/utils/package.json", g_tmpdir);
    if (write_file(path, "{\"name\":\"@test/utils\",\"main\":\"src/index.ts\"}") != 0) {
        return -1;
    }
    snprintf(path, sizeof(path), "%s/packages/utils/src/index.ts", g_tmpdir);
    if (write_file(path, "export function helper() {}") != 0) {
        return -1;
    }
    snprintf(path, sizeof(path), "%s/packages/utils/src/helpers.ts", g_tmpdir);
    if (write_file(path, "export function extra() {}") != 0) {
        return -1;
    }

    /* test-core with "main" entry point */
    snprintf(path, sizeof(path), "%s/packages/core", g_tmpdir);
    if (make_dirs(path) != 0) {
        return -1;
    }
    snprintf(path, sizeof(path), "%s/packages/core/package.json", g_tmpdir);
    if (write_file(path, "{\"name\":\"test-core\",\"main\":\"index.js\"}") != 0) {
        return -1;
    }
    snprintf(path, sizeof(path), "%s/packages/core/index.js", g_tmpdir);
    if (write_file(path, "module.exports = {};") != 0) {
        return -1;
    }

    /* @test/exports with "exports"."."."import" entry point */
    snprintf(path, sizeof(path), "%s/packages/exports/src", g_tmpdir);
    if (make_dirs(path) != 0) {
        return -1;
    }
    snprintf(path, sizeof(path), "%s/packages/exports/package.json", g_tmpdir);
    if (write_file(path,
                   "{\"name\":\"@test/exports\","
                   "\"exports\":{\".\":{\"import\":\"./src/main.ts\"}}}") != 0) {
        return -1;
    }
    snprintf(path, sizeof(path), "%s/packages/exports/src/main.ts", g_tmpdir);
    if (write_file(path, "export {};") != 0) {
        return -1;
    }

    /* Package with no "name" — should be skipped */
    snprintf(path, sizeof(path), "%s/packages/noname", g_tmpdir);
    if (make_dirs(path) != 0) {
        return -1;
    }
    snprintf(path, sizeof(path), "%s/packages/noname/package.json", g_tmpdir);
    if (write_file(path, "{\"version\":\"1.0.0\"}") != 0) {
        return -1;
    }

    /* Package with name but no resolvable entry point — should be skipped */
    snprintf(path, sizeof(path), "%s/packages/noentry", g_tmpdir);
    if (make_dirs(path) != 0) {
        return -1;
    }
    snprintf(path, sizeof(path), "%s/packages/noentry/package.json", g_tmpdir);
    if (write_file(path, "{\"name\":\"test-noentry\",\"main\":\"nonexistent.js\"}") != 0) {
        return -1;
    }

    return 0;
}

/* ================================================================
 * cbm_pipeline_build_pkg_map tests
 * ================================================================ */

TEST(pkgmap_build_returns_map) {
    CBMHashTable *map = cbm_pipeline_build_pkg_map(g_tmpdir, "testproj");
    ASSERT_NOT_NULL(map);
    /* At least @test/utils and test-core must be present */
    ASSERT_TRUE(cbm_ht_count(map) >= 2);
    cbm_pipeline_free_pkg_map(map);
    PASS();
}

TEST(pkgmap_build_contains_scoped_package) {
    CBMHashTable *map = cbm_pipeline_build_pkg_map(g_tmpdir, "testproj");
    ASSERT_NOT_NULL(map);
    ASSERT_TRUE(cbm_ht_has(map, "@test/utils"));
    cbm_pipeline_free_pkg_map(map);
    PASS();
}

TEST(pkgmap_build_contains_bare_package) {
    CBMHashTable *map = cbm_pipeline_build_pkg_map(g_tmpdir, "testproj");
    ASSERT_NOT_NULL(map);
    ASSERT_TRUE(cbm_ht_has(map, "test-core"));
    cbm_pipeline_free_pkg_map(map);
    PASS();
}

TEST(pkgmap_build_skips_unnamed_package) {
    CBMHashTable *map = cbm_pipeline_build_pkg_map(g_tmpdir, "testproj");
    ASSERT_NOT_NULL(map);
    /* No key should exist for the unnamed package */
    ASSERT_FALSE(cbm_ht_has(map, ""));
    cbm_pipeline_free_pkg_map(map);
    PASS();
}

TEST(pkgmap_build_skips_no_entry_point) {
    CBMHashTable *map = cbm_pipeline_build_pkg_map(g_tmpdir, "testproj");
    ASSERT_NOT_NULL(map);
    /* test-noentry has a name but no resolvable entry point → skipped */
    ASSERT_FALSE(cbm_ht_has(map, "test-noentry"));
    cbm_pipeline_free_pkg_map(map);
    PASS();
}

TEST(pkgmap_build_main_entry_qn) {
    CBMHashTable *map = cbm_pipeline_build_pkg_map(g_tmpdir, "testproj");
    ASSERT_NOT_NULL(map);
    /* @test/utils "main": "src/index.ts"
     * Expected QN: testproj.packages.utils.src.index */
    cbm_pkg_entry_t *entry = (cbm_pkg_entry_t *)cbm_ht_get(map, "@test/utils");
    ASSERT_NOT_NULL(entry);
    ASSERT_NOT_NULL(entry->module_qn);
    ASSERT_STR_EQ(entry->module_qn, "testproj.packages.utils.src.index");
    cbm_pipeline_free_pkg_map(map);
    PASS();
}

TEST(pkgmap_build_pkg_dir_correct) {
    CBMHashTable *map = cbm_pipeline_build_pkg_map(g_tmpdir, "testproj");
    ASSERT_NOT_NULL(map);
    cbm_pkg_entry_t *entry = (cbm_pkg_entry_t *)cbm_ht_get(map, "@test/utils");
    ASSERT_NOT_NULL(entry);
    ASSERT_NOT_NULL(entry->pkg_dir);
    ASSERT_STR_EQ(entry->pkg_dir, "packages/utils");
    cbm_pipeline_free_pkg_map(map);
    PASS();
}

TEST(pkgmap_build_exports_dot_import_entry) {
    CBMHashTable *map = cbm_pipeline_build_pkg_map(g_tmpdir, "testproj");
    ASSERT_NOT_NULL(map);
    /* @test/exports has "exports"."."."import": "./src/main.ts"
     * Expected QN: testproj.packages.exports.src.main */
    cbm_pkg_entry_t *entry = (cbm_pkg_entry_t *)cbm_ht_get(map, "@test/exports");
    ASSERT_NOT_NULL(entry);
    ASSERT_NOT_NULL(entry->module_qn);
    ASSERT_STR_EQ(entry->module_qn, "testproj.packages.exports.src.main");
    cbm_pipeline_free_pkg_map(map);
    PASS();
}

TEST(pkgmap_build_null_inputs) {
    /* Both NULL → NULL */
    CBMHashTable *map = cbm_pipeline_build_pkg_map(NULL, NULL);
    ASSERT_NULL(map);

    /* NULL project → NULL */
    map = cbm_pipeline_build_pkg_map(g_tmpdir, NULL);
    ASSERT_NULL(map);

    /* NULL repo → NULL */
    map = cbm_pipeline_build_pkg_map(NULL, "testproj");
    ASSERT_NULL(map);
    PASS();
}

TEST(pkgmap_build_no_packages_dir) {
    /* Empty directory with no package.json → NULL */
    char empty_dir[512];
    snprintf(empty_dir, sizeof(empty_dir), "%s/empty_XXXXXX", g_tmpdir);
    if (!cbm_mkdtemp(empty_dir)) {
        SKIP("mkdtemp failed");
    }
    CBMHashTable *map = cbm_pipeline_build_pkg_map(empty_dir, "testproj");
    ASSERT_NULL(map);
    remove_tree(empty_dir);
    PASS();
}

TEST(pkgmap_free_null_safe) {
    /* Must not crash */
    cbm_pipeline_free_pkg_map(NULL);
    PASS();
}

/* ================================================================
 * cbm_pipeline_resolve_module tests
 * ================================================================ */

/* Build a minimal ctx with the monorepo pkg_map */
static cbm_pipeline_ctx_t make_ctx(CBMHashTable *pkg_map) {
    cbm_pipeline_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.project_name = "testproj";
    ctx.repo_path = g_tmpdir;
    ctx.pkg_map = pkg_map;
    return ctx;
}

TEST(resolve_module_null_pkg_map_falls_through) {
    /* With NULL pkg_map, must fall through to fqn_module */
    cbm_pipeline_ctx_t ctx = make_ctx(NULL);
    char *qn = cbm_pipeline_resolve_module(&ctx, "src/app.ts");
    ASSERT_NOT_NULL(qn);
    /* fqn_module("testproj", "src/app.ts") → "testproj.src.app" */
    ASSERT_STR_EQ(qn, "testproj.src.app");
    free(qn);
    PASS();
}

TEST(resolve_module_relative_path_falls_through) {
    CBMHashTable *map = cbm_pipeline_build_pkg_map(g_tmpdir, "testproj");
    ASSERT_NOT_NULL(map);
    cbm_pipeline_ctx_t ctx = make_ctx(map);

    /* Relative path must bypass the map and go to fqn_module */
    char *qn = cbm_pipeline_resolve_module(&ctx, "./utils");
    ASSERT_NOT_NULL(qn);
    /* fqn_module("testproj", "./utils") treats it as a path segment */
    ASSERT_NOT_NULL(qn);
    free(qn);

    cbm_pipeline_free_pkg_map(map);
    PASS();
}

TEST(resolve_module_exact_scoped_match) {
    CBMHashTable *map = cbm_pipeline_build_pkg_map(g_tmpdir, "testproj");
    ASSERT_NOT_NULL(map);
    cbm_pipeline_ctx_t ctx = make_ctx(map);

    char *qn = cbm_pipeline_resolve_module(&ctx, "@test/utils");
    ASSERT_NOT_NULL(qn);
    ASSERT_STR_EQ(qn, "testproj.packages.utils.src.index");
    free(qn);

    cbm_pipeline_free_pkg_map(map);
    PASS();
}

TEST(resolve_module_exact_bare_match) {
    CBMHashTable *map = cbm_pipeline_build_pkg_map(g_tmpdir, "testproj");
    ASSERT_NOT_NULL(map);
    cbm_pipeline_ctx_t ctx = make_ctx(map);

    char *qn = cbm_pipeline_resolve_module(&ctx, "test-core");
    ASSERT_NOT_NULL(qn);
    ASSERT_STR_EQ(qn, "testproj.packages.core.index");
    free(qn);

    cbm_pipeline_free_pkg_map(map);
    PASS();
}

TEST(resolve_module_subpath_scoped) {
    CBMHashTable *map = cbm_pipeline_build_pkg_map(g_tmpdir, "testproj");
    ASSERT_NOT_NULL(map);
    cbm_pipeline_ctx_t ctx = make_ctx(map);

    /* "@test/utils/src/helpers" → packages/utils/src/helpers.ts exists */
    char *qn = cbm_pipeline_resolve_module(&ctx, "@test/utils/src/helpers");
    ASSERT_NOT_NULL(qn);
    ASSERT_STR_EQ(qn, "testproj.packages.utils.src.helpers");
    free(qn);

    cbm_pipeline_free_pkg_map(map);
    PASS();
}

TEST(resolve_module_unknown_package_falls_through) {
    CBMHashTable *map = cbm_pipeline_build_pkg_map(g_tmpdir, "testproj");
    ASSERT_NOT_NULL(map);
    cbm_pipeline_ctx_t ctx = make_ctx(map);

    /* Unknown package — must not crash, must return a non-NULL string */
    char *qn = cbm_pipeline_resolve_module(&ctx, "react");
    ASSERT_NOT_NULL(qn);
    /* Falls through to fqn_module — result is defined but not a real QN */
    free(qn);

    cbm_pipeline_free_pkg_map(map);
    PASS();
}

TEST(resolve_module_unknown_scoped_subpath_falls_through) {
    CBMHashTable *map = cbm_pipeline_build_pkg_map(g_tmpdir, "testproj");
    ASSERT_NOT_NULL(map);
    cbm_pipeline_ctx_t ctx = make_ctx(map);

    /* Package exists but subpath does not resolve to a file */
    char *qn = cbm_pipeline_resolve_module(&ctx, "@test/utils/nonexistent/deep/path");
    ASSERT_NOT_NULL(qn);
    /* Falls through to fqn_module */
    free(qn);

    cbm_pipeline_free_pkg_map(map);
    PASS();
}

TEST(resolve_module_absolute_path_falls_through) {
    CBMHashTable *map = cbm_pipeline_build_pkg_map(g_tmpdir, "testproj");
    ASSERT_NOT_NULL(map);
    cbm_pipeline_ctx_t ctx = make_ctx(map);

    char *qn = cbm_pipeline_resolve_module(&ctx, "/absolute/path/module");
    ASSERT_NOT_NULL(qn);
    free(qn);

    cbm_pipeline_free_pkg_map(map);
    PASS();
}

TEST(resolve_module_exports_dot_import_resolves) {
    CBMHashTable *map = cbm_pipeline_build_pkg_map(g_tmpdir, "testproj");
    ASSERT_NOT_NULL(map);
    cbm_pipeline_ctx_t ctx = make_ctx(map);

    /* @test/exports uses "exports"."."."import" entry */
    char *qn = cbm_pipeline_resolve_module(&ctx, "@test/exports");
    ASSERT_NOT_NULL(qn);
    ASSERT_STR_EQ(qn, "testproj.packages.exports.src.main");
    free(qn);

    cbm_pipeline_free_pkg_map(map);
    PASS();
}

/* ================================================================
 * Suite
 * ================================================================ */

SUITE(pkgmap) {
    /* Create temp monorepo fixture */
    snprintf(g_tmpdir, sizeof(g_tmpdir), "/tmp/cbm_pkgmap_XXXXXX");
    if (!cbm_mkdtemp(g_tmpdir)) {
        printf("  SKIP (mkdtemp failed)\n");
        return;
    }
    if (setup_monorepo() != 0) {
        remove_tree(g_tmpdir);
        printf("  SKIP (fixture setup failed)\n");
        return;
    }

    /* build_pkg_map */
    RUN_TEST(pkgmap_build_returns_map);
    RUN_TEST(pkgmap_build_contains_scoped_package);
    RUN_TEST(pkgmap_build_contains_bare_package);
    RUN_TEST(pkgmap_build_skips_unnamed_package);
    RUN_TEST(pkgmap_build_skips_no_entry_point);
    RUN_TEST(pkgmap_build_main_entry_qn);
    RUN_TEST(pkgmap_build_pkg_dir_correct);
    RUN_TEST(pkgmap_build_exports_dot_import_entry);
    RUN_TEST(pkgmap_build_null_inputs);
    RUN_TEST(pkgmap_build_no_packages_dir);
    RUN_TEST(pkgmap_free_null_safe);

    /* resolve_module */
    RUN_TEST(resolve_module_null_pkg_map_falls_through);
    RUN_TEST(resolve_module_relative_path_falls_through);
    RUN_TEST(resolve_module_exact_scoped_match);
    RUN_TEST(resolve_module_exact_bare_match);
    RUN_TEST(resolve_module_subpath_scoped);
    RUN_TEST(resolve_module_unknown_package_falls_through);
    RUN_TEST(resolve_module_unknown_scoped_subpath_falls_through);
    RUN_TEST(resolve_module_absolute_path_falls_through);
    RUN_TEST(resolve_module_exports_dot_import_resolves);

    /* Cleanup */
    remove_tree(g_tmpdir);
}
