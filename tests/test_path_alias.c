/*
 * test_path_alias.c -- Tests for build-tool path alias resolution.
 *
 * Covers the in-memory resolver (cbm_path_alias_resolve), the
 * directory-scoped collection lookup (cbm_path_alias_find_for_file),
 * and the resource-cap behaviour. Filesystem-based loading is exercised
 * indirectly via the integration test that builds a tmp tsconfig tree.
 */
#include "test_framework.h"
#include "../src/pipeline/path_alias.h"
#include "../src/foundation/compat.h"

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

/* Build a path alias map programmatically (no file I/O), respecting the
 * specificity ordering invariant the loader establishes via qsort. */
static cbm_path_alias_map_t *make_map(const char *base_url, int count, ...) {
    cbm_path_alias_map_t *map = calloc(1, sizeof(*map));
    map->base_url = base_url ? strdup(base_url) : NULL;
    map->entries = calloc((size_t)count, sizeof(cbm_path_alias_t));
    map->count = count;

    va_list args;
    va_start(args, count);
    for (int i = 0; i < count; i++) {
        const char *alias_pattern = va_arg(args, const char *);
        const char *target_pattern = va_arg(args, const char *);
        const char *star = strchr(alias_pattern, '*');
        if (star) {
            map->entries[i].has_wildcard = true;
            map->entries[i].alias_prefix =
                cbm_strndup(alias_pattern, (size_t)(star - alias_pattern));
            map->entries[i].alias_suffix = strdup(star + 1);
        } else {
            map->entries[i].has_wildcard = false;
            map->entries[i].alias_prefix = strdup(alias_pattern);
            map->entries[i].alias_suffix = strdup("");
        }
        const char *tstar = strchr(target_pattern, '*');
        if (tstar) {
            map->entries[i].target_prefix =
                cbm_strndup(target_pattern, (size_t)(tstar - target_pattern));
            map->entries[i].target_suffix = strdup(tstar + 1);
        } else {
            map->entries[i].target_prefix = strdup(target_pattern);
            map->entries[i].target_suffix = strdup("");
        }
    }
    va_end(args);

    /* Mimic the loader's specificity sort. */
    for (int i = 0; i < count - 1; i++) {
        for (int j = i + 1; j < count; j++) {
            size_t li = strlen(map->entries[i].alias_prefix);
            size_t lj = strlen(map->entries[j].alias_prefix);
            if (lj > li) {
                cbm_path_alias_t tmp = map->entries[i];
                map->entries[i] = map->entries[j];
                map->entries[j] = tmp;
            }
        }
    }
    return map;
}

/* The map produced by make_map() owns all heap memory the same way the
 * loader does, so the public free routine on the wrapping collection
 * works equivalently — but tests build naked maps, so use this helper. */
static void free_map(cbm_path_alias_map_t *map) {
    if (!map) {
        return;
    }
    for (int i = 0; i < map->count; i++) {
        free(map->entries[i].alias_prefix);
        free(map->entries[i].alias_suffix);
        free(map->entries[i].target_prefix);
        free(map->entries[i].target_suffix);
    }
    free(map->entries);
    free(map->base_url);
    free(map);
}

/* ── Basic wildcard alias ──────────────────────────────────────── */

TEST(path_alias_at_wildcard) {
    cbm_path_alias_map_t *m = make_map(NULL, 1, "@/*", "src/*");
    char *r = cbm_path_alias_resolve(m, "@/lib/auth");
    ASSERT_NOT_NULL(r);
    ASSERT_STR_EQ(r, "src/lib/auth");
    free(r);
    free_map(m);
    PASS();
}

TEST(path_alias_at_nested) {
    cbm_path_alias_map_t *m = make_map(NULL, 1, "@/*", "src/*");
    char *r = cbm_path_alias_resolve(m, "@/components/Button");
    ASSERT_NOT_NULL(r);
    ASSERT_STR_EQ(r, "src/components/Button");
    free(r);
    free_map(m);
    PASS();
}

/* ── Specificity ordering: longest matching prefix wins ────────── */

TEST(path_alias_specificity_longest_first) {
    // @/lib/* must beat @/* even though @/* would also match.
    cbm_path_alias_map_t *m =
        make_map(NULL, 2, "@/*", "src/*", "@/lib/*", "src/shared/lib/*");
    char *r = cbm_path_alias_resolve(m, "@/lib/auth");
    ASSERT_NOT_NULL(r);
    ASSERT_STR_EQ(r, "src/shared/lib/auth");
    free(r);
    /* Non-/lib paths still match the broader rule. */
    char *r2 = cbm_path_alias_resolve(m, "@/components/X");
    ASSERT_NOT_NULL(r2);
    ASSERT_STR_EQ(r2, "src/components/X");
    free(r2);
    free_map(m);
    PASS();
}

/* ── Exact match (no wildcard) ─────────────────────────────────── */

TEST(path_alias_exact_match) {
    cbm_path_alias_map_t *m = make_map(NULL, 1, "@app/config", "src/config/index");
    char *r = cbm_path_alias_resolve(m, "@app/config");
    ASSERT_NOT_NULL(r);
    ASSERT_STR_EQ(r, "src/config/index");
    free(r);
    /* Anything else under @app/ should not match. */
    char *miss = cbm_path_alias_resolve(m, "@app/other");
    ASSERT_NULL(miss);
    free_map(m);
    PASS();
}

/* ── Extension stripping ───────────────────────────────────────── */

TEST(path_alias_strips_ext) {
    cbm_path_alias_map_t *m = make_map(NULL, 1, "@/*", "src/*.ts");
    char *r = cbm_path_alias_resolve(m, "@/lib/auth");
    ASSERT_NOT_NULL(r);
    ASSERT_STR_EQ(r, "src/lib/auth");
    free(r);
    free_map(m);
    PASS();
}

/* ── baseUrl fallback for non-relative, non-package imports ────── */

TEST(path_alias_baseurl_fallback) {
    cbm_path_alias_map_t *m = make_map("src", 0);
    /* Looks like a sub-path → resolve against baseUrl. */
    char *r = cbm_path_alias_resolve(m, "lib/auth");
    ASSERT_NOT_NULL(r);
    ASSERT_STR_EQ(r, "src/lib/auth");
    free(r);
    /* Bare package name → not a baseUrl candidate. */
    char *miss = cbm_path_alias_resolve(m, "react");
    ASSERT_NULL(miss);
    free_map(m);
    PASS();
}

/* ── NULL safety ───────────────────────────────────────────────── */

TEST(path_alias_null_safety) {
    ASSERT_NULL(cbm_path_alias_resolve(NULL, "anything"));
    cbm_path_alias_map_t *m = make_map(NULL, 1, "@/*", "src/*");
    ASSERT_NULL(cbm_path_alias_resolve(m, NULL));
    free_map(m);
    /* Free of NULL collection is a no-op, not a crash. */
    cbm_path_alias_collection_free(NULL);
    PASS();
}

/* ── Collection lookup: nearest-ancestor scope wins ───────────── */

TEST(path_alias_find_for_file_nearest_ancestor) {
    /* Build a synthetic collection by hand: two scopes, "" (root) and
     * "apps/manager". A file inside apps/manager/src/... must pick the
     * deeper scope; a file under packages/utils must fall back to root. */
    cbm_path_alias_collection_t *coll = calloc(1, sizeof(*coll));
    coll->scopes = calloc(2, sizeof(cbm_path_alias_scope_t));
    coll->count = 2;

    /* Order matters: most specific first (loader does this via qsort). */
    coll->scopes[0].dir_prefix = strdup("apps/manager");
    coll->scopes[0].map = make_map(NULL, 1, "@/*", "src/*");

    coll->scopes[1].dir_prefix = strdup("");
    coll->scopes[1].map = make_map(NULL, 1, "@root/*", "shared/*");

    const cbm_path_alias_map_t *m1 =
        cbm_path_alias_find_for_file(coll, "apps/manager/src/lib/auth.ts");
    ASSERT_NOT_NULL(m1);
    ASSERT_EQ(m1->count, 1);
    ASSERT_STR_EQ(m1->entries[0].alias_prefix, "@/");

    const cbm_path_alias_map_t *m2 =
        cbm_path_alias_find_for_file(coll, "packages/utils/index.ts");
    ASSERT_NOT_NULL(m2);
    ASSERT_EQ(m2->count, 1);
    ASSERT_STR_EQ(m2->entries[0].alias_prefix, "@root/");

    cbm_path_alias_collection_free(coll);
    PASS();
}

/* ── End-to-end via the loader: real tsconfig in a tmp dir ─────── */

static int write_file(const char *path, const char *content) {
    FILE *f = fopen(path, "w");
    if (!f) {
        return -1;
    }
    size_t len = strlen(content);
    int rc = fwrite(content, 1, len, f) == len ? 0 : -1;
    fclose(f);
    return rc;
}

TEST(path_alias_loader_monorepo) {
    char tmpl[256];
    snprintf(tmpl, sizeof(tmpl), "/tmp/cbm_palias_XXXXXX");
    char *root = cbm_mkdtemp(tmpl);
    ASSERT_NOT_NULL(root);

    char sub[512];
    snprintf(sub, sizeof(sub), "%s/apps", root);
    cbm_mkdir(sub);
    snprintf(sub, sizeof(sub), "%s/apps/manager", root);
    cbm_mkdir(sub);

    char path[512];
    snprintf(path, sizeof(path), "%s/tsconfig.json", root);
    ASSERT_EQ(write_file(path,
                         "{\n  \"compilerOptions\": {\n    \"paths\": {\n"
                         "      \"@root/*\": [\"shared/*\"]\n    }\n  }\n}\n"),
              0);
    snprintf(path, sizeof(path), "%s/apps/manager/tsconfig.json", root);
    ASSERT_EQ(write_file(path,
                         "{\n  // monorepo subpackage\n  \"compilerOptions\": {\n"
                         "    \"paths\": {\n      \"@/*\": [\"./src/*\"]\n    }\n  },\n}\n"),
              0);

    cbm_path_alias_collection_t *coll = cbm_load_path_aliases(root);
    ASSERT_NOT_NULL(coll);
    ASSERT_EQ(coll->count, 2);

    /* sub-package file picks up its own tsconfig. */
    const cbm_path_alias_map_t *m =
        cbm_path_alias_find_for_file(coll, "apps/manager/src/feature/x.ts");
    ASSERT_NOT_NULL(m);
    char *r = cbm_path_alias_resolve(m, "@/lib/auth");
    ASSERT_NOT_NULL(r);
    /* Target paths in the sub-tsconfig are dir_prefix-relative. */
    ASSERT_STR_EQ(r, "apps/manager/src/lib/auth");
    free(r);

    /* Root file falls back to the root tsconfig's aliases. */
    const cbm_path_alias_map_t *m2 = cbm_path_alias_find_for_file(coll, "scripts/build.ts");
    ASSERT_NOT_NULL(m2);
    char *r2 = cbm_path_alias_resolve(m2, "@root/utils");
    ASSERT_NOT_NULL(r2);
    ASSERT_STR_EQ(r2, "shared/utils");
    free(r2);

    cbm_path_alias_collection_free(coll);

    /* Cleanup tmp tree. */
    snprintf(path, sizeof(path), "%s/apps/manager/tsconfig.json", root);
    unlink(path);
    snprintf(path, sizeof(path), "%s/tsconfig.json", root);
    unlink(path);
    snprintf(path, sizeof(path), "%s/apps/manager", root);
    rmdir(path);
    snprintf(path, sizeof(path), "%s/apps", root);
    rmdir(path);
    rmdir(root);
    PASS();
}

/* ── Monorepo alias climbing out of its tsconfig's directory (#730) ── */

TEST(path_alias_loader_monorepo_dotdot_climb) {
    char tmpl[256];
    snprintf(tmpl, sizeof(tmpl), "/tmp/cbm_palias_climb_XXXXXX");
    char *root = cbm_mkdtemp(tmpl);
    ASSERT_NOT_NULL(root);

    char sub[512];
    snprintf(sub, sizeof(sub), "%s/apps", root);
    cbm_mkdir(sub);
    snprintf(sub, sizeof(sub), "%s/apps/web", root);
    cbm_mkdir(sub);

    char path[512];
    snprintf(path, sizeof(path), "%s/apps/web/tsconfig.json", root);
    ASSERT_EQ(write_file(path,
                         "{\n  \"compilerOptions\": {\n    \"paths\": {\n"
                         "      \"@shared/*\": [\"../../packages/shared/src/*\"]\n"
                         "    }\n  }\n}\n"),
              0);

    cbm_path_alias_collection_t *coll = cbm_load_path_aliases(root);
    ASSERT_NOT_NULL(coll);

    const cbm_path_alias_map_t *m =
        cbm_path_alias_find_for_file(coll, "apps/web/src/feature/x.ts");
    ASSERT_NOT_NULL(m);
    char *r = cbm_path_alias_resolve(m, "@shared/utils");
    ASSERT_NOT_NULL(r);
    /* "../.." from apps/web climbs to repo root, then descends into
     * packages/shared/src — not the literal (unmatchable) "apps/web/../../..." */
    ASSERT_STR_EQ(r, "packages/shared/src/utils");
    free(r);

    cbm_path_alias_collection_free(coll);

    snprintf(path, sizeof(path), "%s/apps/web/tsconfig.json", root);
    unlink(path);
    snprintf(path, sizeof(path), "%s/apps/web", root);
    rmdir(path);
    snprintf(path, sizeof(path), "%s/apps", root);
    rmdir(path);
    rmdir(root);
    PASS();
}

/* ── Loader returns NULL when no configs found ─────────────────── */

TEST(path_alias_loader_no_configs) {
    char tmpl[256];
    snprintf(tmpl, sizeof(tmpl), "/tmp/cbm_palias_empty_XXXXXX");
    char *root = cbm_mkdtemp(tmpl);
    ASSERT_NOT_NULL(root);

    cbm_path_alias_collection_t *coll = cbm_load_path_aliases(root);
    ASSERT_NULL(coll);

    rmdir(root);
    PASS();
}

void suite_path_alias(void);
void suite_path_alias(void) {
    RUN_TEST(path_alias_at_wildcard);
    RUN_TEST(path_alias_at_nested);
    RUN_TEST(path_alias_specificity_longest_first);
    RUN_TEST(path_alias_exact_match);
    RUN_TEST(path_alias_strips_ext);
    RUN_TEST(path_alias_baseurl_fallback);
    RUN_TEST(path_alias_null_safety);
    RUN_TEST(path_alias_find_for_file_nearest_ancestor);
    RUN_TEST(path_alias_loader_monorepo);
    RUN_TEST(path_alias_loader_monorepo_dotdot_climb);
    RUN_TEST(path_alias_loader_no_configs);
}
