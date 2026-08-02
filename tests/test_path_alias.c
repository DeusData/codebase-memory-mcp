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
#include "../src/foundation/compat_fs.h"

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

enum {
    PATH_ALIAS_PARENT_DEPTH_CAP = 32,
    PATH_ALIAS_DEEP_FIXTURE_DEPTH = PATH_ALIAS_PARENT_DEPTH_CAP + 4,
    PATH_ALIAS_LONG_FIXTURE_SEGMENTS = 5,
    PATH_ALIAS_LONG_FIXTURE_SEGMENT_BYTES = 100,
    PATH_ALIAS_PARENT_ENTRY_CAP = 256,
    PATH_ALIAS_COMPLETE_ENTRY_COUNT = PATH_ALIAS_PARENT_ENTRY_CAP + 1,
    PATH_ALIAS_PARENT_CONFIG_CAP = 256,
    PATH_ALIAS_COMPLETE_CONFIG_COUNT = PATH_ALIAS_PARENT_CONFIG_CAP + 1,
    PATH_ALIAS_CHILD_CONFIG_COUNT = PATH_ALIAS_COMPLETE_CONFIG_COUNT - 1,
    PATH_ALIAS_SCOPE_NAME_BYTES = 32,
    PATH_ALIAS_PARENT_FILE_CAP_BYTES = 64 * 1024,
    PATH_ALIAS_LARGE_CONFIG_PADDING_BYTES = PATH_ALIAS_PARENT_FILE_CAP_BYTES + 1024,
    PATH_ALIAS_ENTRY_JSON_BYTES = 80,
    PATH_ALIAS_FIXTURE_DIR_MODE = 0700,
};

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
    cbm_path_alias_map_t *m = make_map(NULL, 2, "@/*", "src/*", "@/lib/*", "src/shared/lib/*");
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

    const cbm_path_alias_map_t *m2 = cbm_path_alias_find_for_file(coll, "packages/utils/index.ts");
    ASSERT_NOT_NULL(m2);
    ASSERT_EQ(m2->count, 1);
    ASSERT_STR_EQ(m2->entries[0].alias_prefix, "@root/");

    cbm_path_alias_collection_free(coll);
    PASS();
}

/* ── End-to-end via the loader: real tsconfig in a tmp dir ─────── */

static int write_file(const char *path, const char *content) {
    FILE *f = cbm_fopen(path, "w");
    if (!f) {
        return -1;
    }
    size_t len = strlen(content);
    int rc = fwrite(content, 1, len, f) == len ? 0 : -1;
    fclose(f);
    return rc;
}

typedef struct {
    char *root;
    char **dirs;
    size_t dir_count;
    char *rel_dir;
    char *config_path;
} path_alias_tree_fixture_t;

static char *path_alias_join(const char *left, const char *right) {
    size_t left_len = strlen(left);
    size_t right_len = strlen(right);
    size_t separator = left_len > 0 ? 1U : 0U;
    if (left_len > SIZE_MAX - right_len || left_len + right_len > SIZE_MAX - separator - 1U) {
        return NULL;
    }
    size_t total = left_len + separator + right_len;
    char *result = malloc(total + 1U);
    if (!result) {
        return NULL;
    }
    memcpy(result, left, left_len);
    if (separator > 0) {
        result[left_len] = '/';
    }
    memcpy(result + left_len + separator, right, right_len + 1U);
    return result;
}

static void path_alias_tree_fixture_free(path_alias_tree_fixture_t *fixture) {
    if (!fixture) {
        return;
    }
    if (fixture->config_path) {
        unlink(fixture->config_path);
    }
    for (size_t i = fixture->dir_count; i > 0; i--) {
        rmdir(fixture->dirs[i - 1U]);
        free(fixture->dirs[i - 1U]);
    }
    if (fixture->root) {
        rmdir(fixture->root);
    }
    free(fixture->config_path);
    free(fixture->rel_dir);
    free(fixture->dirs);
    free(fixture->root);
    memset(fixture, 0, sizeof(*fixture));
}

static bool path_alias_tree_fixture_create(path_alias_tree_fixture_t *fixture, size_t segment_count,
                                           size_t segment_bytes, const char *config) {
    memset(fixture, 0, sizeof(*fixture));
    /* Windows cbm_mkdtemp expands /tmp through %TEMP%; retain the centralized
     * capacity contract so a long or Unicode temporary root cannot overwrite
     * this stack buffer. Runtime and auxiliary memory remain O(1). */
    char tmpl[CBM_SZ_256] = "/tmp/cbm_palias_exact_XXXXXX";
    char *root = cbm_mkdtemp(tmpl);
    if (!root) {
        return false;
    }
    fixture->root = strdup(root);
    if (!fixture->root) {
        rmdir(root);
        return false;
    }
    fixture->dirs = calloc(segment_count, sizeof(*fixture->dirs));
    char *current_abs = strdup(fixture->root);
    char *current_rel = strdup("");
    char *segment = segment_bytes < SIZE_MAX ? malloc(segment_bytes + 1U) : NULL;
    if ((segment_count > 0 && !fixture->dirs) || !current_abs || !current_rel || !segment) {
        free(segment);
        free(current_rel);
        free(current_abs);
        path_alias_tree_fixture_free(fixture);
        return false;
    }
    memset(segment, 'd', segment_bytes);
    segment[segment_bytes] = '\0';

    for (size_t i = 0; i < segment_count; i++) {
        char *next_abs = path_alias_join(current_abs, segment);
        char *next_rel = path_alias_join(current_rel, segment);
        /* Reuse the production UTF-8/extended-length directory owner. Each
         * call adds one component, so fixture creation remains O(total path
         * bytes) live memory and O(segment_count * final path bytes) time. */
        if (!next_abs || !next_rel ||
            !cbm_mkdir_p(next_abs, PATH_ALIAS_FIXTURE_DIR_MODE)) {
            free(next_rel);
            free(next_abs);
            free(segment);
            free(current_rel);
            free(current_abs);
            path_alias_tree_fixture_free(fixture);
            return false;
        }
        fixture->dirs[fixture->dir_count++] = next_abs;
        free(current_abs);
        free(current_rel);
        current_abs = strdup(next_abs);
        current_rel = next_rel;
        if (!current_abs) {
            free(segment);
            free(current_rel);
            path_alias_tree_fixture_free(fixture);
            return false;
        }
    }
    free(segment);
    fixture->rel_dir = current_rel;
    fixture->config_path = path_alias_join(current_abs, "tsconfig.json");
    free(current_abs);
    if (!fixture->config_path || write_file(fixture->config_path, config) != 0) {
        path_alias_tree_fixture_free(fixture);
        return false;
    }
    return true;
}

static bool path_alias_fixture_resolves(path_alias_tree_fixture_t *fixture, const char *module,
                                        const char *expected) {
    cbm_path_alias_collection_t *coll = cbm_load_path_aliases(fixture->root);
    char *source = path_alias_join(fixture->rel_dir, "consumer.ts");
    const cbm_path_alias_map_t *map =
        coll && source ? cbm_path_alias_find_for_file(coll, source) : NULL;
    char *resolved = map ? cbm_path_alias_resolve(map, module) : NULL;
    bool exact = resolved && strcmp(resolved, expected) == 0;
    free(resolved);
    free(source);
    cbm_path_alias_collection_free(coll);
    return exact;
}

static char *path_alias_many_entries_config(void) {
    const char prefix[] = "{\"compilerOptions\":{\"paths\":{";
    const char suffix[] = "}}}";
    if ((size_t)PATH_ALIAS_COMPLETE_ENTRY_COUNT >
        (SIZE_MAX - sizeof(prefix) - sizeof(suffix)) / PATH_ALIAS_ENTRY_JSON_BYTES) {
        return NULL;
    }
    size_t capacity = sizeof(prefix) + sizeof(suffix) +
                      (size_t)PATH_ALIAS_COMPLETE_ENTRY_COUNT * PATH_ALIAS_ENTRY_JSON_BYTES;
    char *config = malloc(capacity);
    if (!config) {
        return NULL;
    }
    int prefix_written = snprintf(config, capacity, "%s", prefix);
    if (prefix_written < 0 || (size_t)prefix_written >= capacity) {
        free(config);
        return NULL;
    }
    size_t used = (size_t)prefix_written;
    for (int i = 0; i < PATH_ALIAS_COMPLETE_ENTRY_COUNT; i++) {
        int written =
            snprintf(config + used, capacity - used, "\"@alias%d/*\":[\"src/alias%d/*\"]%s", i, i,
                     i + 1 < PATH_ALIAS_COMPLETE_ENTRY_COUNT ? "," : "");
        if (written < 0 || (size_t)written >= capacity - used) {
            free(config);
            return NULL;
        }
        used += (size_t)written;
    }
    if (snprintf(config + used, capacity - used, "%s", suffix) < 0) {
        free(config);
        return NULL;
    }
    return config;
}

static char *path_alias_large_config(void) {
    const char prefix[] = "{\"compilerOptions\":{";
    const char suffix[] = "\"paths\":{\"@/*\":[\"src/*\"]}}}";
    size_t padding = PATH_ALIAS_LARGE_CONFIG_PADDING_BYTES;
    if (padding > SIZE_MAX - sizeof(prefix) - sizeof(suffix)) {
        return NULL;
    }
    size_t total = (sizeof(prefix) - 1U) + padding + sizeof(suffix);
    char *config = malloc(total);
    if (!config) {
        return NULL;
    }
    memcpy(config, prefix, sizeof(prefix) - 1U);
    memset(config + sizeof(prefix) - 1U, ' ', padding);
    memcpy(config + sizeof(prefix) - 1U + padding, suffix, sizeof(suffix));
    return config;
}

TEST(path_alias_paths_targets_respect_baseurl) {
    const char config[] =
        "{\"compilerOptions\":{\"baseUrl\":\"src\",\"paths\":{\"@/*\":[\"components/*\"]}}}";
    path_alias_tree_fixture_t fixture;
    ASSERT_TRUE(path_alias_tree_fixture_create(&fixture, 0, 0, config));
    bool exact = path_alias_fixture_resolves(&fixture, "@/Button", "src/components/Button");
    path_alias_tree_fixture_free(&fixture);
    ASSERT_TRUE(exact);
    PASS();
}

TEST(path_alias_loader_reaches_beyond_parent_depth_cap) {
    const char config[] = "{\"compilerOptions\":{\"paths\":{\"@/*\":[\"src/*\"]}}}";
    path_alias_tree_fixture_t fixture;
    ASSERT_TRUE(path_alias_tree_fixture_create(&fixture, PATH_ALIAS_DEEP_FIXTURE_DEPTH, 1, config));
    char *expected = path_alias_join(fixture.rel_dir, "src/value");
    bool exact = expected && path_alias_fixture_resolves(&fixture, "@/value", expected);
    free(expected);
    path_alias_tree_fixture_free(&fixture);
    ASSERT_TRUE(exact);
    PASS();
}

TEST(path_alias_loader_preserves_paths_beyond_parent_buffers) {
    const char config[] = "{\"compilerOptions\":{\"paths\":{\"@/*\":[\"src/*\"]}}}";
    path_alias_tree_fixture_t fixture;
    ASSERT_TRUE(path_alias_tree_fixture_create(&fixture, PATH_ALIAS_LONG_FIXTURE_SEGMENTS,
                                               PATH_ALIAS_LONG_FIXTURE_SEGMENT_BYTES, config));
    char *expected = path_alias_join(fixture.rel_dir, "src/value");
    bool exact = expected && path_alias_fixture_resolves(&fixture, "@/value", expected);
    free(expected);
    path_alias_tree_fixture_free(&fixture);
    ASSERT_TRUE(exact);
    PASS();
}

TEST(path_alias_loader_uses_shared_file_size_policy) {
    char *config = path_alias_large_config();
    ASSERT_NOT_NULL(config);
    path_alias_tree_fixture_t fixture;
    bool created = path_alias_tree_fixture_create(&fixture, 0, 0, config);
    free(config);
    ASSERT_TRUE(created);
    bool exact = path_alias_fixture_resolves(&fixture, "@/value", "src/value");
    path_alias_tree_fixture_free(&fixture);
    ASSERT_TRUE(exact);
    PASS();
}

TEST(path_alias_loader_retains_every_configured_entry) {
    char *config = path_alias_many_entries_config();
    ASSERT_NOT_NULL(config);
    path_alias_tree_fixture_t fixture;
    bool created = path_alias_tree_fixture_create(&fixture, 0, 0, config);
    free(config);
    ASSERT_TRUE(created);
    bool exact = path_alias_fixture_resolves(&fixture, "@alias256/value", "src/alias256/value");
    path_alias_tree_fixture_free(&fixture);
    ASSERT_TRUE(exact);
    PASS();
}

TEST(path_alias_loader_retains_every_config_file) {
    const char config[] = "{\"compilerOptions\":{\"paths\":{\"@/*\":[\"src/*\"]}}}";
    path_alias_tree_fixture_t fixture;
    ASSERT_TRUE(path_alias_tree_fixture_create(&fixture, 0, 0, config));
    char **dirs = calloc(PATH_ALIAS_CHILD_CONFIG_COUNT, sizeof(*dirs));
    char **configs = calloc(PATH_ALIAS_CHILD_CONFIG_COUNT, sizeof(*configs));
    size_t created = 0;
    bool complete = dirs && configs;
    while (complete && created < PATH_ALIAS_CHILD_CONFIG_COUNT) {
        char name[PATH_ALIAS_SCOPE_NAME_BYTES];
        int written = snprintf(name, sizeof(name), "scope%03zu", created);
        dirs[created] = written > 0 && (size_t)written < sizeof(name)
                            ? path_alias_join(fixture.root, name)
                            : NULL;
        configs[created] = dirs[created] ? path_alias_join(dirs[created], "tsconfig.json") : NULL;
        complete = dirs[created] && configs[created] && cbm_mkdir(dirs[created]) == 0 &&
                   write_file(configs[created], config) == 0;
        if (complete) {
            created++;
        }
    }

    cbm_path_alias_collection_t *coll = complete ? cbm_load_path_aliases(fixture.root) : NULL;
    bool exact = coll && coll->count == PATH_ALIAS_COMPLETE_CONFIG_COUNT;
    cbm_path_alias_collection_free(coll);
    for (size_t i = created; i > 0; i--) {
        unlink(configs[i - 1U]);
        rmdir(dirs[i - 1U]);
        free(configs[i - 1U]);
        free(dirs[i - 1U]);
    }
    if (!complete && created < PATH_ALIAS_CHILD_CONFIG_COUNT) {
        if (configs) {
            unlink(configs[created]);
            free(configs[created]);
        }
        if (dirs) {
            rmdir(dirs[created]);
            free(dirs[created]);
        }
    }
    free(configs);
    free(dirs);
    path_alias_tree_fixture_free(&fixture);
    ASSERT_TRUE(complete);
    ASSERT_TRUE(exact);
    PASS();
}

TEST(path_alias_loader_config_hit_allocation_failure_is_atomic) {
    const char config[] = "{\"compilerOptions\":{\"paths\":{\"@/*\":[\"src/*\"]}}}";
    path_alias_tree_fixture_t fixture;
    ASSERT_TRUE(path_alias_tree_fixture_create(&fixture, 0, 0, config));
    char *child = path_alias_join(fixture.root, "child");
    char *child_config = child ? path_alias_join(child, "tsconfig.json") : NULL;
    bool complete =
        child && child_config && cbm_mkdir(child) == 0 && write_file(child_config, config) == 0;
    ASSERT_TRUE(complete);

    /* The root config is stored first. Failure while retaining the child must
     * discard that root hit instead of publishing a plausible partial view. */
    cbm_path_alias_test_fail_allocation(CBM_PATH_ALIAS_TEST_ALLOC_CONFIG_HIT, 1);
    cbm_path_alias_collection_t *coll = cbm_load_path_aliases(fixture.root);
    cbm_path_alias_test_fail_allocation(CBM_PATH_ALIAS_TEST_ALLOC_NONE, -1);
    bool failure_is_atomic = coll == NULL;
    cbm_path_alias_collection_free(coll);

    unlink(child_config);
    rmdir(child);
    free(child_config);
    free(child);
    path_alias_tree_fixture_free(&fixture);
    ASSERT_TRUE(failure_is_atomic);
    PASS();
}

TEST(path_alias_loader_scope_allocation_failure_is_atomic) {
    const char config[] = "{\"compilerOptions\":{\"paths\":{\"@/*\":[\"src/*\"]}}}";
    path_alias_tree_fixture_t fixture;
    ASSERT_TRUE(path_alias_tree_fixture_create(&fixture, 0, 0, config));

    cbm_path_alias_test_fail_allocation(CBM_PATH_ALIAS_TEST_ALLOC_SCOPE_PREFIX, 0);
    cbm_path_alias_collection_t *coll = cbm_load_path_aliases(fixture.root);
    cbm_path_alias_test_fail_allocation(CBM_PATH_ALIAS_TEST_ALLOC_NONE, -1);
    bool failure_is_atomic = coll == NULL;
    cbm_path_alias_collection_free(coll);

    path_alias_tree_fixture_free(&fixture);
    ASSERT_TRUE(failure_is_atomic);
    PASS();
}

TEST(path_alias_loader_rejects_posix_symlink_cycle) {
#ifdef _WIN32
    PASS();
#else
    const char config[] = "{\"compilerOptions\":{\"paths\":{\"@/*\":[\"src/*\"]}}}";
    path_alias_tree_fixture_t fixture;
    ASSERT_TRUE(path_alias_tree_fixture_create(&fixture, 0, 0, config));
    char *loop = path_alias_join(fixture.root, "loop");
    bool linked = loop && symlink(fixture.root, loop) == 0;
    cbm_path_alias_collection_t *coll = linked ? cbm_load_path_aliases(fixture.root) : NULL;
    bool exact = coll && coll->count == 1;
    cbm_path_alias_collection_free(coll);
    if (loop) {
        unlink(loop);
    }
    free(loop);
    path_alias_tree_fixture_free(&fixture);
    ASSERT_TRUE(linked);
    ASSERT_TRUE(exact);
    PASS();
#endif
}

TEST(path_alias_loader_monorepo) {
    char tmpl[CBM_SZ_256];
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
    ASSERT_EQ(write_file(path, "{\n  \"compilerOptions\": {\n    \"paths\": {\n"
                               "      \"@root/*\": [\"shared/*\"]\n    }\n  }\n}\n"),
              0);
    snprintf(path, sizeof(path), "%s/apps/manager/tsconfig.json", root);
    ASSERT_EQ(write_file(path, "{\n  // monorepo subpackage\n  \"compilerOptions\": {\n"
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
    char tmpl[CBM_SZ_256];
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
    ASSERT_EQ(write_file(path, "{\n  \"compilerOptions\": {\n    \"paths\": {\n"
                               "      \"@shared/*\": [\"../../packages/shared/src/*\"]\n"
                               "    }\n  }\n}\n"),
              0);

    cbm_path_alias_collection_t *coll = cbm_load_path_aliases(root);
    ASSERT_NOT_NULL(coll);

    const cbm_path_alias_map_t *m = cbm_path_alias_find_for_file(coll, "apps/web/src/feature/x.ts");
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

/* ── Loader honors discovery exclusions (#792) ─────────────────── */

/* find_alias_files must not descend into discovery-excluded subtrees.
 * Control run first (no exclusions → both configs collected) so the
 * exclusion assertion below cannot pass vacuously. */
TEST(path_alias_loader_honors_discovery_exclusions) {
    char tmpl[CBM_SZ_256];
    snprintf(tmpl, sizeof(tmpl), "/tmp/cbm_palias_excl_XXXXXX");
    char *root = cbm_mkdtemp(tmpl);
    ASSERT_NOT_NULL(root);

    char sub[512];
    snprintf(sub, sizeof(sub), "%s/big_generated", root);
    cbm_mkdir(sub);

    char path[512];
    snprintf(path, sizeof(path), "%s/tsconfig.json", root);
    ASSERT_EQ(write_file(path, "{\n  \"compilerOptions\": {\n    \"paths\": {\n"
                               "      \"@root/*\": [\"shared/*\"]\n    }\n  }\n}\n"),
              0);
    snprintf(path, sizeof(path), "%s/big_generated/tsconfig.json", root);
    ASSERT_EQ(write_file(path, "{\n  \"compilerOptions\": {\n    \"paths\": {\n"
                               "      \"@gen/*\": [\"./src/*\"]\n    }\n  }\n}\n"),
              0);

    /* Control: the unexcluded loader collects BOTH configs. */
    cbm_path_alias_collection_t *coll = cbm_load_path_aliases(root);
    ASSERT_NOT_NULL(coll);
    ASSERT_EQ(coll->count, 2);
    cbm_path_alias_collection_free(coll);

    /* Excluding big_generated drops its config; the root one survives. */
    char *excluded[] = {(char *)"big_generated"};
    coll = cbm_load_path_aliases_excluded(root, excluded, 1);
    ASSERT_NOT_NULL(coll);
    ASSERT_EQ(coll->count, 1);
    const cbm_path_alias_map_t *m = cbm_path_alias_find_for_file(coll, "src/x.ts");
    ASSERT_NOT_NULL(m);
    char *r = cbm_path_alias_resolve(m, "@root/utils");
    ASSERT_NOT_NULL(r);
    ASSERT_STR_EQ(r, "shared/utils");
    free(r);
    cbm_path_alias_collection_free(coll);

    snprintf(path, sizeof(path), "%s/big_generated/tsconfig.json", root);
    unlink(path);
    snprintf(path, sizeof(path), "%s/tsconfig.json", root);
    unlink(path);
    snprintf(path, sizeof(path), "%s/big_generated", root);
    rmdir(path);
    rmdir(root);
    PASS();
}

/* ── Loader returns NULL when no configs found ─────────────────── */

TEST(path_alias_loader_no_configs) {
    char tmpl[CBM_SZ_256];
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
    RUN_TEST(path_alias_paths_targets_respect_baseurl);
    RUN_TEST(path_alias_loader_reaches_beyond_parent_depth_cap);
    RUN_TEST(path_alias_loader_preserves_paths_beyond_parent_buffers);
    RUN_TEST(path_alias_loader_uses_shared_file_size_policy);
    RUN_TEST(path_alias_loader_retains_every_configured_entry);
    RUN_TEST(path_alias_loader_retains_every_config_file);
    RUN_TEST(path_alias_loader_config_hit_allocation_failure_is_atomic);
    RUN_TEST(path_alias_loader_scope_allocation_failure_is_atomic);
    RUN_TEST(path_alias_loader_rejects_posix_symlink_cycle);
    RUN_TEST(path_alias_loader_monorepo);
    RUN_TEST(path_alias_loader_monorepo_dotdot_climb);
    RUN_TEST(path_alias_loader_honors_discovery_exclusions);
    RUN_TEST(path_alias_loader_no_configs);
}
