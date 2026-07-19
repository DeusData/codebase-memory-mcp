/*
 * test_schema_declared_property_keys.c — Contract test for the declared
 * node/edge property-key registry: the tables in src/store/store.c
 * (schema_declared_node_property_keys / schema_declared_edge_property_keys)
 * exposed via the accessors declared in src/store/store.h
 * (cbm_store_schema_declared_node_property_keys /
 * cbm_store_schema_declared_edge_property_keys). See the maintenance
 * contract on those definitions for the full list of places that must stay
 * in sync when a pipeline pass or git-context writer emits a new key.
 *
 * Indexes committed mixed-language fixtures, reads the discovered schema
 * (base columns + JSON property keys) via cbm_store_get_schema, and asserts:
 *   1. Every discovered non-base key is present in the declared registry
 *      for its entity kind (forward direction — the registry is complete).
 *   2. The declared tables are sorted and duplicate-free (their sizeof-based
 *      accessors give no other way to detect drift).
 *   3. A representative sample of declared keys is actually observed in the
 *      fixtures (reverse pin — catches dead/rotted registry rows).
 */
#include "test_framework.h"
#include "test_helpers.h"
#include "foundation/constants.h"
#include "foundation/platform.h"
#include "git/git_command.h"
#include "pipeline/pipeline.h"
#include "store/store.h"
#include <stdlib.h>
#include <string.h>

static char g_dpk_tmpdir[CBM_SZ_256];

static int dpk_setup_repo(const char **filenames, const char **contents, int count) {
    const char *cache = cbm_resolve_cache_dir();
    int n = snprintf(g_dpk_tmpdir, sizeof(g_dpk_tmpdir), "%s/cbm-declared-keys-XXXXXX", cache);
    if (n < 0 || (size_t)n >= sizeof(g_dpk_tmpdir) || !cbm_mkdtemp(g_dpk_tmpdir)) {
        g_dpk_tmpdir[0] = '\0';
        return -1;
    }
    for (int i = 0; i < count; i++) {
        if (th_write_file(TH_PATH(g_dpk_tmpdir, filenames[i]), contents[i]) != 0) {
            th_rmtree(g_dpk_tmpdir);
            g_dpk_tmpdir[0] = '\0';
            return -1;
        }
    }
    return 0;
}

/* Best-effort: turn the fixture dir into a real git repo so pass_structure
 * (pipeline.c) creates a Branch node / HAS_BRANCH edge carrying every
 * cbm_git_context_props_json key (is_git, is_worktree, is_detached,
 * root_exists, canonical_root, worktree_root, git_common_dir, branch,
 * head_sha, base_sha) — otherwise those ~10 registry rows are never
 * forward-verified by this test. Non-fatal on failure (matches
 * test_git_context.c's SKIP_PLATFORM tolerance for "git not available");
 * the 3-fixture-only forward check still runs either way. Returns true iff
 * the repo was created, so callers can gate git-only reverse-pin keys. */
static bool dpk_add_git_context(void) {
    if (cbm_git_drain_command(g_dpk_tmpdir, "init -q") != 0 ||
        cbm_git_drain_command(g_dpk_tmpdir, "config user.email test@example.com") != 0 ||
        cbm_git_drain_command(g_dpk_tmpdir, "config user.name Test") != 0 ||
        cbm_git_drain_command(g_dpk_tmpdir, "add .") != 0 ||
        cbm_git_drain_command(g_dpk_tmpdir, "commit -q -m init") != 0) {
        return false;
    }
    return true;
}

static void dpk_teardown_repo(void) {
    if (g_dpk_tmpdir[0])
        th_rmtree(g_dpk_tmpdir);
    g_dpk_tmpdir[0] = '\0';
}

static bool dpk_key_in(const char *key, const char *const *set, int count) {
    for (int i = 0; i < count; i++) {
        if (strcmp(key, set[i]) == 0)
            return true;
    }
    return false;
}

/* DPK_SAMPLE_MIN mirrors the plan's reverse-pin bar: enough hits to prove
 * the sample set is actually observed, not a coincidence. The last two
 * slots are git_context keys, only required when dpk_add_git_context()
 * succeeded (see its use below). */
enum { DPK_SAMPLE_MIN = 6, DPK_SAMPLE_MAX = 8 };

TEST(declared_node_property_keys_sorted_and_deduped) {
    int count = 0;
    const char *const *keys = cbm_store_schema_declared_node_property_keys(&count);
    ASSERT_NOT_NULL(keys);
    ASSERT_TRUE(count > 0);
    for (int i = 1; i < count; i++) {
        if (strcmp(keys[i - 1], keys[i]) >= 0)
            printf("registry order violation: \"%s\" before \"%s\" (insert in ASCII order)\n",
                  keys[i - 1], keys[i]);
        ASSERT_TRUE(strcmp(keys[i - 1], keys[i]) < 0);
    }
    PASS();
}

TEST(declared_edge_property_keys_sorted_and_deduped) {
    int count = 0;
    const char *const *keys = cbm_store_schema_declared_edge_property_keys(&count);
    ASSERT_NOT_NULL(keys);
    ASSERT_TRUE(count > 0);
    for (int i = 1; i < count; i++) {
        if (strcmp(keys[i - 1], keys[i]) >= 0)
            printf("registry order violation: \"%s\" before \"%s\" (insert in ASCII order)\n",
                  keys[i - 1], keys[i]);
        ASSERT_TRUE(strcmp(keys[i - 1], keys[i]) < 0);
    }
    PASS();
}

/* Index mixed-language fixtures (Python Flask route + class, TypeScript
 * async route handler, Rust function) and assert every discovered non-base
 * node/edge property key belongs to the declared registry for its kind. */
TEST(declared_property_keys_cover_discovered_mixed_language_keys) {
    const char *files[] = {"app.py", "handlers.ts", "lib.rs"};
    const char *contents[] = {
        "from flask import Flask\n\napp = Flask(__name__)\n\n\n"
        "class DataProcessor(BaseProcessor):\n"
        "    \"\"\"Transforms request payloads.\"\"\"\n\n"
        "    @staticmethod\n"
        "    def transform(data):\n"
        "        return data\n\n\n"
        "@app.route(\"/items\")\n"
        "def list_items():\n"
        "    \"\"\"Return all items.\"\"\"\n"
        "    items = []\n"
        "    for i in range(3):\n"
        "        items.append(i)\n"
        "    return {\"items\": items}\n",
        "export async function handleRequest(req, res) {\n"
        "  const data = await fetch('/api/items');\n"
        "  return data;\n"
        "}\n",
        "pub fn add(a: i32, b: i32) -> i32 {\n"
        "    a + b\n"
        "}\n"};

    if (dpk_setup_repo(files, contents, 3) != 0)
        FAIL("tmpdir");
    /* Best-effort: also forward-verify the ~10 git_context registry rows
     * (see dpk_add_git_context). Never hard-fails the test — mirrors
     * test_git_context.c's tolerance for "git not available". */
    bool have_git = dpk_add_git_context();

    char db[CBM_SZ_512];
    snprintf(db, sizeof(db), "%s/test.db", g_dpk_tmpdir);

    cbm_pipeline_t *p = cbm_pipeline_new(g_dpk_tmpdir, db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    const char *proj = cbm_pipeline_project_name(p);
    cbm_store_t *s = cbm_store_open_path(db);
    ASSERT_NOT_NULL(s);

    cbm_schema_info_t schema = {0};
    ASSERT_EQ(cbm_store_get_schema(s, proj, &schema), CBM_STORE_OK);

    int base_node_count = 0;
    const char *const *base_node_cols = cbm_store_schema_node_base_properties(&base_node_count);
    int declared_node_count = 0;
    const char *const *declared_node_keys =
        cbm_store_schema_declared_node_property_keys(&declared_node_count);

    int base_edge_count = 0;
    const char *const *base_edge_cols = cbm_store_schema_edge_base_properties(&base_edge_count);
    int declared_edge_count = 0;
    const char *const *declared_edge_keys =
        cbm_store_schema_declared_edge_property_keys(&declared_edge_count);

    bool sample_seen[DPK_SAMPLE_MAX] = {0};
    const char *sample_keys[DPK_SAMPLE_MAX] = {
        "complexity", "cognitive",  "is_test", "is_exported",
        "docstring",  "base_classes", "branch", "is_git"};
    int sample_count = have_git ? DPK_SAMPLE_MAX : DPK_SAMPLE_MIN;

    bool undeclared_found = false;
    for (int i = 0; i < schema.node_label_count; i++) {
        const cbm_label_count_t *lc = &schema.node_labels[i];
        /* If discovery hit the cap, this test can no longer prove registry
         * completeness for this label — fail loudly instead of passing blind. */
        if (lc->property_count >= CBM_STORE_SCHEMA_PROPERTY_KEY_LIMIT) {
            printf("label %s hit the %d-key discovery cap; completeness unprovable\n", lc->label,
                  CBM_STORE_SCHEMA_PROPERTY_KEY_LIMIT);
            undeclared_found = true;
        }
        for (int j = 0; j < lc->property_count; j++) {
            const char *key = lc->properties[j];
            if (dpk_key_in(key, base_node_cols, base_node_count))
                continue;
            if (!dpk_key_in(key, declared_node_keys, declared_node_count)) {
                printf("undeclared node property key: %s (label %s)\n", key, lc->label);
                undeclared_found = true;
            }
            for (int k = 0; k < sample_count; k++) {
                if (!sample_seen[k] && strcmp(key, sample_keys[k]) == 0)
                    sample_seen[k] = true;
            }
        }
    }

    for (int i = 0; i < schema.edge_type_count; i++) {
        const cbm_type_count_t *tc = &schema.edge_types[i];
        if (tc->property_count >= CBM_STORE_SCHEMA_PROPERTY_KEY_LIMIT) {
            printf("edge type %s hit the %d-key discovery cap; completeness unprovable\n",
                  tc->type, CBM_STORE_SCHEMA_PROPERTY_KEY_LIMIT);
            undeclared_found = true;
        }
        for (int j = 0; j < tc->property_count; j++) {
            const char *key = tc->properties[j];
            if (dpk_key_in(key, base_edge_cols, base_edge_count))
                continue;
            if (!dpk_key_in(key, declared_edge_keys, declared_edge_count)) {
                printf("undeclared edge property key: %s (type %s)\n", key, tc->type);
                undeclared_found = true;
            }
        }
    }

    bool all_samples_seen = true;
    for (int k = 0; k < sample_count; k++) {
        if (!sample_seen[k]) {
            printf("reverse-pin sample key never observed: %s\n", sample_keys[k]);
            all_samples_seen = false;
        }
    }

    cbm_store_schema_free(&schema);
    cbm_store_close(s);
    cbm_pipeline_free(p);
    dpk_teardown_repo();

    ASSERT_TRUE(!undeclared_found);
    ASSERT_TRUE(all_samples_seen);
    PASS();
}

SUITE(schema_declared_property_keys) {
    RUN_TEST(declared_node_property_keys_sorted_and_deduped);
    RUN_TEST(declared_edge_property_keys_sorted_and_deduped);
    RUN_TEST(declared_property_keys_cover_discovered_mixed_language_keys);
}
