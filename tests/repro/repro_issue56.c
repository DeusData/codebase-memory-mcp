/*
 * repro_issue56.c — production-pipeline regression for Rust cross-crate calls.
 *
 * The fixture is a two-member Cargo workspace. crate_b calls
 * `crate_a::helper()` and also declares a same-named local helper, so generic
 * bare-name resolution cannot accidentally satisfy the assertion. The test
 * requires a persisted CALLS edge into crate_a's namespace.
 *
 * The direct parallel regression lives in test_parallel.c because the bug was
 * specific to worker-local pipeline context: the root Cargo manifest was
 * parsed by the sequential cross-LSP driver but was not propagated into
 * parallel resolve workers. A confident local result could also suppress the
 * manifest-aware cross pass. This repro remains as an end-to-end guard for the
 * same graph contract through the production harness.
 */

#include "test_framework.h"
#include "repro_harness.h"
#include <store/store.h>

#include <string.h>

/* ── Test ───────────────────────────────────────────────────────────────── */

/*
 * repro_issue56_cross_crate_calls
 *
 * Index a minimal two-crate Cargo workspace through the production
 * rh_index_files pipeline.  The fixture deliberately defines a LOCAL
 * `fn helper()` in crate_b so the name "helper" is no longer unique in
 * the project — the generic name resolver cannot pick crate_a's version
 * by bare-name scoring alone.  The assertion verifies that at least one
 * CALLS edge's TARGET node has a qualified_name containing "crate_a",
 * proving the cross-crate boundary was traversed.
 *
 * Regression condition: no CALLS edge whose target QN contains "crate_a"
 * exists in the store.
 */
TEST(repro_issue56_cross_crate_calls) {
    /*
     * Workspace root Cargo.toml — declares two members so the pipeline
     * (and any cargo-metadata-aware path) can discover the crate layout.
     */
    static const char workspace_toml[] =
        "[workspace]\n"
        "members = [\"crate_a\", \"crate_b\"]\n"
        "resolver = \"2\"\n";

    /*
     * crate_a: a library crate that exposes a single public function.
     * Path: crate_a/Cargo.toml
     */
    static const char crate_a_toml[] =
        "[package]\n"
        "name    = \"crate_a\"\n"
        "version = \"0.1.0\"\n"
        "edition = \"2021\"\n";

    /*
     * crate_a/src/lib.rs — the cross-crate callee lives here.
     * There are NO calls inside this file.
     */
    static const char crate_a_lib_rs[] =
        "/// A simple helper function exposed by crate_a.\n"
        "pub fn helper() {\n"
        "    // intentionally empty — we just need the definition\n"
        "}\n";

    /*
     * crate_b: a binary crate that depends on crate_a.
     * Path: crate_b/Cargo.toml
     */
    static const char crate_b_toml[] =
        "[package]\n"
        "name    = \"crate_b\"\n"
        "version = \"0.1.0\"\n"
        "edition = \"2021\"\n"
        "\n"
        "[dependencies]\n"
        "crate_a = { path = \"../crate_a\" }\n";

    /*
     * crate_b/src/main.rs — the caller.
     * `run()` calls `crate_a::helper()` across the crate boundary.
     *
     * IMPORTANT: a LOCAL `fn helper()` is also defined here.  This makes
     * the name "helper" ambiguous in the project registry (two candidates:
     * crate_a's and crate_b's), so the generic bare-name resolver cannot
     * route `crate_a::helper` to crate_a's node without crate-qualified
     * resolution.  Without this local helper the old ASSERT_GTE(calls, 2)
     * false-passed because bare-name scoring accidentally picked the only
     * "helper" in the project.
     */
    static const char crate_b_main_rs[] =
        "/// Local helper in crate_b — makes 'helper' name ambiguous.\n"
        "fn helper() {}\n"
        "\n"
        "fn run() {\n"
        "    crate_a::helper();\n"
        "}\n"
        "\n"
        "fn main() {\n"
        "    run();\n"
        "}\n";

    static const RFile files[] = {
        { "Cargo.toml",           workspace_toml  },
        { "crate_a/Cargo.toml",   crate_a_toml    },
        { "crate_a/src/lib.rs",   crate_a_lib_rs  },
        { "crate_b/Cargo.toml",   crate_b_toml    },
        { "crate_b/src/main.rs",  crate_b_main_rs },
    };
    static const int nfiles = (int)(sizeof(files) / sizeof(files[0]));

    RProj lp;
    cbm_store_t *store = rh_index_files(&lp, files, nfiles);
    ASSERT_NOT_NULL(store);

    /*
     * PRIMARY ASSERTION — must find a CALLS edge whose target node's
     * qualified_name contains "crate_a".
     *
     * The fixture has two "helper" definitions:
     *   (A) crate_a/src/lib.rs::helper  — QN contains "crate_a"
     *   (B) crate_b/src/main.rs::helper — QN contains "crate_b"
     *
     * Only a crate-qualified resolver (workspace metadata wired into the
     * pipeline, OR Rust lsp_cross enabled) can route `crate_a::helper` to
     * (A).  The generic bare-name resolver either picks (B) (local,
     * same-file-as-caller) or abstains when both are present.
     *
     * RED if no edge with target QN containing "crate_a" is found.
     * GREEN when cross-crate resolution is correctly implemented.
     */
    cbm_edge_t *edges = NULL;
    int edge_count = 0;
    int rc = cbm_store_find_edges_by_type(store, lp.project, "CALLS", &edges, &edge_count);
    ASSERT_EQ(rc, CBM_STORE_OK);

    int found_cross_crate = 0;
    for (int i = 0; i < edge_count && !found_cross_crate; i++) {
        cbm_node_t target_node;
        if (cbm_store_find_node_by_id(store, edges[i].target_id, &target_node) == CBM_STORE_OK) {
            if (target_node.qualified_name &&
                strstr(target_node.qualified_name, "crate_a")) {
                found_cross_crate = 1;
            }
        }
    }
    cbm_store_free_edges(edges, edge_count);

    /*
     * RED: no CALLS edge routes into crate_a's namespace.
     * The cross-crate boundary was not crossed.
     */
    ASSERT_TRUE(found_cross_crate);

    rh_cleanup(&lp, store);
    PASS();
}

/* ── Suite ──────────────────────────────────────────────────────────────── */
SUITE(repro_issue56) {
    RUN_TEST(repro_issue56_cross_crate_calls);
}
