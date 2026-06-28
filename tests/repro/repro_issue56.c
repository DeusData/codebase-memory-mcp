/*
 * repro_issue56.c — Reproduce-first case for OPEN bug #56.
 *
 * Bug #56: "Cross-crate call graphs stop at boundaries" (Rust)
 *
 * ROOT CAUSE (pipeline / Rust LSP path):
 *   The tree-sitter-only Rust extractor has no access to Cargo metadata
 *   at extraction time, so when it sees `crate_a::helper()` inside
 *   crate_b, it records a raw call-site for the path but has no registry
 *   entry for `crate_a::helper` — only the definitions in the *same file*
 *   were seeded.  The LSP resolver therefore cannot match the call-site to
 *   a callee QN across the crate boundary, and the resulting
 *   CBMResolvedCall is either absent or marked with low confidence and
 *   discarded.  When the pipeline writes graph edges for this project, no
 *   CALLS edge is minted for the cross-crate call — the call graph stops
 *   at the crate edge.
 *
 *   v0.8.1 added a hybrid-LSP Rust path that "materially improves" this
 *   (issue comment, maintainer 2026-06-25), but the reporter was asked to
 *   retest; the issue remains OPEN because no retest confirming resolution
 *   was provided.  The workspace-member wiring test
 *   (rustlsp_extra_cargo_wires_workspace_member in test_rust_lsp.c) only
 *   exercises the *single-file LSP* layer with a manually-parsed manifest;
 *   it does NOT verify that the full production pipeline (rh_index_files →
 *   cbm_pipeline → graph store) persists a cross-crate CALLS edge for a
 *   real multi-file Cargo workspace fixture.  That gap is what this test
 *   fills.
 *
 * FIXTURE:
 *   A minimal Cargo workspace with two crates:
 *
 *   [workspace Cargo.toml]           — workspace root, declares members
 *   crate_a/Cargo.toml               — library crate "crate_a"
 *   crate_a/src/lib.rs               — exposes `pub fn helper() {}`
 *   crate_b/Cargo.toml               — binary crate "crate_b", depends on crate_a
 *   crate_b/src/main.rs              — calls `crate_a::helper()` from `fn run()`;
 *                                       also defines a LOCAL `fn helper()` to break
 *                                       bare-name uniqueness (see note below)
 *
 *   The only meaningful cross-crate CALLS edge is:
 *     crate_b::run  →  crate_a::helper
 *
 * EXPECTED (correct) behaviour:
 *   After indexing the workspace through the production MCP pipeline, the
 *   graph store must contain at least one CALLS edge whose TARGET node's
 *   qualified_name contains "crate_a" (i.e. routes into the crate_a
 *   namespace, not into crate_b's local helper).
 *
 * ACTUAL (buggy) behaviour:
 *   The pipeline extracts both files, but the cross-crate path
 *   `crate_a::helper` in crate_b/src/main.rs is not resolved to a graph
 *   node in crate_a because Cargo workspace member metadata is not
 *   plumbed into the per-file extraction phase.  Result: zero CALLS edges
 *   to the crate_a namespace.
 *
 * WHY THIS IS RED ON CURRENT CODE (even post-v0.8.1):
 *   The rustlsp_extra_cargo_wires_workspace_member unit test exercises only
 *   the LSP layer (cbm_run_rust_lsp_with_manifest called with a parsed
 *   CBMCargoManifest) and confirms the resolver *can* route
 *   `engine::boot()` to `engine.boot` when given the manifest explicitly.
 *   BUT: the production pipeline's per-file extraction path
 *   (cbm_extract_file → cbm_run_rust_lsp) does NOT receive a pre-parsed
 *   workspace manifest — it only gets the individual file's content.
 *   Additionally, cbm_pxc_has_cross_lsp() returns false for CBM_LANG_RUST
 *   (pass_lsp_cross.c), so the cross-file LSP pass is never invoked for
 *   Rust.  Therefore a real workspace indexed through index_repository
 *   produces no CALLS edges crossing into crate_a, and this test is RED.
 *
 * WHY THE OLD >= 2 COUNT TEST FALSE-PASSED:
 *   With a unique `helper` name in the project (one definition in crate_a,
 *   no other `helper` anywhere), the generic pipeline name resolver
 *   (registry.c, resolve_name_lookup) resolves `crate_a::helper` to the
 *   sole `helper` candidate by bare-name suffix scoring — WITHOUT needing
 *   any cross-crate workspace metadata.  This produced calls >= 2 (the
 *   intra-file main→run plus the bare-name-resolved run→helper), making
 *   the old ASSERT_GTE(calls, 2) GREEN even though the bug was not fixed.
 *
 *   Fix: add a LOCAL `fn helper()` in crate_b/src/main.rs so there are
 *   now TWO `helper` candidates in the project registry.  The generic
 *   resolver either picks the wrong one (crate_b-local) or abstains
 *   (ambiguous).  Only a correctly crate-qualified resolver routes
 *   `crate_a::helper` specifically to crate_a's node.  The assertion then
 *   checks the TARGET node's qualified_name contains "crate_a" — a count
 *   check is no longer sufficient because the local helper also contributes
 *   a CALLS edge (run_local→helper).
 *
 * UNCERTAINTY:
 *   If a future version plumbs workspace metadata or wires Rust lsp_cross
 *   correctly, this test will go GREEN — that is the intended outcome.
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
 * RED condition:
 *   No CALLS edge whose target QN contains "crate_a" exists in the store.
 *
 * This test is RED on current code because:
 *   1. cbm_run_rust_lsp is called with NULL manifest (cbm.c:645), so no
 *      workspace metadata is available at extraction time.
 *   2. cbm_pxc_has_cross_lsp returns false for CBM_LANG_RUST
 *      (pass_lsp_cross.c:281), so the cross-file LSP pass never runs for
 *      Rust and cannot seed crate_a defs into crate_b's resolver context.
 *   3. With two `helper` candidates (crate_a and crate_b-local), the
 *      generic resolver's qualified_suffix_match fails (neither QN ends
 *      with ".crate_a.helper") and bare-name scoring picks the crate_b-
 *      local one or abstains, never routing to crate_a.
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
