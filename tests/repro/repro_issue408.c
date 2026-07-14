/*
 * repro_issue408.c — Reproduce-first case for OPEN bug #408.
 *
 * Issue #408: "package.json `workspaces` cross-repo IMPORTS still produce
 * zero edges"
 *
 * Root cause (pass_pkgmap.c / pipeline.c):
 *   In a Yarn/Lerna-style JS/TS monorepo, `packages/b` imports a sibling by
 *   its declared package name (`import { x } from '@org/a'`).  pass_pkgmap.c
 *   is supposed to:
 *     1. Walk the repo filesystem for package.json manifests (cbm_pkgmap_scan_repo).
 *     2. Parse each sibling package.json, mapping its `"name"` field to its
 *        entry-point QN (parse_package_json → pkg_entries_push).
 *     3. On import resolution (cbm_pipeline_resolve_module), perform an exact
 *        lookup of `"@org/a"` in the pkgmap hash table to obtain the sibling's
 *        QN, then produce an IMPORTS edge to that node.
 *
 *   The reporter's debug trace (macOS arm64, v0.7.0) shows that the pkgmap
 *   pass never emits any `pkgmap.*` log lines:
 *       pipeline.done nodes=12 edges=9 elapsed_ms=71
 *   — zero IMPORTS edges despite a bare-specifier workspace import.  The
 *   maintainer confirmed: on macOS/Linux cbm_pkgmap_scan_repo may resolve
 *   workspace names at the manifest-parse level (cbm_pkgmap_try_parse), but
 *   the resolved entry-QN is never matched against the in-graph node produced
 *   by indexing `packages/a/index.js`.  The mismatch means the exact-lookup
 *   in cbm_pipeline_resolve_module (step 3) silently falls through to
 *   default (unresolved) QN resolution, and no cross-package IMPORTS edge is
 *   ever produced.
 *
 * Expected (correct) behaviour:
 *   Indexing a minimal monorepo:
 *       root/package.json        { "workspaces": ["packages/<glob>"] }
 *       packages/a/package.json  { "name": "@org/a", "main": "index.js" }
 *       packages/a/index.js      export function fromA() { return 1; }
 *       packages/b/package.json  { "name": "@org/b", "main": "index.js" }
 *       packages/b/index.js      import { fromA } from '@org/a';
 *                                export function useA() { return fromA(); }
 *   must produce AT LEAST ONE IMPORTS edge in the graph.
 *   (The only possible target of `import … from '@org/a'` is the sibling
 *   package — there are no relative imports in this fixture.)
 *
 * Actual (buggy) behaviour:
 *   rh_count_edges(store, project, "IMPORTS") == 0
 *   The assertion ASSERT_GTE(imports, 1) FAILS → RED.
 *
 * Why STRONGER than the existing weak test
 *   (`contract_edge_workspaces_imports_issue408` in tests/test_lang_contract.c):
 *
 *   The existing test asserts `edge_present(f, 5, "IMPORTS", 1)`, which
 *   succeeds whenever ANY IMPORTS edge exists in the indexed project.  In the
 *   original test_lang_contract.c fixture this is satisfied trivially by a
 *   relative import or a self-import resolved within a single package — the
 *   cross-package bare-specifier resolution is never exercised.
 *
 *   This repro fixture is DESIGNED so the only source of IMPORTS edges is the
 *   bare-specifier cross-package import in packages/b/index.js:
 *       import { fromA } from '@org/a';
 *   Neither packages/a/index.js nor packages/b/index.js contains any
 *   relative import ("./…") or intra-package import.  Therefore:
 *       rh_count_edges(..., "IMPORTS") >= 1
 *   is ONLY satisfiable if the cross-package workspace resolution succeeded.
 *   On current (buggy) code this count is 0, so the assertion is RED.
 *
 *   In addition, the fixture omits `"dependencies"` from packages/b/package.json
 *   on purpose: workspace resolution must be driven purely by the monorepo
 *   `"workspaces"` glob, not by an explicit `dependencies` field — matching
 *   the reporter's minimal repro from the issue comments.
 */

#include "test_framework.h"
#include "repro_harness.h"

/* ── Test ──────────────────────────────────────────────────────────── */

/*
 * repro_issue408_workspace_crosspkg_import
 *
 * Indexes a minimal Yarn-style JS monorepo where packages/b imports
 * sibling packages/a by its package.json `"name"` (@org/a).  This is
 * a PURE CROSS-PACKAGE bare-specifier import: no relative imports exist
 * anywhere in the fixture.  Therefore the only possible source of an
 * IMPORTS edge is the workspace-resolved @org/a reference.
 *
 * RED if:
 *   • rh_count_edges(store, project, "IMPORTS") == 0
 *     (workspace resolution did not produce a cross-package IMPORTS edge)
 */
TEST(repro_issue408_workspace_crosspkg_import) {
    /*
     * Fixture layout mirrors the reporter's /tmp/cbm-issue408-repro tree
     * (issue #408 comment, macOS arm64 canonical repro).  Five files:
     *
     *   package.json             — root workspace manifest; workspaces glob
     *   packages/a/package.json  — sibling A's manifest; name = "@org/a"
     *   packages/a/index.js      — sibling A; exports fromA (no imports)
     *   packages/b/package.json  — sibling B's manifest; name = "@org/b"
     *   packages/b/index.js      — sibling B; bare-specifier import of @org/a
     *
     * Note: packages/b/package.json deliberately omits "dependencies" so
     * that workspace resolution cannot be driven by that field.
     *
     * Note: neither .js file contains any relative import; the ONLY import
     * statement is `import { fromA } from '@org/a'` in packages/b/index.js.
     * Therefore rh_count_edges(..., "IMPORTS") >= 1 is satisfied ONLY if
     * the cross-package workspace bare-specifier resolution worked.
     */
    static const RFile files[] = {
        /* Root workspace manifest */
        {
            "package.json",
            "{\"name\":\"monorepo-root\",\"private\":true,"
            "\"workspaces\":[\"packages/*\"]}\n"
        },
        /* Sibling A — the imported package */
        {
            "packages/a/package.json",
            "{\"name\":\"@org/a\",\"version\":\"1.0.0\","
            "\"main\":\"index.js\"}\n"
        },
        {
            "packages/a/index.js",
            "export function fromA() {\n"
            "  return 1;\n"
            "}\n"
        },
        /* Sibling B — the importing package; NO relative imports */
        {
            "packages/b/package.json",
            "{\"name\":\"@org/b\",\"version\":\"1.0.0\","
            "\"main\":\"index.js\"}\n"
        },
        {
            "packages/b/index.js",
            "import { fromA } from '@org/a';\n"
            "\n"
            "export function useA() {\n"
            "  return fromA();\n"
            "}\n"
        }
    };

    RProj lp;
    cbm_store_t *store = rh_index_files(&lp, files, 5);
    ASSERT_NOT_NULL(store);

    /*
     * Count ALL IMPORTS edges in the project graph.
     *
     * Because this fixture contains ONLY one import statement and it is a
     * bare-specifier workspace reference (`import { fromA } from '@org/a'`),
     * the count is:
     *   ≥ 1  → cross-package workspace resolution worked (correct behaviour)
     *     0  → workspace resolution is broken            (bug #408, RED)
     *
     * On current (unfixed) code, pass_pkgmap resolves "@org/a" to a QN that
     * does not match any graph node, so cbm_pipeline_resolve_import_node
     * falls through to default resolution, producing zero IMPORTS edges.
     * This assertion therefore FAILS → RED.
     */
    int imports = rh_count_edges(store, lp.project, "IMPORTS");
    ASSERT_GTE(imports, 1);

    rh_cleanup(&lp, store);
    PASS();
}

/* ── Suite ──────────────────────────────────────────────────────────── */
SUITE(repro_issue408) {
    RUN_TEST(repro_issue408_workspace_crosspkg_import);
}
