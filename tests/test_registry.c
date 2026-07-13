/*
 * test_registry.c — Tests for function registry and FQN helpers.
 *
 * RED phase: Define expected behavior for symbol registration,
 * resolution strategies, and qualified name computation.
 */
#include "test_framework.h"
#include "pipeline/pipeline.h"

#include <stdlib.h>
#include <string.h>

/* ── FQN computation ──────────────────────────────────────────────── */

TEST(fqn_simple) {
    char *qn = cbm_pipeline_fqn_compute("myproj", "cmd/server/main.go", "HandleRequest");
    ASSERT_NOT_NULL(qn);
    ASSERT_STR_EQ(qn, "myproj.cmd.server.main.HandleRequest");
    free(qn);
    PASS();
}

TEST(fqn_no_name) {
    char *qn = cbm_pipeline_fqn_compute("myproj", "pkg/service.go", NULL);
    ASSERT_NOT_NULL(qn);
    ASSERT_STR_EQ(qn, "myproj.pkg.service");
    free(qn);
    PASS();
}

TEST(fqn_python_init) {
    /* __init__.py should be stripped */
    char *qn = cbm_pipeline_fqn_compute("myproj", "pkg/__init__.py", "Foo");
    ASSERT_NOT_NULL(qn);
    ASSERT_STR_EQ(qn, "myproj.pkg.Foo");
    free(qn);
    PASS();
}

TEST(fqn_js_index) {
    /* index.js should be stripped */
    char *qn = cbm_pipeline_fqn_compute("myproj", "src/index.ts", "App");
    ASSERT_NOT_NULL(qn);
    ASSERT_STR_EQ(qn, "myproj.src.App");
    free(qn);
    PASS();
}

TEST(fqn_module) {
    char *qn = cbm_pipeline_fqn_module("myproj", "cmd/server/main.go");
    ASSERT_NOT_NULL(qn);
    ASSERT_STR_EQ(qn, "myproj.cmd.server.main");
    free(qn);
    PASS();
}

TEST(fqn_folder) {
    char *qn = cbm_pipeline_fqn_folder("myproj", "cmd/server");
    ASSERT_NOT_NULL(qn);
    ASSERT_STR_EQ(qn, "myproj.cmd.server");
    free(qn);
    PASS();
}

TEST(fqn_root_file) {
    char *qn = cbm_pipeline_fqn_compute("proj", "main.go", "main");
    ASSERT_NOT_NULL(qn);
    ASSERT_STR_EQ(qn, "proj.main.main");
    free(qn);
    PASS();
}

/* ── FQN collision regression tests ──────────────────────────────── */
/* Bug: __init__.py Module QN collided with Folder QN, causing Folder
 * nodes to be overwritten during extraction. Symbols inside __init__.py
 * must still get clean package QNs (no __init__ in their QN). */

TEST(fqn_init_module_distinct_from_folder) {
    /* Module QN for __init__.py must differ from Folder QN for same dir */
    char *mod_qn = cbm_pipeline_fqn_module("proj", "pkg/__init__.py");
    char *folder_qn = cbm_pipeline_fqn_folder("proj", "pkg");
    ASSERT_NOT_NULL(mod_qn);
    ASSERT_NOT_NULL(folder_qn);
    /* These MUST be different — the old bug was they were both "proj.pkg" */
    ASSERT_STR_NEQ(mod_qn, folder_qn);
    /* Module should contain __init__ as disambiguator */
    ASSERT_NOT_NULL(strstr(mod_qn, "__init__"));
    /* Folder should NOT contain __init__ */
    ASSERT_EQ(strstr(folder_qn, "__init__"), NULL);
    free(mod_qn);
    free(folder_qn);
    PASS();
}

TEST(fqn_init_nested_module_distinct) {
    /* Same collision test for deeply nested __init__.py */
    char *mod_qn =
        cbm_pipeline_fqn_module("proj", "docker-images/cloud-runs/bq-sync-api/__init__.py");
    char *folder_qn = cbm_pipeline_fqn_folder("proj", "docker-images/cloud-runs/bq-sync-api");
    ASSERT_NOT_NULL(mod_qn);
    ASSERT_NOT_NULL(folder_qn);
    ASSERT_STR_NEQ(mod_qn, folder_qn);
    free(mod_qn);
    free(folder_qn);
    PASS();
}

TEST(fqn_index_ts_module_distinct_from_folder) {
    /* Same collision for JS/TS index.ts */
    char *mod_qn = cbm_pipeline_fqn_module("proj", "src/components/index.ts");
    char *folder_qn = cbm_pipeline_fqn_folder("proj", "src/components");
    ASSERT_NOT_NULL(mod_qn);
    ASSERT_NOT_NULL(folder_qn);
    ASSERT_STR_NEQ(mod_qn, folder_qn);
    free(mod_qn);
    free(folder_qn);
    PASS();
}

TEST(fqn_init_symbols_get_clean_package_qn) {
    /* Symbols inside __init__.py must NOT have __init__ in their QN.
     * "proj.pkg.Foo" not "proj.pkg.__init__.Foo" */
    char *sym_qn = cbm_pipeline_fqn_compute("proj", "pkg/__init__.py", "Foo");
    ASSERT_NOT_NULL(sym_qn);
    ASSERT_STR_EQ(sym_qn, "proj.pkg.Foo");
    ASSERT_EQ(strstr(sym_qn, "__init__"), NULL);
    free(sym_qn);
    PASS();
}

TEST(fqn_index_symbols_get_clean_qn) {
    /* Symbols inside index.ts must NOT have index in their QN */
    char *sym_qn = cbm_pipeline_fqn_compute("proj", "src/index.ts", "App");
    ASSERT_NOT_NULL(sym_qn);
    ASSERT_STR_EQ(sym_qn, "proj.src.App");
    free(sym_qn);
    PASS();
}

TEST(fqn_init_file_node_distinct) {
    /* File node QN (name="__file__") for __init__.py must be distinct from Folder */
    char *file_qn = cbm_pipeline_fqn_compute("proj", "pkg/__init__.py", "__file__");
    char *folder_qn = cbm_pipeline_fqn_folder("proj", "pkg");
    ASSERT_NOT_NULL(file_qn);
    ASSERT_NOT_NULL(folder_qn);
    ASSERT_STR_NEQ(file_qn, folder_qn);
    free(file_qn);
    free(folder_qn);
    PASS();
}

TEST(fqn_regular_module_unchanged) {
    /* Non-init modules should be unaffected by the fix */
    char *qn = cbm_pipeline_fqn_module("proj", "pkg/utils.py");
    ASSERT_NOT_NULL(qn);
    ASSERT_STR_EQ(qn, "proj.pkg.utils");
    free(qn);
    PASS();
}

TEST(project_name_from_path) {
    char *name = cbm_project_name_from_path("/Users/dev/project");
    ASSERT_NOT_NULL(name);
    ASSERT_STR_EQ(name, "Users-dev-project");
    free(name);
    PASS();
}

TEST(project_name_from_root) {
    char *name = cbm_project_name_from_path("/");
    ASSERT_NOT_NULL(name);
    ASSERT_STR_EQ(name, "root");
    free(name);
    PASS();
}

/* ── Registry lifecycle ───────────────────────────────────────────── */

TEST(registry_create_free) {
    cbm_registry_t *r = cbm_registry_new();
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(cbm_registry_size(r), 0);
    cbm_registry_free(r);
    PASS();
}

TEST(registry_free_null) {
    cbm_registry_free(NULL); /* should not crash */
    PASS();
}

TEST(registry_add_and_exists) {
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add(r, "main", "proj.cmd.main", "Function");
    ASSERT_EQ(cbm_registry_size(r), 1);
    ASSERT_TRUE(cbm_registry_exists(r, "proj.cmd.main"));
    ASSERT_FALSE(cbm_registry_exists(r, "proj.cmd.other"));
    cbm_registry_free(r);
    PASS();
}

TEST(registry_label_of) {
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add(r, "Foo", "proj.pkg.Foo", "Class");
    cbm_registry_add(r, "bar", "proj.pkg.bar", "Function");

    ASSERT_STR_EQ(cbm_registry_label_of(r, "proj.pkg.Foo"), "Class");
    ASSERT_STR_EQ(cbm_registry_label_of(r, "proj.pkg.bar"), "Function");
    ASSERT_NULL(cbm_registry_label_of(r, "proj.pkg.baz"));

    cbm_registry_free(r);
    PASS();
}

TEST(registry_find_by_name) {
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add(r, "main", "proj.cmd.main", "Function");
    cbm_registry_add(r, "main", "proj.srv.main", "Function");
    cbm_registry_add(r, "helper", "proj.util.helper", "Function");

    const char **out = NULL;
    int count = 0;
    cbm_registry_find_by_name(r, "main", &out, &count);
    ASSERT_EQ(count, 2);

    cbm_registry_find_by_name(r, "helper", &out, &count);
    ASSERT_EQ(count, 1);

    cbm_registry_find_by_name(r, "nonexistent", &out, &count);
    ASSERT_EQ(count, 0);

    cbm_registry_free(r);
    PASS();
}

TEST(registry_no_duplicates) {
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add(r, "main", "proj.cmd.main", "Function");
    cbm_registry_add(r, "main", "proj.cmd.main", "Function"); /* duplicate */
    ASSERT_EQ(cbm_registry_size(r), 1);

    const char **out = NULL;
    int count = 0;
    cbm_registry_find_by_name(r, "main", &out, &count);
    ASSERT_EQ(count, 1); /* no duplicate in byName list */

    cbm_registry_free(r);
    PASS();
}

/* ── Resolution strategies ────────────────────────────────────────── */

TEST(resolve_same_module) {
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add(r, "helper", "proj.pkg.service.helper", "Function");

    /* Call "helper" from the same module → should resolve */
    cbm_resolution_t res = cbm_registry_resolve(r, "helper", "proj.pkg.service", NULL, NULL, 0);
    ASSERT_STR_EQ(res.qualified_name, "proj.pkg.service.helper");
    ASSERT_STR_EQ(res.strategy, "same_module");
    ASSERT_TRUE(res.confidence >= 0.85);

    cbm_registry_free(r);
    PASS();
}

/* A package/namespace-qualified callee whose bare name is defined in several
 * places must resolve to the package named in the call — not collapse onto a
 * single winner. Regression for qualified cross-file calls (e.g. Perl
 * Foo::Bar::sub()) where the same sub name exists in multiple packages. */
TEST(resolve_qualified_disambiguates_same_name) {
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add(r, "save", "proj.lib.App.Alpha.save", "Function");
    cbm_registry_add(r, "save", "proj.lib.App.Beta.save", "Function");
    cbm_registry_add(r, "save", "proj.lib.App.Gamma.save", "Function");

    /* Each fully-qualified call routes to its own package. */
    cbm_resolution_t a =
        cbm_registry_resolve(r, "App::Alpha::save", "proj.lib.App.Caller", NULL, NULL, 0);
    ASSERT_STR_EQ(a.qualified_name, "proj.lib.App.Alpha.save");
    ASSERT_STR_EQ(a.strategy, "qualified_suffix");

    cbm_resolution_t b =
        cbm_registry_resolve(r, "App::Beta::save", "proj.lib.App.Caller", NULL, NULL, 0);
    ASSERT_STR_EQ(b.qualified_name, "proj.lib.App.Beta.save");

    cbm_resolution_t g =
        cbm_registry_resolve(r, "App::Gamma::save", "proj.lib.App.Caller", NULL, NULL, 0);
    ASSERT_STR_EQ(g.qualified_name, "proj.lib.App.Gamma.save");

    /* The dotted callee form (Go/Python/C#) disambiguates identically. */
    cbm_resolution_t dotted =
        cbm_registry_resolve(r, "App.Beta.save", "proj.lib.App.Caller", NULL, NULL, 0);
    ASSERT_STR_EQ(dotted.qualified_name, "proj.lib.App.Beta.save");
    ASSERT_STR_EQ(dotted.strategy, "qualified_suffix");

    /* A qualified callee whose tail matches NO candidate falls through to the
     * existing bare-name scoring (never a qualified_suffix result). */
    cbm_resolution_t nomatch =
        cbm_registry_resolve(r, "Other::Pkg::save", "proj.lib.App.Caller", NULL, NULL, 0);
    ASSERT_TRUE(!nomatch.strategy || strcmp(nomatch.strategy, "qualified_suffix") != 0);

    /* A bare call stays ambiguous (no qualifier → no disambiguation signal). */
    cbm_resolution_t bare =
        cbm_registry_resolve(r, "save", "proj.lib.App.Caller", NULL, NULL, 0);
    ASSERT_TRUE(!bare.strategy || strcmp(bare.strategy, "qualified_suffix") != 0);

    cbm_registry_free(r);
    PASS();
}

/* When two candidates share the same qualified tail, a qualified callee is
 * genuinely ambiguous and must fall through to bare-name scoring rather than
 * pick arbitrarily under the high-confidence qualified_suffix strategy. */
TEST(resolve_qualified_ambiguous_tail_falls_through) {
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add(r, "run", "proj.svcA.Foo.Bar.run", "Function");
    cbm_registry_add(r, "run", "proj.svcB.Foo.Bar.run", "Function");

    /* "Foo::Bar::run" tail matches BOTH candidates → not unique → fall through. */
    cbm_resolution_t res =
        cbm_registry_resolve(r, "Foo::Bar::run", "proj.svcA.Caller", NULL, NULL, 0);
    ASSERT_TRUE(!res.strategy || strcmp(res.strategy, "qualified_suffix") != 0);

    cbm_registry_free(r);
    PASS();
}

TEST(resolve_import_map) {
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add(r, "Process", "proj.pkg.worker.Process", "Function");

    /* Import map: "worker" → "proj.pkg.worker" */
    const char *keys[] = {"worker"};
    const char *vals[] = {"proj.pkg.worker"};

    /* Call "worker.Process" → should resolve via import map */
    cbm_resolution_t res =
        cbm_registry_resolve(r, "worker.Process", "proj.cmd.main", keys, vals, 1);
    ASSERT_STR_EQ(res.qualified_name, "proj.pkg.worker.Process");
    ASSERT_STR_EQ(res.strategy, "import_map");
    ASSERT_TRUE(res.confidence >= 0.90);

    cbm_registry_free(r);
    PASS();
}

/* Bare function call (no dot) routed through import_map. The candidate QN
 * must be module_qn.callee, not module_qn — otherwise lookups fall through
 * to name-based resolution and pick a same-named function from a different
 * file. Regression for the @/lib/auth-style import case. */
TEST(resolve_import_map_bare_function) {
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add(r, "requireAdmin", "proj.lib.authorization.requireAdmin", "Function");
    /* Same name in another module — without the fix this is what gets picked. */
    cbm_registry_add(r, "requireAdmin", "proj.lib.users.requireAdmin", "Function");

    const char *keys[] = {"requireAdmin"};
    const char *vals[] = {"proj.lib.authorization"};

    cbm_resolution_t res =
        cbm_registry_resolve(r, "requireAdmin", "proj.actions.settings", keys, vals, 1);
    ASSERT_STR_EQ(res.qualified_name, "proj.lib.authorization.requireAdmin");
    ASSERT_STR_EQ(res.strategy, "import_map");

    cbm_registry_free(r);
    PASS();
}

/* Aliased bare import (`from m import f as g`, called as g()). The import-map
 * value is the full symbol QN (IMPORTS edge targets the function node), and
 * the callee at the site is the alias g — NOT f. Resolution must return the
 * symbol directly instead of appending the alias (which yields m.f.g → miss).
 * Regression for #875. */
TEST(resolve_import_map_bare_alias) {
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add(r, "scan_bash", "proj.security_scan.scan_bash", "Function");
    /* Import map: alias "_scan_bash" → FULL SYMBOL QN (not the module). */
    const char *keys[] = {"_scan_bash"};
    const char *vals[] = {"proj.security_scan.scan_bash"};
    cbm_resolution_t res =
        cbm_registry_resolve(r, "_scan_bash", "proj.hooks.pre_tool", keys, vals, 1);
    ASSERT_STR_EQ(res.qualified_name, "proj.security_scan.scan_bash");
    ASSERT_STR_EQ(res.strategy, "import_map");
    cbm_registry_free(r);
    PASS();
}

TEST(resolve_unique_name) {
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add(r, "UniqueFunc", "proj.deep.path.UniqueFunc", "Function");

    /* Call "UniqueFunc" — only one candidate project-wide */
    cbm_resolution_t res =
        cbm_registry_resolve(r, "UniqueFunc", "proj.other.module", NULL, NULL, 0);
    ASSERT_STR_EQ(res.qualified_name, "proj.deep.path.UniqueFunc");
    ASSERT_STR_EQ(res.strategy, "unique_name");

    cbm_registry_free(r);
    PASS();
}

TEST(resolve_unresolved) {
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add(r, "foo", "proj.pkg.foo", "Function");

    /* Call "nonexistent" — not in registry */
    cbm_resolution_t res = cbm_registry_resolve(r, "nonexistent", "proj.other", NULL, NULL, 0);
    ASSERT_TRUE(res.qualified_name == NULL || res.qualified_name[0] == '\0');

    cbm_registry_free(r);
    PASS();
}

TEST(resolve_many_nodes) {
    cbm_registry_t *r = cbm_registry_new();
    /* Add 500 functions */
    for (int i = 0; i < 500; i++) {
        char name[32], qn[64];
        snprintf(name, sizeof(name), "func_%d", i);
        snprintf(qn, sizeof(qn), "proj.pkg.func_%d", i);
        cbm_registry_add(r, name, qn, "Function");
    }
    ASSERT_EQ(cbm_registry_size(r), 500);

    /* Resolve one */
    cbm_resolution_t res = cbm_registry_resolve(r, "func_250", "proj.pkg", NULL, NULL, 0);
    ASSERT_STR_EQ(res.qualified_name, "proj.pkg.func_250");

    cbm_registry_free(r);
    PASS();
}

/* ── Confidence band ───────────────────────────────────────────── */

TEST(confidence_band_high) {
    ASSERT_STR_EQ(cbm_confidence_band(0.95), "high");
    ASSERT_STR_EQ(cbm_confidence_band(0.70), "high");
    PASS();
}

TEST(confidence_band_medium) {
    ASSERT_STR_EQ(cbm_confidence_band(0.55), "medium");
    ASSERT_STR_EQ(cbm_confidence_band(0.45), "medium");
    PASS();
}

TEST(confidence_band_speculative) {
    ASSERT_STR_EQ(cbm_confidence_band(0.40), "speculative");
    ASSERT_STR_EQ(cbm_confidence_band(0.25), "speculative");
    ASSERT_STR_EQ(cbm_confidence_band(0.20), "");
    ASSERT_STR_EQ(cbm_confidence_band(0.0), "");
    PASS();
}

/* ── Suffix match resolution ──────────────────────────────────── */

TEST(resolve_suffix_match) {
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add(r, "Process", "proj.svcA.Process", "Function");
    cbm_registry_add(r, "Process", "proj.svcB.Process", "Function");

    /* Caller in svcA — should prefer svcA via import distance */
    cbm_resolution_t res = cbm_registry_resolve(r, "Process", "proj.svcA.caller", NULL, NULL, 0);
    ASSERT_STR_EQ(res.qualified_name, "proj.svcA.Process");
    ASSERT_STR_EQ(res.strategy, "suffix_match");
    ASSERT_TRUE(res.confidence >= 0.50 && res.confidence <= 0.60);

    cbm_registry_free(r);
    PASS();
}

/* A name with more than REG_MAX_CANDIDATES (256) registered definitions is
 * unresolvable by name alone: the candidate penalty floors its confidence to
 * ~3/count (noise), while walking the candidate array per file dominated
 * usage-resolution CPU on the Linux kernel ("flags"/"dev"/"list_head" have
 * 4-7k definitions each). resolve must bail out with an empty result instead
 * of scanning and emitting a near-zero-confidence edge. */
TEST(resolve_caps_unresolvably_ambiguous_names) {
    cbm_registry_t *r = cbm_registry_new();
    for (int i = 0; i < 300; i++) {
        char qn[64];
        snprintf(qn, sizeof(qn), "proj.mod%d.flags", i);
        cbm_registry_add(r, "flags", qn, "Variable");
    }
    cbm_resolution_t res = cbm_registry_resolve(r, "flags", "proj.other.caller", NULL, NULL, 0);
    ASSERT_TRUE(res.qualified_name == NULL || res.qualified_name[0] == '\0');

    /* Same-module resolution still wins regardless of candidate count. */
    res = cbm_registry_resolve(r, "flags", "proj.mod7", NULL, NULL, 0);
    ASSERT_STR_EQ(res.qualified_name, "proj.mod7.flags");
    ASSERT_STR_EQ(res.strategy, "same_module");

    cbm_registry_free(r);
    PASS();
}

/* ── Import map suffix resolution ─────────────────────────────── */

TEST(resolve_import_map_suffix) {
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add(r, "Foo", "proj.other.sub.Foo", "Function");

    const char *keys[] = {"other"};
    const char *vals[] = {"proj.other"};

    /* "other.Foo" → exact "proj.other.Foo" not found →
     * suffix scan finds "proj.other.sub.Foo" */
    cbm_resolution_t res = cbm_registry_resolve(r, "other.Foo", "proj.pkg", keys, vals, 1);
    ASSERT_STR_EQ(res.qualified_name, "proj.other.sub.Foo");
    ASSERT_STR_EQ(res.strategy, "import_map_suffix");
    ASSERT_TRUE(res.confidence >= 0.80 && res.confidence <= 0.90);

    cbm_registry_free(r);
    PASS();
}

/* ── Import reachability tests ────────────────────────────────── */

TEST(resolve_is_import_reachable) {
    /* Test import reachability through unique_name confidence penalty.
     * is_import_reachable is static, so we test it indirectly. */
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add(r, "Helper", "proj.shared.utils.Helper", "Function");

    /* With import covering the module → full confidence */
    const char *keys1[] = {"utils"};
    const char *vals1[] = {"proj.shared.utils"};
    cbm_resolution_t res = cbm_registry_resolve(r, "Helper", "proj.caller", keys1, vals1, 1);
    ASSERT_STR_EQ(res.strategy, "unique_name");
    ASSERT_TRUE(res.confidence >= 0.70); /* 0.75, not halved */

    /* With import NOT covering the module → halved */
    const char *keys2[] = {"other"};
    const char *vals2[] = {"proj.other"};
    res = cbm_registry_resolve(r, "Helper", "proj.caller", keys2, vals2, 1);
    ASSERT_STR_EQ(res.strategy, "unique_name");
    ASSERT_TRUE(res.confidence <= 0.40); /* 0.75 * 0.5 = 0.375 */

    cbm_registry_free(r);
    PASS();
}

TEST(resolve_import_reachable_prefix) {
    /* "proj.handler.sub.Process" should be reachable via import "proj.handler" */
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add(r, "Process", "proj.handler.sub.Process", "Function");

    const char *keys[] = {"handler"};
    const char *vals[] = {"proj.handler"};
    cbm_resolution_t res = cbm_registry_resolve(r, "Process", "proj.caller", keys, vals, 1);
    ASSERT_STR_EQ(res.strategy, "unique_name");
    ASSERT_TRUE(res.confidence >= 0.70); /* reachable → no penalty */

    cbm_registry_free(r);
    PASS();
}

/* ── Negative import evidence ─────────────────────────────────── */

TEST(negative_import_rejects_unimported) {
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add(r, "Process", "proj.billing.Process", "Function");
    cbm_registry_add(r, "Process", "proj.handler.Process", "Function");

    /* Import only handler's module — suffix_match should prefer handler */
    const char *keys[] = {"handler"};
    const char *vals[] = {"proj.handler"};
    cbm_resolution_t res = cbm_registry_resolve(r, "Process", "proj.caller", keys, vals, 1);
    ASSERT_STR_EQ(res.qualified_name, "proj.handler.Process");

    cbm_registry_free(r);
    PASS();
}

/* ── Fuzzy resolve ────────────────────────────────────────────── */

TEST(fuzzy_resolve_single_candidate) {
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add(r, "CreateOrder", "svcA.handlers.CreateOrder", "Function");
    cbm_registry_add(r, "ValidateOrder", "svcB.validators.ValidateOrder", "Function");

    /* FuzzyResolve should find by simple name even with unknown prefix */
    cbm_fuzzy_result_t fr =
        cbm_registry_fuzzy_resolve(r, "unknownPkg.CreateOrder", "svcC.caller", NULL, NULL, 0);
    ASSERT_TRUE(fr.ok);
    ASSERT_STR_EQ(fr.result.qualified_name, "svcA.handlers.CreateOrder");

    cbm_registry_free(r);
    PASS();
}

TEST(fuzzy_resolve_nonexistent) {
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add(r, "CreateOrder", "svcA.handlers.CreateOrder", "Function");

    cbm_fuzzy_result_t fr =
        cbm_registry_fuzzy_resolve(r, "NonExistent", "svcC.caller", NULL, NULL, 0);
    ASSERT_FALSE(fr.ok);

    cbm_registry_free(r);
    PASS();
}

TEST(fuzzy_resolve_multiple_best_by_distance) {
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add(r, "Process", "svcA.handlers.Process", "Function");
    cbm_registry_add(r, "Process", "svcB.handlers.Process", "Function");

    /* Caller in svcA — should prefer svcA */
    cbm_fuzzy_result_t fr =
        cbm_registry_fuzzy_resolve(r, "unknown.Process", "svcA.other", NULL, NULL, 0);
    ASSERT_TRUE(fr.ok);
    ASSERT_STR_EQ(fr.result.qualified_name, "svcA.handlers.Process");

    /* Caller in svcB — should prefer svcB */
    fr = cbm_registry_fuzzy_resolve(r, "unknown.Process", "svcB.other", NULL, NULL, 0);
    ASSERT_TRUE(fr.ok);
    ASSERT_STR_EQ(fr.result.qualified_name, "svcB.handlers.Process");

    cbm_registry_free(r);
    PASS();
}

TEST(fuzzy_resolve_deep_name_extraction) {
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add(r, "DoWork", "myproject.utils.DoWork", "Function");

    /* Deeply qualified callee — should extract "DoWork" */
    cbm_fuzzy_result_t fr =
        cbm_registry_fuzzy_resolve(r, "some.deep.module.DoWork", "myproject.caller", NULL, NULL, 0);
    ASSERT_TRUE(fr.ok);
    ASSERT_STR_EQ(fr.result.qualified_name, "myproject.utils.DoWork");

    cbm_registry_free(r);
    PASS();
}

TEST(fuzzy_resolve_empty_registry) {
    cbm_registry_t *r = cbm_registry_new();

    cbm_fuzzy_result_t fr =
        cbm_registry_fuzzy_resolve(r, "SomeFunc", "myproject.caller", NULL, NULL, 0);
    ASSERT_FALSE(fr.ok);

    cbm_registry_free(r);
    PASS();
}

TEST(fuzzy_resolve_confidence_single) {
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add(r, "Handler", "proj.svc.Handler", "Function");

    cbm_fuzzy_result_t fr =
        cbm_registry_fuzzy_resolve(r, "unknownPkg.Handler", "proj.caller", NULL, NULL, 0);
    ASSERT_TRUE(fr.ok);
    ASSERT_TRUE(fr.result.confidence >= 0.35 && fr.result.confidence <= 0.45);
    ASSERT_STR_EQ(fr.result.strategy, "fuzzy");

    cbm_registry_free(r);
    PASS();
}

TEST(fuzzy_resolve_confidence_distance) {
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add(r, "Process", "proj.svcA.Process", "Function");
    cbm_registry_add(r, "Process", "proj.svcB.Process", "Function");

    cbm_fuzzy_result_t fr =
        cbm_registry_fuzzy_resolve(r, "unknownPkg.Process", "proj.svcA.other", NULL, NULL, 0);
    ASSERT_TRUE(fr.ok);
    ASSERT_TRUE(fr.result.confidence >= 0.25 && fr.result.confidence <= 0.35);
    ASSERT_STR_EQ(fr.result.strategy, "fuzzy");

    cbm_registry_free(r);
    PASS();
}

TEST(fuzzy_penalty_unreachable_import) {
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add(r, "Handler", "proj.billing.Handler", "Function");

    /* Import for different module → confidence halved */
    const char *keys[] = {"other"};
    const char *vals[] = {"proj.other"};
    cbm_fuzzy_result_t fr =
        cbm_registry_fuzzy_resolve(r, "unknown.Handler", "proj.caller", keys, vals, 1);
    ASSERT_TRUE(fr.ok);
    /* 0.40 * 0.5 = 0.20 */
    ASSERT_TRUE(fr.result.confidence >= 0.15 && fr.result.confidence <= 0.25);

    cbm_registry_free(r);
    PASS();
}

TEST(fuzzy_no_import_map_passthrough) {
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add(r, "Handler", "proj.billing.Handler", "Function");

    /* nil import map → full confidence */
    cbm_fuzzy_result_t fr =
        cbm_registry_fuzzy_resolve(r, "unknown.Handler", "proj.caller", NULL, NULL, 0);
    ASSERT_TRUE(fr.ok);
    ASSERT_TRUE(fr.result.confidence >= 0.35 && fr.result.confidence <= 0.45);

    cbm_registry_free(r);
    PASS();
}

/* ── Perl builtin guard (#459 follow-up: call-graph noise) ───────── */

TEST(perl_builtin_set_recognizes_core_builtins) {
    /* Representative core builtins from across the sorted set. */
    ASSERT_TRUE(cbm_perl_is_builtin("push"));
    ASSERT_TRUE(cbm_perl_is_builtin("shift"));
    ASSERT_TRUE(cbm_perl_is_builtin("keys"));
    ASSERT_TRUE(cbm_perl_is_builtin("sprintf"));
    ASSERT_TRUE(cbm_perl_is_builtin("abs"));   /* first element */
    ASSERT_TRUE(cbm_perl_is_builtin("write")); /* last element */
    ASSERT_TRUE(cbm_perl_is_builtin("wantarray"));
    PASS();
}

TEST(perl_builtin_set_rejects_project_subs) {
    /* Genuine project sub names and edge inputs must NOT be flagged. */
    ASSERT_FALSE(cbm_perl_is_builtin("helper"));
    ASSERT_FALSE(cbm_perl_is_builtin("process_request"));
    ASSERT_FALSE(cbm_perl_is_builtin("Push")); /* case-sensitive */
    ASSERT_FALSE(cbm_perl_is_builtin(""));
    ASSERT_FALSE(cbm_perl_is_builtin(NULL));
    PASS();
}

TEST(perl_suppress_drops_weak_builtin_and_method_matches) {
    /* #476: a builtin/method call that landed via a WEAK short-name strategy is
     * generic-resolver noise and must be suppressed. */
    ASSERT_TRUE(cbm_perl_suppress_generic_match(true, false, "push", "suffix_match"));
    ASSERT_TRUE(cbm_perl_suppress_generic_match(true, false, "keys", "unique_name"));
    ASSERT_TRUE(cbm_perl_suppress_generic_match(true, true, "commit", "suffix_match"));
    ASSERT_TRUE(cbm_perl_suppress_generic_match(true, true, "log", "unique_name"));
    PASS();
}

TEST(perl_suppress_keeps_high_confidence_and_genuine_calls) {
    /* #476: high-confidence strategies are kept so a genuine same-file/imported
     * call to a builtin-named sub still resolves (criterion d). */
    ASSERT_FALSE(cbm_perl_suppress_generic_match(true, false, "log", "same_module"));
    ASSERT_FALSE(cbm_perl_suppress_generic_match(true, false, "open", "import_map"));
    /* import_map_suffix is a genuine import resolution (conf 0.85), not a weak
     * short-name guess — a '::'-qualified call resolved this way must be kept. */
    ASSERT_FALSE(cbm_perl_suppress_generic_match(true, true, "Foo::Bar::m", "import_map_suffix"));
    ASSERT_FALSE(cbm_perl_suppress_generic_match(true, true, "commit", "same_module"));
    /* A genuine non-builtin function call is never suppressed (edge survives). */
    ASSERT_FALSE(cbm_perl_suppress_generic_match(true, false, "helper", "suffix_match"));
    /* Non-Perl languages are never affected. */
    ASSERT_FALSE(cbm_perl_suppress_generic_match(false, false, "push", "suffix_match"));
    ASSERT_FALSE(cbm_perl_suppress_generic_match(false, true, "commit", "suffix_match"));
    /* No match (NULL/empty strategy) → nothing to suppress. */
    ASSERT_FALSE(cbm_perl_suppress_generic_match(true, false, "push", NULL));
    ASSERT_FALSE(cbm_perl_suppress_generic_match(true, true, "commit", ""));
    PASS();
}

TEST(tsjs_suppress_drops_weak_method_matches) {
    /* #592/#606: a TS/JS member call whose receiver the LSP could not type, that
     * landed via a WEAK short-name strategy, is generic-resolver noise → drop.
     * The strategies that actually reach the guards are the registry's
     * suffix_match / unique_name and the parallel field_type_hint; "fuzzy" is
     * covered defensively (cbm_registry_fuzzy_resolve is not wired into the
     * resolvers today) so a future wiring cannot silently reintroduce it. */
    ASSERT_TRUE(cbm_tsjs_suppress_weak_method_match(true, true, "suffix_match"));
    ASSERT_TRUE(cbm_tsjs_suppress_weak_method_match(true, true, "unique_name"));
    ASSERT_TRUE(cbm_tsjs_suppress_weak_method_match(true, true, "field_type_hint"));
    ASSERT_TRUE(cbm_tsjs_suppress_weak_method_match(true, true, "fuzzy"));
    PASS();
}

TEST(tsjs_suppress_keeps_high_confidence_and_non_methods) {
    /* Keep every receiver-/import-aware strategy. Because the PARALLEL resolver
     * runs lsp_* strategies through this same guard variable, an explicit
     * drop-list must never touch them — asserting the lsp_* keeps here is the
     * regression guard for the "kills lsp edges" failure mode. The keep set
     * enumerates the resolver's non-weak strategies: registry
     * {import_map, import_map_suffix, same_module, qualified_suffix}, parallel
     * {callee_suffix, service_pattern}, and lsp_*. */
    ASSERT_FALSE(cbm_tsjs_suppress_weak_method_match(true, true, "same_module"));
    ASSERT_FALSE(cbm_tsjs_suppress_weak_method_match(true, true, "import_map"));
    ASSERT_FALSE(cbm_tsjs_suppress_weak_method_match(true, true, "import_map_suffix"));
    ASSERT_FALSE(cbm_tsjs_suppress_weak_method_match(true, true, "qualified_suffix"));
    ASSERT_FALSE(cbm_tsjs_suppress_weak_method_match(true, true, "callee_suffix"));
    ASSERT_FALSE(cbm_tsjs_suppress_weak_method_match(true, true, "service_pattern"));
    ASSERT_FALSE(cbm_tsjs_suppress_weak_method_match(true, true, "lsp_ts_method"));
    ASSERT_FALSE(cbm_tsjs_suppress_weak_method_match(true, true, "lsp_cross"));
    ASSERT_FALSE(cbm_tsjs_suppress_weak_method_match(true, true, "lsp_ts_local"));
    /* A bare call (is_method=false) is a free-function call → never suppressed. */
    ASSERT_FALSE(cbm_tsjs_suppress_weak_method_match(true, false, "unique_name"));
    ASSERT_FALSE(cbm_tsjs_suppress_weak_method_match(true, false, "suffix_match"));
    /* Non-TS/JS languages are never affected. */
    ASSERT_FALSE(cbm_tsjs_suppress_weak_method_match(false, true, "suffix_match"));
    /* No match (NULL/empty strategy) → nothing to suppress. */
    ASSERT_FALSE(cbm_tsjs_suppress_weak_method_match(true, true, NULL));
    ASSERT_FALSE(cbm_tsjs_suppress_weak_method_match(true, true, ""));
    PASS();
}

/* ── Suite ─────────────────────────────────────────────────────── */

/* Method call THROUGH an imported symbol that is itself an indexed node
 * (`from m import sig; sig.send()`). The import-map value is the symbol QN
 * and the callee carries a suffix — resolution must return symbol.send, NOT
 * the bare symbol node. The #979 direct-hit early return swallowed the
 * suffix whenever the base symbol existed as an exact node, degrading
 * django-scale graphs by ~11K CALLS/TESTS edges (e.g. every
 * `user_logged_in.send(...)` bound to the signal VARIABLE instead of
 * Signal.send). Regression guard for #1000. */
TEST(resolve_import_map_alias_with_suffix_hits_method) {
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add(r, "user_logged_in", "proj.auth.signals.user_logged_in", "Variable");
    cbm_registry_add(r, "send", "proj.auth.signals.user_logged_in.send", "Method");
    const char *keys[] = {"user_logged_in"};
    const char *vals[] = {"proj.auth.signals.user_logged_in"};
    cbm_resolution_t res =
        cbm_registry_resolve(r, "user_logged_in.send", "proj.auth.views", keys, vals, 1);
    ASSERT_STR_EQ(res.qualified_name, "proj.auth.signals.user_logged_in.send");
    ASSERT_STR_EQ(res.strategy, "import_map");
    cbm_registry_free(r);
    PASS();
}

SUITE(registry) {
    /* FQN */
    RUN_TEST(fqn_simple);
    RUN_TEST(fqn_no_name);
    RUN_TEST(fqn_python_init);
    RUN_TEST(fqn_js_index);
    RUN_TEST(fqn_module);
    RUN_TEST(fqn_folder);
    RUN_TEST(fqn_root_file);
    /* FQN collision regression (Folder vs __init__.py Module) */
    RUN_TEST(fqn_init_module_distinct_from_folder);
    RUN_TEST(fqn_init_nested_module_distinct);
    RUN_TEST(fqn_index_ts_module_distinct_from_folder);
    RUN_TEST(fqn_init_symbols_get_clean_package_qn);
    RUN_TEST(fqn_index_symbols_get_clean_qn);
    RUN_TEST(fqn_init_file_node_distinct);
    RUN_TEST(fqn_regular_module_unchanged);
    RUN_TEST(project_name_from_path);
    RUN_TEST(project_name_from_root);
    /* Registry lifecycle */
    RUN_TEST(registry_create_free);
    RUN_TEST(registry_free_null);
    RUN_TEST(registry_add_and_exists);
    RUN_TEST(registry_label_of);
    RUN_TEST(registry_find_by_name);
    RUN_TEST(registry_no_duplicates);
    /* Resolution */
    RUN_TEST(resolve_same_module);
    RUN_TEST(resolve_qualified_disambiguates_same_name);
    RUN_TEST(resolve_qualified_ambiguous_tail_falls_through);
    RUN_TEST(resolve_import_map);
    RUN_TEST(resolve_import_map_bare_function);
    RUN_TEST(resolve_import_map_bare_alias);
    RUN_TEST(resolve_import_map_alias_with_suffix_hits_method);
    RUN_TEST(resolve_unique_name);
    RUN_TEST(resolve_unresolved);
    RUN_TEST(resolve_many_nodes);
    /* Confidence band */
    RUN_TEST(confidence_band_high);
    RUN_TEST(confidence_band_medium);
    RUN_TEST(confidence_band_speculative);
    /* Suffix match + import map suffix */
    RUN_TEST(resolve_suffix_match);
    RUN_TEST(resolve_caps_unresolvably_ambiguous_names);
    RUN_TEST(resolve_import_map_suffix);
    /* Import reachability */
    RUN_TEST(resolve_is_import_reachable);
    RUN_TEST(resolve_import_reachable_prefix);
    /* Negative import evidence */
    RUN_TEST(negative_import_rejects_unimported);
    /* Fuzzy resolve */
    RUN_TEST(fuzzy_resolve_single_candidate);
    RUN_TEST(fuzzy_resolve_nonexistent);
    RUN_TEST(fuzzy_resolve_multiple_best_by_distance);
    RUN_TEST(fuzzy_resolve_deep_name_extraction);
    RUN_TEST(fuzzy_resolve_empty_registry);
    RUN_TEST(fuzzy_resolve_confidence_single);
    RUN_TEST(fuzzy_resolve_confidence_distance);
    RUN_TEST(fuzzy_penalty_unreachable_import);
    RUN_TEST(fuzzy_no_import_map_passthrough);

    /* Perl builtin guard */
    RUN_TEST(perl_builtin_set_recognizes_core_builtins);
    RUN_TEST(perl_builtin_set_rejects_project_subs);
    RUN_TEST(perl_suppress_drops_weak_builtin_and_method_matches);
    RUN_TEST(perl_suppress_keeps_high_confidence_and_genuine_calls);
    RUN_TEST(tsjs_suppress_drops_weak_method_matches);
    RUN_TEST(tsjs_suppress_keeps_high_confidence_and_non_methods);
}
