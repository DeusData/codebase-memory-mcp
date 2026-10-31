/*
 * test_registry.c — Tests for function registry and FQN helpers.
 *
 * RED phase: Define expected behavior for symbol registration,
 * resolution strategies, and qualified name computation.
 */
#include "test_framework.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h" /* cbm_python_import_binding_contradicts (#2127) */

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

/* A read, write or value reference that resolved to a Method goes to the
 * method's fenced `#field` twin when the class has one; nothing else moves. */
TEST(registry_value_target_field_twin) {
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add(r, "count", "p.C.count", "Method");
    cbm_registry_add(r, "count", "p.C.count#field", "Field");
    cbm_registry_add(r, "size", "p.C.size", "Method");
    cbm_registry_add(r, "name", "p.C.name", "Field");
    ASSERT_STR_EQ(cbm_registry_value_target(r, "p.C.count"), "p.C.count#field");
    ASSERT_STR_EQ(cbm_registry_value_target(r, "p.C.size"), "p.C.size");
    ASSERT_STR_EQ(cbm_registry_value_target(r, "p.C.name"), "p.C.name");
    ASSERT_STR_EQ(cbm_registry_value_target(r, "p.C.none"), "p.C.none");
    /* the fenced field is still found by its plain name */
    const char **qns = NULL;
    int n = 0;
    ASSERT_EQ(cbm_registry_find_by_name(r, "count", &qns, &n), 0);
    ASSERT_EQ(n, 2);
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
    cbm_resolution_t bare = cbm_registry_resolve(r, "save", "proj.lib.App.Caller", NULL, NULL, 0);
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

/* cbm_registry_add used to discard its `name` argument and re-derive the
 * lookup key from the QN's last dot segment. That made the QN's tail load
 * bearing for the bare-name index every language shares: a Rust cfg twin,
 * minted as "add#cfg(test)", was indexed under that literal string and no bare
 * `add` callee could ever reach it. */
TEST(registry_indexes_by_passed_name_not_qn_tail) {
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add(r, "add", "proj.lib.add#cfg(test)", "Function");

    cbm_resolution_t res = cbm_registry_resolve(r, "add", "proj.lib.caller", NULL, NULL, 0);
    ASSERT_STR_EQ(res.qualified_name, "proj.lib.add#cfg(test)");

    cbm_registry_free(r);
    PASS();
}

/* The derived key stays unconditional, and this pins that.
 *
 * A name may carry segments the QN's tail drops, and then the tail is the key
 * callers actually spell. An HCL block is named "resource.aws_instance.web" by
 * find_hcl_block_name while its QN tail is bare "web", which is how an
 * `aws_instance.web.id` reference reaches it. Re-keying the index on the passed
 * name would lose that lookup outright, so the fence gate above leaves these
 * shapes exactly as they were.
 *
 * The Module row registers a shape production cannot produce: every caller of
 * cbm_registry_add gates on cbm_label_is_registry_symbol, which does not admit
 * "Module". It is here because the registry's own contract is per-label-string,
 * not per-caller, and a future label change should not silently drop the stem
 * lookup a bare module reference needs. */
TEST(registry_indexes_a_dotted_name_under_its_tail_too) {
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add(r, "resource.aws_instance.web", "proj.main.resource.aws_instance.web",
                     "Class");
    cbm_registry_add(r, "helper.py", "proj.pkg.helper", "Module");

    cbm_resolution_t tail = cbm_registry_resolve(r, "web", "proj.main", NULL, NULL, 0);
    ASSERT_STR_EQ(tail.qualified_name, "proj.main.resource.aws_instance.web");

    cbm_resolution_t whole =
        cbm_registry_resolve(r, "resource.aws_instance.web", "proj.main", NULL, NULL, 0);
    ASSERT_STR_EQ(whole.qualified_name, "proj.main.resource.aws_instance.web");

    cbm_resolution_t stem = cbm_registry_resolve(r, "helper", "proj.pkg.caller", NULL, NULL, 0);
    ASSERT_STR_EQ(stem.qualified_name, "proj.pkg.helper");

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

/* Adversarial pin: when import_map already stores the def QN under the alias
 * key, bare alias call must CALLS→def (not invent …M.bridge_execute). Behavior
 * already on main via #875/#979; this locks the Yui G1 shape. */
TEST(resolve_import_map_aliased_from_import) {
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add(r, "execute", "proj.services.satori_bridge.gate.execute", "Function");
    /* Alias ghost must not win if somehow registered. */
    cbm_registry_add(r, "bridge_execute", "proj.services.satori_bridge.gate.bridge_execute",
                     "Function");

    const char *keys[] = {"bridge_execute"};
    const char *vals[] = {"proj.services.satori_bridge.gate.execute"};

    cbm_resolution_t res =
        cbm_registry_resolve(r, "bridge_execute", "proj.services.yui_core.router", keys, vals, 1);
    ASSERT_STR_EQ(res.qualified_name, "proj.services.satori_bridge.gate.execute");
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

/* The per-file reachability memo answers exactly what the uncached check
 * answers, file after file: its key arena opens on the first memoized key and
 * is destroyed with the file, and one key is longer than the arena's first
 * block. The two files import different packages, so a memo leaking between
 * files would flip an answer. */
TEST(reach_cache_memo_matches_uncached_across_files) {
    static char long_qn[6000];
    memset(long_qn, 'a', sizeof(long_qn) - 1);
    long_qn[sizeof(long_qn) - 1] = '\0';
    memcpy(long_qn, "pkg.io.", 7);
    long_qn[5000] = '.';
    const char *cands[] = {"pkg.io.Reader", "pkg.net.Dial", "other.fmt.Println", long_qn};
    enum { NCANDS = 4 };
    const char *imports_io[] = {"pkg.io"};
    const char *imports_net[] = {"pkg.net", "other.fmt"};
    for (int file = 0; file < 4; file++) {
        bool io = (file % 2) == 0;
        const char **imports = io ? imports_io : imports_net;
        int nimports = io ? 1 : 2;
        bool expect[NCANDS];
        for (int i = 0; i < NCANDS; i++) {
            expect[i] = cbm_registry_is_import_reachable(cands[i], imports, nimports);
        }
        /* the uncached answers really differ between the files */
        ASSERT_EQ(expect[0], io);
        ASSERT_EQ(expect[1], !io);
        ASSERT_EQ(expect[3], io);
        cbm_registry_reach_cache_begin(8);
        for (int pass = 0; pass < 2; pass++) { /* the second pass is served by the memo */
            for (int i = 0; i < NCANDS; i++) {
                ASSERT_EQ(cbm_registry_is_import_reachable(cands[i], imports, nimports), expect[i]);
            }
        }
        cbm_registry_reach_cache_end();
    }
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

/* 2026-09-16 probe on torvalds/linux: a Makefile target bound the C function
 * `sk_psock.eval` through unique_name. Build and configuration languages have
 * no cross-language call semantics, so for them a unique match into another
 * language is a collision by construction; a code caller keeps #1572. */
TEST(cross_language_config_caller_drops_unique_name_too) {
    ASSERT_TRUE(cbm_suppress_cross_language_suffix_match(CBM_LANG_MAKEFILE, "include/linux/skmsg.h",
                                                         "unique_name"));
    ASSERT_TRUE(
        cbm_suppress_cross_language_suffix_match(CBM_LANG_CMAKE, "src/main.c", "unique_name"));
    ASSERT_TRUE(
        cbm_suppress_cross_language_suffix_match(CBM_LANG_YAML, "app/models.py", "unique_name"));
    /* A code caller's unique_name into another language is still #1572. */
    ASSERT_FALSE(cbm_suppress_cross_language_suffix_match(
        CBM_LANG_PYTHON, "web/src/pages/Editor.js", "unique_name"));
    /* Same-language config targets are untouched. */
    ASSERT_FALSE(
        cbm_suppress_cross_language_suffix_match(CBM_LANG_MAKEFILE, "lib/Makefile", "unique_name"));
    /* Receiver-aware strategies are never this guard's business. */
    ASSERT_FALSE(
        cbm_suppress_cross_language_suffix_match(CBM_LANG_MAKEFILE, "src/main.c", "same_module"));
    PASS();
}

/* Same-named candidates that tie on test-status and namespace proximity used
 * to be settled by bucket position, i.e. by the order files were registered:
 * two indexes of one kernel tree differed by 632 CALLS edges, `dev_name`
 * landing on any of twenty same-named struct fields or on nothing at all.
 * The tie is now a function of the candidate set: the more plainly callable
 * candidate (the function over the fields), then the smaller QN, whichever
 * order the registry was built in (O9). */
TEST(registry_tie_break_is_independent_of_registration_order) {
    const char *cands[] = {
        "proj.drivers.media.cec.i2c.ch7322.ch7322_conn_match.dev_name", /* struct field */
        "proj.sound.soc.codecs.tas2783-sdw.tas2783_prv.dev_name",       /* struct field */
        "proj.include.linux.device.dev_name",                           /* the function */
        "proj.arch.um.drivers.pty.pty_chan.dev_name",                   /* struct field */
    };
    const char *labels[] = {"Field", "Field", "Function", "Field"};
    const int n = 4;

    cbm_registry_t *forward = cbm_registry_new();
    cbm_registry_t *backward = cbm_registry_new();
    for (int i = 0; i < n; i++) {
        cbm_registry_add(forward, "dev_name", cands[i], labels[i]);
        cbm_registry_add(backward, "dev_name", cands[n - 1 - i], labels[n - 1 - i]);
    }

    /* A far-away caller with no imports: every candidate scores the same. */
    cbm_resolution_t f =
        cbm_registry_resolve(forward, "dev_name", "proj.kernel.irq.msi", NULL, NULL, 0);
    cbm_resolution_t b =
        cbm_registry_resolve(backward, "dev_name", "proj.kernel.irq.msi", NULL, NULL, 0);
    ASSERT_NOT_NULL(f.qualified_name);
    ASSERT_NOT_NULL(b.qualified_name);
    ASSERT_STR_EQ(f.qualified_name, "proj.include.linux.device.dev_name");
    ASSERT_STR_EQ(b.qualified_name, "proj.include.linux.device.dev_name");
    ASSERT_STR_EQ(f.strategy, "suffix_match");
    ASSERT_STR_EQ(b.strategy, "suffix_match");

    /* Equal kind: the smaller QN, from either order. */
    cbm_registry_t *lex_a = cbm_registry_new();
    cbm_registry_t *lex_b = cbm_registry_new();
    cbm_registry_add(lex_a, "sg_next", "proj.tools.virtio.scatterlist.sg_next", "Function");
    cbm_registry_add(lex_a, "sg_next", "proj.include.linux.scatterlist.sg_next", "Function");
    cbm_registry_add(lex_b, "sg_next", "proj.include.linux.scatterlist.sg_next", "Function");
    cbm_registry_add(lex_b, "sg_next", "proj.tools.virtio.scatterlist.sg_next", "Function");
    cbm_resolution_t la =
        cbm_registry_resolve(lex_a, "sg_next", "proj.drivers.scsi.arm_scsi", NULL, NULL, 0);
    cbm_resolution_t lb =
        cbm_registry_resolve(lex_b, "sg_next", "proj.drivers.scsi.arm_scsi", NULL, NULL, 0);
    ASSERT_STR_EQ(la.qualified_name, "proj.include.linux.scatterlist.sg_next");
    ASSERT_STR_EQ(lb.qualified_name, "proj.include.linux.scatterlist.sg_next");

    /* Proximity outranks the tie order: the sibling wins over a shallower stranger. */
    cbm_registry_t *near = cbm_registry_new();
    cbm_registry_add(near, "vnic_rq_free", "proj.lib.vnic_rq_free", "Function");
    cbm_registry_add(near, "vnic_rq_free", "proj.drivers.scsi.fnic.vnic_rq.vnic_rq_free",
                     "Function");
    cbm_resolution_t nr = cbm_registry_resolve(near, "vnic_rq_free",
                                               "proj.drivers.scsi.fnic.fnic_res", NULL, NULL, 0);
    ASSERT_STR_EQ(nr.qualified_name, "proj.drivers.scsi.fnic.vnic_rq.vnic_rq_free");

    cbm_registry_free(forward);
    cbm_registry_free(backward);
    cbm_registry_free(lex_a);
    cbm_registry_free(lex_b);
    cbm_registry_free(near);
    PASS();
}

/* A call names something callable. When same-named candidates tie, a method
 * must not lose to a property or field that merely shares its name. Exposed
 * (Kotlin): `cities.selectAll().where { ... }` bound to the property
 * `AbstractQuery.where` instead of the jdbc `Query.where` function, because the
 * two QNs have the same depth and "exposed-core" sorts first; main moved 785 ->
 * 1,648 Kotlin CALLS edges onto Variable/Field targets this way. */
TEST(registry_tie_prefers_callable_over_property) {
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add(
        r, "where",
        "proj.exposed-core.src.main.kotlin.org.jetbrains.exposed.v1.core.AbstractQuery.where",
        "Variable");
    cbm_registry_add(r, "where",
                     "proj.exposed-jdbc.src.main.kotlin.org.jetbrains.exposed.v1.jdbc.Query.where",
                     "Method");
    /* A deeper method still beats a shallower field: kind decides before order. */
    cbm_registry_add(r, "limit", "proj.exposed-core.Table.limit", "Field");
    cbm_registry_add(r, "limit", "proj.exposed-jdbc.src.main.kotlin.jdbc.Query.limit", "Method");

    const char *caller =
        "proj.exposed-tests.src.test.kotlin.org.jetbrains.exposed.v1.tests.SelectTests";
    cbm_resolution_t w = cbm_registry_resolve(r, "where", caller, NULL, NULL, 0);
    cbm_resolution_t l = cbm_registry_resolve(r, "limit", caller, NULL, NULL, 0);
    ASSERT_STR_EQ(w.qualified_name,
                  "proj.exposed-jdbc.src.main.kotlin.org.jetbrains.exposed.v1.jdbc.Query.where");
    ASSERT_STR_EQ(l.qualified_name, "proj.exposed-jdbc.src.main.kotlin.jdbc.Query.limit");
    cbm_registry_free(r);
    PASS();
}

/* Nesting depth says nothing about which same-named method a call means.
 * django: `Model.objects.filter(...)` from tests/ tied between
 * `QuerySet.filter` and the template `Library.filter`; "least nested wins"
 * picked Library (one segment shallower) for most of the ~5,600 `objects.X`
 * calls (QuerySet share 57 % -> 24 %). Equal-kind ties now fall to the QN
 * order alone, which is still a pure function of the candidate set (O9). */
TEST(registry_tie_ignores_nesting_depth) {
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add(r, "filter", "proj.django.template.library.Library.filter", "Method");
    cbm_registry_add(r, "filter", "proj.django.db.models.query.QuerySet.filter", "Method");
    cbm_resolution_t f =
        cbm_registry_resolve(r, "filter", "proj.tests.lookup.tests", NULL, NULL, 0);
    ASSERT_STR_EQ(f.qualified_name, "proj.django.db.models.query.QuerySet.filter");
    ASSERT_STR_EQ(f.strategy, "suffix_match");
    cbm_registry_free(r);
    PASS();
}

/* .NET reference assemblies (`src/libraries/<Asm>/ref/<Asm>.cs`) declare every
 * public member of an assembly with a `throw null` body in one shallow file.
 * A call that ties between such a stub and the implementation must reach the
 * implementation: dotnet/runtime had 14.1 % of its CALLS on ref/ stubs (4.9 %
 * in v0.11.0) once ties preferred the shallower, then the smaller ("ref" <
 * "src") QN. The preference is C#'s alone: elsewhere a `ref` directory is an
 * ordinary name. */
TEST(registry_tie_prefers_csharp_implementation_over_ref_stub) {
    cbm_registry_t *r = cbm_registry_new();
    /* Shallower stub vs deeper implementation (MemoryStream). */
    cbm_registry_add_lang(
        r, "MemoryStream",
        "proj.src.libraries.System.Runtime.ref.System.Runtime.MemoryStream.MemoryStream", "Method",
        CBM_LANG_CSHARP);
    cbm_registry_add_lang(
        r, "MemoryStream",
        "proj.src.libraries.System.Private.CoreLib.src.System.IO.MemoryStream.MemoryStream",
        "Method", CBM_LANG_CSHARP);
    /* Equal depth, "ref" sorts before "src" (WithCancellation). */
    cbm_registry_add_lang(r, "WithCancellation",
                          "proj.src.libraries.System.Linq.Parallel.ref.System.Linq."
                          "ParallelEnumerable.WithCancellation",
                          "Method", CBM_LANG_CSHARP);
    cbm_registry_add_lang(r, "WithCancellation",
                          "proj.src.libraries.System.Linq.Parallel.src.System.Linq."
                          "ParallelEnumerable.WithCancellation",
                          "Method", CBM_LANG_CSHARP);
    /* Not C#: a `ref` directory gets no special treatment. */
    cbm_registry_add_lang(r, "resolve", "proj.lib.ref.Resolver.resolve", "Method", CBM_LANG_JAVA);
    cbm_registry_add_lang(r, "resolve", "proj.lib.src.Resolver.resolve", "Method", CBM_LANG_JAVA);

    const char *caller = "proj.src.libraries.System.Security.Cryptography.src.CapiHelper";
    cbm_resolution_t ms = cbm_registry_resolve(r, "MemoryStream", caller, NULL, NULL, 0);
    cbm_resolution_t wc = cbm_registry_resolve(r, "WithCancellation", caller, NULL, NULL, 0);
    cbm_resolution_t jv = cbm_registry_resolve(r, "resolve", "proj.app.Main", NULL, NULL, 0);
    ASSERT_STR_EQ(
        ms.qualified_name,
        "proj.src.libraries.System.Private.CoreLib.src.System.IO.MemoryStream.MemoryStream");
    ASSERT_STR_EQ(wc.qualified_name, "proj.src.libraries.System.Linq.Parallel.src.System.Linq."
                                     "ParallelEnumerable.WithCancellation");
    ASSERT_STR_EQ(jv.qualified_name, "proj.lib.ref.Resolver.resolve");
    cbm_registry_free(r);
    PASS();
}

TEST(lang_resolution_compatible_families) {
    /* Same language and the interop families the resolvers already honour. */
    ASSERT_TRUE(cbm_lang_resolution_compatible(CBM_LANG_PYTHON, CBM_LANG_PYTHON));
    ASSERT_TRUE(cbm_lang_resolution_compatible(CBM_LANG_JAVASCRIPT, CBM_LANG_TYPESCRIPT));
    ASSERT_TRUE(cbm_lang_resolution_compatible(CBM_LANG_VUE, CBM_LANG_TSX));
    ASSERT_TRUE(cbm_lang_resolution_compatible(CBM_LANG_C, CBM_LANG_CPP));
    ASSERT_TRUE(cbm_lang_resolution_compatible(CBM_LANG_OBJC, CBM_LANG_C));
    ASSERT_TRUE(cbm_lang_resolution_compatible(CBM_LANG_KOTLIN, CBM_LANG_JAVA));
    ASSERT_TRUE(cbm_lang_resolution_compatible(CBM_LANG_SWIFT, CBM_LANG_OBJC));
    ASSERT_TRUE(cbm_lang_resolution_compatible(CBM_LANG_OBJC, CBM_LANG_SWIFT));
    /* Unknown on either side: no evidence, no filter. */
    ASSERT_TRUE(cbm_lang_resolution_compatible(CBM_LANG_COUNT, CBM_LANG_PYTHON));
    ASSERT_TRUE(cbm_lang_resolution_compatible(CBM_LANG_JAVASCRIPT, CBM_LANG_COUNT));
    /* Across families: never by name alone. */
    ASSERT_FALSE(cbm_lang_resolution_compatible(CBM_LANG_JAVASCRIPT, CBM_LANG_PYTHON));
    ASSERT_FALSE(cbm_lang_resolution_compatible(CBM_LANG_PYTHON, CBM_LANG_TSX));
    ASSERT_FALSE(cbm_lang_resolution_compatible(CBM_LANG_GO, CBM_LANG_C));
    ASSERT_FALSE(cbm_lang_resolution_compatible(CBM_LANG_SWIFT, CBM_LANG_KOTLIN));
    ASSERT_FALSE(cbm_lang_resolution_compatible(CBM_LANG_MAKEFILE, CBM_LANG_C));
    /* Synthetic LSP sources carry their language. */
    ASSERT_EQ(cbm_registry_synthetic_path_language("<python-builtins>"), CBM_LANG_PYTHON);
    ASSERT_EQ(cbm_registry_synthetic_path_language("<kotlin-builtins>"), CBM_LANG_KOTLIN);
    ASSERT_EQ(cbm_registry_synthetic_path_language("app.py"), CBM_LANG_COUNT);
    ASSERT_EQ(cbm_registry_synthetic_path_language(NULL), CBM_LANG_COUNT);
    PASS();
}

/* Resolve with the unfiltered chain, then apply the cross-language veto the
 * way pass_calls.c / pass_parallel.c do. */
static cbm_resolution_t xl_resolve(const cbm_registry_t *r, CBMLanguage lang, const char *callee,
                                   const char *module_qn) {
    cbm_resolution_t res = cbm_registry_resolve(r, callee, module_qn, NULL, NULL, 0);
    if (cbm_registry_name_guess_vetoed(r, lang, &res)) {
        res = (cbm_resolution_t){0};
    }
    return res;
}

TEST(name_guess_vetoed_across_languages) {
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add_lang(r, "get", "builtins.dict.get", "Method", CBM_LANG_PYTHON);
    cbm_registry_add_lang(r, "patch", "proj.web.Panel.patch", "Function", CBM_LANG_TSX);
    cbm_registry_add_lang(r, "patch", "proj.api.client.patch", "Function", CBM_LANG_PYTHON);
    cbm_registry_add(r, "legacy", "proj.x.legacy", "Function"); /* unknown language */

    /* JS `app.get`: the only "get" is Python's -> EMPTY, not unique_name. */
    cbm_resolution_t res = xl_resolve(r, CBM_LANG_JAVASCRIPT, "app.get", "proj.app");
    ASSERT_TRUE(res.qualified_name == NULL || res.qualified_name[0] == '\0');
    /* Python keeps its own builtin. */
    res = xl_resolve(r, CBM_LANG_PYTHON, "d.get", "proj.tool");
    ASSERT_STR_EQ(res.qualified_name, "builtins.dict.get");
    ASSERT_STR_EQ(res.strategy, "unique_name");
    /* Two same-named candidates: the chain still picks by import distance;
     * the answer stands only for a caller that can call the winner. */
    res = xl_resolve(r, CBM_LANG_PYTHON, "patch", "proj.api.x");
    ASSERT_STR_EQ(res.qualified_name, "proj.api.client.patch");
    ASSERT_STR_EQ(res.strategy, "suffix_match");
    res = xl_resolve(r, CBM_LANG_TYPESCRIPT, "patch", "proj.api.x");
    ASSERT_TRUE(res.qualified_name == NULL || res.qualified_name[0] == '\0');
    res = xl_resolve(r, CBM_LANG_TYPESCRIPT, "patch", "proj.web.ui");
    ASSERT_STR_EQ(res.qualified_name, "proj.web.Panel.patch");
    /* Unknown-language entries and an unknown caller are never vetoed. */
    res = xl_resolve(r, CBM_LANG_GO, "legacy", "proj.go");
    ASSERT_STR_EQ(res.qualified_name, "proj.x.legacy");
    res = xl_resolve(r, CBM_LANG_COUNT, "app.get", "proj.app");
    ASSERT_STR_EQ(res.qualified_name, "builtins.dict.get");

    /* Evidence-backed strategies are never vetoed, whatever the language. */
    cbm_resolution_t imp = {.qualified_name = "builtins.dict.get",
                            .strategy = "import_map",
                            .confidence = 0.95,
                            .candidate_count = 1};
    ASSERT_FALSE(cbm_registry_name_guess_vetoed(r, CBM_LANG_JAVASCRIPT, &imp));
    imp.strategy = "lsp_generic_method";
    ASSERT_FALSE(cbm_registry_name_guess_vetoed(r, CBM_LANG_JAVASCRIPT, &imp));
    /* The parallel field-type hint re-picks language-blind: vetoed too. */
    imp.strategy = "field_type_hint";
    ASSERT_TRUE(cbm_registry_name_guess_vetoed(r, CBM_LANG_JAVASCRIPT, &imp));
    ASSERT_FALSE(cbm_registry_name_guess_vetoed(r, CBM_LANG_PYTHON, &imp));
    cbm_registry_free(r);
    PASS();
}

/* tests/cov.c and tests/cov.py share the module QN proj.tests.cov (module QNs
 * drop the extension): a same_module answer across languages is that
 * collision, not evidence. Within the caller's language it stands. */
TEST(same_module_vetoed_across_languages) {
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add_lang(r, "reducer_worker", "proj.tests.cov.reducer_worker", "Function",
                          CBM_LANG_PYTHON);
    cbm_registry_add_lang(r, "reducer_worker", "proj.other.reducer_worker", "Function",
                          CBM_LANG_PYTHON);
    cbm_registry_add_lang(r, "stat_child", "proj.tests.cov.stat_child", "Function", CBM_LANG_C);

    cbm_resolution_t res =
        cbm_registry_resolve(r, "reducer_worker", "proj.tests.cov", NULL, NULL, 0);
    ASSERT_STR_EQ(res.strategy, "same_module");
    ASSERT_TRUE(cbm_registry_name_guess_vetoed(r, CBM_LANG_C, &res));
    ASSERT_FALSE(cbm_registry_name_guess_vetoed(r, CBM_LANG_PYTHON, &res));

    res = cbm_registry_resolve(r, "stat_child", "proj.tests.cov", NULL, NULL, 0);
    ASSERT_STR_EQ(res.qualified_name, "proj.tests.cov.stat_child");
    ASSERT_FALSE(cbm_registry_name_guess_vetoed(r, CBM_LANG_C, &res));
    cbm_registry_free(r);
    PASS();
}

TEST(cross_language_suffix_match_drops_py_vs_js) {
    /* #725: two same-named symbols in different languages. suffix_match is the
     * strategy that collapses them; unique_name is #1572 and must stay. */
    ASSERT_TRUE(cbm_suppress_cross_language_suffix_match(CBM_LANG_PYTHON, "web/src/pages/Editor.js",
                                                         "suffix_match"));
    ASSERT_TRUE(
        cbm_suppress_cross_language_suffix_match(CBM_LANG_JAVASCRIPT, "store.py", "suffix_match"));
    ASSERT_TRUE(
        cbm_suppress_cross_language_suffix_match(CBM_LANG_BASH, "cli/main.py", "suffix_match"));
    ASSERT_FALSE(
        cbm_suppress_cross_language_suffix_match(CBM_LANG_PYTHON, "store.py", "suffix_match"));
    ASSERT_FALSE(cbm_suppress_cross_language_suffix_match(
        CBM_LANG_PYTHON, "web/src/pages/Editor.js", "unique_name"));
    ASSERT_FALSE(cbm_suppress_cross_language_suffix_match(
        CBM_LANG_PYTHON, "web/src/pages/Editor.js", "same_module"));
    ASSERT_FALSE(cbm_suppress_cross_language_suffix_match(CBM_LANG_PYTHON,
                                                          "web/src/pages/Editor.js", "import_map"));
    /* JS/TS/TSX are one family. */
    ASSERT_FALSE(cbm_suppress_cross_language_suffix_match(CBM_LANG_JAVASCRIPT, "lib/util.ts",
                                                          "suffix_match"));
    ASSERT_FALSE(cbm_suppress_cross_language_suffix_match(CBM_LANG_TYPESCRIPT, "ui/Panel.tsx",
                                                          "suffix_match"));
    ASSERT_FALSE(cbm_suppress_cross_language_suffix_match(CBM_LANG_PYTHON, NULL, "suffix_match"));
    ASSERT_FALSE(
        cbm_suppress_cross_language_suffix_match(CBM_LANG_COUNT, "store.py", "suffix_match"));
    PASS();
}

TEST(cross_language_ref_drops_go_vs_c) {
    /* #1928: the USAGE/WRITES/READS analog of #725. Reference edges carry no
     * import-closure evidence, so EVERY registry strategy is a bare-name
     * guess and no strategy-level carve-out applies — the predicate takes no
     * strategy at all. */
    ASSERT_TRUE(cbm_suppress_cross_language_ref(CBM_LANG_GO, "bpf/probe.c"));
    ASSERT_TRUE(cbm_suppress_cross_language_ref(CBM_LANG_GO, "driver/mock.hpp"));
    ASSERT_TRUE(cbm_suppress_cross_language_ref(CBM_LANG_C, "pkg/events/event.go"));
    ASSERT_TRUE(cbm_suppress_cross_language_ref(CBM_LANG_PYTHON, "web/src/pages/Editor.js"));
    /* Same language → keep. */
    ASSERT_FALSE(cbm_suppress_cross_language_ref(CBM_LANG_GO, "pkg/state/state.go"));
    /* C and C++ are one family: .h maps to CBM_LANG_CPP, and a .c file
     * referencing its own header's declarations is not a boundary. */
    ASSERT_FALSE(cbm_suppress_cross_language_ref(CBM_LANG_C, "bpf/probe.h"));
    ASSERT_FALSE(cbm_suppress_cross_language_ref(CBM_LANG_CPP, "driver/compat.c"));
    /* …but Go into a header is still a boundary. */
    ASSERT_TRUE(cbm_suppress_cross_language_ref(CBM_LANG_GO, "bpf/probe.h"));
    /* JS/TS/TSX/ArkTS are one family. */
    ASSERT_FALSE(cbm_suppress_cross_language_ref(CBM_LANG_JAVASCRIPT, "lib/util.ts"));
    ASSERT_FALSE(cbm_suppress_cross_language_ref(CBM_LANG_TYPESCRIPT, "ui/Panel.tsx"));
    /* Unknown caller/target language or no path → nothing to judge → keep. */
    ASSERT_FALSE(cbm_suppress_cross_language_ref(CBM_LANG_COUNT, "store.py"));
    ASSERT_FALSE(cbm_suppress_cross_language_ref(CBM_LANG_GO, NULL));
    ASSERT_FALSE(cbm_suppress_cross_language_ref(CBM_LANG_GO, ""));
    ASSERT_FALSE(cbm_suppress_cross_language_ref(CBM_LANG_GO, "Makefile.inc.unknownext"));
    PASS();
}

TEST(go_bare_ref_never_binds_field) {
    /* #1942/#1962: a bare Go identifier can never denote a struct field —
     * field access is always a selector expression. The extractor strips the
     * receiver before the resolver runs (resolve_lhs_write_name writes the
     * trailing name; is_reference_node records the inner field_identifier),
     * so the selector-vs-bare distinction arrives as the recorded
     * is_member_access signal, never as a dot in the reference text. */
    ASSERT_TRUE(cbm_suppress_bare_field_ref(CBM_LANG_GO, false, "Field"));
    /* The member half of a selector may bind a field. */
    ASSERT_FALSE(cbm_suppress_bare_field_ref(CBM_LANG_GO, true, "Field"));
    /* Bare references to non-fields are untouched. */
    ASSERT_FALSE(cbm_suppress_bare_field_ref(CBM_LANG_GO, false, "Variable"));
    ASSERT_FALSE(cbm_suppress_bare_field_ref(CBM_LANG_GO, false, "Function"));
    /* Other languages reference their own members bare inside methods —
     * never suppressed (cp_reads_writes_cs_static_field pins the C# shape). */
    ASSERT_FALSE(cbm_suppress_bare_field_ref(CBM_LANG_CSHARP, false, "Field"));
    ASSERT_FALSE(cbm_suppress_bare_field_ref(CBM_LANG_JAVA, false, "Field"));
    /* Degenerate input → nothing to judge. */
    ASSERT_FALSE(cbm_suppress_bare_field_ref(CBM_LANG_GO, false, NULL));
    PASS();
}

TEST(c_bare_ref_never_binds_field) {
    /* #2666: a C struct member is named only as the member half of `a.b`,
     * `a->b` or a designator `.b` (field_identifier → is_member_access). A
     * bare C identifier — a macro argument like RUN_SELECTED_SUITE(mcp) — is
     * never one, so it must not bind a same-named Field. */
    ASSERT_TRUE(cbm_suppress_bare_field_ref(CBM_LANG_C, false, "Field"));
    /* The member half still binds (through the C owner rows upstream). */
    ASSERT_FALSE(cbm_suppress_bare_field_ref(CBM_LANG_C, true, "Field"));
    /* A bare C name keeps binding globals, functions and types. */
    ASSERT_FALSE(cbm_suppress_bare_field_ref(CBM_LANG_C, false, "Variable"));
    ASSERT_FALSE(cbm_suppress_bare_field_ref(CBM_LANG_C, false, "Function"));
    ASSERT_FALSE(cbm_suppress_bare_field_ref(CBM_LANG_C, false, "Type"));
    /* C++ (and every `.h`, parsed as C++): a method body names its own
     * members bare, so the bind is kept. */
    ASSERT_FALSE(cbm_suppress_bare_field_ref(CBM_LANG_CPP, false, "Field"));
    PASS();
}

TEST(c_bare_call_does_not_weigh_field_namesakes) {
    /* 2026-10-13 kernel audit: a struct member named like a header function
     * made the bare name two candidates, the answer a suffix_match, and the
     * #725 guard (.h is C++) refused it -- `put_page(page)` 345 -> 9 callers.
     * A bare C call never names a member, so its Field namesakes do not
     * compete: the function is the unique name it was in v0.11.0. */
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add_lang(r, "put_page", "proj.include.linux.mm.put_page", "Function",
                          CBM_LANG_CPP);
    cbm_registry_add_lang(r, "put_page",
                          "proj.arch.arm64.include.asm.kvm_pgtable.kvm_pgtable_mm_ops.put_page",
                          "Field", CBM_LANG_CPP);
    const char *mod = "proj.arch.arm64.kernel.elfcore";
    cbm_resolution_t res =
        cbm_registry_resolve_call_lang(r, "put_page", mod, NULL, NULL, 0, CBM_LANG_C);
    ASSERT_STR_EQ(res.qualified_name, "proj.include.linux.mm.put_page");
    ASSERT_STR_EQ(res.strategy, "unique_name");
    /* The reference resolver and C++ (which reaches its own members bare) still
     * weigh the member: two candidates. */
    res = cbm_registry_resolve_lang(r, "put_page", mod, NULL, NULL, 0, CBM_LANG_C);
    ASSERT_STR_EQ(res.strategy, "suffix_match");
    res = cbm_registry_resolve_call_lang(r, "put_page", mod, NULL, NULL, 0, CBM_LANG_CPP);
    ASSERT_STR_EQ(res.strategy, "suffix_match");
    /* A name only members carry is no bare C call target at all -- neither a
     * struct member nor a C++ method: redis's libc `read()` became the unique
     * name of a Qt adapter's `read` Method once only Fields were set aside. */
    cbm_registry_add_lang(r, "on_close", "proj.net.sock.sock_ops.on_close", "Field", CBM_LANG_C);
    res = cbm_registry_resolve_call_lang(r, "on_close", mod, NULL, NULL, 0, CBM_LANG_C);
    ASSERT_TRUE(res.qualified_name == NULL || res.qualified_name[0] == '\0');
    cbm_registry_add_lang(r, "read", "proj.deps.hiredis.adapters.qt.RedisQtAdapter.read", "Method",
                          CBM_LANG_CPP);
    cbm_registry_add_lang(r, "read", "proj.src.connection.ConnectionType.read", "Field",
                          CBM_LANG_C);
    res = cbm_registry_resolve_call_lang(r, "read", mod, NULL, NULL, 0, CBM_LANG_C);
    ASSERT_TRUE(res.qualified_name == NULL || res.qualified_name[0] == '\0');
    /* C++ still reaches the method by name. */
    res = cbm_registry_resolve_call_lang(r, "read", mod, NULL, NULL, 0, CBM_LANG_CPP);
    ASSERT_TRUE(res.qualified_name && res.qualified_name[0] != '\0');
    cbm_registry_free(r);
    PASS();
}

TEST(call_onto_field_follows_language_and_shape) {
    /* A call the registry resolved onto a Field node. */
    /* Another language: only the spelling connects them. */
    ASSERT_EQ(cbm_call_onto_field_policy(CBM_LANG_BASH, "command", "Field", "src/probe.h"),
              CBM_FIELD_CALL_DROP);
    ASSERT_EQ(cbm_call_onto_field_policy(CBM_LANG_PYTHON, "sorted", "Field", "src/pool.c"),
              CBM_FIELD_CALL_DROP);
    /* A bare C call is never a struct member. */
    ASSERT_EQ(cbm_call_onto_field_policy(CBM_LANG_C, "socket", "Field", "tests/t.c"),
              CBM_FIELD_CALL_DROP);
    /* A C member call is decided by its object's type, in a .c or a .h. */
    ASSERT_EQ(cbm_call_onto_field_policy(CBM_LANG_C, "cb.close", "Field", "src/cb.h"),
              CBM_FIELD_CALL_BY_OWNER);
    ASSERT_EQ(cbm_call_onto_field_policy(CBM_LANG_C, "w->svc->app.cancel", "Field", "src/a.c"),
              CBM_FIELD_CALL_BY_OWNER);
    /* C++ reaches its own members bare, so a bare call keeps its resolution;
     * a member call is typed like C's (`text.size()` is not some struct's
     * `size` member). */
    ASSERT_EQ(cbm_call_onto_field_policy(CBM_LANG_CPP, "handler", "Field", "src/a.h"),
              CBM_FIELD_CALL_KEEP);
    ASSERT_EQ(cbm_call_onto_field_policy(CBM_LANG_CPP, "text.size", "Field", "src/a.h"),
              CBM_FIELD_CALL_BY_OWNER);
    /* The same language as the Field keeps its resolution too. */
    ASSERT_EQ(cbm_call_onto_field_policy(CBM_LANG_GO, "s.fn", "Field", "pkg/s.go"),
              CBM_FIELD_CALL_KEEP);
    /* Not a Field target: not this rule's business. */
    ASSERT_EQ(cbm_call_onto_field_policy(CBM_LANG_BASH, "main", "Function", "src/main.c"),
              CBM_FIELD_CALL_KEEP);
    ASSERT_EQ(cbm_call_onto_field_policy(CBM_LANG_C, "socket", NULL, "tests/t.c"),
              CBM_FIELD_CALL_KEEP);
    PASS();
}

TEST(c_arrow_member_call_is_c_family_and_arrow_only) {
    /* The registry returns nothing for an arrow callee; these are the calls
     * the resolvers then hand to the object's type. */
    ASSERT_TRUE(cbm_c_arrow_member_call(CBM_LANG_C, "o->open"));
    ASSERT_TRUE(cbm_c_arrow_member_call(CBM_LANG_C, "d->ops->shut"));
    ASSERT_TRUE(cbm_c_arrow_member_call(CBM_LANG_CPP, "self->hook"));
    /* A dot call resolves through the registry and its Field policy. */
    ASSERT_FALSE(cbm_c_arrow_member_call(CBM_LANG_C, "cb.close"));
    ASSERT_FALSE(cbm_c_arrow_member_call(CBM_LANG_C, "socket"));
    ASSERT_FALSE(cbm_c_arrow_member_call(CBM_LANG_C, NULL));
    /* Perl and PHP spell method calls with an arrow too: not this rule. */
    ASSERT_FALSE(cbm_c_arrow_member_call(CBM_LANG_PERL, "$obj->method"));
    ASSERT_FALSE(cbm_c_arrow_member_call(CBM_LANG_PHP, "$this->save"));
    PASS();
}

TEST(unique_field_qn_needs_exactly_one_field_of_that_name) {
    /* What an untyped C member access may still bind: a name exactly one
     * Field carries. Two Fields would be a choice; a function or variable of
     * the same name is not a candidate for a member at all. */
    cbm_registry_t *r = cbm_registry_new();
    ASSERT_NOT_NULL(r);
    cbm_registry_add(r, "only_here", "proj.a.Alpha.only_here", "Field");
    cbm_registry_add(r, "count", "proj.a.Alpha.count", "Field");
    cbm_registry_add(r, "count", "proj.b.Beta.count", "Field");
    cbm_registry_add(r, "close", "proj.cb.Callbacks.close", "Field");
    cbm_registry_add(r, "close", "proj.io.close", "Function");
    cbm_registry_add(r, "lonely_fn", "proj.io.lonely_fn", "Function");

    const char *only = cbm_registry_unique_field_qn(r, "only_here");
    ASSERT_NOT_NULL(only);
    ASSERT_STR_EQ(only, "proj.a.Alpha.only_here");
    /* Two fields share the name: no pick. */
    ASSERT_NULL(cbm_registry_unique_field_qn(r, "count"));
    /* One field and one function share the name: the function is not a
     * candidate for a member, so the field is still the only one. */
    const char *member = cbm_registry_unique_field_qn(r, "close");
    ASSERT_NOT_NULL(member);
    ASSERT_STR_EQ(member, "proj.cb.Callbacks.close");
    /* Unique, but not a field. */
    ASSERT_NULL(cbm_registry_unique_field_qn(r, "lonely_fn"));
    /* Unknown name and degenerate input. */
    ASSERT_NULL(cbm_registry_unique_field_qn(r, "absent"));
    ASSERT_NULL(cbm_registry_unique_field_qn(r, NULL));
    ASSERT_NULL(cbm_registry_unique_field_qn(NULL, "count"));
    cbm_registry_free(r);
    PASS();
}

TEST(c_member_access_binds_by_owner_only) {
    /* The member half of a C `a.b` / `a->b` means nothing without the type of
     * `a`: it binds through the C LSP's field-owner rows and never through a
     * bare-name lookup. The selector shape arrives as is_member_access. */
    ASSERT_TRUE(cbm_c_member_binds_by_owner(true, true));
    /* A bare C identifier keeps the ordinary resolution. */
    ASSERT_FALSE(cbm_c_member_binds_by_owner(true, false));
    /* Other languages keep theirs: a C++ or Objective-C method body reaches its
     * own members without a selector, and Go has its own rule above. */
    ASSERT_FALSE(cbm_c_member_binds_by_owner(false, true));
    ASSERT_FALSE(cbm_c_member_binds_by_owner(false, false));
    PASS();
}

TEST(c_member_rule_covers_c_files_and_every_dot_h) {
    ASSERT_TRUE(cbm_c_member_rule_file(CBM_LANG_C, "src/a.c"));
    ASSERT_TRUE(cbm_c_member_rule_file(CBM_LANG_C, NULL));
    /* A header is classified C++ whatever it holds; its inline functions
     * access members like the .c files that include it. */
    ASSERT_TRUE(cbm_c_member_rule_file(CBM_LANG_CPP, "include/linux/list.h"));
    /* Other C++ files keep their own resolution. */
    ASSERT_FALSE(cbm_c_member_rule_file(CBM_LANG_CPP, "src/widget.cpp"));
    ASSERT_FALSE(cbm_c_member_rule_file(CBM_LANG_CPP, "src/widget.hpp"));
    ASSERT_FALSE(cbm_c_member_rule_file(CBM_LANG_CPP, "src/match.hh"));
    ASSERT_FALSE(cbm_c_member_rule_file(CBM_LANG_CPP, ".h"));
    ASSERT_FALSE(cbm_c_member_rule_file(CBM_LANG_CPP, NULL));
    /* No other language, whatever the file is called. */
    ASSERT_FALSE(cbm_c_member_rule_file(CBM_LANG_OBJC, "ui/View.h"));
    ASSERT_FALSE(cbm_c_member_rule_file(CBM_LANG_GO, "pkg/a.go"));
    PASS();
}

TEST(java_call_never_binds_data_member) {
    /* A Java call expression never names data, so a CALLS bind that landed on
     * a Variable or a Field is a spelling collision. The shape that produces
     * it: a record's accessors are implicit, `p.x()` finds no declared `x()`,
     * and the short-name registry hands back the first `x` in the tree — an
     * unrelated class's private field. */
    ASSERT_TRUE(cbm_java_suppress_call_to_data_member(true, "Field"));
    ASSERT_TRUE(cbm_java_suppress_call_to_data_member(true, "Variable"));
    /* Everything callable is kept, including the constructor's Class target. */
    ASSERT_FALSE(cbm_java_suppress_call_to_data_member(true, "Method"));
    ASSERT_FALSE(cbm_java_suppress_call_to_data_member(true, "Function"));
    ASSERT_FALSE(cbm_java_suppress_call_to_data_member(true, "Class"));
    /* Other languages have callable data: a Kotlin property or a C function
     * pointer is a name that is not a method and is called all the same. */
    ASSERT_FALSE(cbm_java_suppress_call_to_data_member(false, "Field"));
    ASSERT_FALSE(cbm_java_suppress_call_to_data_member(false, "Variable"));
    /* Degenerate input → nothing to judge. */
    ASSERT_FALSE(cbm_java_suppress_call_to_data_member(true, NULL));
    PASS();
}

TEST(generated_member_registers_by_qn_only) {
    /* A generated member (a Java record's implicit accessor) is found by its
     * qualified name, never by a short-name guess. The same member registered
     * the ordinary way is exactly what the guess binds. */
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add_qn_only(r, "proj.geo.Point.Point.y", "Method");
    ASSERT_TRUE(cbm_registry_exists(r, "proj.geo.Point.Point.y"));
    ASSERT_STR_EQ(cbm_registry_label_of(r, "proj.geo.Point.Point.y"), "Method");
    const char **qns = NULL;
    int n = -1;
    ASSERT_EQ(cbm_registry_find_by_name(r, "y", &qns, &n), 0);
    ASSERT_EQ(n, 0);
    cbm_resolution_t res = cbm_registry_resolve(r, "v.y", "proj.app.Caller", NULL, NULL, 0);
    ASSERT_NULL(res.qualified_name);
    cbm_registry_add(r, "x", "proj.geo.Point.Point.x", "Method");
    res = cbm_registry_resolve(r, "v.x", "proj.app.Caller", NULL, NULL, 0);
    ASSERT_STR_EQ(res.qualified_name, "proj.geo.Point.Point.x");
    /* Degenerate input registers nothing. */
    cbm_registry_add_qn_only(r, NULL, "Method");
    cbm_registry_add_qn_only(r, "proj.geo.Point.Point.z", NULL);
    cbm_registry_add_qn_only(NULL, "proj.geo.Point.Point.z", "Method");
    ASSERT_EQ(cbm_registry_size(r), 2);
    cbm_registry_free(r);
    PASS();
}

TEST(generated_member_mark_on_defs_and_nodes) {
    static const char *const record[] = {"generated", "record", NULL};
    static const char *const later[] = {"source", "openapi", "generated", "lombok", NULL};
    static const char *const doc[] = {"id", "ADR-7", "status", "accepted", NULL};
    static const char *const keyless[] = {"generated", NULL};
    ASSERT_TRUE(cbm_def_props_generated(record));
    ASSERT_TRUE(cbm_def_props_generated(later));
    ASSERT_FALSE(cbm_def_props_generated(doc));
    ASSERT_FALSE(cbm_def_props_generated(keyless)); /* a key without a value is no fact */
    ASSERT_FALSE(cbm_def_props_generated(NULL));
    ASSERT_TRUE(
        cbm_node_props_generated("{\"parent_class\":\"p.Point\",\"generated\":\"record\"}"));
    ASSERT_FALSE(cbm_node_props_generated("{\"parent_class\":\"p.Point\"}"));
    /* The word inside a value is not the key. */
    ASSERT_FALSE(cbm_node_props_generated("{\"docstring\":\"code generated by protoc\"}"));
    ASSERT_FALSE(cbm_node_props_generated(NULL));
    PASS();
}

TEST(dynamic_suppress_drops_weak_method_matches) {
    /* #592/#606/#1276: a member call whose receiver the LSP could not type, that
     * landed via a WEAK short-name strategy, is generic-resolver noise → drop.
     * The strategies that actually reach the guards are the registry's
     * suffix_match / unique_name and the parallel field_type_hint; "fuzzy" is
     * covered defensively (cbm_registry_fuzzy_resolve is not wired into the
     * resolvers today) so a future wiring cannot silently reintroduce it. */
    ASSERT_TRUE(cbm_suppress_weak_member_match(true, true, "suffix_match"));
    ASSERT_TRUE(cbm_suppress_weak_member_match(true, true, "unique_name"));
    ASSERT_TRUE(cbm_suppress_weak_member_match(true, true, "field_type_hint"));
    ASSERT_TRUE(cbm_suppress_weak_member_match(true, true, "fuzzy"));
    PASS();
}

TEST(dynamic_suppress_keeps_high_confidence_and_non_methods) {
    /* Keep every receiver-/import-aware strategy. Because the PARALLEL resolver
     * runs lsp_* strategies through this same guard variable, an explicit
     * drop-list must never touch them — asserting the lsp_* keeps here is the
     * regression guard for the "kills lsp edges" failure mode. The keep set
     * enumerates the resolver's non-weak strategies: registry
     * {import_map, import_map_suffix, same_module, qualified_suffix}, parallel
     * {callee_suffix, service_pattern}, and lsp_*. */
    ASSERT_FALSE(cbm_suppress_weak_member_match(true, true, "same_module"));
    ASSERT_FALSE(cbm_suppress_weak_member_match(true, true, "import_map"));
    ASSERT_FALSE(cbm_suppress_weak_member_match(true, true, "import_map_suffix"));
    ASSERT_FALSE(cbm_suppress_weak_member_match(true, true, "qualified_suffix"));
    ASSERT_FALSE(cbm_suppress_weak_member_match(true, true, "callee_suffix"));
    ASSERT_FALSE(cbm_suppress_weak_member_match(true, true, "service_pattern"));
    ASSERT_FALSE(cbm_suppress_weak_member_match(true, true, "lsp_ts_method"));
    ASSERT_FALSE(cbm_suppress_weak_member_match(true, true, "lsp_cross"));
    ASSERT_FALSE(cbm_suppress_weak_member_match(true, true, "lsp_ts_local"));
    /* A bare call (is_method=false) is a free-function call → never suppressed. */
    ASSERT_FALSE(cbm_suppress_weak_member_match(true, false, "unique_name"));
    ASSERT_FALSE(cbm_suppress_weak_member_match(true, false, "suffix_match"));
    /* Languages outside the caller's guard set are never affected. */
    ASSERT_FALSE(cbm_suppress_weak_member_match(false, true, "suffix_match"));
    /* No match (NULL/empty strategy) → nothing to suppress. */
    ASSERT_FALSE(cbm_suppress_weak_member_match(true, true, NULL));
    ASSERT_FALSE(cbm_suppress_weak_member_match(true, true, ""));
    PASS();
}

TEST(python_builtin_member_table_matches_builtin_type_methods) {
    /* Ends and middle of the sorted table, so a mis-sorted insert shows up. */
    ASSERT_TRUE(cbm_python_is_builtin_member("add"));
    ASSERT_TRUE(cbm_python_is_builtin_member("extend"));
    ASSERT_TRUE(cbm_python_is_builtin_member("items"));
    ASSERT_TRUE(cbm_python_is_builtin_member("print"));
    ASSERT_TRUE(cbm_python_is_builtin_member("startswith"));
    ASSERT_TRUE(cbm_python_is_builtin_member("zfill"));
    /* Project-specific spellings are not builtin members. */
    ASSERT_FALSE(cbm_python_is_builtin_member("apply_converters"));
    ASSERT_FALSE(cbm_python_is_builtin_member("lazy_model_operation"));
    ASSERT_FALSE(cbm_python_is_builtin_member("describe"));
    ASSERT_FALSE(cbm_python_is_builtin_member(NULL));
    ASSERT_FALSE(cbm_python_is_builtin_member(""));
    PASS();
}

TEST(weak_member_unique_name_exempt_is_python_self_rooted_unique_and_specific_only) {
    /* The exemption: Python, self/cls-rooted receiver, unique_name, callee not
     * a builtin member. */
    ASSERT_TRUE(cbm_weak_member_unique_name_exempt(true, true, "self.compiler.apply_converters",
                                                   "unique_name"));
    ASSERT_TRUE(cbm_weak_member_unique_name_exempt(true, true, "cls.registry.lazy_model_operation",
                                                   "unique_name"));
    /* A bare parameter/local receiver carries no ownership evidence: #1276's
     * accelerator.backward() and c.describe() stay suppressed. */
    ASSERT_FALSE(
        cbm_weak_member_unique_name_exempt(true, false, "accelerator.backward", "unique_name"));
    /* A builtin type's own method stays suppressed even on self. */
    ASSERT_FALSE(
        cbm_weak_member_unique_name_exempt(true, true, "self.parts.extend", "unique_name"));
    ASSERT_FALSE(cbm_weak_member_unique_name_exempt(true, true, "self.out.print", "unique_name"));
    /* Only unique_name carries the one-definition evidence. */
    ASSERT_FALSE(cbm_weak_member_unique_name_exempt(true, true, "self.compiler.apply_converters",
                                                    "suffix_match"));
    ASSERT_FALSE(cbm_weak_member_unique_name_exempt(true, true, "self.compiler.apply_converters",
                                                    "field_type_hint"));
    /* Python only: the JS/TS family keeps its recorded trade. */
    ASSERT_FALSE(
        cbm_weak_member_unique_name_exempt(false, true, "this.compiler.apply", "unique_name"));
    /* Degenerate inputs never exempt. */
    ASSERT_FALSE(cbm_weak_member_unique_name_exempt(true, true, NULL, "unique_name"));
    ASSERT_FALSE(cbm_weak_member_unique_name_exempt(true, true, "", "unique_name"));
    ASSERT_FALSE(cbm_weak_member_unique_name_exempt(true, true, "self.x.y", NULL));
    PASS();
}

TEST(local_binding_suppress_drops_weak_shadowed_bare_calls) {
    /* A bare `run()` whose callee is a parameter of an enclosing scope cannot be
     * the module-level `run`, so a weak short-name match fabricates the edge. */
    ASSERT_TRUE(cbm_suppress_weak_local_binding_call(true, true, "suffix_match"));
    ASSERT_TRUE(cbm_suppress_weak_local_binding_call(true, true, "unique_name"));
    ASSERT_TRUE(cbm_suppress_weak_local_binding_call(true, true, "field_type_hint"));
    ASSERT_TRUE(cbm_suppress_weak_local_binding_call(true, true, "fuzzy"));
    PASS();
}

TEST(local_binding_suppress_keeps_unshadowed_and_strong_strategies) {
    /* THE RECALL PIN. A bare call to a genuine module-level function is NOT
     * locally bound, so it is never suppressed — whatever the callee is spelled.
     * This is the assertion a name-keyed guard (get/run/execute) would fail: it
     * would drop these purely because of how the callee reads. */
    ASSERT_FALSE(cbm_suppress_weak_local_binding_call(true, false, "suffix_match"));
    ASSERT_FALSE(cbm_suppress_weak_local_binding_call(true, false, "unique_name"));
    ASSERT_FALSE(cbm_suppress_weak_local_binding_call(true, false, "field_type_hint"));
    ASSERT_FALSE(cbm_suppress_weak_local_binding_call(true, false, "fuzzy"));
    /* Every receiver-/import-aware strategy is kept even when shadowed. */
    ASSERT_FALSE(cbm_suppress_weak_local_binding_call(true, true, "same_module"));
    ASSERT_FALSE(cbm_suppress_weak_local_binding_call(true, true, "import_map"));
    ASSERT_FALSE(cbm_suppress_weak_local_binding_call(true, true, "import_map_suffix"));
    ASSERT_FALSE(cbm_suppress_weak_local_binding_call(true, true, "qualified_suffix"));
    ASSERT_FALSE(cbm_suppress_weak_local_binding_call(true, true, "callee_suffix"));
    ASSERT_FALSE(cbm_suppress_weak_local_binding_call(true, true, "service_pattern"));
    ASSERT_FALSE(cbm_suppress_weak_local_binding_call(true, true, "lsp_cross"));
    ASSERT_FALSE(cbm_suppress_weak_local_binding_call(true, true, "lsp_py_method"));
    ASSERT_FALSE(cbm_suppress_weak_local_binding_call(true, true, "lsp_direct"));
    /* Languages outside the caller's gate are never affected. */
    ASSERT_FALSE(cbm_suppress_weak_local_binding_call(false, true, "suffix_match"));
    /* No match (NULL/empty strategy) → nothing to suppress. */
    ASSERT_FALSE(cbm_suppress_weak_local_binding_call(true, true, NULL));
    ASSERT_FALSE(cbm_suppress_weak_local_binding_call(true, true, ""));
    PASS();
}

/* #2127: the import-binding guard drops only weak strategies, and only when the
 * caller established that the file's import contradicts the target. */
TEST(import_binding_suppress_drops_only_weak_contradicted_calls) {
    ASSERT_TRUE(cbm_suppress_weak_import_bound_call(true, true, "unique_name"));
    ASSERT_TRUE(cbm_suppress_weak_import_bound_call(true, true, "suffix_match"));
    ASSERT_TRUE(cbm_suppress_weak_import_bound_call(true, true, "fuzzy"));
    ASSERT_FALSE(cbm_suppress_weak_import_bound_call(true, false, "unique_name"));
    ASSERT_FALSE(cbm_suppress_weak_import_bound_call(false, true, "unique_name"));
    ASSERT_FALSE(cbm_suppress_weak_import_bound_call(true, true, "import_map"));
    ASSERT_FALSE(cbm_suppress_weak_import_bound_call(true, true, "same_module"));
    ASSERT_FALSE(cbm_suppress_weak_import_bound_call(true, true, "lsp_py_method"));
    ASSERT_FALSE(cbm_suppress_weak_import_bound_call(true, true, NULL));
    PASS();
}

/* #2127: when does a Python import binding contradict a resolved target? */
TEST(python_import_binding_contradicts_only_foreign_chains) {
    CBMImport items[] = {
        {.local_name = "patch", .module_path = "unittest.mock.patch"},
        {.local_name = "f", .module_path = "pkg.f"},
        {.local_name = "app", .module_path = "app.util"},
        {.local_name = "h", .module_path = "app.util.helper"},
        {.local_name = "rel", .module_path = ".views.rel"},
        {.local_name = "mock", .module_path = "unittest.mock"},
        {.local_name = "Author", .module_path = ".models.Author"},
        {.local_name = "copy", .module_path = "copy.copy"},
    };
    CBMImportArray imps = {.items = items, .count = 8, .cap = 8};
    /* `from copy import copy` is a from-import (leaf == local), not a root
     * package import: its chain `copy` must appear in the target. */
    ASSERT_TRUE(cbm_python_import_binding_contradicts(
        &imps, "copy", "proj.forms.utils.ErrorList.copy", NULL, NULL, NULL));
    /* Member call through an external module import: foreign → dropped. */
    ASSERT_TRUE(cbm_python_import_binding_contradicts(
        &imps, "mock.patch", "proj.views.generic.RedirectView.patch", NULL, NULL, NULL));
    /* Member reached THROUGH an imported class may live on any type: only the
     * import's own chain (`models`) is compared, so the manager call stays. */
    ASSERT_FALSE(cbm_python_import_binding_contradicts(
        &imps, "Author.objects.create", "proj.db.models.query.QuerySet.create", NULL, NULL, NULL));
    /* The report: external mock.patch must not be a project REST handler. */
    ASSERT_TRUE(cbm_python_import_binding_contradicts(
        &imps, "patch", "proj.app.views.PkgConfigView.patch", NULL, NULL, NULL));
    /* Re-export / src layout: the chain `pkg` is present → keep. */
    ASSERT_FALSE(
        cbm_python_import_binding_contradicts(&imps, "f", "proj.src.pkg.core.f", NULL, NULL, NULL));
    /* `import app.util` binds the root package; the callee spells its path. */
    ASSERT_FALSE(cbm_python_import_binding_contradicts(&imps, "app.util.helper",
                                                       "proj.app.util.helper", NULL, NULL, NULL));
    /* Aliased from-import: consistent target kept, foreign target dropped. */
    ASSERT_FALSE(cbm_python_import_binding_contradicts(&imps, "h", "proj.app.util.helper", NULL,
                                                       NULL, NULL));
    ASSERT_TRUE(
        cbm_python_import_binding_contradicts(&imps, "h", "proj.other.helper", NULL, NULL, NULL));
    /* Relative import: leading dots carry no segment; `views` must appear. */
    ASSERT_FALSE(cbm_python_import_binding_contradicts(&imps, "rel", "proj.app.views.rel", NULL,
                                                       NULL, NULL));
    ASSERT_TRUE(cbm_python_import_binding_contradicts(&imps, "rel", "proj.app.models.rel", NULL,
                                                      NULL, NULL));
    /* Not import-bound → never a contradiction (the recall pin). */
    ASSERT_FALSE(
        cbm_python_import_binding_contradicts(&imps, "helper", "proj.x.helper", NULL, NULL, NULL));
    ASSERT_FALSE(
        cbm_python_import_binding_contradicts(NULL, "patch", "proj.x.patch", NULL, NULL, NULL));
    PASS();
}

/* Base-class twin: the django shapes. Every spelling of the stdlib base must
 * contradict the same-named project class; project bases never do, and the
 * gate is Python-only. */
TEST(python_external_base_contradicts_django_shapes) {
    CBMImport items[] = {
        {.local_name = "unittest", .module_path = "unittest"},
        {.local_name = "ut", .module_path = "unittest"},
        {.local_name = "TestCase", .module_path = "unittest.TestCase"},
        {.local_name = "pkg", .module_path = "pkg.test.models"},
    };
    CBMImportArray imps = {.items = items, .count = 4, .cap = 4};
    const char *project_tc = "proj.django.test.testcases.TestCase";
    ASSERT_TRUE(cbm_python_external_base_contradicts(CBM_LANG_PYTHON, &imps, "unittest.TestCase",
                                                     project_tc, NULL, NULL, NULL));
    ASSERT_TRUE(cbm_python_external_base_contradicts(CBM_LANG_PYTHON, &imps, "TestCase", project_tc,
                                                     NULL, NULL, NULL));
    /* `import unittest as ut`: `ut.TestCase` spells `unittest.TestCase`. */
    ASSERT_TRUE(cbm_python_external_base_contradicts(CBM_LANG_PYTHON, &imps, "ut.TestCase",
                                                     project_tc, NULL, NULL, NULL));
    /* A target that does spell the chain (vendored/src-layout unittest). */
    ASSERT_FALSE(cbm_python_external_base_contradicts(
        CBM_LANG_PYTHON, &imps, "ut.TestCase", "proj.src.unittest.TestCase", NULL, NULL, NULL));
    /* Dotted project base through `import pkg.test.models`. */
    ASSERT_FALSE(
        cbm_python_external_base_contradicts(CBM_LANG_PYTHON, &imps, "pkg.test.models.Base",
                                             "proj.pkg.test.models.Base", NULL, NULL, NULL));
    /* Not import-bound: a same-module base is never judged. */
    ASSERT_FALSE(cbm_python_external_base_contradicts(CBM_LANG_PYTHON, &imps, "SimpleTestCase",
                                                      "proj.django.test.testcases.SimpleTestCase",
                                                      NULL, NULL, NULL));
    /* Python only. */
    ASSERT_FALSE(cbm_python_external_base_contradicts(CBM_LANG_JAVA, &imps, "unittest.TestCase",
                                                      project_tc, NULL, NULL, NULL));
    PASS();
}

TEST(weak_call_guards_share_one_drop_list) {
    /* The member guard and the local-binding guard must agree on what "weak"
     * means. They share a single static predicate for exactly this reason; if
     * someone re-inlines one of the lists and edits only that copy, the two
     * guards start disagreeing and this test catches it at the contract level
     * rather than in a corpus months later. */
    static const char *const strategies[] = {"suffix_match",
                                             "unique_name",
                                             "field_type_hint",
                                             "fuzzy",
                                             "same_module",
                                             "import_map",
                                             "import_map_suffix",
                                             "qualified_suffix",
                                             "callee_suffix",
                                             "service_pattern",
                                             "lsp_cross",
                                             "lsp_ts_method",
                                             "lsp_py_method",
                                             "lsp_direct",
                                             "",
                                             NULL};
    for (int i = 0; strategies[i] != NULL; i++) {
        bool member = cbm_suppress_weak_member_match(true, true, strategies[i]);
        bool binding = cbm_suppress_weak_local_binding_call(true, true, strategies[i]);
        if (member != binding) {
            printf("  drop-list divergence on strategy \"%s\": member=%d binding=%d\n",
                   strategies[i], member, binding);
        }
        ASSERT_EQ(member, binding);
        /* The Go selector guard builds on the same list. At a penalized
         * confidence (0.75 * 0.5) it agrees with it exactly; its one narrowing
         * (an unpenalized unique_name) does not apply here. Any drift from
         * the shared list fails here. */
        bool go = cbm_go_suppress_weak_method_match(true, true, strategies[i], 0.375);
        bool go_expected = member;
        if (go != go_expected) {
            printf("  Go drop-list divergence on strategy \"%s\": go=%d expected=%d\n",
                   strategies[i], go, go_expected);
        }
        ASSERT_EQ(go, go_expected);
    }
    PASS();
}

TEST(external_import_shadow_drops_package_bound_bare_call) {
    /* #1355: `import { eq, sql } from "drizzle-orm"` binds both names to a
     * package outside the indexed tree, so no import-map key exists for them and
     * a project-wide same-name guess is a fabricated edge. */
    CBMImport imports[] = {
        {.local_name = "eq", .module_path = "drizzle-orm"},
        {.local_name = "sql", .module_path = "drizzle-orm"},
        {.local_name = "users", .module_path = "./schema"},
    };
    CBMImportArray arr = {.items = imports, .count = 3, .cap = 3};
    /* Only the relative import materialized an IMPORTS edge. */
    const char *keys[] = {"users"};

    ASSERT_TRUE(
        cbm_suppress_external_import_shadow("eq", "unique_name", &arr, keys, 1, NULL, NULL));
    ASSERT_TRUE(
        cbm_suppress_external_import_shadow("sql", "unique_name", &arr, keys, 1, NULL, NULL));
    ASSERT_TRUE(
        cbm_suppress_external_import_shadow("eq", "suffix_match", &arr, keys, 1, NULL, NULL));
    ASSERT_TRUE(
        cbm_suppress_external_import_shadow("eq", "field_type_hint", &arr, keys, 1, NULL, NULL));
    ASSERT_TRUE(cbm_suppress_external_import_shadow("eq", "fuzzy", &arr, keys, 1, NULL, NULL));
    /* A file whose imports all failed to materialize is still covered: the
     * evidence is the import statement, not the map. */
    ASSERT_TRUE(
        cbm_suppress_external_import_shadow("eq", "unique_name", &arr, NULL, 0, NULL, NULL));
    PASS();
}

TEST(external_import_shadow_keeps_everything_else) {
    /* Negative space: every input that is NOT "bare call bound to a package
     * import the graph could not resolve" must keep its edge. */
    CBMImport imports[] = {
        {.local_name = "eq", .module_path = "drizzle-orm"},
        {.local_name = "normalize", .module_path = "./text-utils"},
        {.local_name = "getRootContainer", .module_path = "@repo/shared"},
    };
    CBMImportArray arr = {.items = imports, .count = 3, .cap = 3};
    /* The relative import and the workspace package both materialized. */
    const char *keys[] = {"normalize", "getRootContainer"};
    const int nkeys = 2;

    /* Import-/receiver-/module-aware strategies are never this guard's business. */
    ASSERT_FALSE(
        cbm_suppress_external_import_shadow("eq", "import_map", &arr, keys, nkeys, NULL, NULL));
    ASSERT_FALSE(cbm_suppress_external_import_shadow("eq", "import_map_suffix", &arr, keys, nkeys,
                                                     NULL, NULL));
    ASSERT_FALSE(
        cbm_suppress_external_import_shadow("eq", "same_module", &arr, keys, nkeys, NULL, NULL));
    ASSERT_FALSE(cbm_suppress_external_import_shadow("eq", "qualified_suffix", &arr, keys, nkeys,
                                                     NULL, NULL));
    ASSERT_FALSE(
        cbm_suppress_external_import_shadow("eq", "lsp_ts_import", &arr, keys, nkeys, NULL, NULL));
    ASSERT_FALSE(
        cbm_suppress_external_import_shadow("eq", "lsp_ts_method", &arr, keys, nkeys, NULL, NULL));
    ASSERT_FALSE(cbm_suppress_external_import_shadow("eq", "service_pattern", &arr, keys, nkeys,
                                                     NULL, NULL));
    /* A relative specifier names a path inside the tree — a missing IMPORTS edge
     * there is an in-project gap, not an external binding. */
    ASSERT_FALSE(cbm_suppress_external_import_shadow("normalize", "unique_name", &arr, keys, nkeys,
                                                     NULL, NULL));
    ASSERT_FALSE(
        cbm_suppress_external_import_shadow("normalize", "unique_name", &arr, NULL, 0, NULL, NULL));
    /* A package the import map DOES bind resolved in-graph (workspace package). */
    ASSERT_FALSE(cbm_suppress_external_import_shadow("getRootContainer", "unique_name", &arr, keys,
                                                     nkeys, NULL, NULL));
    /* A name the file never imports (called with no import at all). */
    ASSERT_FALSE(cbm_suppress_external_import_shadow("helper", "unique_name", &arr, keys, nkeys,
                                                     NULL, NULL));
    /* Member and package/namespace-qualified callees belong to the
     * receiver-aware guards, not to this one. */
    ASSERT_FALSE(cbm_suppress_external_import_shadow("eq.apply", "unique_name", &arr, keys, nkeys,
                                                     NULL, NULL));
    ASSERT_FALSE(cbm_suppress_external_import_shadow("eq::apply", "unique_name", &arr, keys, nkeys,
                                                     NULL, NULL));
    /* Empty / absent inputs. */
    ASSERT_FALSE(cbm_suppress_external_import_shadow("eq", NULL, &arr, keys, nkeys, NULL, NULL));
    ASSERT_FALSE(cbm_suppress_external_import_shadow("eq", "", &arr, keys, nkeys, NULL, NULL));
    ASSERT_FALSE(
        cbm_suppress_external_import_shadow(NULL, "unique_name", &arr, keys, nkeys, NULL, NULL));
    ASSERT_FALSE(
        cbm_suppress_external_import_shadow("", "unique_name", &arr, keys, nkeys, NULL, NULL));
    ASSERT_FALSE(
        cbm_suppress_external_import_shadow("eq", "unique_name", NULL, keys, nkeys, NULL, NULL));
    PASS();
}

TEST(external_import_shadow_relative_binding_wins_the_tie) {
    /* A name imported from BOTH a package and a relative path keeps its edge,
     * and does so independently of the order the imports were extracted in —
     * the relative binding is an explicit tie-break, not an emergent property
     * of iteration order. */
    CBMImport pkg_first[] = {
        {.local_name = "eq", .module_path = "drizzle-orm"},
        {.local_name = "eq", .module_path = "./text-utils"},
    };
    CBMImport rel_first[] = {
        {.local_name = "eq", .module_path = "./text-utils"},
        {.local_name = "eq", .module_path = "drizzle-orm"},
    };
    CBMImportArray a = {.items = pkg_first, .count = 2, .cap = 2};
    CBMImportArray b = {.items = rel_first, .count = 2, .cap = 2};
    ASSERT_FALSE(cbm_suppress_external_import_shadow("eq", "unique_name", &a, NULL, 0, NULL, NULL));
    ASSERT_FALSE(cbm_suppress_external_import_shadow("eq", "unique_name", &b, NULL, 0, NULL, NULL));
    PASS();
}

TEST(external_import_shadow_windows_relative_specifier_kept) {
    /* #1355 follow-up: a Windows-style specifier —
     * drive-letter absolute (`C:\...` / `C:/...`) or UNC share (`\\server\...`)
     * — names a path inside the indexed tree exactly like a POSIX "./x" or
     * "/x". pr-smoke runs this pipeline on Windows; before this fix these
     * specifiers fell through to "external package" and the same-name guess
     * they'd otherwise validate got suppressed on that platform only. */
    CBMImport imports[] = {
        {.local_name = "eq", .module_path = "C:\\repo\\src\\text-utils.ts"},
        {.local_name = "sql", .module_path = "C:/repo/src/text-utils.ts"},
        {.local_name = "normalize", .module_path = "\\\\server\\share\\text-utils.ts"},
    };
    CBMImportArray arr = {.items = imports, .count = 3, .cap = 3};

    /* None of these materialized an import-map key (as if resolution missed
     * them, mirroring the reported scenario) — the specifier alone must still
     * keep the edge because it is in-tree, not external. RED before the fix
     * (specifier_is_relative saw '.' / '/' only, so all three were classified
     * external and suppressed); GREEN after. */
    ASSERT_FALSE(
        cbm_suppress_external_import_shadow("eq", "unique_name", &arr, NULL, 0, NULL, NULL));
    ASSERT_FALSE(
        cbm_suppress_external_import_shadow("sql", "unique_name", &arr, NULL, 0, NULL, NULL));
    ASSERT_FALSE(
        cbm_suppress_external_import_shadow("normalize", "unique_name", &arr, NULL, 0, NULL, NULL));
    /* A bare package specifier without any drive letter or leading separator
     * (e.g. "drizzle-orm") must still be external — this predicate must not
     * become "anything with a colon or backslash". */
    CBMImport pkg_only[] = {
        {.local_name = "eq", .module_path = "drizzle-orm"},
    };
    CBMImportArray parr = {.items = pkg_only, .count = 1, .cap = 1};
    ASSERT_TRUE(
        cbm_suppress_external_import_shadow("eq", "unique_name", &parr, NULL, 0, NULL, NULL));
    PASS();
}

TEST(external_import_shadow_workspace_sibling_kept) {
    /* #1355 / #1732: inside a workspace monorepo a BARE specifier can still name
     * in-tree code — `import { eq } from "drizzle-orm"` written inside the
     * drizzle-orm repository is a sibling package, not a dependency. Measured on
     * drizzle-team/drizzle-orm at b7862528: without this check the guard removed
     * 2609 CALLS edges, 1377 of which pointed at exactly the file the specifier
     * names. The package map is keyed by each in-tree manifest's `name`, so the
     * tree's own claim decides. */
    CBMImport imports[] = {
        {.local_name = "eq", .module_path = "drizzle-orm"},
        {.local_name = "pgTable", .module_path = "drizzle-orm/pg-core"},
        {.local_name = "hoisted", .module_path = "@scope/kit/sub"},
        {.local_name = "test", .module_path = "vitest"},
    };
    CBMImportArray arr = {.items = imports, .count = 4, .cap = 4};

    CBMHashTable *pkgs = cbm_ht_create(8);
    ASSERT_NOT_NULL(pkgs);
    cbm_ht_set(pkgs, "drizzle-orm", (void *)"qn");
    cbm_ht_set(pkgs, "@scope/kit", (void *)"qn");

    /* Exact name, subpath, and scoped subpath all resolve to an in-tree
     * package — the same-name fallback keeps its chance. */
    ASSERT_FALSE(
        cbm_suppress_external_import_shadow("eq", "unique_name", &arr, NULL, 0, pkgs, NULL));
    ASSERT_FALSE(
        cbm_suppress_external_import_shadow("pgTable", "suffix_match", &arr, NULL, 0, pkgs, NULL));
    ASSERT_FALSE(
        cbm_suppress_external_import_shadow("hoisted", "unique_name", &arr, NULL, 0, pkgs, NULL));
    /* A genuine third-party dependency is untouched by the new check: the tree
     * declares no package called "vitest", so the #1355 removal still happens. */
    ASSERT_TRUE(
        cbm_suppress_external_import_shadow("test", "suffix_match", &arr, NULL, 0, pkgs, NULL));

    /* A tree with no manifests at all (pkgmap NULL) keeps the pre-existing
     * specifier-shape-only contract — every one of these is external again. */
    ASSERT_TRUE(
        cbm_suppress_external_import_shadow("eq", "unique_name", &arr, NULL, 0, NULL, NULL));
    ASSERT_TRUE(
        cbm_suppress_external_import_shadow("pgTable", "suffix_match", &arr, NULL, 0, NULL, NULL));

    /* An unrelated package map must not accidentally match by prefix: "drizzle"
     * is not "drizzle-orm", and the walk is by path segment, not by substring. */
    CBMHashTable *other = cbm_ht_create(8);
    ASSERT_NOT_NULL(other);
    cbm_ht_set(other, "drizzle", (void *)"qn");
    ASSERT_TRUE(
        cbm_suppress_external_import_shadow("eq", "unique_name", &arr, NULL, 0, other, NULL));

    cbm_ht_free(other);
    cbm_ht_free(pkgs);
    PASS();
}

TEST(external_import_shadow_declared_package_kept) {
    /* #1355: package-path specifiers (Java, Kotlin, C#, PHP) carry NO shape
     * that separates in-tree from third-party — `import
     * org.jetbrains.exposed.v1.tests.shared.assertEquals` is spelled exactly
     * like `import kotlin.test.assertEquals`, and neither is "relative". Only
     * the tree's own `package` declarations tell them apart; the manifests the
     * package map reads (pom.xml, build.gradle) name artifacts, not packages.
     * Measured on JetBrains/Exposed at 0e4d81a4: without this check the guard
     * removed 426 CALLS edges, every one of them a real in-project call into
     * the repository's own Assert.kt. */
    CBMImport imports[] = {
        {.local_name = "assertEquals", .module_path = "org.example.tests.shared.assertEquals"},
        {.local_name = "assertEquals", .module_path = "kotlin.test.assertEquals"},
        {.local_name = "Helper", .module_path = "org.example.util.Helper"},
        {.local_name = "URL", .module_path = "java.net.URL"},
        {.local_name = "Logger", .module_path = "App\\Support\\Logger"},
    };
    CBMImportArray arr = {.items = imports, .count = 5, .cap = 5};

    CBMHashTable *nspkgs = cbm_ht_create(8);
    ASSERT_NOT_NULL(nspkgs);
    cbm_ht_set(nspkgs, "org.example.tests.shared", (void *)"qn");
    cbm_ht_set(nspkgs, "org.example.util", (void *)"qn");
    cbm_ht_set(nspkgs, "App.Support", (void *)"qn");

    /* The specifier names a member of a package the tree declares, so the walk
     * strips the member segment and finds it — the call keeps its edge. */
    ASSERT_FALSE(cbm_suppress_external_import_shadow("assertEquals", "suffix_match", &arr, NULL, 0,
                                                     NULL, nspkgs));
    ASSERT_FALSE(
        cbm_suppress_external_import_shadow("Helper", "unique_name", &arr, NULL, 0, NULL, nspkgs));
    /* PHP writes its separator as a backslash; the key normalization has to
     * match the namespace map's, which stores dots. */
    ASSERT_FALSE(
        cbm_suppress_external_import_shadow("Logger", "unique_name", &arr, NULL, 0, NULL, nspkgs));
    /* Negative space, and the effect the census called valuable: `java.net.URL`
     * walks to "java.net" then "java", neither declared by any project file, so
     * a bare `URL` bound to an in-tree URL class is still cut. */
    ASSERT_TRUE(
        cbm_suppress_external_import_shadow("URL", "suffix_match", &arr, NULL, 0, NULL, nspkgs));
    /* NULL restores the previous contract: without the declared-package set
     * every one of these reads as external again. */
    ASSERT_TRUE(cbm_suppress_external_import_shadow("assertEquals", "suffix_match", &arr, NULL, 0,
                                                    NULL, NULL));
    ASSERT_TRUE(
        cbm_suppress_external_import_shadow("Helper", "unique_name", &arr, NULL, 0, NULL, NULL));

    /* The walk is by dotted segment, not by substring: a tree that declares
     * "org.example.tests" must not claim "org.example.testsuite.helper". */
    CBMImport near[] = {
        {.local_name = "near", .module_path = "org.example.testsuite.helper.near"},
    };
    CBMImportArray narr = {.items = near, .count = 1, .cap = 1};
    CBMHashTable *prefix_only = cbm_ht_create(8);
    ASSERT_NOT_NULL(prefix_only);
    cbm_ht_set(prefix_only, "org.example.tests", (void *)"qn");
    ASSERT_TRUE(cbm_suppress_external_import_shadow("near", "unique_name", &narr, NULL, 0, NULL,
                                                    prefix_only));

    cbm_ht_free(prefix_only);
    cbm_ht_free(nspkgs);
    PASS();
}

TEST(external_import_shadow_declared_prefix_is_permissive) {
    /* #1355, known limitation pinned deliberately. The declared-package walk
     * has no floor: once the tree declares `com.example`, a third-party
     * library publishing under `com.example.vendor` reads as in-tree and the
     * guard stands down. The call then keeps exactly the edge it has on main,
     * so this direction costs a fabricated edge the guard could have removed —
     * it never removes a real one, which is the side a call-resolution
     * suppressor has to fail on.
     *
     * This test exists so the behaviour cannot change silently: tightening the
     * walk (stopping at the first type-shaped segment, say) must flip these
     * two assertions, not slip through unnoticed. */
    CBMImport imports[] = {
        {.local_name = "vendorCall", .module_path = "com.example.vendor.pkg.vendorCall"},
    };
    CBMImportArray arr = {.items = imports, .count = 1, .cap = 1};

    CBMHashTable *declares_prefix = cbm_ht_create(8);
    ASSERT_NOT_NULL(declares_prefix);
    cbm_ht_set(declares_prefix, "com.example", (void *)"qn");
    /* The tree declares only the prefix, yet the whole subtree is treated as
     * in-tree — the permissive direction. */
    ASSERT_FALSE(cbm_suppress_external_import_shadow("vendorCall", "unique_name", &arr, NULL, 0,
                                                     NULL, declares_prefix));

    /* Change nothing but the declared package and the guard fires again, which
     * is what proves the previous assertion came from the prefix walk and not
     * from some other clause standing down. */
    CBMHashTable *declares_other = cbm_ht_create(8);
    ASSERT_NOT_NULL(declares_other);
    cbm_ht_set(declares_other, "com.other", (void *)"qn");
    ASSERT_TRUE(cbm_suppress_external_import_shadow("vendorCall", "unique_name", &arr, NULL, 0,
                                                    NULL, declares_other));

    cbm_ht_free(declares_other);
    cbm_ht_free(declares_prefix);
    PASS();
}

TEST(go_suppress_drops_weak_selector_matches) {
    /* Go selector call with an untyped receiver, landed via a receiver-blind
     * short-name strategy → drop (same failure class as #592/#606).
     * suffix_match/fuzzy drop at any confidence; unique_name drops only when
     * import-unreachability-penalized (CONF_UNIQUE_NAME 0.75 * 0.5 = 0.375 —
     * the `io.Copy` -> project `Copy` stdlib-hijack shape). */
    ASSERT_TRUE(cbm_go_suppress_weak_method_match(true, true, "suffix_match", 0.9));
    ASSERT_TRUE(cbm_go_suppress_weak_method_match(true, true, "suffix_match", 0.11));
    ASSERT_TRUE(cbm_go_suppress_weak_method_match(true, true, "fuzzy", 0.9));
    ASSERT_TRUE(cbm_go_suppress_weak_method_match(true, true, "unique_name", 0.375));
    /* field_type_hint is a receiver-NAME substring match, not type evidence
     * (F-G9): weak for Go like the shared list. */
    ASSERT_TRUE(cbm_go_suppress_weak_method_match(true, true, "field_type_hint", 0.85));
    PASS();
}

TEST(go_suppress_keeps_typed_and_import_aware_matches) {
    /* Unpenalized unique_name = lone candidate inside the caller's import
     * closure (or an import-free file, e.g. same-package) — a genuinely-typed
     * lone-candidate call never enters the field-type-hint upgrade, so it must
     * survive (lrp_go_s8_field_type_hint). */
    ASSERT_FALSE(cbm_go_suppress_weak_method_match(true, true, "unique_name", 0.75));
    ASSERT_FALSE(cbm_go_suppress_weak_method_match(true, true, "same_module", 0.9));
    ASSERT_FALSE(cbm_go_suppress_weak_method_match(true, true, "import_map", 0.95));
    ASSERT_FALSE(cbm_go_suppress_weak_method_match(true, true, "import_map_suffix", 0.9));
    ASSERT_FALSE(cbm_go_suppress_weak_method_match(true, true, "qualified_suffix", 0.9));
    ASSERT_FALSE(cbm_go_suppress_weak_method_match(true, true, "callee_suffix", 0.5));
    ASSERT_FALSE(cbm_go_suppress_weak_method_match(true, true, "service_pattern", 0.5));
    ASSERT_FALSE(cbm_go_suppress_weak_method_match(true, true, "lsp_strategy_cross_file", 0.92));
    ASSERT_FALSE(cbm_go_suppress_weak_method_match(true, true, "lsp_direct", 0.95));
    /* A bare call (is_method=false) is a free-function call → never suppressed. */
    ASSERT_FALSE(cbm_go_suppress_weak_method_match(true, false, "suffix_match", 0.11));
    /* Non-Go languages are never affected by this gate. */
    ASSERT_FALSE(cbm_go_suppress_weak_method_match(false, true, "suffix_match", 0.11));
    /* No match (NULL/empty strategy) → nothing to suppress. */
    ASSERT_FALSE(cbm_go_suppress_weak_method_match(true, true, NULL, 0.5));
    ASSERT_FALSE(cbm_go_suppress_weak_method_match(true, true, "", 0.5));
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

/* Python package scope is an explicit resolver input, not a property inferred
 * from a QN shared by other languages. Keep these fixtures separate from the
 * raw public-API tests above. */
static bool registry_scope_match(cbm_resolution_t result, const char *qn, const char *strategy) {
    return qn && strategy && result.qualified_name && result.strategy &&
           strcmp(result.qualified_name, qn) == 0 && strcmp(result.strategy, strategy) == 0 &&
           result.candidate_count == 1;
}

TEST(python_symbol_scope_len_preserves_file_identity) {
    const char *inputs[] = {"proj.pkg.__init__",
                            "proj.__init__",
                            "proj.pkg.__init__.__init__",
                            "proj.pkg.mod",
                            "proj.pkg.x__init__",
                            "__init__",
                            ".__init__",
                            "proj.pkg.index",
                            "",
                            NULL};
    const char *scopes[] = {"proj.pkg",
                            "proj",
                            "proj.pkg.__init__",
                            "proj.pkg.mod",
                            "proj.pkg.x__init__",
                            "__init__",
                            ".__init__",
                            "proj.pkg.index",
                            "",
                            ""};
    bool lengths = true;
    for (size_t i = 0; i < sizeof(inputs) / sizeof(inputs[0]); i++) {
        lengths = cbm_fqn_symbol_scope_len(inputs[i]) == strlen(scopes[i]) && lengths;
    }
    char *module = cbm_pipeline_fqn_module("proj", "pkg/__init__.py");
    char *symbol = cbm_pipeline_fqn_compute("proj", "pkg/__init__.py", "Field");
    bool identities = module && symbol && strcmp(module, "proj.pkg.__init__") == 0 &&
                      strcmp(symbol, "proj.pkg.Field") == 0 &&
                      cbm_fqn_symbol_scope_len(module) == strlen("proj.pkg");
    free(module);
    free(symbol);
    ASSERT_TRUE(lengths && identities);
    PASS();
}

TEST(registry_python_package_scope_is_explicit) {
    cbm_registry_t *r = cbm_registry_new();
    ASSERT_NOT_NULL(r);
    cbm_registry_add(r, "Field", "proj.forms.fields.Field", "Class");
    cbm_registry_add(r, "Field", "proj.db.fields.Field", "Class");
    cbm_registry_add(r, "Field", "proj.db.fields.__init__.Field", "Class");
    cbm_registry_add(r, "Field", "proj.db.plain.Field", "Class");
    cbm_registry_add(r, "Field", "proj.db.index.Field", "Class");
    const char *keys[] = {"forms"};
    const char *vals[] = {"proj.forms"};
    const char *module = "proj.db.fields.__init__";
    bool seeded = cbm_registry_size(r) == 5;
    cbm_resolution_t package =
        cbm_registry_resolve_lang(r, "Field", module, keys, vals, 1, CBM_LANG_PYTHON);
    bool python = registry_scope_match(package, "proj.db.fields.Field", "same_module");
    cbm_resolution_t plain =
        cbm_registry_resolve_lang(r, "Field", "proj.db.plain", keys, vals, 1, CBM_LANG_PYTHON);
    bool normal = registry_scope_match(plain, "proj.db.plain.Field", "same_module");
    cbm_resolution_t raw = cbm_registry_resolve(r, "Field", module, keys, vals, 1);
    bool preserved = registry_scope_match(raw, "proj.db.fields.__init__.Field", "same_module");
    const CBMLanguage raw_languages[] = {CBM_LANG_JAVA, CBM_LANG_GO, CBM_LANG_JAVASCRIPT,
                                         (CBMLanguage)-1, CBM_LANG_COUNT};
    for (size_t i = 0; i < sizeof(raw_languages) / sizeof(raw_languages[0]); i++) {
        cbm_resolution_t result =
            cbm_registry_resolve_lang(r, "Field", module, keys, vals, 1, raw_languages[i]);
        preserved = registry_scope_match(result, raw.qualified_name, "same_module") &&
                    result.confidence == raw.confidence && preserved;
    }
    cbm_resolution_t index =
        cbm_registry_resolve_lang(r, "Field", "proj.db.index", keys, vals, 1, CBM_LANG_JAVASCRIPT);
    bool index_raw = registry_scope_match(index, "proj.db.index.Field", "same_module");
    cbm_registry_free(r);
    ASSERT_TRUE(seeded && python && normal && preserved && index_raw);
    PASS();
}

TEST(registry_python_scope_keeps_import_precedence_and_raw_scoring) {
    cbm_registry_t *r = cbm_registry_new();
    ASSERT_NOT_NULL(r);
    cbm_registry_add(r, "Field", "proj.vendor.Field", "Class");
    cbm_registry_add(r, "Field", "proj.pkg.Field", "Class");
    cbm_registry_add(r, "Field", "proj.pkg.__init__.Field", "Class");
    cbm_registry_add(r, "choose", "proj.pkg.__init__.child.choose", "Function");
    cbm_registry_add(r, "choose", "proj.pkg.peer.choose", "Function");
    const char *keys[] = {"Field"};
    const char *vals[] = {"proj.vendor.Field"};
    bool seeded = cbm_registry_size(r) == 5;
    cbm_resolution_t imported =
        cbm_registry_resolve_lang(r, "Field", "proj.pkg.__init__", keys, vals, 1, CBM_LANG_PYTHON);
    bool precedence = registry_scope_match(imported, "proj.vendor.Field", "import_map");
    cbm_resolution_t raw = cbm_registry_resolve(r, "choose", "proj.pkg.__init__", NULL, NULL, 0);
    cbm_resolution_t python =
        cbm_registry_resolve_lang(r, "choose", "proj.pkg.__init__", NULL, NULL, 0, CBM_LANG_PYTHON);
    bool scoring =
        raw.qualified_name && python.qualified_name && raw.strategy && python.strategy &&
        strcmp(raw.qualified_name, "proj.pkg.__init__.child.choose") == 0 &&
        strcmp(python.qualified_name, raw.qualified_name) == 0 &&
        strcmp(raw.strategy, "suffix_match") == 0 && strcmp(python.strategy, raw.strategy) == 0 &&
        python.confidence == raw.confidence && python.candidate_count == raw.candidate_count;
    cbm_registry_free(r);
    ASSERT_TRUE(seeded && precedence && scoring);
    PASS();
}

TEST(registry_python_scope_cache_modes_and_raw_lineage) {
    cbm_registry_t *r = cbm_registry_new();
    ASSERT_NOT_NULL(r);
    cbm_registry_add(r, "Entry", "proj.pkg.Entry", "Function");
    cbm_registry_add(r, "Entry", "proj.pkg.__init__.Entry", "Function");
    cbm_registry_add(r, "Reject", "proj.pkg.Reject", "Table");
    cbm_registry_add(r, "Reject", "proj.pkg.__init__.Reject", "Function");
    cbm_registry_add(r, "Accept", "proj.pkg.Accept", "Function");
    cbm_registry_add(r, "Accept", "proj.pkg.__init__.Accept", "Table");
    cbm_registry_add(r, "Relation", "proj.pkg.Relation", "View");
    cbm_registry_add(r, "Relation", "proj.pkg.__init__.Relation", "Table");
    bool seeded = cbm_registry_size(r) == 8;
    bool correct = true;
    /* Each cache lifetime has one fixed module/import map. Alternate modes in
     * both starting orders; repeated same-mode requests also exercise hits. */
    for (int first_python = 0; first_python < 2; first_python++) {
        cbm_registry_resolve_cache_begin(8);
        for (int turn = 0; turn < 16; turn++) {
            bool python = ((turn / 2 + first_python) % 2) != 0;
            const char *names[] = {"Entry", "Reject", "Accept", "Relation", "Missing"};
            for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); i++) {
                cbm_resolution_t result =
                    python ? cbm_registry_resolve_lang(r, names[i], "proj.pkg.__init__", NULL, NULL,
                                                       0, CBM_LANG_PYTHON)
                           : cbm_registry_resolve(r, names[i], "proj.pkg.__init__", NULL, NULL, 0);
                const char *expected = NULL;
                if (i == 0) {
                    expected = python ? "proj.pkg.Entry" : "proj.pkg.__init__.Entry";
                } else if (i == 1 && !python) {
                    expected = "proj.pkg.__init__.Reject";
                } else if (i == 2 && python) {
                    expected = "proj.pkg.Accept";
                }
                correct =
                    (expected ? registry_scope_match(result, expected, "same_module")
                              : result.qualified_name == NULL && result.candidate_count == 0) &&
                    correct;
            }
            /* The cached relation veto must never poison the uncached SQL API,
             * nor may the SQL result bypass the next default cached veto. */
            cbm_resolution_t lineage =
                cbm_registry_resolve_lineage(r, "Relation", "proj.pkg.__init__", NULL, NULL, 0);
            correct = registry_scope_match(lineage, "proj.pkg.__init__.Relation", "same_module") &&
                      correct;
            cbm_resolution_t veto = cbm_registry_resolve_lang(r, "Relation", "proj.pkg.__init__",
                                                              NULL, NULL, 0, CBM_LANG_PYTHON);
            correct = veto.qualified_name == NULL && veto.candidate_count == 0 && correct;
        }
        cbm_registry_resolve_cache_end();
    }
    cbm_registry_free(r);
    ASSERT_TRUE(seeded && correct);
    PASS();
}

static const char *registry_scope_canonical(cbm_registry_t *r, const char *name, const char *qn) {
    const char **candidates = NULL;
    int count = 0;
    cbm_registry_find_by_name(r, name, &candidates, &count);
    for (int i = 0; i < count; i++) {
        if (strcmp(candidates[i], qn) == 0) {
            return candidates[i];
        }
    }
    return NULL;
}

TEST(registry_python_scope_complete_candidate_boundaries) {
    const size_t lengths[] = {511, 512, 513, 1536};
    bool complete = true;
    for (size_t i = 0; i < sizeof(lengths) / sizeof(lengths[0]); i++) {
        cbm_registry_t *r = cbm_registry_new();
        if (!r) {
            complete = false;
            break;
        }
        char scope[2048];
        char module[2048];
        char target[2048];
        const char *name = "LongTarget";
        size_t scope_len = lengths[i] - strlen(name) - 1;
        memcpy(scope, "proj.", 5);
        memset(scope + 5, 'a', scope_len - 5);
        scope[scope_len] = '\0';
        int mn = snprintf(module, sizeof(module), "%s.__init__", scope);
        int tn = snprintf(target, sizeof(target), "%s.%s", scope, name);
        bool shape = mn > 0 && (size_t)mn < sizeof(module) && tn > 0 && (size_t)tn == lengths[i] &&
                     (size_t)tn < sizeof(target);
        cbm_registry_add(r, name, target, "Function");
        if (lengths[i] > 511) {
            char truncated[512];
            memcpy(truncated, target, sizeof(truncated) - 1);
            truncated[sizeof(truncated) - 1] = '\0';
            cbm_registry_add(r, "truncated_decoy", truncated, "Function");
        }
        const char *canonical = registry_scope_canonical(r, name, target);
        cbm_resolution_t python =
            cbm_registry_resolve_lang(r, name, module, NULL, NULL, 0, CBM_LANG_PYTHON);
        cbm_resolution_t normal = cbm_registry_resolve(r, name, scope, NULL, NULL, 0);
        complete = shape && canonical && python.qualified_name == canonical &&
                   normal.qualified_name == canonical &&
                   registry_scope_match(python, target, "same_module") &&
                   registry_scope_match(normal, target, "same_module") && complete;
        cbm_registry_free(r);
    }
    ASSERT_TRUE(complete);
    PASS();
}

TEST(registry_python_scope_qualified_terminal_bucket_uncapped) {
    cbm_registry_t *r = cbm_registry_new();
    ASSERT_NOT_NULL(r);
    /* The exact targets are inserted after the heuristic limit. A long exact
     * comparison must use the canonical terminal bucket, not "Owner.perform"
     * as the bucket key, and must not stop at 256 candidates. */
    bool shape = true;
    for (int i = 0; i < 300; i++) {
        char qn[80];
        int n = snprintf(qn, sizeof(qn), "proj.noise%d.perform", i);
        shape = n > 0 && (size_t)n < sizeof(qn) && shape;
        cbm_registry_add(r, "perform", qn, "Method");
    }
    char scope[1024];
    memcpy(scope, "proj.", 5);
    memset(scope + 5, 'b', 900);
    scope[905] = '\0';
    char module[1024];
    int mn = snprintf(module, sizeof(module), "%s.__init__", scope);
    shape = mn > 0 && (size_t)mn < sizeof(module) && shape;
    const char *names[] = {"Owner.perform", "Owner::perform"};
    bool resolved = true;
    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); i++) {
        char target[1024];
        char prefixed[80];
        int tn = snprintf(target, sizeof(target), "%s.%s", scope, names[i]);
        int pn = snprintf(prefixed, sizeof(prefixed), "receiver.%s", names[i]);
        shape = tn > 0 && (size_t)tn < sizeof(target) && pn > 0 && (size_t)pn < sizeof(prefixed) &&
                shape;
        cbm_registry_add(r, "perform", target, "Method");
        const char *canonical = registry_scope_canonical(r, "perform", target);
        cbm_resolution_t direct =
            cbm_registry_resolve_lang(r, names[i], module, NULL, NULL, 0, CBM_LANG_PYTHON);
        cbm_resolution_t suffix =
            cbm_registry_resolve_lang(r, prefixed, module, NULL, NULL, 0, CBM_LANG_PYTHON);
        resolved = canonical && direct.qualified_name == canonical &&
                   suffix.qualified_name == canonical &&
                   registry_scope_match(direct, target, "same_module") &&
                   registry_scope_match(suffix, target, "same_module") && resolved;
    }
    bool seeded = cbm_registry_size(r) == 302;
    cbm_registry_free(r);
    ASSERT_TRUE(shape && seeded && resolved);
    PASS();
}

/* #1548: a Python def or class declared in a function body is a local of it.
 * It is registered for exact lookups but kept out of the short-name index, so
 * no weak strategy offers it to a caller elsewhere. A class nested in a class
 * (proj.m.Outer.Inner) is an attribute, not a local, and stays nameable; so
 * does the same QN shape from another language. */
TEST(registry_python_function_local_is_exact_only_issue1548) {
    cbm_registry_t *r = cbm_registry_new();
    const size_t web = strlen("proj.web"); /* symbol prefix of web.py */
    static const char *const defs[][3] = {
        {"create_app", "proj.web.create_app", "Function"},
        {"index", "proj.web.create_app.index", "Function"},
        {"Settings", "proj.web.create_app.Settings", "Class"},
        {"value", "proj.web.create_app.Settings.value", "Method"},
        {"Outer", "proj.web.Outer", "Class"},
        {"Inner", "proj.web.Outer.Inner", "Class"},
    };
    for (size_t i = 0; i < sizeof(defs) / sizeof(defs[0]); i++) {
        cbm_registry_add_lang_in_module(r, defs[i][0], defs[i][1], defs[i][2], CBM_LANG_PYTHON,
                                        web);
    }
    cbm_registry_add_lang_in_module(r, "f", "proj.ts.f", "Function", CBM_LANG_TYPESCRIPT,
                                    strlen("proj.ts"));
    cbm_registry_add_lang_in_module(r, "g", "proj.ts.f.g", "Function", CBM_LANG_TYPESCRIPT,
                                    strlen("proj.ts"));

    static const char *const local[] = {"proj.web.create_app.index", "proj.web.create_app.Settings",
                                        "proj.web.create_app.Settings.value"};
    for (int i = 0; i < 3; i++) {
        ASSERT_TRUE(cbm_registry_exists(r, local[i]));
        ASSERT_TRUE(cbm_registry_qn_is_function_local(r, local[i], web));
        const char *simple = strrchr(local[i], '.') + 1;
        const char **out = NULL;
        int count = -1;
        cbm_registry_find_by_name(r, simple, &out, &count);
        ASSERT_EQ(count, 0);
    }
    static const char *const nameable[][2] = {{"create_app", "proj.web.create_app"},
                                              {"Inner", "proj.web.Outer.Inner"},
                                              {"g", "proj.ts.f.g"}};
    for (int i = 0; i < 3; i++) {
        const char **out = NULL;
        int count = 0;
        cbm_registry_find_by_name(r, nameable[i][0], &out, &count);
        ASSERT_EQ(count, 1);
        ASSERT_STR_EQ(out[0], nameable[i][1]);
    }
    ASSERT_FALSE(cbm_registry_qn_is_function_local(r, "proj.web.Outer.Inner", web));
    /* No module prefix: no local detection. */
    ASSERT_FALSE(cbm_registry_qn_is_function_local(r, "proj.web.create_app.index", 0));
    /* A weak strategy cannot reach the local; before, index() anywhere in
     * the project resolved to it by unique_name. */
    cbm_resolution_t res = cbm_registry_resolve(r, "index", "proj.other", NULL, NULL, 0);
    ASSERT_NULL(res.qualified_name);
    cbm_registry_free(r);
    PASS();
}

/* Python LEGB for bare names: the caller's own def first, then each enclosing
 * def outward; a class body is not a scope its methods see; the module is left
 * to the other strategies. A dotted callee is not a bare name. */
TEST(registry_resolve_lexical_walks_enclosing_defs_issue1548) {
    cbm_registry_t *r = cbm_registry_new();
    static const char *const defs[][3] = {
        {"Tree", "proj.m.Tree", "Class"},
        {"visit", "proj.m.Tree.visit", "Method"},
        {"visit", "proj.m.Tree.visit.visit", "Function"},
        {"helper", "proj.m.Tree.helper", "Method"},
        {"factory", "proj.m.factory", "Function"},
        {"helper", "proj.m.factory.helper", "Function"},
        {"Local", "proj.m.factory.Local", "Class"},
        {"run", "proj.m.factory.Local.run", "Method"},
        {"top", "proj.m.top", "Function"},
    };
    for (size_t i = 0; i < sizeof(defs) / sizeof(defs[0]); i++) {
        cbm_registry_add_lang_in_module(r, defs[i][0], defs[i][1], defs[i][2], CBM_LANG_PYTHON,
                                        strlen("proj.m"));
    }

    /* In method Tree.visit, `visit` is its nested def; so it is inside that
     * nested def (its own recursion). */
    cbm_resolution_t res = cbm_registry_resolve_lexical(r, "visit", "proj.m.Tree.visit", "proj.m");
    ASSERT_STR_EQ(res.qualified_name, "proj.m.Tree.visit.visit");
    ASSERT_STR_EQ(res.strategy, "lexical_scope");
    res = cbm_registry_resolve_lexical(r, "visit", "proj.m.Tree.visit.visit", "proj.m");
    ASSERT_STR_EQ(res.qualified_name, "proj.m.Tree.visit.visit");
    /* The class body is skipped: Tree.helper is not a bare name in a method. */
    res = cbm_registry_resolve_lexical(r, "helper", "proj.m.Tree.visit.visit", "proj.m");
    ASSERT_NULL(res.qualified_name);
    /* A method of a local class sees the def enclosing its class. */
    res = cbm_registry_resolve_lexical(r, "helper", "proj.m.factory.Local.run", "proj.m");
    ASSERT_STR_EQ(res.qualified_name, "proj.m.factory.helper");
    res = cbm_registry_resolve_lexical(r, "Local", "proj.m.factory", "proj.m");
    ASSERT_STR_EQ(res.qualified_name, "proj.m.factory.Local");
    /* Module-level names and dotted callees are not this strategy's. */
    res = cbm_registry_resolve_lexical(r, "top", "proj.m.factory", "proj.m");
    ASSERT_NULL(res.qualified_name);
    res = cbm_registry_resolve_lexical(r, "self.helper", "proj.m.factory", "proj.m");
    ASSERT_NULL(res.qualified_name);
    res = cbm_registry_resolve_lexical(r, "helper", "proj.m", "proj.m");
    ASSERT_NULL(res.qualified_name);
    cbm_registry_free(r);
    PASS();
}

/* A package __init__.py function can share a submodule's QN: poetry's
 * layouts/__init__.py `def layout` is proj.pkg.layout, the QN prefix of every
 * symbol in layouts/layout.py. Bounded at its own file, class Layout and its
 * methods are no locals of that function, in either registration order (full
 * index: discovery order; incremental: node-id order), and the lexical walk of
 * a caller in layout.py never enters the __init__ function. */
TEST(registry_python_local_bound_at_own_file_issue1548) {
    const size_t init_prefix = strlen("proj.pkg");      /* pkg/__init__.py */
    const size_t mod_prefix = strlen("proj.pkg.layout"); /* pkg/layout.py */
    for (int order = 0; order < 2; order++) {
        cbm_registry_t *r = cbm_registry_new();
        if (order == 0) {
            cbm_registry_add_lang_in_module(r, "layout", "proj.pkg.layout", "Function",
                                            CBM_LANG_PYTHON, init_prefix);
        }
        cbm_registry_add_lang_in_module(r, "Layout", "proj.pkg.layout.Layout", "Class",
                                        CBM_LANG_PYTHON, mod_prefix);
        cbm_registry_add_lang_in_module(r, "create", "proj.pkg.layout.Layout.create", "Method",
                                        CBM_LANG_PYTHON, mod_prefix);
        cbm_registry_add_lang_in_module(r, "make", "proj.pkg.layout.make", "Function",
                                        CBM_LANG_PYTHON, mod_prefix);
        if (order == 1) {
            cbm_registry_add_lang_in_module(r, "layout", "proj.pkg.layout", "Function",
                                            CBM_LANG_PYTHON, init_prefix);
        }
        static const char *const nameable[][2] = {{"Layout", "proj.pkg.layout.Layout"},
                                                  {"create", "proj.pkg.layout.Layout.create"},
                                                  {"make", "proj.pkg.layout.make"}};
        for (size_t i = 0; i < sizeof(nameable) / sizeof(nameable[0]); i++) {
            const char **out = NULL;
            int count = 0;
            cbm_registry_find_by_name(r, nameable[i][0], &out, &count);
            ASSERT_EQ(count, 1);
            ASSERT_STR_EQ(out[0], nameable[i][1]);
            ASSERT_FALSE(cbm_registry_qn_is_function_local(r, nameable[i][1], mod_prefix));
        }
        /* `Layout()` in layout.py's make(): no lexical local; module strategies own it. */
        cbm_resolution_t res =
            cbm_registry_resolve_lexical(r, "Layout", "proj.pkg.layout.make", "proj.pkg.layout");
        ASSERT_NULL(res.qualified_name);
        cbm_registry_free(r);
    }
    PASS();
}

/* The prefix every def of a file carries, without allocating; the same as
 * cbm_pipeline_fqn_compute(project, path, name) minus ".name". */
TEST(fqn_symbol_prefix_len_matches_fqn_compute) {
    static const char *const paths[] = {"pkg/layout.py", "pkg/__init__.py", "a.py",
                                        "deep/x/y/mod.py", "web/index.py", "noext"};
    for (size_t i = 0; i < sizeof(paths) / sizeof(paths[0]); i++) {
        char *qn = cbm_pipeline_fqn_compute("proj", paths[i], "sym");
        ASSERT_NOT_NULL(qn);
        size_t want = strlen(qn) - strlen(".sym");
        free(qn);
        ASSERT_EQ(cbm_pipeline_fqn_symbol_prefix_len("proj", paths[i]), want);
    }
    ASSERT_EQ(cbm_pipeline_fqn_symbol_prefix_len("proj", "pkg/__init__.py"), strlen("proj.pkg"));
    ASSERT_EQ(cbm_pipeline_fqn_symbol_prefix_len(NULL, "a.py"), 0);
    PASS();
}

SUITE(registry) {
    RUN_TEST(python_symbol_scope_len_preserves_file_identity);
    RUN_TEST(registry_python_package_scope_is_explicit);
    RUN_TEST(registry_python_scope_keeps_import_precedence_and_raw_scoring);
    RUN_TEST(registry_python_scope_cache_modes_and_raw_lineage);
    RUN_TEST(registry_python_scope_complete_candidate_boundaries);
    RUN_TEST(registry_python_scope_qualified_terminal_bucket_uncapped);
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
    RUN_TEST(registry_value_target_field_twin);
    RUN_TEST(registry_find_by_name);
    RUN_TEST(registry_no_duplicates);
    /* Resolution */
    RUN_TEST(resolve_same_module);
    RUN_TEST(resolve_qualified_disambiguates_same_name);
    RUN_TEST(resolve_qualified_ambiguous_tail_falls_through);
    RUN_TEST(registry_indexes_by_passed_name_not_qn_tail);
    RUN_TEST(registry_indexes_a_dotted_name_under_its_tail_too);
    RUN_TEST(resolve_import_map);
    RUN_TEST(resolve_import_map_bare_function);
    RUN_TEST(resolve_import_map_bare_alias);
    RUN_TEST(resolve_import_map_aliased_from_import);
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
    RUN_TEST(reach_cache_memo_matches_uncached_across_files);
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
    RUN_TEST(cross_language_suffix_match_drops_py_vs_js);
    RUN_TEST(lang_resolution_compatible_families);
    RUN_TEST(name_guess_vetoed_across_languages);
    RUN_TEST(same_module_vetoed_across_languages);
    RUN_TEST(cross_language_config_caller_drops_unique_name_too);
    RUN_TEST(registry_tie_break_is_independent_of_registration_order);
    RUN_TEST(registry_tie_prefers_callable_over_property);
    RUN_TEST(registry_tie_ignores_nesting_depth);
    RUN_TEST(registry_tie_prefers_csharp_implementation_over_ref_stub);
    RUN_TEST(cross_language_ref_drops_go_vs_c);
    RUN_TEST(go_bare_ref_never_binds_field);
    RUN_TEST(c_bare_ref_never_binds_field);
    RUN_TEST(call_onto_field_follows_language_and_shape);
    RUN_TEST(c_bare_call_does_not_weigh_field_namesakes);
    RUN_TEST(c_arrow_member_call_is_c_family_and_arrow_only);
    RUN_TEST(unique_field_qn_needs_exactly_one_field_of_that_name);
    RUN_TEST(c_member_access_binds_by_owner_only);
    RUN_TEST(c_member_rule_covers_c_files_and_every_dot_h);
    RUN_TEST(java_call_never_binds_data_member);
    RUN_TEST(generated_member_registers_by_qn_only);
    RUN_TEST(generated_member_mark_on_defs_and_nodes);
    RUN_TEST(dynamic_suppress_drops_weak_method_matches);
    RUN_TEST(dynamic_suppress_keeps_high_confidence_and_non_methods);
    RUN_TEST(python_builtin_member_table_matches_builtin_type_methods);
    RUN_TEST(weak_member_unique_name_exempt_is_python_self_rooted_unique_and_specific_only);
    RUN_TEST(local_binding_suppress_drops_weak_shadowed_bare_calls);
    RUN_TEST(local_binding_suppress_keeps_unshadowed_and_strong_strategies);
    RUN_TEST(import_binding_suppress_drops_only_weak_contradicted_calls);
    RUN_TEST(python_import_binding_contradicts_only_foreign_chains);
    RUN_TEST(python_external_base_contradicts_django_shapes);
    RUN_TEST(weak_call_guards_share_one_drop_list);
    RUN_TEST(external_import_shadow_drops_package_bound_bare_call);
    RUN_TEST(external_import_shadow_keeps_everything_else);
    RUN_TEST(external_import_shadow_relative_binding_wins_the_tie);
    RUN_TEST(external_import_shadow_windows_relative_specifier_kept);
    RUN_TEST(external_import_shadow_workspace_sibling_kept);
    RUN_TEST(external_import_shadow_declared_package_kept);
    RUN_TEST(external_import_shadow_declared_prefix_is_permissive);
    RUN_TEST(go_suppress_drops_weak_selector_matches);
    RUN_TEST(go_suppress_keeps_typed_and_import_aware_matches);
    RUN_TEST(registry_python_function_local_is_exact_only_issue1548);
    RUN_TEST(registry_resolve_lexical_walks_enclosing_defs_issue1548);
    RUN_TEST(registry_python_local_bound_at_own_file_issue1548);
    RUN_TEST(fqn_symbol_prefix_len_matches_fqn_compute);
}
