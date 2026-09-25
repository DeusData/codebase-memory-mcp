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

/* The other direction of the same disagreement: a name may carry segments that
 * the QN's tail drops, and then the QN tail is the key callers actually spell.
 *
 * Two shapes, both taken from what the extractors emit today. An HCL block is
 * named "resource.aws_instance.web" by find_hcl_block_name while its QN tail
 * is bare "web", which is how a `aws_instance.web.id` reference reaches it. A
 * file's Module node is named with the basename INCLUDING its extension in
 * every language, while its QN tail is the extensionless stem
 * a bare module reference is written as.
 *
 * Keying the index on the passed name ALONE loses both lookups, so the index
 * carries both keys. */
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

/* An arity fence is a discriminator, not part of the dotted tail the callee
 * text names. A qualified callee must therefore still find a fenced candidate,
 * and the call site's argument count is what chooses among the arities. */
TEST(resolve_qualified_suffix_sees_through_arity_fence) {
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add(r, "fetch", "proj.lib.store.Fx.Store.fetch#1", "Function");
    cbm_registry_add(r, "fetch", "proj.lib.store.Fx.Store.fetch#3", "Function");
    cbm_registry_add(r, "fetch", "proj.lib.other.Fx.Other.fetch#1", "Function");

    cbm_resolve_ctx_t one = {.container_qn = NULL, .arity = 1};
    cbm_resolution_t a = cbm_registry_resolve_ctx(r, "Fx.Store.fetch", "proj.lib.caller", &one,
                                                  NULL, NULL, 0);
    ASSERT_STR_EQ(a.qualified_name, "proj.lib.store.Fx.Store.fetch#1");

    cbm_resolve_ctx_t three = {.container_qn = NULL, .arity = 3};
    cbm_resolution_t b = cbm_registry_resolve_ctx(r, "Fx.Store.fetch", "proj.lib.caller", &three,
                                                  NULL, NULL, 0);
    ASSERT_STR_EQ(b.qualified_name, "proj.lib.store.Fx.Store.fetch#3");

    /* An arity no candidate carries cannot pick one of the two Fx.Store
     * arities, so the qualified strategy declines rather than guessing. */
    cbm_resolve_ctx_t nine = {.container_qn = NULL, .arity = 9};
    cbm_resolution_t c =
        cbm_registry_resolve_ctx(r, "Fx.Store.fetch", "proj.lib.caller", &nine, NULL, NULL, 0);
    ASSERT_TRUE(!c.strategy || strcmp(c.strategy, "qualified_suffix") != 0);

    cbm_registry_free(r);
    PASS();
}

/* Where a language names its container in the source, the FILE QN is not the
 * container, so an intra-container call cannot be found by the same-module
 * strategy without the caller's own container. Without this it degraded to the
 * bare-name scorer at roughly half the confidence — quietly, because the
 * scorer often still lands on a right-looking answer. */
TEST(resolve_same_module_uses_caller_container) {
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add(r, "fetch", "proj.lib.store.Fx.Store.fetch#1", "Function");
    cbm_registry_add(r, "fetch", "proj.lib.store.Fx.Store.fetch#2", "Function");
    cbm_registry_add(r, "fetch", "proj.lib.other.Fx.Other.fetch#1", "Function");

    cbm_resolve_ctx_t rx = {.container_qn = "proj.lib.store.Fx.Store", .arity = 2};
    cbm_resolution_t res =
        cbm_registry_resolve_ctx(r, "fetch", "proj.lib.store", &rx, NULL, NULL, 0);
    ASSERT_STR_EQ(res.qualified_name, "proj.lib.store.Fx.Store.fetch#2");
    ASSERT_STR_EQ(res.strategy, "same_module");

    /* Without the container the file QN alone cannot compose the candidate. */
    cbm_resolution_t bare =
        cbm_registry_resolve(r, "fetch", "proj.lib.store", NULL, NULL, 0);
    ASSERT_TRUE(!bare.strategy || strcmp(bare.strategy, "same_module") != 0);

    cbm_registry_free(r);
    PASS();
}

/* Resolution must be MONOTONE: registering an unrelated symbol may not
 * re-point an already-resolved edge. It did, because a qualified callee
 * arrived as a bare name — one candidate resolved by unique_name, and a second
 * same-named candidate anywhere in the tree flipped the strategy to
 * proximity scoring, which picked by path distance. */
TEST(resolve_qualified_is_monotone_under_unrelated_additions) {
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add(r, "get", "proj.lib.probe.Fx.Probe.get#1", "Function");

    cbm_resolution_t before =
        cbm_registry_resolve(r, "Fx.Probe.get", "proj.lib.store", NULL, NULL, 0);
    ASSERT_STR_EQ(before.qualified_name, "proj.lib.probe.Fx.Probe.get#1");

    /* One unrelated file, in a module the call never names. */
    cbm_registry_add(r, "get", "proj.lib.zz_collide.Fx.Collide.get#1", "Function");
    cbm_registry_resolve_cache_end();

    cbm_resolution_t after =
        cbm_registry_resolve(r, "Fx.Probe.get", "proj.lib.store", NULL, NULL, 0);
    ASSERT_STR_EQ(after.qualified_name, "proj.lib.probe.Fx.Probe.get#1");

    cbm_registry_free(r);
    PASS();
}

/* A qualified callee naming a container that holds no such symbol must resolve
 * to nothing rather than to an unrelated local of the same bare name. This is
 * the `Keyword.get(opts, :reason)` case: the receiver chain is the only
 * evidence that the project's own get/1 is the wrong answer. */
TEST(resolve_foreign_receiver_does_not_bind_local_name) {
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add(r, "get", "proj.lib.probe.Fx.Probe.get#1", "Function");
    cbm_registry_add(r, "put", "proj.lib.probe.Fx.Probe.put#3", "Function");

    cbm_resolution_t kw = cbm_registry_resolve(r, "Keyword.get", "proj.lib.store", NULL, NULL, 0);
    ASSERT_TRUE(!kw.qualified_name || kw.qualified_name[0] == '\0');

    cbm_resolution_t mp = cbm_registry_resolve(r, "Map.put", "proj.lib.store", NULL, NULL, 0);
    ASSERT_TRUE(!mp.qualified_name || mp.qualified_name[0] == '\0');

    /* The genuinely qualified call still resolves. */
    cbm_resolution_t ok =
        cbm_registry_resolve(r, "Fx.Probe.get", "proj.lib.store", NULL, NULL, 0);
    ASSERT_STR_EQ(ok.qualified_name, "proj.lib.probe.Fx.Probe.get#1");

    cbm_registry_free(r);
    PASS();
}

/* The same-module suffix fallback composes its candidate from the caller's
 * module and the callee's SUFFIX, discarding the qualifier the call site wrote.
 * Routing it through receiver_chain_admits is NOT gated on the new context
 * argument: cbm_registry_resolve reaches it too, so every language that writes
 * receiver chains is covered. Nothing here carries an arity fence and no
 * context is passed, so the container and arity machinery is off by
 * construction and the guard is the only thing under test. */
TEST(resolve_receiver_guard_is_language_agnostic) {
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add(r, "parse", "proj.com.acme.Order.parse", "Method");
    cbm_registry_add(r, "render", "proj.app.views.render", "Function");
    cbm_registry_add(r, "max", "proj.web.cart.max", "Function");

    /* Java: java.time.LocalDate.parse is not Order's own parse. */
    cbm_resolution_t jv =
        cbm_registry_resolve(r, "LocalDate.parse", "proj.com.acme.Order", NULL, NULL, 0);
    ASSERT_TRUE(!jv.qualified_name || jv.qualified_name[0] == '\0');

    /* Python: a Template's render is not the module's own render. */
    cbm_resolution_t py =
        cbm_registry_resolve(r, "Template.render", "proj.app.views", NULL, NULL, 0);
    ASSERT_TRUE(!py.qualified_name || py.qualified_name[0] == '\0');

    /* JavaScript: Math.max is not the cart's own max. */
    cbm_resolution_t js = cbm_registry_resolve(r, "Math.max", "proj.web.cart", NULL, NULL, 0);
    ASSERT_TRUE(!js.qualified_name || js.qualified_name[0] == '\0');

    /* A chain segment naming the candidate's own ancestry still binds, so the
     * guard withholds a foreign receiver rather than every qualified callee. */
    cbm_resolution_t own =
        cbm_registry_resolve(r, "Order.parse", "proj.com.acme.Order", NULL, NULL, 0);
    ASSERT_STR_EQ(own.qualified_name, "proj.com.acme.Order.parse");

    /* Carve-out: a lower-case root names a value whose type the chain does not
     * show, so it passes through unchanged. */
    cbm_registry_add(r, "close", "proj.io.handle.close", "Function");
    cbm_resolution_t lc = cbm_registry_resolve(r, "file.close", "proj.io.handle", NULL, NULL, 0);
    ASSERT_STR_EQ(lc.qualified_name, "proj.io.handle.close");

    /* Carve-out: an ALL_CAPS root with an underscore is a constant holding a
     * value, not a type. */
    cbm_registry_add(r, "lower", "proj.iso.codes.lower", "Function");
    cbm_resolution_t cc =
        cbm_registry_resolve(r, "ISO_4217_URL.lower", "proj.iso.codes", NULL, NULL, 0);
    ASSERT_STR_EQ(cc.qualified_name, "proj.iso.codes.lower");

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
 * The tie is now a function of the candidate set: the least nested
 * definition, then the smaller QN — whichever order the registry was built
 * in (O9). */
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

    /* Equal depth: the smaller QN, from either order. */
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

    /* Proximity still outranks depth: the sibling wins over a shallower stranger. */
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
    ASSERT_TRUE(cbm_go_suppress_bare_field_ref(true, false, "Field"));
    /* The member half of a selector may bind a field. */
    ASSERT_FALSE(cbm_go_suppress_bare_field_ref(true, true, "Field"));
    /* Bare references to non-fields are untouched. */
    ASSERT_FALSE(cbm_go_suppress_bare_field_ref(true, false, "Variable"));
    ASSERT_FALSE(cbm_go_suppress_bare_field_ref(true, false, "Function"));
    /* Other languages reference their own members bare inside methods —
     * never suppressed (cp_reads_writes_cs_static_field pins the C# shape). */
    ASSERT_FALSE(cbm_go_suppress_bare_field_ref(false, false, "Field"));
    /* Degenerate input → nothing to judge. */
    ASSERT_FALSE(cbm_go_suppress_bare_field_ref(true, false, NULL));
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
    }
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
    RUN_TEST(registry_indexes_by_passed_name_not_qn_tail);
    RUN_TEST(registry_indexes_a_dotted_name_under_its_tail_too);
    RUN_TEST(resolve_qualified_suffix_sees_through_arity_fence);
    RUN_TEST(resolve_same_module_uses_caller_container);
    RUN_TEST(resolve_qualified_is_monotone_under_unrelated_additions);
    RUN_TEST(resolve_foreign_receiver_does_not_bind_local_name);
    RUN_TEST(resolve_receiver_guard_is_language_agnostic);
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
    RUN_TEST(cross_language_config_caller_drops_unique_name_too);
    RUN_TEST(registry_tie_break_is_independent_of_registration_order);
    RUN_TEST(cross_language_ref_drops_go_vs_c);
    RUN_TEST(go_bare_ref_never_binds_field);
    RUN_TEST(dynamic_suppress_drops_weak_method_matches);
    RUN_TEST(dynamic_suppress_keeps_high_confidence_and_non_methods);
    RUN_TEST(python_builtin_member_table_matches_builtin_type_methods);
    RUN_TEST(weak_member_unique_name_exempt_is_python_self_rooted_unique_and_specific_only);
    RUN_TEST(local_binding_suppress_drops_weak_shadowed_bare_calls);
    RUN_TEST(local_binding_suppress_keeps_unshadowed_and_strong_strategies);
    RUN_TEST(weak_call_guards_share_one_drop_list);
}
